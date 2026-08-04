package dev.riquet;

import com.google.protobuf.StringValue;
import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;
import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.entities.SchemaReference;
import io.confluent.kafka.schemaregistry.protobuf.ProtobufSchema;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import io.confluent.kafka.serializers.json.KafkaJsonSchemaDeserializer;
import io.confluent.kafka.serializers.json.KafkaJsonSchemaSerializer;
import io.confluent.kafka.serializers.protobuf.KafkaProtobufDeserializer;
import io.confluent.kafka.serializers.protobuf.KafkaProtobufSerializer;
import io.confluent.connect.avro.AvroConverter;
import java.util.LinkedHashMap;
import java.util.Map;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;

public final class ClientInterop {
  private ClientInterop() {}

  public static void main(String[] args) {
    String registry = System.getenv("RIQUET_REGISTRY_URL");
    if (registry == null || registry.isBlank()) {
      throw new IllegalArgumentException("RIQUET_REGISTRY_URL is required");
    }
    Map<String, Object> config = new LinkedHashMap<>();
    config.put("schema.registry.url", registry);
    verifyAvro(config);
    verifyProtobuf(config);
    verifyProtobufReference(config);
    verifyJson(config);
    verifyKafkaConnect(config);
    System.out.println("Confluent Java Avro, Protobuf, and JSON Schema SerDes passed");
  }

  private static void verifyAvro(Map<String, Object> config) {
    Schema schema = new Schema.Parser().parse("""
        {"type":"record","name":"JavaAvro","namespace":"dev.riquet",
         "fields":[{"name":"value","type":"string"}]}
        """);
    GenericRecord input = new GenericData.Record(schema);
    input.put("value", "avro-value");
    KafkaAvroSerializer serializer = new KafkaAvroSerializer();
    KafkaAvroDeserializer deserializer = new KafkaAvroDeserializer();
    serializer.configure(config, false);
    deserializer.configure(config, false);
    byte[] wire = serializer.serialize("java-avro", input);
    assertEnvelope(wire);
    Object output = deserializer.deserialize("java-avro", wire);
    if (!(output instanceof GenericRecord record) || !"avro-value".equals(String.valueOf(record.get("value")))) {
      throw new AssertionError("Avro round trip failed: " + output);
    }
    serializer.close();
    deserializer.close();
  }

  private static void verifyProtobuf(Map<String, Object> base) {
    Map<String, Object> config = new LinkedHashMap<>(base);
    config.put("specific.protobuf.value.type", StringValue.class.getName());
    KafkaProtobufSerializer<StringValue> serializer = new KafkaProtobufSerializer<>();
    KafkaProtobufDeserializer<StringValue> deserializer = new KafkaProtobufDeserializer<>();
    serializer.configure(config, false);
    deserializer.configure(config, false);
    StringValue input = StringValue.of("protobuf-value");
    byte[] wire = serializer.serialize("java-protobuf", input);
    assertEnvelope(wire);
    StringValue output = deserializer.deserialize("java-protobuf", wire);
    if (!input.equals(output)) {
      throw new AssertionError("Protobuf round trip failed: " + output);
    }
    serializer.close();
    deserializer.close();
  }

  private static void verifyProtobufReference(Map<String, Object> config) {
    // Kept as a separate real-client case: the deserializer must retrieve the
    // referenced subject/version through the registry rather than an inline schema.
    String registry = String.valueOf(config.get("schema.registry.url"));
    String commonSource = "syntax = \"proto3\"; package dev.riquet.ref; message Common { string value = 1; }";
    String eventSource = "syntax = \"proto3\"; package dev.riquet.ref; import \"common.proto\"; "
        + "message Event { Common common = 1; }";
    try {
      CachedSchemaRegistryClient client = new CachedSchemaRegistryClient(registry, 32);
      client.register("java-protobuf-common", new ProtobufSchema(commonSource));
      SchemaReference reference = new SchemaReference("common.proto", "java-protobuf-common", 1);
      ProtobufSchema eventSchema = new ProtobufSchema(
          eventSource, java.util.List.of(reference), Map.of("common.proto", commonSource), null, "event.proto");
      client.register("java-protobuf-ref-value", eventSchema);

      Descriptors.Descriptor eventDescriptor = eventSchema.toDescriptor("dev.riquet.ref.Event");
      Descriptors.FieldDescriptor commonField = eventDescriptor.findFieldByName("common");
      Descriptors.Descriptor commonDescriptor = commonField.getMessageType();
      DynamicMessage common = DynamicMessage.newBuilder(commonDescriptor)
          .setField(commonDescriptor.findFieldByName("value"), "referenced-value").build();
      DynamicMessage event = DynamicMessage.newBuilder(eventDescriptor).setField(commonField, common).build();

      KafkaProtobufSerializer<DynamicMessage> serializer = new KafkaProtobufSerializer<>();
      KafkaProtobufDeserializer<DynamicMessage> deserializer = new KafkaProtobufDeserializer<>();
      Map<String, Object> lookupConfig = new LinkedHashMap<>(config);
      lookupConfig.put("auto.register.schemas", false);
      lookupConfig.put("use.latest.version", true);
      serializer.configure(lookupConfig, false);
      deserializer.configure(config, false);
      byte[] wire = serializer.serialize("java-protobuf-ref", null, event, eventSchema);
      assertEnvelope(wire);
      DynamicMessage output = deserializer.deserialize("java-protobuf-ref", wire);
      if (!event.toString().equals(output.toString())) {
        throw new AssertionError("referenced Protobuf round trip failed: " + output);
      }
      serializer.close();
      deserializer.close();
      client.close();
    } catch (Exception error) {
      throw new RuntimeException("referenced Protobuf client case failed", error);
    }
  }

  @SuppressWarnings("unchecked")
  private static void verifyJson(Map<String, Object> base) {
    Map<String, Object> config = new LinkedHashMap<>(base);
    config.put("json.value.type", LinkedHashMap.class.getName());
    KafkaJsonSchemaSerializer<Map<String, Object>> serializer = new KafkaJsonSchemaSerializer<>();
    KafkaJsonSchemaDeserializer<Map<String, Object>> deserializer = new KafkaJsonSchemaDeserializer<>();
    serializer.configure(config, false);
    deserializer.configure(config, false);
    Map<String, Object> input = new LinkedHashMap<>();
    input.put("value", "json-value");
    byte[] wire = serializer.serialize("java-json", input);
    assertEnvelope(wire);
    Map<String, Object> output = deserializer.deserialize("java-json", wire);
    if (!input.equals(output)) {
      throw new AssertionError("JSON Schema round trip failed: " + output);
    }
    serializer.close();
    deserializer.close();
  }

  private static void verifyKafkaConnect(Map<String, Object> config) {
    AvroConverter converter = new AvroConverter();
    converter.configure(config, false);
    org.apache.kafka.connect.data.Schema schema = SchemaBuilder.struct()
        .name("dev.riquet.ConnectValue").field("value", SchemaBuilder.string().build()).build();
    Struct input = new Struct(schema).put("value", "connect-value");
    byte[] wire = converter.fromConnectData("java-connect", schema, input);
    assertEnvelope(wire);
    SchemaAndValue output = converter.toConnectData("java-connect", wire);
    if (!(output.value() instanceof Struct record) || !"connect-value".equals(record.getString("value"))) {
      throw new AssertionError("Kafka Connect AvroConverter round trip failed: " + output.value());
    }
  }

  private static void assertEnvelope(byte[] wire) {
    if (wire == null || wire.length < 6 || wire[0] != 0) {
      throw new AssertionError("invalid Confluent wire envelope");
    }
    int id = ((wire[1] & 0xff) << 24) | ((wire[2] & 0xff) << 16)
        | ((wire[3] & 0xff) << 8) | (wire[4] & 0xff);
    if (id < 1) {
      throw new AssertionError("invalid schema ID " + id);
    }
  }
}
