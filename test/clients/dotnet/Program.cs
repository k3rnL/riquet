using Avro;
using Avro.Generic;
using Confluent.Kafka;
using Confluent.SchemaRegistry;
using Confluent.SchemaRegistry.Serdes;

var registryUrl = Environment.GetEnvironmentVariable("RIQUET_REGISTRY_URL")
    ?? throw new InvalidOperationException("RIQUET_REGISTRY_URL is required");
using var registry = new CachedSchemaRegistryClient(new SchemaRegistryConfig { Url = registryUrl });
var schema = (RecordSchema)Avro.Schema.Parse("""
    {"type":"record","name":"DotnetValue","namespace":"Riquet.Interop",
     "fields":[{"name":"value","type":"string"}]}
    """);
var input = new GenericRecord(schema);
input.Add("value", "dotnet-value");
var context = new SerializationContext(MessageComponentType.Value, "dotnet-avro");
var serializer = new AvroSerializer<GenericRecord>(registry);
var deserializer = new AvroDeserializer<GenericRecord>(registry);
var wire = await serializer.SerializeAsync(input, context);
if (wire.Length < 6 || wire[0] != 0 || BitConverter.ToInt32(wire[1..5].Reverse().ToArray()) < 1)
{
    throw new InvalidOperationException("invalid Confluent wire envelope");
}
var output = await deserializer.DeserializeAsync(wire, false, context);
if (output["value"].ToString() != "dotnet-value")
{
    throw new InvalidOperationException(".NET Avro round trip failed");
}
if (!(await registry.GetAllSubjectsAsync()).Contains("dotnet-avro-value"))
{
    throw new InvalidOperationException(".NET subject was not registered");
}
Console.WriteLine("Confluent .NET Schema Registry Avro SerDe passed");
