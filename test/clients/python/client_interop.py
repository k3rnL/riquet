import os

from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer, AvroSerializer
from confluent_kafka.serialization import MessageField, SerializationContext


registry_url = os.environ["RIQUET_REGISTRY_URL"]
client = SchemaRegistryClient({"url": registry_url})
schema = """
{"type":"record","name":"PythonValue","namespace":"dev.riquet",
 "fields":[{"name":"value","type":"string"}]}
"""
serializer = AvroSerializer(client, schema)
deserializer = AvroDeserializer(client)
context = SerializationContext("python-avro", MessageField.VALUE)
wire = serializer({"value": "python-value"}, context)
assert len(wire) > 5 and wire[0] == 0 and int.from_bytes(wire[1:5], "big") > 0
assert deserializer(wire, context) == {"value": "python-value"}
assert "python-avro-value" in client.get_subjects()
print("Confluent Python Schema Registry Avro SerDe passed")
