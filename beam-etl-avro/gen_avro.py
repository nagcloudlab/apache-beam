from fastavro import writer, parse_schema
import os

schema = {
    "type": "record",
    "name": "Customer",
    "fields": [
        {"name": "id", "type": "string"},
        {"name": "name", "type": "string"},
        {"name": "email", "type": ["null", "string"], "default": None},
        {"name": "age", "type": ["null", "int"], "default": None}
    ]
}

records = [
    {"id": "1", "name": "Alice", "email": "alice@example.com", "age": 30},
    {"id": "2", "name": "Bob", "email": None, "age": 25},
    {"id": "3", "name": "Charlie", "email": "charlie@xyz.com", "age": 19},
    {"id": "4", "name": "Dana", "email": None, "age": 40},
]

os.makedirs("input", exist_ok=True)
with open("input/customers.avro", "wb") as out:
    writer(out, parse_schema(schema), records)

print("✅ Avro file generated at input/customers.avro")
