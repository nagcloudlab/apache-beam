
from fastavro import writer, parse_schema

schema = {
    "type": "record",
    "name": "Transaction",
    "fields": [
        {"name": "transactionId", "type": "string"},
        {"name": "accountId", "type": "string"},
        {"name": "amount", "type": "double"},
        {"name": "timestamp", "type": "long"}
    ]
}

records = [
    {"transactionId": "tx001", "accountId": "acc100", "amount": 500.0, "timestamp": 1716000000000},
    {"transactionId": "tx002", "accountId": "acc101", "amount": 1500.0, "timestamp": 1716003600000},
    {"transactionId": "tx003", "accountId": "acc100", "amount": 2500.0, "timestamp": 1716007200000},
    {"transactionId": "tx004", "accountId": "acc102", "amount": 700.0, "timestamp": 1716010800000},
    {"transactionId": "tx005", "accountId": "acc101", "amount": 3000.0, "timestamp": 1716014400000}
]

parsed_schema = parse_schema(schema)

with open("src/main/resources/transactions.avro", "wb") as out:
    writer(out, parsed_schema, records)
