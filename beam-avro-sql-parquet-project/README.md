
# Apache Beam: Avro → SQL → Parquet (Schema-Aware)

## 🧪 Project Overview
This Beam pipeline reads transaction records from an Avro file, converts them to `Row` using a schema, applies SQL aggregation (`SUM(amount)` grouped by `accountId`), and writes the result to a Parquet file.

## 📁 Project Structure
- `src/main/java/FinalBeamSQLToParquetPipeline.java` – main Beam pipeline
- `transactions.avro` – input file (generated via script)
- `generate_avro.py` – Python script to generate sample input
- `pom.xml` – Maven config

## 🚀 Run Instructions

### 1. Generate Sample Avro File
```bash
pip install fastavro
python generate_avro.py
```

### 2. Build Project
```bash
mvn clean compile
```

### 3. Run Beam Pipeline
```bash
mvn exec:java -Dexec.mainClass=FinalBeamSQLToParquetPipeline
```

### 4. Output
Check the `output/sql_parquet_output-00000-of-00001.parquet` file.

## ✅ Requirements
- Java 11+
- Maven
- Python 3.x (for Avro generator)
