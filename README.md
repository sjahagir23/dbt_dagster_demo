# Banking Pipeline Demo

This is a **Dagster + dbt demo pipeline** for a fictional banking use case.
It demonstrates a full end-to-end data pipeline including:

1. **Seeding data** with dbt seeds
2. **Transforming models** with dbt
3. **Running dbt tests** for validation
4. **Exporting results** to a local CSV file

All operations run inside a Docker container for easy setup.

---

## Project Structure

```
.
├── dbt/                 # dbt project folder
│   ├── models/
│   ├── seeds/
│   ├── macros/
│   └── target/          # CSV output will be written here
├── dagster_pipeline.py  # Dagster pipeline definition
├── docker-compose.yml   # Docker setup for Dagster
├── requirements.txt     # Python dependencies
└── README.md
```

---

## Prerequisites

* Docker & Docker Compose installed
* Python 3.10+ (if running outside container)
* Optional: Databricks access (not required if exporting locally)

---

## Setup

1. **Clone the repository**

```bash
git clone https://github.com/<your-username>/banking_pipeline.git
cd banking_pipeline
```

2. **Build the Docker container**

```bash
docker-compose build
```

3. **Start the container**

```bash
docker-compose up -d
```

This will start the Dagster container and expose **Dagit UI** at:

```
http://localhost:3000
```

---

## Running the Pipeline

You can run the pipeline in two ways:

### 1️⃣ Using Dagit UI

1. Open [http://localhost:3000](http://localhost:3000)
2. Select the `banking_pipeline` job
3. Click **Launch Run**

Dagster will execute steps sequentially:

```
dbt seed → dbt run → dbt test → export CSV locally
```

### 2️⃣ Using the command line inside Docker

```bash
docker exec -it banking_dagster bash
cd /opt/dagster/app

# Run dbt seed, models, tests sequentially
dbt seed --project-dir dbt --profiles-dir dbt --target dev
dbt run --project-dir dbt --profiles-dir dbt --target dev
dbt test --project-dir dbt --profiles-dir dbt --target dev

# Export results to local CSV
python -c "from dagster_pipeline import export_locally_via_sql_connector; from dagster import OpExecutionContext; export_locally_via_sql_connector(OpExecutionContext())"
```

The CSV will be written to:

```
dbt/target/account_interest_summary.csv
```

---

## Notes

* **Intermediate and mart tables** are defined in `dbt_project.yml`
* **Unit tests** are in `dbt/tests/unit`
* Secrets like `DATABRICKS_HOST` and `DATABRICKS_TOKEN` are optional if exporting locally
* `.gitignore` excludes `dbt/target/`, logs, and temporary files

---

## Cleaning Up

To stop the Docker container:

```bash
docker-compose down
```

To remove volumes and start fresh:

```bash
docker-compose down -v
```
