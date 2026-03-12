"""Setup helper to create GCP Pub/Sub topic and BigQuery datasets/tables.

Run this once locally to provision the minimal infra needed by the
pipeline: `python setup.py` (requires `PROJECT_ID` in the environment).
"""

from google.cloud import bigquery
from google.cloud import pubsub_v1
import os
from dotenv import load_dotenv

load_dotenv()
PROJECT_ID = os.getenv("PROJECT_ID")

# Clients used to create resources
bq_client = bigquery.Client()
ps_client = pubsub_v1.PublisherClient()


def create_pubsub():
    """Create the Pub/Sub topic used for appointments (idempotent).

    If the topic already exists the function prints a message and returns.
    """
    topic_path = ps_client.topic_path(PROJECT_ID, "healthcare-appointments")
    try:
        ps_client.create_topic(request={"name": topic_path})
        print("Pub/Sub topic created")
    except Exception:
        print("Pub/Sub topic already exists — skipping")


def create_dataset():
    """Create a BigQuery dataset for the pipeline.

    Uses `exists_ok=True` to avoid failing if the dataset already exists.
    """
    dataset_id = f"{PROJECT_ID}.healthcare_dataset"
    dataset = bigquery.Dataset(dataset_id)
    dataset.location = "US"
    try:
        bq_client.create_dataset(dataset, exists_ok=True)
        print("BigQuery dataset created")
    except Exception as e:
        print(f"Dataset error: {e}")


def create_raw_table():
    """Create the `raw_appointments` table with a simple schema.

    This table stores incoming raw JSON messages before transformation.
    """
    schema = [
        bigquery.SchemaField("app_id", "STRING"),
        bigquery.SchemaField("patient_id", "STRING"),
        bigquery.SchemaField("clinic", "STRING"),
        bigquery.SchemaField("doctor", "STRING"),
        bigquery.SchemaField("status", "STRING"),
        bigquery.SchemaField("wait_time_minutes", "INTEGER"),
        bigquery.SchemaField("timestamp", "FLOAT"),
        bigquery.SchemaField("date", "STRING"),
    ]
    table = bigquery.Table(f"{PROJECT_ID}.healthcare_dataset.raw_appointments", schema=schema)
    try:
        bq_client.create_table(table, exists_ok=True)
        print("Raw appointments table created")
    except Exception as e:
        print(f"Raw table error: {e}")


def create_analytics_table():
    """Create the `analytics_appointments` table used for transformed data.

    Contains additional derived fields used for analysis and alerts.
    """
    schema = [
        bigquery.SchemaField("app_id", "STRING"),
        bigquery.SchemaField("patient_id", "STRING"),
        bigquery.SchemaField("clinic", "STRING"),
        bigquery.SchemaField("doctor", "STRING"),
        bigquery.SchemaField("status", "STRING"),
        bigquery.SchemaField("wait_time_minutes", "INTEGER"),
        bigquery.SchemaField("timestamp", "FLOAT"),
        bigquery.SchemaField("date", "STRING"),
        bigquery.SchemaField("clinic_clean", "STRING"),
        bigquery.SchemaField("is_no_show", "BOOL"),
        bigquery.SchemaField("is_long_wait", "BOOL"),
        bigquery.SchemaField("urgency_level", "STRING"),
        bigquery.SchemaField("transformed_at", "TIMESTAMP"),
    ]
    table = bigquery.Table(f"{PROJECT_ID}.healthcare_dataset.analytics_appointments", schema=schema)
    try:
        bq_client.create_table(table, exists_ok=True)
        print("Analytics appointments table created")
    except Exception as e:
        print(f"Analytics table error: {e}")


if __name__ == "__main__":
    print("Setting up Healthcare Pipeline infrastructure...\n")
    create_pubsub()
    create_dataset()
    create_raw_table()
    create_analytics_table()
    print("\nSetup complete. You can now run python producer.py")