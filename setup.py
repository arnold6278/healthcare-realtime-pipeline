import os
from dotenv import load_dotenv
from google.cloud import bigquery
from google.cloud import pubsub_v1
from google.api_core.exceptions import AlreadyExists, NotFound, Conflict

load_dotenv()

PROJECT_ID = os.getenv("PROJECT_ID")

bq_client  = bigquery.Client()
ps_pub     = pubsub_v1.PublisherClient()
ps_sub     = pubsub_v1.SubscriberClient()


# ── DIVIDER ────────────────────────────────────────────────────────────────────
def divider(title):
    print(f"\n{'─' * 50}")
    print(f"  {title}")
    print(f"{'─' * 50}")


# ── PUB/SUB TOPIC ──────────────────────────────────────────────────────────────
def create_pubsub_topic():
    divider("Pub/Sub Topic")
    topic_path = ps_pub.topic_path(PROJECT_ID, "healthcare-appointments")

    try:
        ps_pub.create_topic(request={"name": topic_path})
        print(f"Topic created: {topic_path}")

    except AlreadyExists:
        # AlreadyExists is raised when the topic was previously created
        # This is not an error — we just skip creation and move on
        print(f"ℹTopic already exists — skipping: {topic_path}")

    except Exception as e:
        # Any other unexpected error — print it so you can investigate
        print(f"Unexpected error creating topic: {e}")
        raise


# ── PUB/SUB SUBSCRIPTION ───────────────────────────────────────────────────────
def create_pubsub_subscription():
    divider("Pub/Sub Subscription")
    topic_path = ps_pub.topic_path(PROJECT_ID, "healthcare-appointments")
    sub_path   = ps_sub.subscription_path(PROJECT_ID, "healthcare-appointments-sub")

    try:
        ps_sub.create_subscription(request={
            "name":  sub_path,
            "topic": topic_path
        })
        print(f"Subscription created: {sub_path}")

    except AlreadyExists:
        # Subscription was already created in a previous run — safe to skip
        print(f"Subscription already exists — skipping: {sub_path}")

    except NotFound:
        # This fires if the topic does not exist yet
        # Should not happen because we create the topic first
        # but if someone deleted the topic manually this will catch it
        print(f"Topic not found — make sure the topic was created successfully first")
        raise

    except Exception as e:
        print(f"Unexpected error creating subscription: {e}")
        raise


# ── BIGQUERY DATASET ───────────────────────────────────────────────────────────
def create_dataset():
    divider("BigQuery Dataset")
    dataset_id  = f"{PROJECT_ID}.healthcare_dataset"
    dataset     = bigquery.Dataset(dataset_id)
    dataset.location    = "US"
    dataset.description = "Healthcare appointment pipeline data"

    try:
        bq_client.create_dataset(dataset, exists_ok=False)
        print(f"Dataset created: {dataset_id}")

    except Conflict:
        # Conflict is BigQuery's way of saying the dataset already exists
        # exists_ok=False lets us catch this explicitly for a clear message
        print(f"Dataset already exists — skipping: {dataset_id}")

    except Exception as e:
        print(f"Unexpected error creating dataset: {e}")
        raise


# ── BIGQUERY RAW TABLE ─────────────────────────────────────────────────────────
def create_raw_table():
    divider("BigQuery Raw Table")
    table_id = f"{PROJECT_ID}.healthcare_dataset.raw_appointments"

    schema = [
        bigquery.SchemaField("app_id",            "STRING",  description="Unique appointment ID e.g. APP-4821"),
        bigquery.SchemaField("patient_id",         "STRING",  description="Patient reference e.g. P-142"),
        bigquery.SchemaField("clinic",             "STRING",  description="Raw clinic name as sent by producer"),
        bigquery.SchemaField("doctor",             "STRING",  description="Doctor name"),
        bigquery.SchemaField("status",             "STRING",  description="Appointment status e.g. Checked-In, No-Show"),
        bigquery.SchemaField("wait_time_minutes",  "INTEGER", description="Wait time in whole minutes"),
        bigquery.SchemaField("timestamp",          "FLOAT",   description="Unix timestamp from producer — seconds since epoch"),
        bigquery.SchemaField("date",               "STRING",  description="Human-readable datetime string from producer"),
    ]

    table = bigquery.Table(table_id, schema=schema)

    try:
        bq_client.create_table(table, exists_ok=False)
        print(f"Raw table created: {table_id}")

    except Conflict:
        # Table already exists from a previous setup run — safe to skip
        print(f"Raw table already exists — skipping: {table_id}")
        _check_schema(table_id, schema)

    except NotFound:
        # Dataset does not exist — this means create_dataset() failed silently
        print(f"Dataset not found — make sure healthcare_dataset was created first")
        raise

    except Exception as e:
        print(f"Unexpected error creating raw table: {e}")
        raise


# ── BIGQUERY ANALYTICS TABLE ───────────────────────────────────────────────────
def create_analytics_table():
    divider("BigQuery Analytics Table")
    table_id = f"{PROJECT_ID}.healthcare_dataset.analytics_appointments"

    schema = [
        # Original fields — identical to raw table
        bigquery.SchemaField("app_id",            "STRING",    description="Unique appointment ID"),
        bigquery.SchemaField("patient_id",         "STRING",    description="Patient reference"),
        bigquery.SchemaField("clinic",             "STRING",    description="Raw clinic name"),
        bigquery.SchemaField("doctor",             "STRING",    description="Doctor name"),
        bigquery.SchemaField("status",             "STRING",    description="Appointment status"),
        bigquery.SchemaField("wait_time_minutes",  "INTEGER",   description="Wait time in minutes"),
        bigquery.SchemaField("timestamp",          "FLOAT",     description="Unix timestamp from producer"),
        bigquery.SchemaField("date",               "STRING",    description="Human-readable datetime from producer"),
        # Derived fields — added by the SQL transform
        bigquery.SchemaField("clinic_clean",       "STRING",    description="Trimmed and title-cased clinic name"),
        bigquery.SchemaField("is_no_show",         "BOOL",      description="TRUE if status is No-Show"),
        bigquery.SchemaField("is_long_wait",       "BOOL",      description="TRUE if wait exceeded 30 minutes"),
        bigquery.SchemaField("urgency_level",      "STRING",    description="LOW / MEDIUM / HIGH based on wait time"),
        bigquery.SchemaField("transformed_at",     "TIMESTAMP", description="UTC timestamp when SQL transform ran"),
    ]

    table = bigquery.Table(table_id, schema=schema)

    try:
        bq_client.create_table(table, exists_ok=False)
        print(f"Analytics table created: {table_id}")

    except Conflict:
        # Table already exists — skip creation but check schema matches
        print(f"Analytics table already exists — skipping: {table_id}")
        _check_schema(table_id, schema)

    except NotFound:
        print(f"Dataset not found — make sure healthcare_dataset was created first")
        raise

    except Exception as e:
        print(f"Unexpected error creating analytics table: {e}")
        raise


# ── SCHEMA CHECKER ─────────────────────────────────────────────────────────────
def _check_schema(table_id, expected_schema):
    """
    When a table already exists, verify its schema matches what we expect.
    This catches the situation where the table exists but was created with
    different or missing columns — which would cause insert errors later.
    """
    try:
        existing_table  = bq_client.get_table(table_id)
        existing_fields = {f.name for f in existing_table.schema}
        expected_fields = {f.name for f in expected_schema}

        missing_fields  = expected_fields - existing_fields
        extra_fields    = existing_fields - expected_fields

        if not missing_fields and not extra_fields:
            print(f" Schema looks correct — all {len(expected_fields)} fields present")
        else:
            if missing_fields:
                print(f"       Missing fields in existing table: {missing_fields}")
                print(f"         You may need to manually add these columns in BigQuery Console")
            if extra_fields:
                print(f"     ℹ Extra fields in existing table (not in setup script): {extra_fields}")

    except Exception as e:
        print(f" Could not verify schema: {e}")


# ── VERIFY ALL RESOURCES ───────────────────────────────────────────────────────
def verify_all():
    """
    After all creation steps, verify every resource actually exists
    by attempting to fetch it. Prints a final status summary.
    """
    divider("Verification Summary")
    all_good = True

    # Check Pub/Sub topic
    try:
        topic_path = ps_pub.topic_path(PROJECT_ID, "healthcare-appointments")
        ps_pub.get_topic(request={"topic": topic_path})
        print("Pub/Sub topic        — EXISTS")
    except NotFound:
        print("Pub/Sub topic        — NOT FOUND")
        all_good = False

    # Check Pub/Sub subscription
    try:
        sub_path = ps_sub.subscription_path(PROJECT_ID, "healthcare-appointments-sub")
        ps_sub.get_subscription(request={"subscription": sub_path})
        print("Pub/Sub subscription — EXISTS")
    except NotFound:
        print("Pub/Sub subscription — NOT FOUND")
        all_good = False

    # Check BigQuery dataset
    try:
        bq_client.get_dataset(f"{PROJECT_ID}.healthcare_dataset")
        print("BigQuery dataset     — EXISTS")
    except NotFound:
        print("BigQuery dataset     — NOT FOUND")
        all_good = False

    # Check raw table
    try:
        bq_client.get_table(f"{PROJECT_ID}.healthcare_dataset.raw_appointments")
        print("Raw table            — EXISTS")
    except NotFound:
        print("Raw table            — NOT FOUND")
        all_good = False

    # Check analytics table
    try:
        bq_client.get_table(f"{PROJECT_ID}.healthcare_dataset.analytics_appointments")
        print("Analytics table      — EXISTS")
    except NotFound:
        print("Analytics table      — NOT FOUND")
        all_good = False

    # Final verdict
    print(f"\n{'─' * 50}")
    if all_good:
        print("All resources verified — pipeline is ready")
        print("   Next step: python producer.py")
    else:
        print("Some resources are missing — review the errors above")
        print("   Re-run: python setup.py")
    print(f"{'─' * 50}\n")


# ── MAIN ───────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    print("\n Setting up Healthcare Pipeline infrastructure...")
    print(f"   Project: {PROJECT_ID}\n")

    if not PROJECT_ID:
        print(" PROJECT_ID is not set — check your .env file")
        exit(1)

    create_pubsub_topic()
    create_pubsub_subscription()
    create_dataset()
    create_raw_table()
    create_analytics_table()
    verify_all()