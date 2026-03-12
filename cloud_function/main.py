"""Cloud Function that ingests Pub/Sub messages into BigQuery.

This module provides `ingest_raw`, a CloudEvent-triggered function
that decodes a Pub/Sub message (base64-encoded JSON) and inserts the
resulting record into the configured BigQuery raw table.
"""

import base64
import json
import os
import functions_framework
from google.cloud import bigquery

# BigQuery client and destination table id read from environment
client = bigquery.Client()
RAW_TABLE_ID = os.getenv("RAW_TABLE_ID")


@functions_framework.cloud_event
def ingest_raw(cloud_event):
    """Handle a CloudEvent from Pub/Sub and write to BigQuery.

    Expects `cloud_event.data['message']['data']` to contain a base64
    encoded JSON payload representing a single appointment record.
    """
    try:
        # Extract base64 payload from the CloudEvent message
        raw_bytes = cloud_event.data["message"]["data"]
        json_str = base64.b64decode(raw_bytes).decode("utf-8")
        record = json.loads(json_str)

        print(f"received: {record.get('app_id', 'UNKNOWN')}")

        # Insert into BigQuery and check for errors
        errors = client.insert_rows_json(RAW_TABLE_ID, [record])

        if errors:
            print(f"insert error: {errors}")
            raise RuntimeError(f"BigQuery insert failed: {errors}")

        print(f"raw record written: {record['app_id']}")

    except Exception as e:
        # Surface errors to Cloud Functions logs and let platform handle retries
        print(f"function error: {e}")
        raise
