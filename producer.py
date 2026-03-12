"""Simple Pub/Sub producer that emits synthetic healthcare appointments.

This script generates mock appointment JSON and publishes it to a
Google Cloud Pub/Sub topic. It is intended for local development and
testing of the realtime pipeline.

Usage: `python producer.py` (ensure `PROJECT_ID` and `TOPIC_ID` are set)
"""

import time
import json
import random
import os
from datetime import datetime
from dotenv import load_dotenv
from google.cloud import pubsub_v1

load_dotenv()

# GCP project and topic (defaults to `healthcare-appointments`)
PROJECT_ID = os.getenv("PROJECT_ID")
TOPIC_ID = os.getenv("TOPIC_ID", "healthcare-appointments")

# Pub/Sub client and fully-qualified topic path
publisher = pubsub_v1.PublisherClient()
topic_path = publisher.topic_path(PROJECT_ID, TOPIC_ID)

# Sample data pools used to construct synthetic records
CLINICS = ["Pretoria Central", "Centurion", "Joburg North", "Sandton", "Soweto"]
STATUSES = ["Scheduled", "Checked-In", "In-Progress", "Completed", "No-Show"]
DOCTORS = ["Dr. Nkosi", "Dr. Patel", "Dr. van der Berg", "Dr. Dlamini", "Dr. Adams"]


def generate_appointment():
    """Return a synthetic appointment record as a dict.

    Fields match the expected schema for the pipeline's raw table.
    """
    return {
        "app_id": f"APP-{random.randint(1000, 9999)}",
        "patient_id": f"P-{random.randint(1, 500)}",
        "clinic": random.choice(CLINICS),
        "doctor": random.choice(DOCTORS),
        "status": random.choice(STATUSES),
        "wait_time_minutes": random.randint(0, 45),
        "timestamp": time.time(),
        "date": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
    }


def main():
    """Continuously publish generated appointment messages to Pub/Sub.

    Sleeps for 5 seconds between messages to avoid flooding.
    """
    print("🏥 Healthcare Appointment Stream Starting...")
    print(f"📡 Publishing to: {topic_path}\n")
    message_count = 0

    while True:
        try:
            appointment = generate_appointment()
            # Encode JSON payload as bytes for Pub/Sub
            data = json.dumps(appointment).encode("utf-8")
            future = publisher.publish(topic_path, data)
            message_id = future.result()  # wait for publish
            message_count += 1
            print(
                f"[{message_count:04d}]  {appointment['app_id']} | {appointment['clinic']:<20} | {appointment['status']:<12} | MsgID: {message_id}"
            )
        except Exception as e:
            # Log errors but continue publishing
            print(f" Error: {e}")
        time.sleep(5)


if __name__ == "__main__":
    main()