CREATE TABLE IF NOT EXISTS `YOUR_PROJECT_ID.healthcare_dataset.raw_appointments`
(
  app_id             STRING,
  patient_id         STRING,
  clinic             STRING,
  doctor             STRING,
  status             STRING,
  wait_time_minutes  INTEGER,
  timestamp          FLOAT64,
  date               STRING
);

CREATE TABLE IF NOT EXISTS `YOUR_PROJECT_ID.healthcare_dataset.analytics_appointments`
(
  app_id             STRING,
  patient_id         STRING,
  clinic             STRING,
  doctor             STRING,
  status             STRING,
  wait_time_minutes  INTEGER,
  timestamp          FLOAT64,
  date               STRING,
  clinic_clean       STRING,
  is_no_show         BOOL,
  is_long_wait       BOOL,
  urgency_level      STRING,
  transformed_at     TIMESTAMP
);
```

---

## File 7 — `requirements.txt`
*(save in your project root folder — for your laptop)*
```
google-cloud-pubsub==2.18.4
google-cloud-bigquery==3.11.4
python-dotenv==1.0.0
pytest==7.4.0
```

---

## File 8 — `.env.example`
*(save in your project root folder)*
```
PROJECT_ID=your-gcp-project-id-here
TOPIC_ID=healthcare-appointments
RAW_TABLE_ID=your-project-id.healthcare_dataset.raw_appointments
ANALYTICS_TABLE_ID=your-project-id.healthcare_dataset.analytics_appointments
REGION=us-central1