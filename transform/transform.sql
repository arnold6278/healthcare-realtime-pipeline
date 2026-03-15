MERGE `healthcare-pipeline-demo.healthcare_dataset.analytics_appointments` AS target
USING (
  SELECT
    app_id,
    patient_id,
    clinic,
    doctor,
    status,
    wait_time_minutes,
    timestamp,
    date,
    TRIM(INITCAP(clinic))                          AS clinic_clean,
    (status = 'No-Show')                           AS is_no_show,
    (COALESCE(wait_time_minutes, 0) > 30)          AS is_long_wait,
    CASE
      WHEN COALESCE(wait_time_minutes, 0) > 30 THEN 'HIGH'
      WHEN COALESCE(wait_time_minutes, 0) > 15 THEN 'MEDIUM'
      ELSE 'LOW'
    END                                            AS urgency_level,
    CURRENT_TIMESTAMP()                            AS transformed_at
  FROM `healthcare-pipeline-demo.healthcare_dataset.raw_appointments`
  WHERE TIMESTAMP_SECONDS(CAST(timestamp AS INT64))
        >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 2 MINUTE)
) AS source

ON target.app_id = source.app_id

WHEN MATCHED THEN UPDATE SET
  target.status            = source.status,
  target.wait_time_minutes = source.wait_time_minutes,
  target.is_no_show        = source.is_no_show,
  target.is_long_wait      = source.is_long_wait,
  target.urgency_level     = source.urgency_level,
  target.transformed_at    = source.transformed_at

WHEN NOT MATCHED THEN INSERT (
  app_id, patient_id, clinic, doctor, status,
  wait_time_minutes, timestamp, date,
  clinic_clean, is_no_show, is_long_wait,
  urgency_level, transformed_at
) VALUES (
  source.app_id, source.patient_id, source.clinic, source.doctor, source.status,
  source.wait_time_minutes, source.timestamp, source.date,
  source.clinic_clean, source.is_no_show, source.is_long_wait,
  source.urgency_level, source.transformed_at
);