// Hospital Patient Vital Signs Monitoring - Cassandra Schema
// Keyspace and 7 query-first tables matching the plan/07 specification

CREATE KEYSPACE IF NOT EXISTS ward WITH replication = {
    'class': 'SimpleStrategy',
    'replication_factor': 1
};

// Q1: Latest N vital readings for one patient, newest first
CREATE TABLE IF NOT EXISTS ward.vitals_by_patient (
    patient_id              text,
    measured_at             timestamp,
    reading_id              uuid,
    bed_id                  text,
    heart_rate              int,
    spo2                    int,
    systolic_bp             int,
    diastolic_bp            int,
    temperature             decimal,
    respiratory_rate        int,
    consciousness           text,
    on_supplemental_oxygen  boolean,
    parameters_missing      int,
    ingest_time             timestamp,
    PRIMARY KEY ((patient_id), measured_at, reading_id)
) WITH CLUSTERING ORDER BY (measured_at DESC, reading_id ASC)
  AND default_time_to_live = 10800;

// Q3: Score history per patient, per scorer version (non-destructive replay)
CREATE TABLE IF NOT EXISTS ward.risk_scores_by_patient (
    patient_id              text,
    scorer_version          text,
    scored_at               timestamp,
    window_start            timestamp,
    window_end              timestamp,
    news2_total             int,
    news2_subscores         map<text, int>,
    any_parameter_is_3      boolean,
    clinical_risk           text,
    lab_contribution        int,
    composite_risk          int,
    risk_tier               text,
    labs_stale              boolean,
    confidence              text,
    hr_slope                decimal,
    spo2_slope              decimal,
    sbp_slope               decimal,
    temp_slope              decimal,
    readings_in_window      int,
    PRIMARY KEY ((patient_id), scorer_version, scored_at)
) WITH CLUSTERING ORDER BY (scorer_version ASC, scored_at DESC)
  AND default_time_to_live = 10800;

// Q2: Live ward risk monitor. Single-partition read sorted worst-first
CREATE TABLE IF NOT EXISTS ward.ward_risk_snapshot (
    ward_id                 text,
    scorer_version          text,
    risk_score              int,
    patient_id              text,
    bed_id                  text,
    risk_tier               text,
    news2_total             int,
    lab_contribution        int,
    labs_stale              boolean,
    scored_at               timestamp,
    hr_latest               int,
    spo2_latest             int,
    sbp_latest              int,
    temp_latest             decimal,
    PRIMARY KEY ((ward_id, scorer_version), risk_score, patient_id)
) WITH CLUSTERING ORDER BY (risk_score DESC, patient_id ASC)
  AND default_time_to_live = 120;

// Q4: Ward alert feed, partitioned by day to bound partition size, and by scorer
// version so a replay writes its alerts beside the live ones instead of into the
// live ward's feed.
CREATE TABLE IF NOT EXISTS ward.alerts_by_ward (
    ward_id                 text,
    sim_date                date,
    scorer_version          text,
    alert_time              timestamp,
    alert_id                text,
    patient_id              text,
    bed_id                  text,
    alert_type              text,
    severity                text,
    news2_total             int,
    composite_risk          int,
    detail                  text,
    acknowledged            boolean,
    PRIMARY KEY ((ward_id, sim_date, scorer_version), alert_time, alert_id)
) WITH CLUSTERING ORDER BY (alert_time DESC, alert_id ASC)
  AND default_time_to_live = 10800;

// Q5: Latest lab results per patient
CREATE TABLE IF NOT EXISTS ward.labs_by_patient (
    patient_id              text,
    test_type               text,
    collected_at            timestamp,
    result_value            decimal,
    unit                    text,
    reference_low           decimal,
    reference_high          decimal,
    out_of_range            boolean,
    sim_date                date,
    PRIMARY KEY ((patient_id), test_type, collected_at)
) WITH CLUSTERING ORDER BY (test_type ASC, collected_at DESC);

// Q6: Pre-computed daily patient summary for daily reports
CREATE TABLE IF NOT EXISTS ward.daily_patient_summary (
    ward_id                 text,
    sim_date                date,
    patient_id              text,
    scorer_version          text,
    bed_id                  text,
    admitting_condition     text,
    max_news2               int,
    mean_news2              decimal,
    max_composite_risk      int,
    final_risk_tier         text,
    lab_contribution        int,
    labs_stale              boolean,
    alert_count             int,
    highest_severity        text,
    readings_count          int,
    readings_rejected       int,
    deterioration_detected  boolean,
    PRIMARY KEY ((ward_id, sim_date), patient_id, scorer_version)
);

// Q7: Cluster metadata and simulation synchronization state
CREATE TABLE IF NOT EXISTS ward.sim_state (
    key                     text PRIMARY KEY,
    value                   text,
    updated_at              timestamp
);
