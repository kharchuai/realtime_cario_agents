-- db/chema.sql
-- Tạo 1 bảng bệnh nhân + 1 bảng ghi lại ca mổ-> thuận tiện cho việc mở rộng sau này
-- Bảng surgeries và các bảng phía dưới chỉa phục vụ cho các ca phấu thuật
-- Tương lai nếu muốn mở rộng ra các chứ năng của bệnh viện thì tạo bảng mới và lien kết với danh sách bệnh nhân
-- 1. patient
CREATE TABLE IF NOT EXISTS patient(
    patient_id          INT PRIMARY KEY,
    age                 INT,
    sex                 CHAR(1) CHECK (sex IN ('M', 'F')),
    height              DECIMAL(5,1),
    weight              DECIMAL(5,1),
    bmi                 DECIMal(5,1)
);


CREATE TABLE IF NOT EXISTS surgeries (
    surgery_id         INT PRIMARY KEY,
    patient_id          INT REFERENCES patient(patient_id),
    asa             INT,
    emop            INT,
    department      VARCHAR(50),
    optype          VARCHAR(50),
    anestart        INT,
    aneend          INT,
    opstart         INT,
    opend           INT,
    death_inhosp    INT,
    clinical_metadata   JSONB,
    created_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);


CREATE TABLE IF NOT EXISTS vital_signs(
    surgery_id     INT REFERENCES surgeries(surgery_id),
    recorded_at TIMESTAMP,
    time_sec    FLOAT,
    hr          INT,
    spo2        DECIMAL(5,2),
    sbp         INT,
    dbp         INT,
    mbp        INT,
    rr  INT,
    bt DECIMAL(4,1),

    PRIMARY KEY (surgery_id, recorded_at)
);


CREATE TABLE IF NOT EXISTS triage_alerts (
    alert_id        SERIAL PRIMARY KEY,
    surgery_id      INT,
    recorded_at     TIMESTAMP,
    risk_level      VARCHAR(20),
    model_score     DECIMAL(5,4),
    alert_reason    TEXT,
    is_resolved     BOOLEAN     DEFAULT FALSE,
    created_at       TIMESTAMP   DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (surgery_id, recorded_at) REFERENCES vital_signs(surgery_id, recorded_at)
);


CREATE INDEX IF NOT EXISTS idx_alerts_unresolved ON triage_alerts(is_resolved) WHERE is_resolved = FALSE;

CREATE INDEX IF NOT EXISTS idx_cases_metadata ON surgeries USING GIN (clinical_metadata);

CREATE INDEX IF NOT EXISTS idx_surgeries_patient ON surgeries(patient_id);