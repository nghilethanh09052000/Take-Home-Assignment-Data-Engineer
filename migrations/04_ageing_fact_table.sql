--Create ageing fact table

CREATE TABLE IF NOT EXISTS ageing_fact_table (
    centre_id VARCHAR(10) NOT NULL,
    class_id VARCHAR(10) NOT NULL,
    document_id VARCHAR(20) NOT NULL,
    document_date DATE NOT NULL,
    student_id VARCHAR(10) NOT NULL,
    day_30 DECIMAL(10,2) DEFAULT 0.00,
    day_60 DECIMAL(10,2) DEFAULT 0.00,
    day_90 DECIMAL(10,2) DEFAULT 0.00,
    day_120 DECIMAL(10,2) DEFAULT 0.00,
    day_150 DECIMAL(10,2) DEFAULT 0.00,
    day_180 DECIMAL(10,2) DEFAULT 0.00,
    day_180_and_above DECIMAL(10,2) DEFAULT 0.00,
    document_type VARCHAR(20) NOT NULL CHECK (document_type IN ('invoice', 'credit_note')),
    as_at_date DATE NOT NULL
);
