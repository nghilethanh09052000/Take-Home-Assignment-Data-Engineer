-- Create credit_notes table
CREATE TABLE IF NOT EXISTS credit_notes (
    id VARCHAR(20) PRIMARY KEY,
    centre_id VARCHAR(10) NOT NULL,
    class_id VARCHAR(10) NOT NULL,
    student_id VARCHAR(10) NOT NULL,
    credit_note_date DATE NOT NULL,
    total_amount DECIMAL(10,2) NOT NULL CHECK (total_amount >= 0)
);
