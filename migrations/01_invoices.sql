-- Create invoices table
CREATE TABLE IF NOT EXISTS invoices (
    id VARCHAR(20) PRIMARY KEY,
    centre_id VARCHAR(10) NOT NULL,
    class_id VARCHAR(10) NOT NULL,
    student_id VARCHAR(10) NOT NULL,
    invoice_date DATE NOT NULL,
    total_amount DECIMAL(10,2) NOT NULL CHECK (total_amount >= 0)
);