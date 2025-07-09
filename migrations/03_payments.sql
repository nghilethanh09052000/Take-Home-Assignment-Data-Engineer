-- Create payments table
CREATE TABLE IF NOT EXISTS payments (
    id VARCHAR(20) PRIMARY KEY,
    document_id VARCHAR(20) NOT NULL,
    document_type VARCHAR(20) NOT NULL CHECK (document_type IN ('invoice', 'credit_note')),
    amount_paid DECIMAL(10,2) NOT NULL CHECK (amount_paid > 0),
    payment_date DATE NOT NULL
);