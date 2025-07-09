-- Create payments table
CREATE TABLE IF NOT EXISTS payments (
    id VARCHAR(20) PRIMARY KEY,
    document_id VARCHAR(20) NOT NULL,
    document_type VARCHAR(20) NOT NULL CHECK (document_type IN ('invoice', 'credit_note')),
    amount_paid DECIMAL(10,2) NOT NULL CHECK (amount_paid > 0),
    payment_date DATE NOT NULL
);

-- Insert payment data
INSERT INTO payments (id, document_id, document_type, amount_paid, payment_date) VALUES
('pay_001', 'inv_001', 'invoice', 150.00, '2025-05-10'),
('pay_002', 'cr_003', 'credit_note', 100.00, '2025-02-01'),
('pay_003', 'inv_002', 'invoice', 200.00, '2025-06-10'),
('pay_004', 'inv_004', 'invoice', 100.00, '2025-01-10'),
('pay_005', 'cr_006', 'credit_note', 40.00, '2025-03-05'),
('pay_006', 'inv_006', 'invoice', 250.00, '2025-05-01'),
('pay_007', 'cr_008', 'credit_note', 90.00, '2025-05-10'),
('pay_008', 'inv_005', 'invoice', 50.00, '2025-03-01');