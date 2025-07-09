-- Create credit_notes table
CREATE TABLE IF NOT EXISTS credit_notes (
    id VARCHAR(20) PRIMARY KEY,
    centre_id VARCHAR(10) NOT NULL,
    class_id VARCHAR(10) NOT NULL,
    student_id VARCHAR(10) NOT NULL,
    credit_note_date DATE NOT NULL,
    total_amount DECIMAL(10,2) NOT NULL CHECK (total_amount >= 0)
);
-- Insert credit note data
INSERT INTO credit_notes (id, centre_id, class_id, student_id, credit_note_date, total_amount) VALUES
('cr_001', 'c_01', 'cls_01', 'stu_001', '2025-05-15', 100.00),
('cr_002', 'c_02', 'cls_02', 'stu_002', '2025-03-10', 50.00),
('cr_003', 'c_02', 'cls_03', 'stu_003', '2024-12-01', 300.00),
('cr_004', 'c_01', 'cls_01', 'stu_004', '2025-01-20', 120.00),
('cr_005', 'c_03', 'cls_02', 'stu_005', '2025-06-01', 200.00),
('cr_006', 'c_03', 'cls_03', 'stu_006', '2025-02-28', 80.00),
('cr_007', 'c_02', 'cls_01', 'stu_007', '2025-05-05', 110.00),
('cr_008', 'c_01', 'cls_02', 'stu_008', '2025-04-25', 90.00);
