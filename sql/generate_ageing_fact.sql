WITH document_payments AS (
    -- Calculate total payments for each document
    SELECT 
        document_id,
        document_type,
        SUM(amount_paid) as total_payments
    FROM payments
    WHERE payment_date <= %s
    GROUP BY document_id, document_type
),
invoices_outstanding AS (
    SELECT 
        i.id as document_id,
        i.centre_id,
        i.class_id,
        i.student_id,
        i.invoice_date as document_date,
        i.total_amount - COALESCE(dp.total_payments, 0) as outstanding_amount,
        'invoice' as document_type,
        %s - i.invoice_date as days_old
    FROM invoices i
    LEFT JOIN document_payments dp ON i.id = dp.document_id AND dp.document_type = 'invoice'
    WHERE i.invoice_date <= %s
),
credit_notes_outstanding AS (
    SELECT 
        cn.id as document_id,
        cn.centre_id,
        cn.class_id,
        cn.student_id,
        cn.credit_note_date as document_date,
        cn.total_amount - COALESCE(dp.total_payments, 0) as outstanding_amount,
        'credit_note' as document_type,
        %s - cn.credit_note_date as days_old
    FROM credit_notes cn
    LEFT JOIN document_payments dp ON cn.id = dp.document_id AND dp.document_type = 'credit_note'
    WHERE cn.credit_note_date <= %s
),
all_documents AS (
    SELECT * FROM invoices_outstanding
    UNION ALL
    SELECT * FROM credit_notes_outstanding
),
ageing_buckets AS (
    -- Apply ageing bucket logic
    SELECT 
        centre_id,
        class_id,
        document_id,
        document_date,
        student_id,
        outstanding_amount,
        document_type,
        days_old,
        CASE 
            WHEN days_old BETWEEN 0 AND 30 THEN outstanding_amount
            ELSE 0.00
        END as day_30,
        CASE 
            WHEN days_old BETWEEN 31 AND 60 THEN outstanding_amount
            ELSE 0.00
        END as day_60,
        CASE 
            WHEN days_old BETWEEN 61 AND 90 THEN outstanding_amount
            ELSE 0.00
        END as day_90,
        CASE 
            WHEN days_old BETWEEN 91 AND 120 THEN outstanding_amount
            ELSE 0.00
        END as day_120,
        CASE 
            WHEN days_old BETWEEN 121 AND 150 THEN outstanding_amount
            ELSE 0.00
        END as day_150,
        CASE 
            WHEN days_old BETWEEN 151 AND 180 THEN outstanding_amount
            ELSE 0.00
        END as day_180,
        CASE 
            WHEN days_old > 180 THEN outstanding_amount
            ELSE 0.00
        END as day_180_and_above
    FROM all_documents
    WHERE outstanding_amount > 0
)
INSERT INTO ageing_fact_table (
    centre_id, class_id, document_id, document_date, student_id,
    day_30, day_60, day_90, day_120, day_150, day_180, day_180_and_above,
    document_type, as_at_date
)
SELECT 
    centre_id,
    class_id,
    document_id,
    document_date,
    student_id,
    day_30,
    day_60,
    day_90,
    day_120,
    day_150,
    day_180,
    day_180_and_above,
    document_type,
    %s as as_at_date
FROM ageing_buckets
ORDER BY centre_id, class_id, document_id; 