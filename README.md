# Data Ageing Fact Table Assignment

This project implements a data pipeline to build a daily ageing snapshot fact table for outstanding invoices and credit notes. The system processes invoices, credit notes, and payments to create an ageing analysis grouped by how long each document has been unpaid.

## Database Schema

![table_schemas](https://github.com/user-attachments/assets/f1c38c95-d4c2-4b11-8a93-91d26f038d0e)

Link: (https://app.diagrams.net/#G1Mvf2_01wcRHAVNsNOkWkuzq0-hVgnWZ-#%7B%22pageId%22%3A%228-2cqByDnq3awB_osbsl%22%7D)

### 1. Invoices Table (`01_invoices.sql`)

**Purpose**: Stores invoice records with their amounts and metadata.

**Schema**:
```sql
CREATE TABLE invoices (
    id VARCHAR(20) PRIMARY KEY,           -- Unique invoice identifier
    centre_id VARCHAR(10) NOT NULL,       -- Centre identifier
    class_id VARCHAR(10) NOT NULL,        -- Class identifier
    student_id VARCHAR(10) NOT NULL,      -- Student identifier
    invoice_date DATE NOT NULL,           -- Date when invoice was issued
    total_amount DECIMAL(10,2) NOT NULL   -- Total invoice amount (>= 0)
);
```

**Sample Data**:
- 8 invoice records with amounts ranging from $100 to $500
- Dates from 2024-12-15 to 2025-06-20
- Multiple centres (c_01, c_02, c_03) and classes

### 2. Credit Notes Table (`02_credit_notes.sql`)

**Purpose**: Stores credit note records for adjustments and refunds.

**Schema**:
```sql
CREATE TABLE credit_notes (
    id VARCHAR(20) PRIMARY KEY,           -- Unique credit note identifier
    centre_id VARCHAR(10) NOT NULL,       -- Centre identifier
    class_id VARCHAR(10) NOT NULL,        -- Class identifier
    student_id VARCHAR(10) NOT NULL,      -- Student identifier
    credit_note_date DATE NOT NULL,       -- Date when credit note was issued
    total_amount DECIMAL(10,2) NOT NULL   -- Total credit note amount (>= 0)
);
```

**Sample Data**:
- Sample data on path sample_data/*.csv
- Sample output for data in sample_data/ageging_output_2025-07-07.csv

### 3. Payments Table (`03_payments.sql`)

**Purpose**: Tracks payments made against invoices and credit notes.

**Schema**:
```sql
CREATE TABLE payments (
    id VARCHAR(20) PRIMARY KEY,           -- Unique payment identifier
    document_id VARCHAR(20) NOT NULL,     -- References invoice/credit note ID
    document_type VARCHAR(20) NOT NULL,   -- 'invoice' or 'credit_note'
    amount_paid DECIMAL(10,2) NOT NULL,  -- Payment amount (> 0)
    payment_date DATE NOT NULL            -- Date when payment was made
);
```

**Sample Data**:
- 8 payment records with amounts ranging from $40 to $250
- Links to both invoices and credit notes
- Payment dates from 2025-01-10 to 2025-06-10

### 4. Ageing Fact Table (`04_ageing_fact_table.sql`)

**Purpose**: Stores the ageing analysis results for outstanding documents.

**Schema**:
```sql
CREATE TABLE ageing_fact_table (
    centre_id VARCHAR(10) NOT NULL,       -- Centre identifier
    class_id VARCHAR(10) NOT NULL,        -- Class identifier
    document_id VARCHAR(20) NOT NULL,     -- Invoice/credit note identifier
    document_date DATE NOT NULL,          -- Original document date
    student_id VARCHAR(10) NOT NULL,      -- Student identifier
    day_30 DECIMAL(10,2) DEFAULT 0.00,   -- Outstanding in 0-30 days
    day_60 DECIMAL(10,2) DEFAULT 0.00,   -- Outstanding in 31-60 days
    day_90 DECIMAL(10,2) DEFAULT 0.00,   -- Outstanding in 61-90 days
    day_120 DECIMAL(10,2) DEFAULT 0.00,  -- Outstanding in 91-120 days
    day_150 DECIMAL(10,2) DEFAULT 0.00,  -- Outstanding in 121-150 days
    day_180 DECIMAL(10,2) DEFAULT 0.00,  -- Outstanding in 151-180 days
    day_180_and_above DECIMAL(10,2) DEFAULT 0.00, -- Outstanding > 180 days
    document_type VARCHAR(20) NOT NULL,   -- 'invoice' or 'credit_note'
    as_at_date DATE NOT NULL,             -- Snapshot date for ageing
);
```


## Ageing SQL Logic (`sql/generate_ageing_fact.sql`)

The ageing fact table generation uses a complex SQL query with Common Table Expressions (CTEs) to calculate outstanding amounts and apply ageing bucket logic.

### **Step 1: Document Payments Calculation**
```sql
WITH document_payments AS (
    SELECT 
        document_id,
        document_type,
        SUM(amount_paid) as total_payments
    FROM payments
    WHERE payment_date <= %s
    GROUP BY document_id, document_type
)
```
**Purpose**: Calculate total payments made against each document up to the as_at_date
- **Filters**: Only payments made on or before the reference date
- **Groups**: By document_id and document_type to handle invoices and credit notes separately
- **Result**: Total payments per document for outstanding calculation

### **Step 2: Outstanding Invoices**
```sql
invoices_outstanding AS (
    SELECT 
        i.id as document_id,
        i.centre_id, i.class_id, i.student_id,
        i.invoice_date as document_date,
        i.total_amount - COALESCE(dp.total_payments, 0) as outstanding_amount,
        'invoice' as document_type,
        %s - i.invoice_date as days_old
    FROM invoices i
    LEFT JOIN document_payments dp ON i.id = dp.document_id AND dp.document_type = 'invoice'
    WHERE i.invoice_date <= %s
)
```
**Purpose**: Calculate outstanding amounts for invoices
- **Outstanding**: `total_amount - total_payments` (COALESCE handles NULL payments)
- **Days Old**: Calculate days from invoice_date to as_at_date
- **Filter**: Only invoices issued on or before as_at_date
- **LEFT JOIN**: Ensures invoices without payments are included

### **Step 3: Outstanding Credit Notes**
```sql
credit_notes_outstanding AS (
    SELECT 
        cn.id as document_id,
        cn.centre_id, cn.class_id, cn.student_id,
        cn.credit_note_date as document_date,
        cn.total_amount - COALESCE(dp.total_payments, 0) as outstanding_amount,
        'credit_note' as document_type,
        %s - cn.credit_note_date as days_old
    FROM credit_notes cn
    LEFT JOIN document_payments dp ON cn.id = dp.document_id AND dp.document_type = 'credit_note'
    WHERE cn.credit_note_date <= %s
)
```
**Purpose**: Calculate outstanding amounts for credit notes
- **Same logic** as invoices but for credit notes
- **Document type**: Set to 'credit_note' for identification
- **Date field**: Uses credit_note_date instead of invoice_date

### **Step 4: Combine Documents**
```sql
all_documents AS (
    SELECT * FROM invoices_outstanding
    UNION ALL
    SELECT * FROM credit_notes_outstanding
)
```
**Purpose**: Combine invoices and credit notes into single dataset
- **UNION ALL**: Preserves all records from both sources
- **Same structure**: Both CTEs return identical column structure

### **Step 5: Ageing Bucket Assignment**
```sql
ageing_buckets AS (
    SELECT 
        centre_id, class_id, document_id, document_date, student_id,
        outstanding_amount, document_type, days_old,
        CASE 
            WHEN days_old BETWEEN 0 AND 30 THEN outstanding_amount
            ELSE 0.00
        END as day_30,
        CASE 
            WHEN days_old BETWEEN 31 AND 60 THEN outstanding_amount
            ELSE 0.00
        END as day_60,
        -- ... similar for day_90, day_120, day_150, day_180
        CASE 
            WHEN days_old > 180 THEN outstanding_amount
            ELSE 0.00
        END as day_180_and_above
    FROM all_documents
    WHERE outstanding_amount > 0
)
```
**Purpose**: Apply ageing bucket logic to outstanding amounts
- **Single Bucket Rule**: Only one bucket column gets the outstanding amount
- **Zero Values**: All other buckets are set to 0.00
- **Filter**: Only documents with outstanding balance > 0
- **Ageing Logic**:
  - `day_30`: 0-30 days old
  - `day_60`: 31-60 days old
  - `day_90`: 61-90 days old
  - `day_120`: 91-120 days old
  - `day_150`: 121-150 days old
  - `day_180`: 151-180 days old
  - `day_180_and_above`: >180 days old

### **Step 6: Insert into Fact Table**
```sql
INSERT INTO ageing_fact_table (
    centre_id, class_id, document_id, document_date, student_id,
    day_30, day_60, day_90, day_120, day_150, day_180, day_180_and_above,
    document_type, as_at_date
)
SELECT ... FROM ageing_buckets
ORDER BY centre_id, class_id, document_id
```
**Purpose**: Insert processed data into the fact table
- **All columns**: Populated with calculated values
- **as_at_date**: Set to the reference date parameter
- **Ordering**: By centre, class, document for consistent results

### **Parameter Usage**
The SQL uses 6 parameters (all the same as_at_date):
1. **Payment date filter**: `WHERE payment_date <= %s`
2. **Invoice days calculation**: `%s - i.invoice_date`
3. **Invoice date filter**: `WHERE i.invoice_date <= %s`
4. **Credit note days calculation**: `%s - cn.credit_note_date`
5. **Credit note date filter**: `WHERE cn.credit_note_date <= %s`
6. **Fact table as_at_date**: `%s as as_at_date`

## Usage

1. **Setup Database**: Run migration files `run_migrations.sh`
2. **Process Ageing Pipeline**: Run `python ageing_processor.py`
