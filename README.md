# Data Warehouse Ageing Fact Table

This project implements a data pipeline to build a daily ageing snapshot fact table for outstanding invoices and credit notes. The system processes invoices, credit notes, and payments to create an ageing analysis grouped by how long each document has been unpaid.

## Database Schema

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
- 8 credit note records with amounts ranging from $50 to $300
- Dates from 2024-12-01 to 2025-06-01
- Same centre/class structure as invoices

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
    as_at_date DATE NOT NULL,             -- Reference date for ageing
);
```


## Usage

1. **Setup Database**: Run migration files in order (01 → 02 → 03 → 04)
2. **Process Ageing**: Run `python ageing_processor.py`
3. **Export Results**: CSV file is automatically generated with ageing data

## Sample Output

The system generates ageing fact table data showing:
- Which documents have outstanding balances
- How long each document has been outstanding
- Distribution across ageing buckets
- Total outstanding amounts by ageing period
