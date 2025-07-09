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

**Sample Data**: sample_data/invoices.csv

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

**Sample Data**: Sample data on path sample_data/credit_notes.csv

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

**Sample Data**:Sample data on path sample_data/payments.csv

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
**Sample Data**:
- Sample data on path sample_data/ageging_output_2025-07-07.csv
- If we run the code with `run_migrations.sql` and `ageing_processor.py` The csv file named is `ageging_output_2025-07-07.csv` and in the same path


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
**Purpose**: Calculate total payments made against each document up to the as_at_date. We filter only payments made on or before the reference date and group by document_id and document_type to handle invoices and credit notes separately


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
**Purpose**: Calculate outstanding amounts for invoices. With `total_amount - total_payments` (COALESCE handles NULL payments) and join with document_payments to process the invoice

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
**Purpose**: Calculate outstanding amounts for credit notes as invoices but for credit notes. We set to 'credit_note' for identification of credit_notes table and we use the same logic just like step 2

### **Step 4: Combine Documents**
```sql
all_documents AS (
    SELECT * FROM invoices_outstanding
    UNION ALL
    SELECT * FROM credit_notes_outstanding
)
```
**Purpose**: Combine invoices and credit notes into single dataset

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

### **Parameter Usage**
The SQL uses 6 parameters (all the same as_at_date):
1. **Payment date filter**: `WHERE payment_date <= %s`
2. **Invoice days calculation**: `%s - i.invoice_date`
3. **Invoice date filter**: `WHERE i.invoice_date <= %s`
4. **Credit note days calculation**: `%s - cn.credit_note_date`
5. **Credit note date filter**: `WHERE cn.credit_note_date <= %s`
6. **Fact table as_at_date**: `%s as as_at_date`

## How To Run Data Pipeline

### 1. Setup Database (`run_migrations.sh`)

The `run_migrations.sh` script sets up the database tables and sample data. It executes the migration files in the correct order to create the required schema.

#### **Database Connection Setup**

Before running the script, modify the database connection parameters in `run_migrations.sh`:

```bash
# Database connection parameters (modify these for your environment)
DB_HOST=${DB_HOST:-localhost}      # Your PostgreSQL host
DB_PORT=${DB_PORT:-5432}          # Your PostgreSQL port
DB_NAME=${DB_NAME:-postgres}      # Your database name
DB_USER=${DB_USER:-postgres}      # Your database username
```

#### **How to Run**

**Option 1: Use default settings**
```bash
chmod +x run_migrations.sh
./run_migrations.sh
```

**Option 2: Override connection parameters**
```bash
DB_HOST=your_host DB_PORT=5432 DB_NAME=your_db DB_USER=your_user ./run_migrations.sh
```

**Option 3: Set environment variables**
```bash
export DB_HOST=your_host
export DB_PORT=5432
export DB_NAME=your_db
export DB_USER=your_user
./run_migrations.sh
```

#### **What the Script Does**

1. **Checks prerequisites**: Verifies `psql` is installed
2. **Validates files**: Ensures all migration files exist
3. **Executes migrations**: Runs SQL files in order:
   - `01_invoices.sql` - Creates invoices table + sample data
   - `02_credit_notes.sql` - Creates credit_notes table + sample data
   - `03_payments.sql` - Creates payments table + sample data
   - `04_ageing_fact_table.sql` - Creates ageing fact table
4. **Reports progress**: Shows success/failure for each migration

#### **Prerequisites**

- **PostgreSQL**: Must be installed and running
- **psql**: Command-line tool must be in PATH
- **Database**: Target database must exist
- **Permissions**: User must have CREATE TABLE and INSERT permissions

### 2. Process Ageing Pipeline (`ageing_processor.py`)

The `ageing_processor.py` script is the main data processing pipeline that generates the ageing fact table. It connects to the database, processes the ageing calculations, and exports results to CSV.

#### **What the Script Does**

1. **Database Connection**: Connects to PostgreSQL using environment variables
2. **Data Clearing**: Removes existing ageing data for the current date (As this is snapshot date, the hardcoding date(2025, 7, 7) on the code is just for testing with the same sample on the Assignment)
3. **Ageing Generation**: Runs the ageing SQL logic to calculate outstanding amounts
4. **CSV Export**: Exports results to a CSV file

#### **How to Run**

```bash
python ageing_processor.py
```

#### **Configuration**

**Database Connection**: The script uses environment variables for database connection:
- `DB_HOST`: PostgreSQL host (default: localhost)
- `DB_PORT`: PostgreSQL port (default: 5432) 
- `DB_NAME`: Database name (default: postgres)
- `DB_USER`: Database username (default: postgres)
- `DB_PASSWORD`: Database password

**Important: Changing the Processing Date**

The `as_at_date` parameter is crucial for ageing calculations. To change the processing date, modify line 149 in `ageing_processor.py`:

```python
# Current setting
as_at_date = date(2025, 7, 7)

# Examples of other dates you can use:
as_at_date = date.today()                    # Process as of today
as_at_date = date(2025, 6, 30)              # Process as of June 30, 2025
as_at_date = date(2024, 12, 31)             # Process as of December 31, 2024
```

**What `as_at_date` Affects**:
- **Document Inclusion**: Only documents created on or before this date are processed
- **Payment Cutoff**: Only payments made on or before this date are considered
- **Ageing Calculation**: Days old is calculated as `as_at_date - document_date`
- **Output File**: CSV filename includes this date (e.g., `ageing_fact_table_2025-07-07.csv`)

**Business Impact**:
- **Earlier dates**: Fewer documents, potentially more outstanding amounts
- **Later dates**: More documents, potentially less outstanding amounts
- Choose based on your reporting requirements and business needs

#### **Output**

The script generates:
- **Database records**: Ageing fact table populated with calculated data
- **CSV file**: `ageing_fact_table_YYYY-MM-DD.csv` with ageing analysis results
- **Logs**: Detailed processing information in console output
