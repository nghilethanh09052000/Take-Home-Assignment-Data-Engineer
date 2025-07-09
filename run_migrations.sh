#!/bin/bash

# Database Setup Script
# This script sets up the database tables and sample data

echo "Setting up database tables and sample data..."

# Check if psql is available
if ! command -v psql &> /dev/null; then
    echo "Error: psql is not installed or not in PATH"
    exit 1
fi

# Database connection parameters (you can modify these)
DB_HOST=${DB_HOST:-localhost}
DB_PORT=${DB_PORT:-5432}
DB_NAME=${DB_NAME:-postgres}
DB_USER=${DB_USER:-postgres}

# Migration files
MIGRATION_FILES=(
    "migrations/01_invoices.sql"
    "migrations/02_credit_notes.sql"
    "migrations/03_payments.sql"
    "migrations/04_ageing_fact_table.sql"
)

# Check if all migration files exist
for file in "${MIGRATION_FILES[@]}"; do
    if [ ! -f "$file" ]; then
        echo "Error: Migration file not found: $file"
        exit 1
    fi
done

echo "All migration files found. Proceeding with database setup..."

# Execute migrations
for file in "${MIGRATION_FILES[@]}"; do
    echo "Executing migration: $file"
    psql -h "$DB_HOST" -p "$DB_PORT" -d "$DB_NAME" -U "$DB_USER" -f "$file"
    
    if [ $? -ne 0 ]; then
        echo "Error: Failed to execute migration $file"
        exit 1
    fi
    
    echo "Migration $file executed successfully"
done

echo "Database setup completed successfully!"
echo "You can now run: python ageing_processor.py" 