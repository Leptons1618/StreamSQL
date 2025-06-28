-- Query to check CDC changes in the database
-- Run this against your DB1 (192.168.1.129 - SampleDB) to see the CDC changes

-- 1. Check if CDC Changes table exists and its structure
SELECT 
    TABLE_NAME,
    COLUMN_NAME,
    DATA_TYPE,
    IS_NULLABLE,
    COLUMN_DEFAULT
FROM INFORMATION_SCHEMA.COLUMNS 
WHERE TABLE_NAME = 'CDCChanges'
ORDER BY ORDINAL_POSITION;

-- 2. View recent CDC changes (latest 50 records)
SELECT TOP 50
    Id,
    SourceDatabase,
    SourceTable,
    Operation,
    RecordId,
    ChangeTimestamp,
    ProcessedAt,
    CASE 
        WHEN Operation = 'c' THEN 'CREATE/INSERT'
        WHEN Operation = 'u' THEN 'UPDATE'
        WHEN Operation = 'd' THEN 'DELETE'
        WHEN Operation = 'r' THEN 'READ/SNAPSHOT'
        ELSE Operation
    END as OperationType
FROM dbo.CDCChanges
ORDER BY ProcessedAt DESC;

-- 3. Count changes by source database and operation
SELECT 
    SourceDatabase,
    Operation,
    CASE 
        WHEN Operation = 'c' THEN 'CREATE/INSERT'
        WHEN Operation = 'u' THEN 'UPDATE'
        WHEN Operation = 'd' THEN 'DELETE'
        WHEN Operation = 'r' THEN 'READ/SNAPSHOT'
        ELSE Operation
    END as OperationType,
    COUNT(*) as ChangeCount
FROM dbo.CDCChanges
GROUP BY SourceDatabase, Operation
ORDER BY SourceDatabase, Operation;

-- 4. View changes for a specific table (e.g., Customers)
SELECT 
    Id,
    SourceDatabase,
    Operation,
    RecordId,
    ChangeTimestamp,
    BeforeData,
    AfterData
FROM dbo.CDCChanges
WHERE SourceTable = 'Customers'
ORDER BY ChangeTimestamp DESC;

-- 5. View changes within the last hour
SELECT 
    Id,
    SourceDatabase,
    SourceTable,
    Operation,
    RecordId,
    ChangeTimestamp,
    ProcessedAt
FROM dbo.CDCChanges
WHERE ProcessedAt >= DATEADD(HOUR, -1, GETDATE())
ORDER BY ProcessedAt DESC;

-- 6. Clean up old records (optional - run manually if needed)
-- DELETE FROM dbo.CDCChanges WHERE ProcessedAt < DATEADD(DAY, -30, GETDATE());
