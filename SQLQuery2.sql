-- Create a sample database
CREATE DATABASE SampleDB;
GO

-- Use the sample database
USE SampleDB;
GO

-- Create a sample table
CREATE TABLE dbo.Customers (
    CustomerID INT PRIMARY KEY IDENTITY(1,1),
    FirstName NVARCHAR(50),
    LastName NVARCHAR(50),
    Email NVARCHAR(100),
    CreatedAt DATETIME DEFAULT GETDATE()
);
GO

-- Insert sample data
INSERT INTO dbo.Customers (FirstName, LastName, Email)
VALUES 
    ('John', 'Doe', 'john.doe@example.com'),
    ('Jane', 'Smith', 'jane.smith@example.com');
GO

USE SampleDB;
GO

-- Enable CDC on the database
EXEC sys.sp_cdc_enable_db;
GO

-- Verify CDC is enabled
SELECT name, is_cdc_enabled FROM sys.databases WHERE name = 'SampleDB';
GO

-- Enable CDC on the Customers table
EXEC sys.sp_cdc_enable_table
    @source_schema = N'dbo',
    @source_name = N'Customers',
    @role_name = NULL, -- No access restriction
    @supports_net_changes = 1; -- Enable net changes support
GO

-- Verify CDC is enabled for the table
SELECT name, is_tracked_by_cdc FROM sys.tables WHERE name = 'Customers';
GO

USE SampleDB;
GO

-- Insert a new record
INSERT INTO dbo.Customers (FirstName, LastName, Email)
VALUES ('Anil', 'K', 'anil.k@axcend.com');
GO

-- Update an existing record
UPDATE dbo.Customers
SET Email = 'john.doe.updated@example.com'
WHERE CustomerID = 1;
GO

-- Delete a record
DELETE FROM dbo.Customers WHERE CustomerID = 2;
GO

SELECT * FROM cdc.dbo_Customers_CT;