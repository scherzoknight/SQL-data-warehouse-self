/*
=============================================================
Create Database and Schemas
=============================================================
Script Purpose:
    This script creates a new database named 'DataWarehouse' after checking if it already exists. 
    If the database exists, it is dropped and recreated. Additionally, the script sets up three schemas 
    within the database: 'bronze', 'silver', and 'gold'.
	
WARNING:
    Running this script will drop the entire 'DataWarehouse' database if it exists. 
    All data in the database will be permanently deleted. Proceed with caution 
    and ensure you have proper backups before running this script.
*/

use master;
go 


-- Drop and recreate the 'DataWarehouse2' database
IF EXISTS (SELECT 1 FROM sys.databases WHERE name = 'DataWarehouse2')
BEGIN
    ALTER DATABASE DataWarehouse2 SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
    DROP DATABASE DataWarehouse2;
END;
GO

-- Create the 'DataWarehouse2' database
CREATE DATABASE DataWarehouse2;
GO

USE DataWarehouse2;
GO

-- Create Schemas
CREATE SCHEMA bronze;
GO

CREATE SCHEMA silver;
GO

CREATE SCHEMA gold;
GO
