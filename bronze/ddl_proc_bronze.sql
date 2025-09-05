/*
===============================================================================
Stored Procedure: Load Bronze Layer (Source -> Bronze)
===============================================================================
Script Purpose:
    This stored procedure loads data into the 'bronze' schema from external CSV files. 
    It performs the following actions:
    - Truncates the bronze tables before loading data.
    - Uses the `BULK INSERT` command to load data from csv Files to bronze tables.

Parameters:
    None. 
	  This stored procedure does not accept any parameters or return any values.

Usage Example:
    EXEC bronze.load_bronze;
===============================================================================
*/

create or alter procedure bronze.load_bronze as
BEGIN 
	declare @start_time datetime, @end_time datetime, @batch_start_time datetime, @batch_end_time datetime ;
	begin try
		set @batch_start_time = getdate();
		print '====================================='; 
		print 'Loading Bronze Layer';
		print '=====================================';

		set @start_time = getdate();
		print '>>> Truncating Table : bronze.olist_customer_database';
		truncate table bronze.olist_customer_database;
		print '>>> Inserting data into : bronze.olist_customer_database';
		bulk insert bronze.olist_customer_database
		from 'D:\SQL\self chained projects - 2\data warehouse\docs\olist_customers_dataset.csv'
		with( 
			firstrow = 2,
			fieldterminator = ',',
			tablock
		);
		set @end_time = getdate();
		print '>>>Load Duration:' + cast(datediff(second, @start_time , @end_time) as nvarchar) + 'seconds';
		print '>>>------------------'; 


		set @start_time = getdate();
		print '>>> Truncating Table : bronze.olist_products_dataset';
		truncate table bronze.olist_products_dataset;
		print '>>> Inserting data into : bronze.olist_products_dataset';
		bulk insert bronze.olist_products_dataset
		from 'D:\SQL\self chained projects - 2\data warehouse\docs\olist_products_dataset_2.csv'
		with( 
			firstrow = 2,
			fieldterminator = ',',
			tablock
		);
		set @end_time = getdate();
		print '>>>Load Duration:' + cast(datediff(second, @start_time , @end_time) as nvarchar) + 'seconds';
		print '>>>------------------';



		set @start_time = getdate();
		print '>>> Truncating Table : bronze.olist_order_items_dataset';
		truncate table bronze.olist_order_items_dataset;
		print '>>> Inserting data into : bronze.olist_order_items_dataset';
		bulk insert bronze.olist_order_items_dataset
		from 'D:\SQL\self chained projects - 2\data warehouse\docs\olist_order_items_dataset.csv'
		with(
			firstrow = 2,
			fieldterminator = ',',
			tablock
		);
		set @end_time = getdate();
		print '>>>Load Duration:' + cast(datediff(second, @start_time , @end_time) as nvarchar) + 'seconds';
		print '>>>------------------';


		set @start_time = getdate();
		print '>>> Truncating Table : bronze.olist_order_payments_dataset';
		truncate table bronze.olist_order_payments_dataset;
		print '>>> Inserting data into : bronze.olist_order_payments_dataset';
		bulk insert bronze.olist_order_payments_dataset
		from '"D:\SQL\self chained projects - 2\data warehouse\docs\olist_order_payments_dataset.csv"'
		with( 
			firstrow = 2,
			fieldterminator = ',',
			tablock
		);
		set @end_time = getdate();
		print '>>>Load Duration:' + cast(datediff(second, @start_time , @end_time) as nvarchar) + 'seconds';
		print '>>>------------------';

		set @start_time = getdate();
		print '>>> Truncating Table : bronze.olist_orders_dataset';
		truncate table bronze.olist_orders_dataset;
		print '>>> Inserting data into : bronze.olist_orders_dataset';
		bulk insert bronze.olist_orders_dataset
		from 'D:\SQL\self chained projects - 2\data warehouse\docs\olist_orders_dataset.csv'
		with(
			firstrow = 2,
			fieldterminator = ',',
			tablock
		);
		set @end_time = getdate();
		print '>>>Load Duration:' + cast(datediff(second, @start_time , @end_time) as nvarchar) + 'seconds';
		print '>>>------------------';
		set @batch_end_time = getdate(); 
		print '=======================================';
		print 'Loading Bronze Layer is Completed';
		print '   - Total Load Duration:' + cast(datediff(second, @batch_start_time , @batch_end_time) as nvarchar) + 'seconds';
		print '=======================================';
	end try
	begin catch 
		print '================================';
		print 'ERROR OCCURED DURING BRONZE LAYER'; 
		PRINT 'ERROR MESSAGE' + error_message();
		print 'error message' + cast(error_number() as nvarchar);
		print 'error message' + cast(error_state() as nvarchar);
		print '================================';
	end catch
end
