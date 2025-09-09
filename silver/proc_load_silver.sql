/*
===============================================================================
Stored Procedure: Load Silver Layer (Bronze -> Silver)
===============================================================================
Script Purpose:
    This stored procedure performs the ETL (Extract, Transform, Load) process to 
    populate the 'silver' schema tables from the 'bronze' schema.
	Actions Performed:
		- Truncates Silver tables.
		- Inserts transformed and cleansed data from Bronze into Silver tables.
		
Parameters:
    None. 
	  This stored procedure does not accept any parameters or return any values.

Usage Example:
    EXEC Silver.load_silver;
===============================================================================
*/

create or alter procedure silver.load_silver as 
begin 
	declare @start_time datetime, @end_time datetime, @batch_start_time datetime, @batch_end_time datetime;
	begin try
		SET @batch_start_time = GETDATE();
		PRINT '================================================';
		PRINT 'Loading Silver Layer';
		PRINT '================================================';
		-- laoding silver.olist_customer_dataset
		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: silver.olist_customer_dataset';
		TRUNCATE TABLE silver.olist_customer_dataset;
		PRINT '>> Inserting Data Into: silver.olist_customer_dataset ';
		insert into silver.olist_cumstomer_dataset(
			customer_id,
			customer_unique_id,
			customer_zip_code,
			customer_city,
			customer_state
		)
		select
			customer_id,
			customer_unique_id,
			customer_zip_code_prefix,
			customer_city,
			customer_state
		from bronze.olist_customers_dataset
		set @end_time = getdate()
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '>> -------------'; 

		-- laoding silver.olist_orders-dataset
		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: silver.olist_orders_dataset';
		TRUNCATE TABLE silver.olist_orders_dataset;
		PRINT '>> Inserting Data Into: silver.olist_customer_dataset ';
		insert into silver.olist_orders_dataset(
			order_id,
			customer_id,
			order_status,
			order_purchase_timestamp,
			order_approved_at,
			order_delivered_carrier_date,
			order_delivered_customer_date,
			order_estimated_delivery_date
		)
		select 
			order_id,
			customer_id,
			order_status,
			order_purchase_timestamp,
			order_approved_at,
			order_delivered_carrier_date,
			order_delivered_customer_date,
			order_estimated_delivery_date
		from bronze.olist_orders_dataset
		where order_approved_at is not null and order_delivered_carrier_date is not null and order_delivered_customer_date is not null
		set @end_time = getdate()
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '>> -------------'; 

		--Loading silver.olist_order_payments_dataset
		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: silver.olist_order_payments_dataset';
		TRUNCATE TABLE silver.olist_order_payments_dataset;
		PRINT '>> Inserting Data Into: silver.olist_order_payments_dataset ';
		insert into silver.olist_order_payments_dataset(
			order_id,
			payment_sequential, 
			payment_type,
			payment_installments,
			payment_value
		)
		select 
			order_id,
			payment_sequential, 
			payment_type,
			payment_installments,
			payment_value
		from bronze.olist_order_payments_dataset
		set @end_time = getdate()
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '>> -------------'; 

		-- LOADING silver.olist_order_items_dataset

		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: silver.olist_order_items_dataset';
		TRUNCATE TABLE silver.silver.olist_order_items_dataset;
		PRINT '>> Inserting Data Into: silver.olist_order_items_dataset ';
		insert into silver.olist_order_items_dataset(
			order_id,
			order_item_id,
			product_id,
			seller_id,
			shipping_limit_date, 
			price,
			freight_value
		)
		select 
			order_id,
			order_item_id,
			product_id,
			seller_id,
			shipping_limit_date, 
			price,
			freight_value
		from bronze.olist_order_items_dataset
		set @end_time = getdate()
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '>> -------------'; 

		--Loading silver.olist_products_dataset
		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: silver.olist_products_dataset';
		TRUNCATE TABLE silver.olist_products_dataset;
		PRINT '>> Inserting Data Into: silver.olist_products_dataset ';
		insert into silver.olist_products_dataset(
			product_id,
			product_category_name,
			product_name_lenght,
			product_description_lenght,
			product_photos_qty,
			product_weight_g,
			product_length_cm,
			product_height_cm
		)
		select
			product_id,
			product_category_name,
			product_name_lenght,
			product_description_lenght,
			product_photos_qty,
			product_weight_g,
			product_length_cm,
			product_height_cm
		from bronze.olist_products_dataset  
		where product_category_name is not null and product_name_lenght is not null and product_description_lenght is not null and product_photos_qty is not null and product_weight_g is not null and product_height_cm is not null

		set @end_time = getdate()
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '>> -------------'; 
		set @batch_end_time = getdate();
		PRINT '==========================================';
		PRINT 'Loading Silver Layer is Completed';
		PRINT '   - Total Load Duration: ' + CAST(DATEDIFF(SECOND, @batch_start_time, @batch_end_time) AS NVARCHAR) + ' seconds';
		PRINT '==========================================';
		
	END TRY
		BEGIN CATCH
			PRINT '=========================================='
			PRINT 'ERROR OCCURED DURING LOADING BRONZE LAYER'
			PRINT 'Error Message' + ERROR_MESSAGE();
			PRINT 'Error Message' + CAST (ERROR_NUMBER() AS NVARCHAR);
			PRINT 'Error Message' + CAST (ERROR_STATE() AS NVARCHAR);
			PRINT '=========================================='
		END CATCH
END
