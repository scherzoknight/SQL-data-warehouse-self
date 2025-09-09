/*
===============================================================================
DDL Script: Create Silver Tables
===============================================================================
Script Purpose:
    This script creates tables in the 'silver' schema, dropping existing tables 
    if they already exist.
	  Run this script to re-define the DDL structure of 'bronze' Tables
===============================================================================
*/

IF OBJECT_ID('silver.olist_cumstomer_dataset', 'U') IS NOT NULL
DROP TABLE silver.olist_customer_dataset;

CREATE TABLE silver.olist_customer_dataset (
	customer_id nvarchar(100),
	customer_unique_id nvarchar(100),
	customer_zip_code float,
	customer_city nvarchar(100),
	customer_state nvarchar(100),
	dwh_create_date datetime2 default GETDATE()
);

IF OBJECT_ID('silver.olist_products_dataset', 'U') IS NOT NULL
DROP TABLE silver.olist_products_dataset;
CREATE TABLE silver.olist_products_dataset (
	product_id nvarchar(100),
	product_category_name nvarchar(100),
	product_name_lenght float,
	product_description_lenght float,
	product_photos_qty float,
	product_weight_g nvarchar(100),
	product_length_cm float,
	product_height_cm float,
	dwh_create_date datetime2 default GETDATE()
);

IF OBJECT_ID('silver.olist_order_items_dataset', 'U') IS NOT NULL
DROP TABLE silver.olist_order_items_dataset;

CREATE TABLE silver.olist_order_items_dataset (
	order_id nvarchar(100),
	order_item_id int,
	product_id nvarchar(100),
	seller_id nvarchar(100),
	shipping_limit_date datetime,
	price float,
	freight_value float,
	dwh_create_date datetime2 default GETDATE()
);

IF OBJECT_ID('silver.olist_orders_dataset', 'U') IS NOT NULL
DROP TABLE silver.olist_orders_dataset;

CREATE TABLE silver.olist_orders_dataset (
	order_id nvarchar(100),
	customer_id nvarchar(100),
	order_status nvarchar(100),
	order_purchase_timestamp datetime,
	order_approved_at datetime,
	order_delivered_carrier_date datetime,
	order_delivered_customer_date datetime,
	order_estimated_delivery_date datetime,
	dwh_create_date datetime2 default GETDATE()
);

IF OBJECT_ID('silver.olist_order_payments_dataset', 'U') IS NOT NULL
DROP TABLE silver.olist_order_payments_dataset;

CREATE TABLE silver.olist_order_payments_dataset (
	order_id nvarchar(100),
	payment_sequential int,
	payment_type nvarchar(100),
	payment_installments int,
	payment_value float,
	dwh_create_date datetime2 default GETDATE()
);
