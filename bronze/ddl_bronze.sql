/*
===============================================================================
DDL Script: Create Bronze Tables
===============================================================================
Script Purpose:
    This script creates tables in the 'bronze' schema, dropping existing tables 
    if they already exist.
	  Run this script to re-define the DDL structure of 'bronze' Tables
===============================================================================
*/




IF OBJECT_ID('bronze.olist_cumstomer_dataset', 'U') IS NOT NULL
    DROP TABLE bronze.olist_cumstomer_dataset;
GO

CREATE TABLE bronze.olist_cumstomer_dataset (
    customer_id varchar(50),
    customer_unique_id varchar(50),
    customer_zip_code float,
    customer_city varchar(50),
    customer_state varchar(50)
);
GO

IF OBJECT_ID('bronze.olist_products_dataset', 'U') IS NOT NULL
    DROP TABLE bronze.olist_products_dataset;
GO

CREATE TABLE bronze.olist_products_dataset (
    product_id varchar(50),
    product_category_name varchar(50),
    product_name_lenght float,
    product_description_lenght float,
    product_photos_qty float,
    product_weight_g nvarchar(100),
    product_length_cm float,
    product_height_cm float,
);
GO

IF OBJECT_ID('bronze.olist_order_items_dataset', 'U') IS NOT NULL
    DROP TABLE bronze.olist_order_items_dataset;
GO

CREATE TABLE bronze.olist_order_items_dataset (
    order_id varchar(50),
    order_item_id int,
    product_id varchar(50),
    seller_id varchar(50),
    shipping_limit_date datetime,
    price float,
    freight_value float
);
GO

IF OBJECT_ID('bronze.olist_orders_dataset', 'U') IS NOT NULL
    DROP TABLE bronze.olist_orders_dataset;
GO

CREATE TABLE bronze.olist_orders_dataset (
    order_id varchar(50),
    customer_id varchar(50),
    order_status varchar(50),
    order_purchase_timestamp datetime,
    order_approved_at datetime,
    order_delivered_carrier_date datetime,
    order_delivered_customer_date datetime,
    order_estimated_delivery_date datetime
);
GO

IF OBJECT_ID('bronze.olist_order_payments_dataset', 'U') IS NOT NULL
    DROP TABLE bronze.olist_order_payments_dataset;
GO

CREATE TABLE bronze.olist_order_payments_dataset (
    order_id varchar(50),
    payment_sequential int,
    payment_type varchar(50),
    payment_installments int,
    payment_value float
);
GO


