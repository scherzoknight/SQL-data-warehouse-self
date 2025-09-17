/*
===============================================================================
Quality Checks
===============================================================================
Script Purpose:
    This script performs various quality checks for data consistency, accuracy, 
    and standardization across the 'silver' layer. It includes checks for:
    - Null or duplicate primary keys.
    - Unwanted spaces in string fields.
    - Data standardization and consistency.
    - Invalid date ranges and orders.
    - Data consistency between related fields.

Usage Notes:
    - Run these checks after data loading Silver Layer.
    - Investigate and resolve any discrepancies found during the checks.
===============================================================================
*/

-- ====================================================================
-- Checking 'silver.crm_cust_info'
-- ====================================================================
-- Check for NULLs or Duplicates in Primary Key
-- Expectation: No Results


select 
	customer_id,
	count(*)
from silver.olist_customer_dataset
group by customer_id
having count(*) > 1 or customer_id is null; 
go

select 
	customer_city 
from silver.olist_customer_dataset
group by customer_city 
having customer_city!= trim(customer_city) or customer_city is null;
go 

select 
	customer_zip_code
from silver.olist_customer_dataset
where customer_zip_code is null
go

select distinct customer_state
from silver.olist_customer_dataset
go 

select * 
from bronze.olist_orders_dataset
go

select 
	order_id,
	count(*)
from bronze.olist_orders_dataset
group by order_id
having count(*) > 1 or order_id is null 

select 
	customer_id,
	count(*) as repeat_id
from bronze.olist_orders_dataset
group by customer_id
having count(*) > 1 or customer_id is null 
go 

select *
from bronze.olist_orders_dataset 

select 
	*
from bronze.olist_orders_dataset  
where order_purchase_timestamp is null or order_approved_at is null or order_delivered_carrier_date is null or order_delivered_customer_date is null or order_estimated_delivery_date is null 
go 
select
*	
from bronze.olist_orders_dataset
go 

select 
	order_id,
	count(*) repeat_list
from bronze.olist_order_payments_dataset
group by order_id 
having count(*) != 1 or order_id is null 
select 
	payment_value
from bronze.olist_order_payments_dataset
group by payment_value 
having payment_value is null
go

select 
	*,
	ROW_NUMBER() over (partition by order_id order by order_id desc) as flag_last 
from bronze.olist_order_payments_dataset
where order_id is not null 

select *
from bronze.olist_order_payments_dataset 
where order_id = '55b6cfcdaae4a8dc9629850dc635995d'
select *
from bronze.olist_order_payments_dataset 
where order_id = '92907309dff7adcd19bbbb29d5fda792' 


select 
	order_id,
	count(*) repeat_list
from bronze.olist_order_payments_dataset
group by order_id 
having count(*) != 1 or order_id is null 

select 
	payment_sequential
from bronze.olist_order_payments_dataset
group by payment_sequential 
having payment_sequential is null

select payment_sequential,count(*) as count_seq
from bronze.olist_order_payments_dataset
group by payment_sequential 
having count(*) != 1 order by count(*) desc

select * from bronze.olist_order_items_dataset

select
	order_id,
	order_item_id,
	count(*) as repeated 
from bronze.olist_order_items_dataset
group by order_id,order_item_id
having order_id is null or order_item_id is null or count(*) != 1

select
	order_item_id,
	count(*) as repeated 
from bronze.olist_order_items_dataset
group by order_item_id
having order_item_id is null or count(*) != 1 order by order_item_id asc

select
	product_id,
	count(*) as repeated 
from bronze.olist_order_items_dataset
group by product_id 
having product_id is null or count(*) != 1 

select 
order_id,
payment_sequential,
count(*)
from bronze.olist_order_payments_dataset
group by order_id , payment_sequential 
having order_id is null or payment_sequential is null or count(*) > 1

select 
shipping_limit_date
from bronze.olist_order_items_dataset
group by shipping_limit_date
having shipping_limit_date is null

select 
* 
from bronze.olist_products_dataset

-- null 
select 
	product_id,
	product_category_name 
from bronze.olist_products_dataset
where product_category_name is null 
--product_category_name has null values

select 
	product_id
from bronze.olist_products_dataset
where product_name_lenght is null
-- also has null values

select 
	product_id
from bronze.olist_products_dataset
where product_description_lenght is null or product_category_name is null or product_height_cm is null 

select 
	product_id
from bronze.olist_products_dataset 
where product_photos_qty is null

select 
	product_id
from bronze.olist_products_dataset
where product_height_cm is null

select 
order_id,
payment_sequential,
count(*)
from bronze.olist_order_payments_dataset
group by order_id, payment_sequential
having count(*) > 1

