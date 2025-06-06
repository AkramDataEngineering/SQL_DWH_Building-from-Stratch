/*
DDL Script: Create Bronze Table structure

Script Purpose : This script create Table in the  'Bronze' schema Dropping Existing Tables if Already exists
Run This Script to redifine the  DDL Structure of Bronze  tables
*/

--CReating the DDL for Bronze Layer
--CREATE for File : cust_info.csv

----------------Start--------------------------------

IF OBJECT_ID('Bronze.crm_cust_info','U') IS NOT NULL -- TO MODIFY automatically
	DROP TABLE Bronze.crm_cust_info;
CREATE TABLE Bronze.crm_cust_info(
cst_id INT,
cst_key NVARCHAR(50),
cst_firstname NVARCHAR(50),
cst_lastname NVARCHAR(50),
cst_material_status NVARCHAR(50),
cst_gndr NVARCHAR(50),
cst_create_date DATE
);

--CREATE for File : prd_info.csv
IF OBJECT_ID('Bronze.crm_prd_info','U') IS NOT NULL -- TO MODIFY automatically
	DROP TABLE Bronze.crm_prd_info;
CREATE TABLE Bronze.crm_prd_info(
prd_id INT,
prd_key NVARCHAR(50),
prd_nm NVARCHAR(50),
prd_cost INT,
prd_line NVARCHAR(50),
prd_start_dt DATETIME,
prd_end_dt DATETIME
);

--CREATE for File : sales_details.csv
IF OBJECT_ID('Bronze.crm_sales_details','U') IS NOT NULL -- TO MODIFY automatically
	DROP TABLE Bronze.crm_sales_details;
CREATE TABLE Bronze.crm_sales_details(
sls_ord_num NVARCHAR(50),
sls_prd_key NVARCHAR(50),
sls_cust_id INT,
sls_order_dt INT,
sls_ship_dt INT,
sls_due_dt INT,
sls_sales INT,
sls_quantity INT,
sls_price INT
);


--CREATE for File : CUST_AZ12.csv
IF OBJECT_ID('Bronze.erp_cust_az12','U') IS NOT NULL -- TO MODIFY automatically
	DROP TABLE Bronze.erp_cust_az12;
CREATE TABLE Bronze.erp_cust_az12(
cid NVARCHAR(100),
bdate DATE,
gen NVARCHAR(50)
);


--CREATE for File : LOC_A101.csv
IF OBJECT_ID('Bronze.erp_loc_a101','U') IS NOT NULL -- TO MODIFY automatically
	DROP TABLE Bronze.erp_loc_a101;
CREATE TABLE Bronze.erp_loc_a101(
cid NVARCHAR(50),
cntry NVARCHAR(50)
);


--CREATE for File : PX_CAT_G1V2.csv
IF OBJECT_ID('Bronze.erp_px_cat_g1v2','U') IS NOT NULL
	DROP TABLE Bronze.erp_px_cat_g1v2;
CREATE TABLE Bronze.erp_px_cat_g1v2 (
id NVARCHAR(50),
cat NVARCHAR(50),
subcat NVARCHAR(50),
maintenance NVARCHAR(50)
);



---END-----------------------------
