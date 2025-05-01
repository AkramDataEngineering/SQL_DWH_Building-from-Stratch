/*BULK INSERT : Method to insert all data Together ,Not Row by Row.
--FULL LOAD using Truncate first and all.
--For FIle : cust_info.csv
--ALONG WITH CREATING PROCEDURE for the same
FEAUTRE OF THE BELOW QUERY:
1. ADDED TRUNCATE To help to Modify the DATA WHEN EVER Updated required by just replacing The Source Link.
2.Using BULK INSERT to upload the data all Together Instead of Row by Row.
3.Created a Store Procudure to avoid Executing the Complete Code Again.
4.Used Error Handling TRY-CATCH to Avoid Error and Debugg the Error with the CATCH Query.
5.Track ELT Duration : Added Query Function to Calculate the Load Time for each Loading Opeartion using DECLARE,SET,GETDATE(),DATEDIFF()


*/
--------------------------START----------------------------------------------------

CREATE OR ALTER PROCEDURE Bronze.load_bronze AS 
BEGIN
	DECLARE @start_time DATETIME , @end_time DATETIME; --for load Duration Calculation
	BEGIN TRY
		PRINT '===================================================';
		PRINT 'LOADING DATA FROM SOURCE';
		PRINT '===================================================';

		PRINT '----------------------------------------------------';
		PRINT 'LOADING DATA FROM CRM TABLE';
		PRINT '----------------------------------------------------';
		PRINT '>> TRUNCATING TABLE Bronze.crm_cust_info';
		
		SET @start_time = GETDATE();
		TRUNCATE TABLE Bronze.crm_cust_info;
		PRINT '>> BULK Inserting Table : Bronze.crm_cust_info from Source';
		BULK INSERT Bronze.crm_cust_info
		FROM 'C:\Users\mdsaq\Desktop\Projects with Baraa\SQL DWH project\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
		WITH (
			FIRSTROW = 2, -- the actual data start from row 2.
			FIELDTERMINATOR = ',',
			TABLOCK	
		);
		SET @end_time = GETDATE();
		PRINT '>>Load Duration : ' + CAST(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR) + ' Seconds';
		PRINT '-----------------';
		---------------------------------------------------------------------------------
		--for File  crm_prd_info.csv
		SET @start_time = GETDATE();
		PRINT '>> TRUNCATING TABLE Bronze.crm_prd_info';
		TRUNCATE TABLE Bronze.crm_prd_info;
		PRINT '>> BULK Inserting Table : Bronze.crm_prd_info from Source';

		BULK INSERT Bronze.crm_prd_info
		FROM 'C:\Users\mdsaq\Desktop\Projects with Baraa\SQL DWH project\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '>>Load Duration : ' + CAST(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR) + ' Seconds';
		PRINT '-----------------';
		----------------------------------------------------------------------------------
		--for file sales_details.csv
		SET @start_time = GETDATE();
		PRINT '>> TRUNCATING TABLE Bronze.crm_sales_details';
		TRUNCATE TABLE Bronze.crm_sales_details;
		PRINT '>> BULK Inserting Table : Bronze.crm_prd_info from Source';

		BULK INSERT Bronze.crm_sales_details
		FROM 'C:\Users\mdsaq\Desktop\Projects with Baraa\SQL DWH project\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);

		SET @start_time = GETDATE();
		PRINT 'Loading Duration : ' + CAST(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR) + ' Seconds';
		PRINT '-----------------';


		PRINT '----------------------------------------------------';
		PRINT 'LOADING DATA FROM ERP TABLE';
		PRINT '----------------------------------------------------';
		-----------------------------------------------------------------
		--For full load of CUST_AZ12.csv file
		SET @start_time = GETDATE();
		PRINT '>> TRUNCATING TABLE Bronze.erp_cust_az12';
		TRUNCATE TABLE Bronze.erp_cust_az12;
		PRINT '>> BULK Inserting Table : Bronze.erp_cust_az12 from Source';

		BULK INSERT Bronze.erp_cust_az12
		FROM 'C:\Users\mdsaq\Desktop\Projects with Baraa\SQL DWH project\sql-data-warehouse-project\datasets\source_erp\CUST_AZ12.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '>>Load Duration : ' + CAST(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR) + ' Seconds';
		PRINT '-----------------';
		-------------------------------------------------------------------
		--For Full Load(TRuncate and Insert)
		SET @start_time = GETDATE();
		PRINT '>> TRUNCATING TABLE Bronze.erp_loc_a101';
		TRUNCATE TABLE Bronze.erp_loc_a101;
		PRINT '>> BULK Inserting Table : Bronze.erp_loc_a101 from Source';

		BULK INSERT Bronze.erp_loc_a101
		FROM 'C:\Users\mdsaq\Desktop\Projects with Baraa\SQL DWH project\sql-data-warehouse-project\datasets\source_erp\LOC_A101.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR= ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '>>Load Duration : ' + CAST(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR) + ' Seconds';
		PRINT '-----------------';

		---------------------------------------------------------
		--FOR Full LOad (TRuncate and Insert) PX_CAT_G1V2.csv file
		SET @start_time = GETDATE();
		PRINT '>> TRUNCATING TABLE Bronze.erp_px_cat_g1v2';
		TRUNCATE TABLE Bronze.erp_px_cat_g1v2;
		PRINT '>> BULK Inserting Table : Bronze.erp_px_cat_g1v2 from Source';

		BULK INSERT Bronze.erp_px_cat_g1v2
		FROM 'C:\Users\mdsaq\Desktop\Projects with Baraa\SQL DWH project\sql-data-warehouse-project\datasets\source_erp\PX_CAT_G1V2.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '>>Load Duration : ' + CAST(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR) + ' Seconds';
		PRINT '-----------------';

	END TRY
	BEGIN CATCH -- TRY-CATCH METHOD TO ERROR HANDLING IF PROCEDURE/QUERY HAVE ERROR
		PRINT '==============================';
		PRINT 'ERROR OCCURED WHILE EXECUTING PROEDURE';
		PRINT 'Error Message' + ERROR_MESSAGE();
		PRINT 'Error Number' + CAST(ERROR_NUMBER() AS NVARCHAR);
		PRINT 'Error Number' + CAST(ERROR_STATE() AS NVARCHAR);
		PRINT '==============================';
	END CATCH;
END

---------------------------------------------------------------
--QUALITY CHECK

--SELECT * FROM Bronze.crm_cust_info;
--SELECT COUNT(*) FROM Bronze.crm_cust_info; -- One Less row as first Now is Header

--EXEC Bronze.load_bronze;

---------------------------END----------------------------------------------------
