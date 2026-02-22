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

-- And TRY...CATCH
-- Ensures error handling, data intgerity, and issue logging for easier debugging.
-- SQL runs the TRY block, and if it fails, it runs the CATCH block to handle the error.

CREATE OR ALTER PROCEDURE Bronze.load_bronze AS
BEGIN
	DECLARE @start_time DATETIME, @end_time DATETIME, @batch_start_time DATETIME, @batch_end_time DATETIME;
	BEGIN TRY
			SET @batch_start_time = GETDATE();
			PRINT '========================================================================';
			PRINT 'Loading Bronze Layer';
			PRINT '========================================================================';

			PRINT '------------------------------------------------------------------------';
			PRINT 'Loading CRM Tables';
			PRINT '------------------------------------------------------------------------';

			-----------------------------------------------------------------------------------------
			-- Truncate: Quickly delete all rows from a table, resetting it to an empty state.
			SET @start_time = GETDATE();
			PRINT '>> Truncating Table: Bronze.crm_cust_info'
			TRUNCATE TABLE Bronze.crm_cust_info

			PRINT '>> Inserting Data Into: Bronze.crm_cust_info'
			BULK INSERT Bronze.crm_cust_info
			FROM 'C:\Users\Aryab\OneDrive\Desktop\Data Analytics\SQL content\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
			WITH (
				FIRSTROW = 2,
				FIELDTERMINATOR = ',',
				TABLOCK
			);
			SET @end_time = GETDATE();
		
			-- DATEDIFF(): Calculates the difference between two dates, return days, months or years.
			PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' second';
			PRINT '>> -----------------';

			-- SELECT * FROM Bronze.crm_cust_info;

			-- Quality Check: Check that the data has not shifted and is in the correct columns.
			-- SELECT COUNT(*) FROM Bronze.crm_cust_info

			-----------------------------------------------------------------------------------------

			SET @start_time = GETDATE();
			PRINT '>> Truncating Table: Bronze.crm_prod_info'
			TRUNCATE TABLE Bronze.crm_prod_info

			PRINT '>> Inserting Data Into: Bronze.crm_prod_info'
			BULK INSERT Bronze.crm_prod_info
			FROM 'C:\Users\Aryab\OneDrive\Desktop\Data Analytics\SQL content\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
			WITH (
				FIRSTROW = 2,
				FIELDTERMINATOR = ',',
				TABLOCK
			);
			SET @end_time = GETDATE();

			PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' second';
			PRINT '>> -----------------';
			-- SELECT * FROM Bronze.crm_prod_info;

			-- Quality Check: Check that the data has not shifted and is in the correct columns.
			-- SELECT COUNT(*) FROM Bronze.crm_prod_info;

			-----------------------------------------------------------------------------------------
		
			SET @start_time = GETDATE();
			PRINT '>> Truncating Table: Bronze.crm_sales_details'
			TRUNCATE TABLE Bronze.crm_sales_details

			PRINT '>> Inserting Data Into: Bronze.crm_sales_details'
			BULK INSERT Bronze.crm_sales_details
			FROM 'C:\Users\Aryab\OneDrive\Desktop\Data Analytics\SQL content\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
			WITH (
				FIRSTROW = 2,
				FIELDTERMINATOR = ',',
				TABLOCK
			);
			SET @end_time = GETDATE();
		
			PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' second';
			PRINT '>> -----------------';

			-- SELECT * FROM Bronze.crm_sales_details;

			-- Quality Check: Check that the data has not shifted and is in the correct columns.
			-- SELECT COUNT(*) FROM Bronze.crm_sales_details;


			PRINT '------------------------------------------------------------------------';
			PRINT 'Loading ERP Tables';
			PRINT '------------------------------------------------------------------------';

			SET @start_time = GETDATE();
			-----------------------------------------------------------------------------------------
			PRINT '>> Truncating Table: Bronze.erp_cust_az12'
			TRUNCATE TABLE Bronze.erp_cust_az12

			PRINT '>> Inserting Data Into: Bronze.erp_cust_az12'
			BULK INSERT Bronze.erp_cust_az12
			FROM 'C:\Users\Aryab\OneDrive\Desktop\Data Analytics\SQL content\sql-data-warehouse-project\datasets\source_erp\CUST_AZ12.csv'
			WITH (
				FIRSTROW = 2,
				FIELDTERMINATOR = ',',
				TABLOCK
			);
			SET @end_time = GETDATE();

			PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' second';
			PRINT '>> -----------------';
			-- SELECT * FROM Bronze.erp_cust_az12;

			-- Quality Check: Check that the data has not shifted and is in the correct columns.
			-- SELECT COUNT(*) FROM Bronze.erp_cust_az12;

			-----------------------------------------------------------------------------------------
	
			SET @start_time = GETDATE();
			PRINT '>> Truncating Table: Bronze.erp_loc_a101'
			TRUNCATE TABLE Bronze.erp_loc_a101;

			PRINT '>> Inserting Data Into: Bronze.erp_loc_a101'
			BULK INSERT Bronze.erp_loc_a101
			FROM 'C:\Users\Aryab\OneDrive\Desktop\Data Analytics\SQL content\sql-data-warehouse-project\datasets\source_erp\LOC_A101.csv'
			WITH (
				FIRSTROW = 2,
				FIELDTERMINATOR = ',',
				TABLOCK
			);
			SET @end_time = GETDATE();

			PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' second';
			PRINT '>> -----------------';
			-- SELECT * FROM Bronze.erp_loc_a101;

			-- Quality Check: Check that the data has not shifted and is in the correct columns.
			-- SELECT COUNT(*) FROM Bronze.erp_loc_a101;

			-----------------------------------------------------------------------------------------
			SET @start_time = GETDATE();
			PRINT '>> Truncating Table: Bronze.erp_px_cat_giv2'
			TRUNCATE TABLE Bronze.erp_px_cat_giv2;

			PRINT '>> Inserting Data Into: Bronze.erp_px_cat_giv2'
			BULK INSERT Bronze.erp_px_cat_giv2
			FROM 'C:\Users\Aryab\OneDrive\Desktop\Data Analytics\SQL content\sql-data-warehouse-project\datasets\source_erp\PX_CAT_G1V2.csv'
			WITH (
				FIRSTROW = 2,
				FIELDTERMINATOR = ',',
				TABLOCK
			);
			SET @end_time = GETDATE();
		
			PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' second';
			PRINT '>> -----------------';
			-- SELECT * FROM Bronze.erp_px_cat_giv2;

			-- Quality Check: Check that the data has not shifted and is in the correct columns.
			-- SELECT COUNT(*) FROM Bronze.erp_px_cat_giv2;

			-----------------------------------------------------------------------------------------
		SET @batch_end_time = GETDATE();
		PRINT '===============================================================';
		PRINT 'Loading Bronze layer is completed';
		PRINT '  Total Load Duration: ' + CAST(DATEDIFF(SECOND, @batch_start_time, @batch_end_time) AS NVARCHAR) + ' seconds';
		PRINT '===============================================================';


	END TRY
	BEGIN CATCH
		PRINT '==========================================================================='
		PRINT 'ERROR OCCURED DURING LOADING BRONZE LAYER'
		PRINT 'Error Message' + ERROR_MESSAGE();
		PRINT 'Error Number' + CAST(ERROR_NUMBER() AS NVARCHAR);
		PRINT 'Error State' + CAST(ERROR_NUMBER() AS NVARCHAR);
		PRINT '==========================================================================='

	END CATCH

END


-- Track ETL Duration
-- Helps to identify bottleneck, optimize performance, monitor trends, detect issues.

EXEC Bronze.load_bronze