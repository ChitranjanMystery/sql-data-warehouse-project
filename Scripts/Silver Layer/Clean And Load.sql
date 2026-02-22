-------------------------------------------------------------------------------------------------------------------------------------------
-- # Clean & Load (crm_cust_info)
-------------------------------------------------------------------------------------------------------------------------------------------

-- Checks for Nulls or Duplicates in Primary key
-- Expectation: No Result

-- Quality check: A Primary key must be unique and not null.
SELECT
	cst_id,
	COUNT(*)
FROM Bronze.crm_cust_info
GROUP BY cst_id
HAVING COUNT(*) > 1 OR cst_id IS NULL

SELECT 
		*,
		ROW_NUMBER() OVER(PARTITION BY cst_id ORDER BY cst_create_date DESC) as flag_last
FROM Bronze.crm_cust_info
WHERE cst_id = 29466

-- ROW_NUMBER (): Assigns a unique number to each row in a result set, based on defined order.
SELECT
	*
FROM (
	SELECT 
		*,
		ROW_NUMBER() OVER(PARTITION BY cst_id ORDER BY cst_create_date DESC) as flag_last
	FROM Bronze.crm_cust_info
)t WHERE flag_last = 1 AND cst_id = 29466

SELECT
	*
FROM (
	SELECT 
		*,
		ROW_NUMBER() OVER(PARTITION BY cst_id ORDER BY cst_create_date DESC) as flag_last
	FROM Bronze.crm_cust_info
)t WHERE flag_last = 1 

-- Quality check: Check for unwanted spaces in string values.
-- If the original value is not equal to the same value after trimming it means there are spaces!
-- Expectation: No Result

SELECT cst_firstname
FROM Bronze.crm_cust_info
WHERE cst_firstname != TRIM(cst_firstname)

SELECT cst_lastname
FROM Bronze.crm_cust_info
WHERE cst_lastname != TRIM(cst_lastname)

SELECT cst_gndr
FROM Bronze.crm_cust_info
WHERE cst_gndr != TRIM(cst_gndr)


SELECT
	cst_id,
	cst_key,
	TRIM(cst_firstname) AS cst_firstname,
	TRIM(cst_lastname) AS cst_lastname,
	cst_marital_status,
	cst_gndr,
	cst_create_date
FROM (
	SELECT 
		*,
		ROW_NUMBER() OVER(PARTITION BY cst_id ORDER BY cst_create_date DESC) as flag_last
	FROM Bronze.crm_cust_info
)t WHERE flag_last = 1 


-- Quality Check: Check the consistency of values in low cardinality columns.
-- Data Standardization & Consistency
SELECT DISTINCT cst_gndr
FROM Bronze.crm_cust_info

SELECT DISTINCT cst_marital_status
FROM Bronze.crm_cust_info

-- In our data warehouse, we aim to store clear and meaningful values rather than using abbreviated terms.
-- In our data warehouse we use the default value 'N/A' for missing values!
-- Apply UPPER() just in case mixed-case values appears later in your column.
-- Apply TRIM() just in case spaces appear later in your column.

SELECT * FROM Silver.crm_cust_info
---------------------------------------------------------------------------------------
/*
1. Remove Unwanted Spaces
	Removes unnecessary spaces to ensures data consistency and uniformity across all records. 
2. Data Normalization & Standardization
	Maps coded values to meaningful, user-friendly descriptions.
3. Handling missing data
	Fills in the blanks by adding a default value.
4. Remove duplicates
	Ensure only one record per entity by identifying and retaining the most relevant row.
*/

PRINT '>> Truncating Table: Silver.crm_cust_info'
TRUNCATE TABLE Silver.crm_cust_info;

PRINT '>> Inserting Data Into: Silver.crm_cust_info';
INSERT INTO Silver.crm_cust_info (
	cst_id,
	cst_key,
	cst_firstname,
	cst_lastname,
	cst_marital_status,
	cst_gndr,
	cst_create_date
)
SELECT
	cst_id,
	cst_key,
	TRIM(cst_firstname) AS cst_firstname,
	TRIM(cst_lastname) AS cst_lastname,
	CASE
		WHEN UPPER(TRIM(cst_marital_status)) = 'S' THEN 'Single'
		WHEN UPPER(TRIM(cst_marital_status)) = 'M' THEN 'Married'
		ELSE 'n/a'
	END cst_material_status, -- Normalize marital status values a readable format
	CASE
		WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'Female'
		WHEN UPPER(TRIM(cst_gndr)) = 'M' THEN 'Male'
		ELSE 'n/a'
	END cst_gndr, -- Normalize gender values a readable format
	cst_create_date
FROM (
	SELECT 
		*,
		ROW_NUMBER() OVER(PARTITION BY cst_id ORDER BY cst_create_date DESC) as flag_last
	FROM Bronze.crm_cust_info
	WHERE cst_id IS NOT NULL
)t WHERE flag_last = 1  -- Select the most recent record per customer

-- Quality Check on Silver Layer
-- Re-run the quality check queries from the bronze layer to verify the quality of data in the silver layer.

-- Check for Nulls or Duplicates in Primary key
-- Expectation : No result
SELECT
	cst_id,
	COUNT(*)
FROM Silver.crm_cust_info
GROUP BY cst_id
HAVING COUNT(*) > 1 OR cst_id IS NULL

-- Check for unwanted spaces
-- Expectation : No result
SELECT cst_firstname
FROM Silver.crm_cust_info
WHERE cst_firstname != TRIM(cst_firstname)

SELECT cst_lastname
FROM Silver.crm_cust_info
WHERE cst_lastname != TRIM(cst_lastname)

-- Data Standardization & Consistency
SELECT DISTINCT cst_gndr
FROM Silver.crm_cust_info

SELECT DISTINCT cst_marital_status
FROM Silver.crm_cust_info

SELECT * FROM Silver.crm_cust_info

-------------------------------------------------------------------------------------------------------------------------------------------
-- # Clean & Load (crm_prd_info)
-------------------------------------------------------------------------------------------------------------------------------------------
SELECT
	prd_id,
	prd_key,
	prd_nm,
	prd_cost,
	prd_line,
	prd_start_dt,
	prd_end_dt
FROM Bronze.crm_prod_info

-- Check for Nulls or Duplicates in Primary key
-- Expectations: No result
SELECT
	prd_id,
	COUNT(*)
FROM Bronze.crm_prod_info
GROUP BY prd_id
HAVING COUNT(*) > 1 or prd_id IS NULL

-- Check for unwanted spaces
-- Expectations: No result
SELECT prd_nm
FROM Silver.crm_prod_info
WHERE prd_nm != TRIM(prd_nm)

-- Check for NULLs or Negative Numbers
-- Expectations: No Results
SELECT
	prd_cost
FROM Bronze.crm_prod_info 
WHERE prd_cost < 0 OR prd_cost IS NULL

-- Data Standardization & Consistency
SELECT DISTINCT prd_line
FROM Bronze.crm_prod_info

-- Check for Invalid Data Orders
SELECT
	*
FROM Bronze.crm_prod_info
WHERE prd_end_dt < prd_start_dt   -- End date must not be earlier than the start date.

/*
For complex transformations in SQL, I typically narrow it done to a specific example and brainstorm multiple solutions approach.
#1 Solution: Switch End Date and Start Date
	Issues: The data are overlapping; Each record must has a start date!!
#2 Solution: Derive the End Date from the Start Date
	i.e., End Date = Start Date of the 'NEXT' Record - 1. 
*/

-- LEAD(): Access values from the next row within a window.

SELECT
	prd_id,
	prd_key,
	prd_nm,
	prd_start_dt,
	prd_end_dt,
	LEAD(prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt) - 1 AS prd_end_dt_test
FROM Bronze.crm_prod_info
WHERE prd_key IN ('AC-HE-HL-U509-R', 'AC-HE-HL-U509')

-- Substring(): Extracts a specific part of a string value.
-- Filter out unmatched data after applying transformation
-- ISNULL(): Replaces NULL values with a specified replacement value. We can say COALESCE as well

PRINT '>> Truncating Table: Silver.crm_prod_info'
TRUNCATE TABLE Silver.crm_prod_info;

PRINT '>> Inserting Data Into: Silver.crm_prod_info';
INSERT INTO Silver.crm_prod_info (
	prd_id,
	cat_id,
	prd_key,
	prd_nm,
	prd_cost,
	prd_line,
	prd_start_dt,
	prd_end_dt
)
SELECT
	prd_id,
	REPLACE(SUBSTRING(prd_key, 1, 5), '-', '_') AS cat_id,    -- Extract category ID
	SUBSTRING(prd_key, 7, LEN(prd_key)) AS prd_key,    -- Extract product key
	prd_nm,
	ISNULL(prd_cost, 0) AS prd_cost,
	CASE UPPER(TRIM(prd_line))    -- Quick CASE WHEN Ideal for simple value mapping
		WHEN 'M' THEN 'Mountain'
		WHEN 'R' THEN 'Road'
		WHEN 'S' THEN 'Other Sales'
		WHEN 'T' THEN 'Touring'
	ELSE 'n/a'
	END AS prd_line,  -- Map product line codes to descriptive values
	/*
	CASE 
		WHEN UPPER(TRIM(prd_line)) = 'M' THEN 'Mountain'
		WHEN UPPER(TRIM(prd_line)) = 'R' THEN 'Road'
		WHEN UPPER(TRIM(prd_line)) = 'S' THEN 'Other Sales'
		WHEN UPPER(TRIM(prd_line)) = 'T' THEN 'Touring'
		ELSE 'n/a'
	END AS prd_line,
	*/
	CAST(prd_start_dt AS DATE) AS prd_start_dt,
	CAST(LEAD(prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt) - 1 AS DATE) AS prd_end_dt -- Calculate end date as one day before the next start date.
FROM Bronze.crm_prod_info


/*
WHERE SUBSTRING(prd_key, 7, LEN(prd_key)) IN (
SELECT sls_prd_key FROM Bronze.crm_sales_details 
)
*/

/*
WHERE REPLACE(SUBSTRING(prd_key, 1, 5), '-', '_') NOT IN 
(SELECT DISTINCT id FROM Bronze.erp_px_cat_giv2)
*/

SELECT distinct id from Bronze.erp_px_cat_giv2
SELECT sls_prd_key FROM Bronze.crm_sales_details


-- Check for Nulls or Duplicates in Primary key
-- Expectations: No result
SELECT
	prd_id,
	COUNT(*)
FROM Silver.crm_prod_info
GROUP BY prd_id
HAVING COUNT(*) > 1 or prd_id IS NULL

-- Check for unwanted spaces
-- Expectations: No result
SELECT prd_nm
FROM Silver.crm_prod_info
WHERE prd_nm != TRIM(prd_nm)

-- Check for NULLs or Negative Numbers
-- Expectations: No Results
SELECT
	prd_cost
FROM Silver.crm_prod_info 
WHERE prd_cost < 0 OR prd_cost IS NULL

-- Data Standardization & Consistency
SELECT DISTINCT prd_line
FROM Silver.crm_prod_info

-- Check for Invalid Data Orders
SELECT
	*
FROM Silver.crm_prod_info
WHERE prd_end_dt < prd_start_dt  

SELECT * FROM Silver.crm_prod_info

/*
Note:
	Derived Columns: Create new columns based on calcualtions or transformatios of exsiting ones.
	Data Enrichment: Add new relevant data to enhance the dataset for analysis.
*/

-------------------------------------------------------------------------------------------------------------------------------------------
-- # Clean & Load (crm_sales_details)
-------------------------------------------------------------------------------------------------------------------------------------------

PRINT '>> Truncating Table: Silver.crm_sales_details'
TRUNCATE TABLE Silver.crm_sales_details;

PRINT '>> Inserting Data Into: Silver.crm_sales_details';
INSERT INTO Silver.crm_sales_details (
	sls_ord_num,
	sls_prd_key,
	sls_cust_id,
	sls_order_dt,
	sls_ship_dt,
	sls_due_dt,
	sls_sales,
	sls_quantity,
	sls_price
)
SELECT
	sls_ord_num,
	sls_prd_key,
	sls_cust_id,
	
	CASE 
		WHEN sls_order_dt = 0 OR LEN(sls_order_dt) != 8 THEN NULL
		ELSE CAST(CAST(sls_order_dt AS VARCHAR) AS DATE)
	END AS sls_order_dt,
	
	CASE 
		WHEN sls_ship_dt = 0 OR LEN(sls_ship_dt) != 8 THEN NULL
		ELSE CAST(CAST(sls_ship_dt AS VARCHAR) AS DATE)
	END AS sls_ship_dt,
	
	CASE 
		WHEN sls_due_dt = 0 OR LEN(sls_due_dt) != 8 THEN NULL
		ELSE CAST(CAST(sls_due_dt AS VARCHAR) AS DATE)
	END AS sls_due_dt,
	
	CASE 
		WHEN sls_sales IS NULL OR sls_sales <= 0 OR sls_sales != sls_quantity * ABS(sls_price)
		THEN sls_quantity * ABS(sls_price)
	    ELSE sls_sales
	END AS sls_sales,     -- Recalculate sales if original value is missing or incorrect
	
	sls_quantity,
	
	CASE 
		WHEN sls_price IS NULL OR sls_price <= 0
		THEN sls_sales / NULLIF(sls_quantity, 0)
		ELSE sls_price
	END AS sls_price   -- Derive price if original is invalid

FROM Bronze.crm_sales_details


-- WHERE sls_cust_id NOT IN (SELECT cst_id FROM Silver.crm_cust_info)

-- WHERE sls_prd_key NOT IN (SELECT prd_key FROM Silver.crm_prod_info)

-- WHERE sls_ord_num != TRIM(sls_ord_num)  # Check unwanted spaces

-- Check for Invalid Dates
-- Negative numbers or Zeros can't be cast to a date
-- NULLIF(): Returns NULL if two given values are equal; otherwise, it returns the first expression.

-- In this scenario, the length of the data must be 8
-- Check for outliers by validating the boundaries of the data range.
SELECT
	NULLIF(sls_order_dt, 0) sls_order_dt
FROM Bronze.crm_sales_details 
WHERE sls_order_dt <= 0 
OR LEN(sls_order_dt) != 8 OR 
sls_order_dt > 20500101 OR sls_order_dt < 19000101

SELECT
	NULLIF(sls_ship_dt, 0) sls_ship_dt
FROM Bronze.crm_sales_details 
WHERE sls_ship_dt <= 0 
OR LEN(sls_ship_dt) != 8 OR 
sls_ship_dt > 20500101 OR sls_ship_dt < 19000101


SELECT
	NULLIF(sls_due_dt, 0) sls_due_dt
FROM Bronze.crm_sales_details 
WHERE sls_due_dt <= 0 
OR LEN(sls_due_dt) != 8 OR 
sls_due_dt > 20500101 OR sls_due_dt < 19000101


-- Order date must always be eariler than the shipping date or due date
-- Check for Invalid Data Orders
SELECT
	*
FROM Bronze.crm_sales_details
WHERE sls_order_dt > sls_ship_dt OR sls_order_dt > sls_due_dt

-- Check Data Consistency: Between Sales, Quantity and price
--- >> Sales = Quantity * Price
-- >> Values must not be NULL, zero or negative

SELECT DISTINCT
	sls_sales,
	sls_quantity,
	sls_price
FROM Bronze.crm_sales_details
WHERE sls_sales != sls_quantity * sls_price
OR sls_sales IS NULL OR sls_quantity IS NULL OR sls_price IS NULL
OR sls_sales <= 0 OR sls_quantity <= 0 OR sls_price <= 0
ORDER BY sls_sales, sls_quantity, sls_price

-- #1 Solution: Data Issues will be fixed direct in source system.
-- #2 Solution: Data Issues has to be fixed in data warehouse.

-- Rules: 
-- If Sales is negative, zero, or null, derive it using Quantity and Price.
-- If Price is zero or null, calculate it using Sales and Quantity
-- If Price is negative, convert into a positive value.

SELECT DISTINCT
	sls_sales AS old_sls_sales,
	sls_quantity,
	sls_price AS old_sales_price,
	
	-- ABS(): Returns absolute value of a number
	
	CASE 
		WHEN sls_sales IS NULL OR sls_sales <= 0 OR sls_sales != sls_quantity * ABS(sls_price)
		THEN sls_quantity * ABS(sls_price)
	    ELSE sls_sales
	END AS sls_sales,
	
	CASE 
		WHEN sls_price IS NULL OR sls_price <= 0
		THEN sls_sales / NULLIF(sls_quantity, 0)
		ELSE sls_price
	END AS sls_price

FROM Bronze.crm_sales_details
WHERE sls_sales != sls_quantity * sls_price
OR sls_sales IS NULL OR sls_quantity IS NULL OR sls_price IS NULL
OR sls_sales <= 0 OR sls_quantity <= 0 OR sls_price <= 0
ORDER BY sls_sales, sls_quantity, sls_price


SELECT * FROM Bronze.crm_sales_details


---------------------------------------------------------------------------------------------------------------------
-- # Clean And Load: erp_cust_az12
---------------------------------------------------------------------------------------------------------------------
PRINT '>> Truncating Table: Silver.erp_cust_az12';
TRUNCATE TABLE Silver.crm_sales_details;

PRINT '>> Inserting Data Into: Silver.erp_cust_az12';
INSERT INTO Silver.erp_cust_az12 (cid, bdate, gen)
SELECT
	CASE
		WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid, 4, LEN(cid))   -- Remove 'NAS' prefix if present.
		ELSE cid
	END cid,

	CASE
		WHEN bdate > GETDATE() THEN NULL
		ELSE bdate
	END AS bdate, -- see future birthdates to NULL
	
	CASE
		WHEN UPPER(TRIM(gen)) IN ('F', 'FEMALE') THEN 'Female'
		WHEN UPPER(TRIM(gen)) IN ('M', 'MALE') THEN 'Male'
		ELSE 'n/a'
	END AS gen  -- Normalize gender valies and handle unknown case
FROM Bronze.erp_cust_az12

/*
WHERE CASE WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid, 4, LEN(cid))
	  ELSE cid
END NOT IN (SELECT DISTINCT cst_key FROM Silver.crm_cust_info)
*/

-- WHERE cid LIKE '%AW00011000%'

-- SELECT * FROM Silver.crm_cust_info;

-- Identify Out-of-Ranges Dates
-- Check for older customers
-- Check for birthdays in the future

SELECT DISTINCT
bdate
FROM Bronze.erp_cust_az12
WHERE bdate < '1924-01-01' OR bdate > GETDATE()

-- Data Standardization & Consistency
SELECT DISTINCT 
	gen,
	CASE
		WHEN UPPER(TRIM(gen)) IN ('F', 'FEMALE') THEN 'Female'
		WHEN UPPER(TRIM(gen)) IN ('M', 'MALE') THEN 'Male'
		ELSE 'n/a'
	END AS gen
FROM Bronze.erp_cust_az12

-- Quality Check

SELECT DISTINCT
bdate
FROM Silver.erp_cust_az12
WHERE bdate < '1924-01-01' OR bdate > GETDATE()

-- Data Standardization & Consistency
SELECT DISTINCT
gen
FROM Silver.erp_cust_az12

SELECT * FROM Silver.erp_cust_az12

--------------------------------------------------------------------------------------------------------
-- # Clean & Load: erp_loc_a101
--------------------------------------------------------------------------------------------------------
PRINT '>> Truncating Table: Silver.erp_loc_a101';
TRUNCATE TABLE Silver.erp_loc_a101;

PRINT '>> Inserting Data Into: Silver.erp_loc_a101';
INSERT INTO Silver.erp_loc_a101 (cid, cntry)
SELECT
	REPLACE(cid, '-', '') cid, 
	CASE
		WHEN TRIM(cntry) = 'DE' THEN 'Germany'
		WHEN TRIM(cntry) IN ('US' ,'USA')  THEN 'United States'
		WHEN TRIM(cntry) = '' OR cntry IS NULL THEN 'n/a'
		ELSE TRIM(cntry)
	END AS cntry   -- Normalise and Handle missing or blank country codes.
FROM Bronze.erp_loc_a101


-- WHERE cid NOT IN
-- (SELECT cst_key FROM Silver.crm_cust_info)
 

-- WHERE REPLACE(cid, '-', '') NOT IN
-- (SELECT cst_key FROM Silver.crm_cust_info)

-- Data Consistency and standardization
SELECT DISTINCT 
	cntry AS old_cntry,
	CASE
		WHEN TRIM(cntry) = 'DE' THEN 'Germany'
		WHEN TRIM(cntry) IN ('US' ,'USA')  THEN 'United States'
		WHEN TRIM(cntry) = '' OR cntry IS NULL THEN 'n/a'
		ELSE TRIM(cntry)
	END AS cntry
FROM Bronze.erp_loc_a101

-- Quality Check
SELECT DISTINCT 
	cntry 
FROM Silver.erp_loc_a101
ORDER BY cntry

SELECT * FROM Silver.erp_loc_a101;


--------------------------------------------------------------------------------------------------------
-- # Clean & Load: erp_px_cat_g1v2
--------------------------------------------------------------------------------------------------------
PRINT '>> Truncating Table: Silver.erp_px_cat_giv2';
TRUNCATE TABLE Silver.erp_px_cat_giv2;

PRINT '>> Inserting Data Into: Silver.erp_px_cat_giv2';
INSERT INTO Silver.erp_px_cat_giv2 (id, cat, subcat, maintenance)
SELECT
	id, 
	cat,
	subcat,
	maintenance
FROM Bronze.erp_px_cat_giv2

SELECT cat_id FROM Silver.crm_prod_info

-- Check for unwanted spaces
SELECT * FROM Bronze.erp_px_cat_giv2
WHERE cat != TRIM(cat) OR subcat != TRIM(subcat) OR maintenance != maintenance

-- Data Standardization & Consistency
SELECT DISTINCT
cat
FROM Bronze.erp_px_cat_giv2

SELECT DISTINCT
subcat
FROM Bronze.erp_px_cat_giv2

SELECT DISTINCT
maintenance
FROM Bronze.erp_px_cat_giv2

SELECT * FROM Silver.erp_px_cat_giv2
