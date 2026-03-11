
use datawarehouse ;


create  file format if not exists bronze.CSV_FORMAT
type = 'CSV'
FIELD_DELIMITER =','
skip_header =1
EMPTY_FIELD_AS_NULL =true;

create or replace stage  bronze.bronze_stage
file_format = bronze.csv_format;



call DATAWAREHOUSE.BRONZE.DATA_LOADING_PROCEDURE_BRONZE();


CREATE or replace PROCEDURE  datawarehouse.bronze.data_loading_procedure_bronze()
RETURNS STRING
LANGUAGE SQL
AS
$$
BEGIN

    SELECT '============================================';
    SELECT '-------- Loading CRM Tables ---------------';
    SELECT '============================================';

    --CRM TABLES---

    TRUNCATE TABLE datawarehouse.bronze.crm_cust_info;

    COPY INTO datawarehouse.bronze.crm_cust_info
    FROM @DATAWAREHOUSE.BRONZE.BRONZE_STAGE
    PATTERN='.*cust_info.*.csv';

    TRUNCATE TABLE datawarehouse.bronze.crm_prd_info;

    COPY INTO datawarehouse.bronze.crm_prd_info
    FROM @DATAWAREHOUSE.BRONZE.BRONZE_STAGE
    PATTERN='.*prd_info.*.csv';

    TRUNCATE TABLE datawarehouse.bronze.crm_sales_details;

    COPY INTO datawarehouse.bronze.crm_sales_details
    FROM @DATAWAREHOUSE.BRONZE.BRONZE_STAGE
    PATTERN='.*sales_details.*.csv';


    SELECT '============================================';
    SELECT '-------- Loading ERP Tables ---------------';
    SELECT '============================================';

    -- ERP TABLES---

    TRUNCATE TABLE datawarehouse.bronze.erp_cust_az12;

    COPY INTO datawarehouse.bronze.erp_cust_az12
    FROM @DATAWAREHOUSE.BRONZE.BRONZE_STAGE
    PATTERN='.*CUST_AZ12.*.csv';

    TRUNCATE TABLE datawarehouse.bronze.erp_loc_a101;

    COPY INTO datawarehouse.bronze.erp_loc_a101
    FROM @DATAWAREHOUSE.BRONZE.BRONZE_STAGE
    PATTERN='.*LOC_A101.*.csv';

    TRUNCATE TABLE datawarehouse.bronze.erp_px_cat_g1v2;

    COPY INTO datawarehouse.bronze.erp_px_cat_g1v2
    FROM @DATAWAREHOUSE.BRONZE.BRONZE_STAGE
    PATTERN='.*PX_CAT_G1V2.*.csv';

    RETURN 'Bronze data loading completed successfully';

END;
$$;

list @DATAWAREHOUSE.BRONZE.Bronze_STAGE;

COPY INTO datawarehouse.bronze.erp_px_cat_g1v2
    FROM @datawarehouse.bronze.bronze_stage
    PATTERN='.*px_cat_g1v2.*.csv';

COPY INTO datawarehouse.bronze.crm_cust_info
FROM @DATAWAREHOUSE.BRONZE.BRONZE_STAGE
PATTERN='.*cust_info.*.csv';





select * from datawarehouse.bronze.erp_loc_a101;





drop stage DATAWAREHOUSE.BRONZE.BORNZE_STAGE;


select * from snowflake.account_usage.copy_history;








select file_name, error_count, status, last_load_time from snowflake.account_usage.copy_history
  order by last_load_time desc
  limit 10;





