
create or replace procedure silver.layer_transformation_dataloading()
returns string
language SQL
as
$$
begin
    --- 1. CRM_CUST_INFO ---
    insert into silver.crm_cust_info (CST_ID, CST_KEY, CST_FIRSTNAME, CST_LASTNAME, CST_MATERIAL_STATUS, CST_GNDR, CST_CREATE_DATE)
    select
        cst_id,
        cst_key,
        trim(cst_firstname),
        trim(cst_lastname),
        case
            when upper(trim(CST_MATERIAL_STATUS)) ='S' then 'Single'
            when upper(trim(CST_MATERIAL_STATUS)) ='M' then 'Married'
            else 'N/A'
        end,
        case  
            when upper(trim(cst_gndr)) = 'F' then 'Female'
            when upper(trim(cst_gndr)) = 'M' then 'Male'
            else 'N/A'
        end,
        cst_create_date
    from (
        select *,
        row_number() over(partition by cst_id order by cst_create_date desc) as flg_lst
        from bronze.crm_cust_info
    ) where flg_lst = 1;

    --- 2. CRM_PRD_INFO ---
    insert into silver.crm_prd_info (PRD_ID, CAT_ID, PRD_KEY, PRD_NM, PRD_COST, PRD_LINE, PRD_START_DT, PRD_END_DT)
    select
        PRD_ID,
        replace(substr(PRD_KEY, 1, 5), '-', '_'), 
        substr(PRD_KEY, 7), 
        PRD_NM,
        ifnull(PRD_COST, 0),
        case upper(trim(prd_line))
            when 'M' Then 'Mountain'
            when 'R' then 'Road'
            when 'S' then 'Other Sales'
            when 'T' then 'Touring'
            else 'N/A'
        end,
        PRD_START_DT::date,
        -- Note: Lead uses the original PRD_KEY from the table here
        to_date(lead(prd_start_dt) over (partition by PRD_KEY order by prd_start_dt)) - 1
    from bronze.crm_prd_info;

    --- 3. CRM_SALES_DETAILS ---
    insert into silver.crm_sales_details (SLS_ORD_NUM, SLS_PRD_KEY, SLS_CUST_ID, SLS_ORDER_DT, SLS_SHIP_DT, SLS_DUE_DT, SLS_SALES, SLS_QUANTITY, SLS_PRICE)
    select 
        SLS_ORD_NUM,
        SLS_PRD_KEY,
        SLS_CUST_ID,
        case when sls_order_dt = 0 or length(sls_order_dt) != 8 then null else to_date(sls_order_dt::varchar, 'YYYYMMDD') end,
        case when SLS_SHIP_DT = 0 or length(SLS_SHIP_DT) != 8 then null else to_date(SLS_SHIP_DT::varchar, 'YYYYMMDD') end,
        case when SLS_DUE_DT = 0 or length(SLS_DUE_DT) != 8 then null else to_date(SLS_DUE_DT::varchar, 'YYYYMMDD') end,
        case when SLS_SALES is null or SLS_SALES <= 0 or SLS_SALES != sls_quantity * abs(sls_price) then sls_quantity * abs(sls_price) else sls_sales end,
        SLS_QUANTITY,
        case when sls_price is null or sls_price <= 0 then sls_sales / nullif(SLS_QUANTITY, 0) else sls_price end
    from bronze.crm_sales_details;

    --- 4. ERP_CUST_AZ12 ---
    insert into silver.erp_cust_az12 (CID, BDATE, GEN)
    select
        case when cid like 'NAS%' then substr(cid, 4) else cid end,
        case when bdate > current_date() then null else bdate end,
        case when upper(trim(gen)) in ('F', 'FEMALE') then 'Female' when upper(trim(gen)) in ('M', 'MALE') then 'Male' else 'N/A' end
    from bronze.erp_cust_az12;

    --- 5. ERP_LOC_A101 ---
    insert into silver.erp_loc_a101 (CID, CNTRY)
    select 
        replace(cid, '-', ''),
        case 
            when trim(cntry) = 'DE' then 'Germany'
            when trim(cntry) in ('US', 'USA') then 'United States'
            when trim(cntry) = '' or cntry is null then 'N/A'
            else trim(cntry)
        end
    from bronze.erp_loc_a101;

    --- 6. ERP_PX_CAT_G1V2 ---
    insert into silver.erp_px_cat_g1v2 (ID, CAT, SUBCAT, MAINTENANCE)
    select ID, CAT, SUBCAT, MAINTENANCE from bronze.erp_px_cat_g1v2;

    return 'All Tables transformations done successfully and loaded into silver layer Tables';
end;
$$;


call DATAWAREHOUSE.SILVER.LAYER_TRANSFORMATION_DATALOADING();
