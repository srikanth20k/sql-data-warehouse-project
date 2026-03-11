

--chech for nulls or duplicates in primary key --

select
cst_id,
count(*)
from silver.crm_cust_info
group by cst_id
having count(*)>1 or cst_id is null ;

select cst_firstname from silver.crm_cust_info
where cst_firstname != trim(cst_firstname);


select* from bronze.crm_cust_info;

show columns in table bronze.crm_cust_info;

show columns in table silver.crm_cust_info;


-- delete
select*
from silver.crm_cust_info;

select * from silver.crm_cust_info;



show columns in table silver.crm_prd_info;





show columns in table silver.crm_prd_info;


select * from silver.crm_prd_info;


-- checks 
select prd_nm
from silver.crm_prd_info
where prd_nm!=trim(prd_nm);


select prd_cost
from silver.crm_prd_info
where prd_cost<0 or prd_cost is null;

select distinct prd_line
from silver.crm_prd_info;


SELECT * from 
silver.crm_prd_info
where prd_end_dt < prd_start_dt;









show columns in table silver.crm_sales_details;



select sls_order_dt,sls_order_dt:: varchar(10),
case 
    when sls_order_dt =0 or length(sls_order_dt)!=8 then null
    else to_date((sls_order_dt :: varchar(10) ),'YYYYMMDD')
end sls_order_dt
    from 
bronze.crm_sales_details;



select * from silver.crm_sales_details;



select distinct
sls_sales,
sls_quantity,
sls_price
from silver.crm_sales_details
where sls_sales != sls_quantity* sls_price
or sls_sales is null or sls_quantity is null or sls_price is null
or sls_sales <=0 or sls_quantity <=0 or sls_price<=0
order by sls_sales,sls_quantity,sls_price;













select * from bronze.erp_cust_az12
where cid not like 'NAS%';

select distinct CID from bronze.erp_cust_az12;

show columns in table silver.erp_cust_az12;


select * from silver.erp_cust_az12;








show columns in table silver.erp_loc_a101;

select distinct cntry from bronze.erp_loc_a101;


select * from silver.erp_loc_a101;















show columns in table silver.erp_px_cat_g1v2;
select * from bronze.erp_px_cat_g1v2;



select * from silver.erp_px_cat_g1v2;
