select 
-- ///// BASE DATA /////
'air_trans_fact' as source
,'' as bkg_itm_id
,a.bkg_id -- bkg_id 
,a.itin_nbr as itin_nbr 
,a.trl as trl 
,a.trans_typ_key as trans_typ_key 
,e.trans_cat_name as trans_cat_name
,e.trans_typ_name as trans_typ_name
,a.tpid as tpid 
,f.tpid_name as tpid_name                   
,a.lgl_entity_key as lgl_entity_key 
,a.mgmt_unit_key as mgmt_unit_key 
,a.oracle_gl_product_key as oracle_gl_product_key 
,a.gl_product_key as gl_product_key 
,a.order_src_sys_id as order_src_sys_id 
-- ///// BOOKING DETAILS /////
,a.bkg_lang_key as trans_language_id
,(case when upper(m.online_offln_ind) like '%UNKNOWN%' then 'unknown' when upper(m.online_offln_ind) like '%OFFLINE%' then 'offline' else 'online' end) as online_offline_ind
,a.purch_trvl_acct_key as trvl_acct_key
,g.trvl_acct_id as trvl_acct_id
,a.bk_datetm -- bk_datetm
,to_date(a.bk_datetm) as bk_date
,a.begin_use_date_key as begin_use_date 
,a.end_use_date_key as end_use_date
,datediff(a.begin_use_date_key,to_date(a.bk_datetm)) as bkg_windw_days
,a.trans_datetm as trans_datetm
,to_date(a.trans_datetm) as trans_date
,datediff(a.end_use_date_key,a.begin_use_date_key) as bkg_los 
,a.air_trans_tckt_cnt as unit_cnt 
,a.air_trans_cnt as trans_cnt 
,d.adult_cnt as adult_cnt 
,d.child_cnt as child_cnt 
,'' as infant_cnt 
,(case when upper(h.pkg_ind) = 'PART OF PACKAGE' then 1 else 0 end) as pkg_ind 
,h.product_ln_name as product_ln_name
,h.business_model_name as business_model_name
,h.business_model_subtyp_name as business_model_subtyp_name
,k.product_ln_component_name as product_ln_component_name
,'' as price_model_name
,'' as credt_card_typ_name
-- ///// SUPPLIER GEO /////
,oa.airpt_latitude as origin_lat
,oa.airpt_longitude as origin_long
,da.airpt_latitude as destination_lat
,da.airpt_longitude as destination_long
,b.orign_airpt_code as origin_cd    
,b.dest_airpt_code as destination_cd 
,b.orign_airpt_city_name as origin_city
,b.dest_airpt_city_name as destination_city 
,b.orign_airpt_state_provnc_name as origin_state_prov
,b.dest_airpt_state_provnc_name as destination_state_prov  
,b.orign_airpt_cntry_name as origin_country
,b.dest_airpt_cntry_name as destination_country
,b.orign_airpt_cntry_code as origin_country_code
,b.dest_airpt_cntry_code as destination_country_code
-- ///// CUSTOMER GEO /////
,'' as cust_origin_city
,b.dest_airpt_city_name as cust_destination_city
,'' as cust_origin_state_prov
,b.dest_airpt_state_provnc_name as cust_destination_state_prov
,'' as cust_origin_country
,b.dest_airpt_cntry_name as cust_destination_country
,'' as cust_origin_country_code
,b.dest_airpt_cntry_code as cust_destination_country_code
,'' as trip_typ_name 
-- ///// PRODUCT /////
,'' as supplier_parent_name
,c.airln_carrier_name as supplier_name  
,a.platng_carrier_key as supplier_key 
,'' as supply_type
,a.air_trans_seg_cnt as air_trans_seg_cnt 
,d.class_of_svc_name as class_of_svc_name
,d.splt_tckt_ind_key as splt_tckt_ind_key 
-- ///// MONEY /////
,a.gross_bkg_amt_usd as gross_bkg_amt_usd 
,a.base_price_amt_usd as base_price_amt_usd 
,a.base_cost_amt_usd as base_cost_amt_usd 
,a.frnt_end_cmsn_amt_usd as frnt_end_cmsn_amt_usd 
,a.totl_tax_price_amt_usd as totl_tax_price_amt_usd 
,a.totl_tax_cost_amt_usd as totl_tax_cost_amt_usd 
,a.totl_fee_price_amt_usd as totl_fee_price_amt_usd 
,a.totl_fee_cost_amt_usd as totl_fee_cost_amt_usd 
,a.margn_amt_usd as margn_amt_usd 
,a.est_gds_rev_price_amt_usd as est_gds_rev_price_amt_usd 
,a.est_gds_rebate_cost_amt_usd as est_gds_rebate_cost_amt_usd 
,a.est_gds_paymnt_amt_usd as est_gds_paymnt_amt_usd 
,a.est_gds_conect_cost_amt_usd as est_gds_conect_cost_amt_usd 
,a.est_totl_gds_rev_amt_usd as est_totl_gds_rev_amt_usd 
,a.est_net_rev_amt_usd as est_net_rev_amt_usd 
,a.est_cost_of_sale_amt_usd as est_cost_of_sale_amt_usd 
,a.est_gross_profit_amt_usd as est_gross_profit_amt_usd 
,a.pkg_save_price_amt_usd as pkg_save_price_amt_usd 
,a.coupn_price_amt_usd as coupn_price_amt_usd 
,a.coupn_key as coupn_key 
-- ///// CUSTOMER /////
,to_date(g.trvl_acct_create_date)  as trvl_acct_create_date
,(case when upper(g.trvl_acct_typ_name) = 'TRAVELER' then 'full' when upper(g.trvl_acct_typ_name) like '%SINGLE USE%' then 'guest' else 'other' end) as trvl_acct_category
,upper(trim(g.trvl_acct_email_addr)) as email_addr
from      dm.air_trans_fact a
                                left outer join dm.route_dim b on a.tckt_route_key = b.route_key
                                left outer join dm.airln_carrier_dim c on a.platng_carrier_key = c.airln_carrier_key
                                left outer join dm.airpt_dim oa on b.orign_airpt_code = oa.airpt_code
                                left outer join dm.airpt_dim da on b.dest_airpt_code = da.airpt_code
                                left outer join 
                                                (
                                                select    sum(case when TRVLR_AGE_CAT_NAME in ('Adult','Senior') then 1 else 0 end) as adult_cnt,
                                                                                sum(case when TRVLR_AGE_CAT_NAME='Child' then 1 else 0 end) as child_cnt,
                                                                                sum(case when TRVLR_AGE_CAT_NAME='Infant' then 1 else 0 end) as infant_cnt,
                                                                                max(splt_tckt_ind_key) as splt_tckt_ind_key,
                                                                                max(air_trip_typ_name) as air_trip_typ_name,
                                                                                max(class_of_svc_name) as class_of_svc_name,
                                                                                bkg_id
                                                from      dm.air_tckt_trans_fact c
                                                                                left outer join dm.air_bkg_ind_dim d on c.air_bkg_ind_key = d.air_bkg_ind_key
                                                where   c.trans_month = '2016-01'
                                                group by bkg_id
                                                ) d on a.bkg_id = d.bkg_id
                                left outer join dm.trans_typ_dim e on a.trans_typ_key = e.trans_typ_key
                                left outer join dm.tpid_dim f on a.tpid = f.tpid
                                left outer join dm.trvl_acct_dim g on a.tpid = g.trvl_acct_tpid and a.purch_trvl_acct_key = g.trvl_acct_key
                                left outer join dm.product_ln_dim h on a.product_ln_key = h.product_ln_key
                                left outer join dm.product_ln_component_dim k on a.bkg_product_ln_component_key = k.product_ln_component_key
                                left outer join dm.bkg_ind_dim m on a.air_bkg_ind_key = m.bkg_ind_key
where a.trans_month = '2016-01'
and a.tpid = 1
limit 100
