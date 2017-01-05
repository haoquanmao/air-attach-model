set mapred.job.queue.name = edwbiz;
set hive.cli.print.header = true; 
set hive.exec.parallel = true;
set hive.cbo.enable = true;
set hive.execution.engine = tez;



--get flight trl table


DROP TABLE IF EXISTS ewwgao.hmao_aa_flight;
CREATE TABLE ewwgao.hmao_aa_flight AS
select a.*
		from
			(select a.*
					,c.trvl_acct_email_addr
					,e.mktg_carrier_key
			from 
					(select un_table.tpid
							,un_table.trl
							,un_table.begin_use_date_key
							,un_table.end_use_date_key
							,un_table.purch_trvl_acct_key
							,un_table.tckt_route_key
							,un_table.bkg_ind_key
							,un_table.tckt_air_fare_typ_key
							,un_table.air_trip_typ_id
							,un_table.trans_date_key
							,un_table.bk_date_key
							,un_table.platng_carrier_key
							,un_table.tckt_cnt
							,un_table.gross_bkg_amt
							,un_table.base_cost
							,un_table.frnt_end_cmsn_amt
							,un_table.est_net_rev_amt
							,un_table.est_gross_profit_amt
							,un_table.tckt_seg_cnt
							,un_table.base_price
							,un_table.child_cnt
							,un_table.senior_cnt
							,un_table.infant_cnt
							,un_table.los
							,un_table.bk_window
							,un_table.product_ln_key
							,un_table.if_fst_class
							,un_table.if_bzns_class
							,un_table.if_cch_class
							,un_table.if_pcch_class
							,un_table.if_etckt
							,un_table.if_ptckt
							,un_table.if_tcktls
							,un_table.if_sat
							,un_table.if_nsat
						from
						(
						select attf.tpid
								,attf.trl
								,attf.begin_use_date_key
								,attf.end_use_date_key
								,attf.purch_trvl_acct_key
								,attf.tckt_route_key
								,max(attf.bkg_ind_key) as bkg_ind_key
								,max(attf.tckt_air_fare_typ_key) as tckt_air_fare_typ_key
								,max(abid.air_trip_typ_id) as air_trip_typ_id
								,min(attf.trans_date_key) as trans_date_key
								,min(attf.bk_date_key) as bk_date_key
								,max(attf.platng_carrier_key) as platng_carrier_key
								,sum(attf.tckt_cnt) as tckt_cnt
								,1.0*sum(attf.gross_bkg_amt_usd) as gross_bkg_amt
								,1.0*sum(attf.tckt_seg_cnt)  as tckt_seg_cnt
								,1.0*sum(attf.base_price_amt_usd)  as base_price
								,1.0*sum(attf.base_cost_amt_usd)  as base_cost
								,1.0*sum(attf.frnt_end_cmsn_amt_usd)  as frnt_end_cmsn_amt
								,1.0*sum(attf.est_net_rev_amt_usd) as est_net_rev_amt
								,1.0*sum(attf.est_gross_profit_amt_usd)  as est_gross_profit_amt			
								,sum(child_dummy) as child_cnt
								,sum(senior_dummy) as senior_cnt
								,sum(infant_dummy) as infant_cnt
								,datediff(attf.end_use_date_key,attf.begin_use_date_key) + 1 as los
								,datediff(attf.begin_use_date_key,min(attf.bk_date_key)) + 1 as bk_window
								,count(distinct abid.air_trip_typ_id) as dist_typ_cnt
								,max(attf.product_ln_key) as product_ln_key
								,case when sum(fst_class_dummy) > 0 then 1 else 0 end as if_fst_class
								,case when sum(bzns_class_dummy) > 0 then 1 else 0 end as if_bzns_class
								,case when sum(cch_class_dummy) > 0 then 1 else 0 end as if_cch_class
								,case when sum(pcch_class_dummy) > 0 then 1 else 0 end as if_pcch_class
								,case when sum(etckt_dummy) > 0 then 1 else 0 end as if_etckt
								,case when sum(ptckt_dummy) > 0 then 1 else 0 end as if_ptckt
								,case when sum(tcktls_dummy) > 0 then 1 else 0 end as if_tcktls
								,case when sum(sat_dummy) > 0 then 1 else 0 end as if_sat
								,case when sum(nsat_dummy) > 0 then 1 else 0 end as if_nsat
						from (select *
									,case when air_bkg_ind_key between 625 and 699 then 1 else 0 end as child_dummy
									,case when air_bkg_ind_key between 550 and 624 then 1 else 0 end as senior_dummy
									,case when air_bkg_ind_key between 700 and 774 then 1 else 0 end as infant_dummy
									from dm.air_tckt_trans_fact) attf
							inner join (select * 
											,case when class_of_svc_id = 1 then 1 else 0 end as fst_class_dummy
											,case when class_of_svc_id = 2 then 1 else 0 end as bzns_class_dummy
											,case when class_of_svc_id = 3 then 1 else 0 end as cch_class_dummy
											,case when class_of_svc_id = 5 then 1 else 0 end as pcch_class_dummy
											,case when air_flfill_methd_id = 2 then 1 else 0 end as etckt_dummy
											,case when air_flfill_methd_id = 3 then 1 else 0 end as ptckt_dummy
											,case when air_flfill_methd_id = 4 then 1 else 0 end as tcktls_dummy
											,case when lower(substr(sat_night_stay_ind,1,1)) = 'i' then 1 else 0 end as sat_dummy
											,case when lower(substr(sat_night_stay_ind,1,1)) = 'e' then 1 else 0 end as nsat_dummy
											from dm.air_bkg_ind_dim) abid on attf.air_bkg_ind_key = abid.air_bkg_ind_key
						where attf.bk_date_key between '2014-01-01' and '2016-12-31'
								and attf.trans_typ_key = 101
								and abid.air_trip_typ_id in (1,2)
								and attf.splt_tckt_ind_key <> 101
						group by attf.tpid
								,attf.trl
								,attf.begin_use_date_key
								,attf.end_use_date_key
								,attf.purch_trvl_acct_key
								,attf.tckt_route_key

						union all
						
						select attf.tpid
								,attf.trl
								,min(attf.begin_use_date_key) as begin_use_date_key
								,max(attf.end_use_date_key) as end_use_date_key
								,attf.purch_trvl_acct_key
								,int(substr(min(composite_begin_route),12,100)) as tckt_route_key
								,max(attf.bkg_ind_key) as bkg_ind_key
								,max(attf.tckt_air_fare_typ_key) as tckt_air_fare_typ_key
								,999 as air_trip_typ_id
								,min(attf.trans_date_key) as trans_date_key
								,min(attf.bk_date_key) as bk_date_key
								,max(attf.platng_carrier_key) as platng_carrier_key
								,sum(attf.tckt_cnt) as tckt_cnt
								,1.0*sum(attf.gross_bkg_amt_usd)  as gross_bkg_amt				
								,1.0*sum(attf.tckt_seg_cnt)  as tckt_seg_cnt
								,1.0*sum(attf.base_price_amt_usd)  as base_price
								,1.0*sum(attf.base_cost_amt_usd)  as base_cost
								,1.0*sum(attf.frnt_end_cmsn_amt_usd)  as frnt_end_cmsn_amt
								,1.0*sum(attf.est_net_rev_amt_usd)  as est_net_rev_amt
								,1.0*sum(attf.est_gross_profit_amt_usd)  as est_gross_profit_amt					
								,sum(child_dummy) as child_cnt
								,sum(senior_dummy) as senior_cnt
								,sum(infant_dummy) as infant_cnt
								,datediff(max(attf.end_use_date_key),min(attf.begin_use_date_key)) + 1 as los
								,datediff(min(attf.begin_use_date_key),min(attf.bk_date_key)) + 1 as bk_window
								,count(distinct abid.air_trip_typ_id) as dist_typ_cnt
								,max(attf.product_ln_key) as product_ln_key
								,case when sum(fst_class_dummy) > 0 then 1 else 0 end as if_fst_class
								,case when sum(bzns_class_dummy) > 0 then 1 else 0 end as if_bzns_class
								,case when sum(cch_class_dummy) > 0 then 1 else 0 end as if_cch_class
								,case when sum(pcch_class_dummy) > 0 then 1 else 0 end as if_pcch_class
								,case when sum(etckt_dummy) > 0 then 1 else 0 end as if_etckt
								,case when sum(ptckt_dummy) > 0 then 1 else 0 end as if_ptckt
								,case when sum(tcktls_dummy) > 0 then 1 else 0 end as if_tcktls
								,case when sum(sat_dummy) > 0 then 1 else 0 end as if_sat
								,case when sum(nsat_dummy) > 0 then 1 else 0 end as if_nsat				
						from (select *
									,case when air_bkg_ind_key between 625 and 699 then 1 else 0 end as child_dummy
									,case when air_bkg_ind_key between 550 and 624 then 1 else 0 end as senior_dummy
									,case when air_bkg_ind_key between 700 and 774 then 1 else 0 end as infant_dummy
									,concat(begin_use_date_key,'_',tckt_route_key) as composite_begin_route
									from dm.air_tckt_trans_fact) attf
							inner join (select * 
											,case when class_of_svc_id = 1 then 1 else 0 end as fst_class_dummy
											,case when class_of_svc_id = 2 then 1 else 0 end as bzns_class_dummy
											,case when class_of_svc_id = 3 then 1 else 0 end as cch_class_dummy
											,case when class_of_svc_id = 5 then 1 else 0 end as pcch_class_dummy
											,case when air_flfill_methd_id = 2 then 1 else 0 end as etckt_dummy
											,case when air_flfill_methd_id = 3 then 1 else 0 end as ptckt_dummy
											,case when air_flfill_methd_id = 4 then 1 else 0 end as tcktls_dummy
											,case when lower(substr(sat_night_stay_ind,1,1)) = 'i' then 1 else 0 end as sat_dummy
											,case when lower(substr(sat_night_stay_ind,1,1)) = 'e' then 1 else 0 end as nsat_dummy
											from dm.air_bkg_ind_dim) abid on attf.air_bkg_ind_key = abid.air_bkg_ind_key
						where attf.bk_date_key between '2014-01-01' and '2016-12-31'
								and attf.trans_typ_key = 101
								and abid.air_trip_typ_id in (1,2)
								and attf.splt_tckt_ind_key = 101
						group by attf.tpid
								,attf.trl
								,attf.purch_trvl_acct_key
						) un_table
						where un_table.dist_typ_cnt = 1
					) a
					inner join (select *
								from dm.trvl_acct_dim
								where lower(trvl_acct_email_addr) != 'unknown' and trvl_acct_tpid > 0) c on a.purch_trvl_acct_key = c.trvl_acct_key and a.tpid = c.trvl_acct_tpid
					left join (select tpid,trl,max(mktg_carrier_key) as mktg_carrier_key
								from dm.air_seg_trans_fact 
								group by tpid,trl) e on a.tpid = e.tpid and a.trl = e.trl
			) a								
			left join
			(select 
				a.tpid 
				,a.trl
			 from dm.air_trans_fact a
			 where trans_typ_key between 102 and 104
			 group by a.tpid 
					,a.trl
				) b on a.tpid = b.tpid and a.trl = b.trl
		where b.tpid is Null 
		and datediff(a.end_use_date_key,a.begin_use_date_key) >= 0
		and a.tpid > 0
		and a.trl > 0;












DROP TABLE IF EXISTS ewwgao.hmao_aa_flight2;
CREATE TABLE ewwgao.hmao_aa_flight2 AS
select a.*
	,b.visit_mktg_code
	,b.rlt_mktg_code
	,b.site_id
from ewwgao.hmao_aa_flight a
left join
(select * 
from 
(select a.*	
		,rank() over (partition by tpid,trl,trvl_acct_email_addr,begin_use_date_key order by site_id) as rank
from
(select tpid,trl,trvl_acct_email_addr,begin_use_date_key,site_id,max(visit_mktg_code) as visit_mktg_code,max(rlt_mktg_code) as rlt_mktg_code
from
(select a.*,b.trvl_acct_email_addr
from
(select *
from dm.dm_omniture_trl_level_summary 
where not (product_ln_name  like '%PKG%')
		and trl > 0
		and tpid > 0) a
inner join (select * 
			from dm.trvl_acct_dim
			where lower(trvl_acct_email_addr) != 'unknown' and trvl_acct_tpid > 0) b on a.tuid = b.trvl_acct_id and a.tpid = b.trvl_acct_tpid
) a
group by tpid,trl,trvl_acct_email_addr,begin_use_date_key,site_id
) a
) a
where rank = 1) b on a.tpid = b.tpid and a.trl = b.trl and a.begin_use_date_key = b.begin_use_date_key	
					and a.trvl_acct_email_addr = b.trvl_acct_email_addr;


		


		

--get flight core table 
DROP TABLE IF EXISTS ewwgao.hmao_aa_flight_core_v2;
CREATE TABLE ewwgao.hmao_aa_flight_core_v2 AS
select core.tpid
		,core.begin_use_date_key
		,core.end_use_date_key
		,core.trvl_acct_email_addr
		,core.tckt_route_key
		,collect_set(core.trl) as list_trl
		,collect_set(core.visit_mktg_code) as list_visit_mktg_code
		,collect_set(core.rlt_mktg_code) as list_rlt_mktg_code
		,collect_set(core.mktg_carrier_key) as list_mktg_carrier_key
		,count(distinct core.trl) as num_trl
		,count(distinct core.visit_mktg_code) as num_visit_mktg_code
		,count(distinct core.rlt_mktg_code) as num_rlt_mktg_code
		,count(distinct core.mktg_carrier_key) as num_mktg_carrier_key
		,min(site_id) as site_id
		,max(core.visit_mktg_code) as visit_mktg_code
		,max(core.rlt_mktg_code) as rlt_mktg_code
		,max(core.mktg_carrier_key) as mktg_carrier_key
		,max(core.platng_carrier_key) as platng_carrier_key
		,max(core.bkg_ind_key) as bkg_ind_key
		,max(core.tckt_air_fare_typ_key) as tckt_air_fare_typ_key
		,max(core.air_trip_typ_id) as air_trip_typ_id
		,min(core.trans_date_key) as trans_date_key
		,min(core.bk_date_key) as bk_date_key
		,sum(core.tckt_cnt) as tckt_cnt
		,sum(core.gross_bkg_amt) as gross_bkg_amt
		,sum(core.base_cost) as base_cost
		,sum(core.frnt_end_cmsn_amt) as frnt_end_cmsn_amt
		,sum(core.est_net_rev_amt) as est_net_rev_amt
		,sum(core.est_gross_profit_amt) as est_gross_profit_amt
		,sum(core.tckt_seg_cnt) as tckt_seg_cnt
		,sum(core.base_price) as base_price
		,sum(core.child_cnt) as child_cnt
		,sum(core.senior_cnt) as senior_cnt
		,sum(core.infant_cnt) as infant_cnt
		,max(core.los) as los
		,max(core.bk_window) as  bk_window
		,max(core.product_ln_key) as  product_ln_key
		,max(core.if_fst_class) as if_fst_class
		,max(core.if_bzns_class) as if_bzns_class
		,max(core.if_cch_class) as if_cch_class
		,max(core.if_pcch_class) as if_pcch_class
		,max(core.if_etckt) as if_etckt
		,max(core.if_ptckt) as if_ptckt
		,max(core.if_tcktls) as if_tcktls
		,max(core.if_sat) as if_sat
		,max(core.if_nsat) as if_nsat
from ewwgao.hmao_aa_flight2 core
group by core.tpid
		,core.begin_use_date_key
		,core.end_use_date_key
		,core.trvl_acct_email_addr
		,core.tckt_route_key;



--add other information
'''
drop table if exists ewwgao.hmao_cntry_holiday_list;
create external table ewwgao.hmao_cntry_holiday_list 
(cntry_code string, h_date string) 
row format delimited 
fields terminated by ',' 
stored as textfile location "/user/hmao/cntry_holiday_list2"
'''

'''
alter table ewwgao.hmao_cntry_holiday_list add columns (r_date date);
insert into table ewwgao.hmao_cntry_holiday_list
select *,to_date(FROM_UNIXTIME(unix_timestamp(h_date, 'mm/dd/yyyy'))) as r_date
from
ewwgao.hmao_cntry_holiday_list
;
'''

DROP TABLE IF EXISTS ewwgao.hmao_aa_flight_core_extend_v2;
CREATE TABLE ewwgao.hmao_aa_flight_core_extend_v2 AS
select core.tpid
		,core.begin_use_date_key
		,core.end_use_date_key
		,core.trvl_acct_email_addr
		,core.tckt_route_key
		,core.list_trl
		,core.list_visit_mktg_code
		,core.list_rlt_mktg_code
		,core.list_mktg_carrier_key
		,core.num_trl
		,core.num_visit_mktg_code
		,core.num_rlt_mktg_code
		,core.num_mktg_carrier_key
		,core.site_id
		,core.visit_mktg_code
		,core.rlt_mktg_code
		,core.mktg_carrier_key
		,core.platng_carrier_key
		,core.bkg_ind_key
		,core.tckt_air_fare_typ_key
		,core.air_trip_typ_id
		,core.trans_date_key
		,core.bk_date_key
		,core.tckt_cnt
		,core.gross_bkg_amt
		,core.base_cost
		,core.frnt_end_cmsn_amt
		,core.est_net_rev_amt
		,core.est_gross_profit_amt
		,core.tckt_seg_cnt
		,core.base_price
		,core.child_cnt
		,core.senior_cnt
		,core.infant_cnt
		,core.los
		,core.bk_window
		,core.product_ln_key
		,core.if_fst_class
		,core.if_bzns_class
		,core.if_cch_class
		,core.if_pcch_class
		,core.if_etckt
		,core.if_ptckt
		,core.if_tcktls
		,core.if_sat
		,core.if_nsat
		,bid.online_offln_ind
		,dest_info.route_name
		,dest_info.orign_airpt_name
		,dest_info.dest_airpt_name
		,dest_info.orign_airpt_metro_code
		,dest_info.dest_airpt_metro_code
		,dest_info.orign_airpt_cntry_name
		,dest_info.dest_airpt_cntry_name
		,dest_info.orign_airpt_iata_regn_name
		,dest_info.dest_airpt_iata_regn_name
		,dest_info.orign_airpt_city_name
		,dest_info.dest_airpt_city_name
		,dest_info.orign_airpt_state_provnc_name
		,dest_info.dest_airpt_state_provnc_name
		,dest_info.long_haul_short_haul_ind
		,chl.cntry_code
		,dest_info.geo_typ
		,dest_info.dest_airpt_lat
		,dest_info.dest_airpt_log
		,dest_info.orign_airpt_lat
		,dest_info.orign_airpt_log
		,dest_info.dest_num_hotels
		,case 
			when chl.cntry_code is NULL then 2
			when chl.cntry_code is not NULL and sum(case when datediff(core.begin_use_date_key,chl.r_date) between -5 and 5 then 1 else 0 end) > 0 then 1
			else 0 end as proximity_holidy_ind
		,case 
			when air_trip_typ_id = 1 then 0
			else 1.0 * base_price / (los * tckt_cnt) end as price_per_day
from ewwgao.hmao_aa_flight_core_v2 core
    inner join	(select rd.route_key
					,rd.route_name
					,rd.orign_airpt_name
					,rd.dest_airpt_name
					,rd.orign_airpt_metro_code2 as orign_airpt_metro_code
					,rd.dest_airpt_metro_code2 as dest_airpt_metro_code
					,rd.orign_airpt_cntry_name
					,rd.dest_airpt_cntry_name
					,rd.orign_airpt_iata_regn_name
					,rd.dest_airpt_iata_regn_name
					,rd.orign_airpt_city_name
					,rd.dest_airpt_city_name
					,rd.orign_airpt_state_provnc_name
					,rd.dest_airpt_state_provnc_name
					,rd.long_haul_short_haul_ind
					,rd.orign_airpt_iata_cntry_code
					,case when rd.orign_airpt_iata_regn_name = rd.dest_airpt_iata_regn_name and rd.orign_airpt_iata_cntry_name = rd.dest_airpt_iata_cntry_name then 'DOM'
						when rd.orign_airpt_iata_regn_name = rd.dest_airpt_iata_regn_name and rd.orign_airpt_iata_cntry_name <> rd.dest_airpt_iata_cntry_name then 'INTRA'
						else 'INTL' end as geo_typ
					,case 
						when ad.airpt_latitude is null then -9998
						else ad.airpt_latitude end as dest_airpt_lat
					,case 
						when ad.airpt_longitude is null then -9998
						else ad.airpt_longitude end as dest_airpt_log
					,case 
						when ad2.airpt_latitude is null then -9998
						else ad2.airpt_latitude end as orign_airpt_lat
					,case 
						when ad2.airpt_longitude is null then -9998
						else ad2.airpt_longitude end as orign_airpt_log
					,case when nh.num_hotels is NULL then 0 else nh.num_hotels end as dest_num_hotels
				from (select *
							,case when orign_airpt_metro_code like 'n/a%' then orign_airpt_code else orign_airpt_metro_code end as orign_airpt_metro_code2
							,case when dest_airpt_metro_code like 'n/a%' then dest_airpt_code else dest_airpt_metro_code end as dest_airpt_metro_code2
						from dm.route_dim) rd
					left join (select * from dm.airpt_dim where airpt_latitude <>-9998) ad on lower(rd.dest_airpt_code) = lower(ad.airpt_code)
					left join (select * from dm.airpt_dim where airpt_latitude <>-9998) ad2 on lower(rd.orign_airpt_code) = lower(ad2.airpt_code)
					left join (select a.airpt_code,count(a.lodg_property_key) as num_hotels
								from	(select apd.airpt_code
												,lpd.lodg_property_key									
										from dm.airpt_dim apd
											inner join dm.lodg_property_dim lpd on lower(apd.airpt_cntry_name) = lower(lpd.property_cntry_name)
										where exp_distance(apd.airpt_latitude, apd.airpt_longitude, lpd.property_latitude, lpd.property_longitude) <= 100
										) a
								group by a.airpt_code										
								) nh  on lower(rd.dest_airpt_code) = lower(nh.airpt_code)
				) dest_info	 on core.tckt_route_key = dest_info.route_key	
	left join (select *
					, to_date(FROM_UNIXTIME(unix_timestamp(h_date, 'MM/dd/yyyy'))) as r_date
				from ewwgao.hmao_cntry_holiday_list) chl on lower(dest_info.orign_airpt_iata_cntry_code) = lower(chl.cntry_code) 
	left join (select bkg_ind_key,online_offln_ind
				from dm.bkg_ind_dim
				group by bkg_ind_key,online_offln_ind) bid on core.bkg_ind_key = bid.bkg_ind_key
group by core.tpid
		,core.begin_use_date_key
		,core.end_use_date_key
		,core.trvl_acct_email_addr
		,core.tckt_route_key
		,core.list_trl
		,core.list_visit_mktg_code
		,core.list_rlt_mktg_code
		,core.list_mktg_carrier_key
		,core.num_trl
		,core.num_visit_mktg_code
		,core.num_rlt_mktg_code
		,core.num_mktg_carrier_key
		,core.site_id
		,core.visit_mktg_code
		,core.rlt_mktg_code
		,core.mktg_carrier_key
		,core.platng_carrier_key
		,core.bkg_ind_key
		,core.tckt_air_fare_typ_key
		,core.air_trip_typ_id
		,core.trans_date_key
		,core.bk_date_key
		,core.tckt_cnt
		,core.gross_bkg_amt
		,core.base_cost
		,core.frnt_end_cmsn_amt
		,core.est_net_rev_amt
		,core.est_gross_profit_amt
		,core.tckt_seg_cnt
		,core.base_price
		,core.child_cnt
		,core.senior_cnt
		,core.infant_cnt
		,core.los
		,core.bk_window
		,core.product_ln_key
		,core.if_fst_class
		,core.if_bzns_class
		,core.if_cch_class
		,core.if_pcch_class
		,core.if_etckt
		,core.if_ptckt
		,core.if_tcktls
		,core.if_sat
		,core.if_nsat
		,bid.online_offln_ind	
		,dest_info.route_name
		,dest_info.orign_airpt_name
		,dest_info.dest_airpt_name
		,dest_info.orign_airpt_metro_code
		,dest_info.dest_airpt_metro_code
		,dest_info.orign_airpt_cntry_name
		,dest_info.dest_airpt_cntry_name
		,dest_info.orign_airpt_iata_regn_name
		,dest_info.dest_airpt_iata_regn_name
		,dest_info.orign_airpt_city_name
		,dest_info.dest_airpt_city_name
		,dest_info.orign_airpt_state_provnc_name
		,dest_info.dest_airpt_state_provnc_name
		,dest_info.long_haul_short_haul_ind
		,chl.cntry_code
		,dest_info.geo_typ
		,dest_info.dest_airpt_lat
		,dest_info.dest_airpt_log
		,dest_info.orign_airpt_lat
		,dest_info.orign_airpt_log
		,dest_info.dest_num_hotels;
		
		
		
		

		

		
		
		
--other LOB table 	
	
DROP TABLE IF EXISTS ewwgao.hmao_aa_hotel_v2;
CREATE TABLE ewwgao.hmao_aa_hotel_v2 AS		
select purch_hotel.*
from	(select tad.trvl_acct_email_addr
				,ltf.tpid
				,ltf.trl
				,ltf.lodg_property_key
				,ltf.begin_use_date_key
				,lpd.property_latitude
				,lpd.property_longitude
				,min(ltf.bk_date_key) as bk_date_key
				,min(ltf.trans_date_key) as trans_date_key
				,sum(ltf.rm_cnt) as rm_cnt
				,sum(ltf.rm_night_cnt) as rm_night_cnt
				,1.0 * sum(ltf.gross_bkg_amt_usd)  as gross_bkg_amt_usd
				,1.0 * sum(ltf.base_price_amt_usd)  as base_price_amt_usd
				,1.0 * sum(ltf.base_cost_amt_usd)  as base_cost_amt_usd
				,1.0 * sum(ltf.frnt_end_cmsn_amt_usd) as frnt_end_cmsn_amt_usd
				,1.0 * sum(ltf.est_net_rev_amt_usd)  as est_net_rev_amt_usd
				,1.0 * sum(ltf.est_gross_profit_amt_usd)  as est_gross_profit_amt_usd
		from dm.lodg_rm_trans_fact ltf
		inner join (select *
								from dm.trvl_acct_dim
								where lower(trvl_acct_email_addr) != 'unknown' and trvl_acct_tpid > 0) tad on ltf.purch_trvl_acct_key = tad.trvl_acct_key and ltf.tpid = tad.trvl_acct_tpid
		inner join dm.lodg_property_dim lpd on ltf.lodg_property_key = lpd.lodg_property_key
		where ltf.trans_typ_key = 101 and ltf.bk_date_key between '2014-01-01' and '2016-12-31' and lpd.property_latitude != 0
		group by tad.trvl_acct_email_addr
				,ltf.tpid
				,ltf.trl
				,ltf.lodg_property_key
				,ltf.begin_use_date_key
				,lpd.property_latitude
				,lpd.property_longitude) purch_hotel
left join (select tpid,trl 
			from dm.lodg_rm_trans_fact 
			where trans_typ_key >= 102 and trans_typ_key <= 104
			group by tpid,trl) cncl_hotel on purch_hotel.tpid = cncl_hotel.tpid and purch_hotel.trl = cncl_hotel.trl
where cncl_hotel.tpid is NULL 
and purch_hotel.tpid > 0;






DROP TABLE IF EXISTS ewwgao.hmao_aa_car_v2;
CREATE TABLE ewwgao.hmao_aa_car_v2 AS
select purch_car.*
		,cld.car_loc_metro_code
		,cld.car_loc_city_name
		,cld.car_loc_cntry_name
		,cld.car_loc_state_provnc_name
from	(select tad.trvl_acct_email_addr
				,crtf.tpid
				,crtf.trl
				,crtf.car_pick_up_loc_key
				,crtf.begin_use_date_key				
				,min(crtf.bk_date_key) as bk_date_key
				,min(crtf.trans_date_key) as trans_date_key
				,sum(crtf.car_rentl_cnt) as car_rentl_cnt
				,sum(crtf.rentl_day_cnt) as rentl_day_cnt
				,1.0 * sum(crtf.gross_bkg_amt_usd)  as gross_bkg_amt_usd
				,1.0 * sum(crtf.base_price_amt_usd)  as base_price_amt_usd
				,1.0 * sum(crtf.base_cost_amt_usd) as base_cost_amt_usd
				,1.0 * sum(crtf.frnt_end_cmsn_amt_usd)  as frnt_end_cmsn_amt_usd
				,1.0 * sum(crtf.est_net_rev_amt_usd)  as est_net_rev_amt_usd
				,1.0 * sum(crtf.est_gross_profit_amt_usd)  as est_gross_profit_amt_usd
		from dm.car_rentl_trans_fact crtf
		inner join (select *
								from dm.trvl_acct_dim
								where lower(trvl_acct_email_addr) != 'unknown' and trvl_acct_tpid > 0) tad on crtf.purch_trvl_acct_key = tad.trvl_acct_key and crtf.tpid = tad.trvl_acct_tpid
		where crtf.trans_typ_key = 101 and crtf.bk_date_key between '2014-01-01' and '2016-12-31'		
		group by tad.trvl_acct_email_addr
				,crtf.tpid
				,crtf.trl
				,crtf.car_pick_up_loc_key
				,crtf.begin_use_date_key) purch_car
left join (select tpid,trl 
			from dm.car_rentl_trans_fact 
			where trans_typ_key >= 102 and trans_typ_key <= 104
			group by tpid,trl) cncl_car on purch_car.tpid = cncl_car.tpid and purch_car.trl = cncl_car.trl
inner join dm.car_loc_dim cld on purch_car.car_pick_up_loc_key = cld.car_loc_key
where cncl_car.tpid is NULL
and purch_car.tpid > 0;






DROP TABLE IF EXISTS ewwgao.hmao_aa_ins_v2;
CREATE TABLE ewwgao.hmao_aa_ins_v2 AS
select purch_ins.*
from	(select tad.trvl_acct_email_addr
				,ittf.tpid
				,ittf.trl
				,min(ittf.bk_date_key) as bk_date_key
				,min(ittf.trans_date_key) as trans_date_key
				,sum(ittf.ins_itm_cnt) as ins_itm_cnt
				,1.0 * sum(ittf.gross_bkg_amt_usd) as gross_bkg_amt_usd
				,1.0 * sum(ittf.base_price_amt_usd) as base_price_amt_usd				
				,1.0 * sum(ittf.base_cost_amt_usd)  as base_cost_amt_usd
				,1.0 * sum(ittf.frnt_end_cmsn_amt_usd)  as rnt_end_cmsn_amt_usd
				,1.0 * sum(ittf.est_net_rev_amt_usd)  as est_net_rev_amt_usd
				,1.0 * sum(ittf.est_gross_profit_amt_usd)  as est_gross_profit_amt_usd				
		from  dm.ins_itm_trans_fact ittf
		inner join (select *
								from dm.trvl_acct_dim
								where lower(trvl_acct_email_addr) != 'unknown' and trvl_acct_tpid > 0) tad on ittf.purch_trvl_acct_key = tad.trvl_acct_key and ittf.tpid = tad.trvl_acct_tpid
		where ittf.trans_typ_key = 101 and ittf.bk_date_key between '2014-01-01' and '2016-12-31'
		group by tad.trvl_acct_email_addr
				,ittf.tpid
				,ittf.trl) purch_ins	
left join 	(select tpid,trl 
			from dm.ins_itm_trans_fact 
			where trans_typ_key >= 102 and trans_typ_key <= 104
			group by tpid,trl) cncl_ins on purch_ins.tpid = cncl_ins.tpid and purch_ins.trl = cncl_ins.trl
where cncl_ins.tpid is NULL
and purch_ins.tpid > 0;






DROP TABLE IF EXISTS ewwgao.hmao_aa_lx_v2;
CREATE TABLE ewwgao.hmao_aa_lx_v2 AS
select purch_lx.tpid
		,purch_lx.trl
		,purch_lx.begin_use_date_key
		,purch_lx.dest_svc_srch_loc_key
		,purch_lx.trvl_acct_email_addr
		,purch_lx.atlas_regn_name
		,purch_lx.bk_date_key
		,purch_lx.trans_date_key
		,purch_lx.totl_dest_svc_tckt_cnt
		,purch_lx.gross_bkg_amt_usd
		,purch_lx.base_price_amt_usd
		,purch_lx.base_cost_amt_usd
		,purch_lx.est_net_rev_amt_usd
		,purch_lx.est_gross_profit_amt_usd
		,lpd.property_latitude
		,lpd.property_longitude
from	(select dstf.tpid
				,dstf.trl
				,dstf.begin_use_date_key
				,dstf.dest_svc_srch_loc_key
				,tad.trvl_acct_email_addr
				,ard.atlas_property_mkt_key
				,ard.atlas_regn_name
				,min(dstf.bk_date_key) as bk_date_key
				,min(dstf.trans_date_key) as trans_date_key
				,sum(dstf.totl_dest_svc_tckt_cnt) as totl_dest_svc_tckt_cnt
				,1.0 * sum(dstf.gross_bkg_amt_usd)  as gross_bkg_amt_usd
				,1.0 * sum(dstf.base_price_amt_usd)  as base_price_amt_usd				
				,1.0 * sum(dstf.base_cost_amt_usd)  as base_cost_amt_usd
				,1.0 * sum(dstf.est_net_rev_amt_usd)  as est_net_rev_amt_usd
				,1.0 * sum(dstf.est_gross_profit_amt_usd)  as est_gross_profit_amt_usd	
		from dm.dest_svc_trans_fact dstf
		inner join (select *
								from dm.trvl_acct_dim
								where lower(trvl_acct_email_addr) != 'unknown' and trvl_acct_tpid > 0) tad on dstf.purch_trvl_acct_key = tad.trvl_acct_key and dstf.tpid = tad.trvl_acct_tpid
		inner join dm.atlas_regn_dim ard on dstf.dest_svc_srch_loc_key = ard.atlas_regn_key
		where dstf.trans_typ_key = 101 and dstf.bk_date_key between '2014-01-01' and '2016-12-31'
		group by dstf.tpid
				,dstf.trl
				,dstf.begin_use_date_key
				,dstf.dest_svc_srch_loc_key
				,tad.trvl_acct_email_addr
				,ard.atlas_property_mkt_key
				,ard.atlas_regn_name) purch_lx
left join  (select tpid,trl 
			from dm.dest_svc_trans_fact
			where trans_typ_key >= 102 and trans_typ_key <= 104
			group by tpid,trl) cncl_lx on purch_lx.tpid = cncl_lx.tpid and purch_lx.trl = cncl_lx.trl
left join	(select distinct property_mkt_key,property_mkt_id from dm.property_mkt_dim where not(lower(property_mkt_name) rlike 'unknown') and not(lower(property_mkt_name) rlike 'applicable')) pmd on purch_lx.atlas_property_mkt_key = pmd.property_mkt_key
left join   (select distinct property_mkt_id,property_latitude,property_longitude from dm.lodg_property_dim) lpd on pmd.property_mkt_id = lpd.property_mkt_id
where cncl_lx.tpid is NULL and lpd.property_latitude != 0 and purch_lx.tpid > 0
group by purch_lx.tpid
		,purch_lx.trl
		,purch_lx.begin_use_date_key
		,purch_lx.dest_svc_srch_loc_key
		,purch_lx.trvl_acct_email_addr
		,purch_lx.atlas_regn_name
		,purch_lx.bk_date_key
		,purch_lx.trans_date_key
		,purch_lx.totl_dest_svc_tckt_cnt
		,purch_lx.gross_bkg_amt_usd
		,purch_lx.base_price_amt_usd
		,purch_lx.base_cost_amt_usd
		,purch_lx.est_net_rev_amt_usd
		,purch_lx.est_gross_profit_amt_usd
		,lpd.property_latitude
		,lpd.property_longitude;

		

--Begin attaching other LOBs


		--hotel
DROP TABLE IF EXISTS ewwgao.hmao_aa_hotel_attach_v2;
CREATE TABLE ewwgao.hmao_aa_hotel_attach_v2 AS	
select base.*
		,case when base.airpt_htl_distance  <= 80.467 then 1 else 0 end as if_50_mile
		,case when base.airpt_htl_distance  <= 160.934 then 1 else 0 end as if_100_mile
		,case when base.airpt_htl_distance  <= 804.672 then 1 else 0 end as if_500_mile
		,case when base.airpt_htl_distance  <= 1609.344 then 1 else 0 end as if_1000_mile
from
	(select base.trvl_acct_email_addr
			,base.tpid
			,base.trl
			,base.lodg_property_key
			,base.begin_use_date_key
			,base.bk_date_key
			,base.trans_date_key
			,base.property_latitude
			,base.property_longitude
			,base.rm_cnt
			,base.rm_night_cnt
			,base.gross_bkg_amt_usd
			,base.base_price_amt_usd
			,base.base_cost_amt_usd
			,base.frnt_end_cmsn_amt_usd
			,base.est_net_rev_amt_usd
			,base.est_gross_profit_amt_usd
			,base.flt_begin_use_date_key
			,base.flt_end_use_date_key
			,base.tckt_route_key 
			,base.airpt_htl_distance_dest
			,base.airpt_htl_distance_orign
			,case when base.airpt_htl_distance_dest <= base.airpt_htl_distance_orign then base.airpt_htl_distance_dest 
					else base.airpt_htl_distance_orign end as airpt_htl_distance
	from 	
		(select base.*	
				,rank() over (partition by base.trvl_acct_email_addr,base.tpid,base.trl,base.lodg_property_key,base.begin_use_date_key order by htl_flt_diff) as rank
		from	(select hotel.trvl_acct_email_addr
						,hotel.tpid
						,hotel.trl
						,hotel.lodg_property_key
						,hotel.begin_use_date_key
						,hotel.bk_date_key
						,hotel.trans_date_key
						,hotel.property_latitude
						,hotel.property_longitude
						,hotel.rm_cnt
						,hotel.rm_night_cnt
						,hotel.gross_bkg_amt_usd
						,hotel.base_price_amt_usd
						,hotel.base_cost_amt_usd
						,hotel.frnt_end_cmsn_amt_usd
						,hotel.est_net_rev_amt_usd
						,hotel.est_gross_profit_amt_usd
						,core.begin_use_date_key as flt_begin_use_date_key
						,core.end_use_date_key as flt_end_use_date_key
						,core.tckt_route_key 
						,case 
							when abs(datediff(hotel.begin_use_date_key,core.begin_use_date_key)) > abs(datediff(hotel.begin_use_date_key,core.end_use_date_key)) then abs(datediff(hotel.begin_use_date_key,core.end_use_date_key)) + rand()/10
							else abs(datediff(hotel.begin_use_date_key,core.begin_use_date_key)) + rand()/10 end as htl_flt_diff
						,case 
							when hotel.begin_use_date_key between core.expanded_begin_date and core.expanded_end_date then 1
							else 0 end as date_match_ind
						,exp_distance(core.dest_airpt_lat, core.dest_airpt_log, hotel.property_latitude, hotel.property_longitude) as airpt_htl_distance_dest
						,exp_distance(core.orign_airpt_lat, core.orign_airpt_log, hotel.property_latitude, hotel.property_longitude) as airpt_htl_distance_orign
				from (select * 
							,date_sub(begin_use_date_key,3) as expanded_begin_date
							,date_add(end_use_date_key,3) as expanded_end_date
						from ewwgao.hmao_aa_flight_core_extend_v2) core
				inner join ewwgao.hmao_aa_hotel_v2 hotel on core.trvl_acct_email_addr = hotel.trvl_acct_email_addr and core.tpid = hotel.tpid
				) base
		where date_match_ind = 1
		) base
	where rank = 1) base;







		--car
DROP TABLE IF EXISTS ewwgao.hmao_aa_car_attach_v2;
CREATE TABLE ewwgao.hmao_aa_car_attach_v2 AS
select 	base.trvl_acct_email_addr
		,base.tpid
		,base.trl
		,base.car_pick_up_loc_key
		,base.begin_use_date_key
		,base.bk_date_key
		,base.trans_date_key
		,base.car_rentl_cnt
		,base.rentl_day_cnt
		,base.gross_bkg_amt_usd
		,base.base_price_amt_usd
		,base.base_cost_amt_usd
		,base.frnt_end_cmsn_amt_usd
		,base.est_net_rev_amt_usd
		,base.est_gross_profit_amt_usd
		,base.flt_begin_use_date_key
		,base.flt_end_use_date_key
		,base.tckt_route_key 
from				
	(select base.*	
			,rank() over (partition by base.trvl_acct_email_addr,base.tpid,base.trl,base.car_pick_up_loc_key,base.begin_use_date_key order by car_flt_diff) as rank
	from	(select car.trvl_acct_email_addr
					,car.tpid
					,car.trl
					,car.car_pick_up_loc_key
					,car.begin_use_date_key
				    ,car.bk_date_key
					,car.trans_date_key
					,car.car_rentl_cnt
					,car.rentl_day_cnt
					,car.gross_bkg_amt_usd
					,car.base_price_amt_usd
					,car.base_cost_amt_usd
					,car.frnt_end_cmsn_amt_usd
					,car.est_net_rev_amt_usd
					,car.est_gross_profit_amt_usd
					,core.begin_use_date_key as flt_begin_use_date_key
					,core.end_use_date_key as flt_end_use_date_key
					,core.tckt_route_key 
					,case 
						when abs(datediff(car.begin_use_date_key,core.begin_use_date_key)) > abs(datediff(car.begin_use_date_key,core.end_use_date_key)) then abs(datediff(car.begin_use_date_key,core.end_use_date_key)) + rand()/10
						else abs(datediff(car.begin_use_date_key,core.begin_use_date_key)) + rand()/10 end as car_flt_diff
					,case 
						when lower(car.car_loc_metro_code) = lower(core.dest_airpt_metro_code) then 1
						when lower(car.car_loc_cntry_name) = lower(core.dest_airpt_cntry_name) and lower(car.car_loc_state_provnc_name) = lower(core.dest_airpt_state_provnc_name) and not(lower(car.car_loc_state_provnc_name) rlike 'unknown') then 1
						when lower(car.car_loc_cntry_name) = lower(core.dest_airpt_cntry_name) and lower(car.car_loc_city_name) = lower(core.dest_airpt_city_name) and not(lower(car.car_loc_city_name) rlike 'unknown') then 1
						else 0 end as loc_match_ind
					,case 
						when car.begin_use_date_key between core.expanded_begin_date and core.expanded_end_date then 1
						else 0 end as date_match_ind
			from (select * 
						,date_sub(begin_use_date_key,3) as expanded_begin_date
						,date_add(end_use_date_key,3) as expanded_end_date
					from ewwgao.hmao_aa_flight_core_extend_v2) core
			inner join ewwgao.hmao_aa_car_v2 car on core.trvl_acct_email_addr = car.trvl_acct_email_addr and core.tpid = car.tpid
			) base
	where date_match_ind = 1 and loc_match_ind = 1
	) base	
where rank = 1;


		
		
		
		--ins
DROP TABLE IF EXISTS ewwgao.hmao_aa_ins_attach_v2;
CREATE TABLE ewwgao.hmao_aa_ins_attach_v2 AS	
select ins.trvl_acct_email_addr
		,ins.tpid
		,ins.trl
		,ins.bk_date_key
		,ins.trans_date_key
		,ins.ins_itm_cnt
		,ins.gross_bkg_amt_usd
		,ins.base_price_amt_usd				
		,ins.base_cost_amt_usd
		,ins.rnt_end_cmsn_amt_usd
		,ins.est_net_rev_amt_usd
		,ins.est_gross_profit_amt_usd
		,core.begin_use_date_key as flt_begin_use_date_key
		,core.end_use_date_key as flt_end_use_date_key
		,core.tckt_route_key 
from ewwgao.hmao_aa_flight_core_extend_v2 core
inner join ewwgao.hmao_aa_ins_v2 ins on core.trvl_acct_email_addr = ins.trvl_acct_email_addr and core.tpid = ins.tpid
where array_contains(core.list_trl,ins.trl) = True;
	
	
	
		
		--lx
DROP TABLE IF EXISTS ewwgao.hmao_aa_lx_attach_v2;
CREATE TABLE ewwgao.hmao_aa_lx_attach_v2 AS	
select base.tpid
		,base.trl
		,base.begin_use_date_key
		,base.dest_svc_srch_loc_key
		,base.trvl_acct_email_addr
		,base.atlas_regn_name
		,base.bk_date_key
		,base.trans_date_key
		,base.totl_dest_svc_tckt_cnt
		,base.gross_bkg_amt_usd
		,base.base_price_amt_usd
		,base.base_cost_amt_usd
		,base.est_net_rev_amt_usd
		,base.est_gross_profit_amt_usd
		,base.flt_begin_use_date_key
		,base.flt_end_use_date_key
		,base.tckt_route_key 
		,base.airpt_htl_distance
		,case when base.airpt_htl_distance  <= 804.672 then 1 else 0 end as if_lx_attach
from 
		(select base.*
				,rank() over(partition by base.tpid,base.trl,base.begin_use_date_key,base.dest_svc_srch_loc_key,base.trvl_acct_email_addr,base.atlas_regn_name order by lx_flt_diff) as rank
		from 
				(select base.tpid
						,base.trl
						,base.begin_use_date_key
						,base.dest_svc_srch_loc_key
						,base.trvl_acct_email_addr
						,base.atlas_regn_name
						,base.bk_date_key
						,base.trans_date_key
						,base.totl_dest_svc_tckt_cnt
						,base.gross_bkg_amt_usd
						,base.base_price_amt_usd
						,base.base_cost_amt_usd
						,base.est_net_rev_amt_usd
						,base.est_gross_profit_amt_usd
						,base.flt_begin_use_date_key
						,base.flt_end_use_date_key
						,base.tckt_route_key 
						,base.date_match_ind
						,base.lx_flt_diff
						,min(base.airpt_htl_distance) as airpt_htl_distance
				from	(select lx.tpid
								,lx.trl
								,lx.begin_use_date_key
								,lx.dest_svc_srch_loc_key
								,lx.trvl_acct_email_addr
								,lx.atlas_regn_name
								,lx.bk_date_key
								,lx.trans_date_key
								,lx.totl_dest_svc_tckt_cnt
								,lx.gross_bkg_amt_usd
								,lx.base_price_amt_usd
								,lx.base_cost_amt_usd
								,lx.est_net_rev_amt_usd
								,lx.est_gross_profit_amt_usd
								,lx.property_latitude
								,lx.property_longitude
								,core.begin_use_date_key as flt_begin_use_date_key
								,core.end_use_date_key as flt_end_use_date_key
								,core.tckt_route_key 
								,case 
									when abs(datediff(lx.begin_use_date_key,core.begin_use_date_key)) > abs(datediff(lx.begin_use_date_key,core.end_use_date_key)) then abs(datediff(lx.begin_use_date_key,core.end_use_date_key)) + rand()/10
									else abs(datediff(lx.begin_use_date_key,core.begin_use_date_key)) + rand()/10 end as lx_flt_diff
								,case 
									when lx.begin_use_date_key between core.expanded_begin_date and core.expanded_end_date then 1
									else 0 end as date_match_ind
								,exp_distance(core.dest_airpt_lat, core.dest_airpt_log, lx.property_latitude, lx.property_longitude) as airpt_htl_distance
						from (select * 
									,date_sub(begin_use_date_key,3) as expanded_begin_date
									,date_add(end_use_date_key,3) as expanded_end_date
								from ewwgao.hmao_aa_flight_core_extend_v2) core
						inner join ewwgao.hmao_aa_lx_v2 lx on core.trvl_acct_email_addr = lx.trvl_acct_email_addr and core.tpid = lx.tpid
						) base
				where date_match_ind = 1 
				group by base.tpid
						,base.trl
						,base.begin_use_date_key
						,base.dest_svc_srch_loc_key
						,base.trvl_acct_email_addr
						,base.atlas_regn_name
						,base.bk_date_key
						,base.trans_date_key
						,base.totl_dest_svc_tckt_cnt
						,base.gross_bkg_amt_usd
						,base.base_price_amt_usd
						,base.base_cost_amt_usd
						,base.est_net_rev_amt_usd
						,base.est_gross_profit_amt_usd
						,base.flt_begin_use_date_key
						,base.flt_end_use_date_key
						,base.tckt_route_key 
						,base.date_match_ind
						,base.lx_flt_diff
				) base
		) base
where rank = 1;	
		
		
		
		
-----another way for lx attach 
DROP TABLE IF EXISTS ewwgao.hmao_aa_lx_attach_mid_v2;
CREATE TABLE ewwgao.hmao_aa_lx_attach_mid_v2 AS		
select *
from		
(select base.*
		,rank() over(partition by base.tpid,base.trl,base.begin_use_date_key,base.dest_svc_srch_loc_key,base.trvl_acct_email_addr,base.atlas_regn_name order by lx_flt_diff) as rank
from (select * 
		from		
			(select lx.tpid
					,lx.trl
					,lx.begin_use_date_key
					,lx.dest_svc_srch_loc_key
					,lx.trvl_acct_email_addr
					,lx.atlas_regn_name
					,lx.bk_date_key
					,lx.trans_date_key
					,lx.totl_dest_svc_tckt_cnt
					,lx.gross_bkg_amt_usd
					,lx.base_price_amt_usd
					,lx.base_cost_amt_usd
					,lx.est_net_rev_amt_usd
					,lx.est_gross_profit_amt_usd
					,core.begin_use_date_key as flt_begin_use_date_key
					,core.end_use_date_key as flt_end_use_date_key
					,core.tckt_route_key 
					,core.dest_airpt_lat
					,core.dest_airpt_log
					,case 
						when abs(datediff(lx.begin_use_date_key,core.begin_use_date_key)) > abs(datediff(lx.begin_use_date_key,core.end_use_date_key)) then abs(datediff(lx.begin_use_date_key,core.end_use_date_key)) + rand()/10
						else abs(datediff(lx.begin_use_date_key,core.begin_use_date_key)) + rand()/10 end as lx_flt_diff
					,case 
						when lx.begin_use_date_key between core.expanded_begin_date and core.expanded_end_date then 1
						else 0 end as date_match_ind
			from (select * 
						,date_sub(begin_use_date_key,3) as expanded_begin_date
						,date_add(end_use_date_key,3) as expanded_end_date
					from ewwgao.hmao_aa_flight_core_extend_v2) core
			inner join (select lx.tpid
								,lx.trl
								,lx.begin_use_date_key
								,lx.dest_svc_srch_loc_key
								,lx.trvl_acct_email_addr
								,lx.atlas_regn_name
								,lx.bk_date_key
								,lx.trans_date_key
								,lx.totl_dest_svc_tckt_cnt
								,lx.gross_bkg_amt_usd
								,lx.base_price_amt_usd
								,lx.base_cost_amt_usd
								,lx.est_net_rev_amt_usd
								,lx.est_gross_profit_amt_usd
						from ewwgao.hmao_aa_lx_v2 lx 
						group by lx.tpid
								,lx.trl
								,lx.begin_use_date_key
								,lx.dest_svc_srch_loc_key
								,lx.trvl_acct_email_addr
								,lx.atlas_regn_name
								,lx.bk_date_key
								,lx.trans_date_key
								,lx.totl_dest_svc_tckt_cnt
								,lx.gross_bkg_amt_usd
								,lx.base_price_amt_usd
								,lx.base_cost_amt_usd
								,lx.est_net_rev_amt_usd
								,lx.est_gross_profit_amt_usd)lx on core.trvl_acct_email_addr = lx.trvl_acct_email_addr and core.tpid = lx.tpid
			) base
		where date_match_ind = 1
	) base 	
) base	
where rank = 1;

		
		


DROP TABLE IF EXISTS ewwgao.hmao_aa_lx_attach_v2;
CREATE TABLE ewwgao.hmao_aa_lx_attach_v2 AS	
select *
		,case when base.airpt_htl_distance  <= 804.672 then 1 else 0 end as if_lx_attach
from  (select lam.tpid
				,lam.trl
				,lam.begin_use_date_key
				,lam.dest_svc_srch_loc_key
				,lam.trvl_acct_email_addr
				,lam.atlas_regn_name
				,lam.bk_date_key
				,lam.trans_date_key
				,lam.totl_dest_svc_tckt_cnt
				,lam.gross_bkg_amt_usd
				,lam.base_price_amt_usd
				,lam.base_cost_amt_usd
				,lam.est_net_rev_amt_usd
				,lam.est_gross_profit_amt_usd
				,lam.flt_begin_use_date_key
				,lam.flt_end_use_date_key
				,lam.tckt_route_key 
				,min(exp_distance(lam.dest_airpt_lat, lam.dest_airpt_log, al.property_latitude, al.property_longitude)) as airpt_htl_distance
		from ewwgao.hmao_aa_lx_v2 al
		right join ewwgao.hmao_aa_lx_attach_mid_v2 lam on al.tpid = lam.tpid
											and al.trl = lam.trl
											and al.begin_use_date_key = lam.begin_use_date_key
											and al.dest_svc_srch_loc_key = lam.dest_svc_srch_loc_key
											and al.trvl_acct_email_addr = lam.trvl_acct_email_addr
											and al.atlas_regn_name = lam.atlas_regn_name
											and al.bk_date_key = lam.bk_date_key
											and al.trans_date_key = lam.trans_date_key
											and al.totl_dest_svc_tckt_cnt = lam.totl_dest_svc_tckt_cnt
											and al.gross_bkg_amt_usd = lam.gross_bkg_amt_usd
											and al.base_price_amt_usd = lam.base_price_amt_usd
											and al.base_cost_amt_usd = lam.base_cost_amt_usd
											and al.est_net_rev_amt_usd = lam.est_net_rev_amt_usd
											and al.est_gross_profit_amt_usd = lam.est_gross_profit_amt_usd
		group by lam.tpid
				,lam.trl
				,lam.begin_use_date_key
				,lam.dest_svc_srch_loc_key
				,lam.trvl_acct_email_addr
				,lam.atlas_regn_name
				,lam.bk_date_key
				,lam.trans_date_key
				,lam.totl_dest_svc_tckt_cnt
				,lam.gross_bkg_amt_usd
				,lam.base_price_amt_usd
				,lam.base_cost_amt_usd
				,lam.est_net_rev_amt_usd
				,lam.est_gross_profit_amt_usd
				,lam.flt_begin_use_date_key
				,lam.flt_end_use_date_key
				,lam.tckt_route_key
		) base;		

		
		
		
		
		
		
		
		
		
		
		
		
		
		
		
		
		
		
		
		
		
		
		
		
		
		
		
		
		
		
		
		
		
		
		
		
		
		
		
		
		
		

--combine LOB attach table with core

DROP TABLE IF EXISTS ewwgao.hmao_aa_all_model_v2;
CREATE TABLE ewwgao.hmao_aa_all_model_v2 AS
select core.tpid
		,core.begin_use_date_key
		,core.end_use_date_key
		,core.trvl_acct_email_addr
		,core.tckt_route_key
		,core.list_trl
		,core.list_visit_mktg_code
		,core.list_rlt_mktg_code
		,core.list_mktg_carrier_key
		,core.num_trl
		,core.num_visit_mktg_code
		,core.num_rlt_mktg_code
		,core.num_mktg_carrier_key
		,core.site_id
		,core.visit_mktg_code
		,core.rlt_mktg_code
		,core.mktg_carrier_key
		,core.platng_carrier_key
		,core.bkg_ind_key
		,core.tckt_air_fare_typ_key
		,core.air_trip_typ_id
		,core.trans_date_key
		,core.bk_date_key
		,core.tckt_cnt
		,core.gross_bkg_amt
		,core.base_cost
		,core.frnt_end_cmsn_amt
		,core.est_net_rev_amt
		,core.est_gross_profit_amt
		,core.tckt_seg_cnt
		,core.base_price
		,core.child_cnt
		,core.senior_cnt
		,core.infant_cnt
		,core.los
		,core.bk_window
		,core.product_ln_key
		,core.if_fst_class
		,core.if_bzns_class
		,core.if_cch_class
		,core.if_pcch_class
		,core.if_etckt
		,core.if_ptckt
		,core.if_tcktls
		,core.if_sat
		,core.if_nsat
		,core.online_offln_ind	
		,core.route_name
		,core.orign_airpt_name
		,core.dest_airpt_name
		,core.orign_airpt_metro_code
		,core.dest_airpt_metro_code
		,core.orign_airpt_cntry_name
		,core.dest_airpt_cntry_name
		,core.orign_airpt_iata_regn_name
		,core.dest_airpt_iata_regn_name
		,core.orign_airpt_city_name
		,core.dest_airpt_city_name
		,core.orign_airpt_state_provnc_name
		,core.dest_airpt_state_provnc_name
		,core.long_haul_short_haul_ind
		,core.cntry_code
		,core.geo_typ
		,core.dest_airpt_lat
		,core.dest_airpt_log
		,core.orign_airpt_lat
		,core.orign_airpt_log
		,core.dest_num_hotels
		,core.proximity_holidy_ind
		,core.price_per_day
		,case when sum(case
						when hotel.if_500_mile = 1 then 1
						else 0 end) > 0 then 1 else 0 end as if_hotel_attach
		,case 
			when sum(case
						when car.tpid is NULL then 0
						else 1 end) > 0 then 1 
			else 0 end as if_car_attach
		,case when sum(case
				when lx.if_lx_attach = 1 then 1
				else 0 end) > 0 then 1 else 0 end as if_lx_attach
		,case 
			when sum(case
						when ins.tpid is NULL then 0
						else 1 end) > 0 then 1 
			else 0 end as if_ins_attach
		,case when sum(case
						when hotel.if_500_mile = 1 then 1
						else 0 end) > 0
				or sum(case
						when car.tpid is NULL then 0
						else 1 end) > 0
				or sum(case
						when lx.if_lx_attach = 1 then 1
						else 0 end) > 0
				or sum(case
						when ins.tpid is NULL then 0
						else 1 end) > 0
				then 1 else 0 end as if_attach
		,sum(hotel.rm_cnt) as hotel_rm_cnt
		,sum(hotel.rm_night_cnt) as hotel_rm_night_cnt
		,sum(hotel.gross_bkg_amt_usd) as hotel_gross_bkg_amt_usd
		,sum(hotel.base_price_amt_usd) as hotel_hbase_price_amt_usd
		,sum(hotel.base_cost_amt_usd) as hotel_base_cost_amt_usd
		,sum(hotel.frnt_end_cmsn_amt_usd) as hotel_frnt_end_cmsn_amt_usd
		,sum(hotel.est_net_rev_amt_usd) as hotel_est_net_rev_amt_usd
		,sum(hotel.est_gross_profit_amt_usd) as hotel_est_gross_profit_amt_usd
		,sum(car.car_rentl_cnt) as car_rentl_cnt
		,sum(car.rentl_day_cnt) as car_rentl_day_cnt
		,sum(car.gross_bkg_amt_usd) as car_gross_bkg_amt_usd
		,sum(car.base_price_amt_usd) as car_hbase_price_amt_usd
		,sum(car.base_cost_amt_usd) as car_base_cost_amt_usd
		,sum(car.frnt_end_cmsn_amt_usd) as car_frnt_end_cmsn_amt_usd
		,sum(car.est_net_rev_amt_usd) as car_est_net_rev_amt_usd
		,sum(car.est_gross_profit_amt_usd) as car_est_gross_profit_amt_usd
		,sum(lx.totl_dest_svc_tckt_cnt) as lx_totl_dest_svc_tckt_cnt
		,sum(lx.gross_bkg_amt_usd) as lx_gross_bkg_amt_usd
		,sum(lx.base_price_amt_usd) as lx_hbase_price_amt_usd
		,sum(lx.base_cost_amt_usd) as lx_base_cost_amt_usd
		,sum(lx.est_net_rev_amt_usd) as lx_est_net_rev_amt_usd
		,sum(lx.est_gross_profit_amt_usd) as lx_est_gross_profit_amt_usd
		,sum(ins.ins_itm_cnt) as ins_itm_cnt
		,sum(ins.gross_bkg_amt_usd) as ins_gross_bkg_amt_usd
		,sum(ins.base_price_amt_usd) as ins_hbase_price_amt_usd
		,sum(ins.base_cost_amt_usd) as ins_base_cost_amt_usd
		,sum(ins.rnt_end_cmsn_amt_usd) as ins_rnt_end_cmsn_amt_usd
		,sum(ins.est_net_rev_amt_usd) as ins_est_net_rev_amt_usd
		,sum(ins.est_gross_profit_amt_usd) as ins_est_gross_profit_amt_usd

from (select a.*
		from ewwgao.hmao_aa_flight_core_extend_v2 a
		left join (select distinct a.trvl_acct_email_addr
					from dm.trvl_acct_dim a
					inner join ewwgao.hmao_trvl_agent_list b on a.trvl_acct_id = b.tuid
					) b on a.trvl_acct_email_addr = b.trvl_acct_email_addr			
		where b.trvl_acct_email_addr is NULL         
		) core
left join ewwgao.hmao_aa_hotel_attach_v2 hotel on core.tpid = hotel.tpid
												and core.begin_use_date_key = hotel.flt_begin_use_date_key
												and core.end_use_date_key = hotel.flt_end_use_date_key
												and core.trvl_acct_email_addr = hotel.trvl_acct_email_addr
												and core.tckt_route_key = hotel.tckt_route_key
left join ewwgao.hmao_aa_car_attach_v2 car on core.tpid = car.tpid
												and core.begin_use_date_key = car.flt_begin_use_date_key
												and core.end_use_date_key = car.flt_end_use_date_key
												and core.trvl_acct_email_addr = car.trvl_acct_email_addr
												and core.tckt_route_key = car.tckt_route_key
left join ewwgao.hmao_aa_lx_attach_v2 lx on core.tpid = lx.tpid
												and core.begin_use_date_key = lx.flt_begin_use_date_key
												and core.end_use_date_key = lx.flt_end_use_date_key
												and core.trvl_acct_email_addr = lx.trvl_acct_email_addr
												and core.tckt_route_key = lx.tckt_route_key
left join ewwgao.hmao_aa_ins_attach_v2 ins on core.tpid = ins.tpid
												and core.begin_use_date_key = ins.flt_begin_use_date_key
												and core.end_use_date_key = ins.flt_end_use_date_key
												and core.trvl_acct_email_addr = ins.trvl_acct_email_addr
												and core.tckt_route_key = ins.tckt_route_key												
group by core.tpid
		,core.begin_use_date_key
		,core.end_use_date_key
		,core.trvl_acct_email_addr
		,core.tckt_route_key
		,core.list_trl
		,core.list_visit_mktg_code
		,core.list_rlt_mktg_code
		,core.list_mktg_carrier_key
		,core.num_trl
		,core.num_visit_mktg_code
		,core.num_rlt_mktg_code
		,core.num_mktg_carrier_key
		,core.site_id
		,core.visit_mktg_code
		,core.rlt_mktg_code
		,core.mktg_carrier_key
		,core.platng_carrier_key
		,core.bkg_ind_key
		,core.tckt_air_fare_typ_key
		,core.air_trip_typ_id
		,core.trans_date_key
		,core.bk_date_key
		,core.tckt_cnt
		,core.gross_bkg_amt
		,core.base_cost
		,core.frnt_end_cmsn_amt
		,core.est_net_rev_amt
		,core.est_gross_profit_amt
		,core.tckt_seg_cnt
		,core.base_price
		,core.child_cnt
		,core.senior_cnt
		,core.infant_cnt
		,core.los
		,core.bk_window
		,core.product_ln_key
		,core.if_fst_class
		,core.if_bzns_class
		,core.if_cch_class
		,core.if_pcch_class
		,core.if_etckt
		,core.if_ptckt
		,core.if_tcktls
		,core.if_sat
		,core.if_nsat
		,core.online_offln_ind	
		,core.route_name
		,core.orign_airpt_name
		,core.dest_airpt_name
		,core.orign_airpt_metro_code
		,core.dest_airpt_metro_code
		,core.orign_airpt_cntry_name
		,core.dest_airpt_cntry_name
		,core.orign_airpt_iata_regn_name
		,core.dest_airpt_iata_regn_name
		,core.orign_airpt_city_name
		,core.dest_airpt_city_name
		,core.orign_airpt_state_provnc_name
		,core.dest_airpt_state_provnc_name
		,core.long_haul_short_haul_ind
		,core.cntry_code
		,core.geo_typ
		,core.dest_airpt_lat
		,core.dest_airpt_log
		,core.orign_airpt_lat
		,core.orign_airpt_log
		,core.dest_num_hotels
		,core.proximity_holidy_ind
		,core.price_per_day;
		
-----------------------------------------------------------------------------------------------------------------------
drop table if exists ewwgao.hmao_aa_chnnl_name_list;
CREATE TABLE ewwgao.hmao_aa_chnnl_name_list AS
select a.mktg_code,a.site_id,b.tpid,max(a.mktg_chnnl_name) as mktg_chnnl_name,max(a.mktg_sub_chnnl_name) as mktg_sub_chnnl_name
from (select site_id,mktg_code,max(a.mktg_chnnl_name) as mktg_chnnl_name,max(a.mktg_sub_chnnl_name) as mktg_sub_chnnl_name
		from dm.dm_omniture_chnnl_dim a
		where site_id is not NULL
		group by site_id,mktg_code) a
left join (select site_id,tpid from dm.dm_omniture_trl_level_summary where tpid is not NULL group by site_id,tpid) b on a.site_id = b.site_id
group by a.mktg_code,a.site_id,b.tpid;
	
		
		
		
		

DROP TABLE IF EXISTS ewwgao.hmao_aa_all_model_v3;
CREATE TABLE ewwgao.hmao_aa_all_model_v3 AS
select a.*
		,month(begin_use_date_key) as bg_m
		,month(bk_date_key) as bkg_m
		,year(begin_use_date_key) as bg_y
		,year(bk_date_key) as bkg_y
		,trunc(begin_use_date_key, 'MM') as bg_ym
		,trunc(bk_date_key, 'MM') as bkg_ym
		,from_unixtime(unix_timestamp(begin_use_date_key,'yyyyMMdd'),'u') as bg_dow
		,from_unixtime(unix_timestamp(bk_date_key,'yyyyMMdd'),'u') as bkg_dow
		,1.0 * gross_bkg_amt / tckt_cnt as avg_gbv
		,1.0 * base_cost / tckt_cnt as avg_bc
		,1.0 * est_net_rev_amt / tckt_cnt as avg_rev
		,1.0 * (base_price - base_cost + (case when tckt_cnt  is NULL  or tckt_cnt = 0 or frnt_end_cmsn_amt is NULL then 0
				else 1.0 * frnt_end_cmsn_amt / tckt_cnt end)) / tckt_cnt as avg_margin
		,1.0 * est_gross_profit_amt / tckt_cnt as avg_gp
		,1.0 * tckt_seg_cnt / tckt_cnt as avg_tseg
		,1.0 * child_cnt / tckt_cnt as child_pcnt
		,1.0 * senior_cnt / tckt_cnt as senior_pcnt
		,(case when geo_typ = "DOM" then 1 
				when geo_typ = "INTL" then 2 
				else 3 end ) geo_typ2
		,(case when long_haul_short_haul_ind = "Short Haul" then 1 
				when long_haul_short_haul_ind = "Long Haul" then 2 
				else 3 end ) long_haul_short_haul_ind2
		,(case when product_ln_key in (101,104,107,186,190,191) then 1 else 0 end) pkg_ind
		,case when c.mktg_chnnl_name is not NULL then c.mktg_chnnl_name
				else 'NULL' end as rlt_mktg_chnnl_name
		,case when c.mktg_sub_chnnl_name is not NULL then c.mktg_sub_chnnl_name
				else 'NULL' end as rlt_mktg_sub_chnnl_name
		,case when d.mktg_chnnl_name is not NULL then d.mktg_chnnl_name
				else 'NULL' end as visit_mktg_chnnl_name
		,case when d.mktg_sub_chnnl_name is not NULL then d.mktg_sub_chnnl_name
				else 'NULL' end as visit_mktg_sub_chnnl_name		
from ewwgao.hmao_aa_all_model_v2 a
left join ewwgao.hmao_aa_chnnl_name_list c
on a.rlt_mktg_code = c.mktg_code and a.tpid = c.tpid and a.site_id = c.site_id
left join ewwgao.hmao_aa_chnnl_name_list d
on a.visit_mktg_code = d.mktg_code and a.tpid = d.tpid and a.site_id = d.site_id;





DROP TABLE IF EXISTS ewwgao.hmao_aa_all_model_v4;
CREATE TABLE ewwgao.hmao_aa_all_model_v4 AS
select core.*
		,day(begin_use_date_key) as bg_d
		,day(bk_date_key) as bkg_d
		,dest_key.airpt_key as dest_airpt_key
		,orign_key.airpt_key as orign_airpt_key
		,case when dest_airpt_lat =-9998 or orign_airpt_lat =-9998 then -100
			else exp_distance(dest_airpt_lat, dest_airpt_log, orign_airpt_lat, orign_airpt_log) end as od_distance
from ewwgao.hmao_aa_all_model_v3 core
left join (select a.dest_airpt_name,max(b.airpt_key) as airpt_key,max(airpt_latitude) as dest_airpt_latitude,max(airpt_longitude) as dest_airpt_longitude
			from dm.route_dim a
			join dm.airpt_dim b on a.dest_airpt_code = b.airpt_code
			group by a.dest_airpt_name) dest_key on core.dest_airpt_name = dest_key.dest_airpt_name
left join (select a.orign_airpt_name,max(b.airpt_key) as airpt_key,max(airpt_latitude) as orign_airpt_latitude,max(airpt_longitude) as orign_airpt_longitude
			from dm.route_dim a
			join dm.airpt_dim b on a.orign_airpt_code = b.airpt_code
			group by a.orign_airpt_name) orign_key on core.orign_airpt_name = orign_key.orign_airpt_name		

-----------------------------------------------------------------------------------------------------------------------------------



drop table if exists ewwgao.hmao_aa_attr_month_route;
CREATE TABLE ewwgao.hmao_aa_attr_month_route AS
select 'bg_ym' as ym_type,orign_airpt_key,dest_airpt_key,bg_ym as ym,count(*) as trvl_cnt,1.0 * sum(if_attach) / count(*) as att_rate
from ewwgao.hmao_aa_all_model_v4
group by orign_airpt_key,dest_airpt_key,bg_ym

union all 

select 'bkg_ym' as ym_type,orign_airpt_key,dest_airpt_key,bkg_ym as ym,count(*) as trvl_cnt,1.0 * sum(if_attach) / count(*) as att_rate
from ewwgao.hmao_aa_all_model_v4
group by orign_airpt_key,dest_airpt_key,bkg_ym;




drop table if exists ewwgao.hmao_aa_attr_month_cntry;
CREATE TABLE ewwgao.hmao_aa_attr_month_cntry AS
select 'bg_ym' as ym_type,orign_airpt_cntry_name,dest_airpt_cntry_name,bg_ym as ym,count(*) as trvl_cnt,1.0 * sum(if_attach) / count(*) as att_rate
from ewwgao.hmao_aa_all_model_v4
group by orign_airpt_cntry_name,dest_airpt_cntry_name,bg_ym

union all 

select 'bkg_ym' as ym_type,orign_airpt_cntry_name,dest_airpt_cntry_name,bkg_ym as ym,count(*) as trvl_cnt,1.0 * sum(if_attach) / count(*) as att_rate
from ewwgao.hmao_aa_all_model_v4
group by orign_airpt_cntry_name,dest_airpt_cntry_name,bkg_ym;



drop table if exists ewwgao.hmao_aa_attr_month_regn;
CREATE TABLE ewwgao.hmao_aa_attr_month_regn AS
select 'bg_ym' as ym_type,orign_airpt_iata_regn_name,dest_airpt_iata_regn_name,bg_ym as ym,count(*) as trvl_cnt,1.0 * sum(if_attach) / count(*) as att_rate
from ewwgao.hmao_aa_all_model_v4
group by orign_airpt_iata_regn_name,dest_airpt_iata_regn_name,bg_ym

union all 

select 'bkg_ym' as ym_type,orign_airpt_iata_regn_name,dest_airpt_iata_regn_name,bkg_ym as ym,count(*) as trvl_cnt,1.0 * sum(if_attach) / count(*) as att_rate
from ewwgao.hmao_aa_all_model_v4
group by orign_airpt_iata_regn_name,dest_airpt_iata_regn_name,bkg_ym;
		






drop table if exists ewwgao.hmao_aa_attr_month_metro;
CREATE TABLE ewwgao.hmao_aa_attr_month_metro AS
select 'bg_ym' as ym_type,orign_airpt_metro_code,dest_airpt_metro_code,bg_ym as ym,count(*) as trvl_cnt,1.0 * sum(if_attach) / count(*) as att_rate
from ewwgao.hmao_aa_all_model_v4
group by orign_airpt_metro_code,dest_airpt_metro_code,bg_ym

union all 

select 'bkg_ym' as ym_type,orign_airpt_metro_code,dest_airpt_metro_code,bkg_ym as ym,count(*) as trvl_cnt,1.0 * sum(if_attach) / count(*) as att_rate
from ewwgao.hmao_aa_all_model_v4
group by orign_airpt_metro_code,dest_airpt_metro_code,bkg_ym;



----------------------------------------------------------------------------------------------------------------------------------------------------
DROP TABLE IF EXISTS ewwgao.hmao_aa_all_model_v4_ds2;
CREATE TABLE ewwgao.hmao_aa_all_model_v4_ds2 AS
select core.*
		,rbg.trvl_cnt as trvl_cnt_bg_l1_rt
		,rbg.att_rate as att_rate_bg_l1_rt
		,rbg2.trvl_cnt as trvl_cnt_bkg_l1_rt
		,rbg2.att_rate as att_rate_bkg_l1_rt	
		,rbg3.trvl_cnt as trvl_cnt_bg_l12_rt
		,rbg3.att_rate as att_rate_bg_l12_rt	
		,rbg4.trvl_cnt as trvl_cnt_bkg_l12_rt
		,rbg4.att_rate as att_rate_bkg_l12_rt		
		,rbg5.trvl_cnt as trvl_cnt_bg_l13_rt
		,rbg5.att_rate as att_rate_bg_l13_rt		
		,rbg6.trvl_cnt as trvl_cnt_bkg_l13_rt
		,rbg6.att_rate as att_rate_bkg_l13_rt		
		,rmc.trvl_cnt as trvl_cnt_bg_l1_cntry
		,rmc.att_rate as att_rate_bg_l1_cntry
		,rmc2.trvl_cnt as trvl_cnt_bkg_l1_cntry
		,rmc2.att_rate as att_rate_bkg_l1_cntry		
		,rmc3.trvl_cnt as trvl_cnt_bg_l12_cntry
		,rmc3.att_rate as att_rate_bg_l12_cntry		
		,rmc4.trvl_cnt as trvl_cnt_bkg_l12_cntry
		,rmc4.att_rate as att_rate_bkg_l12_cntry	
		,rmc5.trvl_cnt as trvl_cnt_bg_l13_cntry
		,rmc5.att_rate as att_rate_bg_l13_cntry		
		,rmc6.trvl_cnt as trvl_cnt_bkg_l13_cntry
		,rmc6.att_rate as att_rate_bkg_l13_cntry	
		,rmr.trvl_cnt as trvl_cnt_bg_l1_regn
		,rmr.att_rate as att_rate_bg_l1_regn
		,rmr2.trvl_cnt as trvl_cnt_bkg_l1_regn
		,rmr2.att_rate as att_rate_bkg_l1_regn
		,rmr3.trvl_cnt as trvl_cnt_bg_l12_regn
		,rmr3.att_rate as att_rate_bg_l12_regn		
		,rmr4.trvl_cnt as trvl_cnt_bkg_l12_regn
		,rmr4.att_rate as att_rate_bkg_l12_regn		
		,rmr5.trvl_cnt as trvl_cnt_bg_l13_regn
		,rmr5.att_rate as att_rate_bg_l13_regn		
		,rmr6.trvl_cnt as trvl_cnt_bkg_l13_regn
		,rmr6.att_rate as att_rate_bkg_l13_regn			
		,rmm.trvl_cnt as trvl_cnt_bg_l1_metro
		,rmm.att_rate as att_rate_bg_l1_metro
		,rmm2.trvl_cnt as trvl_cnt_bkg_l1_metro
		,rmm2.att_rate as att_rate_bkg_l1_metro		
		,rmm3.trvl_cnt as trvl_cnt_bg_l12_metro
		,rmm3.att_rate as att_rate_bg_l12_metro	
		,rmm4.trvl_cnt as trvl_cnt_bkg_l12_metro
		,rmm4.att_rate as att_rate_bkg_l12_metro	
		,rmm5.trvl_cnt as trvl_cnt_bg_l13_metro
		,rmm5.att_rate as att_rate_bg_l13_metro		
		,rmm6.trvl_cnt as trvl_cnt_bkg_l13_metro
		,rmm6.att_rate as att_rate_bkg_l13_metro	
from (select *
			,add_months(bg_ym, -1) as bg_ym_l1
			,add_months(bg_ym, -12) as bg_ym_l12
			,add_months(bg_ym, -13) as bg_ym_l13
			,add_months(bkg_ym, -1) as bkg_ym_l1
			,add_months(bkg_ym, -12) as bkg_ym_l12
			,add_months(bkg_ym, -13) as bkg_ym_l13
		from ewwgao.hmao_aa_all_model_v4
		where bk_date_key between '2015-07-01' and '2016-10-31') core
left join (select * from ewwgao.hmao_aa_attr_month_route
			where ym_type = 'bg_ym') rbg on core.bg_ym_l1 = rbg.ym and core.orign_airpt_key = rbg.orign_airpt_key and core.dest_airpt_key = rbg.dest_airpt_key
left join (select * from ewwgao.hmao_aa_attr_month_route
			where ym_type = 'bkg_ym') rbg2 on core.bkg_ym_l1 = rbg2.ym and core.orign_airpt_key = rbg2.orign_airpt_key and core.dest_airpt_key = rbg2.dest_airpt_key
left join (select * from ewwgao.hmao_aa_attr_month_route
			where ym_type = 'bg_ym') rbg3 on core.bg_ym_l12 = rbg3.ym and core.orign_airpt_key = rbg3.orign_airpt_key and core.dest_airpt_key = rbg3.dest_airpt_key
left join (select * from ewwgao.hmao_aa_attr_month_route
			where ym_type = 'bkg_ym') rbg4 on core.bkg_ym_l12 = rbg4.ym and core.orign_airpt_key = rbg4.orign_airpt_key and core.dest_airpt_key = rbg4.dest_airpt_key			
left join (select * from ewwgao.hmao_aa_attr_month_route
			where ym_type = 'bg_ym') rbg5 on core.bg_ym_l13 = rbg5.ym and core.orign_airpt_key = rbg5.orign_airpt_key and core.dest_airpt_key = rbg5.dest_airpt_key
left join (select * from ewwgao.hmao_aa_attr_month_route
			where ym_type = 'bkg_ym') rbg6 on core.bkg_ym_l13 = rbg6.ym and core.orign_airpt_key = rbg6.orign_airpt_key and core.dest_airpt_key = rbg6.dest_airpt_key
left join (select * from ewwgao.hmao_aa_attr_month_cntry
			where ym_type = 'bg_ym') rmc on core.bg_ym_l1 = rmc.ym and core.orign_airpt_cntry_name = rmc.orign_airpt_cntry_name and core.dest_airpt_cntry_name = rmc.dest_airpt_cntry_name
left join (select * from ewwgao.hmao_aa_attr_month_cntry
			where ym_type = 'bkg_ym') rmc2 on core.bkg_ym_l1 = rmc2.ym and core.orign_airpt_cntry_name = rmc2.orign_airpt_cntry_name and core.dest_airpt_cntry_name = rmc2.dest_airpt_cntry_name
left join (select * from ewwgao.hmao_aa_attr_month_cntry
			where ym_type = 'bg_ym') rmc3 on core.bg_ym_l12 = rmc3.ym and core.orign_airpt_cntry_name = rmc3.orign_airpt_cntry_name and core.dest_airpt_cntry_name = rmc3.dest_airpt_cntry_name
left join (select * from ewwgao.hmao_aa_attr_month_cntry
			where ym_type = 'bkg_ym') rmc4 on core.bkg_ym_l12 = rmc4.ym and core.orign_airpt_cntry_name = rmc4.orign_airpt_cntry_name and core.dest_airpt_cntry_name = rmc4.dest_airpt_cntry_name			
left join (select * from ewwgao.hmao_aa_attr_month_cntry
			where ym_type = 'bg_ym') rmc5 on core.bg_ym_l13 = rmc5.ym and core.orign_airpt_cntry_name = rmc5.orign_airpt_cntry_name and core.dest_airpt_cntry_name = rmc5.dest_airpt_cntry_name
left join (select * from ewwgao.hmao_aa_attr_month_cntry
			where ym_type = 'bkg_ym') rmc6 on core.bkg_ym_l13 = rmc6.ym and core.orign_airpt_cntry_name = rmc6.orign_airpt_cntry_name and core.dest_airpt_cntry_name = rmc6.dest_airpt_cntry_name
left join (select * from ewwgao.hmao_aa_attr_month_regn
			where ym_type = 'bg_ym') rmr on core.bg_ym_l1 = rmr.ym and core.orign_airpt_iata_regn_name = rmr.orign_airpt_iata_regn_name and core.dest_airpt_iata_regn_name = rmr.dest_airpt_iata_regn_name
left join (select * from ewwgao.hmao_aa_attr_month_regn
			where ym_type = 'bkg_ym') rmr2 on core.bkg_ym_l1 = rmr2.ym and core.orign_airpt_iata_regn_name = rmr2.orign_airpt_iata_regn_name and core.dest_airpt_iata_regn_name = rmr2.dest_airpt_iata_regn_name
left join (select * from ewwgao.hmao_aa_attr_month_regn
			where ym_type = 'bg_ym') rmr3 on core.bg_ym_l12 = rmr3.ym and core.orign_airpt_iata_regn_name = rmr3.orign_airpt_iata_regn_name and core.dest_airpt_iata_regn_name = rmr3.dest_airpt_iata_regn_name
left join (select * from ewwgao.hmao_aa_attr_month_regn
			where ym_type = 'bkg_ym') rmr4 on core.bkg_ym_l12 = rmr4.ym and core.orign_airpt_iata_regn_name = rmr4.orign_airpt_iata_regn_name and core.dest_airpt_iata_regn_name = rmr4.dest_airpt_iata_regn_name			
left join (select * from ewwgao.hmao_aa_attr_month_regn
			where ym_type = 'bg_ym') rmr5 on core.bg_ym_l13 = rmr5.ym and core.orign_airpt_iata_regn_name = rmr5.orign_airpt_iata_regn_name and core.dest_airpt_iata_regn_name = rmr5.dest_airpt_iata_regn_name
left join (select * from ewwgao.hmao_aa_attr_month_regn
			where ym_type = 'bkg_ym') rmr6 on core.bkg_ym_l13 = rmr6.ym and core.orign_airpt_iata_regn_name = rmr6.orign_airpt_iata_regn_name and core.dest_airpt_iata_regn_name = rmr6.dest_airpt_iata_regn_name
left join (select * from ewwgao.hmao_aa_attr_month_metro
			where ym_type = 'bg_ym') rmm on core.bg_ym_l1 = rmm.ym and core.orign_airpt_metro_code = rmm.orign_airpt_metro_code and core.dest_airpt_metro_code = rmm.dest_airpt_metro_code
left join (select * from ewwgao.hmao_aa_attr_month_metro
			where ym_type = 'bkg_ym') rmm2 on core.bkg_ym_l1 = rmm2.ym and core.orign_airpt_metro_code = rmm2.orign_airpt_metro_code and core.dest_airpt_metro_code = rmm2.dest_airpt_metro_code
left join (select * from ewwgao.hmao_aa_attr_month_metro
			where ym_type = 'bg_ym') rmm3 on core.bg_ym_l12 = rmm3.ym and core.orign_airpt_metro_code = rmm3.orign_airpt_metro_code and core.dest_airpt_metro_code = rmm3.dest_airpt_metro_code
left join (select * from ewwgao.hmao_aa_attr_month_metro
			where ym_type = 'bkg_ym') rmm4 on core.bkg_ym_l12 = rmm4.ym and core.orign_airpt_metro_code = rmm4.orign_airpt_metro_code and core.dest_airpt_metro_code = rmm4.dest_airpt_metro_code			
left join (select * from ewwgao.hmao_aa_attr_month_metro
			where ym_type = 'bg_ym') rmm5 on core.bg_ym_l13 = rmm5.ym and core.orign_airpt_metro_code = rmm5.orign_airpt_metro_code and core.dest_airpt_metro_code = rmm5.dest_airpt_metro_code
left join (select * from ewwgao.hmao_aa_attr_month_metro
			where ym_type = 'bkg_ym') rmm6 on core.bkg_ym_l13 = rmm6.ym and core.orign_airpt_metro_code = rmm6.orign_airpt_metro_code and core.dest_airpt_metro_code = rmm6.dest_airpt_metro_code
;





