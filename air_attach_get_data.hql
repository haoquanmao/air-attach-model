set mapred.job.queue.name = edwbiz;
set hive.cli.print.header = true; 
set hive.exec.parallel = true;
set hive.cbo.enable = true;
set hive.execution.engine = tez;



--get core table first
DROP TABLE IF EXISTS ewwgao.hmao_aa_flight_core;
CREATE TABLE ewwgao.hmao_aa_flight_core AS
select core.tpid
		,core.begin_use_date_key
		,core.end_use_date_key
		,core.trvl_acct_email_addr
		,core.tckt_route_key
		,collect_set(core.trl) as list_trl
		,count(distinct core.trl) as num_trl
		,max(core.air_trip_typ_id) as air_trip_typ_id
		,max(core.plating_carrier_key) as plating_carrier_key
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
from 
		(select a.*
		from
			(select a.*
					,c.trvl_acct_email_addr
			from 
					(select un_table.tpid
							,un_table.trl
							,un_table.begin_use_date_key
							,un_table.end_use_date_key
							,un_table.purch_trvl_acct_key
							,un_table.tckt_route_key
							,un_table.air_trip_typ_id
							,un_table.trans_date_key
							,un_table.bk_date_key
							,un_table.plating_carrier_key
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
								,max(abid.air_trip_typ_id) as air_trip_typ_id
								,min(attf.trans_date_key) as trans_date_key
								,min(attf.bk_date_key) as bk_date_key
								,max(attf.plating_carrier_key) as plating_carrier_key
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
						where attf.bk_date_key between '2015-01-01' and '2016-10-16'
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
								,999 as air_trip_typ_id
								,min(attf.trans_date_key) as trans_date_key
								,min(attf.bk_date_key) as bk_date_key
								,max(attf.plating_carrier_key) as plating_carrier_key
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
						where attf.bk_date_key between '2015-01-01' and '2016-10-16'
								and attf.trans_typ_key = 101
								and abid.air_trip_typ_id in (1,2)
								and attf.splt_tckt_ind_key = 101
						group by attf.tpid,attf.trl,attf.purch_trvl_acct_key
						) un_table
						where un_table.dist_typ_cnt = 1
					) a
					inner join (select * 
								from dm.trvl_acct_dim
								where lower(trvl_acct_email_addr) != 'unknown') c on a.purch_trvl_acct_key = c.trvl_acct_key and a.tpid = c.trvl_acct_tpid
			) a								
			left join
			(
				select 
					a.tpid 
					,a.trl
				from dm.air_trans_fact a
				where trans_typ_key between 102 and 104
				group by a.tpid 
						,a.trl
				) b on a.tpid = b.tpid and a.trl = b.trl
		where b.tpid is Null
		) core
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
stored as textfile location '/user/hmao/cntry_holiday_list2'
'''

'''
alter table ewwgao.hmao_cntry_holiday_list add columns (r_date date);
insert into table ewwgao.hmao_cntry_holiday_list
select *,to_date(FROM_UNIXTIME(unix_timestamp(h_date, 'mm/dd/yyyy'))) as r_date
from
ewwgao.hmao_cntry_holiday_list
;
'''


DROP TABLE IF EXISTS ewwgao.hmao_aa_flight_core_extend;
CREATE TABLE ewwgao.hmao_aa_flight_core_extend AS
select core.tpid
		,core.begin_use_date_key
		,core.end_use_date_key
		,core.trvl_acct_email_addr
		,core.tckt_route_key
		,core.list_trl
		,core.num_trl
		,core.air_trip_typ_id
		,core.platng_carrier_key
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
from ewwgao.hmao_aa_flight_core core
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
group by core.tpid
		,core.begin_use_date_key
		,core.end_use_date_key
		,core.trvl_acct_email_addr
		,core.tckt_route_key
		,core.list_trl
		,core.num_trl
		,core.air_trip_typ_id
		,core.platng_carrier_key
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
	
DROP TABLE IF EXISTS ewwgao.hmao_aa_hotel;
CREATE TABLE ewwgao.hmao_aa_hotel AS		
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
					where lower(trvl_acct_email_addr) != 'unknown') tad on ltf.purch_trvl_acct_key = tad.trvl_acct_key and ltf.tpid = tad.trvl_acct_tpid
		inner join dm.lodg_property_dim lpd on ltf.lodg_property_key = lpd.lodg_property_key
		where ltf.trans_typ_key = 101 and ltf.bk_date_key between '2015-01-01' and '2016-10-25' and lpd.property_latitude != 0
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






DROP TABLE IF EXISTS ewwgao.hmao_aa_car;
CREATE TABLE ewwgao.hmao_aa_car AS
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
					where lower(trvl_acct_email_addr) != 'unknown') tad on crtf.purch_trvl_acct_key = tad.trvl_acct_key and crtf.tpid = tad.trvl_acct_tpid
		where crtf.trans_typ_key = 101 and crtf.bk_date_key between '2015-01-01' and '2016-10-25'		
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






DROP TABLE IF EXISTS ewwgao.hmao_aa_ins;
CREATE TABLE ewwgao.hmao_aa_ins AS
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
					where lower(trvl_acct_email_addr) != 'unknown') tad on ittf.purch_trvl_acct_key = tad.trvl_acct_key and ittf.tpid = tad.trvl_acct_tpid
		where ittf.trans_typ_key = 101 and ittf.bk_date_key between '2015-01-01' and '2016-10-25'
		group by tad.trvl_acct_email_addr
				,ittf.tpid
				,ittf.trl) purch_ins	
left join 	(select tpid,trl 
			from dm.ins_itm_trans_fact 
			where trans_typ_key >= 102 and trans_typ_key <= 104
			group by tpid,trl) cncl_ins on purch_ins.tpid = cncl_ins.tpid and purch_ins.trl = cncl_ins.trl
where cncl_ins.tpid is NULL	






DROP TABLE IF EXISTS ewwgao.hmao_aa_lx;
CREATE TABLE ewwgao.hmao_aa_lx AS
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
					where lower(trvl_acct_email_addr) != 'unknown') tad on dstf.purch_trvl_acct_key = tad.trvl_acct_key and dstf.tpid = tad.trvl_acct_tpid
		inner join dm.atlas_regn_dim ard on dstf.dest_svc_srch_loc_key = ard.atlas_regn_key
		where dstf.trans_typ_key = 101 and dstf.bk_date_key between '2015-01-01' and '2016-10-25'
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
where cncl_lx.tpid is NULL and lpd.property_latitude != 0
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
		,lpd.property_longitude

		

--Begin attaching other LOBs


		--hotel
DROP TABLE IF EXISTS ewwgao.hmao_aa_hotel_attach;
CREATE TABLE ewwgao.hmao_aa_hotel_attach AS	
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
						from ewwgao.hmao_aa_flight_core_extend) core
				inner join ewwgao.hmao_aa_hotel hotel on core.trvl_acct_email_addr = hotel.trvl_acct_email_addr and core.tpid = hotel.tpid
				) base
		where date_match_ind = 1
		) base
	where rank = 1) base







		--car
DROP TABLE IF EXISTS ewwgao.hmao_aa_car_attach;
CREATE TABLE ewwgao.hmao_aa_car_attach AS
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
					from ewwgao.hmao_aa_flight_core_extend) core
			inner join ewwgao.hmao_aa_car car on core.trvl_acct_email_addr = car.trvl_acct_email_addr and core.tpid = car.tpid
			) base
	where date_match_ind = 1 and loc_match_ind = 1
	) base	
where rank = 1;


		
		
		
		--ins
DROP TABLE IF EXISTS ewwgao.hmao_aa_ins_attach;
CREATE TABLE ewwgao.hmao_aa_ins_attach AS	
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
from ewwgao.hmao_aa_flight_core_extend core
inner join ewwgao.hmao_aa_ins ins on core.trvl_acct_email_addr = ins.trvl_acct_email_addr and core.tpid = ins.tpid
where array_contains(core.list_trl,ins.trl) = True;
	
	
	
		
		--lx
DROP TABLE IF EXISTS ewwgao.hmao_aa_lx_attach;
CREATE TABLE ewwgao.hmao_aa_lx_attach AS	
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
		,case when base.airpt_htl_distance  <= 80.467 then 1 else 0 end as if_50_mile
		,case when base.airpt_htl_distance  <= 160.934 then 1 else 0 end as if_100_mile
		,case when base.airpt_htl_distance  <= 804.672 then 1 else 0 end as if_500_mile
		,case when base.airpt_htl_distance  <= 1609.344 then 1 else 0 end as if_1000_mile
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
								from ewwgao.hmao_aa_flight_core_extend) core
						inner join ewwgao.hmao_aa_lx lx on core.trvl_acct_email_addr = lx.trvl_acct_email_addr and core.tpid = lx.tpid
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
		
		


--combine LOB attach table with core

DROP TABLE IF EXISTS ewwgao.hmao_aa_hotel_model;
CREATE TABLE ewwgao.hmao_aa_hotel_model AS
select core.tpid
		,core.begin_use_date_key
		,core.end_use_date_key
		,core.trvl_acct_email_addr
		,core.tckt_route_key
		,core.air_trip_typ_id
		,core.platng_carrier_key
		,core.bk_date_key
		,core.trans_date_key
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
		,core.price_per_day
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
		,core.geo_typ
		,core.dest_airpt_lat
		,core.dest_airpt_log
		,core.dest_num_hotels
		,core.proximity_holidy_ind
		,case when sum(case
						when hotel.if_50_mile = 1 then 1
						else 0 end) > 0 then 1 else 0 end as if_50_mile
		,case when sum(case
						when hotel.if_100_mile = 1 then 1
						else 0 end) > 0 then 1 else 0 end as if_100_mile
		,case when sum(case
						when hotel.if_500_mile = 1 then 1
						else 0 end) > 0 then 1 else 0 end as if_500_mile
		,case when sum(case
						when hotel.if_1000_mile = 1 then 1
						else 0 end) > 0 then 1 else 0 end as if_1000_mile		
		,case 
			when sum(case
						when hotel.tpid is NULL then 0
						else 1 end) > 0 then 1 
			else 0 end as if_hotel_attach
		,sum(hotel.rm_cnt) as rm_cnt
		,sum(hotel.rm_night_cnt) as rm_night_cnt
		,sum(hotel.gross_bkg_amt_usd) as gross_bkg_amt_usd
		,sum(hotel.base_price_amt_usd) as hbase_price_amt_usd
		,sum(hotel.base_cost_amt_usd) as base_cost_amt_usd
		,sum(hotel.frnt_end_cmsn_amt_usd) as frnt_end_cmsn_amt_usd
		,sum(hotel.est_net_rev_amt_usd) as est_net_rev_amt_usd
		,sum(hotel.est_gross_profit_amt_usd) as est_gross_profit_amt_usd
from (select a.*
		from ewwgao.hmao_aa_flight_core_extend a
		left join (select distinct a.trvl_acct_email_addr
					from dm.trvl_acct_dim a
					inner join ewwgao.hmao_trvl_agent_list b on a.trvl_acct_id = b.tuid
					) b on a.trvl_acct_email_addr = b.trvl_acct_email_addr			
		where b.trvl_acct_email_addr is NULL         
		) core
left join ewwgao.hmao_aa_hotel_attach hotel on core.tpid = hotel.tpid
												and core.begin_use_date_key = hotel.flt_begin_use_date_key
												and core.end_use_date_key = hotel.flt_end_use_date_key
												and core.trvl_acct_email_addr = hotel.trvl_acct_email_addr
												and core.tckt_route_key = hotel.tckt_route_key
group by core.tpid
		,core.begin_use_date_key
		,core.end_use_date_key
		,core.trvl_acct_email_addr
		,core.tckt_route_key
		,core.air_trip_typ_id
		,core.platng_carrier_key
		,core.bk_date_key
		,core.trans_date_key
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
		,core.price_per_day
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
		,core.geo_typ
		,core.dest_airpt_lat
		,core.dest_airpt_log
		,core.dest_num_hotels
		,core.proximity_holidy_ind
		
-----------------------------------------------------------------------------------------------------------------------


DROP TABLE IF EXISTS ewwgao.hmao_aa_car_model;
CREATE TABLE ewwgao.hmao_aa_car_model AS
select core.tpid
		,core.begin_use_date_key
		,core.end_use_date_key
		,core.trvl_acct_email_addr
		,core.tckt_route_key
		,core.air_trip_typ_id
		,core.plating_carrier_key
		,core.bk_date_key
		,core.trans_date_key
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
		,core.price_per_day
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
		,core.geo_typ
		,core.dest_airpt_lat
		,core.dest_airpt_log
		,core.dest_num_hotels
		,core.proximity_holidy_ind
		,case 
			when sum(case
						when car.tpid is NULL then 0
						else 1 end) > 0 then 1 
			else 0 end as if_car_attach
		,sum(car.car_rentl_cnt) as car_rentl_cnt
		,sum(car.rentl_day_cnt) as rentl_day_cnt
		,sum(car.gross_bkg_amt_usd) as gross_bkg_amt_usd
		,sum(car.base_price_amt_usd) as hbase_price_amt_usd
		,sum(car.base_cost_amt_usd) as base_cost_amt_usd
		,sum(car.frnt_end_cmsn_amt_usd) as frnt_end_cmsn_amt_usd
		,sum(car.est_net_rev_amt_usd) as est_net_rev_amt_usd
		,sum(car.est_gross_profit_amt_usd) as est_gross_profit_amt_usd
from (select a.*
		from ewwgao.hmao_aa_flight_core_extend a
		left join (select distinct a.trvl_acct_email_addr
					from dm.trvl_acct_dim a
					inner join ewwgao.hmao_trvl_agent_list b on a.trvl_acct_id = b.tuid
					) b on a.trvl_acct_email_addr = b.trvl_acct_email_addr			
		where b.trvl_acct_email_addr is NULL         
		) core
left join ewwgao.hmao_aa_car_attach car on core.tpid = car.tpid
												and core.begin_use_date_key = car.flt_begin_use_date_key
												and core.end_use_date_key = car.flt_end_use_date_key
												and core.trvl_acct_email_addr = car.trvl_acct_email_addr
												and core.tckt_route_key = car.tckt_route_key
group by core.tpid
		,core.begin_use_date_key
		,core.end_use_date_key
		,core.trvl_acct_email_addr
		,core.tckt_route_key
		,core.air_trip_typ_id
		,core.plating_carrier_key
		,core.bk_date_key
		,core.trans_date_key
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
		,core.price_per_day
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
		,core.geo_typ
		,core.dest_airpt_lat
		,core.dest_airpt_log
		,core.dest_num_hotels
		,core.proximity_holidy_ind




---------------------------------------------------------------------------------------------------------------------------



DROP TABLE IF EXISTS ewwgao.hmao_aa_lx_model;
CREATE TABLE ewwgao.hmao_aa_lx_model AS
select core.tpid
		,core.begin_use_date_key
		,core.end_use_date_key
		,core.trvl_acct_email_addr
		,core.tckt_route_key
		,core.air_trip_typ_id
		,core.plating_carrier_key
		,core.bk_date_key
		,core.trans_date_key
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
		,core.price_per_day
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
		,core.geo_typ
		,core.dest_airpt_lat
		,core.dest_airpt_log
		,core.dest_num_hotels
		,core.proximity_holidy_ind
		,case when sum(case
						when lx.if_50_mile = 1 then 1
						else 0 end) > 0 then 1 else 0 end as if_50_mile
		,case when sum(case
						when lx.if_100_mile = 1 then 1
						else 0 end) > 0 then 1 else 0 end as if_100_mile
		,case when sum(case
						when lx.if_500_mile = 1 then 1
						else 0 end) > 0 then 1 else 0 end as if_500_mile
		,case when sum(case
						when lx.if_1000_mile = 1 then 1
						else 0 end) > 0 then 1 else 0 end as if_1000_mile
		,case 
			when sum(case
						when lx.tpid is NULL then 0
						else 1 end) > 0 then 1 
			else 0 end as if_lx_attach
		,sum(lx.totl_dest_svc_tckt_cnt) as totl_dest_svc_tckt_cnt
		,sum(lx.gross_bkg_amt_usd) as gross_bkg_amt_usd
		,sum(lx.base_price_amt_usd) as hbase_price_amt_usd
		,sum(lx.base_cost_amt_usd) as base_cost_amt_usd
		,sum(lx.est_net_rev_amt_usd) as est_net_rev_amt_usd
		,sum(lx.est_gross_profit_amt_usd) as est_gross_profit_amt_usd
from (select a.*
		from ewwgao.hmao_aa_flight_core_extend a
		left join (select distinct a.trvl_acct_email_addr
					from dm.trvl_acct_dim a
					inner join ewwgao.hmao_trvl_agent_list b on a.trvl_acct_id = b.tuid
					) b on a.trvl_acct_email_addr = b.trvl_acct_email_addr			
		where b.trvl_acct_email_addr is NULL         
		) core
left join ewwgao.hmao_aa_lx_attach lx on core.tpid = lx.tpid
												and core.begin_use_date_key = lx.flt_begin_use_date_key
												and core.end_use_date_key = lx.flt_end_use_date_key
												and core.trvl_acct_email_addr = lx.trvl_acct_email_addr
												and core.tckt_route_key = lx.tckt_route_key
group by core.tpid
		,core.begin_use_date_key
		,core.end_use_date_key
		,core.trvl_acct_email_addr
		,core.tckt_route_key
		,core.air_trip_typ_id
		,core.plating_carrier_key
		,core.bk_date_key
		,core.trans_date_key
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
		,core.price_per_day
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
		,core.geo_typ
		,core.dest_airpt_lat
		,core.dest_airpt_log
		,core.dest_num_hotels
		,core.proximity_holidy_ind



------------------------------------------------------------------------------------------------------------------------------------


DROP TABLE IF EXISTS ewwgao.hmao_aa_ins_model;
CREATE TABLE ewwgao.hmao_aa_ins_model AS
select core.tpid
		,core.begin_use_date_key
		,core.end_use_date_key
		,core.trvl_acct_email_addr
		,core.tckt_route_key
		,core.air_trip_typ_id
		,core.plating_carrier_key
		,core.bk_date_key
		,core.trans_date_key
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
		,core.price_per_day
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
		,core.geo_typ
		,core.dest_airpt_lat
		,core.dest_airpt_log
		,core.dest_num_hotels
		,core.proximity_holidy_ind
		,case 
			when sum(case
						when ins.tpid is NULL then 0
						else 1 end) > 0 then 1 
			else 0 end as if_ins_attach
		,sum(ins.ins_itm_cnt) as ins_itm_cnt
		,sum(ins.gross_bkg_amt_usd) as gross_bkg_amt_usd
		,sum(ins.base_price_amt_usd) as hbase_price_amt_usd
		,sum(ins.base_cost_amt_usd) as base_cost_amt_usd
		,sum(ins.rnt_end_cmsn_amt_usd) as rnt_end_cmsn_amt_usd
		,sum(ins.est_net_rev_amt_usd) as est_net_rev_amt_usd
		,sum(ins.est_gross_profit_amt_usd) as est_gross_profit_amt_usd
from (select a.*
		from ewwgao.hmao_aa_flight_core_extend a
		left join (select distinct a.trvl_acct_email_addr
					from dm.trvl_acct_dim a
					inner join ewwgao.hmao_trvl_agent_list b on a.trvl_acct_id = b.tuid
					) b on a.trvl_acct_email_addr = b.trvl_acct_email_addr			
		where b.trvl_acct_email_addr is NULL         
		) core
left join ewwgao.hmao_aa_ins_attach ins on core.tpid = ins.tpid
												and core.begin_use_date_key = ins.flt_begin_use_date_key
												and core.end_use_date_key = ins.flt_end_use_date_key
												and core.trvl_acct_email_addr = ins.trvl_acct_email_addr
												and core.tckt_route_key = ins.tckt_route_key
group by core.tpid
		,core.begin_use_date_key
		,core.end_use_date_key
		,core.trvl_acct_email_addr
		,core.tckt_route_key
		,core.air_trip_typ_id
		,core.plating_carrier_key
		,core.bk_date_key
		,core.trans_date_key
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
		,core.price_per_day
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
		,core.geo_typ
		,core.dest_airpt_lat
		,core.dest_airpt_log
		,core.dest_num_hotels
		,core.proximity_holidy_ind

---------------------------------------------------------------------------------------------------------------------------------



































