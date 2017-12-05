

---Week in Churn Analysis---

---TA Save/Fail---

--Previous Behaviour (Value Segment?)
--Sky Go and On Demand Activity---


select top 100 * from sk_prod.cust_anytime_plus_downloads where account_number = '620041578563' 
select top 100 * from sk_prod.SKY_PLAYER_USAGE_DETAIL where account_number = '620041578563' order by activity_dt desc;

select network_code
, cast(last_modified_dt as date) as modified_date
,sum(x_download_size_mb) as downloaded_mb
,count(*) as records
from  sk_prod.cust_anytime_plus_downloads
where last_modified_dt>='2013-08-01'
group by network_code
,modified_date
;
commit;

select asset_name
,provider_brand 
,sum(x_download_size_mb) as downloaded_mb
,count(*) as records
from  sk_prod.cust_anytime_plus_downloads
where last_modified_dt>='2013-08-01'
group by asset_name
,provider_brand
order by records desc
;



select broadcast_channel
,activity_dt
,count(*) as streams
,count(distinct account_number) as accounts

from  sk_prod.SKY_PLAYER_USAGE_DETAIL 
where x_usage_type='Live Viewing' and activity_dt>='2013-04-01'
group by broadcast_channel
,activity_dt
;

commit;






select top 100 * from sk_prod.cust_anytime_plus_downloads 
where cast(last_modified_dt as date) >= '2013-03-25'
and             cast(last_modified_dt as date) <= '2013-03-25'   and asset_id is null

select cs_uri,asset_name,episode_title,count(*) as records, min (last_modified_dt), max(last_modified_dt)
from sk_prod.cust_anytime_plus_downloads
group by cs_uri,asset_name,episode_title
order by records desc, cs_uri,asset_name,episode_title


select provider_brand, min(asset_id) , max(asset_id),min(asset_name) ,max(asset_name), cs_uri,sum(download_count) as downloads
 from sk_prod.cust_anytime_plus_downloads
where cast(last_modified_dt as date) >= '2013-03-25'
and             cast(last_modified_dt as date) <= '2013-03-25'      
group by provider_brand, asset_id,cs_uri,asset_name
order by downloads desc
;

commit;





--obtain all game of thrones downloads & create flag for that particular episode;
select          account_number
                ,asset_name
                ,synopsis
                ,cast(last_modified_dt as date) lmd            
                ,case when ucase(synopsis) like '%VALAR DOHAERIS%' then 1 else 0 end as s03e01
into            #anytime_plus_dl
from            sk_prod.cust_anytime_plus_downloads
where           cast(last_modified_dt as date) >= '2013-04-01'
and             cast(last_modified_dt as date) <= '2013-04-10'                     
and             ucase(asset_name) like '%THRONES%'
;
--determine episode then feed back up to above;
select          count(*)
                ,synopsis   
from            #anytime_plus_dl
group by        asset_name
                ,synopsis
order by        asset_name
                ,synopsis
;
commit;



select  asset_name , episode_title
,count(*) as records
,count(distinct account_number) as accounts
from sk_prod.cust_anytime_plus_downloads
where           cast(last_modified_dt as date) >= '2013-04-08'
and             cast(last_modified_dt as date) <= '2013-04-14'  
and download_count=1
group by asset_name , episode_title
order by records desc
;

commit;

select  cb_data_date 
        ,count(*) as usage_records
        ,count(distinct account_number) as accounts
--        ,sum(SKY_GO_USAGE)

from sk_prod.SKY_PLAYER_USAGE_DETAIL AS usage
--        inner join v159_accounts_for_profiling_dec2011_active AS Base
--         ON usage.account_number = Base.account_number
group by cb_data_date
order by cb_data_date;

select count(*) from  sk_prod.SKY_PLAYER_USAGE_DETAIL 

select count(*) from  sk_prod.REMOTE_RECORD_ACTIVITY
select  * into #bb_detail from  sk_prod.CUST_BROADBAND_DOWNLOAD_DETAIL where account_number = '200000852737'
commit;

select * from  #bb_detail order by usage_date

commit;


select top 100 * from sk_prod.experian_consumerview
