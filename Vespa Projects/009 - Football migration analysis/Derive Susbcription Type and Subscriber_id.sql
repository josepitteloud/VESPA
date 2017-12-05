-- B01 - Create Base

--creating a date variable to use throughout the code (Date used in for end of day prior to target date (e.g., status at end of 28th Nov)
  create variable @target_date date;
     set @target_date = '20111128';


if object_id('sky_base_2011_11_29') is not null drop table vespa_analysts.sky_base_2011_11_29;
CREATE TABLE vespa_analysts.sky_base_2011_11_29 ( -- drop table govt_region_base
         account_number                  varchar(30)   NOT NULL
         ,cb_key_household                 bigint      
         ,current_short_description      varchar(70)
         ,postcode                        varchar(10)  default 'Unknown'
        ,service_instance_id            varchar(50) 
        ,SUBSCRIPTION_SUB_TYPE          varchar(50) 
);

--alter table vespa_analysts.sky_base_2011_11_29 add cb_key_household2 bigint;
--update  vespa_analysts.sky_base_2011_11_29 drop cb_key_household;
--update vespa_analysts.sky_base_2011_11_29 rename cb_key_household2 to cb_key_household;

--select count(*) from vespa_analysts.sky_base_2011_11_29 ;

--drop  index   vespa_analysts.sky_base_2011_11_29.idx1;

create hg index idx1 on vespa_analysts.sky_base_2011_11_29(account_number);
create hg index idx2 on vespa_analysts.sky_base_2011_11_29(cb_key_household);

grant all on vespa_analysts.sky_base_2011_11_29               to public;


select account_number
        , cb_key_household
        , csh.current_short_description
        ,service_instance_id
        ,SUBSCRIPTION_SUB_TYPE
        , rank() over (partition by account_number ,SUBSCRIPTION_SUB_TYPE,service_instance_id order by effective_from_dt, cb_row_id) as rank
into #sky_accounts -- drop table #sky_accounts
from sk_prod.cust_subs_hist as csh
where SUBSCRIPTION_SUB_TYPE in ('DTV Primary Viewing','DTV Extra Subscription') --the DTV + Multiroom sub Type
   and status_code in ('AC','PC')               --Active Status Codes (ACtive Block Removed)
   and effective_from_dt <= @target_date             --Start on or before 1st Jan
   and effective_to_dt > @target_date                --ends after 1st Jan
   and effective_from_dt<>effective_to_dt            --ignore all but the last thing each customer did in a day
--and cb_key_household > 0
--and cb_key_household is not null
and account_number is not null;
commit;


delete from #sky_accounts where rank>1;
commit;

create hg index idx1 on #sky_accounts(service_instance_id);

create  hg index idx2 on #sky_accounts(cb_key_household);
--select count(distinct account_number) from #sky_accounts;
--select count(account_number), count(distinct account_number), count(cb_key_household), count(distinct cb_key_household), count(*) from vespa_analysts.sky_base_2011_11_29
--10035768        10035768        10035768        9546754 10035768

insert into vespa_analysts.sky_base_2011_11_29 (account_number, cb_key_household, current_short_description,service_instance_id,SUBSCRIPTION_SUB_TYPE)
select account_number, cb_key_household, current_short_description,service_instance_id,SUBSCRIPTION_SUB_TYPE
from #sky_accounts
;
commit;


---Create src_system_id lookup

select src_system_id
,min(cast(si_external_identifier as integer)) as subscriberid
into #subs_details
from
sk_prod.CUST_SERVICE_INSTANCE as b
where si_service_instance_type in ('Primary DTV','Secondary DTV (extra digiboxes)')
group by src_system_id
;


commit;
exec sp_create_tmp_table_idx '#subs_details', 'src_system_id';

commit;


--alter table vespa_analysts.F1_analysis_20111104 delete subscription_type;
alter table vespa_analysts.sky_base_2011_11_29 add subscriber_id bigint;

update vespa_analysts.sky_base_2011_11_29
set subscriber_id=b.subscriberid
from vespa_analysts.sky_base_2011_11_29 as a
left outer join #subs_details as b
on a.service_instance_id=b.src_system_id
;
commit;
