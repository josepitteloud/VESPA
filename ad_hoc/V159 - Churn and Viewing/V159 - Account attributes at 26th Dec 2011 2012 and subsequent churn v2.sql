----V159----
---Churn and Viewing analsysis---

----Create List of Accounts active (AC) as at 26th dec 2011/2012 and details of Weighting (ie., on Viewing Panel) and subsequent churn attempts (PC/AB) Events---

select
                csh.account_number
                ,max( case when effective_from_dt<= '2011-12-25' and effective_to_dt>'2011-12-25' 
                then status_code else null end) as status_at_2011_12_26
                ,max( case when  effective_from_dt<= '2012-12-25' and effective_to_dt>'2012-12-25'
                then status_code else null end) as status_at_2012_12_26             
into            account_status_at_period_start
FROM            sk_prod.cust_subs_hist csh
WHERE          csh.subscription_sub_type = 'DTV Primary Viewing'
group by csh.account_number
;
Commit;

---Only include accounts AC at 1 or both time points for analysis

delete from account_status_at_period_start where status_at_2011_12_26<>'AC' and status_at_2012_12_26<>'AC';
commit;

create hg index idx1 on account_status_at_period_start(account_number);
commit;
--select status_at_2011_12_26 , status_at_2012_12_26,count(*) from account_status_at_period_start group by status_at_2011_12_26,status_at_2012_12_26

---Add on Account Weight (from Vespa) as at 26th Dec 2012----
--select top 100 * from vespa_analysts.SC2_weightings
--select scaling_day,sum(sum_of_weights) as weighted_base_value,sum(vespa_accounts) as total_vespa_accounts from vespa_analysts.SC2_weightings group by scaling_day order by scaling_day;
select a.account_number
,b.weighting
into #account_weight_2012_12_26
from  vespa_analysts.SC2_intervals as a
inner join vespa_analysts.SC2_weightings as b
on  cast('2012-12-26' as date) = b.scaling_day
and a.scaling_segment_ID = b.scaling_segment_ID
and cast('2012-12-26' as date) between a.reporting_starts and a.reporting_ends
;
create  hg index idx1 on #account_weight_2012_12_26(account_number);
commit;

alter table  account_status_at_period_start add vespa_weight_2012_12_26 double;

update account_status_at_period_start
set  vespa_weight_2012_12_26 =b.weighting
from account_status_at_period_start  as a
left outer join #account_weight_2012_12_26 as b
on a.account_number = b.account_number
;
commit;

----Add on Subsequent PC/AB Activity between 26th Dec and 28th Feb----
---Get all Events since 26/12/11
select
                csh.account_number
                ,csh.created_by_id
                ,csh.status_start_dt as pc_dt
                ,cal.x_subs_month_and_year
                ,cal.subs_week_and_year
into           #pc_events
FROM            sk_prod.cust_subs_hist csh
        inner join sk_prod.sky_calendar as cal
                on cal.calendar_date = csh.status_start_dt
WHERE           csh.status_start_dt between '2011-12-26' and '2013-02-28'
AND             csh.status_code_changed = 'Y'
AND             csh.status_code in ('PC')
AND             csh.prev_status_code in ('AC')
and             csh.subscription_sub_type = 'DTV Primary Viewing'
;
commit;

create  hg index idx1 on #pc_events(account_number);



--Excludes those who enter and exit AB status on same day---
select
                csh.account_number
                ,csh.created_by_id
                ,csh.status_start_dt as pc_dt
                ,cal.x_subs_month_and_year
                ,cal.subs_week_and_year
into           #AB_events
FROM            sk_prod.cust_subs_hist csh
        inner join sk_prod.sky_calendar as cal
                on cal.calendar_date = csh.status_start_dt
WHERE           csh.status_start_dt between '2011-12-26' and '2013-02-28'
AND             csh.status_code_changed = 'Y'
AND             csh.status_code in ('AB')
AND             csh.prev_status_code in ('AC','PC')
and             csh.subscription_sub_type = 'DTV Primary Viewing'
and effective_from_dt<effective_to_dt
;
commit;
create  hg index idx1 on #AB_events(account_number);
commit;

select account_number
,max(case when pc_dt between '2011-12-26' and '2012-02-28' then 1 else 0 end) as pc_2011_12
,max(case when pc_dt between '2012-12-26' and '2013-02-28' then 1 else 0 end) as pc_2012_13
into #pc_event_summary
from #pc_events
group by account_number
;commit;

create  hg index idx1 on #pc_event_summary(account_number);

select account_number
,max(case when pc_dt between '2011-12-26' and '2012-02-28' then 1 else 0 end) as ab_2011_12
,max(case when pc_dt between '2012-12-26' and '2013-02-28' then 1 else 0 end) as ab_2012_13
into #ab_event_summary
from #ab_events
group by account_number
;commit;

create  hg index idx1 on #ab_event_summary(account_number);



alter table  account_status_at_period_start add pc_events_2011_12 integer;
alter table  account_status_at_period_start add pc_events_2012_13 integer;
alter table  account_status_at_period_start add ab_events_2011_12 integer;
alter table  account_status_at_period_start add ab_events_2012_13 integer;



--select distinct pty_country_code from sk_prod.cust_single_account_view;

update account_status_at_period_start
set  pc_events_2011_12 =case when b.pc_2011_12=1 then 1 else 0 end
,pc_events_2012_13 =case when b.pc_2012_13=1 then 1 else 0 end
from account_status_at_period_start  as a
left outer join #pc_event_summary as b
on a.account_number = b.account_number
;
commit;

update account_status_at_period_start
set  ab_events_2011_12 =case when b.ab_2011_12=1 then 1 else 0 end
,ab_events_2012_13 =case when b.ab_2012_13=1 then 1 else 0 end
from account_status_at_period_start  as a
left outer join #ab_event_summary as b
on a.account_number = b.account_number
;
commit;

alter table  account_status_at_period_start add country_code varchar(3);
update account_status_at_period_start
set  country_code =b.pty_country_code
from account_status_at_period_start  as a
left outer join sk_prod.cust_single_account_view as b
on a.account_number = b.account_number
;
commit;

alter table  account_status_at_period_start add acct_type varchar(10);
update account_status_at_period_start
set  acct_type =b.acct_type
from account_status_at_period_start  as a
left outer join sk_prod.cust_single_account_view as b
on a.account_number = b.account_number
;
commit;

Delete from  account_status_at_period_start where acct_type<>'Standard'; commit;
Delete from  account_status_at_period_start where country_code not in ('GBR','IRL'); commit;
Delete from  account_status_at_period_start where country_code is null; commit;

alter table  account_status_at_period_start add activation_date date;
update account_status_at_period_start
set  activation_date =b.ph_subs_first_activation_dt
from account_status_at_period_start  as a
left outer join sk_prod.cust_single_account_view as b
on a.account_number = b.account_number
;
commit;
--select acct_type , count(*) from account_status_at_period_start group by acct_type
--select country_code , count(*) from account_status_at_period_start group by country_code


select country_code
,sum(case when status_at_2011_12_26='AC' then 1 else 0 end) as active_2011_12_26
,sum(case when status_at_2011_12_26='AC' and ( ab_events_2011_12+ pc_events_2011_12)>0 then 1 else 0 end) as active_2011_12_26_ab_pc_in_period
,sum(case when status_at_2012_12_26='AC' then 1 else 0 end) as active_2012_12_26
,sum(case when status_at_2012_12_26='AC' and ( ab_events_2012_13+ pc_events_2012_13)>0 then 1 else 0 end) as active_2012_12_26_ab_pc_in_period
,sum(case when status_at_2012_12_26='AC' and ( ab_events_2012_13)>0 then 1 else 0 end) as active_2012_12_26_ab_in_period
,sum(case when status_at_2012_12_26='AC' and vespa_weight_2012_12_26>0 then 1 else 0 end) as active_2012_12_26_on_vespa
,sum(case when status_at_2012_12_26='AC' and vespa_weight_2012_12_26>0 and ( ab_events_2012_13+ pc_events_2012_13)>0 then 1 else 0 end) as active_2012_12_26_ab_pc_in_period_on_vespa
,sum(case when status_at_2012_12_26='AC' and vespa_weight_2012_12_26>0 then vespa_weight_2012_12_26 else 0 end) as active_2012_12_26_on_vespa_ac
,sum(case when status_at_2012_12_26='AC' and vespa_weight_2012_12_26>0 and ( ab_events_2012_13+ pc_events_2012_13)>0 then vespa_weight_2012_12_26 else 0 end) as active_2012_12_26_ab_pc_in_period_on_vespa_ac
,sum(case when status_at_2012_12_26='AC' and vespa_weight_2012_12_26>0 and ( ab_events_2012_13)>0 then vespa_weight_2012_12_26 else 0 end) as active_2012_12_26_ab_in_period_on_vespa_ac

from account_status_at_period_start
group by country_code
;

---V2 Simplified
select country_code
,sum(case when status_at_2012_12_26='AC' then 1 else 0 end) as active_2012_12_26
,sum(case when status_at_2012_12_26='AC' and vespa_weight_2012_12_26>0 then vespa_weight_2012_12_26 else 0 end) as active_2012_12_26_on_vespa_weighted
,sum(case when status_at_2012_12_26='AC' and vespa_weight_2012_12_26>0 then 1 else 0 end) as active_2012_12_26_on_vespa
,sum(case when status_at_2011_12_26='AC' and ( ab_events_2011_12+ pc_events_2011_12)>0 then 1 else 0 end) as active_2011_12_26_ab_pc_in_period
,sum(case when status_at_2012_12_26='AC' and vespa_weight_2012_12_26>0 and ( ab_events_2012_13+ pc_events_2012_13)>0 then 1 else 0 end) as active_2012_12_26_ab_pc_in_period_on_vespa
,sum(case when status_at_2012_12_26='AC' and vespa_weight_2012_12_26>0 and ( ab_events_2012_13+ pc_events_2012_13)>0 then vespa_weight_2012_12_26 else 0 end) as active_2012_12_26_ab_pc_in_period_on_vespa_weighted

from account_status_at_period_start
group by country_code
;

commit;


----Split By Tenure
select case when cast(dateformat(activation_date,'DD') as integer)>26 then 
     datediff(mm,activation_date,cast('2012-12-26' as date))-1 else datediff(mm,activation_date,cast('2012-12-26' as date)) end as full_months_tenure
,sum(case when status_at_2012_12_26='AC' then 1 else 0 end) as active_2012_12_26
,sum(case when status_at_2012_12_26='AC' and vespa_weight_2012_12_26>0 then vespa_weight_2012_12_26 else 0 end) as active_2012_12_26_on_vespa_weighted
,sum(case when status_at_2012_12_26='AC' and vespa_weight_2012_12_26>0 then 1 else 0 end) as active_2012_12_26_on_vespa
,sum(case when status_at_2012_12_26='AC' and ( ab_events_2012_13+ pc_events_2012_13)>0 then 1 else 0 end) as active_2012_12_26_ab_pc_in_period
,sum(case when status_at_2012_12_26='AC' and vespa_weight_2012_12_26>0 and ( ab_events_2012_13+ pc_events_2012_13)>0 then 1 else 0 end) as active_2012_12_26_ab_pc_in_period_on_vespa
,sum(case when status_at_2012_12_26='AC' and vespa_weight_2012_12_26>0 and ( ab_events_2012_13+ pc_events_2012_13)>0 then vespa_weight_2012_12_26 else 0 end) as active_2012_12_26_ab_pc_in_period_on_vespa_weighted

from account_status_at_period_start
where country_code = 'GBR' and status_at_2012_12_26='AC' and activation_date<='2012-12-26'
group by full_months_tenure
order by full_months_tenure
;

---Initial Analysis just for accounts in tenure mths 10-16
--drop table v159_accounts_10_16mth_tenure;
select account_number
,case when cast(dateformat(activation_date,'DD') as integer)>26 then 
     datediff(mm,activation_date,cast('2012-12-26' as date))-1 else datediff(mm,activation_date,cast('2012-12-26' as date)) end as full_months_tenure
into v159_accounts_10_16mth_tenure
from account_status_at_period_start
where country_code = 'GBR' and status_at_2012_12_26='AC' and activation_date<='2012-12-26' 
and full_months_tenure between 10 and 16 and  vespa_weight_2012_12_26>0
;
commit;
--select count(*) from v159_accounts_10_16mth_tenure;
create hg index idx1 on v159_accounts_10_16mth_tenure(account_number);
commit;

------Creation of Viewing Table------


--------------------------------------------------------------------------------
--PART A: Viewing Data (For Customers active in the snapshot period)
--------------------------------------------------------------------------------

---Get details of Programmes Watched 3+ Minutes of---
CREATE VARIABLE @snapshot_start_dt              datetime;
CREATE VARIABLE @snapshot_end_dt                datetime;
CREATE VARIABLE @viewing_var_sql                varchar(5000);
CREATE VARIABLE @viewing_scanning_day           datetime;
--CREATE VARIABLE @viewing_var_num_days           smallint;


-- Date range of programmes to capture
SET @snapshot_start_dt  = '2012-09-26';
SET @snapshot_end_dt    = '2012-12-25';

/*
-- How many days (after end of broadcast period) to check for timeshifted viewing
SET @viewing_var_num_days = 29;
commit;
*/

IF object_ID ('V159_Tenure_10_16mth_Viewing') IS NOT NULL THEN
            DROP TABLE V159_Tenure_10_16mth_Viewing
END IF;

CREATE TABLE V159_Tenure_10_16mth_Viewing
    ( cb_row_ID                                         bigint       not null --primary key
            ,Account_Number                             varchar(20)  not null
            ,Subscriber_Id                              bigint
            ,programme_trans_sk                         bigint
            ,timeshifting                               varchar(15)
            ,viewing_starts                             datetime
            ,viewing_stops                              datetime
            ,viewing_Duration                           decimal(10,0)
            ,capped_flag                                tinyint
            ,capped_event_end_time                      datetime
            ,service_key                                int
            ,Channel_Name                               varchar(30)
            ,epg_title                                  varchar(50)
            ,duration                                   int
            ,Genre_Description                          varchar(30)
            ,Sub_Genre_Description                      varchar(30)
            ,epg_group_Name                             varchar(30)
            ,network_indicator                          varchar(50)
            ,tx_date_utc                                date
            ,x_broadcast_Time_Of_Day                    varchar(15)
            ,pay_free_indicator                         varchar(50)
)
;

--select top 100 * from vespa_analysts.VESPA_DAILY_AUGS_20121001
--select top 100 * from V159_Tenure_10_16mth_Viewing
-- Build string with placeholder for changing daily table reference
SET @viewing_var_sql = '
        insert into V159_Tenure_10_16mth_Viewing(
                cb_row_ID
                ,Account_Number
                ,Subscriber_Id
                ,programme_trans_sk
                ,timeshifting
                ,viewing_starts
                ,viewing_stops
                ,viewing_Duration
                ,capped_flag
               ,capped_event_end_time
)
        select
                a.cb_row_ID
                ,a.Account_Number
                ,a.Subscriber_Id
                ,a.programme_trans_sk
                ,a.timeshifting
                ,a.viewing_starts
                ,a.viewing_stops
                ,a.viewing_Duration
                ,a.capped_flag
                ,a.capped_event_end_time
from vespa_analysts.VESPA_DAILY_AUGS_##^^*^*## as a
left outer join  v159_accounts_10_16mth_tenure as b
on a.account_number = b.account_number
where b.account_number is not null
'
;
--select top 100 * from vespa_analysts.VESPA_DAILY_AUGS_20121104;

--select top 100 * from V159_Tenure_10_16mth_Viewing;
-- Filter for viewing events is applied on the daily augs table already.
-- Loop over the days in the period, extracting all the data.


SET @viewing_scanning_day = @snapshot_start_dt;

while @viewing_scanning_day <= @snapshot_end_dt
begin
    EXECUTE(replace(@viewing_var_sql,'##^^*^*##',dateformat(@viewing_scanning_day, 'yyyymmdd')))
    commit

    set @viewing_scanning_day = dateadd(day, 1, @viewing_scanning_day)
end

--select top 100 * from sk_prod.VESPA_STB_PROG_EVENTS_20121105; 
--select count(*) from V159_Tenure_10_16mth_Viewing
--select count(distinct(account_number)) from V159_Tenure_10_16mth_Viewing

create hg index idx2 on V159_Tenure_10_16mth_Viewing(programme_trans_sk);



--------------------------------------------------------------------------------------------------------------------------------------------------
-- PART B: Get programme data from sk_prod.VESPA_EPG_DIM
--------------------------------------------------------------------------------------------------------------------------------------------------

--Create Extra Variable to return EPG data for programmes broadcast pre start of analysis period
--Allow 3 mths pre analysis period
CREATE VARIABLE @playback_snapshot_start_dt              datetime;
SET @playback_snapshot_start_dt  = '2012-06-26';

--select count(*) from V159_Tenure_10_16mth_Viewing_detail
IF object_id('V159_Tenure_10_16mth_Viewing_detail') IS NOT NULL THEN
        DROP TABLE V159_Tenure_10_16mth_Viewing_detail
END IF;

--select top 100 * from sk_prod.Vespa_programme_schedule

SELECT      pk_programme_instance_dim as programme_trans_sk
            ,service_key
            ,Channel_Name
            ,programme_instance_name as epg_title
            ,programme_instance_duration as duration
            ,Genre_Description
            ,Sub_Genre_Description
            ,epg_group_Name
            ,network_indicator
            ,broadcast_start_date_time_utc as tx_date_utc
            ,broadcast_daypart as x_broadcast_Time_Of_Day
            ,pay_free_indicator
INTO  V159_Tenure_10_16mth_Viewing_detail
FROM sk_prod.Vespa_programme_schedule
WHERE (tx_date_utc between @playback_snapshot_start_dt  and  @snapshot_end_dt)
;


create hg index idx2 on V159_Tenure_10_16mth_Viewing_detail(programme_trans_sk);
--select top 500 * from V159_Tenure_10_16mth_Viewing_detail;
--------------------------------------------------------------------------------
--PART C: Append EPG channel Detail to the viewing data
--------------------------------------------------------------------------------

--select top 10 * from V159_Tenure_10_16mth_Viewing

--select top 100 * from V159_Tenure_10_16mth_Viewing_detail where programme_trans_sk = 100196282
--select top 100 * from sk_prod.Vespa_programme_schedule where dk_programme_instance_dim = 101578963

update V159_Tenure_10_16mth_Viewing
set v.service_key              = dt.service_key
     ,v.Channel_Name            = dt.Channel_Name
     ,v.epg_title               = dt.epg_title
     ,v.duration                = dt.duration
     ,v.Genre_Description       = dt.Genre_Description
     ,v.Sub_Genre_Description   = dt.Sub_Genre_Description
     ,v.epg_group_Name          = dt.epg_group_Name
     ,v.network_indicator       = dt.network_indicator
     ,v.tx_date_utc             = dt.tx_date_utc
     ,v.x_broadcast_Time_Of_Day = dt.x_broadcast_Time_Of_Day
     ,v.pay_free_indicator      = dt.pay_free_indicator
from V159_Tenure_10_16mth_Viewing as v
inner join V159_Tenure_10_16mth_Viewing_detail as dt
on v.programme_trans_sk = dt.programme_trans_sk
;
commit;
--select top 100 * from V159_Tenure_10_16mth_Viewing
--select count(*) from V159_Tenure_10_16mth_Viewing

--------------------------------------------------------------------------------
--PART D: Data manipulation and append
--------------------------------------------------------------------------------

  --select count(*) from V159_Tenure_10_16mth_Viewing
  --select top 100 * from V159_Tenure_10_16mth_Viewing
  --select lower(Epg_Title) as epg_title_lowercase, count(*) from V159_Tenure_10_16mth_Viewing group by epg_title_lowercase

  -- Add the following fields to the viewing table
  Alter table V159_Tenure_10_16mth_Viewing Add hd_channel       tinyint     default 0;
  Alter table V159_Tenure_10_16mth_Viewing Add Pay_channel      tinyint     default 0;
  Alter table V159_Tenure_10_16mth_Viewing Add viewing_category varchar(50);

  update V159_Tenure_10_16mth_Viewing
  set hd_channel = 1
  where upper(channel_name) like '%HD%'
  ;

  --select top 100 * from V159_Tenure_10_16mth_Viewing

  --select * from vespa_analysts.channel_name_and_techedge_channel
  --NOTE
  --dedupe from the vespa_analysts.channel_name_and_techedge_channel

  --drop table channel_name_and_techedge_channel
/*
  select distinct channel
         ,channel_name_grouped
         ,channel_name_inc_hd
  into channel_name_and_techedge_channel
  from vespa_analysts.channel_name_and_techedge_channel;
  ;
*/

--select top 100 *   from channel_name_and_techedge_channel;
  --drop table channel_table
/*
  select *
         ,rank() over (partition by channel order by channel_name_grouped, Channel_Name_Inc_Hd) as rank_id
   into channel_table
   from channel_name_and_techedge_channel
  ;

  --select * from channel_table order by channel;

  delete from channel_table where rank_id > 1;
*/
  --select count(channel) from #channel_table
  --select count(distinct(channel)) from channel_name_and_techedge_channel

alter table  V159_Tenure_10_16mth_Viewing Add channel_name_inc_hd       varchar(90);

update V159_Tenure_10_16mth_Viewing
set channel_name_inc_hd=b.channel_name_inc_hd
from V159_Tenure_10_16mth_Viewing as a
left outer join channel_table as b
on  upper(a.Channel_Name) = upper(b.Channel)
;
commit;

Update V159_Tenure_10_16mth_Viewing
set channel_name_inc_hd =  
        case    when channel_name ='Sky Sports 1 HD' then 'Sky Sports 1'
                when channel_name ='Playhouse Disney' then 'Disney Junior'
                when channel_name ='Sky Sports 2 HD' then 'Sky Sports 2'
                when channel_name ='ITV1 Tyne Tees' then 'ITV1'
                when channel_name ='Watch HD' then 'Watch'
                when channel_name ='Dave HD' then 'Dave'
                when channel_name ='Disney Chnl HD' then 'Disney Channel'
                when channel_name ='Sky Sports 3 HD' then 'Sky Sports 3'
                when channel_name ='Sky Sports 4 HD' then 'Sky Sports 4'
                when channel_name ='Sky 007 HD' then 'Sky Movies 007'
                when channel_name ='Sky Spts F1 HD' then 'Sky Sports F1'
                when channel_name ='MTV HD' then 'MTV'
                when channel_name ='alibi HD' then 'Alibi'
                when channel_name ='Cartoon Net HD' then 'Cartoon Network'
                when channel_name ='Star Plus HD' then 'Star Plus'
                when channel_name ='Sky Sports 2 HD' then 'Sky Sports 2'
                when channel_name ='Sky Sports 2 HD' then 'Sky Sports 2'
                when channel_name ='Sky Sports 2 HD' then 'Sky Sports 2'               
                when channel_name ='Eurosport 2 HD' then 'Eurosport 2'
                when channel_name ='AnimalPlnt HD' then 'Animal Planet' 
            when channel_name_inc_hd is not null then channel_name_inc_hd else channel_name end
;
commit;

   update V159_Tenure_10_16mth_Viewing 
    set Pay_channel = 1
    from V159_Tenure_10_16mth_Viewing  
    where Channel_Name_Inc_Hd in
          ('Animal Planet','Animal Plnt+1','Attheraces','BET','BET+1','Bio','Bio HD','Blighty','Bliss','Boomerang',
           'Boomerang +1','Bravo','Bravo Player','Bravo+1','Cartoon Network','Cartoonito','CBS Action','CBS Drama',
           'CBS Reality','CBS Reality +1','Challenge TV','Challenge TV+1','Channel AKA','Channel One','Channel One+1',
           'Chart Show TV','Clubland TV','CNTOO','Comedy Central','Comedy Central Extra','Comedy Central Extra+1',
           'ComedyCentral+1','Crime & Investigation','Crime & Investigation +1','Current TV','DanceNationTV','Dave ja vu',
           'Disc.Knowldge','Disc.knowledge +1','Disc.RealTime','Disc.RT+1','Disc.Sci+1','Disc.Science','Discovery',
           'Discovery Shed','Discovery Turbo','Discovery+1','Disney','Disney +1','Disney Cinemagic','Disney Cinemagic +1',
           'Disney Playhouse','Disney Playhouse+','Disney XD','Disney XD+1','Diva TV','Diva TV +1','DMAX','DMAX +1',
           'DMAX+2','E! Entertainment','Eden','Eden+1','ESPN','ESPN Classic','Euronews','Extreme Sports','Film24',
           'Flava','Food Network','Food Network+1','Fox FX','FX','FX+','Good Food','History','History +1','Home and Health',
           'Home&Health+','horror ch+1','horror channel','Horror Channel','Horse & Country TV','Kerrang','Kiss TV',
           'Kix!','Liverpool FCTV','LIVING','Living','LIVING +1','LIVING2','Living2+1','LIVINGit','LIVINGit+1','Magic',
           'MGM','Military History','Motors TV','Mov4Men2 +1','movies 24','Movies 24','Movies 24+1','Movies4Men +1',
           'Movies4Men.','Movies4Men2','MTV','MTV Base','MTV CLASSIC','MTV Dance','MTV Hits','MTV ROCKS','MTV SHOWS',
           'MTV+1','NatGeo + 1hr','NatGeo Wild','National Geo','Nick Jr','Nick Jr 2','Nick Toons','Nick Toonster',
           'Nickelodeon','Nickelodeon Replay','NME TV','N''Toons Replay','POP','Pop Girl','Pop Girl+1','Q Channel',
           'Quest','QUEST +1','Really','Scuzz NEW','Sky Arts 1','Sky Arts 2','Sky Movies Action & Adventure','Sky Movies Classics',
           'Sky Movies Comedy','Sky Movies Crime & Thriller','Sky Movies Drama & Romance','Sky Movies Family',
           'Sky Movies Indie','Sky Movies Modern','Sky Movies Modern Greats','Sky Movies Premiere','Sky Movies Premiere +1',
           'Sky Movies Sci-Fi / Horror','Sky Movies Sci-Fi/Horror','Sky Movies Showcase','Sky News','Sky News Enhanced',
           'Sky Sports 1','Sky Sports 2','Sky Sports 3','Sky Sports 4','Sky Sports News','Sky Spts News','Sky Thriller',
           'Sky Thriller HD','Sky1','Sky2','Sky3','Sky3+1','Smash Hits','Sunrise TV','Syfy','Syfy +1','TCM_UK','TCM2',
           'The Box','The Music Factory','The Vault','Tiny Pop','Tiny Pop +1','Travel','Travel & Living','Travel Ch +1',
           'True Ent','True Movies','True Movies 2','UKTV Alibi','UKTV Alibi+1','UKTV Dave','UKTV Dave+1','UKTV Food',
           'UKTV Food+1','UKTV G.O.L.D','UKTV G.O.L.D +1','UKTV GOLD','UKTV Style','UKTV Style Plus','Universal',
           'Universal +1','VH1','VH1 Classic','Vintage TV','Virgin 1','Virgin 1 +1','VIVA','Watch','Watch +1','Wedding TV',
           'wedding tv','Nat Geo','MTV Music','Sky Movies Mdn Greats','GOLD  (TV)','Sky DramaRom','Good Food +1',
           'Sky Living +1','Discovery +1hr','Premier Sports','Discovery RealTime +1','Nat Geo+1hr','Nick Replay',
           'Football First 4','Challenge','Football First 6','MUTV','Showcase','ESPN America','Chelsea TV','Alibi',
           'YeSTERDAY +1','Sky Movies Thriller','Eden +1','CineMoi Movies','Sky 1','Sky Living Loves','5* +1','Challenge +1',
           'Home+1','Home & Health +1','HD Retail Info','Home & Health','FX +','Disc. Shed','Discovery RealTime',
           'Sky Premiere','Sky Prem+1','Football First 7','Disney XD +1','Playhouse Disney','YeSTERDAY','Nat Geo Wild',
           'DMax','Home','HD MTV','Sky Movies Action','SBO','MGM HD','Animal Planet +1','Sky Box Office','TCM 2',
           'Sky Livingit +1','Dave','At The Races','History +1 hour','Sky 3D','horror channel +1','TCM','Anytime',
           'Comedy Central Extra +1','PopGirl+1','Smash Hits!','Nicktoons TV','Comedy Central +1','5*','Football First 2',
           'Alibi +1','MTV BASE','Sky Atlantic','Sky 2','MTV HITS','Disc. History','Disc. History+1','Sky Livingit',
           'Football First 3','Racing UK','DMax +2','MTV DANCE','Disc.Science +1','DMax +1','GOLD +1','Sky Living',
           'Ideal & More','CNToo','Disney Junior','Disney Junior+','Christmas 24','Christmas 24+','Sky Sports F1','Football First 1'
,'Football First 2'
,'Football First 3'
,'Football First 4'
,'Football First 5'
,'Football First 6')
    ;
commit;

--select channel_name_inc_hd,sum(Pay_channel),sum(


--select max(viewing_starts) from V159_Tenure_10_16mth_Viewing;
--select top 500 * from V159_Tenure_10_16mth_Viewing where genre_description is null;

select genre_description
,sub_genre_description
,sum(viewing_duration) as tot_dur
from V159_Tenure_10_16mth_Viewing
group by genre_description
,sub_genre_description
order by tot_dur desc
;

commit;

select channel_name_inc_hd
,sum(viewing_duration) as tot_dur
,sum(Pay_channel) as pay
from V159_Tenure_10_16mth_Viewing
group by channel_name_inc_hd
order by tot_dur desc
;

---Create One Record per account----
--drop table V159_Tenure_10_16mth_Viewing_summary;

select account_number
,count(distinct cast(viewing_starts as date)) as distinct_days_viewing
,sum(viewing_duration) as total_viewing_duration
,sum(case when channel_name_inc_hd in (
'ITV1',
'BBC ONE',
'Channel 4',
'BBC TWO',
'Channel 5') then viewing_duration else 0 end) as Terrestrial_Channels
,sum(case when channel_name_inc_hd not in (
'ITV1',
'BBC ONE',
'Channel 4',
'BBC TWO',
'Channel 5') and Pay_channel=0 then viewing_duration else 0 end) as Non_Terrestrial_Free_Channels
,sum(case when channel_name_inc_hd not in (

'Football First 1'
,'Football First 2'
,'Football First 3'
,'Football First 4'
,'Football First 5'
,'Football First 6'
,'Sky Christmas'
,'Sky ChristmsHD'
,'Sky DramaRom'
,'Sky Movies 007'
,'Sky Movies Action'
,'Sky Movies Classics'
,'Sky Movies Comedy'
,'Sky Movies Family'
,'Sky Movies Indie'
,'Sky Movies Mdn Greats'
,'Sky Movies Sci-Fi/Horror'
,'Sky Movies Showcase'
,'Sky Movies Thriller'
,'Sky Prem+1'
,'Sky Premiere'
,'Sky ShowcseHD'
,'Sky Sports 1'
,'Sky Sports 2'
,'Sky Sports 3'
,'Sky Sports 4'
,'Sky Sports F1'
,'Christmas 24'
,'Christmas 24+') and Pay_channel=1 then viewing_duration else 0 end) as Pay_Channels_Exc_Sky_Sports_Movies
,sum(case when channel_name_inc_hd  in (

'Football First 1'
,'Football First 2'
,'Football First 3'
,'Football First 4'
,'Football First 5'
,'Football First 6'
,'Sky Christmas'
,'Sky ChristmsHD'
,'Sky DramaRom'
,'Sky Movies 007'
,'Sky Movies Action'
,'Sky Movies Classics'
,'Sky Movies Comedy'
,'Sky Movies Family'
,'Sky Movies Indie'
,'Sky Movies Mdn Greats'
,'Sky Movies Sci-Fi/Horror'
,'Sky Movies Showcase'
,'Sky Movies Thriller'
,'Sky Prem+1'
,'Sky Premiere'
,'Sky ShowcseHD'
,'Sky Sports 1'
,'Sky Sports 2'
,'Sky Sports 3'
,'Sky Sports 4'
,'Sky Sports F1'
,'Christmas 24'
,'Christmas 24+') then viewing_duration else 0 end) as All_Sky_Sports_Movies_Channels
into V159_Tenure_10_16mth_Viewing_summary
from V159_Tenure_10_16mth_Viewing
group by account_number
;
commit;
create hg index idx2 on V159_Tenure_10_16mth_Viewing_summary(account_number);
--select top 500 * from V159_Tenure_10_16mth_Viewing_summary;

select distinct_days_viewing
,count(*) as accounts
from V159_Tenure_10_16mth_Viewing_summary
group by distinct_days_viewing
order by distinct_days_viewing
;

commit;

---Select Split values for each variable---

select round(terrestrial_channels/3600,0) as hours_terrestrial
,count(*) as accounts
from  V159_Tenure_10_16mth_Viewing_summary
group by hours_terrestrial
order by hours_terrestrial
;

select round(Non_Terrestrial_Free_Channels/3600,0) as hours_non_terrestrial_free
,count(*) as accounts
from  V159_Tenure_10_16mth_Viewing_summary
group by hours_non_terrestrial_free
order by hours_non_terrestrial_free
;

select round(Pay_Channels_Exc_Sky_Sports_Movies/3600,0) as pay_exp_sky_sports_movies
,count(*) as accounts
from  V159_Tenure_10_16mth_Viewing_summary
group by pay_exp_sky_sports_movies
order by pay_exp_sky_sports_movies
;

select round(All_Sky_Sports_Movies_Channels/3600,0) as sky_sports_movies
,count(*) as accounts
from  V159_Tenure_10_16mth_Viewing_summary
group by sky_sports_movies
order by sky_sports_movies
;

commit;

select round(total_viewing_duration/3600,0) as total_viewing_hours
,count(*) as accounts
from  V159_Tenure_10_16mth_Viewing_summary
group by total_viewing_hours
order by total_viewing_hours
;


--select top 100 * from V159_Tenure_10_16mth_Viewing;
--------Create Profile Info for accounts as at Start of 26/12/11 and 26/12/12------

---Create Table of Account Attributes as at 26th Dec 2011-----


select account_number
into #v159_accounts_for_profiling_dec2011_active
from account_status_at_period_start
where  country_code = 'GBR' and status_at_2011_12_26='AC' and activation_date<='2011-12-26'
group by account_number
;
select * into v159_accounts_for_profiling_dec2011_active from #v159_accounts_for_profiling_dec2011_active;
commit;
create  hg index idx1 on v159_accounts_for_profiling_dec2011_active (account_number);
commit;

--Create Package Details for actual date of analysis (14th Nov 2012)


SELECT csh.account_number
      ,csh.cb_key_household
      ,csh.first_activation_dt
      ,CASE WHEN  cel.mixes = 0                     THEN 'A) 0 Mixes'
            WHEN  cel.mixes = 1
             AND (style_culture = 1 OR variety = 1) THEN 'B) 1 Mix - Variety or Style&Culture'
            WHEN  cel.mixes = 1                     THEN 'C) 1 Mix - Other'
            WHEN  cel.mixes = 2
             AND  style_culture = 1
             AND  variety = 1                       THEN 'D) 2 Mixes - Variety and Style&Culture'
            WHEN  cel.mixes = 2
             AND (style_culture = 0 OR variety = 0) THEN 'E) 2 Mixes - Other Combination'
            WHEN  cel.mixes = 3                     THEN 'F) 3 Mixes'
            WHEN  cel.mixes = 4                     THEN 'G) 4 Mixes'
            WHEN  cel.mixes = 5                     THEN 'H) 5 Mixes'
            WHEN  cel.mixes = 6                     THEN 'I) 6 Mixes'
            ELSE                                         'J) Unknown'
        END as mix_type
       ,CAST(NULL AS VARCHAR(20)) AS new_package
       ,cel.prem_sports
        ,cel.prem_movies
        
  INTO #mixes
  FROM sk_prod.cust_subs_hist as csh
       INNER JOIN sk_prod.cust_entitlement_lookup as cel
               ON csh.current_short_description = cel.short_description
 WHERE csh.subscription_sub_type ='DTV Primary Viewing'
   AND csh.subscription_type = 'DTV PACKAGE'
   AND csh.status_code in ('AC','AB','PC')
   AND csh.effective_from_dt <= '2011-12-25'
   AND csh.effective_to_dt   >  '2011-12-25'
   AND csh.effective_from_dt != csh.effective_to_dt
;

UPDATE #mixes
   Set new_package = CASE WHEN mix_type IN ( 'A) 0 Mixes'
                                            ,'B) 1 Mix - Variety or Style&Culture'
                                            ,'D) 2 Mixes - Variety and Style&Culture')
                          THEN 'Entertainment'

                          WHEN mix_type IN ( 'C) 1 Mix - Other'
                                            ,'E) 2 Mixes - Other Combination'
                                            ,'F) 3 Mixes'
                                            ,'G) 4 Mixes'
                                            ,'H) 5 Mixes'
                                            ,'I) 6 Mixes')
                          THEN  'Entertainment Extra'
                          ELSE  'Unknown'
                     END;

commit;

exec sp_create_tmp_table_idx '#mixes', 'account_number';

--select top 500 * from sk_prod.cust_entitlement_lookup;
alter table v159_accounts_for_profiling_dec2011_active add prem_sports integer default 0;
alter table v159_accounts_for_profiling_dec2011_active add prem_movies integer default 0;
alter table v159_accounts_for_profiling_dec2011_active add mixes_type varchar(30) default 'Unknown';

update v159_accounts_for_profiling_dec2011_active 
set prem_sports=b.prem_sports
,prem_movies=b.prem_movies
,mixes_type=b.new_package
from v159_accounts_for_profiling_dec2011_active  as a
left outer join #mixes as b
on a.account_number=b.account_number
;
commit;


select account_number
into #accounts_with_3d  
FROM sk_prod.cust_subs_hist as csh
      
 WHERE subscription_type = 'A-LA-CARTE' and subscription_sub_type = '3DTV'
   AND csh.status_code in ('AC','AB','PC')
   AND csh.effective_from_dt <= '2011-12-25'
   AND csh.effective_to_dt   >  '2011-12-25'
group by account_number
;

exec sp_create_tmp_table_idx '#accounts_with_3d', 'account_number';

alter table v159_accounts_for_profiling_dec2011_active add subscription_3d integer default 0;

update v159_accounts_for_profiling_dec2011_active
set subscription_3d=case when b.account_number is not null then 1 else 0 end
from v159_accounts_for_profiling_dec2011_active as a
left outer join #accounts_with_3d  as b
on a.account_number = b.account_number
;

-------------------------------------------------  02 - Active MR AND HD Subscription
--code_location_08
SELECT  csh.account_number
           ,MAX(CASE  WHEN csh.subscription_sub_type ='DTV Extra Subscription'
                       AND csh.status_code in  ('AC','AB','PC') THEN 1 ELSE 0 END) AS multiroom
           ,MAX(CASE  WHEN csh.subscription_sub_type ='DTV HD'
                       AND csh.status_code in  ('AC','AB','PC') THEN 1 ELSE 0 END) AS hdtv
           ,MAX(CASE  WHEN csh.subscription_sub_type ='DTV Sky+'
                       AND csh.status_code in  ('AC','AB','PC') THEN 1 ELSE 0 END) AS skyplus
INTO v159_MR_HD
      FROM sk_prod.cust_subs_hist AS csh 
     WHERE csh.subscription_sub_type  IN ('DTV Extra Subscription'
                                         ,'DTV HD'
                                         ,'DTV Sky+')
       AND csh.effective_from_dt <> csh.effective_to_dt
       AND csh.effective_from_dt <= '2011-12-25'
       AND csh.effective_to_dt    >  '2011-12-25'
GROUP BY csh.account_number;
commit;

commit;
create  hg index idx1 on v159_MR_HD (account_number);
alter table v159_accounts_for_profiling_dec2011_active add hdtv                    tinyint          default 0    ;     
alter table v159_accounts_for_profiling_dec2011_active add multiroom                    tinyint          default 0    ;     
alter table v159_accounts_for_profiling_dec2011_active add skyplus                    tinyint          default 0    ;     
commit;


update v159_accounts_for_profiling_dec2011_active
set hdtv=case when b.hdtv is null then 0 else b.hdtv end
,multiroom=case when b.multiroom is null then 0 else b.multiroom end
,skyplus=case when b.skyplus is null then 0 else b.skyplus end
from v159_accounts_for_profiling_dec2011_active as a
left outer join v159_MR_HD as b
on a.account_number=b.account_number
;
commit;
drop table v159_MR_HD;
commit;

--select top 100 * from v141_live_playback_viewing;

----Add on extra variables from product holdings and consumerview---

alter table v159_accounts_for_profiling_dec2011_active add talk_product              VARCHAR(50)     default 'NA' ;        -- Current Sky Talk product
--alter table v159_accounts_for_profiling_dec2011_active add sky_id                    bigint          default 0    ;        -- Sky id created
alter table v159_accounts_for_profiling_dec2011_active add distinct_usage_days                INTEGER         default 0     ;       -- Sky Go days in 3mth period
alter table v159_accounts_for_profiling_dec2011_active add usage_records                INTEGER         default 0     ;       -- Sky Go usage records in 3mth period
alter table v159_accounts_for_profiling_dec2011_active add BB_type                   VARCHAR(50)     default 'NA'  ;       -- Current BB product
alter table v159_accounts_for_profiling_dec2011_active add Anytime_plus              INTEGER         default 0    ;        -- Anytime+ activated
alter table v159_accounts_for_profiling_dec2011_active add isba_tv_region             VARCHAR(50)     default 'Unknown'         ;   
alter table v159_accounts_for_profiling_dec2011_active add cb_key_household           bigint   ;        -- Current Sky Talk product
--drop table nodupes;
commit;

update v159_accounts_for_profiling_dec2011_active
set isba_tv_region=b.isba_tv_region
,cb_key_household=b.cb_key_household
from v159_accounts_for_profiling_dec2011_active as a
left outer join sk_prod.cust_single_account_view as b
on a.account_number=b.account_number
;
commit;
--select top 100 * from v159_accounts_for_profiling_dec2011_active;
-------------------------------------------------  02 - Active Sky Talk
--code_location_09
--drop table talk;
--commit;

SELECT DISTINCT base.account_number
       ,CASE WHEN UCASE(current_product_description) LIKE '%UNLIMITED%'
             THEN 'Unlimited'
             ELSE 'Freetime'
          END as talk_product
      ,rank() over(PARTITION BY base.account_number ORDER BY effective_to_dt desc) AS rank_id
      ,effective_to_dt
         INTO talk
FROM sk_prod.cust_subs_hist AS CSH
    inner join AdSmart AS Base
    ON csh.account_number = base.account_number
WHERE subscription_sub_type = 'SKY TALK SELECT'
     AND(     status_code = 'A'
          OR (status_code = 'FBP' AND prev_status_code IN ('PC','A'))
          OR (status_code = 'RI'  AND prev_status_code IN ('FBP','A'))
          OR (status_code = 'PC'  AND prev_status_code = 'A'))
     AND effective_to_dt != effective_from_dt
     AND csh.effective_from_dt <= '2011-12-25'
     AND csh.effective_to_dt > '2011-12-25'
GROUP BY base.account_number, talk_product,effective_to_dt;
commit;

DELETE FROM talk where rank_id >1;
commit;


--      create index on talk
CREATE   HG INDEX idx09 ON talk(account_number);
commit;

--      update AdSmart file
UPDATE v159_accounts_for_profiling_dec2011_active
SET  talk_product = talk.talk_product
FROM v159_accounts_for_profiling_dec2011_active  AS Base
  INNER JOIN talk AS talk
        ON base.account_number = talk.account_number
ORDER BY base.account_number;
commit;

DROP TABLE talk;
commit;


-------------------------------------------------  02 - Sky Go and Downloads
--code_location_06
/*SELECT base.account_number
       ,count(distinct base.account_number) AS Sky_Go_Reg
INTO Sky_Go
FROM   sk_prod.SKY_PLAYER_REGISTRANT  AS Sky_Go
        inner join AdSmart as Base
         on Sky_Go.account_number = Base.account_number
GROUP BY base.account_number;
*/
select account_number
        ,count(distinct cb_data_date) as distinct_usage_days
        ,count(*) as usage_records
--        ,sum(SKY_GO_USAGE)
into skygo_usage
from sk_prod.SKY_PLAYER_USAGE_DETAIL AS usage
--        inner join v159_accounts_for_profiling_dec2011_active AS Base
--         ON usage.account_number = Base.account_number
where cb_data_date >= '2011-09-26'
        AND cb_data_date <'2011-12-25'
group by account_number;
commit;

--      create index on Sky_Go file
CREATE   HG INDEX idx06 ON skygo_usage(account_number);
commit;

--      update AdSmart file
UPDATE v159_accounts_for_profiling_dec2011_active
SET distinct_usage_days = sky_go.distinct_usage_days
,usage_records=sky_go.usage_records
FROM v159_accounts_for_profiling_dec2011_active  AS Base
       INNER JOIN skygo_usage AS sky_go
        ON base.account_number = sky_go.account_number
ORDER BY base.account_number;
commit;

DROP TABLE skygo_usage;
commit;



-------------------------------------------------  02 - Active BB Type
--code_location_10
--drop table bb;
--commit;

Select distinct base.account_number
           ,CASE WHEN current_product_sk=43373 THEN '1) Unlimited (New)'
                 WHEN current_product_sk=42128 THEN '2) Unlimited (Old)'
                 WHEN current_product_sk=42129 THEN '3) Everyday'
                 WHEN current_product_sk=42130 THEN '4) Everyday Lite'
                 WHEN current_product_sk=42131 THEN '5) Connect'
                 ELSE 'NA'
                 END AS BB_type
               ,rank() over(PARTITION BY base.account_number ORDER BY effective_to_dt desc) AS rank_id
               ,effective_to_dt
        ,count(*) AS total
INTO bb
FROM sk_prod.cust_subs_hist AS CSH
    inner join v159_accounts_for_profiling_dec2011_active AS Base
    ON csh.account_number = base.account_number
WHERE subscription_sub_type = 'Broadband DSL Line'
   AND csh.effective_from_dt <= '2011-12-25'
   AND csh.effective_to_dt > '2011-12-25'
      AND effective_from_dt != effective_to_dt
      AND (status_code IN ('AC','AB') OR (status_code='PC' AND prev_status_code NOT IN ('?','RQ','AP','UB','BE','PA') )
            OR (status_code='CF' AND prev_status_code='PC')
            OR (status_code='AP' AND sale_type='SNS Bulk Migration'))
GROUP BY base.account_number, bb_type, effective_to_dt;
commit;

--select top 10 * from bb

DELETE FROM bb where rank_id >1;
commit;

--drop table bbb;
--commit;

select distinct account_number, BB_type
               ,rank() over(PARTITION BY account_number ORDER BY BB_type desc) AS rank_id
into bbb
from bb;
commit;

DELETE FROM bbb where rank_id >1;
commit;

--      create index on BB
CREATE   HG INDEX idx10 ON BB(account_number);
commit;

--      update v159_accounts_for_profiling_dec2011_active file
UPDATE v159_accounts_for_profiling_dec2011_active
SET  BB_type = BB.BB_type
FROM v159_accounts_for_profiling_dec2011_active  AS Base
  INNER JOIN BB AS BB
        ON base.account_number = BB.account_number
            ORDER BY base.account_number;
commit;


drop table bb; commit;
DROP TABLE BBB; commit;


-------------------------------------------------  02 - Anytime + activated
--code_location_05     code changed in line with changes to Wiki
/*SELECT base.account_number
       ,1 AS Anytime_plus
INTO Anytime_plus
FROM   sk_prod.CUST_SUBS_HIST AS Aplus
        inner join AdSmart as Base
         on Aplus.account_number = Base.account_number
WHERE  subscription_sub_type = 'PDL subscriptions'
AND    status_code = 'AC'
AND    Aplus.effective_from_dt >= @today
AND    Aplus.effective_to_dt > @today
GROUP BY base.account_number;
*/


SELECT base.account_number
       ,1 AS Anytime_plus
INTO Anytime_plus
FROM   sk_prod.CUST_SUBS_HIST AS Aplus
        inner join v159_accounts_for_profiling_dec2011_active as Base
         on Aplus.account_number = Base.account_number
WHERE  subscription_sub_type = 'PDL subscriptions'
AND        status_code='AC'
AND        first_activation_dt<'2011-12-25'              -- (END)
AND        first_activation_dt>='2010-01-01'        -- (START) Oct 2010 was the soft launch of A+, no one should have it before then
AND        base.account_number is not null
AND        base.account_number <> '?'
GROUP BY   base.account_number;
commit;


--      create index on Anytime_plus file
CREATE   HG INDEX idx05 ON Anytime_plus(account_number);
commit;

--      update AdSmart file
UPDATE v159_accounts_for_profiling_dec2011_active
SET Anytime_plus = Aplus.Anytime_plus
FROM v159_accounts_for_profiling_dec2011_active  AS Base
       INNER JOIN Anytime_plus AS Aplus
        ON base.account_number = APlus.account_number
ORDER BY base.account_number;
commit;

DROP TABLE Anytime_plus;
commit;

---Anytime Plus Used---
--select top 100 * from sk_prod.CUST_ANYTIME_PLUS_DOWNLOADS;

SELECT  account_number
,count(distinct cast(last_modified_dt as date)) as unique_dates_with_anytime_plus_downloads
,count(*) as total_anytime_plus_download_records
into anytime_plus_downloads
--into v141_anytime_plus_users
FROM   sk_prod.CUST_ANYTIME_PLUS_DOWNLOADS
WHERE  last_modified_dt BETWEEN '2011-09-26' and '2011-12-25'
AND    x_content_type_desc = 'PROGRAMME'  --  to exclude trailers
AND    download_count=1    -- to exclude any spurious header/trailer download records
group by account_number
;
commit;


CREATE   HG INDEX idx01 ON anytime_plus_downloads(account_number);
commit;

alter table v159_accounts_for_profiling_dec2011_active add unique_dates_with_anytime_plus_downloads tinyint default 0;
alter table v159_accounts_for_profiling_dec2011_active add total_anytime_plus_download_records tinyint default 0;
update  v159_accounts_for_profiling_dec2011_active
set unique_dates_with_anytime_plus_downloads = b.unique_dates_with_anytime_plus_downloads
,total_anytime_plus_download_records=b.total_anytime_plus_download_records
from v159_accounts_for_profiling_dec2011_active as a
left outer join anytime_plus_downloads as b
on a.account_number=b.account_number
;
commit;

DROP TABLE anytime_plus_downloads;
commit;

--select unique_dates_with_anytime_plus_downloads , count(*) from v159_accounts_for_profiling_dec2011_active  group by unique_dates_with_anytime_plus_downloads;

--select top 500 * from v159_accounts_for_profiling_dec2011_active;

----Update Nulls to 0---

update v159_accounts_for_profiling_dec2011_active
set hdtv=case when hdtv is null then 0 else hdtv end
,multiroom=case when multiroom is null then 0 else multiroom end
,skyplus=case when skyplus is null then 0 else skyplus end
,unique_dates_with_anytime_plus_downloads=case when unique_dates_with_anytime_plus_downloads is null then 0 else unique_dates_with_anytime_plus_downloads end
,total_anytime_plus_download_records=case when total_anytime_plus_download_records is null then 0 else total_anytime_plus_download_records end
from v159_accounts_for_profiling_dec2011_active
;
commit;


--select sum(hdtv) from v159_accounts_for_profiling_dec2011_active





---Create Table of Account Attributes as at 26th Dec 2012 and 26th 2012-----


select account_number
into #v159_accounts_for_profiling_dec2012_active
from account_status_at_period_start
where  country_code = 'GBR' and status_at_2012_12_26='AC' and activation_date<='2012-12-26'
group by account_number
;
select * into v159_accounts_for_profiling_dec2012_active from #v159_accounts_for_profiling_dec2012_active;
commit;
create  hg index idx1 on v159_accounts_for_profiling_dec2012_active (account_number);
commit;

--Create Package Details for actual date of analysis (14th Nov 2012)


SELECT csh.account_number
      ,csh.cb_key_household
      ,csh.first_activation_dt
      ,CASE WHEN  cel.mixes = 0                     THEN 'A) 0 Mixes'
            WHEN  cel.mixes = 1
             AND (style_culture = 1 OR variety = 1) THEN 'B) 1 Mix - Variety or Style&Culture'
            WHEN  cel.mixes = 1                     THEN 'C) 1 Mix - Other'
            WHEN  cel.mixes = 2
             AND  style_culture = 1
             AND  variety = 1                       THEN 'D) 2 Mixes - Variety and Style&Culture'
            WHEN  cel.mixes = 2
             AND (style_culture = 0 OR variety = 0) THEN 'E) 2 Mixes - Other Combination'
            WHEN  cel.mixes = 3                     THEN 'F) 3 Mixes'
            WHEN  cel.mixes = 4                     THEN 'G) 4 Mixes'
            WHEN  cel.mixes = 5                     THEN 'H) 5 Mixes'
            WHEN  cel.mixes = 6                     THEN 'I) 6 Mixes'
            ELSE                                         'J) Unknown'
        END as mix_type
       ,CAST(NULL AS VARCHAR(20)) AS new_package
       ,cel.prem_sports
        ,cel.prem_movies
        
  INTO #mixes
  FROM sk_prod.cust_subs_hist as csh
       INNER JOIN sk_prod.cust_entitlement_lookup as cel
               ON csh.current_short_description = cel.short_description
 WHERE csh.subscription_sub_type ='DTV Primary Viewing'
   AND csh.subscription_type = 'DTV PACKAGE'
   AND csh.status_code in ('AC','AB','PC')
   AND csh.effective_from_dt <= '2012-12-25'
   AND csh.effective_to_dt   >  '2012-12-25'
   AND csh.effective_from_dt != csh.effective_to_dt
;

UPDATE #mixes
   Set new_package = CASE WHEN mix_type IN ( 'A) 0 Mixes'
                                            ,'B) 1 Mix - Variety or Style&Culture'
                                            ,'D) 2 Mixes - Variety and Style&Culture')
                          THEN 'Entertainment'

                          WHEN mix_type IN ( 'C) 1 Mix - Other'
                                            ,'E) 2 Mixes - Other Combination'
                                            ,'F) 3 Mixes'
                                            ,'G) 4 Mixes'
                                            ,'H) 5 Mixes'
                                            ,'I) 6 Mixes')
                          THEN  'Entertainment Extra'
                          ELSE  'Unknown'
                     END;

commit;

exec sp_create_tmp_table_idx '#mixes', 'account_number';

--select top 500 * from sk_prod.cust_entitlement_lookup;
alter table v159_accounts_for_profiling_dec2012_active add prem_sports integer default 0;
alter table v159_accounts_for_profiling_dec2012_active add prem_movies integer default 0;
alter table v159_accounts_for_profiling_dec2012_active add mixes_type varchar(30) default 'Unknown';

update v159_accounts_for_profiling_dec2012_active 
set prem_sports=b.prem_sports
,prem_movies=b.prem_movies
,mixes_type=b.new_package
from v159_accounts_for_profiling_dec2012_active  as a
left outer join #mixes as b
on a.account_number=b.account_number
;
commit;


select account_number
into #accounts_with_3d  
FROM sk_prod.cust_subs_hist as csh
      
 WHERE subscription_type = 'A-LA-CARTE' and subscription_sub_type = '3DTV'
   AND csh.status_code in ('AC','AB','PC')
   AND csh.effective_from_dt <= '2012-12-25'
   AND csh.effective_to_dt   >  '2012-12-25'
group by account_number
;

exec sp_create_tmp_table_idx '#accounts_with_3d', 'account_number';

alter table v159_accounts_for_profiling_dec2012_active add subscription_3d integer default 0;

update v159_accounts_for_profiling_dec2012_active
set subscription_3d=case when b.account_number is not null then 1 else 0 end
from v159_accounts_for_profiling_dec2012_active as a
left outer join #accounts_with_3d  as b
on a.account_number = b.account_number
;

-------------------------------------------------  02 - Active MR AND HD Subscription
--code_location_08
SELECT  csh.account_number
           ,MAX(CASE  WHEN csh.subscription_sub_type ='DTV Extra Subscription'
                       AND csh.status_code in  ('AC','AB','PC') THEN 1 ELSE 0 END) AS multiroom
           ,MAX(CASE  WHEN csh.subscription_sub_type ='DTV HD'
                       AND csh.status_code in  ('AC','AB','PC') THEN 1 ELSE 0 END) AS hdtv
           ,MAX(CASE  WHEN csh.subscription_sub_type ='DTV Sky+'
                       AND csh.status_code in  ('AC','AB','PC') THEN 1 ELSE 0 END) AS skyplus
INTO v159_MR_HD
      FROM sk_prod.cust_subs_hist AS csh 
     WHERE csh.subscription_sub_type  IN ('DTV Extra Subscription'
                                         ,'DTV HD'
                                         ,'DTV Sky+')
       AND csh.effective_from_dt <> csh.effective_to_dt
       AND csh.effective_from_dt <= '2012-12-25'
       AND csh.effective_to_dt    >  '2012-12-25'
GROUP BY csh.account_number;
commit;

commit;
create  hg index idx1 on v159_MR_HD (account_number);
alter table v159_accounts_for_profiling_dec2012_active add hdtv                    tinyint          default 0    ;     
alter table v159_accounts_for_profiling_dec2012_active add multiroom                    tinyint          default 0    ;     
alter table v159_accounts_for_profiling_dec2012_active add skyplus                    tinyint          default 0    ;     
commit;


update v159_accounts_for_profiling_dec2012_active
set hdtv=b.hdtv
,multiroom=b.multiroom
,skyplus=b.skyplus
from v159_accounts_for_profiling_dec2012_active as a
left outer join v159_MR_HD as b
on a.account_number=b.account_number
;
commit;
drop table v159_MR_HD;
commit;

---HD programme viewing---
select account_number
,max(hd_channel) as watched_hd_channel
into #hd_viewing
from v141_live_playback_viewing
where overall_project_weighting>0
group by account_number
;
commit;
create  hg index idx1 on #hd_viewing (account_number);
--alter table v159_accounts_for_profiling_dec2012_active delete HD_Viewing
alter table v159_accounts_for_profiling_dec2012_active add HD_Viewing                    tinyint          default 0    ;  
update v159_accounts_for_profiling_dec2012_active
set HD_Viewing=case when b.watched_hd_channel=1 then 1 else 0 end
from v159_accounts_for_profiling_dec2012_active as a
left outer join #hd_viewing as b
on a.account_number=b.account_number
;
commit;
--select top 100 * from v141_live_playback_viewing;

----Add on extra variables from product holdings and consumerview---

alter table v159_accounts_for_profiling_dec2012_active add talk_product              VARCHAR(50)     default 'NA' ;        -- Current Sky Talk product
--alter table v159_accounts_for_profiling_dec2012_active add sky_id                    bigint          default 0    ;        -- Sky id created
alter table v159_accounts_for_profiling_dec2012_active add distinct_usage_days                INTEGER         default 0     ;       -- Sky Go days in 3mth period
alter table v159_accounts_for_profiling_dec2012_active add usage_records                INTEGER         default 0     ;       -- Sky Go usage records in 3mth period
alter table v159_accounts_for_profiling_dec2012_active add BB_type                   VARCHAR(50)     default 'NA'  ;       -- Current BB product
alter table v159_accounts_for_profiling_dec2012_active add Anytime_plus              INTEGER         default 0    ;        -- Anytime+ activated
alter table v159_accounts_for_profiling_dec2012_active add isba_tv_region             VARCHAR(50)     default 'Unknown'         ;   
alter table v159_accounts_for_profiling_dec2012_active add cb_key_household           bigint   ;        -- Current Sky Talk product
--drop table nodupes;
commit;

update v159_accounts_for_profiling_dec2012_active
set isba_tv_region=b.isba_tv_region
,cb_key_household=b.cb_key_household
from v159_accounts_for_profiling_dec2012_active as a
left outer join sk_prod.cust_single_account_view as b
on a.account_number=b.account_number
;
commit;
--select top 100 * from v159_accounts_for_profiling_dec2012_active;
-------------------------------------------------  02 - Active Sky Talk
--code_location_09
--drop table talk;
--commit;

SELECT DISTINCT base.account_number
       ,CASE WHEN UCASE(current_product_description) LIKE '%UNLIMITED%'
             THEN 'Unlimited'
             ELSE 'Freetime'
          END as talk_product
      ,rank() over(PARTITION BY base.account_number ORDER BY effective_to_dt desc) AS rank_id
      ,effective_to_dt
         INTO talk
FROM sk_prod.cust_subs_hist AS CSH
    inner join AdSmart AS Base
    ON csh.account_number = base.account_number
WHERE subscription_sub_type = 'SKY TALK SELECT'
     AND(     status_code = 'A'
          OR (status_code = 'FBP' AND prev_status_code IN ('PC','A'))
          OR (status_code = 'RI'  AND prev_status_code IN ('FBP','A'))
          OR (status_code = 'PC'  AND prev_status_code = 'A'))
     AND effective_to_dt != effective_from_dt
     AND csh.effective_from_dt <= '2012-12-25'
     AND csh.effective_to_dt > '2012-12-25'
GROUP BY base.account_number, talk_product,effective_to_dt;
commit;

DELETE FROM talk where rank_id >1;
commit;


--      create index on talk
CREATE   HG INDEX idx09 ON talk(account_number);
commit;

--      update AdSmart file
UPDATE v159_accounts_for_profiling_dec2012_active
SET  talk_product = talk.talk_product
FROM v159_accounts_for_profiling_dec2012_active  AS Base
  INNER JOIN talk AS talk
        ON base.account_number = talk.account_number
ORDER BY base.account_number;
commit;

DROP TABLE talk;
commit;


-------------------------------------------------  02 - Sky Go and Downloads
--code_location_06
/*SELECT base.account_number
       ,count(distinct base.account_number) AS Sky_Go_Reg
INTO Sky_Go
FROM   sk_prod.SKY_PLAYER_REGISTRANT  AS Sky_Go
        inner join AdSmart as Base
         on Sky_Go.account_number = Base.account_number
GROUP BY base.account_number;
*/
select account_number
        ,count(distinct cb_data_date) as distinct_usage_days
        ,count(*) as usage_records
--        ,sum(SKY_GO_USAGE)
into skygo_usage
from sk_prod.SKY_PLAYER_USAGE_DETAIL AS usage
--        inner join v159_accounts_for_profiling_dec2012_active AS Base
--         ON usage.account_number = Base.account_number
where cb_data_date >= '2012-09-26'
        AND cb_data_date <'2012-12-25'
group by account_number;
commit;

--      create index on Sky_Go file
CREATE   HG INDEX idx06 ON skygo_usage(account_number);
commit;

--      update AdSmart file
UPDATE v159_accounts_for_profiling_dec2012_active
SET distinct_usage_days = sky_go.distinct_usage_days
,usage_records=sky_go.usage_records
FROM v159_accounts_for_profiling_dec2012_active  AS Base
       INNER JOIN skygo_usage AS sky_go
        ON base.account_number = sky_go.account_number
ORDER BY base.account_number;
commit;

DROP TABLE skygo_usage;
commit;



-------------------------------------------------  02 - Active BB Type
--code_location_10
--drop table bb;
--commit;

Select distinct base.account_number
           ,CASE WHEN current_product_sk=43373 THEN '1) Unlimited (New)'
                 WHEN current_product_sk=42128 THEN '2) Unlimited (Old)'
                 WHEN current_product_sk=42129 THEN '3) Everyday'
                 WHEN current_product_sk=42130 THEN '4) Everyday Lite'
                 WHEN current_product_sk=42131 THEN '5) Connect'
                 ELSE 'NA'
                 END AS BB_type
               ,rank() over(PARTITION BY base.account_number ORDER BY effective_to_dt desc) AS rank_id
               ,effective_to_dt
        ,count(*) AS total
INTO bb
FROM sk_prod.cust_subs_hist AS CSH
    inner join v159_accounts_for_profiling_dec2012_active AS Base
    ON csh.account_number = base.account_number
WHERE subscription_sub_type = 'Broadband DSL Line'
   AND csh.effective_from_dt <= '2012-12-25'
   AND csh.effective_to_dt > '2012-12-25'
      AND effective_from_dt != effective_to_dt
      AND (status_code IN ('AC','AB') OR (status_code='PC' AND prev_status_code NOT IN ('?','RQ','AP','UB','BE','PA') )
            OR (status_code='CF' AND prev_status_code='PC')
            OR (status_code='AP' AND sale_type='SNS Bulk Migration'))
GROUP BY base.account_number, bb_type, effective_to_dt;
commit;

--select top 10 * from bb

DELETE FROM bb where rank_id >1;
commit;

--drop table bbb;
--commit;

select distinct account_number, BB_type
               ,rank() over(PARTITION BY account_number ORDER BY BB_type desc) AS rank_id
into bbb
from bb;
commit;

DELETE FROM bbb where rank_id >1;
commit;

--      create index on BB
CREATE   HG INDEX idx10 ON BB(account_number);
commit;
--select top 500 * from  v159_accounts_for_profiling_dec2012_active;
--      update v159_accounts_for_profiling_dec2012_active file
UPDATE v159_accounts_for_profiling_dec2012_active
SET  BB_type = BB.BB_type
FROM v159_accounts_for_profiling_dec2012_active  AS Base
  INNER JOIN BB AS BB
        ON base.account_number = BB.account_number
            ORDER BY base.account_number;
commit;


drop table bb; commit;
DROP TABLE BBB; commit;


-------------------------------------------------  02 - Anytime + activated
--code_location_05     code changed in line with changes to Wiki
/*SELECT base.account_number
       ,1 AS Anytime_plus
INTO Anytime_plus
FROM   sk_prod.CUST_SUBS_HIST AS Aplus
        inner join AdSmart as Base
         on Aplus.account_number = Base.account_number
WHERE  subscription_sub_type = 'PDL subscriptions'
AND    status_code = 'AC'
AND    Aplus.effective_from_dt >= @today
AND    Aplus.effective_to_dt > @today
GROUP BY base.account_number;
*/


SELECT base.account_number
       ,1 AS Anytime_plus
INTO Anytime_plus
FROM   sk_prod.CUST_SUBS_HIST AS Aplus
        inner join v159_accounts_for_profiling_dec2012_active as Base
         on Aplus.account_number = Base.account_number
WHERE  subscription_sub_type = 'PDL subscriptions'
AND        status_code='AC'
AND        first_activation_dt<'2012-12-25'              -- (END)
AND        first_activation_dt>='2010-01-01'        -- (START) Oct 2010 was the soft launch of A+, no one should have it before then
AND        base.account_number is not null
AND        base.account_number <> '?'
GROUP BY   base.account_number;
commit;


--      create index on Anytime_plus file
CREATE   HG INDEX idx05 ON Anytime_plus(account_number);
commit;

--      update AdSmart file
UPDATE v159_accounts_for_profiling_dec2012_active
SET Anytime_plus = Aplus.Anytime_plus
FROM v159_accounts_for_profiling_dec2012_active  AS Base
       INNER JOIN Anytime_plus AS Aplus
        ON base.account_number = APlus.account_number
ORDER BY base.account_number;
commit;

DROP TABLE Anytime_plus;
commit;

---Anytime Plus Used---
--select top 100 * from sk_prod.CUST_ANYTIME_PLUS_DOWNLOADS;
SELECT  account_number
,count(distinct cast(last_modified_dt as date)) as unique_dates_with_anytime_plus_downloads
,count(*) as total_anytime_plus_download_records
into anytime_plus_downloads
--into v141_anytime_plus_users
FROM   sk_prod.CUST_ANYTIME_PLUS_DOWNLOADS
WHERE  last_modified_dt BETWEEN '2012-09-26' and '2012-12-25'
AND    x_content_type_desc = 'PROGRAMME'  --  to exclude trailers
AND    download_count=1    -- to exclude any spurious header/trailer download records
group by account_number
;
commit;


CREATE   HG INDEX idx01 ON anytime_plus_downloads(account_number);
commit;
alter table v159_accounts_for_profiling_dec2012_active add unique_dates_with_anytime_plus_downloads tinyint default 0;
alter table v159_accounts_for_profiling_dec2012_active add total_anytime_plus_download_records tinyint default 0;

update  v159_accounts_for_profiling_dec2012_active
set unique_dates_with_anytime_plus_downloads = b.unique_dates_with_anytime_plus_downloads
,total_anytime_plus_download_records=b.total_anytime_plus_download_records
from v159_accounts_for_profiling_dec2012_active as a
left outer join anytime_plus_downloads as b
on a.account_number=b.account_number
;
commit;

DROP TABLE anytime_plus_downloads;
commit;

--select top 500 * from v159_accounts_for_profiling_dec2012_active;
----Update Nulls to 0---

update v159_accounts_for_profiling_dec2012_active
set hdtv=case when hdtv is null then 0 else hdtv end
,multiroom=case when multiroom is null then 0 else multiroom end
,skyplus=case when skyplus is null then 0 else skyplus end
,unique_dates_with_anytime_plus_downloads=case when unique_dates_with_anytime_plus_downloads is null then 0 else unique_dates_with_anytime_plus_downloads end
,total_anytime_plus_download_records=case when total_anytime_plus_download_records is null then 0 else total_anytime_plus_download_records end
from v159_accounts_for_profiling_dec2012_active
;
commit;

--select sum(hdtv) from v159_accounts_for_profiling_dec2012_active

---Create Table With Affluence HH Details (Current status)----
--select *  FROM sk_prod.EXPERIAN_CONSUMERVIEW where cb_address_postcode = 'HP23 5PS' and cb_address_buildingno='6'
--select cb_change_date , count(*) from sk_prod.EXPERIAN_CONSUMERVIEW group by cb_change_date;

select cb_key_household
,max(h_household_composition) as hh_composition
,max(h_affluence_v2) as hh_affluence
,max(h_age_coarse) as head_hh_age
,max(h_number_of_children_in_household_2011) as num_children_in_hh
,max(h_number_of_adults) as number_of_adults
,max(h_number_of_bedrooms) as number_of_bedrooms
,max(h_length_of_residency) as length_of_residency
,max(h_residence_type_v2) as residence_type
,max(h_tenure_v2) as own_rent_status
into #experian_hh_summary
FROM sk_prod.EXPERIAN_CONSUMERVIEW AS CV
where cb_change_date='2013-02-25'
and cb_address_status = '1' and cb_address_dps IS NOT NULL and cb_address_organisation IS NULL
group by cb_key_household;
commit;

exec sp_create_tmp_table_idx '#experian_hh_summary', 'cb_key_household';
commit;

---Add HH Key to Account Table---
alter table account_status_at_period_start add cb_key_household           bigint   ;        -- Current Sky Talk product

update account_status_at_period_start
set cb_key_household=b.cb_key_household
from account_status_at_period_start as a
left outer join sk_prod.cust_single_account_view as b
on a.account_number=b.account_number
;
commit;

---Add Experian Values to main account table
alter table account_status_at_period_start add hh_composition             VARCHAR(2)     default 'U'         ;   
alter table account_status_at_period_start add hh_affluence             VARCHAR(2)     default 'U'         ;   
alter table account_status_at_period_start add head_hh_age             VARCHAR(1)     default 'U'         ;   
alter table account_status_at_period_start add num_children_in_hh             VARCHAR(1)            ;   

alter table account_status_at_period_start add number_of_adults            bigint         ;   
alter table account_status_at_period_start add number_of_bedrooms             VARCHAR(1)            ;   
alter table account_status_at_period_start add length_of_residency             VARCHAR(2)           ;  
alter table account_status_at_period_start add residence_type             VARCHAR(1)            ;   
alter table account_status_at_period_start add own_rent_status             VARCHAR(1)            ;   


update account_status_at_period_start
set hh_composition=b.hh_composition
,hh_affluence=b.hh_affluence
,head_hh_age=b.head_hh_age
,num_children_in_hh=b.num_children_in_hh

,number_of_adults=b.number_of_adults
,number_of_bedrooms=b.number_of_bedrooms
,length_of_residency=b.length_of_residency

,residence_type=b.residence_type
,own_rent_status=b.own_rent_status

from account_status_at_period_start as a
left outer join #experian_hh_summary as b
on a.cb_key_household=b.cb_key_household
;
commit;


-----Analysis of Churn activity by attributes

---001 - BB Holdings---

select case   when bb_type<>'NA' and talk_product<>'NA' then 'a) BB/Talk/TV'
        when bb_type<>'NA' then 'b) BB/TV'
        when talk_product<>'NA' then 'c) Talk/TV'
        else 'd) TV Only' end as bb_talk_holdings
,sum(case when status_at_2011_12_26='AC' then 1 else 0 end) as active_2011_12_26
,sum(case when status_at_2011_12_26='AC' and (ab_events_2011_12+ pc_events_2011_12)>0 then 1 else 0 end) as active_2011_12_26_ab_pc_in_period

from account_status_at_period_start as a
left outer join v159_accounts_for_profiling_dec2011_active as b
on a.account_number = b.account_number
where country_code = 'GBR' and activation_date<='2011-12-26'
group by bb_talk_holdings
order by bb_talk_holdings
;

--select top 100 * from account_status_at_period_start;

select case   when bb_type<>'NA' and talk_product<>'NA' then 'a) BB/Talk/TV'
        when bb_type<>'NA' then 'b) BB/TV'
        when talk_product<>'NA' then 'c) Talk/TV'
        else 'd) TV Only' end as bb_talk_holdings
,sum(case when status_at_2012_12_26='AC' then 1 else 0 end) as active_2012_12_26
,sum(case when status_at_2012_12_26='AC' and (ab_events_2012_13+ pc_events_2012_13)>0 then 1 else 0 end) as active_2012_12_26_ab_pc_in_period

,sum(case when status_at_2012_12_26='AC' and vespa_weight_2012_12_26>0 then vespa_weight_2012_12_26 else 0 end) as active_2012_12_26_on_vespa_weighted
,sum(case when status_at_2012_12_26='AC' and vespa_weight_2012_12_26>0 and ( ab_events_2012_13+ pc_events_2012_13)>0 then vespa_weight_2012_12_26 else 0 end) as active_2012_12_26_ab_pc_in_period_on_vespa_weighted
from account_status_at_period_start as a
left outer join v159_accounts_for_profiling_dec2012_active as b
on a.account_number = b.account_number
where country_code = 'GBR' and activation_date<='2012-12-26'
group by bb_talk_holdings
order by bb_talk_holdings
;
--select top 100 * from v159_accounts_for_profiling_dec2012_active;

----HDTV---
select hdtv
,sum(case when status_at_2011_12_26='AC' then 1 else 0 end) as active_2011_12_26
,sum(case when status_at_2011_12_26='AC' and (ab_events_2011_12+ pc_events_2011_12)>0 then 1 else 0 end) as active_2011_12_26_ab_pc_in_period

from account_status_at_period_start as a
left outer join v159_accounts_for_profiling_dec2011_active as b
on a.account_number = b.account_number
where country_code = 'GBR' and activation_date<='2011-12-26'
group by hdtv
order by hdtv
;


select hdtv
,sum(case when status_at_2012_12_26='AC' then 1 else 0 end) as active_2012_12_26
,sum(case when status_at_2012_12_26='AC' and (ab_events_2012_13+ pc_events_2012_13)>0 then 1 else 0 end) as active_2012_12_26_ab_pc_in_period

,sum(case when status_at_2012_12_26='AC' and vespa_weight_2012_12_26>0 then vespa_weight_2012_12_26 else 0 end) as active_2012_12_26_on_vespa_weighted
,sum(case when status_at_2012_12_26='AC' and vespa_weight_2012_12_26>0 and ( ab_events_2012_13+ pc_events_2012_13)>0 then vespa_weight_2012_12_26 else 0 end) as active_2012_12_26_ab_pc_in_period_on_vespa_weighted
,count(*) as records
from account_status_at_period_start as a
left outer join v159_accounts_for_profiling_dec2012_active as b
on a.account_number = b.account_number
where country_code = 'GBR' and activation_date<='2012-12-26'
group by hdtv
order by hdtv
;

commit;

----Use Viewing Data As Split---

--select top 500 * from V159_Tenure_10_16mth_Viewing_summary;

select case when total_viewing_duration/3600<270 then 'a) Under 3hrs average per day'
when total_viewing_duration/3600<450 then 'b) >=3 and <5hrs average per day'
when total_viewing_duration/3600<720 then 'c) >=5 and <8hrs average per day'
when total_viewing_duration/3600>=720 then 'd) 8+hrs average per day' else 'e) Other' end as tv_viewing_per_day_average
,sum(case when status_at_2012_12_26='AC' then 1 else 0 end) as active_2012_12_26
,sum(case when status_at_2012_12_26='AC' and (ab_events_2012_13+ pc_events_2012_13)>0 then 1 else 0 end) as active_2012_12_26_ab_pc_in_period
,sum(case when status_at_2012_12_26='AC' and (pc_events_2012_13)>0 then 1 else 0 end) as active_2012_12_26_pc_in_period
,sum(case when status_at_2012_12_26='AC' and (ab_events_2012_13)>0 then 1 else 0 end) as active_2012_12_26_ab_in_period

--Weighted Figures
,sum(case when status_at_2012_12_26='AC' and vespa_weight_2012_12_26>0 then vespa_weight_2012_12_26 else 0 end) as active_2012_12_26_on_vespa_weighted
,sum(case when status_at_2012_12_26='AC' and vespa_weight_2012_12_26>0 and ( ab_events_2012_13+ pc_events_2012_13)>0 then vespa_weight_2012_12_26 else 0 end) as active_2012_12_26_ab_pc_in_period_on_vespa_weighted
,sum(case when status_at_2012_12_26='AC' and vespa_weight_2012_12_26>0 and (pc_events_2012_13)>0 then vespa_weight_2012_12_26 else 0 end) as active_2012_12_26_pc_in_period_on_vespa_weighted
,sum(case when status_at_2012_12_26='AC' and vespa_weight_2012_12_26>0 and (ab_events_2012_13)>0 then vespa_weight_2012_12_26 else 0 end) as active_2012_12_26_ab_in_period_on_vespa_weighted

,count(*) as records
from account_status_at_period_start as a
left outer join v159_accounts_for_profiling_dec2012_active as b
on a.account_number = b.account_number
left outer join V159_Tenure_10_16mth_Viewing_summary as c
on a.account_number = c.account_number
where country_code = 'GBR' and activation_date<='2012-12-26' and distinct_days_viewing>=70
group by tv_viewing_per_day_average
order by tv_viewing_per_day_average
;

select case when prem_sports=0 and prem_movies=0 then 'a) No Sports or Movies Premiums'
when all_sky_sports_movies_channels/3600<30 then 'b) Under 30hrs Sports or Movies Viewing'
when all_sky_sports_movies_channels/3600<50 then 'c) >=30hrs and <50hrs Sports/Movies viewing in period'
when all_sky_sports_movies_channels/3600<90 then 'd) >=50hrs and <90hrs Sports/Movies viewing in period'
when all_sky_sports_movies_channels/3600>=90 then 'e) >=90hrs of Sports/Movies viewing in period'
 else 'f) Other' end as sky_sports_movies_viewing

,sum(case when status_at_2012_12_26='AC' then 1 else 0 end) as active_2012_12_26
,sum(case when status_at_2012_12_26='AC' and (ab_events_2012_13+ pc_events_2012_13)>0 then 1 else 0 end) as active_2012_12_26_ab_pc_in_period
,sum(case when status_at_2012_12_26='AC' and (pc_events_2012_13)>0 then 1 else 0 end) as active_2012_12_26_pc_in_period
,sum(case when status_at_2012_12_26='AC' and (ab_events_2012_13)>0 then 1 else 0 end) as active_2012_12_26_ab_in_period

--Weighted Figures
,sum(case when status_at_2012_12_26='AC' and vespa_weight_2012_12_26>0 then vespa_weight_2012_12_26 else 0 end) as active_2012_12_26_on_vespa_weighted
,sum(case when status_at_2012_12_26='AC' and vespa_weight_2012_12_26>0 and ( ab_events_2012_13+ pc_events_2012_13)>0 then vespa_weight_2012_12_26 else 0 end) as active_2012_12_26_ab_pc_in_period_on_vespa_weighted
,sum(case when status_at_2012_12_26='AC' and vespa_weight_2012_12_26>0 and (pc_events_2012_13)>0 then vespa_weight_2012_12_26 else 0 end) as active_2012_12_26_pc_in_period_on_vespa_weighted
,sum(case when status_at_2012_12_26='AC' and vespa_weight_2012_12_26>0 and (ab_events_2012_13)>0 then vespa_weight_2012_12_26 else 0 end) as active_2012_12_26_ab_in_period_on_vespa_weighted

,count(*) as records
from account_status_at_period_start as a
left outer join v159_accounts_for_profiling_dec2012_active as b
on a.account_number = b.account_number
left outer join V159_Tenure_10_16mth_Viewing_summary as c
on a.account_number = c.account_number
where country_code = 'GBR' and activation_date<='2012-12-26' and distinct_days_viewing>=70
group by sky_sports_movies_viewing
order by sky_sports_movies_viewing
;

commit;
commit;
--select top 100 * from v159_accounts_for_profiling_dec2012_active;
--select top 100 * from V159_Tenure_10_16mth_Viewing_summary;
--select * from account_status_at_period_start;

select case when pay_channels_exc_sky_sports_movies/3600<90 then 'a) Under 1hr per day average'
when pay_channels_exc_sky_sports_movies/3600<180 then 'b)  >=1hr and <2hr per day average'
when pay_channels_exc_sky_sports_movies/3600<270 then 'c) >=2hr and <3hr per day average'
when pay_channels_exc_sky_sports_movies/3600>=270 then 'd) >=3hr per day average'
else 'e) Other' end as pay_exc_sports_movies_viewing
,sum(case when status_at_2012_12_26='AC' then 1 else 0 end) as active_2012_12_26
,sum(case when status_at_2012_12_26='AC' and (ab_events_2012_13+ pc_events_2012_13)>0 then 1 else 0 end) as active_2012_12_26_ab_pc_in_period
,sum(case when status_at_2012_12_26='AC' and (pc_events_2012_13)>0 then 1 else 0 end) as active_2012_12_26_pc_in_period
,sum(case when status_at_2012_12_26='AC' and (ab_events_2012_13)>0 then 1 else 0 end) as active_2012_12_26_ab_in_period

--Weighted Figures
,sum(case when status_at_2012_12_26='AC' and vespa_weight_2012_12_26>0 then vespa_weight_2012_12_26 else 0 end) as active_2012_12_26_on_vespa_weighted
,sum(case when status_at_2012_12_26='AC' and vespa_weight_2012_12_26>0 and ( ab_events_2012_13+ pc_events_2012_13)>0 then vespa_weight_2012_12_26 else 0 end) as active_2012_12_26_ab_pc_in_period_on_vespa_weighted
,sum(case when status_at_2012_12_26='AC' and vespa_weight_2012_12_26>0 and (pc_events_2012_13)>0 then vespa_weight_2012_12_26 else 0 end) as active_2012_12_26_pc_in_period_on_vespa_weighted
,sum(case when status_at_2012_12_26='AC' and vespa_weight_2012_12_26>0 and (ab_events_2012_13)>0 then vespa_weight_2012_12_26 else 0 end) as active_2012_12_26_ab_in_period_on_vespa_weighted
,count(*) as records
from account_status_at_period_start as a
left outer join v159_accounts_for_profiling_dec2012_active as b
on a.account_number = b.account_number
left outer join V159_Tenure_10_16mth_Viewing_summary as c
on a.account_number = c.account_number
where country_code = 'GBR' and activation_date<='2012-12-26' and distinct_days_viewing>=70
group by pay_exc_sports_movies_viewing
order by pay_exc_sports_movies_viewing
;
commit;




----Split by Own/Rent Status
select case when own_rent_status = '0' then 'a) Owner occupied'
when own_rent_status = '1' then 'b) Privately rented'
when own_rent_status = '2' then 'c) Council / housing association' else 'd) Unknown' end as own_rent_type

,sum(case when status_at_2012_12_26='AC' then 1 else 0 end) as active_2012_12_26
,sum(case when status_at_2012_12_26='AC' and (ab_events_2012_13+ pc_events_2012_13)>0 then 1 else 0 end) as active_2012_12_26_ab_pc_in_period
,sum(case when status_at_2012_12_26='AC' and (pc_events_2012_13)>0 then 1 else 0 end) as active_2012_12_26_pc_in_period
,sum(case when status_at_2012_12_26='AC' and (ab_events_2012_13)>0 then 1 else 0 end) as active_2012_12_26_ab_in_period

--Weighted Figures
,sum(case when status_at_2012_12_26='AC' and vespa_weight_2012_12_26>0 then vespa_weight_2012_12_26 else 0 end) as active_2012_12_26_on_vespa_weighted
,sum(case when status_at_2012_12_26='AC' and vespa_weight_2012_12_26>0 and ( ab_events_2012_13+ pc_events_2012_13)>0 then vespa_weight_2012_12_26 else 0 end) as active_2012_12_26_ab_pc_in_period_on_vespa_weighted
,sum(case when status_at_2012_12_26='AC' and vespa_weight_2012_12_26>0 and (pc_events_2012_13)>0 then vespa_weight_2012_12_26 else 0 end) as active_2012_12_26_pc_in_period_on_vespa_weighted
,sum(case when status_at_2012_12_26='AC' and vespa_weight_2012_12_26>0 and (ab_events_2012_13)>0 then vespa_weight_2012_12_26 else 0 end) as active_2012_12_26_ab_in_period_on_vespa_weighted

,count(*) as records
from account_status_at_period_start as a
left outer join v159_accounts_for_profiling_dec2012_active as b
on a.account_number = b.account_number
left outer join V159_Tenure_10_16mth_Viewing_summary as c
on a.account_number = c.account_number
where country_code = 'GBR' and activation_date<='2012-12-26' and distinct_days_viewing>=70
group by own_rent_type
order by own_rent_type
;

----Split by Own/Rent Status
select case when length_of_residency in ('00','01') then 'a) <2 Years'
when length_of_residency in ('02') then 'b) 2 Years'
when length_of_residency in ('03','04','05') then 'c) 3-5 Years'
when length_of_residency in ('06','07','08','09','10','11') then 'd) 6+ Years' else 'e) Unknown' end as residency_length

,sum(case when status_at_2012_12_26='AC' then 1 else 0 end) as active_2012_12_26
,sum(case when status_at_2012_12_26='AC' and (ab_events_2012_13+ pc_events_2012_13)>0 then 1 else 0 end) as active_2012_12_26_ab_pc_in_period
,sum(case when status_at_2012_12_26='AC' and (pc_events_2012_13)>0 then 1 else 0 end) as active_2012_12_26_pc_in_period
,sum(case when status_at_2012_12_26='AC' and (ab_events_2012_13)>0 then 1 else 0 end) as active_2012_12_26_ab_in_period

--Weighted Figures
,sum(case when status_at_2012_12_26='AC' and vespa_weight_2012_12_26>0 then vespa_weight_2012_12_26 else 0 end) as active_2012_12_26_on_vespa_weighted
,sum(case when status_at_2012_12_26='AC' and vespa_weight_2012_12_26>0 and ( ab_events_2012_13+ pc_events_2012_13)>0 then vespa_weight_2012_12_26 else 0 end) as active_2012_12_26_ab_pc_in_period_on_vespa_weighted
,sum(case when status_at_2012_12_26='AC' and vespa_weight_2012_12_26>0 and (pc_events_2012_13)>0 then vespa_weight_2012_12_26 else 0 end) as active_2012_12_26_pc_in_period_on_vespa_weighted
,sum(case when status_at_2012_12_26='AC' and vespa_weight_2012_12_26>0 and (ab_events_2012_13)>0 then vespa_weight_2012_12_26 else 0 end) as active_2012_12_26_ab_in_period_on_vespa_weighted

,count(*) as records
from account_status_at_period_start as a
left outer join v159_accounts_for_profiling_dec2012_active as b
on a.account_number = b.account_number
left outer join V159_Tenure_10_16mth_Viewing_summary as c
on a.account_number = c.account_number
where country_code = 'GBR' and activation_date<='2012-12-26' and distinct_days_viewing>=70
group by residency_length
order by residency_length
;
commit;


--select top 500 * from account_status_at_period_start;
----Output for Pivot----
select case when total_viewing_duration/3600<270 then 'a) Under 3hrs average per day'
when total_viewing_duration/3600<450 then 'b) >=3 and <5hrs average per day'
when total_viewing_duration/3600<720 then 'c) >=5 and <8hrs average per day'
when total_viewing_duration/3600>=720 then 'd) 8+hrs average per day' else 'e) Other' end as tv_viewing_per_day_average

,case when pay_channels_exc_sky_sports_movies/3600<90 then 'a) Under 1hr per day average'
when pay_channels_exc_sky_sports_movies/3600<180 then 'b)  >=1hr and <2hr per day average'
when pay_channels_exc_sky_sports_movies/3600<270 then 'c) >=2hr and <3hr per day average'
when pay_channels_exc_sky_sports_movies/3600>=270 then 'd) >=3hr per day average'
else 'e) Other' end as pay_exc_sports_movies_viewing

,case when prem_sports=0 and prem_movies=0 then 'a) No Sports or Movies Premiums'
when all_sky_sports_movies_channels/3600<30 then 'b) Under 30hrs Sports or Movies Viewing'
when all_sky_sports_movies_channels/3600<50 then 'c) >=30hrs and <50hrs Sports/Movies viewing in period'
when all_sky_sports_movies_channels/3600<90 then 'd) >=50hrs and <90hrs Sports/Movies viewing in period'
when all_sky_sports_movies_channels/3600>=90 then 'e) >=90hrs of Sports/Movies viewing in period'
 else 'f) Other' end as sky_sports_movies_viewing

,case when own_rent_status = '0' then 'a) Owner occupied'
when own_rent_status = '1' then 'b) Privately rented'
when own_rent_status = '2' then 'c) Council / housing association' else 'd) Unknown' end as own_rent_type
,case  
        when num_children_in_hh in ('1','2','3','4') then 1 else 0 end any_kids_in_hh

,case  when residence_type IN ('0')       THEN '1) Detached'
        when residence_type IN ('1')       THEN '2) Semi-detached'
        when residence_type IN ('2')       THEN '3) Bungalow'
        when residence_type IN ('3')       THEN '4) Terraced'
        when residence_type IN ('4')       THEN '5) Flat'
        when residence_type IN ('U')       THEN '6) Unclassified' else '6) Unclassified'
                                                END as property_type 
,  case         WHEN hh_affluence IN ('00','01','02')       THEN 'A) Very Low'
                                                WHEN hh_affluence IN ('03','04', '05')      THEN 'B) Low'
                                                WHEN hh_affluence IN ('06','07','08')       THEN 'C) Mid Low'
                                                WHEN hh_affluence IN ('09','10','11')       THEN 'D) Mid'
                                                WHEN hh_affluence IN ('12','13','14')       THEN 'E) Mid High'
                                                WHEN hh_affluence IN ('15','16','17')       THEN 'F) High'
                                                WHEN hh_affluence IN ('18','19')            THEN 'G) Very High' else 'H) Unknown' END as affluence
,case when prem_sports=2 and prem_movies=2 then 'a) All Premiums'
                                   when prem_sports=2 then 'b) Duel Sports'
                                   when prem_movies=2 then 'c) Duel Movies'
                                   when prem_sports+prem_movies>0 then 'd) Other Premiums' else 'e) No Premiums' end as premiums_type
,case   when bb_type<>'NA' and talk_product<>'NA' then 'a) BB/Talk/TV'
        when bb_type<>'NA' then 'b) BB/TV'
        when talk_product<>'NA' then 'c) Talk/TV'
        else 'd) TV Only' end as bb_talk_holdings
, case when hdtv is null then 0 else hdtv end as has_hdtv
,case when multiroom is null then 0 else multiroom end as has_multiroom
,case when skyplus is null then 0 else skyplus end as has_skyplus
,case when distinct_usage_days is null then 'a) not used Sky go in previous 3 months'
when distinct_usage_days =0 then 'a) not used Sky go in previous 3 months'
when distinct_usage_days <=5 then 'b) Used Sky go 1-5 days in 3mth period'
when distinct_usage_days >5 then 'c) Used Sky go 6+ days in 3mth period' else 'd) Other' end as Sky_go_usage_last_3m
,case when length_of_residency in ('00','01') then 'a) <2 Years'
when length_of_residency in ('02') then 'b) 2 Years'
when length_of_residency in ('03','04','05') then 'c) 3-5 Years'
when length_of_residency in ('06','07','08','09','10','11') then 'd) 6+ Years' else 'e) Unknown' end as residency_length

,case when status_at_2012_12_26='AC' then 1 else 0 end as active_2012_12_26
,case when status_at_2012_12_26='AC' and (ab_events_2012_13+ pc_events_2012_13)>0 then 1 else 0 end as active_2012_12_26_ab_pc_in_period
,case when status_at_2012_12_26='AC' and (pc_events_2012_13)>0 then 1 else 0 end as active_2012_12_26_pc_in_period
,case when status_at_2012_12_26='AC' and (ab_events_2012_13)>0 then 1 else 0 end as active_2012_12_26_ab_in_period

--Weighted Figures
,case when status_at_2012_12_26='AC' and vespa_weight_2012_12_26>0 then vespa_weight_2012_12_26 else 0 end as active_2012_12_26_on_vespa_weighted
,case when status_at_2012_12_26='AC' and vespa_weight_2012_12_26>0 and (ab_events_2012_13+ pc_events_2012_13)>0 then vespa_weight_2012_12_26 else 0 end as active_2012_12_26_ab_pc_in_period_on_vespa_weighted
,case when status_at_2012_12_26='AC' and vespa_weight_2012_12_26>0 and (pc_events_2012_13)>0 then vespa_weight_2012_12_26 else 0 end as active_2012_12_26_pc_in_period_on_vespa_weighted
,case when status_at_2012_12_26='AC' and vespa_weight_2012_12_26>0 and (ab_events_2012_13)>0 then vespa_weight_2012_12_26 else 0 end as active_2012_12_26_ab_in_period_on_vespa_weighted
into v159_10_16mth_tenure_pivot
from account_status_at_period_start as a
left outer join v159_accounts_for_profiling_dec2012_active as b
on a.account_number = b.account_number
left outer join V159_Tenure_10_16mth_Viewing_summary as c
on a.account_number = c.account_number
where country_code = 'GBR' and activation_date<='2012-12-26' and distinct_days_viewing>=70 and c.account_number is not null
;

commit;
grant all on v159_10_16mth_tenure_pivot to public;
commit;

----Create an Attribute Pivot for 2012/13 pending churn
select case when own_rent_status = '0' then 'a) Owner occupied'
when own_rent_status = '1' then 'b) Privately rented'
when own_rent_status = '2' then 'c) Council / housing association' else 'd) Unknown' end as own_rent_type
,case  
        when num_children_in_hh in ('1','2','3','4') then 1 else 0 end any_kids_in_hh

,case  when residence_type IN ('0')       THEN '1) Detached'
        when residence_type IN ('1')       THEN '2) Semi-detached'
        when residence_type IN ('2')       THEN '3) Bungalow'
        when residence_type IN ('3')       THEN '4) Terraced'
        when residence_type IN ('4')       THEN '5) Flat'
        when residence_type IN ('U')       THEN '6) Unclassified' else '6) Unclassified'
                                                END as property_type 
,  case         WHEN hh_affluence IN ('00','01','02')       THEN 'A) Very Low'
                                                WHEN hh_affluence IN ('03','04', '05')      THEN 'B) Low'
                                                WHEN hh_affluence IN ('06','07','08')       THEN 'C) Mid Low'
                                                WHEN hh_affluence IN ('09','10','11')       THEN 'D) Mid'
                                                WHEN hh_affluence IN ('12','13','14')       THEN 'E) Mid High'
                                                WHEN hh_affluence IN ('15','16','17')       THEN 'F) High'
                                                WHEN hh_affluence IN ('18','19')            THEN 'G) Very High' else 'H) Unknown' END as affluence
,case when prem_sports=2 and prem_movies=2 then 'a) All Premiums'
                                   when prem_sports=2 then 'b) Duel Sports'
                                   when prem_movies=2 then 'c) Duel Movies'
                                   when prem_sports+prem_movies>0 then 'd) Other Premiums' else 'e) No Premiums' end as premiums_type
,case   when bb_type<>'NA' and talk_product<>'NA' then 'a) BB/Talk/TV'
        when bb_type<>'NA' then 'b) BB/TV'
        when talk_product<>'NA' then 'c) Talk/TV'
        else 'd) TV Only' end as bb_talk_holdings
, case when hdtv is null then 0 else hdtv end as has_hdtv
,case when multiroom is null then 0 else multiroom end as has_multiroom
,case when skyplus is null then 0 else skyplus end as has_skyplus
,case when distinct_usage_days is null then 'a) not used Sky go in previous 3 months'
when distinct_usage_days =0 then 'a) not used Sky go in previous 3 months'
when distinct_usage_days <=5 then 'b) Used Sky go 1-5 days in 3mth period'
when distinct_usage_days >5 then 'c) Used Sky go 6+ days in 3mth period' else 'd) Other' end as Sky_go_usage_last_3m
,case when length_of_residency in ('00','01') then 'a) <2 Years'
when length_of_residency in ('02') then 'b) 2 Years'
when length_of_residency in ('03','04','05') then 'c) 3-5 Years'
when length_of_residency in ('06','07','08','09','10','11') then 'd) 6+ Years' else 'e) Unknown' end as residency_length


,CASE   WHEN datediff(day,activation_date,cast('2012-12-25' as date)) <=  365 THEN 'a) <=1 Year'
                         WHEN datediff(day,activation_date,cast('2012-12-25' as date)) <=  730 THEN 'b) >1 Year <=2 Years'
                         WHEN datediff(day,activation_date,cast('2012-12-25' as date)) <= 1825 THEN 'c) >2 Years and <=5 Years'
                         WHEN datediff(day,activation_date,cast('2012-12-25' as date)) <= 3650 THEN 'd) >5 Years and <=10 Years'
                         WHEN datediff(day,activation_date,cast('2012-12-25' as date)) > 3650 THEN  'e) 10 Years+'
                         ELSE 'f) Unknown' end as tenure_at_dec_25_2012
,sum(case when status_at_2012_12_26='AC' then 1 else 0 end) as active_2012_12_26
,sum(case when status_at_2012_12_26='AC' and (ab_events_2012_13+ pc_events_2012_13)>0 then 1 else 0 end) as active_2012_12_26_ab_pc_in_period
,sum(case when status_at_2012_12_26='AC' and (pc_events_2012_13)>0 then 1 else 0 end) as active_2012_12_26_pc_in_period
,sum(case when status_at_2012_12_26='AC' and (ab_events_2012_13)>0 then 1 else 0 end) as active_2012_12_26_ab_in_period
into v159_2012_13_pending_churn_pivot
from account_status_at_period_start as a
left outer join v159_accounts_for_profiling_dec2012_active as b
on a.account_number = b.account_number
where country_code = 'GBR' and activation_date<='2012-12-26' 
group by own_rent_type
,any_kids_in_hh
, property_type 
,affluence
, premiums_type
, bb_talk_holdings
, has_hdtv
,has_multiroom
,has_skyplus
,Sky_go_usage_last_3m
,residency_length


, tenure_at_dec_25_2012
;
commit;
grant all on v159_2012_13_pending_churn_pivot to public;
commit;
--select count(*) from v159_2012_13_pending_churn_pivot;
--.


commit;

---Ad hoc analysis of Base---

select case when cast(dateformat(activation_date,'DD') as integer)>26 then 
     datediff(mm,activation_date,cast('2012-12-26' as date))-1 else datediff(mm,activation_date,cast('2012-12-26' as date)) end as full_months_tenure
,sum(case when status_at_2012_12_26='AC' then 1 else 0 end) as active_2012_12_26
,sum(case when status_at_2012_12_26='AC' and (ab_events_2012_13+ pc_events_2012_13)>0 then 1 else 0 end) as active_2012_12_26_ab_pc_in_period
,sum(case when status_at_2012_12_26='AC' and (pc_events_2012_13)>0 then 1 else 0 end) as active_2012_12_26_pc_in_period
,sum(case when status_at_2012_12_26='AC' and (ab_events_2012_13)>0 then 1 else 0 end) as active_2012_12_26_ab_in_period
from account_status_at_period_start
where country_code = 'GBR' and status_at_2012_12_26='AC' and activation_date<='2012-12-26'
group by full_months_tenure
order by full_months_tenure
;

---Broader Analysis on Impact of viewing on Subesequent Churn----
--select count(distinct account_number) from V159_Daily_viewing_summary;

---Look at Analysing Likelihood to PC/AB within subsequent period based on account stats including viewing (Create Pivot)---

---Add On Viewing Stats (Vespa Panel Only)

---Create Summary Stats of Viewing Between 26th Non-25th Dec and 26th Sep-25th Dec (1 month and 3 month Views)

---Reworked using updated Daily Viewing to include kids/football/sport etc.,

--drop table v159_previous_1_and_3_month_views;
select a.account_number
,sum(case when viewing_day  between '2012-11-26' and '2012-12-25' then viewing_post_6am else 0 end) as pre_pc_last_01_month_with_viewing
,sum(case when viewing_day  between '2012-11-26' and '2012-12-25' then viewing_duration else 0 end) as pre_pc_viewing_duration_last_01_month
,sum(case when viewing_day  between '2012-11-26' and '2012-12-25' then viewing_Duration_live else 0 end) as pre_pc_viewing_duration_live_last_01_month
,sum(case when viewing_day  between '2012-11-26' and '2012-12-25' then total_duration_pay_exc_premiums else 0 end) as pre_pc_viewing_duration_exc_premiums_last_01_month
,sum(case when viewing_day  between '2012-11-26' and '2012-12-25' then total_duration_premiums else 0 end) as pre_pc_viewing_premiums_last_01_month
,sum(case when viewing_day  between '2012-11-26' and '2012-12-25' then total_duration_terrestrial else 0 end) as pre_pc_viewing_terrestrial_last_01_month
,sum(case when viewing_day  between '2012-11-26' and '2012-12-25' then total_duration_free_non_terrestrial else 0 end) as pre_pc_viewing_free_non_terrestrial_last_01_month

,sum(case when viewing_day  between '2012-11-26' and '2012-12-25' then total_duration_pay_football else 0 end) as pre_pc_viewing_pay_football_last_01_month
,sum(case when viewing_day  between '2012-11-26' and '2012-12-25' then total_duration_free_football else 0 end) as pre_pc_viewing_free_football_last_01_month
,sum(case when viewing_day  between '2012-11-26' and '2012-12-25' then total_duration_pay_sport_exc_football else 0 end) as pre_pc_viewing_pay_sport_exc_football_last_01_month
,sum(case when viewing_day  between '2012-11-26' and '2012-12-25' then total_duration_free_sport_exc_football else 0 end) as pre_pc_viewing_free_sport_exc_football_last_01_month

,sum(case when viewing_day  between '2012-11-26' and '2012-12-25' then total_duration_pay_movies else 0 end) as pre_pc_viewing_pay_movies_last_01_month
,sum(case when viewing_day  between '2012-11-26' and '2012-12-25' then total_duration_free_movies else 0 end) as pre_pc_viewing_free_movies_last_01_month

,sum(case when viewing_day  between '2012-11-26' and '2012-12-25' then total_duration_pay_kids else 0 end) as pre_pc_viewing_pay_kids_last_01_month
,sum(case when viewing_day  between '2012-11-26' and '2012-12-25' then total_duration_free_kids else 0 end) as pre_pc_viewing_free_kids_last_01_month

---repeat for Last 3 Months

,sum(case when viewing_day  between '2012-09-26' and '2012-12-25' then viewing_post_6am else 0 end) as pre_pc_last_03_month_with_viewing
,sum(case when viewing_day  between '2012-09-26' and '2012-12-25' then viewing_Duration_live else 0 end) as pre_pc_viewing_duration_live_last_03_month
,sum(case when viewing_day  between '2012-09-26' and '2012-12-25' then viewing_duration else 0 end) as pre_pc_viewing_duration_last_03_month
,sum(case when viewing_day  between '2012-09-26' and '2012-12-25' then total_duration_pay_exc_premiums else 0 end) as pre_pc_viewing_duration_exc_premiums_last_03_month
,sum(case when viewing_day  between '2012-09-26' and '2012-12-25' then total_duration_premiums else 0 end) as pre_pc_viewing_premiums_last_03_month
,sum(case when viewing_day  between '2012-09-26' and '2012-12-25' then total_duration_terrestrial else 0 end) as pre_pc_viewing_terrestrial_last_03_month
,sum(case when viewing_day  between '2012-09-26' and '2012-12-25' then total_duration_free_non_terrestrial else 0 end) as pre_pc_viewing_free_non_terrestrial_last_03_month


,sum(case when viewing_day  between '2012-09-26' and '2012-12-25' then total_duration_pay_football else 0 end) as pre_pc_viewing_pay_football_last_03_month
,sum(case when viewing_day  between '2012-09-26' and '2012-12-25' then total_duration_free_football else 0 end) as pre_pc_viewing_free_football_last_03_month
,sum(case when viewing_day  between '2012-09-26' and '2012-12-25' then total_duration_pay_sport_exc_football else 0 end) as pre_pc_viewing_pay_sport_exc_football_last_03_month
,sum(case when viewing_day  between '2012-09-26' and '2012-12-25' then total_duration_free_sport_exc_football else 0 end) as pre_pc_viewing_free_sport_exc_football_last_03_month

,sum(case when viewing_day  between '2012-09-26' and '2012-12-25' then total_duration_pay_movies else 0 end) as pre_pc_viewing_pay_movies_last_03_month
,sum(case when viewing_day  between '2012-09-26' and '2012-12-25' then total_duration_free_movies else 0 end) as pre_pc_viewing_free_movies_last_03_month

,sum(case when viewing_day  between '2012-09-26' and '2012-12-25' then total_duration_pay_kids else 0 end) as pre_pc_viewing_pay_kids_last_03_month
,sum(case when viewing_day  between '2012-09-26' and '2012-12-25' then total_duration_free_kids else 0 end) as pre_pc_viewing_free_kids_last_03_month



into v159_previous_1_and_3_month_views
from  account_status_at_period_start as a
left outer join V159_Daily_viewing_summary as b
on a.account_number=b.account_number
where country_code = 'GBR' and status_at_2012_12_26='AC' and activation_date<='2012-12-26' and vespa_weight_2012_12_26 >0
group by a.account_number
;

commit;

--select top 500 * from v159_previous_1_and_3_month_views
--select top 500 * from account_status_at_period_start
--select top 500 * from V159_Daily_viewing_summary


--select pre_pc_last_03_month_with_viewing,count(*) from v159_previous_1_and_3_month_views group by pre_pc_last_03_month_with_viewing order by pre_pc_last_03_month_with_viewing;

---Overall Viewing Last 1 Month---

select case when pre_pc_viewing_duration_last_01_month/pre_pc_last_01_month_with_viewing/3600>=10 then '01: 10 hrs+ per day'
 when pre_pc_viewing_duration_last_01_month/pre_pc_last_01_month_with_viewing/3600>=8 then '02: >=8 and <10 hours per day'
 when pre_pc_viewing_duration_last_01_month/pre_pc_last_01_month_with_viewing/3600>=6 then '03: >=6 and <8 hours per day'
 when pre_pc_viewing_duration_last_01_month/pre_pc_last_01_month_with_viewing/3600>=4 then '04: >=4 and <6 hours per day'
 else '05: Under 4 Hours per day' end as total_viewing_per_day
,sum(case when status_at_2012_12_26='AC' then 1 else 0 end) as active_2012_12_26
,sum(case when status_at_2012_12_26='AC' and (ab_events_2012_13+ pc_events_2012_13)>0 then 1 else 0 end) as active_2012_12_26_ab_pc_in_period
,sum(case when status_at_2012_12_26='AC' and (pc_events_2012_13)>0 then 1 else 0 end) as active_2012_12_26_pc_in_period
,sum(case when status_at_2012_12_26='AC' and (ab_events_2012_13)>0 then 1 else 0 end) as active_2012_12_26_ab_in_period
from account_status_at_period_start as a
left outer join v159_previous_1_and_3_month_views as b
on a.account_number = b.account_number
where country_code = 'GBR' and status_at_2012_12_26='AC' and activation_date<='2012-12-26' and vespa_weight_2012_12_26 >0
---Return at least 25 viewing days in period
and pre_pc_last_01_month_with_viewing>=25
group by total_viewing_per_day
order by total_viewing_per_day
;



---Last 1 month split by affluence---
select case when hh_affluence is null then 'U' else hh_affluence end as afflu
,sum(case when status_at_2012_12_26='AC' then 1 else 0 end) as active_2012_12_26
,sum(case when status_at_2012_12_26='AC' and (ab_events_2012_13+ pc_events_2012_13)>0 then 1 else 0 end) as active_2012_12_26_ab_pc_in_period
,sum(case when status_at_2012_12_26='AC' and (pc_events_2012_13)>0 then 1 else 0 end) as active_2012_12_26_pc_in_period
,sum(case when status_at_2012_12_26='AC' and (ab_events_2012_13)>0 then 1 else 0 end) as active_2012_12_26_ab_in_period
from account_status_at_period_start as a
left outer join v159_previous_1_and_3_month_views as b
on a.account_number = b.account_number
where country_code = 'GBR' and status_at_2012_12_26='AC' and activation_date<='2012-12-26' and vespa_weight_2012_12_26 >0
---Return at least 25 viewing days in period
and pre_pc_last_01_month_with_viewing>=25
group by afflu
order by afflu
;

---Last 1 Month split by Affluence
select case when pre_pc_viewing_duration_last_01_month/pre_pc_last_01_month_with_viewing/3600>=9 then '01: 9hrs+ per day'
 when pre_pc_viewing_duration_last_01_month/pre_pc_last_01_month_with_viewing/3600>=7 then '02: >=7 and <9 hours per day'
 when pre_pc_viewing_duration_last_01_month/pre_pc_last_01_month_with_viewing/3600>=5 then '03: >=5 and <7 hours per day'
 when pre_pc_viewing_duration_last_01_month/pre_pc_last_01_month_with_viewing/3600>=3 then '04: >=3 and <5 hours per day'
 else '05: Under 3 Hours per day' end as total_viewing_per_day
,case         WHEN hh_affluence IN ('00','01','02')       THEN 'A) Low'
                                                WHEN hh_affluence IN ('03','04', '05')      THEN 'A) Low'
                                                WHEN hh_affluence IN ('06','07','08')       THEN 'B) Mid'
                                                WHEN hh_affluence IN ('09','10','11')       THEN 'B) Mid'
                                                WHEN hh_affluence IN ('12','13','14')       THEN 'B) Mid'
                                                WHEN hh_affluence IN ('15','16','17')       THEN 'C) High'
                                                WHEN hh_affluence IN ('18','19')            THEN 'C) High' else 'D) Unknown' END as affluence
,sum(case when status_at_2012_12_26='AC' then 1 else 0 end) as active_2012_12_26
,sum(case when status_at_2012_12_26='AC' and (ab_events_2012_13+ pc_events_2012_13)>0 then 1 else 0 end) as active_2012_12_26_ab_pc_in_period
,sum(case when status_at_2012_12_26='AC' and (pc_events_2012_13)>0 then 1 else 0 end) as active_2012_12_26_pc_in_period
,sum(case when status_at_2012_12_26='AC' and (ab_events_2012_13)>0 then 1 else 0 end) as active_2012_12_26_ab_in_period
from account_status_at_period_start as a
left outer join v159_previous_1_and_3_month_views as b
on a.account_number = b.account_number
where country_code = 'GBR' and status_at_2012_12_26='AC' and activation_date<='2012-12-26' and vespa_weight_2012_12_26 >0
---Return at least 25 viewing days in period
and pre_pc_last_01_month_with_viewing>=25
group by total_viewing_per_day,affluence
order by total_viewing_per_day,affluence
;

commit;
--pre_pc_viewing_duration_exc_premiums_last
---Pay Viewing Last 1 Month---
select case when pre_pc_viewing_duration_exc_premiums_last_01_month/pre_pc_last_01_month_with_viewing/3600>=4 then '01: 4hrs+ per day'
 when pre_pc_viewing_duration_exc_premiums_last_01_month/pre_pc_last_01_month_with_viewing/3600>=3 then '02: >=3 and <4 hours per day'
 when pre_pc_viewing_duration_exc_premiums_last_01_month/pre_pc_last_01_month_with_viewing/3600>=2 then '03: >=2 and <3 hours per day'
 when pre_pc_viewing_duration_exc_premiums_last_01_month/pre_pc_last_01_month_with_viewing/3600>=1 then '04: >=1 and <2 hours per day'
 else '05: Under 1 Hour per day' end as total_viewing_per_day
,sum(case when status_at_2012_12_26='AC' then 1 else 0 end) as active_2012_12_26
,sum(case when status_at_2012_12_26='AC' and (ab_events_2012_13+ pc_events_2012_13)>0 then 1 else 0 end) as active_2012_12_26_ab_pc_in_period
,sum(case when status_at_2012_12_26='AC' and (pc_events_2012_13)>0 then 1 else 0 end) as active_2012_12_26_pc_in_period
,sum(case when status_at_2012_12_26='AC' and (ab_events_2012_13)>0 then 1 else 0 end) as active_2012_12_26_ab_in_period
,sum(pre_pc_viewing_duration_exc_premiums_last_01_month/pre_pc_last_01_month_with_viewing) as totview
from account_status_at_period_start as a
left outer join v159_previous_1_and_3_month_views as b
on a.account_number = b.account_number
where country_code = 'GBR' and status_at_2012_12_26='AC' and activation_date<='2012-12-26' and vespa_weight_2012_12_26 >0
---Return at least 25 viewing days in period
and pre_pc_last_01_month_with_viewing>=25
group by total_viewing_per_day
order by total_viewing_per_day
;

commit;
commit;
---Premium Viewing Last 1 Month---
select case when prem_sports+prem_movies=0 then '05: No Premiums' when pre_pc_viewing_premiums_last_01_month/pre_pc_last_01_month_with_viewing/3600>=2 then '01: 2hrs+ per day'
 when pre_pc_viewing_premiums_last_01_month/pre_pc_last_01_month_with_viewing/3600>=1 then '02: >=1 and <3 hours per day'
 when pre_pc_viewing_premiums_last_01_month/pre_pc_last_01_month_with_viewing/1800>=1 then '03: >=30 min and <1 hour per day'
-- when pre_pc_viewing_premiums_last_01_month/pre_pc_last_01_month_with_viewing/3600>=1 then '04: >=1 and <2 hours per day'
 else '04: Under 30 min per day' end as total_viewing_per_day
,sum(case when status_at_2012_12_26='AC' then 1 else 0 end) as active_2012_12_26
,sum(case when status_at_2012_12_26='AC' and (ab_events_2012_13+ pc_events_2012_13)>0 then 1 else 0 end) as active_2012_12_26_ab_pc_in_period
,sum(case when status_at_2012_12_26='AC' and (pc_events_2012_13)>0 then 1 else 0 end) as active_2012_12_26_pc_in_period
,sum(case when status_at_2012_12_26='AC' and (ab_events_2012_13)>0 then 1 else 0 end) as active_2012_12_26_ab_in_period
,sum(pre_pc_viewing_duration_exc_premiums_last_01_month/pre_pc_last_01_month_with_viewing) as totview
from account_status_at_period_start as a
left outer join v159_previous_1_and_3_month_views as b
on a.account_number = b.account_number
left outer join v159_accounts_for_profiling_dec2012_active as c
on a.account_number = c.account_number
where country_code = 'GBR' and status_at_2012_12_26='AC' and activation_date<='2012-12-26' and vespa_weight_2012_12_26 >0
---Return at least 25 viewing days in period
and pre_pc_last_01_month_with_viewing>=25
group by total_viewing_per_day
order by total_viewing_per_day
;
--select top 100 * from account_status_at_period_start;
---Pay Football Viewing Last 1 Month---
select case when prem_sports=0 then '06: No Sports Premiums' when  pre_pc_viewing_pay_football_last_01_month/pre_pc_last_01_month_with_viewing/60>=30 then '01: >=30 min per day'
 when  pre_pc_viewing_pay_football_last_01_month/pre_pc_last_01_month_with_viewing/60>=15 then '02: >=15 and <30 min per day'
 when  pre_pc_viewing_pay_football_last_01_month/pre_pc_last_01_month_with_viewing/60>=5 then '03: >=5 min and <15 min per day'
 when  pre_pc_viewing_pay_football_last_01_month/pre_pc_last_01_month_with_viewing/60>0 then '04: <5 min per day'
-- when pre_pc_viewing_premiums_last_01_month/pre_pc_last_01_month_with_viewing/3600>=1 then '04: >=1 and <2 hours per day'
 else '05: No Pay Football Viewing' end as total_viewing_per_day
,sum(case when status_at_2012_12_26='AC' then 1 else 0 end) as active_2012_12_26
,sum(case when status_at_2012_12_26='AC' and (ab_events_2012_13+ pc_events_2012_13)>0 then 1 else 0 end) as active_2012_12_26_ab_pc_in_period
,sum(case when status_at_2012_12_26='AC' and (pc_events_2012_13)>0 then 1 else 0 end) as active_2012_12_26_pc_in_period
,sum(case when status_at_2012_12_26='AC' and (ab_events_2012_13)>0 then 1 else 0 end) as active_2012_12_26_ab_in_period
,sum(pre_pc_viewing_duration_exc_premiums_last_01_month/pre_pc_last_01_month_with_viewing) as totview
from account_status_at_period_start as a
left outer join v159_previous_1_and_3_month_views as b
on a.account_number = b.account_number
left outer join v159_accounts_for_profiling_dec2012_active as c
on a.account_number = c.account_number
where country_code = 'GBR' and status_at_2012_12_26='AC' and activation_date<='2012-12-26' and vespa_weight_2012_12_26 >0
---Return at least 25 viewing days in period
and pre_pc_last_01_month_with_viewing>=25
group by total_viewing_per_day
order by total_viewing_per_day
;

---Free Football Viewing Last 1 Month---
select case when  pre_pc_viewing_free_football_last_01_month/pre_pc_last_01_month_with_viewing/60>=30 then '01: >=30 min per day'
 when  pre_pc_viewing_free_football_last_01_month/pre_pc_last_01_month_with_viewing/60>=15 then '02: >=15 and <30 min per day'
 when  pre_pc_viewing_free_football_last_01_month/pre_pc_last_01_month_with_viewing/60>=5 then '03: >=5 min and <15 min per day'
 when  pre_pc_viewing_free_football_last_01_month/pre_pc_last_01_month_with_viewing/60>0 then '04: <5 min per day'
-- when pre_pc_viewing_premiums_last_01_month/pre_pc_last_01_month_with_viewing/3600>=1 then '04: >=1 and <2 hours per day'
 else '05: No free Football Viewing' end as total_viewing_per_day
,sum(case when status_at_2012_12_26='AC' then 1 else 0 end) as active_2012_12_26
,sum(case when status_at_2012_12_26='AC' and (ab_events_2012_13+ pc_events_2012_13)>0 then 1 else 0 end) as active_2012_12_26_ab_pc_in_period
,sum(case when status_at_2012_12_26='AC' and (pc_events_2012_13)>0 then 1 else 0 end) as active_2012_12_26_pc_in_period
,sum(case when status_at_2012_12_26='AC' and (ab_events_2012_13)>0 then 1 else 0 end) as active_2012_12_26_ab_in_period
,sum(pre_pc_viewing_duration_exc_premiums_last_01_month/pre_pc_last_01_month_with_viewing) as totview
from account_status_at_period_start as a
left outer join v159_previous_1_and_3_month_views as b
on a.account_number = b.account_number
left outer join v159_accounts_for_profiling_dec2012_active as c
on a.account_number = c.account_number
where country_code = 'GBR' and status_at_2012_12_26='AC' and activation_date<='2012-12-26' and vespa_weight_2012_12_26 >0
---Return at least 25 viewing days in period
and pre_pc_last_01_month_with_viewing>=25
group by total_viewing_per_day
order by total_viewing_per_day
;


---Pay Kids Viewing Last 1 Month---
select case  when  pre_pc_viewing_pay_kids_last_01_month/pre_pc_last_01_month_with_viewing/60>=30 then '01: >=30 min per day'
 when  pre_pc_viewing_pay_kids_last_01_month/pre_pc_last_01_month_with_viewing/60>=15 then '02: >=15 and <30 min per day'
 when  pre_pc_viewing_pay_kids_last_01_month/pre_pc_last_01_month_with_viewing/60>=5 then '03: >=5 min and <15 min per day'
 when  pre_pc_viewing_pay_kids_last_01_month/pre_pc_last_01_month_with_viewing/60>0 then '04: <5 min per day'
-- when pre_pc_viewing_premiums_last_01_month/pre_pc_last_01_month_with_viewing/3600>=1 then '04: >=1 and <2 hours per day'
 else '05: No Pay Kids Viewing' end as total_viewing_per_day
,sum(case when status_at_2012_12_26='AC' then 1 else 0 end) as active_2012_12_26
,sum(case when status_at_2012_12_26='AC' and (ab_events_2012_13+ pc_events_2012_13)>0 then 1 else 0 end) as active_2012_12_26_ab_pc_in_period
,sum(case when status_at_2012_12_26='AC' and (pc_events_2012_13)>0 then 1 else 0 end) as active_2012_12_26_pc_in_period
,sum(case when status_at_2012_12_26='AC' and (ab_events_2012_13)>0 then 1 else 0 end) as active_2012_12_26_ab_in_period
--,sum(pre_pc_viewing_duration_exc_premiums_last_01_month/pre_pc_last_01_month_with_viewing) as totview
from account_status_at_period_start as a
left outer join v159_previous_1_and_3_month_views as b
on a.account_number = b.account_number
left outer join v159_accounts_for_profiling_dec2012_active as c
on a.account_number = c.account_number
where country_code = 'GBR' and status_at_2012_12_26='AC' and activation_date<='2012-12-26' and vespa_weight_2012_12_26 >0
---Return at least 25 viewing days in period
and pre_pc_last_01_month_with_viewing>=25
group by total_viewing_per_day
order by total_viewing_per_day
;


---Free Kids Viewing Last 1 Month---
select case  when  pre_pc_viewing_free_kids_last_01_month/pre_pc_last_01_month_with_viewing/60>=30 then '01: >=30 min per day'
 when  pre_pc_viewing_free_kids_last_01_month/pre_pc_last_01_month_with_viewing/60>=15 then '02: >=15 and <30 min per day'
 when  pre_pc_viewing_free_kids_last_01_month/pre_pc_last_01_month_with_viewing/60>=5 then '03: >=5 min and <15 min per day'
 when  pre_pc_viewing_free_kids_last_01_month/pre_pc_last_01_month_with_viewing/60>0 then '04: <5 min per day'
-- when pre_pc_viewing_premiums_last_01_month/pre_pc_last_01_month_with_viewing/3600>=1 then '04: >=1 and <2 hours per day'
 else '05: No free kids Viewing' end as total_viewing_per_day
,sum(case when status_at_2012_12_26='AC' then 1 else 0 end) as active_2012_12_26
,sum(case when status_at_2012_12_26='AC' and (ab_events_2012_13+ pc_events_2012_13)>0 then 1 else 0 end) as active_2012_12_26_ab_pc_in_period
,sum(case when status_at_2012_12_26='AC' and (pc_events_2012_13)>0 then 1 else 0 end) as active_2012_12_26_pc_in_period
,sum(case when status_at_2012_12_26='AC' and (ab_events_2012_13)>0 then 1 else 0 end) as active_2012_12_26_ab_in_period
--,sum(pre_pc_viewing_duration_exc_premiums_last_01_month/pre_pc_last_01_month_with_viewing) as totview
from account_status_at_period_start as a
left outer join v159_previous_1_and_3_month_views as b
on a.account_number = b.account_number
left outer join v159_accounts_for_profiling_dec2012_active as c
on a.account_number = c.account_number
where country_code = 'GBR' and status_at_2012_12_26='AC' and activation_date<='2012-12-26' and vespa_weight_2012_12_26 >0
---Return at least 25 viewing days in period
and pre_pc_last_01_month_with_viewing>=25
group by total_viewing_per_day
order by total_viewing_per_day
;



---Pay movies Viewing Last 1 Month---
select case when prem_movies=0 then '06: No Movie Premiums' when  pre_pc_viewing_pay_movies_last_01_month/pre_pc_last_01_month_with_viewing/60>=30 then '01: >=30 min per day'
 when  pre_pc_viewing_pay_movies_last_01_month/pre_pc_last_01_month_with_viewing/60>=15 then '02: >=15 and <30 min per day'
 when  pre_pc_viewing_pay_movies_last_01_month/pre_pc_last_01_month_with_viewing/60>=5 then '03: >=5 min and <15 min per day'
 when  pre_pc_viewing_pay_movies_last_01_month/pre_pc_last_01_month_with_viewing/60>0 then '04: <5 min per day'
-- when pre_pc_viewing_premiums_last_01_month/pre_pc_last_01_month_with_viewing/3600>=1 then '04: >=1 and <2 hours per day'
 else '05: No Pay movies Viewing' end as total_viewing_per_day
,sum(case when status_at_2012_12_26='AC' then 1 else 0 end) as active_2012_12_26
,sum(case when status_at_2012_12_26='AC' and (ab_events_2012_13+ pc_events_2012_13)>0 then 1 else 0 end) as active_2012_12_26_ab_pc_in_period
,sum(case when status_at_2012_12_26='AC' and (pc_events_2012_13)>0 then 1 else 0 end) as active_2012_12_26_pc_in_period
,sum(case when status_at_2012_12_26='AC' and (ab_events_2012_13)>0 then 1 else 0 end) as active_2012_12_26_ab_in_period
,sum(pre_pc_viewing_duration_exc_premiums_last_01_month/pre_pc_last_01_month_with_viewing) as totview
from account_status_at_period_start as a
left outer join v159_previous_1_and_3_month_views as b
on a.account_number = b.account_number
left outer join v159_accounts_for_profiling_dec2012_active as c
on a.account_number = c.account_number
where country_code = 'GBR' and status_at_2012_12_26='AC' and activation_date<='2012-12-26' and vespa_weight_2012_12_26 >0
---Return at least 25 viewing days in period
and pre_pc_last_01_month_with_viewing>=25
group by total_viewing_per_day
order by total_viewing_per_day
;

---Free movies Viewing Last 1 Month---
select case when  pre_pc_viewing_free_movies_last_01_month/pre_pc_last_01_month_with_viewing/60>=30 then '01: >=30 min per day'
 when  pre_pc_viewing_free_movies_last_01_month/pre_pc_last_01_month_with_viewing/60>=15 then '02: >=15 and <30 min per day'
 when  pre_pc_viewing_free_movies_last_01_month/pre_pc_last_01_month_with_viewing/60>=5 then '03: >=5 min and <15 min per day'
 when  pre_pc_viewing_free_movies_last_01_month/pre_pc_last_01_month_with_viewing/60>0 then '04: <5 min per day'
-- when pre_pc_viewing_premiums_last_01_month/pre_pc_last_01_month_with_viewing/3600>=1 then '04: >=1 and <2 hours per day'
 else '05: No free movies Viewing' end as total_viewing_per_day
,sum(case when status_at_2012_12_26='AC' then 1 else 0 end) as active_2012_12_26
,sum(case when status_at_2012_12_26='AC' and (ab_events_2012_13+ pc_events_2012_13)>0 then 1 else 0 end) as active_2012_12_26_ab_pc_in_period
,sum(case when status_at_2012_12_26='AC' and (pc_events_2012_13)>0 then 1 else 0 end) as active_2012_12_26_pc_in_period
,sum(case when status_at_2012_12_26='AC' and (ab_events_2012_13)>0 then 1 else 0 end) as active_2012_12_26_ab_in_period
,sum(pre_pc_viewing_duration_exc_premiums_last_01_month/pre_pc_last_01_month_with_viewing) as totview
from account_status_at_period_start as a
left outer join v159_previous_1_and_3_month_views as b
on a.account_number = b.account_number
left outer join v159_accounts_for_profiling_dec2012_active as c
on a.account_number = c.account_number
where country_code = 'GBR' and status_at_2012_12_26='AC' and activation_date<='2012-12-26' and vespa_weight_2012_12_26 >0
---Return at least 25 viewing days in period
and pre_pc_last_01_month_with_viewing>=25
group by total_viewing_per_day
order by total_viewing_per_day
;

commit;

---Pay Sports exc football Viewing Last 1 Month---
select case when prem_sports=0 then '06: No Sport Premiums' when pre_pc_viewing_pay_sport_exc_football_last_01_month/pre_pc_last_01_month_with_viewing/60>=30 then '01: >=30 min per day'
 when  pre_pc_viewing_pay_sport_exc_football_last_01_month/pre_pc_last_01_month_with_viewing/60>=15 then '02: >=15 and <30 min per day'
 when  pre_pc_viewing_pay_sport_exc_football_last_01_month/pre_pc_last_01_month_with_viewing/60>=5 then '03: >=5 min and <15 min per day'
 when  pre_pc_viewing_pay_sport_exc_football_last_01_month/pre_pc_last_01_month_with_viewing/60>0 then '04: <5 min per day'
-- when pre_pc_viewing_premiums_last_01_month/pre_pc_last_01_month_with_viewing/3600>=1 then '04: >=1 and <2 hours per day'
 else '05: No Pay Sports Viewing' end as total_viewing_per_day
,sum(case when status_at_2012_12_26='AC' then 1 else 0 end) as active_2012_12_26
,sum(case when status_at_2012_12_26='AC' and (ab_events_2012_13+ pc_events_2012_13)>0 then 1 else 0 end) as active_2012_12_26_ab_pc_in_period
,sum(case when status_at_2012_12_26='AC' and (pc_events_2012_13)>0 then 1 else 0 end) as active_2012_12_26_pc_in_period
,sum(case when status_at_2012_12_26='AC' and (ab_events_2012_13)>0 then 1 else 0 end) as active_2012_12_26_ab_in_period
,sum(pre_pc_viewing_duration_exc_premiums_last_01_month/pre_pc_last_01_month_with_viewing) as totview
from account_status_at_period_start as a
left outer join v159_previous_1_and_3_month_views as b
on a.account_number = b.account_number
left outer join v159_accounts_for_profiling_dec2012_active as c
on a.account_number = c.account_number
where country_code = 'GBR' and status_at_2012_12_26='AC' and activation_date<='2012-12-26' and vespa_weight_2012_12_26 >0
---Return at least 25 viewing days in period
and pre_pc_last_01_month_with_viewing>=25
group by total_viewing_per_day
order by total_viewing_per_day
;

---Free Sports exc football Viewing Last 1 Month---

select case when  pre_pc_viewing_free_sport_exc_football_last_01_month/pre_pc_last_01_month_with_viewing/60>=30 then '01: >=30 min per day'
 when  pre_pc_viewing_free_sport_exc_football_last_01_month/pre_pc_last_01_month_with_viewing/60>=15 then '02: >=15 and <30 min per day'
 when  pre_pc_viewing_free_sport_exc_football_last_01_month/pre_pc_last_01_month_with_viewing/60>=5 then '03: >=5 min and <15 min per day'
 when  pre_pc_viewing_free_sport_exc_football_last_01_month/pre_pc_last_01_month_with_viewing/60>=1 then '04: >=1 and <5 min per day'
-- when pre_pc_viewing_premiums_last_01_month/pre_pc_last_01_month_with_viewing/3600>=1 then '04: >=1 and <2 hours per day'
 else '05: No free Sports Viewing' end as total_viewing_per_day
,sum(case when status_at_2012_12_26='AC' then 1 else 0 end) as active_2012_12_26
,sum(case when status_at_2012_12_26='AC' and (ab_events_2012_13+ pc_events_2012_13)>0 then 1 else 0 end) as active_2012_12_26_ab_pc_in_period
,sum(case when status_at_2012_12_26='AC' and (pc_events_2012_13)>0 then 1 else 0 end) as active_2012_12_26_pc_in_period
,sum(case when status_at_2012_12_26='AC' and (ab_events_2012_13)>0 then 1 else 0 end) as active_2012_12_26_ab_in_period
,sum(pre_pc_viewing_duration_exc_premiums_last_01_month/pre_pc_last_01_month_with_viewing) as totview
from account_status_at_period_start as a
left outer join v159_previous_1_and_3_month_views as b
on a.account_number = b.account_number
left outer join v159_accounts_for_profiling_dec2012_active as c
on a.account_number = c.account_number
where country_code = 'GBR' and status_at_2012_12_26='AC' and activation_date<='2012-12-26' and vespa_weight_2012_12_26 >0
---Return at least 25 viewing days in period
and pre_pc_last_01_month_with_viewing>=25
group by total_viewing_per_day
order by total_viewing_per_day
;

commit;

---Live/Playback split
select case when round(pre_pc_viewing_duration_live_last_01_month/pre_pc_viewing_duration_last_01_month,2)<=0.70 then '01: 30%+ duration via Playback'
            when round(pre_pc_viewing_duration_live_last_01_month/pre_pc_viewing_duration_last_01_month,2)<=0.80 then '02: >= 20% and <30% duration via Playback'
            when round(pre_pc_viewing_duration_live_last_01_month/pre_pc_viewing_duration_last_01_month,2)<=0.90 then '03: >= 10% and <20% duration via Playback'
            when round(pre_pc_viewing_duration_live_last_01_month/pre_pc_viewing_duration_last_01_month,2)<1 then '04: Under 10% duration via Playback'
            when round(pre_pc_viewing_duration_live_last_01_month/pre_pc_viewing_duration_last_01_month,2)=1 then '05: No duration via Playback' else '06: Other' 
end as prop_live

,sum(case when status_at_2012_12_26='AC' then 1 else 0 end) as active_2012_12_26
,sum(case when status_at_2012_12_26='AC' and (ab_events_2012_13+ pc_events_2012_13)>0 then 1 else 0 end) as active_2012_12_26_ab_pc_in_period
,sum(case when status_at_2012_12_26='AC' and (pc_events_2012_13)>0 then 1 else 0 end) as active_2012_12_26_pc_in_period
,sum(case when status_at_2012_12_26='AC' and (ab_events_2012_13)>0 then 1 else 0 end) as active_2012_12_26_ab_in_period
--,sum(pre_pc_viewing_duration_exc_premiums_last_01_month/pre_pc_last_01_month_with_viewing) as totview
from account_status_at_period_start as a
left outer join v159_previous_1_and_3_month_views as b
on a.account_number = b.account_number
left outer join v159_accounts_for_profiling_dec2012_active as c
on a.account_number = c.account_number
where country_code = 'GBR' and status_at_2012_12_26='AC' and activation_date<='2012-12-26' and vespa_weight_2012_12_26 >0
---Return at least 25 viewing days in period
and pre_pc_last_01_month_with_viewing>=25
group by prop_live
order by prop_live
;


---Pay/Free split
select case when round((pre_pc_viewing_terrestrial_last_01_month+pre_pc_viewing_free_non_terrestrial_last_01_month)/(pre_pc_viewing_duration_last_01_month),3)<0.4 then '01: 60%+ of Viewing from Pay Channels' 
            when round((pre_pc_viewing_terrestrial_last_01_month+pre_pc_viewing_free_non_terrestrial_last_01_month)/(pre_pc_viewing_duration_last_01_month),3)<0.5 then '03: >=50% and <60% of Viewing from Pay Channels' 
            when round((pre_pc_viewing_terrestrial_last_01_month+pre_pc_viewing_free_non_terrestrial_last_01_month)/(pre_pc_viewing_duration_last_01_month),3)<0.6 then '03: >=40% and <50% of Viewing from Pay Channels' 
            when round((pre_pc_viewing_terrestrial_last_01_month+pre_pc_viewing_free_non_terrestrial_last_01_month)/(pre_pc_viewing_duration_last_01_month),3)<0.7 then '04: >=30% and <40% of Viewing from Pay Channels'
            when round((pre_pc_viewing_terrestrial_last_01_month+pre_pc_viewing_free_non_terrestrial_last_01_month)/(pre_pc_viewing_duration_last_01_month),3)<0.8 then '05: >=20% and <30% of Viewing from Pay Channels'
            when round((pre_pc_viewing_terrestrial_last_01_month+pre_pc_viewing_free_non_terrestrial_last_01_month)/(pre_pc_viewing_duration_last_01_month),3)<0.9 then '06: Under 20% Viewing from Pay Channels'

    else '06: Under 20% Viewing from Pay Channels' end as pay_vs_free

,case when pre_pc_viewing_duration_last_01_month/pre_pc_last_01_month_with_viewing/3600>=9 then '01: 9hrs+ per day'
 when pre_pc_viewing_duration_last_01_month/pre_pc_last_01_month_with_viewing/3600>=7 then '02: >=7 and <9 hours per day'
 when pre_pc_viewing_duration_last_01_month/pre_pc_last_01_month_with_viewing/3600>=5 then '03: >=5 and <7 hours per day'
 when pre_pc_viewing_duration_last_01_month/pre_pc_last_01_month_with_viewing/3600>=3 then '04: >=3 and <5 hours per day'
 else '05: Under 3 Hours per day' end as total_viewing_per_day
,sum(case when status_at_2012_12_26='AC' then 1 else 0 end) as active_2012_12_26
,sum(case when status_at_2012_12_26='AC' and (ab_events_2012_13+ pc_events_2012_13)>0 then 1 else 0 end) as active_2012_12_26_ab_pc_in_period
,sum(case when status_at_2012_12_26='AC' and (pc_events_2012_13)>0 then 1 else 0 end) as active_2012_12_26_pc_in_period
,sum(case when status_at_2012_12_26='AC' and (ab_events_2012_13)>0 then 1 else 0 end) as active_2012_12_26_ab_in_period
--,sum(pre_pc_viewing_duration_exc_premiums_last_01_month/pre_pc_last_01_month_with_viewing) as totview
from account_status_at_period_start as a
left outer join v159_previous_1_and_3_month_views as b
on a.account_number = b.account_number
left outer join v159_accounts_for_profiling_dec2012_active as c
on a.account_number = c.account_number
where country_code = 'GBR' and status_at_2012_12_26='AC' and activation_date<='2012-12-26' and vespa_weight_2012_12_26 >0
---Return at least 25 viewing days in period
and pre_pc_last_01_month_with_viewing>=25
group by pay_vs_free,total_viewing_per_day
order by pay_vs_free,total_viewing_per_day
;
commit;




------3 Month View----
commit;

---Overall Viewing Last 3 months---

select case when pre_pc_viewing_duration_last_03_month/pre_pc_last_03_month_with_viewing/3600>=9 then '01: 9hrs+ per day'
 when pre_pc_viewing_duration_last_03_month/pre_pc_last_03_month_with_viewing/3600>=7 then '02: >=7 and <9 hours per day'
 when pre_pc_viewing_duration_last_03_month/pre_pc_last_03_month_with_viewing/3600>=5 then '03: >=5 and <7 hours per day'
 when pre_pc_viewing_duration_last_03_month/pre_pc_last_03_month_with_viewing/3600>=3 then '04: >=3 and <5 hours per day'
 else '05: Under 3 Hours per day' end as total_viewing_per_day
,sum(case when status_at_2012_12_26='AC' then 1 else 0 end) as active_2012_12_26
,sum(case when status_at_2012_12_26='AC' and (ab_events_2012_13+ pc_events_2012_13)>0 then 1 else 0 end) as active_2012_12_26_ab_pc_in_period
,sum(case when status_at_2012_12_26='AC' and (pc_events_2012_13)>0 then 1 else 0 end) as active_2012_12_26_pc_in_period
,sum(case when status_at_2012_12_26='AC' and (ab_events_2012_13)>0 then 1 else 0 end) as active_2012_12_26_ab_in_period
from account_status_at_period_start as a
left outer join v159_previous_1_and_3_month_views as b
on a.account_number = b.account_number
where country_code = 'GBR' and status_at_2012_12_26='AC' and activation_date<='2012-12-26' and vespa_weight_2012_12_26 >0
---Return at least 25 viewing days in period
and pre_pc_last_03_month_with_viewing>=80
group by total_viewing_per_day
order by total_viewing_per_day
;
---Add Value segment as at 2nd Dec 2012---
alter table account_status_at_period_start add value_segment varchar(20);
update account_status_at_period_start 
set value_segment = b.value_segment
from account_status_at_period_start as a
left outer join sk_prod.VALUE_SEGMENTS_FIVE_YRS as b
on a.account_number = b.account_number 
where b.value_seg_date='2012-12-02'
;

grant all on account_status_at_period_start  to public;
grant all on v159_previous_1_and_3_month_views to public;
grant all on v159_accounts_for_profiling_dec2012_active  to public;


---Create Combined Pivot of Viewing Attributes---------
commit;
--drop table v159_2012_13_pending_churn_pivot_vespa_panel_only;

----Create an Attribute Pivot for 2012/13 pending churn
select 
case when pre_pc_viewing_duration_last_01_month/pre_pc_last_01_month_with_viewing/3600>=10 then '01: 10 hrs+ per day'
 when pre_pc_viewing_duration_last_01_month/pre_pc_last_01_month_with_viewing/3600>=8 then '02: >=8 and <10 hours per day'
 when pre_pc_viewing_duration_last_01_month/pre_pc_last_01_month_with_viewing/3600>=6 then '03: >=6 and <8 hours per day'
 when pre_pc_viewing_duration_last_01_month/pre_pc_last_01_month_with_viewing/3600>=4 then '04: >=4 and <6 hours per day'
 else '05: Under 4 Hours per day' end as average_viewing_per_day
,case when pre_pc_viewing_duration_exc_premiums_last_01_month/pre_pc_last_01_month_with_viewing/3600>=4 then '01: 4hrs+ per day'
 when pre_pc_viewing_duration_exc_premiums_last_01_month/pre_pc_last_01_month_with_viewing/3600>=3 then '02: >=3 and <4 hours per day'
 when pre_pc_viewing_duration_exc_premiums_last_01_month/pre_pc_last_01_month_with_viewing/3600>=2 then '03: >=2 and <3 hours per day'
 when pre_pc_viewing_duration_exc_premiums_last_01_month/pre_pc_last_01_month_with_viewing/3600>=1 then '04: >=1 and <2 hours per day'
 else '05: Under 1 Hour per day' end as average_pay_viewing_per_day
,case when prem_sports+prem_movies=0 then '05: No Premiums' when pre_pc_viewing_premiums_last_01_month/pre_pc_last_01_month_with_viewing/3600>=2 then '01: 2hrs+ per day'
 when pre_pc_viewing_premiums_last_01_month/pre_pc_last_01_month_with_viewing/3600>=1 then '02: >=1 and <3 hours per day'
 when pre_pc_viewing_premiums_last_01_month/pre_pc_last_01_month_with_viewing/1800>=1 then '03: >=30 min and <1 hour per day'
-- when pre_pc_viewing_premiums_last_01_month/pre_pc_last_01_month_with_viewing/3600>=1 then '04: >=1 and <2 hours per day'
 else '04: Under 30 min per day' end as average_premiums_viewing_per_day

,case when  pre_pc_viewing_pay_football_last_01_month/pre_pc_last_01_month_with_viewing/60>=30 then '01: >=30 min per day'
 when  pre_pc_viewing_pay_football_last_01_month/pre_pc_last_01_month_with_viewing/60>=15 then '02: >=15 and <30 min per day'
 when  pre_pc_viewing_pay_football_last_01_month/pre_pc_last_01_month_with_viewing/60>=5 then '03: >=5 min and <15 min per day'
 when  pre_pc_viewing_pay_football_last_01_month/pre_pc_last_01_month_with_viewing/60>0 then '04: <5 min per day'
-- when pre_pc_viewing_premiums_last_01_month/pre_pc_last_01_month_with_viewing/3600>=1 then '04: >=1 and <2 hours per day'
 else '05: No Pay Football Viewing' end as average_pay_football_viewing_per_day

,case when  pre_pc_viewing_free_football_last_01_month/pre_pc_last_01_month_with_viewing/60>=30 then '01: >=5 min per day'
 when  pre_pc_viewing_free_football_last_01_month/pre_pc_last_01_month_with_viewing/60>=15 then '01: >=5 min per day'
 when  pre_pc_viewing_free_football_last_01_month/pre_pc_last_01_month_with_viewing/60>=5 then '01: >=5 min per day'
 when  pre_pc_viewing_free_football_last_01_month/pre_pc_last_01_month_with_viewing/60>0 then '02: >0 and <5 min per day'
-- when pre_pc_viewing_premiums_last_01_month/pre_pc_last_01_month_with_viewing/3600>=1 then '04: >=1 and <2 hours per day'
 else '03: No free Football Viewing' end as average_free_football_viewing_per_day

,case  when  pre_pc_viewing_pay_kids_last_01_month/pre_pc_last_01_month_with_viewing/60>=30 then '01: >=30 min per day'
 when  pre_pc_viewing_pay_kids_last_01_month/pre_pc_last_01_month_with_viewing/60>=15 then '02: >=15 and <30 min per day'
 when  pre_pc_viewing_pay_kids_last_01_month/pre_pc_last_01_month_with_viewing/60>=5 then '03: >=5 min and <15 min per day'
 when  pre_pc_viewing_pay_kids_last_01_month/pre_pc_last_01_month_with_viewing/60>0 then '04: <5 min per day'
-- when pre_pc_viewing_premiums_last_01_month/pre_pc_last_01_month_with_viewing/3600>=1 then '04: >=1 and <2 hours per day'
 else '05: No Pay Kids Viewing' end as average_pay_kids_viewing_per_day

,case  when  pre_pc_viewing_free_kids_last_01_month/pre_pc_last_01_month_with_viewing/60>=30 then '01: >=30 min per day'
 when  pre_pc_viewing_free_kids_last_01_month/pre_pc_last_01_month_with_viewing/60>=15 then '02: >=15 and <30 min per day'
 when  pre_pc_viewing_free_kids_last_01_month/pre_pc_last_01_month_with_viewing/60>=5 then '03: >=5 min and <15 min per day'
 when  pre_pc_viewing_free_kids_last_01_month/pre_pc_last_01_month_with_viewing/60>0 then '04: <5 min per day'
-- when pre_pc_viewing_premiums_last_01_month/pre_pc_last_01_month_with_viewing/3600>=1 then '04: >=1 and <2 hours per day'
 else '05: No Free kids Viewing' end as average_free_kids_viewing_per_day

,case when  pre_pc_viewing_pay_movies_last_01_month/pre_pc_last_01_month_with_viewing/60>=30 then '01: >=30 min per day'
 when  pre_pc_viewing_pay_movies_last_01_month/pre_pc_last_01_month_with_viewing/60>=15 then '02: >=15 and <30 min per day'
 when  pre_pc_viewing_pay_movies_last_01_month/pre_pc_last_01_month_with_viewing/60>=5 then '03: >=5 min and <15 min per day'
 when  pre_pc_viewing_pay_movies_last_01_month/pre_pc_last_01_month_with_viewing/60>0 then '04: <5 min per day'
-- when pre_pc_viewing_premiums_last_01_month/pre_pc_last_01_month_with_viewing/3600>=1 then '04: >=1 and <2 hours per day'
 else '05: No Pay movies Viewing' end as average_pay_movies_per_day

,case when  pre_pc_viewing_free_movies_last_01_month/pre_pc_last_01_month_with_viewing/60>=30 then '01: >=30 min per day'
 when  pre_pc_viewing_free_movies_last_01_month/pre_pc_last_01_month_with_viewing/60>=15 then '02: >=15 and <30 min per day'
 when  pre_pc_viewing_free_movies_last_01_month/pre_pc_last_01_month_with_viewing/60>=5 then '03: >=5 min and <15 min per day'
 when  pre_pc_viewing_free_movies_last_01_month/pre_pc_last_01_month_with_viewing/60>0 then '04: <5 min per day'
-- when pre_pc_viewing_premiums_last_01_month/pre_pc_last_01_month_with_viewing/3600>=1 then '04: >=1 and <2 hours per day'
 else '05: No Free movies Viewing' end as average_free_movies_viewing_per_day


,case  when pre_pc_viewing_pay_sport_exc_football_last_01_month/pre_pc_last_01_month_with_viewing/60>=30 then '01: >=30 min per day'
 when  pre_pc_viewing_pay_sport_exc_football_last_01_month/pre_pc_last_01_month_with_viewing/60>=15 then '02: >=15 and <30 min per day'
 when  pre_pc_viewing_pay_sport_exc_football_last_01_month/pre_pc_last_01_month_with_viewing/60>=5 then '03: >=5 min and <15 min per day'
 when  pre_pc_viewing_pay_sport_exc_football_last_01_month/pre_pc_last_01_month_with_viewing/60>0 then '04: <5 min per day'
-- when pre_pc_viewing_premiums_last_01_month/pre_pc_last_01_month_with_viewing/3600>=1 then '04: >=1 and <2 hours per day'
 else '05: No Pay Sports Viewing' end as average_pay_sport_exc_football_viewing_per_day

,case when  pre_pc_viewing_free_sport_exc_football_last_01_month/pre_pc_last_01_month_with_viewing/60>=30 then '01: >=30 min per day'
 when  pre_pc_viewing_free_sport_exc_football_last_01_month/pre_pc_last_01_month_with_viewing/60>=15 then '02: >=15 and <30 min per day'
 when  pre_pc_viewing_free_sport_exc_football_last_01_month/pre_pc_last_01_month_with_viewing/60>=5 then '03: >=5 min and <15 min per day'
 when  pre_pc_viewing_free_sport_exc_football_last_01_month/pre_pc_last_01_month_with_viewing/60>=1 then '04: >=1 and <5 min per day'
-- when pre_pc_viewing_premiums_last_01_month/pre_pc_last_01_month_with_viewing/3600>=1 then '04: >=1 and <2 hours per day'
 else '05: No Free Sports Viewing' end as average_free_sport_exc_football_viewing_per_day

,case when round(pre_pc_viewing_duration_live_last_01_month/pre_pc_viewing_duration_last_01_month,2)<=0.70 then '01: 30%+ duration via Playback'
            when round(pre_pc_viewing_duration_live_last_01_month/pre_pc_viewing_duration_last_01_month,2)<=0.80 then '02: >= 20% and <30% duration via Playback'
            when round(pre_pc_viewing_duration_live_last_01_month/pre_pc_viewing_duration_last_01_month,2)<=0.90 then '03: >= 10% and <20% duration via Playback'
            when round(pre_pc_viewing_duration_live_last_01_month/pre_pc_viewing_duration_last_01_month,2)<1 then '04: Under 10% duration via Playback'
            when round(pre_pc_viewing_duration_live_last_01_month/pre_pc_viewing_duration_last_01_month,2)=1 then '05: No duration via Playback' else '06: Other' 
end as proportion_viewing_via_playback

,case when round((pre_pc_viewing_terrestrial_last_01_month+pre_pc_viewing_free_non_terrestrial_last_01_month)/(pre_pc_viewing_duration_last_01_month),3)<0.4 then '01: 60%+ of Viewing from Pay Channels' 
            when round((pre_pc_viewing_terrestrial_last_01_month+pre_pc_viewing_free_non_terrestrial_last_01_month)/(pre_pc_viewing_duration_last_01_month),3)<0.5 then '02: >=50% and <60% of Viewing from Pay Channels' 
            when round((pre_pc_viewing_terrestrial_last_01_month+pre_pc_viewing_free_non_terrestrial_last_01_month)/(pre_pc_viewing_duration_last_01_month),3)<0.6 then '03: >=40% and <50% of Viewing from Pay Channels' 
            when round((pre_pc_viewing_terrestrial_last_01_month+pre_pc_viewing_free_non_terrestrial_last_01_month)/(pre_pc_viewing_duration_last_01_month),3)<0.7 then '04: >=30% and <40% of Viewing from Pay Channels'
            when round((pre_pc_viewing_terrestrial_last_01_month+pre_pc_viewing_free_non_terrestrial_last_01_month)/(pre_pc_viewing_duration_last_01_month),3)<0.8 then '05: >=20% and <30% of Viewing from Pay Channels'
            when round((pre_pc_viewing_terrestrial_last_01_month+pre_pc_viewing_free_non_terrestrial_last_01_month)/(pre_pc_viewing_duration_last_01_month),3)<0.9 then '06: Under 20% Viewing from Pay Channels'

    else '06: Under 20% Viewing from Pay Channels' end as pay_viewing_proportion

,case when own_rent_status = '0' then 'a) Owner occupied'
when own_rent_status = '1' then 'b) Privately rented'
when own_rent_status = '2' then 'c) Council / housing association' else 'd) Unknown' end as own_rent_type
,case  
        when num_children_in_hh in ('1','2','3','4') then 1 else 0 end any_kids_in_hh

,case  when residence_type IN ('0')       THEN '1) Detached'
        when residence_type IN ('1')       THEN '2) Semi-detached'
        when residence_type IN ('2')       THEN '3) Bungalow'
        when residence_type IN ('3')       THEN '4) Terraced'
        when residence_type IN ('4')       THEN '5) Flat'
        when residence_type IN ('U')       THEN '6) Unclassified' else '6) Unclassified'
                                                END as property_type 
,  case         WHEN hh_affluence IN ('00','01','02')       THEN 'A) Very Low'
                                                WHEN hh_affluence IN ('03','04', '05')      THEN 'B) Low'
                                                WHEN hh_affluence IN ('06','07','08')       THEN 'C) Mid Low'
                                                WHEN hh_affluence IN ('09','10','11')       THEN 'D) Mid'
                                                WHEN hh_affluence IN ('12','13','14')       THEN 'E) Mid High'
                                                WHEN hh_affluence IN ('15','16','17')       THEN 'F) High'
                                                WHEN hh_affluence IN ('18','19')            THEN 'G) Very High' else 'H) Unknown' END as affluence
,case when prem_sports=2 and prem_movies=2 then 'a) All Premiums'
                                   when prem_sports=2 then 'b) Duel Sports'
                                   when prem_movies=2 then 'c) Duel Movies'
                                   when prem_sports+prem_movies>0 then 'd) Other Premiums' else 'e) No Premiums' end as premiums_type
,mixes_type
,case   when bb_type<>'NA' and talk_product<>'NA' then 'a) BB/Talk/TV'
        when bb_type<>'NA' then 'b) BB/TV'
        when talk_product<>'NA' then 'c) Talk/TV'
        else 'd) TV Only' end as bb_talk_holdings
, case when hdtv is null then 0 else hdtv end as has_hdtv
,case when multiroom is null then 0 else multiroom end as has_multiroom
,case when skyplus is null then 0 else skyplus end as has_skyplus
,case when distinct_usage_days is null then 'a) not used Sky go in previous 3 months'
when distinct_usage_days =0 then 'a) not used Sky go in previous 3 months'
when distinct_usage_days <=5 then 'b) Used Sky go 1-5 days in 3mth period'
when distinct_usage_days >5 then 'c) Used Sky go 6+ days in 3mth period' else 'd) Other' end as Sky_go_usage_last_3m
,case when length_of_residency in ('00','01') then 'a) <2 Years'
when length_of_residency in ('02') then 'b) 2 Years'
when length_of_residency in ('03','04','05') then 'c) 3-5 Years'
when length_of_residency in ('06','07','08','09','10','11') then 'd) 6+ Years' else 'e) Unknown' end as residency_length


,CASE  when value_segment = 'Bedding In'  THEN 'a) <=2 Years' WHEN datediff(day,activation_date,cast('2012-12-25' as date)) <=  365 THEN 'a) <=2 Years'
                         WHEN datediff(day,activation_date,cast('2012-12-25' as date)) <=  730 THEN 'a) <=2 Years'
                         WHEN datediff(day,activation_date,cast('2012-12-25' as date)) <= 1825 THEN 'b) >2 Years and <=5 Years'
                         WHEN datediff(day,activation_date,cast('2012-12-25' as date)) <= 3650 THEN 'c) >5 Years and <=10 Years'
                         WHEN datediff(day,activation_date,cast('2012-12-25' as date)) > 3650 THEN  'd) 10 Years+'
                         ELSE 'e) Unknown' end as tenure_at_dec_25_2012
,case when own_rent_status = '0' 
            and datediff(day,activation_date,cast('2012-12-25' as date)) > 3650 
            and  prem_sports+prem_movies>0
            and hh_affluence IN ('12','13','14','15','16','17','18','19')  then '01: Affluent Hard Core Fan'
        when datediff(day,activation_date,cast('2012-12-25' as date)) > 3650  then '02: Long Term Fan'
        when datediff(day,activation_date,cast('2012-12-25' as date))  <=  730 then '03: Under 2 yr Tenure'
        when own_rent_status = '0' 
            and hh_affluence  IN ('12','13','14','15','16','17','18','19')  then '04: Comfortable Homeowner'
        when own_rent_status = '0' 
            and hh_affluence not IN ('12','13','14','15','16','17','18','19')  then '05: Stretched Homeowner'
        when own_rent_status not in ('0') then '06: Non-Homeowner' else '07: Unknown'
end as account_segment
,case when value_segment = 'Platinum' then '01: Platinum'
      when value_segment = 'Gold' then '02: Gold'
      when value_segment = 'Silver' then '03: Silver'
      when value_segment = 'Bronze' then '04: Bronze'
      when value_segment = 'Copper' then '05: Copper'
      when value_segment = 'Bedding In' then '06: Bedding In'
      when value_segment = 'Unstable' then '07: Unstable' else '07: Unstable' end as value_segments
,case when value_segment = 'Unstable' then '07: Unstable'
when own_rent_status = '0' 
            and datediff(day,activation_date,cast('2012-12-25' as date)) > 3650 
            and  prem_sports+prem_movies>0
            and hh_affluence IN ('12','13','14','15','16','17','18','19')  then '01: Affluent Hard Core Fan'
        when datediff(day,activation_date,cast('2012-12-25' as date)) > 3650  then '02: Long Term Fan'
        when value_segment = 'Bedding In' then '03: Under 2 yr Tenure'
        when datediff(day,activation_date,cast('2012-12-25' as date))  <=  730 then '03: Under 2 yr Tenure'
        when own_rent_status = '0' 
            and hh_affluence  IN ('12','13','14','15','16','17','18','19')  then '04: Comfortable Homeowner'
        when own_rent_status = '0' 
            and hh_affluence not IN ('12','13','14','15','16','17','18','19')  then '05: Stretched Homeowner'
        when own_rent_status not in ('0') then '06: Non-Homeowner' else '08: Unknown'
end 
 as account_value_segment_combined
,sum(case when status_at_2012_12_26='AC' then 1 else 0 end) as active_2012_12_26
,sum(case when status_at_2012_12_26='AC' and (ab_events_2012_13+ pc_events_2012_13)>0 then 1 else 0 end) as active_2012_12_26_ab_pc_in_period
,sum(case when status_at_2012_12_26='AC' and (pc_events_2012_13)>0 then 1 else 0 end) as active_2012_12_26_pc_in_period
,sum(case when status_at_2012_12_26='AC' and (ab_events_2012_13)>0 then 1 else 0 end) as active_2012_12_26_ab_in_period
into v159_2012_13_pending_churn_pivot_vespa_panel_only
from account_status_at_period_start as a
left outer join v159_accounts_for_profiling_dec2012_active as b
on a.account_number = b.account_number
left outer join v159_previous_1_and_3_month_views as c
on a.account_number = c.account_number
where country_code = 'GBR' and activation_date<='2012-12-26' and vespa_weight_2012_12_26 >0
---Return at least 25 viewing days in period
and pre_pc_last_01_month_with_viewing>=25
group by 
average_premiums_viewing_per_day
,average_pay_viewing_per_day
,average_viewing_per_day
,average_pay_football_viewing_per_day
,average_free_football_viewing_per_day
,average_pay_kids_viewing_per_day
,average_free_kids_viewing_per_day
,average_pay_movies_per_day
,average_free_movies_viewing_per_day
,average_pay_sport_exc_football_viewing_per_day
,average_free_sport_exc_football_viewing_per_day
,proportion_viewing_via_playback
,pay_viewing_proportion
,own_rent_type
,any_kids_in_hh
, property_type 
,affluence
, premiums_type
,mixes_type
, bb_talk_holdings
, has_hdtv
,has_multiroom
,has_skyplus
,Sky_go_usage_last_3m
,residency_length
,account_segment
,value_segments
,account_value_segment_combined
, tenure_at_dec_25_2012
;
commit;

grant all on v159_2012_13_pending_churn_pivot_vespa_panel_only to public;
commit;

grant all on dbarnett.v159_2012_13_pending_churn_pivot_vespa_panel_only to public;commit;

--select distinct value_segment from account_status_at_period_start;

/*
select account_segment , count(*) from  v159_2012_13_pending_churn_pivot_vespa_panel_only group by account_segment order by account_segment
select * from dbarnett.v159_2012_13_pending_churn_pivot_vespa_panel_only;
select count(*) from dbarnett. v141_churn_pivot_output_with_channel_splits;

select top 100 * from sk_prod.VALUE_SEGMENTS_FIVE_YRS ;commit;

select value_seg_date , count(*) as records from sk_prod.VALUE_SEGMENTS_FIVE_YRS group by value_seg_date order by value_seg_date
*/


/*
select top 100 * from  V159_account_level_viewing_summary as a
select top 100 * from V159_Daily_viewing_summary as b
,case when status_at_2012_12_26='AC' and vespa_weight_2012_12_26>0 then vespa_weight_2012_12_26 else 0 end as active_2012_12_26_on_vespa_weighted
,case when status_at_2012_12_26='AC' and vespa_weight_2012_12_26>0 and (ab_events_2012_13+ pc_events_2012_13)>0 then vespa_weight_2012_12_26 else 0 end as active_2012_12_26_ab_pc_in_period_on_vespa_weighted
,case when status_at_2012_12_26='AC' and vespa_weight_2012_12_26>0 and (pc_events_2012_13)>0 then vespa_weight_2012_12_26 else 0 end as active_2012_12_26_pc_in_period_on_vespa_weighted
,case when status_at_2012_12_26='AC' and vespa_weight_2012_12_26>0 and (ab_events_2012_13)>0 then vespa_weight_2012_12_26 else 0 end as active_2012_12_26_ab_in_period_on_vespa_weighted
*/


--drop table v159_;
/*
select length_of_residency
,count(*)
from  account_status_at_period_start as a
left outer join v159_accounts_for_profiling_dec2012_active as b
on a.account_number = b.account_number
left outer join V159_Tenure_10_16mth_Viewing_summary as c
on a.account_number = c.account_number
where country_code = 'GBR' and activation_date<='2012-12-26' and distinct_days_viewing>=70
group by length_of_residency
order by length_of_residency

select max(event_start_date_time_utc) from sk_prod.VESPA_AP_PROG_VIEWED_201303 -- sk_prod.vespa_events_all

select top 100 * from sk_prod.VESPA_AP_PROG_VIEWED_201303

select          cast(broadcast_start_date_time_utc as date) as txdt,count(*)
from            sk_prod.vespa_programme_schedule as epg
group by txdt  order by txdt
commit;

select count(distinct account_number) from vespa_analysts.ph1_VESPA_DAILY_AUGS_20120701 

commit;
select * from vespa_analysts.sc2_metrics order by 1;



select  round((pre_pc_viewing_terrestrial_last_01_month+pre_pc_viewing_free_non_terrestrial_last_01_month)/(pre_pc_viewing_duration_last_01_month),2) as pay_vs_free

,sum(case when status_at_2012_12_26='AC' then 1 else 0 end) as active_2012_12_26
,sum(case when status_at_2012_12_26='AC' and (ab_events_2012_13+ pc_events_2012_13)>0 then 1 else 0 end) as active_2012_12_26_ab_pc_in_period
,sum(case when status_at_2012_12_26='AC' and (pc_events_2012_13)>0 then 1 else 0 end) as active_2012_12_26_pc_in_period
,sum(case when status_at_2012_12_26='AC' and (ab_events_2012_13)>0 then 1 else 0 end) as active_2012_12_26_ab_in_period
--,sum(pre_pc_viewing_duration_exc_premiums_last_01_month/pre_pc_last_01_month_with_viewing) as totview
from account_status_at_period_start as a
left outer join v159_previous_1_and_3_month_views as b
on a.account_number = b.account_number
left outer join v159_accounts_for_profiling_dec2012_active as c
on a.account_number = c.account_number
where country_code = 'GBR' and status_at_2012_12_26='AC' and activation_date<='2012-12-26' and vespa_weight_2012_12_26 >0
---Return at least 25 viewing days in period
and pre_pc_last_01_month_with_viewing>=25
group by pay_vs_free
order by pay_vs_free
;
commit;

*/