-- okay, so we're going to break this out into a table with updates so we can build the
-- waterfall thing that we want... flags are flat in this table, might make the cascading
-- slightly tricky, but most likely just verbose. We're using bit=1 to denote that an
-- account passes each particular test, so the good panel at the end will be all 1's.

-- Sep2012 update - rearranging slightly


if object_id('VirtPan_03_Waterfall_Base') is not null then drop table VirtPan_03_Waterfall_Base end if;
select
       sav.account_number
      ,min(case when sav.PROD_LATEST_DTV_STATUS = 'Active'        then 1 else 0 end) as l07_prod_latest_dtv
      ,min(case when sav.PTY_COUNTRY_CODE = 'GBR'                 then 1 else 0 end) as l08_country
      ,min(case when sav.CUST_VIEWING_DATA_CAPTURE_ALLOWED = 'Y'  then 1 else 0 end) as l09_data_capture
      ,min(case when sav.CB_NAME_SURNAME IS NOT NULL and
                     sav.CB_NAME_SURNAME <> ''                    then 1 else 0 end) as l10_surname
      ,min(case when (sav.CUST_PERSON_TYPE IN ('STD','?') OR sav.CUST_PERSON_TYPE IS NULL) and
                      sav.ACCT_TYPE = 'Standard'                  then 1 else 0 end) as l11_standard_accounts
      ,cast(0 as bit)                                                                as l12_TSA_opt_in
      ,cast(0 as bit)                                                                as l13_hibernators
      ,cast(0 as bit)                                                                as l14_not_vespa_panel
      ,cast(0 as bit)                                                                as l15_sky_view_panel
      ,cast(0 as bit)                                                                as l22_known_prefix
      ,cast(0 as bit)                                                                as l23_empty_prefix
      ,cast(0 as bit)                                                                as l24_last_callback_dt
      ,cast(0 as bit)                                                                as l28_HD_box
      ,cast(0 as bit)                                                                as l29_callback_day_in_range
      ,cast(0 as bit)                                                                as l30_ondemand_downloads
      ,cast(0 as decimal(10,1))                                                      as knockout_level
  into VirtPan_03_Waterfall_Base
  from sk_prod.CUST_SINGLE_ACCOUNT_VIEW as sav
 where sav.CUST_ACTIVE_DTV = 1
 group by sav.account_number;
commit;

create unique hg index idx1 on VirtPan_03_Waterfall_Base(account_number);
create lf index idx2 on VirtPan_03_Waterfall_Base(knockout_level);


  -- ##### Account based rules #####
  -- Rule 12
update VirtPan_03_Waterfall_Base
   set l12_TSA_opt_in = 1
 where ACCOUNT_NUMBER NOT IN
        ( SELECT SAM_REGISTRANT.ACCOUNT_NUMBER
            FROM sk_prod.SAM_REGISTRANT
           WHERE SAM_REGISTRANT.TSA_OPT_IN = 'N'
             AND SAM_REGISTRANT.ACCOUNT_NUMBER IS NOT NULL
        );
commit;


  -- Rule 13
update VirtPan_03_Waterfall_Base
   set l13_hibernators = 1
 where ACCOUNT_NUMBER not IN (select account_number from sk_prod.VESPA_PANEL_HIBERNATIONS);
commit;


  -- Rule 14:
update VirtPan_03_Waterfall_Base
   set l14_not_vespa_panel = 1
 where ACCOUNT_NUMBER NOT IN (SELECT VESPA_PANEL_STATUS.ACCOUNT_NUMBER FROM sk_prod.VESPA_PANEL_STATUS WHERE VESPA_PANEL_STATUS.PANEL_NO in (6, 7, 12));
commit;


  -- Rule 15:
update VirtPan_03_Waterfall_Base
   set l15_sky_view_panel = 1
 where ACCOUNT_NUMBER NOT IN (SELECT ACCOUNT_NUMBER FROM sk_prod.VESPA_SKY_VIEW_PANEL);
commit;


  -- Rule 30 - Anytime+ downloads
update VirtPan_03_Waterfall_Base as bas
   set l30_ondemand_downloads = 1
  from (select
              account_number,
              min(last_modified_dt) as first_dl_date,
              max(last_modified_dt) as last_dl_date
          from sk_prod.CUST_ANYTIME_PLUS_DOWNLOADS
         -- where x_content_type_desc = 'PROGRAMME'  --  to exclude trailers
         --  and x_actual_downloaded_size_mb > 1     -- to exclude any spurious header/trailer download records
         group by account_number) as apd
 where bas.account_number = apd.account_number;
commit;





  -- ##### Box based rules #####
  -- Get account-sub Id link
if object_id('VirtPan_04_Box_Rules') is not null then drop table VirtPan_04_Box_Rules end if;
select
      account_number,
      cast(card_subscriber_id as int) as subscriber_id,
      service_instance_id
  into VirtPan_04_Box_Rules
  from sk_prod.cust_card_subscriber_link
 where current_flag = 'Y'
 group by account_number, card_subscriber_id, service_instance_id;
commit;

create hg index idx1 on VirtPan_04_Box_Rules(account_number);


  -- Get Subscriber Ids allocated to multiple Account Numbers
if object_id('VirtPan_tmp_Ambiguous_Sub_Ids') is not null then drop table VirtPan_tmp_Ambiguous_Sub_Ids end if;
select
      subscriber_id,
      count(*) as cnt
  into VirtPan_tmp_Ambiguous_Sub_Ids
  from VirtPan_04_Box_Rules
 group by subscriber_id having count(*) > 1;
commit;
create unique hg index idx2 on VirtPan_tmp_Ambiguous_Sub_Ids(subscriber_id);

  -- Delete these accounts
delete from VirtPan_04_Box_Rules
 where account_number in (select
                                account_number
                            from VirtPan_tmp_Ambiguous_Sub_Ids a,
                                 VirtPan_04_Box_Rules b
                           where a.subscriber_id = b.subscriber_id);
commit;


create unique hg index idx2 on VirtPan_04_Box_Rules(subscriber_id);
create hg index idx3 on VirtPan_04_Box_Rules(service_instance_id);


  -- Apend box-based rules now
alter table VirtPan_04_Box_Rules
  add (known_prefix        bit default 0,
       empty_prefix        bit default 0,
       last_callback_dt    bit default 0,
       HD_box              bit default 0,
       callback_day_in_range bit default 0);


  -- prefixes - null or empty (Rules 22 & 23)
update VirtPan_04_Box_Rules as bas
   set bas.known_prefix       = case when cb.subscriber_id is not null then 1 else 0 end,           -- Callback record prosent for the box
       bas.empty_prefix       = case when trim(prefix) = '' or prefix is null then 1 else 0 end
  from VirtPan_01_callback_data  cb
 where bas.subscriber_id = cb.subscriber_id
   and cb.callback_seq = 1;
commit;


  -- last_callback_dt in the last 6 months (Rule 24)
create variable @varLastCallbackDate date;
select @varLastCallbackDate = max(dt) from VirtPan_01_callback_data where dt < now();


/*
  -- Dialback L6m - raw dialback files
update VirtPan_04_Box_Rules as bas
   set last_callback_dt = 0;
commit;

update VirtPan_04_Box_Rules as bas
   set last_callback_dt = 1
  from (select
              subscriber_id
          from VirtPan_01_callback_data
         where dt >= @varLastCallbackDate - 180
           and dt <= @varLastCallbackDate
           and dt is not null
         group by subscriber_id) gol
 where bas.subscriber_id = gol.subscriber_id
commit;
*/


  -- Dialback L6m - summary file
update VirtPan_04_Box_Rules as bas
   set last_callback_dt = 0;
commit;

update VirtPan_04_Box_Rules as bas
   set last_callback_dt = 1
  from (select
              subscriber_id
          from VirtPan_02_SCMS_callback_data
         where Missing_Cbcks < Expected_Cbcks
         group by subscriber_id) gol
 where bas.subscriber_id = gol.subscriber_id
commit;


  -- HD box in  (Rule 28)
if object_id('VirtPan_tmp_HD_boxes') is not null then drop table VirtPan_tmp_HD_boxes end if;
select
      a.subscriber_id,
      a.service_instance_id,
      1 as HD_Box
  into VirtPan_tmp_HD_boxes
  from VirtPan_04_Box_Rules a,
       (select service_instance_id
              ,rank () over (partition by service_instance_id order by ph_non_subs_link_sk desc) rank
          from sk_prod.cust_Set_top_box
         where x_box_type like '%HD%') b
 where a.service_instance_id = b.service_instance_id
   and b.rank = 1;
commit;

create unique hg index idx2 on VirtPan_tmp_HD_boxes(subscriber_id);

update VirtPan_04_Box_Rules as bas
   set bas.HD_box = b.HD_Box
  from VirtPan_tmp_HD_boxes b
 where bas.subscriber_id = b.subscriber_id
commit;


  -- Callback days in range (Rule 29)
update VirtPan_04_Box_Rules as bas
   set callback_day_in_range  = 0;      -- Reset for re-runs
commit;

update VirtPan_04_Box_Rules as bas
   set callback_day_in_range  = case when cbk_day between 9 and 28 then 1 else 0 end
  from VirtPan_02_SCMS_callback_data  gol
 where bas.subscriber_id = gol.subscriber_id;
commit;





  -- #### Apply box infor & apply rules to accoutns ####
update VirtPan_03_Waterfall_Base base
   set base.l22_known_prefix        = det.known_prefix,
       base.l23_empty_prefix        = det.empty_prefix,
       base.l24_last_callback_dt    = det.last_callback_dt,
       base.l28_HD_box              = det.HD_box,
       base.l29_callback_day_in_range = det.callback_day_in_range
  from (select
              account_number,
              min(known_prefix)      as known_prefix,         -- ALL boxes must have dial back information
              min(empty_prefix)      as empty_prefix,         -- ALL boxes must have empty prefix
              min(last_callback_dt)  as last_callback_dt,     -- ALL boxes must dial back between selected days
              max(HD_box)            as HD_box,               -- at least one box for account
              max(callback_day_in_range) as callback_day_in_range     -- at least one box must dial back between selected days
          from VirtPan_04_Box_Rules
         group by account_number) det
 where base.account_number = det.account_number;
commit;


/*
  -- #### Check overlap between callback files & waterfall base ####
  -- Individual files
select
      case
        when b.account_number is null then 'Base only'
        when a.account_number is null then 'Callback files'
          else 'Both'
      end as Src,
      count(*) as Cnt
  from VirtPan_03_Waterfall_Base a full join (select
                                                    account_number
                                                from VirtPan_01_callback_data
                                               group by account_number) b
    on a.account_number = b.account_number
 group by Src;


  -- Summary file
select
      case
        when b.account_number is null then 'Base only'
        when a.account_number is null then 'Callback summary'
          else 'Both'
      end as Src,
      count(*) as Cnt
  from VirtPan_03_Waterfall_Base a full join (select
                                                    account_number
                                                from VirtPan_02_SCMS_callback_data
                                               group by account_number) b
    on a.account_number = b.account_number
 group by Src;

  -- Between callback files
select
      case
        when b.account_number is null then 'Callback files'
        when a.account_number is null then 'Callback summary'
          else 'Both'
      end as Src,
      count(*) as Cnt
  from (select
              account_number
          from VirtPan_01_callback_data
         group by account_number) a
       full join
       (select
              account_number
          from VirtPan_02_SCMS_callback_data
         group by account_number) b
    on a.account_number = b.account_number
 group by Src;

*/


  -- #### Waterfall results ####
update VirtPan_03_Waterfall_Base
   set knockout_level  = case
                           when l07_prod_latest_dtv        = 0 then 7
                           when l08_country                = 0 then 8
                           when l09_data_capture           = 0 then 9
                           when l10_surname                = 0 then 10
                           when l11_standard_accounts      = 0 then 11
                           when l12_TSA_opt_in             = 0 then 12
                           when l13_hibernators            = 0 then 13
                           when l14_not_vespa_panel        = 0 then 14
                           when l15_sky_view_panel         = 0 then 15
                           when l22_known_prefix           = 0 then 22
                           when l23_empty_prefix           = 0 then 23
                           when l24_last_callback_dt       = 0 then 24
                           when l28_hd_box                 = 0 then 28
                           when l29_callback_day_in_range  = 0 then 29
                           when l30_ondemand_downloads     = 0 then 30
                             else 9999 -- pass!
                         end;
commit;


  -- Drop at each stage
select
      knockout_level,
      count(*) as Drop_Out
  from VirtPan_03_Waterfall_Base
 group by knockout_level
 order by knockout_level;


  -- Accounts meeting criteria
select
      count(*)                                as Total_Volume,
      count(*) - sum(l07_prod_latest_dtv)     as l07_prod_latest_dtv,
      count(*) - sum(l08_country)             as l08_country,
      count(*) - sum(l09_data_capture)        as l09_data_capture,
      count(*) - sum(l10_surname)             as l10_surname,
      count(*) - sum(l11_standard_accounts )  as l11_standard_accounts,
      count(*) - sum(l12_TSA_opt_in)          as l12_TSA_opt_in,
      count(*) - sum(l13_hibernators)         as l13_hibernators,
      count(*) - sum(l14_not_vespa_panel)     as l14_not_vespa_panel,
      count(*) - sum(l15_sky_view_panel)      as l15_sky_view_panel,
      count(*) - sum(l22_known_prefix)        as l22_known_prefix,
      count(*) - sum(l23_empty_prefix)        as l23_empty_prefix,
      count(*) - sum(l24_last_callback_dt)    as l24_last_callback_dt,
      count(*) - sum(l28_hd_box)              as l28_hd_box,
      count(*) - sum(l29_callback_day_in_range) as l29_callback_day_in_range,
      count(*) - sum(l30_ondemand_downloads)  as l30_ondemand_downloads
  from VirtPan_03_Waterfall_Base;




  -- ##### Load sent list of accounts to table structures #####
if object_id('VirtPan_10_Selection_20130301_JG') is not null then drop table VirtPan_10_Selection_20130301_JG end if;
create table VirtPan_10_Selection_20130301_JG
  (row_id         bigint identity,
   Account_Number varchar(50),
   Callback_Day   tinyint
);

create unique hg index idx1 on VirtPan_10_Selection_20130301_JG(Account_Number);

truncate table VirtPan_10_Selection_20130301_JG;
commit;
load table VirtPan_10_Selection_20130301_JG
(
  Account_Number',',
  Callback_Day'\n'
)
from '/ETL013/prod/sky/olive/data/share/clarityq/export/Decision_Sciences/Sebastian/Broadcaster reporting - 2013-03-05/panel_adds_Feb2013.csv'
QUOTES OFF
ESCAPES OFF
NOTIFY 200000
DELIMITED BY ','
SKIP 1
;



if object_id('VirtPan_11_Selection_20130301_Disablement_List') is not null then drop table VirtPan_11_Selection_20130301_Disablement_List end if;
create table VirtPan_11_Selection_20130301_Disablement_List
  (row_id         bigint identity,
   Account_Number varchar(50)
);

create unique hg index idx1 on VirtPan_11_Selection_20130301_Disablement_List(Account_Number);

truncate table VirtPan_11_Selection_20130301_Disablement_List;
commit;
load table VirtPan_11_Selection_20130301_Disablement_List
(
  Account_Number'\n'
)
from '/ETL013/prod/sky/olive/data/share/clarityq/export/Decision_Sciences/Sebastian/Broadcaster reporting - 2013-03-05/Selection - 2013-03-05 Disablement file.csv'
QUOTES OFF
ESCAPES OFF
NOTIFY 200000
DELIMITED BY ','
SKIP 1
;
update VirtPan_11_Selection_20130301_Disablement_List
   set account_number = replace(account_number, '\x0d', '');
commit;



if object_id('VirtPan_12_Selection_20130301_40k_Top_Up') is not null then drop table VirtPan_12_Selection_20130301_40k_Top_Up end if;
create table VirtPan_12_Selection_20130301_40k_Top_Up
  (row_id         bigint identity,
   Account_Number varchar(50),
   Callback_Day   tinyint
);

create unique hg index idx1 on VirtPan_12_Selection_20130301_40k_Top_Up(Account_Number);

truncate table VirtPan_12_Selection_20130301_40k_Top_Up;
commit;
load table VirtPan_12_Selection_20130301_40k_Top_Up
(
  Account_Number',',
  Callback_Day'\n'
)
from '/ETL013/prod/sky/olive/data/share/clarityq/export/Decision_Sciences/Sebastian/Broadcaster reporting - 2013-03-05/Selection - 2013-03-05 40k top up (dialback days 9-28) - revised.csv'
QUOTES OFF
ESCAPES OFF
NOTIFY 200000
DELIMITED BY ','
SKIP 1
;



  -- Final table
if object_id('VirtPan_13_Selection_20130301_Final') is not null then drop table VirtPan_13_Selection_20130301_Final end if;
create table VirtPan_13_Selection_20130301_Final
  (row_id         bigint identity,
   Account_Number varchar(50),
   Callback_Day   tinyint
);

create unique hg index idx1 on VirtPan_13_Selection_20130301_Final(Account_Number);

insert into VirtPan_13_Selection_20130301_Final (Account_Number, Callback_Day)
  select
        Account_Number,
        Callback_Day
    from VirtPan_10_Selection_20130301_JG;
commit;

delete from VirtPan_13_Selection_20130301_Final
 where Account_Number in (select Account_Number from VirtPan_11_Selection_20130301_Disablement_List);
commit;

insert into VirtPan_13_Selection_20130301_Final (Account_Number, Callback_Day)
  select
        Account_Number,
        Callback_Day
    from VirtPan_12_Selection_20130301_40k_Top_Up;
commit;


























