-- okay, so we're going to break this out into a table with updates so we can build the
-- waterfall thing that we want... flags are flat in this table, might make the cascading
-- slightly tricky, but most likely just verbose. We're using bit=1 to denote that an
-- account passes each particular test, so the good panel at the end will be all 1's.

-- Sep2012 update - rearranging slightly













drop table PanMan_adhoc_waterfall_base;

--create the base table with some flags
  select sav.account_number
        ,service_instance_id
        ,cast(0 as varchar) as subscriber_id --it doesn't like updating as an integer field for some reason
        ,convert(bit,1)                                                          as l06_active_dtv         -- CUST_ACTIVE_DTV in the filer instead, it's the first of the checks
        ,case when sav.PROD_LATEST_DTV_STATUS = 'Active'       then 1 else 0 end as l07_prod_latest_dtv
        ,case when sav.PTY_COUNTRY_CODE = 'GBR'                then 1 else 0 end as l08_country
        ,case when sav.CUST_VIEWING_DATA_CAPTURE_ALLOWED = 'Y' then 1 else 0 end as l09_data_capture
        ,case when sav.CB_NAME_SURNAME IS NOT NULL             then 1 else 0 end as l10_surname
        ,case when (sav.CUST_PERSON_TYPE IN ('STD','?') OR sav.CUST_PERSON_TYPE IS NULL)
                   AND sav.ACCT_TYPE = 'Standard'              then 1 else 0 end as l11_standard_accounts
        ,convert(bit,0)                                                          as l12_TSA_opt_in
        ,convert(bit,0)                                                          as l13_hibernators
        ,convert(bit,0)                                                          as l14_panel_4
        ,convert(bit,0)                                                          as l15_sky_view_panel
        ,convert(bit,0)                                                          as l16_multi_dial_supression
        ,convert(bit,0)                                                          as l18_onnet
        ,convert(bit,0)                                                          as l22_active_box_flag
        ,convert(bit,0)                                                          as null_prefix        --23.1
        ,convert(bit,0)                                                          as empty_prefix       --23.2
        ,convert(bit,0)                                                          as l24_last_callback_dt
        ,cast(null  as varchar(10))                                              as expected_callbacks --25.1
        ,cast(null  as varchar(10))                                              as ontime_callbacks   --25.2
        ,cast(null  as varchar(10))                                              as missing_callbacks  --25.3
        ,cast(0 as bit)                                                          as anytime_plus       --26
        ,cast(0 as bit)                                                          as l28_HD_box
        ,cast(0 as bit)                                                          as l29_day1to5
        ,convert(bit,0)                                                          as ondemand_downloads --30
        ,cast (0 as decimal(10,1))                                               as knockout_level
    into PanMan_adhoc_waterfall_base
    from sk_prod.CUST_SINGLE_ACCOUNT_VIEW as sav
         inner join sk_prod.cust_set_top_box as stb on sav.account_number = stb.account_number
   where sav.CUST_ACTIVE_DTV = 1
     and box_replaced_dt = '9999-09-09'
; --13,469,344

-- account_number probably not even unique...
commit;
create index fake_pk on PanMan_adhoc_waterfall_base (account_number);
create index for_grouping on PanMan_adhoc_waterfall_base (knockout_level);
create index idx_service_instance_id_hg on PanMan_adhoc_waterfall_base (service_instance_id);

update PanMan_adhoc_waterfall_base as bas
   set subscriber_id = si_external_identifier
  from sk_prod.cust_service_instance as csi
 where bas.service_instance_id = csi.src_system_id
;

--add in the values we can work out
-- line 12...
update PanMan_adhoc_waterfall_base
set l12_TSA_opt_in = 1
where ACCOUNT_NUMBER NOT IN
    ( SELECT SAM_REGISTRANT.ACCOUNT_NUMBER FROM sk_prod.SAM_REGISTRANT WHERE
        (SAM_REGISTRANT.TSA_OPT_IN = 'N')
     AND
        (SAM_REGISTRANT.ACCOUNT_NUMBER IS NOT NULL)
    )
; --12693994

-- line 14:
update PanMan_adhoc_waterfall_base
set l14_panel_4 = 1
where ACCOUNT_NUMBER NOT IN
    (SELECT VESPA_PANEL_STATUS.ACCOUNT_NUMBER FROM sk_prod.VESPA_PANEL_STATUS WHERE VESPA_PANEL_STATUS.PANEL_NO in (6,7,12))
; --11609094

-- line 15:
update PanMan_adhoc_waterfall_base
set l15_sky_view_panel = 1
where ACCOUNT_NUMBER NOT IN
    (SELECT ACCOUNT_NUMBER FROM sk_prod.VESPA_SKY_VIEW_PANEL)
; --13374709

-- line 23
-- select * into #stb_active from
--      (select account_number
--             ,x_model_number
--             ,rank () over (partition by service_instance_id order by ph_non_subs_link_sk desc) rank
--         from sk_prod.cust_Set_top_box) as sub
--  where rank = 1 and x_model_number = 'DRX 595';
-- --
--
-- UPDATE PanMan_adhoc_waterfall_base
--    set l23_active_box_flag = 1
--  where ACCOUNT_NUMBER NOT IN (SELECT ACCOUNT_NUMBER FROM #stb_active)
-- ;


-- A new Line 13 replaces the old lines 13 and 19-22
update PanMan_adhoc_waterfall_base
set l13_hibernators = 1
where ACCOUNT_NUMBER not IN (select account_number from sk_prod.VESPA_PANEL_HIBERNATIONS)
; --13437033

--line 23 - flag subscriber IDs that have a prefix and it is not null
-- select subscriber_id
--       ,max(dt) as maxdt
--       ,null as prefix
--   into #maxdt
--   from callback_data
-- group by subscriber_id
-- ; --15,346,677
--
-- create hg index idx1 on #maxdt(maxdt);
-- create hg index idx2 on #maxdt(subscriber_id);
-- create hg index idx3 on #maxdt(prefix);
--
-- update #maxdt as mxd
--    set prefix = cal.prefix
--   from callback_data as cal
--  where cal.dt = mxd.maxdt
--    and cal.subscriber_id = mxd.subscriber_id
-- ; --15,346,676
--
-- update PanMan_adhoc_waterfall_base as bas
--    set null_prefix = 1
-- ;
-- update PanMan_adhoc_waterfall_base as bas
--    set null_prefix = 0
--   from #maxdt as mxd
--  where cast(bas.subscriber_id as int) = mxd.subscriber_id
--    and prefix is not null
-- ; --2088010
--

  select min(prefix) as px
        ,account_number
    into #golden
    from bednaszs.golden_boxes
group by account_number

update PanMan_adhoc_waterfall_base as bas
   set null_prefix = 1
  from #golden as gol
 where bas.account_number = gol.account_number
   and gol.px is null
;

--line23a flag boxes if we don't know whether they have a prefix
update PanMan_adhoc_waterfall_base as bas
   set empty_prefix = 1
  from bednaszs.golden_boxes as gol
 where bas.account_number = gol.account_number
   and latest_callback < '9999-09-09'
;

--line 24 - base it on within 60 days of the latest date that is in the table
  select account_number
        ,max(latest_callback) as latest_date
    into #latest_date_ac
    from bednaszs.golden_boxes
   where latest_callback < '9999-09-09'
group by account_number
;

create variable @latest_date date;
select @latest_date = max(latest_date) from #latest_date_ac
;

update PanMan_adhoc_waterfall_base as bas
   set l24_last_callback_dt = 1
  from #latest_date_ac as gol
 where bas.account_number = gol.account_number
   and gol.latest_date >= @latest_date - 180
   and latest_date is not null
   and latest_date < '9999-09-09'
;

-- Callback data
update PanMan_adhoc_waterfall_base as bas
   set expected_callbacks = cast(expected_cbcks as varchar(10))
      ,ontime_callbacks   = cast(on_time as varchar(10))
      ,missing_callbacks  = cast(missing_cbcks as varchar(10))
  from golden_boxes as gol
 where bas.subscriber_id = gol.subscriber_id
;

-- --line 26 - Anytime+ enabled
-- update PanMan_adhoc_waterfall_base as bas
--    set anytime_plus = 1
--   from greenj.golden_boxes as gol
--  where bas.subscriber_id = gol.subscriber_id
--    and anytimeplus = 1
-- ;

--line 27 - Anytime+ downloads (this is only available by account number)
update PanMan_adhoc_waterfall_base as bas
   set ondemand_downloads = 1
  from sk_prod.CUST_ANYTIME_PLUS_DOWNLOADS as apd
 where bas.account_number = apd.account_number
--   and x_content_type_desc = 'PROGRAMME'  --  to exclude trailers
--   and x_actual_downloaded_size_mb > 1    -- to exclude any spurious header/trailer download records
;

-- --line 28 - HD box in household
-- select * into #stb_active from
--      (select account_number
--             ,x_box_type
--             ,rank () over (partition by service_instance_id order by ph_non_subs_link_sk desc) rank
--         from sk_prod.cust_Set_top_box
--        where box_replaced_dt='9999-09-09') as sub
--  where rank = 1;
--
-- update PanMan_adhoc_waterfall_base as bas
--    set l28_HD_box=1
--   from #stb_active as stb
--  where bas.account_number = stb.account_number
--    and x_box_type like '%HD%'
-- ;

--29 callback day is day 1-5 of month
  select max(case when cbk_day=1 then 101 else cbk_day end) as cd
        ,account_number
    into #cbd
    from bednaszs.golden_boxes
group by account_number
;


update PanMan_adhoc_waterfall_base as bas
   set l29_day1to5=1
  from #cbd as gol
 where bas.account_number = gol.account_number
   and cd between 2 and 11
;


-- -- Oh, hey, manual patch, the multi-dialer input is broken and so it's killing everything.
-- update PanMan_adhoc_waterfall_base
-- set l16_multi_dial_supression = 1
--
-- OK, now with all the flags built we can identify the points of first failure, and then
-- cascade that waterfall down (were just numbering them after their line of failure):
update PanMan_adhoc_waterfall_base
set knockout_level = case
    when l06_active_dtv          = 0 then 6
    when l07_prod_latest_dtv     = 0 then 7
    when l08_country             = 0 then 8
    when l09_data_capture        = 0 then 9
    when l10_surname             = 0 then 10
    when l11_standard_accounts   = 0 then 11
    when l12_TSA_opt_in          = 0 then 12
    when l13_hibernators         = 0 then 13
    when l14_panel_4             = 0 then 14
    when l15_sky_view_panel      = 0 then 15
    when null_prefix             = 0 then 23.1
    when empty_prefix            = 0 then 23.2
    when ondemand_downloads      = 0 then 27
    when l29_day1to5             = 0 then 29
    when l24_last_callback_dt    = 0 then 31
    else 9999 -- pass!
end
;

commit;

-- Now to extract the actual waterfall! Boxes
  select count(1) as boxes
        ,knockout_level
    from PanMan_adhoc_waterfall_base
group by knockout_level
;

--accounts
  select account_number
        ,max(knockout_level) as max_knockout
    into #accounts_knockout
    from PanMan_adhoc_waterfall_base
group by account_number
;

  select max_knockout
        ,count(1) as accounts
    from #accounts_knockout
group by max_knockout
order by max_knockout
;

--total that fail each test
  select min(l06_active_dtv) as l06
        ,min(l07_prod_latest_dtv) as l07
        ,min(l08_country) as l08
        ,min(l09_data_capture) as l09
        ,min(l10_surname) as l10
        ,min(l11_standard_accounts) as l11
        ,min(l12_TSA_opt_in) as l12
        ,min(l13_hibernators) as l13
        ,min(l14_panel_4) as l14
        ,min(l15_sky_view_panel) as l15
        ,min(null_prefix) as null_prefix
        ,min(empty_prefix) as empty_prefix
        ,min(ondemand_downloads) as l27
        ,min(l29_day1to5) as l29
        ,min(l24_last_callback_dt) as l31
    into #totals
   from PanMan_adhoc_waterfall_base
group by account_number
;

select sum(1-l06) as l06
      ,sum(1-l07) as l07
      ,sum(1-l08) as l08
      ,sum(1-l09) as l09
      ,sum(1-l10) as l10
      ,sum(1-l11) as l11
      ,sum(1-l12) as l12
      ,sum(1-l13) as l13
      ,sum(1-l14) as l14
      ,sum(1-l15) as l15
      ,sum(1-null_prefix) as lnp
      ,sum(1-empty_prefix) as lep
      ,sum(1-l27) as l27
      ,sum(1-l29) as l29
      ,sum(1-l31) as l31
from #totals

--Feb 2013
select distinct pan.account_number
  into greenj.virtual_panel_c4
  from vespa_analysts.Vespa_PanMan_all_households as pan
       inner join sk_prod.CUST_ANYTIME_PLUS_DOWNLOADS as apd on pan.account_number = apd.account_number
   and x_content_type_desc = 'PROGRAMME'  --  to exclude trailers
   and x_actual_downloaded_size_mb > 1    -- to exclude any spurious header/trailer download records
 where reporting_quality > 0
;--224,012

  select account_number
        ,cast(0 as int) as callback_day
    into greenj.virtual_panel_c4_part2
    from PanMan_adhoc_waterfall_base
   where knockout_level in (31, 9999)
group by account_number
; --

alter table virtual_panel_c4_part2 add knockout_level float;

update virtual_panel_c4_part2 as bas
   set callback_day = cd
  from #cbd
 where bas.account_number = #cbd.account_number
;

update virtual_panel_c4_part2 as bas
   set knockout_level = wat.knockout_level
  from PanMan_adhoc_waterfall_base as wat
 where bas.account_number = wat.account_number

delete from virtual_panel_c4_part2
where knockout_level = 31
and callback_day > 8

select * from virtual_panel_c4_part2


--Feb 2013 work ends here (the below is previous analysis)
-------------------------






  select count(1)
        ,expected_callbacks
        ,anytime_plus
    from PanMan_adhoc_waterfall_base
   where knockout_level is null or knockout_level = 17
group by expected_callbacks
        ,anytime_plus
order by anytime_plus
        ,expected_callbacks desc

  select count(1)
        ,ontime_callbacks
        ,anytime_plus
    from PanMan_adhoc_waterfall_base
   where knockout_level is null or knockout_level = 17
group by ontime_callbacks
        ,anytime_plus
order by anytime_plus
        ,ontime_callbacks desc

select count(1)
        ,missing_callbacks
        ,anytime_plus
    from PanMan_adhoc_waterfall_base
   where knockout_level is null or knockout_level = 17
group by missing_callbacks
        ,anytime_plus
order by anytime_plus
        ,missing_callbacks desc

--Oct2012 - profile the potential new additions by value segment
--drop table potential;
  select account_number
        ,cast(subscriber_id as int) as subscriber_id
        ,cast(null as varchar(20)) as value_segment
        ,cast(null as varchar(10)) as box_type
        ,cast(null as varchar(50)) as model_number
        ,cast(0 as bit) as atrisk
        ,cast(0 as bit) as cuscan_risk
    into potential
    from PanMan_adhoc_waterfall_base
   where (knockout_level is null
      or knockout_level = 17) --without DRX595
--      or knockout_level in (17,23)) --include box DRX595
     and subscriber_id > 0
group by account_number,subscriber_id
;

  update potential as bas
     set bas.value_segment = vsd.value_seg
    from sk_prod.VALUE_SEGMENTS_DATA AS vsd
   where bas.account_number = vsd.account_number
;

--also need current panel by value segment
--drop table current_panel;
  select account_number
        ,cast(card_subscriber_id as int) as subscriber_id
        ,cast(null as varchar(20)) as value_segment
        ,cast(null as varchar(10)) as box_type
        ,panel_no
        ,cast(0 as bit) as atrisk
        ,cast(0 as bit) as cuscan_risk
    into current_panel
    from sk_prod.VESPA_SUBSCRIBER_STATUS
   where panel_no in (6,7,12)
     and subscriber_id > 0
     and result = 'Enabled'
group by account_number,subscriber_id,panel_no
;

  update current_panel as bas
     set bas.value_segment = vsd.value_seg
    from sk_prod.VALUE_SEGMENTS_DATA AS vsd
   where bas.account_number = vsd.account_number
;

--Now add box type. There is no existing code to get HDx by box, so have to derive it

--accounts with an HD subscription
--drop table #hd;
  SELECT  csh.account_number
    INTO greenj.v059_hd
    FROM sk_prod.cust_subs_hist                AS csh
         left join potential as pot on csh.account_number = pot.account_number
         left join current_panel as cur on csh.account_number = cur.account_number
   WHERE csh.effective_FROM_dt <= '2012-10-04'
     AND csh.effective_to_dt    > '2012-10-04'
     AND csh.status_code IN ('AC','AB','PC')
     AND csh.SUBSCRIPTION_SUB_TYPE = 'DTV HD'
     and (pot.account_number is not null or cur.account_number is not null)
  GROUP BY csh.account_number
--2063206

commit;
create hg index idx1 on v059_hd(account_number);
  select service_instance_id
        ,cast(0 as int) as subscriber_id
        ,ph_non_subs_link_sk
        ,cast(0 as int) as rankage
        ,stb.account_number
        ,case when x_box_type in ('Sky+HD', 'Basic HD') and hd.account_number is not null then 'HD'
              when x_box_type in ('Sky+HD', 'Basic HD')                                    then 'HDx'
              else                                                                              x_box_type
         end as box_type
        ,case when x_model_number = 'Unknown' then 'Unknown (' || x_description || ')'
              when x_model_number is null then 'Unknown (Unknwown)'
              else x_model_number end as model_number
    into greenj.v059_box_type_initial
    from sk_prod.cust_set_top_box as stb
         left join greenj.v059_hd as hd  on stb.account_number = hd.account_number
         left join potential      as pot on stb.account_number = pot.account_number
         left join current_panel  as cur on stb.account_number = cur.account_number
   where pot.account_number is not null
      or cur.account_number is not null
group by service_instance_id,ph_non_subs_link_sk,stb.account_number,model_number,box_type
;--9275541

--rank thing isn't working so...
  select service_instance_id
        ,max(ph_non_subs_link_sk) as maxph
    into greenj.v059_maxph
    from greenj.v059_box_type_initial
group by service_instance_id
; --6226146

  select box.*
    into greenj.v059_box_type
    from greenj.v059_box_type_initial as box
         inner join greenj.v059_maxph as mph on box.service_instance_id = mph.service_instance_id
                                 and box.ph_non_subs_link_sk = mph.maxph
; --6226146

--check that there are no duplicate service_instance_ids
select count(1),count(distinct service_instance_id) from greenj.v059_box_type;

commit;
create hg index idx2 on greenj.v059_box_type(service_instance_id);

  update greenj.v059_box_type as box
     set subscriber_id = coalesce(cast(si_external_identifier as int),0)
    from sk_prod.cust_service_instance as csi
   where box.service_instance_id = csi.src_system_id
; --

  update current_panel as bas
     set bas.box_type = box.box_type
    from greenj.v059_box_type as box
   where bas.subscriber_id = box.subscriber_id
; --675,502

  update potential as bas
     set bas.box_type = box.box_type
    from greenj.v059_box_type as box
   where bas.subscriber_id = box.subscriber_id
; --4424775

--update flags for at risk and cuscan at risk
update potential as bas
   set atrisk = 1
  from models.at_risk_monthly_table as mod
 where bas.account_number = mod.account_number
;

update potential as bas
   set cuscan_risk = 1
  from models.at_risk_monthly_table as mod
 where bas.account_number = mod.account_number
   and (   max_newend_percentile    is not null
        or max_newout_percentile    is not null
        or max_reinearly_percentile is not null
        or max_reinend_percentile   is not null
        or max_reinmid_percentile   is not null)
;

update current_panel as bas
   set atrisk = 1
  from models.at_risk_monthly_table as mod
 where bas.account_number = mod.account_number
;

update current_panel as bas
   set cuscan_risk = 1
  from models.at_risk_monthly_table as mod
 where bas.account_number = mod.account_number
   and (   max_newend_percentile    is not null
        or max_newout_percentile    is not null
        or max_reinearly_percentile is not null
        or max_reinend_percentile   is not null
        or max_reinmid_percentile   is not null)
;

--Results
  select value_segment
        ,sum(case when box_type = 'Basic' then 1 else 0 end) as basic
        ,sum(case when box_type = 'HD'    then 1 else 0 end) as hd
        ,sum(case when box_type = 'HDx'   then 1 else 0 end) as hdx
        ,sum(case when box_type = 'Sky+'  then 1 else 0 end) as skyplus
        ,sum(case when box_type is null   then 1 else 0 end) as unknown_
    from potential
group by value_segment
;

  select value_segment
        ,count(distinct account_number)
    from potential
group by value_segment
;

  select value_segment
        ,sum(case when box_type = 'Basic' then 1 else 0 end) as basic
        ,sum(case when box_type = 'HD'    then 1 else 0 end) as hd
        ,sum(case when box_type = 'HDx'   then 1 else 0 end) as hdx
        ,sum(case when box_type = 'Sky+'  then 1 else 0 end) as skyplus
        ,sum(case when box_type is null   then 1 else 0 end) as unknown_
    from current_panel
group by value_segment
;

  select value_segment
        ,count(distinct account_number)
    from current_panel
group by value_segment
;

--split the 4.1m potential group by model number
  update potential as bas
     set bas.model_number = box.model_number
    from greenj.v059_box_type as box
   where bas.subscriber_id = box.subscriber_id
;

select count(1),count(distinct subscriber_id) from potential;
  select count(1)
        ,model_number
    from potential
group by model_number
;

--breakdown by value segment for the 1.5m that have the fix after R2 now
  select value_segment
        ,sum(case when box_type = 'Basic' then 1 else 0 end) as basic
        ,sum(case when box_type = 'HD'    then 1 else 0 end) as hd
        ,sum(case when box_type = 'HDx'   then 1 else 0 end) as hdx
        ,sum(case when box_type = 'Sky+'  then 1 else 0 end) as skyplus
        ,sum(case when box_type is null   then 1 else 0 end) as unknown_
    from potential
   where model_number in ('DRX 890', 'DRX 895')
group by value_segment
;

  select value_segment
        ,count(distinct account_number)
    from potential
   where model_number in ('DRX 890', 'DRX 895')
group by value_segment
;

--breakdown for the boxesd that have the fix after R2/R3 in Dec
  select value_segment
        ,sum(case when box_type = 'Basic' then 1 else 0 end) as basic
        ,sum(case when box_type = 'HD'    then 1 else 0 end) as hd
        ,sum(case when box_type = 'HDx'   then 1 else 0 end) as hdx
        ,sum(case when box_type = 'Sky+'  then 1 else 0 end) as skyplus
        ,sum(case when box_type is null   then 1 else 0 end) as unknown_
    from potential
   where model_number in ('DRX 890', 'DRX 895', 'DRX 780', 'Unknown (Samsung HD PVR4)', 'TDS850NB', 'Unknown (Samsung HD PVR5)')
group by value_segment
;

  select value_segment
        ,count(distinct account_number)
    from potential
   where model_number in ('DRX 890', 'DRX 895', 'DRX 780', 'Unknown (Samsung HD PVR4)', 'TDS850NB', 'Unknown (Samsung HD PVR5)')
group by value_segment

--Sky base by value segment
  select pan.account_number
        ,subscriber_id
        ,vsd.value_seg
    into #skybase
    from PanMan_adhoc_waterfall_base as pan
         left join sk_prod.VALUE_SEGMENTS_DATA AS vsd on pan.account_number = vsd.account_number
;

select value_seg,count(1),count(distinct account_number)
from #skybase
group by value_seg

--breakdown by atrisk and cuscan_risk
  select atrisk
        ,cuscan_risk
        ,sum(case when box_type = 'Basic' then 1 else 0 end) as basic
        ,sum(case when box_type = 'HD'    then 1 else 0 end) as hd
        ,sum(case when box_type = 'HDx'   then 1 else 0 end) as hdx
        ,sum(case when box_type = 'Sky+'  then 1 else 0 end) as skyplus
        ,sum(case when box_type is null   then 1 else 0 end) as unknown_
    from potential
group by atrisk
        ,cuscan_risk
;

  select atrisk
        ,cuscan_risk
        ,count(distinct account_number)
    from potential
group by atrisk
        ,cuscan_risk
;

  select atrisk
        ,cuscan_risk
        ,sum(case when box_type = 'Basic' then 1 else 0 end) as basic
        ,sum(case when box_type = 'HD'    then 1 else 0 end) as hd
        ,sum(case when box_type = 'HDx'   then 1 else 0 end) as hdx
        ,sum(case when box_type = 'Sky+'  then 1 else 0 end) as skyplus
        ,sum(case when box_type is null   then 1 else 0 end) as unknown_
    from current_panel
--   where panel_no = 12
--   where panel_no = 6
   where panel_no = 7
group by atrisk
        ,cuscan_risk
;

  select atrisk
        ,cuscan_risk
        ,count(distinct account_number)
    from current_panel
--   where panel_no = 12
--   where panel_no = 6
   where panel_no = 7
group by atrisk
        ,cuscan_risk
;


select count(1),panel_no
 from sk_prod.VESPA_SUBSCRIBER_STATUS
     where result = 'Enabled'
     group by panel_no

  select atrisk
        ,cuscan_risk
        ,sum(case when box_type = 'Basic' then 1 else 0 end) as basic
        ,sum(case when box_type = 'HD'    then 1 else 0 end) as hd
        ,sum(case when box_type = 'HDx'   then 1 else 0 end) as hdx
        ,sum(case when box_type = 'Sky+'  then 1 else 0 end) as skyplus
        ,sum(case when box_type is null   then 1 else 0 end) as unknown_
    from potential
--   where model_number in ('DRX 890', 'DRX 895') --with R2 fix
   where model_number in ('DRX 890', 'DRX 895', 'DRX 780', 'Unknown (Samsung HD PVR4)', 'TDS850NB', 'Unknown (Samsung HD PVR5)') --with R2/R3 fix
group by atrisk
        ,cuscan_risk
;

  select atrisk
        ,cuscan_risk
        ,count(distinct account_number)
    from potential
--   where model_number in ('DRX 890', 'DRX 895') --with R2 fix
   where model_number in ('DRX 890', 'DRX 895', 'DRX 780', 'Unknown (Samsung HD PVR4)', 'TDS850NB', 'Unknown (Samsung HD PVR5)') --with R2/R3 fix
group by atrisk
        ,cuscan_risk
;

  select pan.account_number
        ,subscriber_id
        ,cast(0 as bit) as atrisk
        ,cast(0 as bit) as cuscan_risk
    into #skybase
    from PanMan_adhoc_waterfall_base as pan
;

update #skybase as bas
   set atrisk = 1
  from models.at_risk_monthly_table as mod
 where bas.account_number = mod.account_number
;

update #skybase as bas
   set cuscan_risk = 1
  from models.at_risk_monthly_table as mod
 where bas.account_number = mod.account_number
   and (   max_newend_percentile    is not null
        or max_newout_percentile    is not null
        or max_reinearly_percentile is not null
        or max_reinend_percentile   is not null
        or max_reinmid_percentile   is not null)
;

  select atrisk
        ,cuscan_risk
        ,count(1)
        ,count(distinct account_number)
    from #skybase
group by atrisk
        ,cuscan_risk
;






---
  select account_number
        ,max(case when latest_callback = '9999-09-09' then cast('1900-01-01' as date) else latest_callback end) as dt
    into #golden
    from bednaszs.golden_boxes
group by account_number
;

create variable @current date;
   set @current = (select max(dt) from #goldej);

  select count(distinct sbv.account_number)
    from vespa_analysts.vespa_single_box_view as sbv
         inner join #golden as gol on sbv.account_number = gol.account_number
   where status_vespa='Enabled'
     and dt >= @current-28
     and reporting_quality>0
--37,146 --33,206

select max(dt) from callback_data;--same

  select count(distinct sbv.account_number)
    from vespa_analysts.vespa_single_box_view as sbv
         inner join callback_data as cal on sbv.account_number = cal.account_number
   where status_vespa='Enabled'
     and dt >= @current-180
     and reporting_quality>0
--37,110 --33,234

--total
  select count(distinct sbv.account_number)
    from vespa_analysts.vespa_single_box_view as sbv
   where status_vespa='Enabled'
     and reporting_quality>0
--550,476 --500,878

  select sbv.account_number
        ,reporting_quality
    into #temp
    from vespa_analysts.vespa_single_box_view as sbv
         inner join #golde as gol on sbv.account_number = gol.account_number
   where status_vespa='Enabled'
     and dt >= @current-180
     and reporting_quality>0
group by sbv.account_number

select avg(reporting_quality) from #temp as sub

  select sbv.account_number
        ,reporting_quality
    into #total
    from vespa_analysts.vespa_single_box_view as sbv
   where status_vespa='Enabled'
     and reporting_quality>0
group by sbv.account_number
;

  select avg(reporting_quality)
    from #total as tot
         left join #temp as tmp on tot.account_number = tmp.account_number
   where tmp.account_number is null




create table jon_temp(field1 int)
select * from vespa_analysts.jon_temp

grant select on vespa_analysts.jon_temp to vespa_group_low_security
drop table vespa_analysts.jon_temp



select account_number,min(cast(cbk_day as int)) as cd
into #temp2
from golden_boxes
group by account_number

select cd,count(1) from #temp2 group by cd


select top 10 * from bednaszs.golden_boxes
select cbk_day,count(1) from bednaszs.golden_boxes group by cbk_day

select dt2,count(1) from (
  select sbv.account_number
--        ,max(case when dt='9999-09-09' then '1900-01-01' else dt end) as dt
        ,max(dt) as dt2
    from vespa_analysts.vespa_single_box_view as sbv
         inner join #golde as gol on sbv.account_number = gol.account_number
   where status_vespa='Enabled'
group by sbv.account_number
) as sub group by dt2


  select dt, count(distinct sbv.account_number)
    from vespa_analysts.vespa_single_box_view as sbv
         inner join #golden as gol on sbv.account_number = gol.account_number
--   where dt >= @current-28
group by dt

select sbv.account_number
  from vespa_analysts.vespa_single_box_view as sbv
       left join #golden as gol on sbv.account_number = gol.account_number
where gol.account_number is null


select * from callback_data where account_number in (
'200001994678'
,'220000027155'
,'220000080295'
,'220000132963'
,'220000151427'
,'220000221816'
,'210000429198'
,'200003506926'
,'200001104120'
,'200001071634'
)

select * from bednaszs.golden_boxes where account_number in (
'200001994678'
,'220000027155'
,'220000080295'
,'220000132963'
,'220000151427'
,'220000221816'
,'210000429198'
,'200003506926'
,'200001104120'
,'200001071634'
)




select top 10 * from sk_prod.VESPA_panel_report status_hist
where account_number in (
'200002305023'
,'210000385305'
,'200002518203'
,'210002637717'
,'200002877427'
)

