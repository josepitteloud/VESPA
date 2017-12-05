/*###############################################################################
# Created on:   16/07/2012
# Created by:   Sebastian Bednaszynski (SBE)
# Description:  Investigation data return issues on mutli day distinct metrics
#
#################################################################################
# Dependencies
# ------------------------------------------------------------------------------
# (none)
#
#################################################################################
# Change log
# ------------------------------------------------------------------------------
# 16/07/2012  SBE   Initial version
#
###############################################################################*/

  -- Set up variabales
CREATE VARIABLE @var_prog_period_start  datetime;
CREATE VARIABLE @var_prog_period_end    datetime;
CREATE VARIABLE @var_sql                varchar(15000);
CREATE VARIABLE @var_cntr               smallint;
CREATE VARIABLE @var_num_days           smallint;

SET @var_prog_period_start  = '2012-02-01';
SET @var_prog_period_end    = '2012-03-31';




  -- Get universe - Panel ID 4 boxes
if object_id('VESPA_v073_01_Universe_By_Box') is not null then drop table VESPA_v073_01_Universe_By_Box endif;
select
      Account_Number,
      Subscriber_Id,
      Panel_ID_Vespa as Panel_Id,
      Enablement_Date
  into VESPA_v073_01_Universe_By_Box
  from Vespa_Analysts.Vespa_Single_Box_View box
 where Panel_ID_Vespa = 4
   and Enablement_Date is not null;
commit;

create hg index idx1 on VESPA_v073_01_Universe_By_Box(Account_Number);
create unique hg index idx2 on VESPA_v073_01_Universe_By_Box(Subscriber_Id);



  -- Add last enablement date per account and number of boxes
alter table VESPA_v073_01_Universe_By_Box
  add (Last_Enablement_Date  date null,
       Boxes_Num smallint default 0);

select
      Account_Number,
      max(case
            when Enablement_Date is null then cast('9999-09-09' as date)
              else Enablement_Date
          end) as Last_Enablement_Date,
      count(distinct Subscriber_Id) as Boxes_Num
  into --drop table
       #Box_Details
  from VESPA_v073_01_Universe_By_Box
 group by Account_Number;
commit;

create unique hg index idx1 on #Box_Details(Account_Number);


update VESPA_v073_01_Universe_By_Box base
   set base.Last_Enablement_Date  = box.Last_Enablement_Date,
       base.Boxes_Num             = box.Boxes_Num
  from #Box_Details box
 where base.Account_Number = box.Account_Number;
commit;
-- select top 500 * from  VESPA_v073_01_Universe_By_Box;


--  ##########################################################################################
  -- Create HH universe
if object_id('VESPA_v073_02_Universe_HH') is not null then drop table VESPA_v073_02_Universe_HH endif;
select
      Account_Number,
      max(Last_Enablement_Date) as Last_Enablement_Dt,
      ceil(rand(abs(cast(cast(newid() as varbinary) as bigint))) * 100) as Rand100,
      max(Boxes_Num) as Boxes_Num_VESPA,
      max(CASE
            WHEN Boxes_Num = 0 THEN 'No TV'
            WHEN Boxes_Num = 1 THEN 'A) Single box HH'
            WHEN Boxes_Num = 2 THEN 'B) Dual box HH'
            WHEN Boxes_Num > 2 THEN 'C) Multiple box HH'
              ELSE '??'
          END) as Universe_VESPA
  into VESPA_v073_02_Universe_HH
  from VESPA_v073_01_Universe_By_Box
 where Last_Enablement_Date < '2012-02-01'
 group by Account_Number;
commit;

create unique hg index idx1 on VESPA_v073_02_Universe_HH(Account_Number);
-- select * from  VESPA_v073_02_Universe_HH;
-- select rand100,count(*)from  VESPA_v073_02_Universe_HH group by rand100 order by rand100;



--  ##########################################################################################
  -- Create an alternative Universe breakdown - based on DTV subs
alter table VESPA_v073_02_Universe_HH
  add (Boxes_Num_CSH smallint default 0,
       Universe_CSH varchar(20) null);

SELECT
      csh.Account_Number
      ,sum(CASE  WHEN csh.SUBSCRIPTION_SUB_TYPE ='DTV Primary Viewing'    THEN 1 ELSE 0  END) AS TV
      ,sum(CASE  WHEN csh.SUBSCRIPTION_SUB_TYPE ='DTV Sky+'               THEN 1 ELSE 0  END) AS SP
      ,sum(CASE  WHEN csh.SUBSCRIPTION_SUB_TYPE ='DTV Extra Subscription' THEN 1 ELSE 0  END) AS MR
      ,sum(CASE  WHEN csh.SUBSCRIPTION_SUB_TYPE ='DTV HD'                 THEN 1 ELSE 0  END) AS HD
      ,convert(varchar(30), null) as box_type
      ,convert(smallint, 0) as boxes_num
  INTO --drop table
       #box_type
  FROM VESPA_v073_02_Universe_HH AS acc,
       sk_prod.cust_subs_hist AS csh,
 WHERE csh.account_number = acc.account_number
   AND csh.effective_FROM_dt <= '2012-02-06'
   AND csh.effective_to_dt    > '2012-02-06'
   AND csh.status_code IN  ('AC','AB','PC')
   AND csh.SUBSCRIPTION_SUB_TYPE IN ('DTV Primary Viewing','DTV Sky+', 'DTV Extra Subscription','DTV HD' )
   AND csh.effective_FROM_dt <> csh.effective_to_dt
 GROUP BY csh.account_number;
commit;

create unique hg index idx1 on #box_type(Account_Number);

update #box_type
   set box_type   = CASE
                      WHEN TV + MR = 0 THEN 'No TV'
                      WHEN TV + MR = 1 THEN 'A) Single box HH'
                      WHEN TV + MR = 2 THEN 'B) Dual box HH'
                      WHEN TV + MR > 2 THEN 'C) Multiple box HH'
                        ELSE '??'
                    END,
       boxes_num  = TV + MR;
commit;
-- select box_type, count(*) from #box_type group by box_type order by box_type;


update VESPA_v073_02_Universe_HH base
   set Boxes_Num_CSH  = case
                          when det.boxes_num is null then 0
                            else det.boxes_num
                        end,
       Universe_CSH   = case
                          when det.box_type is null then '??'
                            else det.box_type
                        end
  from #box_type det
 where base.Account_Number *= det.Account_Number
commit;
-- select * from VESPA_v073_02_Universe_HH;
-- select Universe_CSH, count(*) from VESPA_v073_02_Universe_HH group by Universe_CSH order by Universe_CSH;


--  ##########################################################################################
  -- Append weights
if object_id('VESPA_v073_03_Weights') is not null then drop table VESPA_v073_03_Weights endif;
select
      base.Account_Number,
      acc.Reporting_Starts,
      acc.Reporting_Ends,
      seg.Scaling_Day,
      seg.Weighting,
      seg.Scaling_Segment_Id,
      lk.Universe                       -- These column holds Scaling 2 values - DO NOT USE in Scaling 1
  into VESPA_v073_03_Weights
  from VESPA_v073_02_Universe_HH base,
       vespa_analysts.scaling_dialback_intervals acc,
       vespa_analysts.scaling_weightings seg,
       vespa_analysts.scaling_segments_lookup lk
 where acc.Scaling_Segment_Id = seg.Scaling_Segment_Id
   and base.Account_Number = acc.Account_Number
   and base.Boxes_Num_VESPA > 0
   and seg.Scaling_Day between acc.Reporting_Starts and acc.Reporting_Ends
   and seg.Scaling_Day between '2012-02-01' and '2012-04-01'
   and seg.Scaling_Segment_Id = lk.Scaling_Segment_Id;
commit;

create hg index idx1 on VESPA_v073_03_Weights(Account_Number);
create date index idx2 on VESPA_v073_03_Weights(Scaling_Day);
create unique index idx3 on VESPA_v073_03_Weights(Account_Number, Scaling_Day);


alter table VESPA_v073_02_Universe_HH
  add (Weight_0201 double null, Weight_0202 double null, Weight_0203 double null, Weight_0204 double null, Weight_0205 double null,
       Weight_0206 double null, Weight_0207 double null, Weight_0208 double null, Weight_0209 double null, Weight_0210 double null,
       Weight_0211 double null, Weight_0212 double null, Weight_0213 double null, Weight_0214 double null, Weight_0215 double null,
       Weight_0216 double null, Weight_0217 double null, Weight_0218 double null, Weight_0219 double null, Weight_0220 double null,
       Weight_0221 double null, Weight_0222 double null, Weight_0223 double null, Weight_0224 double null, Weight_0225 double null,
       Weight_0226 double null, Weight_0227 double null, Weight_0228 double null, Weight_0229 double null, Weight_0301 double null,
       Weight_0302 double null, Weight_0303 double null, Weight_0304 double null, Weight_0305 double null, Weight_0306 double null,
       Weight_0307 double null, Weight_0308 double null, Weight_0309 double null, Weight_0310 double null, Weight_0311 double null,
       Weight_0312 double null, Weight_0313 double null, Weight_0314 double null, Weight_0315 double null, Weight_0316 double null,
       Weight_0317 double null, Weight_0318 double null, Weight_0319 double null, Weight_0320 double null, Weight_0321 double null,
       Weight_0322 double null, Weight_0323 double null, Weight_0324 double null, Weight_0325 double null, Weight_0326 double null,
       Weight_0327 double null, Weight_0328 double null, Weight_0329 double null, Weight_0330 double null, Weight_0331 double null);

update VESPA_v073_02_Universe_HH base
   set
      base.Weight_0201  = det.Weight_0201,
      base.Weight_0202  = det.Weight_0202,
      base.Weight_0203  = det.Weight_0203,
      base.Weight_0204  = det.Weight_0204,
      base.Weight_0205  = det.Weight_0205,
      base.Weight_0206  = det.Weight_0206,
      base.Weight_0207  = det.Weight_0207,
      base.Weight_0208  = det.Weight_0208,
      base.Weight_0209  = det.Weight_0209,
      base.Weight_0210  = det.Weight_0210,
      base.Weight_0211  = det.Weight_0211,
      base.Weight_0212  = det.Weight_0212,
      base.Weight_0213  = det.Weight_0213,
      base.Weight_0214  = det.Weight_0214,
      base.Weight_0215  = det.Weight_0215,
      base.Weight_0216  = det.Weight_0216,
      base.Weight_0217  = det.Weight_0217,
      base.Weight_0218  = det.Weight_0218,
      base.Weight_0219  = det.Weight_0219,
      base.Weight_0220  = det.Weight_0220,
      base.Weight_0221  = det.Weight_0221,
      base.Weight_0222  = det.Weight_0222,
      base.Weight_0223  = det.Weight_0223,
      base.Weight_0224  = det.Weight_0224,
      base.Weight_0225  = det.Weight_0225,
      base.Weight_0226  = det.Weight_0226,
      base.Weight_0227  = det.Weight_0227,
      base.Weight_0228  = det.Weight_0228,
      base.Weight_0229  = det.Weight_0229,
      base.Weight_0301  = det.Weight_0301,
      base.Weight_0302  = det.Weight_0302,
      base.Weight_0303  = det.Weight_0303,
      base.Weight_0304  = det.Weight_0304,
      base.Weight_0305  = det.Weight_0305,
      base.Weight_0306  = det.Weight_0306,
      base.Weight_0307  = det.Weight_0307,
      base.Weight_0308  = det.Weight_0308,
      base.Weight_0309  = det.Weight_0309,
      base.Weight_0310  = det.Weight_0310,
      base.Weight_0311  = det.Weight_0311,
      base.Weight_0312  = det.Weight_0312,
      base.Weight_0313  = det.Weight_0313,
      base.Weight_0314  = det.Weight_0314,
      base.Weight_0315  = det.Weight_0315,
      base.Weight_0316  = det.Weight_0316,
      base.Weight_0317  = det.Weight_0317,
      base.Weight_0318  = det.Weight_0318,
      base.Weight_0319  = det.Weight_0319,
      base.Weight_0320  = det.Weight_0320,
      base.Weight_0321  = det.Weight_0321,
      base.Weight_0322  = det.Weight_0322,
      base.Weight_0323  = det.Weight_0323,
      base.Weight_0324  = det.Weight_0324,
      base.Weight_0325  = det.Weight_0325,
      base.Weight_0326  = det.Weight_0326,
      base.Weight_0327  = det.Weight_0327,
      base.Weight_0328  = det.Weight_0328,
      base.Weight_0329  = det.Weight_0329,
      base.Weight_0330  = det.Weight_0330,
      base.Weight_0331  = det.Weight_0331
  from (select
              Account_Number,
              max(case when (Scaling_Day = '2012-02-01') then Weighting else null end) as Weight_0201,
              max(case when (Scaling_Day = '2012-02-02') then Weighting else null end) as Weight_0202,
              max(case when (Scaling_Day = '2012-02-03') then Weighting else null end) as Weight_0203,
              max(case when (Scaling_Day = '2012-02-04') then Weighting else null end) as Weight_0204,
              max(case when (Scaling_Day = '2012-02-05') then Weighting else null end) as Weight_0205,
              max(case when (Scaling_Day = '2012-02-06') then Weighting else null end) as Weight_0206,
              max(case when (Scaling_Day = '2012-02-07') then Weighting else null end) as Weight_0207,
              max(case when (Scaling_Day = '2012-02-08') then Weighting else null end) as Weight_0208,
              max(case when (Scaling_Day = '2012-02-09') then Weighting else null end) as Weight_0209,
              max(case when (Scaling_Day = '2012-02-10') then Weighting else null end) as Weight_0210,
              max(case when (Scaling_Day = '2012-02-11') then Weighting else null end) as Weight_0211,
              max(case when (Scaling_Day = '2012-02-12') then Weighting else null end) as Weight_0212,
              max(case when (Scaling_Day = '2012-02-13') then Weighting else null end) as Weight_0213,
              max(case when (Scaling_Day = '2012-02-14') then Weighting else null end) as Weight_0214,
              max(case when (Scaling_Day = '2012-02-15') then Weighting else null end) as Weight_0215,
              max(case when (Scaling_Day = '2012-02-16') then Weighting else null end) as Weight_0216,
              max(case when (Scaling_Day = '2012-02-17') then Weighting else null end) as Weight_0217,
              max(case when (Scaling_Day = '2012-02-18') then Weighting else null end) as Weight_0218,
              max(case when (Scaling_Day = '2012-02-19') then Weighting else null end) as Weight_0219,
              max(case when (Scaling_Day = '2012-02-20') then Weighting else null end) as Weight_0220,
              max(case when (Scaling_Day = '2012-02-21') then Weighting else null end) as Weight_0221,
              max(case when (Scaling_Day = '2012-02-22') then Weighting else null end) as Weight_0222,
              max(case when (Scaling_Day = '2012-02-23') then Weighting else null end) as Weight_0223,
              max(case when (Scaling_Day = '2012-02-24') then Weighting else null end) as Weight_0224,
              max(case when (Scaling_Day = '2012-02-25') then Weighting else null end) as Weight_0225,
              max(case when (Scaling_Day = '2012-02-26') then Weighting else null end) as Weight_0226,
              max(case when (Scaling_Day = '2012-02-27') then Weighting else null end) as Weight_0227,
              max(case when (Scaling_Day = '2012-02-28') then Weighting else null end) as Weight_0228,
              max(case when (Scaling_Day = '2012-02-29') then Weighting else null end) as Weight_0229,
              max(case when (Scaling_Day = '2012-03-01') then Weighting else null end) as Weight_0301,
              max(case when (Scaling_Day = '2012-03-02') then Weighting else null end) as Weight_0302,
              max(case when (Scaling_Day = '2012-03-03') then Weighting else null end) as Weight_0303,
              max(case when (Scaling_Day = '2012-03-04') then Weighting else null end) as Weight_0304,
              max(case when (Scaling_Day = '2012-03-05') then Weighting else null end) as Weight_0305,
              max(case when (Scaling_Day = '2012-03-06') then Weighting else null end) as Weight_0306,
              max(case when (Scaling_Day = '2012-03-07') then Weighting else null end) as Weight_0307,
              max(case when (Scaling_Day = '2012-03-08') then Weighting else null end) as Weight_0308,
              max(case when (Scaling_Day = '2012-03-09') then Weighting else null end) as Weight_0309,
              max(case when (Scaling_Day = '2012-03-10') then Weighting else null end) as Weight_0310,
              max(case when (Scaling_Day = '2012-03-11') then Weighting else null end) as Weight_0311,
              max(case when (Scaling_Day = '2012-03-12') then Weighting else null end) as Weight_0312,
              max(case when (Scaling_Day = '2012-03-13') then Weighting else null end) as Weight_0313,
              max(case when (Scaling_Day = '2012-03-14') then Weighting else null end) as Weight_0314,
              max(case when (Scaling_Day = '2012-03-15') then Weighting else null end) as Weight_0315,
              max(case when (Scaling_Day = '2012-03-16') then Weighting else null end) as Weight_0316,
              max(case when (Scaling_Day = '2012-03-17') then Weighting else null end) as Weight_0317,
              max(case when (Scaling_Day = '2012-03-18') then Weighting else null end) as Weight_0318,
              max(case when (Scaling_Day = '2012-03-19') then Weighting else null end) as Weight_0319,
              max(case when (Scaling_Day = '2012-03-20') then Weighting else null end) as Weight_0320,
              max(case when (Scaling_Day = '2012-03-21') then Weighting else null end) as Weight_0321,
              max(case when (Scaling_Day = '2012-03-22') then Weighting else null end) as Weight_0322,
              max(case when (Scaling_Day = '2012-03-23') then Weighting else null end) as Weight_0323,
              max(case when (Scaling_Day = '2012-03-24') then Weighting else null end) as Weight_0324,
              max(case when (Scaling_Day = '2012-03-25') then Weighting else null end) as Weight_0325,
              max(case when (Scaling_Day = '2012-03-26') then Weighting else null end) as Weight_0326,
              max(case when (Scaling_Day = '2012-03-27') then Weighting else null end) as Weight_0327,
              max(case when (Scaling_Day = '2012-03-28') then Weighting else null end) as Weight_0328,
              max(case when (Scaling_Day = '2012-03-29') then Weighting else null end) as Weight_0329,
              max(case when (Scaling_Day = '2012-03-30') then Weighting else null end) as Weight_0330,
              max(case when (Scaling_Day = '2012-03-31') then Weighting else null end) as Weight_0331
          from VESPA_v073_03_Weights
         group by Account_Number) det
 where base.Account_Number = det.Account_Number;
commit;


--  ##########################################################################################
  -- Recalculate returned data since weight in Scaling 1 for multiple HH are calculated if at least
  -- single box returned data, whereas the current definition is "all boxes must have returned data"
SET @var_cntr = 0;
SET @var_num_days = @var_prog_period_end - @var_prog_period_start;       -- Get events for the period

-- To store all the viewing records:
if object_id('VESPA_v073_04_All_Logs') is not null then drop table VESPA_v073_04_All_Logs endif;
create table VESPA_v073_04_All_Logs (
      Account_Number                varchar(20) not null,
      Subscriber_Id                 decimal(8,0) not null,
      Log_Date                      date null,
      Source                        varchar(10) null,
      Empty_Log_Flag                bit default 0,
      Any_Event_Flag                bit default 0
);

create hg index idx1 on VESPA_v073_04_All_Logs(Account_Number);
create hg index idx2 on VESPA_v073_04_All_Logs(Subscriber_Id);
create date index idx3 on VESPA_v073_04_All_Logs(Log_Date);


-- Build string with placeholder for changing daily table reference
SET @var_sql = '
    insert into VESPA_v073_04_All_Logs
    select
          base.Account_Number,
          vw.Subscriber_Id,
          date(vw.Document_Creation_Date) as Log_Date,
          max(''##^^*^*##'') as Source,
          max(case
                when vw.Event_Type = ''evEmptyLog'' then 1
                  else 0
              end) as thenEmpty_Log_Flag,
          max(case
                when vw.Event_Type <> ''evEmptyLog'' then 1
                  else 0
              end) as Any_Event_Flag
      from VESPA_v073_02_Universe_HH base,
           sk_prod.VESPA_STB_PROG_EVENTS_##^^*^*## vw
     where base.Account_Number = vw.Account_Number
     group by base.Account_Number, vw.Subscriber_Id, Log_Date
     ';

  -- ####### Loop through daily tables ######
FLT_1: LOOP

    EXECUTE(replace(@var_sql, '##^^*^*##', dateformat(dateadd(day, @var_cntr, @var_prog_period_start), 'yyyymmdd')));

    SET @var_cntr = @var_cntr + 1;
    IF @var_cntr > @var_num_days THEN LEAVE FLT_1;
    END IF ;

END LOOP FLT_1;
  -- ####### End of loop ######

commit;


  -- Create final aggregated table by account/box/log date
if object_id('VESPA_v073_05_All_Logs_Final') is not null then drop table VESPA_v073_05_All_Logs_Final endif;
select
      Account_Number,
      Log_Date,
      count(distinct Subscriber_Id) as Subscriber_Ids,
      max(Empty_Log_Flag) as Empty_Log_Flag,
      max(Any_Event_Flag) as Any_Event_Flag
  into VESPA_v073_05_All_Logs_Final
  from VESPA_v073_04_All_Logs
 group by Account_Number, Log_Date;
commit;

create hg index idx1 on VESPA_v073_05_All_Logs_Final(Account_Number);
create date index idx2 on VESPA_v073_05_All_Logs_Final(Log_Date);
create unique index idx3 on VESPA_v073_05_All_Logs_Final(Account_Number, Log_Date);


  -- Update weight - remove for days when data was not returned
update VESPA_v073_02_Universe_HH base
   set Weight_0201   = case when det.Subscriber_Ids < base.Boxes_Num_VESPA or det.Subscriber_Ids is null then null else Weight_0201 end
  from VESPA_v073_05_All_Logs_Final det where base.Account_Number = det.Account_Number and Log_Date = '2012-02-01';

update VESPA_v073_02_Universe_HH base
   set Weight_0202   = case when det.Subscriber_Ids < base.Boxes_Num_VESPA or det.Subscriber_Ids is null then null else Weight_0202 end
  from VESPA_v073_05_All_Logs_Final det where base.Account_Number = det.Account_Number and Log_Date = '2012-02-02';

update VESPA_v073_02_Universe_HH base
   set Weight_0203   = case when det.Subscriber_Ids < base.Boxes_Num_VESPA or det.Subscriber_Ids is null then null else Weight_0203 end
  from VESPA_v073_05_All_Logs_Final det where base.Account_Number = det.Account_Number and Log_Date = '2012-02-03';

update VESPA_v073_02_Universe_HH base
   set Weight_0204   = case when det.Subscriber_Ids < base.Boxes_Num_VESPA or det.Subscriber_Ids is null then null else Weight_0204 end
  from VESPA_v073_05_All_Logs_Final det where base.Account_Number = det.Account_Number and Log_Date = '2012-02-04';

update VESPA_v073_02_Universe_HH base
   set Weight_0205   = case when det.Subscriber_Ids < base.Boxes_Num_VESPA or det.Subscriber_Ids is null then null else Weight_0205 end
  from VESPA_v073_05_All_Logs_Final det where base.Account_Number = det.Account_Number and Log_Date = '2012-02-05';

update VESPA_v073_02_Universe_HH base
   set Weight_0206   = case when det.Subscriber_Ids < base.Boxes_Num_VESPA or det.Subscriber_Ids is null then null else Weight_0206 end
  from VESPA_v073_05_All_Logs_Final det where base.Account_Number = det.Account_Number and Log_Date = '2012-02-06';

update VESPA_v073_02_Universe_HH base
   set Weight_0207   = case when det.Subscriber_Ids < base.Boxes_Num_VESPA or det.Subscriber_Ids is null then null else Weight_0207 end
  from VESPA_v073_05_All_Logs_Final det where base.Account_Number = det.Account_Number and Log_Date = '2012-02-07';

update VESPA_v073_02_Universe_HH base
   set Weight_0208   = case when det.Subscriber_Ids < base.Boxes_Num_VESPA or det.Subscriber_Ids is null then null else Weight_0208 end
  from VESPA_v073_05_All_Logs_Final det where base.Account_Number = det.Account_Number and Log_Date = '2012-02-08';

update VESPA_v073_02_Universe_HH base
   set Weight_0209   = case when det.Subscriber_Ids < base.Boxes_Num_VESPA or det.Subscriber_Ids is null then null else Weight_0209 end
  from VESPA_v073_05_All_Logs_Final det where base.Account_Number = det.Account_Number and Log_Date = '2012-02-09';

update VESPA_v073_02_Universe_HH base
   set Weight_0210   = case when det.Subscriber_Ids < base.Boxes_Num_VESPA or det.Subscriber_Ids is null then null else Weight_0210 end
  from VESPA_v073_05_All_Logs_Final det where base.Account_Number = det.Account_Number and Log_Date = '2012-02-10';

update VESPA_v073_02_Universe_HH base
   set Weight_0211   = case when det.Subscriber_Ids < base.Boxes_Num_VESPA or det.Subscriber_Ids is null then null else Weight_0211 end
  from VESPA_v073_05_All_Logs_Final det where base.Account_Number = det.Account_Number and Log_Date = '2012-02-11';

update VESPA_v073_02_Universe_HH base
   set Weight_0212   = case when det.Subscriber_Ids < base.Boxes_Num_VESPA or det.Subscriber_Ids is null then null else Weight_0212 end
  from VESPA_v073_05_All_Logs_Final det where base.Account_Number = det.Account_Number and Log_Date = '2012-02-12';

update VESPA_v073_02_Universe_HH base
   set Weight_0213   = case when det.Subscriber_Ids < base.Boxes_Num_VESPA or det.Subscriber_Ids is null then null else Weight_0213 end
  from VESPA_v073_05_All_Logs_Final det where base.Account_Number = det.Account_Number and Log_Date = '2012-02-13';

update VESPA_v073_02_Universe_HH base
   set Weight_0214   = case when det.Subscriber_Ids < base.Boxes_Num_VESPA or det.Subscriber_Ids is null then null else Weight_0214 end
  from VESPA_v073_05_All_Logs_Final det where base.Account_Number = det.Account_Number and Log_Date = '2012-02-14';

update VESPA_v073_02_Universe_HH base
   set Weight_0215   = case when det.Subscriber_Ids < base.Boxes_Num_VESPA or det.Subscriber_Ids is null then null else Weight_0215 end
  from VESPA_v073_05_All_Logs_Final det where base.Account_Number = det.Account_Number and Log_Date = '2012-02-15';

update VESPA_v073_02_Universe_HH base
   set Weight_0216   = case when det.Subscriber_Ids < base.Boxes_Num_VESPA or det.Subscriber_Ids is null then null else Weight_0216 end
  from VESPA_v073_05_All_Logs_Final det where base.Account_Number = det.Account_Number and Log_Date = '2012-02-16';

update VESPA_v073_02_Universe_HH base
   set Weight_0217   = case when det.Subscriber_Ids < base.Boxes_Num_VESPA or det.Subscriber_Ids is null then null else Weight_0217 end
  from VESPA_v073_05_All_Logs_Final det where base.Account_Number = det.Account_Number and Log_Date = '2012-02-17';

update VESPA_v073_02_Universe_HH base
   set Weight_0218   = case when det.Subscriber_Ids < base.Boxes_Num_VESPA or det.Subscriber_Ids is null then null else Weight_0218 end
  from VESPA_v073_05_All_Logs_Final det where base.Account_Number = det.Account_Number and Log_Date = '2012-02-18';

update VESPA_v073_02_Universe_HH base
   set Weight_0219   = case when det.Subscriber_Ids < base.Boxes_Num_VESPA or det.Subscriber_Ids is null then null else Weight_0219 end
  from VESPA_v073_05_All_Logs_Final det where base.Account_Number = det.Account_Number and Log_Date = '2012-02-19';

update VESPA_v073_02_Universe_HH base
   set Weight_0220   = case when det.Subscriber_Ids < base.Boxes_Num_VESPA or det.Subscriber_Ids is null then null else Weight_0220 end
  from VESPA_v073_05_All_Logs_Final det where base.Account_Number = det.Account_Number and Log_Date = '2012-02-20';

update VESPA_v073_02_Universe_HH base
   set Weight_0221   = case when det.Subscriber_Ids < base.Boxes_Num_VESPA or det.Subscriber_Ids is null then null else Weight_0221 end
  from VESPA_v073_05_All_Logs_Final det where base.Account_Number = det.Account_Number and Log_Date = '2012-02-21';

update VESPA_v073_02_Universe_HH base
   set Weight_0222   = case when det.Subscriber_Ids < base.Boxes_Num_VESPA or det.Subscriber_Ids is null then null else Weight_0222 end
  from VESPA_v073_05_All_Logs_Final det where base.Account_Number = det.Account_Number and Log_Date = '2012-02-22';

update VESPA_v073_02_Universe_HH base
   set Weight_0223   = case when det.Subscriber_Ids < base.Boxes_Num_VESPA or det.Subscriber_Ids is null then null else Weight_0223 end
  from VESPA_v073_05_All_Logs_Final det where base.Account_Number = det.Account_Number and Log_Date = '2012-02-23';

update VESPA_v073_02_Universe_HH base
   set Weight_0224   = case when det.Subscriber_Ids < base.Boxes_Num_VESPA or det.Subscriber_Ids is null then null else Weight_0224 end
  from VESPA_v073_05_All_Logs_Final det where base.Account_Number = det.Account_Number and Log_Date = '2012-02-24';

update VESPA_v073_02_Universe_HH base
   set Weight_0225   = case when det.Subscriber_Ids < base.Boxes_Num_VESPA or det.Subscriber_Ids is null then null else Weight_0225 end
  from VESPA_v073_05_All_Logs_Final det where base.Account_Number = det.Account_Number and Log_Date = '2012-02-25';

update VESPA_v073_02_Universe_HH base
   set Weight_0226   = case when det.Subscriber_Ids < base.Boxes_Num_VESPA or det.Subscriber_Ids is null then null else Weight_0226 end
  from VESPA_v073_05_All_Logs_Final det where base.Account_Number = det.Account_Number and Log_Date = '2012-02-26';

update VESPA_v073_02_Universe_HH base
   set Weight_0227   = case when det.Subscriber_Ids < base.Boxes_Num_VESPA or det.Subscriber_Ids is null then null else Weight_0227 end
  from VESPA_v073_05_All_Logs_Final det where base.Account_Number = det.Account_Number and Log_Date = '2012-02-27';

update VESPA_v073_02_Universe_HH base
   set Weight_0228   = case when det.Subscriber_Ids < base.Boxes_Num_VESPA or det.Subscriber_Ids is null then null else Weight_0228 end
  from VESPA_v073_05_All_Logs_Final det where base.Account_Number = det.Account_Number and Log_Date = '2012-02-28';

update VESPA_v073_02_Universe_HH base
   set Weight_0229   = case when det.Subscriber_Ids < base.Boxes_Num_VESPA or det.Subscriber_Ids is null then null else Weight_0229 end
  from VESPA_v073_05_All_Logs_Final det where base.Account_Number = det.Account_Number and Log_Date = '2012-02-29';

update VESPA_v073_02_Universe_HH base
   set Weight_0301   = case when det.Subscriber_Ids < base.Boxes_Num_VESPA or det.Subscriber_Ids is null then null else Weight_0301 end
  from VESPA_v073_05_All_Logs_Final det where base.Account_Number = det.Account_Number and Log_Date = '2012-03-01';

update VESPA_v073_02_Universe_HH base
   set Weight_0302   = case when det.Subscriber_Ids < base.Boxes_Num_VESPA or det.Subscriber_Ids is null then null else Weight_0302 end
  from VESPA_v073_05_All_Logs_Final det where base.Account_Number = det.Account_Number and Log_Date = '2012-03-02';

update VESPA_v073_02_Universe_HH base
   set Weight_0303   = case when det.Subscriber_Ids < base.Boxes_Num_VESPA or det.Subscriber_Ids is null then null else Weight_0303 end
  from VESPA_v073_05_All_Logs_Final det where base.Account_Number = det.Account_Number and Log_Date = '2012-03-03';

update VESPA_v073_02_Universe_HH base
   set Weight_0304   = case when det.Subscriber_Ids < base.Boxes_Num_VESPA or det.Subscriber_Ids is null then null else Weight_0304 end
  from VESPA_v073_05_All_Logs_Final det where base.Account_Number = det.Account_Number and Log_Date = '2012-03-04';

update VESPA_v073_02_Universe_HH base
   set Weight_0305   = case when det.Subscriber_Ids < base.Boxes_Num_VESPA or det.Subscriber_Ids is null then null else Weight_0305 end
  from VESPA_v073_05_All_Logs_Final det where base.Account_Number = det.Account_Number and Log_Date = '2012-03-05';

update VESPA_v073_02_Universe_HH base
   set Weight_0306   = case when det.Subscriber_Ids < base.Boxes_Num_VESPA or det.Subscriber_Ids is null then null else Weight_0306 end
  from VESPA_v073_05_All_Logs_Final det where base.Account_Number = det.Account_Number and Log_Date = '2012-03-06';

update VESPA_v073_02_Universe_HH base
   set Weight_0307   = case when det.Subscriber_Ids < base.Boxes_Num_VESPA or det.Subscriber_Ids is null then null else Weight_0307 end
  from VESPA_v073_05_All_Logs_Final det where base.Account_Number = det.Account_Number and Log_Date = '2012-03-07';

update VESPA_v073_02_Universe_HH base
   set Weight_0308   = case when det.Subscriber_Ids < base.Boxes_Num_VESPA or det.Subscriber_Ids is null then null else Weight_0308 end
  from VESPA_v073_05_All_Logs_Final det where base.Account_Number = det.Account_Number and Log_Date = '2012-03-08';

update VESPA_v073_02_Universe_HH base
   set Weight_0309   = case when det.Subscriber_Ids < base.Boxes_Num_VESPA or det.Subscriber_Ids is null then null else Weight_0309 end
  from VESPA_v073_05_All_Logs_Final det where base.Account_Number = det.Account_Number and Log_Date = '2012-03-09';

update VESPA_v073_02_Universe_HH base
   set Weight_0310   = case when det.Subscriber_Ids < base.Boxes_Num_VESPA or det.Subscriber_Ids is null then null else Weight_0310 end
  from VESPA_v073_05_All_Logs_Final det where base.Account_Number = det.Account_Number and Log_Date = '2012-03-10';

update VESPA_v073_02_Universe_HH base
   set Weight_0311   = case when det.Subscriber_Ids < base.Boxes_Num_VESPA or det.Subscriber_Ids is null then null else Weight_0311 end
  from VESPA_v073_05_All_Logs_Final det where base.Account_Number = det.Account_Number and Log_Date = '2012-03-11';

update VESPA_v073_02_Universe_HH base
   set Weight_0312   = case when det.Subscriber_Ids < base.Boxes_Num_VESPA or det.Subscriber_Ids is null then null else Weight_0312 end
  from VESPA_v073_05_All_Logs_Final det where base.Account_Number = det.Account_Number and Log_Date = '2012-03-12';

update VESPA_v073_02_Universe_HH base
   set Weight_0313   = case when det.Subscriber_Ids < base.Boxes_Num_VESPA or det.Subscriber_Ids is null then null else Weight_0313 end
  from VESPA_v073_05_All_Logs_Final det where base.Account_Number = det.Account_Number and Log_Date = '2012-03-13';

update VESPA_v073_02_Universe_HH base
   set Weight_0314   = case when det.Subscriber_Ids < base.Boxes_Num_VESPA or det.Subscriber_Ids is null then null else Weight_0314 end
  from VESPA_v073_05_All_Logs_Final det where base.Account_Number = det.Account_Number and Log_Date = '2012-03-14';

update VESPA_v073_02_Universe_HH base
   set Weight_0315   = case when det.Subscriber_Ids < base.Boxes_Num_VESPA or det.Subscriber_Ids is null then null else Weight_0315 end
  from VESPA_v073_05_All_Logs_Final det where base.Account_Number = det.Account_Number and Log_Date = '2012-03-15';

update VESPA_v073_02_Universe_HH base
   set Weight_0316   = case when det.Subscriber_Ids < base.Boxes_Num_VESPA or det.Subscriber_Ids is null then null else Weight_0316 end
  from VESPA_v073_05_All_Logs_Final det where base.Account_Number = det.Account_Number and Log_Date = '2012-03-16';

update VESPA_v073_02_Universe_HH base
   set Weight_0317   = case when det.Subscriber_Ids < base.Boxes_Num_VESPA or det.Subscriber_Ids is null then null else Weight_0317 end
  from VESPA_v073_05_All_Logs_Final det where base.Account_Number = det.Account_Number and Log_Date = '2012-03-17';

update VESPA_v073_02_Universe_HH base
   set Weight_0318   = case when det.Subscriber_Ids < base.Boxes_Num_VESPA or det.Subscriber_Ids is null then null else Weight_0318 end
  from VESPA_v073_05_All_Logs_Final det where base.Account_Number = det.Account_Number and Log_Date = '2012-03-18';

update VESPA_v073_02_Universe_HH base
   set Weight_0319   = case when det.Subscriber_Ids < base.Boxes_Num_VESPA or det.Subscriber_Ids is null then null else Weight_0319 end
  from VESPA_v073_05_All_Logs_Final det where base.Account_Number = det.Account_Number and Log_Date = '2012-03-19';

update VESPA_v073_02_Universe_HH base
   set Weight_0320   = case when det.Subscriber_Ids < base.Boxes_Num_VESPA or det.Subscriber_Ids is null then null else Weight_0320 end
  from VESPA_v073_05_All_Logs_Final det where base.Account_Number = det.Account_Number and Log_Date = '2012-03-20';

update VESPA_v073_02_Universe_HH base
   set Weight_0321   = case when det.Subscriber_Ids < base.Boxes_Num_VESPA or det.Subscriber_Ids is null then null else Weight_0321 end
  from VESPA_v073_05_All_Logs_Final det where base.Account_Number = det.Account_Number and Log_Date = '2012-03-21';

update VESPA_v073_02_Universe_HH base
   set Weight_0322   = case when det.Subscriber_Ids < base.Boxes_Num_VESPA or det.Subscriber_Ids is null then null else Weight_0322 end
  from VESPA_v073_05_All_Logs_Final det where base.Account_Number = det.Account_Number and Log_Date = '2012-03-22';

update VESPA_v073_02_Universe_HH base
   set Weight_0323   = case when det.Subscriber_Ids < base.Boxes_Num_VESPA or det.Subscriber_Ids is null then null else Weight_0323 end
  from VESPA_v073_05_All_Logs_Final det where base.Account_Number = det.Account_Number and Log_Date = '2012-03-23';

update VESPA_v073_02_Universe_HH base
   set Weight_0324   = case when det.Subscriber_Ids < base.Boxes_Num_VESPA or det.Subscriber_Ids is null then null else Weight_0324 end
  from VESPA_v073_05_All_Logs_Final det where base.Account_Number = det.Account_Number and Log_Date = '2012-03-24';

update VESPA_v073_02_Universe_HH base
   set Weight_0325   = case when det.Subscriber_Ids < base.Boxes_Num_VESPA or det.Subscriber_Ids is null then null else Weight_0325 end
  from VESPA_v073_05_All_Logs_Final det where base.Account_Number = det.Account_Number and Log_Date = '2012-03-25';

update VESPA_v073_02_Universe_HH base
   set Weight_0326   = case when det.Subscriber_Ids < base.Boxes_Num_VESPA or det.Subscriber_Ids is null then null else Weight_0326 end
  from VESPA_v073_05_All_Logs_Final det where base.Account_Number = det.Account_Number and Log_Date = '2012-03-26';

update VESPA_v073_02_Universe_HH base
   set Weight_0327   = case when det.Subscriber_Ids < base.Boxes_Num_VESPA or det.Subscriber_Ids is null then null else Weight_0327 end
  from VESPA_v073_05_All_Logs_Final det where base.Account_Number = det.Account_Number and Log_Date = '2012-03-27';

update VESPA_v073_02_Universe_HH base
   set Weight_0328   = case when det.Subscriber_Ids < base.Boxes_Num_VESPA or det.Subscriber_Ids is null then null else Weight_0328 end
  from VESPA_v073_05_All_Logs_Final det where base.Account_Number = det.Account_Number and Log_Date = '2012-03-28';

update VESPA_v073_02_Universe_HH base
   set Weight_0329   = case when det.Subscriber_Ids < base.Boxes_Num_VESPA or det.Subscriber_Ids is null then null else Weight_0329 end
  from VESPA_v073_05_All_Logs_Final det where base.Account_Number = det.Account_Number and Log_Date = '2012-03-29';

update VESPA_v073_02_Universe_HH base
   set Weight_0330   = case when det.Subscriber_Ids < base.Boxes_Num_VESPA or det.Subscriber_Ids is null then null else Weight_0330 end
  from VESPA_v073_05_All_Logs_Final det where base.Account_Number = det.Account_Number and Log_Date = '2012-03-30';

update VESPA_v073_02_Universe_HH base
   set Weight_0331   = case when det.Subscriber_Ids < base.Boxes_Num_VESPA or det.Subscriber_Ids is null then null else Weight_0331 end
  from VESPA_v073_05_All_Logs_Final det where base.Account_Number = det.Account_Number and Log_Date = '2012-03-31';
commit;



alter table VESPA_v073_02_Universe_HH
  add (All_Weights_Flag_1Wk bit default 0,           -- 06 Feb - 12 Feb -> 1 week
       All_Weights_Flag_6Wk bit default 0);          -- 06 Feb - 18 Mar -> 6 weeks

update VESPA_v073_02_Universe_HH base
   set
      All_Weights_Flag_1Wk  = case
                                when (Weight_0206 is not null) and
                                     (Weight_0207 is not null) and (Weight_0208 is not null) and (Weight_0209 is not null) and
                                     (Weight_0210 is not null) and (Weight_0211 is not null) and (Weight_0212 is not null)
                                        then 1
                                  else 0
                              end,
      All_Weights_Flag_6Wk  = case
                                when --(Weight_0201 is not null) and (Weight_0202 is not null) and (Weight_0203 is not null) and
                                     --(Weight_0204 is not null) and (Weight_0205 is not null) and
                                     (Weight_0206 is not null) and
                                     (Weight_0207 is not null) and (Weight_0208 is not null) and (Weight_0209 is not null) and
                                     (Weight_0210 is not null) and (Weight_0211 is not null) and (Weight_0212 is not null) and
                                     (Weight_0213 is not null) and (Weight_0214 is not null) and (Weight_0215 is not null) and
                                     (Weight_0216 is not null) and (Weight_0217 is not null) and (Weight_0218 is not null) and
                                     (Weight_0219 is not null) and (Weight_0220 is not null) and (Weight_0221 is not null) and
                                     (Weight_0222 is not null) and (Weight_0223 is not null) and (Weight_0224 is not null) and
                                     (Weight_0225 is not null) and (Weight_0226 is not null) and (Weight_0227 is not null) and
                                     (Weight_0228 is not null) and (Weight_0229 is not null) and (Weight_0301 is not null) and
                                     (Weight_0302 is not null) and (Weight_0303 is not null) and (Weight_0304 is not null) and
                                     (Weight_0305 is not null) and (Weight_0306 is not null) and (Weight_0307 is not null) and
                                     (Weight_0308 is not null) and (Weight_0309 is not null) and (Weight_0310 is not null) and
                                     (Weight_0311 is not null) and (Weight_0312 is not null) and (Weight_0313 is not null) and
                                     (Weight_0314 is not null) and (Weight_0315 is not null) and (Weight_0316 is not null) and
                                     (Weight_0317 is not null) and (Weight_0318 is not null)
                                     --and (Weight_0319 is not null) and
                                     --(Weight_0320 is not null) and (Weight_0321 is not null) and (Weight_0322 is not null) and
                                     --(Weight_0323 is not null) and (Weight_0324 is not null) and (Weight_0325 is not null) and
                                     --(Weight_0326 is not null) and (Weight_0327 is not null) and (Weight_0328 is not null) and
                                     --(Weight_0329 is not null) and (Weight_0330 is not null) and (Weight_0331 is not null)
                                        then 1
                                  else 0
                              end;
commit;


--  ##########################################################################################
  -- Calculate number of days data was not returned within each time period
  -- (i.e. weight doe not exists)
alter table VESPA_v073_02_Universe_HH
  add (Days_Returned_Wk1 smallint default 0,          --    06 Feb - 12 Feb
       Days_Returned_Wk2 smallint default 0,          --    13 Feb - 19 Feb
       Days_Returned_Wk3 smallint default 0,          --    20 Feb - 26 Feb
       Days_Returned_Wk4 smallint default 0,          --    27 Feb - 04 Mar
       Days_Returned_Wk5 smallint default 0,          --    05 Mar - 11 Mar
       Days_Returned_Wk6 smallint default 0);         --    12 Mar - 18 Mar
commit;

update VESPA_v073_02_Universe_HH base
   set
      Days_Returned_Wk1 = case when Weight_0206 is null then 0 else 1 end +
                          case when weight_0207 is null then 0 else 1 end +
                          case when weight_0208 is null then 0 else 1 end +
                          case when weight_0209 is null then 0 else 1 end +
                          case when weight_0210 is null then 0 else 1 end +
                          case when weight_0211 is null then 0 else 1 end +
                          case when weight_0212 is null then 0 else 1 end,

      Days_Returned_Wk2 = case when Weight_0213 is null then 0 else 1 end +
                          case when weight_0214 is null then 0 else 1 end +
                          case when weight_0215 is null then 0 else 1 end +
                          case when weight_0216 is null then 0 else 1 end +
                          case when weight_0217 is null then 0 else 1 end +
                          case when weight_0218 is null then 0 else 1 end +
                          case when weight_0219 is null then 0 else 1 end,

      Days_Returned_Wk3 = case when Weight_0220 is null then 0 else 1 end +
                          case when weight_0221 is null then 0 else 1 end +
                          case when weight_0222 is null then 0 else 1 end +
                          case when weight_0223 is null then 0 else 1 end +
                          case when weight_0224 is null then 0 else 1 end +
                          case when weight_0225 is null then 0 else 1 end +
                          case when weight_0226 is null then 0 else 1 end,

      Days_Returned_Wk4 = case when Weight_0227 is null then 0 else 1 end +
                          case when weight_0228 is null then 0 else 1 end +
                          case when weight_0229 is null then 0 else 1 end +
                          case when weight_0301 is null then 0 else 1 end +
                          case when weight_0302 is null then 0 else 1 end +
                          case when weight_0303 is null then 0 else 1 end +
                          case when weight_0304 is null then 0 else 1 end,

      Days_Returned_Wk5 = case when Weight_0305 is null then 0 else 1 end +
                          case when weight_0306 is null then 0 else 1 end +
                          case when weight_0307 is null then 0 else 1 end +
                          case when weight_0308 is null then 0 else 1 end +
                          case when weight_0309 is null then 0 else 1 end +
                          case when weight_0310 is null then 0 else 1 end +
                          case when weight_0311 is null then 0 else 1 end,

      Days_Returned_Wk6 = case when Weight_0312 is null then 0 else 1 end +
                          case when weight_0313 is null then 0 else 1 end +
                          case when weight_0314 is null then 0 else 1 end +
                          case when weight_0315 is null then 0 else 1 end +
                          case when weight_0316 is null then 0 else 1 end +
                          case when weight_0317 is null then 0 else 1 end +
                          case when weight_0318 is null then 0 else 1 end;
commit;


--  ##########################################################################################
  -- Get viewing data for the universe
if object_id('VESPA_v073_06_Chn_Programmes') is not null drop table VESPA_v073_06_Chn_Programmes;
select
      programme_trans_sk
      ,Channel_Name
      ,Epg_Title
      ,Genre_Description
      ,Sub_Genre_Description
      ,Tx_Start_Datetime_UTC
      ,Tx_End_Datetime_UTC
  into VESPA_v073_06_Chn_Programmes
  from sk_prod.VESPA_EPG_DIM
 where tx_date_time_utc >= @var_prog_period_start
   and tx_date_time_utc <= @var_prog_period_end
   and Channel_Name = 'Sky Living';

create unique hg index idx1 on VESPA_v073_06_Chn_Programmes(programme_trans_sk);


-- To store all the viewing records:
if object_id('VESPA_v073_07_All_Viewing') is not null drop table VESPA_v073_07_All_Viewing;
create table VESPA_v073_07_All_Viewing (
      Account_Number                varchar(20) not null,
      Subscriber_Id                 decimal(8,0) not null,
      Log_Date                      date null,
      Source                        varchar(10) null
      ,Event_Type                   varchar(20) not null
      ,X_Type_Of_Viewing_Event      varchar(40) not null
      ,Adjusted_Event_Start_Time    datetime
      ,X_Adjusted_Event_End_Time    datetime
      ,X_Viewing_Start_Time         datetime
      ,X_Viewing_End_Time           datetime
      ,Tx_Start_Datetime_UTC        datetime
      ,Tx_End_Datetime_UTC          datetime
      ,Tx_Start_Date_UTC            date
      ,Recorded_Time_UTC            datetime
      ,Play_Back_Speed              decimal(4,0)
      ,X_Event_Duration             decimal(10,0)
      ,X_Programme_Duration         decimal(10,0)
      ,X_Programme_Viewed_Duration  decimal(10,0)
      ,X_Programme_Percentage_Viewed decimal(3,0)

);

create hg index idx1 on VESPA_v073_07_All_Viewing(Account_Number);
create dttm index idx2 on VESPA_v073_07_All_Viewing(Tx_Start_Datetime_UTC);
create date index idx3 on VESPA_v073_07_All_Viewing(Tx_Start_Date_UTC);


SET @var_cntr = 0;
SET @var_num_days = 60;       -- Get events up to 60 days of log return time

  -- Build string with placeholder for changing daily table reference
SET @var_sql = '
    insert into VESPA_v073_07_All_Viewing
    select
        vw.Account_Number,
        vw.Subscriber_Id,
        vw.Document_Creation_Date,
        ''##^^*^*##'',
        vw.Event_Type,
        vw.X_Type_Of_Viewing_Event,
        vw.Adjusted_Event_Start_Time,
        vw.X_Adjusted_Event_End_Time,
        vw.X_Viewing_Start_Time,
        vw.X_Viewing_End_Time,
        prog.Tx_Start_Datetime_UTC,
        prog.Tx_End_Datetime_UTC,
        date(prog.Tx_Start_Datetime_UTC),
        vw.Recorded_Time_UTC,
        vw.Play_Back_Speed,
        vw.X_Event_Duration,
        vw.X_Programme_Duration,
        vw.X_Programme_Viewed_Duration,
        vw.X_Programme_Percentage_Viewed
     from VESPA_v073_02_Universe_HH base,
          sk_prod.VESPA_STB_PROG_EVENTS_##^^*^*## as vw,
          VESPA_v073_06_Chn_Programmes as prog
     where base.Account_Number = vw.Account_Number
       and vw.programme_trans_sk = prog.programme_trans_sk
       and (play_back_speed is null or play_back_speed = 2)
       and x_programme_viewed_duration > 0
       and Panel_Id in (4,5)
       and x_type_of_viewing_event <> ''Non viewing event'';

     commit;

    ';

  -- ####### Loop through daily tables ######
FLT_1: LOOP

    EXECUTE(replace(@var_sql, '##^^*^*##', dateformat(dateadd(day, @var_cntr, @var_prog_period_start), 'yyyymmdd')));

    SET @var_cntr = @var_cntr + 1;
    IF @var_cntr > @var_num_days THEN LEAVE FLT_1;
    END IF ;

END LOOP FLT_1;
  -- ####### End of loop ######

commit;


--  ##########################################################################################
  -- Get channel impacts
alter table VESPA_v073_02_Universe_HH
  add (Impacts_0206 smallint default 0, Impacts_0207 smallint default 0, Impacts_0208 smallint default 0, Impacts_0209 smallint default 0, Impacts_0210 smallint default 0,
       Impacts_0211 smallint default 0, Impacts_0212 smallint default 0, Impacts_0213 smallint default 0, Impacts_0214 smallint default 0, Impacts_0215 smallint default 0,
       Impacts_0216 smallint default 0, Impacts_0217 smallint default 0, Impacts_0218 smallint default 0, Impacts_0219 smallint default 0, Impacts_0220 smallint default 0,
       Impacts_0221 smallint default 0, Impacts_0222 smallint default 0, Impacts_0223 smallint default 0, Impacts_0224 smallint default 0, Impacts_0225 smallint default 0,
       Impacts_0226 smallint default 0, Impacts_0227 smallint default 0, Impacts_0228 smallint default 0, Impacts_0229 smallint default 0, Impacts_0301 smallint default 0,
       Impacts_0302 smallint default 0, Impacts_0303 smallint default 0, Impacts_0304 smallint default 0, Impacts_0305 smallint default 0, Impacts_0306 smallint default 0,
       Impacts_0307 smallint default 0, Impacts_0308 smallint default 0, Impacts_0309 smallint default 0, Impacts_0310 smallint default 0, Impacts_0311 smallint default 0,
       Impacts_0312 smallint default 0, Impacts_0313 smallint default 0, Impacts_0314 smallint default 0, Impacts_0315 smallint default 0, Impacts_0316 smallint default 0,
       Impacts_0317 smallint default 0, Impacts_0318 smallint default 0);


update VESPA_v073_02_Universe_HH base
   set
      base.Impacts_0206  = det.Impacts_0206,
      base.Impacts_0207  = det.Impacts_0207,
      base.Impacts_0208  = det.Impacts_0208,
      base.Impacts_0209  = det.Impacts_0209,
      base.Impacts_0210  = det.Impacts_0210,
      base.Impacts_0211  = det.Impacts_0211,
      base.Impacts_0212  = det.Impacts_0212,
      base.Impacts_0213  = det.Impacts_0213,
      base.Impacts_0214  = det.Impacts_0214,
      base.Impacts_0215  = det.Impacts_0215,
      base.Impacts_0216  = det.Impacts_0216,
      base.Impacts_0217  = det.Impacts_0217,
      base.Impacts_0218  = det.Impacts_0218,
      base.Impacts_0219  = det.Impacts_0219,
      base.Impacts_0220  = det.Impacts_0220,
      base.Impacts_0221  = det.Impacts_0221,
      base.Impacts_0222  = det.Impacts_0222,
      base.Impacts_0223  = det.Impacts_0223,
      base.Impacts_0224  = det.Impacts_0224,
      base.Impacts_0225  = det.Impacts_0225,
      base.Impacts_0226  = det.Impacts_0226,
      base.Impacts_0227  = det.Impacts_0227,
      base.Impacts_0228  = det.Impacts_0228,
      base.Impacts_0229  = det.Impacts_0229,
      base.Impacts_0301  = det.Impacts_0301,
      base.Impacts_0302  = det.Impacts_0302,
      base.Impacts_0303  = det.Impacts_0303,
      base.Impacts_0304  = det.Impacts_0304,
      base.Impacts_0305  = det.Impacts_0305,
      base.Impacts_0306  = det.Impacts_0306,
      base.Impacts_0307  = det.Impacts_0307,
      base.Impacts_0308  = det.Impacts_0308,
      base.Impacts_0309  = det.Impacts_0309,
      base.Impacts_0310  = det.Impacts_0310,
      base.Impacts_0311  = det.Impacts_0311,
      base.Impacts_0312  = det.Impacts_0312,
      base.Impacts_0313  = det.Impacts_0313,
      base.Impacts_0314  = det.Impacts_0314,
      base.Impacts_0315  = det.Impacts_0315,
      base.Impacts_0316  = det.Impacts_0316,
      base.Impacts_0317  = det.Impacts_0317,
      base.Impacts_0318  = det.Impacts_0318
  from (select
              Account_Number,
              sum(case when (Tx_Start_Date_UTC = '2012-02-06') and (X_Event_Duration >= 180) then 1 else 0 end) as Impacts_0206,
              sum(case when (Tx_Start_Date_UTC = '2012-02-07') and (X_Event_Duration >= 180) then 1 else 0 end) as Impacts_0207,
              sum(case when (Tx_Start_Date_UTC = '2012-02-08') and (X_Event_Duration >= 180) then 1 else 0 end) as Impacts_0208,
              sum(case when (Tx_Start_Date_UTC = '2012-02-09') and (X_Event_Duration >= 180) then 1 else 0 end) as Impacts_0209,
              sum(case when (Tx_Start_Date_UTC = '2012-02-10') and (X_Event_Duration >= 180) then 1 else 0 end) as Impacts_0210,
              sum(case when (Tx_Start_Date_UTC = '2012-02-11') and (X_Event_Duration >= 180) then 1 else 0 end) as Impacts_0211,
              sum(case when (Tx_Start_Date_UTC = '2012-02-12') and (X_Event_Duration >= 180) then 1 else 0 end) as Impacts_0212,
              sum(case when (Tx_Start_Date_UTC = '2012-02-13') and (X_Event_Duration >= 180) then 1 else 0 end) as Impacts_0213,
              sum(case when (Tx_Start_Date_UTC = '2012-02-14') and (X_Event_Duration >= 180) then 1 else 0 end) as Impacts_0214,
              sum(case when (Tx_Start_Date_UTC = '2012-02-15') and (X_Event_Duration >= 180) then 1 else 0 end) as Impacts_0215,
              sum(case when (Tx_Start_Date_UTC = '2012-02-16') and (X_Event_Duration >= 180) then 1 else 0 end) as Impacts_0216,
              sum(case when (Tx_Start_Date_UTC = '2012-02-17') and (X_Event_Duration >= 180) then 1 else 0 end) as Impacts_0217,
              sum(case when (Tx_Start_Date_UTC = '2012-02-18') and (X_Event_Duration >= 180) then 1 else 0 end) as Impacts_0218,
              sum(case when (Tx_Start_Date_UTC = '2012-02-19') and (X_Event_Duration >= 180) then 1 else 0 end) as Impacts_0219,
              sum(case when (Tx_Start_Date_UTC = '2012-02-20') and (X_Event_Duration >= 180) then 1 else 0 end) as Impacts_0220,
              sum(case when (Tx_Start_Date_UTC = '2012-02-21') and (X_Event_Duration >= 180) then 1 else 0 end) as Impacts_0221,
              sum(case when (Tx_Start_Date_UTC = '2012-02-22') and (X_Event_Duration >= 180) then 1 else 0 end) as Impacts_0222,
              sum(case when (Tx_Start_Date_UTC = '2012-02-23') and (X_Event_Duration >= 180) then 1 else 0 end) as Impacts_0223,
              sum(case when (Tx_Start_Date_UTC = '2012-02-24') and (X_Event_Duration >= 180) then 1 else 0 end) as Impacts_0224,
              sum(case when (Tx_Start_Date_UTC = '2012-02-25') and (X_Event_Duration >= 180) then 1 else 0 end) as Impacts_0225,
              sum(case when (Tx_Start_Date_UTC = '2012-02-26') and (X_Event_Duration >= 180) then 1 else 0 end) as Impacts_0226,
              sum(case when (Tx_Start_Date_UTC = '2012-02-27') and (X_Event_Duration >= 180) then 1 else 0 end) as Impacts_0227,
              sum(case when (Tx_Start_Date_UTC = '2012-02-28') and (X_Event_Duration >= 180) then 1 else 0 end) as Impacts_0228,
              sum(case when (Tx_Start_Date_UTC = '2012-02-29') and (X_Event_Duration >= 180) then 1 else 0 end) as Impacts_0229,
              sum(case when (Tx_Start_Date_UTC = '2012-03-01') and (X_Event_Duration >= 180) then 1 else 0 end) as Impacts_0301,
              sum(case when (Tx_Start_Date_UTC = '2012-03-02') and (X_Event_Duration >= 180) then 1 else 0 end) as Impacts_0302,
              sum(case when (Tx_Start_Date_UTC = '2012-03-03') and (X_Event_Duration >= 180) then 1 else 0 end) as Impacts_0303,
              sum(case when (Tx_Start_Date_UTC = '2012-03-04') and (X_Event_Duration >= 180) then 1 else 0 end) as Impacts_0304,
              sum(case when (Tx_Start_Date_UTC = '2012-03-05') and (X_Event_Duration >= 180) then 1 else 0 end) as Impacts_0305,
              sum(case when (Tx_Start_Date_UTC = '2012-03-06') and (X_Event_Duration >= 180) then 1 else 0 end) as Impacts_0306,
              sum(case when (Tx_Start_Date_UTC = '2012-03-07') and (X_Event_Duration >= 180) then 1 else 0 end) as Impacts_0307,
              sum(case when (Tx_Start_Date_UTC = '2012-03-08') and (X_Event_Duration >= 180) then 1 else 0 end) as Impacts_0308,
              sum(case when (Tx_Start_Date_UTC = '2012-03-09') and (X_Event_Duration >= 180) then 1 else 0 end) as Impacts_0309,
              sum(case when (Tx_Start_Date_UTC = '2012-03-10') and (X_Event_Duration >= 180) then 1 else 0 end) as Impacts_0310,
              sum(case when (Tx_Start_Date_UTC = '2012-03-11') and (X_Event_Duration >= 180) then 1 else 0 end) as Impacts_0311,
              sum(case when (Tx_Start_Date_UTC = '2012-03-12') and (X_Event_Duration >= 180) then 1 else 0 end) as Impacts_0312,
              sum(case when (Tx_Start_Date_UTC = '2012-03-13') and (X_Event_Duration >= 180) then 1 else 0 end) as Impacts_0313,
              sum(case when (Tx_Start_Date_UTC = '2012-03-14') and (X_Event_Duration >= 180) then 1 else 0 end) as Impacts_0314,
              sum(case when (Tx_Start_Date_UTC = '2012-03-15') and (X_Event_Duration >= 180) then 1 else 0 end) as Impacts_0315,
              sum(case when (Tx_Start_Date_UTC = '2012-03-16') and (X_Event_Duration >= 180) then 1 else 0 end) as Impacts_0316,
              sum(case when (Tx_Start_Date_UTC = '2012-03-17') and (X_Event_Duration >= 180) then 1 else 0 end) as Impacts_0317,
              sum(case when (Tx_Start_Date_UTC = '2012-03-18') and (X_Event_Duration >= 180) then 1 else 0 end) as Impacts_0318
          from VESPA_v073_07_All_Viewing
         group by Account_Number) det
 where base.Account_Number = det.Account_Number;
commit;



--  ##########################################################################################
--  ##########################################################################################
  -- Data export
select *
  from VESPA_v073_02_Universe_HH
 where All_Weights_Flag_1Wk = 1;

select *
  from VESPA_v073_02_Universe_HH
 where All_Weights_Flag_6Wk = 1;



--  ##########################################################################################
--  ##########################################################################################
  -- Summaries

  -- Number od days when data was returned over 6 periods
select
      Days_Returned_Wk1 as Days_In_Period,
      sum(case when Universe_VESPA = 'A) Single box HH' then 1 else 0 end) as A_Single_Box_HH,
      sum(case when Universe_VESPA = 'B) Dual box HH' then 1 else 0 end) as B_Dual_box_HH,
      sum(case when Universe_VESPA = 'C) Multiple box HH' then 1 else 0 end) as C_Multiple_box_HH
  from VESPA_v073_02_Universe_HH
 group by Days_In_Period
 order by Days_In_Period;

select
      Days_Returned_Wk1 + Days_Returned_Wk2 as Days_In_Period,
      sum(case when Universe_VESPA = 'A) Single box HH' then 1 else 0 end) as A_Single_Box_HH,
      sum(case when Universe_VESPA = 'B) Dual box HH' then 1 else 0 end) as B_Dual_box_HH,
      sum(case when Universe_VESPA = 'C) Multiple box HH' then 1 else 0 end) as C_Multiple_box_HH
  from VESPA_v073_02_Universe_HH
 group by Days_In_Period
 order by Days_In_Period;

select
      Days_Returned_Wk1 + Days_Returned_Wk2 + Days_Returned_Wk3 as Days_In_Period,
      sum(case when Universe_VESPA = 'A) Single box HH' then 1 else 0 end) as A_Single_Box_HH,
      sum(case when Universe_VESPA = 'B) Dual box HH' then 1 else 0 end) as B_Dual_box_HH,
      sum(case when Universe_VESPA = 'C) Multiple box HH' then 1 else 0 end) as C_Multiple_box_HH
  from VESPA_v073_02_Universe_HH
 group by Days_In_Period
 order by Days_In_Period;

select
      Days_Returned_Wk1 + Days_Returned_Wk2 + Days_Returned_Wk3 +
          Days_Returned_Wk4 as Days_In_Period,
      sum(case when Universe_VESPA = 'A) Single box HH' then 1 else 0 end) as A_Single_Box_HH,
      sum(case when Universe_VESPA = 'B) Dual box HH' then 1 else 0 end) as B_Dual_box_HH,
      sum(case when Universe_VESPA = 'C) Multiple box HH' then 1 else 0 end) as C_Multiple_box_HH
  from VESPA_v073_02_Universe_HH
 group by Days_In_Period
 order by Days_In_Period;

select
      Days_Returned_Wk1 + Days_Returned_Wk2 + Days_Returned_Wk3 +
          Days_Returned_Wk4 + Days_Returned_Wk5 as Days_In_Period,
      sum(case when Universe_VESPA = 'A) Single box HH' then 1 else 0 end) as A_Single_Box_HH,
      sum(case when Universe_VESPA = 'B) Dual box HH' then 1 else 0 end) as B_Dual_box_HH,
      sum(case when Universe_VESPA = 'C) Multiple box HH' then 1 else 0 end) as C_Multiple_box_HH
  from VESPA_v073_02_Universe_HH
 group by Days_In_Period
 order by Days_In_Period;

select
      Days_Returned_Wk1 + Days_Returned_Wk2 + Days_Returned_Wk3 +
          Days_Returned_Wk4 + Days_Returned_Wk5 + Days_Returned_Wk6 as Days_In_Period,
      sum(case when Universe_VESPA = 'A) Single box HH' then 1 else 0 end) as A_Single_Box_HH,
      sum(case when Universe_VESPA = 'B) Dual box HH' then 1 else 0 end) as B_Dual_box_HH,
      sum(case when Universe_VESPA = 'C) Multiple box HH' then 1 else 0 end) as C_Multiple_box_HH
  from VESPA_v073_02_Universe_HH
 group by Days_In_Period
 order by Days_In_Period;


--  ##########################################################################################
  -- Data return consistency
  -- ### The same number of days over 6 weeks ###
select
      Days_Returned_Wk1 as Days_Returned,
      sum(case
            when (Days_Returned_Wk1 = Days_Returned_Wk1) and
                 (Days_Returned_Wk2 = Days_Returned_Wk1) and
                 (Days_Returned_Wk3 = Days_Returned_Wk1) and
                 (Days_Returned_Wk4 = Days_Returned_Wk1) and
                 (Days_Returned_Wk5 = Days_Returned_Wk1) and
                 (Days_Returned_Wk6 = Days_Returned_Wk1) and
                 (Universe_VESPA = 'A) Single box HH') then 1
              else 0
          end) as A_Single_Box_HH,

      sum(case
            when (Days_Returned_Wk1 = Days_Returned_Wk1) and
                 (Days_Returned_Wk2 = Days_Returned_Wk1) and
                 (Days_Returned_Wk3 = Days_Returned_Wk1) and
                 (Days_Returned_Wk4 = Days_Returned_Wk1) and
                 (Days_Returned_Wk5 = Days_Returned_Wk1) and
                 (Days_Returned_Wk6 = Days_Returned_Wk1) and
                 (Universe_VESPA = 'B) Dual box HH') then 1
              else 0
          end) as B_Dual_box_HH,

      sum(case
            when (Days_Returned_Wk1 = Days_Returned_Wk1) and
                 (Days_Returned_Wk2 = Days_Returned_Wk1) and
                 (Days_Returned_Wk3 = Days_Returned_Wk1) and
                 (Days_Returned_Wk4 = Days_Returned_Wk1) and
                 (Days_Returned_Wk5 = Days_Returned_Wk1) and
                 (Days_Returned_Wk6 = Days_Returned_Wk1) and
                 (Universe_VESPA = 'C) Multiple box HH') then 1
              else 0
          end) as C_Multiple_box_HH

  from VESPA_v073_02_Universe_HH
 group by Days_Returned
 order by Days_Returned;


  -- ### The same or higher number of days over 6 weeks ###
select
      Days_Returned_Wk1 as Days_Returned,
      sum(case
            when (Days_Returned_Wk1 >= Days_Returned_Wk1) and
                 (Days_Returned_Wk2 >= Days_Returned_Wk1) and
                 (Days_Returned_Wk3 >= Days_Returned_Wk1) and
                 (Days_Returned_Wk4 >= Days_Returned_Wk1) and
                 (Days_Returned_Wk5 >= Days_Returned_Wk1) and
                 (Days_Returned_Wk6 >= Days_Returned_Wk1) and
                 (Universe_VESPA = 'A) Single box HH') then 1
              else 0
          end) as A_Single_Box_HH,

      sum(case
            when (Days_Returned_Wk1 >= Days_Returned_Wk1) and
                 (Days_Returned_Wk2 >= Days_Returned_Wk1) and
                 (Days_Returned_Wk3 >= Days_Returned_Wk1) and
                 (Days_Returned_Wk4 >= Days_Returned_Wk1) and
                 (Days_Returned_Wk5 >= Days_Returned_Wk1) and
                 (Days_Returned_Wk6 >= Days_Returned_Wk1) and
                 (Universe_VESPA = 'B) Dual box HH') then 1
              else 0
          end) as B_Dual_box_HH,

      sum(case
            when (Days_Returned_Wk1 >= Days_Returned_Wk1) and
                 (Days_Returned_Wk2 >= Days_Returned_Wk1) and
                 (Days_Returned_Wk3 >= Days_Returned_Wk1) and
                 (Days_Returned_Wk4 >= Days_Returned_Wk1) and
                 (Days_Returned_Wk5 >= Days_Returned_Wk1) and
                 (Days_Returned_Wk6 >= Days_Returned_Wk1) and
                 (Universe_VESPA = 'C) Multiple box HH') then 1
              else 0
          end) as C_Multiple_box_HH

  from VESPA_v073_02_Universe_HH
 group by Days_Returned
 order by Days_Returned;


  -- ### +/- 1 day over 6 weeks ###
select
      Days_Returned_Wk1 as Days_Returned,
      sum(case
            when (Days_Returned_Wk1 between Days_Returned_Wk1 - 1 and Days_Returned_Wk1 + 1) and
                 (Days_Returned_Wk2 between Days_Returned_Wk1 - 1 and Days_Returned_Wk1 + 1) and
                 (Days_Returned_Wk3 between Days_Returned_Wk1 - 1 and Days_Returned_Wk1 + 1) and
                 (Days_Returned_Wk4 between Days_Returned_Wk1 - 1 and Days_Returned_Wk1 + 1) and
                 (Days_Returned_Wk5 between Days_Returned_Wk1 - 1 and Days_Returned_Wk1 + 1) and
                 (Days_Returned_Wk6 between Days_Returned_Wk1 - 1 and Days_Returned_Wk1 + 1) and
                 (Universe_VESPA = 'A) Single box HH') then 1
              else 0
          end) as A_Single_Box_HH,

      sum(case
            when (Days_Returned_Wk1 between Days_Returned_Wk1 - 1 and Days_Returned_Wk1 + 1) and
                 (Days_Returned_Wk2 between Days_Returned_Wk1 - 1 and Days_Returned_Wk1 + 1) and
                 (Days_Returned_Wk3 between Days_Returned_Wk1 - 1 and Days_Returned_Wk1 + 1) and
                 (Days_Returned_Wk4 between Days_Returned_Wk1 - 1 and Days_Returned_Wk1 + 1) and
                 (Days_Returned_Wk5 between Days_Returned_Wk1 - 1 and Days_Returned_Wk1 + 1) and
                 (Days_Returned_Wk6 between Days_Returned_Wk1 - 1 and Days_Returned_Wk1 + 1) and
                 (Universe_VESPA = 'B) Dual box HH') then 1
              else 0
          end) as B_Dual_box_HH,

      sum(case
            when (Days_Returned_Wk1 between Days_Returned_Wk1 - 1 and Days_Returned_Wk1 + 1) and
                 (Days_Returned_Wk2 between Days_Returned_Wk1 - 1 and Days_Returned_Wk1 + 1) and
                 (Days_Returned_Wk3 between Days_Returned_Wk1 - 1 and Days_Returned_Wk1 + 1) and
                 (Days_Returned_Wk4 between Days_Returned_Wk1 - 1 and Days_Returned_Wk1 + 1) and
                 (Days_Returned_Wk5 between Days_Returned_Wk1 - 1 and Days_Returned_Wk1 + 1) and
                 (Days_Returned_Wk6 between Days_Returned_Wk1 - 1 and Days_Returned_Wk1 + 1) and
                 (Universe_VESPA = 'C) Multiple box HH') then 1
              else 0
          end) as C_Multiple_box_HH

  from VESPA_v073_02_Universe_HH
 group by Days_Returned
 order by Days_Returned;


  -- ### +/- 2 days over 6 weeks ###
select
      Days_Returned_Wk1 as Days_Returned,
      sum(case
            when (Days_Returned_Wk1 between Days_Returned_Wk1 - 2 and Days_Returned_Wk1 + 2) and
                 (Days_Returned_Wk2 between Days_Returned_Wk1 - 2 and Days_Returned_Wk1 + 2) and
                 (Days_Returned_Wk3 between Days_Returned_Wk1 - 2 and Days_Returned_Wk1 + 2) and
                 (Days_Returned_Wk4 between Days_Returned_Wk1 - 2 and Days_Returned_Wk1 + 2) and
                 (Days_Returned_Wk5 between Days_Returned_Wk1 - 2 and Days_Returned_Wk1 + 2) and
                 (Days_Returned_Wk6 between Days_Returned_Wk1 - 2 and Days_Returned_Wk1 + 2) and
                 (Universe_VESPA = 'A) Single box HH') then 1
              else 0
          end) as A_Single_Box_HH,

      sum(case
            when (Days_Returned_Wk1 between Days_Returned_Wk1 - 2 and Days_Returned_Wk1 + 2) and
                 (Days_Returned_Wk2 between Days_Returned_Wk1 - 2 and Days_Returned_Wk1 + 2) and
                 (Days_Returned_Wk3 between Days_Returned_Wk1 - 2 and Days_Returned_Wk1 + 2) and
                 (Days_Returned_Wk4 between Days_Returned_Wk1 - 2 and Days_Returned_Wk1 + 2) and
                 (Days_Returned_Wk5 between Days_Returned_Wk1 - 2 and Days_Returned_Wk1 + 2) and
                 (Days_Returned_Wk6 between Days_Returned_Wk1 - 2 and Days_Returned_Wk1 + 2) and
                 (Universe_VESPA = 'B) Dual box HH') then 1
              else 0
          end) as B_Dual_box_HH,

      sum(case
            when (Days_Returned_Wk1 between Days_Returned_Wk1 - 2 and Days_Returned_Wk1 + 2) and
                 (Days_Returned_Wk2 between Days_Returned_Wk1 - 2 and Days_Returned_Wk1 + 2) and
                 (Days_Returned_Wk3 between Days_Returned_Wk1 - 2 and Days_Returned_Wk1 + 2) and
                 (Days_Returned_Wk4 between Days_Returned_Wk1 - 2 and Days_Returned_Wk1 + 2) and
                 (Days_Returned_Wk5 between Days_Returned_Wk1 - 2 and Days_Returned_Wk1 + 2) and
                 (Days_Returned_Wk6 between Days_Returned_Wk1 - 2 and Days_Returned_Wk1 + 2) and
                 (Universe_VESPA = 'C) Multiple box HH') then 1
              else 0
          end) as C_Multiple_box_HH

  from VESPA_v073_02_Universe_HH
 group by Days_Returned
 order by Days_Returned;



--  ##########################################################################################
  -- Data return consistency - averages
select
      max(1) as Wk,
      avg(case when (Universe_VESPA = 'A) Single box HH')   then Days_Returned_Wk1 else null end) as A_Single_Box_HH,
      avg(case when (Universe_VESPA = 'B) Dual box HH')     then Days_Returned_Wk1 else null end) as B_Dual_box_HH,
      avg(case when (Universe_VESPA = 'C) Multiple box HH') then Days_Returned_Wk1 else null end) as C_Multiple_box_HH
  from VESPA_v073_02_Universe_HH
 where Days_Returned_Wk1 between 1 and 6        -- Exclude cases with no returns and for all days
    union all
select
      max(2) as Wk,
      avg(case when (Universe_VESPA = 'A) Single box HH')   then Days_Returned_Wk2 else null end) as A_Single_Box_HH,
      avg(case when (Universe_VESPA = 'B) Dual box HH')     then Days_Returned_Wk2 else null end) as B_Dual_box_HH,
      avg(case when (Universe_VESPA = 'C) Multiple box HH') then Days_Returned_Wk2 else null end) as C_Multiple_box_HH
  from VESPA_v073_02_Universe_HH
 where Days_Returned_Wk2 between 1 and 6        -- Exclude cases with no returns and for all days
    union all
 select
      max(3) as Wk,
      avg(case when (Universe_VESPA = 'A) Single box HH')   then Days_Returned_Wk3 else null end) as A_Single_Box_HH,
      avg(case when (Universe_VESPA = 'B) Dual box HH')     then Days_Returned_Wk3 else null end) as B_Dual_box_HH,
      avg(case when (Universe_VESPA = 'C) Multiple box HH') then Days_Returned_Wk3 else null end) as C_Multiple_box_HH
  from VESPA_v073_02_Universe_HH
 where Days_Returned_Wk3 between 1 and 6        -- Exclude cases with no returns and for all days
   union all
select
      max(4) as Wk,
      avg(case when (Universe_VESPA = 'A) Single box HH')   then Days_Returned_Wk4 else null end) as A_Single_Box_HH,
      avg(case when (Universe_VESPA = 'B) Dual box HH')     then Days_Returned_Wk4 else null end) as B_Dual_box_HH,
      avg(case when (Universe_VESPA = 'C) Multiple box HH') then Days_Returned_Wk4 else null end) as C_Multiple_box_HH
  from VESPA_v073_02_Universe_HH
 where Days_Returned_Wk4 between 1 and 6        -- Exclude cases with no returns and for all days
    union all
 select
      max(5) as Wk,
      avg(case when (Universe_VESPA = 'A) Single box HH')   then Days_Returned_Wk5 else null end) as A_Single_Box_HH,
      avg(case when (Universe_VESPA = 'B) Dual box HH')     then Days_Returned_Wk5 else null end) as B_Dual_box_HH,
      avg(case when (Universe_VESPA = 'C) Multiple box HH') then Days_Returned_Wk5 else null end) as C_Multiple_box_HH
  from VESPA_v073_02_Universe_HH
 where Days_Returned_Wk5 between 1 and 6        -- Exclude cases with no returns and for all days
   union all
select
      max(6) as Wk,
      avg(case when (Universe_VESPA = 'A) Single box HH')   then Days_Returned_Wk6 else null end) as A_Single_Box_HH,
      avg(case when (Universe_VESPA = 'B) Dual box HH')     then Days_Returned_Wk6 else null end) as B_Dual_box_HH,
      avg(case when (Universe_VESPA = 'C) Multiple box HH') then Days_Returned_Wk6 else null end) as C_Multiple_box_HH
  from VESPA_v073_02_Universe_HH
 where Days_Returned_Wk6 between 1 and 6        -- Exclude cases with no returns and for all days
  ;





























