/*###############################################################################
# Created on:   28/02/2014
# Created by:   Sebastian Bednaszynski(SBE)
# Description:  EPL rights project - universe creation
#
#################################################################################
# Dependencies
# ------------------------------------------------------------------------------
# => Tables/objects required:
#
#################################################################################
# Change log
# ------------------------------------------------------------------------------
# 28/02/2014  SBE   Initial version
#
###############################################################################*/

  -- Period descriptions
    -- "1"  - original analysis period between 01/08/2013 and 28/02/2014 - based on viewing
    -- "2"  - additional analysis period between 01/02/2013 and 31/07/2013 - based on viewing
    -- "3"  -
    -- "4"  - additional period for CL analysis between 01/03/2014 and 31/05/2014 - based on
    --        - those who were active at the end of period "1". Most of the data (e.g. BT Sport
    --        - flags, Valid_Account_Flag etc.) are based on period "1" values!!!


  -- ##############################################################################################################
  -- ##### Get a list of daily accounts available within the period                                           #####
  -- ##############################################################################################################
if object_id('EPL_01_Universe') is not null then drop table EPL_01_Universe end if;
create table EPL_01_Universe (
    Pk_Identifier                           bigint            identity,
    Account_Number                          varchar(20)       null      default null,
    Period                                  tinyint           null      default 0,
    Data_Day                                date              null      default null,
    Valid_Account_Flag                      bit               null      default 0,
    Days_Data_Available                     smallint          null      default 0,
    First_ESPN_Viewing                      date              null      default null,
    First_BT_Viewing                        date              null      default null,

    DTV_Flag                                bit               null      default 0,
    Sky_Sports_Flag                         bit               null      default 0,
    BT_Sport_Flag                           bit               null      default 0,
    ESPN_Flag                               bit               null      default 0,

    Boxes_Expected                          tinyint           null      default 0,
    Boxes_Returned                          tinyint           null      default 0,
    ESPN_Viewing_Flag                       bit               null      default null,
    BT_Viewing_Flag                         bit               null      default null,

    BT_Max_Cons_Duration                    bigint            null      default 0,
    BT_Total_Duration                       bigint            null      default 0,

    Updated_On                              datetime          not null  default timestamp,
    Updated_By                              varchar(30)       not null  default user_name()
);
create unique hg   index idx01 on EPL_01_Universe(Account_Number, Period, Data_Day);
create        hg   index idx02 on EPL_01_Universe(Account_Number);
create        date index idx03 on EPL_01_Universe(Data_Day);
create        lf   index idx04 on EPL_01_Universe(Period);
grant select on EPL_01_Universe to vespa_group_low_security;


if object_id('EPL_1_Create_Universe') is not null then drop procedure EPL_1_Create_Universe end if;
create procedure EPL_1_Create_Universe
      @parMonthDate             date = null,
      @parPeriod                tinyint = 0
as
begin

      declare @varTableSuffix                 varchar(6)
      declare @varSQL                         varchar(25000)

      set @varTableSuffix       = (dateformat(@parMonthDate, 'yyyymm'))

      execute logger_add_event 0, 0, '##### Processing period: ' || dateformat(@parMonthDate, 'mm/yyyy') || ' #####', null
      execute logger_add_event 0, 0, 'Source table used: sk_prod.vespa_dp_prog_viewed_' || @varTableSuffix, null

      delete from EPL_01_Universe
       where year(Data_Day) = year(@parMonthDate)
         and month(Data_Day) = month(@parMonthDate)
      commit

      execute logger_add_event 0, 0, 'Viewing data rows for current month removed', @@rowcount

      set @varSQL = '
                      insert into EPL_01_Universe
                            (Account_Number, Period, Data_Day, Boxes_Returned, ESPN_Viewing_Flag, BT_Viewing_Flag,
                             BT_Max_Cons_Duration, BT_Total_Duration)
                        select
                              Account_Number,
                              ' || @parPeriod || ',
                              date(event_start_date_time_utc) as Event_Dt,
                              count(distinct Subscriber_Id),
                              max(case                                        -- ESPN
                                    when Service_Key in (3141, 4040) and Video_Playing_Flag = 1 then 1
                                      else 0
                                  end),
                              max(case                                        -- BT Sport
                                    when Service_Key in (3625, 3627, 3661, 3663) and Video_Playing_Flag = 1 then 1
                                      else 0
                                  end),

                              max(case                                        -- BT Sport - maximum consecutive viewing in a day
                                    when vw.Service_Key is null then 0
                                    when vw.Service_Key not in (3625, 3627, 3661, 3663) or vw.Video_Playing_Flag = 0 then 0
                                    when vw.capped_partial_flag = 1 then datediff(second, vw.instance_start_date_time_utc, vw.capping_end_date_time_utc)
                                      else datediff(second, vw.instance_start_date_time_utc, vw.instance_end_date_time_utc)
                                  end),
                              sum(case                                        -- BT Sport - total viewing in a day
                                    when vw.Service_Key is null then 0
                                    when vw.Service_Key not in (3625, 3627, 3661, 3663) or vw.Video_Playing_Flag = 0 then 0
                                    when vw.capped_partial_flag = 1 then datediff(second, vw.instance_start_date_time_utc, vw.capping_end_date_time_utc)
                                      else datediff(second, vw.instance_start_date_time_utc, vw.instance_end_date_time_utc)
                                  end)

                          from sk_prod.vespa_dp_prog_viewed_' || @varTableSuffix || ' vw
                         where dk_capping_end_datehour_dim > 0                                        -- Events received on time for capping
                           and panel_id = 12                                                          -- Panel 12 only
                           and instance_start_date_time_utc < instance_end_date_time_utc              -- Valid events
                           and account_number is not null
                           and subscriber_id is not null
                         group by Account_Number, Event_Dt
                      commit

                      execute logger_add_event 0, 0, ''Month processed'', @@rowcount
                    '
      execute(@varSQL)

end;

execute EPL_1_Create_Universe '2013-02-01', 2;
execute EPL_1_Create_Universe '2013-03-01', 2;
execute EPL_1_Create_Universe '2013-04-01', 2;
execute EPL_1_Create_Universe '2013-05-01', 2;
execute EPL_1_Create_Universe '2013-06-01', 2;
execute EPL_1_Create_Universe '2013-07-01', 2;

execute EPL_1_Create_Universe '2013-08-01', 1;
execute EPL_1_Create_Universe '2013-09-01', 1;
execute EPL_1_Create_Universe '2013-10-01', 1;
execute EPL_1_Create_Universe '2013-11-01', 1;
execute EPL_1_Create_Universe '2013-12-01', 1;
execute EPL_1_Create_Universe '2014-01-01', 1;
execute EPL_1_Create_Universe '2014-02-01', 1;



  -- ##############################################################################################################
  -- ##### Checks                                                                                             #####
  -- ##############################################################################################################
select
      Data_Day,
      Period,
      count(*) as Cnt
  from EPL_01_Universe
 group by
      Data_Day,
      Period
 order by 1;


  -- ##############################################################################################################
  -- ##### Update period metadata                                                                             #####
  -- ##############################################################################################################
  -- Append Sky Sports eligibility and DTV status
update EPL_01_Universe base
   set base.DTV_Flag        = 1,
       base.Sky_Sports_Flag = case
                                when det.Prem_Sports > 0 then 1                                             -- Any Sports counted as "entitled"
                                  else 0
                              end
  from (select
              csh.Account_Number,
              csh.Effective_From_Dt,
              csh.Effective_To_Dt,
              cel.Prem_Sports,
              cel.Sport_1,
              cel.Sport_2
          from sk_prod.cust_subs_hist csh
                  left join sk_prod.cust_entitlement_lookup as cel  on csh.current_short_description = cel.short_description
         where csh.subscription_sub_type = 'DTV Primary Viewing'
           and csh.status_code in ('AC', 'PC', 'AB')
           and csh.effective_from_dt < csh.effective_to_dt) det
 where base.Account_Number = det.Account_Number
   and det.Effective_From_Dt <= base.Data_Day
   and det.Effective_To_Dt > base.Data_Day;
commit;


  -- ESPN & BT Sports viewing and number of days data available
update EPL_01_Universe base
   set base.Days_Data_Available   = det.Days_Data_Available,
       base.First_ESPN_Viewing    = det.First_ESPN_Viewing,
       base.First_BT_Viewing      = det.First_BT_Viewing,
       base.ESPN_Flag             = case
                                      when det.First_ESPN_Viewing <= base.Data_Day and DTV_Flag = 1 then 1
                                        else 0
                                    end,
       base.BT_Sport_Flag         = case
                                      when det.First_BT_Viewing <= base.Data_Day and DTV_Flag = 1 then 1
                                        else 0
                                    end
  from (select
              Account_Number,
              Period,
              count(distinct Data_Day) as Days_Data_Available,
              min(case
                    when ESPN_Viewing_Flag = 1 then Data_Day
                      else null
                  end) as First_ESPN_Viewing,
              min(case
                    when Data_Day in ('2013-10-25', '2013-10-26', '2013-10-27', '2014-12-22',               -- BT free weekend
                                      '2014-02-15') then null
                    when BT_Viewing_Flag = 1 and BT_Max_Cons_Duration >= 15 * 60 then Data_Day              -- Must have watched for at at least 15minutes consecutively to be counted
                      else null
                  end) as First_BT_Viewing
          from EPL_01_Universe
         where Data_Day not in ('2013-10-19', '2013-10-26', '2013-11-02',                                   -- BT Sport issue
                                '2013-02-04', '2013-02-23', '2013-04-30', '2013-06-12', '2013-06-13',       -- Poor source data
                                '2013-06-14', '2013-06-15', '2013-06-16', '2013-06-17', '2013-07-01',
                                '2013-07-02', '2013-07-04', '2013-09-04', '2013-09-05', '2013-11-07',
                                '2013-11-08', '2013-11-21', '2013-11-22', '2014-02-01', '2014-02-02')
         group by Account_Number, Period) det
 where base.Account_Number = det.Account_Number
   and base.Period = det.Period;
commit;


  -- Finally get the valid account flag - for each period
update EPL_01_Universe base
   set base.Valid_Account_Flag    = case
                                      when base.Period = 1 and base.Days_Data_Available >= 151 then 1      -- Aug '13 - Feb '14
                                      when base.Period = 2 and base.Days_Data_Available >= 127 then 1      -- Feb '13 - Jul '13
                                        else 0
                                    end
 where Data_Day not in ('2013-10-19', '2013-10-26', '2013-11-02', '2013-09-04',                 -- BT Sport issue
                        '2013-02-04', '2013-02-23', '2013-04-30', '2013-06-12', '2013-06-13',   -- Poor source data
                        '2013-06-14', '2013-06-15', '2013-06-16', '2013-06-17', '2013-07-01',
                        '2013-07-02', '2013-07-04',
                        '2013-09-04', '2013-09-05', '2013-11-07', '2013-11-08',
                        '2013-11-21', '2013-11-22', '2014-02-01', '2014-02-02');
commit;



  -- ##############################################################################################################
  -- ######  Add period "4"                                                                                  ######
  -- ##############################################################################################################
if object_id('EPL_1_Create_Universe_Period_4') is not null then drop procedure EPL_1_Create_Universe_Period_4 end if;
create procedure EPL_1_Create_Universe_Period_4
      @parStartDate             date = null,
      @parEndDate               date = null
as
begin

      declare @varSQL                         varchar(25000)

      execute logger_add_event 0, 0, '##### Processing period: ' || dateformat(@parStartDate, 'dd/mm/yyyy') || ' - '  || dateformat(@parEndDate, 'dd/mm/yyyy') || '#####', null

      if object_id('EPL_tmp_Accounts_Period_4') is not null drop table EPL_tmp_Accounts_Period_4
      select
            Account_Number,
            max(Data_Day) as Data_Day
        into EPL_tmp_Accounts_Period_4
        from EPL_01_Universe
       where Period = 1
         and Valid_Account_Flag = 1
       group by Account_Number
      commit
      execute logger_add_event 0, 0, 'List of account created', @@rowcount
      create unique hg   index idx01 on EPL_tmp_Accounts_Period_4(Account_Number, Data_Day)

      while @parStartDate <= @parEndDate
        begin
            set @varSQL = '
                            execute logger_add_event 0, 0, ''~~~~~ Processing ' || dateformat(@parStartDate, 'dd/mm/yyyy') || ' ~~~~~'', null

                            delete from EPL_01_Universe
                             where Data_Day = ''' || @parStartDate || '''
                               and Period = 4
                            commit
                            execute logger_add_event 0, 0, ''Existing records removed'', @@rowcount

                            insert into EPL_01_Universe
                                  (Account_Number, Period, Data_Day, Valid_Account_Flag, Days_Data_Available, First_ESPN_Viewing,
                                   First_BT_Viewing, DTV_Flag, Sky_Sports_Flag, BT_Sport_Flag, ESPN_Flag)
                              select
                                    a.Account_Number,
                                    4,
                                    ''' || @parStartDate || ''',        -- Data_Day
                                    a.Valid_Account_Flag,
                                    a.Days_Data_Available,
                                    a.First_ESPN_Viewing,
                                    a.First_BT_Viewing,
                                    0,                                  -- DTV_Flag
                                    0,                                  -- Sky_Sports_Flag
                                    a.BT_Sport_Flag,
                                    a.ESPN_Flag
                               from EPL_01_Universe a,
                                    EPL_tmp_Accounts_Period_4 b
                              where a.Account_Number = b.Account_Number
                                and a.Data_Day = b.Data_Day
                            commit

                            execute logger_add_event 0, 0, ''New records added'', @@rowcount
                          '
            execute(@varSQL)

            set @parStartDate = @parStartDate + 1

        end

      execute logger_add_event 0, 0, '##### Process completed #####', null

end;

execute EPL_1_Create_Universe_Period_4 '2014-03-01', '2014-05-31';


update EPL_01_Universe base
   set base.DTV_Flag        = 1,
       base.Sky_Sports_Flag = case
                                when det.Prem_Sports > 0 then 1                                             -- Any Sports counted as "entitled"
                                  else 0
                              end
  from (select
              csh.Account_Number,
              csh.Effective_From_Dt,
              csh.Effective_To_Dt,
              cel.Prem_Sports,
              cel.Sport_1,
              cel.Sport_2
          from sk_prod.cust_subs_hist csh
                  left join sk_prod.cust_entitlement_lookup as cel  on csh.current_short_description = cel.short_description
         where csh.subscription_sub_type = 'DTV Primary Viewing'
           and csh.status_code in ('AC', 'PC', 'AB')
           and csh.effective_from_dt < csh.effective_to_dt) det
 where base.Account_Number = det.Account_Number
   and det.Effective_From_Dt <= base.Data_Day
   and det.Effective_To_Dt > base.Data_Day
   and base.Period = 4;
commit;

update EPL_01_Universe base
   set base.Valid_Account_Flag = 0
 where base.DTV_Flag = 0
   and base.Period = 4;
commit;



  -- ##############################################################################################################
  -- ##############################################################################################################
/*
  -- Manual updates to the universe data
if object_id('EPL_1_Universe_Manual') is not null then drop procedure EPL_1_Universe_Manual end if;
create procedure EPL_1_Universe_Manual
      @parStartDate             date = null,
      @parEndDate               date = null,
      @parPeriod                tinyint = 0
as
begin
      declare @varTableSuffix                 varchar(6)
      declare @varSQL                         varchar(25000)

      set @varTableSuffix       = (dateformat(@parStartDate, 'yyyymm'))

      execute logger_add_event 0, 0, '##### Processing period: ' || dateformat(@parStartDate, 'mm/yyyy') || ' #####', null
      execute logger_add_event 0, 0, 'Source table used: sk_prod.vespa_dp_prog_viewed_' || @varTableSuffix, null

      set @varSQL = '
                      update EPL_01_Universe base
                         set base.BT_Max_Cons_Duration  = 0,
                             base.BT_Total_Duration     = 0
                       where base.Data_Day between ''' || @parStartDate || ''' and ''' || @parEndDate || '''
                         and base.Period = ' || @parPeriod || '
                      commit
                      execute logger_add_event 0, 0, ''Values reset to 0'', @@rowcount


                      update EPL_01_Universe base
                         set base.BT_Max_Cons_Duration  = det.BT_Max_Cons_Duration,
                             base.BT_Total_Duration     = det.BT_Total_Duration
                        from (select
                                    Account_Number,
                                    date(event_start_date_time_utc) as Event_Dt,
                                    max(case                                        -- BT Sport - maximum consecutive viewing in a day
                                          when vw.Service_Key is null then 0
                                          when vw.Service_Key not in (3625, 3627, 3661, 3663) or vw.Video_Playing_Flag = 0 then 0
                                          when vw.capped_partial_flag = 1 then datediff(second, vw.instance_start_date_time_utc, vw.capping_end_date_time_utc)
                                            else datediff(second, vw.instance_start_date_time_utc, vw.instance_end_date_time_utc)
                                        end) as BT_Max_Cons_Duration,
                                    sum(case                                        -- BT Sport - total viewing in a day
                                          when vw.Service_Key is null then 0
                                          when vw.Service_Key not in (3625, 3627, 3661, 3663) or vw.Video_Playing_Flag = 0 then 0
                                          when vw.capped_partial_flag = 1 then datediff(second, vw.instance_start_date_time_utc, vw.capping_end_date_time_utc)
                                            else datediff(second, vw.instance_start_date_time_utc, vw.instance_end_date_time_utc)
                                        end) as BT_Total_Duration
                                from sk_prod.vespa_dp_prog_viewed_' || @varTableSuffix || ' vw
                               where dk_capping_end_datehour_dim > 0                                        -- Events received on time for capping
                                 and panel_id = 12                                                          -- Panel 12 only
                                 and instance_start_date_time_utc < instance_end_date_time_utc              -- Valid events
                                 and account_number is not null
                                 and subscriber_id is not null
                               group by Account_Number, Event_Dt) det
                       where base.Account_Number = det.Account_Number
                         and base.Data_Day = det.Event_Dt
                         and base.Data_Day between ''' || @parStartDate || ''' and ''' || @parEndDate || '''
                         and base.Period = ' || @parPeriod || '
                      commit
                      execute logger_add_event 0, 0, ''Month processed'', @@rowcount
                    '
      execute(@varSQL)

end;

execute EPL_1_Universe_Manual '2013-08-01', '2013-08-31', 1;
execute EPL_1_Universe_Manual '2013-09-01', '2013-09-30', 1;
execute EPL_1_Universe_Manual '2013-10-01', '2013-10-31', 1;
execute EPL_1_Universe_Manual '2013-11-01', '2013-11-30', 1;
execute EPL_1_Universe_Manual '2013-12-01', '2013-12-31', 1;
execute EPL_1_Universe_Manual '2014-01-01', '2014-01-31', 1;
execute EPL_1_Universe_Manual '2014-02-01', '2014-02-28', 1;

update EPL_01_Universe
   set bt_max_cons_duration = 0,
       bt_total_duration = 0
 where BT_Viewing_Flag = 0
   and bt_max_cons_duration > 0
   and Data_Day >= '2014-02-25';
commit;


select * from EPL_01_Universe
where (
       (bt_max_cons_duration = 0 and bt_total_duration <> 0) or
       (bt_max_cons_duration <> 0 and bt_total_duration = 0)
      )
and period = 1;

select * from EPL_01_Universe
where (
       (bt_max_cons_duration = 0 and BT_Viewing_Flag <> 0) or
       (bt_max_cons_duration <> 0 and BT_Viewing_Flag = 0)
      )
and period = 1;


  -- Reset values
update EPL_01_Universe base
   set base.First_BT_Viewing      = null,
       base.BT_Sport_Flag         = 0;
commit;

update EPL_01_Universe base
   set base.First_BT_Viewing      = det.First_BT_Viewing,
       base.BT_Sport_Flag         = case
                                      when det.First_BT_Viewing <= base.Data_Day and DTV_Flag = 1 then 1
                                        else 0
                                    end
  from (select
              Account_Number,
              Period,
              count(distinct Data_Day) as Days_Data_Available,
              min(case
                    when ESPN_Viewing_Flag = 1 then Data_Day
                      else null
                  end) as First_ESPN_Viewing,
              min(case
                    when Data_Day in ('2013-10-25', '2013-10-26', '2013-10-27', '2014-12-22',               -- BT free weekend
                                      '2014-02-15') then null
                    when BT_Viewing_Flag = 1 and BT_Max_Cons_Duration >= 15 * 60 then Data_Day              -- Must have watched for at at least 15minutes consecutively to be counted
                      else null
                  end) as First_BT_Viewing
          from EPL_01_Universe
         where Data_Day not in ('2013-10-19', '2013-10-26', '2013-11-02',                                   -- BT Sport issue
                                '2013-02-04', '2013-02-23', '2013-04-30', '2013-06-12', '2013-06-13',       -- Poor source data
                                '2013-06-14', '2013-06-15', '2013-06-16', '2013-06-17', '2013-07-01',
                                '2013-07-02', '2013-07-04', '2013-09-04', '2013-09-05', '2013-11-07',
                                '2013-11-08', '2013-11-21', '2013-11-22', '2014-02-01', '2014-02-02')
         group by Account_Number, Period) det
 where base.Account_Number = det.Account_Number
   and base.Period = det.Period;
commit;

  -- Test
select account_number, data_day, valid_account_flag, dtv_flag, bt_viewing_flag, bt_max_cons_duration, first_bt_viewing, bt_sport_flag from EPL_01_Universe
where account_number in ('621288449609', '621288450508', '621288457362', '621288458386', '621288517355')
and period = 1
order by 1,2;

*/






















