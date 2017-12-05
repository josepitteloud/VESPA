/*###############################################################################
# Created on:   10/06/2014
# Created by:   Sebastian Bednaszynski(SBE)
# Description:
#
#################################################################################
# Dependencies
# ------------------------------------------------------------------------------
# => Tables/objects required:
#
#################################################################################
# Change log
# ------------------------------------------------------------------------------
# 10/06/2014  SBE   Initial version
#
###############################################################################*/


  -- ##############################################################################################################
  -- ##### Setup tables                                                                                       #####
  -- ##############################################################################################################
if object_id('SPG_BTS_01_Universe') is not null then drop table SPG_BTS_01_Universe end if;
create table SPG_BTS_01_Universe (
    Pk_Identifier                           bigint            identity,
    Updated_On                              datetime          not null  default timestamp,
    Updated_By                              varchar(30)       not null  default user_name(),

    Account_Number                          varchar(20)       null      default null,
    DTV_Active                              bit               not null  default 0,

    Prod_Sky_Sports                         varchar(5)        null      default 'No',
    Prod_Sky_Movies                         varchar(5)        null      default 'No',
    Prod_HD                                 varchar(5)        null      default 'No',
    Prod_Multiscreen                        varchar(5)        null      default 'No',
    Prod_Broadband                          varchar(5)        null      default 'No',
    Prod_Sky_Talk                           varchar(5)        null      default 'No',

    Downgrade_DTV                           date              null      default null,
    Downgrade_Sky_Sports                    date              null      default null,
    Downgrade_Sky_Movies                    date              null      default null,
    Downgrade_HD                            date              null      default null,
    Downgrade_Multiscreen                   date              null      default null,
    Downgrade_Broadband                     date              null      default null,
    Downgrade_Sky_Talk                      date              null      default null,

    BT_Sport_Watched__DTV                   bigint            null      default 0,         -- Time BT Sport was watched prior to the event (within given period)
    BT_Sport_Watched__Sky_Sports            bigint            null      default 0,         -- Time BT Sport was watched prior to the event (within given period)
    BT_Sport_Watched__Sky_Movies            bigint            null      default 0,         -- Time BT Sport was watched prior to the event (within given period)
    BT_Sport_Watched__HD                    bigint            null      default 0,         -- Time BT Sport was watched prior to the event (within given period)
    BT_Sport_Watched__Multiscreen           bigint            null      default 0,         -- Time BT Sport was watched prior to the event (within given period)
    BT_Sport_Watched__Broadband             bigint            null      default 0,         -- Time BT Sport was watched prior to the event (within given period)
    BT_Sport_Watched__Sky_Talk              bigint            null      default 0          -- Time BT Sport was watched prior to the event (within given period)

);
create unique hg index idx1 on SPG_BTS_01_Universe(Account_Number);


if object_id('SPG_BTS_02_Product_Statuses') is not null then drop table SPG_BTS_02_Product_Statuses end if;
create table SPG_BTS_02_Product_Statuses (
    Pk_Identifier                           bigint            identity,
    Updated_On                              datetime          not null  default timestamp,
    Updated_By                              varchar(30)       not null  default user_name(),

    Account_Number                          varchar(20)       null      default null,
    Product                                 varchar(20)       null      default null,
    Effective_From_Dt                       date              null      default null,
    Effective_To_Dt                         date              null      default null,
    Status                                  varchar(20)       null      default null,

);
create        hg index idx1 on SPG_BTS_02_Product_Statuses(Account_Number);
create      date index idx2 on SPG_BTS_02_Product_Statuses(Effective_From_Dt);
create        lf index idx3 on SPG_BTS_02_Product_Statuses(Product);


if object_id('SPG_BTS_03_BT_Sport_Viewing') is not null then drop table SPG_BTS_03_BT_Sport_Viewing end if;
create table SPG_BTS_03_BT_Sport_Viewing (
    Pk_Identifier                           bigint            identity,
    Updated_On                              datetime          not null  default timestamp,
    Updated_By                              varchar(30)       not null  default user_name(),

    Account_Number                          varchar(20)       null      default null,
    Service_Key                             smallint          null      default null,
    Viewing_Start_Date                      date              null      default null,
    Viewing_Start_Time                      datetime          null      default null,
    Viewing_Duration                        bigint            null      default 0,
    Instances_Num                           smallint          null      default 0

);
create        hg index idx1 on SPG_BTS_03_BT_Sport_Viewing(Account_Number);
create      date index idx2 on SPG_BTS_03_BT_Sport_Viewing(Viewing_Start_Date);
create      dttm index idx3 on SPG_BTS_03_BT_Sport_Viewing(Viewing_Start_Time);


if object_id('SPG_BTS_04_Event_Dates') is not null then drop table SPG_BTS_04_Event_Dates end if;
create table SPG_BTS_04_Event_Dates (
    Pk_Identifier                           bigint            identity,
    Updated_On                              datetime          not null  default timestamp,
    Updated_By                              varchar(30)       not null  default user_name(),

    Account_Number                          varchar(20)       null      default null,
    Reference_Date                          date              null      default null,
    Observation_Period                      bigint            null      default 0,
    BT_Sport_Viewing                        bigint            null      default 0

);
create        hg index idx1 on SPG_BTS_04_Event_Dates(Account_Number);
create      date index idx2 on SPG_BTS_04_Event_Dates(Reference_Date);
create unique hg index idx3 on SPG_BTS_04_Event_Dates(Account_Number, Reference_Date);



if object_id('SPG_BTS_10_Summary') is not null then drop table SPG_BTS_10_Summary end if;
create table SPG_BTS_10_Summary (
    Pk_Identifier                           bigint            identity,
    Updated_On                              datetime          not null  default timestamp,
    Updated_By                              varchar(30)       not null  default user_name(),

    Product                                 varchar(50)       null      default null,
    Downgrade_Flag                          varchar(20)       null      default null,
    BT_Sports_Viewing                       varchar(20)       null      default null,
    Accounts_Unscaled                       bigint            null      default 0


);
create        lf index idx1 on SPG_BTS_10_Summary(Product);
create        lf index idx2 on SPG_BTS_10_Summary(BT_Sports_Viewing);
create        lf index idx3 on SPG_BTS_10_Summary(Downgrade_Flag);





  -- ##############################################################################################################
  -- ##### Get list of unique accounts who watched anything in July/August to create the universe             #####
  -- ##############################################################################################################
-- truncate table SPG_BTS_01_Universe;
insert into SPG_BTS_01_Universe
       (Account_Number)
  select distinct
        Account_Number
    from sk_prod.vespa_dp_prog_viewed_201307
  union
  select distinct
        Account_Number
    from sk_prod.vespa_dp_prog_viewed_201308;
commit;


  -- ##############################################################################################################
  -- ##### Calculate product holding as of 01/08/2013                                                         #####
  -- ##############################################################################################################
update SPG_BTS_01_Universe a
   set a.DTV_Active       = case
                              when DTV_Flag = 1 then 1
                                else 0
                            end,
       a.Prod_Sky_Sports  = case
                              when Num_Prem_Sports > 0 then 'Yes'
                                else 'No'
                            end,
       a.Prod_Sky_Movies  = case
                              when Num_Prem_Movies > 0 then 'Yes'
                                else 'No'
                            end,
       a.Prod_HD          = case
                              when HD_Flag = 1 then 'Yes'
                                else 'No'
                            end,
       a.Prod_Multiscreen = case
                              when b.Multiscreen_Flag = 1 then 'Yes'
                                else 'No'
                            end,
       a.Prod_Broadband   = case
                              when b.BB_Flag = 1 then 'Yes'
                                else 'No'
                            end,
       a.Prod_Sky_Talk    = 'N/A'                                               -- Not required/calculated at this moment

  from (select
              a.Account_Number,

              max(case
                    when a.subscription_sub_type = 'DTV Primary Viewing' and a.status_code in ('AC','AB','PC') then 1
                      else 0
                  end) as DTV_Flag,

              max(case
                    when a.subscription_sub_type = 'DTV Primary Viewing' then b.Prem_Sports
                      else null
                  end) as Num_Prem_Sports,

              max(case
                    when a.subscription_sub_type = 'DTV Primary Viewing' then b.Prem_Movies
                      else null
                  end) as Num_Prem_Movies,

              max(case
                    when a.subscription_sub_type = 'DTV HD' and a.status_code in ('AC','AB','PC') then 1                                              -- HD Basic
                    when a.subscription_sub_type = 'HD Pack' and a.status_code in ('AC','AB','PC') then 1                                             -- HD Premium
                    when a.subscription_sub_type = 'DTV HD' and a.status_code in ('AC','AB','PC') and current_short_description in ('SKY_HD') then 1  -- HD Premium
                      else 0
                  end) as HD_Flag,

              max(case
                    when a.subscription_sub_type = 'DTV Extra Subscription' and a.status_code in ('AC','AB','PC') then 1
                      else 0
                  end) as Multiscreen_Flag,

              max(case
                    when a.subscription_sub_type = 'Broadband DSL Line' and
                         (       a.status_code in ('AC','AB')
                             or (a.status_code='PC' AND a.prev_status_code not in ('?','RQ','AP','UB','BE','PA') )
                             or (a.status_code='CF' AND a.prev_status_code = 'PC'                                )
                             or (a.status_code='AP' AND a.sale_type = 'SNS Bulk Migration'                       )
                          ) then 1
                      else 0
                  end) as BB_Flag

          from sk_prod.cust_subs_hist a left join sk_prod.cust_entitlement_lookup b
                  on a.current_short_description = b.short_description
         where a.effective_from_dt  < '2013-09-15'
           and a.effective_to_dt >= '2013-09-15'
         group by a.Account_Number) b
 where a.Account_Number = b.Account_Number;
commit;


  -- ##############################################################################################################
  -- ##### Get all product holding changes                                                                    #####
  -- ##############################################################################################################
  -- DTV based products (packages)
  -- PRODUCTS
insert into SPG_BTS_02_Product_Statuses
      (Account_Number, Product, Effective_From_Dt, Effective_To_Dt, Status)
  select
        csh.Account_Number,
        csh.Product,
        csh.Effective_From_Dt,
        csh.Effective_To_Dt,
        case
          when csh.Product <> 'Broadband' and csh.Prev_Status_Code in ('AC', 'PC', 'AB') and csh.Status_Code in ('PO', 'SC') then 'Downgrade'
          when csh.Product <> 'Broadband' and csh.Prev_Status_Code not in ('AC', 'PC', 'AB') and csh.Status_Code in ('AC', 'PC', 'AB') then 'Upgrade'
          when csh.Product = 'Broadband' and
                 -- Previous
               (       csh.Prev_Status_Code in ('AC','AB')
                   or (csh.Prev_Status_Code = 'PC' AND csh.Prev_Prev_Status_Code not in ('?','RQ','AP','UB','BE','PA') )
                   or (csh.Prev_Status_Code = 'CF' AND csh.Prev_Prev_Status_Code = 'PC'                                )
                   or (csh.Prev_Status_Code = 'AP' AND csh.Sale_Type = 'SNS Bulk Migration'                       )
                ) and

                 -- Current
               not(       csh.Status_Code in ('AC','AB')
                     or (csh.Status_Code = 'PC' AND csh.Prev_Status_Code not in ('?','RQ','AP','UB','BE','PA') )
                     or (csh.Status_Code = 'CF' AND csh.Prev_Status_Code = 'PC'                                )
                     or (csh.Status_Code = 'AP' AND csh.Sale_Type = 'SNS Bulk Migration'                       )
                  ) then 'Downgrade'
          when csh.Product = 'Broadband' and
                 -- Previous
               not(       csh.Prev_Status_Code in ('AC','AB')
                     or (csh.Prev_Status_Code = 'PC' AND csh.Prev_Prev_Status_Code not in ('?','RQ','AP','UB','BE','PA') )
                     or (csh.Prev_Status_Code = 'CF' AND csh.Prev_Prev_Status_Code = 'PC'                                )
                     or (csh.Prev_Status_Code = 'AP' AND csh.Sale_Type = 'SNS Bulk Migration'                       )
                  ) and

                 -- Current
               (       csh.Status_Code in ('AC','AB')
                   or (csh.Status_Code = 'PC' AND csh.Prev_Status_Code not in ('?','RQ','AP','UB','BE','PA') )
                   or (csh.Status_Code = 'CF' AND csh.Prev_Status_Code = 'PC'                                )
                   or (csh.Status_Code = 'AP' AND csh.Sale_Type = 'SNS Bulk Migration'                       )
                ) then 'Upgrade'
            else 'Other'
        end

    from (select
                a.Account_Number,
                case
                  when a.Subscription_Sub_Type = 'DTV Primary Viewing' then 'DTV'
                  when a.Subscription_Sub_Type in ('DTV HD', 'HD Pack') then 'HD'
                  when a.Subscription_Sub_Type = 'DTV Extra Subscription' then 'Multiscreen'
                  when a.Subscription_Sub_Type = 'Broadband DSL Line' then 'Broadband'
                    else '???'
                end as Product,
                a.Effective_From_Dt,
                a.Effective_To_Dt,
                a.Subscription_Id,
                lag(a.Prev_Status_Code, 1) over(partition by a.Subscription_Id order by a.Effective_From_Dt, a.Effective_To_Dt, a.Effective_From_Dt_Seq) as Prev_Prev_Status_Code,
                a.Prev_Status_Code,
                a.Status_Code,
                a.Sale_Type,
                a.Effective_From_Dt_Seq

            from SPG_BTS_01_Universe un,
                 sk_prod.cust_subs_hist a
           where un.Account_Number = a.Account_Number
             and a.Subscription_Sub_Type in ('DTV Primary Viewing', 'DTV HD', 'HD Pack', 'DTV Extra Subscription', 'Broadband DSL Line')
             and a.Effective_From_Dt >= '2013-09-15'                                                          -- Status changes within the period
             and a.Effective_From_Dt <= '2014-06-05') csh;                                                    -- Status changes within the period
commit;


  -- SPORTS
insert into SPG_BTS_02_Product_Statuses
      (Account_Number, Product, Effective_From_Dt, Effective_To_Dt, Status)
  select
        a.Account_Number,
        'Sports Premium',
        Effective_From_Dt,
        Effective_To_Dt,
        case
          when b.Prem_Sports in (1, 2) and c.Prem_Sports = 0 then 'Downgrade'
          when b.Prem_Sports = 0 and c.Prem_Sports in (1, 2) then 'Upgrade'
            else 'No change'
        end
    from SPG_BTS_01_Universe un,
         sk_prod.cust_subs_hist a
            left join sk_prod.cust_entitlement_lookup b on a.previous_short_description = b.short_description
            left join sk_prod.cust_entitlement_lookup c on a.current_short_description = c.short_description
   where un.Account_Number = a.Account_Number
     and a.subscription_sub_type = 'DTV Primary Viewing'
     and b.Prem_Sports <> c.Prem_Sports
     and a.Status_Code in ('AC', 'PC', 'AB')
     and a.effective_from_dt >= '2013-09-15'                                                          -- Status changes within the period
     and a.effective_from_dt <= '2014-06-05';                                                         -- Status changes within the period
commit;


  -- MOVIES
insert into SPG_BTS_02_Product_Statuses
      (Account_Number, Product, Effective_From_Dt, Effective_To_Dt, Status)
  select
        a.Account_Number,
        'Movies Premium',
        Effective_From_Dt,
        Effective_To_Dt,
        case
          when b.Prem_Movies in (1, 2) and c.Prem_Movies = 0 then 'Downgrade'
          when b.Prem_Movies = 0 and c.Prem_Movies in (1, 2) then 'Upgrade'
            else 'No change'
        end
    from SPG_BTS_01_Universe un,
         sk_prod.cust_subs_hist a
            left join sk_prod.cust_entitlement_lookup b on a.previous_short_description = b.short_description
            left join sk_prod.cust_entitlement_lookup c on a.current_short_description = c.short_description
   where un.Account_Number = a.Account_Number
     and a.subscription_sub_type = 'DTV Primary Viewing'
     and b.Prem_Movies <> c.Prem_Movies
     and a.Status_Code in ('AC', 'PC', 'AB')
     and a.effective_from_dt >= '2013-09-15'                                                          -- Status changes within the period
     and a.effective_from_dt <= '2014-06-05';                                                         -- Status changes within the period
commit;


  -- Check
select Product, Status, count(*) as Cnt
  from SPG_BTS_02_Product_Statuses
 group by Product, Status
 order by Product, Status;


  -- Update main table
update SPG_BTS_01_Universe base
   set base.Downgrade_DTV           = det.Downgrade_DTV,
       base.Downgrade_Sky_Sports    = det.Downgrade_Sky_Sports,
       base.Downgrade_Sky_Movies    = det.Downgrade_Sky_Movies,
       base.Downgrade_HD            = det.Downgrade_HD,
       base.Downgrade_Multiscreen   = det.Downgrade_Multiscreen,
       base.Downgrade_Broadband     = det.Downgrade_Broadband,
       base.Downgrade_Sky_Talk      = det.Downgrade_Sky_Talk
  from (select
              Account_Number,
              min(case when Product = 'DTV' and Status = 'Downgrade' then Effective_From_Dt else null end) as Downgrade_DTV,
              min(case when Product = 'Sports Premium' and Status = 'Downgrade' then Effective_From_Dt else null end) as Downgrade_Sky_Sports,
              min(case when Product = 'Movies Premium' and Status = 'Downgrade' then Effective_From_Dt else null end) as Downgrade_Sky_Movies,
              min(case when Product = 'HD' and Status = 'Downgrade' then Effective_From_Dt else null end) as Downgrade_HD,
              min(case when Product = 'Multiscreen' and Status = 'Downgrade' then Effective_From_Dt else null end) as Downgrade_Multiscreen,
              min(case when Product = 'Broadband' and Status = 'Downgrade' then Effective_From_Dt else null end) as Downgrade_Broadband,
              min(case when Product = 'Sky Talk' and Status = 'Downgrade' then Effective_From_Dt else null end) as Downgrade_Sky_Talk          -- Not handled at the moment
          from SPG_BTS_02_Product_Statuses
         where Effective_From_Dt + 1 < Effective_To_Dt                                                    -- Downgrades lasting at least 1 full day
         group by Account_Number) det
 where base.Account_Number = det.Account_Number;
commit;



  -- ##############################################################################################################
  -- ##### Get all instances when BT Sports was watched in the period                                         #####
  -- ##############################################################################################################
if object_id('p_SPG_BTS_01_BTS_Viewing') is not null then drop procedure p_SPG_BTS_01_BTS_Viewing end if;
create procedure p_SPG_BTS_01_BTS_Viewing
      @parStartDate             date = null,
      @parEndDate               date = null      -- Must be the same month as Start Date!!!!
as
begin
      declare @varStartDateHour               bigint
      declare @varEndDateHour                 bigint
      declare @varTableSuffix                 varchar(6)
      declare @varSQL                         varchar(25000)

      set @varStartDateHour = dateformat(@parStartDate, 'yyyymmdd00')
      set @varEndDateHour   = dateformat(@parEndDate, 'yyyymmdd23')

      set @varTableSuffix       = (dateformat(@parStartDate, 'yyyymm'))


      execute logger_add_event 0, 0, '##### Processing period: ' || dateformat(@parStartDate, 'dd/mm/yyyy') || ' - '  || dateformat(@parEndDate, 'dd/mm/yyyy') || ' #####', null
      execute logger_add_event 0, 0, 'Date-hours used: ' || @varStartDateHour || ' - '  || @varEndDateHour, null
      execute logger_add_event 0, 0, 'Source table used: sk_prod.vespa_dp_prog_viewed_' || @varTableSuffix, null


      set @varSQL = '
                      delete from SPG_BTS_03_BT_Sport_Viewing
                       where Viewing_Start_Date between ''' || @parStartDate || ''' and ''' || @parEndDate || '''
                      commit

                      execute logger_add_event 0, 0, ''Viewing data rows for current period removed'', @@rowcount


                      insert into SPG_BTS_03_BT_Sport_Viewing
                             (Account_Number, Service_Key, Viewing_Start_Date, Viewing_Start_Time, Viewing_Duration, Instances_Num)
                      select
                            un.Account_Number,
                            vw.Service_Key,
                            date(vw.Event_Start_Date_Time_UTC) as xViewing_Start_Date,
                            vw.Event_Start_Date_Time_UTC,
                            sum(case
                                  when vw.capped_partial_flag = 1 then datediff(second, vw.instance_start_date_time_utc, vw.capping_end_date_time_utc)
                                    else datediff(second, vw.instance_start_date_time_utc, vw.instance_end_date_time_utc)
                                end) as xViewing_Duration,                                          -- Viewing_Duration (summarised for each event)
                            count(*) as xInstances_Num

                        from SPG_BTS_01_Universe un,
                             sk_prod.sk_prod.vespa_dp_prog_viewed_' || @varTableSuffix || ' vw
                                left join VESPA_Analysts.Channel_Map_Prod_Service_Key_Attributes cm       on vw.Service_Key = cm.Service_Key
                                                                                                         and cm.Effective_From < date(broadcast_start_date_time_utc)
                                                                                                         and cm.Effective_To >= date(broadcast_start_date_time_utc)

                       where un.Account_Number = vw.Account_Number
                         and vw.Dk_Event_Start_Datehour_Dim between ' || @varStartDateHour || ' and ' || @varEndDateHour || '
                         and vw.Service_Key in (3625, 3661, 3627, 3663)                             -- BT Sports 1/2
                         and vw.dk_capping_end_datehour_dim > 0                                     -- Events received on time for capping
                         and vw.capped_full_flag = 0                                                -- Filter out instances beyond capping point
                         and vw.Instance_Start_Date_Time_UTC < vw.Capping_End_Date_Time_UTC         -- Fallback criterion when capped full flag is not set when it is supposed to be
                         and vw.panel_id in (11, 12)                                                -- Daily panels
                         and vw.instance_start_date_time_utc < vw.instance_end_date_time_utc        -- Remove 0sec instances
                         and vw.account_number is not null                                          -- Known Ids only
                         and vw.subscriber_id is not null                                           -- Known Ids only
                         and vw.broadcast_start_date_time_utc >= dateadd(hour, -(24*28), vw.Event_Start_Date_Time_UTC)  -- Viewed withing 28 days of recording time
                         and (vw.reported_playback_speed is null or vw.reported_playback_speed = 2) -- Live/playback true viewing only
                         and (
                               vw.type_of_viewing_event in (''HD Viewing Event'', ''TV Channel Viewing'')
                               or
                               (
                                 vw.type_of_viewing_event = ''Other Service Viewing Event''
                                 and
                                 cm.Channel_Type in (''Retail - Pay-per-night'', ''Retail - Pay-per-view'',
                                                     ''Retail - PPV HD'', ''NR - Pay-per-view'')
                               )
                               or
                               (
                                 vw.type_of_viewing_event = ''Sky+ time-shifted viewing event''
                                 and
                                 cm.Channel_Type <> ''NR - FTA - Radio''
                               )
                             )
                         and vw.Video_Playing_Flag = 1
                       group by
                            un.Account_Number,
                            vw.Service_Key,
                            xViewing_Start_Date,
                            vw.Event_Start_Date_Time_UTC
                      commit

                      execute logger_add_event 0, 0, ''Data has been processed'', @@rowcount
                    '

      execute(@varSQL)


end;

execute p_SPG_BTS_01_BTS_Viewing '2013-08-01', '2013-08-31';
execute p_SPG_BTS_01_BTS_Viewing '2013-09-01', '2013-09-30';
execute p_SPG_BTS_01_BTS_Viewing '2013-10-01', '2013-10-31';
execute p_SPG_BTS_01_BTS_Viewing '2013-11-01', '2013-11-30';
execute p_SPG_BTS_01_BTS_Viewing '2013-12-01', '2013-12-31';
execute p_SPG_BTS_01_BTS_Viewing '2014-01-01', '2014-01-31';
execute p_SPG_BTS_01_BTS_Viewing '2014-02-01', '2014-02-28';
execute p_SPG_BTS_01_BTS_Viewing '2014-03-01', '2014-03-31';
execute p_SPG_BTS_01_BTS_Viewing '2014-04-01', '2014-04-30';
execute p_SPG_BTS_01_BTS_Viewing '2014-05-01', '2014-05-31';
execute p_SPG_BTS_01_BTS_Viewing '2014-06-01', '2014-06-05';



  -- ##############################################################################################################
  -- ##### Append BT Sport viewing                                                                            #####
  -- ##############################################################################################################
  -- Get all possible dates
truncate table SPG_BTS_04_Event_Dates;
insert into SPG_BTS_04_Event_Dates
      (Account_Number, Reference_Date, Observation_Period)
    -- Downgrade_DTV
  select
        Account_Number,
        case when Downgrade_DTV is null then cast('2014-06-06' as date) else Downgrade_DTV end,                         -- No movement then set the date 1 day after analysis period
        case when Downgrade_DTV is null then datediff(day, cast('2013-08-01' as date), cast('2014-06-06' as date)) else 45 end
    from SPG_BTS_01_Universe
  union
    -- Downgrade_Sky_Sports
  select
        Account_Number,
        case when Downgrade_Sky_Sports is null then cast('2014-06-06' as date) else Downgrade_Sky_Sports end,           -- No movement then set the date 1 day after analysis period
        case when Downgrade_Sky_Sports is null then datediff(day, cast('2013-08-01' as date), cast('2014-06-06' as date)) else 45 end
    from SPG_BTS_01_Universe
  union
    -- Downgrade_Sky_Movies
  select
        Account_Number,
        case when Downgrade_Sky_Movies is null then cast('2014-06-06' as date) else Downgrade_Sky_Movies end,           -- No movement then set the date 1 day after analysis period
        case when Downgrade_Sky_Movies is null then datediff(day, cast('2013-08-01' as date), cast('2014-06-06' as date)) else 45 end
    from SPG_BTS_01_Universe
  union
    -- Downgrade_HD
  select
        Account_Number,
        case when Downgrade_HD is null then cast('2014-06-06' as date) else Downgrade_HD end,                           -- No movement then set the date 1 day after analysis period
        case when Downgrade_HD is null then datediff(day, cast('2013-08-01' as date), cast('2014-06-06' as date)) else 45 end
    from SPG_BTS_01_Universe
  union
    -- Downgrade_Multiscreen
  select
        Account_Number,
        case when Downgrade_Multiscreen is null then cast('2014-06-06' as date) else Downgrade_Multiscreen end,         -- No movement then set the date 1 day after analysis period
        case when Downgrade_Multiscreen is null then datediff(day, cast('2013-08-01' as date), cast('2014-06-06' as date)) else 45 end
    from SPG_BTS_01_Universe
  union
    -- Downgrade_Broadband
  select
        Account_Number,
        case when Downgrade_Broadband is null then cast('2014-06-06' as date) else Downgrade_Broadband end,             -- No movement then set the date 1 day after analysis period
        case when Downgrade_Broadband is null then datediff(day, cast('2013-08-01' as date), cast('2014-06-06' as date)) else 45 end
    from SPG_BTS_01_Universe
  union
    -- Downgrade_DTV
  select
        Account_Number,
        case when Downgrade_Sky_Talk is null then cast('2014-06-06' as date) else Downgrade_Sky_Talk end,               -- No movement then set the date 1 day after analysis period
        case when Downgrade_Sky_Talk is null then datediff(day, cast('2013-08-01' as date), cast('2014-06-06' as date)) else 45 end
    from SPG_BTS_01_Universe;
commit;


  -- Calculate BT Sport viewing in defined periods
update SPG_BTS_04_Event_Dates base
   set base.BT_Sport_Viewing  = det.Max_Viewing_Duration
  from (select
              a.Account_Number,
              a.Reference_Date,
              max(case when b.Viewing_Duration is null then 0 else b.Viewing_Duration end) as Max_Viewing_Duration
          from SPG_BTS_04_Event_Dates a left join SPG_BTS_03_BT_Sport_Viewing b
                  on a.Account_Number = b.Account_Number
                 and b.Viewing_Start_Date between a.Reference_Date - a.Observation_Period and a.Reference_Date
         group by a.Account_Number, a.Reference_Date) det
 where base.Account_Number = det.Account_Number
   and base.Reference_Date = det.Reference_Date;
commit;

  -- UPDATE MASTER TABLE NOW
  -- BT_Sport_Watched__DTV
update SPG_BTS_01_Universe base
   set base.BT_Sport_Watched__DTV = det.BT_Sport_Viewing
  from SPG_BTS_04_Event_Dates det
 where base.Account_Number = det.Account_Number
   and (
        base.Downgrade_DTV = det.Reference_Date
        or
        (base.Downgrade_DTV is null and det.Reference_Date = '2014-06-06')
       );
commit;

  -- BT_Sport_Watched__Sky_Sports
update SPG_BTS_01_Universe base
   set base.BT_Sport_Watched__Sky_Sports = det.BT_Sport_Viewing
  from SPG_BTS_04_Event_Dates det
 where base.Account_Number = det.Account_Number
   and (
        base.Downgrade_Sky_Sports = det.Reference_Date
        or
        (base.Downgrade_Sky_Sports is null and det.Reference_Date = '2014-06-06')
       );
commit;

  -- BT_Sport_Watched__Sky_Movies
update SPG_BTS_01_Universe base
   set base.BT_Sport_Watched__Sky_Movies = det.BT_Sport_Viewing
  from SPG_BTS_04_Event_Dates det
 where base.Account_Number = det.Account_Number
   and (
        base.Downgrade_Sky_Movies = det.Reference_Date
        or
        (base.Downgrade_Sky_Movies is null and det.Reference_Date = '2014-06-06')
       );
commit;

  -- BT_Sport_Watched__HD
update SPG_BTS_01_Universe base
   set base.BT_Sport_Watched__HD = det.BT_Sport_Viewing
  from SPG_BTS_04_Event_Dates det
 where base.Account_Number = det.Account_Number
   and (
        base.Downgrade_HD = det.Reference_Date
        or
        (base.Downgrade_HD is null and det.Reference_Date = '2014-06-06')
       );
commit;

  -- BT_Sport_Watched__Multiscreen
update SPG_BTS_01_Universe base
   set base.BT_Sport_Watched__Multiscreen = det.BT_Sport_Viewing
  from SPG_BTS_04_Event_Dates det
 where base.Account_Number = det.Account_Number
   and (
        base.Downgrade_Multiscreen = det.Reference_Date
        or
        (base.Downgrade_Multiscreen is null and det.Reference_Date = '2014-06-06')
       );
commit;

  -- BT_Sport_Watched__Broadband
update SPG_BTS_01_Universe base
   set base.BT_Sport_Watched__Broadband = det.BT_Sport_Viewing
  from SPG_BTS_04_Event_Dates det
 where base.Account_Number = det.Account_Number
   and (
        base.Downgrade_Broadband = det.Reference_Date
        or
        (base.Downgrade_Broadband is null and det.Reference_Date = '2014-06-06')
       );
commit;

  -- BT_Sport_Watched__Sky_Talk
update SPG_BTS_01_Universe base
   set base.BT_Sport_Watched__Sky_Talk = det.BT_Sport_Viewing
  from SPG_BTS_04_Event_Dates det
 where base.Account_Number = det.Account_Number
   and (
        base.Downgrade_Sky_Talk = det.Reference_Date
        or
        (base.Downgrade_Sky_Talk is null and det.Reference_Date = '2014-06-06')
       );
commit;



  -- ##############################################################################################################
  -- ##### Summarise                                                                                          #####
  -- ##############################################################################################################
  -- ~~~~~ DTV ~~~~~
delete from SPG_BTS_10_Summary
 where Product = 'DTV';
commit;

insert into SPG_BTS_10_Summary
      (Product, Downgrade_Flag, BT_Sports_Viewing, Accounts_Unscaled)
  select
        'DTV',
        case when Downgrade_DTV is null then 'No' else 'Yes' end              as xDowngrade_Flag,
        case when BT_Sport_Watched__DTV >= 15 * 60 then 'Yes' else 'No' end   as xBT_Sports_Viewing,
        count(Account_Number)
    from SPG_BTS_01_Universe
   where DTV_Active = 1                                                     -- Active DTV customer
   group by xDowngrade_Flag, xBT_Sports_Viewing;
commit;


  -- ~~~~~ Sky Sports ~~~~~
delete from SPG_BTS_10_Summary
 where Product = 'Sports Premium';
commit;

insert into SPG_BTS_10_Summary
      (Product, Downgrade_Flag, BT_Sports_Viewing, Accounts_Unscaled)
  select
        'Sports Premium',
        case when Downgrade_Sky_Sports is null then 'No' else 'Yes' end               as xDowngrade_Flag,
        case when BT_Sport_Watched__Sky_Sports >= 15 * 60 then 'Yes' else 'No' end    as xBT_Sports_Viewing,
        count(Account_Number)
    from SPG_BTS_01_Universe
   where DTV_Active = 1                                                     -- Active DTV customer
     and Prod_Sky_Sports = 'Yes'                                            -- Active product
     and (
          Downgrade_DTV is null                                             -- No DTV churn at all
          or
          Downgrade_DTV not between Downgrade_Sky_Sports - 35 and Downgrade_Sky_Sports + 35 -- DTV churn no adjacent to product churn
         )
   group by xDowngrade_Flag, xBT_Sports_Viewing;
commit;


  -- ~~~~~ Sky Movies ~~~~~
delete from SPG_BTS_10_Summary
 where Product = 'Movies Premium';
commit;

insert into SPG_BTS_10_Summary
      (Product, Downgrade_Flag, BT_Sports_Viewing, Accounts_Unscaled)
  select
        'Movies Premium',
        case when Downgrade_Sky_Movies is null then 'No' else 'Yes' end               as xDowngrade_Flag,
        case when BT_Sport_Watched__Sky_Movies >= 15 * 60 then 'Yes' else 'No' end    as xBT_Sports_Viewing,
        count(Account_Number)
    from SPG_BTS_01_Universe
   where DTV_Active = 1                                                     -- Active DTV customer
     and Prod_Sky_Movies = 'Yes'                                            -- Active product
     and (
          Downgrade_DTV is null                                             -- No DTV churn at all
          or
          Downgrade_DTV not between Downgrade_Sky_Movies - 35 and Downgrade_Sky_Movies + 35 -- DTV churn no adjacent to product churn
         )
   group by xDowngrade_Flag, xBT_Sports_Viewing;
commit;


  -- ~~~~~ HD ~~~~~
delete from SPG_BTS_10_Summary
 where Product = 'HD';
commit;

insert into SPG_BTS_10_Summary
      (Product, Downgrade_Flag, BT_Sports_Viewing, Accounts_Unscaled)
  select
        'HD',
        case when Downgrade_HD is null then 'No' else 'Yes' end               as xDowngrade_Flag,
        case when BT_Sport_Watched__HD >= 15 * 60 then 'Yes' else 'No' end    as xBT_Sports_Viewing,
        count(Account_Number)
    from SPG_BTS_01_Universe
   where DTV_Active = 1                                                     -- Active DTV customer
     and Prod_HD = 'Yes'                                                    -- Active product
     and (
          Downgrade_DTV is null                                             -- No DTV churn at all
          or
          Downgrade_DTV not between Downgrade_HD - 35 and Downgrade_HD + 35 -- DTV churn no adjacent to product churn
         )
   group by xDowngrade_Flag, xBT_Sports_Viewing;
commit;


  -- ~~~~~ Multiscreen ~~~~~
delete from SPG_BTS_10_Summary
 where Product = 'Multiscreen';
commit;

insert into SPG_BTS_10_Summary
      (Product, Downgrade_Flag, BT_Sports_Viewing, Accounts_Unscaled)
  select
        'Multiscreen',
        case when Downgrade_Multiscreen is null then 'No' else 'Yes' end               as xDowngrade_Flag,
        case when BT_Sport_Watched__Multiscreen >= 15 * 60 then 'Yes' else 'No' end    as xBT_Sports_Viewing,
        count(Account_Number)
    from SPG_BTS_01_Universe
   where DTV_Active = 1                                                     -- Active DTV customer
     and Prod_Multiscreen = 'Yes'                                           -- Active product
     and (
          Downgrade_DTV is null                                             -- No DTV churn at all
          or
          Downgrade_DTV not between Downgrade_Multiscreen - 35 and Downgrade_Multiscreen + 35 -- DTV churn no adjacent to product churn
         )
   group by xDowngrade_Flag, xBT_Sports_Viewing;
commit;


  -- ~~~~~ Broadband ~~~~~
delete from SPG_BTS_10_Summary
 where Product = 'Broadband';
commit;

insert into SPG_BTS_10_Summary
      (Product, Downgrade_Flag, BT_Sports_Viewing, Accounts_Unscaled)
  select
        'Broadband',
        case when Downgrade_Broadband is null then 'No' else 'Yes' end               as xDowngrade_Flag,
        case when BT_Sport_Watched__Broadband >= 15 * 60 then 'Yes' else 'No' end    as xBT_Sports_Viewing,
        count(Account_Number)
    from SPG_BTS_01_Universe
   where DTV_Active = 1                                                     -- Active DTV customer
     and Prod_Broadband = 'Yes'                                           -- Active product
     and (
          Downgrade_DTV is null                                             -- No DTV churn at all
          or
          Downgrade_DTV not between Downgrade_Broadband - 35 and Downgrade_Broadband + 35 -- DTV churn no adjacent to product churn
         )
   group by xDowngrade_Flag, xBT_Sports_Viewing;
commit;


select
      Product,
      Downgrade_Flag,
      BT_Sports_Viewing,
      Accounts_Unscaled
  from SPG_BTS_10_Summary;







/*
TEST QUERY
create variable @acc varchar(50);
set @acc = '';

  select
        'DTV',
        case when Downgrade_DTV is null then 'No' else 'Yes' end              as xDowngrade_Flag,
        case when BT_Sport_Watched__DTV >= 15 * 60 then 'Yes' else 'No' end   as xBT_Sports_Viewing
    from SPG_BTS_01_Universe
   where DTV_Active = 1                                                     -- Active DTV customer
     and account_number = @acc

union all

  select
        'Sports Premium',
        case when Downgrade_Sky_Sports is null then 'No' else 'Yes' end               as xDowngrade_Flag,
        case when BT_Sport_Watched__Sky_Sports >= 15 * 60 then 'Yes' else 'No' end    as xBT_Sports_Viewing
    from SPG_BTS_01_Universe
   where DTV_Active = 1                                                     -- Active DTV customer
     and Prod_Sky_Sports = 'Yes'                                            -- Active product
     and (
          Downgrade_DTV is null                                             -- No DTV churn at all
          or
          Downgrade_DTV not between Downgrade_Sky_Sports - 35 and Downgrade_Sky_Sports + 35 -- DTV churn no adjacent to product churn
         )
     and account_number = @acc

union all

  select
        'Movies Premium',
        case when Downgrade_Sky_Movies is null then 'No' else 'Yes' end               as xDowngrade_Flag,
        case when BT_Sport_Watched__Sky_Movies >= 15 * 60 then 'Yes' else 'No' end    as xBT_Sports_Viewing
    from SPG_BTS_01_Universe
   where DTV_Active = 1                                                     -- Active DTV customer
     and Prod_Sky_Movies = 'Yes'                                            -- Active product
     and (
          Downgrade_DTV is null                                             -- No DTV churn at all
          or
          Downgrade_DTV not between Downgrade_Sky_Movies - 35 and Downgrade_Sky_Movies + 35 -- DTV churn no adjacent to product churn
         )
     and account_number = @acc

union all

  select
        'HD',
        case when Downgrade_HD is null then 'No' else 'Yes' end               as xDowngrade_Flag,
        case when BT_Sport_Watched__HD >= 15 * 60 then 'Yes' else 'No' end    as xBT_Sports_Viewing
    from SPG_BTS_01_Universe
   where DTV_Active = 1                                                     -- Active DTV customer
     and Prod_HD = 'Yes'                                                    -- Active product
     and (
          Downgrade_DTV is null                                             -- No DTV churn at all
          or
          Downgrade_DTV not between Downgrade_HD - 35 and Downgrade_HD + 35 -- DTV churn no adjacent to product churn
         )
     and account_number = @acc

union all

  select
        'Multiscreen',
        case when Downgrade_Multiscreen is null then 'No' else 'Yes' end               as xDowngrade_Flag,
        case when BT_Sport_Watched__Multiscreen >= 15 * 60 then 'Yes' else 'No' end    as xBT_Sports_Viewing
    from SPG_BTS_01_Universe
   where DTV_Active = 1                                                     -- Active DTV customer
     and Prod_Multiscreen = 'Yes'                                           -- Active product
     and (
          Downgrade_DTV is null                                             -- No DTV churn at all
          or
          Downgrade_DTV not between Downgrade_Multiscreen - 35 and Downgrade_Multiscreen + 35 -- DTV churn no adjacent to product churn
         )
     and account_number = @acc

union all

  select
        'Broadband',
        case when Downgrade_Broadband is null then 'No' else 'Yes' end               as xDowngrade_Flag,
        case when BT_Sport_Watched__Broadband >= 15 * 60 then 'Yes' else 'No' end    as xBT_Sports_Viewing
    from SPG_BTS_01_Universe
   where DTV_Active = 1                                                     -- Active DTV customer
     and Prod_Broadband = 'Yes'                                           -- Active product
     and (
          Downgrade_DTV is null                                             -- No DTV churn at all
          or
          Downgrade_DTV not between Downgrade_Broadband - 35 and Downgrade_Broadband + 35 -- DTV churn no adjacent to product churn
         )
     and account_number = @acc

order by 1, 2, 3;
*/





