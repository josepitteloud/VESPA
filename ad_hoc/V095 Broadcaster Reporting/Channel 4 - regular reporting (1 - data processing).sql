/*###############################################################################
# Created on:   25/03/2013
# Created by:   Sebastian Bednaszynski (SBE)
# Description:  Broadcaster reporting - Channel 4
#               This is a temporary workaround process until all required viewing
#               data is made available in Composite
#
# To do:
#               - N/A
#
#################################################################################
# Dependencies
# ------------------------------------------------------------------------------
# This process requires an input table holding subscriber information for
# Virtual Panel 1 (Channel 4) with the following fields:
#   Account_Id                      - Account Number
#   Scms_Subscriber_Id              - Subcriber Id
#   Customer_Account_Id_C4_Export   - Hashed (obfuscated) version of Account Number
#   Scms_Subscriber_Id_C4_Export    - Hashed (obfuscated) version of Account Number
#   Postcode_Area                   - Customer postcode area
#   Social_Class                    - Customer social class
#
# Currently, this table is created by Gavin Meggs, and can be found in:
#   meggsg.customer_obfuscation_extract_20130323
#
# Table must be unique on Subscriber Id
#
#################################################################################
# Change log
# ------------------------------------------------------------------------------
# 25/03/2013  SBE   v01 - initial version
#
###############################################################################*/

/*
  -- One-off table creation script in VA to hold all incrementally loaded viewing data
if object_id('BroadcasterReporting_C4_Viewing_Snapshots') is not null then drop table BroadcasterReporting_C4_Viewing_Snapshots end if;
create table BroadcasterReporting_C4_Viewing_Snapshots (
   Row_Id                     bigint        identity,
   Run_Date                   date          default today(),
   Account_Number             varchar(50)   default null,
   Subscriber_id              bigint        default null,
   Account_Number_Obfuscated  bigint        default null,
   Subscriber_id_Obfuscated   bigint        default null,

   Postcode_Area              varchar(4)    default '',
   Social_Class               varchar(12)   default 'UNCLASSIFIED',

   Viewing_Date               date          default null,
   Traffic_Key                varchar(40)   default null,
   Service_Key                bigint        default null,

   Viewing_Instances          unsigned int  default null,
   Viewing_Duration           unsigned int  default null,

   Created_By                 varchar(30)   default user,
   Created_On                 timestamp     default timestamp
);

create date index idx1 on BroadcasterReporting_C4_Viewing_Snapshots(Run_Date);
create hg index idx2 on BroadcasterReporting_C4_Viewing_Snapshots(Account_Number);
create hg index idx3 on BroadcasterReporting_C4_Viewing_Snapshots(Subscriber_id);
create hg index idx4 on BroadcasterReporting_C4_Viewing_Snapshots(Account_Number_Obfuscated);
create hg index idx5 on BroadcasterReporting_C4_Viewing_Snapshots(Subscriber_id_Obfuscated);
create lf index idx6 on BroadcasterReporting_C4_Viewing_Snapshots(Postcode_Area);
create lf index idx7 on BroadcasterReporting_C4_Viewing_Snapshots(Social_Class);
create date index idx8 on BroadcasterReporting_C4_Viewing_Snapshots(Viewing_Date);
create hg index idx9 on BroadcasterReporting_C4_Viewing_Snapshots(Traffic_Key);
create lf index idx10 on BroadcasterReporting_C4_Viewing_Snapshots(Service_Key);

grant select on BroadcasterReporting_C4_Viewing_Snapshots to vespa_group_low_security;
grant insert on BroadcasterReporting_C4_Viewing_Snapshots to bednaszs;

*/



  -- #################################################################################
  -- ##### Get structures in place                                               #####
  -- #################################################################################
  -- Temp table holding all viewing records
if object_id('BroadcasterReporting_C4_tmp_Viewing_Details') is not null then drop table BroadcasterReporting_C4_tmp_Viewing_Details end if;
create table BroadcasterReporting_C4_tmp_Viewing_Details (
    Row_Id                     bigint        identity,
    Instance_Id                bigint        default null,
    Account_Number             varchar(50)   default null,
    Subscriber_id              bigint        default null,
    Account_Number_Obfuscated  bigint        default null,
    Subscriber_id_Obfuscated   bigint        default null,

    Postcode_Area              varchar(4)    default '',
    Social_Class               varchar(12)   default 'UNCLASSIFIED',

    Viewing_Date               date          default null,
    Traffic_Key                varchar(40)   default null,
    Service_Key                bigint        default null,
    Channel_Name               varchar(25)   default null,

    Viewing_Instances          unsigned int  default null,
    Viewing_Duration           unsigned int  default null
);

create hg index idx1 on BroadcasterReporting_C4_tmp_Viewing_Details(Instance_Id);
create hg index idx2 on BroadcasterReporting_C4_tmp_Viewing_Details(Account_Number);
create hg index idx3 on BroadcasterReporting_C4_tmp_Viewing_Details(Subscriber_id);
create hg index idx4 on BroadcasterReporting_C4_tmp_Viewing_Details(Account_Number_Obfuscated);
create hg index idx5 on BroadcasterReporting_C4_tmp_Viewing_Details(Subscriber_id_Obfuscated);
create date index idx6 on BroadcasterReporting_C4_tmp_Viewing_Details(Viewing_Date);
create lf index idx7 on BroadcasterReporting_C4_tmp_Viewing_Details(Channel_Name);


  -- A view over the required information from composite
  -- MUST BE UNIQUE ON SUBSCRIBER ID
drop view if exists BroadcasterReporting_C4_tmp_Composite_Data;
create view BroadcasterReporting_C4_tmp_Composite_Data as
  select
        ACCOUNT_ID,
        cast(SCMS_SUBSCRIBER_ID as bigint) as SCMS_SUBSCRIBER_ID,
        CUSTOMER_ACCOUNT_ID_C4_EXPORT,
        SCMS_SUBSCRIBER_ID_C4_EXPORT,
        POSTCODE_AREA,
        SOCIAL_CLASS,
        CARD_ID
    from meggsg.customer_obfuscation_extract_20130323;



  -- #################################################################################
  -- ##### Pull raw viewing for the period of interest                           #####
  -- #################################################################################
truncate table BroadcasterReporting_C4_tmp_Viewing_Details;

begin

    declare @varBuildId   unsigned int
    declare @varStartDate date
    declare @varEndDate   date
    declare @varSql       varchar(15000)

    set @varStartDate = '2013-02-25'
    set @varEndDate   = '2013-03-03'

    execute logger_create_run 'BrdcstRep C4', 'Weekly run', @varBuildId output

    execute logger_add_event @varBuildId, 3, 'Pulling data from AUG tables'

    while @varStartDate <= @varEndDate
        begin

            set @varSql = '
                            insert into BroadcasterReporting_C4_tmp_Viewing_Details
                                        (Instance_Id, Account_Number, Subscriber_id, Account_Number_Obfuscated,
                                         Subscriber_id_Obfuscated, Postcode_Area, Social_Class, Viewing_Date,
                                         Viewing_Instances, Viewing_Duration)
                              select
                                    aug.Cb_Row_Id,
                                    aug.Account_Number,
                                    aug.Subscriber_Id,
                                    comp.Customer_Account_Id_C4_Export,
                                    comp.Scms_Subscriber_Id_C4_Export,
                                    comp.Postcode_Area,
                                    comp.Social_Class,
                                    date(aug.Viewing_Starts),
                                    1,                                          -- Each record in AUG tables represents a single instance
                                    aug.Viewing_Duration
                                from vespa_analysts.Vespa_Daily_Augs_##^1^## aug,
                                     BroadcasterReporting_C4_tmp_Composite_Data comp
                               where aug.Subscriber_Id = comp.Scms_Subscriber_id
                                 and aug.Viewing_Duration >= 180

                            commit
                          '

            execute(replace(@varSql,'##^1^##', dateformat(@varStartDate, 'yyyymmdd')))

            execute logger_add_event @varBuildId, 3, ' -> Date processed: ' || convert(varchar(10), @varStartDate, 123)

            set @varStartDate = @varStartDate + 1
        end


      -- Get Traffic Key, Service Key & Channel Name
    update BroadcasterReporting_C4_tmp_Viewing_Details base
       set base.Traffic_Key       = det.Traffic_Key,
           base.Service_Key       = det.Service_Key,
           base.Channel_Name      = det.Channel_Name
      from sk_prod.vespa_dp_prog_viewed_current det
     where base.Instance_Id = det.Pk_Viewing_Prog_Instance_Fact
    commit

    execute logger_add_event @varBuildId, 3, 'Missing information appended'

    execute logger_get_latest_job_events 'BrdcstRep C4', 4

end;



  -- #################################################################################
  -- ##### Populate final snapshot                                               #####
  -- #################################################################################
  -- Get list of dates already in the table so the same dates are not loaded numerous
  -- times - to override just truncate and do not populate this table
if object_id('BroadcasterReporting_C4_tmp_Existing_Dates') is not null then drop table BroadcasterReporting_C4_tmp_Existing_Dates end if;
create table BroadcasterReporting_C4_tmp_Existing_Dates (
    Processed_Viewing_Date     date          default null,
    Records_Volume             unsigned int  default null
);

create date index idx1 on BroadcasterReporting_C4_tmp_Existing_Dates(Processed_Viewing_Date);

truncate table BroadcasterReporting_C4_tmp_Existing_Dates;

insert into BroadcasterReporting_C4_tmp_Existing_Dates
            (Processed_Viewing_Date, Records_Volume)
  select
        Viewing_Date,
        count(*)
    from vespa_analysts.BroadcasterReporting_C4_Viewing_Snapshots
   group by Viewing_Date;
commit;


  -- Load new data
insert into vespa_analysts.BroadcasterReporting_C4_Viewing_Snapshots
            (Run_Date, Account_Number, Subscriber_id, Account_Number_Obfuscated,
             Subscriber_id_Obfuscated, Postcode_Area, Social_Class, Viewing_Date,
             Traffic_Key, Service_Key, Viewing_Instances, Viewing_Duration)
  select
        today(),
        Account_Number,
        Subscriber_id,
        Account_Number_Obfuscated,
        Subscriber_id_Obfuscated,
        case
          when (Postcode_Area is null) then ''
            else Postcode_Area
        end as x_Postcode_Area,
        case
          when (Social_Class is null) or (Social_Class = '') then 'UNCLASSIFIED'
            else Social_Class
        end as x_Social_Class,
        Viewing_Date,
        Traffic_Key,
        Service_Key,
        sum(Viewing_Instances),
        sum(Viewing_Duration)
   from BroadcasterReporting_C4_tmp_Viewing_Details a left join BroadcasterReporting_C4_tmp_Existing_Dates b
        on a.Viewing_Date = b.Processed_Viewing_Date
  where lower(trim(channel_name)) in ('4seven', 'channel 4', 'channel 4 +1', 'channel 4 hd', 'e4',
                                      'e4 hd', 'e4+1', 'film4', 'film4 +1', 'more4', 'more4 +1')
    and b.Processed_Viewing_Date is null                                                                -- Only process non-existing dates
  group by Account_Number, Subscriber_id, Account_Number_Obfuscated, Subscriber_id_Obfuscated,
           x_Postcode_Area, x_Social_Class, Viewing_Date, Traffic_Key, Service_Key;
commit;



  -- #################################################################################
  -- #################################################################################




















