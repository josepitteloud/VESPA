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
# "vespa_analysts.BroadcasterReporting_C4_Viewing_Snapshots" must be processed
# for the required period
#
#################################################################################
# Change log
# ------------------------------------------------------------------------------
# 25/03/2013  SBE   v01 - initial version
#
###############################################################################*/



  -- #################################################################################
  -- ##### Create export views                                                   #####
  -- #################################################################################
drop view if exists BroadcasterReporting_C4_Report_Linear_Viewing;
create view BroadcasterReporting_C4_Report_Linear_Viewing as
  select
        Viewing_Date                as DATE_OF_VIEW,
        Traffic_Key                 as BROADCASTER_ASSET_ID,
        Account_Number_Obfuscated   as HOUSEHOLD_ID,
        Subscriber_id_Obfuscated    as STB_ID,
        Service_Key                 as CHANNEL_ID,
        sum(Viewing_Instances)      as TOTAL_NUMBER_VIEWING_FRAGMENTS,
        sum(Viewing_Duration) / sum(Viewing_Instances) as AVG_SECONDS_VIEWED_PER_VIEWING_FRAGMENT
    from vespa_analysts.BroadcasterReporting_C4_Viewing_Snapshots
   where Viewing_Date >= '2013-02-25'
     and Viewing_Date <= '2013-03-03'
   group by Viewing_Date, Traffic_Key, Service_Key, Account_Number_Obfuscated, Subscriber_id_Obfuscated;
commit;


drop view if exists BroadcasterReporting_C4_Report_Social_Class;
create view BroadcasterReporting_C4_Report_Social_Class as
  select
        Viewing_Date                as DATE_OF_VIEW,
        Traffic_Key                 as BROADCASTER_ASSET_ID,
        Service_Key                 as CHANNEL_ID,
        Social_Class                as SOCIAL_CLASS,
        sum(Viewing_Instances)      as TOTAL_NUMBER_VIEWING_FRAGMENTS,
        sum(Viewing_Duration) / sum(Viewing_Instances) as AVG_SECONDS_VIEWED_PER_VIEWING_FRAGMENT
    from vespa_analysts.BroadcasterReporting_C4_Viewing_Snapshots
   where Viewing_Date >= '2013-02-25'
     and Viewing_Date <= '2013-03-03'
   group by Viewing_Date, Traffic_Key, Service_Key, Social_Class;
commit;


drop view if exists BroadcasterReporting_C4_Report_Postcode_Area;
create view BroadcasterReporting_C4_Report_Postcode_Area as
  select
        Viewing_Date                as DATE_OF_VIEW,
        Traffic_Key                 as BROADCASTER_ASSET_ID,
        Service_Key                 as CHANNEL_ID,
        Postcode_Area               as POSTAL_AREA,
        sum(Viewing_Instances)      as TOTAL_NUMBER_VIEWING_FRAGMENTS,
        sum(Viewing_Duration) / sum(Viewing_Instances) as AVG_SECONDS_VIEWED_PER_VIEWING_FRAGMENT
    from vespa_analysts.BroadcasterReporting_C4_Viewing_Snapshots
   where Viewing_Date >= '2013-02-25'
     and Viewing_Date <= '2013-03-03'
   group by Viewing_Date, Traffic_Key, Service_Key, Postcode_Area;
commit;



  -- #################################################################################
  -- ##### Create exports                                                        #####
  -- #################################################################################
  -- Account based file
select list("name", '\x09' order by column_number asc)
  from sa_describe_query('select * from BroadcasterReporting_C4_Report_Linear_Viewing');
output to "C:\_Playpen_\2013-02-27 Broadcaster reporting\Reports\2013-03-25 C4 (Linear Viewing).txt" format ascii quote '' delimited by '\x09' escapes off;

select top 100 *
  from BroadcasterReporting_C4_Report_Linear_Viewing;
output to "C:\_Playpen_\2013-02-27 Broadcaster reporting\Reports\2013-03-25 C4 (Linear Viewing).txt" format ascii quote '' append delimited by '\x09';


  -- By social class
select list("name", '\x09' order by column_number asc)
  from sa_describe_query('select * from BroadcasterReporting_C4_Report_Social_Class');
output to "C:\_Playpen_\2013-02-27 Broadcaster reporting\Reports\2013-03-25 C4 (Social Class).txt" format ascii quote '' delimited by '\x09' escapes off;

select top 100 *
  from BroadcasterReporting_C4_Report_Social_Class;
output to "C:\_Playpen_\2013-02-27 Broadcaster reporting\Reports\2013-03-25 C4 (Social Class).txt" format ascii quote '' append delimited by '\x09';


  -- By Postcode
select list("name", '\x09' order by column_number asc)
  from sa_describe_query('select * from BroadcasterReporting_C4_Report_Postcode_Area');
output to "C:\_Playpen_\2013-02-27 Broadcaster reporting\Reports\2013-03-25 C4 (Postcode Area).txt" format ascii quote '' delimited by '\x09' escapes off;

select top 100 *
  from BroadcasterReporting_C4_Report_Postcode_Area;
output to "C:\_Playpen_\2013-02-27 Broadcaster reporting\Reports\2013-03-25 C4 (Postcode Area).txt" format ascii quote '' append delimited by '\x09';



  -- #################################################################################
  -- #################################################################################




















