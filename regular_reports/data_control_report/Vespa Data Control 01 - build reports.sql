/******************************************************************************
**
** Project Vespa: Data Control Report
**                  - Weekly Refresh Script
**
** The data control report is sort of a regression test that Analytics owns;
** checking that all the tables we expect are in the right place, that their
** definitions are what we expect, and that the categorisations still match
** what our queries use. It's also since turning into a business-level unit
** test script for the single box view thing that's been built.
**
** There aren't all that many outputs for this guy, mainly it's a bunch of
** dull logger tracking stuff which if we're lucky will never flag anything.
**
** Code sections:
**      Part A: A01 - Initialisation, Logger
**
**      Part B:       Table existence and column naming
**              B01 - Daily table existence ( & naming conventions)
**              B02 - Daily table expected columns
**              B03 - Subscriber status expeced columns
**              B04 - EPG DIM expected columns
**              B05 - Important channel lookup colmns
**              B06 - Log snapshot expected columns
**              B07 - Permissions - can we select from the important tables?
**
**      Part C:       Relational checks
**              C01 - Reporting boxes appear in subscriber status
**              C02 - Reporting boxes appear in stb log snapshot
**              C03 - Snapshot boxes appear in subscriber status
**              C04 - Programme keys appear in EPG
**
**      Part D:       Events classifications and flags
**              D01 - Panel_id
**              D02 - Event_Type
**              D03 - x_type_of_viewing_event
**              D04 - x_si_service_type
**              D05 - video_playing_flag
**              D06 - play_back_speed
**
**      Part E:       EPG classifications and flags
**              E01 - Um... what flags do we use on the programme table?
**
**
**      Part F:       Operational control totals
**              F01 - Boxes reporting
**              F02 - Total live viewing
**              F03 - Total playback viewing
**
**      Part G:       SBV Unit Tests
**              G01 - Overlap between Vespa and Sky View panels (they shouldn't)
**              G02 - Boxes without records in the customer database
**              G03 - Problems with marking the P/S flags
**              G04 - Active boxes without enablement dates
**              G05 - Open vs Closed Loop enablement discrepancies
**              G06 - Apparently active boxes without DTV subscriptions of non-standard acounts
**              G07 - Validation of the derived single panel flag
**              G08 - Checks on 3rd party data linking
**              
** Thing still to do:
**  4. Every other line of testing (maybe leave till after phase two drops?)
**
** Things done:
**  1. The SBV tests
**  2. And then the stored-procedure-ification
**  3. And then add it to the scheduled tasks list
**
** So we're not pushing these tests into the logger, but we will track progress
** of this guy with the logger. We're marking severity 0 - 5, as in:
**      0: slight inconvenience
**      1: erm... what?
**      2: SNAFU
**      3: Ok, this might actually cause us pain
**      4: threatening terminal impacts upon the project
**      5: impending project failure.
** All the weightings are a bit arbitrary at the moment though, we'll have to
** get them reviewed. There will be a data audit specialist arriving at some
** point? We'll just hand it over to them.
**
******************************************************************************/

if object_id('Vespa_refresh_data_control_report') is not null
   drop procedure Vespa_refresh_data_control_report;

go

create procedure Vespa_refresh_data_control_report
as
begin

/****************** A01: SETTING UP, AND THE LOGGER ******************/


delete from Vespa_DataCont_Flag_log
where noted = today()

DECLARE @QA_catcher             bigint
DECLARE @population             bigint
DECLARE @dynamic_SQL_hackery    varchar(2000) -- we'll need this for queries on daily tables
declare @DC_log_ID              integer
declare @run_Identifier         varchar(60)

-- Checking if the report is being run by an analyst, or by the scheduler:
if lower(user) = 'kmandal'
    set @run_Identifier = 'VespaDataControl'
else
    set @run_Identifier = 'DataCont test ' || upper(right(user,1)) || upper(left(user,2))

EXECUTE citeam.logger_create_run @run_Identifier, convert(varchar(10),today(),123) || ' data control build', @DC_log_ID output


/****************** B01: EXISTNECE OF DAILY TABLES ******************/



/****************** B02: ESSENTIAL COLUMNS ON DAILY TABLES ******************/



/****************** B03: ESSENTIAL COLUMNS ON SUBSCRIBER STATUS ******************/



/****************** B04: ESSENTIAL COLUMNS ON THE EPG LOOKUP ******************/


/****************** B05: ESSENTIAL COLUMNS ON THE CHANNEL LOOKUP ******************/


/****************** B06: ESSENTIAL COLUMNS ON THE LOG SNAPSHOT TABLE ******************/


/****************** C01: REPORTING BOXES ARE IN THE SUBSCRIBER STATUS ******************/



/****************** C02: REPORTING BOXES ARE IN STB LOG SNAPSHOT ******************/



/****************** C03: STB LOG SNAPSHOT TABLES ARE IN THE SUBSCRIBER STATUS ******************/



/****************** C04: PROGRAMME KEYS ARE IN THE EPG ******************/


/****************** C05: CHANNEL KEYS ARE IN THE CHANNEL LOOKUP ******************/


/****************** D01: PANEL ID FLAGS ARE EXPECTED ******************/



/****************** D02: EVENT TYPE FLAGS ARE EXPECTED ******************/



/****************** D03: VIEWING EVENT TYPE FLAGS ARE EXPECTED ******************/



/****************** D04: SERVICE TYPE FLAGS ARE EXPECTED ******************/


/****************** D05: VIDEO PLAYING FLAGS ARE EXPECTED ******************/


/****************** D06: PLAYBACK SPEED FLAGS ARE EXPECTED ******************/

/****************** F01: TOTAL BOXES REPORTING ******************/



/****************** F02: TOTAL LIVE VIEWING ******************/



/****************** F03: TOTAL PLAYBACK VIEWING ******************/


/****************** F03: TOTAL HD VIEWING ******************/


/****************** G01: SKY VIEW / VESPA PANEL OVERLAP ******************/

-- OK, so let's set the population to everything in the SBV:
select @population = count(1) from vespa_single_box_view

-- These panels shuoldn't have any common boxes....
set @QA_catcher = -1

select @QA_catcher = count(1)
from vespa_single_box_view 
where Closed_loop_enabled = 1 and Is_Sky_View_Selected = 1

if @QA_catcher is null or @QA_catcher <> 0
    insert into Vespa_DataCont_Flag_log (
        issue_table
        ,issue_class
        ,issue_description
        ,issue_count
        ,issue_severity
    )
    select
        'Single Box View'
        ,'Panel overlap'
        ,'Boxes active on both Sky View and Vespa panels'
        ,coalesce(@QA_catcher, -1)
        ,2
        
commit

-- whether the two panels are trying to claim the same box
set @QA_catcher = -1

select @QA_catcher = count(1)
from vespa_single_box_view 
--where Open_loop_enabled = 1 and Is_Sky_View_candidate = 1 and (Closed_loop_enabled = 0 or Is_Sky_View_Selected = 0)
where Is_Sky_View_candidate = 1 and (Closed_loop_enabled = 0 or Is_Sky_View_Selected = 0)

if @QA_catcher is null or @QA_catcher <> 0
    insert into Vespa_DataCont_Flag_log (
        issue_table
        ,issue_class
        ,issue_description
        ,issue_count
        ,issue_severity
    )
    select
        'Single Box View'
        ,'Panel overlap'
        ,'Sky View and Vespa panel trying to enable the same box'
        ,coalesce(@QA_catcher, -1)
        ,1
        
commit

-- Also going to check that Vespa Live panel doesn't overlap with the Alternate Day panels 6 or 7:

set @QA_catcher = -1

select @QA_catcher = count(1)
from vespa_single_box_view 
where Closed_loop_enabled = 1 and alternate_panel_6 = 1

if @QA_catcher is null or @QA_catcher <> 0
    insert into Vespa_DataCont_Flag_log (
        issue_table
        ,issue_class
        ,issue_description
        ,issue_count
        ,issue_severity
    )
    select
        'single_box_view'
        ,'Panel overlap'
        ,'Vespa Live panel not disjoint with Alternate Day panel 6'
        ,coalesce(@QA_catcher, -1)
        ,2
        
commit

set @QA_catcher = -1

select @QA_catcher = count(1)
from vespa_single_box_view 
where Closed_loop_enabled = 1 and alternate_panel_7 = 1

if @QA_catcher is null or @QA_catcher <> 0
    insert into Vespa_DataCont_Flag_log (
        issue_table
        ,issue_class
        ,issue_description
        ,issue_count
        ,issue_severity
    )
    select
        'single_box_view'
        ,'Panel overlap'
        ,'Vespa Live panel not disjoint with Alternate Day panel 7'
        ,coalesce(@QA_catcher, -1)
        ,2
        
commit

-- The Alternate Day panels shouldn't overlap with the Sky View panel either:

set @QA_catcher = -1

select @QA_catcher = count(1)
from vespa_single_box_view 
where Is_Sky_View_Selected = 1 and alternate_panel_6 = 1

if @QA_catcher is null or @QA_catcher <> 0
    insert into Vespa_DataCont_Flag_log (
        issue_table
        ,issue_class
        ,issue_description
        ,issue_count
        ,issue_severity
    )
    select
        'single_box_view'
        ,'Panel overlap'
        ,'Sky View shares boxes with Vespa Alternate Day panel 6'
        ,coalesce(@QA_catcher, -1)
        ,2
        
commit

set @QA_catcher = -1

select @QA_catcher = count(1)
from vespa_single_box_view 
where Is_Sky_View_Selected = 1 and alternate_panel_7 = 1

if @QA_catcher is null or @QA_catcher <> 0
    insert into Vespa_DataCont_Flag_log (
        issue_table
        ,issue_class
        ,issue_description
        ,issue_count
        ,issue_severity
    )
    select
        'single_box_view'
        ,'Panel overlap'
        ,'Sky View shares boxes with Vespa Alternate Day panel 7'
        ,coalesce(@QA_catcher, -1)
        ,2
        
commit

-- Alternate panels 6 and 7 should also be disjoint:

set @QA_catcher = -1

select @QA_catcher = count(1)
from vespa_single_box_view 
where alternate_panel_6 = 1 and alternate_panel_7 = 1

if @QA_catcher is null or @QA_catcher <> 0
    insert into Vespa_DataCont_Flag_log (
        issue_table
        ,issue_class
        ,issue_description
        ,issue_count
        ,issue_severity
    )
    select
        'single_box_view'
        ,'Panel overlap'
        ,'Vespa Alternate Day panels 6 & 7 have box overlap'
        ,coalesce(@QA_catcher, -1)
        ,2
        
commit

/****************** G02: BOXES NOT RECORDED IN CUSTOMER DATABASE ******************/

set @QA_catcher = -1
-- Boxes that didn't get a service instance ID (means they're not in cust_service_instance)
select @QA_catcher = count(1)
from vespa_single_box_view 
where service_instance_id is null

if @QA_catcher is null or @QA_catcher <> 0
    insert into Vespa_DataCont_Flag_log (
        issue_table
        ,issue_class
        ,issue_description
        ,issue_count
        ,issue_severity
    )
    select
        'Single Box View'
        ,'Customer DB inconsistency'
        ,'Service Instance ID not attached to boxes'
        ,coalesce(@QA_catcher, -1)
        ,0
        
commit

set @QA_catcher = -1
-- Accounts that aren't in SAV:
select @QA_catcher = count(distinct b.account_number)
from vespa_single_box_view as b
left join sk_prod.cust_single_account_view as a
on b.account_number = a.account_number
where a.account_number is null

if @QA_catcher is null or @QA_catcher <> 0
    insert into Vespa_DataCont_Flag_log (
        issue_table
        ,issue_class
        ,issue_description
        ,issue_count
        ,issue_severity
    )
    select
        'Single Box View'
        ,'Customer DB inconsistency'
        ,'Account not recognised in SAV'
        ,coalesce(@QA_catcher, -1)
        ,2
        
commit

set @QA_catcher = -1
-- "Enabled" accounts that don't have DTV subscriptions
select @QA_catcher = count(1)
from vespa_single_box_view 
where cust_active_dtv is null

if @QA_catcher is null or @QA_catcher <> 0
    insert into Vespa_DataCont_Flag_log (
        issue_table
        ,issue_class
        ,issue_description
        ,issue_count
        ,issue_severity
    )
    select
        'Single Box View'
        ,'Customer DB inconsistency'
        ,'Account has no active DTV subscription'
        ,coalesce(@QA_catcher, -1)
        ,1
        
commit

/****************** G03: HOLES IN THE P/S FLAGS ******************/

set @QA_catcher = -1
-- boxes that we don't have a flag for
select @QA_catcher = count(1)
from vespa_single_box_view 
where PS_flag = 'U'

if @QA_catcher is null or @QA_catcher <> 0
    insert into Vespa_DataCont_Flag_log (
        issue_table
        ,issue_class
        ,issue_description
        ,issue_count
        ,issue_severity
    )
    select
        'Single Box View'
        ,'P/S flags'
        ,'Unknown box definition'
        ,coalesce(@QA_catcher, -1)
        ,0
        
commit

set @QA_catcher = -1
-- boxes that have conflicting flags somewhere
select @QA_catcher = count(1)
from vespa_single_box_view 
where PS_flag is null or PS_flag not in ('P', 'S', 'U')

if @QA_catcher is null or @QA_catcher <> 0
    insert into Vespa_DataCont_Flag_log (
        issue_table
        ,issue_class
        ,issue_description
        ,issue_count
        ,issue_severity
    )
    select
        'Single Box View'
        ,'P/S flags'
        ,'Conflicting box definitions'
        ,coalesce(@QA_catcher, -1)
        ,1
        
commit

/****************** G04: ACTIVE BOXES WITHOUT AN ENABLEMENT DATE ******************/

-- Checked these in SAV build, we'll repeat it here:

set @QA_catcher = -1
-- Are there boxes requested for enablement that don't have selection dates?
select @QA_catcher = count(1) from vespa_single_box_view
--where  Open_loop_enabled = 1 and Selection_date is null
where Selection_date is null

if @QA_catcher is null or @QA_catcher <> 0
    insert into Vespa_DataCont_Flag_log (
        issue_table
        ,issue_class
        ,issue_description
        ,issue_count
        ,issue_severity
    )
    select
        'Single Box View'
        ,'Date completeness'
        ,'Open loop boxes without selection date'
        ,coalesce(@QA_catcher, -1)
        ,1

commit

set @QA_catcher = -1
-- Are there enabled boxes that don't have enablement dates?
select @QA_catcher = count(1) from vespa_single_box_view
where  Closed_loop_enabled = 1 and enablement_date is null

if @QA_catcher is null or @QA_catcher <> 0
    insert into Vespa_DataCont_Flag_log (
        issue_table
        ,issue_class
        ,issue_description
        ,issue_count
        ,issue_severity
    )
    select
        'Single Box View'
        ,'Date completeness'
        ,'Closed loop boxes without enablement dates'
        ,coalesce(@QA_catcher, -1)
        ,1
    
commit


/****************** G05: OPEN VS CLOSED LOOP ENABLEMENT DISCREPANCY ******************/

set @QA_catcher = -1
-- How are we doing with the panel managemnent administration bit?
select @QA_catcher = count(1)
from vespa_single_box_view 
-- where Open_loop_enabled = 1 and Closed_loop_enabled = 0
where Closed_loop_enabled = 0

if @QA_catcher is null or @QA_catcher <> 0
    insert into Vespa_DataCont_Flag_log (
        issue_table
        ,issue_class
        ,issue_description
        ,issue_count
        ,issue_severity
    )
    select
        'Single Box View'
        ,'Canonical enablement'
        ,'Boxes which are open loop but not closed loop enabled'
        ,coalesce(@QA_catcher, -1)
        ,0
        
commit

/****************** G06: INVALID PANEL BOXES - NO DTV SUBSCRIPTIONS OR IRREGULAR ACCOUNTS ******************/

set @QA_catcher = -1
-- So we're already excluding these in the table, but we'll check if these are coming through our feeds or not
select @QA_catcher = count(1)
from vespa_single_box_view 
where Panel_ID_4_cells_confirm & uk_standard_account
    & case when Status_Vespa in ('Enabled', 'DisablePending', 'EnablePending', 'Trumped') then 1 else 0 end
    & ~CUST_ACTIVE_DTV = 1
-- Bitwise operators!

if @QA_catcher is null or @QA_catcher <> 0
    insert into Vespa_DataCont_Flag_log (
        issue_table
        ,issue_class
        ,issue_description
        ,issue_count
        ,issue_severity
    )
    select
        'Single Box View'
        ,'Invalid enablement'
        ,'Boxes which would be Vespa closed loop enabled except no DTV subscription'
        ,coalesce(@QA_catcher, -1)
        ,1
        
commit

set @QA_catcher = -1
-- And we also want to know about people who are on the excluded accounts list but would
-- otherwise be on the Panel listing
select @QA_catcher = count(1)
from vespa_single_box_view 
where Panel_ID_4_cells_confirm & CUST_ACTIVE_DTV & ~uk_standard_account = 1
-- More bitwise operators!

if @QA_catcher is null or @QA_catcher <> 0
    insert into Vespa_DataCont_Flag_log (
        issue_table
        ,issue_class
        ,issue_description
        ,issue_count
        ,issue_severity
    )
    select
        'Single Box View'
        ,'Invalid enablement'
        ,'Non-standard UK account would be in Vespa Panel (ROI? Staff?)'
        ,coalesce(@QA_catcher, -1)
        ,2
        
commit


set @QA_catcher = -1
-- But for Sky View, we're actually not checking in the feeds... so this guy is perhaps more of a concern?
select @QA_catcher = count(1)
from vespa_single_box_view 
where Is_Sky_View_Selected & ~CUST_ACTIVE_DTV = 1
-- Bitwise operators!

if @QA_catcher is null or @QA_catcher <> 0
    insert into Vespa_DataCont_Flag_log (
        issue_table
        ,issue_class
        ,issue_description
        ,issue_count
        ,issue_severity
    )
    select
        'Single Box View'
        ,'Invalid enablement'
        ,'Boxes which would be Sky View Selected except no DTV subscription'
        ,coalesce(@QA_catcher, -1)
        ,2
        
commit


set @QA_catcher = -1
-- Sky View panel is more closely monitored, so we shouldn't really get any invalid accounts here?
select @QA_catcher = count(1)
from vespa_single_box_view 
where Is_Sky_View_Selected & ~uk_standard_account = 1
-- Again, the bitwise operators

if @QA_catcher is null or @QA_catcher <> 0
    insert into Vespa_DataCont_Flag_log (
        issue_table
        ,issue_class
        ,issue_description
        ,issue_count
        ,issue_severity
    )
    select
        'Single Box View'
        ,'Invalid enablement'
        ,'Non-standard UK accounts selected for Sky View panel (ROI? Staff?)'
        ,coalesce(@QA_catcher, -1)
        ,2
        
commit


/****************** G07: CHECKING THE SINGLE PANEL FLAG WE BUILT ******************/

set @QA_catcher = -1
-- First of, there should only be a few known established values:
select @QA_catcher = count(1)
from vespa_single_box_view 
where panel is not null and panel not in ('VESPA', 'ALT6', 'ALT7', 'SKYVIEW', 'CLASH!')
-- 'CLASH!' is permitted in this test because it's one of the known acceptable values the process might apply...

if @QA_catcher is null or @QA_catcher <> 0
    insert into Vespa_DataCont_Flag_log (
        issue_table
        ,issue_class
        ,issue_description
        ,issue_count
        ,issue_severity
    )
    select
        'Single Box View'
        ,'Panel flag'
        ,'Invalid panel flag applied'
        ,coalesce(@QA_catcher, -1)
        ,2
        
commit

set @QA_catcher = -1
-- But 'CLASH!' isn't actually a valid panel code, so we'll flag that separately
select @QA_catcher = count(1)
from vespa_single_box_view 
where panel = 'CLASH!'

if @QA_catcher is null or @QA_catcher <> 0
    insert into Vespa_DataCont_Flag_log (
        issue_table
        ,issue_class
        ,issue_description
        ,issue_count
        ,issue_severity
    )
    select
        'Single Box View'
        ,'Panel flag'
        ,'SBV could not decide panel membership'
        ,coalesce(@QA_catcher, -1)
        ,0
        
commit

set @QA_catcher = -1
-- We also don't want any cases where different boxes of the same account are active on
-- different panels:
select @QA_catcher = count(1) from (
    select account_number
    from vespa_single_box_view
    group by account_number
    having count(distinct panel) > 1
) as t

if @QA_catcher is null or @QA_catcher <> 0
    insert into Vespa_DataCont_Flag_log (
        issue_table
        ,issue_class
        ,issue_description
        ,issue_count
        ,issue_severity
    )
    select
        'Single Box View'
        ,'Panel flag'
        ,'Accounts with active boxes on multiple panels'
        ,coalesce(@QA_catcher, -1)
        ,1
        
commit

/****************** G08: CHECKING LINNKS TO 3RD PARTY DATA ******************/

-- First off, are there holes in the 3rd party links we're trying to build?
set @QA_catcher = -1

select @QA_catcher = count(1)
from vespa_single_box_view
where cb_key_individual is null or cb_key_individual = 0

if @QA_catcher is null or @QA_catcher <> 0
    insert into Vespa_DataCont_Flag_log (
        issue_table
        ,issue_class
        ,issue_description
        ,issue_count
        ,issue_severity
    )
    select
        'Single Box View'
        ,'3rd party link'
        ,'Boxes without valid cb_key_individual link.'
        ,coalesce(@QA_catcher, -1)
        ,0
        
commit

-- So specifically for Experian Consumerview, are there things which aren't
-- linked into consumerview?
set @QA_catcher = -1

select @QA_catcher = count(1)
from vespa_single_box_view
where consumerview_cb_row_id is null or consumerview_cb_row_id = 0

if @QA_catcher is null or @QA_catcher <> 0
    insert into Vespa_DataCont_Flag_log (
        issue_table
        ,issue_class
        ,issue_description
        ,issue_count
        ,issue_severity
    )
    select
        'Single Box View'
        ,'3rd party link'
        ,'Boxes without valid consumerview_cb_row_id link.'
        ,coalesce(@QA_catcher, -1)
        ,0
        
commit

-- Ok, so then, are there duplicates where we are linking different accounts into
-- the same 3rd party data?
set @QA_catcher = -1

select @QA_catcher = count(1) from
(
    select cb_key_individual
    from vespa_single_box_view
    where cb_key_individual is not null
    group by cb_key_individual
    having count(distinct account_number) > 1
) as t

if @QA_catcher is null or @QA_catcher <> 0
    insert into Vespa_DataCont_Flag_log (
        issue_table
        ,issue_class
        ,issue_description
        ,issue_count
        ,issue_severity
    )
    select
        'Single Box View'
        ,'3rd Party Link'
        ,'Same cb_key_individual linked to multiple accounts'
        ,coalesce(@QA_catcher, -1)
        ,2
        
commit


EXECUTE citeam.logger_add_event @DC_log_ID, 3, 'G: Complete! (SBV unit tests)'


EXECUTE citeam.logger_add_event @DC_log_ID, 3, 'Data control report complete!'

end; -- of procedure "Vespa_refresh_data_control_report"

-- Don't care who runs it, only selected people can get to the results table...
grant execute on Vespa_refresh_data_control_report to public;