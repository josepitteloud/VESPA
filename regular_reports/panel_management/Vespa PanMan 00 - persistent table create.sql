/******************************************************************************
**
** Project Vespa: Panel Management Report
**                  - Persistent Table Creation
**
** Permanent tables for the Panel Management report, looking at the structure
** and segment distribution of the Vespa Panel with respect to stability and
** accuracy of scaling. Refer also to:
**
**      http://rtci/vespa1/Panel%20Management%20Report.aspx
**
** See also "Vespa PanMan 01 - build reports.sql" for outstanding task list.
**
******************************************************************************/

-- Accounts and profiling:

if object_id('Vespa_PanMan_all_households') is not null
   drop table Vespa_PanMan_all_households;
-- This guy eventually holds all households, not just those which have returned data,
-- though it happens to get data return metrics on it as well.
create table Vespa_PanMan_all_households (
    account_number                  varchar(20)         not null primary key
    ,hh_box_count                   tinyint             not null
    ,accno_SHA1                     varchar(40)         -- We'll use SHA1 hashes for random yet repeatable tiebreakers
    ,most_recent_enablement         date                not null
    ,reporting_categorisation       varchar(20) -- a categorisation into 4: relaible, somewhat reliable (which we might drop because it doesn't happen often), unreliable, and zero reporting.
    ,panel                          varchar(10)
    -- Other things we tack on later to help make prioritisation decisions:
    ,scaling_segment_ID             int
    ,non_scaling_segment_ID         int
    ,reporting_quality              float       -- holds the minimum of the reporting quality metric we derived on the SBV
    ,reporting_rank                 int         -- the rank of (reporting_quality) within the segment, ties broken by account_number for uniqueness (It's already within a segment, slight systematic bias from account number is fine)
    ,selection                      varchar(10)
    -- We're also tracking the weights we assign at each round of selection
    ,round_1_utility                float
    ,round_2_utility                float
    ,round_3_utility                float
    ,round_4_utility                float
);

-- Because we need to be able to use it as a tiebreaker:
create index hash_force on Vespa_PanMan_all_households (accno_SHA1);

-- So building the SHA1 hashes during the insert appears super slow, so instead
-- we'll cache a table of them and each time check if we can get the has from
-- this table rather than rebuilding them all each week;
/* And we're not dropping / recreating with each refresh because the whole point
** is that it's an archive
if object_id('Vespa_PanMan_SHA1_archive') is not null
    drop table Vespa_PanMan_SHA1_archive;
create table Vespa_PanMan_SHA1_archive (
    account_number                  varchar(20)         not null primary key
    ,accno_SHA1                     varchar(40)
);
*/

commit;
go

/*********** Segmentation management (except the names and lookups) ***********/

if object_id('Vespa_PanMan_Scaling_Segment_Profiling') is not null
   drop table Vespa_PanMan_Scaling_Segment_Profiling;
create table Vespa_PanMan_Scaling_Segment_Profiling (
    Panel                                               varchar(10)
    ,scaling_segment_id                                 int             -- All combinations of variables used in scaling
    ,scaling_segment_name                               varchar(150)
    ,non_scaling_segment_id                             int             -- All combinations of other variables that aren't used in scaling
    ,non_scaling_segment_name                           varchar(100)
    ,Sky_Base_Households                                int             -- duplicated across panels, but that's okay
    ,Panel_households                                   int
    ,Acceptably_reliable_households                     int             -- Some denormalisation in here, this is closer to the format in which results are delivered
    ,Unreliable_households                              int
    ,Zero_reporting_households                          int
    ,Recently_enabled_households                        int
    ,Acceptably_reporting_index                         decimal(6,2)  default null
    ,primary key (scaling_segment_id, non_scaling_segment_id, panel)
    -- Redundancy index being moved to it's own table
);

-- These guys needs their own indexes because we'll need to join through them:
create index for_joining on Vespa_PanMan_Scaling_Segment_Profiling (non_scaling_segment_id);
commit;

-- A few tables for managing the redundancy indices, because they have a different format
if object_id('Vespa_PanMan_panel_redundancy_calculations') is not null
   drop table Vespa_PanMan_panel_redundancy_calculations;
create table Vespa_PanMan_panel_redundancy_calculations (
    scaling_segment_id                                  int
    ,non_scaling_segment_id                             int
    ,Sky_Base_Households                                int -- not sure if we'll need this here?
    ,Vespa_acceptable_HHs                               int
    ,Panel_6_acceptable_HHs                             int
    ,Panel_7_acceptable_HHs                             int
    ,Redundancy_index                                   decimal(6,2) default null -- of Vespa panel into the alternate panels?
    ,primary key (scaling_segment_id, non_scaling_segment_id)
); 

/* And it's a historic table, we don't really want to be killing it each time we refresh the
** structurals eh...
if object_id('Vespa_PanMan_Historic_Panel_Metrics') is not null
   drop table Vespa_PanMan_Historic_Panel_Metrics;
create table Vespa_PanMan_Historic_Panel_Metrics (
    metric_date                         date            not null primary key        -- Date to which the following metrics apply
    ,submission_date                    date            default today()
    ,sky_base_coverage                  decimal(6,4)    not null
    ,reliability_rating                 decimal(6,4)    not null
    ,households_reliably_reporting      integer         not null
);

grant select on Vespa_PanMan_Historic_Panel_Metrics              to greenj, dbarnett, jacksons, stafforr, sarahm, poveys, gillh, rombaoad, louredaj, patelj, kinnairt, sbednaszynski, vespa_group_low_security, sk_prodreg;
*/

commit;
go

/*********** Segmentation lookups and names ***********/

-- For the non-scaling variables only, the segmentation stuff gets built by Scaling.

-- The table of all the different segmentations we're profiling / selecting on but not scaling with:
if object_id('Vespa_PanMan_non_scaling_segments_lookup') is not null
   drop table Vespa_PanMan_non_scaling_segments_lookup;
create table Vespa_PanMan_non_scaling_segments_lookup (
    non_scaling_segment_id                              int identity primary key
    ,non_scaling_segment_name                           varchar(100)
    ,value_segment                                      varchar(10)     -- Comes from internal value segments table
    -- What else do we want in here? This'll do for the moment
    ,MOSAIC_segment                                     varchar(1)      -- Currently uses EXPERIAN_CONSUMERVIEW.h_mosaic_uk_2009_group
    ,Financial_strategy_segment                         varchar(1)      -- Currently uses EXPERIAN_CONSUMERVIEW.h_fss_group
    ,is_OnNet                                           bit
    ,uses_sky_go                                        bit
);

commit;

select distinct value_seg
into #Value_segment_categories
from sk_prod.VALUE_SEGMENTS_DATA;

-- So we're turning any NULLs into U for Unknown, which is usually somewhere within the
-- data itself but to make sure we're also adding it specifically:
select distinct h_mosaic_uk_group -- h_mosaic_uk_2009_group
into #mosaic_categories
from sk_prod.EXPERIAN_CONSUMERVIEW
where h_mosaic_uk_group is not null and h_mosaic_uk_group <> 'U';
-- Done carefully to ensure uniqueness
insert into #mosaic_categories select 'U';

commit;

select distinct h_fss_group
into #financial_strategy_categories
from sk_prod.EXPERIAN_CONSUMERVIEW
where h_fss_group is not null and h_fss_group <> 'U';
-- Done carefully to ensure uniqueness
insert into #financial_strategy_categories select 'U';

-- Now also need Onnet / Offnet which is just a binary flag:
select convert(bit,1) as yesno
into #bit_categories;
insert into #bit_categories values (0);

commit;

-- Thing is, we also want to put all the segments we want into this table:
insert into Vespa_PanMan_non_scaling_segments_lookup (
    value_segment
    ,MOSAIC_segment
    ,Financial_strategy_segment
    ,is_OnNet
    ,uses_sky_go
)
select vs.value_seg
    ,mc.h_mosaic_uk_group
    ,fs.h_fss_group
    ,onc.yesno
    ,sg.yesno
from #Value_segment_categories              as vs
inner join #mosaic_categories               as mc   on 1=1
inner join #financial_strategy_categories   as fs   on 1=1
inner join #bit_categories                  as onc  on 1=1  -- OnNet / OffNet categories
inner join #bit_categories                  as sg   on 1=1  -- Sky Go use categories
;

-- 1568 segments. That's a few, but not tooo many... except that it goes as a product
-- into the scaling segments, of which there are already a lot...

commit;
go

-- For the name, we might eventually CASE these guys out so that the actual
-- Experian segment names get used rather than just the codes. But, not yet.

go
update Vespa_PanMan_non_scaling_segments_lookup
set non_scaling_segment_name = '(' || ltrim(rtrim(value_segment)) || ') - ('
                                   || case ltrim(rtrim(MOSAIC_segment)) 
                                        when 'A' then 'Alpha Territory'
                                        when 'B' then 'Professional Rewards'
                                        when 'C' then 'Rural Solitude'
                                        when 'D' then 'Small Town Diversity'
                                        when 'E' then 'Active Retirement'
                                        when 'F' then 'Suburban Mindsets'
                                        when 'G' then 'Careers and Kids'
                                        when 'H' then 'New Homemakers'
                                        when 'I' then 'Ex-Council Community'
                                        when 'J' then 'Claimant Cultures'
                                        when 'K' then 'Upper Floor Living'
                                        when 'L' then 'Elderly Needs'
                                        when 'M' then 'Industrial Heritage'
                                        when 'N' then 'Terraced Melting Pot'
                                        when 'O' then 'Liberal Opinions'
                                        else 'Unknown MOSAIC'
                                      end || ') - ('
                                   || case ltrim(rtrim(Financial_strategy_segment))
                                        when 'A' then 'Successful Start'
                                        when 'B' then 'Happy Housemates'
                                        when 'C' then 'Surviving Singles'
                                        when 'D' then 'On The Breadline'
                                        when 'E' then 'Flourishing Families'
                                        when 'F' then 'Credit Hungry Families'
                                        when 'G' then 'Gilt Edged Lifestyles'
                                        when 'H' then 'Mid Life Affluence'
                                        when 'I' then 'Modest Mid Years'
                                        when 'J' then 'Advancing Status'
                                        when 'K' then 'Ageing Workers'
                                        when 'L' then 'Wealthy Retirement'
                                        when 'M' then 'Elderly Deprivation'
                                        else 'Unknown FSS'
                                      end || ') - ('
                                   || case is_OnNet
                                        when 1 then 'OnNet'
                                        when 0 then 'OffNet'
                                      end || ') - ('
                                   || case uses_sky_go
                                        when 1 then 'Uses Sky Go'
                                        when 0 then 'No Sky Go'
                                    end || ')'
commit;

-- Now it's populated, throw on the index that will help us join stuff:
create unique index for_joining on Vespa_PanMan_non_scaling_segments_lookup
    (value_segment, MOSAIC_segment, Financial_strategy_segment, is_OnNet, uses_sky_go);
-- And also for consistency / completeness, all ofd the names should be unique too:
create unique index name_checking on Vespa_PanMan_non_scaling_segments_lookup (non_scaling_segment_name);
commit;

-- Also, we get the scaling segments build from the nightly scaling builds, but the non-
-- scaling segmentations we'd have to build ourselves, so they'll go into this table:
if object_id('Vespa_PanMan_this_weeks_non_scaling_segmentation') is not null
    drop table Vespa_PanMan_this_weeks_non_scaling_segmentation;
create table Vespa_PanMan_this_weeks_non_scaling_segmentation (
    account_number                      varchar(20) primary key
    ,non_scaling_segment_id             int
    ,value_segment                      varchar(10)
    ,consumerview_cb_row_id             bigint
    ,MOSAIC_segment                     varchar(1)
    ,Financial_strategy_segment         varchar(1)
    ,is_OnNet                           bit         default 0
    ,uses_sky_go                        bit         default 0
    -- Dunno what else we're profiling in here yet by the way
);

commit;

-- Index for bringing in the ID flag:
create index for_joining    on Vespa_PanMan_this_weeks_non_scaling_segmentation (value_segment, MOSAIC_segment, Financial_strategy_segment);
create index for_updating   on Vespa_PanMan_this_weeks_non_scaling_segmentation (consumerview_cb_row_id);

commit;
go

/**************** QUASI-RESULTS STRUCTURES! ****************/

-- So this table holds all the single variable aggregation results and for
-- the result pulss, we just filter on the panel and aggregation variable
-- to pull out what we need in each instance.
if object_id('Vespa_PanMan_all_aggregated_results') is not null
    drop table Vespa_PanMan_all_aggregated_results;
create table Vespa_PanMan_all_aggregated_results (
    panel                                               varchar(6)
    ,aggregation_variable                               varchar(30)
    ,scaling_or_not                                     bit
    ,variable_value                                     varchar(60)
    ,Sky_Base_Households                                int
    ,Panel_Households                                   int
    ,Acceptable_Households                              int
    ,Unreliable_Households                              int
    ,Zero_reporting_Households                          int
    ,Recently_enabled_households                        int
    ,Good_Household_Index                               decimal(6,2)
    ,primary key (panel, aggregation_variable, variable_value)
    -- We could recast all the unknowns in the scaling segments table to "Unknown Lifestage"
    -- and "Unknown Affluence" as applicable, but that'll be messy around the place, could
    -- have a collection of other affects. Right now, we do need aggregation_variable in the
    -- key too, since we'll have the same value (the unknowns) across different variables.
);

commit;
go

/**************** HISTORICAL VIEW OF SUMMARY FIGURES ****************/

-- Since 21/02/2013, this table is containing the history of figures shown on the summary section of the report
-- aiming to spot the trend of viewing consent...

if object_id('Vespa_PanMan_hist_summary') is not null
    drop table Vespa_PanMan_hist_summary;

commit;

create table Vespa_PanMan_hist_summary (
	weekending		date		not null
	,concept		varchar(20)	not null
	,daily_panel	integer		not null
	,alt6			integer		not null
	,alt7			integer		not null
);

commit;

create date index vespa_panman_index1 on Vespa_PanMan_hist_summary(weekending);
create lf index vespa_panman_index1 on Vespa_PanMan_hist_summary(concept);

commit;
go

/**************** HISTORICAL VIEW OF TRAFFIC LIGHTS ****************/

if object_id('Vespa_PanMan_hist_trafficlight') is not null
	drop table Vespa_PanMan_hist_trafficlight;
	
commit;

create table Vespa_PanMan_hist_trafficlight (
	weekending			date			not null
	,scaling_variable	varchar(25)		not null
	,daily_panel		decimal(15,2)	not null
	,alt_6				decimal(15,2)	not null
	,alt_7				decimal(15,2)	not null
);

commit;

create date index vespa_panman_dindex1	on Vespa_PanMan_hist_trafficlight(weekending);
create lf index vespa_panman_lfindex1 	on Vespa_PanMan_hist_trafficlight(scaling_variable);

commit;
go
/**************** PERMISSIONS! ****************/

grant select on Vespa_PanMan_all_households                      	to vespa_group_low_security, sk_prodreg;
grant select on Vespa_PanMan_Scaling_Segment_Profiling           	to vespa_group_low_security, sk_prodreg;
grant select on Vespa_PanMan_all_aggregated_results              	to vespa_group_low_security, sk_prodreg;
grant select on Vespa_PanMan_this_weeks_non_scaling_segmentation 	to vespa_group_low_security, sk_prodreg;
grant select on Vespa_PanMan_hist_summary							to vespa_group_low_security, sk_prodreg;
grant select on Vespa_PanMan_hist_trafficlight						to vespa_group_low_security, sk_prodreg;
commit;
go
