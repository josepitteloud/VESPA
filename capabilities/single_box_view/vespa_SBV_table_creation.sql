/******************************************************************************
**
** Project Vespa: Single box view
**                  - Table structure
**
** This is kind of a prototype build for something Ops might build for us. We
** own it, which means we can change it easily when we need to. It should act
** as a lookup for basic common current things we like to refer to from time
** to time.
**
** Current box scope are all those for which Panel_ID=4 enablement is requested,
** plus those which the DB actually considers active, plus anything which is in
** the Sky View panel too. Refer to
**
**      http://rtci/vespa1/Single%20box%20view.aspx
**
** for further details and documentation of available variables and their
** associated definitions.
**
******************************************************************************/

IF object_id('vespa_single_box_view') IS NOT NULL
    DROP TABLE vespa_single_box_view;
-- For the commented out columns we don't have rules yet / havent implemented
-- the flag population.
create table vespa_single_box_view (

    -- Linking and identificatioin stuff:

    subscriber_id                   decimal(10)     not null primary key
    ,card_subscriber_id             varchar(10)     not null                -- for joining to the tables that have the keys as varchars
    ,account_number                 varchar(20)     not null
    ,service_instance_id            varchar(30)                             -- used for linking back to customer database (sometimes service_instance_id, sometimes src_system_id)
    ,cb_key_individual              bigint          default null            -- for joining into 3rd party data when required
    ,consumerview_cb_row_id         bigint          default null            -- also for joinging into 3rd party data; use as required
    
    -- Enablement & panel flags
    
    ,Panel                          varchar(10)     default null            -- Single reference flag for panel the box is on
    ,Panel_ID_Vespa                 tinyint                                 -- Panel ID from the log snapshot: 5 -> Old Vespa, 4 -> New Vespa, 1-> Sky View
    ,Panel_ID_4_cells_confirm       bit             default 0               -- Membership in the confirmed Vespa Panel from Midas campaign cells. (No longer panel 4, but this remains for historic reasons)
    ,in_vespa_panel		            bit             default 0               -- Refering to whether the box belongs to the vespa panel (despite if its enabled or not)...
	,in_vespa_panel_11				bit				default 0 				-- Refering to whether the box belongs to panel 11 - the above is for 12 -- cortb 17/01/2014
    ,Is_Sky_View_candidate          bit             default 0               -- Whether or not the box can be considered for Sky View panel (which actually goes by account, and is slightly outdated now)
    ,Is_Sky_View_Selected           bit             default 0               -- definitive list as to whether box should be returning data for Sky View. One-off data feed as of 7 Feb 2012.
    ,Status_Vespa                   varchar(20)                             -- The box status from Subscriber Status
    ,cust_active_dtv                bit             default 0               -- Active DTV customer or not (probably should be eh?)
    ,uk_standard_account            bit             default 0               -- Standard UK account or not (excludes staff, ROI, etc)
	,alternate_panel_5				bit				default 0				-- Flag for boxes on alternate panel 5 -- cortb added 17/10/2014
    ,alternate_panel_6              bit             default 0               -- Flag for boxes on alternate panel 6 (Not yet populated, but necessary for Panel Management)
    ,alternate_panel_7              bit             default 0               -- Flag for boxes on alternate panel 7 (Not yet populated, but necessary for Panel Management)

    -- For deciding the enablement date:
    
    ,Enablement_date                date                                    -- Date to consider for box enablement
    ,Enablement_date_source         varchar(20)     default 'None'          -- Which of the following rules was used for the date:
    ,vss_request_dt                 date                                    -- Enabled according to Vespa DB
    ,Sky_View_load_date             date                                    -- Date associated with Sky View panel load batch
    ,historic_result_date           date                                    -- Most recent enablement date pulled out of subscriber history table
    ,Selection_date                 date                                    -- Writeback date on campaign cells (Vespa open-loop enabled only)
    ,vss_created_date               date                                    -- Date record was created in the subscriber status table
    
    -- Whether or not the box is reporting:
    
--    ,Has_returned_data_ever         bit             default 0               -- Perhaps redundant given STB log snapshot?
--    ,Has_returned_data_this week    bit             default 0               --  ^^
--    ,Dials_back_every_day_in_last_month bit         default 0               -- This flag is still relevant; 
    ,In_stb_log_snapshot            bit             default 0               -- should align with returning data ever flag, but probably won't
    ,logs_every_day_30d             bit             default 0               -- If we have data returned every day for the last 30 days
    ,logs_returned_in_30d           tinyint         default 0               -- Number of days for which we have logs, a little more detail for boxes that aren't quite at 30
    ,reporting_quality              float           default null            -- A metric between 0 and 1 which rates how well the box is returning data. NULL for new boxes which haven't had a chance to be properly rated yet.

    -- Profiling variables:
    ,PS_Olive                       varchar(1)      default 'U'             -- 'U' for Unknown, or the usual 'P' / 'S'
    ,PS_Vespa                       varchar(1)      default 'U'             -- 'U' for Unknown, or the usual 'P' / 'S'
    ,PS_inferred_primary            bit             default 0
    ,PS_flag                        varchar(1)      default 'U'             -- A combined mark from all sources; 'P', 'S', 'U', or '!' for collisions.
    ,PS_source                      varchar(10)     default 'None'          -- to mark how we came up with the P/S that we did
    ,Box_type_subs                  varchar(20)                             -- The standard definition of box type from cust_subs_hits and cust_entitlement_lookup
    ,Box_type_physical              varchar(20)                             -- The actual physical type of box from cust_set_top_box
    ,HD_box_subs                    bit             default 0               -- Whether or not the box has a HD subscription
    ,HD_box_physical                bit             default 0               -- If the box is physically capable of HD
    ,HD_1TB_physical                bit             default 0
    ,Box_is_3D                      bit             default 0
    ,Account_anytime_plus           bit             default 0
    ,Box_has_anytime_plus           bit             default 0
    ,PVR                            bit             default 0
    -- we never have that many premium channels,
    ,prem_sports                    tinyint         default 0               -- Goes by account rather than box, but gets used all the time. Keeps the raw format
    ,prem_movies                    tinyint         default 0               --  as we always end up doing different groupings for each report.

);
-- We considered having a different "active" flag for trackign churn etc, but
-- properly catching all of the active / churned / opted out permutations is
-- going to be messy so we'll just rebuild it from the base each time.

create        index account_number_index        on vespa_single_box_view (account_number);
create unique index card_subscriber_id_index    on vespa_single_box_view (card_subscriber_id);
create        index service_id_index            on vespa_single_box_view (service_instance_id); -- This one should be unique? except there are nulls.
create        index cb_key_individual_index     on vespa_single_box_view (cb_key_individual);
create        index consumerview_link_index     on vespa_single_box_view (consumerview_cb_row_id);



/****************** SUPPORT TABLES: GET POPULATED AND NUKED ON EACH REBUILD ******************/

-- A table to collect the reporting details inthe last month for each box. Needs
-- to be permanent because it gets populated within a dynamic EXEC
IF object_id('Vespa_SBV_logs_dump') IS NOT NULL
    DROP TABLE Vespa_SBV_logs_dump;
create table Vespa_SBV_logs_dump
(
    subscriber_id               decimal(10)
    ,doc_date_from_6am          date
);

-- Previously didn't need any keys, but now we're grouping and joining a bunch,
-- so we kind of do need them. Might slow down the build, but hey.
create index maybe_PK on Vespa_SBV_logs_dump (subscriber_id, doc_date_from_6am);
-- This guy gets dropped and recreated each iteration (so as to not rebuild the
-- index a bunch of times as we add data from each daily table) but it should be
-- considered an essential part of the table

commit;

/****************** SBV REPORTING QUALITY HISTORY ******************/

if object_id('vespa_sbv_hist_qualitycheck') is not null
	drop table vespa_sbv_hist_qualitycheck;
	
commit;

create table vespa_sbv_hist_qualitycheck(
weekending			date			not null
,account_number		varchar(20)		not null
,subscriber_id		decimal(10,0)	not null
,panel_id			tinyint
,reporting_quality	real			default null
,logs_every_day_30d	bit				not null
,auditdate			timestamp			null
);

commit;

create date index 	sbv_hist_index1 on vespa_sbv_hist_qualitycheck(weekending);
create hg index 	sbv_hist_index2 on vespa_sbv_hist_qualitycheck(account_number);
create hg index 	sbv_hist_index3 on vespa_sbv_hist_qualitycheck(subscriber_id);
create lf index 	sbv_hist_index4 on vespa_sbv_hist_qualitycheck(panel_id);

commit;

/****************** CSI TABLE CREATION **********/


create table csi (si_external_identifier varchar(48) not null
                    , rank_             int     default 0
                    , si_service_instance_type varchar(255)
                    )

commit;

/****************** PRIVILEGES ******************/

grant select on vespa_single_box_view 		to vespa_group_low_security, sk_prodreg;
grant select on vespa_sbv_hist_qualitycheck to vespa_group_low_security, sk_prodreg;
grant select on csi						    to vespa_group_low_security, sk_prodreg;

commit;