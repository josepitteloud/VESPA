/*


                         $$$
                        I$$$
                        I$$$
               $$$$$$$$ I$$$    $$$$$      $$$ZDD    DDDDDDD.
             ,$$$$$$$$  I$$$   $$$$$$$    $$$ ODD  ODDDZ 7DDDD
             ?$$$,      I$$$ $$$$. $$$$  $$$= ODD  DDD     NDD
              $$$$$$$$= I$$$$$$$    $$$$.$$$  ODD +DD$     +DD$
                  :$$$$~I$$$ $$$$    $$$$$$   ODD  DDN     NDD.
               ,.   $$$+I$$$  $$$$    $$$$=   ODD  NDDN   NDDN
              $$$$$$$$$ I$$$   $$$$   .$$$    ODD   ZDDDDDDDN
                                      $$$      .      $DDZ
                                     $$$             ,NDDDDDDD
                                    $$$?

                      CUSTOMER INTELLIGENCE SERVICES

--------------------------------------------------------------------------------------------------------------
**Project Name:							OPS 2.0
**Analysts:                             Angel Donnarumma (angel.donnarumma_mirabel@skyiq.co.uk)
**Lead(s):                              Jose Loureda
**Stakeholder:                          Operational Reports / SIG
**Due Date:                             20/09/2013
**Project Code (Insight Collation):     
**Sharepoint Folder:                    
                                                                        
**Business Brief:

Creating Tables required for the Measurement & Algorithims Single View Generator Unit (MASVG)...

** List of tables involved in the project:

+	SIG_SBV_YYYYMMDD					<Managed by Project>		[OUTPUT]
+	SIG_SINGLE_BOX_VIEW					<Managed by Project>		[OUTPUT]
+	SIG_SBV_HIST_VIEW					<Managed by Project>		[OUTPUT]
+	SIG_SBV_HIST_SUMMARY				<Managed by Project>		[OUTPUT]
+	SIG_SAV_YYYYMMDD					<Managed by Project>		[OUTPUT]
+	SIG_SINGLE_ACCOUNT_VIEW				<Managed by Project>		[OUTPUT]
+	SIG_SAV_HIST_VIEW					<Managed by Project>		[OUTPUT]
+ 	SIG_SAV_HIST_SUMMARY				<Managed by Project>		[OUTPUT]
+	sig_non_scaling_segments_lookup		<Managed by Project>		[OUTPUT] -- former: VESPA_NON_SCALING_SEGMENTS_LOOKUP
+	sig_current_non_scaling_segments	<Managed by Project>		[OUTPUT]
+	m01_t1_process_manager				<Managed by Project>		[OUTPUT]
+	m04_t1_panel_sample_stage0			<Managed by Project>		[TRANSIENT]
+ 	m05_t1_panel_performance_stage0		<Managed by Project>		[TRANSIENT]
+ 	m06_t1_panel_balance_stage0			<Managed by Project>		[TRANSIENT]
+	m07_t1_box_base_stage0				<Managed by Project>		[TRANSIENT]
+ 	m08_t1_account_base_stage0			<Managed by Project>		[TRANSIENT]

-- Setting Privileges (at the end)

--------------------------------------------------------------------------------------------------------------
*/

-------------------
-- SIG_SBV_YYYYMMDD
-------------------

/*if object_id ((suser_name()||'.SIG_SBV_YYYYMMDD')) is not null
	drop table SIG_SBV_YYYYMMDD;
	
commit;

create table SIG_SBV_YYYYMMDD(

	Weekending						date			identity
	,Subscriber_id					decimal(10) 	not null
	,Card_Subscriber_ID				varchar(10)		not null
	,Account_number					varchar(20) 	not null
	,Service_instance_ID			varchar(30) 	default null
	,consumerview_cb_row_id			bigint 			default null
	,Panel							varchar(10) 	default null
	,Panel_ID_4_cells_confirm		bit 			default 0
	,Is_Sky_view_cancidate			bit 			default 0
	,Is_Sky_view_selected			bit 			default 0
	,status_vespa					varchar(20) 	default null
	,Enablement_date				date 			default null
	,Enablement_date_source			varchar(20) 	default null --default 'None'
	,vss_request_dt					date 			default null
	,Sky_view_load_date				date 			default null
	,historic_result_date			date 			default null
	,Selection_date					date 			default null
	,vss_created_date				date 			default null
	,Num_logs_sent_30d				integer			default null --default 0
	,Num_logs_sent_7d				integer			default null --default 0
	,Continued_trans_30d			integer			default null --default 0
	,Continued_trans_7d				integer			default null --default 0
	,reporting_quality				decimal(15,3) 	default null
	,PS_Olive						varchar(1) 		default null --default 'U'
	,PS_vespa						varchar(1) 		default null --default 'U'
	,PS_inferred_primary			bit 			default null --default 0
	,ps_flag						varchar(1) 		default null --default 'U'
	,ps_source						varchar(10) 	default null --default 'None'
	,box_type_physical				varchar(20) 	default null
	,HD_box_physical				bit 			default 0
	,box_storage_capacity			varchar(20)		default null
	,Box_is_3D						bit 			default 0
	,Box_has_anytime_plus			bit 			default 0
	--,PVR							bit 		default 0 ?????
	,Scaling_segment_ID				integer			default null	
	,Non_Scaling_segment_ID			integer			default null
	,Box_model						varchar(20)		default null
	,Adsmart_flag					bit				default 0

);

create hg index SIG_SBV_YYYYMMDD_hg1 on SIG_SBV_YYYYMMDD(subscriber_id);
create hg index SIG_SBV_YYYYMMDD_hg2 on SIG_SBV_YYYYMMDD(card_subscriber_id);
create hg index SIG_SBV_YYYYMMDD_hg3 on SIG_SBV_YYYYMMDD(account_number);
create hg index SIG_SBV_YYYYMMDD_hg4 on SIG_SBV_YYYYMMDD(Scaling_segment_ID);
create hg index SIG_SBV_YYYYMMDD_hg5 on SIG_SBV_YYYYMMDD(Non_Scaling_segment_ID);

commit;*/

----------------------
-- SIG_SINGLE_BOX_VIEW
----------------------

/*
	This is a view of latest SIG_SBV_YYYYMMDD
	
	The idea is to standardise the access of the data to easy the analysis...
*/

--------------------
-- SIG_SBV_HIST_VIEW
--------------------

/*
	This is a view of latest 12 weeks (3 Months) of SIG_SBV_YYYYMMDD
	
	The idea is to standardise the access of the data to easy the analysis...
*/

-----------------------
-- SIG_SBV_HIST_SUMMARY
-----------------------

-- Coming soon...

-------------------
-- SIG_SAV_YYYYMMDD
-------------------

/*if object_id ((suser_name()||'.SIG_SAV_YYYYMMDD')) is not null
	drop table SIG_SAV_YYYYMMDD;
	
commit;

create table SIG_SAV_YYYYMMDD(

	Weekending					date			identity
	,Account_number				varchar(20) 	not null
	,viewing_consent_flag		bit				default 0
	,Scaling_segment_ID			integer			default null
	,Non_Scaling_segment_ID		integer			default null
	,Adsmart_flag				bit				default 0
	,Num_boxes					tinyint			default 0
	,Num_adsmartable_boxes		tinyint			default 0
	,Panel						varchar(10) 	default null
	,Skygo_subs					bit				default 0
	,Anytime_plus_subs			bit				default 0
	,Reporting_performance		varchar(10)		default null -- categories: [ 'reliably', 'unreliably', 'zero recent' ]
	,Panel_recency				date			default null
	,avg_reporting_quality		decimal(15,3)	default null
	,min_reporting_quality		float 			default null
	,status_vespa				varchar(20) 	default null
	,Panel_activation_date		date			default null
	,cb_key_individual			bigint			default null
	,Cust_active_DTV			bit				default 0
	,UK_Standard_account		varchar(3)		default null
	,box_type_subs				varchar(20)		default null
	,HD_box_subs				bit				default 0
	,RTM						varchar(30)		default null
	,Weight						decimal(8,0)	default null
	,Weight_date				date			default null
	,prem_sports				tinyint			default null
	,prem_movies				tinyint			default null
	,cust_active_dt				date			default null

);

create hg index SIG_SAV_YYYYMMDD_hg1 on SIG_SBV_YYYYMMDD(account_number);
create hg index SIG_SAV_YYYYMMDD_hg2 on SIG_SBV_YYYYMMDD(Scaling_segment_ID);
create hg index SIG_SAV_YYYYMMDD_hg3 on SIG_SBV_YYYYMMDD(Non_Scaling_segment_ID);

commit;*/

--------------------------
-- SIG_SINGLE_ACCOUNT_VIEW
--------------------------

/*
	This is a view of latest SIG_SAV_YYYYMMDD
	
	The idea is to standardise the access of the data to easy the analysis...
*/

--------------------
-- SIG_SAV_HIST_VIEW
--------------------

/*
	This is a view of latest 12 weeks (3 Months) of SIG_SAV_YYYYMMDD
	
	The idea is to standardise the access of the data to easy the analysis...
*/

-----------------------
-- SIG_SAV_HIST_SUMMARY
-----------------------

-- Coming soon...

-------------------------
-- m01_t1_process_manager
-------------------------

if object_id('m01_t1_process_manager') is not null
        drop table m01_t1_process_manager;
        
commit;
--go

create table m01_t1_process_manager(
        sequencer       integer    	default autoincrement
        ,task           varchar(50) not null
        ,status         bit         default 0
        ,weekending     date            
        ,audit_date     date        not null
);

commit;
--go

-- insert  into m01_t1_process_manager(task,audit_date) Values(<MODULE 02 NAME>,cast(now() as date));
insert  into m01_t1_process_manager(task,audit_date) Values('sig_masvg_m08_account_base',cast(now() as date));
insert  into m01_t1_process_manager(task,audit_date) Values('sig_masvg_m07_box_base',cast(now() as date));
insert  into m01_t1_process_manager(task,audit_date) Values('sig_masvg_m04_panel_composition',cast(now() as date));
insert  into m01_t1_process_manager(task,audit_date) Values('sig_masvg_m05_panel_performance',cast(now() as date));
insert  into m01_t1_process_manager(task,audit_date) Values('sig_masvg_m06_panel_balance',cast(now() as date));
insert  into m01_t1_process_manager(task,audit_date) Values('sig_masvg_m09_boxview_generator',cast(now() as date));
insert  into m01_t1_process_manager(task,audit_date) Values('sig_masvg_m10_accountview_generator',cast(now() as date));
insert	into m01_t1_process_manager(task,audit_date) Values('sig_masvg_m11_panel_measurement_generator',cast(now() as date));

commit;
--go


-----------------------------
-- m04_t1_panel_sample_stage0
-----------------------------


if object_id ('m04_t1_panel_sample_stage0') is not null
	drop table m04_t1_panel_sample_stage0;
	
commit;

create table m04_t1_panel_sample_stage0(

	account_number				varchar(20)	not null
	,card_subscriber_id			varchar(10)	not null
	,subscriber_id				decimal(10) not null
	,Panel						varchar(10) default null
	,Panel_ID_4_cells_confirm	bit 		default 0
	,Is_Sky_view_candidate		bit 		default 0
	,Is_Sky_view_selected		bit 		default 0
	,status_vespa				varchar(20) default null
	,Enablement_date			date 		default null
	,Enablement_date_source		varchar(20) default null 	--default 'None'
	,vss_request_dt				date 		default null
	,Sky_view_load_date			date 		default null
	,historic_result_date		date 		default null
	,Selection_date				date 		default null
	,vss_created_date			date 		default null
	,PS_Olive					varchar(1) 	default null 	--default 'U'
	,PS_vespa					varchar(1) 	default null 	--default 'U'
	,PS_inferred_primary		bit 		default 0
	,ps_flag					varchar(1) 	default null 	--default 'U'
	,ps_source					varchar(10) default null 	--default 'None'
	,Panel_recency				date		default null
	,Panel_activation_date		date		default null

);

create hg index m04_t1_panel_sample_stage0_hg1 on m04_t1_panel_sample_stage0(account_number);
create hg index m04_t1_panel_sample_stage0_hg2 on m04_t1_panel_sample_stage0(card_subscriber_id);
create hg index m04_t1_panel_sample_stage0_hg3 on m04_t1_panel_sample_stage0(subscriber_id);

commit;


----------------------------------
-- m05_t1_panel_performance_stage0
----------------------------------

if object_id ('m05_t1_panel_performance_stage0') is not null
	drop table m05_t1_panel_performance_stage0;
	
commit;

create table m05_t1_panel_performance_stage0(

	account_number			varchar(20)		not null
	,subscriber_id			decimal(10) 	not null
	,Num_logs_sent_30d		tinyint			default 0
	,Num_logs_sent_7d		tinyint			default 0
	,Continued_trans_30d	tinyint			default 0
	,Continued_trans_7d		tinyint			default 0
	,return_data_7d			bit				default 0
	,return_data_30d		bit				default 0
	,reporting_quality		decimal(15,3)	default 0
	--,Reporting_performance	varchar(10)		default null -- categories: [ 'reliably', 'unreliably', 'zero recent' ]
	--,avg_reporting_quality	decimal(15,3)	default null
	--,min_reporting_quality	decimal(15,3)	default null
	,num_ac_returned_30d	tinyint			default 0
	,num_ac_returned_7d    	tinyint			default 0
	,ac_full_returned_30d	bit				default 0
	,ac_full_returned_7d    bit				default 0
	,reporting_quality_s	decimal(15,3)	default 0
	,total_calls_in_30d		tinyint			default null
	,avg_events_in_logs		decimal(15,3)	default null
	
);


create hg index m05_t1_panel_performance_stage0_hg1 on m05_t1_panel_performance_stage0(account_number);
create hg index m05_t1_panel_performance_stage0_hg2 on m05_t1_panel_performance_stage0(subscriber_id);

commit;


------------------------------
-- m06_t1_panel_balance_stage0
------------------------------

if object_id ('m06_t1_panel_balance_stage0') is not null
	drop table m06_t1_panel_balance_stage0;
	
commit;

create table m06_t1_panel_balance_stage0(

	account_number			varchar(20)		primary key
	,Scaling_segment_ID		integer			default null	
	,Non_Scaling_segment_ID	integer			default null
	,Weight					float			default null
	,viq_weight				float			default null
	,Weight_date			date			default null
	
);

commit;

create hg index m06_hg1 	on m06_t1_panel_balance_stage0(Scaling_segment_ID);
create hg index m06_hg2 	on m06_t1_panel_balance_stage0(Non_Scaling_segment_ID);
create date index m06_date1 on m06_t1_panel_balance_stage0(weight_date);

commit;


----------------------------------
-- sig_non_scaling_segments_lookup
----------------------------------

if object_id('sig_non_scaling_segments_lookup') is not null
	drop table sig_non_scaling_segments_lookup;
   
create table sig_non_scaling_segments_lookup (

    non_scaling_segment_id			int identity primary key
    ,non_scaling_segment_name       varchar(100)
    ,value_segment                  varchar(10)     -- Comes from internal value segments table
    ,MOSAIC_segment                 varchar(1)
    ,Financial_strategy_segment     varchar(1)
    ,is_OnNet                       bit
    ,uses_sky_go                    bit
	
);

commit;

select	distinct 
		value_seg
into 	#Value_segment_categories
from 	sk_prod.VALUE_SEGMENTS_DATA;

-- So we're turning any NULLs into U for Unknown, which is usually somewhere within the
-- data itself but to make sure we're also adding it specifically:
select	distinct 
		h_mosaic_uk_group -- h_mosaic_uk_2009_group
into 	#mosaic_categories
from 	sk_prod.EXPERIAN_CONSUMERVIEW
where 	h_mosaic_uk_group is not null 
and 	h_mosaic_uk_group <> 'U';

-- Done carefully to ensure uniqueness
insert into #mosaic_categories select 'U';

commit;

select	distinct 
		h_fss_group
into 	#financial_strategy_categories
from 	sk_prod.EXPERIAN_CONSUMERVIEW
where 	h_fss_group is not null 
and 	h_fss_group <> 'U';

-- Done carefully to ensure uniqueness
insert into #financial_strategy_categories select 'U';

-- Now also need Onnet / Offnet which is just a binary flag:
select	convert(bit,1) as yesno
into 	#bit_categories;

insert into #bit_categories values (0);

commit;

-- Thing is, we also want to put all the segments we want into this table:
insert into sig_non_scaling_segments_lookup (
    value_segment
    ,MOSAIC_segment
    ,Financial_strategy_segment
    ,is_OnNet
    ,uses_sky_go
)
select	vs.value_seg
		,mc.h_mosaic_uk_group
		,fs.h_fss_group
		,onc.yesno
		,sg.yesno
from 	#Value_segment_categories              as vs
		inner join #mosaic_categories               as mc   
		on 1=1
		inner join #financial_strategy_categories   as fs   
		on 1=1
		inner join #bit_categories                  as onc  
		on 1=1  -- OnNet / OffNet categories
		inner join #bit_categories                  as sg   
		on 1=1  -- Sky Go use categories
;

-- 6272 segments. 

commit;
--go

-- For the name, we might eventually CASE these guys out so that the actual
-- Experian segment names get used rather than just the codes. But, not yet.

--go
update	sig_non_scaling_segments_lookup
set 	non_scaling_segment_name =	'(' || ltrim(rtrim(value_segment)) || ') - ('
									|| 	case ltrim(rtrim(MOSAIC_segment)) 
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
									|| 	case ltrim(rtrim(Financial_strategy_segment))
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
									|| 	case is_OnNet
											when 1 then 'OnNet'
											when 0 then 'OffNet'
										end || ') - ('
									|| 	case uses_sky_go
											when 1 then 'Uses Sky Go'
											when 0 then 'No Sky Go'
										end || ')';
commit;

-- Now it's populated, throw on the index that will help us join stuff:
create	unique index for_joining on sig_non_scaling_segments_lookup (value_segment, MOSAIC_segment, Financial_strategy_segment, is_OnNet, uses_sky_go);

-- And also for consistency / completeness, all ofd the names should be unique too:
create 	unique index name_checking on sig_non_scaling_segments_lookup (non_scaling_segment_name);

commit;


-----------------------------------
-- sig_current_non_scaling_segments
-----------------------------------

if object_id('sig_current_non_scaling_segments') is not null
	drop table sig_current_non_scaling_segments;
	
create table sig_current_non_scaling_segments (

    account_number                  varchar(20)	primary key
    ,non_scaling_segment_id         int
    ,value_segment                  varchar(10)
    ,consumerview_cb_row_id         bigint
    ,MOSAIC_segment                 varchar(1)
    ,Financial_strategy_segment		varchar(1)
    ,is_OnNet                       bit         default 0
    ,uses_sky_go                    bit         default 0
	
);

commit;

-- Index for bringing in the ID flag:
create hg index _for_joining    on sig_current_non_scaling_segments (value_segment, MOSAIC_segment, Financial_strategy_segment);
create hg index _for_updating   on sig_current_non_scaling_segments (consumerview_cb_row_id);
create hg index _for_joining2	on sig_current_non_scaling_segments (non_scaling_segment_id);
commit;
--go 


-----------------------------
-- m07_t1_box_base_stage0
-----------------------------

if object_id('m07_t1_box_base_stage0') is not null
	drop table m07_t1_box_base_stage0;
	
create table m07_t1_box_base_stage0	(

    account_number          varchar(20)     not null
    ,card_Subscriber_ID     varchar(10)     not null -- this should be the PK but not with current bug...
	,subscriber_ID          decimal(10)     default null
    ,service_instance_ID    varchar(30)     default null
    ,Adsmart_flag           bit             default 0
	,box_type_physical      varchar(20)     default null     
	,HD_box_physical        bit             default 0
    ,box_storage_capacity	varchar(20)     default null
    ,Box_model              varchar(20)     default null
    ,Box_is_3D              bit             default 0
    ,Box_has_anytime_plus   bit             default 0
	,PVR					smallint		default 0
	,PVR_type				varchar(10)		default null
	,description			varchar(50)		default null
);

commit;

create hg index m07hg1 on m07_t1_box_base_stage0( card_subscriber_id);
create hg index m07hg2 on m07_t1_box_base_stage0( subscriber_id);
create hg index m07hg3 on m07_t1_box_base_stage0( service_instance_ID);
create hg index m07hg4 on m07_t1_box_base_stage0( account_number);

commit;
--go 



-----------------------------
-- m08_t1_account_base_stage0
-----------------------------

if object_id('m08_t1_account_base_stage0') is not null
	drop table m08_t1_account_base_stage0;
	
create table m08_t1_account_base_stage0 (

	account_number			varchar(20)	primary key
	,viewing_consent_flag	varchar(1)	default null
	,Skygo_subs				bit			default 0
	,Anytime_plus_subs		bit			default 0
	,cb_key_individual		bigint		default null
	,Cust_active_DTV		bit			default 0
	,UK_Standard_account	varchar(3)	default null
	,box_type_subs			varchar(20)	default null
	,HD_box_subs			bit 		default 0
	,RTM					varchar(30)	default null
	,prem_sports			tinyint		default null
	,prem_movies			tinyint		default null
	,cust_active_dt			date		default null
	
);


commit;
--go 

---------------------
-- Setting Privileges
---------------------

--grant select on SIG_SBV_YYYYMMDD					to vespa_group_low_security;
--grant select on SIG_SINGLE_BOX_VIEW					to vespa_group_low_security;		
--grant select on SIG_SBV_HIST_VIEW					to vespa_group_low_security;
--grant select on SIG_SBV_HIST_SUMMARY				to vespa_group_low_security;
--grant select on SIG_SAV_YYYYMMDD					to vespa_group_low_security;
--grant select on SIG_SINGLE_ACCOUNT_VIEW				to vespa_group_low_security;
--grant select on SIG_SAV_HIST_VIEW					to vespa_group_low_security;
--grant select on SIG_SAV_HIST_SUMMARY				to vespa_group_low_security;
grant select on sig_non_scaling_segments_lookup		to vespa_group_low_security;
grant select on sig_current_non_scaling_segments	to vespa_group_low_security;
grant select on m01_t1_process_manager				to vespa_group_low_security;
grant select on m04_t1_panel_sample_stage0			to vespa_group_low_security;
grant select on m05_t1_panel_performance_stage0		to vespa_group_low_security;
grant select on m06_t1_panel_balance_stage0			to vespa_group_low_security;
grant select on m07_t1_box_base_stage0				to vespa_group_low_security;
grant select on m08_t1_account_base_stage0			to vespa_group_low_security; /*


                         $$$
                        I$$$
                        I$$$
               $$$$$$$$ I$$$    $$$$$      $$$ZDD    DDDDDDD.
             ,$$$$$$$$  I$$$   $$$$$$$    $$$ ODD  ODDDZ 7DDDD
             ?$$$,      I$$$ $$$$. $$$$  $$$= ODD  DDD     NDD
              $$$$$$$$= I$$$$$$$    $$$$.$$$  ODD +DD$     +DD$
                  :$$$$~I$$$ $$$$    $$$$$$   ODD  DDN     NDD.
               ,.   $$$+I$$$  $$$$    $$$$=   ODD  NDDN   NDDN
              $$$$$$$$$ I$$$   $$$$   .$$$    ODD   ZDDDDDDDN
                                      $$$      .      $DDZ
                                     $$$             ,NDDDDDDD
                                    $$$?

                      CUSTOMER INTELLIGENCE SERVICES

--------------------------------------------------------------------------------------------------------------
**Project Name:							OPS 2.0
**Analysts:                             Angel Donnarumma (angel.donnarumma_mirabel@skyiq.co.uk)
**Lead(s):                              Jose Loureda
**Stakeholder:                          Operational Reports / SIG
**Due Date:                             20/09/2013
**Project Code (Insight Collation):     
**Sharepoint Folder:                    
                                                                        
**Business Brief:

**Modules:

M01: MASVG Process Manager
        M01.0 - Initialising environment
		M01.1 - Housekeeping
        M01.2 - Identifying Pending Tasks
		M01.3 - Tasks Execution
        M01.4 - Returning results
		M01.5 - Setting Privileges

**Stats:

	-- running time: 1 sec...
	
--------------------------------------------------------------------------------------------------------------
*/

-----------------------------------
-- M01.0 - Initialising environment
-----------------------------------

create or replace procedure sig_masvg_m01_process_manager
	@fresh_start bit = 0
as begin

	MESSAGE cast(now() as timestamp)||' | Begining M01.0 - Initialising environment' TO CLIENT

	-- Local Variables
	declare @thetask    varchar(50)
	declare @sql_       varchar(2000)
	declare @exe_status	integer
	declare @log_id		bigint
	declare @gtg_flag	bit
	declare	@Module_id	varchar(3)
	
	set @Module_id = 'M01'
	set @exe_status = -1
	
	-- Initialising the project...
	execute @exe_status = sig_masvg_m02_base_initialisation @log_id output ,@gtg_flag output
	
	execute citeam.logger_add_event @log_ID, 1, @Module_id || ' : EXE M02', @exe_status
	
	MESSAGE cast(now() as timestamp)||' | @ M01.0: Logger instantiation DONE' TO CLIENT
	
	MESSAGE cast(now() as timestamp)||' | @ M01.0: Initialisation DONE' TO CLIENT
	
	-- if all went well and we have enough data, lets carry on then...
	if (@exe_status = 0 and @gtg_flag = 1)
	begin
	
-----------------------	
-- M01.1 - Housekeeping
-----------------------

		MESSAGE cast(now() as timestamp)||' | Begining M01.1 - Housekeeping' TO CLIENT
		
		execute sig_masvg_m03_housekeeping @fresh_start
		
		MESSAGE cast(now() as timestamp)||' | @ M01.0: Housekeeping DONE' TO CLIENT
		
------------------------------------
-- M01.2 - Identifying Pending Tasks
------------------------------------
		
		MESSAGE cast(now() as timestamp)||' | Begining M01.2 - Identifying Pending Tasks' TO CLIENT

		while exists    (
							select 	first status
							from	m01_t1_process_manager 
							where	status = 0			--> Any tasks Pending?...
						)
		begin
		
			MESSAGE cast(now() as timestamp)||' | @ M01.2: Pending Tasks Found' TO CLIENT

			-- What task to execute?...
			select  @thetask = task
			from    m01_t1_process_manager
			where   sequencer = (
									select  min(sequencer)
									from    m01_t1_process_manager
									where   status = 0
								)
			
			MESSAGE cast(now() as timestamp)||' | @ M01.2: Task '||@thetask||' Pending' TO CLIENT
			
--------------------------
-- M01.3 - Tasks Execution		
--------------------------

			MESSAGE cast(now() as timestamp)||' | Begining M01.3 - Tasks Execution for: '||@thetask TO CLIENT
			
			set @exe_status = -1
			
			set @sql_ = 'execute @exe_status = '||@thetask
			execute (@sql_)
			
			if @exe_status = 0
			begin
				update	m01_t1_process_manager
				set		status = 1
				where	task = @thetask
				and		status = 0
				
				MESSAGE cast(now() as timestamp)||' | @ M01.3: '||@thetask||' DONE' TO CLIENT
				
				commit
			end
			else
			begin
				MESSAGE cast(now() as timestamp)||' | @ M01.3: '||@thetask||' FAILED('||@exe_status||')' TO CLIENT
				execute citeam.logger_add_event @log_ID, 1, @Module_id || ' : EXE M04 (ERROR) T-' ||@thetaks , @exe_status
				break
			end
			execute citeam.logger_add_event @log_ID, 1, @Module_id || ' : EXE M01 T-' ||@thetask , @exe_status
			
		end
	end
	else
	begin
		MESSAGE cast(now() as timestamp)||' | M01 Finished (insufficient records on viewing table to proceed)' TO CLIENT
		execute citeam.logger_add_event @log_ID, 1, @Module_id || ' : insufficient records on viewing table to proceed' , @exe_status
	end

----------------------------
-- M01.4 - Returning results
----------------------------

	MESSAGE cast(now() as timestamp)||' | M01 Finished' TO CLIENT
	commit
	
end;

-----------------------------
-- M01.5 - Setting Privileges
-----------------------------

commit;
grant execute on sig_masvg_m01_process_manager to vespa_group_low_security;
commit; /*


                         $$$
                        I$$$
                        I$$$
               $$$$$$$$ I$$$    $$$$$      $$$ZDD    DDDDDDD.
             ,$$$$$$$$  I$$$   $$$$$$$    $$$ ODD  ODDDZ 7DDDD
             ?$$$,      I$$$ $$$$. $$$$  $$$= ODD  DDD     NDD
              $$$$$$$$= I$$$$$$$    $$$$.$$$  ODD +DD$     +DD$
                  :$$$$~I$$$ $$$$    $$$$$$   ODD  DDN     NDD.
               ,.   $$$+I$$$  $$$$    $$$$=   ODD  NDDN   NDDN
              $$$$$$$$$ I$$$   $$$$   .$$$    ODD   ZDDDDDDDN
                                      $$$      .      $DDZ
                                     $$$             ,NDDDDDDD
                                    $$$?

                      CUSTOMER INTELLIGENCE SERVICES

--------------------------------------------------------------------------------------------------------------
**Project Name:							OPS 2.0
**Analysts:                             Angel Donnarumma (angel.donnarumma_mirabel@skyiq.co.uk)
**Lead(s):                              Jose Loureda
**Stakeholder:                          Operational Reports / SIG
**Due Date:                             20/09/2013
**Project Code (Insight Collation):     
**Sharepoint Folder:                    
                                                                        
**Business Brief:

**Modules:

M02: MASVG Base Initialisation
		M02.0 - Initialize Variables
		M02.1 - Setting up the logger
		M02.2 - Verifying data completeness
		M02.3 - Returning Results
		M02.4 - Setting Privileges

**Stats:

	-- running time: 1 sec...
	
--------------------------------------------------------------------------------------------------------------
*/

-------------------------------
-- M02.0 - Initialize Variables
-------------------------------

create or replace procedure sig_masvg_m02_base_initialisation
	@log_id		bigint 	output
	,@gtg_flag 	bit 	output
as begin

	MESSAGE cast(now() as timestamp)||' | Begining M02.0 - Initialising environment' TO CLIENT
	
	-- Local Variables...
	declare @logbatch_id	varchar(20)
	declare @logrefres_id	varchar(40)
	declare @Module_id		varchar(3)
	declare @from_dt    	integer
	declare @to_dt      	integer
	
	set	@Module_id = 'M02'
	
	MESSAGE cast(now() as timestamp)||' | @ M02.0: Initialisation DONE' TO CLIENT
	
--------------------------------
-- M02.1 - Setting up the logger
--------------------------------

	MESSAGE cast(now() as timestamp)||' | Begining M02.1 - Setting up the logger' TO CLIENT
	
	-- Now automatically detecting if it's a test build and logging appropriately...
	
	if lower(user) = 'vespa_analysts'
		set @logbatch_id = 'OPS_2'
	else
		set @logbatch_id = 'OPS_2 test ' || upper(right(user,1)) || upper(left(user,2))

	set @logrefres_id = convert(varchar(10),today(),123) || ' OPS2 refresh'
	
	execute citeam.logger_create_run @logbatch_id, @logrefres_id, @log_ID output

	execute citeam.logger_add_event @log_ID, 3, @Module_id || ' : Log initialised'
	set @log_ID = 18
	MESSAGE cast(now() as timestamp)||' | @ M02.0: Logger instantiation DONE' TO CLIENT
	
--------------------------------------
-- M02.2 - Verifying data completeness
--------------------------------------

	MESSAGE cast(now() as timestamp)||' | Begining M02.2 - Verifying data completeness' TO CLIENT

	select  @from_dt =  cast    (
									(
										cast   (
													(   
														dateformat (
																		(
																			case   when datepart(weekday,now()) = 7
																					then now()
																					else (now() - datepart(weekday,now()))
																			end
																		)
																		,'YYYYMMDD'
																	)
													) 	as varchar(10)
												)
										||'00'
									) as integer
								)


	set @to_dt = @from_dt + 23

	select @gtg_flag = 	case	when	(
											select  count(1) as hits
											from    sk_prod.vespa_dp_prog_viewed_current
											where   dk_event_start_datehour_dim between @from_dt and @to_dt
										)
										> 1000000 -- this should be more... up to 10m 
										then 1 
										else 0
						end
	
	MESSAGE cast(now() as timestamp)||' | @ M02.2: Data Verification DONE' TO CLIENT
	
----------------------------
-- M02.3 - Returning Results
----------------------------
	
	commit
	MESSAGE cast(now() as timestamp)||' | M02 Finished' TO CLIENT
	
	
end;

-----------------------------
-- M02.4 - Setting Privileges
-----------------------------

commit;
grant execute on sig_masvg_m02_base_initialisation to vespa_group_low_security;
commit;
----------------------------------------------------------------- THE END... 
/*


                         $$$
                        I$$$
                        I$$$
               $$$$$$$$ I$$$    $$$$$      $$$ZDD    DDDDDDD.
             ,$$$$$$$$  I$$$   $$$$$$$    $$$ ODD  ODDDZ 7DDDD
             ?$$$,      I$$$ $$$$. $$$$  $$$= ODD  DDD     NDD
              $$$$$$$$= I$$$$$$$    $$$$.$$$  ODD +DD$     +DD$
                  :$$$$~I$$$ $$$$    $$$$$$   ODD  DDN     NDD.
               ,.   $$$+I$$$  $$$$    $$$$=   ODD  NDDN   NDDN
              $$$$$$$$$ I$$$   $$$$   .$$$    ODD   ZDDDDDDDN
                                      $$$      .      $DDZ
                                     $$$             ,NDDDDDDD
                                    $$$?

                      CUSTOMER INTELLIGENCE SERVICES

--------------------------------------------------------------------------------------------------------------
**Project Name:							OPS 2.0
**Analysts:                             Angel Donnarumma (angel.donnarumma_mirabel@skyiq.co.uk)
**Lead(s):                              Jose Loureda
**Stakeholder:                          Operational Reports / SIG
**Due Date:                             20/09/2013
**Project Code (Insight Collation):     
**Sharepoint Folder:                    
                                                                        
**Business Brief:

**Modules:

M03: MASVG Panel Composition
        M03.0 - Initialising environment
        M03.1 - Cleaning Before Input (CBI)
		M03.2 - Cleaning After Output (CAO)
		M03.3 - Returning results
		M03.4 - Setting Privileges

--------------------------------------------------------------------------------------------------------------
*/
-----------------------------------
-- M03.0 - Initialising environment
-----------------------------------

create or replace procedure sig_masvg_m03_housekeeping
	@reset	bit	= 0
as begin

	MESSAGE cast(now() as timestamp)||' | Begining M03.0 - Initialising environment' TO CLIENT
	
	declare @done 		tinyint
	declare @alltasks 	tinyint
	
	select	@alltasks = count(1)
	from	m01_t1_process_manager
	
	select	@done = count(1)
	from	m01_t1_process_manager
	where	status = 1
	
	MESSAGE cast(now() as timestamp)||' | @ M03.0: Initialisation DONE' TO CLIENT
	
--------------------------------------
-- M03.1 - Cleaning Before Input (CBI)
--------------------------------------

	if	(@reset = 1 or @alltasks = @done)
		begin
			
			MESSAGE cast(now() as timestamp)||' | Begining M03.1 - Cleaning Before Imput (CBI)' TO CLIENT
			
			delete from m04_t1_panel_sample_stage0
			delete from	m05_t1_panel_performance_stage0
			delete from m06_t1_panel_balance_stage0
			delete from m07_t1_box_base_stage0
			delete from m08_t1_account_base_stage0
			delete from sig_current_non_scaling_segments
			
			MESSAGE cast(now() as timestamp)||' | @ M03.1: Trunkating output tables DONE' TO CLIENT
			commit
			
			Update 	m01_t1_process_manager
			set		status = 0
				
			MESSAGE cast(now() as timestamp)||' | @ M03.1: All tasks completed, Re-initialising DONE' TO CLIENT
			commit
			
		end


--------------------------------------
-- M03.2 - Cleaning After Output (CAO)
--------------------------------------
	--else
		--begin
		
			--MESSAGE cast(now() as timestamp)||' | Begining M03.0 - Cleaning After Output (CAO)' TO CLIENT
		
			--Update 	m01_t1_process_manager
			--set		status = 0
			
			--MESSAGE cast(now() as timestamp)||' | @ M03.0: Forcing Re-initialisation DONE' TO CLIENT
			--commit
			
		--end

	commit
		
----------------------------		
-- M03.3 - Returning results
----------------------------

	MESSAGE cast(now() as timestamp)||' | M03 Finished' TO CLIENT
	commit

end;

-----------------------------
-- M03.4 - Setting Privileges
-----------------------------

commit;
grant execute on sig_masvg_m03_housekeeping to vespa_group_low_security;
commit; /*


                         $$$
                        I$$$
                        I$$$
               $$$$$$$$ I$$$    $$$$$      $$$ZDD    DDDDDDD.
             ,$$$$$$$$  I$$$   $$$$$$$    $$$ ODD  ODDDZ 7DDDD
             ?$$$,      I$$$ $$$$. $$$$  $$$= ODD  DDD     NDD
              $$$$$$$$= I$$$$$$$    $$$$.$$$  ODD +DD$     +DD$
                  :$$$$~I$$$ $$$$    $$$$$$   ODD  DDN     NDD.
               ,.   $$$+I$$$  $$$$    $$$$=   ODD  NDDN   NDDN
              $$$$$$$$$ I$$$   $$$$   .$$$    ODD   ZDDDDDDDN
                                      $$$      .      $DDZ
                                     $$$             ,NDDDDDDD
                                    $$$?

                      CUSTOMER INTELLIGENCE SERVICES

--------------------------------------------------------------------------------------------------------------
**Project Name:							OPS 2.0
**Analysts:                             Angel Donnarumma (angel.donnarumma_mirabel@skyiq.co.uk)
**Lead(s):                              Jose Loureda
**Stakeholder:                          Operational Reports / SIG
**Due Date:                             20/09/2013
**Project Code (Insight Collation):     
**Sharepoint Folder:                    
                                                                        
**Business Brief:
	
	This unit is to compile in a single table all relevant derivations/aggregations to the composition of the panel,
	which will be used later on to assemble both views (box and account)...

**Modules:

M04: MASVG Panel Composition
        M04.0 - Initialising environment
        M04.1 - Building Blocks for Panel Composition
		M04.2 - Assembling Panel snapshot
        M04.3 - QAing results
        M04.4 - Returning results

**Stats:

	-- running time: 8 min approx...
	
--------------------------------------------------------------------------------------------------------------
*/


-----------------------------------
-- M04.0 - Initialising environment
-----------------------------------

create or replace procedure sig_masvg_m04_panel_composition
as begin
	
	MESSAGE cast(now() as timestamp)||' | Beginig M04.0 - Initialising environment' TO CLIENT
	
    declare	@profiling_day	date

	select @profiling_day = max(cb_data_date) from sk_prod.cust_single_account_view
	
	MESSAGE cast(now() as timestamp)||' | @ M04.0: Initialisation DONE' TO CLIENT
	
	------------------------------------------------
	-- M04.1 - Building Blocks for Panel Composition
	------------------------------------------------
	
	MESSAGE cast(now() as timestamp)||' | Beginig M04.1 - Building Blocks for Panel Composition' TO CLIENT
	
	-- 0) 	Collecting a list of boxes from the Panel Disablement/enablement Campaigns
	--		meaning boxes that will be off/ are still on the Panel...


	select  base.account_number
			,base.card_subscriber_id
			,base.cell_name
			,base.writeback_datetime
	into	#from_campaigns		
	from    (
				select  account_number      
						,card_subscriber_id
						,cell_name
						,writeback_datetime
						,rank() over (partition by card_subscriber_id order by writeback_datetime desc) as most_recent
				from 	sk_prod.campaign_history_cust  a
						inner join sk_prod.campaign_history_lookup_cust   b
						on	a.cell_id = b.cell_id
				where 	(cell_name like 'Vespa Disablement%' or cell_name like 'Vespa Enablement%')
				and 	writeback_datetime >= cast('2011-10-01' as datetime)
			)   as base
			inner join sk_prod.cust_card_subscriber_link as ccsl
			on  base.card_subscriber_id = ccsl.card_subscriber_id
			and base.account_number = ccsl.account_number
			and ccsl.current_flag = 'Y'
	where   base.most_recent = 1    -- removing duplicates...
	and     cell_name not like 'Vespa_Disablement%'
																

	commit
	
	MESSAGE cast(now() as timestamp)||' | @ M04.1: list of boxes from campaigns DONE' TO CLIENT
	--[ NFQA ]


	-- 1)	Getting any box that is seating in the panel snapshot table...

	select  distinct 
			account_number
			,card_subscriber_id
			,panel_no
			,result
			,request_dt
			,created_dt
	into	#from_vss
	from    sk_prod.VESPA_SUBSCRIBER_STATUS

	commit
	MESSAGE cast(now() as timestamp)||' | @ M04.1: list of boxes from Subscriber Status DONE' TO CLIENT
	
	--[ NFQA ]


	-- 2)	Getting any box in the dialback table (this one shows who dialled back and when)...

	select  account_number
			,card_subscriber_id
	into	#from_summary
	from    (
				select  distinct
						convert(varchar(12),account_number) as account_number
						,right(replicate('0',8) || convert(varchar(20), subscriber_id), 8) as card_subscriber_id
				from    sk_prod.vespa_stb_log_summary
				where   account_number is not null
			)   as base

	commit
	MESSAGE cast(now() as timestamp)||' | @ M04.1: list of boxes from Log Summary DONE' TO CLIENT

	--[ NFQA ]


	-- 3)	Complementing the sample with active boxes that are in the Sky Panel...

	select	csl.account_number
			,csl.card_subscriber_id
			--,csl.cb_change_date
	into	#from_skypanel
	from 	sk_prod.cust_card_subscriber_link as csl
			inner join  (
							select  distinct account_number
							from    sk_prod.vespa_sky_view_panel
						)   as sva
			on	csl.account_number = sva.account_number
	where 	current_flag = 'Y'
	and 	@profiling_day between effective_from_dt and effective_to_dt -- 135229


	commit
	MESSAGE cast(now() as timestamp)||' | @ M04.1: list of boxes from Subscriber Link DONE' TO CLIENT
	
	-- [ NFQA ]


	-- 4)	Complementing the sample with confirmed list of Sky View panel seleted boxes...

	select 	account_number
			,card_subscriber_id
			,load_date
	into	#from_skymembers
	from 	vespa_analysts.verified_Sky_View_members
	where 	account_number is not null

	commit
	MESSAGE cast(now() as timestamp)||' | @ M04.1: list of boxes from SkyView DONE' TO CLIENT

	-- [ NFQA ]


	------------------------------------
	-- M04.2 - Assembling Panel snapshot
	------------------------------------
	
	MESSAGE cast(now() as timestamp)||' | Begining M04.2 - Assembling Panel snapshot' TO CLIENT
	
	-- getting into the table everyone in the panel...
	truncate table m04_t1_panel_sample_stage0
	commit
	
	insert	into m04_t1_panel_sample_stage0	(	
												account_number
												,card_subscriber_id
												,subscriber_id
											)
	select	distinct
			account_number
			,card_subscriber_id
			,convert(decimal(10), card_subscriber_id) as box
	from	#from_campaigns
	where	box is not null
	union
	select	distinct
			account_number
			,card_subscriber_id
			,convert(decimal(10), card_subscriber_id) as box
	from	#from_vss
	where	box is not null
	union
	select	distinct
			account_number
			,card_subscriber_id
			,convert(decimal(10), card_subscriber_id) as box
	from	#from_summary
	where	box is not null
	union
	select	distinct
			account_number
			,card_subscriber_id
			,convert(decimal(10), card_subscriber_id) as box
	from	#from_skypanel
	where	box is not null
	union
	select	distinct
			account_number
			,card_subscriber_id
			,convert(decimal(10), card_subscriber_id) as box
	from	#from_skymembers
	where	box is not null

	commit
	MESSAGE cast(now() as timestamp)||' | @ M04.2: Listing Panel Universe DONE' TO CLIENT

	-- Now deriving the Panel Composition fields we need on this section

	-- Panel [DONE]
		 
	update  m04_t1_panel_sample_stage0
	set     panel               =   case    when vss.panel_no = 12 then    'VESPA'
											when vss.panel_no = 6 then     'ALT6'
											when vss.panel_no = 7 then     'ALT7'
											when vss.panel_no = 5 then     'ALT5'
											when vss.panel_no = 11 then    'VESPA11'
									end
			,Status_Vespa       = vss.result
			,vss_request_dt     = case when vss.result = 'Enabled' then convert(date, vss.request_dt) else null end
			,vss_created_date   = case when vss.result = 'Enabled' then convert(date, vss.created_dt) else null end
	from    m04_t1_panel_sample_stage0  as base
			inner join #from_vss        as vss
			on  base.card_subscriber_id = vss.card_subscriber_id

	commit
	MESSAGE cast(now() as timestamp)||' | @ M04.2: Deriving Panel field DONE' TO CLIENT
	
	---------------------------------------------------------------------------------------------------------------------

	-- sky candidate , sky selected , Sky_View_load_date [DONE]

	update  m04_t1_panel_sample_stage0
	set     Is_Sky_View_candidate   = 1
			,Sky_View_load_date     = vsvp.cb_change_date -- This will get overwritten for verified members, but that's okay & intended
	from    m04_t1_panel_sample_stage0              as base
			inner join sk_prod.vespa_sky_view_panel as vsvp
			on  base.account_number = vsvp.account_number
		 
	update	m04_t1_panel_sample_stage0
	set		Is_Sky_View_Selected = 1
			,Sky_View_load_date  = vsvm.load_date
	from	m04_t1_panel_sample_stage0	as base
			inner join vespa_analysts.verified_Sky_View_members as vsvm
			on	base.subscriber_id = vsvm.subscriber_id

	commit
	MESSAGE cast(now() as timestamp)||' | @ M04.2: Deriving Sky candidate, selected and load date DONE' TO CLIENT
	---------------------------------------------------------------------------------------------------------------------	
	 
	-- historic_result_date [DONE] 

    -- Since sybase does not support RANKING on updates, breaking down the logic into to two steps...
    -- we need to get here the most recent boxes per accounts...
    select	*
    into    #temp_shelf
    from	(
	            select	account_number
				        ,card_subscriber_id
				        ,result
						,coalesce(request_dt, modified_dt) as request_dt
						,rank() over (partition by account_number, card_subscriber_id order by request_dt desc, modified_dt desc, created_dt desc) as most_recent
				from 	sk_prod.vespa_subscriber_status_hist
				where 	result in ('Enabled', 'Disabled')
			)	as base
	where	most_recent = 1
	and		result <> 'Disable'

    commit
    create hg index fake_hg1 on #temp_shelf(account_number)
    create hg index fake_hg2 on #temp_shelf(card_subscriber_id)
    commit

	update  m04_t1_panel_sample_stage0
	set     historic_result_date = convert(date, pe.request_dt)
	from    m04_t1_panel_sample_stage0  as base
			inner join	#temp_shelf 	as pe
			on  base.card_subscriber_id = pe.card_subscriber_id
			and base.account_number = pe.account_number	 
			
	commit
    drop table #temp_shelf
    commit
	MESSAGE cast(now() as timestamp)||' | @ M04.2: Deriving Historic Results DONE' TO CLIENT
	---------------------------------------------------------------------------------------------------------------------

	-- selection_date [DONE] 

	update	m04_t1_panel_sample_stage0
	set 	Panel_ID_4_cells_confirm = 1            
			,Selection_date = tvb.writeback_datetime
	from 	m04_t1_panel_sample_stage0	as base
			inner join #from_campaigns	as tvb
			on	base.card_subscriber_id = tvb.card_subscriber_id

	commit

	 -- So now creating from the subscriber history table a list of boxes whose last status was panel 12 (regardless they are disabled or enabled)
	 -- to get last date of writeback_datetime value (panel_id derivation is done further in the code)...

	select	n.*
			,vssh2.panel_no
	into    #subs_hist_vespa_boxes
	from    (
				select	base.card_subscriber_id
						,max(vssh.writeback_datetime) as writeback_datetime
				from    m04_t1_panel_sample_stage0 as base
						inner join sk_prod.vespa_subscriber_status_hist as vssh
						on	base.card_subscriber_id = vssh.card_subscriber_id
				group   by  base.card_subscriber_id
			) 	as n
			inner join sk_prod.vespa_subscriber_status_hist as vssh2
			on	n.card_subscriber_id = vssh2.card_subscriber_id
			and n.writeback_datetime = vssh2.writeback_datetime
			and	vssh2.panel_no in (12,11)


	update  m04_t1_panel_sample_stage0
	set     Panel_ID_4_cells_confirm = 1
			,Selection_date = shvb.writeback_datetime
	from    m04_t1_panel_sample_stage0 as base
			inner join #subs_hist_vespa_boxes as shvb
			on base.card_subscriber_id = shvb.card_subscriber_id
	where   base.selection_date is null
		
	commit
	MESSAGE cast(now() as timestamp)||' | @ M04.2: Deriving Selection Date DONE' TO CLIENT
	---------------------------------------------------------------------------------------------------------------------
		
	-- ps_olive [DONE]

	select	b.service_instance_id
			,convert(integer,min(si_external_identifier)) as subscriber_id
			,convert(bit, max(case when si_service_instance_type = 'Primary DTV' then 1 else 0 end)) as primary_box
			,convert(bit, max(case when si_service_instance_type = 'Secondary DTV (extra digiboxes)' then 1 else 0 end)) as secondary_box
	into 	#subscriber_details
	from 	sk_prod.CUST_SERVICE_INSTANCE as b
			inner join m04_t1_panel_sample_stage0 as base
			on	base.card_subscriber_id = b.si_external_identifier
	where 	si_service_instance_type in ('Primary DTV','Secondary DTV (extra digiboxes)')
	and 	@profiling_day between effective_from_dt and effective_to_dt
	group 	by	b.service_instance_id

	commit
	create index for_stuff on #subscriber_details (subscriber_id)
	commit

	-- Then push those box types onto the subscriber level summary
	update	m04_t1_panel_sample_stage0
	set		PS_Olive = case	when b.subscriber_id is null then 'U'
								when b.primary_box = 1 and secondary_box = 0 then 'P'
								when b.primary_box = 0 and secondary_box = 1 then 'S'
								else '?' 
						end
	from 	m04_t1_panel_sample_stage0 			as base
			left outer join #subscriber_details as b
			on base.subscriber_id = b.subscriber_id
		 
	commit	
	MESSAGE cast(now() as timestamp)||' | @ M04.2: Deriving PS Olive DONE' TO CLIENT
	---------------------------------------------------------------------------------------------------------------------
		 
	-- ps_vespa [DONE]

    --
    select  distinct
            si_external_identifier
            ,si_service_instance_type
    into   #unique_box_list
    from    (
                select	csi.si_external_identifier
            	        ,rank() over(partition by csi.si_external_identifier order by csi.effective_from_dt desc) as rank_
            			,csi.si_service_instance_type
            	from 	sk_prod.cust_service_instance           as csi
                        inner join m04_t1_panel_sample_stage0   as m04
                        on	m04.card_subscriber_id = CSI.si_external_identifier
            	where 	csi.si_service_instance_type like '%DTV%'
            )   as listing
    where   rank_ = 1 

    commit
    create unique index fake_key on #unique_box_list(si_external_identifier)
    commit

	update	m04_t1_panel_sample_stage0
	set 	PS_Vespa = left(csi.si_service_instance_type,1)
	from 	m04_t1_panel_sample_stage0      as base
			inner join	#unique_box_list    as CSI 
			on	base.card_subscriber_id = CSI.si_external_identifier
			
	commit		
    drop table #unique_box_list
    commit
	MESSAGE cast(now() as timestamp)||' | @ M04.2: Deriving PS Vespa DONE' TO CLIENT
	
	---------------------------------------------------------------------------------------------------------------------
				
	-- inferred flag [DONE]

	select	account_number
			,convert(bit, 0) as has_MR
	into 	#maybe_single_households
	from 	m04_t1_panel_sample_stage0
	group 	by	account_number
	having 	count(1) = 1 
			and sum(case when PS_Vespa = 'U' and PS_Olive = 'U' then 1 else 0 end)=1

	-- So this should get us to a pretty concise population that shouldn't take too long to process...

	commit
	create unique index fake_pk on #maybe_single_households (account_number)
	commit

	-- ok, so now let's figure out which have MR and which have multiple associated boxes...
	update	#maybe_single_households
	set 	has_MR = 1
	from 	#maybe_single_households					as hh
			inner join sk_prod.cust_single_account_view as csh
			on	hh.account_number = csh.account_number
	where 	prod_active_multiroom = 1

	commit

	-- Now we have the marks, put them back on SBV; can just join by account number, by
	-- construction these are households with only one box

	update	m04_t1_panel_sample_stage0
	set 	PS_inferred_primary = 1
	from 	m04_t1_panel_sample_stage0			as base
			inner join #maybe_single_households as msh
			on 	base.account_number = msh.account_number
	where 	msh.has_MR = 0


	commit
	MESSAGE cast(now() as timestamp)||' | @ M04.2: Deriving Infered Flag DONE' TO CLIENT
	---------------------------------------------------------------------------------------------------------------------

	-- ps_flag / ps_source [DONE]

	update	m04_t1_panel_sample_stage0
	set		PS_flag = 	case	when PS_Olive = PS_Vespa and PS_Olive <> 'U' then PS_Olive
								when PS_inferred_primary = 1 then 'P' 						-- Only populated for the questionable boxes
								when PS_Olive = 'U' or PS_Olive is null then PS_Vespa
								when PS_Vespa = 'U' or PS_Vespa is null then PS_Olive
								else '!'    												-- this should only leave the case where one of Olive / Vespa says 'P' and the other says 'S'
						end
			,PS_source =	case	when PS_Olive = PS_Vespa and PS_Olive <> 'U' then 'Both agree'
									when PS_inferred_primary = 1 then 'Inferred'
									when PS_Vespa = 'U' or PS_Vespa is null then 'Olive'
									when PS_Olive = 'U' or PS_Olive is null then 'Vespa'
									else 'Collision!'
							end

	commit
	MESSAGE cast(now() as timestamp)||' | @ M04.2: Deriving PS Flag and Source DONE' TO CLIENT
	
	---------------------------------------------------------------------------------------------------------------------	   
		   
	--  enablement date , enablement source [DONE] 
	update	m04_t1_panel_sample_stage0
	set 	Enablement_date =	case	when vss_request_dt         is not null and Status_Vespa = 'Enabled'    then vss_request_dt         -- If the box doesn't say 'Enabled' then there should be a historic enablement to fall back with
										when Sky_View_load_date     is not null                                 then Sky_View_load_date
										when historic_result_date   is not null                                 then historic_result_date
										when Selection_date         is not null                                 then Selection_date
										when vss_created_date       is not null                                 then vss_created_date
								end
			,Enablement_date_source =	case	when vss_request_dt         is not null and Status_Vespa = 'Enabled'    then 'vss_request_dt'
												when Sky_View_load_date     is not null                                 then 'Sky View'
												when historic_result_date   is not null                                 then 'historic'
												when Selection_date         is not null                                 then 'writeback'
												when vss_created_date       is not null                                 then 'vss_created_dt'
										end
		
	commit
	MESSAGE cast(now() as timestamp)||' | @ M04.2: Deriving enablement Date and Source DONE' TO CLIENT
	
	---------------------------------------------------------------------------------------------------------------------
		
	-----------------------
	-- M04.3 - QAing results
	-----------------------

	-- [ NFQA ]
	/*
	select  card_subscriber_id
			,count(1)
	from    m04_t1_panel_sample_stage0
	group   by  card_subscriber_id
	having  count(1) >1
	*/

	----------------------------
	-- M04.4 - Returning results
	----------------------------

	-- ... m04_t1_panel_sample_stage0
	MESSAGE cast(now() as timestamp)||' | M04 Finished, table m04_t1_panel_sample_stage0 BUILT' TO CLIENT


    commit


end;

commit;
grant execute on sig_masvg_m04_panel_composition to vespa_group_low_security;
commit;

/*
STATS:

RUNNING TIME: c. 8 min
*/ /*


                         $$$
                        I$$$
                        I$$$
               $$$$$$$$ I$$$    $$$$$      $$$ZDD    DDDDDDD.
             ,$$$$$$$$  I$$$   $$$$$$$    $$$ ODD  ODDDZ 7DDDD
             ?$$$,      I$$$ $$$$. $$$$  $$$= ODD  DDD     NDD
              $$$$$$$$= I$$$$$$$    $$$$.$$$  ODD +DD$     +DD$
                  :$$$$~I$$$ $$$$    $$$$$$   ODD  DDN     NDD.
               ,.   $$$+I$$$  $$$$    $$$$=   ODD  NDDN   NDDN
              $$$$$$$$$ I$$$   $$$$   .$$$    ODD   ZDDDDDDDN
                                      $$$      .      $DDZ
                                     $$$             ,NDDDDDDD
                                    $$$?

                      CUSTOMER INTELLIGENCE SERVICES

--------------------------------------------------------------------------------------------------------------
**Project Name:							OPS 2.0
**Analysts:                             Angel Donnarumma (angel.donnarumma_mirabel@skyiq.co.uk)
**Lead(s):                              Jose Loureda
**Stakeholder:                          Operational Reports / SIG
**Due Date:                             20/09/2013
**Project Code (Insight Collation):     
**Sharepoint Folder:                    
                                                                        
**Business Brief:

**Modules:

M05: MASVG Panel Composition
        M05.0 - Initialising environment
        M05.1 - Deriving Metrics for Panel Performance on DP
				-- Num_logs_sent_30d
				-- reporting_quality
				-- Num_logs_sent_7d
				-- Continued_trans_30d
				-- Continued_trans_7d
				-- Reporting_performance
				-- avg_reporting_quality
				-- min_reporting_quality
				-- avg_events_in_logs
				-- total_calls_in_30d
		M05.2 - Deriving Metrics for Panel Performance on AP
				-- Num_logs_sent_30d
				-- reporting_quality
				-- Reporting_performance
				-- avg_reporting_quality
				-- min_reporting_quality
		M05.3 - General Panel Performance KPIs
				-- return_data_7d
				-- return_data_30d
				-- reporting_quality_s
        M05.4 - QAing results
        M05.5 - Returning results

**Stats:

	-- running time: 18 min approx...
	
--------------------------------------------------------------------------------------------------------------
*/

-----------------------------------
-- M05.0 - Initialising environment
-----------------------------------

create or replace procedure sig_masvg_m05_panel_performance
as begin


if object_id ('M04_t1_panel_sample_stage0') is not null
begin
    
    delete from m05_t1_panel_performance_stage0
    commit
    
	MESSAGE cast(now() as timestamp)||' | Beginig M05.0 - Initialising environment' TO CLIENT
	
	-- VARIABLES
	
	--declare @last_full_date		date
	declare @from_dt			date
	declare	@to_dt				date
	declare @event_from_date 	integer
	declare @event_to_date      integer
	declare @profiling_day		date
	
	--execute vespa_analysts.Regulars_Get_report_end_date @last_full_date output								-- YYYY-MM-DD
	select @profiling_day 	= max(cb_data_date) from sk_prod.cust_single_account_view
	
	set @to_dt 				= @profiling_day 																-- YYYY-MM-DD
	set @from_dt 			= @profiling_day -60															-- YYYY-MM-DD
	set @event_from_date    = convert(integer,dateformat(dateadd(day, -60, @profiling_day),'yyyymmddhh'))	-- YYYYMMDD00
	set @event_to_date      = convert(integer,dateformat(@profiling_day,'yyyymmdd')+'23')	                -- YYYYMMDD23
	
	
	
	-- Getting everyone in the panel into the performance table to measure them...
	insert	into m05_t1_panel_performance_stage0	(
														account_number
														,subscriber_id
													)
	select	distinct
			account_number
			,subscriber_id
	from	M04_t1_panel_sample_stage0
		
	commit
	
	-- Constructing the base to measure...	
	-- Sampling the period of interest to derived the panel performance metrics we need...
	select	subscriber_id
			,convert(date, dateadd(hh, -6, log_received_start_date_time_utc))		as log_received_date
			,min(dateadd(hour,1, EVENT_START_DATE_TIME_UTC))						as first_event_mark
			,max(dateadd(hour,1, EVENT_END_DATE_TIME_UTC))							as last_event_mark
			,count(1)																as num_events_in_logs
			,datepart(hh, min(dateadd(hour,1, LOG_RECEIVED_START_DATE_TIME_UTC)))	as hour_received
	into	#measure_base_stage_1
	from   	sk_prod.VESPA_DP_PROG_VIEWED_CURRENT
	where  	panel_id in (12,11)
	and     dk_event_start_datehour_dim between @event_from_date and @event_to_date
	and     LOG_RECEIVED_START_DATE_TIME_UTC is not null
	and     LOG_START_DATE_TIME_UTC is not null
	and     subscriber_id > 0 -- to avoid nulls and -1...
	group 	by	subscriber_id
				,log_received_start_date_time_utc
	having 	log_received_start_date_time_utc is not null	
	
	commit	
		
	delete from #measure_base_stage_1 where	log_received_date > @profiling_day
	
	commit -- > 12 min up to here... is too much...
	MESSAGE cast(now() as timestamp)||' | @ M05.0: Sampling from the Viewing Tables DONE' TO CLIENT
	
	-- Preparing the data to slice the 30 days stats...
	
	if object_id ('measure_base_stage_2') is not null
		drop table measure_base_stage_2
		
	commit
	
	select	subscriber_id
			,log_received_date			as log_date
			,count(1) 					as num_of_calls
			,min(first_event_mark)		as first_event_mark
			,max(last_event_mark)		as last_event_mark
			,sum(num_events_in_logs)	as size_of_logs
			,min(hour_received)			as hour_received
	into	measure_base_stage_2
	from 	#measure_base_stage_1
	group 	by 	subscriber_id
				,log_date

	commit
	create hg index fake_hg1 on measure_base_stage_2(subscriber_id)
	commit
	MESSAGE cast(now() as timestamp)||' | @ M05.0: Preparing data for slicing 30days stats DONE' TO CLIENT
	
	
	-- Preparing the data to slice the 7 days stats...
	
	if object_id ('measure_base_stage_3') is not null
		drop table measure_base_stage_3
		
	commit
	
	select	subscriber_id
			,convert(date, log_received_date)	as log_date
			,count(1) 							as num_of_calls
			,min(first_event_mark)				as first_event_mark
			,max(last_event_mark)				as last_event_mark
			,sum(num_events_in_logs)			as size_of_logs
			,min(hour_received)					as hour_received
	into	measure_base_stage_3
	from 	(
				select	*
				from	#measure_base_stage_1
				where	log_received_date+7> @profiling_day
                and     subscriber_id > 0
			)	as stage_3
	group 	by 	subscriber_id
				,log_date
	
	commit --> 12 mins up to here... is too much
	create hg index fake_hg1 on measure_base_stage_3(subscriber_id)
	commit
	MESSAGE cast(now() as timestamp)||' | @ M05.0: Preparing data for slicing 7days stats DONE' TO CLIENT
	
	drop table #measure_base_stage_1
	
	commit
	MESSAGE cast(now() as timestamp)||' | @ M05.0: Initialisation DONE' TO CLIENT
	
	-- [ NFQA ]
		
-------------------------------------------------------
-- M05.1 - Deriving Metrics for Panel Performance on DP
-------------------------------------------------------

	MESSAGE cast(now() as timestamp)||' | Beginig M05.1 - Deriving Metrics for Panel Performance on DP' TO CLIENT

	-- Num_logs_sent_30d
	-- reporting_quality
	-- avg_events_in_logs
	-- total_calls_in_30d

	select	subscriber_id
			,min(log_date) as start_scan
	into	#first_reports 
	from	measure_base_stage_2
	group	by subscriber_id
	
	update	#first_reports
    set 	start_scan = 	case	when start_scan > dateadd(day, -30, @profiling_day) then start_scan
									else dateadd(day, -29, @profiling_day)
							end

	update	#first_reports
    set 	start_scan =	case	when start_scan > enablement_date then start_scan
									else enablement_date
							end
    from 	m04_t1_panel_sample_stage0	as m04
    where	#first_reports.subscriber_id = m04.subscriber_id						
			
	delete from #first_reports where   start_scan > @profiling_day
	commit
	
	-- Since grouping is not supported when updating a table in Sybase, temp table it is...		
	select  base.subscriber_id
			,count(distinct (case when base.log_date > dateadd(day,-30,@profiling_day) then base.log_Date else null end))   as logs_in_30d
			,case when max(panel_sample.panel) in('VESPA','VESPA11','ALT5')	then convert(float, datediff(day, min(start_scan), @profiling_day)+1)         end as full_dividend
			,round((case when max(panel_sample.panel) in ('ALT6','ALT7')    then ((convert(float, datediff(day, min(start_scan), @profiling_day)+1))/2)   end),0) as alte_dividend
			,case   when max(panel_sample.panel) in ('VESPA','VESPA11','ALT5')
						then cast(logs_in_30d as float)/full_dividend
					when max(panel_sample.panel) in ('ALT6','ALT7')
						then cast(logs_in_30d as float)/alte_dividend
			end     as reporting_quality
			,sum(base.num_of_calls)         as total_calls_in_30d --> this is a potential metric we could start using in the future...
			,round(avg(size_of_logs),2) as avg_events_in_logs
	into	#temp_shelf
	from    measure_base_stage_2                    as base
			inner join #first_reports				as fr
			on	base.subscriber_id = fr.subscriber_id
			left join m04_t1_panel_sample_stage0    as panel_sample
			on  base.subscriber_id = panel_sample.subscriber_id
	group   by  base.subscriber_id


	update	m05_t1_panel_performance_stage0
	set		num_logs_sent_30d = shelf.logs_in_30d
			,reporting_quality =	shelf.reporting_quality
			,avg_events_in_logs = shelf.avg_events_in_logs
			,total_calls_in_30d = shelf.total_calls_in_30d
	from	m05_t1_panel_performance_stage0	as m05
			inner join	#temp_shelf	as shelf
			on	m05.subscriber_id = shelf.subscriber_id

	drop table #temp_shelf
	drop table #first_reports

	commit --> 26 secs
	MESSAGE cast(now() as timestamp)||' | @ M05.1: Deriving Logs sent in 30d, Rep Qual, size of logs and total calls DONE' TO CLIENT
	
	
	-- Num_logs_sent_7d

	-- Since grouping is not supported when updating a table in Sybase, temp table it is...		
	select  subscriber_id
			,count(distinct log_date)	as logs_in_7d
			,sum(num_of_calls)         	as total_calls_in_7d --> this is a potential metric we could start using in the future...
			,round(avg(size_of_logs),2) as avg_events_in_logs
	into	#temp_shelf
	from    measure_base_stage_3
	group   by  subscriber_id

	update	m05_t1_panel_performance_stage0
	set		num_logs_sent_7d = shelf.logs_in_7d
	from	m05_t1_panel_performance_stage0	as m05
			inner join	#temp_shelf	as shelf
			on	m05.subscriber_id = shelf.subscriber_id

	drop table #temp_shelf

	commit
	MESSAGE cast(now() as timestamp)||' | @ M05.1: Deriving logs sent in 7d DONE' TO CLIENT
			
	-- Continued_trans_30d

	-- To get the intervales we first want to get at what date did each interval starts and ends so we can do a date diff 
	-- and hence the interval lenght...

	-- getting all starting dates of the intervals...
	select	r.subscriber_id
			,r.log_date
			,rank() over (partition by r.subscriber_id order by r.log_date) as interval_sequencer
	into 	#intervals_starts
	from 	measure_base_stage_2 as l
			right join measure_base_stage_2 as r
			on	l.subscriber_id = r.subscriber_id
			and	l.log_date+1 = r.log_date
	where 	l.subscriber_id is null

	-- getting all ending dates of the intervals...
	select	l.subscriber_id
			,l.log_date
			,rank() over (partition by l.subscriber_id order by l.log_date) as interval_sequencer
	into 	#intervals_ends    
	from 	measure_base_stage_2 as l
			left join measure_base_stage_2 as r
			on	l.subscriber_id = r.subscriber_id
			and l.log_date+1 = r.log_date
	where 	r.subscriber_id is null

	-- per box, what was the max interval it had on last 30 days...
	select  l.subscriber_id
			,max(datediff(day, l.log_date, r.log_date) +1) as interval_length
	into	#temp_shelf
	from 	#intervals_starts as l
			inner join #intervals_ends as r
			on	l.subscriber_id = r.subscriber_id
			and	l.interval_sequencer = r.interval_sequencer
	group   by  l.subscriber_id

	/* -> POTENTIAL FOR A NEW PANEL PERFORMANCE TABLE <- */

	-- Updating output table with max interval per box...
	update	m05_t1_panel_performance_stage0
	set		Continued_trans_30d = shelf.interval_length
	from 	m05_t1_panel_performance_stage0	as m05
			inner join #temp_shelf			as shelf
			on	m05.subscriber_id = shelf.subscriber_id

	commit

	drop table #intervals_starts
	drop table #intervals_ends
	drop table #temp_shelf
	--drop table measure_base_stage_2
	
	commit
	MESSAGE cast(now() as timestamp)||' | @ M05.1: Deriving Continuity Over 30d DONE' TO CLIENT
	
	-- Continued_trans_7d

	select	r.subscriber_id
			,r.log_date
			,rank() over (partition by r.subscriber_id order by r.log_date) as interval_sequencer
	into 	#intervals_starts
	from 	measure_base_stage_3 as l
			right join measure_base_stage_3 as r
			on	l.subscriber_id = r.subscriber_id
	and 	l.log_date+1 = r.log_date
	where 	l.subscriber_id is null


	select	l.subscriber_id
			,l.log_date
			,rank() over (partition by l.subscriber_id order by l.log_date) as interval_sequencer
	into 	#intervals_ends    
	from 	measure_base_stage_3 as l
			left join measure_base_stage_3 as r
			on	l.subscriber_id = r.subscriber_id
	and 	l.log_date+1 = r.log_date
	where 	r.subscriber_id is null


	-- per box, what was the max interval it had on last 7 days...
	select  l.subscriber_id
			,max(datediff(day, l.log_date, r.log_date) +1) as interval_length
	into	#temp_shelf
	from 	#intervals_starts as l
			inner join #intervals_ends as r
			on	l.subscriber_id = r.subscriber_id
			and	l.interval_sequencer = r.interval_sequencer
	group   by  l.subscriber_id


	-- Updating output table with max interval per box...
	update	m05_t1_panel_performance_stage0
	set		Continued_trans_7d = shelf.interval_length
	from 	m05_t1_panel_performance_stage0	as m05
			inner join #temp_shelf			as shelf
			on	m05.subscriber_id = shelf.subscriber_id

	commit

	drop table #intervals_starts
	drop table #intervals_ends
	drop table #temp_shelf
	--drop table measure_base_stage_3

	commit
	MESSAGE cast(now() as timestamp)||' | @ M05.1: Deriving Continuity Over 7d DONE' TO CLIENT


	/*

	-- avg_reporting_quality -- se puede derivar al final cuando este construyendo el output table...
	-- min_reporting_quality -- se puede derivar al final cuando este construyendo el output table...
	-- Reporting_performance

	*/



	-------------------------------------------------------
	-- M05.2 - Deriving Metrics for Panel Performance on AP
	-------------------------------------------------------
	
	MESSAGE cast(now() as timestamp)||' | Beginig M05.2 - Deriving Metrics for Panel Performance on AP' TO CLIENT
	
	-- Num_logs_sent_30d
	-- reporting_quality

	-- Since Sybase doesn't allow to group on Update commands, storing results into a temp shelf...

	select  cast(alt.subscriber_id as integer)      as subscriber_id
			,alt.panel
			,count(distinct (case when alt.dt > dateadd(day,-30,@profiling_day) then alt.dt else null end)) as Num_logs_sent_30d
			,count(distinct (case when alt.dt > dateadd(day,-7,@profiling_day) then alt.dt else null end))	as Num_logs_sent_7d
			,min(alt.dt) as scan1
			,case   when scan1 > dateadd(day,-30,@profiling_day) then scan1
					else dateadd(day,-29,@profiling_day)
			end     as scan2
			,case   when scan2 > min(m04.enablement_date) then scan2
					else min(m04.enablement_date)
			end     scan3
			,case   when scan3 > @profiling_day then @profiling_day
					else scan3
			end     as start_scan
			,case   when alt.panel = 5 then convert(float, datediff(day, start_scan, @profiling_day)+1)
					else ((convert(float, datediff(day, start_scan, @profiling_day)+1))/2)
			end     as dividend
			,cast(Num_logs_sent_30d as float) / dividend as reporting_quality
	into	#temp_shelf
	from    vespa_analysts.panel_data           as alt
			inner join  m04_t1_panel_sample_stage0  as m04
			on  cast(alt.subscriber_id as integer) = m04.subscriber_id
			and m04.panel in ('ALT6','ALT7','ALT5')
			inner join m05_t1_panel_performance_stage0  as m05
			on  m04.subscriber_id = m05.subscriber_id
	where   alt.dt > @from_dt 
	and     alt.dt <= @to_dt
	and     alt.data_received = 1
	group   by  subscriber_id
				,alt.panel

	commit

	-- updating metrics for AP...

	update	m05_t1_panel_performance_stage0
	set		Num_logs_sent_30d	= shelf.Num_logs_sent_30d
			,Num_logs_sent_7d	= shelf.Num_logs_sent_7d
			,reporting_quality 	= shelf.reporting_quality
	from	m05_t1_panel_performance_stage0	as m05
			inner join #temp_shelf			as shelf
			on	m05.subscriber_id = shelf.subscriber_id

	commit

	drop table #temp_shelf

	commit
	MESSAGE cast(now() as timestamp)||' | @ M05.2: Deriving Logs Sent Over 30d, Rep Qual DONE' TO CLIENT

/*

-- avg_reporting_quality -- se puede derivar al final cuando este construyendo el output table...
-- min_reporting_quality -- se puede derivar al final cuando este construyendo el output table...
-- Reporting_performance

*/

	-----------------------------------------
	-- M05.3 - General Panel Performance KPIs
	-----------------------------------------
	
	-- AT BOX LEVEL
	-- return_data_7d...
	-- return_data_30d...
	
	update	m05_t1_panel_performance_stage0 as m05
	set		return_data_30d	=	case	when m04.panel in ('ALT6','ALT7') and m05.Num_logs_sent_30d >= 15	then 1
										when m05.Num_logs_sent_30d >= 30 									then 1
                                        else 0
								end
			,return_data_7d =	case	when m04.panel in ('ALT6','ALT7') and m05.Num_logs_sent_7d >= 3	then 1
										when m05.Num_logs_sent_7d >= 7 								    then 1
                                        else 0
								end
	from	M04_t1_panel_sample_stage0		as m04
	where	m04.subscriber_id	= m05.subscriber_id
	
	commit
	---------------------------------------------------------------
	
	-- num_ac_returned_30d
	-- num_ac_returned_7d
	-- ac_full_returned_30d
	-- ac_full_returned_7d

	declare @todt	date	

	-- calculating the date for last Saturday... which is the end of the week for our time frame...
	-- this is the mark from where we analyse the performance of boxes and accounts back into 30 days...
	select  @todt =	case	when datepart(weekday,today()) = 7 then today()
							else (today() - datepart(weekday,today()))
					end
    
	-- a list of accounts and how many boxes each has...
	select  panel		
			,account_number
			,count(distinct subscriber_id) 	as num_boxes
			,min(enablement_date)			as enablement_date
	into	#acview
	from    m04_t1_panel_sample_stage0
	where	panel is not null
	group   by  panel
				,account_number
	
	commit
	create hg index hg1 on #acview(account_number)
	create lf index lf1 on #acview(panel)
	commit
	
	-- counting for each day on the past 30 days the number of boxes that dialed
	-- for every single account...
	select  perf.dt
			,boxview.account_number
			,count(distinct perf.subscriber_id) as dialling_b
	into	#panel_data
	from    vespa_analysts.panel_data               as perf
			inner join  m04_t1_panel_sample_stage0	as boxview
			on  perf.subscriber_id = boxview.subscriber_id
			and boxview.panel is not null
			and boxview.status_vespa = 'Enabled'
			and	boxview.panel in ('ALT5','ALT6','ALT7')
	where   perf.panel is not null
	and     perf.data_received = 1
	and     perf.dt between @todt-29 and @todt
	group   by  perf.dt 
				,boxview.account_number
	
	commit
	create date index date1 on #panel_data(dt)
	create hg index hg1 	on #panel_data(account_number)
	commit

	
	-- For AP
		
	select  acview.panel
			,acview.account_number
			,count(distinct panel_data.dt)                                                                      as num_ac_returned_30d
			,count(distinct (case when panel_data.dt > dateadd(day,-7,@todt) then panel_data.dt else null end)) as num_ac_returned_7d
			,case	when acview.panel in ('ALT6','ALT7') and num_ac_returned_30d >= 15  then 1
					when num_ac_returned_30d >= 30 									    then 1
					else 0
			end     as ac_full_returned_30d
			,case	when acview.panel in ('ALT6','ALT7') and num_ac_returned_7d >= 3    then 1
					when num_ac_returned_7d >= 7								        then 1
					else 0
			end     as ac_full_returned_7d
	into    #AP_ac_performance
	from    #acview	as acview
			inner join  #panel_data	as panel_Data
			on  acview.account_number   = panel_data.account_number
	where   panel_data.dialling_b >= acview.num_boxes
	group   by  acview.panel
				,acview.account_number

	commit
	create hg index hg1 on #ap_ac_performance(Account_number)
	commit
	
	update	m05_t1_panel_performance_stage0	as m05
	set		num_ac_returned_30d		=	base.num_ac_returned_30d
	        ,num_ac_returned_7d  	=	base.num_ac_returned_7d
	        ,ac_full_returned_30d	=	base.ac_full_returned_30d
	        ,ac_full_returned_7d 	=	base.ac_full_returned_7d
	from	#AP_ac_performance	as base
	where	base.account_number	=	m05.account_number
	
	commit
	drop table #ap_ac_performance
	commit
	
	-- For DP
	
	select  account_number
			,count(distinct dt)                                                             as num_ac_returned_30d
			,count(distinct (case when dt > dateadd(day,-7,@todt) then dt else null end))   as num_ac_returned_7d
			,case when num_ac_returned_30d >= 30    then 1 else 0 end                       as ac_full_returned_30d
			,case when num_ac_returned_7d >= 7      then 1 else 0 end                       as ac_full_returned_7d
	into    #viq_data
	from    (
				select  adjusted_event_start_Date_vespa as dt
						,account_number
				from    sk_prod.VIQ_VIEWING_DATA_SCALING
				where   adjusted_event_start_Date_vespa between @todt-29 and @todt
			)   as base
	group   by  account_number
	
	commit
	create hg index hg1 	on #viq_data(account_number)
	commit
	
	update	m05_t1_panel_performance_stage0	as m05
	set		num_ac_returned_30d		=	base.num_ac_returned_30d
	        ,num_ac_returned_7d  	=	base.num_ac_returned_7d
	        ,ac_full_returned_30d	=	base.ac_full_returned_30d
	        ,ac_full_returned_7d 	=	base.ac_full_returned_7d
	from	#viq_data	as base
	where	base.account_number	=	m05.account_number
	
	commit
	drop table #viq_data
	commit
	
	-- reporting_quality_s
	/*
		As part of a new KPI since 07/10/2014, we now want to check the frequency at which
		an account is a candidate for the scaling sample over the last 30 days 
		This KPI will have the shape of a ratio hence the formula is plain simple as:
		
		X = num of days where an account returned data / (30 if you are in panel 12,11,5 or 15 if you are in 6,7)
	*/
	
	-- treating accounts on the AP
	
	/*
		Since this new KPI is based on Scaling and the AP does not participate in this
		process, we are replicating here the same business rules to select accounts for
		scaling so we can flag the SCALING CANDIDATES in the AP
	*/

    if object_id('rq_scaling_final') is not null
        drop table rq_scaling_final

    commit

    select  acview.panel
			,acview.account_number
			,count(distinct dial.dt)    as dials
			,case   when @todt-29 <= min(acview.enablement_date)  then min(acview.enablement_date)
					else @todt-29
			end     as start_scan
			,cast(dials as float) / cast    (
												(
													case    when acview.panel = 'ALT5' then convert(float, datediff(day, start_scan, @todt)+1)
															else ((convert(float, datediff(day, start_scan, @todt)+1))/2)
													end
												)   as float
											)   as RQ
    into    rq_scaling_final
	from    #panel_data			as dial
			inner join #acview  as acview
			on  dial.account_number = acview.account_number
			and dial.dialling_b >= acview.num_boxes -- This is the condition that flags whether an account returned data or not
	group   by  acview.panel
				,acview.account_number

    commit
    create lf index lf1 on rq_scaling_final(panel)
    create hg index hg1 on rq_scaling_final(account_number)
    commit
	
	-- treating accounts on the DP
	
    insert  into rq_scaling_final
	select  acview.panel
			,acview.account_number
			,count(distinct viq.adjusted_event_start_date_vespa)                        as dials
			,case   when @todt-29 <= min(acview.enablement_date)  then min(acview.enablement_date)
					else @todt-29
			end     as start_scan
			,cast(dials as float) / convert(float, datediff(day, start_scan, @todt)+1)  as RQ
	from    sk_prod.VIQ_VIEWING_DATA_SCALING    as viq
			inner join #acview              as acview
			on  viq.account_number  = acview.account_number
			and acview.panel in ('VESPA','VESPA11')
	where   viq.adjusted_event_start_date_vespa between @todt-29 and @todt
	group   by  acview.panel
				,acview.account_number

    commit
	
	drop table #panel_data 
	drop table #acview
	commit

	update  m05_t1_panel_performance_stage0 as m05
	set     reporting_quality_s = rqs.rq
	from    rq_scaling_final                as rqs
	where   m05.account_number  = rqs.account_number
	
	commit
	drop table rq_scaling_final
	commit
	
------------------------
-- M05.4 - QAing results
------------------------

----------------------------
-- M05.5 - Returning results
----------------------------

-- m05_t1_panel_performance_stage0...
	MESSAGE cast(now() as timestamp)||' | M05 Finished, table m05_t1_panel_performance_stage0 BUILT' TO CLIENT

end
else
begin
	MESSAGE cast(now() as timestamp)||' | Exiting M05: Required Input not found or empty (M04_t1_panel_sample_stage0)' TO CLIENT
end

end;

commit;
grant execute on sig_masvg_m05_panel_performance to vespa_group_low_security;
commit;  /*


                         $$$
                        I$$$
                        I$$$
               $$$$$$$$ I$$$    $$$$$      $$$ZDD    DDDDDDD.
             ,$$$$$$$$  I$$$   $$$$$$$    $$$ ODD  ODDDZ 7DDDD
             ?$$$,      I$$$ $$$$. $$$$  $$$= ODD  DDD     NDD
              $$$$$$$$= I$$$$$$$    $$$$.$$$  ODD +DD$     +DD$
                  :$$$$~I$$$ $$$$    $$$$$$   ODD  DDN     NDD.
               ,.   $$$+I$$$  $$$$    $$$$=   ODD  NDDN   NDDN
              $$$$$$$$$ I$$$   $$$$   .$$$    ODD   ZDDDDDDDN
                                      $$$      .      $DDZ
                                     $$$             ,NDDDDDDD
                                    $$$?

                      CUSTOMER INTELLIGENCE SERVICES

--------------------------------------------------------------------------------------------------------------
**Project Name:							OPS 2.0
**Analysts:                             Angel Donnarumma (angel.donnarumma_mirabel@skyiq.co.uk)
**Lead(s):                              Jose Loureda
**Stakeholder:                          Operational Reports / SIG
**Due Date:                             20/09/2013
**Project Code (Insight Collation):     
**Sharepoint Folder:                    
                                                                        
**Business Brief:

**Modules:

M06: MASVG Panel Balance
        M06.0 - Initialising environment
		M06.1 - Snapshoting current non scaling segment sample
        M06.2 - Deriving Metrics for Panel Balance
				-- scaling_segment_id
				-- non_scaling_segment_id
				-- weight
				-- viq_weight
				-- weight_dt
        M06.3 - QAing results
        M06.4 - Returning results

**Stats:

	-- running time: 8 min approx...
	
--------------------------------------------------------------------------------------------------------------
*/


-----------------------------------
-- M06.0 - Initialising environment
-----------------------------------

create or replace procedure sig_masvg_m06_panel_balance
as begin


    MESSAGE cast(now() as timestamp)||' | Beginig M06.0 - Initialising environment' TO CLIENT
	
	-- local variables...
	
	declare @profiling_thursday date

	
	/*
		below procedure is supposed to bring the previous Saturday, then we will
		subtract 2 days to get the previous Thursday...
	*/
	execute vespa_analysts.Regulars_Get_report_end_date @profiling_thursday output 
	
	set @profiling_thursday = @profiling_thursday - 2

	--  As this module is fully related to the Scaling Excercise
	--	we need to sample here the accounst used for such on the most updated
	--	batch processed...

	select	account_number,scaling_segment_id
	into 	#Scaling_weekly_sample
	from 	vespa_analysts.SC2_Sky_base_segment_snapshots
	where 	profiling_date =	(
									select	max(profiling_date)
									from 	vespa_analysts.SC2_Sky_base_segment_snapshots
								)

	commit
	create unique index fake_key on #scaling_weekly_sample(account_number)
	commit
    MESSAGE cast(now() as timestamp)||' | @ M06.0: Initialisation DONE' TO CLIENT

---------------------------------------------------------
-- M06.1 - Snapshoting current non scaling segment sample
---------------------------------------------------------

    MESSAGE cast(now() as timestamp)||' | Beginig M06.1 - Snapshoting current non scaling segment sample' TO CLIENT

	-- again, what are the accounts on latest Scaling batch we should consider...
	truncate table sig_current_non_scaling_segments
	commit
	
	insert	into sig_current_non_scaling_segments	(
														account_number
													)
    select 	distinct(account_number)
    from 	#Scaling_weekly_sample
	
    MESSAGE cast(now() as timestamp)||' | @ M06.1: Sampling Accoutns DONE' TO CLIENT

	-- Deriving: VALUE SEGMENTS 	Non Scaling Variable
	
    update	sig_current_non_scaling_segments	as segments
    set 	value_segment = coalesce(vsd.value_seg, 'Bedding In')
    from 	sk_prod.VALUE_SEGMENTS_DATA			as vsd 
	where	segments.account_number = vsd.account_number
			
    commit
    MESSAGE cast(now() as timestamp)||' | @ M06.1: Deriving Value Segments DONE' TO CLIENT

	----------------------------------------------------	
	
	-- Deriving: CONSUMER VIEW 		Non Scaling Variable
	-- Deriving: FINANCIAL STRATEGY Non Scaling Variable
	-- Deriving: MOSAIC 			Non Scaling Variable
	
	--	As SAV holds the cbKeys, we want to get all those from accounts matching our sample...
    select  sig.account_number
            ,min(sav.cb_key_individual) as cb_key_individual
	into	#active_uk_ac_lookup
    from 	sk_prod.cust_single_account_view as sav
            inner join sig_current_non_scaling_segments as sig
            on  sav.account_number = sig.account_number
	where   sav.pty_country_code = 'GBR'
    group   by  sig.account_number
	
	commit
	create unique index fake_pk on #active_uk_ac_lookup(account_number)
	commit
    MESSAGE cast(now() as timestamp)||' | @ M06.1: Extracting CB Key Individuals from SAV DONE' TO CLIENT

	/*
	!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
	NOTE:	COMMENTED DUE TO BELOW BUG FOUND ON SYBASE 15.2
	!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
	
	--	with the CbKeys we can derive the fields we want from Experian...
    select	exp_nodupes.account_number
            ,expe.cb_key_individual
			,expe.cb_row_id 						as consumerview_cb_row_id
			,coalesce(expe.h_mosaic_uk_group, 'U')	as MOSAIC_segment
            ,coalesce(expe.h_fss_group, 'U')        as Financial_strategy_segment
    into	#consumerview_lookup
	from 	sk_prod.experian_consumerview	as expe
            inner join  (	--	because Experian has duplicates we need to go as below...
                            select  lookup.account_number
                                    ,A.cb_key_individual
                                    ,min(A.cb_row_id)     as cb_row_id
                            from    sk_prod.experian_consumerview   as A
                                    inner join #active_uk_ac_lookup	as lookup
						                        on	A.cb_key_individual = lookup.cb_key_individual
                            group   by  lookup.account_number
                                        ,A.cb_key_individual
                        )   as exp_nodupes
            on  expe.cb_row_id = exp_nodupes.cb_row_id
	*/
	
	select  lookup.account_number
			,A.cb_key_individual
			,min(A.cb_row_id)     as consumerview_cb_row_id
	into	#consumerview_lookup
	from    sk_prod.experian_consumerview   as A
			inner join #active_uk_ac_lookup	as lookup
						on	A.cb_key_individual = lookup.cb_key_individual
	group   by  lookup.account_number
				,A.cb_key_individual
				
    commit
	
    create unique index fake_pk on  #consumerview_lookup(account_number)
    create hg index fake_hg on 	    #consumerview_lookup(cb_key_individual)
	create hg index fake_hg2 on     #consumerview_lookup(consumerview_cb_row_id)
	
	drop table #active_uk_ac_lookup
	
    commit
    MESSAGE cast(now() as timestamp)||' | @ M06.1: Getting Relevant Rows IDs from Experian per Account DONE' TO CLIENT

	/*
	!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
	NOTE:	IN THE WEIRD WORLD OF SYBASE, BELOW UPDATE IS NOT VALID... WILL CRASH THE DB
			WORK AROUND THAT AS ON FOLLOWING LINES...
	!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
	
     -- OK, now get those fields onto the segmentation table...
    update	sig_current_non_scaling_segments	as scnss
    set 	consumerview_cb_row_id 		= cl.consumerview_cb_row_id
			,MOSAIC_segment				= cl.MOSAIC_segment
			,Financial_strategy_segment	= cl.Financial_strategy_segment
    from 	#consumerview_lookup  				as cl
	where	scnss.account_number = cl.account_number
	
	*/
	--	So to go with the work around, lets at least get the row ids per accounts...
	update	sig_current_non_scaling_segments	as scnss
	set		consumerview_cb_row_id = cl.consumerview_cb_row_id
	from	#consumerview_lookup				as cl
	where	cl.account_number = scnss.account_number
	
	commit
	drop table #consumerview_lookup
	commit
	MESSAGE cast(now() as timestamp)||' | @ M06.1: Storing Rows IDs DONE' TO CLIENT

	--	now, weirdly enough (0) creating a mirror of the non scaling segments table
	--	and (1) filling it in by joining the tables we need and (2) then putting the records
	--	back to the original table seems to work...
	
	-- (0) Mirroring Non Scaling Segments Table...
	
	if object_id('weird_bug_fixing') is not null
		drop table weird_bug_fixing
		
	commit
	
	create table weird_bug_fixing	(
	
		account_number       		varchar(20) primary key
		,non_scaling_segment_id     int
		,value_segment              varchar(10)
		,consumerview_cb_row_id     bigint
		,MOSAIC_segment             varchar(1)
		,Financial_strategy_segment	varchar(1)
		,is_OnNet                   bit         default 0
		,uses_sky_go                bit         default 0
		
     )
	
	commit
	
	--	(1) Filling it in by joining the tables we need and getting the derivations out...
	insert	into weird_bug_fixing	(
										account_number
										,non_scaling_segment_id
										,value_segment
										,consumerview_cb_row_id
										,MOSAIC_segment
										,Financial_strategy_segment
										,is_OnNet
										,uses_sky_go
									)
	select	scnss.account_number
            ,scnss.non_scaling_segment_id
            ,scnss.value_segment
            ,scnss.consumerview_cb_row_id
            ,coalesce(expe.h_mosaic_uk_group, 'U') as MOSAIC_segment
            ,coalesce(expe.h_fss_group, 'U')          as Financial_strategy_segment
            ,scnss.is_OnNet
            ,scnss.uses_sky_go
	from	sig_current_non_scaling_segments			as scnss
			left join sk_prod.experian_consumerview	as expe
			on	scnss.consumerview_cb_row_id = expe.cb_row_id
	
    commit
    create index for_updating   on weird_bug_fixing (consumerview_cb_row_id)
	commit
    MESSAGE cast(now() as timestamp)||' | @ M06.1: Initialising Bug Fixture DONE' TO CLIENT    

	truncate table sig_current_non_scaling_segments
	MESSAGE cast(now() as timestamp)||' | @ M06.1: Trunkating Non Scaling Segment table due to Bug DONE' TO CLIENT
	
	--	(2) Putting the records back from the mirror table into the original one (F**ing weird)...
	insert into sig_current_non_scaling_segments	(
														account_number
														,non_scaling_segment_id
														,value_segment
														,consumerview_cb_row_id
														,MOSAIC_segment
														,Financial_strategy_segment
														,is_OnNet
														,uses_sky_go
													)
	select 	account_number
			,non_scaling_segment_id
			,value_segment
			,consumerview_cb_row_id
			,MOSAIC_segment
			,Financial_strategy_segment
			,is_OnNet
			,uses_sky_go
	from 	weird_bug_fixing
	
    commit
    drop table weird_bug_fixing
    commit
	MESSAGE cast(now() as timestamp)||' | @ M06.1: Deriving Consumer View, MOSAIC, Financial Strategy DONE' TO CLIENT

	----------------------------------------------------
	
	-- Deriving: SKY GO 			Non Scaling Variable
	
	/*
	-- Finally (for now) the Sky Go use marks
    select	distinct account_number
    into 	#skygousers
    from 	sk_prod.SKY_PLAYER_USAGE_DETAIL
    where 	activity_dt >= '2011-08-18'

    commit
    create unique index fakle_pk on #skygousers(account_number)
    commit

    update	sig_current_non_scaling_segments
    set 	uses_sky_go = 1
    from 	sig_current_non_scaling_segments	as segments
			inner join #skygousers as sgu 
			on segments.account_number = sgu.account_number
	*/		
	update	sig_current_non_scaling_segments	as nons
	set		uses_sky_go = 1
	from	m08_t1_account_base_stage0			as m08
    where	m08.account_number = nons.account_number
	and		m08.Skygo_subs = 1
	
    commit -- 4241198 row(s) updated
    /*
	drop table #skygousers
    commit
	*/
    MESSAGE cast(now() as timestamp)||' | @ M06.1: Deriving Sky Go DONE' TO CLIENT

	----------------------------------------------------
	
	-- Deriving: ONNET FLAG 		Non Scaling Variable
	
	-- The OnNet goes by postcode...
	
    select	scnss.account_number
            ,min(sav.cb_address_postcode)   as postcode
            ,convert(bit, 0)                as onnet
    into 	#onnet_patch
    from 	sig_current_non_scaling_segments    as scnss
            inner join  (
                            select  account_number
                                    ,cb_address_postcode
                            from    sk_prod.cust_single_account_view 
                            where 	cust_active_dtv = 1 -- OK, so we're getting account number duplicates, that's annoying...
                            and		pty_country_code = 'GBR'
                        )   as sav 
			on	sav.account_number = scnss.account_number
    group 	by	scnss.account_number -- If there are account_number duplicates, they're postcodes for an active account, so whatever...
    
    update  #onnet_patch
    set     postcode = upper(REPLACE(postcode,' ',''))
    
    commit
    create unique index fake_pk on #onnet_patch (account_number)
    create index joinsy on #onnet_patch (postcode)
    commit
    MESSAGE cast(now() as timestamp)||' | @ M06.1: Extracting Pcode from SAV DONE' TO CLIENT

    -- 1) Get BROADBAND_POSTCODE_EXCHANGE postcodes...

    SELECT	cb_address_postcode as postcode
			,MAX(mdfcode) 								as exchID
    INTO 	#bpe
    FROM 	sk_prod.BROADBAND_POSTCODE_EXCHANGE
    GROUP 	BY	postcode

    update  #bpe
    set     postcode = upper(REPLACE( postcode,' ',''))

    commit
    create unique index fake_pk on #bpe(postcode)
    commit
    MESSAGE cast(now() as timestamp)||' | @ M06.1: Extracting Pcode from Pcode Exchange DONE' TO CLIENT

    -- 2) Get BB_POSTCODE_TO_EXCHANGE postcodes...
	 
    SELECT	postcode
			,MAX(exchange_id) 					as exchID
    INTO 	#p2e
    FROM 	sk_prod.BB_POSTCODE_TO_EXCHANGE
    GROUP 	BY	postcode

    update  #p2e
    set     postcode = upper(REPLACE( postcode,' ',''))

    commit
    create unique index fake_pk on #p2e (postcode)
    commit
    MESSAGE cast(now() as timestamp)||' | @ M06.1: Extracting Pcode from BB Pcode DONE' TO CLIENT

    -- 3) Combine postcode lists taking BB_POSTCODE_TO_EXCHANGE exchange_id's where possible

    SELECT	COALESCE(#p2e.postcode, #bpe.postcode)	AS postcode
			,COALESCE(#p2e.exchID, #bpe.exchID) 	as exchange_id
			,'OFFNET' as exchange
    INTO 	#onnet_lookup
    FROM 	#bpe FULL JOIN #p2e 
			ON	#bpe.postcode = #p2e.postcode

    commit
    create unique index fake_pk on #onnet_lookup (postcode)
    commit

    -- 4) Update with latest Easynet exchange information

    UPDATE	#onnet_lookup					as base
    SET 	exchange = 'ONNET'
    FROM 	sk_prod.easynet_rollout_data 	as easy 
	where	base.exchange_id = easy.exchange_id
    and 	easy.exchange_status = 'ONNET'

	-- 5) Flag your base table with onnet exchange data. Note that this uses a postcode field with
	--   spaces removed so your table will either need to have a similar filed or use a REPLACE
	--   function in the join

    UPDATE	#onnet_patch	as base
    SET 	onnet = CASE WHEN tgt.exchange = 'ONNET'
                         THEN 1
                         ELSE 0
            END
    FROM 	#onnet_lookup	AS tgt 
	where	base.postcode = tgt.postcode
    
	commit
    MESSAGE cast(now() as timestamp)||' | @ M06.1: Constructing Pcode Lookup DONE' TO CLIENT

    update	sig_current_non_scaling_segments	as scnss
    set 	is_OnNet = op.onnet
    from 	#onnet_patch 						as op 
	where	scnss.account_number = op.account_number

    commit

    -- Clear out all those tables that got sprayed about the place:
    drop table #onnet_patch
    drop table #onnet_lookup
    drop table #p2e
    drop table #bpe
    commit
    MESSAGE cast(now() as timestamp)||' | @ M06.1: Deriving OnNet Flag DONE' TO CLIENT

	-------------------------------------------------------------------------------
	
	-- Now what is pending to do is to check how belongs to what non scaling segment...
	update	sig_current_non_scaling_segments	as scnss
    set		non_scaling_segment_id = snssl.non_scaling_segment_id
    from 	sig_non_scaling_segments_lookup 	as snssl 
	where	scnss.value_segment					= snssl.value_segment
	and 	scnss.MOSAIC_segment             	= snssl.MOSAIC_segment
	and 	scnss.Financial_strategy_segment	= snssl.Financial_strategy_segment
	and 	scnss.is_OnNet                   	= snssl.is_OnNet
	and 	scnss.uses_sky_go                	= snssl.uses_sky_go
			
	commit -- 7183647 row(s) updated
    MESSAGE cast(now() as timestamp)||' | @ M06.1: Integrating Accounts Sample to Non Scaling Segments DONE' TO CLIENT
	
---------------------------------------------
-- M06.2 - Deriving Metrics for Panel Balance
---------------------------------------------

    MESSAGE cast(now() as timestamp)||' | Beginig M06.2 - Deriving Metrics for Panel Balance' TO CLIENT

-- scaling_segment_id
	truncate table m06_t1_panel_balance_stage0
	commit

	insert	into m06_t1_panel_balance_stage0	(
													account_number
													,scaling_segment_id
												)
	select	distinct
			account_number
			,scaling_segment_id
	from	#Scaling_weekly_sample
	
	commit
	drop table #Scaling_weekly_sample
	commit
	MESSAGE cast(now() as timestamp)||' | @ M06.2: Deriving Scaling Segment ID DONE' TO CLIENT

-- non_scaling_segment_id

	update	m06_t1_panel_balance_stage0			as m06
	set		non_scaling_segment_id	= scnss.non_scaling_segment_id
	from	sig_current_non_scaling_segments	as scnss
	where	m06.account_number = scnss.account_number
			
	commit
    MESSAGE cast(now() as timestamp)||' | @ M06.2: Deriving Non Scaling Segment ID DONE' TO CLIENT
	
-- weight
-- weight_dt

	update	m06_t1_panel_balance_stage0	as m06
	set		weight		    = weights.weighting
			,weight_date    = weights.scaling_day
	from	(		
				/*
					Getting the sample of accounts scaled on the given Thursday
					and their weights assigned...
				*/
				select  inter.account_number
						,inter.scaling_segment_id
						,weight.weighting
						,weight.scaling_day
				from    vespa_analysts.SC2_Intervals    as inter
						inner join  (
										-- Getting the weights for the given Thursday
										select  *
										from    vespa_analysts.sc2_weightings
										where   scaling_day = @profiling_thursday
									)   as weight
						on  inter.scaling_segment_id = weight.scaling_segment_id
				where   @profiling_thursday	between inter.reporting_starts and inter.reporting_ends
			)	as weights
	where	m06.scaling_segment_id 	= weights.scaling_segment_id
	and		m06.account_number		= weights.account_number
	
	commit
	
-- viq_weight (extracted for the same date as above)

	update	m06_t1_panel_balance_stage0			as m06
	set		viq_weight = viq.calculated_scaling_weight
	from	sk_prod.VIQ_VIEWING_DATA_SCALING	as viq
	where	viq.account_number	= m06.account_number
	and   	viq.adjusted_event_start_date_vespa = @profiling_thursday
	
	commit		
	
	MESSAGE cast(now() as timestamp)||' | @ M06.2: Deriving Weights Values and Dates DONE' TO CLIENT

------------------------
-- M06.3 - QAing results
------------------------

----------------------------
-- M06.4 - Returning results
----------------------------

-- m06_t1_panel_balance_stage0...
    MESSAGE cast(now() as timestamp)||' | M06 Finished, table m06_t1_panel_balance_stage0 BUILT' TO CLIENT

    commit



end;

commit;
grant execute on sig_masvg_m06_panel_balance to vespa_group_low_security;
commit; /*


                         $$$
                        I$$$
                        I$$$
               $$$$$$$$ I$$$    $$$$$      $$$ZDD    DDDDDDD.
             ,$$$$$$$$  I$$$   $$$$$$$    $$$ ODD  ODDDZ 7DDDD
             ?$$$,      I$$$ $$$$. $$$$  $$$= ODD  DDD     NDD
              $$$$$$$$= I$$$$$$$    $$$$.$$$  ODD +DD$     +DD$
                  :$$$$~I$$$ $$$$    $$$$$$   ODD  DDN     NDD.
               ,.   $$$+I$$$  $$$$    $$$$=   ODD  NDDN   NDDN
              $$$$$$$$$ I$$$   $$$$   .$$$    ODD   ZDDDDDDDN
                                      $$$      .      $DDZ
                                     $$$             ,NDDDDDDD
                                    $$$?

                      CUSTOMER INTELLIGENCE SERVICES

--------------------------------------------------------------------------------------------------------------
**Project Name:                                                 OPS 2.0
**Analysts:                             Berwyn Cort (berwyn.cort@skyiq.co.uk) Angel Donnarumma (angel.donnarumma_mirabel@skyiq.co.uk)
**Lead(s):                              Jose Loureda
**Stakeholder:                          Operational Reports / SIG
**Due Date:                             20/09/2013
**Project Code (Insight Collation):
**Sharepoint Folder:

**Business Brief:

**Modules:
        M07.0 - Initialising environment
        M07.1 - Snapshot of current active UK boxes
        M07.2 - Deriving Features of active UK boxes
                -- Add Box_is_3D
                -- Add Box_has_anytime_plus
                -- Add PVR
        M07.3 - QAing results
        M07.4 - Returning results

**Stats:

	-- running time: 17 min approx...

--------------------------------------------------------------------------------------------------------------
*/

-----------------------------------
-- M07.0 - Initialising environment
-----------------------------------

CREATE OR REPLACE PROCEDURE sig_masvg_m07_box_base
AS BEGIN

	MESSAGE cast(now() as timestamp)||' | Beginig M07.0 - Initialising environment' TO CLIENT

    -- Local Variables...
    DECLARE @profiling_day DATE

    SELECT  @profiling_day = MAX(cb_data_date) FROM sk_prod.cust_single_account_view

	MESSAGE cast(now() as timestamp)||' | @ M07.0: Initialisation DONE' TO CLIENT
	
----------------------------------------------
-- M07.1 - Snapshot of current active UK boxes
----------------------------------------------

	MESSAGE cast(now() as timestamp)||' | Beginig M07.1 - Snapshoting current Active UK Boxes' TO CLIENT

	if object_id ('tlbxTemp') is not null
		drop table tlbxTemp
		
    SELECT  *
    INTO    tlbxTemp
    FROM    (vespa_analysts.sig_toolbox_04_Active_Sky_Box_List())


    COMMIT
    CREATE HG INDEX ac_num_index 			ON tlbxTemp (account_number)
    CREATE unique INDEX serv_inst_id_index 	ON tlbxTemp (service_instance_id)
    COMMIT
	
	if object_id ('stage_1') is not null
		drop table stage_1
	
	select	m08.account_number
			,thetemp.service_instance_id
			,thetemp.x_model_number
			,thetemp.Adsmart_flag
			,thetemp.x_pvr_type
			,thetemp.x_manufacturer
			,thetemp.x_box_type
			,thetemp.currency_code
			,thetemp.x_anytime_plus_enabled
			,thetemp.x_description
			,thetemp.x_personal_storage_capacity
	into	stage_1
	from	m08_t1_account_base_stage0 as m08
			left join tlbxtemp	as thetemp
			on	m08.account_number = thetemp.account_number
    
	COMMIT
	drop table tlbxtemp
    CREATE HG INDEX fake_hg1 		ON stage_1 (account_number)
    CREATE unique INDEX fake_key1	ON stage_1 (service_instance_id)
    COMMIT
	
	MESSAGE cast(now() as timestamp)||' | @ M07.1: Snapshot DONE' TO CLIENT
	
	---------------------
	--	BUG PATCH [BEGIN]
	---------------------
	
	--	1/2: we need here to link back to Sky Base as not all Active Accounts
	--	are matching with CSTB table... hence we need to get what possible from CSTB
	--	and work a way around to get the details for the remaining accounts
	-- 	(if they are active we want the details...)

	/*
		THIS COULD ACTUALLY COME IN HANDY AS A COMPLEMENT FOR THE BUG ... FROM SIMON LEARY
		
		select  count(1) as nrows
				,count(distinct decoder_nds_number) as ndecoders
		from    sk_prod.cust_service_instance
		where   decoder_nds_number in   (
											select  distinct last_decoder_nds_number -- 19664
											from    sk_prod.CUST_STB_CALLBACK_SUMMARY
											where   account_number in   (
																			select  distinct account_number
																			from    stage_1
																			where   service_instance_id is null
																		) -- 18367
										)
			
	*/
	-------------------
	--	BUG PATCH [END]
	-------------------
	truncate table m07_t1_box_base_stage0
	commit
	
	insert  into m07_t1_box_base_stage0	(
											account_number
											,card_Subscriber_ID
											,subscriber_ID
											,service_instance_ID
											,Adsmart_flag
											,box_type_physical
											,HD_box_physical
											,box_storage_capacity
											,Box_model
											,pvr_type
											,description
										)
	SELECT  account_number
			,card_Subscriber_ID
			,cast(card_Subscriber_ID as decimal(10)) subscriber_ID
			,service_instance_ID
			,Adsmart_flag
            ,x_box_type
			,HD_box_physical
			,x_personal_storage_capacity
			,x_model_number
			,x_pvr_type
			,x_description
	from    (
				select  distinct 
						a.account_number
						,b.si_external_identifier as card_subscriber_id
						,a.service_instance_id
						,b.si_start_dt
						,rank() over    (
											partition by    a.account_number
															,card_subscriber_id
											order by        a.service_instance_id
															,b.si_start_dt desc
										)   as ranking
						,A.adsmart_flag
						,A.x_box_type
						,CASE   WHEN A.x_description like '%HD%'    THEN 1
																	ELSE 0
						END     AS   HD_box_physical
						,A.x_personal_storage_capacity
						,A.x_model_number
						,A.x_pvr_type
						,x_description
				from    stage_1 as A
						inner join sk_prod.cust_service_instance as B
						on  A.service_instance_id = B.src_system_id
			)   as base
	where   ranking = 1
	
	commit
	MESSAGE cast(now() as timestamp)||' | @ M07.1: Saving Box list into output table DONE' TO CLIENT
	
	---------------------
	--	BUG PATCH [BEGIN]
	---------------------
	
	/*
		-- 01/11/2013
		
		NOTE1:	This fix is put in place due to the gap existing between CSI and CSTB, at the moment we cannot find the
				service intance id for some accounts and that impact on searching box details (because we don't know the box id)
		NOTE2:	Any drop shown in the m07 table after following insert will be due to discrepancies between
				CSI and SAV... but potentially we could avoid this situation (yet carrying on with accounts that
				will not have box details due the known gap between CSTB and SAV)
				
				The reason why is this taking place is because the intention is to keep consistency with the volume of 
				active accounts in the Sky base...		
	*/
	MESSAGE cast(now() as timestamp)||' | @ M07.1: ### BUG-PATCH BEGIN ###' TO CLIENT
	insert	into m07_t1_box_base_stage0 (
											account_number
											,card_Subscriber_ID
										)
	select  distinct
			account_number
			,'unknown'      as card_Subscriber_ID
	from    stage_1
	where   service_instance_id is null
	
	commit
	
	-------------------
	--	BUG PATCH [END]
	-------------------
	
	commit
	MESSAGE cast(now() as timestamp)||' | @ M07.1: Placing Accounts missing Service Instance IDs DONE' TO CLIENT
	MESSAGE cast(now() as timestamp)||' | @ M07.1: ### BUG-PATCH END ###' TO CLIENT
	
---------------------------------------------------
-- M07.2 - Deriving Features of active UK boxes
---------------------------------------------------

	MESSAGE cast(now() as timestamp)||' | Beginig M07.2 - Deriving Features of active UK boxes' TO CLIENT

-- Add Box_is_3D (not needed, basically if the box can HD then can 3D)...
/*
    SELECT  DISTINCT service_instance_id
    INTO    #accounts_with_3dtv
    FROM    sk_prod.cust_subs_hist
    WHERE   subscription_sub_type = '3DTV'
    AND     status_code IN ('AC','PC','AB')
    AND     effective_from_dt <= @profiling_day
    AND     effective_to_dt   >  @profiling_day


    COMMIT
    CREATE  UNIQUE INDEX fake_pk ON #accounts_with_3dtv (service_instance_id)
    COMMIT

    UPDATE  m07_t1_box_base_stage0
    SET     Box_is_3D = 1
    FROM    m07_t1_box_base_stage0
            INNER JOIN #accounts_with_3dtv AS gw3d 
			ON m07_t1_box_base_stage0.service_instance_id = gw3d.service_instance_id

    COMMIT

    DROP   TABLE #accounts_with_3dtv
*/
-- Add Box_has_anytime_plus

    UPDATE  m07_t1_box_base_stage0  as m07
	SET     Box_has_anytime_plus = 1
	FROM    stage_1 as s1
			inner join sk_prod.cust_card_subscriber_link as link
			ON  s1.service_instance_id = link.service_instance_id
			AND link.current_flag = 'Y'
	WHERE   m07.service_instance_id = s1.service_instance_id
	AND     s1.x_anytime_plus_enabled = 'Y'
	
    COMMIT
	MESSAGE cast(now() as timestamp)||' | @ M07.2: Deriving Anytime Plus Capability DONE' TO CLIENT
	
-- Add PVR

	UPDATE  m07_t1_box_base_stage0 AS m07
	SET     pvr = 1
	FROM    sk_prod.cust_subs_hist              as csh
	WHERE   csh.service_instance_id = m07.service_instance_id
	AND     csh.effective_from_dt <= @profiling_day
	AND     csh.effective_to_dt > @profiling_day
	AND     csh.subscription_sub_type in ('DTV Primary Viewing', 'DTV Extra subscription')
	AND     m07.pvr_type like '%PVR%'

    COMMIT
    DROP TABLE stage_1
    COMMIT
	MESSAGE cast(now() as timestamp)||' | @ M07.2: Deriving PVR flag DONE' TO CLIENT
	
------------------------
-- M07.3 - QAing results
------------------------

----------------------------
-- M07.4 - Returning results
----------------------------

-- m07_t1_box_base_stage0...
	MESSAGE cast(now() as timestamp)||' | M07 Finished, table m07_t1_box_base_stage0 BUILT' TO CLIENT

END;

COMMIT;
grant execute on sig_masvg_m07_box_base to vespa_group_low_security;
commit; /*


                         $$$
                        I$$$
                        I$$$
               $$$$$$$$ I$$$    $$$$$      $$$ZDD    DDDDDDD.
             ,$$$$$$$$  I$$$   $$$$$$$    $$$ ODD  ODDDZ 7DDDD
             ?$$$,      I$$$ $$$$. $$$$  $$$= ODD  DDD     NDD
              $$$$$$$$= I$$$$$$$    $$$$.$$$  ODD +DD$     +DD$
                  :$$$$~I$$$ $$$$    $$$$$$   ODD  DDN     NDD.
               ,.   $$$+I$$$  $$$$    $$$$=   ODD  NDDN   NDDN
              $$$$$$$$$ I$$$   $$$$   .$$$    ODD   ZDDDDDDDN
                                      $$$      .      $DDZ
                                     $$$             ,NDDDDDDD
                                    $$$?

                      CUSTOMER INTELLIGENCE SERVICES

--------------------------------------------------------------------------------------------------------------
**Project Name:							OPS 2.0
**Analysts:                             Angel Donnarumma (angel.donnarumma_mirabel@skyiq.co.uk)
**Lead(s):                              Jose Loureda
**Stakeholder:                          Operational Reports / SIG
**Due Date:                             20/09/2013
**Project Code (Insight Collation):     
**Sharepoint Folder:                    
                                                                        
**Business Brief:

**Modules:
		M08.0 - Initialising environment
		M08.1 - Snapshoting current Active UK Customers
        M08.2 - Deriving Features of Active UK Customers
				--	Skygo_subs
				--	Anytime_plus_subs
				--	box_type_subs
				--	HD_box_subs
				--	RTM
				--	prem_sports
				--prem_movies
        M08.2 - QAing results
        M08.3 - Returning results
		
**Stats:

	-- running time: 20 min approx...

--------------------------------------------------------------------------------------------------------------
*/

-----------------------------------
-- M08.0 - Initialising environment
-----------------------------------

create or replace procedure sig_masvg_m08_account_base
as begin

	MESSAGE cast(now() as timestamp)||' | Beginig M08.0 - Initialising environment' TO CLIENT
	
	-- Local Variables...
	declare @profiling_day date
	
	select	@profiling_day = max(cb_data_date) from sk_prod.cust_single_account_view
    
    commit

	MESSAGE cast(now() as timestamp)||' | @ M08.0: Initialisation DONE' TO CLIENT
	
--------------------------------------------------
-- M08.1 - Snapshoting current Active UK Customers
--------------------------------------------------
    
	MESSAGE cast(now() as timestamp)||' | Beginig M08.1 - Snapshoting current Active UK Customers' TO CLIENT
	
	select  account_number
			,cust_viewing_data_capture_allowed  as viewing_consent
			,cb_key_individual 				    as cb_key_individual
			,CUST_ACTIVE_DTV					as cust_active_dtv
			,pty_country_code					as UK_Standard_account
			,PROD_DTV_ACTIVATION_DT			    as cust_active_dt
	into    #temp_shelf
	from    (	
				-- Applying a rank based on recent activation date
				-- to shield up against potential duplicates...
				select  *
						,rank() over (
										partition by    account_number
										order by        prod_dtv_activation_dt desc
									)   as ranking
				from    (vespa_analysts.sig_toolbox_03_ActiveUKCust())
			)   as deduping
	where   ranking = 1            
	
	update	#temp_shelf as bas
	set 	viewing_consent = 'N'
	from 	vespa_analysts.ConsentIssue_05_Revised_Consent_Info as exc
	where 	bas.account_number = exc.account_number
	
	commit
	
	truncate table m08_t1_account_base_stage0
	commit
	
	insert	into m08_t1_account_base_stage0	(
												account_number
												,viewing_consent_flag
												,cb_key_individual
												,cust_active_dtv
												,UK_Standard_account
												,cust_active_dt
											)
	select	*
	from	#temp_shelf
	
	commit
	drop table #temp_shelf
	commit
	MESSAGE cast(now() as timestamp)||' | @ M08.1: Extracting Active UK Base DONE' TO CLIENT
	
	-- Extracting History of Active UK Customers...
	select  csh.account_number
			,csh.subscription_sub_type
			,csh.status_code
			,csh.effective_from_dt
			,csh.effective_to_dt
			,csh.subscription_type
			,csh.current_short_description
			--,csh.service_instance_id -- Don't think we're gonna need this tbh...
	into    #cshcompact
	from    sk_prod.cust_subs_hist as csh
			inner join m08_t1_account_base_stage0 as m08
			on  csh.account_number = m08.account_number
	where 	csh.subscription_sub_type IN ('DTV Primary Viewing','DTV Sky+', 'DTV Extra Subscription','DTV HD','PDL subscriptions') 
	and    	csh.status_code IN  ('AC','AB','PC')
	and     csh.effective_FROM_dt <> csh.effective_to_dt
	and   	@profiling_day between csh.effective_from_dt and csh.effective_to_dt
	
	commit
	create hg 	index fake_1 on #cshcompact(account_number)
	create lf 	index fake_2 on #cshcompact(subscription_sub_type)
	create lf 	index fake_3 on #cshcompact(status_code)
	create date index fake_4 on #cshcompact(effective_from_dt)
	create date index fake_5 on #cshcompact(effective_to_dt)
	create lf 	index fake_6 on #cshcompact(subscription_type)
	create lf 	index fake_7 on #cshcompact(current_short_description)
	commit
	MESSAGE cast(now() as timestamp)||' | @ M08.1: Compacting Active UK Base History DONE' TO CLIENT
	
---------------------------------------------------		
-- M08.2 - Deriving Features of Active UK Customers
---------------------------------------------------

	MESSAGE cast(now() as timestamp)||' | Beginig M08.2 - Deriving Features of Active UK Customers' TO CLIENT
	
	--Skygo_subs
    
    update  m08_t1_account_base_stage0  as m08
    set     skygo_subs = case when adsmart.sky_go_reg = 'Yes' then 1 else 0 end
    from    sk_prod.adsmart  as adsmart
    where   m08.account_number = adsmart.account_number

	commit
	MESSAGE cast(now() as timestamp)||' | @ M08.1: Deriving Sky Go Flag DONE' TO CLIENT
	
	--Anytime_plus_subs
	
	update 	m08_t1_account_base_stage0	as m08
    set 	Anytime_plus_subs = 1
    from 	#cshcompact    	as csh
	where 	m08.account_number = csh.account_number
	and 	csh.subscription_sub_type = 'PDL subscriptions'
	and    	csh.status_code = 'AC'
	
	commit
	MESSAGE cast(now() as timestamp)||' | @ M08.1: Deriving Anytime+ Flag DONE' TO CLIENT
	
	--box_type_subs
	
	select	* 
	into 	#stb_active 
    from	(
				select	account_number
						,service_instance_id
						,active_box_flag
						,box_installed_dt
						,box_replaced_dt
						,x_pvr_type
						,x_anytime_enabled
						,current_product_description
						,x_anytime_plus_enabled
						,x_box_type
						,CASE WHEN x_description like '%HD%2TB%' THEN 1 ELSE 0 END AS HD2TB
						,CASE WHEN x_description like '%HD%1TB%' THEN 1 ELSE 0 END AS HD1TB
						,CASE WHEN x_description like '%HD%'     THEN 1 ELSE 0 END AS HD
						,x_manufacturer
						,x_description
						,x_model_number
						,rank () over (partition by service_instance_id order by ph_non_subs_link_sk desc) as active_flag
				from 	sk_prod.cust_set_top_box
			) 	as t
	where 	active_flag = 1

    commit
    create index #stb_active_accnum on #stb_active(account_number)
    create index #stb_active_siid   on #stb_active(service_instance_id)
	commit
	
    --Creates a list of accounts with active HD capable boxes
    SELECT	stb.account_number
            ,max(HD) AS HD
            ,max(HD1TB) AS HD1TB
            ,max(HD2TB) as HD2TB
    INTO 	#hda
    FROM 	#stb_active AS stb
            INNER JOIN m08_t1_account_base_stage0 AS m08 
			on	stb.account_number = m08.account_number
    GROUP 	BY	stb.account_number
	
	commit
	create hg index fake_hg1 on #hda(account_number)
	commit
	
	SELECT  csh.account_number
            ,max(CASE  WHEN csh.SUBSCRIPTION_SUB_TYPE ='DTV Primary Viewing'    THEN 1 ELSE 0  END) AS TV
            ,max(CASE  WHEN csh.SUBSCRIPTION_SUB_TYPE ='DTV Sky+'               THEN 1 ELSE 0  END) AS SP
            ,max(CASE  WHEN csh.SUBSCRIPTION_SUB_TYPE ='DTV Extra Subscription' THEN 1 ELSE 0  END) AS MR
            ,max(CASE  WHEN csh.SUBSCRIPTION_SUB_TYPE ='DTV HD'                 THEN 1 ELSE 0  END) AS HD
            ,max(CASE  WHEN #hda.HD = 1                                         THEN 1 ELSE 0  END) AS HDstb
            ,max(CASE  WHEN #hda.HD1TB = 1                                      THEN 1 ELSE 0  END) AS HD1TBstb
            ,max(CASE  WHEN #hda.HD2TB = 1            THEN 1 ELSE 0  END) AS HD2TBstb
            ,convert(varchar(30), null) as box_type
    INTO 	#box_type
    FROM 	#cshcompact 							        AS csh
			LEFT OUTER JOIN sk_prod.cust_entitlement_lookup	as cel
			ON	csh.current_short_description = cel.short_description
			LEFT OUTER JOIN #hda 
			ON	csh.account_number = #hda.account_number 
	WHERE 	csh.SUBSCRIPTION_SUB_TYPE IN ('DTV Primary Viewing','DTV Sky+', 'DTV Extra Subscription','DTV HD' )
	GROUP 	BY	csh.account_number
	HAVING 	TV = 1

    commit
    create unique index maybe_fake_pk on #box_type(account_number)
    commit

    update	#box_type
    set 	box_type =  CASE    WHEN HD =1 AND MR = 1 AND HD2TBstb = 1      THEN 'A) HD Combi 2TB'
								WHEN HD =1 AND HD2TBstb = 1                 THEN 'B) HD 2TB'
								WHEN HD =1 AND MR = 1 AND HD1TBstb = 1      THEN 'A) HD Combi 1TB'
								WHEN HD =1 AND HD1TBstb = 1                 THEN 'B) HD 1TB'
								WHEN HD =1 AND MR = 1 AND HDstb = 1         THEN 'A) HD Combi'
								WHEN HD =1 AND HDstb = 1                    THEN 'B) HD'
								WHEN SP =1 AND MR = 1 AND HD2TBstb = 1      THEN 'C) HDx Combi 2TB'
								WHEN SP =1 AND HD2TBstb = 1                 THEN 'D) HDx 2TB'
								WHEN SP =1 AND MR = 1 AND HD1TBstb = 1      THEN 'C) HDx Combi 1TB'
								WHEN SP =1 AND HD1TBstb = 1                 THEN 'D) HDx 1TB'
								WHEN SP =1 AND MR = 1 AND HDstb = 1         THEN 'C) HDx Combi'
								WHEN SP =1 AND HDstb = 1                    THEN 'D) HDx'
								WHEN SP =1 AND MR = 1                       THEN 'E) SkyPlus Combi'
								WHEN SP =1                                  THEN 'F) SkyPlus '
								WHEN MR =1                                  THEN 'G) Multiroom'
								ELSE                                        'H) FDB'
						END
    
    commit

    UPDATE	m08_t1_account_base_stage0
	SET 	box_type_subs = coalesce(bt.box_type, 'Unknown')
	from 	m08_t1_account_base_stage0 	as m08
			left join #box_type     as bt 
			on	m08.account_number = bt.account_number

	commit
	drop table #hda
	drop table #box_type
	drop table #stb_active
	commit
	MESSAGE cast(now() as timestamp)||' | @ M08.1: Deriving Box Type subs DONE' TO CLIENT
	
	--HD_box_subs
	
	update	m08_t1_account_base_stage0
    set  	HD_box_subs = 1
    where 	account_number in	(
									select	distinct 
											m08.account_number
									from  	m08_t1_account_base_stage0  as m08
											inner join #cshcompact	    as csh
											on	m08.account_number = csh.account_number
									where  	csh.subscription_sub_type = 'DTV HD' 
								)
								
								
	commit
	MESSAGE cast(now() as timestamp)||' | @ M08.1: Deriving HD sub DONE' TO CLIENT
	
	--RTM
	
	select	distinct
			account_number
            ,case 	when	(
								RTM not in (
												 'Direct Internet'
												,'Direct Internet Telephony'
												,'Direct Telephone'
												,'Events'
												,'Existing Customer Sales'
												,'Retail Independent'
												,'Retail Multiple'
												,'Sky Homes'
												,'Sky Retail Stores'
												,'Tesco'
												,'Walkers Cobra'
												,'Walkers North'
											)
								or RTM is null
							) 
					then	'Other' 
					else 	RTM 
			end 	as fix_rtm
	into	#temp_shelf
    from	(
				SELECT	base.account_number
						,RANK() OVER	(
											PARTITION BY	ord.account_number
											ORDER BY 		ord.cb_row_id ASC
										) 	AS rank
						,case	WHEN ord.currency_code = 'EUR' AND ord.route_to_market LIKE '%Direct%'                                    THEN 'ROI Direct'
								WHEN ord.currency_code = 'EUR'                                                                            THEN 'ROI Retail'
								WHEN ( ord.retailer_ASA_GROUP_NUMBER ) IN ('11164','11167') AND ord.retailer_ASA_BRANCH_NUMBER LIKE '8%'  THEN 'Tesco'
								WHEN ( ord.retailer_ASA_GROUP_NUMBER ) IN ('43000')                                                       THEN 'Events'
								WHEN ord.retailer_asa_group_number IN ('42000','48000')                                                   THEN 'Walkers North'
								WHEN ord.route_to_market LIKE '%Walkers%' OR ord.retailer_asa_group_number IN ('45000')                   THEN 'Walkers Cobra'
								WHEN ord.ROUTE_TO_MARKET = 'Direct'                                                                       THEN 'Direct Telephone'
								WHEN ord.route_to_market IN ('Direct Internet','Online')                                                  THEN 'Direct Internet'
								ELSE ord.route_to_market 
						END 	AS RTM
                FROM 	m08_t1_account_base_stage0    		AS base
						LEFT JOIN sk_prod.CUST_ORDER_DETAIL AS ord 
						ON	base.account_number = ord.account_number
                where 	base.cust_active_dt <= @profiling_day
            ) 	as base
    where 	rank = 1

	update	m08_t1_account_base_stage0	as m08
	set		rtm = shelf.fix_rtm
	from	#temp_shelf	as shelf
	where	m08.account_number = shelf.account_number
	
	commit
	drop table #temp_shelf
	commit
	
	MESSAGE cast(now() as timestamp)||' | @ M08.1: Deriving RTMs DONE' TO CLIENT
	
	--prem_sports
	--prem_movies
	
	select	csh.account_number
			,min(cel.prem_sports) prem_sports -- this is a nasty fix
			,min(cel.prem_movies) prem_movies -- need to notify this issue in the CSH table...
    into 	#premiums_lookup 
    from  	#cshcompact 						as csh
			inner join sk_prod.cust_entitlement_lookup 	as cel
			on	csh.current_short_description = cel.short_description
	WHERE	csh.subscription_sub_type ='DTV Primary Viewing'
	AND     csh.subscription_type = 'DTV PACKAGE'
	group 	by	csh.account_number
	
	update	m08_t1_account_base_stage0	as m08
	set		prem_sports		= prems.prem_sports
			,prem_movies	= prems.prem_movies
	from	#premiums_lookup			as prems
	where	m08.account_number = prems.account_number
	
	commit
	drop table #premiums_lookup
	commit
	
	MESSAGE cast(now() as timestamp)||' | @ M08.1: Deriving Premiums for Sports DONE' TO CLIENT
	MESSAGE cast(now() as timestamp)||' | @ M08.1: Deriving Premiums for Movies DONE' TO CLIENT
	
	
------------------------
-- M08.2 - QAing results
------------------------

----------------------------
-- M08.3 - Returning results
----------------------------
	
	drop table #cshcompact
	commit
	
-- m08_t1_account_base_stage0

	MESSAGE cast(now() as timestamp)||' | M08 Finished, table m08_t1_account_base_stage0 BUILT' TO CLIENT

    commit



end;

commit;
grant execute on sig_masvg_m08_account_base to vespa_group_low_security;
commit; /*


                         $$$
                        I$$$
                        I$$$
               $$$$$$$$ I$$$    $$$$$      $$$ZDD    DDDDDDD.
             ,$$$$$$$$  I$$$   $$$$$$$    $$$ ODD  ODDDZ 7DDDD
             ?$$$,      I$$$ $$$$. $$$$  $$$= ODD  DDD     NDD
              $$$$$$$$= I$$$$$$$    $$$$.$$$  ODD +DD$     +DD$
                  :$$$$~I$$$ $$$$    $$$$$$   ODD  DDN     NDD.
               ,.   $$$+I$$$  $$$$    $$$$=   ODD  NDDN   NDDN
              $$$$$$$$$ I$$$   $$$$   .$$$    ODD   ZDDDDDDDN
                                      $$$      .      $DDZ
                                     $$$             ,NDDDDDDD
                                    $$$?

                      CUSTOMER INTELLIGENCE SERVICES

--------------------------------------------------------------------------------------------------------------
**Project Name:							OPS 2.0
**Analysts:                             Angel Donnarumma (angel.donnarumma_mirabel@skyiq.co.uk)
**Lead(s):                              Jose Loureda
**Stakeholder:                          Operational Reports / SIG
**Due Date:                             20/09/2013
**Project Code (Insight Collation):     
**Sharepoint Folder:                    
                                                                        
**Business Brief:

**Modules:

	M09: MASVG Box View Generator
        M09.0 - Initialising environment
		M09.1 - Assembling SBV weekly snapshot
		M09.2 - Assembling SBV historical snapshot
        M09.2 - QAing results
		M09.3 - Setting Access Privileges
		M09.4 - Returning Results
		
**Sections:

--------------------------------------------------------------------------------------------------------------
*/


-----------------------------------
-- M09.0 - Initialising environment
-----------------------------------

create or replace procedure sig_masvg_m09_boxview_generator
as begin

    MESSAGE cast(now() as timestamp)||' | Beginig M09.0 - Initialising environment' TO CLIENT
    
    -- Local Variables
    declare @weekending_tag     varchar(8)
    declare @weeklyview_name    varchar(16)
    declare @weekending         date
    declare @sql_               varchar(5000)
    declare @profiling_thursday date
    declare @tname 				varchar(20)
	declare @hist_access		bit

    -- A Saturday...
    execute vespa_analysts.Regulars_Get_report_end_date @profiling_thursday output 
    -- so we make it the Thursday before...
    set @profiling_thursday    = dateadd(day, -8, @profiling_thursday)

    select  @weekending_tag =   case    when datepart(weekday,now()) = 7
                                        then dateformat( now(), 'YYYYMMDD')
                                        else dateformat((now() - datepart(weekday,now())), 'YYYYMMDD')
                                end
    select  @weekending =   case    when datepart(weekday,now()) = 7
                                    then dateformat( now(), 'YYYY-MM-DD')
                                    else dateformat((now() - datepart(weekday,now())), 'YYYY-MM-DD')
                            end
    --  Composing the name of the table
    set @weeklyview_name = 'SIG_SBV_'||@weekending_tag

   if object_id(@weeklyview_name) is not null --select 1 else select 0
        begin
            set @sql_ = 'drop table '||@weeklyview_name
            execute (@sql_)
            commit
        end
        
    set @sql_ = 'create table '||@weeklyview_name||'(

                    Weekending                      date            not null
                    ,Subscriber_id                  decimal(10)     default null
                    ,Card_Subscriber_ID             varchar(10)     default null
                    ,Account_number                 varchar(20)     not null
                    ,Service_instance_ID            varchar(30)     default null
                    ,consumerview_cb_row_id         bigint          default null
                    ,Panel                          varchar(10)     default null
                    ,Panel_ID_4_cells_confirm       bit             default 0
                    ,Is_Sky_view_candidate          bit             default 0
                    ,Is_Sky_view_selected           bit             default 0
                    ,status_vespa                   varchar(20)     default null
                    ,Enablement_date                date            default null
                    ,Enablement_date_source         varchar(20)     default null
                    ,vss_request_dt                 date            default null
                    ,Sky_view_load_date             date            default null
                    ,historic_result_date           date            default null
                    ,Selection_date                 date            default null
                    ,vss_created_date               date            default null
                    ,Num_logs_sent_30d              integer         default null
                    ,Num_logs_sent_7d               integer         default null
                    ,Continued_trans_30d            integer         default null
                    ,Continued_trans_7d             integer         default null
					,returned_data_30d				tinyint			default null
					,returned_data_7d    			tinyint			default null
                    ,reporting_quality              decimal(15,3)   default null
                    ,PS_Olive                       varchar(1)      default null
                    ,PS_vespa                       varchar(1)      default null
                    ,PS_inferred_primary            bit             default 0
                    ,ps_flag                        varchar(1)      default null
                    ,ps_source                      varchar(10)     default null
                    ,box_type_physical              varchar(20)     default null
                    ,HD_box_physical                bit             default 0
                    ,box_storage_capacity           varchar(20)     default null
                    ,Box_is_3D                      bit             default 0
                    ,Box_has_anytime_plus           bit             default 0
                    ,Scaling_segment_ID             integer         default null    
                    ,Non_Scaling_segment_ID         integer         default null
                    ,Box_model                      varchar(20)     default null
                    ,Adsmart_flag                   bit             default 0
					,description					varchar(50)		default null

                );

                create date index '||@weeklyview_name||'_dt1 on '||@weeklyview_name||'(weekending);
                create hg index '||@weeklyview_name||'_hg1 on '||@weeklyview_name||'(subscriber_id);
                create hg index '||@weeklyview_name||'_hg2 on '||@weeklyview_name||'(card_subscriber_id);
                create hg index '||@weeklyview_name||'_hg3 on '||@weeklyview_name||'(account_number);
                create hg index '||@weeklyview_name||'_hg4 on '||@weeklyview_name||'(Scaling_segment_ID);
                create hg index '||@weeklyview_name||'_hg5 on '||@weeklyview_name||'(Non_Scaling_segment_ID);'
            
    --select @sql_  
    execute (@sql_)

    commit
    MESSAGE cast(now() as timestamp)||' | @ M09.0: Table ' || @weeklyview_name || ' creation DONE' TO CLIENT

    drop view if exists SIG_SINGLE_BOX_VIEW
    commit
    
    set @sql_ = 'create view SIG_SINGLE_BOX_VIEW as
                    select * from '||@weeklyview_name

    execute (@sql_)
    
    grant select on SIG_SINGLE_BOX_VIEW to vespa_group_low_security
    
    commit
    MESSAGE cast(now() as timestamp)||' | @ M09.0: SIG_SINGLE_BOX_VIEW Refreshment (now pointing to' ||@weeklyview_name||') DONE' TO CLIENT
    
    MESSAGE cast(now() as timestamp)||' | @ M09.0: Initialisation DONE' TO CLIENT

-----------------------------------------
-- M09.1 - Assembling SBV weekly snapshot
-----------------------------------------

	MESSAGE cast(now() as timestamp)||' | Beginig M09.1 - Assembling SBV weekly snapshot' TO CLIENT

    set @sql_ = 'insert  into '||@weeklyview_name||'('||
                                                        'weekending'||
                                                        ',Subscriber_id'||
                                                        ',Card_Subscriber_ID'||
                                                        ',Account_number'||
                                                        ',Service_instance_ID'||
                                                        ',box_type_physical'||
                                                        ',HD_box_physical'||
                                                        ',box_storage_capacity'||
                                                        ',Box_is_3D'||
                                                        ',Box_has_anytime_plus'||
                                                        ',Box_model'||
                                                        ',Adsmart_flag'||
                                                        ',panel'||
                                                        ',Panel_ID_4_cells_confirm'||
                                                        ',Is_Sky_view_candidate'||
                                                        ',Is_Sky_view_selected'||
                                                        ',status_vespa'||
                                                        ',Enablement_date'||
                                                        ',Enablement_date_source'||
                                                        ',vss_request_dt'||
                                                        ',Sky_view_load_date'||
                                                        ',historic_result_date'||
                                                        ',Selection_date'||
                                                        ',vss_created_date'||
                                                        ',PS_Olive'||
                                                        ',PS_vespa'||
                                                        ',PS_inferred_primary'||
                                                        ',ps_flag'||
                                                        ',ps_source'||
                                                        ',Num_logs_sent_30d'||
                                                        ',Num_logs_sent_7d'||
                                                        ',Continued_trans_30d'||
                                                        ',Continued_trans_7d'||
                                                        ',reporting_quality'||
                                                        ',Scaling_segment_ID'||
                                                        ',Non_Scaling_segment_ID'||
														',description'||
														',returned_data_30d'||
														',returned_data_7d '||														
                                                    ')'||
                ' select  '''||@weekending||
                        ''',m07.Subscriber_id
                        ,m07.Card_Subscriber_ID
                        ,m07.Account_number
                        ,m07.Service_instance_ID
                        ,m07.box_type_physical
                        ,coalesce(m07.HD_box_physical,0)
                        ,m07.box_storage_capacity
                        ,coalesce(m07.Box_is_3D,0)
                        ,coalesce(m07.Box_has_anytime_plus,0)
                        ,m07.Box_model
                        ,coalesce(m07.Adsmart_flag,0)
                        ,m04.panel
                        ,coalesce(m04.Panel_ID_4_cells_confirm,0)
                        ,coalesce(m04.Is_Sky_view_candidate,0)
                        ,coalesce(m04.Is_Sky_view_selected,0)
                        ,m04.status_vespa           
                        ,m04.Enablement_date        
                        ,m04.Enablement_date_source 
                        ,m04.vss_request_dt         
                        ,m04.Sky_view_load_date     
                        ,m04.historic_result_date   
                        ,m04.Selection_date         
                        ,m04.vss_created_date
                        ,m04.PS_Olive
                        ,m04.PS_vespa
                        ,coalesce(m04.PS_inferred_primary,0) as PS_inferred_primary
                        ,m04.ps_flag
                        ,m04.ps_source
                        ,m05.Num_logs_sent_30d
                        ,m05.Num_logs_sent_7d
                        ,m05.Continued_trans_30d
                        ,m05.Continued_trans_7d
                        ,m05.reporting_quality
                        ,m06.Scaling_segment_ID     
                        ,m06.Non_Scaling_segment_ID
						,m07.description
						,m05.return_data_30d
						,m05.return_data_7d
                from    m07_t1_box_base_stage0                      as m07
                        left join m04_t1_panel_sample_stage0        as m04
                        on  m07.account_number      = m04.account_number
                        and m07.card_subscriber_id  = m04.card_subscriber_id
                        left join m05_t1_panel_performance_stage0   as m05
                        on  m07.account_number      = m05.account_number
                        and m07.subscriber_id  = m05.subscriber_id
                        left join m06_t1_panel_balance_stage0       as m06
                        on  m07.account_number      = m06.account_number'

    execute (@sql_)

    commit
               
    MESSAGE cast(now() as timestamp)||' | @ M09.1: Weekly Snapshot for Boxes DONE' TO CLIENT

---------------------------------------------
-- M09.2 - Assembling SBV historical snapshot
---------------------------------------------
	
	MESSAGE cast(now() as timestamp)||' | Beginig M09.2 - Assembling SBV historical snapshot' TO CLIENT
	
	
	if exists	(
					select  tname
					from    sys.syscatalog
					where   upper(tname) like 'SIG_SBV_%'
					and     cast(right(tname,8) as date) between cast((@weekending-105) as date) and cast((@weekending) as date)
					and		creator = 'vespa_analysts'
				)
	begin
			
		MESSAGE cast(now() as timestamp)||' | @ M09.2: Historical tables found' TO CLIENT
		
		-- dropping the view to refresh it
		drop view if exists SIG_SBV_HIST_VIEW 
		set @sql_ = ''
		commit
		
		-- querying the syscatalog to check what are the last 4 weeks available for the history
		declare thecursor cursor for        
        
			-- Creating a list of tables fit for historical view (only 3 month)...
			select  tname
			from    sys.syscatalog
			where   upper(tname) like 'SIG_SBV_%'
			and     cast(right(tname,8) as date) between cast((@weekending-105) as date) and cast((@weekending) as date)
			and		creator = 'vespa_analysts'
			order   by  tname
			
		for read only

		open thecursor
		fetch next thecursor into @tname
        
		while (sqlstate = 0)
		begin

			set @sql_ = @sql_ ||'select * from vespa_analysts.'||@tname|| ' union all '

			fetch next thecursor into @tname

		end

		deallocate thecursor
		
		set @sql_ = 'create view SIG_SBV_HIST_VIEW as '|| left(@sql_,(length(@sql_)-10))
        
		-- refreshing the view
		execute (@sql_)
		set @hist_access = 1
		commit
		
		MESSAGE cast(now() as timestamp)||' | @ M09.2: Historical Snapshot for Boxes DONE' TO CLIENT
	end
	else
	begin
		MESSAGE cast(now() as timestamp)||' | @ M09.2: No Historical Tables available, Historical View NOT Created' TO CLIENT
	end
	
	commit

	MESSAGE cast(now() as timestamp)||' | @ M09.2 - Assembling SBV historical snapshot DONE' TO CLIENT
	
------------------------
-- M09.3 - QAing results
------------------------

------------------------------------
-- M09.4 - Setting Access Privileges
------------------------------------

	MESSAGE cast(now() as timestamp)||' | Beginig M09.4 - Setting Access Privileges' TO CLIENT

    set @sql_ = 'grant select on '||@weeklyview_name||' to vespa_group_low_security'
    execute (@sql_)
    grant select on SIG_SINGLE_BOX_VIEW to vespa_group_low_security
	
	if @hist_access = 1
		grant select on SIG_SBV_HIST_VIEW	to vespa_group_low_security
    
	commit

	MESSAGE cast(now() as timestamp)||' | @ M09.4 - Setting Access Privileges DONE' TO CLIENT
	
----------------------------
-- M09.5 - Returning Results
----------------------------

    MESSAGE cast(now() as timestamp)||' | M09 Finished' TO CLIENT

    commit

end;

commit;
grant execute on sig_masvg_m09_boxview_generator to vespa_group_low_security;
commit; /*


                         $$$
                        I$$$
                        I$$$
               $$$$$$$$ I$$$    $$$$$      $$$ZDD    DDDDDDD.
             ,$$$$$$$$  I$$$   $$$$$$$    $$$ ODD  ODDDZ 7DDDD
             ?$$$,      I$$$ $$$$. $$$$  $$$= ODD  DDD     NDD
              $$$$$$$$= I$$$$$$$    $$$$.$$$  ODD +DD$     +DD$
                  :$$$$~I$$$ $$$$    $$$$$$   ODD  DDN     NDD.
               ,.   $$$+I$$$  $$$$    $$$$=   ODD  NDDN   NDDN
              $$$$$$$$$ I$$$   $$$$   .$$$    ODD   ZDDDDDDDN
                                      $$$      .      $DDZ
                                     $$$             ,NDDDDDDD
                                    $$$?

                      CUSTOMER INTELLIGENCE SERVICES

--------------------------------------------------------------------------------------------------------------
**Project Name:							OPS 2.0
**Analysts:                             Angel Donnarumma (angel.donnarumma_mirabel@skyiq.co.uk)
**Lead(s):                              Jose Loureda
**Stakeholder:                          Operational Reports / SIG
**Due Date:                             20/09/2013
**Project Code (Insight Collation):     
**Sharepoint Folder:                    
                                                                        
**Business Brief:

	This module assambles the optput from the production modules to generate the
	SIG_Single_Account_View and manage the history for the same context (Accounts)...

**Modules:

	M10: MASVG Account View Generator
        M10.0 - Initialising environment
		M10.1 - Assembling SAV weekly snapshot
		M10.2 - Assembling SAV historical snapshot
		M10.3 - DB Maintenance
        M10.4 - QAing results
		M10.5 - Setting Access Privileges
		M10.6 - Returning Results

**Sections:

--------------------------------------------------------------------------------------------------------------
*/

-----------------------------------
-- M10.0 - Initialising environment
-----------------------------------

create or replace procedure sig_masvg_m10_accountview_generator
as begin

	MESSAGE cast(now() as timestamp)||' | Beginig M10.0 - Initialising environment' TO CLIENT
    
    -- Local Variables
    declare @weekending_tag     varchar(8)
    declare @weeklyview_name    varchar(16)
    declare @weekending         date
    declare @sql_               varchar(6000)
    declare @profiling_thursday date
	declare @tname 				varchar(20)
	declare @hist_access		bit
	
    -- A Saturday...
    execute vespa_analysts.Regulars_Get_report_end_date @profiling_thursday output 
    -- so we make it the Thursday before...
    set @profiling_thursday    = dateadd(day, -8, @profiling_thursday)

    select  @weekending_tag =   case    when datepart(weekday,now()) = 7
                                        then dateformat( now(), 'YYYYMMDD')
                                        else dateformat((now() - datepart(weekday,now())), 'YYYYMMDD')
                                end
								
    select  @weekending =   case    when datepart(weekday,now()) = 7 then dateformat( now(), 'YYYY-MM-DD')
									else dateformat((now() - datepart(weekday,now())), 'YYYY-MM-DD')
							end
    --  Composing the name of the table
    set @weeklyview_name = 'SIG_SAV_'||@weekending_tag

    if object_id(@weeklyview_name) is not null --select 1 else select 0
        begin
            set @sql_ = 'drop table '||@weeklyview_name
            execute (@sql_)
            commit
        end


    set @sql_ = 'create table '||@weeklyview_name||'(

                    Weekending                  date            not null
                    ,Account_number             varchar(20)     not null
                    ,viewing_consent_flag       varchar(1)      default null
                    ,Scaling_segment_ID         integer         default null
                    ,Non_Scaling_segment_ID     integer         default null
                    ,Adsmart_flag               bit             default 0
                    ,Num_boxes                  tinyint         default 0
                    ,Num_adsmartable_boxes      tinyint         default 0
                    ,Panel                      varchar(10)     default null
                    ,Skygo_subs                 bit             default 0
                    ,Anytime_plus_subs          bit             default 0
                    ,Reporting_performance      varchar(20)     default null
                    ,Panel_recency              date            default null
                    ,avg_reporting_quality      decimal(15,3)   default null
                    ,min_reporting_quality      float           default null
					,scaling_reporting_quality	decimal(15,3)   default null
                    ,status_vespa               varchar(20)     default null
                    ,Panel_activation_date      date            default null
                    ,cb_key_individual          bigint          default null
                    ,Cust_active_DTV            bit             default 0
                    ,UK_Standard_account        varchar(3)      default null
                    ,box_type_subs              varchar(20)     default null
                    ,HD_box_subs                bit             default 0
                    ,RTM                        varchar(30)     default null
                    ,Weight                     float    		default null
					,viq_weight					float    		default null
                    ,Weight_date                date            default null
                    ,prem_sports                tinyint         default null
                    ,prem_movies                tinyint         default null
                    ,cust_active_dt             date            default null
					,num_ac_returned_30d		tinyint			default null
					,num_ac_returned_7d    		tinyint			default null
					,ac_full_returned_30d		bit				default 0
					,ac_full_returned_7d    	bit				default 0
					
                );

                create hg index '||@weeklyview_name||'_hg1 on '||@weeklyview_name||'(account_number);
                create hg index '||@weeklyview_name||'_hg2 on '||@weeklyview_name||'(Scaling_segment_ID);
                create hg index '||@weeklyview_name||'_hg3 on '||@weeklyview_name||'(Non_Scaling_segment_ID);'

    execute (@sql_)

    commit
    MESSAGE cast(now() as timestamp)||' | @ M10.0: Table ' || @weeklyview_name || ' creation DONE' TO CLIENT
    
    
    drop view if exists SIG_SINGLE_ACCOUNT_VIEW
    commit
    
    set @sql_ = 'create view SIG_SINGLE_ACCOUNT_VIEW as
                    select * from '||@weeklyview_name

    execute (@sql_)
    
    grant select on SIG_SINGLE_ACCOUNT_VIEW to vespa_group_low_security
    
    commit
    MESSAGE cast(now() as timestamp)||' | @ M10.0: SIG_SINGLE_ACCOUNT_VIEW Refreshment (now pointing to' ||@weeklyview_name||') DONE' TO CLIENT
    
    MESSAGE cast(now() as timestamp)||' | @ M10.0: Initialisation DONE' TO CLIENT
    
-----------------------------------------
-- M10.1 - Assembling SAV weekly snapshot
-----------------------------------------

    set @sql_ =     'insert into '||@weeklyview_name||' ('||
                                                                'weekending'||
                                                                ',account_number'||
                                                                ',viewing_consent_flag'||
                                                                ',Anytime_plus_subs'||
																',Reporting_performance'||
                                                                ',Cust_active_DTV'||
                                                                ',UK_Standard_account'||
                                                                ',box_type_subs'||
                                                                ',HD_box_subs'||
                                                                ',RTM'||
                                                                ',prem_sports'||
                                                                ',prem_movies'||
                                                                ',cust_active_dt'||
                                                                ',Scaling_segment_ID'||
                                                                ',Non_Scaling_segment_ID'||
                                                                ',Weight'||
                                                                ',Weight_date'||
                                                                ',adsmart_flag'||
                                                                ',num_boxes'||
                                                                ',Num_adsmartable_boxes'||
                                                                ',panel'||
																',status_vespa'||
																',num_ac_returned_7d'||
																',num_ac_returned_30d'||
																',avg_reporting_quality'||
																',min_reporting_quality'||
																',scaling_reporting_quality'||
																',viq_weight'||
																',ac_full_returned_30d'||
																',ac_full_returned_7d '||															
                                                            ')' ||
                    ' select  '''||@weekending||
                            ''',m08.account_number
                            ,m08.viewing_consent_flag
                            ,m08.Anytime_plus_subs
							,m05.Reporting_performance
                            ,m08.Cust_active_DTV
                            ,m08.UK_Standard_account
                            ,m08.box_type_subs
                            ,m08.HD_box_subs
                            ,m08.RTM
                            ,m08.prem_sports
                            ,m08.prem_movies
                            ,m08.cust_active_dt
                            ,m06.Scaling_segment_ID
                            ,m06.Non_Scaling_segment_ID
                            ,m06.Weight
                            ,m06.Weight_date
                            ,coalesce(m07.ads_flag,0) as adsmart_flag
                            ,m07.num_boxes
                            ,m07.Num_adsmartable_boxes
                            ,m04.panel
							,m04.status_vespa
							,m05.nacr7d
							,m05.nacr30d
							,m05.rep_qual
							,m05.min_reporting_quality
							,m05.rqs
							,m06.viq_weight
							,coalesce(m05.acf30d,0)
							,coalesce(m05.acf7d,0)
                    from    m08_t1_account_base_stage0              as m08
                            left join m06_t1_panel_balance_stage0   as m06
                            on  m08.account_number = m06.account_number
                            left join   (
                                            select  account_number
                                                    ,max(adsmart_flag)              as ads_flag
                                                    ,count(distinct subscriber_id)  as num_boxes
                                                    ,sum(adsmart_flag)              as Num_adsmartable_boxes
                                            from    m07_t1_box_base_stage0
                                            group   by  account_number
                                        )   as m07
                            on  m08.account_number = m07.account_number
                            left join   (
                                            select  account_number
                                                    ,panel
													,max(status_vespa)	as status_vespa
                                            from    m04_t1_panel_sample_stage0
                                            where   panel is not null
											group	by	account_number
														,panel
                                        )   as m04
                            on  m08.account_number = m04.account_number
                            left join   (
                                            select  mod05.account_number
                                                    ,avg(mod05.reporting_quality) as rep_qual
                                                    ,min(mod05.reporting_quality) as min_reporting_quality
                                                    ,case   when datediff(day, max(mod04.Enablement_date), '''||@profiling_thursday||''') < 15	then ''Recently enabled''
                                                            when min(mod05.return_data_30d) = 1                                           		then ''Acceptable''
                                                            when min(mod05.num_logs_sent_30d) >= 27 or min(mod05.reporting_quality) >= 0.9  	then ''Acceptable''
                                                            when max(mod05.num_logs_sent_30d) = 0                                           	then ''Zero reporting''
																																				else ''Unreliable''
                                                    end     as Reporting_performance
													,min(mod05.ac_full_returned_7d)		as acf7d
													,min(mod05.ac_full_returned_30d)	as acf30d
													,min(mod05.num_ac_returned_30d)		as nacr30d
													,min(mod05.num_ac_returned_7d)		as nacr7d
													,min(mod05.reporting_quality_s)	as rqs
                                            from    m05_t1_panel_performance_stage0         as mod05
                                                    inner join m04_t1_panel_sample_stage0   as mod04
                                                    on  mod05.account_number = mod04.account_number
                                            where   mod04.panel is not null
                                            group   by  mod05.account_number
                                        )   as m05
                            on  m08.account_number = m05.account_number'
	
	
    execute (@sql_)

    commit
    MESSAGE cast(now() as timestamp)||' | @ M10.1: Weekly Accounts Snapshot DONE' TO CLIENT


---------------------------------------------
-- M10.2 - Assembling SAV historical snapshot
---------------------------------------------
	
	MESSAGE cast(now() as timestamp)||' | Beginig M10.2 - Assembling SAV historical snapshot' TO CLIENT
	
	
	if exists	(
					select  tname
					from    sys.syscatalog
					where   upper(tname) like 'SIG_SAV_%'
					and     cast(right(tname,8) as date) between cast((@weekending-105) as date) and cast((@weekending) as date)
					and		creator = 'vespa_analysts'
				)
	begin
	
		MESSAGE cast(now() as timestamp)||' | @ M10.2: Historical tables found' TO CLIENT
	
		-- dropping the view to refresh it
		drop view if exists SIG_SAV_HIST_VIEW
		set @sql_ = ''	
		commit
		
		-- querying the syscatalog to check what are the last 4 weeks available for the history
		declare thecursor cursor for
			
			-- Creating a list of tables fit for historical view (only 3 month)...
			select  tname
			from    sys.syscatalog
			where   upper(tname) like 'SIG_SAV_%'
			and     cast(right(tname,8) as date) between cast((@weekending-105) as date) and cast((@weekending) as date)
			and		creator = 'vespa_analysts'
			order   by  tname
			
		for read only

		open thecursor
		fetch next thecursor into @tname

		while (sqlstate = 0)
		begin
			
			set @sql_ = @sql_ ||'select * from vespa_analysts.'||@tname|| ' union all '

			fetch next thecursor into @tname

		end

		deallocate thecursor

		set @sql_ = 'create view SIG_SAV_HIST_VIEW as '|| left(@sql_,(length(@sql_)-10))

		-- refreshing the view
		execute (@sql_)
		set @hist_access = 1
		commit
		
		MESSAGE cast(now() as timestamp)||' | @ M10.2: Historical Snapshot for Accounts DONE' TO CLIENT
	end
	else
	begin
		MESSAGE cast(now() as timestamp)||' | @ M10.2: No Historical Tables available, Historical View NOT Created' TO CLIENT
	end
	
	commit

	MESSAGE cast(now() as timestamp)||' | @ M10.2 - Assembling SAV historical snapshot DONE' TO CLIENT
	
-------------------------
-- M10.3 - DB Maintenance
-------------------------

	-- NYIP!
	-- dropping old weekly snapshot that are out-with the scope (anything older than 3 month)

------------------------
-- M10.4 - QAing results
------------------------

------------------------------------
-- M10.5 - Setting Access Privileges
------------------------------------

    set @sql_ = 'grant select on '||@weeklyview_name||' to vespa_group_low_security'
    execute (@sql_)
    grant select on SIG_SINGLE_ACCOUNT_VIEW to vespa_group_low_security
	grant select on SIG_SAV_HIST_VIEW 		to vespa_group_low_security
    commit

----------------------------
-- M10.6 - Returning results
----------------------------

    MESSAGE cast(now() as timestamp)||' | M10 Finished' TO CLIENT

    commit
	
end;

commit;
grant execute on sig_masvg_m10_accountview_generator to vespa_group_low_security;
commit; /*


                         $$$
                        I$$$
                        I$$$
               $$$$$$$$ I$$$    $$$$$      $$$ZDD    DDDDDDD.
             ,$$$$$$$$  I$$$   $$$$$$$    $$$ ODD  ODDDZ 7DDDD
             ?$$$,      I$$$ $$$$. $$$$  $$$= ODD  DDD     NDD
              $$$$$$$$= I$$$$$$$    $$$$.$$$  ODD +DD$     +DD$
                  :$$$$~I$$$ $$$$    $$$$$$   ODD  DDN     NDD.
               ,.   $$$+I$$$  $$$$    $$$$=   ODD  NDDN   NDDN
              $$$$$$$$$ I$$$   $$$$   .$$$    ODD   ZDDDDDDDN
                                      $$$      .      $DDZ
                                     $$$             ,NDDDDDDD
                                    $$$?

                      CUSTOMER INTELLIGENCE SERVICES

--------------------------------------------------------------------------------------------------------------
**Project Name:                                                 OPS 2.0
**Analysts:                             James McKane (james.mckane@skyiq.co.uk)
**Lead(s):                              Angel Donnarumma
**Stakeholder:                          Operational Reports / SIG
**Due Date:                             03/03/2014
**Project Code (Insight Collation):
**Sharepoint Folder:                    
                                                                        
**Business Brief:

        This module assembles the Panel balance Traffic Lights fur use in PanMan and Xdash reports

**Modules:

        M11: MASVG Panel Measurements
        M11.0 - Initialising environment
        M11.1 - VESPA Traffic Lights Created
        M11.2 - QAing results
        M11.3 - Setting Access Privileges
        M11.4 - Returning Results

**Stats:
	
	6 Minutes run... End-to-End...
--------------------------------------------------------------------------------------------------------------
*/

-----------------------------------
-- M11.0 - Initialising environment
-----------------------------------

create or replace procedure sig_masvg_m11_panel_measurement_generator
as begin


    MESSAGE cast(now() as timestamp)||' | Beginig M11.0 - Initialising environment' TO CLIENT
    

	-- Accounts and profiling:
	if object_id('Vespa_all_households') is not null
		drop table Vespa_all_households
		
	-- This guy eventually holds all households, not just those which have returned data,
	-- though it happens to get data return metrics on it as well.
	create table Vespa_all_households(
		account_number                  varchar(20)         not null primary key
		,hh_box_count                   tinyint             not null
		,most_recent_enablement         date                not null
		,reporting_categorisation       varchar(20)
		,panel                          varchar(10)
		,scaling_segment_ID             int
		,non_scaling_segment_ID         int
		,reporting_quality              float
	)

	commit
	grant select on Vespa_all_households to vespa_group_low_security
	commit
	
	MESSAGE cast(now() as timestamp)||' | @ M11.0: Table ''Vespa_all_households'' creation DONE' TO CLIENT    

	/*-*********** Segmentation management (except the names and lookups) **********-*/

	if object_id('Vespa_Scaling_Segment_Profiling') is not null
		drop table Vespa_Scaling_Segment_Profiling

	create table Vespa_Scaling_Segment_Profiling (
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
	)

	-- These guys needs their own indexes because we'll need to join through them:
	create hg index for_joining on Vespa_Scaling_Segment_Profiling (non_scaling_segment_id)
	commit
	grant select on Vespa_Scaling_Segment_Profiling to vespa_group_low_security
	commit

	MESSAGE cast(now() as timestamp)||' | @ M11.0: Table ''Vespa_Scaling_Segment_Profiling'' creation DONE' TO CLIENT

	/*-*************** QUASI-RESULTS STRUCTURES! ***************-*/

	-- So this table holds all the single variable aggregation results and for
	-- the result pluss, we just filter on the panel and aggregation variable
	-- to pull out what we need in each instance.
	if object_id('Vespa_all_aggregated_results') is not null
		drop table Vespa_all_aggregated_results
		
	create table Vespa_all_aggregated_results (
		panel                                               varchar(10)
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
	)

	commit
	grant select on Vespa_Scaling_Segment_Profiling to vespa_group_low_security
	commit

	MESSAGE cast(now() as timestamp)||' | @ M11.0: Table ''Vespa_all_aggregated_results'' creation DONE' TO CLIENT    

	/*-*************** Traffic Light Hist Table ***************-*/
	if object_id('vespa_traffic_lights_hist') is null
	create table vespa_traffic_lights_hist (
		 panel                                              varchar(10)
		,variable_name                                      varchar(30)
		,sequencer                                          int
		,imbalance_rating                                   float
		,weekending                                         date
	)

	commit
	grant select on vespa_traffic_lights_hist to vespa_group_low_security
	commit
	
	declare @profiling_day date
	-- so we're going to set this to last Thursday when everything was updated?
	-- SAV refresh permitting of course. Oh, hey, there's a cheap way of doing it;

	select @profiling_day = max(sbv.weekending)from SIG_SINGLE_BOX_VIEW as SBV

	declare @weekending date

	select @weekending =	case	when datepart(weekday,@profiling_day) = 7 then @profiling_day
									else (@profiling_day + (7 - datepart(weekday,@profiling_day))) 
							end

	if exists	(
					select	first *
					from 	vespa_traffic_lights_hist
					where 	weekending = @weekending
				)
	begin
		delete  from vespa_traffic_lights_hist where weekending = @weekending
		commit
	end

	MESSAGE cast(now() as timestamp)||' | @ M11.0: Initialisation DONE' TO CLIENT

	---------------------------------------
	-- M11.1 - VESPA Traffic Lights Created
	---------------------------------------


	--Acceptable Reporting - VESPA Panel
	insert into Vespa_all_households	(
											account_number
											,hh_box_count       -- not directly used? but might be interesting
											,most_recent_enablement
											,reporting_categorisation
											,reporting_quality
											,panel
											,scaling_segment_id
											,non_scaling_segment_id
									   )
	select	distinct sbv.account_number
			,count(1)
			,max(Enablement_date)
			,sav.reporting_performance
			,min(sbv.reporting_quality)   -- Used much later in the box selection bit, but may as well build it now
			,'DP'               -- This guy should be unique per account, we test for that coming off SBV
			,sav.scaling_segment_id
			,sav.non_scaling_segment_id
	from 	SIG_SINGLE_BOX_VIEW as SBV
			inner join SIG_SINGLE_ACCOUNT_VIEW as SAV
			on	sbv.account_number = sav.account_number
	where 	sav.panel in ('VESPA', 'VESPA11')
	and 	sbv.status_vespa = 'Enabled'
	group 	by	sbv.account_number
				,sav.reporting_performance
				,sav.panel
				,sav.scaling_segment_id
				,sav.non_scaling_segment_id

	MESSAGE cast(now() as timestamp)||' | @ M11.1: Acceptable Reporting - VESPA Panel DONE' TO CLIENT

	--Acceptable Reporting - ALT Panels
	insert into Vespa_all_households(
										account_number
										,hh_box_count       -- not directly used? but might be interesting
										,most_recent_enablement
										,reporting_categorisation
										,reporting_quality
										,panel
										,scaling_segment_id
										,non_scaling_segment_id
									)
	select	distinct sbv.account_number
			,count(1)
			,max(Enablement_date)
			,sav.reporting_performance
			,min(sbv.reporting_quality)   -- Used much later in the box selection bit, but may as well build it now
			,'AP'               -- This guy should be unique per account, we test for that coming off SBV
			,sav.scaling_segment_id
			,sav.non_scaling_segment_id
	from 	SIG_SINGLE_BOX_VIEW as SBV
			inner join SIG_SINGLE_ACCOUNT_VIEW as SAV
			on	sbv.account_number = sav.account_number
	where 	sav.panel in ('ALT5', 'ALT6', 'ALT7')
	and 	sbv.status_vespa = 'Enabled'
	group 	by	sbv.account_number
				,sav.reporting_performance
				,sav.panel
				,sav.scaling_segment_id
				,sav.non_scaling_segment_id

	MESSAGE cast(now() as timestamp)||' | @ M11.1: Acceptable Reporting - ALT Panels DONE' TO CLIENT

	-- So we need to start off with a view of the whole Sky base, and then add in the details for the stuff on each panel...
	select 	scaling_segment_id
			,non_scaling_segment_id
			,count(1) as Sky_Base_Households
	into 	#sky_base_segmentation
	from 	SIG_SINGLE_ACCOUNT_VIEW
	group 	by	scaling_segment_ID
				,non_scaling_segment_id
				
	-- It has to go into a temp table because we duplicate all these number for each panel

	commit

	--Panels Totals
	select	panel
			,count(1) as panel_reporters
	into 	#panel_totals
	from 	Vespa_all_households
	where 	reporting_categorisation = 'Acceptable'
	group 	by 	panel

	commit


	--Scaling Segment Profiling
	insert into Vespa_Scaling_Segment_Profiling	(
													panel
												   ,scaling_segment_id
												   ,non_scaling_segment_id
												   ,Sky_Base_Households
												)
	select 	pt.panel
			,sb.*
	from 	#sky_base_segmentation as sb
			cross join	(
							select  distinct panel
							from    Vespa_all_households
						)   as pt
	where 	scaling_segment_id is not null            -- segment data missing from approx. 250k accounts
	and 	non_scaling_segment_id is not null          -- segment data missing from approx. 250k accounts
	
	commit

	MESSAGE cast(now() as timestamp)||' | @ M11.1: Scaling Segment Profiling CREATED' TO CLIENT

	--Panel Segmentation
	select	panel
			,scaling_segment_id
			,non_scaling_segment_id
			,count(1) as Panel_Households
			,sum(case when reporting_categorisation = 'Acceptable'       then 1 else 0 end) as Acceptably_reliable_households
			,sum(case when reporting_categorisation = 'Unreliable'       then 1 else 0 end) as Unreliable_households
			,sum(case when reporting_categorisation = 'Zero reporting'   then 1 else 0 end) as Zero_reporting_households
			,sum(case when reporting_categorisation = 'Recently enabled' then 1 else 0 end) as Recently_enabled_households
	into 	#panel_segmentation
	from 	Vespa_all_households as hr
	where 	scaling_segment_ID is not null and non_scaling_segment_id is not null
	group 	by	panel
				,scaling_segment_ID
				,non_scaling_segment_id

	commit
	create unique index fake_pk on #panel_segmentation (panel, scaling_segment_id, non_scaling_segment_id)
	commit

	-- Now with the totals built for each panel, we can throw them into the table with the Sky base:
	update	Vespa_Scaling_Segment_Profiling
	set 	Panel_Households                = ps.Panel_Households
			,Acceptably_reliable_households = ps.Acceptably_reliable_households
			,Unreliable_households          = ps.Unreliable_households
			,Zero_reporting_households      = ps.Zero_reporting_households
			,Recently_enabled_households    = ps.Recently_enabled_households
	from 	Vespa_Scaling_Segment_Profiling
			inner join #panel_segmentation as ps 
			on	Vespa_Scaling_Segment_Profiling.panel                    = ps.panel
			and Vespa_Scaling_Segment_Profiling.scaling_segment_id       = ps.scaling_segment_id
			and Vespa_Scaling_Segment_Profiling.non_scaling_segment_id   = ps.non_scaling_segment_id

	commit
	drop table #sky_base_segmentation
	drop table #panel_segmentation


	-- We need the size of the sky base for indexing calculations
	declare @total_sky_base                 int

	select	@total_sky_base     = sum(Sky_Base_Households)
	from 	Vespa_Scaling_Segment_Profiling
	where 	panel ='DP'

	commit

	-- Patch in the scaling segment name from the lookup...
	update	Vespa_Scaling_Segment_Profiling
	set 	scaling_segment_name = ssl.scaling_segment_name
	from 	Vespa_Scaling_Segment_Profiling
			inner join vespa_analysts.SC2_Segments_Lookup_v2_1 as ssl 
			on	Vespa_Scaling_Segment_Profiling.scaling_segment_ID = ssl.scaling_segment_ID

	update	Vespa_Scaling_Segment_Profiling
	set 	non_scaling_segment_name = nss.non_scaling_segment_name
	from 	Vespa_Scaling_Segment_Profiling
			inner join vespa_analysts.sig_non_scaling_segments_lookup as nss 
			on	Vespa_Scaling_Segment_Profiling.non_scaling_segment_ID = nss.non_scaling_segment_ID

	MESSAGE cast(now() as timestamp)||' | @ M11.1: Scaling Segment Profiling DONE' TO CLIENT

	--Aggregated Results
	insert	into Vespa_all_aggregated_results
	select	ssp.panel
			,'UNIVERSE' -- Name of variable being profiled
			,1          -- Whether the variable is used for scaling or not (determintes the results sheet pull)
			,ssl.universe
			,sum(Sky_Base_Households)
			,sum(Panel_households)
			,sum(Acceptably_reliable_households)
			,sum(Unreliable_households)
			,sum(Zero_reporting_households)
			,sum(Recently_enabled_households)
			,null
	from 	Vespa_Scaling_Segment_Profiling as ssp
			inner join vespa_analysts.SC2_Segments_Lookup_v2_1 as ssl 
			on	ssp.scaling_segment_ID = ssl.scaling_segment_ID
	group 	by	ssp.panel
				,ssl.universe

	commit

	insert	into Vespa_all_aggregated_results
	select 	ssp.panel
			,'REGION'   -- Name of variable being profiled
			,1          -- Whether the variable is used for scaling or not (determintes the results sheet pull)
			,ssl.isba_tv_region
			,sum(Sky_Base_Households)
			,sum(Panel_households)
			,sum(Acceptably_reliable_households)
			,sum(Unreliable_households)
			,sum(Zero_reporting_households)
			,sum(Recently_enabled_households)
			,null
	from 	Vespa_Scaling_Segment_Profiling as ssp
			inner join vespa_analysts.SC2_Segments_Lookup_v2_1 as ssl 
			on	ssp.scaling_segment_ID = ssl.scaling_segment_ID
	group 	by	ssp.panel
				,ssl.isba_tv_region

	commit

	insert	into Vespa_all_aggregated_results
	select 	ssp.panel
			,'HHCOMP'
			,1
			,ssl.hhcomposition
			,sum(Sky_Base_Households)
			,sum(Panel_households)
			,sum(Acceptably_reliable_households)
			,sum(Unreliable_households)
			,sum(Zero_reporting_households)
			,sum(Recently_enabled_households)
			,null
	from 	Vespa_Scaling_Segment_Profiling as ssp
			inner join vespa_analysts.SC2_Segments_Lookup_v2_1 as ssl 
			on	ssp.scaling_segment_ID = ssl.scaling_segment_ID
	group 	by	ssp.panel
				,ssl.hhcomposition

	commit

	insert	into Vespa_all_aggregated_results
	select	ssp.panel
			,'PACKAGE'
			,1
			,ssl.package
			,sum(Sky_Base_Households)
			,sum(Panel_households)
			,sum(Acceptably_reliable_households)
			,sum(Unreliable_households)
			,sum(Zero_reporting_households)
			,sum(Recently_enabled_households)
			,null
	from 	Vespa_Scaling_Segment_Profiling as ssp 
			inner join vespa_analysts.SC2_Segments_Lookup_v2_1 as ssl 
			on	ssp.scaling_segment_ID = ssl.scaling_segment_ID
	group 	by	ssp.panel
				,ssl.package

	commit

	insert	into Vespa_all_aggregated_results
	select	ssp.panel
			,'TENURE'
			,1
			,ssl.tenure
			,sum(Sky_Base_Households)
			,sum(Panel_households)
			,sum(Acceptably_reliable_households)
			,sum(Unreliable_households)
			,sum(Zero_reporting_households)
			,sum(Recently_enabled_households)
			,null
	from 	Vespa_Scaling_Segment_Profiling as ssp
			inner join vespa_analysts.SC2_Segments_Lookup_v2_1 as ssl 
			on	ssp.scaling_segment_ID = ssl.scaling_segment_ID
	group 	by	ssp.panel
				,ssl.tenure

	commit

	insert	into Vespa_all_aggregated_results
	select 	ssp.panel
			,'BOXTYPE'
			,1
			,ssl.boxtype
			,sum(Sky_Base_Households)
			,sum(Panel_households)
			,sum(Acceptably_reliable_households)
			,sum(Unreliable_households)
			,sum(Zero_reporting_households)
			,sum(Recently_enabled_households)
			,null
	from 	Vespa_Scaling_Segment_Profiling as ssp
			inner join vespa_analysts.SC2_Segments_Lookup_v2_1 as ssl on ssp.scaling_segment_ID = ssl.scaling_segment_ID
	group 	by	ssp.panel
				,ssl.boxtype

	commit
	
	MESSAGE cast(now() as timestamp)||' | @ M11.1: Scaling Segment Aggregated Results DONE' TO CLIENT 

	-- Then other things that we're not scaling by, but we'd still like for panel balance:
	insert	into Vespa_all_aggregated_results
	select 	ssp.panel
			,'VALUESEG'
			,0          -- indicates we're not scaling by this, because these variables are pulled onto a different sheet
			,nss.value_segment
			,sum(Sky_Base_Households)
			,sum(Panel_households)
			,sum(Acceptably_reliable_households)
			,sum(Unreliable_households)
			,sum(Zero_reporting_households)
			,sum(Recently_enabled_households)
			,null
	from 	Vespa_Scaling_Segment_Profiling as ssp
			inner join vespa_analysts.sig_non_scaling_segments_lookup as nss 
			on	ssp.non_scaling_segment_ID = nss.non_scaling_segment_ID
	group 	by	ssp.panel
				,nss.value_segment

	commit

	insert	into Vespa_all_aggregated_results
	select 	ssp.panel
			,'MOSAIC'
			,0
			,nss.Mosaic_segment -- Special treatment for the MOSAIC segment names gets handled at the end
			,sum(Sky_Base_Households)
			,sum(Panel_households)
			,sum(Acceptably_reliable_households)
			,sum(Unreliable_households)
			,sum(Zero_reporting_households)
			,sum(Recently_enabled_households)
			,null
	from 	Vespa_Scaling_Segment_Profiling as ssp
			inner join vespa_analysts.sig_non_scaling_segments_lookup as nss 
			on	ssp.non_scaling_segment_ID = nss.non_scaling_segment_ID
	group 	by	ssp.panel
				,nss.Mosaic_segment

	commit

	insert	into Vespa_all_aggregated_results
	select 	ssp.panel
			,'FINANCIALSTRAT'
			,0
			,nss.Financial_strategy_segment
			,sum(Sky_Base_Households)
			,sum(Panel_households)
			,sum(Acceptably_reliable_households)
			,sum(Unreliable_households)
			,sum(Zero_reporting_households)
			,sum(Recently_enabled_households)
			,null
	from 	Vespa_Scaling_Segment_Profiling as ssp
			inner join vespa_analysts.sig_non_scaling_segments_lookup as nss 
			on	ssp.non_scaling_segment_ID = nss.non_scaling_segment_ID
	group 	by	ssp.panel
				,nss.Financial_strategy_segment

	commit

	insert	into Vespa_all_aggregated_results
	select 	ssp.panel
			,'ONNET'
			,0
			,case when nss.is_OnNet = 1 then '1.) OnNet' else '2.) OffNet' end
			,sum(Sky_Base_Households)
			,sum(Panel_households)
			,sum(Acceptably_reliable_households)
			,sum(Unreliable_households)
			,sum(Zero_reporting_households)
			,sum(Recently_enabled_households)
			,null
	from 	Vespa_Scaling_Segment_Profiling as ssp
			inner join vespa_analysts.sig_non_scaling_segments_lookup as nss 
			on	ssp.non_scaling_segment_ID = nss.non_scaling_segment_ID
	group 	by	ssp.panel
				,nss.is_OnNet

	commit

	insert	into Vespa_all_aggregated_results
	select 	ssp.panel
			,'SKYGO'
			,0
			,case when nss.uses_sky_go = 1 then '1.) Uses Sky Go' else '2.) No Sky Go' end
			,sum(Sky_Base_Households)
			,sum(Panel_households)
			,sum(Acceptably_reliable_households)
			,sum(Unreliable_households)
			,sum(Zero_reporting_households)
			,sum(Recently_enabled_households)
			,null
	from 	Vespa_Scaling_Segment_Profiling as ssp
			inner join vespa_analysts.sig_non_scaling_segments_lookup as NSS 
			on	ssp.non_scaling_segment_ID = nss.non_scaling_segment_ID
	group 	by	ssp.panel
				,nss.uses_sky_go

	commit

	MESSAGE cast(now() as timestamp)||' | @ M11.1: Non-Scaling Segment Aggregated Results DONE' TO CLIENT

	--Good Household Index
	update	Vespa_all_aggregated_results
	set 	Good_Household_Index =
							case    when pt.panel_reporters > 0  then 	(
																			case    when 200 < 100 * (Acceptable_Households) * @total_sky_base / convert(float, Sky_Base_Households) / pt.panel_reporters then 200
																					else       100 * (Acceptable_Households) * @total_sky_base / convert(float, Sky_Base_Households) / pt.panel_reporters
																			end
																		)
									else 0
							end
	from 	Vespa_all_aggregated_results
			left join #panel_totals as pt 
			on	Vespa_all_aggregated_results.panel = pt.panel

	MESSAGE cast(now() as timestamp)||' | @ M11.1: Good Household Index DONE' TO CLIENT


	--Traffic Lights Table
	insert	into vespa_traffic_lights_hist
	select  panel -- it gets denormalised in the extraction query though...
			,case aggregation_variable
				   when 'UNIVERSE'         then 'Universe'
				   when 'REGION'           then 'Region'
				   when 'HHCOMP'           then 'Household composition'
				   when 'PACKAGE'          then 'Package'
				   when 'TENURE'           then 'Tenure'
				   when 'BOXTYPE'          then 'Box type'
				   when 'VALUESEG'         then 'Value segment'
				   when 'MOSAIC'           then 'MOSAIC'
				   when 'FINANCIALSTRAT'   then 'FSS'
				   when 'ONNET'            then 'OnNet / Offnet'
				   when 'SKYGO'            then 'Sky Go users'
				   else 'FAIL!'
			end
			,case aggregation_variable
				   when 'UNIVERSE'         then 1
				   when 'REGION'           then 2
				   when 'HHCOMP'           then 3
				   when 'PACKAGE'          then 4
				   when 'TENURE'           then 5
				   when 'BOXTYPE'          then 6
				   when 'VALUESEG'         then 7
				   when 'MOSAIC'           then 8
				   when 'FINANCIALSTRAT'   then 9
				   when 'ONNET'            then 10
				   when 'SKYGO'            then 11
				   else -1
			end -- so the results go out into the excel thing in the right order
			,sqrt(avg((Good_Household_Index - 100) * (Good_Household_Index - 100)))
			,@weekending
	from 	Vespa_all_aggregated_results
	group 	by	panel
				,aggregation_variable

	commit

	if exists	(
					select	first *
					from  	vespa_traffic_lights_hist
					where 	weekending = @weekending
				)
		MESSAGE cast(now() as timestamp)||' | @ M11.1: Traffic Lights HIST COMPLETED' TO CLIENT
	else
		MESSAGE cast(now() as timestamp)||' | @ M11.1: Traffic Lights HIST INCOMPLETE' TO CLIENT

	commit

	MESSAGE cast(now() as timestamp)||' | @ M11.1: Traffic Lights DONE' TO CLIENT


	------------------------
	-- M11.2 - QAing results
	------------------------


		 

	------------------------------------
	-- M11.3 - Setting Access Privileges
	------------------------------------

	grant select on Vespa_all_households            to vespa_group_low_security
	grant select on Vespa_Scaling_Segment_Profiling to vespa_group_low_security
	grant select on Vespa_all_aggregated_results    to vespa_group_low_security
	grant select on vespa_traffic_lights_hist		to vespa_group_low_security
	commit
	

	MESSAGE cast(now() as timestamp)||' | @ M11.3: Setting Access Privileges DONE' TO CLIENT

	----------------------------
	-- M11.4 - Returning results
	----------------------------

	-- Project Vespa: Panel Management Report - traffic lights, showing balance of panel over each single variable
	
	/*
	select	weekending
			,variable_name
			,sum(case when panel = 'DP' then imbalance_rating else 0 end) 	as DP_Imbalance
			,sum(case when panel = 'AP' then imbalance_rating else 0 end)	as AP_Imbalance
	from 	vespa_traffic_lights_hist
	where 	weekending = (select max(weekending) from vespa_traffic_lights_hist)
	group 	by	variable_name, weekending
	order 	by 	min(sequencer)
	*/

    MESSAGE cast(now() as timestamp)||' | M11 Finished' TO CLIENT

    commit

end;

commit;
grant execute on sig_masvg_m11_panel_measurement_generator to vespa_group_low_security;
commit;
