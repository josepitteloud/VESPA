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
grant select on m08_t1_account_base_stage0			to vespa_group_low_security;