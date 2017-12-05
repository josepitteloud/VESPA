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
**Project Name:                                                 SkyView Futures
**Analysts:                             Angel Donnarumma
**Lead(s):                              Jason Thompson, Jose Pitteloud, Hoi Yu Tang
**Stakeholder:                          SkyIQ, Sky Media
**Due Date:                             31/07/2014
**Project Code (Insight Collation):     V289
**Sharepoint Folder:                    
                                                                        
**Business Brief:

This script embodies one module within the larger framework of a Household-to-Individual algorithm that takes
Vespa viewing data, and assigns an audience at the granularity of an individual viewer. Audience determination
begins by compilation of probability matrices based on BARB viewing data, where the viewing habits of individuals
are recorded. Monte Carlo selection of individuals available to a household based upon these probability matrices
then gives us gender and age assigns per Vespa viewing event.

The algorithm presented in this script forms one of the final stages within the household-to-individual 
algorithm, in that it takes as inputs the various probability matrices and viewing events with an assigned 
audience size, and determines the individuals that make up that audience.

**Sections:
        S0              - Initialise tables
        S1              - Initialise variables
        S2              - Produce date-wise and date-agnostic PIV matrices and re-normalise
        S3              - Prepare viewing data
        S4              - Assign audience for single-occupancy households and whole-household audiences
        S5              - Assign audience for non-lapping events
        S6              - Assign audience for overlapping events from the same account
        SXX             - Finish
        
**Notes:
        When testing and debugging the code in script-form, simply uncomment the block-commented query terminators 

--------------------------------------------------------------------------------------------------------------
*/

-- Create procedure
create or replace procedure v289_M10_individuals_selection as begin

MESSAGE cast(now() as timestamp)||' | M10 - Individuals assignment module start' TO CLIENT
commit --(^_^ )!


-- Initialise progress log and update

				if  exists(  select tname from syscatalog
                        where creator = user_name()
                        and upper(tname) = upper('V289_M10_log')
                        and     tabletype = 'TABLE')
				drop table V289_M10_log commit --(^_^ )!

create table V289_M10_log(
                section_id                      varchar(255)            null default null
        ,       dt_completed            datetime                        null default null
        ,       completed                       bit                     not null default 0
        )
commit --(^_^ )!

grant select on V289_M10_log to vespa_group_low_security commit --(^_^ )!

insert into V289_M10_log(section_id)
select section_id
from
        (
                select 'JOB START'																			as section_id, -1 as num
                union
                select 'S0 - INITIALISE TABLES'																as section_id, 0 as num
                union
                select 'S1 - INITIALISE VARIABLES'															as section_id, 1 as num
                union
                select 'S2 - Produce date-agnostic PIV and re-normalise'									as section_id, 2 as num
                union
                select 'S3 - PREPARE VIEWING DATA'															as section_id, 3 as num
                union
                select 'S4 - Assign audience for single-occupancy households and whole-household audiences'	as section_id, 4 as num
                union
                select 'S5 - Assign audience for non-overlapping events'									as section_id, 5 as num
                union
                select 'S6 - Assign audience for overlapping events from the same account'					as section_id, 6 as num
                union
                select 'SXX - FINISH'																		as section_id, 9999 as num
        )       as      t
order by num
commit --(^_^ )!

update V289_M10_log
set
                dt_completed    =       now()
        ,       completed               =       1
where section_id = 'JOB START'
commit --(^_^ )!



-- Check for empty input tables 
if
    (
                select
                        sum(
                                case
                                        when n > 0      then 1
                                        else 0
                                end
                                )
                from
                        (SELECT count() as n    from    V289_M07_dp_data
                                union
                                select count() as n     from    V289_M08_SKY_HH_composition
                                union
                                select count() as n     from    V289_PIV_Grouped_Segments_desc
                                union
                                select count() as n     from    v289_genderage_matrix
                        )       as      t
        ) < 4   
                begin
                        insert into V289_M10_log(
                                        section_id
                                ,       dt_completed
                                )
                        select 
                                        'At least one input table is empty! Please check data.'
                                ,       now()
                        commit
                        
                end -- if
commit --(^_^ )!

-------------------------
-------------------------
-- S0 - INITIALISE TABLES
-------------------------
-------------------------

-- The module dependencies, input and output tables are defined and described here.
MESSAGE cast(now() as timestamp)||' | M10 S0.0 - Initialise tables' TO CLIENT
commit --(^_^ )!

------------------------------------------------------------------
--      S0.1
--      Input tables - Just for definition of what data is required...
------------------------------------------------------------------
/*
[angeld].V289_M07_dp_data(      
                account_number          varchar(20) not null
        ,       subscriber_id           decimal(10) not null
        ,       event_id                        bigint          not null
        ,       event_Start_utc         timestamp       not null
        ,       event_end_utc           timestamp       not null
        ,       event_start_dim         int                     not null
        ,       event_end_dim           int                     not null
        ,       duration                        int                     not null
        ,       programme_genre         varchar(20)     default null
        ,       session_daypart         varchar(11)     default null
        ,       hhsize                          tinyint         default 0
        ,       channel_pack            varchar(40) default null
        ,       segment_id                      int                     default null
        ,       Overlap_batch           int                     default null
        ,       session_size            int                     default null
        )
;



-- The probability matrix for a particular age-gender combination to be in the audience
[angeld].v289_genderage_matrix(
                thedate
        ,       session_daypart
        ,       hhsize
        ,       channel_pack
        ,       programme_genre
        ,       household_number
        ,       person_number
        ,       sex
        ,       ageband
        ,       uk_hhwatched
        ,       segment_ID???
        ,       PIV
        )
;



-- Experian reference data
[thompsonja].V289_M08_SKY_HH_composition(
                row_id
        ,       account_number
        ,       cb_key_household
        ,       exp_cb_key_db_person
        ,       cb_key_individual
        ,       cb_key_db_person
        ,       db_address_line_1
        ,       HH_person_number
        ,       person_gender
        ,       person_age
        ,       person_ageband
        ,       exp_person_head
        ,       person_income
        ,       person_head
        ,       demographic_ID
        ,       Updated_On
        ,       Updated_By
        ,       household_size
        )
;



-- Segment definitions
[pitteloudj].V289_PIV_Grouped_Segments_desc(
                row_id
        ,       channel_pack
        ,       daypart
        ,       Genre
        ,       segment_id
        ,       active_flag
        ,       Updated_On
        ,       Updated_By
        )
;



-- Copy reference tables into local schema for testing
if object_id('V289_M07_dp_data') is not null drop table V289_M07_dp_data; 
select * into V289_M07_dp_data from [angeld].V289_M07_dp_data;

if object_id('v289_genderage_matrix') is not null drop table v289_genderage_matrix;
select * into v289_genderage_matrix from [angeld].v289_genderage_matrix;

if object_id('V289_M08_SKY_HH_composition') is not null drop table V289_M08_SKY_HH_composition;
select * into V289_M08_SKY_HH_composition from [angeld].V289_M08_SKY_HH_composition;

if object_id('V289_PIV_Grouped_Segments_desc') is not null drop table V289_PIV_Grouped_Segments_desc;
select * into V289_PIV_Grouped_Segments_desc from [angeld].V289_PIV_Grouped_Segments_desc;

*/



--------------------
--      S0.2
--      Transient tables
--------------------
MESSAGE cast(now() as timestamp)||' | M10 S0.2 - Initialise transient tables' TO CLIENT
commit --(^_^ )!



-- Combined event data that gives all possible audience individuals from the household of each viewing event and their PIV
				if  exists(  select tname from syscatalog
                        where creator = user_name()
                        and upper(tname) = upper('V289_M10_combined_event_data')
                        and     tabletype = 'TABLE')
				drop table V289_M10_combined_event_data commit --(^_^ )!

create table V289_M10_combined_event_data(
        account_number                  varchar(20)             not     null
        ,       hh_person_number                tinyint                 not     null
    ,   subscriber_id                   decimal(10)             not     null
    ,   event_id                                bigint                  not     null
    ,   event_start_utc                 datetime                not     null
    ,   chunk_start                             datetime            null    default null
    ,   overlap_batch                   int                          null   default null
    ,   programme_genre                 varchar(20)             null	 default null
    ,   session_daypart                 varchar(11)             null	 default null
    ,   channel_pack                    varchar(40)             null	 default null
    ,   segment_id                              int                             null	 default null
    ,   numrow                                  int                             not     null
    ,   session_size                    tinyint                 null 	default null
    ,   person_gender                   varchar(1)              null 	default null
    ,   person_ageband                  varchar(10)             null	default null
    ,   household_size                  tinyint                 default null	null
    ,   viewer_hhsize           		tinyint                 default null	null
    ,   assigned                bit             not null	 default 0
    ,   dt_assigned             datetime        null	default null
        ,       PIV                                             double                  null	default null
        ,       individuals_assigned    int                            not null	 default 0
        )
commit --(^_^ )!

create hg       index   V289_M10_combined_event_data_hg_idx_1   on V289_M10_combined_event_data(account_number) commit --(^_^ )!
create hg       index   V289_M10_combined_event_data_hg_idx_2   on V289_M10_combined_event_data(event_id) commit --(^_^ )!
create hg       index   V289_M10_combined_event_data_hg_idx_3   on V289_M10_combined_event_data(numrow) commit --(^_^ )!
create lf       index   V289_M10_combined_event_data_lf_idx_4   on V289_M10_combined_event_data(session_size) commit --(^_^ )!
create lf       index   V289_M10_combined_event_data_lf_idx_5   on V289_M10_combined_event_data(person_gender) commit --(^_^ )!
create lf       index   V289_M10_combined_event_data_lf_idx_6   on V289_M10_combined_event_data(person_ageband) commit --(^_^ )!
create lf       index   V289_M10_combined_event_data_lf_idx_7   on V289_M10_combined_event_data(household_size) commit --(^_^ )!
create lf       index   V289_M10_combined_event_data_lf_idx_8   on V289_M10_combined_event_data(viewer_hhsize) commit --(^_^ )!

grant select on V289_M10_combined_event_data to vespa_group_low_security commit --(^_^ )!




-- Date-agnostic gender-age PIV (default)

				if  exists(  select tname from syscatalog
                        where creator = user_name()
                        and upper(tname) = upper('V289_M10_PIV_default')
                        and     tabletype = 'TABLE')
				drop table V289_M10_PIV_default commit --(^_^ )!

create table V289_M10_PIV_default(
                hhsize                                          int                         null    default null
        ,       segment_id                                      int                         null    default null
        ,   sex                                                 varchar(10)             	null	default null
        ,   ageband                                             varchar(5)              	null	default null
        ,   sum_hours_watched                   				int                         null	default null
        ,   sum_hours_over_all_demog    						int                         null    default null
    ,   PIV_default                                     		double                  	null	default null
        )
commit --(^_^ )!


-- Date-agnostic gender-age PIV (by date)

				if  exists(  select tname from syscatalog
                        where creator = user_name()
                        and upper(tname) = upper('V289_M10_PIV_by_date')
                        and     tabletype = 'TABLE')
				drop table V289_M10_PIV_by_date commit --(^_^ )!

create table V289_M10_PIV_by_date(
                thedate                                         date                    null	default null
        ,       hhsize                                          int                     null	default null
        ,       segment_id                                      int                     null	default null
        ,   sex                                                 varchar(10)             null	default null
        ,   ageband                                             varchar(5)              null	default null
        ,   sum_hours_watched                   				int                		null	default null
        ,   sum_hours_over_all_demog    						int        				null	default null
    ,   PIV_by_date                                     		double             		null	default null
        )
commit --(^_^ )!



-- Working PIV matrix per event. This will be continually truncated and inserted into within the loop.
create table #working_PIV(
                account_number                  varchar(20)             not null
        ,       subscriber_id                   decimal(10)             null	default null
        ,       event_id                                bigint          null	default null
        ,       overlap_batch                   int                     null    default null    -- will only be used when assigning individuals to overlapping events
        ,       hh_person_number                tinyint                 not null    -- unique identifier of a person for a given account_number
        ,       cumsum_PIV                              double          null    default null    -- cumulative sum of PIV
        ,       norm_total                              double          null    default null    -- sum of PIV for normalisation
        ,       PIV_range                               double          null    default null    -- transformed PIV range covering all individuals
        ,       rnd                                     double          null    default null    -- random number between 0 and 1
        )
commit --(^_^ )!



-----------------
--      S0.3
--      Output tables
-----------------
MESSAGE cast(now() as timestamp)||' | M10 S0.3 - Initialise output tables' TO CLIENT
commit --(^_^ )!


-- Gender and age assignments per viewing event
-- This table will be continuously appended to as the audience individuals are assigned. 
-- It will only viewing events from the input table [V289_M07_dp_data] if certain conditions are met, such as there being a valid session_size etc.

				if  exists(  select tname from syscatalog
                        where creator = user_name()
                        and upper(tname) = upper('V289_M10_session_individuals')
                        and     tabletype = 'TABLE')
				drop table V289_M10_session_individuals
commit --(^_^ )!

create table V289_M10_session_individuals(
        event_date                      date         null   default null
    ,   event_id                        bigint       null 	default null
    ,   account_number                  varchar(20)  null   default null
    ,   overlap_batch                   int          null   default null
    ,   chunk_start                     datetime     null   default null
    ,   person_ageband          		varchar(5)   null	default null
    ,   person_gender           		varchar(10)  null 	default null
    ,   hh_person_number                tinyint      null   default null
    ,   last_modified_dt        		datetime     null	default null
	,	provider_id 					varchar(20)	 null	
	,	provider_id_number				integer		 null	
	,	viewing_type_flag				tinyint		 null			
		
		
		
    )
commit --(^_^ )!


create hg index                 V289_M10_session_individuals_hg_idx_1           on V289_M10_session_individuals(event_id) commit --(^_^ )!
create hg index                 V289_M10_session_individuals_hg_idx_2           on V289_M10_session_individuals(account_number) commit --(^_^ )!
create lf index                 V289_M10_session_individuals_lf_idx_3           on V289_M10_session_individuals(overlap_batch) commit --(^_^ )!
create dttm index               V289_M10_session_individuals_dttm_idx_4         on V289_M10_session_individuals(chunk_start) commit --(^_^ )!
create date index               V289_M10_session_individuals_dttm_idx_5         on V289_M10_session_individuals(event_date) commit --(^_^ )!
create dttm index               V289_M10_session_individuals_dttm_idx_6         on V289_M10_session_individuals(last_modified_dt) commit --(^_^ )!

grant select on V289_M10_session_individuals to vespa_group_low_security commit --(^_^ )!



-- Update log
update V289_M10_log
set
                dt_completed    = now()
        ,       completed               = 1
where section_id = 'S0 - INITIALISE TABLES'
commit --(^_^ )!



----------------------------
----------------------------
-- S1 - INITIALISE VARIABLES
----------------------------
----------------------------

-- Create and initialise variables
MESSAGE cast(now() as timestamp)||' | M10 S1.0 - Initialise variables' TO CLIENT
commit --(^_^ )!


-- -- For dev/testing in script form...
-- create variable      @total_number_of_events         int;                    -- the total number of viewing events - this will also act as the iterator limit
-- create variable @i                           int;                    -- loop counter for iterations over each unique viewing event/chunk
-- create variable @j                           tinyint;                -- loop counter for iterations over each audience member when assigning individuals to a viewing event
-- create variable @event_id                                    bigint;                 -- unique identifier of Vespa viewing event
-- create variable @account_number                              varchar(20);    -- account number
-- create variable @segment_id                  int;                    -- segment ID capturing the daypart, channel_pack and genre of the viewing event
-- create variable @session_size                int;                    -- the audience size for a given viewing event
-- create variable @household_size              tinyint;                -- the number of occupants associated with a customer account
-- create variable @overlap_batch               tinyint;                -- an identifier for concurrent events corresponding to a single account

-- create variable @j_person_gender                     varchar(6);             -- the gender of the j-th individual during the MC audience assignment process
-- create variable @j_person_ageband                    varchar(5);             -- the ageband of the j-th individual during the MC audience assignment process
-- create variable @j_hh_person_number                  tinyint;                -- the unique person identifier within a given household 

-- create variable      @max_household_size                     tinyint;                -- maximum allowable household_size to limit the iteration
-- set  @max_household_size     =       15;

-- create variable  @max_chunk_session_size    tinyint;        -- maximum chunk audience size to limit the iteration for overlapping events


-- For execution as a stored procedure...
declare @total_number_of_events         int
declare @i                                                      int
declare @j                                      tinyint
declare @event_id                                       bigint
declare @account_number                         varchar(20)
declare @segment_id                             int
declare @session_size                           int
declare @household_size                         tinyint
declare @overlap_batch                          tinyint

declare @j_person_gender                varchar(6)
declare @j_person_ageband               varchar(5)
declare @j_hh_person_number             tinyint

declare @max_household_size             tinyint
set     @max_household_size     =       15

declare @max_chunk_session_size    tinyint





-- Update log
update V289_M10_log
set
                dt_completed    = now()
        ,       completed               = 1
where section_id = 'S1 - INITIALISE VARIABLES'
commit --(^_^ )!





------------------------------------------------------------------------------------------------------------
------------------------------------------------------------------------------------------------------------
-- S2 - Produce date-agnostic PIV and re-normalise to create default matrix as well the probabilites by date
------------------------------------------------------------------------------------------------------------
------------------------------------------------------------------------------------------------------------

-------------------------------------------------------------------------------------------
--      S2.1
--      This produces a date-agnostic PIV, and can be treated as a "default" probability matrix
--      (We can derive this from the date-wise PIV in S2.2 if we calculate that first...)
-------------------------------------------------------------------------------------------
MESSAGE cast(now() as timestamp)||' | M10 S2.1 - Calculate default PIV' TO CLIENT
commit --(^_^ )!

-- Generate 1:15 vector to give household size dimension
SELECT	row_num		AS hhsize
INTO	#t15
FROM	sa_rowgenerator( 1, 15 )
commit --(^_^ )!


-- Generate segment ID vector
SELECT	DISTINCT	segment_id
INTO	#tseg
FROM	V289_PIV_Grouped_Segments_desc
commit --(^_^ )!


-- Generate gender-axe combinations and add kids
SELECT	DISTINCT
		sex
	,	ageband
INTO	#tsex
FROM	v289_genderage_matrix
commit --(^_^ )!
/*
INSERT INTO	#tsex
SELECT
		'Undefined'	as	sex
	,	'0-19'		as	ageband
commit --(^_^ )!
*/

-- Cross join to get all combinations and add default PIV value
insert into V289_M10_PIV_default (
		hhsize
	,	segment_id
    ,   sex
    ,   ageband
    ,   sum_hours_watched
    ,   sum_hours_over_all_demog
    ,   PIV_default
	)
SELECT
		hhsize
	,	segment_id
    ,   sex
    ,   ageband
    ,   0		AS sum_hours_watched
    ,   0		AS sum_hours_over_all_demog
    ,   1e-3	AS PIV_default
FROM
					#tseg	as	b
	CROSS JOIN      #tsex	as	c
	CROSS JOIN      #t15	as	d
commit --(^_^ )!


-- Remove unwanted gender-age combinations
DELETE FROM V289_M10_PIV_default
WHERE sex not like '%Undef%' AND ageband like '0-19%'
DELETE FROM V289_M10_PIV_default
WHERE sex like 'Female%' AND ageband IN ('0-11', '12-19')
commit --(^_^ )!


UPDATE V289_M10_PIV_default
SET sex = 'Undefined' 
WHERE ageband IN ('0-11', '12-19')

-- Calculate the default PIV from available BARB data
SELECT
		hhsize
	,	segment_id
	,	sex
	,	ageband
	,	sum_hours_watched
	,	sum_hours_over_all_demog
	,	PIV_default
INTO	#PIV
FROM
		(
			SELECT
					hhsize
				,	segment_id
				,	CAST	(CASE WHEN ageband  IN ('0-19','12-19','0-11')  THEN  'Undefined'
								  ELSE sex	end     AS VARCHAR(10))				as	sex
				,	ageband
				,	uk_hhwatched
				,	case
						when (uk_hhwatched = 0 or uk_hhwatched is null)	then	1e-3
						else													uk_hhwatched
					end																			as  uk_hhwatched_nonzero
				,	sum(uk_hhwatched_nonzero)	over	(
															partition by
																	segment_id
																,	hhsize
																,	sex
																,	ageband
														)										as	sum_hours_watched
				,	sum(uk_hhwatched_nonzero)	over    (
															partition by
																	segment_id
																,	hhsize
														)										as	sum_hours_over_all_demog
				,	1.0 * sum_hours_watched / sum_hours_over_all_demog							as	PIV_default
			FROM	v289_genderage_matrix
			WHERE	ageband	<>	'Undefined'
                        and     full_session_flag = 0 -- Only consider events where the session_size < viewer_hhsize
		)	as	t
GROUP BY 
		hhsize
	,	segment_id
	,	sex
	,	ageband
	,	sum_hours_watched
	,	sum_hours_over_all_demog
	,	PIV_default
commit --(^_^ )!

MESSAGE cast(now() as timestamp)||' #PIV generated: '||@@rowcount TO CLIENT
commit --(^_^ )!

-- Now insert the BARB-calculated default PIV values
UPDATE	V289_M10_PIV_default
SET
		a.sum_hours_watched			= 	j.sum_hours_watched
	,	a.sum_hours_over_all_demog	=	j.sum_hours_over_all_demog
	,	a.PIV_default				=	j.PIV_default
FROM
				V289_M10_PIV_default	AS	a
	INNER JOIN	#PIV					AS	j	ON	a.hhsize			=	j.hhsize 
												AND a.segment_id		=	j.segment_id
												AND LEFT(a.sex,1)		=	LEFT(j.sex,1)
												AND LEFT(a.ageband,2)	=	LEFT(j.ageband,2)
commit --(^_^ )!
        
MESSAGE cast(now() as timestamp)||' V289_M10_PIV_default Table Updated: ' ||@@rowcount TO CLIENT
commit --(^_^ )!




-----------------------------
--      S2.2
--      This produces PIV by date
-----------------------------
MESSAGE cast(now() as timestamp)||' | M10 S2.2 - Calculate date-wise PIV' TO CLIENT
commit --(^_^ )!

insert into V289_M10_PIV_by_date
select
		thedate
	,	hhsize
	,	segment_id
    ,	sex
    ,	ageband
    ,	sum_hours_watched
    ,	sum_hours_over_all_demog
    ,	PIV_by_date
from
		(
			select
					thedate
				,	hhsize
				,	segment_id
				,	case    when    ageband  IN ('0-19','12-19','0-11')  then    'Undefined'
							else    				sex
					end																	as      sex
				,	ageband
				,	uk_hhwatched
				,	case
						when (uk_hhwatched = 0 or uk_hhwatched is null) then    1e-3
						else    uk_hhwatched
					end                                                                 as  uk_hhwatched_nonzero
				,	sum(uk_hhwatched_nonzero)	over	(
															partition by
																	thedate
																,	hhsize
																,	segment_id
																,	sex
																,	ageband
														)								as      sum_hours_watched
				,	sum(uk_hhwatched_nonzero)	over    (
															partition by
																	thedate
																,	hhsize
																,	segment_id
														)								as      sum_hours_over_all_demog
				,	1.0 * sum_hours_watched / sum_hours_over_all_demog					as      PIV_by_date
			from    v289_genderage_matrix
			where   ageband	<>	'Undefined'
                        and     full_session_flag = 0 -- Only consider events where the session_size < viewer_hhsize
		)	as	t
group by
		thedate
	,	hhsize
	,	segment_id
    ,   sex
    ,   ageband
    ,   sum_hours_watched
    ,   sum_hours_over_all_demog
    ,   PIV_by_date
commit --(^_^ )!




/* Checks...

select top 20 * from V289_M10_PIV_default;

*/

-- Update log
update V289_M10_log
set
                dt_completed    = now()
        ,       completed               = 1
where section_id = 'S2 - Produce date-agnostic PIV and re-normalise'
commit --(^_^ )!


----------------------------
----------------------------
-- S3 - PREPARE VIEWING DATA
----------------------------
----------------------------

----------------------------------------------------------------------------------------------------------------
--      S3.1
--      Join events to Experian individual data to give all possible audience members within a single working table. 
--      We do this so as to avoid having to perform the same kind of join as we iterate through each viewing event.
----------------------------------------------------------------------------------------------------------------
MESSAGE cast(now() as timestamp)||' | M10 S3.1 - Join all possible individuals to viewing data' TO CLIENT
commit --(^_^ )!

insert into V289_M10_combined_event_data(
        account_number
	,	hh_person_number
    ,   subscriber_id
    ,   event_id
    ,   event_start_utc
    ,   chunk_start
    ,   overlap_batch
    ,   programme_genre
    ,   session_daypart
    ,   channel_pack
    ,   segment_id
    ,   numrow
    ,   session_size
    ,   person_gender
    ,   person_ageband
    ,   household_size
	,	viewer_hhsize
        )
select
        a.account_number
	,	b.hh_person_number
    ,   a.subscriber_id
    ,   a.event_id
    ,   a.event_start_utc
    ,   a.chunk_start
    ,   a.overlap_batch
    ,   a.programme_genre
    ,   a.session_daypart
    ,   a.channel_pack
    ,   a.segment_id
    ,   a.numrow
    ,   a.session_size
    ,   b.person_gender
    ,   b.person_ageband
    ,   a.hhsize
	,	a.viewer_hhsize
from
                (
                    select
                            account_number
                        ,   subscriber_id
                        ,   event_id
                        ,   event_start_utc
                        ,   chunk_start
                        ,   overlap_batch
                        ,   programme_genre
                        ,   session_daypart
                        ,   channel_pack
                        ,   segment_id
                        ,   session_size
                        ,   hhsize
						,	viewer_hhsize
                        ,   row_number()    over    (order by account_number, subscriber_id, event_id, overlap_batch)   as  numrow  -- won't need this anymore once we move away from an event-wise iteration
                    from    V289_M07_dp_data
                    where
                            session_size > 0                -- ignore events without an assign audience size
                        and segment_id is not null          -- ignore any events without a valid segment ID
                )   as  a
    inner join  (
                    select
                            account_number
						,	hh_person_number
						,	person_gender
						,	person_ageband
                        ,   count() over (partition by account_number)  as  valid_viewers
                    from    V289_M08_SKY_HH_composition
                    where
                                                                person_ageband is not null
                        -- and     person_gender <> 'U'
                        and     hh_person_number is not null
						and		non_viewer	=	0
						AND 	PANEL_FLAG  =	1
                )   as  b       on      a.account_number	=   b.account_number
								and     a.viewer_hhsize		=   b.valid_viewers
where   session_size <= a.viewer_hhsize
commit --(^_^ )!

MESSAGE cast(now() as timestamp)||' | M10 S3.1 - V289_M10_combined_event_data Table populated: '||@@rowcount TO CLIENT
commit --(^_^ )!



-- Initiate log of unique individuals/viewers to be used to reduce the number of unassigned viewers at the end of the process
				if  exists(  select tname from syscatalog
                        where creator = user_name()
                        and upper(tname) = upper('V289_M10_UNIQUE_VIEWERS_EVENT_ASSIGNMENT')
                        and     tabletype = 'TABLE')
				drop table V289_M10_UNIQUE_VIEWERS_EVENT_ASSIGNMENT
commit --(^_^ )!

SELECT
		ACCOUNT_NUMBER
	,	HH_PERSON_NUMBER
    ,	ACCOUNT_NUMBER || '-' || HH_PERSON_NUMBER   AS  PERSON_ID
    ,   0                                           AS  ASSIGNED
INTO    V289_M10_UNIQUE_VIEWERS_EVENT_ASSIGNMENT
FROM    V289_M10_combined_event_data
GROUP BY
		ACCOUNT_NUMBER
	,	HH_PERSON_NUMBER
	,	PERSON_ID
    ,   ASSIGNED
ORDER BY
		ACCOUNT_NUMBER
	,	HH_PERSON_NUMBER
	,	PERSON_ID
    ,   ASSIGNED
commit --(^_^ )!

CREATE UNIQUE HG INDEX UHG_IDX_1 ON V289_M10_UNIQUE_VIEWERS_EVENT_ASSIGNMENT(PERSON_ID)
commit --(^_^ )!
CREATE HG INDEX HG_IDX_1 ON V289_M10_UNIQUE_VIEWERS_EVENT_ASSIGNMENT(ACCOUNT_NUMBER)
commit --(^_^ )!
CREATE HG INDEX HG_IDX_2 ON V289_M10_UNIQUE_VIEWERS_EVENT_ASSIGNMENT(HH_PERSON_NUMBER)
commit --(^_^ )!


/*	-- For testing only ...
SELECT TOP 20 * FROM V289_M10_UNIQUE_VIEWERS_EVENT_ASSIGNMENT;

SELECT
		ASSIGNED
	,	COUNT()
FROM	V289_M10_UNIQUE_VIEWERS_EVENT_ASSIGNMENT
GROUP BY	ASSIGNED
ORDER BY	ASSIGNED
;

*/





/*
-------------------------------------------------------------------------------------------------------
--      S3.2
--      Filter: For overlapping events, keep only those where the total session sizes are <= household_size
-------------------------------------------------------------------------------------------------------


select
                account_number
        ,       household_size
        ,   overlap_batch
        ,   sum(session_size)   as  total_session_size
into    #V289_M10_overlap_accounts_to_remove
from    V289_M10_combined_event_data --V289_M07_dp_data
where   overlap_batch is not null
group by
                account_number
        ,       household_size
        ,   overlap_batch
having  total_session_size > household_size
commit --(^_^ )!

-- select count() from #V289_M10_overlap_accounts_to_remove;
-- select top 200 * from #V289_M10_overlap_accounts_to_remove;


-- Now remove those rows where these bad session_size assignments occur
delete from V289_M10_combined_event_data
from
                                V289_M10_combined_event_data            as      a
        inner join      #V289_M10_overlap_accounts_to_remove    as      b               on      a.account_number        = b.account_number
                                                                                                                                        and     a.overlap_batch         = b.overlap_batch
commit --(^_^ )!
*/



-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
--      S3.3
--      Filter: For overlapping batches, keep only those where the number of occurrences of any overlap_batch number is greater than the number of available boxes for that account
-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
MESSAGE cast(now() as timestamp)||' | M10 S3.3 - Filter out overlapping events with more overlaps than available STBs' TO CLIENT
commit --(^_^ )!
/*
select
                a.account_number
        ,       overlap_batch
    -- ,        boxes
        ,       count()         as      batch_occurances
into #V289_M10_batch_overcount
from
                                        V289_M10_combined_event_data    as      a
        inner join      (
                                select
                                account_number
                        ,       count(distinct subscriber_id)   as      boxes
                    from        V289_M10_combined_event_data
                    group by account_number
                )                                                                       as      b       on      a.account_number = b.account_number
where   overlap_batch is not null
group by
                a.account_number
        ,       overlap_batch
        ,       boxes
having  batch_occurances > boxes
commit --(^_^ )!

MESSAGE cast(now() as timestamp)||' | M10 S3.3 - #V289_M10_batch_overcount Table populated: '||@@rowcount TO CLIENT
commit --(^_^ )!

-- Now remove those rows where these apparent batch overcounts occur
delete from V289_M10_combined_event_data
from
                                V289_M10_combined_event_data            as      a
        inner join      #V289_M10_batch_overcount                               as      b               on      a.account_number        = b.account_number
                                                                                                                                        and     a.overlap_batch         = b.overlap_batch
commit --(^_^ )!
*/

SELECT DISTINCT  b.event_id
    , a.overlap_batch
    , a.account_number
    , a.subscriber_id
    , b.numrow
    , dense_rank() OVER (PARTITION BY         a.account_number         , a.overlap_batch        , a.subscriber_id ORDER BY  b.event_id DESC) AS rankk
INTO #temp_del
FROM	
		(
			SELECT
					account_number
				,	overlap_batch
				,	subscriber_id
				,	COUNT(DISTINCT event_id) hits
			FROM	V289_M10_combined_event_data
			WHERE	overlap_batch is not null
			GROUP BY
					account_number
				,	overlap_batch
				,	subscriber_id
			HAVING	hits > 1
		)                AS  a
		JOIN	V289_M10_combined_event_data   AS  b   	ON	a.overlap_batch = b.overlap_batch
														AND	a.account_number = b.account_number
commit --(^_^ )!

DELETE FROM V289_M10_combined_event_data
FROM
			V289_M10_combined_event_data   AS a
	JOIN	#temp_del                      AS b		ON a.overlap_batch = b.overlap_batch
													AND a.account_number = b.account_number
WHERE	rankk > 1
commit --(^_^ )!

MESSAGE cast(now() as timestamp)||' | M10 S3.3 - #V289_M10_batch_overcount overcounts removed: '||@@rowcount TO CLIENT
commit --(^_^ )!


--------------------------------------------------------------------------------------------
--      S3.4
-- Add the default PIV per individual, reverting to the latest current value where available
--------------------------------------------------------------------------------------------
MESSAGE cast(now() as timestamp)||' | M10 S3.4 - Append PIVs to individuals' TO CLIENT
commit --(^_^ )!

UPDATE	V289_M10_combined_event_data
SET		PIV	=   c.PIV_by_date   
FROM    V289_M10_combined_event_data            AS      a
JOIN    V289_M10_PIV_by_date                    AS      c       ON	DATE (a.event_start_utc) = c.thedate    
																AND a.segment_id = c.segment_id
																AND a.household_size = c.hhsize
																AND a.person_gender = left(c.sex,1)
																AND LEFT(a.person_ageband,2) = LEFT(c.ageband,2)
commit --(^_^ )!

SELECT DISTINCT event_id 
INTO #tev1
FROM V289_M10_combined_event_data
WHERE PIV is null 
commit --(^_^ )!

CREATE HG INDEX evi ON #tev1(event_id)
commit --(^_^ )!


UPDATE V289_M10_combined_event_data
SET             PIV =   b.PIV_default
FROM
			V289_M10_combined_event_data            AS	a       
	JOIN    #tev1									AS  z   ON  a.event_id = z.event_id                                                                                                     
	JOIN    V289_M10_PIV_default                    AS  b   ON	a.segment_id = b.segment_id             
															AND a.household_size = b.hhsize
															AND a.person_gender = left(b.sex,1)
															AND LEFT(a.person_ageband,2) = LEFT(b.ageband,2)
WHERE   a.PIV is null	-- This should be a redundant condition since it was already applied in creating #tev1
commit --(^_^ )!

DROP TABLE #tev1
commit --(^_^ )!

delete from V289_M10_combined_event_data
where PIV is null
commit --(^_^ )!


MESSAGE cast(now() as timestamp)||' | M10 S3.4 - Deleted from V289_M10_combined_event_data due to null PIV: '||@@rowcount TO CLIENT
commit --(^_^ )!


---------------------------------------------------------------------------------
--      S3.5
--      Delete rows where there are less expected individuals than the household size
---------------------------------------------------------------------------------
MESSAGE cast(now() as timestamp)||' | M10 S3.5 - Filter out accounts with fewer individuals than the expected household size' TO CLIENT
commit --(^_^ )!

select  event_id
into #tmp
from
        (
        select
                        *
                ,       count()         over    (partition by event_id) as      individuals_with_PIV
        from            V289_M10_combined_event_data
        )       as      t
where individuals_with_PIV < viewer_hhsize
commit --(^_^ )!

delete from V289_M10_combined_event_data
from
                                V289_M10_combined_event_data    as      a
        inner join      #tmp   									as      b       on      a.event_id = b.event_id
commit --(^_^ )!

MESSAGE cast(now() as timestamp)||' | M10 S3.5 - Deleted from V289_M10_combined_event_data due to less expected individuals than the household size: '||@@rowcount TO CLIENT
commit --(^_^ )!

drop table #tmp commit --(^_^ )!




---------------------------------------------------------------------------------------------
--      S3.6
--      Remove any results from the central output table for the dates that the input data covers
---------------------------------------------------------------------------------------------
MESSAGE cast(now() as timestamp)||' | M10 S3.6 - Delete existing results from current date of data' TO CLIENT
commit --(^_^ )!

delete from     V289_M10_session_individuals
where   event_date      in      (
                                                        select  date(event_start_utc)   as      event_date
                                                        from    V289_M10_combined_event_data
                                                        group by        event_date
                                                )
commit --(^_^ )!



--------------
--      S3.X
--      Update log
--------------
update V289_M10_log
set
                dt_completed    = now()
        ,       completed               = 1
where section_id = 'S3 - PREPARE VIEWING DATA'
commit --(^_^ )!




-------------------------------------------------------------------------------------
-------------------------------------------------------------------------------------
-- S4 - Assign audience for single-occupancy households and whole-household audiences
-------------------------------------------------------------------------------------
-------------------------------------------------------------------------------------
MESSAGE cast(now() as timestamp)||' | M10 S4.0 - Assign audience for single-occupancy households and whole-household audiences' TO CLIENT
commit --(^_^ )!

-- Simply append all of the household individuals per event to the output table
insert into V289_M10_session_individuals(
                event_date
        ,       event_id
        ,       account_number
    ,   person_ageband
    ,   person_gender
    ,   hh_person_number
        ,       last_modified_dt
    )
select
                date(event_start_utc)   as      event_date
    ,   event_id
        ,       account_number
    ,   person_ageband
    ,   person_gender
    ,   hh_person_number
        ,       now()                                   as      last_modified_dt
from    V289_M10_combined_event_data
where   viewer_hhsize = session_size
group by
                event_date
    ,   event_id
        ,       account_number
    ,   person_ageband
    ,   person_gender
    ,   hh_person_number
        ,       last_modified_dt
commit --(^_^ )!

MESSAGE cast(now() as timestamp)||' | M10 S4.0 - Individual assigned due to single-occupancy households and whole-household audiences'|| @@rowcount TO CLIENT
commit --(^_^ )!


-- 157976 Row(s) affected
-- ~0.58s


-- Update combined events table
update V289_M10_combined_event_data
set
		assigned				=	1
	,	dt_assigned				=	now()
	,	individuals_assigned	=	session_size
where	viewer_hhsize 	=	session_size
commit --(^_^ )!


-- Update log
update V289_M10_log
set
                dt_completed    = now()
        ,       completed               = 1
where section_id = 'S4 - Assign audience for single-occupancy households and whole-household audiences'
commit --(^_^ )!



-- Update the unique assigned individuals list
UPDATE	V289_M10_UNIQUE_VIEWERS_EVENT_ASSIGNMENT	AS	BAS
SET		ASSIGNED	=	1
FROM	(
            SELECT	ACCOUNT_NUMBER || '-' || HH_PERSON_NUMBER	AS	PERSON_ID
            FROM	V289_M10_session_individuals
            GROUP BY	PERSON_ID
    	)	AS	A
WHERE	BAS.PERSON_ID	=	A.PERSON_ID
commit --(^_^ )!

MESSAGE cast(now() as timestamp)||' | M10 S4.0 - Unique individuals assigned : '|| @@rowcount TO CLIENT
commit --(^_^ )!

--------------------------------------------------
--------------------------------------------------
-- S5 - Assign audience for non-overlapping events
--------------------------------------------------
--------------------------------------------------
MESSAGE cast(now() as timestamp)||' | M10 S5.0 - Assign audience for non-overlapping events' TO CLIENT
commit --(^_^ )!


---------------------------------------------------------------------------------
--      S5.01
--      Before Looping adjust PIV's on Segment level
---------------------------------------------------------------------------------


-- Get the age/gender PIV of each segment
select          segment_id
                ,household_size
                ,person_gender
                ,person_ageband
                ,min(piv) as seg_piv
into            #age_gender_pivs
from            V289_M10_combined_event_data
where           overlap_batch is null
and             individuals_assigned < session_size        -- select events that still need to be filled
and             assigned    =       0
group by        segment_id
                ,household_size
                ,person_gender
                ,person_ageband
commit


-- Get totaal age/gender PIV so we can normalise
select          segment_id
                ,household_size
                ,sum(seg_piv) as seg_tot_piv
into            #age_gender_tot_piv
from            #age_gender_pivs
group by        segment_id
                ,household_size
commit

-- Normalise PIV's
select          a.segment_id
                ,a.household_size
                ,a.person_gender
                ,a.person_ageband
                ,cast(seg_piv as double) / cast(seg_tot_piv as double) as normalised_piv
into            #age_gender_normalise_piv
from            #age_gender_pivs a
inner join      #age_gender_tot_piv b on a.segment_id = b.segment_id and a.household_size = b.household_size
commit

-- Get Total Target to assign for segment
select segment_id, household_size, sum(event_session) as segment_target
into            #session_targets
from
(select          segment_id
                ,household_size
                ,event_id
                ,min(session_size) as event_session
from            V289_M10_combined_event_data
where           overlap_batch is null
and             individuals_assigned < session_size        -- select events that still need to be filled
and             assigned    =       0
group by        segment_id
                ,household_size
                ,event_id
) a
group by segment_id, household_size
commit


-- Get Target at age/gender level
select          a.segment_id
                ,a.household_size
                ,a.person_gender
                ,a.person_ageband
                ,normalised_piv * segment_target as age_gender_target
into            #age_gender_targets
from            #age_gender_normalise_piv a
inner join      #session_targets b on a.segment_id = b.segment_id and a.household_size = b.household_size
commit

-- Get number of possible events that each age/gender could be assigned to
select          segment_id
                ,household_size
                ,person_gender
                ,person_ageband
                ,count(1) as possible_event_count
into            #age_gender_event_count
from            V289_M10_combined_event_data
where           overlap_batch is null
and             individuals_assigned < session_size        -- select events that still need to be filled
and             assigned    =       0
group by        segment_id
                ,household_size
                ,person_gender
                ,person_ageband
commit

-- Calculate new adjusted PIV [note that these can generate numbers > 1 which is OK]
select          a.segment_id
                ,a.household_size
                ,a.person_gender
                ,a.person_ageband
                ,cast(a.age_gender_target as double) / cast(b.possible_event_count as double) as new_piv
into            #new_pivs
from            #age_gender_targets a
inner join      #age_gender_event_count b on  a.segment_id      = b.segment_id
                                          and a.household_size  = b.household_size
                                          and a.person_gender   = b.person_gender
                                          and a.person_ageband  = b.person_ageband
commit

-- Now update the PIV with the new calculated PIVs
update          V289_M10_combined_event_data a
set             piv = new_piv
from            #new_pivs b
where           a.segment_id      = b.segment_id
                                          and a.household_size  = b.household_size
                                          and a.person_gender   = b.person_gender
                                          and a.person_ageband  = b.person_ageband
                                          and a.assigned = 0
                                          and a.overlap_batch is null
                                          and a.individuals_assigned < a.session_size
commit









---------------------------------------------------------------------------------
--      S5.1
--      Loop over individuals instead of events (first tackle non-overlapping events)
---------------------------------------------------------------------------------

set @i = 0 commit --(^_^ )!

while @i < @max_household_size begin

        set @i = @i + 1
		commit

          -- select @i
		MESSAGE cast(now() as timestamp)||' | M10 S5.0 - Assign audience for non-overlapping events. Iteration : ' || cast(@i as int) TO CLIENT
		commit

		MESSAGE
				cast(now() as timestamp)
			||	' | M10 S5.0 - Assign audience for non-overlapping events. Iteration : '
			||	cast(@i as int)
			||	'. Checkpoint A'
		TO CLIENT
		commit

        -- Get all events that (still) need an individual assigned, transform the available PIVs into a 0->1 range, then select an individual via MC

			-- First subquery renormalised the PIVs per event
			select
					bas.account_number
				,	bas.event_id
				,	bas.hh_person_number
				,	sum(bas.PIV)    over    (
												partition by bas.event_id
												rows between unbounded preceding and current row
											)													as  cumsum_PIV
				,	sum(bas.PIV)    over    (   partition by bas.event_id   )					as  norm_total
				,	cumsum_PIV / norm_total														as  PIV_range
				,	rand(bas.numrow	+	datepart(us,now()))										as  rnd
			into	#t1
			from	V289_M10_combined_event_data		as	bas
			where
						bas.overlap_batch is null							-- non-lapping events only
				and     bas.individuals_assigned	<	bas.session_size	-- select events that still need to be filled
				and     bas.assigned	=	0								-- select the individuals yet to be assigned
			commit
			
			create hg index #t1_hg_idx_1 on #t1(account_number) commit
			create hg index #t1_hg_idx_2 on #t1(event_id) commit


			-- rank by PIV and perform first level filter
			select
					*
				,   row_number() over (partition by event_id order by PIV_range)    as  rnk
			into	#t2
			from	#t1
			where   rnd < PIV_range

			create hg index #t2_hg_idx_1 on #t2(account_number) commit
			create hg index #t2_hg_idx_2 on #t2(event_id) commit
			
			drop table #t1
			commit

			-- Final filter to select an individual per event
			delete from #t2
			where	rnk <> 1
			commit


		-- Finally, update the working PIV
        insert into #working_PIV(
				account_number
			,	event_id
			,	hh_person_number
			,	cumsum_PIV
			,	norm_total
			,	PIV_range
			,	rnd
			)
        select
				account_number
			,	event_id
			,	hh_person_number
			,	cumsum_PIV
			,	norm_total
			,	PIV_range
			,	rnd     
     from	#t2
		commit


		drop table #t2
		commit


		MESSAGE
				cast(now() as timestamp)
			||	' | M10 S5.0 - Assign audience for non-overlapping events. Iteration : '
			||	cast(@i as int)
			||	'. Checkpoint B'
		TO CLIENT

        
		-- Now join back into the global combined viewing event data and update the individuals that have been assigned
		update V289_M10_combined_event_data
		set
				assigned = 1
			,   dt_assigned = now()
		from
						V289_M10_combined_event_data    as  a
			inner join  #working_PIV                                as  b   on  a.event_id = b.event_id
																	and a.account_number = b.account_number
																	and a.hh_person_number = b.hh_person_number
		commit
		
				MESSAGE
				cast(now() as timestamp)
			||	' | M10 S5.0 - Assign audience for non-overlapping events. Iteration : '
			||	cast(@i as int)
			||	'. Checkpoint C'
		TO CLIENT
		commit
		-- Update list of assigned individuals/viewers
		UPDATE	V289_M10_UNIQUE_VIEWERS_EVENT_ASSIGNMENT	AS	BAS
		SET		BAS.ASSIGNED	=	1
		FROM	(
					SELECT	ACCOUNT_NUMBER || '-' || HH_PERSON_NUMBER	AS	PERSON_ID
					FROM	V289_M10_combined_event_data
					WHERE	ASSIGNED	=	1
					GROUP BY	PERSON_ID
				)	AS	A
		WHERE	BAS.PERSON_ID	=	A.PERSON_ID
		COMMIT
		
/*	-- For testing only ...
		SELECT
				ASSIGNED
			,	COUNT()
		FROM	V289_M10_UNIQUE_VIEWERS_EVENT_ASSIGNMENT
		GROUP BY	ASSIGNED
		ORDER BY	ASSIGNED
		COMMIT
*/
			
        -- Clean up
        truncate table #working_PIV
		commit
		MESSAGE
				cast(now() as timestamp)
			||	' | M10 S5.0 - Assign audience for non-overlapping events. Iteration : '
			||	cast(@i as int)
			||	'. Checkpoint D'
		TO CLIENT
		commit
    
        -- Update number of assigned individuals per event
		update      V289_M10_combined_event_data
		set         individuals_assigned    =       b.total_assigned
        from
					V289_M10_combined_event_data	as	a
        inner join	(
                        select
                                        event_id
                            ,   sum(cast(assigned as int))      as      total_assigned
                        from    V289_M10_combined_event_data
                        where   overlap_batch is null
                        group by	event_id
					)								as	b       on	a.event_id	=	b.event_id
		commit
        
        		MESSAGE
				cast(now() as timestamp)
			||	' | M10 S5.0 - Assign audience for non-overlapping events. Iteration : '
			||	cast(@i as int)
			||	'. Checkpoint E'
		TO CLIENT
		commit
		
        -- Break out of while loop if assignments are complete
        -- if   (
		if  not     exists  (
								select  1
								from    V289_M10_combined_event_data
								where   
										overlap_batch is null
									and	individuals_assigned < session_size
							)       break

                                                                                                                                
end     -- while @i < @max_household_size begin
commit --(^_^ )!


------------------------------------------------------------------------
--      S5.2
--      Finally, remove the unassigned individuals from the processed events
------------------------------------------------------------------------
delete from V289_M10_combined_event_data
where   
                        overlap_batch is null
        and             individuals_assigned = session_size
        and             assigned = 0
commit --(^_^ )!



/* Checks
select
                dt_assigned
    ,   count()
from    V289_M10_combined_event_data
group by dt_assigned


*/



------------------------------------------------------------------
--      S5.3
--      Append results from non-overlapping events to the output table
------------------------------------------------------------------
insert into V289_M10_session_individuals(
                event_date
        ,       event_id
        ,       account_number
    ,   person_ageband
    ,   person_gender
    ,   hh_person_number
        ,       last_modified_dt
    )
select
                date(event_start_utc)   as      event_date
    ,   event_id
        ,       account_number
    ,   person_ageband
    ,   person_gender
    ,   hh_person_number
        ,       now()                                   as      last_modified_dt
from    V289_M10_combined_event_data
where   
						overlap_batch is null
        and             assigned = 1
        and not			(viewer_hhsize = session_size) -- these have already beeb added to the table
group by
                event_date
    ,   event_id
        ,       account_number
    ,   person_ageband
    ,   person_gender
    ,   hh_person_number
        ,       last_modified_dt
commit --(^_^ )!


-- Update the unique assigned individuals list (not really needed since this is updated within each iteration, but just leaving this here for safety for now)
UPDATE	V289_M10_UNIQUE_VIEWERS_EVENT_ASSIGNMENT	AS	BAS
SET		ASSIGNED	=	1
FROM	(
            SELECT	ACCOUNT_NUMBER || '-' || HH_PERSON_NUMBER	AS	PERSON_ID
            FROM	V289_M10_session_individuals
            GROUP BY	PERSON_ID
    	)	AS	A
WHERE	BAS.PERSON_ID	=	A.PERSON_ID
commit --(^_^ )!




--------------
--      S5.X
--      Update log
--------------
update V289_M10_log
set
                dt_completed    = now()
        ,       completed               = 1
where section_id = 'S5 - Assign audience for non-overlapping events'
commit --(^_^ )!




----------------------------------------------
----------------------------------------------
-- S6 - Assign audience for overlapping events
----------------------------------------------
----------------------------------------------
MESSAGE cast(now() as timestamp)||' | M10 S6.0 - Assign audience for overlapping events' TO CLIENT
commit --(^_^ )!


-----------------------------------------------------------------------------------------------------------
--      S6.1
--      Define the maximum number of iterations that we'll need to cover all individuals for overlapping events
-----------------------------------------------------------------------------------------------------------
MESSAGE cast(now() as timestamp)||' | M10 S6.0 - Assign audience for overlapping events - Calculate iteration limit.' TO CLIENT
commit --(^_^ )!
select @max_chunk_session_size = max(chunk_session_size)
from
    (
        select
                account_number
            ,   overlap_batch
            ,   sum(session_size)   as  chunk_session_size
        into    #chunk_sessions
        from    V289_M07_dp_data
        where   overlap_batch   is not null
        group by
                account_number
            ,   overlap_batch
    )   as  t
commit --(^_^ )!

-- select @max_chunk_session_size;



---------------------------------------------------------------------------------
--      S6.11
--      Before Looping adjust PIV's on Segment level For Overlapping Events
---------------------------------------------------------------------------------


-- Get the age/gender PIV of each segment
select          segment_id
                ,household_size
                ,person_gender
                ,person_ageband
                ,min(piv) as seg_piv
into            #age_gender_pivs_ov
from            V289_M10_combined_event_data
where           overlap_batch is not null
and             individuals_assigned < session_size        -- select events that still need to be filled
and             assigned    =       0
group by        segment_id
                ,household_size
                ,person_gender
                ,person_ageband
commit


-- Get totaal age/gender PIV so we can normalise
select          segment_id
                ,household_size
                ,sum(seg_piv) as seg_tot_piv
into            #age_gender_tot_piv_ov
from            #age_gender_pivs_ov
group by        segment_id
                ,household_size
commit

-- Normalise PIV's
select          a.segment_id
                ,a.household_size
                ,a.person_gender
                ,a.person_ageband
                ,cast(seg_piv as double) / cast(seg_tot_piv as double) as normalised_piv
into            #age_gender_normalise_piv_ov
from            #age_gender_pivs_ov a
inner join      #age_gender_tot_piv_ov b on a.segment_id = b.segment_id and a.household_size = b.household_size
commit

-- Get Total Target to assign for segment
select segment_id, household_size, sum(event_session) as segment_target
into            #session_targets_ov
from
(select          segment_id
                ,household_size
                ,event_id
                ,min(session_size) as event_session
from            V289_M10_combined_event_data
where           overlap_batch is not null
and             individuals_assigned < session_size        -- select events that still need to be filled
and             assigned    =       0
group by        segment_id
                ,household_size
                ,event_id
) a
group by segment_id, household_size
commit


-- Get Target at age/gender level
select          a.segment_id
                ,a.household_size
                ,a.person_gender
                ,a.person_ageband
                ,normalised_piv * segment_target as age_gender_target
into            #age_gender_targets_ov
from            #age_gender_normalise_piv_ov a
inner join      #session_targets_ov b on a.segment_id = b.segment_id and a.household_size = b.household_size
commit

-- Get number of possible events that each age/gender could be assigned to
select          segment_id
                ,household_size
                ,person_gender
                ,person_ageband
                ,count(1) as possible_event_count
into            #age_gender_event_count_ov
from            V289_M10_combined_event_data
where           overlap_batch is not null
and             individuals_assigned < session_size        -- select events that still need to be filled
and             assigned    =       0
group by        segment_id
                ,household_size
                ,person_gender
                ,person_ageband
commit

-- Calculate new adjusted PIV [note that these can generate numbers > 1 which is OK]
select          a.segment_id
                ,a.household_size
                ,a.person_gender
                ,a.person_ageband
                ,cast(a.age_gender_target as double) / cast(b.possible_event_count as double) as new_piv
into            #new_pivs_ov
from            #age_gender_targets_ov a
inner join      #age_gender_event_count_ov b on  a.segment_id      = b.segment_id
                                          and a.household_size  = b.household_size
                                          and a.person_gender   = b.person_gender
                                          and a.person_ageband  = b.person_ageband
commit

-- Now update the PIV with the new calculated PIVs
update          V289_M10_combined_event_data a
set             piv = new_piv
from            #new_pivs_ov b
where           a.segment_id      = b.segment_id
                                          and a.household_size  = b.household_size
                                          and a.person_gender   = b.person_gender
                                          and a.person_ageband  = b.person_ageband
                                          and a.assigned = 0
                                          and a.overlap_batch is not null
                                          and a.individuals_assigned < a.session_size
commit







------------------------------------------------------------
--      S6.2
--      Iterate over individuals per chunk of overlapping events
------------------------------------------------------------
MESSAGE cast(now() as timestamp)||' | M10 S6.2 - Iterate over individuals per chunk of overlapping events.' TO CLIENT
commit --(^_^ )!
set @i = 0 commit --(^_^ )!

while @i < @max_chunk_session_size  begin

        set @i = @i + 1
		commit

        -- select @i
				MESSAGE cast(now() as timestamp)||' | M10 S6.2 - Assign audience for overlapping events. Iteration : ' || cast(@i as int) TO CLIENT
		commit

		MESSAGE
				cast(now() as timestamp)
			||	' | M10 S6.2 - Assign audience for overlapping events. Iteration : '
			||	cast(@i as int)
			||	'. Checkpoint A'
		TO CLIENT
		commit
        
        -- Get all events that (still) need an individual assigned, transform the available PIVs into a 0->1 range, then select an individual via MC
     
			-- First subquery renormalised the PIVs per event

											select
													bas.account_number
												,	bas.subscriber_id
												,	bas.overlap_batch
												,	bas.hh_person_number
												,	sum(bas.PIV)    over    (
																				partition by bas.account_number, bas.overlap_batch
																				rows between unbounded preceding and current row
																			)                                                       as  cumsum_PIV
												,   sum(bas.PIV)    over    (   partition by account_number, overlap_batch   )		as  norm_total
												,   cumsum_PIV / norm_total                                                     as  PIV_range
												,   rand    (
																	bas.numrow
																+   bas.hh_person_number
																+   datepart(us,now())
															)																		as  rnd
											into	#t1
											from		V289_M10_combined_event_data		as	bas
											where
														bas.overlap_batch is not null						-- non-lapping events only
												and		bas.individuals_assigned	<	bas.session_size	-- select events that still need to be filled
												and		bas.assigned = 0									-- select the individuals yet to be assigned
				commit

			create hg index #t1_hg_idx_1 on #t1(account_number) commit


			-- rank by PIV and perform first level filter
			select
					*
				,	row_number() over (partition by account_number, overlap_batch       order by PIV_range)    as  rnk
			into	#t2
			from	#t1
			where   rnd	<	PIV_range
			commit


			drop table #t1
			commit
			
			-- Final filter to select an individual per event
			delete from #t2
			where	rnk <> 1
			commit


		-- Finally, update the working PIV
		insert into #working_PIV(
				account_number
			,	subscriber_id
			,	overlap_batch
			,	hh_person_number
			,	cumsum_PIV
			,	norm_total
			,	PIV_range
			,	rnd
			)
        select
				account_number
			,	subscriber_id
			,	overlap_batch
			,	hh_person_number
			,	cumsum_PIV
			,	norm_total
			,	PIV_range
			,	rnd     
        from	#t2
		commit


		drop table #t2
		commit


		MESSAGE
				cast(now() as timestamp)
			||	' | M10 S6.2 - Assign audience for overlapping events. Iteration : '
			||	cast(@i as int)
			||	'. Checkpoint B'
		TO CLIENT
		commit


		-- Now join back into the global combined viewing event data and update the individuals that have been assigned TO THAT PARTICULAR BOX
		update V289_M10_combined_event_data
		set
				assigned = 1
			,   dt_assigned = now()
		from
						V289_M10_combined_event_data	as  a
			inner join  #working_PIV					as  c		on  a.account_number    =   c.account_number
																	and a.subscriber_id     =   c.subscriber_id
																	and a.hh_person_number  =   c.hh_person_number
																	and a.overlap_batch     =   c.overlap_batch
		commit

    		MESSAGE
				cast(now() as timestamp)
			||	' | M10 S6.2 - Assign audience for overlapping events. Iteration : '
			||	cast(@i as int)
			||	'. Checkpoint C'
		TO CLIENT
		commit

		-- Update list of assigned individuals/viewers
		UPDATE	V289_M10_UNIQUE_VIEWERS_EVENT_ASSIGNMENT	AS	BAS
		SET		BAS.ASSIGNED	=	1
		FROM	(
					SELECT	ACCOUNT_NUMBER || '-' || HH_PERSON_NUMBER	AS	PERSON_ID
					FROM	V289_M10_combined_event_data
					WHERE	ASSIGNED	=	1
					GROUP BY	PERSON_ID
				)	AS	A
		WHERE	BAS.PERSON_ID	=	A.PERSON_ID
		COMMIT
		
/*	-- For testing only ...
		SELECT
				ASSIGNED
			,	COUNT()
		FROM	V289_M10_UNIQUE_VIEWERS_EVENT_ASSIGNMENT
		GROUP BY	ASSIGNED
		ORDER BY	ASSIGNED
		COMMIT
*/


		MESSAGE
				cast(now() as timestamp)
			||	' | M10 S6.2 - Assign audience for overlapping events. Iteration : '
			||	cast(@i as int)
			||	'. Checkpoint D'
		TO CLIENT
		commit
		
		-- Now we'll also need to remove those individuals to avoid being assigned to other overlapping events
		delete from V289_M10_combined_event_data
		from
						V289_M10_combined_event_data    as  a
			inner join  #working_PIV					as  c		on  a.account_number    =   c.account_number
																	and a.subscriber_id     <>  c.subscriber_id
																	and a.hh_person_number  =   c.hh_person_number
																	and a.overlap_batch     =   c.overlap_batch
		where   a.assigned = 0
		commit


		-- Clean up
		truncate table #working_PIV
		commit

		MESSAGE
				cast(now() as timestamp)
			||	' | M10 S6.2 - Assign audience for overlapping events. Iteration : '
			||	cast(@i as int)
			||	'. Checkpoint E'
		TO CLIENT
		commit
		-- Update number of assigned individuals per account_number-overlap_batch combination
		update  V289_M10_combined_event_data
		set     individuals_assigned    =   b.total_assigned
		from
						V289_M10_combined_event_data    as  a
			inner join  (
							select
									account_number
								,   subscriber_id
								,   overlap_batch
								,   sum(cast(assigned as int))  as  total_assigned
							from    V289_M10_combined_event_data
							where   overlap_batch is not null
							group by
									account_number
								,   subscriber_id
								,   overlap_batch
						)								as  b   on  a.account_number    =   b.account_number
																and a.subscriber_id     =   b.subscriber_id
																and a.overlap_batch     =   b.overlap_batch
		commit

		MESSAGE
				cast(now() as timestamp)
			||	' | M10 S6.2 - Assign audience for overlapping events. Iteration : '
			||	cast(@i as int)
			||	'. Checkpoint F'
		TO CLIENT
		commit
		-- Break out of while loop if assignments are complete
		if  not exists      (
								select  1
								from    V289_M10_combined_event_data
								where
											overlap_batch is not null
									and		individuals_assigned < session_size
							)       break

end -- @i < @max_household_size begin
commit --(^_^ )!



------------------------------------------------------------------
--      S6.3
--      Append results from non-overlapping events to the output table
------------------------------------------------------------------
insert into V289_M10_session_individuals(
                event_date
        ,       event_id
        ,       account_number
        ,       overlap_batch
    ,   person_ageband
    ,   person_gender
    ,   hh_person_number
        ,       last_modified_dt
    )
select
                date(event_start_utc)   as      event_date
    ,   event_id
        ,       account_number
        ,       overlap_batch
    ,   person_ageband
    ,   person_gender
    ,   hh_person_number
        ,       now()                                   as      last_modified_dt
from    V289_M10_combined_event_data
where   
                        overlap_batch is not null
        and             assigned = 1
group by
                event_date
    ,   event_id
        ,       account_number
        ,       overlap_batch
    ,   person_ageband
    ,   person_gender
    ,   hh_person_number
        ,       last_modified_dt
commit --(^_^ )!




--------------
--      S6.X
--      Update log
--------------
update V289_M10_log
set
                dt_completed    = now()
        ,       completed               = 1
where section_id = 'S6 - Assign audience for overlapping events from the same account'
commit --(^_^ )!





------------------------------------------------------------------------------------------
--		S7.0
--		Assign remaining unassigned viewers to their most likely event using a second pass
------------------------------------------------------------------------------------------

-- Update the unique assigned individuals list (not really needed since this is updated within each iteration, but just leaving this here for safety for now)
UPDATE	V289_M10_UNIQUE_VIEWERS_EVENT_ASSIGNMENT	AS	BAS
SET		ASSIGNED	=	1
FROM	(
            SELECT	ACCOUNT_NUMBER || '-' || HH_PERSON_NUMBER	AS	PERSON_ID
            FROM	V289_M10_session_individuals
            GROUP BY	PERSON_ID
    	)	AS	A
WHERE	BAS.PERSON_ID	=	A.PERSON_ID
commit --(^_^ )!



/* Viewer adjustment quick results from 5% sample
 SELECT
		ASSIGNED
	,	COUNT()
FROM	V289_M10_UNIQUE_VIEWERS_EVENT_ASSIGNMENT
GROUP BY	ASSIGNED
ORDER BY	ASSIGNED
;

-- Before:
-- ASSIGNED    COUNT()
-- 0   1563
-- 1   54765
-- 
-- After:
-- -- ASSIGNED    COUNT()
-- 0   1117
-- 1   55211


SELECT  TOP 20 *
FROM    V289_M10_UNIQUE_VIEWERS_EVENT_ASSIGNMENT
WHERE   ASSIGNED = 0
;


*/


-- Combined event data that gives all possible audience individuals from the household of each viewing event and their PIV
				if  exists(  select tname from syscatalog
                        where creator = user_name()
                        and upper(tname) = upper('V289_M10_combined_event_data_adj')
                        and     tabletype = 'TABLE')
				drop table V289_M10_combined_event_data_adj commit --(^_^ )!

create table V289_M10_combined_event_data_adj(
        account_number                  varchar(20)             not     null
	,	hh_person_number                tinyint                 not     null
    ,   subscriber_id                   decimal(10)             not     null
    ,   event_id                        bigint                  not     null
    ,   event_start_utc                 datetime                not     null
    ,   chunk_start                     datetime                null	default null
    ,   overlap_batch                   int                     null    default null
    ,   programme_genre                 varchar(20)             null	default null
    ,   session_daypart                 varchar(11)             null	default null
    ,   channel_pack                    varchar(40)             null	default null
    ,   segment_id                      int                     null       default null
    ,   numrow                         	int                     not     null
    ,   session_size                    tinyint                 null	default null
    ,   person_gender                   varchar(1)              null	default null
    ,   person_ageband                  varchar(10)             null	default null
    ,   household_size                  tinyint                 default null	null
    ,   viewer_hhsize           		tinyint                 default null	null
    ,   assigned                		bit             		not null	default 0
    ,   dt_assigned             		datetime        		default null
        ,       PIV                     double                  null	default null
        ,       individuals_assigned    int                     not null        default 0
        )
commit --(^_^ )!

create hg       index   V289_M10_combined_event_data_adj_hg_idx_1   on V289_M10_combined_event_data_adj(account_number) commit --(^_^ )!
create hg       index   V289_M10_combined_event_data_adj_hg_idx_2   on V289_M10_combined_event_data_adj(event_id) commit --(^_^ )!
create hg       index   V289_M10_combined_event_data_adj_hg_idx_3   on V289_M10_combined_event_data_adj(numrow) commit --(^_^ )!
create lf       index   V289_M10_combined_event_data_adj_lf_idx_4   on V289_M10_combined_event_data_adj(session_size) commit --(^_^ )!
create lf       index   V289_M10_combined_event_data_adj_lf_idx_5   on V289_M10_combined_event_data_adj(person_gender) commit --(^_^ )!
create lf       index   V289_M10_combined_event_data_adj_lf_idx_6   on V289_M10_combined_event_data_adj(person_ageband) commit --(^_^ )!
create lf       index   V289_M10_combined_event_data_adj_lf_idx_7   on V289_M10_combined_event_data_adj(household_size) commit --(^_^ )!
create lf       index   V289_M10_combined_event_data_adj_lf_idx_8   on V289_M10_combined_event_data_adj(viewer_hhsize) commit --(^_^ )!

grant select on V289_M10_combined_event_data_adj to vespa_group_low_security commit --(^_^ )!


-- truncate table V289_M10_combined_event_data_adj;

MESSAGE cast(now() as timestamp)||' | M10 S7.1 - Join all possible individuals to viewing data' TO CLIENT
commit --(^_^ )!


insert into V289_M10_combined_event_data_adj(
        account_number
	,	hh_person_number
    ,   subscriber_id
    ,   event_id
    ,   event_start_utc
    ,   chunk_start
    ,   overlap_batch
    ,   programme_genre
    ,   session_daypart
    ,   channel_pack
    ,   segment_id
    ,   numrow
    ,   session_size
    ,   person_gender
    ,   person_ageband
    ,   household_size
	,	viewer_hhsize
        )
select
        a.account_number
	,	b.hh_person_number
    ,   a.subscriber_id
    ,   a.event_id
    ,   a.event_start_utc
    ,   a.chunk_start
    ,   a.overlap_batch
    ,   a.programme_genre
    ,   a.session_daypart
    ,   a.channel_pack
    ,   a.segment_id
    ,   a.numrow
    ,   a.session_size
    ,   b.person_gender
    ,   b.person_ageband
    ,   a.hhsize
	,	a.viewer_hhsize
from
                (
                    select
                            account_number
                        ,   subscriber_id
                        ,   event_id
                        ,   event_start_utc
                        ,   chunk_start
                        ,   overlap_batch
                        ,   programme_genre
                        ,   session_daypart
                        ,   channel_pack
                        ,   segment_id
                        ,   session_size
                        ,   hhsize
						,	viewer_hhsize
                        ,   row_number()    over    (order by account_number, subscriber_id, event_id, overlap_batch)   as  numrow  -- won't need this anymore once we move away from an event-wise iteration
                    from    V289_M07_dp_data
                    where
                            session_size > 0                -- ignore events without an assign audience size
                        and segment_id is not null          -- ignore any events without a valid segment ID
                )   								as  a
    inner join  (
                    select
                            account_number
						,	hh_person_number
						,	person_gender
						,	person_ageband
                        ,   count() over (partition by account_number)  as  valid_viewers
                    from    V289_M08_SKY_HH_composition
                    where
                                                                person_ageband is not null
                        -- and     person_gender <> 'U'
                        and     hh_person_number is not null
						and		non_viewer	=	0
						AND 	PANEL_FLAG  =   1
                )   								as  b       on      a.account_number    =   b.account_number
																and     a.viewer_hhsize		=   b.valid_viewers
	inner join	V289_M10_UNIQUE_VIEWERS_EVENT_ASSIGNMENT	as	c		on		c.assigned			=	0		-- This inner join ensures that we only get combinations from remaining unassigned viewers
																		and		a.account_number	=	c.account_number
																		and		b.hh_person_number	=	c.hh_person_number
where   session_size <= a.viewer_hhsize
commit --(^_^ )!

MESSAGE cast(now() as timestamp)||' | M10 S7.1 - V289_M10_combined_event_data_adj Table populated: '||@@rowcount TO CLIENT
commit --(^_^ )!




--------------------------------------------------------------------------------------------
--  S7.1    (S3.4)
-- Add the default PIV per individual, reverting to the latest current value where available
--------------------------------------------------------------------------------------------
MESSAGE cast(now() as timestamp)||' | M10 S7.4 - Append PIVs to individuals' TO CLIENT
commit --(^_^ )!

UPDATE V289_M10_combined_event_data_adj
SET             PIV =   c.PIV_by_date   
FROM    V289_M10_combined_event_data_adj            AS      a
JOIN    V289_M10_PIV_by_date                    AS      c       ON      DATE (a.event_start_utc) = c.thedate    
                                                                                                                AND a.segment_id = c.segment_id
                                                                                                                AND a.household_size = c.hhsize
                                                                                                                AND a.person_gender = left(c.sex,1)
                                                                                                                AND LEFT(a.person_ageband,2) = LEFT(c.ageband,2)
commit --(^_^ )!

SELECT DISTINCT event_id 
INTO #tev1
FROM V289_M10_combined_event_data_adj
WHERE PIV is null 
commit --(^_^ )!

CREATE HG INDEX evi ON #tev1(event_id)
commit --(^_^ )!


UPDATE V289_M10_combined_event_data_adj
SET             PIV =   b.PIV_default
FROM    V289_M10_combined_event_data_adj            AS      a       
JOIN    #tev1                                                           AS  z   ON  a.event_id = z.event_id                                                                                                     
JOIN    V289_M10_PIV_default                    AS  b   ON      a.segment_id = b.segment_id             
                                                                                                        AND a.household_size = b.hhsize
                                                                                                        AND a.person_gender = left(b.sex,1)
                                                                                                        AND LEFT(a.person_ageband,2) = LEFT(b.ageband,2)
commit --(^_^ )!

DROP TABLE #tev1
commit --(^_^ )!

delete from V289_M10_combined_event_data_adj
where PIV is null
commit --(^_^ )!


MESSAGE cast(now() as timestamp)||' | M10 S7.4 - Deleted from V289_M10_combined_event_data_adj due to null PIV: '||@@rowcount TO CLIENT
commit --(^_^ )!





-- Crudely select one event per individual (maximise their PIV)
select	*
into	#tmp_combined_event_data_adj
from
	(
		select
        		account_number
        	,	hh_person_number
        	,	subscriber_id
        	,	event_id
        	,	PIV
			,	person_ageband
			,	person_gender
			,	event_start_utc
			,	overlap_batch
        	,	row_number()	over	(	-- we want to identify the most likely event to put the individual in
                							partition by 	account_number, hh_person_number
                							order by		PIV desc
                						)	as	PIV_rank
        from	V289_M10_combined_event_data_adj
	)	as	a
where a.PIV_rank = 1
commit --(^_^ )!

create hg       index   tmp_combined_event_data_adj_hg_idx_1   on #tmp_combined_event_data_adj(account_number) commit --(^_^ )!
create hg       index   tmp_combined_event_data_adj_hg_idx_2   on #tmp_combined_event_data_adj(event_id) commit --(^_^ )!
create hg       index   tmp_combined_event_data_adj_hg_idx_3   on #tmp_combined_event_data_adj(PIV_rank) commit --(^_^ )!
create lf       index   tmp_combined_event_data_adj_lf_idx_5   on #tmp_combined_event_data_adj(person_gender) commit --(^_^ )!
create lf       index   tmp_combined_event_data_adj_lf_idx_6   on #tmp_combined_event_data_adj(person_ageband) commit --(^_^ )!



-------------------------------------------------------------------------------------
-------------------------------------------------------------------------------------
-- S4 - Assign audience for single-occupancy households and whole-household audiences
-------------------------------------------------------------------------------------
-------------------------------------------------------------------------------------
MESSAGE cast(now() as timestamp)||' | M10 S7.0 - Assign audience for single-occupancy households and whole-household audiences' TO CLIENT
commit --(^_^ )!

-- Simply append all of the household individuals per event to the output table
insert into V289_M10_session_individuals(
		event_date
	,	event_id
	,	account_number
	,	overlap_batch
    ,   person_ageband
    ,   person_gender
    ,   hh_person_number
	,	last_modified_dt
    )
select
		date(event_start_utc)   as	event_date
    ,   event_id
	,	account_number
	,	overlap_batch
    ,   person_ageband
    ,   person_gender
    ,   hh_person_number
	,	now()					as	last_modified_dt
from    #tmp_combined_event_data_adj
commit --(^_^ )!

MESSAGE cast(now() as timestamp)||' | M10 S7.0 - Individuals assigned due to single-occupancy households and whole-household audiences : '|| @@rowcount TO CLIENT
commit --(^_^ )!


-- Update the unique assigned individuals list one last time
UPDATE	V289_M10_UNIQUE_VIEWERS_EVENT_ASSIGNMENT	AS	BAS
SET		ASSIGNED	=	1
FROM	(
            SELECT	ACCOUNT_NUMBER || '-' || HH_PERSON_NUMBER	AS	PERSON_ID
            FROM	V289_M10_session_individuals
            GROUP BY	PERSON_ID
    	)	AS	A
WHERE	BAS.PERSON_ID	=	A.PERSON_ID
commit --(^_^ )!

MESSAGE cast(now() as timestamp)||' | M10 S7.0 - Unique individuals assigned : '|| @@rowcount TO CLIENT
commit --(^_^ )!


------------------------------------------------------------
-- Update the revised session size value in V289_M07_dp_data
------------------------------------------------------------
MESSAGE cast(now() as timestamp)||' | M10 S7.0 - Update the revised session size value in V289_M07_dp_data' TO CLIENT
commit --(^_^ )!

update	V289_M07_dp_data		as	bas
set		bas.session_size	=	bas.session_size	+	a.session_size_actual
from
	(
		select	-- additions to the session sizes (quite likely that the majority of these are 1)
				event_id
			,	overlap_batch
			,	count()								as	rows_check
			,	count(distinct hh_person_number)	as	session_size_actual
		from	#tmp_combined_event_data_adj
		group by
				event_id
			,	overlap_batch
	)	as	a
where
		bas.event_id		=	a.event_id
	and	bas.overlap_batch	=	a.overlap_batch
commit --(^_^ )!

MESSAGE cast(now() as timestamp)||' | M10 S7.0 - Update the revised session size value in V289_M07_dp_data... DONE. Rows affected: ' || @@rowcount TO CLIENT


UPDATE	V289_M10_session_individuals	
SET             provider_id =   dpd.provider_id
				,provider_id_number =   dpd.provider_id_number
				,viewing_type_flag =   dpd.viewing_type_flag
FROM    V289_M10_session_individuals            AS      si       
INNER JOIN   V289_M07_dp_data AS  dpd  
ON  dpd.event_id = si.event_id
commit



---------------
---------------
-- SXX - FINISH
---------------
---------------

-- Update log
update V289_M10_log
set
                dt_completed    = now()
        ,       completed               = 1
where section_id = 'SXX - FINISH'
commit --(^_^ )!

MESSAGE cast(now() as timestamp)||' | M10 - Individuals assignment complete!' TO CLIENT
commit --(^_^ )!


end; -- create or replace procedure H2I_M10_individuals_selection as begin

commit;
grant execute on v289_M10_individuals_selection to vespa_group_low_security;
commit;
