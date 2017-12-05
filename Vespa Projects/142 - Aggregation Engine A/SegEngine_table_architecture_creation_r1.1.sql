/*
                         $$$
                        I$$$
                        I$$$
               $$$$$$$$ I$$$    $$$$$      $$$     ZDD    DDDDDDD.
             ,$$$$$$$$  I$$$   $$$$$$$    $$$      ODD  ODDDZ 7DDDD
             ?$$$,      I$$$ $$$$. $$$$  $$$=      ODD  DDD     NDD
              $$$$$$$$= I$$$$$$$    $$$$.$$$       ODD +DD$     +DD$
                  :$$$$~I$$$ $$$$    $$$$$$        ODD  DDN     NDD.
               ,.   $$$+I$$$  $$$$    $$$$=        ODD  NDDN   NDDN
              $$$$$$$$$ I$$$   $$$$   .$$$         ODD   ZDDDDDDDN
                                      $$$           .      $DDZ
                                     $$$                  ,NDDDDDDD
                                    $$$?

                      CUSTOMER INTELLIGENCE SERVICES


        SIG - Aggregation Engine
        --------------------------------
        Author  : Alan Barber
        Date    : September 20th 2013



SECTIONS
----------------

code_location_A01


PROCEDURES CALLED
-----------------
SEG01_log
SEG01_define_segment
SEG01_assign_default_tag_states
SEG01_define_association
SEG01_define_selfaware
SEG01_create_temporal_period
SEG01_create_temporal_definition
SEG01_create_temporal_library
SEG01_create_prog_event_table_library
SEG01_add_to_metric_library
SEG01_create_temporal_root_aggregations


VERSIONS
-----------------
1.0  Inital release, capable of building root aggregations  Author: Alan Barber


----------------------------------------------------------
*/



/*********************************************************************************
 **  Some examples to show how to use and execute the SEG01_define_segment procedure
 *********************************************************************************/

truncate table seg01_log_tbl
commit

exec SEG01_SETUP_EXE_proc


--create tables required to run the engine. Run once, the FIRST TIME the engine is run
--exec SEG01_setup_table_data_proc

-- this fills tables with information/data so the engine can run. This sql run
-- by this procedure should be drive by a GUI in future releases of the engine.
--exec SEG01_setup_create_table_data_proc




--create the library of events tables
exec SEG01_create_prog_event_table_library







/*******************************************************************************
 ****
 ****   T E M P O R A L    L I B R A R Y    S E T - U P
 ****
 ****      constructs the following tables:
 ****           - SEG01_temporal_period_tbl
 ****           - SEG01_temporal_definition_tbl
 ****           - SEG01_temporal_library_tbl
 ****
 ****      Two Periods are required to define a Definition:
 ****           - one for the recurrence, and
 ****           - the second for the event data period
 ****
 ****      The Definitions can then be given a start date that determines when
 ****      the recurrence begins, and entered into the Library.
 ****
 *******************************************************************************/





/****************************************************************************************
 **  BUILD the Temporal Library
 **     add some library entries
 **
 ****************************************************************************************/


--usage <occurrence_start, occurrence_end, period_start, period_end>
declare @tid bigint
exec SEG01_create_temporal_library_wrapper '2013-05-01 00:00:00.000000', '2013-05-03 00:00:00.000000', '2013-05-01 00:00:00.000000', '2013-05-03 00:00:00.000000', @tid


--test
/*
select *
  from SEG01_temporal_library_tbl
*/



/****************************************************************************************
 **  CREATE THE TEMPORAL ROOTS - code up the individual viewing instances with an ID
 **  need to know the use of the viewing data here - is aggregate going to be based on:
 **         1) viewing time [this is the only current option by default]
 **         2) broadcast time
 **         3) event time
 ****************************************************************************************/



--*******************
--   Generate all the root aggregations against the temporal library
-- **  there might be some efficiency savings here by tagging the temporal
-- **  roots with a period_id or similar, rather than the temporal_id
-- **  or, use a mapping table between the period_id and the temporal_id **

exec SEG01_build_temporal_roots_from_library_definitions




--test
/*
select top 1000 *
  from SEG01_root_temporal_tbl
 where temporal_library_id = 8

select top 1000 *
  from SEG01_root_temporal_tbl

select *
from SEG01_viewed_dp_event_table_summary_tbl
*/
--end test





/*******************************************************************************
 ****
 ****  CONSTRUCT THE ROOT AGGREGATIONS
 ****
 *******************************************************************************/




/***
 * IF a ROOT aggregation has the need to have more than one condition... for example:
 *   1) >  6 seconds
 *   2) < 10 seconds
 *  this can be achieved using 'x between 6 and 10'  ['x'=attribute, 'between'=operator, '6 and 10'=condition]
 *  OR, x in (6,8,9,10)   [if rounded seconds only]
 */


-- [ params to create an RIA aggregation ]
/*****
 *****  THIS BIT DOESN'T WORK as not related to the viewing events, but accounts!
 *****/
/*
INSERT into SEG01_root_build_schedule_tbl (attribute, operator, condition)
 values ('olive_prod.bednaszs.VAggr_02_Channel_Mapping.format', '=', '3D')
INSERT into SEG01_root_build_schedule_tbl (attribute, operator, condition)
 values ('olive_prod.vespa_shared.Aggr_Account_Attributes.ent_tv3d_sub', '=', '1')
INSERT into SEG01_root_build_schedule_tbl (attribute, operator, condition)
 values ('olive_prod.vespa_shared.Aggr_Account_Attributes.movmt_tv3d_sub', '=', '0')
commit
*/
--- > end


-- try this instead - just for the standard restriction of panel 12
INSERT into SEG01_root_build_schedule_tbl (attribute, operator, condition)
  values ('olive_prod.sk_prod.%event%.panel_id', '=', '12')
commit




/****************************************************************
 *  Create a build schedule at this point that combines
 *  the roots to be built, and the temporal period
 ***************************************************************/


-- select root_build ID
/*
select top 5 *
from SEG01_root_build_schedule_tbl

--select temporal_id
select top 1000 *
from SEG01_temporal_library_tbl
*/

INSERT into SEG01_root_temporal_build_schedule_tbl(root_schedule_id, temporal_id) values(1, 39)
commit

/*
select top 100 *
  from SEG01_root_temporal_build_schedule_tbl
*/


--this proc reads from  'SEG01_root_temporal_build_schedule_tbl'
exec SEG01_create_root_segments





-- table holding the newly created  <  R O O T   S E G M E N T A T I O N S  >
/*
select max(segment_id)
from SEG01_root_segment_tbl

select top 1000 *
from SEG01_root_segment_tbl
where segment_id = 132


select top 1000 *
from SEG01_root_segment_desc_tbl
where segment_id = 136


select top 1000 *
from SEG01_root_temporal_tbl


select *
  from SEG01_temporal_library_tbl
  where uniqid = 8

select distinct uniqid
  from SEG01_temporal_library_tbl

select *
from SEG01_metric_library_tbl

*/

----

--use
select *
  from SEG01_trunk_filter_defn_tbl
where filter_id = 2


select *
  from SEG01_metric_build_schedule_tbl
where metric_schedule_id = 1



-------make an entry in the trunk filter table

--make filter_id
DECLARE @filter_id          bigint
DECLARE @aggreagation_id    bigint
DECLARE @aggreagation_type  integer
DECLARE @temporal_id        bigint
DECLARE @temporal_type      integer
DECLARE @operator_type      integer
DECLARE @auto_commit        bit


exec SEG01_assign_filterid @filter_id
SET @aggreagation_id    = 136
SET @aggreagation_type  = 0
SET @temporal_id        = 39
SET @temporal_type      = 1
SET @operator_type      = 0
SET @auto_commit        = 1


--this proc inserts into<SEG01_trunk_filter_defn_tbl>
exec SEG01_insert_trunk_filter_defn_tbl
                 @filter_id,
                 @aggregation_id,
                 @aggregation_type,
                 @temporal_id,
                 @temporal_type, --(viewing)
                 @operator_type,
                 @auto_commit --[0]false, [1]true


--test
select *
from SEG01_trunk_filter_defn_tbl


--Start script here -->


declare @filter_id      bigint
declare @schedule_id    bigint

SET @filter_id   = 4
SET @schedule_id = 1

--runs all the metrics that can be calculated on the same dataset
exec SEG01_create_trunk_aggregation @filter_id, @schedule_id



--do some checks...
select *
from SEG01_log_tbl


select max(trunk_id)
  from SEG01_trunk_aggregation_results_tbl r

select top 1000 *
  from SEG01_combined_events_20131118171936_39tmp

select top 100 *
from SEG01_trunk_aggregation_results_tbl r

select top 1000 *
  from SEG01_trunk_aggregation_results_tbl r,
       --SEG01_trunk_filter_defn_tbl f,
       SEG01_metric_library_tbl m
where --r.filter_id = f.filter_id
  --and
  r.metric_id = m.uniqid
  and r.trunk_id = 23



select top 10 *
from SEG01_metric_library_tbl

select top 10 *
  from seg01_trunk_filter_defn_tbl


/*
 select top 1000 * from bednaszs.VAggr_02_Channel_Mapping cm
 select top 1000 * from vespa_shared.Aggr_Account_Attributes aa
 select top 1000 * from SEG01_pk_prog_temporal_list_tmp
select distinct segment_id from SEG01_root_segment_tbl
*/



/******
 **  Create leaf aggregations...
******/


DECLARE @leaf_id bigint
exec SEG01_assign_leafid @leaf_id
select @leaf_id

/*
exec SEG01_insert_leaf_construct_defn_tbl
                 @leaf_id,
                 6,       --aggregation_id
                 1,       --aggregation_type [0]root, [1]trunk, [2]branch, [3]leaf
                 0,       --operator_type    [0]+, [1]-, [2]*, [3]/
                 1        -- auto commit [0]false, [1]true
*/
exec SEG01_insert_leaf_construct_defn_tbl @leaf_id, 23, 1, 0, 1
exec SEG01_insert_leaf_construct_defn_tbl @leaf_id, 23, 1, 3, 1

commit

select top 5 *
from SEG01_leaf_aggregation_defn_tbl





--now use this definition to do the calculation and create the account level leaf aggregation

account_number, leaf_id, construct_id, value --construct_id should be associated with the metric

select *
from SEG01_leaf_build_schedule_tmp



declare @p1 double
declare @p2 double
declare @op varchar(1)

set @p1 = 20
set @p2 = 10
set @op = '/'

declare @xsql varchar(500)

set @xsql =
       ' handle_calc(23,''+'',7) add_, '||
       ' handle_calc(19,''-'',9) min_0, '||
       ' handle_calc(1,''*'',0) mul_0, '||
       ' handle_calc(8,''/'',4) div_, '||
       ' handle_calc(1,''/'',0) div_0, '||
       ' handle_calc(@p1,@op,@p2) div_dyn '

execute('select '||@xsql||' into #SEG01_dyn_tmp ')

select  *
from #SEG01_dyn_tmp




select top 10 * from SEG01_root_account_aggregate_tbl    --aggregate_id
select top 10 * from SEG01_trunk_aggregation_results_tbl --trunk_id
select top 10 * from SEG01_branch_aggregation_results_tbl
select top 10 * from SEG01_leaf_aggregation_results_tbl  --leaf_id





/*************************************
 **   convert this into a proc...   **
 *************************************/

--------------------
---- START SCRIPT -->
------
declare @leaf_id            bigint
declare @aggregation_type   integer
declare @aggregation_id     bigint
declare @operator_type      integer
declare @construct_id       bigint        --what's this for?
declare @max_rank           integer
declare @rank               integer
declare @table_name         varchar(48)
declare @operator_type_str  varchar(1)
declare @aggid_field_str    varchar(48)


SET @leaf_id = 12


--work out what the construct_id should be for the rule that defines this leaf
--left out for now... construct_id should represent the metric used (or combined) in generating the aggregation_value
SET @construct_id = -1
/*
select *
from SEG01_leaf_aggregation_defn_tbl a
*/


--drop tables in preparation for fresh run of procedure
IF object_id('SEG01_leaf_build_schedule_tmp') IS NOT NULL
  BEGIN
    DROP TABLE SEG01_leaf_build_schedule_tmp
  END
IF object_id('SEG01_leaf_calculation_tmp') IS NOT NULL
  BEGIN
    DROP TABLE SEG01_leaf_calculation_tmp
  END
IF object_id('SEG01_handle_calc_tmp') IS NOT NULL
  BEGIN
    DROP TABLE SEG01_handle_calc_tmp
  END


select a.*, dense_rank() over(order by uniqid asc) rank
  into SEG01_leaf_build_schedule_tmp
  from SEG01_leaf_aggregation_defn_tbl a
 where leaf_id = @leaf_id
order by uniqid


SET @rank = 1

select @max_rank = max(rank)
  from SEG01_leaf_build_schedule_tmp



while(@rank<=@max_rank)
    BEGIN

        exec SEG01_log '    Starting aggregation run '||@rank

        select @aggregation_type = aggregation_type,
               @aggregation_id = aggregation_id,
               @operator_type = operator_type
          from SEG01_leaf_build_schedule_tmp
         where rank = @rank

        exec SEG01_log '      > aggregation_id<'||@aggregation_id||'>'
        exec SEG01_log '      > aggregation_type<'||@aggregation_type||'>'
        exec SEG01_log '      > operator_type<'||@operator_type||'>'
        --do some magic


        -- 1. aggregation_type [0]root, [1]trunk, [2]branch, [3]leaf
             if(@aggregation_type=0)
                BEGIN
                  SET @table_name = 'SEG01_root_account_aggregate_tbl'
                  SET @aggid_field_str = 'aggregate_id'
                END
        else if(@aggregation_type=1)
                BEGIN
                  SET @table_name = 'SEG01_trunk_aggregation_results_tbl'
                  SET @aggid_field_str = 'trunk_id'
                END
        else if(@aggregation_type=2)
                BEGIN
                  SET @table_name = 'SEG01_branch_aggregation_results_tbl'
                  SET @aggid_field_str = ''
                END
        else if(@aggregation_type=3)
                BEGIN
                  SET @table_name = 'SEG01_leaf_aggregation_results_tbl'
                  SET @aggid_field_str = 'leaf_id'
                END


        -- 2. identify operator type:  [0]+, [1]-, [2]*, [3]/
        --construct query... lets do this one step at a time...
        -- will be slower than all in one go.. but easier to code in the first instance
        -- build up the results slowly
             if(@operator_type=0) begin SET @operator_type_str = '+' end
        else if(@operator_type=1) begin SET @operator_type_str = '-' end
        else if(@operator_type=2) begin SET @operator_type_str = '*' end
        else if(@operator_type=3) begin SET @operator_type_str = '/' end


        --if first-time, then create table - else operate on table
        if (@rank = 1)
            BEGIN
                --exec SEG01_log '     rank = 1<'||@rank||'>'

                execute(' select account_number, '||@leaf_id||' leaf_id, '||@aggregation_id||' as aggregation_id, '||@aggregation_type||' as aggregation_type, '||@construct_id||' as construct_id, aggregation_value '||
                        '   into SEG01_leaf_calculation_tmp '||
                        '   from '||@table_name||' b'||
                        '  where b.'||@aggid_field_str||' = '||@aggregation_id)

                --add index
                create unique index SEG01_leaf_calculation_ac_idx on SEG01_leaf_calculation_tmp(account_number)
                commit

                -- also create audit information at this point
                INSERT into SEG01_leaf_aggregation_desc_tbl(leaf_id, aggregation_id, aggregation_type, operator_type, operator_type_str, construct_id) values(@leaf_id, @aggregation_id, @aggregation_type, null, null, @construct_id)
                commit
            END
        else
            BEGIN
                -- IT'S SLOW (DON'T KNOW HOW SLOW COMPARED - BUT IT WORKS!!
                --exec SEG01_log '     rank greater than 1<'||@rank||'>'

                --lets make a table of accounts and values - assigned by the aggregation_type field
                execute(' select b.account_number, b.aggregation_value '||
                        '   into #SEG01_ac_value_tmp '||
                        '   from '||@table_name||' b '||
                        '  where b.'||@aggid_field_str||' = '||@aggregation_id)

                if(@operator_type=0)
                  BEGIN
                    select a.account_number, handle_calc(a.aggregation_value, '+', b.aggregation_value) new_value
                      into SEG01_handle_calc_tmp
                      from SEG01_leaf_calculation_tmp a, #SEG01_ac_value_tmp b
                     where a.account_number = b.account_number
                  END
                else if(@operator_type=1)
                  BEGIN
                    select a.account_number, handle_calc(a.aggregation_value, '-', b.aggregation_value) new_value
                      into SEG01_handle_calc_tmp
                      from SEG01_leaf_calculation_tmp a, #SEG01_ac_value_tmp b
                     where a.account_number = b.account_number
                  END
                else if(@operator_type=2)
                  BEGIN
                    select a.account_number, handle_calc(a.aggregation_value, '*', b.aggregation_value) new_value
                      into SEG01_handle_calc_tmp
                      from SEG01_leaf_calculation_tmp a, #SEG01_ac_value_tmp b
                     where a.account_number = b.account_number
                  END
                else if(@operator_type=3)
                  BEGIN
                    select a.account_number, handle_calc(a.aggregation_value, '/', b.aggregation_value) new_value
                      into SEG01_handle_calc_tmp
                      from SEG01_leaf_calculation_tmp a, #SEG01_ac_value_tmp b
                     where a.account_number = b.account_number
                  END

                UPDATE SEG01_leaf_calculation_tmp c
                   SET c.aggregation_value = b.new_value
                  from SEG01_handle_calc_tmp b
                 where c.account_number = b.account_number
                   and c.aggregation_id = @aggregation_id
                   and c.aggregation_type = @aggregation_type
                commit

                drop table SEG01_handle_calc_tmp
                commit

                -- also create audit information at this point
                INSERT into SEG01_leaf_aggregation_desc_tbl(leaf_id, aggregation_id, aggregation_type, operator_type, operator_type_str, construct_id) values(@leaf_id, @aggregation_id, @aggregation_type, @operator_type, @operator_type_str, @construct_id)
                commit
            END

        exec SEG01_log '    Run aggregation '||@rank

        SET @rank = @rank + 1
    END



    select top 100000 * from SEG01_leaf_calculation_tmp
    where aggregation_value = 1

---end script <-----------
---------------------------
---------------------------


select *
from SEG01_leaf_aggregation_desc_tbl


--test1

select top 100 account_number, leaf_id, aggregation_id, count(1) sample_count
from SEG01_leaf_calculation_tmp
group by account_number, leaf_id, aggregation_id
having sample_count > 1


select *
from SEG01_leaf_calculation_tmp
where account_number = '621101306036'

select *
from SEG01_trunk_aggregation_results_tbl
where account_number = '621101306036'

--dataset
select *
  into SEG01_tst_data_tmp
from SEG01_trunk_aggregation_results_tbl
where account_number in ('621057736251',
'210128206856',
'620058025243',
'620000015573',
'630033367917',
'220022800381',
'210042200829',
'621010613217',
'620056878221',
'621164999677')
commit

select *
from SEG01_tst_data_tmp

select a.aggregation_value / b.aggregation_value div, a.*, b.*
from SEG01_tst_data_tmp a, SEG01_tst_data_tmp b
where a.account_number = b.account_number
  and a.trunk_id = 23
  and b.trunk_id = 23

drop table SEG01_tst_data_tmp
commit

select top 10 *
from SEG01_leaf_calculation_tmp a

drop table SEG01_leaf_calculation_copy_tmp

select *
into SEG01_leaf_calculation_copy_tmp
from SEG01_leaf_calculation_tmp
commit


--this is what we're trying to use to update the leaf_calculation_tmp table
--we need a workaround as using handle_calc in the update statement doesn't work
select --top 1000
       a.account_number, handle_calc(a.aggregation_value, '/', b.aggregation_value) new_value
  into #SEG01_handle_operator_tmp
from SEG01_leaf_calculation_copy_tmp a, SEG01_trunk_aggregation_results_tbl b
where a.account_number = b.account_number
  and a.aggregation_id = 23
  and a.aggregation_type = 1
  and b.trunk_id = 23

UPDATE SEG01_leaf_calculation_copy_tmp c
   SET c.aggregation_value = b.new_value
from #SEG01_handle_operator_tmp b
where c.account_number = b.account_number
  and c.aggregation_id = 23
  and c.aggregation_type = 1

commit


select top 1000 *
from SEG01_leaf_calculation_copy_tmp

--declare @table_name         varchar(56)
declare @leaf_id            bigint
declare @aggregation_id     bigint
declare @aggregation_type   integer
declare @construct_id       bigint
declare @operator_type_str  varchar(2)


--set @table_name       = 'SEG01_trunk_aggregation_results_tbl'
set @leaf_id          = 12
set @aggregation_id   = 23
set @aggregation_type = 1
set @construct_id     = -1
set @operator_type_str = '/'


-- ' INSERT into SEG01_leaf_calculation_tmp '
  select b.account_number,
         @leaf_id,
         @aggregation_id,
         @aggregation_type,
         @construct_id,
         handle_calc(a.aggregation_value, @operator_type_str, b.aggregation_value),
         a.*,b.*
    from SEG01_leaf_calculation_tmp a, SEG01_trunk_aggregation_results_tbl b
   where a.account_number = b.account_number and a.aggregation_id = @aggregation_id
     and b.account_number in (621057736251,
210128206856,
620058025243,
620000015573,
630033367917,
220022800381,
210042200829,
621010613217,
620056878221,
621164999677)


--ends test1

commit

select *
from SEG01_log_tbl


select top 1000 *
from SEG01_leaf_build_schedule_tmp

select top 1000 *
from SEG01_leaf_calculation_tmp



--test
declare @aggregation_id bigint
declare @aggregation_type integer
declare @leaf_id bigint
declare @construct_id bigint
declare @sqlx1            varchar(500)
declare @sqlx2            varchar(500)
declare @sqlx3            varchar(500)
declare @sqlx4            varchar(500)
declare @table_name varchar(48)

set @aggregation_id = 23
set @aggregation_type = 1
set @leaf_id = 12
set @construct_id = -1

 if(@aggregation_type=0) SET @table_name = 'SEG01_root_account_aggregate_tbl'
        else if(@aggregation_type=1) SET @table_name = 'SEG01_trunk_aggregation_results_tbl'
        else if(@aggregation_type=2) SET @table_name = 'SEG01_branch_aggregation_results_tbl'
        else if(@aggregation_type=3) SET @table_name = 'SEG01_leaf_aggregation_results_tbl'

 SET @sqlx1 = ' select account_number, '||@leaf_id||' leaf_id, '||@aggregation_id||' as aggregation_id, '||@aggregation_type||' as aggregation_type, '||@construct_id||' as construct_id, aggregation_value '
 SET @sqlx2 = '   into #SEG01_tmp '
 SET @sqlx3 = '   from '||@table_name||' '
 SET @sqlx4 = '  where aggregation_id = '||@aggregation_id

execute(@sqlx1||@sqlx2||@sqlx3||@sqlx4)

select top 10000 * from #SEG01_tmp

--end test


select top  10 *
from SEG01_trunk_aggregation_results_tbl


--SEG01_combined_events_20131112085728_8tmp
select top 1000 *
from SEG01_event_tbls_20131113130202_8tmp

truncate table SEG01_log_tbl

commit
select *
  from SEG01_log_tbl


/*****************************
 ***************************
 ***     RAW EXAMPLE
 ************************/


  DECLARE @_comma_seperated_list_of_tags_required  varchar(500) --should be set from a proc variable above
  DECLARE @_root_id                   bigint
  DECLARE @_temporal_library_rule_id  bigint
  DECLARE @_datetime_type_field       integer
  DECLARE @_db_table_name             varchar(64)

  --ideally this would be set from the Meta-data that relates to the Trunk aggregation descriptor
  SET @_comma_seperated_list_of_tags_required = ' e.pk_viewing_prog_instance_fact, '||
                                                ' e.dk_programme_instance_dim, '||
                                                ' t.period_duration, '||
                                                ' coalesce(aa.days_data_returned, 0) as days_data_returned, '||
                                                ' coalesce(aa.ent_tv3d_sub,       0) as ent_tv3d_sub, '||
                                                ' coalesce(aa.movmt_tv3d_sub,     0) as movmt_tv3d_sub, '||
                                                ' cm.channel_type, '||
                                                ' cm.format '

  SET @_root_id = 133 --ideally this would be the Trunk aggregation (but here is a Root aggregation)
  SET @_temporal_library_rule_id = 8
  SET @_datetime_type_field = 1 --currently only viewing time is supported (1)


  exec SEG01_create_tmp_dataset_tbl @_root_id, @_temporal_library_rule_id, @_datetime_type_field, @_comma_seperated_list_of_tags_required, @_db_table_name

select @_db_table_name





/*********************************************************************
 ***                                                               ***
 **    Let's use this combined table to calculate aggregations      **
 ***                                                               ***
 *********************************************************************/


--now aggregate to the account level
DECLARE @metric_id bigint
DECLARE @metric_name varchar(100)
DECLARE @universe_id bigint

SET @metric_name = 'sum(period_duration)'
--SET @metric_name = 'sum(period_duration) over()'    -- <--- doesn't seem to work
--SET @metric_name = 'count(pk_viewing_prog_instance_fact)'


SELECT @metric_id = uniqid
  from SEG01_metric_library_tbl
 where calculation = @metric_name

--- we need to record the metric along with this aggregate record.
-- we also need to record the universe for this aggregation

--set the ID for this universe
--exec SEG01_universe_insert_proc 'SEG01_combined_events_20130918219018_8tmp', @universe_id
exec SEG01_universe_insert_proc @_db_table_name, @universe_id


/*
 so the unique aggregate ID should be built from:
    1) universe_id,
    2) root_id,
    3) temporal_library_rule_id,
    4) metric_id
*/



INSERT into SEG01_root_aggregation_built_tbl(universe_id, root_id, temporal_id, metric_id)
    select distinct @universe_id, aggregation_id/*root_id*/, temporal_library_rule_id, @metric_id /*metric_id*/
      from SEG01_combined_events_20130918219018_8tmp
commit

select top 10 *
  from SEG01_root_aggregation_built_tbl -- <--- holds the IDs




--start test...
--this section copied from above for testing
drop table seg01_agg_tmp

DECLARE @metric_id bigint
DECLARE @metric_name varchar(100)
DECLARE @next_uniqid bigint


SET @metric_name = 'sum(period_duration)'
--SET @metric_name = 'sum(period_duration) over()'    -- <--- doesn't seem to work
--SET @metric_name = 'count(pk_viewing_prog_instance_fact)'


SELECT @metric_id = uniqid
  from SEG01_metric_library_tbl
 where calculation = @metric_name



--really we need a universe manager here - to assign a unique ID if all the accounts are different to one previously defined
select @next_uniqid = coalesce(max(uniqid),0) +1
  from SEG01_universe_tbl



--end testing section... continue by including the insert statement below...

--so include this table to get the aggregation_id in the join
--INSERT into SEG01_root_account_aggregate_tbl '||
        --' select top 1000 '|| --need to make a uniqid from the: segment_id, temporal_library_id, and @metric_id
execute(' select account_number, '||
        '        a.uniqid as aggregation_id,   '||
        '        a.universe_id,  '||
        '        a.root_id,      '||
        '        a.temporal_id, '||
        '        a.metric_id,    '||
        '       '||@metric_name||' as '''||@metric_name||''''||
        '   into seg01_agg_tmp '||
        '   from SEG01_combined_events_20130918219018_8tmp e, SEG01_root_aggregation_built_tbl a'||
        '  where '||@next_uniqid||' = a.universe_id '||
        '    and e.aggregation_id = a.root_id '||
        '    and e.temporal_library_rule_id = a.temporal_id '||
        '    and '||@metric_id||' = a.metric_id '||
        --'   from '||@_db_table_name||
        ' group by account_number, a.uniqid, universe_id, root_id, temporal_id, metric_id ')

commit

select top 1000 *
from seg01_agg_tmp  -- <--- so this should be the SEG01_root_account_aggregate_tbl


INSERT into SEG01_root_account_aggregate_tbl
select *
from seg01_agg_tmp
commit

select top 100 *
from SEG01_root_account_aggregate_tbl


--what does the temporal_id refer to?...

select top 10 *
  from SEG01_temporal_library_tbl

------------------------------------




-- so the automated version of this using the constructed data set would be:
--execute('INSERT into SEG01_root_account_aggregate_tbl '||
execute('INSERT into SEG01_root_account_aggregate_tbl '||
        --' select top 1000 '|| --need to make a uniqid from the: segment_id, temporal_library_id, and @metric_id
        'select '||
        ' account_number, '||@next_uniqid||' universe_id, root_id, temporal_library_rule_id, '||@metric_id||' as metric_id, '||@metric_name||' as '''||@metric_name||''''||
        --' into #seg01_agg_tmp '||
        '   from SEG01_combined_events_20130918219018_8tmp '||
        --'   from '||@_db_table_name||
        ' group by account_number, universe_id, root_id, temporal_library_rule_id, metric_id ')
commit

--basically this is the aggregation table

select top 100 *
from SEG01_root_account_aggregate_tbl

select top 100 *
from SEG01_universe_tbl


select *
from #seg01_agg_tmp






/*******************************     Do we need this as we could just use the universal table below
 **    BUILD  AGGREGATION     **    <--------------------------------------------------------------
 *******************************
create table SEG01_root_account_aggregate_tbl (
        account_number      varchar(24)   NOT NULL,
                                                    <--- universe_id ??
        aggregation_id      bigint        NOT NULL,
        temporal_id         bigint        NOT NULL,
        metric_id           bigint        NOT NULL,
        aggregation_value   double        NOT NULL
)
*/



/*********************************************
 **
 **  Mapping table for aggregations based on a group of accounts
 **
 *********************************************/
/*
create table SEG01_universe_tbl(
    uniqid           bigint        NOT NULL, --universe_id
    account_number   varchar(24)   NOT NULL)

create table SEG01_universal_aggregtate_tbl(
    uniqid              bigint        NOT NULL   identity,
    universe_id         bigint        NOT NULL,
    aggregation_id      bigint        NOT NULL,
    temporal_id         bigint        NOT NULL,
    metric_id           bigint        NOT NULL,
    aggregation_value   double        NOT NULL)*/








--------------------------------------------------------------------------------------------------------




-- >>>>>>>>>>>>>>>>>>>>>>>  *************************************   <<<<<<<<<<<<<<<<<<<<<<<<<<  >
-- >>>>>>>>>>>>>>>>>>>>>>>                                          <<<<<<<<<<<<<<<<<<<<<<<<<<  >
-- >>>>>>>>>>>>>>>>>>>>>>>      T H I S   I S   T H E   B I T       <<<<<<<<<<<<<<<<<<<<<<<<<<  >
-- >>>>>>>>>>>>>>>>>>>>>>>                                          <<<<<<<<<<<<<<<<<<<<<<<<<<  >
-- >>>>>>>>>>>>>>>>>>>>>>>                 I'M                      <<<<<<<<<<<<<<<<<<<<<<<<<<  >
-- >>>>>>>>>>>>>>>>>>>>>>>                                          <<<<<<<<<<<<<<<<<<<<<<<<<<  >
-- >>>>>>>>>>>>>>>>>>>>>>>    C U R R E N T L Y   E D I T I N G     <<<<<<<<<<<<<<<<<<<<<<<<<<  >
-- >>>>>>>>>>>>>>>>>>>>>>>                                          <<<<<<<<<<<<<<<<<<<<<<<<<<  >
-- *******************************************************************************************  >
--     |              |                |                |               |                |
--     |              |                |                |               |                |
--    \|/            \|/              \|/              \|/             \|/              \|/
--     v              v                v                v               v                v






 select top 1000 *
   from SEG01_root_segment_tbl r, SEG01_root_temporal_tbl t
  where r.pk_viewing_prog_instance_fact = t.pk_viewing_prog_instance_fact
    and r.segment_id = 133




--continue as if we just want Total Duration

select top 1000 *
  from SEG01_root_account_aggregate_tbl

select top 1000 *
  from SEG01_root_segment_tbl

select top 10 *
from SEG01_root_temporal_tbl

select top 100 *
from SEG01_metric_library_tbl




------------>>

declare @metric_id         bigint
declare @calculation       VARCHAR(64)
declare @over_group        VARCHAR(24)


select @metric_id   = uniqid,
       @calculation = calculation,
       @over_group  = over_group
  from SEG01_metric_library_tbl
 where metric_name  = 'Total Account Duration'


--select @metric_id,  @calculation,  @over_group



execute(' INSERT into SEG01_root_account_aggregate_tbl '||
  ' select '||--top 1000
  '        '||@over_group||', segment_id as root_id, temporal_library_id, '||@metric_id||', '||@calculation||
  '     from SEG01_root_segment_tbl r, SEG01_root_temporal_tbl t '                            ||
  '    where r.pk_viewing_prog_instance_fact = t.pk_viewing_prog_instance_fact '              ||
  '      and r.segment_id = 133 '                                                             ||
  '      and t.temporal_library_id = 8 '                                                      ||
  ' group by '||@over_group||', segment_id, temporal_library_id, '||@metric_id)




/*************************************************************************
 **    TO MAKE -TRUNK- AGGREGATIONS (from multiple root aggregations    **
 *************************************************************************/

--need to pick out the correct aggregations
/** so, to extract facts that are related to 3 segmentations you have to
 *  join to the root_segment_tbl the same number of times that you are
 *  restricting by. As follows:                                                */
select a.pk_viewing_prog_instance_fact
from SEG01_root_segment_tbl a,
     SEG01_root_segment_tbl b--,
    -- SEG01_root_segment_tbl c
 where a.pk_viewing_prog_instance_fact = b.pk_viewing_prog_instance_fact -- 2 joins for three tables
  -- and a.pk_viewing_prog_instance_fact = c.pk_viewing_prog_instance_fact
   and a.segment_id = 132     -- restriction for each segmentation
   and b.segment_id = 133
  -- and c.segment_id = 7         --     422,348 (facts are related to all 3 segmentations)



-- and the viewing instances that are within a period??

select top 1000 *
from SEG01_root_segment_tbl r, SEG01_root_temporal_tbl t
where r.pk_viewing_prog_instance_fact = t.pk_viewing_prog_instance_fact

--------------------------------------------




-- hmmmm... can we work out the other metrics from  SEG01_root_temporal_tbl t

select top 1000 *
from  SEG01_root_temporal_tbl t

--if we required number of programmes viewed, we would have to go back to the unioned event tables and join by pk__instance_fact
--then group by programme, and count(distinct facts)





--aggregated, with description
select r.account_number, r.segment_id, r.temporal_library_id, sum(period_duration)

select top 1000 *
from SEG01_root_segment_tbl r, SEG01_root_temporal_tbl t, SEG01_temporal_library_tbl tl, SEG01_root_segment_desc_tbl sd
where r.pk_viewing_prog_instance_fact = t.pk_viewing_prog_instance_fact
  and sd.segment_id = r.segment_id
  and tl.uniqid = t.temporal_library_id
  and r.segment_id = 133
  and t.temporal_library_id = 8

  group by account_number, segment_id, temporal_library_id




---so if we want to aggregate by something like channel then we need to join back via the event tables
-- the dimension should dictate how this is rolled-up
--so we need a proc that reads the pk_viewing_prog_instance_facts from the root tables and joins with
--the event data again, and just output the final table: a/c, aggregated_id, temporal_id, metric_id, value

select top 1000 *
from SEG01_root_segment_tbl r, SEG01_root_temporal_tbl t  --, SEG01_temporal_library_tbl tl, SEG01_root_segment_desc_tbl sd
where r.pk_viewing_prog_instance_fact = t.pk_viewing_prog_instance_fact


--so - using...
select top 1000 *
from SEG01_root_segment_tbl r, SEG01_root_temporal_tbl t
where r.pk_viewing_prog_instance_fact = t.pk_viewing_prog_instance_fact
  and r.segment_id = 133
  and t.temporal_library_id = 8
----


DECLARE @current_id_max             bigint
DECLARE @current_id                 bigint
DECLARE @_temporal_library_rule_id  bigint
DECLARE @_datetime_type_field       integer
DECLARE @_table_name                varchar(48)
DECLARE @segment_id                 bigint

SET @segment_id = 133
SET @_temporal_library_rule_id  = 8
SET @_datetime_type_field       = 1


-- this returns the name of a table (using the parameter @_table_name) that contains
-- a list of event tables that relevent to the period
exec SEG01_get_relevant_event_tables @_temporal_library_rule_id,  @_datetime_type_field,  @_table_name


--iterate through the event table list (find the max number of tables to iterate through first)
execute(' select @current_id_max = max(uniqid) '||
           'from '||@_table_name)

SET @current_id = 1

while @current_id <= @current_id_max

  BEGIN

      --get the event table details
      execute('select @event_table_name = table_name, '||
              '       @schema_name      = schema_name '||
              '  from '||@_table_name                  ||
              ' where uniqid = '||@current_id)

    select @schema_name, @event_table_name



--create sub-table that joins to the relevant tables
 exec SEG01_create_tmp_dataset_tbl ...


--There is an example at the end of the file <SEG01_create_tmp_dataset_tbl_proc.sql> that
-- attempts to create the RIA 3D Aggregation.....




-- <!------- ENDS -------->

/*******************************************************************************/




-- run this query on the sub table
    execute(' select account_number, %groupByDimension%, %metric%  '||
  --here we need to be aggregating to the account level
            '   from '||@schema_name||'.'||@event_table_name||' e SEG01_root_segment_tbl r, SEG01_root_temporal_tbl t '||
            '  where r.pk_viewing_prog_instance_fact = t.pk_viewing_prog_instance_fact '||
            '    and r.pk_viewing_prog_instance_fact = e.pk_viewing_prog_instance_fact '||
            '    and r.segment_id = '||@segment_id||
            '    and t.temporal_library_id = '||@_temporal_library_rule_id||
            ' group by account_number )



    --EXTRACT information from the table to build aggregate
  /*   SET @sql_insert_str1 =
    ' INSERT INTO '||@insert_table||
     ' select e.pk_viewing_prog_instance_fact, '||@segmentid||' segmentid '||
     --'   from '||@schema_name||'.'||@table_name||
     '   from '||@schema_name||'.'||@event_table_name||' e, SEG01_root_temporal_tbl t '||
     '  where e.'||@col_name||' '||@operator||' '||@condition||
     '    and e.pk_viewing_prog_instance_fact = t.pk_viewing_prog_instance_fact '||
     '    and t.temporal_library_id = '||@_temporal_library_rule_id
  */




    SET @current_id = @current_id + 1

  END

execute(' drop table '||@_table_name)
execute(' commit ')



------------------------------




/*   ************************************************
   ***************************************************
  ***                                               ***
 ***    THE FOLLOWING SECTION IS JUST THOUGHTS       ***
  ***                                               ***
   ***************************************************
    ************************************************   */



-- query this table to get a sub-population
select distinct segment_id
from SEG01_root_segment_tbl
-- segment_id
-- 5
-- 6
-- 7


/** so, to extract facts that are related to 3 segmentations you have to
 *  join to the root_segment_tbl the same number of times that you are
 *  restricting by. As follows:                                                */
select a.pk_viewing_prog_instance_fact
from SEG01_root_segment_tbl a,
     SEG01_root_segment_tbl b--,
    -- SEG01_root_segment_tbl c
 where a.pk_viewing_prog_instance_fact = b.pk_viewing_prog_instance_fact -- 2 joins for three tables
  -- and a.pk_viewing_prog_instance_fact = c.pk_viewing_prog_instance_fact
   and a.segment_id = 132     -- restriction for each segmentation
   and b.segment_id = 133
  -- and c.segment_id = 7         --     422,348 (facts are related to all 3 segmentations)



-- and the viewing instances that are within a period??

select top 1000 *
from SEG01_root_segment_tbl r, SEG01_root_temporal_tbl t
where r.pk_viewing_prog_instance_fact = t.pk_viewing_prog_instance_fact



--- when is the period of the sample data?
select top 1000 *
from SampleSegIntoBau -- currently August eg. 2012-08-13 21:30:00.000000


drop table SEG01_query_setup_tmp;
commit;






/*********************************
** NOW ADD THE METRIC DEFINITIONS
**********************************/

-- this section should possibly come before the base segment definitions....

--create metrics table
/*drop table SEG01_Metrics_tbl;
drop table SEG01_Aggregations_tbl;
drop table SEG01_Viewing_Metrics_tbl;

create table SEG01_Metrics_tbl
(uniqid                 BIGINT          NOT NULL identity,
 metric_name            VARCHAR(32)     NOT NULL
)

create table SEG01_Aggregations_tbl
(uniqid                 BIGINT          NOT NULL identity,
 aggregation_name            VARCHAR(32)     NOT NULL
)

create table SEG01_Viewing_Metrics_tbl
(uniqid                 BIGINT          NOT NULL identity,
 metric_uid             BIGINT          NOT NULL,
 aggregation_uid        BIGINT          NOT NULL
)
commit;


-- set-up tables with values
INSERT into SEG01_Metrics_tbl (metric_name) VALUES ('Total Viewing');
INSERT into SEG01_Metrics_tbl (metric_name) VALUES ('Share of Viewing');
--metric for between...
commit;

--aggregation group
INSERT into SEG01_Aggregations_tbl (aggregation_name) VALUES ('account_number');
INSERT into SEG01_Aggregations_tbl (aggregation_name) VALUES ('*');
commit;

INSERT into SEG01_Viewing_Metrics_tbl (metric_uid, aggregation_uid)
select m.uniqid metric_uid, a.uniqid aggregation_uid
  from SEG01_Aggregations_tbl a, SEG01_Metrics_tbl m;
commit;


--- test
select top 1000 vm.*, m.metric_name, a.aggregation_name
from SEG01_Viewing_Metrics_tbl vm, SEG01_Metrics_tbl m, SEG01_Aggregations_tbl a
where vm.metric_uid = m.uniqid
  and vm.aggregation_uid = a.uniqid;

--- end test

*/



