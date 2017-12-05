
/**********************************************************************************
 **
 **  Builds TEMPORAL root aggregations according to the input parameter list
 **
 **     [    Parameter Input:                                       ]
 **     [       here we want the period from and to, and the        ]
 **     [       interval/aggregate level for tagging, or a          ]
 **     [       period rule for grouping (such as day of week)      ]
 **     [       use a temporal rule table for this                  ]
 **
 **
 **  Tags can be re-used for the same rule, but for different periods
 **  a tag per rule is required. For example, all Mondays or weekends could have the
 **  same tag to denote all Mondays or weekend respectively. However 1wk should have
 **  a different tag to a 2nd wk if they were to be aggregated seperately
 **
 **  Need a table that outputs the: 1) pk_instance_prog_viewing_fact
 **                                 2) Tag_id for the rule/period
 **                                 3) Duration of the fact associated with the period
 **********************************************************************************/

CREATE or replace procedure SEG01_create_temporal_root_aggregations(
                in @_temporal_library_rule_id bigint,
                in @_temporal_type            integer     /* 1:viewed, 2:broadcast, 3:event */
                ) AS
BEGIN


  -- **********
  --start process here -->


DECLARE @temporal_type              integer
--DECLARE @temporal_library_rule_id   bigint
DECLARE @min_datetime_type_str      varchar(36)
DECLARE @max_datetime_type_str      varchar(36)
DECLARE @datetime_type_col_str      varchar(36)
DECLARE @current_id                 bigint
DECLARE @current_id_max             bigint
DECLARE @event_table_name           varchar(52)
DECLARE @schema_name                varchar(36)
DECLARE @period_start_datetime      datetime
DECLARE @period_end_datetime        datetime


--temporary parameters
    SET @_temporal_type = 1 -- currently always default to 1 as the queries below are not dynamic yet
/*    SET @_temporal_library_rule_id = 5
*/
------


IF @_temporal_type = 2
  BEGIN
    SET @min_datetime_type_str = 'min_broadcast_datetime'
    SET @max_datetime_type_str = 'max_broadcast_datetime'
  END
ELSE IF @_temporal_type = 3
  BEGIN
    SET @min_datetime_type_str = 'min_event_datetime'
    SET @max_datetime_type_str = 'max_event_datetime'
  END
ELSE
  BEGIN
    SET @min_datetime_type_str = 'min_viewed_datetime'
    SET @max_datetime_type_str = 'max_viewed_datetime'
  END




--temp
 IF object_id('SEG01_event_tables_tmp') IS NOT NULL
   BEGIN
     DROP TABLE SEG01_event_tables_tmp
   END
-- temp end




--let's assume we are going to use viewed_time for now...
--any table that has an event between these two dates is required
execute(
   ' select dense_rank() over(order by table_name) uniqid, ev_tbls.schema_name, table_name          '||
   --'   into #SEG01_event_tables_tmp                                                                 '||
   '   into SEG01_event_tables_tmp                                                                 '||
   '   from SEG01_viewed_dp_event_table_summary_tbl ev_tbls, SEG01_temporal_library_tbl lib         '||
   '  where lib.uniqid = '||@_temporal_library_rule_id||
   '    and (period_start_datetime between '||@min_datetime_type_str||' and '||@max_datetime_type_str||
   '      or period_end_datetime   between '||@min_datetime_type_str||' and '||@max_datetime_type_str||')')


/*
select *
  from SEG01_viewed_dp_event_table_summary_tbl ev_tbls


select dense_rank() over(order by table_name) uniqid, ev_tbls.schema_name, table_name, ev_tbls.*, lib.*
      from SEG01_viewed_dp_event_table_summary_tbl ev_tbls, SEG01_temporal_library_tbl lib
     where lib.uniqid = 5


select *
  from  #SEG01_event_tables_tmp
*/


/**
 * At this point we have a list of event tables (and schemas?)
 * - VESPA_DP_PROG_VIEWED_201303
 * - VESPA_DP_PROG_VIEWED_201212
***/

--now we need to apply one of these rules to root_viewing_aggregations

--we're using 'SEG01_root_temporal_tbl' to store this information
/*
select top 100 *
  from SEG01_root_temporal_tbl
*/


--what period are we actually looking at here????
--temp 2 lines (replace with 2 lines above)
--select @period_start_datetime = '2012-12-15 19:00:00.000000',
--       @period_end_datetime   = '2012-12-15 20:00:00.000000'
select @period_start_datetime = period_start_datetime,
       @period_end_datetime   = period_end_datetime
  from SEG01_temporal_library_tbl
  where uniqid = @_temporal_library_rule_id--depending on rule required


select @current_id_max = max(uniqid)
--  from #SEG01_event_tables_tmp
  from SEG01_event_tables_tmp


--start extraction loop here -->
SET @current_id = 1


/* create tmp table to insert into - for combining more than one event table
 * only required if joining more than one table together... but at the moment
 * lets do this as a standard process.
 */
 IF object_id('SEG01_combined_event_tmp') IS NOT NULL
   BEGIN
     DROP TABLE SEG01_combined_event_tmp
   END

create table SEG01_combined_event_tmp(
        pk_viewing_prog_instance_fact   bigint,
        account_number                  varchar(24),
        instance_start_date_time_utc    datetime,
        instance_end_date_time_utc      datetime,
        capping_end_date_time_utc       datetime/*,
        max_from_date                   datetime,
        min_to_date                     datetime*/
)--commit



while @current_id <= @current_id_max

  BEGIN

     select @event_table_name = table_name,
            @schema_name      = schema_name
--       from #SEG01_event_tables_tmp
       from SEG01_event_tables_tmp
      where uniqid = @current_id


     /* Insert each of the required viewing tables into a tempory table */
     execute(' INSERT SEG01_combined_event_tmp '||
             ' select pk_viewing_prog_instance_fact, account_number, instance_start_date_time_utc, instance_end_date_time_utc, capping_end_date_time_utc '||
         /*    '        case when instance_start_date_time_utc <   '''||@period_start_datetime||''' then '''||@period_start_datetime||''''||
             '             when instance_start_date_time_utc >=  '''||@period_start_datetime||''' then instance_start_date_time_utc    '||
             '             else null      '||
             '         end, '|| --max_from_date
             '        case when instance_end_date_time_utc     <= '''||@period_end_datetime||'''  AND instance_end_date_time_utc     <= capping_end_date_time_utc      then instance_end_date_time_utc    '||
             '             when '''||@period_end_datetime||''' <= instance_end_date_time_utc      AND '''||@period_end_datetime||''' <= capping_end_date_time_utc      then '''||@period_end_datetime||''''||
             '             when capping_end_date_time_utc      <= instance_end_date_time_utc      AND capping_end_date_time_utc      <= '''||@period_end_datetime||''' then capping_end_date_time_utc     '||
             '             else null   '||
             '         end '|| --min_to_date
           */
             '   from '||@schema_name||'.'||@event_table_name||
             --time related restriction on the relevant temporal field - treat as a dimenstion, so select from a tag and assign a restriction
             '  where instance_end_date_time_utc  >=  '''||@period_start_datetime||''''||   -- <-- input required from user, currently following same period as temporal_definition
             '    and instance_start_date_time_utc <= '''||@period_end_datetime  ||'''')

      SET @current_id = @current_id + 1

   END



--we'll have to use this until we can put the case statment bits in the above query....
-- A fix for this has been provided by TonyK, but not implemented yet....


--put all this into the temporal_table
--truncate table SEG01_root_temporal_tbl
INSERT into SEG01_root_temporal_tbl
     select --top 1000
            pk_viewing_prog_instance_fact,
            account_number,
            @_temporal_library_rule_id temporal_id,
            --@period_start_datetime,
            --@period_end_datetime,
            --instance_start_date_time_utc,
            --instance_end_date_time_utc,
            --capping_end_date_time_utc,
            case when instance_start_date_time_utc < @period_start_datetime then @period_start_datetime
                 when instance_start_date_time_utc >=  @period_start_datetime then instance_start_date_time_utc
                 else null
             end max_from_date,
            case when instance_end_date_time_utc <= @period_end_datetime        AND instance_end_date_time_utc <= capping_end_date_time_utc  then instance_end_date_time_utc
                 when @period_end_datetime       <= instance_end_date_time_utc  AND @period_end_datetime       <= capping_end_date_time_utc  then @period_end_datetime
                 when capping_end_date_time_utc  <= instance_end_date_time_utc  AND capping_end_date_time_utc  <= @period_end_datetime       then capping_end_date_time_utc
                 else null
             end min_to_date,
            --duration_attributed,
            datediff(ss, instance_start_date_time_utc, instance_end_date_time_utc) total_duration,
            --coalesce(duration_attributed, duration) instance_duration,
            --datediff(ss, @period_start_datetime, @period_end_datetime) recurrence_duration,
            datediff(ss, max_from_date, min_to_date) period_duration
            --datediff(ss, instance_start_date_time_utc, @period_start_datetime) btwn_instance_n_period_start, --  +'ve period means that the instance started before the period began
            --datediff(ss, @period_end_datetime, instance_end_date_time_utc) btwn_period_n_instance_end        --  +'ve period means that the instance ends after the period ends
       from SEG01_combined_event_tmp
      where max_from_date <= min_to_date
        and datediff(ss, instance_start_date_time_utc, instance_end_date_time_utc) > 6  --we could add this total_period restriction on at this point

 /*

re-instate this after de-bugging...

 IF object_id('SEG01_combined_event_tmp') IS NOT NULL
   BEGIN
     DROP TABLE SEG01_combined_event_tmp
   END
*/
commit


END;



commit;


------------------------------proc ends here
----------------------------------------------

---test


select top 1000 *
from SEG01_combined_event_tmp


select top 1000 *
from SEG01_event_tables_tmp



 ' select dense_rank() over(order by table_name) uniqid, ev_tbls.schema_name, table_name          '||
   --'   into #SEG01_event_tables_tmp                                                                 '||
   '   into SEG01_event_tables_tmp                                                                 '||
   '   from SEG01_viewed_dp_event_table_summary_tbl ev_tbls, SEG01_temporal_library_tbl lib         '||
   '  where lib.uniqid = '||@_temporal_library_rule_id||
   '    and (period_start_datetime between '||@min_datetime_type_str||' and '||@max_datetime_type_str||
   '      or period_end_datetime   between '||@min_datetime_type_str||' and '||@max_datetime_type_str||')')





 select a.*
    from seg01_build_list_tmp2 a

select *
from seg01_build_list_tmp2 rs;


  select top 10000
         rs.segment_id, a.schema_name, a.table_name, a.col_name, a.operator, a.condition, count(1) sample_count
    from SEG01_root_segment_tbl rs,
         seg01_build_list_tmp2 a
   where rs.segment_id = a.uniqid
group by rs.segment_id, a.schema_name, a.table_name, a.col_name, a.operator, a.condition



--for top 1000 records for the last segment_id created
select top 1000 *
from SEG01_root_segment_tbl
where segment_id = (select max(segment_id) from SEG01_root_segment_tbl)


select *
from SEG01_root_segment_desc_tbl


--both together
select top 1000 *
from SEG01_root_segment_tbl r,
     SEG01_root_segment_desc_tbl d
where r.segment_id = d.segment_id
  and r.segment_id = 100



select *
from seg01_build_list_tmp2

--truncate table seg01_log_tbl

select *
from seg01_log_tbl


--- end test

---------------------- old code


-- how to handle the period of data extracted????
-- @ possibly have a lookup table that lists all the viewing tables available and which periods they cover?
-- @ actually what's probably better is to have a procedure that extracts, as we need to handle the tail
--   ends of the months too, so we know what's actually viewed within the month(or other defined) period






--to extract combined segmentation
select account_number,
       9102 segment_id,
       sum(duration) total_football_greater_6_secs_speed_equals_1,-- a.pk_viewing_prog_instance_fact
       count(duration) sessions
  from SEG01_root_segment_tbl a,
       SEG01_root_segment_tbl b,
       SEG01_root_segment_tbl c,
       barbera.samplesegintobau z
 where a.pk_viewing_prog_instance_fact = b.pk_viewing_prog_instance_fact -- this should be subscriber_id really??...
   and a.pk_viewing_prog_instance_fact = c.pk_viewing_prog_instance_fact -- this should be subscriber_id really??...
   and a.pk_viewing_prog_instance_fact = z.pk_viewing_prog_instance_fact
   and a.segment_id = 99
   and b.segment_id = 100
   and c.segment_id = 101
   --tmp condition
   and account_number = '200004544389'
group by account_number


select *
from barbera.samplesegintobau z
where account_number = '200004544389'
order by instance_start_date_time_utc

------ end old code
------------------------


--drop the temporary build list
drop table seg01_build_list_tmp2;



--optimiser <optimises the code to make segmentation creation efficient>


END;
