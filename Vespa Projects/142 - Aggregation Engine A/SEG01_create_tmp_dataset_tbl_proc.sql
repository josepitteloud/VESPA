

/**************************************************************************
 **                                                                      **
 **  So what we need here is a proc that extracts a very mimimal set     **
 **  of data that we will use later for calculating the aggregation on   **


Want:
    1. account_number
    2. tag that is going to be used in aggregation
    3.

 **                                                                      **
 **************************************************************************/

CREATE or replace procedure SEG01_create_tmp_dataset_tbl(
                 in @_root_id                                bigint,
                 in @_temporal_library_rule_id               bigint,
                 in @_datetime_type_field                    integer,   --time related field that the temporal rule will be based on
                 in @_comma_seperated_list_of_tags_required  varchar(500),
                out @_db_table_name                          varchar(48)
                ) AS
BEGIN

  DECLARE @_tag_name                  varchar(64)
  DECLARE @col_name                   varchar(48)
  DECLARE @_metric_str                varchar(24)
  DECLARE @event_table_list           varchar(48)
  DECLARE @current_id_max             bigint
  DECLARE @current_id                 bigint
  DECLARE @event_table_name           varchar(48)
  DECLARE @schema_name                varchar(24)

  DECLARE @sql_str1                   varchar(500)
  DECLARE @sql_str2                   varchar(500)
  --DECLARE @sql_str3                   varchar(500)


exec seg01_log 'SEG01_create_tmp_dataset_tbl'

--temp declarations
/*  DECLARE @_comma_seperated_list_of_tags_required  varchar(128) --should be set from a proc variable above
  DECLARE @_root_id                bigint
  DECLARE @_temporal_library_rule_id  bigint
  DECLARE @_datetime_type_field       integer
  DECLARE @_db_table_name             varchar(64)

  SET @_comma_seperated_list_of_tags_required = 'e.pk_viewing_prog_instance_fact, e.dk_programme_instance_dim'
  SET @_root_id = 129
  SET @_temporal_library_rule_id = 5
  SET @_datetime_type_field = 1
*/
--end temp





  --so basically if they are event tables - just join to the extract above, then join to any additional tables that are mentioned
--  IF '%event%' in ( select distinct table_proxy
--                      from SEG01_Segment_Dictionary_Tag_Types_tbl
--                    where tag_name = @_tag_name)

       -- This can be considered at a leter date... we will do a mass join here for now, and select wanted columns from it.

exec seg01_log 1

       --get event tables required for this temporal period
       exec SEG01_get_relevant_event_tables @_temporal_library_rule_id,  @_datetime_type_field,  @event_table_list

exec seg01_log 2

       SET @_db_table_name = 'SEG01_combined_events_'||dateformat(getdate(),'yyyymmddhhnnss')||'_'||@_temporal_library_rule_id||'tmp'

exec seg01_log 3

       --Set up the results table - this should be auto named really
       --better make sure this is truncated before we begin
       /*IF object_id('SEG01_combined_events_tmp') IS NOT NULL
         BEGIN
           drop table SEG01_combined_events_tmp
           commit
         END
        */


--  ***
--this query need to be constructed from the tag type library.....
--  ***



/********************************************************************************
 **  DON'T NEED THIS NOW AS BEING CREATED DURING THE PROCESS BELOW...
 **********/
       --create table with the correct columns
       --    SEG01_combined_events_tmp
/*     execute(' create table '||@_db_table_name||'( '||
             ' account_number                  varchar(20), '||
             ' aggregation_id                  bigint,  '    ||
             ' temporal_library_rule_id        bigint,  '    ||
             ' pk_viewing_prog_instance_fact   bigint,  '    ||
             ' dk_programme_instance_dim       bigint,  '    ||
             ' period_duration                 bigint      default NULL,  '    ||
             ' days_data_returned              smallint    default NULL,  '    ||
             ' ent_tv3d_sub                    bit         default 0,     '    ||
             ' movmt_tv3d_sub                  bit         default 0,     '    ||
             ' channel_type                    varchar(200), '||
             ' format                          varchar(200) '||
             ' ) ')

       commit
*/--------------------------------------------------------------



       --iterate through
       execute(' select @current_id_max = max(uniqid) '||
                  'from '||@event_table_list)


       SET @current_id = 1

exec seg01_log 4
exec seg01_log '@event_table_list = '||@event_table_list
exec seg01_log '@current_id = '||@current_id
exec seg01_log '@current_id_max = '||@current_id_max


       while @current_id <= @current_id_max

          BEGIN
exec seg01_log 5
             --set the event table details to query from
             execute('select @event_table_name = table_name, '||
                     '       @schema_name      = schema_name '||
                     ' from '||@event_table_list              ||
                     ' where uniqid = '||@current_id)


             --use the table (this is where the joins take place)
             --select @schema_name, @event_table_name


exec seg01_log '@schema_name = '||@schema_name
exec seg01_log '@event_table_name = '||@event_table_name
exec seg01_log '@_db_table_name = '||@_db_table_name

             /*******************************************************************
              ** The primary reason for doing this is so that we can join to the event table
              ** then aggregate on fields that are specified...
              **
              ** Maybe just do this section for pulling out a definable set of columns
              ** from the events table - joined to the other relevent aggregation information.
              ** Then use this master table to calculate the aggregations from. In 2 stages
              **
              *******************************************************************/



             -- N O   G R O U P I N G   R E Q U I R E D   H E R E ,  J U S T   R A W   D A T A   I N T O   1   T A B L E


            -- run this query, inserting results into a sub table
            --  execute(' select e.account_number,'||@_root_id||', '||@_temporal_library_rule_id||', '||@_metric_id||', '||@_metric_str||'  '||
            --    . . .
            --          ' group by e.account_number, '||@_root_id||', '||@_temporal_library_rule_id)



             /********************************************************************
              **   I F   Y O U   W A N T   T O   J O I N   T O   A N Y          **
              **   T A B L E   T H A T   I S   N O T   E V E N T   B A S E D,   **
              **   T H I S   I S   T H E   P L A C E   T O   D O   I T          **
              ********************************************************************/

exec seg01_log 6
             IF  @current_id = 1
                 BEGIN--think this should be re-labeled: 'segment_id' = 'root_id'
                     SET @sql_str1 = ' select e.account_number, '||@_root_id||', '||@_temporal_library_rule_id||', '||@_comma_seperated_list_of_tags_required||' '
                     SET @sql_str2 = ' into '||@_db_table_name
exec seg01_log 7
                 END
             ELSE --IF @current_id <> 1
                 BEGIN
                     SET @sql_str1 = ' INSERT into '||@_db_table_name||' '--auto name and use an out parameter
                     SET @sql_str2 = ' select e.account_number, '||@_root_id||', '||@_temporal_library_rule_id||', '||@_comma_seperated_list_of_tags_required||' '
exec seg01_log 8
                 END

exec seg01_log 9

        -- add Channel Mapping etc... to this list.

             execute(--'INSERT into '||@_db_table_name||' '||--auto name and use an out parameter
                     --' select e.account_number,'||@_root_id||', '||@_temporal_library_rule_id||', '||@_comma_seperated_list_of_tags_required||' '||
                     @sql_str1||--dynamic set above
                     @sql_str2||--dynamic set above

                     '   from '||@schema_name||'.'||@event_table_name||' e, SEG01_root_segment_tbl r, SEG01_root_temporal_tbl t, '||
                    -- '   from bednaszs.VAggr_02_Viewing_Events e, SEG01_root_segment_tbl r, SEG01_root_temporal_tbl t, '||--temp row
                     --------------
                     '        bednaszs.VAggr_02_Channel_Mapping cm,    '||
                     '        vespa_shared.Aggr_Account_Attributes aa '||
                     '  where r.pk_viewing_prog_instance_fact = t.pk_viewing_prog_instance_fact '||
                     '    and r.pk_viewing_prog_instance_fact = e.pk_viewing_prog_instance_fact '||
                     '    and e.Service_Key = cm.Service_Key '||
                     '    and cm.Effective_To = ''2999-12-31 00:00:00.000'' '||
                     '    and e.account_number = aa.account_number '||
                     '    and r.segment_id = '||@_root_id||
                     '    and t.temporal_library_id = '||@_temporal_library_rule_id||
                     '    and aa.period_key = 5 '|| -- FIX FOR THIS ... month is a manual hack!!!!
                     --additional restrictions used by the RIA project
                     '    and e.capped_full_flag = 0 '||
                     '    and e.panel_id = 12 '||
                     '    and e.instance_start_date_time_utc < e.instance_end_date_time_utc  '||             -- Remove 0sec instances
                     '    and (e.reported_playback_speed is null or e.reported_playback_speed = 2) '||
                     '    and e.broadcast_start_date_time_utc >= dateadd(hour, -(24*28), e.event_start_date_time_utc) '||
                     '    and e.account_number is not null '||
                     '    and e.subscriber_id is not null '||
                     '    and e.type_of_viewing_event in (''HD Viewing Event'', ''Sky+ time-shifted viewing event'', ''TV Channel Viewing'', ''Other Service Viewing Event'' )')

                commit

exec seg01_log 10
             SET @current_id = @current_id + 1

          END


  execute(' drop table '||@event_table_list)
  execute(' commit ')

END;



---test
/*
 select top 10000 *
   from SEG01_combined_events_tmp

 select count(1)
   from SEG01_combined_events_tmp


select top 100 *
  from vespa_shared.Aggr_Account_Attributes


select top 1000 *
  from  SEG01_root_temporal_tbl t

*/




/*****************************
 ***************************
 ***     EXAMPLE
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


 -- select @_db_table_name


  INSERT into SEG01_tmp_event_table_log_tbl(table_name, root_id, temporal_id, datetime_type_field, tags_required)
      values(@_db_table_name, @_root_id, @_temporal_library_rule_id, @_datetime_type_field, @_comma_seperated_list_of_tags_required)
  commit


--SEG01_combined_events_20131002134314_8tmp



--ok, so to get the details of the table_name that meets the requirements: root_id = 133    and temporal_id = 8
--get the list of account s that are filtered from creating the TRUNK

declare @table_name varchar(60)
select @table_name = table_name   from SEG01_tmp_event_table_log_tbl e  where root_id = 133    and temporal_id = 8

execute('select top 10 * into #tmp_e from '||@table_name)
select *
from #tmp_e




--use the table name that is stored in the parameter @_db_table_name

select top 100 *
  from --SEG01_combined_events_20130918219018_8tmp   -- <-----@_db_table_name
--SEG01_combined_events_20130924119011_8tmp
SEG01_combined_events_20131002134314_8tmp -- <---- seems to have duplicates in


select top 1 *
from vespa_shared.Aggr_Account_Attributes aa
where aa.period_key = 5


  --role this table up into an aggregation
--TOTAL VIEWING
select account_number, sum(period_duration)
  from SEG01_combined_events_20130918219018_8tmp
group by account_number

-- 3D VIEWING
select account_number, sum(period_duration)
  from SEG01_combined_events_20130918219018_8tmp
 where format = '3D'
   and ent_tv3d_sub = 1
   and movmt_tv3d_sub = 0
group by account_number



/**
 * Create the RIA Aggregation for 3D Viewing
 */

-- 3D SOV
select viewing_total.account_number, viewing_3D.period period_3D, viewing_total.period period_total,  viewing_total.period/data_returned.days period_total_av, cast(viewing_3D.period as double)/cast(viewing_total.period as double) as SOV_3D
  from
   --TOTAL VIEWING
   (select account_number, sum(period_duration) period
      from SEG01_combined_events_20130918219018_8tmp
  group by account_number) viewing_total,
   -- 3D VIEWING
   (select account_number, sum(period_duration) period
      from SEG01_combined_events_20130918219018_8tmp
     where format = '3D'
   and ent_tv3d_sub = 1
   and movmt_tv3d_sub = 0
  group by account_number) viewing_3D,
  -- DAYS DATA RETURNED
 (select distinct account_number, avg(days_data_returned) days
      from SEG01_combined_events_20130918219018_8tmp
  group by account_number) data_returned
where viewing_total.account_number = viewing_3D.account_number
  and viewing_total.account_number = data_returned.account_number

-----------------


grant select on SEG01_combined_events_20130918219018_8tmp to ngm
commit

select account_number, count(1) sample_count
  from SEG01_combined_events_20130918219018_8tmp t
 where format = '3D'
group by account_number
order by sample_count desc


select account_number, round(avg(days_data_returned),0) days_data_returned
from SEG01_combined_events_20130918219018_8tmp t
group by account_number





--test

select *
  from SEG01_combined_events_20131008090159_8tmp


  DECLARE @_comma_seperated_list_of_tags_required  varchar(500) --should be set from a proc variable above
  DECLARE @_root_id                bigint
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
  SET @_db_table_name = 'SEG01_combined_events_20131008090159_8tmp'

  INSERT into SEG01_tmp_event_table_log_tbl(table_name, root_id, temporal_id, datetime_type_field, tags_required)
      values(@_db_table_name, @_root_id, @_temporal_library_rule_id, @_datetime_type_field, @_comma_seperated_list_of_tags_required)
  commit

select *
  from SEG01_tmp_event_table_log_tbl

--end test



