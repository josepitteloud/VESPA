

/**************************************************************************
 **                                                                      **
 **  So what we need here is a proc that extracts a very mimimal set     **
 **  of data that we will use later for calculating the aggregation on   **


Want:
    1. @_root_temporal_defn_tbl table containing the following columns:
         <aggregation_type, aggregation_id, temporal_id, datetime_type_field>
    2. tags that are going to be used in aggregation
    3.

 **                                                                      **
 **************************************************************************/

CREATE or replace procedure SEG01_create_tmp_dataset_multiroot_tbl(
            --combine into a table. 1 row per root/trunk
                 in @_filter_id                              bigint,
             --    in @_root_temporal_defn_tbl                 varchar(48),   --time related field that the temporal rule will be based on
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


exec seg01_log 'SEG01_create_tmp_dataset_multiroot_tbl'



IF object_id('SEG01_pk_list_tmp') IS NOT NULL
    BEGIN
        drop table SEG01_pk_list_tmp
    END
IF object_id('SEG01_pk_tmp') IS NOT NULL
    BEGIN
        drop table SEG01_pk_tmp
    END
IF object_id('SEG01_pk_prog_temporal_list_tmp') IS NOT NULL
    BEGIN
        drop table SEG01_pk_prog_temporal_list_tmp
    END
IF object_id('SEG01_trunk_audit_desc_tmp') IS NOT NULL
    BEGIN
        drop table SEG01_trunk_audit_desc_tmp
    END
commit



--ok so we want a list of pk_viewing_instances

execute(' select t.*, dense_rank() over(order by uniqid asc) rank '||
        '   into SEG01_pk_tmp '||
        '   from SEG01_trunk_filter_defn_tbl t'||
        '  where t.filter_id = '||@_filter_id)
commit



DECLARE  @max_row              bigint
DECLARE  @current_row          bigint
DECLARE  @aggregation_type     integer
DECLARE  @aggregation_id       bigint
DECLARE  @trunk_id             bigint
DECLARE  @temporal_id          bigint
DECLARE  @datetime_type_field  integer
DECLARE  @filter_type          integer
DECLARE  @filter_id            bigint


select @max_row = max(rank),
       @current_row = min(rank)
  from SEG01_pk_tmp

--basically the objective of this routine is to filter out pk_viewing_prog_instance_facts that are not wanted
create table SEG01_pk_prog_temporal_list_tmp(
    pk_viewing_prog_instance_fact   bigint  not null,
    total_duration                  bigint  not null,
    period_duration                 bigint  not null
)
commit

--not currently being used
create table SEG01_trunk_audit_desc_tmp (
    uniqid              bigint      not null    identity,
    trunk_id            bigint      not null,
    aggregation_id      bigint      not null,
    aggregation_type    integer     not null
)
commit


--set trunk_id...
--exec SEG01_assign_trunk_aggregation_id @trunk_id



--so the rules will be - if ADD defined by the root meta-data, then remove from this list
-- if OR defined by the root meta-data, then add to this list



WHILE @current_row <= @max_row
    BEGIN
        --get the first row, set params - get rules
        execute(
            ' select @aggregation_type    = aggregation_type,    '||
            '        @aggregation_id      = aggregation_id,      '||
            '        @temporal_id         = temporal_id,         '||
            '        @datetime_type_field = datetime_type_field, '||
            '        @filter_type         = filter_type,         '|| -- AND[0]/OR[1]
            '        @filter_id           = filter_id            '||
            '   from SEG01_pk_tmp '||
            '  where rank = '||@current_row)


        --record the audit info for the trunk
        --this is just a filtered data set at the moment... don't need this trunk desc
 --       INSERT into SEG01_trunk_audit_desc_tmp(trunk_id, aggregation_id, aggregation_type)
 --           values(@trunk_id, @aggregation_id, @aggregation_type)


        --exec seg01_log 'multiroot: 0'
        exec seg01_log '@temporal_id: '||@temporal_id


        /************************
         ** So this now:
         **   1. makes a list of pk_viewing_instances & duration, according to the cross-section of roots (aggregation + temporal)
         **   2. uses this list against the viewing events
         ***********************/
        if @aggregation_type = 0
            BEGIN -- then root aggregation, joined with temporal info

                --  IF OR (or SEG01_pk_prog_temporal_list_tmp.size = 0) : insert into table
                declare @list_size integer

-- exec seg01_log 'multiroot: 0.1'

                select @list_size = count(1)
                  from SEG01_pk_prog_temporal_list_tmp


                if(@list_size = 0) OR (@filter_type = 1)
                    BEGIN
--                        exec seg01_log 'multiroot: 1'
                        execute(--only insert if not in the list already??
                            ' INSERT into SEG01_pk_prog_temporal_list_tmp '||
                            ' select s.pk_viewing_prog_instance_fact, t.total_duration, t.period_duration '||
                            '   from SEG01_root_segment_tbl s, SEG01_root_temporal_tbl t '||
                            '  where s.pk_viewing_prog_instance_fact = t.pk_viewing_prog_instance_fact '||
                            '    and s.segment_id = '||@aggregation_id||
                            '    and t.temporal_library_id = '||@temporal_id||
                            '    and s.pk_viewing_prog_instance_fact not in (select pk_viewing_prog_instance_fact from SEG01_pk_prog_temporal_list_tmp)')
                        commit
--                        exec seg01_log 'multiroot: 2'
                    END

                --  IF AND: join to table, and store inner join
                -- 1. join into temporary table  USING 'AND' meta-data method
                ELSE
                    BEGIN
--                        exec seg01_log 'multiroot: 3'
                        execute(
                            ' SELECT * into SEG01_pk_list_tmp FROM '||
                            ' (select s.pk_viewing_prog_instance_fact, t.total_duration, t.period_duration '||
                            '   from SEG01_root_segment_tbl s, SEG01_root_temporal_tbl t '||
                            '  where s.pk_viewing_prog_instance_fact = t.pk_viewing_prog_instance_fact '||
                            '    and s.segment_id = '||@aggregation_id||
                            '    and t.temporal_library_id = '||@temporal_id||') data, '||
                            ' SEG01_pk_prog_temporal_list_tmp list '||--join to existing table
                            ' WHERE data.pk_viewing_prog_instance_fact = list.pk_viewing_prog_instance_fact ')
                        commit

--                        exec seg01_log 'multiroot: 4'

                -- 2. truncate table
                        truncate table SEG01_pk_prog_temporal_list_tmp
                -- 3. insert into truncated table
                        INSERT into SEG01_pk_prog_temporal_list_tmp select * from SEG01_pk_list_tmp
                        commit

--                        exec seg01_log 'multiroot: 5'

                    END
            END

        SET @current_row = @current_row+1
    END

    --THIS TABLE <SEG01_pk_prog_temporal_list_tmp>, WILL BE JOINED TO THE EVENTS TABLE LATER IN THE CODE




    --get event tables required for this temporal period
    exec SEG01_get_relevant_event_tables @temporal_id,  @datetime_type_field,  @event_table_list


    exec seg01_log ' Error on return<'||@@error||'>'


    SET @_db_table_name = 'SEG01_combined_events_'||dateformat(getdate(),'yyyymmddhhnnss')||'_'||@temporal_id||'tmp'



    -- set the number of event tables we'll need to iterate through
    execute(' select @current_id_max = max(uniqid) '||
            '   from '||@event_table_list)


    SET @current_id = 1


    exec seg01_log '@event_table_list = '||@event_table_list
    exec seg01_log '@current_id = '||@current_id
    exec seg01_log '@current_id_max = '||@current_id_max


    --tmp line
    --execute(' select top 100 * '||
    --        '   from '||@event_table_list)



    --iterate through
    while @current_id <= @current_id_max

       BEGIN

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





          /********************************************************************
           **   I F   Y O U   W A N T   T O   J O I N   T O   A N Y          **
           **   T A B L E   T H A T   I S   N O T   E V E N T   B A S E D,   **
           **   T H I S   I S   T H E   P L A C E   T O   D O   I T          **
           ********************************************************************/


          IF  @current_id = 1
              BEGIN--think this should be re-labeled: 'segment_id' = 'root_id'
                  SET @sql_str1 = ' select e.account_number, '||@filter_id||' filter_id, '||@temporal_id||' temporal_id, '||@_comma_seperated_list_of_tags_required||' '
                  SET @sql_str2 = ' into '||@_db_table_name
              END
          ELSE --IF @current_id <> 1
              BEGIN
                  SET @sql_str1 = ' INSERT into '||@_db_table_name||' '--auto name and use an out parameter
                  SET @sql_str2 = ' select e.account_number, '||@filter_id||' filter_id, '||@temporal_id||' temporal_id, '||@_comma_seperated_list_of_tags_required||' '
              END

          -- add Channel Mapping etc... to this list.
          --join to the table of pk_prog_viewing_instance_facts created earlier from the combination of roots process


          execute(--'INSERT into '||@_db_table_name||' '||--auto name and use an out parameter
                  --' select e.account_number,'||@_root_id||', '||@_temporal_library_rule_id||', '||@_comma_seperated_list_of_tags_required||' '||
                  @sql_str1||--dynamic set above
                  @sql_str2||--dynamic set above
                  --  '   from '||@schema_name||'.'||@event_table_name||' e, SEG01_root_segment_tbl r, SEG01_root_temporal_tbl t, '||
                     '   from '||@schema_name||'.'||@event_table_name||' e, SEG01_pk_prog_temporal_list_tmp t, '||-- all PKs now in this list
                     --------------
                     '        bednaszs.VAggr_02_Channel_Mapping cm,    '||
                     '        vespa_shared.Aggr_Account_Attributes aa '||
                     '  where e.pk_viewing_prog_instance_fact = t.pk_viewing_prog_instance_fact '||
                     --'    and r.pk_viewing_prog_instance_fact = e.pk_viewing_prog_instance_fact '||
                     '    and e.Service_Key = cm.Service_Key '||
                     '    and cm.Effective_To = ''2999-12-31 00:00:00.000'' '||
                     '    and e.account_number = aa.account_number '||
                     --'    and r.segment_id = '||@_root_id||
                     --'    and t.temporal_library_id = '||@_temporal_library_rule_id||
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

          SET @current_id = @current_id + 1

       END

--uncomment these to tidy-up...
      -- execute(' drop table '||@event_table_list)
      -- execute(' commit ')

END;

commit

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


--make filter_id

DECLARE @filter_id bigint

exec SEG01_assign_filterid @filter_id

--fill the table with come restrictions
INSERT into SEG01_trunk_filter_defn_tbl(filter_id, aggregation_id, aggregation_type, temporal_id, datetime_type_field, filter_type)  values(@filter_id, 133, 0, 8, 1, 0)
INSERT into SEG01_trunk_filter_defn_tbl(filter_id, aggregation_id, aggregation_type, temporal_id, datetime_type_field, filter_type)  values(@filter_id, 127, 0, 8, 1, 0)
INSERT into SEG01_trunk_filter_defn_tbl(filter_id, aggregation_id, aggregation_type, temporal_id, datetime_type_field, filter_type)  values(@filter_id, 129, 0, 8, 1, 0)
commit

--test with proc
DECLARE @filter_id bigint
exec SEG01_assign_filterid @filter_id
exec SEG01_insert_trunk_filter_defn_tbl
                 @filter_id,
                 133,
                 0,
                 39,
                 1,
                 0,
                 1 -- auto commit [0]false, [1]true



select *
from SEG01_trunk_filter_defn_tbl


----------------
--now start here -->


  DECLARE @_comma_seperated_list_of_tags_required  varchar(500) --should be set from a proc variable above
 -- DECLARE @_root_id                   bigint
 -- DECLARE @_temporal_library_rule_id  bigint
 -- DECLARE @_datetime_type_field       integer
  DECLARE @_db_table_name             varchar(64)
  DECLARE @_filter_id                 bigint

  --ideally this would be set from the Meta-data that relates to the Trunk aggregation descriptor
  SET @_comma_seperated_list_of_tags_required = ' e.pk_viewing_prog_instance_fact, '||
                                                ' e.dk_programme_instance_dim, '||
                                                ' t.period_duration, '||
                                                ' coalesce(aa.days_data_returned, 0) as days_data_returned, '||
                                                ' coalesce(aa.ent_tv3d_sub,       0) as ent_tv3d_sub, '||
                                                ' coalesce(aa.movmt_tv3d_sub,     0) as movmt_tv3d_sub, '||
                                                ' cm.channel_type, '||
                                                ' cm.format '

  SET @_filter_id = 2


/*****************************************************
 * Set-up parameters for combining data into the Trunk
 *****************************************************/



--this could be changed just to use the filter_id for creating the dataset
  exec SEG01_create_tmp_dataset_multiroot_tbl @_filter_id, @_comma_seperated_list_of_tags_required, @_db_table_name


select @_db_table_name





commit



select *
from SEG01_trunk_audit_desc_tmp

select *
from SEG01_trunk_filter_defn_tbl


truncate table SEG01_log_tbl
commit

select *
from SEG01_log_tbl


--
select top 1000 *
  from SEG01_combined_events_20131022111621_tmp

select top 1000 *
  from SEG01_combined_events_20131018152411_8tmp

select distinct aggregation_id
  from SEG01_combined_events_20131018152411_8tmp

select top 1000 *
  from SEG01_event_tbls_20131016154117_tmp

select *
from SEG01_log_tbl

select *
  from SEG01_root_trunk_comb_defn_tmp





  exec SEG01_create_tmp_dataset_multiroot_tbl 'SEG01_root_trunk_comb_defn_tmp', @_comma_seperated_list_of_tags_required, @_db_table_name




----------------------------------------------------- END

 -- select @_db_table_name


  INSERT into SEG01_tmp_event_table_log_tbl(table_name, filter_id, tags_required)
      values(@_db_table_name, @_filter_id, @_comma_seperated_list_of_tags_required)
  commit


select top 100 *
from SEG01_trunk_filter_defn_tbl

select top 100 *
from SEG01_tmp_event_table_log_tbl

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
SEG01_combined_events_20131002134314_8tmp -- <---- seems to have duplicates in [due to history in the account_attributes table]


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



