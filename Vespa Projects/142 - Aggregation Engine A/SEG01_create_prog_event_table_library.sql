
/***********************************************************************************
 **  CREATE the EVENT TABLE LIBRARY:
 **     This creates a lookup table holding all the tables holding viewing events
 **     Table name <SEG01_viewed_dp_event_table_summary_tbl> is used. The basis of time
 **     is recorded for: viewied, broadcast, and time of event in the table.
 **     [future extension would be to set the table name as a parameter]
 ***********************************************************************************/

CREATE or replace procedure SEG01_create_prog_event_table_library() AS
BEGIN

   DECLARE @_schema_name             VARCHAR(24)
   DECLARE @current_event_table_str  VARCHAR(76)
   DECLARE @max_uniqid               BIGINT
   DECLARE @current_uniqid           BIGINT
   DECLARE @min_viewed_datetime      DATETIME
   DECLARE @max_viewed_datetime      DATETIME
   DECLARE @min_broadcast_datetime   DATETIME
   DECLARE @max_broadcast_datetime   DATETIME
   DECLARE @min_event_datetime       DATETIME
   DECLARE @max_event_datetime       DATETIME


   SET @_schema_name = 'sk_prod'

   --auto list of event tables
    IF object_id('SEG01_viewed_dp_event_table_summary_tbl') IS NOT NULL
    BEGIN
      DROP TABLE SEG01_viewed_dp_event_table_summary_tbl
    END

   CREATE TABLE SEG01_viewed_dp_event_table_summary_tbl(
     uniqid                         BIGINT         NOT NULL identity,
     schema_name                    VARCHAR(24)    NOT NULL,
     table_name                     VARCHAR(52)    NOT NULL,
     min_viewed_datetime            DATETIME       DEFAULT NULL,
     max_viewed_datetime            DATETIME       DEFAULT NULL,
     min_broadcast_datetime         DATETIME       DEFAULT NULL,
     max_broadcast_datetime         DATETIME       DEFAULT NULL,
     min_event_datetime             DATETIME       DEFAULT NULL,
     max_event_datetime             DATETIME       DEFAULT NULL)


   --- get a list of table names that are named Vespa_DP_PROG_VIEWED_XXXXXX
   INSERT INTO SEG01_viewed_dp_event_table_summary_tbl (schema_name, table_name)
     select distinct creator, tname
       --into SEG01_event_tbl_list_tmp
       from sys.syscolumns a
      where creator = @_schema_name
        and upper(tname) like '%VESPA_DP_PROG_VIEWED_%'
        and upper(tname) not like '%BACKUP%'


   --now work through this list of event tables and extract the min & max dates from each one, viewed & broadcast
   select @max_uniqid = coalesce(max(uniqid), -1)
     --from SEG01_event_tbl_list_tmp
     from SEG01_viewed_dp_event_table_summary_tbl


   SET @current_uniqid = 1

   WHILE @current_uniqid <= @max_uniqid
     BEGIN
        SELECT @current_event_table_str  =  schema_name||'.'||table_name
          --from SEG01_event_tbl_list_tmp
          from SEG01_viewed_dp_event_table_summary_tbl
         where uniqid = @current_uniqid



        execute('select @min_viewed_datetime    = min(instance_start_date_time_utc),  '||
                '       @max_viewed_datetime    = max(instance_end_date_time_utc),    '||
                '       @min_broadcast_datetime = min(broadcast_start_date_time_utc), '||
                '       @max_broadcast_datetime = max(broadcast_end_date_time_utc),   '||
                '       @min_event_datetime     = min(event_start_date_time_utc), '||
                '       @max_event_datetime     = max(event_end_date_time_utc)    '||
                '  from '||@current_event_table_str)

        --commit
/*
select top 10 *
from sk_prod.VESPA_DP_PROG_VIEWED_201301
*/


        execute('UPDATE SEG01_viewed_dp_event_table_summary_tbl '||
                '   SET min_viewed_datetime    = '''||@min_viewed_datetime   ||''', '||
                '       max_viewed_datetime    = '''||@max_viewed_datetime   ||''', '||
                '       min_broadcast_datetime = '''||@min_broadcast_datetime||''', '||
                '       max_broadcast_datetime = '''||@max_broadcast_datetime||''', '||
                '       min_event_datetime     = '''||@min_event_datetime    ||''', '||
                '       max_event_datetime     = '''||@max_event_datetime    ||'''  '||
                ' where uniqid = '||@current_uniqid)
        --commit

        SET @current_uniqid = @current_uniqid + 1
     END
     --commit all changes at the same time (mainly to avoid queing issues on the server)
     commit


IF object_id('SEG01_event_tbl_list_tmp') IS NOT NULL
    BEGIN
      DROP TABLE SEG01_event_tbl_list_tmp
      commit
    END

/*
select *
  from SEG01_viewed_dp_event_table_summary_tbl
*/


END;


