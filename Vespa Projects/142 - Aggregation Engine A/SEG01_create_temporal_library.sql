
/**********************************************************************************
 **
 **  ** Builds **    T E M P O R A L   L I B R A R Y
 **
 **  Tags can be re-used for the same rule, but for different periods
 **  a tag per rule is required. For example, all Mondays or weekends could have the
 **  same tag to denote all Mondays or weekend respectively. However 1wk should have
 **  a different tag to a 2nd wk if they were to be aggregated seperately
 **
 **********************************************************************************/

CREATE or replace procedure SEG01_create_temporal_library_new(
                 in @_definition_id       bigint,
                 in @_recurrence_start    datetime, --first start of the recurrence
                out @out_id          bigint
                ) AS
BEGIN

exec seg01_log 'SEG01_create_temporal_library_new(v4.1)['||now()||']'

exec seg01_log '_definition_id<'||@_definition_id||'>'
exec seg01_log '_recurrence_start<'||@_recurrence_start||'>'


   DECLARE @temporal_id                    bigint
   DECLARE @definition_id                  bigint
   DECLARE @recurrence_id                  bigint
   DECLARE @period_id                      bigint
   DECLARE @recurrence_start_datetime      datetime
   DECLARE @recurrence_start_plus          datetime --test this one
   DECLARE @recurrence_period_start        datetime --test this one
   DECLARE @recurrence_period              bigint
   DECLARE @recurrence_end_datetime        datetime
   DECLARE @recurrence_day_part_abr        varchar(3)
   DECLARE @recurrence_day_part_qty        integer

   DECLARE @period_lag                     bigint
   DECLARE @period_start_datetime          datetime
   DECLARE @period_start_plus              datetime -- test this one
   DECLARE @period_duration                bigint
   DECLARE @period_end_datetime            datetime
   DECLARE @period_day_part_abr            varchar(3)
   DECLARE @period_day_part_qty            integer


  -- SET @_recurrence_start   =  getdate()

   --set ss mi hh to 00:00:00
   SET @_recurrence_start = datepart(yy, @_recurrence_start)||'-'||
                            datepart(mm, @_recurrence_start)||'-'||
                            datepart(dd, @_recurrence_start)||' 00:00:00.000'

   IF object_id('SEG01_temporal_library_build_tmp') IS NOT NULL
      BEGIN
        DROP TABLE SEG01_temporal_library_build_tmp

      END


--need to work out what the actual period of the recurrence and period are here.. ie if it's a month, how long is it?



  -- this bit still won't quite work
--execute('SET @to_date = dateadd('||@_date_part_abbreviation||', '||@_period_quantity||', '''||@_recurrence_start||''')')
/*
@period_start_datetime     = dateadd(ss, period_lag, @_recurrence_start)
@period_end_datetime = dateadd(date_part_abbreviation, quantity, @period_start_datetime)

period = datediff(ss, @period_start_datetime, @period_end_datetime)
*/


   --retrieve period information for the definition (recurrence + period)
   select   distinct
            d.uniqid definition_id,
            t.uniqid period_id,
            t.date_part_abbreviation,
            t.quantity,
            t.period_start,
            t.period, -- convert this to period considering the recurrence start
            d.period_lag,
            d.period_type -- [0]Occurrence, [1]Period
            --dense_rank() over(partition by d.uniqid order by period desc) rank -- recurrence = 1, period = 2
       into SEG01_temporal_library_build_tmp
       from SEG01_temporal_period_tbl t, SEG01_temporal_definition_tbl d
      where t.uniqid = d.period_id
        and d.uniqid = @_definition_id
     order by definition_id, period_type

     -- Set the Recurrence information
     select @definition_id             = definition_id, --set the ID here as the same idea for rank=1 or 2
            @recurrence_id             = period_id,
            @recurrence_start_datetime = dateformat(datepart(yy, @_recurrence_start)||'-'||
                                         datepart(mm, @_recurrence_start)||'-'||
                                         datepart(dd, @_recurrence_start)||' '||
                                         datepart(hh, period_start)||':'||
                                         datepart(mi, period_start)||':'||
                                         datepart(ss, period_start),'yyyy-mm-dd hh:ms:ss'),
            @recurrence_period_start   = period_start,
            @recurrence_day_part_abr   = date_part_abbreviation,
            @recurrence_day_part_qty   = quantity
--            @recurrence_period         = period,   --this can't be set during this step... can't use period here (has to be set dynamically)
            /*@recurrence_end_datetime   = dateadd(ss, period, dateformat(datepart(yy, @_recurrence_start)||'-'||
                                         datepart(mm, @_recurrence_start)||'-'||
                                         datepart(dd, @_recurrence_start)||' '||
                                         datepart(hh, period_start)||':'||
                                         datepart(mi, period_start)||':'||
                                         datepart(ss, period_start),'yyyy-mm-dd hh:ms:ss'))*/
       from SEG01_temporal_library_build_tmp
      where period_type = 0  -- [0 = occurrence]

     -- Set the Period information
     select @period_id                 = period_id,
            @period_lag                = period_lag,
            @period_start_datetime     = dateadd(ss, period_lag, @_recurrence_start),
            @period_day_part_abr       = date_part_abbreviation,
            @period_day_part_qty       = quantity
            --@period_duration           = period,
            --@period_end_datetime       = dateadd(ss, period, dateadd(ss, period_lag, @_recurrence_start))
       from SEG01_temporal_library_build_tmp
      where period_type = 1  -- [1 = period]
     -- where period_type = (select max(period_type)
     --                        from SEG01_temporal_library_build_tmp)


-- now we need to set the following variables that can be dependant on the date the recurrence starts (months for example)
/*
 @recurrence_period
 @recurrence_end_datetime
 @period_duration
 @period_end_datetime
*/





exec seg01_log '@recurrence_day_part_abr<'||@recurrence_day_part_abr||'>'
exec seg01_log '@period_day_part_abr<'||@period_day_part_abr||'>'
exec seg01_log '@recurrence_day_part_qty<'||@recurrence_day_part_qty||'>'
exec seg01_log '@period_day_part_qty<'||@period_day_part_qty||'>'
exec seg01_log '@recurrence_start_datetime<'||@recurrence_start_datetime||'>'
exec seg01_log '@period_start_datetime<'||@period_start_datetime||'>'



--- this set of 2 IF statements is a stupid fix to get around a bug, where you can't just execute
--  a string with the day_part_abbreviation in it... when calling the proc using JDBC... blah..

if @recurrence_day_part_abr = 'yy'
    SET @recurrence_start_plus = dateadd(yy, @recurrence_day_part_qty, @recurrence_start_datetime)
if @recurrence_day_part_abr = 'mm'
    SET @recurrence_start_plus = dateadd(mm, @recurrence_day_part_qty, @recurrence_start_datetime)
if @recurrence_day_part_abr = 'dd'
    SET @recurrence_start_plus = dateadd(dd, @recurrence_day_part_qty, @recurrence_start_datetime)
if @recurrence_day_part_abr = 'hh'
    SET @recurrence_start_plus = dateadd(hh, @recurrence_day_part_qty, @recurrence_start_datetime)
if @recurrence_day_part_abr = 'mi'
    SET @recurrence_start_plus = dateadd(mi, @recurrence_day_part_qty, @recurrence_start_datetime)
if @recurrence_day_part_abr = 'ss'
    SET @recurrence_start_plus = dateadd(ss, @recurrence_day_part_qty, @recurrence_start_datetime)

if @period_day_part_abr = 'yy'
    SET @period_start_plus = dateadd(yy, @period_day_part_qty, @period_start_datetime)
if @period_day_part_abr = 'mm'
    SET @period_start_plus = dateadd(mm, @period_day_part_qty, @period_start_datetime)
if @period_day_part_abr = 'dd'
    SET @period_start_plus = dateadd(dd, @period_day_part_qty, @period_start_datetime)
if @period_day_part_abr = 'hh'
    SET @period_start_plus = dateadd(hh, @period_day_part_qty, @period_start_datetime)
if @period_day_part_abr = 'mi'
    SET @period_start_plus = dateadd(mi, @period_day_part_qty, @period_start_datetime)
if @period_day_part_abr = 'ss'
    SET @period_start_plus = dateadd(ss, @period_day_part_qty, @period_start_datetime)


--execute('SET @recurrence_start_plus = dateadd('||@recurrence_day_part_abr||', @recurrence_day_part_qty, @recurrence_start_datetime)')
--execute('SET @period_start_plus = dateadd('||@period_day_part_abr||', @period_day_part_qty, @period_start_datetime)')


exec seg01_log 'SET @recurrence_start_plus<'||@recurrence_start_plus||'>'
exec seg01_log 'SET @period_start_plus<'||@period_start_plus||'>'


SET @recurrence_end_datetime = @recurrence_start_plus  --not necessary to have two params covering the same thing really...
SET @period_end_datetime = @period_start_plus  --not necessary to have two params covering the same thing really...

SET @recurrence_period = datediff(ss, @recurrence_start_datetime, @recurrence_start_plus)
SET @period_duration = datediff(ss, @period_start_datetime, @period_start_plus)

exec seg01_log 'assign temporal_id<'||@definition_id||'|'||@recurrence_start_datetime||'>'



--don't think we need this as the uniqid in the table should act as the temporal_id ????
--exec SEG01_assign_temporal_library_id @definition_id, @recurrence_start_datetime, @temporal_id
     select @temporal_id = uniqid
       from SEG01_temporal_library_tbl
      where definition_id             = @definition_id
        and recurrence_start_datetime = @recurrence_start_datetime

     IF @temporal_id IS NULL
        BEGIN
           INSERT into SEG01_temporal_library_tbl(
                  -- temporal_id,
                   definition_id,
                   recurrence_id,
                   period_id,
                   recurrence_start_datetime,
                   recurrence_period,
                   period_lag,
                   period_start_datetime,
                   period_duration,
                   period_end_datetime,
                   recurrence_end_datetime)
           VALUES (--@library_id will be generated by identity column
                  -- @temporal_id,
                   @definition_id,
                   @recurrence_id,
                   @period_id,
                   @recurrence_start_datetime,
                   @recurrence_period,
                   @period_lag,
                   @period_start_datetime,
                   @period_duration,
                   @period_end_datetime,
                   @recurrence_end_datetime)
           commit
      END


exec seg01_log 'definition_id<'||@definition_id||'>'
exec seg01_log 'recurrence_id<'||@recurrence_id||'>'
exec seg01_log 'period_id<'||@period_id||'>'
exec seg01_log 'recurrence_start_datetime<'||@recurrence_start_datetime||'>'
exec seg01_log 'recurrence_period<'||@recurrence_period||'>'
exec seg01_log 'period_lag<'||@period_lag||'>'
exec seg01_log 'period_start_datetime<'||@period_start_datetime||'>'
exec seg01_log 'period_duration<'||@period_duration||'>'
exec seg01_log 'period_end_datetime<'||@period_end_datetime||'>'
exec seg01_log 'recurrence_end_datetime<'||@recurrence_end_datetime||'>'



 -- drop table SEG01_temporal_library_build_tmp
--  commit

SELECT @out_id = uniqid
  FROM SEG01_temporal_library_tbl
 WHERE definition_id             = @definition_id
   AND recurrence_id             = @recurrence_id
   AND period_id                 = @period_id
   AND recurrence_start_datetime = @recurrence_start_datetime
   AND recurrence_period         = @recurrence_period
   AND period_lag                = @period_lag
   AND period_start_datetime     = @period_start_datetime
   AND period_duration           = @period_duration
   AND period_end_datetime       = @period_end_datetime
   AND recurrence_end_datetime   = @recurrence_end_datetime

exec seg01_log 'out_id(or temporal_id)<'|| @out_id||'>'


END;

commit;

------------------------------proc ends here
----------------------------------------------




--test

truncate table SEG01_log_tbl;

commit

select *
from SEG01_log_tbl


select *
from  SEG01_temporal_library_build_tmp


select *
from  SEG01_temporal_lib_build_tmp2


select *
from SEG01_temporal_definition_tbl

-- the library
select *
from SEG01_temporal_library_tbl


select *
from SEG01_temporal_period_tbl



 DECLARE @_definition_id  bigint
  DECLARE @_period_id      bigint
  SET @_definition_id = 0
  SET @_period_id = 5
 DECLARE @insert_temporal_definition_tbl         varchar(42)
  SET @insert_temporal_definition_tbl    = 'SEG01_temporal_definition_tbl'
select 'INSERT into '||@insert_temporal_definition_tbl||'(uniqid, period_id)'||
                '             VALUES('||@_definition_id||', '||@_period_id||')'

INSERT into SEG01_temporal_definition_tbl(uniqid, period_id)             VALUES(0, 5)
commit;

SELECT count(uniqid)
    from SEG01_temporal_definition_tbl --should be parameterised
   where uniqid = 1
     and period_id = 4

commit
truncate table SEG01_temporal_definition_tbl
truncate table SEG01_temporal_library_tbl
commit


------

select *
from SEG01_temporal_period_tbl;


select *
from SEG01_temporal_definition_tbl
--45
--2013-08-22 00:00:00.000000


declare @temporal_id bigint
exec SEG01_create_temporal_library_new 17, '2013-11-02 00:00:00.000000', @temporal_id

select @temporal_id


select *
from SEG01_temporal_library_tbl


---little test
declare @recurrence_day_part_abr    varchar(2)
declare @recurrence_day_part_qty    int
declare @recurrence_start_datetime  datetime
declare @recurrence_start_plus      datetime

set @recurrence_day_part_abr = 'dd'
set @recurrence_day_part_qty = 3
set @recurrence_start_datetime = '2013-11-02 00:00:00.000000'

execute('SET @recurrence_start_plus = dateadd('||@recurrence_day_part_abr||', @recurrence_day_part_qty, @recurrence_start_datetime)')

select @recurrence_start_plus

--end little test


--********************************************
---another little test
 select *
 from SEG01_temporal_library_build_tmp


-- start -->
declare @_recurrence_start  datetime
set @_recurrence_start = '2013-11-02 00:00:00.000000'
/*
 dateformat(datepart(yy, @_recurrence_start)||'-'||
                                         datepart(mm, @_recurrence_start)||'-'||
                                         datepart(dd, @_recurrence_start)||' '||
                                         datepart(hh, period_start)||':'||
                                         datepart(mi, period_start)||':'||
                                         datepart(ss, period_start),'yyyy-mm-dd hh:ms:ss')
*/


select max(case when period_type = 0 then t.date_part_abbreviation else null end) recurrence_day_part_abr,
       max(case when period_type = 1 then t.date_part_abbreviation else null end) period_day_part_abr,
       max(case when period_type = 0 then t.quantity else null end) recurrence_day_part_qty,
       max(case when period_type = 1 then t.quantity else null end) period_day_part_qty,
       max(case when period_type = 0 then dateformat(datepart(yy, @_recurrence_start)||'-'||
                  datepart(mm, @_recurrence_start)||'-'||
                  datepart(dd, @_recurrence_start)||' '||
                  datepart(hh, period_start)||':'||
                  datepart(mi, period_start)||':'||
                  datepart(ss, period_start),'yyyy-mm-dd hh:ms:ss') else null end) recurrence_start_datetime,
       max(case when period_type = 1 then dateadd(ss, period_lag, @_recurrence_start) else null end) period_start_datetime
--       dateadd(t.date_part_abbreviation,t.quantity, recurrence_start_datetime) recurrence_start_plus,
--       t.*
  into #seg01_build_tmp2
  from SEG01_temporal_library_build_tmp t
-- where period_type = 0 --recurrence

declare @recurrence_day_part_abr  varchar(2)
declare @period_day_part_abr      varchar(2)

select @recurrence_day_part_abr = recurrence_day_part_abr,
       @period_day_part_abr = period_day_part_abr
 from #seg01_build_tmp2

execute('select dateadd('||@recurrence_day_part_abr||', recurrence_day_part_qty, recurrence_start_datetime) recurrence_start_plus, '||
        '       dateadd('||@period_day_part_abr||', period_day_part_qty, period_start_datetime) period_start_plus '||
        ' into #SEG01_temporal_lib_build_tmp3 '||
        ' from #seg01_build_tmp2')
commit

select *
from #SEG01_temporal_lib_build_tmp3

--- and another little test

