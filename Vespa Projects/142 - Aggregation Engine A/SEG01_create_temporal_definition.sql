
/**********************************************************************************
 **
 **  ** Builds **    T E M P O R A L   D E F I N I T I O N
 **
 **  Tags can be re-used for the same rule, but for different periods
 **  a tag per rule is required. For example, all Mondays or weekends could have the
 **  same tag to denote all Mondays or weekend respectively. However 1wk should have
 **  a different tag to a 2nd wk if they were to be aggregated seperately
 **
 **  This proceedure should take as input:
 **             recurrence              period_id
 **             period                  period_id
 **             lag_to_period_start     period_lag
 **
 **********************************************************************************/

CREATE or replace procedure SEG01_create_temporal_definition(
                 --in @_definition_id       bigint, --add in start date for this definition
                 in @_recurrence_id       bigint, -- Recurrence period (period_id)
                 in @_period_id           bigint, -- Period (period_id)
                 in @_period_lag          bigint, -- lag between the recurrence start
                                                  -- and this period start in seconds.
                                                  -- Ususally = 0, unless the period forms
                                                  -- a small section within another larger
                                                  -- period. For example, a Tuesday morning
                                                  -- once a week where the lag is the time
                                                  -- from the start of the week, until the
                                                  -- start of the morning period.
                out @out_id               bigint
                ) AS
BEGIN

--temp section
/*       DECLARE @_date_part_abbreviation    varchar(3)   -- what time variable we're specifying ss, mm, hh
       DECLARE @_period_quantity           bigint       -- number of secs, mins, hours making up the period
       DECLARE @_period_start              time     -- period_start based on time-of-day

       SET @_date_part_abbreviation = 'HH'
       SET @_period_quantity        = 3
       SET @_period_start           = convert(datetime, '09:12:34', 108) --DATETIME( '11-24-2009 0:0:0.000' )     -- period_start based on time-of-day
*/
--end temp section


  DECLARE @insert_temporal_definition_tbl         varchar(42)
  DECLARE @definition_id                          bigint


   --test for existing definition_id
   -- is there a uniqid with both the period and recurrence IDs, and with the period_lag
   --select a.uniqid definition_id, a.period_id recurrence_id, a.period_lag recurrence_lag, b.period_id, b.period_lag
   select @definition_id = a.uniqid
     from
        (select * --t.uniqid, t.period_id, t.period_lag
           from SEG01_temporal_definition_tbl t
          where period_id = @_recurrence_id) a,
        (select * --t.uniqid, t.period_id, t.period_lag
           from SEG01_temporal_definition_tbl t
          where period_id  = @_period_id
            and period_lag = @_period_lag) b
    where a.uniqid = b.uniqid




  SET @insert_temporal_definition_tbl    = 'SEG01_temporal_definition_tbl'
--  SET @exists_already = 0

  --if definition_id = 0 then this is a new definition
  IF @definition_id IS NULL
    BEGIN
       select @definition_id = coalesce(max(uniqid)+1,1)--if table has no rows will return null, therefore start at uniqid=1
         from SEG01_temporal_definition_tbl


 /* SELECT @exists_already = count(uniqid)
    from SEG01_temporal_definition_tbl --should be parameterised
   where uniqid = @_definition_id
     and period_id = @_period_id
     and period_lag = @_period_lag
*/
  --if exists, then we should check that the specified period_lag is possible [but we'll skip this for now]


--  IF @exists_already = 0 --doesn't exist
  --  BEGIN
       INSERT into SEG01_temporal_definition_tbl(uniqid, period_id, period_lag, period_type) VALUES(@definition_id, @_recurrence_id, 0,            0/*occurrence*/)
       INSERT into SEG01_temporal_definition_tbl(uniqid, period_id, period_lag, period_type) VALUES(@definition_id, @_period_id,     @_period_lag, 1/*period*/)
        --execute('INSERT into '||@insert_temporal_definition_tbl||'(uniqid, period_id)'||
        --        '             VALUES('||@_definition_id||', '||@_period_id||')')
        commit

    END
        SET @out_id = @definition_id



END;

commit;

------------------------------proc ends here
----------------------------------------------


--test

truncate table SEG01_log_tbl;

select *
from SEG01_log_tbl;

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


truncate table SEG01_temporal_definition_tbl
commit



--test for new insert code
select *
from SEG01_temporal_definition_tbl


--start -->
declare @_recurrence_id       bigint
declare @_period_id           bigint
declare @_period_lag          bigint
declare @existing_uniqid      bigint
--declare @sample_count         int


set @_recurrence_id = 63
set @_period_id = 64--65
set @_period_lag = 0

-- is there a uniqid with both the period and recurrence IDs, and with the period_lag
select a.uniqid definition_id, a.period_id recurrence_id, a.period_lag recurrence_lag, b.period_id, b.period_lag
from
(select * --t.uniqid, t.period_id, t.period_lag
from SEG01_temporal_definition_tbl t
where period_id = @_recurrence_id) a,
(select * --t.uniqid, t.period_id, t.period_lag
from SEG01_temporal_definition_tbl t
where period_id  = @_period_id
  and period_lag = @_period_lag) b
where a.uniqid = b.uniqid




--select @existing_uniqid = e.uniqid
--from (
select d.uniqid, count(1) sample_count, dense_rank() over(order by d.uniqid) as rank
from (
select t.uniqid, t.period_id, t.period_lag
from SEG01_temporal_definition_tbl t
where period_id = @_recurrence_id
union all
select t.uniqid, t.period_id, t.period_lag
  from SEG01_temporal_definition_tbl t
 where period_id = @_period_id
   and period_lag = @_period_lag) d

group by uniqid
having  sample_count > 0
--) e
--where rank = 1


select  @existing_uniqid


