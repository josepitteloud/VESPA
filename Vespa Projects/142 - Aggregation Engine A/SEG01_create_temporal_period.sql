
/**********************************************************************************
 **
 **  ** Builds **    T E M P O R A L   P E R I O D
 **
 **  Tags can be re-used for the same rule, but for different periods
 **  a tag per rule is required. For example, all Mondays or weekends could have the
 **  same tag to denote all Mondays or weekend respectively. However 1wk should have
 **  a different tag to a 2nd wk if they were to be aggregated seperately
 **
 **********************************************************************************/

CREATE or replace procedure SEG01_create_temporal_period(
                in @_date_part_abbreviation    varchar(3),   -- what time variable we're specifying ss, mm, hh
                in @_period_quantity           bigint,       -- number of secs, mins, hours making up the period
                in @_period_start              time,     -- period_start based on time-of-day
               out @out_id                     bigint
                ) AS
BEGIN


exec seg01_log 'SEG01_create_temporal_period'

--temp section
/*       DECLARE @_date_part_abbreviation    varchar(3)   -- what time variable we're specifying ss, mm, hh
       DECLARE @_period_quantity           bigint       -- number of secs, mins, hours making up the period
       DECLARE @_period_start              time     -- period_start based on time-of-day

       SET @_date_part_abbreviation = 'HH'
       SET @_period_quantity        = 3
       SET @_period_start           = convert(datetime, '09:12:34', 108) --DATETIME( '11-24-2009 0:0:0.000' )     -- period_start based on time-of-day
*/
--end temp section


  DECLARE @insert_temporal_period_tbl         varchar(42)
  DECLARE @exists_already                     bigint
  DECLARE @now                                datetime
  DECLARE @to_date                            datetime

  SET @insert_temporal_period_tbl    = 'SEG01_temporal_period_tbl'
  SET @exists_already = 0
  SET @_period_start = convert(datetime, @_period_start, 108)

  SELECT @exists_already = count(uniqid)
    from SEG01_temporal_period_tbl
   where date_part_abbreviation = @_date_part_abbreviation
     and quantity = @_period_quantity
     and period_start = @_period_start

  --work out what the period in seconds is for this: date_part x quantity
  SET @now = getdate()
  --SET @to_date = @now

--select @to_date

  --SET @to_date = dateadd('||@_date_part_abbreviation||', '||@_period_quantity||', '''||@now||''')'




-- SOMETHING NOT QUITE RIGHT HAPPENING HERE, AS SHOULD Add a whole month from 2013-05-01 00:00:00 to 2013-06-01 00:00:00
--dateformat(@now,'yyyy-mm-dd')||' '||dateformat(@_period_start,'hh:ms:ss')

exec seg01_log 1

  execute('SET @to_date = dateadd('||@_date_part_abbreviation||', '||@_period_quantity||', '''||@now||''')')

exec seg01_log 2

--  execute('SET @to_date =

--  select @now, @to_date




--USING PERIOD IN THE TABLE HERE IS REALLY BAD AS IT COULD BE BASED ON A MONTH WITH A DIFFRENT NUMBER OF DAYS
  IF @exists_already = 0
    BEGIN
        execute('INSERT into '||@insert_temporal_period_tbl||'(date_part_abbreviation, quantity, period_start, period)'||
                '        VALUES('''||@_date_part_abbreviation||''', '||@_period_quantity||', '''||@_period_start||''','||datediff(ss,@now,@to_date)||')')
        commit
    END

exec seg01_log 3

DECLARE @xxsql varchar(500)
SET @xxsql =
        ' select @out_id = uniqid '||
        '   from '||@insert_temporal_period_tbl||' '||
        '  where date_part_abbreviation = '''||@_date_part_abbreviation||''''||
        '    and quantity = '||@_period_quantity||' '||
        '    and period_start = '''||@_period_start||''''||
        '    and period = '||datediff(ss,@now,@to_date)

exec seg01_log 4

--exec seg01_log @xxsql

execute(@xxsql)

-- RETURN THE uniqid ID for this record as an out param

END;



------------------------------proc ends here
----------------------------------------------


--test

truncate table SEG01_log_tbl;

commit
select *
from SEG01_log_tbl

commit
select *
from SEG01_temporal_period_tbl


