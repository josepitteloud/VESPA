


CREATE or replace procedure SEG01_get_date_abbreviation_qty_fit(
                 in   @start_datetime    datetime,
                 in   @end_datetime      datetime, --first start of the recurrence
                 out  @abbreviation      varchar(2),
                 out  @qty               int
                ) AS
BEGIN

--work out date_part_abbreviation, and QTY for
--  1. reccurrence
--  2. period
DECLARE @diff          double



SET @diff = datediff(ss, @start_datetime, @end_datetime)
IF datediff(ss, dateadd(ss, @diff, @start_datetime), @end_datetime) = 0
   BEGIN
      SET @abbreviation = 'ss'
      SET @qty = @diff
    END

SET @diff = datediff(mi, @start_datetime, @end_datetime)
IF datediff(ss, dateadd(mi, @diff, @start_datetime), @end_datetime) = 0
   BEGIN
      SET @abbreviation = 'mi'
      SET @qty = @diff
    END

SET @diff = datediff(hh, @start_datetime, @end_datetime)
IF datediff(ss, dateadd(hh, @diff, @start_datetime), @end_datetime) = 0
   BEGIN
      SET @abbreviation = 'hh'
      SET @qty = @diff
    END

SET @diff = datediff(dd, @start_datetime, @end_datetime)
IF datediff(ss, dateadd(dd, @diff, @start_datetime), @end_datetime) = 0
   BEGIN
      SET @abbreviation = 'dd'
      SET @qty = @diff
   END


--select @diff, @start_datetime, dateadd(dd, @diff, @start_datetime), datediff(ss, dateadd(dd, @diff, @start_datetime), @end_datetime)


SET @diff = datediff(mm, @start_datetime, @end_datetime)
IF datediff(ss, dateadd(mm, @diff, @start_datetime), @end_datetime) = 0
   BEGIN
      SET @abbreviation = 'mm'
      SET @qty = @diff
   END

SET @diff = datediff(yy, @start_datetime, @end_datetime)
IF datediff(ss, dateadd(yy, @diff, @start_datetime), @end_datetime) = 0
   BEGIN
      SET @abbreviation = 'yy'
      SET @qty = @diff
    END


END;

