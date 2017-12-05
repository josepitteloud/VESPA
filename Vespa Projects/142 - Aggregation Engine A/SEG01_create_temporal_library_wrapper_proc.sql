




CREATE or replace procedure SEG01_create_temporal_library_wrapper(
                 in @_recurrence_start    datetime,
                 in @_recurrence_end      datetime,
                 in @_period_start        datetime,
                 in @_period_end          datetime,
                out @temporal_id          bigint
                ) AS
BEGIN

--work out date_part_abbreviation, and QTY for
--  1. reccurrence
--  2. period
DECLARE @diff            double
DECLARE @abbreviation    varchar(2)
DECLARE @qty             int
DECLARE @period_id       bigint
DECLARE @recurrence_id   bigint
DECLARE @definition_id   bigint
--DECLARE @temporal_id     bigint


--    exec SEG01_create_temporal_period
exec SEG01_get_date_abbreviation_qty_fit  @_recurrence_start,  @_recurrence_end,  @abbreviation,  @qty
exec SEG01_create_temporal_period  @abbreviation,  @qty,  cast(@_period_start as TIME),  @recurrence_id
commit
--select @recurrence_id

exec SEG01_get_date_abbreviation_qty_fit  @_period_start,  @_period_end,  @abbreviation,  @qty
exec SEG01_create_temporal_period  @abbreviation,  @qty,  cast(@_period_start as TIME),  @period_id
commit
--select @period_id

--    exec SEG01_create_temporal_definition

declare @period_lag bigint

select @period_lag = datediff(ss, @_recurrence_start,  @_period_start)

exec SEG01_create_temporal_definition   @recurrence_id,   @period_id,  @period_lag,  @definition_id
--select  @definition_id

--    exec SEG01_create_temporal_library
exec SEG01_create_temporal_library_new @definition_id,  @_recurrence_start,  @temporal_id

exec seg01_log 'inserted into library: temporal_id<'||@temporal_id||'>'



END;


--test


--exec SEG01_create_temporal_library_wrapper '2013-11-02 00:00:00.000000', '2013-12-01 00:00:00.000000', '2013-11-17 01:00:00.000000', '2013-11-18 00:00:00.000000'
declare @temporal_id bigint
execute  SEG01_create_temporal_library_wrapper '2013-05-01 00:00:00.000000',  '2013-05-02 00:00:00.000000',  '2013-05-01 00:00:00.000000',  '2013-05-02 00:00:00.000000', @temporal_id
commit




declare @period_lag bigint

select @period_lag = datediff(ss, '2013-05-01 00:00:00.000000',  '2013-10-01 00:00:00.000000')
select @period_lag

--test

truncate table SEG01_log_tbl

commit

select *
from SEG01_log_tbl

-- the library
select *
from SEG01_temporal_library_tbl


select *
from SEG01_temporal_definition_tbl


select *
from SEG01_temporal_period_tbl


truncate table SEG01_temporal_period_tbl
truncate table SEG01_temporal_definition_tbl
truncate table SEG01_temporal_library_tbl


