
/**********************************************************************************
 **
 **  ** Builds **    M E T R I C    L I B R A R Y
 **
 **   a library with a rule name, and what the engine has to do
 **
 **          metric            calculation            over()
 **     'Total Duration', 'sum(period_duration)', 'account_number'
 **
 **********************************************************************************/

CREATE or replace procedure SEG01_add_to_metric_library(
                   in @_metric_name       VARCHAR(24),
                   in @_calculation       VARCHAR(64),
                   in @_over_group        VARCHAR(24)
                ) AS
BEGIN



/* using:
 *   select *
 *     from SEG01_metric_library_tbl
 */


INSERT into SEG01_metric_library_tbl (metric_name, calculation, over_group )
   values(@_metric_name, @_calculation, @_over_group)
commit



END;
commit

