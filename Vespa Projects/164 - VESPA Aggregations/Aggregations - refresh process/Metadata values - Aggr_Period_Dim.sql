/*###############################################################################
# Created on:   18/09/2013
# Created by:   Sebastian Bednaszynski (SBE)
# Description:  VESPA Aggregations - metadata information creation:
#               Aggr_Period_Dim
#
#               (updated for historical purposes only, not to be run unless all
#                data has to be recreated from scratch)
#
#################################################################################
# Dependencies
# ------------------------------------------------------------------------------
# => Tables/objects required:
#     - VESAP_Shared.Aggr_Period_Dim
#
#################################################################################
# Change log
# ------------------------------------------------------------------------------
# 18/09/2013  SBE   Initial version
# 13/01/2014  SBE   Definitions for 2014 added
#
###############################################################################*/



--truncate table VESPA_Shared.Aggr_Period_Dim;
insert into VESPA_Shared.Aggr_Period_Dim (Period_Key, Period_Start, Period_End, Period_Interval_Description, Period_Num_Days, Period_Num_Hours, Period_Num_Minutes, Period_Num_Seconds) values ( 1, '2013-01-01 00:00:00', '2013-01-31 23:59:59.999', 'calendar month', 31, 31*24, 31*24*60, 31*24*60*60);
insert into VESPA_Shared.Aggr_Period_Dim (Period_Key, Period_Start, Period_End, Period_Interval_Description, Period_Num_Days, Period_Num_Hours, Period_Num_Minutes, Period_Num_Seconds) values ( 2, '2013-02-01 00:00:00', '2013-02-28 23:59:59.999', 'calendar month', 28, 28*24, 28*24*60, 28*24*60*60);
insert into VESPA_Shared.Aggr_Period_Dim (Period_Key, Period_Start, Period_End, Period_Interval_Description, Period_Num_Days, Period_Num_Hours, Period_Num_Minutes, Period_Num_Seconds) values ( 3, '2013-03-01 00:00:00', '2013-03-31 23:59:59.999', 'calendar month', 31, 31*24, 31*24*60, 31*24*60*60);
insert into VESPA_Shared.Aggr_Period_Dim (Period_Key, Period_Start, Period_End, Period_Interval_Description, Period_Num_Days, Period_Num_Hours, Period_Num_Minutes, Period_Num_Seconds) values ( 4, '2013-04-01 00:00:00', '2013-04-30 23:59:59.999', 'calendar month', 30, 30*24, 30*24*60, 30*24*60*60);
insert into VESPA_Shared.Aggr_Period_Dim (Period_Key, Period_Start, Period_End, Period_Interval_Description, Period_Num_Days, Period_Num_Hours, Period_Num_Minutes, Period_Num_Seconds) values ( 5, '2013-05-01 00:00:00', '2013-05-31 23:59:59.999', 'calendar month', 31, 31*24, 31*24*60, 31*24*60*60);
insert into VESPA_Shared.Aggr_Period_Dim (Period_Key, Period_Start, Period_End, Period_Interval_Description, Period_Num_Days, Period_Num_Hours, Period_Num_Minutes, Period_Num_Seconds) values ( 6, '2013-06-01 00:00:00', '2013-06-30 23:59:59.999', 'calendar month', 30, 30*24, 30*24*60, 30*24*60*60);
insert into VESPA_Shared.Aggr_Period_Dim (Period_Key, Period_Start, Period_End, Period_Interval_Description, Period_Num_Days, Period_Num_Hours, Period_Num_Minutes, Period_Num_Seconds) values ( 7, '2013-07-01 00:00:00', '2013-07-31 23:59:59.999', 'calendar month', 31, 31*24, 31*24*60, 31*24*60*60);
insert into VESPA_Shared.Aggr_Period_Dim (Period_Key, Period_Start, Period_End, Period_Interval_Description, Period_Num_Days, Period_Num_Hours, Period_Num_Minutes, Period_Num_Seconds) values ( 8, '2013-08-01 00:00:00', '2013-08-31 23:59:59.999', 'calendar month', 31, 31*24, 31*24*60, 31*24*60*60);
insert into VESPA_Shared.Aggr_Period_Dim (Period_Key, Period_Start, Period_End, Period_Interval_Description, Period_Num_Days, Period_Num_Hours, Period_Num_Minutes, Period_Num_Seconds) values ( 9, '2013-09-01 00:00:00', '2013-09-30 23:59:59.999', 'calendar month', 30, 30*24, 30*24*60, 30*24*60*60);
insert into VESPA_Shared.Aggr_Period_Dim (Period_Key, Period_Start, Period_End, Period_Interval_Description, Period_Num_Days, Period_Num_Hours, Period_Num_Minutes, Period_Num_Seconds) values (10, '2013-10-01 00:00:00', '2013-10-31 23:59:59.999', 'calendar month', 31, 31*24, 31*24*60, 31*24*60*60);
insert into VESPA_Shared.Aggr_Period_Dim (Period_Key, Period_Start, Period_End, Period_Interval_Description, Period_Num_Days, Period_Num_Hours, Period_Num_Minutes, Period_Num_Seconds) values (11, '2013-11-01 00:00:00', '2013-11-30 23:59:59.999', 'calendar month', 30, 30*24, 30*24*60, 30*24*60*60);
insert into VESPA_Shared.Aggr_Period_Dim (Period_Key, Period_Start, Period_End, Period_Interval_Description, Period_Num_Days, Period_Num_Hours, Period_Num_Minutes, Period_Num_Seconds) values (12, '2013-12-01 00:00:00', '2013-12-31 23:59:59.999', 'calendar month', 31, 31*24, 31*24*60, 31*24*60*60);
commit;

insert into VESPA_Shared.Aggr_Period_Dim (Period_Key, Period_Start, Period_End, Period_Interval_Description, Period_Num_Days, Period_Num_Hours, Period_Num_Minutes, Period_Num_Seconds) values (13, '2014-01-01 00:00:00', '2014-01-31 23:59:59.999', 'calendar month', 31, 31*24, 31*24*60, 31*24*60*60);
insert into VESPA_Shared.Aggr_Period_Dim (Period_Key, Period_Start, Period_End, Period_Interval_Description, Period_Num_Days, Period_Num_Hours, Period_Num_Minutes, Period_Num_Seconds) values (14, '2014-02-01 00:00:00', '2014-02-28 23:59:59.999', 'calendar month', 28, 28*24, 28*24*60, 28*24*60*60);
insert into VESPA_Shared.Aggr_Period_Dim (Period_Key, Period_Start, Period_End, Period_Interval_Description, Period_Num_Days, Period_Num_Hours, Period_Num_Minutes, Period_Num_Seconds) values (15, '2014-03-01 00:00:00', '2014-03-31 23:59:59.999', 'calendar month', 31, 31*24, 31*24*60, 31*24*60*60);
insert into VESPA_Shared.Aggr_Period_Dim (Period_Key, Period_Start, Period_End, Period_Interval_Description, Period_Num_Days, Period_Num_Hours, Period_Num_Minutes, Period_Num_Seconds) values (16, '2014-04-01 00:00:00', '2014-04-30 23:59:59.999', 'calendar month', 30, 30*24, 30*24*60, 30*24*60*60);
insert into VESPA_Shared.Aggr_Period_Dim (Period_Key, Period_Start, Period_End, Period_Interval_Description, Period_Num_Days, Period_Num_Hours, Period_Num_Minutes, Period_Num_Seconds) values (17, '2014-05-01 00:00:00', '2014-05-31 23:59:59.999', 'calendar month', 31, 31*24, 31*24*60, 31*24*60*60);
insert into VESPA_Shared.Aggr_Period_Dim (Period_Key, Period_Start, Period_End, Period_Interval_Description, Period_Num_Days, Period_Num_Hours, Period_Num_Minutes, Period_Num_Seconds) values (18, '2014-06-01 00:00:00', '2014-06-30 23:59:59.999', 'calendar month', 30, 30*24, 30*24*60, 30*24*60*60);
insert into VESPA_Shared.Aggr_Period_Dim (Period_Key, Period_Start, Period_End, Period_Interval_Description, Period_Num_Days, Period_Num_Hours, Period_Num_Minutes, Period_Num_Seconds) values (19, '2014-07-01 00:00:00', '2014-07-31 23:59:59.999', 'calendar month', 31, 31*24, 31*24*60, 31*24*60*60);
insert into VESPA_Shared.Aggr_Period_Dim (Period_Key, Period_Start, Period_End, Period_Interval_Description, Period_Num_Days, Period_Num_Hours, Period_Num_Minutes, Period_Num_Seconds) values (20, '2014-08-01 00:00:00', '2014-08-31 23:59:59.999', 'calendar month', 31, 31*24, 31*24*60, 31*24*60*60);
insert into VESPA_Shared.Aggr_Period_Dim (Period_Key, Period_Start, Period_End, Period_Interval_Description, Period_Num_Days, Period_Num_Hours, Period_Num_Minutes, Period_Num_Seconds) values (21, '2014-09-01 00:00:00', '2014-09-30 23:59:59.999', 'calendar month', 30, 30*24, 30*24*60, 30*24*60*60);
insert into VESPA_Shared.Aggr_Period_Dim (Period_Key, Period_Start, Period_End, Period_Interval_Description, Period_Num_Days, Period_Num_Hours, Period_Num_Minutes, Period_Num_Seconds) values (22, '2014-10-01 00:00:00', '2014-10-31 23:59:59.999', 'calendar month', 31, 31*24, 31*24*60, 31*24*60*60);
insert into VESPA_Shared.Aggr_Period_Dim (Period_Key, Period_Start, Period_End, Period_Interval_Description, Period_Num_Days, Period_Num_Hours, Period_Num_Minutes, Period_Num_Seconds) values (23, '2014-11-01 00:00:00', '2014-11-30 23:59:59.999', 'calendar month', 30, 30*24, 30*24*60, 30*24*60*60);
insert into VESPA_Shared.Aggr_Period_Dim (Period_Key, Period_Start, Period_End, Period_Interval_Description, Period_Num_Days, Period_Num_Hours, Period_Num_Minutes, Period_Num_Seconds) values (24, '2014-12-01 00:00:00', '2014-12-31 23:59:59.999', 'calendar month', 31, 31*24, 31*24*60, 31*24*60*60);
commit;














