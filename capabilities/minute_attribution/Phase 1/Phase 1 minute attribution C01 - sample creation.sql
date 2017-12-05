/*###############################################################################
# Created on:   06/08/2012
# Created by:   Sebastian Bednaszynski (SBE)
# Description:  Creates a sample table for minute attribution process (based on
#               Phase 1 data)
#
#################################################################################
# Dependencies
# ------------------------------------------------------------------------------
# (none)
#
#################################################################################
# Change log
# ------------------------------------------------------------------------------
# 06/08/2012  SBE   v01 - initial version
# 20/08/2012  SBE   v02 - adjusted to meet requirements of C02 v02
#
###############################################################################*/


  -- ###############################################################################
  -- ##### Input table must include the following columns:                     #####
  -- #####  - cb_row_id                                                        #####
  -- #####  - subscriber_id                                                    #####
  -- #####  - viewing_starts                                                   #####
  -- #####  - viewing_stops                                                    #####
  -- #####  - viewing_duration                                                 #####
  -- #####  - recorded_time_UTC                                                #####
  -- #####  - Live_Flag                                                        #####
  -- #####  - BARB_minute_start                                                #####
  -- #####  - BARB_minute_end                                                  #####
  -- ###############################################################################


  -- ##############################################################################################################
  -- ##### SAMPLE 1: Daily viewing, sample accounts                                                           #####
  -- ##############################################################################################################
if object_id('VESPA_BARBMin_UN_Daily_Sample') is not null then drop table VESPA_BARBMin_UN_Daily_Sample endif;
select
      cb_row_id,
      subscriber_id,
      viewing_starts,
      viewing_stops,
      viewing_duration,
      case
        when timeshifting = 'LIVE' then 1
          else 0
      end as Live_Flag,
      cast(null as datetime) as BARB_minute_start,
      cast(null as datetime) as BARB_minute_end
  into VESPA_BARBMin_UN_Daily_Sample
  from vespa_analysts.vespa_daily_augs_20120203
 where Account_Number in ('200000847349','200000847620','200000850798','200000852109','200000853511','200000853990','200000854345',
                          '200000855904','200000872321','200000881751','200000882767','200000884623','200000885927','200000890943',
                          '200000895116','200000898987','200000908430','200000908455','200000913646','200000925350','200000935284',
                          '200000940086','200000940441','200000940813','200000941225','200000941266','200000945952','200000946224',
                          '200000946315','200000947222','200000949392','200000952255','200000956967','200000957445','200000963930',
                          '200000972634','200000983185','200000983474','200000998282','200000999181','200000999686','200001001177',
                          '200001010178','200001046321','200001046677','200001055223','200001059415','200001059423','200001070537',
                          '200001073499','200001074620','200001090881','200001103940','200001115167','200001117858','200001123534',
                          '200001129820','200001131875','200001136189','200001151899','200001153234','200001155262','200001156435',
                          '200001158274','200001168034','200001173331','200001178017','200001179890','200001183074','200001187331',
                          '200001191481','200001197884','200001198536','200001198676','200001200951','200001206339','200001210059',
                          '200001211347','200001211677','200001214895','200001216114','200001216999','200001224258','200001231055',
                          '200001234463','200001242409','200001243035','200001244223','200001245048','200001246228','200001251020',
                          '200001254115','200001257977','200001258124','200001260518','200001261847','200001264932','200001267224',
                          '200001269592','200001275284', '621057230610')
 order by Account_Number, Subscriber_Id, viewing_Starts, viewing_stops;           -- #################!!!!!!!!!!!!!!!! DEV & DEBUG FEATURE ONLY
commit;

create unique hg index idx1 on VESPA_BARBMin_UN_Daily_Sample(cb_row_id);
create hg index idx2 on VESPA_BARBMin_UN_Daily_Sample(Subscriber_Id);
create dttm index idx3 on VESPA_BARBMin_UN_Daily_Sample(viewing_Starts);
create dttm index idx4 on VESPA_BARBMin_UN_Daily_Sample(viewing_stops);
create lf index idx5 on VESPA_BARBMin_UN_Daily_Sample(Live_Flag);

-- select * from VESPA_BARBMin_UN_Daily_Sample;


  -- ##############################################################################################################
  -- ##### SAMPLE 2: Daily viewing, full augmented table                                                      #####
  -- ##############################################################################################################
if object_id('VESPA_BARBMin_UN_Daily') is not null then drop table VESPA_BARBMin_UN_Daily endif;
select
      cb_row_id,
      subscriber_id,
      viewing_starts,
      viewing_stops,
      viewing_duration,
      case
        when timeshifting = 'LIVE' then 1
          else 0
      end as Live_Flag,
      cast(null as datetime) as BARB_minute_start,
      cast(null as datetime) as BARB_minute_end
  into VESPA_BARBMin_UN_Daily
  from vespa_analysts.vespa_daily_augs_20120203;
commit;

create unique hg index idx1 on VESPA_BARBMin_UN_Daily(cb_row_id);
create hg index idx2 on VESPA_BARBMin_UN_Daily(Subscriber_Id);
create dttm index idx3 on VESPA_BARBMin_UN_Daily(viewing_Starts);
create dttm index idx4 on VESPA_BARBMin_UN_Daily(viewing_stops);
create lf index idx5 on VESPA_BARBMin_UN_Daily(Live_Flag);

-- select * from VESPA_BARBMin_UN_Daily;


  -- ##############################################################################################################
  -- ##### SAMPLE 3: Scenarios from the Excel simulation tool                                                 #####
  -- ##############################################################################################################
  -- NOTE: Require slight modification within the procedure to override table re-creation
delete from VESPA_BARBMin_01_Viewing_Delta;
insert into VESPA_BARBMin_01_Viewing_Delta values (1, 1, '2011-02-01 10:08:55', '2011-02-01 10:09:25', 30, null, null, 1, 1, null, null);
insert into VESPA_BARBMin_01_Viewing_Delta values (2, 1, '2011-02-01 10:09:25', '2011-02-01 10:09:37', 12, null, null, 8, 1, null, null);
insert into VESPA_BARBMin_01_Viewing_Delta values (3, 1, '2011-02-01 10:09:37', '2011-02-01 10:12:37', 180, null, null, 2, 1, null, null);
insert into VESPA_BARBMin_01_Viewing_Delta values (4, 2, '2011-02-01 10:08:30', '2011-02-01 10:08:50', 20, null, null, 1, 1, null, null);
insert into VESPA_BARBMin_01_Viewing_Delta values (5, 2, '2011-02-01 10:08:50', '2011-02-01 10:08:55', 5, null, null, 5, 1, null, null);
insert into VESPA_BARBMin_01_Viewing_Delta values (6, 2, '2011-02-01 10:08:55', '2011-02-01 10:09:09', 14, null, null, 6, 1, null, null);
insert into VESPA_BARBMin_01_Viewing_Delta values (7, 2, '2011-02-01 10:09:09', '2011-02-01 10:09:39', 30, null, null, 5, 1, null, null);
insert into VESPA_BARBMin_01_Viewing_Delta values (8, 2, '2011-02-01 10:09:39', '2011-02-01 10:11:39', 120, null, null, 4, 1, null, null);
insert into VESPA_BARBMin_01_Viewing_Delta values (9, 2, '2011-02-01 10:11:39', '2011-02-01 10:14:39', 180, null, null, 6, 1, null, null);
insert into VESPA_BARBMin_01_Viewing_Delta values (10, 3, '2011-02-01 10:08:30', '2011-02-01 10:08:45', 15, null, null, 1, 1, null, null);
insert into VESPA_BARBMin_01_Viewing_Delta values (11, 3, '2011-02-01 10:08:45', '2011-02-01 10:08:50', 5, null, null, 5, 1, null, null);
insert into VESPA_BARBMin_01_Viewing_Delta values (12, 3, '2011-02-01 10:08:50', '2011-02-01 10:09:04', 14, null, null, 6, 1, null, null);
insert into VESPA_BARBMin_01_Viewing_Delta values (13, 3, '2011-02-01 10:09:04', '2011-02-01 10:09:14', 10, null, null, 5, 1, null, null);
insert into VESPA_BARBMin_01_Viewing_Delta values (14, 3, '2011-02-01 10:09:14', '2011-02-01 10:09:28', 14, null, null, 4, 1, null, null);
insert into VESPA_BARBMin_01_Viewing_Delta values (15, 3, '2011-02-01 10:09:28', '2011-02-01 10:10:30', 62, null, null, 6, 1, null, null);
insert into VESPA_BARBMin_01_Viewing_Delta values (16, 3, '2011-02-01 10:10:30', '2011-02-01 10:14:27', 237, null, null, 3, 1, null, null);
insert into VESPA_BARBMin_01_Viewing_Delta values (17, 3, '2011-02-01 10:14:27', '2011-02-01 10:15:28', 61, null, null, 3, 1, null, null);
insert into VESPA_BARBMin_01_Viewing_Delta values (18, 3, '2011-02-01 10:15:28', '2011-02-01 10:16:29', 61, null, null, 3, 1, null, null);
insert into VESPA_BARBMin_01_Viewing_Delta values (19, 3, '2011-02-01 10:16:29', '2011-02-01 10:17:30', 61, null, null, 3, 1, null, null);
insert into VESPA_BARBMin_01_Viewing_Delta values (20, 3, '2011-02-01 10:17:30', '2011-02-01 10:18:31', 61, null, null, 3, 1, null, null);
insert into VESPA_BARBMin_01_Viewing_Delta values (21, 3, '2011-02-01 10:18:31', '2011-02-01 10:19:32', 61, null, null, 3, 1, null, null);
insert into VESPA_BARBMin_01_Viewing_Delta values (22, 3, '2011-02-01 10:19:32', '2011-02-01 10:20:33', 61, null, null, 3, 1, null, null);
insert into VESPA_BARBMin_01_Viewing_Delta values (23, 4, '2011-02-01 10:08:30', '2011-02-01 10:08:40', 10, null, null, 1, 1, null, null);
insert into VESPA_BARBMin_01_Viewing_Delta values (24, 4, '2011-02-01 10:08:40', '2011-02-01 10:09:05', 25, null, null, 5, 1, null, null);
insert into VESPA_BARBMin_01_Viewing_Delta values (25, 4, '2011-02-01 10:09:05', '2011-02-01 10:09:20', 15, null, null, 6, 1, null, null);
insert into VESPA_BARBMin_01_Viewing_Delta values (26, 4, '2011-02-01 10:09:20', '2011-02-01 10:09:52', 32, null, null, 8, 1, null, null);
insert into VESPA_BARBMin_01_Viewing_Delta values (27, 4, '2011-02-01 10:09:52', '2011-02-01 10:10:52', 60, null, null, 4, 1, null, null);
insert into VESPA_BARBMin_01_Viewing_Delta values (28, 4, '2011-02-01 10:10:52', '2011-02-01 10:11:54', 62, null, null, 2, 1, null, null);
insert into VESPA_BARBMin_01_Viewing_Delta values (29, 4, '2011-02-01 10:14:29', '2011-02-01 10:18:30', 241, null, null, 3, 1, null, null);
insert into VESPA_BARBMin_01_Viewing_Delta values (30, 4, '2011-02-01 11:14:30', '2011-02-01 11:18:31', 241, null, null, 3, 1, null, null);
insert into VESPA_BARBMin_01_Viewing_Delta values (31, 4, '2011-02-01 12:14:31', '2011-02-01 12:18:32', 241, null, null, 3, 1, null, null);
insert into VESPA_BARBMin_01_Viewing_Delta values (32, 4, '2011-02-01 13:14:32', '2011-02-01 13:18:33', 241, null, null, 3, 1, null, null);
insert into VESPA_BARBMin_01_Viewing_Delta values (33, 5, '2011-02-01 10:08:30', '2011-02-01 10:08:40', 10, null, null, 1, 1, null, null);
insert into VESPA_BARBMin_01_Viewing_Delta values (34, 5, '2011-02-01 10:08:40', '2011-02-01 10:09:25', 45, null, null, 5, 1, null, null);
insert into VESPA_BARBMin_01_Viewing_Delta values (35, 5, '2011-02-01 10:09:25', '2011-02-01 10:09:35', 10, null, null, 6, 1, null, null);
insert into VESPA_BARBMin_01_Viewing_Delta values (36, 5, '2011-02-01 10:09:35', '2011-02-01 10:09:48', 13, null, null, 8, 1, null, null);
insert into VESPA_BARBMin_01_Viewing_Delta values (37, 5, '2011-02-01 10:09:48', '2011-02-01 10:10:02', 14, null, null, 4, 1, null, null);
insert into VESPA_BARBMin_01_Viewing_Delta values (38, 5, '2011-02-01 10:10:02', '2011-02-01 10:12:02', 120, null, null, 2, 1, null, null);
insert into VESPA_BARBMin_01_Viewing_Delta values (39, 5, '2011-02-01 11:14:27', '2011-02-01 11:15:28', 61, null, null, 3, 1, null, null);
insert into VESPA_BARBMin_01_Viewing_Delta values (40, 5, '2011-02-01 12:14:28', '2011-02-01 12:15:29', 61, null, null, 3, 1, null, null);
insert into VESPA_BARBMin_01_Viewing_Delta values (41, 5, '2011-02-01 13:14:29', '2011-02-01 13:15:30', 61, null, null, 3, 1, null, null);
insert into VESPA_BARBMin_01_Viewing_Delta values (42, 5, '2011-02-01 14:15:30', '2011-02-01 14:16:31', 61, null, null, 3, 1, null, null);
insert into VESPA_BARBMin_01_Viewing_Delta values (43, 5, '2011-02-01 15:17:31', '2011-02-01 15:18:32', 61, null, null, 3, 1, null, null);
insert into VESPA_BARBMin_01_Viewing_Delta values (44, 5, '2011-02-01 16:18:32', '2011-02-01 16:19:33', 61, null, null, 3, 1, null, null);
insert into VESPA_BARBMin_01_Viewing_Delta values (45, 6, '2011-02-01 10:08:30', '2011-02-01 10:08:40', 10, null, null, 1, 1, null, null);
insert into VESPA_BARBMin_01_Viewing_Delta values (46, 6, '2011-02-01 10:08:40', '2011-02-01 10:09:10', 30, null, null, 5, 1, null, null);
insert into VESPA_BARBMin_01_Viewing_Delta values (47, 6, '2011-02-01 10:09:10', '2011-02-01 10:09:30', 20, null, null, 6, 1, null, null);
insert into VESPA_BARBMin_01_Viewing_Delta values (48, 6, '2011-02-01 10:09:30', '2011-02-01 10:09:40', 10, null, null, 8, 1, null, null);
insert into VESPA_BARBMin_01_Viewing_Delta values (49, 6, '2011-02-01 10:09:40', '2011-02-01 10:10:10', 30, null, null, 7, 1, null, null);
insert into VESPA_BARBMin_01_Viewing_Delta values (50, 6, '2011-02-01 10:10:10', '2011-02-01 10:12:10', 120, null, null, 2, 1, null, null);
insert into VESPA_BARBMin_01_Viewing_Delta values (51, 7, '2011-02-01 10:08:30', '2011-02-01 10:08:40', 10, null, null, 1, 1, null, null);
insert into VESPA_BARBMin_01_Viewing_Delta values (52, 7, '2011-02-01 10:08:40', '2011-02-01 10:09:10', 30, null, null, 5, 1, null, null);
insert into VESPA_BARBMin_01_Viewing_Delta values (53, 8, '2011-02-01 10:08:30', '2011-02-01 10:08:40', 10, null, null, 1, 1, null, null);
insert into VESPA_BARBMin_01_Viewing_Delta values (54, 8, '2011-02-01 10:08:40', '2011-02-01 10:09:10', 30, null, null, 5, 1, null, null);
insert into VESPA_BARBMin_01_Viewing_Delta values (55, 8, '2011-02-01 10:09:10', '2011-02-01 10:10:02', 52, null, null, 6, 1, null, null);
insert into VESPA_BARBMin_01_Viewing_Delta values (56, 8, '2011-02-01 10:10:02', '2011-02-01 10:10:47', 45, null, null, 8, 1, null, null);
insert into VESPA_BARBMin_01_Viewing_Delta values (57, 9, '2011-02-01 10:08:30', '2011-02-01 10:09:00', 30, null, null, 1, 1, null, null);
insert into VESPA_BARBMin_01_Viewing_Delta values (58, 9, '2011-02-01 10:09:00', '2011-02-01 10:13:00', 240, null, null, 5, 1, null, null);
insert into VESPA_BARBMin_01_Viewing_Delta values (59, 9, '2011-02-01 10:13:00', '2011-02-01 10:13:15', 15, null, null, 4, 1, null, null);
insert into VESPA_BARBMin_01_Viewing_Delta values (60, 10, '2011-02-01 10:09:25', '2011-02-01 10:09:40', 15, null, null, 1, 1, null, null);
insert into VESPA_BARBMin_01_Viewing_Delta values (61, 10, '2011-02-01 10:09:40', '2011-02-01 10:09:55', 15, null, null, 5, 1, null, null);
insert into VESPA_BARBMin_01_Viewing_Delta values (62, 100, '2011-02-01 10:08:30', '2011-02-01 10:09:19', 49, null, null, 1, 1, null, null);
insert into VESPA_BARBMin_01_Viewing_Delta values (63, 100, '2011-02-01 10:09:19', '2011-02-01 10:09:41', 22, null, null, 5, 1, null, null);
insert into VESPA_BARBMin_01_Viewing_Delta values (64, 100, '2011-02-01 10:09:41', '2011-02-01 10:10:36', 55, null, null, 1, 1, null, null);
insert into VESPA_BARBMin_01_Viewing_Delta values (65, 101, '2011-02-01 10:08:30', '2011-02-01 10:09:18', 48, null, null, 1, 1, null, null);
insert into VESPA_BARBMin_01_Viewing_Delta values (66, 101, '2011-02-01 10:09:18', '2011-02-01 10:09:41', 23, null, null, 5, 1, null, null);
insert into VESPA_BARBMin_01_Viewing_Delta values (67, 101, '2011-02-01 10:09:41', '2011-02-01 10:10:36', 55, null, null, 1, 1, null, null);
insert into VESPA_BARBMin_01_Viewing_Delta values (68, 102, '2011-02-01 10:08:30', '2011-02-01 10:09:13', 43, null, null, 1, 1, null, null);
insert into VESPA_BARBMin_01_Viewing_Delta values (69, 102, '2011-02-01 10:09:13', '2011-02-01 10:09:26', 13, null, null, 5, 1, null, null);
insert into VESPA_BARBMin_01_Viewing_Delta values (70, 102, '2011-02-01 10:09:26', '2011-02-01 10:09:42', 16, null, null, 1, 1, null, null);
insert into VESPA_BARBMin_01_Viewing_Delta values (71, 102, '2011-02-01 10:09:42', '2011-02-01 10:09:58', 16, null, null, 5, 1, null, null);
insert into VESPA_BARBMin_01_Viewing_Delta values (72, 102, '2011-02-01 10:09:58', '2011-02-01 10:10:41', 43, null, null, 1, 1, null, null);
insert into VESPA_BARBMin_01_Viewing_Delta values (73, 103, '2011-02-01 10:08:30', '2011-02-01 10:09:13', 43, null, null, 1, 1, null, null);
insert into VESPA_BARBMin_01_Viewing_Delta values (74, 103, '2011-02-01 10:09:13', '2011-02-01 10:09:26', 13, null, null, 5, 1, null, null);
insert into VESPA_BARBMin_01_Viewing_Delta values (75, 103, '2011-02-01 10:09:26', '2011-02-01 10:09:42', 16, null, null, 1, 1, null, null);
insert into VESPA_BARBMin_01_Viewing_Delta values (76, 103, '2011-02-01 10:09:42', '2011-02-01 10:09:59', 17, null, null, 5, 1, null, null);
insert into VESPA_BARBMin_01_Viewing_Delta values (77, 103, '2011-02-01 10:09:59', '2011-02-01 10:10:41', 42, null, null, 1, 1, null, null);
insert into VESPA_BARBMin_01_Viewing_Delta values (78, 104, '2011-02-01 10:08:30', '2011-02-01 10:09:11', 41, null, null, 1, 1, null, null);
insert into VESPA_BARBMin_01_Viewing_Delta values (79, 104, '2011-02-01 10:09:11', '2011-02-01 10:09:21', 10, null, null, 5, 1, null, null);
insert into VESPA_BARBMin_01_Viewing_Delta values (80, 104, '2011-02-01 10:09:21', '2011-02-01 10:09:26', 5, null, null, 1, 1, null, null);
insert into VESPA_BARBMin_01_Viewing_Delta values (81, 104, '2011-02-01 10:09:26', '2011-02-01 10:09:42', 16, null, null, 4, 1, null, null);
insert into VESPA_BARBMin_01_Viewing_Delta values (82, 104, '2011-02-01 10:09:42', '2011-02-01 10:09:49', 7, null, null, 1, 1, null, null);
insert into VESPA_BARBMin_01_Viewing_Delta values (83, 104, '2011-02-01 10:09:49', '2011-02-01 10:09:58', 9, null, null, 4, 1, null, null);
insert into VESPA_BARBMin_01_Viewing_Delta values (84, 104, '2011-02-01 10:09:58', '2011-02-01 10:10:41', 43, null, null, 1, 1, null, null);
insert into VESPA_BARBMin_01_Viewing_Delta values (85, 105, '2011-02-01 10:08:54', '2011-02-01 10:09:06', 12, null, null, 6, 1, null, null);
insert into VESPA_BARBMin_01_Viewing_Delta values (86, 105, '2011-02-01 10:09:06', '2011-02-01 10:09:17', 11, null, null, 5, 1, null, null);
insert into VESPA_BARBMin_01_Viewing_Delta values (87, 105, '2011-02-01 10:09:17', '2011-02-01 10:09:34', 17, null, null, 6, 1, null, null);
insert into VESPA_BARBMin_01_Viewing_Delta values (88, 105, '2011-02-01 10:09:34', '2011-02-01 10:09:38', 4, null, null, 1, 1, null, null);
insert into VESPA_BARBMin_01_Viewing_Delta values (89, 105, '2011-02-01 10:09:38', '2011-02-01 10:09:46', 8, null, null, 6, 1, null, null);
insert into VESPA_BARBMin_01_Viewing_Delta values (90, 105, '2011-02-01 10:09:46', '2011-02-01 10:09:56', 10, null, null, 5, 1, null, null);
insert into VESPA_BARBMin_01_Viewing_Delta values (91, 105, '2011-02-01 10:09:56', '2011-02-01 10:10:13', 17, null, null, 8, 1, null, null);

insert into VESPA_BARBMin_01_Viewing_Delta values (92, 11, '2011-02-01 10:08:55', '2011-02-01 10:09:24', 29, '2012-01-01 01:53:13', null, 1, 0, null, null);
insert into VESPA_BARBMin_01_Viewing_Delta values (93, 11, '2011-02-01 10:08:55', '2011-02-01 10:09:25', 30, '2012-01-02 01:53:13', null, 1, 0, null, null);
insert into VESPA_BARBMin_01_Viewing_Delta values (94, 11, '2011-02-01 10:08:55', '2011-02-01 10:09:26', 31, '2012-01-03 01:53:13', null, 1, 0, null, null);
insert into VESPA_BARBMin_01_Viewing_Delta values (95, 11, '2011-02-01 10:08:55', '2011-02-01 10:09:27', 32, '2012-01-04 01:53:13', null, 1, 0, null, null);
insert into VESPA_BARBMin_01_Viewing_Delta values (96, 12, '2011-02-01 10:08:55', '2011-02-01 10:09:55', 60, '2012-01-05 01:53:28', null, 1, 0, null, null);
insert into VESPA_BARBMin_01_Viewing_Delta values (97, 12, '2011-02-01 10:08:55', '2011-02-01 10:09:54', 59, '2012-01-06 01:53:29', null, 1, 0, null, null);
insert into VESPA_BARBMin_01_Viewing_Delta values (98, 12, '2011-02-01 10:08:55', '2011-02-01 10:09:53', 58, '2012-01-07 01:53:30', null, 1, 0, null, null);
insert into VESPA_BARBMin_01_Viewing_Delta values (99, 12, '2011-02-01 10:08:55', '2011-02-01 10:09:52', 57, '2012-01-08 01:53:31', null, 1, 0, null, null);
insert into VESPA_BARBMin_01_Viewing_Delta values (100, 12, '2011-02-01 10:08:55', '2011-02-01 10:09:51', 56, '2012-01-09 01:53:32', null, 1, 0, null, null);
insert into VESPA_BARBMin_01_Viewing_Delta values (101, 13, '2011-02-01 10:08:55', '2011-02-01 10:09:52', 57, '2012-01-10 01:53:32', null, 1, 0, null, null);
insert into VESPA_BARBMin_01_Viewing_Delta values (102, 13, '2011-02-01 10:08:55', '2011-02-01 10:09:53', 58, '2012-01-11 01:53:32', null, 1, 0, null, null);
insert into VESPA_BARBMin_01_Viewing_Delta values (103, 13, '2011-02-01 10:08:55', '2011-02-01 10:09:54', 59, '2012-01-12 01:53:32', null, 1, 0, null, null);
insert into VESPA_BARBMin_01_Viewing_Delta values (104, 13, '2011-02-01 10:08:55', '2011-02-01 10:09:55', 60, '2012-01-13 01:53:32', null, 1, 0, null, null);
insert into VESPA_BARBMin_01_Viewing_Delta values (105, 13, '2011-02-01 10:08:55', '2011-02-01 10:09:56', 61, '2012-01-14 01:53:32', null, 1, 0, null, null);
insert into VESPA_BARBMin_01_Viewing_Delta values (106, 14, '2011-02-01 10:08:55', '2011-02-01 10:09:55', 60, '2012-01-15 01:53:28', null, 1, 0, null, null);
insert into VESPA_BARBMin_01_Viewing_Delta values (107, 14, '2011-02-01 10:08:55', '2011-02-01 10:09:55', 60, '2012-01-16 01:53:29', null, 1, 0, null, null);
insert into VESPA_BARBMin_01_Viewing_Delta values (108, 14, '2011-02-01 10:08:55', '2011-02-01 10:09:55', 60, '2012-01-17 01:53:30', null, 1, 0, null, null);
insert into VESPA_BARBMin_01_Viewing_Delta values (109, 14, '2011-02-01 10:08:55', '2011-02-01 10:09:55', 60, '2012-01-18 01:53:31', null, 1, 0, null, null);
insert into VESPA_BARBMin_01_Viewing_Delta values (110, 14, '2011-02-01 10:08:55', '2011-02-01 10:09:55', 60, '2012-01-19 01:53:32', null, 1, 0, null, null);
insert into VESPA_BARBMin_01_Viewing_Delta values (111, 15, '2011-02-01 10:08:55', '2011-02-01 10:13:05', 250, '2012-01-20 01:53:25', null, 1, 0, null, null);
insert into VESPA_BARBMin_01_Viewing_Delta values (112, 15, '2011-02-01 10:08:55', '2011-02-01 10:13:03', 248, '2012-01-21 01:53:26', null, 1, 0, null, null);
insert into VESPA_BARBMin_01_Viewing_Delta values (113, 15, '2011-02-01 10:08:55', '2011-02-01 10:13:01', 246, '2012-01-22 01:53:27', null, 1, 0, null, null);
insert into VESPA_BARBMin_01_Viewing_Delta values (114, 15, '2011-02-01 10:08:55', '2011-02-01 10:12:59', 244, '2012-01-23 01:53:28', null, 1, 0, null, null);
insert into VESPA_BARBMin_01_Viewing_Delta values (115, 15, '2011-02-01 10:08:55', '2011-02-01 10:12:57', 242, '2012-01-24 01:53:29', null, 1, 0, null, null);
insert into VESPA_BARBMin_01_Viewing_Delta values (116, 15, '2011-02-01 10:08:55', '2011-02-01 10:12:55', 240, '2012-01-25 01:53:30', null, 1, 0, null, null);
insert into VESPA_BARBMin_01_Viewing_Delta values (117, 15, '2011-02-01 10:08:55', '2011-02-01 10:12:53', 238, '2012-01-26 01:53:31', null, 1, 0, null, null);
insert into VESPA_BARBMin_01_Viewing_Delta values (118, 15, '2011-02-01 10:08:55', '2011-02-01 10:12:51', 236, '2012-01-27 01:53:32', null, 1, 0, null, null);
insert into VESPA_BARBMin_01_Viewing_Delta values (119, 15, '2011-02-01 10:08:55', '2011-02-01 10:12:49', 234, '2012-01-27 01:53:33', null, 1, 0, null, null);
insert into VESPA_BARBMin_01_Viewing_Delta values (120, 15, '2011-02-01 10:08:55', '2011-02-01 10:12:47', 232, '2012-01-27 01:53:34', null, 1, 0, null, null);
commit;



  -- ##############################################################################################################
  -- ##############################################################################################################
  -- ##############################################################################################################

















