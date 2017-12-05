2014-08-13 12:22:11.375 | Begining  M01.0 - Initialising Environment
2014-08-13 12:22:11.401 | Begining M00.0 - Initialising Environment
2014-08-13 12:22:11.401 | @ M00.0: Initialising Environment DONE
2014-08-13 12:22:11.401 | Begining M00.1 - Initialising Tables
2014-08-13 12:22:13.344 | Begining M00.2 - Initialising Views DONE
2014-08-13 12:22:17.632 | Begining M00.1 - Initialising Tables DONE
2014-08-13 12:22:17.634 | Begining M00.2 - Initialising Views
2014-08-13 12:22:17.812 | M00 Finished
2014-08-13 12:22:17.836 | Begining  M02.0 - Initialising Environment
2014-08-13 12:22:17.836 | @ M02.0: Initialising Environment DONE
2014-08-13 12:22:17.836 | Begining  M02.1 - Checking for Fresh Start flag
2014-08-13 12:22:17.836 | @ M02.1: Fresh Start requested: Resting process table
2014-08-13 12:22:17.926 | @ M02.1: Checking for Fresh Start flag DONE
2014-08-13 12:22:17.926 | @ M02.2: Cleaning Base tables
2014-08-13 12:22:19.775 | @ M02.2: Cleaning Base tables DONE
2014-08-13 12:22:19.775 | @ M02.2: Maintaining Base tables DONE
2014-08-13 12:22:19.775 | Begining  M02.3 - Initialising the logger
2014-08-13 12:22:21.196 | @ M02.3: Initialising the logger DONE
2014-08-13 12:22:21.196 | Begining  M02.4 - Returning Results
2014-08-13 12:22:21.196 | @ M02.4: Returning Results DONE
2014-08-13 12:22:21.196 | M02 Finished
2014-08-13 12:22:21.368 | @ M01.0: Initialising Environment DONE
2014-08-13 12:22:21.368 | Begining  M01.1 - Identifying Pending TasksHousekeeping
2014-08-13 12:22:21.412 | @ M01.1: Pending Tasks Found
2014-08-13 12:22:21.421 | @ M01.1: Task v289_m08_Experian_data_preparation Pending
2014-08-13 12:22:21.421 | @ M01.1: Identifying Pending TasksHousekeeping DONE
2014-08-13 12:22:21.421 | Begining  M01.2 - Tasks Execution
2014-08-13 12:22:21.421 | @ M01.2: Executing ->v289_m08_Experian_data_preparation
2014-08-13 12:22:21.436 | Begining M08.0 - Initialising Environment
2014-08-13 12:22:21.439 | @ M08.0: Initialising Environment DONE
2014-08-13 12:22:21.439 | Begining M08.1 - Account extraction from SAV
2014-08-13 12:24:23.556 | @ M08.1 TABLE V289_M08_SKY_HH_view Populated
2014-08-13 12:24:23.556 | Begining M08.2 - Experian HH Info Extraction
2014-08-13 12:31:52.583 | @ M08.2 1st round finished 
2014-08-13 12:31:52.583 | Begining M08.3 - Experian HH Info Extraction (2nd round)
2014-08-13 12:33:07.206 | @ M08.3 2nd round finished 
2014-08-13 12:33:07.206 | Begining M08.4 - Experian HH Info Extraction (3nd round)
2014-08-13 12:33:15.159 | @ M08.4 3rd round finished 
2014-08-13 12:33:15.159 | Begining M08.5 - Individual TABLE POPULATION
2014-08-13 12:39:27.110 | @ M08.5 Individual table populated
2014-08-13 12:39:27.110 | Begining M08.6 - Add Head of Household
2014-08-13 12:44:11.692 | @ M08.6 Head of household added
2014-08-13 12:44:11.692 | Begining M08.7 - Add Individual Children
2014-08-13 12:44:54.334 | @ M08.7 kids data added
2014-08-13 12:44:54.334 | Begining M08.8 - Final Tidying of Data
2014-08-13 12:54:29.221 | @ M08.8: Final Tidying of Data DONE
2014-08-13 12:54:29.394 | M08.8 Process completed
2014-08-13 12:54:29.537 | @ M01.2: v289_m08_Experian_data_preparation DONE
2014-08-13 12:54:30.395 | @ M01.2: Tasks Execution DONE
2014-08-13 12:54:30.421 | @ M01.1: Pending Tasks Found
2014-08-13 12:54:30.454 | @ M01.1: Task v289_m04_barb_data_preparation Pending
2014-08-13 12:54:30.455 | @ M01.1: Identifying Pending TasksHousekeeping DONE
2014-08-13 12:54:30.455 | Begining  M01.2 - Tasks Execution
2014-08-13 12:54:30.455 | @ M01.2: Executing ->v289_m04_barb_data_preparation
2014-08-13 12:54:30.464 | Begining M04.0 - Initialising Environment
2014-08-13 12:54:30.550 | @ M04.0: Initialising Environment DONE
2014-08-13 12:54:30.550 | Begining M04.1 - Preparing transient tables
2014-08-13 12:54:31.898 | @ M04.1: Preparing transient tables DONE
2014-08-13 12:54:31.898 | Begining M04.2 - Final BARB Data Preparation
2014-08-13 13:03:01.932 | @ M04.1: Final BARB Data Preparation DONE
2014-08-13 13:03:01.932 | M04 Finished
2014-08-13 13:03:01.980 | @ M01.2: v289_m04_barb_data_preparation DONE
2014-08-13 13:03:02.117 | @ M01.2: Tasks Execution DONE
2014-08-13 13:03:02.134 | @ M01.1: Pending Tasks Found
2014-08-13 13:03:02.140 | @ M01.1: Task v289_m05_barb_Matrices_generation Pending
2014-08-13 13:03:02.140 | @ M01.1: Identifying Pending TasksHousekeeping DONE
2014-08-13 13:03:02.140 | Begining  M01.2 - Tasks Execution
2014-08-13 13:03:02.140 | @ M01.2: Executing ->v289_m05_barb_Matrices_generation
2014-08-13 13:03:02.148 | Begining M05.0 - Initialising Environment
2014-08-13 13:03:02.148 | @ M05.0: Initialising Environment DONE
2014-08-13 13:03:02.148 | Begining M05.1 - Aggregating transient tables
2014-08-13 13:03:05.625 | @ M05.1: Base Table Generation DONE
2014-08-13 13:03:30.028 | @ M05.1: Base2 Table Generation DONE
2014-08-13 13:03:30.028 | @ M05.1: Aggregating transient tables DONE
2014-08-13 13:03:30.028 | Begining M05.1 - Generating Matrices
2014-08-13 13:03:31.202 | @ M05.1: Sex/Age Matrix Generation DONE (v289_genderage_matrix)
2014-08-13 13:03:32.400 | @ M05.1: Session size Matrix Generation DONE (v289_sessionsize_matrix)
2014-08-13 13:03:34.342 | @ M05.1: DEFAULT Session size Matrix Generation DONE (v289_sessionsize_matrix_default)
2014-08-13 13:03:34.342 | @ M05.1: Generating Matrices DONE
2014-08-13 13:03:34.342 | M05 Finished
2014-08-13 13:03:34.432 | @ M01.2: v289_m05_barb_Matrices_generation DONE
2014-08-13 13:03:34.564 | @ M01.2: Tasks Execution DONE
2014-08-13 13:03:34.599 | @ M01.1: Pending Tasks Found
2014-08-13 13:03:34.604 | @ M01.1: Task v289_m06_DP_data_extraction Pending
2014-08-13 13:03:34.604 | @ M01.1: Identifying Pending TasksHousekeeping DONE
2014-08-13 13:03:34.604 | Begining  M01.2 - Tasks Execution
2014-08-13 13:03:34.604 | @ M01.2: Executing ->v289_m06_DP_data_extraction
2014-08-13 13:03:34.612 | Begining M06.0 - Initialising Environment
2014-08-13 13:03:34.613 | @ M06.0: Initialising Environment DONE
2014-08-13 13:03:34.613 | Begining M06.1 - Composing Table Name
2014-08-13 13:03:34.613 | @ M06.1: Composing Table Name DONE
2014-08-13 13:03:34.613 | Begining M06.2 - Data Extraction
2014-08-13 13:08:04.870 | @ M06.2: Data Extraction DONE
2014-08-13 13:08:04.870 | Begining M06.3 - Trimming Sample
2014-08-13 13:08:19.027 | @ M06.3: Trimming Sample DONE
2014-08-13 13:08:19.092 | M06 Finished
2014-08-13 13:08:19.144 | @ M01.2: v289_m06_DP_data_extraction DONE
2014-08-13 13:08:19.285 | @ M01.2: Tasks Execution DONE
2014-08-13 13:08:19.305 | @ M01.1: Pending Tasks Found
2014-08-13 13:08:19.313 | @ M01.1: Task v289_m07_dp_data_preparation Pending
2014-08-13 13:08:19.313 | @ M01.1: Identifying Pending TasksHousekeeping DONE
2014-08-13 13:08:19.313 | Begining  M01.2 - Tasks Execution
2014-08-13 13:08:19.313 | @ M01.2: Executing ->v289_m07_dp_data_preparation
2014-08-13 13:08:19.353 | Begining  M07.0 - Initialising Environment
2014-08-13 13:08:19.428 | @ M07.0: Initialising Environment DONE
2014-08-13 13:08:19.428 | Begining  M07.1 - Compacting Data at Event level
2014-08-13 13:09:16.185 | @ M07.1: Compacting Data at Event level DONE
2014-08-13 13:09:16.185 | Begining  M07.2 - Appending Dimensions
2014-08-13 13:09:19.966 | @ M07.2: Appending Session_Daypart DONE
2014-08-13 13:09:39.044 | @ M07.2: Appending Channel_Pack DONE
2014-08-13 13:09:52.629 | @ M07.2: Appending HHSize DONE
2014-08-13 13:10:00.348 | @ M07.2: Appending Segment_ID DONE
2014-08-13 13:10:00.348 | @ M07.2: Appending Dimensions DONE
2014-08-13 13:10:00.348 | Begining  M07.3 - Flagging Overlapping Events
2014-08-13 13:12:03.052 | @ M07.3: Flagging Overlapping Events DONE
2014-08-13 13:12:25.459 | @ M07.3: Breaking Overlapping Events into Chunks DONE
2014-08-13 13:12:34.607 | @ M07.3: Assembling batches of overlaps DONE
2014-08-13 13:12:34.607 | Begining  M07.4 - Returning Results
2014-08-13 13:13:26.740 | @ M07.4: Output table V289_M07_DP_DATA DONE
2014-08-13 13:13:26.740 | M07 Finished
2014-08-13 13:13:26.823 | @ M01.2: v289_m07_dp_data_preparation DONE
2014-08-13 13:13:26.973 | @ M01.2: Tasks Execution DONE
2014-08-13 13:13:27.037 | @ M01.1: Pending Tasks Found
2014-08-13 13:13:27.044 | @ M01.1: Task v289_m09_Session_size_definition Pending
2014-08-13 13:13:27.044 | @ M01.1: Identifying Pending TasksHousekeeping DONE
2014-08-13 13:13:27.044 | Begining  M01.2 - Tasks Execution
2014-08-13 13:13:27.044 | @ M01.2: Executing ->v289_m09_Session_size_definition
2014-08-13 13:13:27.057 | Begining M09.0 - Initialising Environment
2014-08-13 13:14:02.181 | @ M09.1: temp_Event Table created: 11158541
2014-08-13 13:14:22.225 | @ M09.2: Single Box events done: 10082094
2014-08-13 13:14:22.225 | @ M09.3: Multi Box events started 10082094
2014-08-13 13:14:24.598 | @ M09.3: Multi Box primary box table populated: 534002
2014-08-13 13:14:28.434 | @ M09.3: Multi Box primary box events updated: 534002
2014-08-13 13:14:28.470 | @ M09.4: Multi Box Other boxes loop started 
2014-08-13 13:14:28.601 | @ M09.4: Multi Box start box #: 2
2014-08-13 13:14:35.539 | @ M09.4: Multi Box events_1_box table populated: 533948
2014-08-13 13:14:38.003 | @ M09.3: Multi Box box#: 2  events updated: 533948
2014-08-13 13:14:38.212 | @ M09.4: Multi Box start box #: 3
2014-08-13 13:14:43.421 | @ M09.4: Multi Box events_1_box table populated: 8454
2014-08-13 13:14:43.702 | @ M09.3: Multi Box box#: 3  events updated: 8454
2014-08-13 13:14:43.881 | @ M09.4: Multi Box start box #: 4
2014-08-13 13:14:49.209 | @ M09.4: Multi Box events_1_box table populated: 43
2014-08-13 13:14:50.027 | @ M09.3: Multi Box box#: 4  events updated: 43
2014-08-13 13:14:52.301 | @ M09.4: Multi Box events updated: 1351829
2014-08-13 13:15:00.347 | @ M09.4: Single Box events updated: 9806712
2014-08-13 13:15:00.638 | @ M01.2: v289_m09_Session_size_definition DONE
2014-08-13 13:15:00.867 | @ M01.2: Tasks Execution DONE
2014-08-13 13:15:00.892 | @ M01.1: Pending Tasks Found
2014-08-13 13:15:00.902 | @ M01.1: Task v289_M10_individuals_selection Pending
2014-08-13 13:15:00.902 | @ M01.1: Identifying Pending TasksHousekeeping DONE
2014-08-13 13:15:00.902 | Begining  M01.2 - Tasks Execution
2014-08-13 13:15:00.902 | @ M01.2: Executing ->v289_M10_individuals_selection
2014-08-13 13:15:00.933 | M10 - Individuals assignment module start
2014-08-13 13:15:02.048 | M10 S0.0 - Initialise tables
2014-08-13 13:15:02.048 | M10 S0.2 - Initialise transient tables
2014-08-13 13:15:04.790 | M10 S0.3 - Initialise output tables
2014-08-13 13:15:05.804 | M10 S1.0 - Initialise variables
2014-08-13 13:15:05.884 | M10 S2.1 - Calculate default PIV
2014-08-13 13:15:06.197 | M10 S2.2 - Calculate date-wise PIV
2014-08-13 13:15:06.761 | M10 S2.3 - Add PIV for undefined gender
2014-08-13 13:15:07.098 | M10 S3.1 - Join all possible individuals to viewing data
2014-08-13 13:17:02.846 | M10 S3.3 - Filter out overlapping events with too more overlaps than available STBs
2014-08-13 13:17:43.574 | M10 S3.4 - Append PIVs to individuals
2014-08-13 13:18:00.217 | M10 S3.5 - Filter out accounts with fewer individuals than the expected household size
2014-08-13 13:18:09.449 | M10 S3.6 - Delete existing results from current date of data
2014-08-13 13:18:10.773 | M10 S4.0 - Assign audience for single-occupancy households and whole-household audiences
2014-08-13 13:18:27.598 | M10 S5.0 - Assign audience for non-overlapping events
2014-08-13 13:20:27.024 | M10 S6.0 - Assign audience for overlapping events
2014-08-13 13:20:37.953 | M10 - Individuals assignment complete!
2014-08-13 13:20:38.002 | @ M01.2: v289_M10_individuals_selection DONE
2014-08-13 13:20:38.126 | @ M01.2: Tasks Execution DONE
2014-08-13 13:20:38.147 | @ M01.1: Pending Tasks Found
2014-08-13 13:20:38.154 | @ M01.1: Task V289_M11_01_SC3_v1_1__do_weekly_segmentation Pending
2014-08-13 13:20:38.154 | @ M01.1: Identifying Pending TasksHousekeeping DONE
2014-08-13 13:20:38.154 | Begining  M01.2 - Tasks Execution
2014-08-13 13:20:38.154 | @ M01.2: Executing ->V289_M11_01_SC3_v1_1__do_weekly_segmentation
2014-08-13 13:40:33.764 | @ M01.2: V289_M11_01_SC3_v1_1__do_weekly_segmentation DONE
2014-08-13 13:40:34.064 | @ M01.2: Tasks Execution DONE
2014-08-13 13:40:34.095 | @ M01.1: Pending Tasks Found
2014-08-13 13:40:34.133 | @ M01.1: Task V289_M11_02_SC3_v1_1__prepare_panel_members Pending
2014-08-13 13:40:34.133 | @ M01.1: Identifying Pending TasksHousekeeping DONE
2014-08-13 13:40:34.133 | Begining  M01.2 - Tasks Execution
2014-08-13 13:40:34.133 | @ M01.2: Executing ->V289_M11_02_SC3_v1_1__prepare_panel_members
2014-08-13 13:42:03.266 | @ M01.2: V289_M11_02_SC3_v1_1__prepare_panel_members DONE
2014-08-13 13:42:03.383 | @ M01.2: Tasks Execution DONE
2014-08-13 13:42:03.400 | @ M01.1: Pending Tasks Found
2014-08-13 13:42:03.407 | @ M01.1: Task V289_M11_03_SC3I_v1_1__add_individual_data Pending
2014-08-13 13:42:03.407 | @ M01.1: Identifying Pending TasksHousekeeping DONE
2014-08-13 13:42:03.407 | Begining  M01.2 - Tasks Execution
2014-08-13 13:42:03.407 | @ M01.2: Executing ->V289_M11_03_SC3I_v1_1__add_individual_data
2014-08-13 13:47:55.419 | @ M01.2: V289_M11_03_SC3I_v1_1__add_individual_data DONE
2014-08-13 13:47:55.524 | @ M01.2: Tasks Execution DONE
2014-08-13 13:47:55.543 | @ M01.1: Pending Tasks Found
2014-08-13 13:47:55.549 | @ M01.1: Task V289_M11_04_SC3I_v1_1__make_weights_BARB Pending
2014-08-13 13:47:55.549 | @ M01.1: Identifying Pending TasksHousekeeping DONE
2014-08-13 13:47:55.549 | Begining  M01.2 - Tasks Execution
2014-08-13 13:47:55.549 | @ M01.2: Executing ->V289_M11_04_SC3I_v1_1__make_weights_BARB
2014-08-13 13:48:31.482 | @ M01.2: V289_M11_04_SC3I_v1_1__make_weights_BARB DONE
2014-08-13 13:48:31.594 | @ M01.2: Tasks Execution DONE
2014-08-13 13:48:31.613 | Begining  M01.3 - Returning results
2014-08-13 13:48:31.613 | @ M01.3: Returning results DONE
2014-08-13 13:48:31.613 | M01 Finished
Execution time: 5180.517 seconds