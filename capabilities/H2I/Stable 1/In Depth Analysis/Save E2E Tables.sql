


-- Suffix:  _v22_3_0_M10results



/*--------------------------------------------------------------------------------------------
----------------------------------------------------------------------------------------------
------    SAVE DATA TABLES
----------------------------------------------------------------------------------------------
--------------------------------------------------------------------------------------------*/

----- M00 Reference Tables
select * into V289_PIV_Grouped_Segments_desc_v22_3_0_M10results
from V289_PIV_Grouped_Segments_desc

grant select on V289_PIV_Grouped_Segments_desc_v22_3_0_M10results to vespa_group_low_security


select * into barb_weights_v22_3_0_M10results
from barb_weights

grant select on barb_weights_v22_3_0_M10results to vespa_group_low_security




----- M04 Barb Data
select * into skybarb_v22_3_0_M10results
from skybarb

grant select on skybarb_v22_3_0_M10results to vespa_group_low_security


select * into skybarb_fullview_v22_3_0_M10results
from skybarb_fullview

grant select on skybarb_fullview_v22_3_0_M10results to vespa_group_low_security




----- M05 Matrices
select * into v289_nonviewers_matrix_v22_3_0_M10results
from v289_nonviewers_matrix

grant select on v289_nonviewers_matrix_v22_3_0_M10results to vespa_group_low_security


select * into v289_genderage_matrix_v22_3_0_M10results
from v289_genderage_matrix

grant select on v289_genderage_matrix_v22_3_0_M10results to vespa_group_low_security


select * into v289_sessionsize_matrix_v22_3_0_M10results
from v289_sessionsize_matrix

grant select on v289_sessionsize_matrix_v22_3_0_M10results to vespa_group_low_security


select * into V289_VSIZEALLOC_MATRIX_SMALL_v22_3_0_M10results
from V289_VSIZEALLOC_MATRIX_SMALL

grant select on V289_VSIZEALLOC_MATRIX_SMALL_v22_3_0_M10results to vespa_group_low_security



select * into V289_VSIZEALLOC_MATRIX_BIG_v22_3_0_M10results
from V289_VSIZEALLOC_MATRIX_BIG

grant select on V289_VSIZEALLOC_MATRIX_BIG_v22_3_0_M10results to vespa_group_low_security



select * into v289_sessionsize_matrix_default_v22_3_0_M10results
from v289_sessionsize_matrix_default

grant select on v289_sessionsize_matrix_default_v22_3_0_M10results to vespa_group_low_security




----- M07 Viewing Data
select * into V289_M07_dp_data_v22_3_0_M10results
from V289_M07_dp_data

grant select on V289_M07_dp_data_v22_3_0_M10results to vespa_group_low_security


----- M08 Experian Data
select * into V289_M08_SKY_HH_composition_v22_3_0_M10results
from V289_M08_SKY_HH_composition

grant select on V289_M08_SKY_HH_composition_v22_3_0_M10results to vespa_group_low_security



/*--------------------------------------------------------------------------------------------
----------------------------------------------------------------------------------------------
------    SAVE ALGORITHM TABLES
----------------------------------------------------------------------------------------------
--------------------------------------------------------------------------------------------*/


----- M10 Assign Viewers
select * into V289_M10_session_individuals_v22_3_0_M10results
from V289_M10_session_individuals

grant select on V289_M10_session_individuals_v22_3_0_M10results to vespa_group_low_security


select * into V289_M10_PIV_default_v22_3_0_M10results
from V289_M10_PIV_default

grant select on V289_M10_PIV_default_v22_3_0_M10results to vespa_group_low_security



select * into V289_M10_combined_event_data_v22_3_0_M10results
from V289_M10_combined_event_data

grant select on V289_M10_combined_event_data_v22_3_0_M10results to vespa_group_low_security



select * into V289_M10_combined_event_data_adj_v22_3_0_M10results
from V289_M10_combined_event_data_adj

grant select on V289_M10_combined_event_data_adj_v22_3_0_M10results to vespa_group_low_security




