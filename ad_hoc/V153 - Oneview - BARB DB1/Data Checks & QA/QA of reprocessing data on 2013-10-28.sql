/*

QA the genration of BARB DB1 data for 1-7 Jun 2012

Author: Claudio Lima
Date: 2013-10-28

*/

-- What tables to look for?
select table_name from sp_tables() where table_owner = 'igonorp' and lower(table_name) like 'barb%' order by 1

-- Household
select count(*) from igonorp.BARB_PANEL_VIEWING_FILE_HOME_CHARACTERISTICS_2012_06_01_07 -- 40,657
select count(*) from igonorp.BARB_PANEL_VIEWING_FILE_HOME_CHARACTERISTICS_2012_06_01_07_Code_Descr -- 40,657
select top 100 * from igonorp.BARB_PANEL_VIEWING_FILE_HOME_CHARACTERISTICS_2012_06_01_07_Code_Descr

-- Panel members
select count(*) from igonorp.BARB_VIEWING_FILE_INDIVIDUAL_PANEL_MEMBER_DETAILS_2012_06_01_07 -- 97,293
select count(*) from igonorp.BARB_VIEWING_FILE_INDIVIDUAL_PANEL_MEMBER_DETAILS_2012_06_01_07_Code_Descr -- 97,293
select top 100 * from igonorp.BARB_VIEWING_FILE_INDIVIDUAL_PANEL_MEMBER_DETAILS_2012_06_01_07_Code_Descr

-- TV sets
select count(*) from igonorp.BARB_PANEL_VIEWING_FILE_TV_SET_CHARACTERISTICS_2012_06_01_07 -- 68,613
select count(*) from igonorp.BARB_PANEL_VIEWING_FILE_TV_SET_CHARACTERISTICS_2012_06_01_07_Code_Descr -- 68,613
select top 100 * from igonorp.BARB_PANEL_VIEWING_FILE_TV_SET_CHARACTERISTICS_2012_06_01_07_Code_Descr

-- Panel members viewing data
select count(*) from igonorp.BARB_VIEWING_RECORD_PANEL_MEMBERS_2012_06_01_07
select count(*) from igonorp.BARB_VIEWING_RECORD_PANEL_MEMBERS_2012_06_01_07_Code_Descr -- 830,257
select count(*) from igonorp.BARB_VIEWING_RECORD_PANEL_MEMBERS_2012_06_01_07_Time_Format -- 830,257

-- Panel guests viewing data
select count(*) from igonorp.BARB_VIEWING_RECORD_PANEL_GUESTS_2012_06_01_07
select count(*) from igonorp.BARB_VIEWING_RECORD_PANEL_GUESTS_2012_06_01_07_Code_Descr -- 54,188
select count(*) from igonorp.BARB_VIEWING_RECORD_PANEL_GUESTS_2012_06_01_07_Time_Format -- 54,188

-- Scaling weights 
select count(*) from igonorp.BARB_PANEL_VIEWING_FILE_RESPONSE_WEIGHT_VIEWING_CATEGORY_2012_06_01_07
select count(*) from igonorp.BARB_PANEL_VIEWING_FILE_RESPONSE_WEIGHT_VIEWING_CATEGORY_2012_06_01_07_CODE_DESCR -- 853,833
select top 100 * from igonorp.BARB_PANEL_VIEWING_FILE_RESPONSE_WEIGHT_VIEWING_CATEGORY_2012_06_01_07_CODE_DESCR

-- 
select count(*) from igonorp.Final_Combination_Panel_Members_Guest_with_Prog_Instances_dedups -- 1,203,794

select count(*) from (
select household_number,set_number,date_of_activity_db1,event_start_date_time
from igonorp.Final_Combination_Panel_Members_Guest_with_Prog_Instances_dedups
group by household_number,set_number,date_of_activity_db1,event_start_date_time
) t
-- 845,645

select top 100 * from igonorp.Final_Combination_Panel_Members_Guest_with_Prog_Instances_dedups

select number_panel_members,count(*) 
from igonorp.Final_Combination_Panel_Members_Guest_with_Prog_Instances_dedups 
where total_individual_weight <0.001
group by number_panel_members
order by number_panel_members

select v.*,w.*
from igonorp.Final_Combination_Panel_Members_Guest_with_Prog_Instances_dedups v
inner join igonorp.BARB_PANEL_VIEWING_FILE_RESPONSE_WEIGHT_VIEWING_CATEGORY_2012_06_01_07_CODE_DESCR w
on v.household_number = w.household_number
and v.date_of_activity_db1 = w.date_of_activity_db1
where v.total_individual_weight <0.001
and v.number_panel_members > 0
and w.reporting_panel_code = 50


