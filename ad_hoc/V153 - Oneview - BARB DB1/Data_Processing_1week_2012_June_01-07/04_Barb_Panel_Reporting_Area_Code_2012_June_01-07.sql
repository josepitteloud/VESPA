select top 10* from BARB_Panel_Reporting_Area_Codes
select top 10* from BARB_PANEL_VIEWING_FILE_RESPONSE_WEIGHT_VIEWING_CATEGORY_2012_06_01_07

--62 Row(s) affected


select count(*) from BARB_PANEL_VIEWING_FILE_RESPONSE_WEIGHT_VIEWING_CATEGORY_2012_06_01_07
--853,833 Rows Affected

select   WC.Household_Number
        ,WC.Person_Number
        ,WC.Reporting_Panel_Code
        ,AC.Description
        ,WC.Date_of_Activity_DB1
        ,WC.Response_Code
        ,WC.Processing_Weight
        ,WC.Adults_Commercial_TV_Viewing_Sextile
        ,WC.ABC1_Adults_Commercial_TV_Viewing_Sextile
        ,WC.Adults_Total_Viewing_Sextile
        ,WC.ABC1_Adults_Total_Viewing_Sextile
        ,WC.Adults_16_34_Commercial_TV_Viewing_Sextile
        ,WC.Adults_16_34_Total_Viewing_Sextile
into BARB_PANEL_VIEWING_FILE_RESPONSE_WEIGHT_VIEWING_CATEGORY_2012_06_01_07_CODE_DESCR
from BARB_PANEL_VIEWING_FILE_RESPONSE_WEIGHT_VIEWING_CATEGORY_2012_06_01_07 WC
left join BARB_Panel_Reporting_Area_Codes AC
on WC.Reporting_Panel_Code = AC.Panel_Code
--853,833 Row(s) affected


select top 10* from BARB_PANEL_VIEWING_FILE_RESPONSE_WEIGHT_VIEWING_CATEGORY_2012_06_01_07_CODE_DESCR

grant all on BARB_PANEL_VIEWING_FILE_RESPONSE_WEIGHT_VIEWING_CATEGORY_2012_06_01_07_CODE_DESCR to limac;
grant all on BARB_PANEL_VIEWING_FILE_RESPONSE_WEIGHT_VIEWING_CATEGORY_2012_06_01_07_CODE_DESCR to thompsonja;

select top 10* from BARB_PANEL_VIEWING_FILE_RESPONSE_WEIGHT_VIEWING_CATEGORY_2012_06_01_07

select Reporting_Panel_Code, Description, count(*) from BARB_PANEL_VIEWING_FILE_RESPONSE_WEIGHT_VIEWING_CATEGORY_2012_06_01_07_CODE_DESCR
group by Reporting_Panel_Code, Description

