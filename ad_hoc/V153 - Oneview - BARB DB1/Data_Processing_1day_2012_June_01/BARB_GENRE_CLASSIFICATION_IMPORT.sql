/*
Author Patrick Igonor
14 of June 2013
*/

---Creating the Genre classification table -----

CREATE TABLE BARB_GENRE_CLASSIFICATION
                (Genre Varchar(15) DEFAULT NULL,
                 Sub_Genre Varchar(38) DEFAULT NULL,
                 Code Varchar (4) DEFAULT NULL)


Delete from BARB_GENRE_CLASSIFICATION

-- Importing Raw data

LOAD TABLE BARB_GENRE_CLASSIFICATION (Genre, Sub_Genre, Code '\n' )
FROM '/ETL013/prod/sky/olive/data/share/clarityq/export/Jim/PI_Barb/BARB_Genre_Classif.csv' QUOTES OFF ESCAPES OFF NOTIFY 1000 DELIMITED BY ',' START ROW ID 1

--Checks
select * from BARB_GENRE_CLASSIFICATION


select Genre, Sub_Genre,case when cast(Code as integer) < 1000 then 0 || Code else Code end as New_Code
into BARB_GENRE_CLASSIF
from BARB_GENRE_CLASSIFICATION

select * from BARB_GENRE_CLASSIF

------------------------------Panel_Code_Description

CREATE TABLE BARB_Panel_Reporting_Area_Codes
                (Panel_Code  Int,
                 Description Varchar(50) DEFAULT NULL)


Delete from BARB_Panel_Reporting_Area_Codes

-- Importing Raw data

LOAD TABLE BARB_Panel_Reporting_Area_Codes (Panel_Code, Description '\n' )
FROM '/ETL013/prod/sky/olive/data/share/clarityq/export/PatrickI/BARB_Panel_Reporting_Area_Codes.csv' QUOTES OFF ESCAPES OFF NOTIFY 1000 DELIMITED BY ',' START ROW ID 1

select * from BARB_Panel_Reporting_Area_Codes
