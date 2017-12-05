/*
                         $$$
                        I$$$
                        I$$$
               $$$$$$$$ I$$$    $$$$$      $$$ZDD    DDDDDDD.
             ,$$$$$$$$  I$$$   $$$$$$$    $$$ ODD  ODDDZ 7DDDD
             ?$$$,      I$$$ $$$$. $$$$  $$$= ODD  DDD     NDD
              $$$$$$$$= I$$$$$$$    $$$$.$$$  ODD +DD$     +DD$
                  :$$$$~I$$$ $$$$    $$$$$$   ODD  DDN     NDD.
               ,.   $$$+I$$$  $$$$    $$$$=   ODD  NDDN   NDDN
              $$$$$$$$$ I$$$   $$$$   .$$$    ODD   ZDDDDDDDN
                                      $$$      .      $DDZ
                                     $$$             ,NDDDDDDD
                                    $$$?

                      CUSTOMER INTELLIGENCE SERVICES


        VESPA Panel Value - A Profiling Exercise
        --------------------------------
        Author  : Don Rombaoa (Original code from Jitesh Patel "SKY ADSMART CUSTOMER BASE FILE CREATION" - 23 April 2012)
        Date    : 13 Aug 2012

SECTIONS
----------------

 LOOKUP VARIABLES
----------------- A01) Create local variables
----------------- A02) Create Local Variables
             
code_location
        --      code_location_01        Create Main Table
        --      code_location_02        Add Affluence (Consumerview)
        --      code_location_03        Add TV Region
        --      code_location_04        Add Sky Customers 
        --      code_location_05        Flag Placement for Sky Customers and World
        --      code_location_06        Add VESPA Flag
        --      code_location_07        ADD Affluence from Axciom
        --      code_location_08        Add Risk (CQM) flags and groupings
        --      code_location_09        Add Postcode
		--		code_location_10		New Accounts Flag
        --      code_location_11        Affordability and Income Figures
		--      code_location_12        Delphi Scores 
		--      code_location_13		ABC1 Social Grades
		--      code_location_14		Create CubeTables for Excel

      
*/
---------------------------     A01 - Create local variables------------------
CREATE VARIABLE @today            date;
SET             @today =          '20120716';

SELECT cb_change_date, count(*)
FROM sk_prod.EXPERIAN_CONSUMERVIEW
GROUP BY cb_change_date
ORDER BY cb_change_date;

CREATE VARIABLE @experian         date;
SET             @experian =       '20120622';



------------------------------------------------------------------------------------CREATE Main Table-------------------
--CREATE Main Table NoDupes and populate with Consumer View information and rank them to remove duplicate accounts
--code_location_01
drop table nodupes;
SELECT   CV.cb_key_household
        ,CV.cb_row_id
        ,CV.decision_maker_type
        ,CV.family_lifestage
        ,CV.h_length_of_residency
        ,CV.h_mosaic_uk_2009_group
        ,CV.household_age
        ,CV.household_composition
        ,CV.household_income_bands
        ,CV.h_fss_v3_group 
        ,CV.lifestage
        ,CV.property_type
        ,CV.residence_type
        ,CV.tenure
        ,rank() over(PARTITION BY cv.cb_key_household ORDER BY cb_row_id desc) AS rank_id
INTO nodupes
FROM sk_prod.EXPERIAN_CONSUMERVIEW AS CV
WHERE cb_change_date=@experian
GROUP BY CV.cb_key_household
        ,CV.cb_row_id
        ,CV.decision_maker_type
        ,CV.family_lifestage
        ,CV.h_length_of_residency
        ,CV.h_mosaic_uk_2009_group
        ,CV.household_age
        ,CV.household_composition
        ,CV.household_income_bands
        ,CV.h_fss_v3_group
        ,CV.lifestage
        ,CV.property_type
        ,CV.residence_type
        ,CV.tenure
ORDER BY cv.cb_key_household;

DELETE FROM nodupes where rank_id >1;      -- Deduping


CREATE   HG INDEX idx01 ON nodupes (cb_key_household);

--alter table to include other variables of interest (just field names at the moment and not populated yet)
ALTER table     NoDupes ADD     account_number          VARCHAR(20)   NULL              ;   -- Account Number
ALTER table     NoDupes ADD     Sky_Acct_Holder         INTEGER       default 0         ;   -- Sky Account Flag
ALTER table     NoDupes ADD     cb_address_postcode     VARCHAR(10)   NULL              ;   -- postcode 
ALTER table     NoDupes ADD     currency_code           VARCHAR(50)   NULL              ;   -- Distiguish ROI accounts
ALTER table     NoDupes ADD     World                   INTEGER       default 0         ;   -- World Flag
ALTER table     NoDupes ADD     vespa                   INTEGER       default 0         ;   -- Box returning data
ALTER table     NoDupes ADD     panel_id_vespa          INTEGER       NULL              ;   -- panel_id
ALTER table     NoDupes ADD     hh_lifestage            varchar(50)   default 'missing' ;	-- From Experian Consumerview
ALTER table     NoDupes ADD     fam_lifestage           varchar(50)   default 'missing' ;	-- From Experian Consumerview
ALTER table     NoDupes ADD     Type_of_decision_maker  varchar(50)   default 'missing' ;	-- From Experian Consumerview
ALTER table     NoDupes ADD     hh_length_of_residency  varchar(50)   default 'missing' ;	-- From Experian Consumerview	
ALTER table     NoDupes ADD     hh_age                  varchar(50)   default 'missing' ;	-- From Experian Consumerview
ALTER table     NoDupes ADD     hh_composition          varchar(50)   default 'missing' ;	-- From Experian Consumerview
ALTER table     NoDupes ADD     hh_income_bands         varchar(50)   default 'missing' ;	-- From Experian Consumerview
ALTER table     NoDupes ADD     Type_of_Property        varchar(50)   default 'missing' ;	-- From Experian Consumerview
ALTER table     NoDupes ADD     Type_of_Residence       varchar(50)   default 'missing' ;	-- From Experian Consumerview
ALTER table     NoDupes ADD     HomeOwner               varchar(50)   default 'missing' ;	-- From Experian Consumerview (named Tenure in Consumerview)
ALTER table     NoDupes ADD     Financial_outlook       varchar(50)   default 'missing' ;	-- From Experian Consumerview (named as h_fss_v3_group in Consumerview)
ALTER table     NoDupes ADD     Demographic             varchar(50)   default 'missing' ;	-- From Experian Consumerview (named as h_mosaic_uk_2009_group in Consumerview)
ALTER table     NoDupes ADD     H_Affluence             varchar(50)   default 'missing' ;	-- From Experian Consumerview (via sk_prodreg.EXP_AFFLUENCE_MODEL_20120416 - not yet available until sept 2012)
ALTER table     NoDupes ADD     barb_id_itv             SMALLINT      NULL              ;  	-- data defined from table sk_prod.BARB_TV_REGIONS
ALTER table     NoDupes ADD     barb_id_bbc             SMALLINT      NULL              ;  	-- data defined from table sk_prod.BARB_TV_REGIONS
ALTER table     NoDupes ADD     barb_desc_itv           VARCHAR(50)   default 'missing' ;  	-- data defined from table sk_prod.BARB_TV_REGIONS
ALTER table     NoDupes ADD     barb_desc_bbc           VARCHAR(50)   default 'missing' ;  	-- data defined from table sk_prod.BARB_TV_REGIONS
                ;


--      Update the NoDupes file so the variables above can be populated 
UPDATE NoDupes
SET      base.hh_lifestage = EXP.lifestage                  
        ,base.fam_Lifestage = EXP.family_lifestage      
        ,base.Type_of_decision_maker = EXP.decision_maker_type 
        ,base.hh_length_of_residency = EXP.h_length_of_residency 
        ,base.hh_age = EXP.household_age                     	
        ,base.hh_composition = EXP.household_composition    
        ,base.hh_income_bands = EXP.household_income_bands   
        ,base.Type_of_Property = EXP.property_type 
        ,base.Type_of_Residence = EXP.residence_type 
        ,base.HomeOwner = EXP.tenure                    
        ,base.Financial_outlook = EXP.h_fss_v3_group    
        ,base.Demographic = EXP.h_mosaic_uk_2009_group 
      
FROM NoDupes AS Base
LEFT JOIN sk_prod.EXPERIAN_CONSUMERVIEW AS EXP
        ON EXP.cb_key_household = base.cb_key_household AND EXP.cb_row_id=base.cb_row_id;



-- NEED to Case some of the codes in order to have a better read when profiling
update NoDupes
   SET  hh_lifestage      = CASE lifestage                  WHEN '00'  THEN 'a) Very young family'
                                                            WHEN '01'  THEN 'b) Very young single'
                                                            WHEN '02'  THEN 'c) Very young homesharers'
                                                            WHEN '03'  THEN 'd) Young family'
                                                            WHEN '04'  THEN 'e) Young single'
                                                            WHEN '05'  THEN 'f) Young homesharers'
                                                            WHEN '06'  THEN 'g) Mature family'
                                                            WHEN '07'  THEN 'h) Mature singles'
                                                            WHEN '08'  THEN 'i) Mature homesharers'
                                                            WHEN '09'  THEN 'j) Older family'
                                                            WHEN '10'  THEN 'k) Older single'
                                                            WHEN '11'  THEN 'l) Older homesharers'
                                                            WHEN '12'  THEN 'm) Elderly family'
                                                            WHEN '13'  THEN 'n) Elderly single'
                                                            WHEN '14'  THEN 'o) Elderly homesharers'
                                                            WHEN 'U'   THEN 'p) Unclassified'
                                                            ELSE            'q) missing'
                                                            END

,fam_lifestage  =  CASE  family_lifestage                   WHEN '00'   THEN 'a) Young singles/homesharers'
                                                            WHEN '01'   THEN 'b) Young family no children <18'
                                                            WHEN '02'   THEN 'c) Young family with children <18'
                                                            WHEN '03'   THEN 'd) Young household with children <18'
                                                            WHEN '04'   THEN 'e) Mature singles/homesharers'
                                                            WHEN '05'   THEN 'f) Mature family no children <18'
                                                            WHEN '06'   THEN 'g) Mature family with children <18'
                                                            WHEN '07'   THEN 'h) Mature household with children <18'
                                                            WHEN '08'   THEN 'i) Older single'
                                                            WHEN '09'   THEN 'j) Older family no children <18'
                                                            WHEN '10'   THEN 'k) Older family/household with children<18'
                                                            WHEN '11'   THEN 'l) Elderly single'
                                                            WHEN '12'   THEN 'm) Elderly family no children <18'
                                                            WHEN 'U'    THEN 'n) Unclassified'
                                                            ELSE             'o) missing'
                                                            END

,Type_of_decision_maker  =  CASE  decision_maker_type       WHEN '00' THEN 'a) Male - young'
                                                            WHEN '01' THEN 'b) Male - middle'
                                                            WHEN '02' THEN 'c) Male - old'
                                                            WHEN '03' THEN 'd) Female - young'
                                                            WHEN '04' THEN 'e) Female - middle'
                                                            WHEN '05' THEN 'f) Female - old'
                                                            WHEN '06' THEN 'g) Couple - young'
                                                            WHEN '07' THEN 'h) Couple - middle'
                                                            WHEN '08' THEN 'i) Couple - old'
                                                            WHEN '09' THEN 'j) Sharers - young'
                                                            WHEN '10' THEN 'k) Sharers - middle'
                                                            WHEN '11' THEN 'l) Sharers - old'
                                                            WHEN 'U'  THEN 'm) Unclassified'
                                                            ELSE           'n) missing'
                                                            END

,hh_length_of_residency = CASE h_length_of_residency        WHEN '00' THEN 'a) Up to 1 year'
                                                            WHEN '01' THEN 'b) 1 year'
                                                            WHEN '02' THEN 'c) 2 years'
                                                            WHEN '03' THEN 'd) 3 years'
                                                            WHEN '04' THEN 'e) 4 years'
                                                            WHEN '05' THEN 'f) 5 years'
                                                            WHEN '06' THEN 'g) 6 years'
                                                            WHEN '07' THEN 'h) 7 years'
                                                            WHEN '08' THEN 'i) 8 years'
                                                            WHEN '09' THEN 'j) 9 years'
                                                            WHEN '10' THEN 'k) 10 years'
                                                            WHEN '11' THEN 'l) 11+ years'
                                                            ELSE           'm) missing'
                                                            END



,hh_age = CASE household_age                                WHEN 'A' THEN 'a) Age 18-25'
                                                            WHEN 'B' THEN 'b) Age 26-35'
                                                            WHEN 'C' THEN 'c) Age 36-45'
                                                            WHEN 'D' THEN 'd) Age 46-55'
                                                            WHEN 'E' THEN 'e) Age 56-65'
                                                            WHEN 'F' THEN 'f) Age 66+'
                                                            WHEN 'U' THEN 'g) Unclassified'
                                                            ELSE          'h) missing'        
                                                            END

,hh_composition = CASE household_composition                WHEN '00' THEN 'a) Families'
                                                            WHEN '01' THEN 'b) Extended family'
                                                            WHEN '02' THEN 'c) Extended household'
                                                            WHEN '03' THEN 'd) Pseudo family'
                                                            WHEN '04' THEN 'e) Single male'
                                                            WHEN '05' THEN 'f) Single female'
                                                            WHEN '06' THEN 'g) Male homesharers'
                                                            WHEN '07' THEN 'h) Female homesharers'
                                                            WHEN '08' THEN 'i) Mixed homesharers'
                                                            WHEN '09' THEN 'j) Abbreviated male families'
                                                            WHEN '10' THEN 'k) Abbreviated female families'
                                                            WHEN '11' THEN 'l) Multi-occupancy dwelling'
                                                            WHEN 'U'  THEN 'm) Unclassified'
                                                            ELSE           'n) missing'                                                            
                                                            END

,hh_income_bands = CASE  household_income_bands             WHEN '0' THEN 'a) < £10,000'
                                                            WHEN '1' THEN 'b) £10,000 - £14,999'
                                                            WHEN '2' THEN 'c) £15,000 - £19,999'
                                                            WHEN '3' THEN 'd) £20,000 - £24,999'
                                                            WHEN '4' THEN 'e) £25,000 - £29,999'
                                                            WHEN '5' THEN 'f) £30,000 - £39,999'
                                                            WHEN '6' THEN 'g) £40,000 - £49,999'
                                                            WHEN '7' THEN 'h) £50,000 - £59,999'
                                                            WHEN '8' THEN 'i) £60,000 - £74,999'
                                                            WHEN '9' THEN 'j) £75,000 +'
                                                            WHEN 'u' THEN 'k) Unclassified'
                                                            ELSE          'l) missing'                                                            
                                                            END


,Type_of_Property = CASE property_type                      WHEN '0' THEN 'a) Purpose built flats'
                                                            WHEN '1' THEN 'b) Converted flats'
                                                            WHEN '2' THEN 'c) Farm'
                                                            WHEN '3' THEN 'd) Named building'
                                                            WHEN '4' THEN 'e) Other type'
                                                            ELSE          'f) missing'                                                            
                                                            END


,Type_of_Residence  = CASE residence_type                   WHEN '0' THEN 'a) Detached'
                                                            WHEN '1' THEN 'b) Semi-detached'
                                                            WHEN '2' THEN 'c) Bungalow'
                                                            WHEN '3' THEN 'd) Terraced'
                                                            WHEN '4' THEN 'e) Flat'
                                                            ELSE          'f) missing'                                                            
                                                            END

,HomeOwner  = CASE tenure                                   WHEN '0' THEN 'a) Owner occupied'
                                                            WHEN '1' THEN 'b) Privately rented'
                                                            WHEN '2' THEN 'c) Council / housing association'
                                                            ELSE          'd) missing'                                                            
                                                            END


,Financial_Outlook = CASE h_fss_v3_group                    WHEN    'A' THEN    'a) Accumulated Wealth'
                                                            WHEN    'B' THEN    'b) Balancing Budgets'
                                                            WHEN    'C' THEN    'c) Bright Futures'
                                                            WHEN    'D' THEN    'd) Consolidating Assets'
                                                            WHEN    'E' THEN    'e) Established Reserves'
                                                            WHEN    'F' THEN    'f) Family Interest'
                                                            WHEN    'G' THEN    'g) Growing Rewards'
                                                            WHEN    'H' THEN    'h) Platinum Pensions'
                                                            WHEN    'I' THEN    'h) Seasoned Economy'
                                                            WHEN    'J' THEN    'i) Single Endeavours'
                                                            WHEN    'K' THEN    'j) Stretched Finances'
                                                            WHEN    'L' THEN    'k) Sunset Security'
                                                            WHEN    'M' THEN    'l) Traditional Thrift'
                                                            WHEN    'N' THEN    'm) Young Essentials'
                                                            WHEN    'U' THEN    'n) Unclassified'
                                                            ELSE    'o) missing'
                                                            END


,Demographic = CASE h_mosaic_uk_2009_group                  WHEN    'A' THEN    'a) Alpha Territory'
                                                            WHEN    'B' THEN    'b) Professional Rewards'
                                                            WHEN    'C' THEN    'c) Rural Solitude'
                                                            WHEN    'D' THEN    'd) Small Town Diversity'
                                                            WHEN    'E' THEN    'e) Active Retirement'
                                                            WHEN    'F' THEN    'f) Suburban Mindsets'
                                                            WHEN    'G' THEN    'g) Careers and Kids'
                                                            WHEN    'H' THEN    'h) New Homemakers'
                                                            WHEN    'I' THEN    'i) Ex-Council Community'
                                                            WHEN    'J' THEN    'j) Claimant Cultures'
                                                            WHEN    'K' THEN    'k) Upper Floor Living'
                                                            WHEN    'L' THEN    'l) Elderly Needs'
                                                            WHEN    'M' THEN    'm) Industrial Heritage'
                                                            WHEN    'N' THEN    'n) Terraced Melting Pot'
                                                            WHEN    'O' THEN    'o) Liberal Opinions'
                                                            WHEN    'U' THEN    'p) Unclassified'
                                                            ELSE    'q) missing'
                                                            END


;


------------------------------------------------------------------------------------------
--                                                                                      --
--   create affluence file this will need to ba changed once file is into production    --
--                                                                                      --
------------------------------------------------------------------------------------------
--code_location_02
SELECT cb_key_household, H_AFFLUENCE
INTO #H_AFFLUENCE
FROM sk_prodreg.EXP_AFFLUENCE_MODEL_20120416
GROUP BY cb_key_household, H_AFFLUENCE;

execute CITeam.VES024_Make_Reports;


Commit;

CREATE   HG INDEX idx07 ON #H_AFFLUENCE(cb_key_household);

--update NoDupes file with affluence data
UPDATE NoDupes
SET    H_AFFLUENCE             = aff.H_AFFLUENCE
      FROM NoDupes  AS Base
         INNER JOIN #H_AFFLUENCE AS aff
         ON base.cb_key_household = aff.cb_key_household;


drop table #H_AFFLUENCE;

----------------------------------------   adding in TV region data -------------------------
--code_location_03
UPDATE nodupes 
SET base.cb_address_postcode = exp.cb_address_postcode				--- populates postcode
FROM NoDupes AS Base
LEFT JOIN sk_prod.EXPERIAN_CONSUMERVIEW AS EXP
        ON EXP.cb_key_household = base.cb_key_household AND EXP.cb_row_id=base.cb_row_id;


Update NoDupes
set      base.barb_id_bbc   = si.barb_id_bbc
        ,base.barb_desc_bbc = si.barb_desc_bbc
        ,base.barb_id_itv   = si.barb_id_itv
        ,base.barb_desc_itv = si.barb_desc_itv
from NoDupes as base inner join sk_prod.BARB_TV_REGIONS as si
on base.cb_address_postcode = si.postcode;


-------------------------------------------------  Add Sky Customers to NoDupes Base file --------------------
--- code_location_04

SELECT DISTINCT account_number, currency_code,cb_key_household 
INTO #temp_Sky
FROM sk_prod.cust_subs_hist
 WHERE subscription_sub_type IN ('DTV Primary Viewing')
   AND status_code IN ('AC','AB','PC')
   AND effective_from_dt <= @today
   AND effective_to_dt > @today
   AND EFFECTIVE_FROM_DT IS NOT NULL
   AND cb_key_household > 0             --UK Only
   AND cb_key_household IS NOT NULL
   AND account_number IS NOT NULL
   AND service_instance_id IS NOT NULL;
   
commit; -- must write commit before indexing

--      create index on #temp_Sky
CREATE   HG INDEX idx03 ON #temp_Sky(account_number);
CREATE   HG INDEX idx04 ON #temp_Sky(cb_key_household);

--      insert into NoDupes file
UPDATE NoDupes
SET      base.account_number = Sky.account_number                  
        ,base.currency_code = Sky.currency_code      
  
FROM NoDupes AS Base
LEFT JOIN #temp_Sky AS Sky
        ON Sky.cb_key_household = base.cb_key_household
		

------------------------------ Flag Placement for Sky Customers and World and update NoDupes Base File ---------------------------
-- code_location_05 - Placement of Sky Account Holder Flag into Nodupes Table. 
UPDATE  nodupes 
SET     sky_acct_holder     =   0;		--populate Sky_Acct_Holder to zero first to clean out that there are only zeros and ones. Reason is that we had account numbers that were not flagged as sky accounts when we just did a straight flag rather than zeroing everything first. 

UPDATE  nodupes 
SET     sky_acct_holder     =   1		--now flagging accounts
where account_number IS NOT NULL;

-- Placement of WORLD Flag into Nodupes Table
UPDATE  nodupes 
SET     world     =   1
where cb_key_household IS NOT NULL;

CREATE   HG INDEX idx08 ON NoDupes(account_number);


----------------------------- Add VESPA returning data boxes and flag into NoDupes base file------------------------------------
--code_location_06

--Retrieve all account numbers with VESPA Panel Id's. Only want one ID per account number so used MAX(Panel_id_vespa)
select DISTINCT (account_number), MAX (panel_id_vespa) as Panel_ID_Vespa 	
into #temp_vespa_panel
from Vespa_Analysts.Vespa_Single_Box_View
GROUP BY account_number

--Deleting all accounts that are not in our VESPA target groups of 4, 12 ,6 7
DELETE 
FROM #temp_vespa_panel
WHERE panel_id_vespa NOT in (4,12,6,7); 		

--Flag Placement of VESPA Flag into temporary table
SELECT DISTINCT account_number, 1 AS VESPA
INTO #vespa
FROM #temp_vespa_panel
WHERE panel_id_vespa in (4,12,6,7); 

--Must commit to proceed
commit; 

--update NoDupes file by inserting VESPA flag
UPDATE NoDupes
SET  VESPA = ves.VESPA
FROM NoDupes AS Base
  LEFT JOIN #VESPA AS ves
        ON base.account_number = ves.account_number;

--update NoDupes file by inserting VESPA Panel Ids
UPDATE NoDupes
SET  panel_id_vespa = ves.panel_id_vespa
FROM NoDupes  AS Base
  LEFT JOIN #temp_vespa_panel AS ves
        ON base.account_number = ves.account_number;

--delete temp files
drop table #temp_vespa_panel;
drop table #vespa;

---------------------------------- ADD Affluence from Axciom into NoDupes base file -------------------------------------

--code_location_07
SELECT          cb_row_id, account_number,
                CASE    WHEN P1 = 1  THEN 1
                        WHEN P2 = 1  THEN 2
                        ELSE              3        END AS Correspondent,
                rank() over(PARTITION BY account_number ORDER BY Correspondent asc
, cb_row_id desc) AS rank
INTO            tmpDSOdb_ilu
FROM            (SELECT         ilu.cb_row_id, base.account_number,
                            MAX(CASE WHEN ilu.ilu_correspondent = 'P1' THEN 1 ELSE 0 END) AS P1,
                            MAX(CASE WHEN ilu.ilu_correspondent = 'P2' THEN 1 ELSE 0 END) AS P2,
                            MAX(CASE WHEN ilu.ilu_correspondent = 'OR' THEN 1 ELSE 0 END) AS OR1
                FROM            sk_prod.ilu AS ilu
                        INNER JOIN nodupes AS base
                                ON ilu.cb_key_household = base.cb_key_household
                WHERE           ilu.cb_key_household IS NOT NULL
                AND             ilu.cb_key_household <>0      
                GROUP BY        ilu.cb_row_id, base.account_number
                HAVING          P1 + P2 + OR1 > 0) AS tgt;

--Delete any duplicates
DELETE FROM tmpDSOdb_ilu where rank > 1;

CREATE  HG INDEX idx01 on tmpDSOdb_ilu(cb_row_id);
CREATE  HG INDEX idx02 on tmpDSOdb_ilu(account_number);

--adding on Axciom Affluence to NoDupes base file
ALTER TABLE nodupes        ADD (Affluence_group  varchar(25));

UPDATE          nodupes AS base
SET             affluence_group = case WHEN ilu.ILU_HHAfflu IN (01,02,03,04) THEN 'A) Very Low'
                                       WHEN ilu.ILU_HHAfflu IN (05,06)       THEN 'B) Low'
                                       WHEN ilu.ILU_HHAfflu IN (07,08)       THEN 'C) Mid Low'
                                       WHEN ilu.ILU_HHAfflu IN (09,10)       THEN 'D) Mid'
                                       WHEN ilu.ILU_HHAfflu IN (11,12)       THEN 'E) Mid High'
                                       WHEN ilu.ILU_HHAfflu IN (13,14,15)    THEN 'F) High'
                                       WHEN ilu.ILU_HHAfflu IN (16,17)       THEN 'G) Very High' end
FROM            sk_prod.ilu AS ilu INNER JOIN tmpDSOdb_ilu
ON              ilu.cb_row_id = tmpDSOdb_ilu.cb_row_id
WHERE           base.account_number = tmpDSOdb_ilu.account_number;

DROP TABLE tmpDSOdb_ilu

--------------------------------------------------------------------------------
-- Add Risk (CQM) flags and groupings ~
--------------------------------------------------------------------------------
--code_location_08
-- CQM scores are joined on cb_key_household which is in the daily viewing table - lets add the CQM score to NoDUPES table and the banding definitions

--Add columns to nodupes for population  
alter table nodupes		add 	cqm_score 		tinyint 	default 	null
						,add 	cqm_group 		varchar(30) default 	null
						,add 	cqm_indicator 	varchar(20) default 	null

--Populate nodupes
update NoDupes as base
set base.cqm_score = zz.model_score -- this is the raw score bcos people can change thier minds
,base.cqm_group = case when zz.model_score between 1 and 10 then 'a) Low Risk' -- these are standard groupings - aquisition??
                       when zz.model_score between 11 and 26 then 'b) Medium Risk'
                       when zz.model_score between 27 and 36 then 'c) High Risk'
                       else 'd) Unknown'
                       end
,base.cqm_indicator = case when zz.model_score between 1 and 22 then 'High quality'-- from Matt Oakman via email via tom
                       when zz.model_score between 23 and 36 then 'Low quality'
                       else 'No Score!'
                       end
from sk_prod.id_v_universe_all zz
where base.cb_key_household = zz.cb_key_household;
----Missing CQM scores associated with rows that have nulls in CQM scores (65,269 rows). See script below.
--SELECT COUNT (cb_key_household)
--FROM nodupes WHERE cb_key_household NOT IN 
--(SELECT cb_key_household 
--FROM nodupes 
--WHERE cqm_group is NOT NULL) 


-------------------------------------------------  Add Postcode ----------------------------------------------
--code_location_09
-- get postcode
SELECT  distinct base.cb_key_household
        ,sav.cb_address_postcode
      ,rank() over(PARTITION BY SAV.cb_key_household ORDER BY SAV.cb_address_postcode desc) AS rank_id
INTO #postcode
  FROM sk_prod.CUST_SINGLE_ACCOUNT_VIEW as SAV
      inner join NoDupes as base
on base.cb_key_household = SAV.cb_key_household        ---used account_number as matching key previously but it led to most columns being empty if not an account holder. Changed matching key to cb_key_household so that all those WITHOUT accounts also are populated with information
where cust_active_dtv = 1;

--Delete duplicates
DELETE FROM #postcode where rank_id > 1;

--Must commit
Commit;

--create index on #BB
CREATE   HG INDEX idx10 ON #postcode(account_number);

--update NoDupes file
UPDATE NoDupes
SET  cb_address_postcode = ves.cb_address_postcode
FROM NoDupes  AS Base
  LEFT JOIN #postcode AS ves
        ON base.account_number = ves.account_number;


----------------------------------------------	New Accounts Flag-----------------------------
--code_location_10
SELECT account_number, booking_dt, dtv_activation_dt 
INTO #new_accts_flag 
FROM CITeam.dsodb_base
where booking_dt between '2011-04-01' and '2012-03-31'  and (dtv_activation_dt between booking_dt and dateadd(dd, 56, booking_dt)) 
order by booking_dt asc;

--Add flag for new accounts. First step is to clear out the columns and place a zero 
Alter table nodupes   ADD new_accounts_flag    integer default 0
Commit;

--Now add flag
UPDATE nodupes
SET new_accounts_flag = 1
from nodupes as A inner join #new_accts_flag as B on A.account_number = B.account_number;

		
-----------------------------------------------  Affordability and Income Figures---------------------------
--code_location_11 
--Create temporary table to populate some variables of affordability and income from ILU (aka Axciom)
DROP temp_affordability;
SELECT
	cb_row_id
	,cb_key_household
	,ILU_HHAffordabilityRank
	,OUT_HHTotal_PW
	,OUT_HHTotal_Band
	,OUT_HHSupermarket_PW
	,ILU_HHNetIncome_Band                  
	,ILU_HHDiscretionaryIncome_Band
	,ILU_HHEquivIncome
	,ILU_HHEquivIncome_Index
	,rank() over(PARTITION BY cb_key_household ORDER BY cb_row_id desc) AS rank_id
INTO temp_affordability
FROM sk_prod.ilu_affordability;
DELETE FROM temp_affordability where rank_id >1;      -- Deduping
Commit;
CREATE   Unique INDEX idx01 ON temp_affordability (cb_key_household);

--create new fields
ALTER TABLE  NODUPES     ADD ILU_HHAffordabilityRank         Decimal (2,0)   default     NULL;
ALTER TABLE  NODUPES     ADD OUT_HHTotal_PW                  Decimal (6,2)   default     NULL;
ALTER TABLE  NODUPES     ADD OUT_HHTotal_Band                Decimal (2,0)   default     NULL;
ALTER TABLE  NODUPES     ADD OUT_HHSupermarket_PW            Decimal (6,2)   default     NULL;
ALTER TABLE  NODUPES     ADD ILU_HHNetIncome_Band            Decimal (2,0)   default     NULL;                  
ALTER TABLE  NODUPES     ADD ILU_HHDiscretionaryIncome_Band  Decimal (2,0)   default     NULL;
ALTER TABLE  NODUPES     ADD ILU_HHEquivIncome               Decimal (2,0)   default     NULL;  
ALTER TABLE  NODUPES     ADD ILU_HHEquivIncome_Index         Decimal (4,0)   default     NULL;

--populate the fields
UPDATE NODUPES
SET
        NODUPES.ILU_HHAffordabilityRank         =aff.ILU_HHAffordabilityRank   
        ,NODUPES.OUT_HHTotal_PW                  =aff.OUT_HHTotal_PW
        ,NODUPES.OUT_HHTotal_Band                =aff.OUT_HHTotal_Band
        ,NODUPES.OUT_HHSupermarket_PW            =aff.OUT_HHSupermarket_PW
        ,NODUPES.ILU_HHNetIncome_Band            =aff.ILU_HHNetIncome_Band  
        ,NODUPES.ILU_HHDiscretionaryIncome_Band  =aff.ILU_HHDiscretionaryIncome_Band
        ,NODUPES.ILU_HHEquivIncome               =aff.ILU_HHEquivIncome   
        ,NODUPES.ILU_HHEquivIncome_Index         =aff.ILU_HHEquivIncome_Index  
FROM NODUPES
INNER JOIN temp_affordability AS aff
on nodupes.cb_key_household=aff.cb_key_household;			--389k people don't match consumerview with Axciom cb_household_key. 

--Need to create a new set of fields in nodupes for the case statements -- need to run
ALTER TABLE  NODUPES     ADD HHTotalOutgoings_Band       VARCHAR(15)   NULL              ;   
ALTER TABLE  NODUPES     ADD HHNetIncome_Band            VARCHAR(15)   NULL              ;                    
ALTER TABLE  NODUPES     ADD HHDiscretionaryIncome_Band  VARCHAR(15)   NULL              ;  
ALTER TABLE  NODUPES     ADD HHEquivIncome               VARCHAR(15)   NULL              ;   

UPDATE NODUPES
SET  HHTotalOutgoings_Band = case OUT_HHTotal_Band when 1 then '<£40'
                                                          when 2 then '£41-£60'
                                                          when 3 then '£61-£80'
                                                          when 4 then '£81-£95'
                                                          when 5 then '£96-£115'
                                                          when 6 then '£116-£140'
                                                          when 7 then '£141-£170'
                                                          when 8 then '£171-£215'
                                                          when 9 then '£216-£290'
                                                          when 10 then '>£290'
                                                          else 'Unknown'
                                                          end
       ,HHNetIncome_Band  = case ILU_HHNetIncome_Band when 1 then '<£130'
                                                                  when 2 then '£131-£185'
                                                                  when 3 then '£186-£260'
                                                                  when 4 then '£261-£330'
                                                                  when 5 then '£331-£400'
                                                                  when 6 then '£401-£480'
                                                                  when 7 then '£481-£555'
                                                                  when 8 then '£556-£725'
                                                                  when 9 then '£726-£1060'
                                                                  when 10 then '>£1060'
                                                                  else 'Unknown'
                                                                  end	
       ,HHDiscretionaryIncome_Band = case ILU_HHDiscretionaryIncome_Band when 1 then '<-£100'
                                                                                      when 2 then '-£100 to -£45'
                                                                                      when 3 then '-£44 to £0'
                                                                                      when 4 then '0-£45'
                                                                                      when 5 then '£46-£80'
                                                                                      when 6 then '£81-£130'
                                                                                      when 7 then '£131-£195'
                                                                                      when 8 then '£196-£295'
                                                                                      when 9 then '£296-£530'
                                                                                      when 10 then '>£530'
                                                                                      else 'Unknown'
                                                                                      end	
  ,HHEquivIncome = case when ILU_HHEquivIncome = 1 then '<5k'
                                      when ILU_HHEquivIncome = 2 then '5-10k'
                                      when ILU_HHEquivIncome between 3 and 4 then '10-20k'
                                      when ILU_HHEquivIncome between 5 and 6 then '20-30k'
                                      when ILU_HHEquivIncome between 7 and 8 then '30-40k'
                                      when ILU_HHEquivIncome = 9 then '40-50k'
                                      when ILU_HHEquivIncome = 10 then '50-75k'
                                      when ILU_HHEquivIncome = 11 then '75k+'
                                      else 'Unknown'
                                      end;																					  
														  
--Request by Tom Khabaza for other variables to be inputted
ALTER TABLE  NODUPES     ADD OUT_HHFinance_PW           Decimal (6,2)   default     NULL;
ALTER TABLE  NODUPES     ADD OUT_HHGoods_PW            	Decimal (6,2)   default    	NULL;														  
ALTER TABLE  NODUPES     ADD OUT_HHMotorTotal_PW        Decimal (6,2)   default     NULL;															  
ALTER TABLE  NODUPES     ADD OUT_HHClothing_PW          Decimal (6,2)   default     NULL;															  
ALTER TABLE  NODUPES     ADD OUT_HHHousingEnergy_Index  INTEGER   		default     NULL;

--Populate directly from sk_prod.ilu_affordability instead of temp_affordability to save time
UPDATE NODUPES
SET
a.OUT_HHFinance_PW=b.OUT_HHFinance_PW,
a.OUT_HHGoods_PW=b.OUT_HHGoods_PW,
a.OUT_HHMotorTotal_PW=b.OUT_HHMotorTotal_PW,
a.OUT_HHClothing_PW=b.OUT_HHClothing_PW,
a.OUT_HHHousingEnergy_Index=b.OUT_HHHousingEnergy_Index
FROM nodupes AS a
Inner join sk_prod.ilu_affordability AS b
on a.cb_key_household=b.cb_key_household; 

--NOTE: there are missing values due to nulls not being counted. We find that the amount of account holders that have a spend(8,308,455) is not equal to the amount of account holders (8,705,809). 4.6% are missing due to nulls. Script below:
--select count(1) -- places a flag on all acount numbers that do not have a value (i.e NULL) and counts them
--from rombaoad.nodupes 
--where Sky_Acct_Holder = 1 and account_number not in (select account_number
--from rombaoad.nodupes
--where OUT_HHGoods_PW >0 and Sky_Acct_Holder = 1)
--RESULT is the the 397,354 that have nulls (5%), hence we can now account for all the numbers. 
--Please note that this is the same for UK so will need to run a check on this too.
--select count(1)   -- Counting cb_key_hh instead as account numbers will not be available for all the UK
--from rombaoad.nodupes 
--where cb_key_household NOT IN (select cb_key_household
--from rombaoad.nodupes
--where OUT_HHGoods_PW >0 )
--RESULT 1,364,270 (5.7%) that are not being counted and now we have the full count of the UK which is 24,892,028! Yahoo! 

--Would like to group or CASE variable called OUT_HHHousingEnergy_Index
ALTER TABLE     NODUPES     ADD EnergyIndex_Bands        VARCHAR(15)   DEFAULT NULL              ;

UPDATE nodupes
SET   EnergyIndex_Bands = case                when OUT_HHHousingEnergy_Index between 0 and 49 then '<50'
                                              when OUT_HHHousingEnergy_Index between 50 and 79 then '50-79'
                                              when OUT_HHHousingEnergy_Index between 80 and 120 then '80-120'
                                              when OUT_HHHousingEnergy_Index between 121 and 200 then '121-200'
                                              when OUT_HHHousingEnergy_Index > 200 then '>200'
                                              else 'Unknown'
                                              end


											 
--Number of people in the UK who have Energy Consumption Index values
SELECT COUNT (OUT_HHHousingEnergy_Index)
FROM nodupes
--RESULT UK - 23,527,758
--Note that totals don't add up to UK  households(24,892,208). Missing values (totalling 1,364,270) are NULLS and were not part of the calculation 

--Number of people in the UK consuming more than average energy (i.e. index>100)
SELECT COUNT (OUT_HHHousingEnergy_Index)
FROM nodupes
WHERE OUT_HHHousingEnergy_Index>100
--Result UK - 11,000,729

--Number of people in the Sky Customer Base who have Energy Consumption Index values
SELECT COUNT (OUT_HHHousingEnergy_Index)
FROM nodupes
WHERE sky_acct_holder=1
--Result Sky account holders  - 8,308,455
--Note that Sky Account Holder totals don't add up to 8,705,809. Missing values (totalling 397,354) are NULLS and were not part of the calculation 

--Number of Sky Account holders consuming more than average energy (i.e. index>100)
SELECT COUNT (OUT_HHHousingEnergy_Index)
FROM nodupes
WHERE sky_acct_holder=1 AND OUT_HHHousingEnergy_Index>100
--Result SKY account holders - 4,597,494

---------------------------------------------Delphi Scores-------------------------------
--------------------Create Local Variable (A02)
CREATE VARIABLE @today            date;
SET             @today =          '20120716';	-- note that this is different from the the base date of my NoDupes file - which is 22/06/2012 (referenced as A01)

--code_location_12 
--Add Delphi Scores (credit risk scores from Experian Consumerview) to a Base file called Delphi_UK
--Would like to make two household tables (Sky v. non-Sky account holders) and their delphi scores and then merge them later on to get a UK-wide table of max delphi scores. 
--Since there are multiple individuals in a household, we are interested only in the top individual scorer in that household (hence the "max" function).  
--The HH scores are mutually exclusive so that a household that has individuals that have a sky account and those that do not have a sky account will be pushed towards the Sky Account Holder table. 
--We are looking to merge the two tables so that we can compare the proportion of Sky households with good credit scores to the rest of the UK 

--Add Account Numbers (same way that we placed account numbers into the nodupes file)
SELECT DISTINCT account_number, currency_code,cb_key_household,cb_key_individual
INTO temp_Sky2
FROM sk_prod.cust_subs_hist
 WHERE subscription_sub_type IN ('DTV Primary Viewing')
   AND status_code IN ('AC','AB','PC')
   AND effective_from_dt <= @today
   AND effective_to_dt > @today
   AND EFFECTIVE_FROM_DT IS NOT NULL
   AND cb_key_household > 0             --UK Only
   AND cb_key_household IS NOT NULL
   AND account_number IS NOT NULL
   AND service_instance_id IS NOT NULL;
grant select on temp_Sky2 to vespa_group_low_security;
commit;
--completed with 10,020,506 rows

--Indexing
CREATE   HG INDEX idx10 ON temp_Sky2(account_number);
CREATE   HG INDEX idx11 ON temp_Sky2(cb_key_household);
CREATE   HG INDEX idx12 ON temp_Sky2(cb_key_individual);

--Create Table 1 "delphi_sky" - Delphi scores of sky account holders (want the top score only within the household)
SELECT a.cb_key_household, MAX(a.delphi_8_score_dfm8_as_nc), b.account_number
INTO delphi_sky
FROM sk_prod.EXPERIAN_CONSUMERVIEW AS a INNER JOIN temp_sky2 AS b
ON a.cb_key_individual=b.cb_key_individual
GROUP by a.cb_key_household, b.account_number
--RESULT 7,277,558

--Create Table 2 "delphi_no_sky"- Delphi scores of those households with no sky (only want the top score)
SELECT cb_key_household, MAX(delphi_8_score_dfm8_as_nc) 
INTO delphi_no_sky
FROM sk_prod.EXPERIAN_CONSUMERVIEW
WHERE cb_key_household NOT IN (SELECT DISTINCT(cb_key_household) 
                                FROM delphi_sky)
GROUP by cb_key_household
--RESULT 17,652,272

--Create Merge Table "delphi_uk"
--Need to merge the two tables by creating and populating the delphi_uk table with sky account holders...
select cb_key_household, expression, account_number
into delphi_uk
from delphi_sky

--UPDATE the Delphi_uk table with none account holders 
insert into delphi_uk (cb_key_household, expression, account_number)
select cb_key_household, expression, account_number
from delphi_no_sky
--At this stage the delphi_uk table is unique at the household level 
--Households that have a sky account number will have the sky household max score populated and those households that don't have an account but with the same household ID are not contained in the table.
--Please note that the Delphi score is actually the column called "expression"

--Updating the Delphi_UK file to include appropriate groupings
ALTER TABLE     Delphi_uk       ADD     Delphi_Bands                            VARCHAR(15)   DEFAULT NULL              ;

UPDATE delphi_uk
SET Delphi_Bands = CASE WHEN cast(expression as integer) is null THEN 'Unknown'
               WHEN expression = '-99999'               THEN 'Unknown'
               WHEN cast(expression as integer) < 440   THEN 'A) Very high'
               WHEN cast(expression as integer) < 520   THEN 'B) High'
               WHEN cast(expression as integer) < 600   THEN 'C) Above average'
               WHEN cast(expression as integer) < 680   THEN 'D) Below average'
               WHEN cast(expression as integer) < 760   THEN 'E) Low'
               ELSE                                          'F) Very low'
            END

--Result 24,929,830 rows

--Calculation - Distribution of delphi_bands in UK
SELECT delphi_bands, count(delphi_bands)
FROM delphi_uk
GROUP by delphi_bands
--Problem!!!! Bands show that the population is more towards the 760 plus scores which I would not have expected.

--Updating the Delphi_sky file to include appropriate groupings
ALTER TABLE     Delphi_sky     ADD Delphi_Bands                            VARCHAR(15)   DEFAULT NULL              ;

UPDATE delphi_sky
SET Delphi_Bands = CASE WHEN cast(expression as integer) is null THEN 'Unknown'
               WHEN expression = '-99999'               THEN 'Unknown'
               WHEN cast(expression as integer) < 440   THEN 'A) Very high'
               WHEN cast(expression as integer) < 520   THEN 'B) High'
               WHEN cast(expression as integer) < 600   THEN 'C) Above average'
               WHEN cast(expression as integer) < 680   THEN 'D) Below average'
               WHEN cast(expression as integer) < 760   THEN 'E) Low'
               ELSE                                          'F) Very low'
            END
--Result 7,277,558 rows

--Calculation - Distribution of delphi_bands in SKY
SELECT delphi_bands, count(delphi_bands)
FROM delphi_sky
GROUP by delphi_bands


--Updating the Delphi_no_sky file to include appropriate groupings
ALTER TABLE     Delphi_no_sky     ADD Delphi_Bands                            VARCHAR(15)   DEFAULT NULL              ;

UPDATE delphi_no_sky
SET Delphi_Bands = CASE WHEN cast(expression as integer) is null THEN 'Unknown'
               WHEN expression = '-99999'               THEN 'Unknown'
               WHEN cast(expression as integer) < 440   THEN 'A) Very high'
               WHEN cast(expression as integer) < 520   THEN 'B) High'
               WHEN cast(expression as integer) < 600   THEN 'C) Above average'
               WHEN cast(expression as integer) < 680   THEN 'D) Below average'
               WHEN cast(expression as integer) < 760   THEN 'E) Low'
               ELSE                                          'F) Very low'
            END


--Calculation - Distribution of delphi_bands in of households with no_sky 
SELECT delphi_bands, count(delphi_bands)
FROM delphi_no_sky
GROUP by delphi_bands
--Result 17,652,272 updated rows

----------------------------------------------------- ABC1 Social Grades -----------------
--code_location_13 
--Taken from a different date stamp than rest of nodupes - current date stamp is Experian Consumerview 18/07/2012 while previous one associated with nodupes was 22/06/2012)
--Taken from Jitesh. We are using Mosaic UK HH type to identify those who are in ABC1 social grades and placing them in nodupes

--Add new columns to place mosaic type and ABC1 flag
ALTER  TABLE       nodupes     ADD  h_mosaic_uk_2009_type    VARCHAR (2)    Default NULL;
ALTER  TABLE       nodupes     ADD  ABC1_Flag                VARCHAR (10)    Default NULL;

UPDATE nodupes
SET     a.h_mosaic_uk_2009_type=b.h_mosaic_uk_2009_type,  
        a.ABC1_Flag=(CASE   b.h_mosaic_uk_2009_type         WHEN    '01'  THEN    'Y'
                                                            WHEN    '02'  THEN    'Y'
                                                            WHEN    '03'  THEN    'Y'
                                                            WHEN    '04'  THEN    'Y'
                                                            WHEN    '05'  THEN    'Y'
                                                            WHEN    '06'  THEN    'Y'
                                                            WHEN    '07'  THEN    'Y'
                                                            WHEN    '08'  THEN    'Y'
                                                            WHEN    '09'  THEN    'Y'
                                                            WHEN    '10'  THEN    'Y'
                                                            WHEN    '11'  THEN    'Y'
                                                            WHEN    '15'  THEN    'Y'
                                                            WHEN    '20'  THEN    'Y'
                                                            WHEN    '22'  THEN    'Y'
                                                            WHEN    '29'  THEN    'Y'
                                                            WHEN    '30'  THEN    'Y'
                                                            WHEN    '31'  THEN    'Y'
                                                            WHEN    '33'  THEN    'Y'
                                                            WHEN    '61'  THEN    'Y'
                                                            WHEN    '62'  THEN    'Y'
                                                            WHEN    '63'  THEN    'Y'
                                                            WHEN    '65'  THEN    'Y'
                                                            WHEN    '66'  THEN    'Y'
                                                            ELSE                  'N'
                                                            END)
FROM  nodupes AS a INNER JOIN sk_prod.EXPERIAN_CONSUMERVIEW AS b
ON a.cb_address_postcode=b.cb_address_postcode
--Calculations for Sky Account Holders 
--Number of Sky_Account Holders in the ABC1 social grade = 2,325,273
--Number of Sky_Account Holders NOT in the ABC1 social grade=6,380,385
--TOTAL Sky Account Holders who have been assigned a Social Grade is 8,705,658 calculated from the script below.
--% of people in the Sky Base who are ABC1 is therefore 26.7%
--100% match rate (only 151 postcodes not matching)

--SELECT COUNT (*)
--FROM nodupes
--WHERE sky_acct_holder=1 AND (abc1_flag='N' or abc1_flag='Y') 

--But our total active is 8,705,809 hence we are missing 151 account holders calculated from the script below. May be postcodes that were typos or most likely old postcodes.
--select cb_address_postcode, cb_row_id
--from nodupes
--where sky_acct_holder=1 and cb_address_postcode not in (
--SELECT cb_address_postcode
--FROM nodupes
--WHERE sky_acct_holder=1 AND (abc1_flag='N' or abc1_flag='Y') )

--Calculations for the universe ABC1 HH 
--24,892,028 	-TOTAl UK HH
--6,124,649 - UK ABC1 
--18,766,071-	UK non-ABC1
--1,308		- Residual due to postcodes not matching
--% of people in the UK who are ABC1 is 24.6%
--99.99% match rate on cb_address_postcode

----------------------------------------------- Create CubeTables for Excel ------------------------
--code_location_14
--We are creating 3 tables (Aggregate_Table1, Aggregate_Table2, Aggregate_Table3) as there is too much if just one table. 
--What links all of them are the flags so we can profile and measure the difference between WORLD (UK), Sky Customer, and VESPA panelists
--Aggregate_Table1 should answer all the questions asked by Tom Khabaza though for this project but the other tables are in case he needs more information.

--Creating Aggregate_Table1			
SELECT COUNT (*) AS counts
        World,
,Sky_Acct_Holder
,new_accounts_flag
,vespa
,hh_lifestage
,hh_composition
,hh_income_bands
,H_Affluence
,Demographic
,cqm_group
,barb_desc_ITV


INTO    Aggregate_Table1
FROM    nodupes
GROUP by World,
,Sky_Acct_Holder
,new_accounts_flag
,vespa
,hh_lifestage
,hh_composition
,hh_income_bands
,H_Affluence
,Demographic
,cqm_group
,barb_desc_ITV


--Creating Aggregate_Table2			           
SELECT COUNT (*) AS counts
            ,World
,Sky_Acct_Holder
,new_accounts_flag
,vespa
,ILU_HHAffordabilityRank
,ILU_HHEquivIncome_Index
,HHTotalOutgoings_Band
,HHNetIncome_Band
,HHDiscretionaryIncome_Band
,HHEquivIncome
INTO    Aggregate_Table2
FROM    nodupes
GROUP by     World
,Sky_Acct_Holder
,new_accounts_flag
,vespa
,ILU_HHAffordabilityRank
,ILU_HHEquivIncome_Index
,HHTotalOutgoings_Band
,HHNetIncome_Band
,HHDiscretionaryIncome_Band
,HHEquivIncome

grant select on Aggregate_Table2 to vespa_group_low_security;

			

        
----------------------------------------END of SQL Scripts for QA ----
--Rest is for my own checks and reminders

		
--Creating Aggregate_Table3			
SELECT COUNT (*) AS counts
        World,
,Sky_Acct_Holder
,new_accounts_flag
,vespa
,OUT_HHFinance_PW
,OUT_HHGoods_PW
,OUT_HHMotorTotal_PW
,OUT_HHClothing_PW
,OUT_HHHousingEnergy_Index
,OUT_HHSupermarket_PW
,OUT_HHTotal_PW

INTO    Aggregate_Table3
FROM    nodupes
GROUP by World,
,Sky_Acct_Holder
,new_accounts_flag
,vespa
,OUT_HHFinance_PW
,OUT_HHGoods_PW
,OUT_HHMotorTotal_PW
,OUT_HHClothing_PW
,OUT_HHHousingEnergy_Index
,OUT_HHSupermarket_PW
,OUT_HHTotal_PW


--CALCULATIONS for Affluence
--UK distribution
SELECT Distinct (H_AFFLUENCE), COUNT (H_AFFLUENCE)-- UK distribution 
FROM nodupes 
GROUP BY H_AFFLUENCE
ORDER by H_AFFLUENCE

--Sky Account Holders Distribution
SELECT Distinct (H_AFFLUENCE), COUNT (H_AFFLUENCE) -- Sky distribution
FROM nodupes 
WHERE sky_acct_holder=1
GROUP BY H_AFFLUENCE
ORDER by H_AFFLUENCE

--New accounts distribution
SELECT Distinct (H_AFFLUENCE), COUNT (H_AFFLUENCE) -- New accounts distribution
FROM nodupes 
WHERE new_accounts_flag=1
GROUP BY H_AFFLUENCE
ORDER by H_AFFLUENCE

-- expend per week out of sky customers...
SELECT round(AVG(OUT_HHTotal_PW),2)
FROM NODUPES
Where Sky_Acct_Holder=1 and ILU_HHDiscretionaryIncome_Band  is not NULL;

SELECT round(AVG(OUT_HHTotal_PW),2)
FROM NODUPES
Where ILU_HHDiscretionaryIncome_Band  is not NULL;

--World
SELECT round(AVG(OUT_HHGoods_PW),2)
FROM NODUPES
Where World=1 and OUT_HHGoods_PW is not NULL;

--Sky
SELECT round(AVG(OUT_HHGoods_PW),2)
FROM NODUPES
Where Sky_Acct_Holder=1 and OUT_HHGoods_PW is not NULL;

--Vespa
SELECT round(AVG(OUT_HHGoods_PW),2)
FROM NODUPES
Where vespa=1 and OUT_HHGoods_PW is not NULL;

--NEW
SELECT round(AVG(OUT_HHMotorTotal_PW),2)
FROM NODUPES
Where new_accounts_flag=1 and OUT_HHMotorTotal_PW is not NULL;

---------------------------------------------------Calculations for HH Composition (UK and Sky)
--2,054,380 UK Homesharers
SELECT COUNT(household_composition)
FROM nodupes
WHERE household_composition IN ('06','07','08','11','U')

--5,203,676 UK Family + other adults
SELECT COUNT(household_composition)
FROM nodupes
WHERE household_composition IN ('01','02','09','10')

--9,387,831 UK Singles
SELECT COUNT(household_composition)
FROM nodupes
WHERE household_composition IN ('04','05')

--6,201,680 UK Families
SELECT COUNT(household_composition)
FROM nodupes
WHERE household_composition IN ('00')

--2,044,461 UK Pseudo Family
SELECT COUNT(household_composition)
FROM nodupes
WHERE household_composition IN ('03')

----Sky account holders

--655,079 SKY Homesharers
SELECT COUNT(household_composition)
FROM nodupes
WHERE household_composition IN ('06','07','08','11','U') AND sky_acct_holder=1

--9,387,831 UK Singles
SELECT COUNT(household_composition)
FROM nodupes
WHERE household_composition IN ('04','05')

--6,201,680 UK Families
SELECT COUNT(household_composition)
FROM nodupes
WHERE household_composition IN ('00')

--2,044,461 UK Pseudo Family
SELECT COUNT(household_composition)
FROM nodupes
WHERE household_composition IN ('03')

----Sky account holders

--655,079 SKY Homesharers
SELECT COUNT(household_composition)
FROM nodupes
WHERE household_composition IN ('06','07','08','11','U') AND sky_acct_holder=1

--2,336,900 Sky Family + other adults
SELECT COUNT(household_composition)
FROM nodupes
WHERE household_composition IN ('01','02','09','10') AND sky_acct_holder=1

--2,335,787 Sky Singles
SELECT COUNT(household_composition)
FROM nodupes
WHERE household_composition IN ('04','05') AND sky_acct_holder=1

--2,588,725 Sky Families
SELECT COUNT(household_composition)
FROM nodupes
WHERE household_composition IN ('00') AND sky_acct_holder=1

--789,318 Sky Pseudo Family
SELECT COUNT(household_composition)
FROM nodupes
WHERE household_composition IN ('03') AND sky_acct_holder=1

----New account holders

--116,736 NEW SKY Homesharers
SELECT COUNT(household_composition)
FROM nodupes
WHERE household_composition IN ('06','07','08','11','U') AND new_accounts_flag=1

--162,453 NEW Sky Family + other adults
SELECT COUNT(household_composition)
FROM nodupes
WHERE household_composition IN ('01','02','09','10') AND new_accounts_flag=1

--2,336,900 Sky Family + other adults
SELECT COUNT(household_composition)
FROM nodupes
WHERE household_composition IN ('01','02','09','10') AND sky_acct_holder=1

--2,335,787 Sky Singles
SELECT COUNT(household_composition)
FROM nodupes
WHERE household_composition IN ('04','05') AND sky_acct_holder=1

--2,588,725 Sky Families
SELECT COUNT(household_composition)
FROM nodupes
WHERE household_composition IN ('00') AND sky_acct_holder=1

--789,318 Sky Pseudo Family
SELECT COUNT(household_composition)
FROM nodupes
WHERE household_composition IN ('03') AND sky_acct_holder=1
Order by demographic

SELECT demographic, COUNT (demographic)	--Sky Account Holders
from nodupes
Where Sky_acct_holder=1
GROUP by demographic
Order by demographic

SELECT demographic, COUNT (demographic)	--New Account Holders (Total new account holders - 840,541 - matches with classfications total)
from nodupes
Where new_accounts_flag=1
GROUP by demographic
Order by demographic

-----------------------Calculations for Income Distributions

--UK distribution for Income Bands - total return is 24,892,028 with 171,420 HH's shown as "missing"
SELECT HH_income_bands, COUNT (HH_income_bands)
from nodupes
GROUP by HH_income_bands 
Order by HH_income_bands

--Alpha Territory Only - total return is 859,832 with 0 "missing"
SELECT HH_income_bands, COUNT (HH_income_bands)
from nodupes
WHERE demographic = 'a) Alpha Territory'
GROUP by HH_income_bands 
Order by HH_income_bands

--Professional Rewards onlyfor sure - total return is 2,031,442 with 0 "missing"
SELECT HH_income_bands, COUNT (HH_income_bands)
from nodupes
WHERE demographic = 'b) Professional Rewards'
GROUP by HH_income_bands 
Order by HH_income_bands

--Alpha Territory and Professional Rewards - total return 2,891,274 with 0 "missing"
SELECT HH_income_bands, COUNT (HH_income_bands)
from nodupes
WHERE demographic IN ('a) Alpha Territory','b) Professional Rewards')
GROUP by HH_income_bands 
Order by HH_income_bands

--Sky's Alpha Territory and Professional Rewards Group combined - total return is  1,224,888 with 0 "missing"
SELECT HH_income_bands, COUNT (HH_income_bands)
from nodupes
WHERE demographic IN ('a) Alpha Territory','b) Professional Rewards') and Sky_Acct_Holder=1
GROUP by HH_income_bands 
Order by HH_income_bands

--Income Distribution for  Households aged 26-65
SELECT HH_income_bands, COUNT (HH_income_bands)
from nodupes
WHERE household_age BETWEEN '1' AND '4'
GROUP by HH_income_bands 
Order by HH_income_bands




-------------------------Calculations for ABC1
----ABC1 Social Grades
SELECT COUNT(*)     -- UK with ABC1 Social grade 24,890,720
FROM nodupes
WHERE abc1_flag='N' or abc1_flag='Y' 

SELECT COUNT (*)  -- UK -  non-ABC1 social grades 18,766,071
FROM nodupes
WHERE abc1_flag='N'

SELECT COUNT(*)     -- UK with ABC1 Social grade 6,124,649
FROM nodupes
Where abc1_flag='Y'

SELECT COUNT (*)  -- Sky Account Holders - 6,380,385 
FROM nodupes
WHERE sky_acct_holder=1 AND abc1_flag='N'

SELECT COUNT (*) -- Total Sky Accounts with ABC1 yes or no flags 8,705,658 (missing 151 records as our total account holders s 8,705,809)
FROM nodupes
WHERE sky_acct_holder=1 AND (abc1_flag='N' or abc1_flag='Y') 

SELECT COUNT (*)  -- New Accounts with ABC1 social grade - 164,266
FROM nodupes
WHERE new_accounts_flag=1 AND abc1_flag='Y'

SELECT COUNT (*)  -- New Accounts with non-ABC1 social grade - 676,257
FROM nodupes
WHERE new_accounts_flag=1 AND abc1_flag='N'

SELECT COUNT (*) -- Total New Accounts with ABC1 yes or no flags 840,523 (missing 18 records as our New Account Holders is 840,541)
FROM nodupes
WHERE new_accounts_flag=1 AND (abc1_flag='N' or abc1_flag='Y') 

----New account holders

--116,736 NEW SKY Homesharers
SELECT COUNT(household_composition)
FROM nodupes
WHERE household_composition IN ('06','07','08','11','U') AND new_accounts_flag=1

--162,453 NEW Sky Family + other adults
SELECT COUNT(household_composition)
FROM nodupes
WHERE household_composition IN ('01','02','09','10') AND new_accounts_flag=1

--316,016 NEW Sky Singles
SELECT COUNT(household_composition)
FROM nodupes
WHERE household_composition IN ('04','05') AND new_accounts_flag=1

--144,430 NEW Sky Families
SELECT COUNT(household_composition)
FROM nodupes
WHERE household_composition IN ('00') AND new_accounts_flag=1

--100,906 NEW Sky Pseudo Family
SELECT COUNT(household_composition)
FROM nodupes
WHERE household_composition IN ('03') AND new_accounts_flag=1

----------------------------Calculations for Mosaic Groups
SELECT demographic, COUNT (demographic)	--UK 
from nodupes
GROUP by demographic
Order by demographic

SELECT demographic, COUNT (demographic)	--Sky Account Holders
from nodupes
Where Sky_acct_holder=1
GROUP by demographic
Order by demographic

SELECT demographic, COUNT (demographic)	--New Account Holders (Total new account holders - 840,541 - matches with classfications total)
from nodupes
Where new_accounts_flag=1
GROUP by demographic
Order by demographic

-----------------------Calculations for Income Distributions

--UK distribution for Income Bands - total return is 24,892,028 with 171,420 HH's shown as "missing"
SELECT HH_income_bands, COUNT (HH_income_bands)
from nodupes
GROUP by HH_income_bands 
Order by HH_income_bands

--Alpha Territory Only - total return is 859,832 with 0 "missing"
SELECT HH_income_bands, COUNT (HH_income_bands)
from nodupes
WHERE demographic = 'a) Alpha Territory'
GROUP by HH_income_bands 
Order by HH_income_bands

--Professional Rewards onlyfor sure - total return is 2,031,442 with 0 "missing"
SELECT HH_income_bands, COUNT (HH_income_bands)
from nodupes
WHERE demographic = 'b) Professional Rewards'
GROUP by HH_income_bands 
Order by HH_income_bands

--Alpha Territory and Professional Rewards - total return 2,891,274 with 0 "missing"
SELECT HH_income_bands, COUNT (HH_income_bands)
from nodupes
WHERE demographic IN ('a) Alpha Territory','b) Professional Rewards')
GROUP by HH_income_bands 
Order by HH_income_bands

--Sky's Alpha Territory and Professional Rewards Group combined - total return is  1,224,888 with 0 "missing"
SELECT HH_income_bands, COUNT (HH_income_bands)
from nodupes
WHERE demographic IN ('a) Alpha Territory','b) Professional Rewards') and Sky_Acct_Holder=1
GROUP by HH_income_bands 
Order by HH_income_bands

--Income Distribution for  Households aged 26-65
SELECT HH_income_bands, COUNT (HH_income_bands)
from nodupes
WHERE household_age BETWEEN '1' AND '4'
GROUP by HH_income_bands 
Order by HH_income_bands




-------------------------Calculations for ABC1
----ABC1 Social Grades
SELECT COUNT(*)     -- UK with ABC1 Social grade 24,890,720
FROM nodupes
WHERE abc1_flag='N' or abc1_flag='Y' 

SELECT COUNT (*)  -- UK -  non-ABC1 social grades 18,766,071
FROM nodupes
WHERE abc1_flag='N'

SELECT COUNT(*)     -- UK with ABC1 Social grade 6,124,649
FROM nodupes
Where abc1_flag='Y'

SELECT COUNT (*)  -- Sky Account Holders - 6,380,385 
FROM nodupes
WHERE sky_acct_holder=1 AND abc1_flag='N'

SELECT COUNT (*) -- Total Sky Accounts with ABC1 yes or no flags 8,705,658 (missing 151 records as our total account holders s 8,705,809)
FROM nodupes
WHERE sky_acct_holder=1 AND (abc1_flag='N' or abc1_flag='Y') 

SELECT COUNT (*)  -- New Accounts with ABC1 social grade - 164,266
FROM nodupes
WHERE new_accounts_flag=1 AND abc1_flag='Y'

SELECT COUNT (*)  -- New Accounts with non-ABC1 social grade - 676,257
FROM nodupes
WHERE new_accounts_flag=1 AND abc1_flag='N'

SELECT COUNT (*) -- Total New Accounts with ABC1 yes or no flags 840,523 (missing 18 records as our New Account Holders is 840,541)
FROM nodupes
WHERE new_accounts_flag=1 AND (abc1_flag='N' or abc1_flag='Y') 

----------------------------------------Just some checks for myself to make sure things are being populated will delete later on-------------------------
SELECT Top 100 *
from NoDupes;

SELECT COUNT (*)
FROM NoDupes;

SELECT 
from sk_prod.BARB_TV_REGIONS;

--Check on data dupes---
SELECT COUNT (DISTINCT(cb_key_household))
FROM sk_prod.EXPERIAN_CONSUMERVIEW
where cb_data_date = '2012-06-22';

SELECT COUNT (*)
from nodupes
WHERE account_number is NOT NULL;   -- Counts what is the population number for our table at the moment

SELECT Panel_id_vespa, COUNT (*)    -- Counts how many people are in each panel
from NoDupes
GROUP by Panel_id_vespa;

-------Check for duplicates
SELECT COUNT(*), COUNT(DISTINCT(cb_key_household))
FROM NoDupes
WHERE cb_key_household is NULL

SELECT COUNT (DISTINCT(cb_key_household))
FROM sk_prod.EXPERIAN_CONSUMERVIEW
where cb_data_date = '2012-06-22';

select top 10 *
FROM sk_prod.EXPERIAN_CONSUMERVIEW;

SELECT *  FROM SYS.syscolumns
WHERE CREATOR = 'sk_prod'
and tname = 'EXPERIAN_CONSUMERVIEW'

select count(*), count(distinct(account_number))
from nodupes

----Gets column names list "SYS.columns" is standard. Schema "rombaoad" is the creator and file name is "nodupes"
SELECT *  
FROM SYS.syscolumns
WHERE CREATOR = 'rombaoad'
and tname = 'nodupes'


--CHECK Aggregate_Table1
SELECT Top 1000 *               -- There are many niche combinations with low counts or even "1" as count. This code is to check the biggest numbers
FROM Aggregate_Table1
ORDER by COUNTS desc;           --Row Count of Aggregate_Table_1 = 127,547 
--CHECK Aggregate_Table2
SELECT Top 100 *
FROM Aggregate_Table1;

SELECT COUNT (*)        --Row count of Aggregate_Table2 = 511k   
FROM Aggregate_Table2;  

--CHECK Aggregate_Table3
SELECT Top 100 *
FROM Aggregate_Table3;

SELECT COUNT (*)        --Row count of Aggregate_Table2 = 4k   
FROM Aggregate_Table3;  

------FOR ROBERT-----
Rob, I am doing a profiling exercise on Sky Customer Base v. UK. 
I am having problems matching sky account holders from my base file called NODUPES (which holds records for all of CONSUMERVIEW and includes flags on whether they are a Sky account holder and whether they are VESPA) to Sky Account holders before they got merged into nodupes. 
In addtiong, the Sky account numbers also change to 8.5 million by the time they reach my pivot table despite the fact that in Nodupes they were 8.7 million. I just need to be able to find out what's happening to all these people. Sarah asked me to put in a note to help clarify the situation on my output. Something about matchrates and also the fact consumerview has no ROI. So I am guessing she just wants some figures so that they all add up. 
Please note that CB key household is the merging point.


select sky.account_number
into #temp_acc
from #temp_Sky sky inner join nodupes base
on Sky.cb_key_household = base.cb_key_household; - 8.8 million

select count (distinct (account_number)), count (account_number)
FROM nodupes  -- 8.7 million 



***for use to check after sky accounts have been imported 
select Sky_Acct_Holder, case when account_number is null then 'yah' else 'neh' end as on_base
    ,count(1) from rombaoad.nodupes
group by Sky_Acct_Holder, on_base
order by Sky_Acct_Holder, on_base;


Affordability Income and Spend

SELECT COUNT(*), COUNT(DISTINCT(cb_key_household))
FROM sk_prod.ilu_affordability;

CREATE VARIABLE
@march312012
@april012011

------get the rest of the VESPA accounts
select distinct account_number, coalesce(b.new_panel, a.panel) as Panel
Into #temp_fullvespa
from stafforr.V068_ad_hoc_box_reporting_raw as a left join stafforr.V068_transfer_requests as b
on a.subscriber_id = b.subscriber_id
where coalesce(b.new_panel, a.panel) = 12
