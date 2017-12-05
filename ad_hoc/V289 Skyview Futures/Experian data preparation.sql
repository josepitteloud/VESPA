---------------------------------------------------------------------------------------------------------
---------------------------------------------------------------------------------------------------------
---------------------           00--        PIV Grouped Segments script -------------------------------------
---------------------------------------------------------------------------------------------------------
IF object_id('V289_M08_SKY_HH_composition') IS NOT NULL DROP TABLE V289_M08_SKY_HH_composition

CREATE TABLE V289_M08_SKY_HH_composition (
      row_id                INT         IDENTITY
    , account_number        VARCHAR(20) NOT NULL
    , cb_key_household      BIGINT      NOT NULL
    , exp_cb_key_db_person  BIGINT      -- Unique person identifying in Experian and enables join to sk_prod.PLAYPEN_CONSUMERVIEW_PERSON_AND_HOUSEHOLD
    , cb_key_individual     BIGINT
    , cb_key_db_person      BIGINT
    , cb_address_line_1     VARCHAR (200)
    , HH_person_number      TINYINT
    , person_gender         CHAR (1)                -- Used for scaling. If we had a demographics_id lookup table then not needed
    , person_age            TINYINT
    , person_ageband        varchar(10)             -- Used for scaling. If we had a demographics_id lookup table then not needed
    , exp_person_head       TINYINT                 -- Head of hhd as defined by Experian. Usually 2, a male & a female
    , person_income         NUMERIC
    , person_head           char(1)     DEFAULT '0'   -- For Skyview define 1 and only 1 person to be head of household
    , household_size        TINYINT
    , demographic_ID        TINYINT
    , Updated_On            DATETIME    DEFAULT TIMESTAMP
    , Updated_By            VARCHAR(30) DEFAULT user_name())

COMMIT

IF object_id('V289_M08_SKY_HH_view') IS NOT NULL DROP TABLE V289_M08_SKY_HH_view
CREATE TABLE V289_M08_SKY_HH_view    (
    account_number          VARCHAR(20) NOT NULL
    , cb_key_household      BIGINT      NOT NULL
    , cb_address_line_1     VARCHAR (200)
    , HH_composition        TINYINT
    , Children_count        TINYINT     DEFAULT 0
    , non_matching_flag     BIT         DEFAULT 0
    , edited_add_flag       BIT         DEFAULT 0
    , Updated_On            DATETIME    DEFAULT TIMESTAMP
    , Updated_By            VARCHAR(30) DEFAULT user_name())


--MESSAGE '00 ok' type action  to CLIENT
---------------------------------------------------------------------------------------------------------
---------------------------------------------------------------------------------------------------------
---------------------           B01 --      TABLE POPULATION        -------------------------------------
---------------------------------------------------------------------------------------------------------
IF object_id('V289_M08_SKY_HH_view') IS NOT NULL  TRUNCATE TABLE V289_M08_SKY_HH_view

---------------------           B02 --      Account extraction from SAV ---------------------------------

INSERT INTO V289_M08_SKY_HH_view (account_number, cb_key_household, cb_address_line_1)
SELECT DISTINCT
      sav.account_number
    , sav.cb_key_household
    , sav.cb_address_line_1
FROM  sk_prod.CUST_SINGLE_ACCOUNT_VIEW as sav
--JOIN sk_prod.vespa_dp_prog_viewed_201309  as vw       ON sav.account_number = vw.account_number -----     REPLACE by Vespa_single_box_view in the production stream
WHERE sav.cb_key_household > 0
AND   sav.cb_key_household IS NOT NULL
AND   sav.account_number IS NOT NULL
-- AND   sav.cust_active_dtv = 1 -- JT: if we are running for historic time periods might not be right? Scaling 3.0 code uses sk_prod.cust_subs_hist to define skybase for day being run

COMMIT

CREATE HG INDEX idhh ON V289_M08_SKY_HH_view(cb_key_household)
CREATE HG INDEX idac ON V289_M08_SKY_HH_view(account_number)
CREATE HG INDEX idal ON V289_M08_SKY_HH_view(cb_address_line_1)

COMMIT

---------------------           B03 --      Experian HH Info Extraction (1st round - Only hh_key and address line matching accounts) ---------------------------------
SELECT account_number
    , vh.cb_key_household
    , vh.cb_address_line_1
    , COUNT(DISTINCT ex.cb_key_db_person) + MAX(CAST(h_number_of_children_in_household_2011 as INT))  AS     HH_composition
    , Children_count        = MAX(CAST(h_number_of_children_in_household_2011 as INT))
INTO #t1
FROM V289_M08_SKY_HH_view AS vh
JOIN sk_prod.EXPERIAN_CONSUMERVIEW as ex ON ex.cb_key_household = vh.cb_key_household AND ex.cb_address_line_1 = vh.cb_address_line_1
GROUP BY account_number
    , vh.cb_key_household
    , vh.cb_address_line_1
COMMIT

CREATE HG INDEX idhh ON #t1(cb_key_household)
CREATE HG INDEX idac ON #t1(account_number)
CREATE HG INDEX idal ON #t1(cb_address_line_1)

COMMIT
---------------------           B04 --      Table Update                ---------------------------------
UPDATE V289_M08_SKY_HH_view
SET     a.Children_count        = b.Children_count
    ,   a.HH_composition        = b.HH_composition
    ,   a.non_matching_flag     = 1
FROM V289_M08_SKY_HH_view as a
JOIN #t1 as b ON a.account_number = b.account_number  AND a.cb_key_household = b.cb_key_household AND a.cb_address_line_1 = b.cb_address_line_1

COMMIT

--MESSAGE 'B04 ok' type action  to CLIENT
--------------------------------------------------------------------------------------------------------------

---------------------           B05 --      Experian HH Info Extraction (2nd round - Non-matching address line accounts AND hh > 10 people) ---------------------
SELECT vh.account_number
    , vh.cb_key_household
    , vh.cb_address_line_1
    , ex.cb_address_line_1 AS linex
    , COUNT(DISTINCT ex.cb_key_db_person) + MAX(CAST(h_number_of_children_in_household_2011 as INT))  AS  HH_composition
    , Children_count        = MAX(CAST(h_number_of_children_in_household_2011 as INT))
    , RANK () OVER (PARTITION BY vh.cb_key_household ORDER by HH_composition DESC) as rank_1
INTO #t2
FROM V289_M08_SKY_HH_view as vh
JOIN sk_prod.EXPERIAN_CONSUMERVIEW as ex ON ex.cb_key_household = vh.cb_key_household
WHERE (vh.non_matching_flag = 0)
GROUP BY vh.account_number
    , vh.cb_key_household
    , vh.cb_address_line_1
    , linex
HAVING HH_composition <= 10

COMMIT

CREATE HG INDEX idhh ON #t2(cb_key_household)
CREATE HG INDEX idac ON #t2(account_number)
CREATE HG INDEX idal ON #t2(cb_address_line_1)
COMMIT
---------------------           B06 --      Table Update                ---------------------------------
UPDATE V289_M08_SKY_HH_view
SET     a.Children_count        = b.Children_count
    ,   a.HH_composition        = b.HH_composition
    ,   a.cb_address_line_1     = b.linex
    ,   a.non_matching_flag     = 1
    ,   a.edited_add_flag       = 1
FROM V289_M08_SKY_HH_view as a
JOIN #t2 as b ON a.account_number = b.account_number  AND a.cb_key_household = b.cb_key_household AND a.cb_address_line_1 = b.cb_address_line_1 and rank_1 = 1

COMMIT
--MESSAGE 'B06 ok' type action  to CLIENT
---------------------           B07 --      Experian HH Info Extraction (3nd round - Non-matching address line accounts A) ---------------------
SELECT vh.account_number
    , vh.cb_key_household
    , vh.cb_address_line_1
    , ex.cb_address_line_1 AS linex
    , COUNT(DISTINCT ex.cb_key_db_person) + MAX(CAST(h_number_of_children_in_household_2011 as INT))  AS  HH_composition
    , Children_count        = MAX(CAST(h_number_of_children_in_household_2011 as INT))
    , RANK () OVER (PARTITION BY vh.cb_key_household ORDER by HH_composition ASC) as rank_1
INTO #t3
FROM V289_M08_SKY_HH_view as vh
JOIN sk_prod.EXPERIAN_CONSUMERVIEW as ex ON ex.cb_key_household = vh.cb_key_household
WHERE (vh.non_matching_flag = 0)
GROUP BY vh.account_number
    , vh.cb_key_household
    , vh.cb_address_line_1
    , linex


COMMIT

CREATE HG INDEX idhh ON #t3(cb_key_household)
CREATE HG INDEX idac ON #t3(account_number)
CREATE HG INDEX idal ON #t3(cb_address_line_1)
COMMIT
---------------------           B08 --      Table Update                ---------------------------------
UPDATE V289_M08_SKY_HH_view
SET     a.Children_count        = b.Children_count
    ,   a.HH_composition        = b.HH_composition
    ,   a.cb_address_line_1     = b.linex
    ,   a.non_matching_flag     = 1
    ,   a.edited_add_flag       = 1
FROM V289_M08_SKY_HH_view as a
JOIN #t3 as b ON a.account_number = b.account_number  AND a.cb_key_household = b.cb_key_household AND a.cb_address_line_1 = b.cb_address_line_1 and rank_1 = 1

COMMIT

--MESSAGE 'B08 ok' type action  to CLIENT
---------------------------------------------------------------------------------------------------------
---------------------           C01 --      Individual TABLE POPULATION ---------------------------------
---------------------------------------------------------------------------------------------------------
IF object_id('V289_M08_SKY_HH_composition') IS NOT NULL  TRUNCATE TABLE V289_M08_SKY_HH_composition
INSERT INTO V289_M08_SKY_HH_composition (account_number, cb_key_household, exp_cb_key_db_person, cb_address_line_1
                                        , cb_key_db_person, person_age, person_ageband, HH_person_number, person_gender, person_income, demographic_ID)
SELECT
      vh.account_number
    , vh.cb_key_household
    , ex.exp_cb_key_db_person
    , vh.cb_address_line_1
    , ex.cb_key_db_person
    , person_age                = ex.p_actual_age
    , person_ageband            = CASE WHEN person_age <= 19 then '0-19'
                                       WHEN person_age BETWEEN 20 AND 24 then '20-24'
                                       WHEN person_age BETWEEN 25 AND 34 then '25-34'
                                       WHEN person_age BETWEEN 35 AND 44 then '35-44'
                                       WHEN person_age BETWEEN 45 AND 64 then '45-64'
                                       WHEN person_age >= 65 then '65+'
                                  END
    , HH_person_number          = RANK () OVER(PARTITION BY  vh.account_number ORDER BY person_age, p_gender, ex.cb_key_db_person)
    , person_gender             = CASE  WHEN ex.p_gender = '0' THEN 'M'
                                        WHEN ex.p_gender = '1' THEN 'F'
                                        ELSE 'U' END
    , person_income             = ex.p_personal_income_value
    , demographic_ID    = CASE  WHEN p_gender = '0' AND p_actual_age <= 19                      THEN 7
                                WHEN p_gender = '0' AND p_actual_age BETWEEN 20 AND 24          THEN 6
                                WHEN p_gender = '0' AND p_actual_age BETWEEN 25 AND 34          THEN 5
                                WHEN p_gender = '0' AND p_actual_age BETWEEN 35 AND 44          THEN 4
                                WHEN p_gender = '0' AND p_actual_age BETWEEN 45 AND 64          THEN 3
                                WHEN p_gender = '0' AND p_actual_age >= 65                      THEN 2
                                ---------- FEMALES
                                WHEN p_gender = '1' AND p_actual_age <= 19                      THEN 14
                                WHEN p_gender = '1' AND p_actual_age BETWEEN 20 AND 24          THEN 13
                                WHEN p_gender = '1' AND p_actual_age BETWEEN 25 AND 34          THEN 12
                                WHEN p_gender = '1' AND p_actual_age BETWEEN 35 AND 44          THEN 11
                                WHEN p_gender = '1' AND p_actual_age BETWEEN 45 AND 64          THEN 10
                                WHEN p_gender = '1' AND p_actual_age >= 65                      THEN 9
                                ---------- UNDEFINED GENDER
                                WHEN p_gender = 'U' AND p_actual_age <= 19                      THEN 15
                                WHEN p_gender = 'U' AND p_actual_age BETWEEN 20 AND 24          THEN 16
                                WHEN p_gender = 'U' AND p_actual_age BETWEEN 25 AND 34          THEN 17
                                WHEN p_gender = 'U' AND p_actual_age BETWEEN 35 AND 44          THEN 18
                                WHEN p_gender = 'U' AND p_actual_age BETWEEN 45 AND 64          THEN 19
                                WHEN p_gender = 'U' AND p_actual_age >= 65                      THEN 20
                                ---------- UNDEFINED AGE
                                WHEN p_gender = '1' AND p_actual_age IS NULL                    THEN 21
                                WHEN p_gender = '0' AND p_actual_age IS NULL                    THEN 22
                                ---------- UNDIFINED ALL
                                WHEN p_gender = 'U' AND p_actual_age IS NULL                    THEN 23
                                ELSE 0 END
FROM V289_M08_SKY_HH_view AS vh
JOIN sk_prod.EXPERIAN_CONSUMERVIEW as ex ON ex.cb_key_household = vh.cb_key_household AND ex.cb_address_line_1 = vh.cb_address_line_1

COMMIT

CREATE HG INDEX idhh ON V289_M08_SKY_HH_composition(cb_key_household)
CREATE HG INDEX idac ON V289_M08_SKY_HH_composition(account_number)
CREATE HG INDEX idal ON V289_M08_SKY_HH_composition(cb_address_line_1)
CREATE HG INDEX idab ON V289_M08_SKY_HH_composition(cb_key_db_person)
COMMIT


---------------------------------------------------------------------------------------------------------
---------------------           C02 --      Add Head of Household       ---------------------------------
---------------------------------------------------------------------------------------------------------

-- Get Experian Head of Household
UPDATE  V289_M08_SKY_HH_composition s
SET     exp_person_head = p_head_of_household
FROM    sk_prod.PLAYPEN_CONSUMERVIEW_PERSON_AND_HOUSEHOLD e
WHERE   s.exp_cb_key_db_person = e.exp_cb_key_db_person
COMMIT


---------------------------------------------------------------------------------
--- Based upon Experian Head of hhd select a single head of hhd for each hhd
--- A hhd is defined by cb_key_household and cb_address_line_1
--- Experian generally assigns BOTH a male and a female as head of hhd
--- We need to select ONE. This is done based upon highest personal income
---------------------------------------------------------------------------------

-- Identify highest personal income from indiviuals in a hhd who are head of hhd
select exp_cb_key_db_person, cb_key_household, cb_address_line_1
        ,rank() OVER (PARTITION by cb_key_household, cb_address_line_1 ORDER BY person_income DESC, exp_cb_key_db_person DESC ) rank_1
into #a1
from V289_M08_SKY_HH_composition
where exp_person_head = 1
commit

create hg index ind1 on #a1(exp_cb_key_db_person)
commit

-- Assign a single individual in each hhd as head of hhd based upon above
update V289_M08_SKY_HH_composition e
        set person_head = '1'
        from #a1 t
        where e.exp_cb_key_db_person = t.exp_cb_key_db_person
        and t.rank_1 = 1
commit


--- Not all hhds have a defined head of hhd from Experian. So will assign highest personal income in these cases
-- First count number of heads of hhd as per our definition for each hhd
select cb_key_household, cb_address_line_1
        , sum(case when person_head = '1' then 1 else 0 end) as head_count
into #a2
from V289_M08_SKY_HH_composition
group by cb_key_household, cb_address_line_1
commit

create hg index ind1 on #a2(cb_key_household)
create hg index ind2 on #a2(cb_address_line_1)
create lf index ind3 on #a2(head_count)
commit


-- Those hhds where above is zero need to be allocated individual with highest income
select p.exp_cb_key_db_person, p.cb_key_household, p.cb_address_line_1
        ,rank() OVER (PARTITION by p.cb_key_household, p.cb_address_line_1 ORDER BY p.person_income DESC, p.exp_cb_key_db_person DESC ) rank_1
into #a3
from
        V289_M08_SKY_HH_composition p
     inner join
        #a2 t
     on p.cb_key_household = t.cb_key_household and p.cb_address_line_1 = t.cb_address_line_1
where t.head_count = 0
commit

create hg index ind1 on #a3(exp_cb_key_db_person)
commit

update V289_M08_SKY_HH_composition p
        set person_head = '1'
        from #a3 t
        where p.exp_cb_key_db_person = t.exp_cb_key_db_person
        and t.rank_1 = 1
commit


---------------------------------------------------------------------------------------------------------
---------------------           C03 --      Add Individual Children     ---------------------------------
---------------------------------------------------------------------------------------------------------

-- Experian tables do not have individual data for children less than 17
---- Need to append rows for these
--- They cannot be head of hhd either so can be run after that code

-- Will need to add a row for each child, these multiple rows in this table will enable
-- the right number of individuals to be added to the data
select 1 as number_of_kids, 1 as unique_row into #PIV_append_kids_rows
commit

create lf index ind1 on #PIV_append_kids_rows(number_of_kids)
commit

insert into #PIV_append_kids_rows values (2, 2)
insert into #PIV_append_kids_rows values (2, 3)
insert into #PIV_append_kids_rows values (3, 4)
insert into #PIV_append_kids_rows values (3, 5)
insert into #PIV_append_kids_rows values (3, 6)
insert into #PIV_append_kids_rows values (4, 7)
insert into #PIV_append_kids_rows values (4, 8)
insert into #PIV_append_kids_rows values (4, 9)
insert into #PIV_append_kids_rows values (4, 10)
insert into #PIV_append_kids_rows values (5, 11)
insert into #PIV_append_kids_rows values (5, 12)
insert into #PIV_append_kids_rows values (5, 13)
insert into #PIV_append_kids_rows values (5, 14)
insert into #PIV_append_kids_rows values (5, 15)
insert into #PIV_append_kids_rows values (6, 16)
insert into #PIV_append_kids_rows values (6, 17)
insert into #PIV_append_kids_rows values (6, 18)
insert into #PIV_append_kids_rows values (6, 19)
insert into #PIV_append_kids_rows values (6, 20)
insert into #PIV_append_kids_rows values (6, 21)
insert into #PIV_append_kids_rows values (7, 22)
insert into #PIV_append_kids_rows values (7, 23)
insert into #PIV_append_kids_rows values (7, 24)
insert into #PIV_append_kids_rows values (7, 25)
insert into #PIV_append_kids_rows values (7, 26)
insert into #PIV_append_kids_rows values (7, 27)
insert into #PIV_append_kids_rows values (7, 28)
insert into #PIV_append_kids_rows values (8, 29)
insert into #PIV_append_kids_rows values (8, 30)
insert into #PIV_append_kids_rows values (8, 31)
insert into #PIV_append_kids_rows values (8, 32)
insert into #PIV_append_kids_rows values (8, 33)
insert into #PIV_append_kids_rows values (8, 34)
insert into #PIV_append_kids_rows values (8, 35)
insert into #PIV_append_kids_rows values (8, 36)
insert into #PIV_append_kids_rows values (9, 37)
insert into #PIV_append_kids_rows values (9, 38)
insert into #PIV_append_kids_rows values (9, 39)
insert into #PIV_append_kids_rows values (9, 40)
insert into #PIV_append_kids_rows values (9, 41)
insert into #PIV_append_kids_rows values (9, 42)
insert into #PIV_append_kids_rows values (9, 43)
insert into #PIV_append_kids_rows values (9, 44)
insert into #PIV_append_kids_rows values (9, 45)
insert into #PIV_append_kids_rows values (10, 46)
insert into #PIV_append_kids_rows values (10, 47)
insert into #PIV_append_kids_rows values (10, 48)
insert into #PIV_append_kids_rows values (10, 49)
insert into #PIV_append_kids_rows values (10, 50)
insert into #PIV_append_kids_rows values (10, 51)
insert into #PIV_append_kids_rows values (10, 52)
insert into #PIV_append_kids_rows values (10, 53)
insert into #PIV_append_kids_rows values (10, 54)
insert into #PIV_append_kids_rows values (10, 55)
insert into #PIV_append_kids_rows values (11, 56)
insert into #PIV_append_kids_rows values (11, 57)
insert into #PIV_append_kids_rows values (11, 58)
insert into #PIV_append_kids_rows values (11, 59)
insert into #PIV_append_kids_rows values (11, 60)
insert into #PIV_append_kids_rows values (11, 61)
insert into #PIV_append_kids_rows values (11, 62)
insert into #PIV_append_kids_rows values (11, 63)
insert into #PIV_append_kids_rows values (11, 64)
insert into #PIV_append_kids_rows values (11, 65)
insert into #PIV_append_kids_rows values (11, 66)
insert into #PIV_append_kids_rows values (12, 67)
insert into #PIV_append_kids_rows values (12, 68)
insert into #PIV_append_kids_rows values (12, 69)
insert into #PIV_append_kids_rows values (12, 70)
insert into #PIV_append_kids_rows values (12, 71)
insert into #PIV_append_kids_rows values (12, 72)
insert into #PIV_append_kids_rows values (12, 73)
insert into #PIV_append_kids_rows values (12, 74)
insert into #PIV_append_kids_rows values (12, 75)
insert into #PIV_append_kids_rows values (12, 76)
insert into #PIV_append_kids_rows values (12, 77)
insert into #PIV_append_kids_rows values (12, 78)
insert into #PIV_append_kids_rows values (13, 79)
insert into #PIV_append_kids_rows values (13, 80)
insert into #PIV_append_kids_rows values (13, 81)
insert into #PIV_append_kids_rows values (13, 82)
insert into #PIV_append_kids_rows values (13, 83)
insert into #PIV_append_kids_rows values (13, 84)
insert into #PIV_append_kids_rows values (13, 85)
insert into #PIV_append_kids_rows values (13, 86)
insert into #PIV_append_kids_rows values (13, 87)
insert into #PIV_append_kids_rows values (13, 88)
insert into #PIV_append_kids_rows values (13, 89)
insert into #PIV_append_kids_rows values (13, 90)
insert into #PIV_append_kids_rows values (13, 91)
insert into #PIV_append_kids_rows values (14, 92)
insert into #PIV_append_kids_rows values (14, 93)
insert into #PIV_append_kids_rows values (14, 94)
insert into #PIV_append_kids_rows values (14, 95)
insert into #PIV_append_kids_rows values (14, 96)
insert into #PIV_append_kids_rows values (14, 97)
insert into #PIV_append_kids_rows values (14, 98)
insert into #PIV_append_kids_rows values (14, 99)
insert into #PIV_append_kids_rows values (14, 100)
insert into #PIV_append_kids_rows values (14, 101)
insert into #PIV_append_kids_rows values (14, 102)
insert into #PIV_append_kids_rows values (14, 103)
insert into #PIV_append_kids_rows values (14, 104)
insert into #PIV_append_kids_rows values (14, 105)
insert into #PIV_append_kids_rows values (15, 106)
insert into #PIV_append_kids_rows values (15, 107)
insert into #PIV_append_kids_rows values (15, 108)
insert into #PIV_append_kids_rows values (15, 109)
insert into #PIV_append_kids_rows values (15, 110)
insert into #PIV_append_kids_rows values (15, 111)
insert into #PIV_append_kids_rows values (15, 112)
insert into #PIV_append_kids_rows values (15, 113)
insert into #PIV_append_kids_rows values (15, 114)
insert into #PIV_append_kids_rows values (15, 115)
insert into #PIV_append_kids_rows values (15, 116)
insert into #PIV_append_kids_rows values (15, 117)
insert into #PIV_append_kids_rows values (15, 118)
insert into #PIV_append_kids_rows values (15, 119)
insert into #PIV_append_kids_rows values (15, 120)
insert into #PIV_append_kids_rows values (16, 121)
insert into #PIV_append_kids_rows values (16, 122)
insert into #PIV_append_kids_rows values (16, 123)
insert into #PIV_append_kids_rows values (16, 124)
insert into #PIV_append_kids_rows values (16, 125)
insert into #PIV_append_kids_rows values (16, 126)
insert into #PIV_append_kids_rows values (16, 127)
insert into #PIV_append_kids_rows values (16, 128)
insert into #PIV_append_kids_rows values (16, 129)
insert into #PIV_append_kids_rows values (16, 130)
insert into #PIV_append_kids_rows values (16, 131)
insert into #PIV_append_kids_rows values (16, 132)
insert into #PIV_append_kids_rows values (16, 133)
insert into #PIV_append_kids_rows values (16, 134)
insert into #PIV_append_kids_rows values (16, 135)
insert into #PIV_append_kids_rows values (16, 136)
insert into #PIV_append_kids_rows values (17, 137)
insert into #PIV_append_kids_rows values (17, 138)
insert into #PIV_append_kids_rows values (17, 139)
insert into #PIV_append_kids_rows values (17, 140)
insert into #PIV_append_kids_rows values (17, 141)
insert into #PIV_append_kids_rows values (17, 142)
insert into #PIV_append_kids_rows values (17, 143)
insert into #PIV_append_kids_rows values (17, 144)
insert into #PIV_append_kids_rows values (17, 145)
insert into #PIV_append_kids_rows values (17, 146)
insert into #PIV_append_kids_rows values (17, 147)
insert into #PIV_append_kids_rows values (17, 148)
insert into #PIV_append_kids_rows values (17, 149)
insert into #PIV_append_kids_rows values (17, 150)
insert into #PIV_append_kids_rows values (17, 151)
insert into #PIV_append_kids_rows values (17, 152)
insert into #PIV_append_kids_rows values (17, 153)
insert into #PIV_append_kids_rows values (18, 154)
insert into #PIV_append_kids_rows values (18, 155)
insert into #PIV_append_kids_rows values (18, 156)
insert into #PIV_append_kids_rows values (18, 157)
insert into #PIV_append_kids_rows values (18, 158)
insert into #PIV_append_kids_rows values (18, 159)
insert into #PIV_append_kids_rows values (18, 160)
insert into #PIV_append_kids_rows values (18, 161)
insert into #PIV_append_kids_rows values (18, 162)
insert into #PIV_append_kids_rows values (18, 163)
insert into #PIV_append_kids_rows values (18, 164)
insert into #PIV_append_kids_rows values (18, 165)
insert into #PIV_append_kids_rows values (18, 166)
insert into #PIV_append_kids_rows values (18, 167)
insert into #PIV_append_kids_rows values (18, 168)
insert into #PIV_append_kids_rows values (18, 169)
insert into #PIV_append_kids_rows values (18, 170)
insert into #PIV_append_kids_rows values (18, 171)
insert into #PIV_append_kids_rows values (19, 172)
insert into #PIV_append_kids_rows values (19, 173)
insert into #PIV_append_kids_rows values (19, 174)
insert into #PIV_append_kids_rows values (19, 175)
insert into #PIV_append_kids_rows values (19, 176)
insert into #PIV_append_kids_rows values (19, 177)
insert into #PIV_append_kids_rows values (19, 178)
insert into #PIV_append_kids_rows values (19, 179)
insert into #PIV_append_kids_rows values (19, 180)
insert into #PIV_append_kids_rows values (19, 181)
insert into #PIV_append_kids_rows values (19, 182)
insert into #PIV_append_kids_rows values (19, 183)
insert into #PIV_append_kids_rows values (19, 184)
insert into #PIV_append_kids_rows values (19, 185)
insert into #PIV_append_kids_rows values (19, 186)
insert into #PIV_append_kids_rows values (19, 187)
insert into #PIV_append_kids_rows values (19, 188)
insert into #PIV_append_kids_rows values (19, 189)
insert into #PIV_append_kids_rows values (19, 190)
insert into #PIV_append_kids_rows values (20, 191)
insert into #PIV_append_kids_rows values (20, 192)
insert into #PIV_append_kids_rows values (20, 193)
insert into #PIV_append_kids_rows values (20, 194)
insert into #PIV_append_kids_rows values (20, 195)
insert into #PIV_append_kids_rows values (20, 196)
insert into #PIV_append_kids_rows values (20, 197)
insert into #PIV_append_kids_rows values (20, 198)
insert into #PIV_append_kids_rows values (20, 199)
insert into #PIV_append_kids_rows values (20, 200)
insert into #PIV_append_kids_rows values (20, 201)
insert into #PIV_append_kids_rows values (20, 202)
insert into #PIV_append_kids_rows values (20, 203)
insert into #PIV_append_kids_rows values (20, 204)
insert into #PIV_append_kids_rows values (20, 205)
insert into #PIV_append_kids_rows values (20, 206)
insert into #PIV_append_kids_rows values (20, 207)
insert into #PIV_append_kids_rows values (20, 208)
insert into #PIV_append_kids_rows values (20, 209)
insert into #PIV_append_kids_rows values (20, 210)
commit


INSERT INTO V289_M08_SKY_HH_composition (account_number, cb_key_household, cb_address_line_1
                                                        , person_gender, person_ageband, demographic_ID)
select
        hh.account_number
        ,hh.cb_key_household
        ,hh.cb_address_line_1
        ,'U'
        ,'0-19'
       ,15 -- demographic_ID for gender ='U' and age <=19
from
        V289_M08_SKY_HH_view hh
     inner join
        #PIV_append_kids_rows k
     on hh.children_count = k.number_of_kids



---- There are a small number of 0-19 in the Experian data (these were 18-19 in Experian data)
--- These will have a gender. But because they are a small number distort the scaling
--- Change the gender of these to U

update V289_M08_SKY_HH_composition
set person_gender = 'U'
where person_ageband = '0-19'
commit



---------------------------------------------------------------------------------------------------------
---------------------           C04 --      Final Tidying of Data     ---------------------------------
---------------------------------------------------------------------------------------------------------

-- Everyone with the same account_number gets a unique number
select     row_id
           ,RANK () OVER (PARTITION BY  account_number ORDER BY person_head DESC, row_id) as rank1
into       #a4
from        V289_M08_SKY_HH_composition
group by    account_number, person_head, row_id
commit

create hg index ind1 on #a4(row_id)
commit

update V289_M08_SKY_HH_composition h
set HH_person_number = rank1
from #a4 r
where h.row_id = r.row_id
commit


-- Calculate household size and delete any > 15

select account_number, count(1) as hhd_count
into #a5
from V289_M08_SKY_HH_composition
group by account_number
commit

update V289_M08_SKY_HH_composition c
set household_size = hhd_count
from #a5 a
where c.account_number = a.account_number
commit

delete from V289_M08_SKY_HH_composition
where household_size > 15
commit






 -- NOTE THESE QA NUMBERS NEED TO BE REFRESHED
---------------------------------  QA
 ---------------------------------  V289_M08_SKY_HH_view
 ---- account_number             9,929,864
 ---- cb_key_household           9,542,183
 ---- cb_address_line_1          6,036,344
 ---- matching_flag              8,734,222
 ---- edited_add_flag              365,499
 ---- HH Children_count          3,207,144
 ---- COUNT()                            9,929,864
---------------------------------
--------------------------------- V289_M08_SKY_HH_composition (individuals)
---- account_number      8,734,222
---- cb_key_household    8,615,848
---- cb_address_line_1   5,465,916
---- cb_key_db_person   18,375,549
---- individual                 17,898,812
---- COUNT(*)                   19,087,944
--------------------------------------
