commit;

--SQL code below
-- create who lives in the household data
--select cb_change_date , count(*) from sk_prod.EXPERIAN_CONSUMERVIEW group by cb_change_date order by cb_change_date
--drop table #TEMP_HOUSE;

create variable @experian date;
set @experian = '2012-04-25';


SELECT cb_key_household
        ,MF = (CASE WHEN p_gender = '0' then 1                  --male
                    WHEN p_gender = '1' then 100                --female
                    WHEN p_gender = 'U' then 1000               --unknow
                    ELSE 9000                                   --missing data
              END)
INTO #TEMP_HOUSE
FROM sk_prod.EXPERIAN_CONSUMERVIEW
where cb_change_date = @experian
GROUP BY cb_key_household, MF
ORDER BY cb_key_household;

--select top 500 *  FROM sk_prod.EXPERIAN_CONSUMERVIEW;
--select  cb_change_date  FROM sk_prod.EXPERIAN_CONSUMERVIEW where cb_address_postcode = 'HP23 5PS' and cb_address_buildingno = '6';

--select  *  FROM sk_prod.EXPERIAN_CONSUMERVIEW where cb_address_postcode = 'HP23 5PS' and cb_address_buildingno = '6'; output to 'C:\Users\barnetd\Documents\Git\Vespa\Vespa Projects\XX - Mirror Segment definition in Experian Consumerview\ consumerview.xls' format excel;

commit;


--drop table #TEMP_HOUSE_COUNT;
select cb_key_household, sum(MF) AS House_binary, count(MF) AS House_num
INTO #TEMP_HOUSE_COUNT
from #TEMP_HOUSE
group by cb_key_household;
drop table #TEMP_HOUSE;

/*
house_binary    UFM FLAG
1               M only household
100             F only household
101             FM household
1001            UM household
1100            UF household
1101            UFM household
1000            U household
*/


--- temp Age file
--drop table #temp_AGE;
SELECT cb_key_household
        ,MAX(cast(person_age AS integer )) AS MAX_AGE  -- could del if not needed
        ,MIN(cast(person_age AS integer )) AS MIN_AGE
        ,p_gender
INTO #temp_AGE
FROM sk_prod.EXPERIAN_CONSUMERVIEW
where cb_change_date = @experian
GROUP BY cb_key_household, p_gender;


--- temp Age and household file
--drop table AGE_HOUSE;
SELECT A.cb_key_household,MAX_AGE,MIN_AGE,House_binary,p_gender
INTo  vespa_analysts.dbarnett_mirror_gender_AGE_HOUSE
FROM  #TEMP_HOUSE_COUNT AS A,  #temp_AGE AS B
WHERE A.cb_key_household = B.cb_key_household;


--drop table #temp_AGE;
--drop table #TEMP_HOUSE_COUNT;


ALTER table     vespa_analysts.dbarnett_mirror_gender_AGE_HOUSE ADD   MIRROR_MEN_MIN     varchar(4);
--ALTER table     AGE_HOUSE ADD   MIRROR_MEN_MAX     varchar(4);
ALTER table     vespa_analysts.dbarnett_mirror_gender_AGE_HOUSE ADD   MIRROR_WOMEN_MIN   varchar(4);
--ALTER table     AGE_HOUSE ADD   MIRROR_WOMEN_MAX   varchar(4);


update vespa_analysts.dbarnett_mirror_gender_AGE_HOUSE
   SET
/*        MIRROR_MEN_MAX  = CASE WHEN house_binary in (1)               AND MAX_AGE =   0      AND   p_gender = '0'      THEN 'WI'
                               WHEN house_binary in (101,1001,1101)   AND MAX_AGE in (0,1)   AND   p_gender = '0'      THEN 'WII'
                               WHEN house_binary in (1)               AND MAX_AGE in (1)     AND   p_gender = '0'      THEN 'WII'
                               WHEN house_binary in (1,101,1001,1101) AND MAX_AGE in (2)     AND   p_gender = '0'      THEN 'WIII'
                               WHEN house_binary in (1,101,1001,1101) AND MAX_AGE in (3)     AND   p_gender = '0'      THEN 'WIV'
                               WHEN house_binary in (1)               AND MAX_AGE in (4,5)   AND   p_gender = '0'      THEN 'WV'
                               WHEN house_binary in (100,1100)                                                         THEN 'WVI'
                               WHEN house_binary in (1000)                                                             THEN 'WVII'
                               END
   ,
   */
        MIRROR_MEN_MIN  = CASE WHEN house_binary = 1                  AND MIN_AGE =   0      AND   p_gender = '0'      THEN 'WI'
                               WHEN house_binary in (101,1001,1101)   AND MIN_AGE in (0,1)   AND   p_gender = '0'      THEN 'WII'
                               WHEN house_binary in (1)               AND MIN_AGE in (1)     AND   p_gender = '0'      THEN 'WII'
                               WHEN house_binary in (1,101,1001,1101) AND MIN_AGE in (2)     AND   p_gender = '0'      THEN 'WIII'
                               WHEN house_binary in (1,101,1001,1101) AND MIN_AGE in (3)     AND   p_gender = '0'      THEN 'WIV'
                               WHEN house_binary in (1)               AND MIN_AGE in (4,5)   AND   p_gender = '0'      THEN 'WV'
                               WHEN house_binary in (100,1100)                                                         THEN 'WVI'
                               WHEN house_binary in (1000)                                                             THEN 'WVII'
                               END
   /*,  MIRROR_WOMEN_MAX  = CASE WHEN house_binary = 1                  AND MAX_AGE =   0      AND   p_gender = '1'      THEN 'WI'
                               WHEN house_binary in (101,1001,1101)   AND MAX_AGE in (0,1)   AND   p_gender = '1'      THEN 'WII'
                               WHEN house_binary in (101,1001,1101)   AND MAX_AGE in (2)     AND   p_gender = '1'      THEN 'WIII'
                               WHEN house_binary in (101,1001,1101)   AND MAX_AGE in (3)     AND   p_gender = '1'      THEN 'WIV'
                               WHEN house_binary =1                   AND MAX_AGE in (4,5)   AND   p_gender = '1'      THEN 'WV'
                               WHEN house_binary in (100,1100)                                                         THEN 'WVI'
                               WHEN house_binary in (1000)                                                             THEN 'WVII'
                               END
                               */

    , MIRROR_WOMEN_MIN  = CASE WHEN house_binary = 1                  AND MIN_AGE =   0      AND   p_gender = '1'      THEN 'WI'
                               WHEN house_binary in (101,1001,1101)   AND MIN_AGE in (0,1)   AND   p_gender = '1'      THEN 'WII'
                               WHEN house_binary in (1)               AND MIN_AGE in (1)     AND   p_gender = '1'      THEN 'WII'
                               WHEN house_binary in (1,101,1001,1101) AND MIN_AGE in (2)     AND   p_gender = '1'      THEN 'WIII'
                               WHEN house_binary in (1,101,1001,1101) AND MIN_AGE in (3)     AND   p_gender = '1'      THEN 'WIV'
                               WHEN house_binary in (1)               AND MIN_AGE in (4,5)   AND   p_gender = '1'      THEN 'WV'
                               WHEN house_binary in (100,1100)                                                         THEN 'WVI'
                               WHEN house_binary in (1000)                                                             THEN 'WVII'
                               END;

commit;


--select top 500 * from vespa_analysts.dbarnett_mirror_gender_AGE_HOUSE;


select MIRROR_MEN_MIN
,count(*) as records
from vespa_analysts.dbarnett_mirror_gender_AGE_HOUSE
group by MIRROR_MEN_MIN
order by MIRROR_MEN_MIN;


select MIRROR_MEN_MIN
,p_gender
,count(*) as records
from vespa_analysts.dbarnett_mirror_gender_AGE_HOUSE
group by MIRROR_MEN_MIN,p_gender
order by MIRROR_MEN_MIN,p_gender;



select house_binary
,count(*) as records
from vespa_analysts.dbarnett_mirror_gender_AGE_HOUSE
group by house_binary
order by house_binary;


----Mosaic Split----
select cb_key_household
,min(h_mosaic_uk_2009_type) as min_hh_uk_type
,max(h_mosaic_uk_2009_type) as max_hh_uk_type
into #mosaic_by_hh
FROM sk_prod.EXPERIAN_CONSUMERVIEW
where cb_change_date = '2012-04-25'
group by cb_key_household
;

select min_hh_uk_type
,max_hh_uk_type
,count(*) as households
from #mosaic_by_hh
group by min_hh_uk_type
,max_hh_uk_type
order by min_hh_uk_type
,max_hh_uk_type
;


select min_hh_uk_type
,count(*) as households
from #mosaic_by_hh
group by min_hh_uk_type
order by min_hh_uk_type
;

commit;




