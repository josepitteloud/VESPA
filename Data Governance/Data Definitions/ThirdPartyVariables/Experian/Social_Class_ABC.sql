/*###############################################################################
# Created on:   22/02/2013
# Created by:   Tony Kinnaird (TKD)
# Description:  Gives a user a sample select for identifying ABC1 members of a 
#		household.
#
#################################################################################
# Dependencies
# ------------------------------------------------------------------------------
# (none)
#
#################################################################################
# Change log
# ------------------------------------------------------------------------------
# 22/02/2013  TKD   v01 - initial version
#
###############################################################################*/

-- ##############################################################################################################
-- ##### STEP 1.0 - Basic Select									    #####
-- ##############################################################################################################


---------------------------------------------------------------------------------
--  Men in Household Age
---------------------------------------------------------------------------------
drop table temp_AGEM;
SELECT cb_key_household,
       CASE WHEN MIN(cast(p_age_coarse AS integer )) = 0       THEN '18-25' --replace person_age
            WHEN MIN(cast(p_age_coarse AS integer )) = 1       THEN '26-35' --replace person_age
            WHEN MIN(cast(p_age_coarse AS integer )) = 2       THEN '36-45' --replace person_age
            WHEN MIN(cast(p_age_coarse AS integer )) = 3       THEN '46-55' --replace person_age
            WHEN MIN(cast(p_age_coarse AS integer )) = 4       THEN '56-65'   --replace person_age
            WHEN MIN(cast(p_age_coarse AS integer )) = 5       THEN '66+'   --replace person_age
            ELSE                                                    'UNKNOWN'
        END MIN_AGE_MALE
INTO temp_AGEM
FROM sk_prod.EXPERIAN_CONSUMERVIEW
where cb_change_date = @max_change_date
AND p_gender = '0'
GROUP BY cb_key_household;
commit;

        --      update file
UPDATE nodupes_iq
SET      MEN_IN_HH       = EXP.MIN_AGE_MALE
     FROM nodupes_iq  AS Base
        INNER JOIN temp_AGEM AS EXP
        ON base.cb_key_household = EXP.cb_key_household;
commit;

--select mirror_men, count(*) from nodupes_iq group by mirror_men

UPDATE nodupes_iq
set  MEN_IN_HH = 'No men in HH'
where mirror_men = 'MVII' ;
commit;

drop table temp_AGEM;



---------------------------------------------------------------------------------
--  Female in Household Age
---------------------------------------------------------------------------------
drop table temp_AGEF;
SELECT cb_key_household,
       CASE WHEN MIN(cast(p_age_coarse AS integer )) = 0       THEN '18-25'
            WHEN MIN(cast(p_age_coarse AS integer )) = 1       THEN '26-35'
            WHEN MIN(cast(p_age_coarse AS integer )) = 2       THEN '36-45'
            WHEN MIN(cast(p_age_coarse AS integer )) = 3       THEN '46-55'
            WHEN MIN(cast(p_age_coarse AS integer )) = 4       THEN '56-65'
            WHEN MIN(cast(p_age_coarse AS integer )) = 5       THEN '66+'
            ELSE                                                    'UNKNOWN'
        END MIN_AGE_FEMALE
INTO temp_AGEF
FROM sk_prod.EXPERIAN_CONSUMERVIEW
where cb_change_date = @max_change_date
AND p_gender = '1'
GROUP BY cb_key_household;
commit;

        --      update file
UPDATE nodupes_iq
SET      WOMEN_IN_HH       = EXP.MIN_AGE_FEMALE
     FROM nodupes_iq  AS Base
        INNER JOIN temp_AGEF AS EXP
        ON base.cb_key_household = EXP.cb_key_household;
commit;

UPDATE nodupes_iq
set  WOMEN_IN_HH = 'No women in HH'
where mirror_women = 'WVII' ;
commit;

drop table temp_AGEF;


---------------------------------------------------------------------------------
--  Adults in Household Age
---------------------------------------------------------------------------------
--drop table temp_AGEA;
SELECT cb_key_household,
        CASE WHEN MIN(cast(p_age_coarse AS integer )) = 0       THEN '18-25'
             WHEN MIN(cast(p_age_coarse AS integer )) = 1       THEN '26-35'
             WHEN MIN(cast(p_age_coarse AS integer )) = 2       THEN '36-45'
             WHEN MIN(cast(p_age_coarse AS integer )) = 3       THEN '46-55'
             WHEN MIN(cast(p_age_coarse AS integer )) = 4       THEN '56-65'
             WHEN MIN(cast(p_age_coarse AS integer )) = 5       THEN '66+'
             ELSE                                                    'UNKNOWN'
         END MIN_AGE_
INTO temp_AGEA
FROM sk_prod.EXPERIAN_CONSUMERVIEW
where cb_change_date = @max_change_date
GROUP BY cb_key_household;
commit;

        --      update file
UPDATE nodupes_iq
SET      ADULTS_IN_HH       = EXP.MIN_AGE_
     FROM nodupes_iq  AS Base
        INNER JOIN temp_AGEA AS EXP
        ON base.cb_key_household = EXP.cb_key_household;
commit;

--select ADULTS_IN_HH, mirror_women,mirror_men, count(*) from nodupes_iq group by ADULTS_IN_HH, mirror_women,mirror_men

UPDATE nodupes_iq
set  ADULTS_IN_HH = 'No adults in HH'
where mirror_women = 'WVII' and mirror_men = 'MVII';
commit;


---------------------------------------------------------------------------

select distinct c.cb_row_id
        ,c.cb_key_individual
        ,c.cb_key_household
        ,c.lukcat_fr_de_nrs AS social_grade
        ,playpen.p_head_of_household
        ,rank() over(PARTITION BY c.cb_key_household ORDER BY playpen.p_head_of_household desc, c.lukcat_fr_de_nrs asc, c.cb_row_id desc) as rank_id
into caci_sc
from sk_prod.CACI_SOCIAL_CLASS as c,
     sk_prod.PLAYPEN_CONSUMERVIEW_PERSON_AND_HOUSEHOLD as playpen,
     sk_prod.experian_consumerview as e
where e.exp_cb_key_individual = playpen.exp_cb_key_individual
  and e.cb_key_individual = c.cb_key_individual
  and c.cb_address_dps is NOT NULL
order by c.cb_key_household;
commit;

delete from caci_sc where rank_id > 1;
commit;
--17392466 Row(s) affected

--select count(cb_key_household) from caci_sc;
--select count(distinct(cb_key_household)) from caci_sc;
--select top 10 * from sk_prod.CACI_SOCIAL_CLASS;


CREATE HG INDEX idx16 ON caci_sc(cb_key_household);

--Alter table VIQ_HH_ACCOUNT_TMP Add  social_grade varchar(15)  default 'UNCLASSIFIED';

Update VIQ_HH_ACCOUNT_TMP
set base.SOCIAL_CLASS = sc.social_grade
from VIQ_HH_ACCOUNT_TMP as base inner join caci_sc as sc
on base.cb_key_household = sc.cb_key_household;
commit;
--select count(*) from VIQ_HH_ACCOUNT_TMP;
--select top 100 * from VIQ_HH_ACCOUNT_TMP;

/*
--Add age and gender to the table
Alter table VIQ_HH_ACCOUNT_TMP add p_gender     varchar(5);
Alter table VIQ_HH_ACCOUNT_TMP add p_actual_age integer;
*/

/*
select ADULTS_IN_HH, count(*)
from VIQ_HH_ACCOUNT_TMP
group by ADULTS_IN_HH
*/

--Overwrite Mirror_ABC1 with CACI Social Class

--select WOMEN_IN_HH, count(*) from VIQ_HH_ACCOUNT_TMP group by WOMEN_IN_HH

Update VIQ_HH_ACCOUNT_TMP
set ABC1_MALES_IN_HH = case when social_class in ('A','B','C1') and MEN_IN_HH    NOT IN ('UNKNOWN', 'No men in HH')
                            then 1 else 0 end
,ABC1_FEMALES_IN_HH  = case when social_class in ('A','B','C1') and WOMEN_IN_HH  NOT IN ('UNKNOWN', 'No women in HH')
                            then 1 else 0 end
,ABC1_ADULTS_IN_HH   = case when social_class in ('A','B','C1') and ADULTS_IN_HH NOT IN ('UNKNOWN', 'No adults in HH')
                            then 1 else 0 end
;
commit;



-- ##############################################################################################################
-- ##### STEP 1.0 Ended									    #####
-- ##############################################################################################################
