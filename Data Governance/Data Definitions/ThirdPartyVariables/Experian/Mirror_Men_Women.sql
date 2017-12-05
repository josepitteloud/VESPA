/*###############################################################################
# Created on:   14/01/2013
# Created by:   Tony Kinnaird (TKD)
# Description:  Gives a user a process for identifying the Mirror values
#		within the Household
#
#################################################################################
# Dependencies
# ------------------------------------------------------------------------------
# (none)
#
#################################################################################
# Change log
# ------------------------------------------------------------------------------
# 14/01/2013  TKD   v01 - initial version
#
###############################################################################*/

-- ##############################################################################################################
-- ##### STEP 1.0 - Process									    #####
-- ##############################################################################################################

-------------------------------------------------  02 - Platform Notice

select max(cb_change_date)
into @cb_change_date
from sk_prod.EXPERIAN_CONSUMERVIEW;

--drop table ageband

SELECT cb_key_household
        ,(case when p_gender = '0' then 'Male'
               when p_gender = '1' then 'Female'
               else null
          end) as person_gender
        ,(case when person_gender = 'Male'      then 1 else 0 end) as Male
        ,(case when person_gender = 'Female'    then 1 else 0 end) as Female
        ,(case when p_actual_age >= 16 and p_actual_age < 25 then '16 to 24'
               when p_actual_age >= 25 and p_actual_age < 35 then '25 to 34'
               when p_actual_age >= 35 and p_actual_age < 45 then '35 to 44'
               when p_actual_age >= 45 and p_actual_age < 55 then '45 to 54'
               when p_actual_age >= 55                       then '55 Plus'
               else null
          end) as age_band
        ,(case when age_band = '16 to 24' then 1 else 0 end) as age16to24
        ,(case when age_band = '25 to 34' then 1 else 0 end) as age25to34
        ,(case when age_band = '35 to 44' then 1 else 0 end) as age35to44
        ,(case when age_band = '45 to 54' then 1 else 0 end) as age45to54
        ,(case when age_band = '55 Plus'  then 1 else 0 end) as age55plus

        ,(case when male = 1 and age16to24 = 1 then 1 else 0 end) as male_age16to24
        ,(case when male = 1 and age25to34 = 1 then 1 else 0 end) as male_age25to34
        ,(case when male = 1 and age35to44 = 1 then 1 else 0 end) as male_age35to44
        ,(case when male = 1 and age45to54 = 1 then 1 else 0 end) as male_age45to54
        ,(case when male = 1 and age55plus = 1 then 1 else 0 end) as male_age55plus

        ,(case when female = 1 and age16to24 = 1 then 1 else 0 end) as female_age16to24
        ,(case when female = 1 and age25to34 = 1 then 1 else 0 end) as female_age25to34
        ,(case when female = 1 and age35to44 = 1 then 1 else 0 end) as female_age35to44
        ,(case when female = 1 and age45to54 = 1 then 1 else 0 end) as female_age45to54
        ,(case when female = 1 and age55plus = 1 then 1 else 0 end) as female_age55plus

INTO ageband
FROM sk_prod.EXPERIAN_CONSUMERVIEW
where cb_change_date = @cb_change_date;

--select top 100 * from ageband

--drop table mirror_men_and_women;

select cb_key_household
        ,count(cb_key_household)        as number_in_HH
        ,sum(Male)                      as number_of_male
        ,sum(Female)                    as number_of_female

        ,sum(male_age16to24)            as num_of_male_age16to24
        ,sum(male_age25to34)            as num_of_male_age25to34
        ,sum(male_age35to44)            as num_of_male_age35to44
        ,sum(male_age45to54)            as num_of_male_age45to54
        ,sum(male_age55plus)            as num_of_male_age55plus

        ,sum(female_age16to24)          as num_of_female_age16to24
        ,sum(female_age25to34)          as num_of_female_age25to34
        ,sum(female_age35to44)          as num_of_female_age35to44
        ,sum(female_age45to54)          as num_of_female_age45to54
        ,sum(female_age55plus)          as num_of_female_age55plus
into mirror_men_and_women
from ageband
group by cb_key_household
order by cb_key_household;

--select top 100 * from mirror_men_and_women
--drop table mirror_men_and_women2

select cb_key_household
       --mirror men
        ,(case when number_of_male > 0 and (number_of_male = num_of_male_age16to24) then 1 else 0 end) as MI
        ,(case when number_of_male > 0 and (num_of_male_age16to24 > 0 or num_of_male_age25to34 > 0) then 1 else 0 end) as MII
        ,(case when number_of_male > 0 and (num_of_male_age16to24 > 0 or num_of_male_age25to34 > 0 or num_of_male_age35to44 > 0) then 1 else 0 end) as MIII
        ,(case when number_of_male > 0 and (num_of_male_age16to24 > 0 or num_of_male_age25to34 > 0 or num_of_male_age35to44 > 0 or num_of_male_age45to54 > 0) then 1 else 0 end) as MIV
        ,(case when number_of_male > 0 and (number_of_male = num_of_male_age55plus) then 1 else 0 end) as MV
        ,(case when number_of_male = 0 then 1 else 0 end) as MVI

        --mirror women
        ,(case when number_of_female > 0 and (number_of_female = num_of_female_age16to24) then 1 else 0 end) as WI
        ,(case when number_of_female > 0 and (num_of_female_age16to24 > 0 or num_of_female_age25to34 > 0) then 1 else 0 end) as WII
        ,(case when number_of_female > 0 and (num_of_female_age16to24 > 0 or num_of_female_age25to34 > 0 or num_of_female_age35to44 > 0) then 1 else 0 end) as WIII
        ,(case when number_of_female > 0 and (num_of_female_age16to24 > 0 or num_of_female_age25to34 > 0 or num_of_female_age35to44 > 0 or num_of_female_age45to54 > 0) then 1 else 0 end) as WIV
        ,(case when number_of_female > 0 and (number_of_female = num_of_female_age55plus) then 1 else 0 end) as WV
        ,(case when number_of_female = 0 then 1 else 0 end) as WVI
into mirror_men_and_women2
from mirror_men_and_women;

--select top 100 * from mirror_men_and_women2

--Group into mirror men and mirror women segmentation
--drop table mirror_men_and_women_seg
select *
        ,(case when MI = 1      then 'MI'
              when MII = 1      then 'MII'
              when MIII = 1     then 'MIII'
              when MIV = 1      then 'MIV'
              when MV = 1       then 'MV'
              when MVI = 1      then 'MVI'
              else 'MVII'
           end) as Mirror_Men
        ,(case when WI = 1      then 'WI'
              when WII = 1      then 'WII'
              when WIII = 1     then 'WIII'
              when WIV = 1      then 'WIV'
              when WV = 1       then 'WV'
              when WVI = 1      then 'WVI'
              else 'WVII'
           end) as Mirror_Women
into mirror_men_and_women_seg
from mirror_men_and_women2;

-- ##############################################################################################################
-- ##### STEP 1.0 Ended									    #####
-- ##############################################################################################################


