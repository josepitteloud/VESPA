/*###############################################################################
# Created on:   20/08/2012
# Created by:   Tony Kinnaird (TKD)
# Description:  Gives a user a sample select for identifying if # there are men in the HH and the minimum age
#		
#
#################################################################################
# Dependencies
# ------------------------------------------------------------------------------
# (none)
#
#################################################################################
# Change log
# ------------------------------------------------------------------------------
# 19/09/2012  TKD   v01 - initial version
#
###############################################################################*/

-- ##############################################################################################################
-- ##### STEP 1.0 - Basic Select									    #####
-- ##############################################################################################################

declare @max_change_date date

select @max_change_date = max(cb_data_date)
from sk_prod.experian_consumerview


drop table #temp_AGEM;
SELECT cb_key_household,
       CASE WHEN MIN(cast(p_age_coarse AS integer )) = 0       THEN '18-25' --replace person_age
            WHEN MIN(cast(p_age_coarse AS integer )) = 1       THEN '26-35' --replace person_age
            WHEN MIN(cast(p_age_coarse AS integer )) = 2       THEN '36-45' --replace person_age
            WHEN MIN(cast(p_age_coarse AS integer )) = 3       THEN '46-55' --replace person_age
            WHEN MIN(cast(p_age_coarse AS integer )) = 4       THEN '56-65'   --replace person_age
            WHEN MIN(cast(p_age_coarse AS integer )) = 5       THEN '66+'   --replace person_age
            ELSE                                                    'UNKNOWN'
        END MIN_AGE_MALE
INTO #temp_AGEM
FROM sk_prod.EXPERIAN_CONSUMERVIEW
where cb_change_date = @max_change_date
AND p_gender = '0'
GROUP BY cb_key_household;

commit;



-- ##############################################################################################################
-- ##### STEP 1.0 Ended									    #####
-- ##############################################################################################################
