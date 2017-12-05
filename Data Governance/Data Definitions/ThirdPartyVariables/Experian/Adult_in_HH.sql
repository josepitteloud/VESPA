/*###############################################################################
# Created on:   19/09/2012
# Created by:   Tony Kinnaird (TKD)
# Description:  Gives a user a sample select for identifying if # there are is an adult in the HH and the minimum age of those.
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
        CASE WHEN MIN(cast(person_age AS integer )) = 0       THEN '18-25'
             WHEN MIN(cast(person_age AS integer )) = 1       THEN '26-35'
             WHEN MIN(cast(person_age AS integer )) = 2       THEN '36-45'
             WHEN MIN(cast(person_age AS integer )) = 3       THEN '46-55'
             WHEN MIN(cast(person_age AS integer )) = 4       THEN '55+'
             WHEN MIN(cast(person_age AS integer )) = 5       THEN '55+'
             ELSE                                                  'UNKNOWN'
         END MIN_AGE_
INTO #temp_AGEA
FROM sk_prod.EXPERIAN_CONSUMERVIEW
where cb_change_date = @max_change_date
GROUP BY cb_key_household;

commit;



-- ##############################################################################################################
-- ##### STEP 1.0 Ended									    #####
-- ##############################################################################################################
