/*###############################################################################
# Created on:   20/08/2012
# Created by:   Tony Kinnaird (TKD)
# Description:  Gives a user a sample select for identifying Government_region of 
#		a household
#
#################################################################################
# Dependencies
# ------------------------------------------------------------------------------
# (none)
#
#################################################################################
# Change log
# ------------------------------------------------------------------------------
# 20/08/2012  TKD   v01 - initial version
#
###############################################################################*/

-- ##############################################################################################################
-- ##### STEP 1.0 - Basic Select									    #####
-- ##############################################################################################################




select sav.cb_address_postcode as Postcode
        ,government_region as Govt_Region
    into #Govt_Region_Lookup
    from sk_prod.cust_single_account_view as sav
         left join sk_prod.BROADBAND_POSTCODE_EXCHANGE as bpe
                   on sav.cb_address_postcode = bpe.cb_address_postcode
                   where Postcode is not null
                   group by Postcode, government_region
                   order by Postcode;

-- ##############################################################################################################
-- ##### STEP 1.0 Ended									    #####
-- ##############################################################################################################

