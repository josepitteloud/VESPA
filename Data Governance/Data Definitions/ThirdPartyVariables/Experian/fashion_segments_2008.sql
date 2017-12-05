/*###############################################################################
# Created on:   20/08/2012
# Created by:   Tony Kinnaird (TKD)
# Description:  Gives a user a sample select for identifying the fashion_Segments 
#		an individual falls into within a household.
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


/*###############################################################################
#p_fashion_segments	Fashion segments	
#
#Fashion Segments classifies every adult in the United Kingdom based upon their attitudes 
#and behaviour towards fashion shopping. It links qualitative information on fashion shopping 
#The Segments classify all adults into 20 female types and 15 male types. 
#These comprehensively describe their typical attitudes towards fashion and brands, 
#and their typical behaviour towards types of clothes they purchase, stores they visit and frequency, 
#value and purpose of shopping trips. Key socio-economic 
#and demographic characteristics 
#are also provided.motivations and attitudes with quantitative information on shopping behaviour 
#and overall demographics to provide a unique insight into consumers within the fashion market.
###############################################################################*/



-- ##############################################################################################################
-- ##### STEP 1.0 - Basic Select									    #####
-- ##############################################################################################################

select top 100 cb_key_household, 
case p_new_fashion_segments_2008 
when 'F01' then 	'A01 Ana: Individual Urban Trend'
when 'F02' then 	'A02 Maria: Practical Comfort'
when 'F03' then 	'A03 Katie: High Fashion for Less'
when 'F04' then 	'A04 Sara: Family Sense'
when 'F05' then 	'B05 Natasha: Restricted Wardrobe'
when 'F06' then 	'B06 Sarah-Jane: Shoes and Mortgages'
when 'F07' then 	'B07 Stacey: Shoestring Style'
when 'F08' then 	'C08 Michelle: Kitted-out Kids'
when 'F09' then 	'C09 Tammy: Image as Identity'
when 'F10' then 	'C10 Christine: Price and Practicality'
when 'F11' then 	'C11 Lynn: Family Necessities'
when 'F12' then 	'D12 Annabel: Best Dressed Fashionistas'
when 'F13' then 	'D13 Virginia: Big Spenders'
when 'F14' then 	'E14 Jane: Hit-and-Run Shoppers'
when 'F15' then 	'E15 Marilyn: Local Essentials'
when 'F16' then 	'F16 Agnes: Repetitive Purchasers'
when 'F17' then 	'F17 Nancy: Autumn Style'
when 'F18' then 	'G18 Peggy: Catalogue Classics'
when 'F19' then 	'G19 Marjorie: Traditional Tailoring'
when 'F20' then 	'G20 Cynthia: Moderation and Sales'
when 'M01' then 	'A01 Philip: Youthful Aspiration'
when 'M02' then 	'A02 Malcolm: Any Shirt Will Do'
when 'M03' then 	'A03 Lee: Functional Fashion Seekers'
when 'M04' then 	'A04 Jason: Sport Basics'
when 'M05' then 	'A05 Nathan: Budget Image'
when 'M06' then 	'B06 Stephen: Mainstream Fathers'
when 'M07' then 	'B07 Tim: Professional Look'
when 'M08' then 	'B07 Clive: Mid-range Suitability'
when 'M09' then 	'B09 Howard: Distinguished Classics'
when 'M10' then 	'C10 Simon: Quality not Quantity'
when 'M11' then 	'C11 Luke: Brand Boy'
when 'M12' then 	'C12 Dominic: Dressed in the Best'
when 'M13' then 	'D13 Fred: Low-cost and Long-lasting'
when 'M14' then 	'D14 Roy: Conventional Appearance '
when 'M15' then 	'D15 Hubert: Selective Habit'
when '99' then 'Unclassified'
else 'Unclassified' end 'Fashion segments'
from sk_prod.experian_consumerview
where p_new_fashion_segments_2008 is not null


-- ##############################################################################################################
-- ##### STEP 1.0 Ended									    #####
-- ##############################################################################################################
