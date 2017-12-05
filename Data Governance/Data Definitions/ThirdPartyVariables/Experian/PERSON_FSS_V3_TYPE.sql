/*###############################################################################
# Created on:   20/08/2012
# Created by:   Tony Kinnaird (TKD)
# Description:  Gives a user a sample select for identifying a person's FSS 
#		classification for an individual within each household.
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
#p_fss_v3_type	Person FSS V3 Type	
#
#
#Financial Strategy Segments (FSS) is Experian’s leading consumer classification focused on financial behaviours 
#and was developed to support financial services companies target their products and services.  
#FSS identifies the underlying factors that influence consumer behaviour segmenting the UK 
#into 93 distinct person types, 50 Household types and 15 Household Groups each with unique characteristics. 
#These distinct financial types comprehensively describe their typical financial product holdings, 
#behavioural and future intentions as well as summarising their key socio-economic and demographic characteristics.
#
###############################################################################*/


-- ##############################################################################################################
-- ##### STEP 1.0 - Basic Select									    #####
-- ##############################################################################################################


select top 100 cb_key_household,
case p_fss_v3_type 
when 'A01a' then 'Ross or Emma'
when 'A02a' then 'Hassan'
when 'A02b' then	'Anna'
when 'A03a' then	'Sam and Jodie'
when 'B04a' then	'Imran and Saima'
when 'B05a' then	'Ali'
when 'B05b' then	'Nadia'
when 'B06a' then	'Lukasz or Ewa'
when 'B07a' then	'Tom or Lauren'
when 'C08a' then	'Ashley or Chantelle'
when 'C09a' then	'Damian or Kerry'
when 'D10a' then	'Max'
when 'D10b' then	'Lara'
when 'D11a' then	'Simon'
when 'D11b' then	'Juliet'
when 'D12a' then	'Syed'
when 'D12b' then	'Deepa'
when 'D13a' then	'Mark'
when 'D13b' then	'Clare'
when 'E14a' then	'Matthew'
when 'E14b' then	'Vikki'
when 'E15a' then	'Neil'
when 'E15b' then	'Lisa'
when 'F16a' then	'Alastair and Gabrielle'
when 'F17a' then	'Dominic and Susannah'
when 'F18a' then	'Howard'
when 'F18b' then	'Virginia'
when 'F18c' then	'Cameron and Alexandra'
when 'F19a' then	'Hamish'
when 'F19b' then	'Annabel'
when 'F19c' then	'Calum and Georgia'
when 'F20a' then	'Geoffrey'
when 'F20b' then	'Vivien'
when 'F20c' then	'Benjamin and Kate'
when 'G21a' then	'Ian'
when 'G21b' then	'Kim'
when 'G21c' then	'Jake and Holly'
when 'G22a' then	'Will'
when 'G22b' then	'Susie'
when 'G23a' then	'Nigel'
when 'G23b' then	'Karen'
when 'G24a' then	'Bharat'
when 'G24b' then	'Jayshree'
when 'G24c' then	'Jay and Sabrina'
when 'H25a' then	'Philip'
when 'H25b' then	'Beverley'
when 'H25c' then	'Liam and Ashleigh'
when 'H26a' then	'Glen'
when 'H26b' then	'Maxine'
when 'H26c' then	'Connor and Chloe'
when 'H27a' then	'Adrian or Yvette'
when 'H28a' then	'Kevin'
when 'H28b' then	'Julie'
when 'H29a' then	'Garry'
when 'H29b' then	'Dawn'
when 'I30a' then	'Dean and Terri'
when 'I31a' then	'Reece'
when 'I31b' then	'Sharon'
when 'I32a' then	'Danny'
when 'I32b' then	'Debbie'
when 'I33a' then	'Tony'
when 'I33b' then	'Lorraine'
when 'J34a' then	'Roderick'
when 'J34b' then	'Janis'
when 'J34c' then	'Greg and Beth'
when 'J35a' then	'Trevor'
when 'J35b' then	'Sandra'
when 'J35c' then	'Adam and Katy'
when 'J36a' then	'Terrence '
when 'J36b' then	'Susan'
when 'J36c' then	'Luke and Dee'
when 'J37a' then	'Dudley or Glenys'
when 'K38a' then	'Kenneth'
when 'K38b' then	'Ann'
when 'K38c' then	'Scott and Nicky'
when 'K39a' then	'Barry or Carol'
when 'K40a' then	'Winston'
when 'K40b' then	'Gloria'
when 'L41a' then	'Ralph and Diana'
when 'L42a' then	'Maurice and Jeanne'
when 'L43a' then	'Ivor or Dorothy'
when 'M44a' then	'Alfred'
when 'M44b' then	'Molly'
when 'M44c' then	'Ray and Kay'
when 'M45a' then	'Ira'
when 'M45b' then	'Hannah'
when 'M46a' then	'Harry or Olive'
when 'M47a' then	'Albert or Mabel'
when 'N48a' then	'Frederick'
when 'N48b' then	'Lilian'
when 'N49a' then	'Raymond '
when 'N49b' then	'Norma'
when 'N50a' then	'Arnold or Bessie'
when 'U99u' then	'Unallocated'
else null end p_fss_v3_type
from sk_prod.experian_consumerview
where p_fss_v3_type is not null

-- ##############################################################################################################
-- ##### STEP 1.0 Ended									    #####
-- ##############################################################################################################
