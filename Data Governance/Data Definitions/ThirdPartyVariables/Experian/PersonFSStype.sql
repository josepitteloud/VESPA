/*###############################################################################
# Created on:   20/08/2012
# Created by:   Tony Kinnaird (TKD)
# Description:  Gives a user a sample select for identifying an individual's FSS 
#		type.
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
#p_fss_type	Person FSS type	
#
#
#Financial Strategy Segments (FSS) is a person and household level segmentation developed 
#to help financial services companies target their financial services products and services.  
#Financial Strategy Segments classify all adults in the United Kingdom into  13 household level groups (A–M) 
#and  45 household level types. The household level types are further split into 82 person level types. 
#These are distinct financial lifestyle types which comprehensively describe their typical financial product holdings, 
#behaviour and future intentions, as well as summarising their key socio-economic and demographic characteristics.
#
###############################################################################*/


-- ##############################################################################################################
-- ##### STEP 1.0 - Basic Select									    #####
-- ##############################################################################################################


select top 100 cb_key_household,
case p_fss_v3_type 
when 'A01a' then 	'Toby'
when 'A01b'	then 	'Claudia'
when 'A02a'	then 	'Mike'
when 'A02b'	then 	'Gill'
when 'A03a'	then 	'Matt'
when 'A03b'	then 	'Jo'
when 'B04a'	then 	'Muhammad'
when 'B04b'	then 	'Salma'
when 'B05a'	then 	'Mehmet and Amina'
when 'B06a'	then 	'Rob and Abigail'
when 'B07a'	then 	'Tom and Sophie'
when 'C08a'	then 	'Jason'
when 'C08b'	then 	'Tara'
when 'C09a'	then 	'Pete or Jane'
when 'D10a'	then 	'Lee or Kelly'
when 'D11a'	then 	'Shane'
when 'D11b'	then 	'Donna'
when 'D12a'	then 	'Ahmed or Leanne'
when 'E13a'	then 	'Giles'
when 'E13b'	then 	'Philippa'
when 'E14a'	then 	'Sanjay'
when 'E14b'	then 	'Alison'
when 'E15a'	then 	'Justin and Kirstie'
when 'E16a'	then 	'Spencer'
when 'E16b'	then 	'Justine'
when 'E17a'	then 	'Glenn'
when 'E17b'	then 	'Tania'
when 'F18a'	then 	'Darren'
when 'F18b'	then 	'Claire'
when 'F19a'	then 	'Shaun'
when 'F19b'	then 	'Tracy'
when 'F20a'	then 	'Dale'
when 'F20b'	then 	'Lyndsey'
when 'F21a'	then 	'Wayne'
when 'F21b'	then 	'Annemarie'
when 'G22a'	then 	'Rupert and Camilla'
when 'G23a'	then 	'Roger and Penelope'
when 'G23b'	then 	'Oliver and Bryony'
when 'G24a'	then 	'Deepak'
when 'G24b'	then 	'Nisha'
when 'G25a'	then 	'Robin and Tessa'
when 'G26a'	then 	'Benedict'
when 'G26b'	then 	'Felicity'
when 'G26c'	then 	'Joshua and Rosie'
when 'H27a'	then 	'Gerald and Celia'
when 'H27b'	then 	'Bethany and Euan'
when 'H28a'	then 	'Clive'
when 'H28b'	then 	'Marilyn'
when 'H28c'	then 	'Holly and Benjamin'
when 'H29a'	then 	'Angus'
when 'H29b'	then 	'Bethan'
when 'H29c'	then 	'Rhys and Rhian'
when 'I30a'	then 	'Graham'
when 'I30b'	then 	'Linda'
when 'I30c'	then 	'Gemma and Luke'
when 'I31a'	then 	'Stephen'
when 'I31b'	then 	'Denise'
when 'I31c'	then 	'Carly and Aaron'
when 'J32a'	then 	'Leslie and Rosa'
when 'J32b'	then 	'Gwen and Eddie'
when 'J33a'	then 	'Melvyn'
when 'J33b'	then 	'Glenis'
when 'J33c'	then 	'Adam and Katie'
when 'J34a'	then 	'Roy and Valerie'
when 'K35a'	then 	'Archibald and Kathleen'
when 'K35b'	then 	'Sinead and Ciaran'
when 'K36a'	then 	'Terence'
when 'K36b'	then 	'Marlene'
when 'K36c'	then 	'Ryan and Kayleigh'
when 'K37a'	then 	'Maureen or Rodney'
when 'K38a'	then 	'Brian'
when 'K38b'	then 	'Noreen'
when 'K38c'	then 	'Stacey and Craig'
when 'L39a'	then 	'Edgar and Sybil'
when 'L40a'	then 	'Lionel or Ursula'
when 'L41a'	then 	'Sidney'
when 'L41b'	then 	'Betty'
when 'L42a'	then 	'Reginald or Peggy'
when 'M43a'	then 	'Herbert'
when 'M43b'	then 	'Ivy'
when 'M44a'	then 	'Horace or Jessie'
when 'M45a'	then 	'Clarence or Elsie'
when 'U99u'	then 	'Unclassified'
else 'Unclassified' end 'Person FSS type'
from sk_prod.experian_consumerview
where p_fss_v3_type is not null

-- ##############################################################################################################
-- ##### STEP 1.0 Ended									    #####
-- ##############################################################################################################
