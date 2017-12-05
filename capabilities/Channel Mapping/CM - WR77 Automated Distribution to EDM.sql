 /*


                         $$$
                        I$$$
                        I$$$
               $$$$$$$$ I$$$    $$$$$      $$$ZDD    DDDDDDD.
             ,$$$$$$$$  I$$$   $$$$$$$    $$$ ODD  ODDDZ 7DDDD
             ?$$$,      I$$$ $$$$. $$$$  $$$= ODD  DDD     NDD
              $$$$$$$$= I$$$$$$$    $$$$.$$$  ODD +DD$     +DD$
                  :$$$$~I$$$ $$$$    $$$$$$   ODD  DDN     NDD.
               ,.   $$$+I$$$  $$$$    $$$$=   ODD  NDDN   NDDN
              $$$$$$$$$ I$$$   $$$$   .$$$    ODD   ZDDDDDDDN
                                      $$$      .      $DDZ
                                     $$$             ,NDDDDDDD
                                    $$$?

                      CUSTOMER INTELLIGENCE SERVICES

--------------------------------------------------------------------------------------------------------------
**Project Name:                         CHANNEL MAPPING ETL
**Analysts:                             Angel Donnarumma
**Lead(s):                              Jose Loureda
**Stakeholder:                          Operational Reports / SIG
**Due Date:                             02/04/2014
**Project Code (Insight Collation):
**Sharepoint Folder:                    
                                                                        
**Business Brief:

        This script handles the preparation of the CM data for Exports...

**Sections:
	
		S0 - Initialising Environment
        S1 - Assembling CHANNEL_MAP_PROD_SERVICE_KEY_ATTRIBUTES_DESCRIPTION
		S2 - Closing Prior CM Version

**Stats:
	
	Less than 1 sec...
--------------------------------------------------------------------------------------------------------------
*/

--------------------------------
-- S0 - Initialising Environment
--------------------------------

create or replace procedure ska_description_fill
as begin

-- Local variables
declare @effective_from date
declare @effective_to   date
declare @version        tinyint

select  @effective_from = max(amend_date) from vespa_analysts.CHANNEL_MAP_PROD_SERVICE_KEY_ATTRIBUTES
set     @effective_to = '2999-12-31' -- Actual version
select  @version = max(SERVICE_ATTRIBUTE_VERSION) from vespa_analysts.CHANNEL_MAP_PROD_SERVICE_KEY_ATTRIBUTES

----------------------------------------------------------------------
-- S1 - Assembling CHANNEL_MAP_PROD_SERVICE_KEY_ATTRIBUTES_DESCRIPTION
----------------------------------------------------------------------

-- Checking if the CM version we are currently running already exists in the description
-- table then we get rid of those records for refreshment...

if exists   (
                select  first *
                from    CHANNEL_MAP_PROD_SERVICE_KEY_ATTRIBUTES_DESCRIPTION
                where   SERVICE_ATTRIBUTE_VERSION = @version
            )
begin
    
    delete  from CHANNEL_MAP_PROD_SERVICE_KEY_ATTRIBUTES_DESCRIPTION
    where   SERVICE_ATTRIBUTE_VERSION = @version
    
    commit
    
end

Insert into CHANNEL_MAP_PROD_SERVICE_KEY_ATTRIBUTES_DESCRIPTION values('SERVICE_KEY','SERVICE_KEY','Service Key',@effective_from,@effective_to,@version,'Y')
Insert into CHANNEL_MAP_PROD_SERVICE_KEY_ATTRIBUTES_DESCRIPTION values('PROVIDER_ID','PROVIDER_ID','Provider Id',@effective_from,@effective_to,@version,'Y')
Insert into CHANNEL_MAP_PROD_SERVICE_KEY_ATTRIBUTES_DESCRIPTION values('EFFECTIVE_FROM','EFFECTIVE_FROM','Effective From',@effective_from,@effective_to,@version,'Y')
Insert into CHANNEL_MAP_PROD_SERVICE_KEY_ATTRIBUTES_DESCRIPTION values('EFFECTIVE_TO','EFFECTIVE_TO','Effective To',@effective_from,@effective_to,@version,'Y')
--Insert into CHANNEL_MAP_PROD_SERVICE_KEY_ATTRIBUTES_DESCRIPTION values('VERSION','VERSION','Version',@effective_from,@effective_to,@version,'Y')
Insert into CHANNEL_MAP_PROD_SERVICE_KEY_ATTRIBUTES_DESCRIPTION values('SERVICE_ATTRIBUTE_VERSION','SERVICE_ATTRIBUTE_VERSION','Service Attribute Version',@effective_from,@effective_to,@version,'Y')
Insert into CHANNEL_MAP_PROD_SERVICE_KEY_ATTRIBUTES_DESCRIPTION values('CHARACTER_01','FULL_NAME','Full Channel Name',@effective_from,@effective_to,@version,'N')
Insert into CHANNEL_MAP_PROD_SERVICE_KEY_ATTRIBUTES_DESCRIPTION values('CHARACTER_02','EPG_NUMBER','Epg Number',@effective_from,@effective_to,@version,'N')
Insert into CHANNEL_MAP_PROD_SERVICE_KEY_ATTRIBUTES_DESCRIPTION values('CHARACTER_03','EPG_NAME','Epg Channel Name',@effective_from,@effective_to,@version,'N')
Insert into CHANNEL_MAP_PROD_SERVICE_KEY_ATTRIBUTES_DESCRIPTION values('CHARACTER_04','VESPA_NAME','Vespa Channel Name',@effective_from,@effective_to,@version,'N')
Insert into CHANNEL_MAP_PROD_SERVICE_KEY_ATTRIBUTES_DESCRIPTION values('CHARACTER_05','CHANNEL_NAME','Channel Name',@effective_from,@effective_to,@version,'N')
Insert into CHANNEL_MAP_PROD_SERVICE_KEY_ATTRIBUTES_DESCRIPTION values('CHARACTER_06','TECHEDGE_NAME','Techedge Name',@effective_from,@effective_to,@version,'N')
Insert into CHANNEL_MAP_PROD_SERVICE_KEY_ATTRIBUTES_DESCRIPTION values('CHARACTER_07','INFOSYS_NAME','Infosys Channel name',@effective_from,@effective_to,@version,'N')
Insert into CHANNEL_MAP_PROD_SERVICE_KEY_ATTRIBUTES_DESCRIPTION values('CHARACTER_08','BARB_REPORTED','BARB Reported',@effective_from,@effective_to,@version,'N')
Insert into CHANNEL_MAP_PROD_SERVICE_KEY_ATTRIBUTES_DESCRIPTION values('CHARACTER_09','ACTIVEX','Activex',@effective_from,@effective_to,@version,'N')
Insert into CHANNEL_MAP_PROD_SERVICE_KEY_ATTRIBUTES_DESCRIPTION values('CHARACTER_10','CHANNEL_OWNER','Channel Owner',@effective_from,@effective_to,@version,'N')
Insert into CHANNEL_MAP_PROD_SERVICE_KEY_ATTRIBUTES_DESCRIPTION values('CHARACTER_11','OLD_PACKAGING','Old Packaging',@effective_from,@effective_to,@version,'N')
Insert into CHANNEL_MAP_PROD_SERVICE_KEY_ATTRIBUTES_DESCRIPTION values('CHARACTER_12','NEW_PACKAGING','New Packaging',@effective_from,@effective_to,@version,'N')
Insert into CHANNEL_MAP_PROD_SERVICE_KEY_ATTRIBUTES_DESCRIPTION values('CHARACTER_13','PAY_FREE_INDICATOR','Pay Free Indicator',@effective_from,@effective_to,@version,'N')
Insert into CHANNEL_MAP_PROD_SERVICE_KEY_ATTRIBUTES_DESCRIPTION values('CHARACTER_14','CHANNEL_GENRE','Channel Genre',@effective_from,@effective_to,@version,'N')
Insert into CHANNEL_MAP_PROD_SERVICE_KEY_ATTRIBUTES_DESCRIPTION values('CHARACTER_15','CHANNEL_TYPE','Conditional Access Type',@effective_from,@effective_to,@version,'N')
Insert into CHANNEL_MAP_PROD_SERVICE_KEY_ATTRIBUTES_DESCRIPTION values('CHARACTER_16','FORMAT','Channel Format',@effective_from,@effective_to,@version,'N')
Insert into CHANNEL_MAP_PROD_SERVICE_KEY_ATTRIBUTES_DESCRIPTION values('CHARACTER_17','TIMESHIFT_STATUS','Timeshift Status',@effective_from,@effective_to,@version,'N')
Insert into CHANNEL_MAP_PROD_SERVICE_KEY_ATTRIBUTES_DESCRIPTION values('CHARACTER_18','TIMESHIFT_MINUTES','Timeshift Minutes',@effective_from,@effective_to,@version,'N')
Insert into CHANNEL_MAP_PROD_SERVICE_KEY_ATTRIBUTES_DESCRIPTION values('CHARACTER_19','RETAIL','Retail Type',@effective_from,@effective_to,@version,'N')
Insert into CHANNEL_MAP_PROD_SERVICE_KEY_ATTRIBUTES_DESCRIPTION values('CHARACTER_20','CHANNEL_REACH','Channel Reach',@effective_from,@effective_to,@version,'N')
Insert into CHANNEL_MAP_PROD_SERVICE_KEY_ATTRIBUTES_DESCRIPTION values('CHARACTER_21','HD_SWAP_EPG_NUMBER','Epg Hd Swap Number',@effective_from,@effective_to,@version,'N')
Insert into CHANNEL_MAP_PROD_SERVICE_KEY_ATTRIBUTES_DESCRIPTION values('CHARACTER_22','SENSITIVE_CHANNEL','Sensitive Channel',@effective_from,@effective_to,@version,'N')
Insert into CHANNEL_MAP_PROD_SERVICE_KEY_ATTRIBUTES_DESCRIPTION values('CHARACTER_23','SPOT_SOURCE','Spot Source',@effective_from,@effective_to,@version,'N')
Insert into CHANNEL_MAP_PROD_SERVICE_KEY_ATTRIBUTES_DESCRIPTION values('CHARACTER_24','PROMO_SOURCE','Promo Source',@effective_from,@effective_to,@version,'N')
Insert into CHANNEL_MAP_PROD_SERVICE_KEY_ATTRIBUTES_DESCRIPTION values('CHARACTER_25','CHANNEL_PACK','Channel Pack',@effective_from,@effective_to,@version,'N')
Insert into CHANNEL_MAP_PROD_SERVICE_KEY_ATTRIBUTES_DESCRIPTION values('CHARACTER_26','PARENT_SERVICE_KEY','Parent Service Key',@effective_from,@effective_to,@version,'N')
Insert into CHANNEL_MAP_PROD_SERVICE_KEY_ATTRIBUTES_DESCRIPTION values('CHARACTER_27','TYPE_ID','Type Id',@effective_from,@effective_to,@version,'N')
Insert into CHANNEL_MAP_PROD_SERVICE_KEY_ATTRIBUTES_DESCRIPTION values('CHARACTER_28','PRIMARY_SALES_HOUSE','Primary Sales House',@effective_from,@effective_to,@version,'N')
Insert into CHANNEL_MAP_PROD_SERVICE_KEY_ATTRIBUTES_DESCRIPTION values('CHARACTER_29','CHANNEL_GROUP','Channel Group',@effective_from,@effective_to,@version,'N')
Insert into CHANNEL_MAP_PROD_SERVICE_KEY_ATTRIBUTES_DESCRIPTION values('CHARACTER_30','AMEND_DATE','Amend Date',@effective_from,@effective_to,@version,'N')
Insert into CHANNEL_MAP_PROD_SERVICE_KEY_ATTRIBUTES_DESCRIPTION values('CHARACTER_31','PAY_SPORTS_FLAG','Pay Sports Flag',@effective_from,@effective_to,@version,'N')
Insert into CHANNEL_MAP_PROD_SERVICE_KEY_ATTRIBUTES_DESCRIPTION values('CHARACTER_32','PAY_SKY_SPORTS_FLAG','Pay Sky Sports Flag',@effective_from,@effective_to,@version,'N')
Insert into CHANNEL_MAP_PROD_SERVICE_KEY_ATTRIBUTES_DESCRIPTION values('CHARACTER_33','PAY_TV_FLAG','Pay TV Flag',@effective_from,@effective_to,@version,'N')
Insert into CHANNEL_MAP_PROD_SERVICE_KEY_ATTRIBUTES_DESCRIPTION values('CHARACTER_34','KEY_PAY_ENTERTAINMENT_FLAG','Key Pay Entertainment Flag',@effective_from,@effective_to,@version,'N')
Insert into CHANNEL_MAP_PROD_SERVICE_KEY_ATTRIBUTES_DESCRIPTION values('CHARACTER_35','SKY_SPORTS_NEWS_FLAG','Sky Sports News Flag',@effective_from,@effective_to,@version,'N')
Insert into CHANNEL_MAP_PROD_SERVICE_KEY_ATTRIBUTES_DESCRIPTION values('CHARACTER_36','SKY_MOVIES_FLAG','Sky Movies Flag',@effective_from,@effective_to,@version,'N')
Insert into CHANNEL_MAP_PROD_SERVICE_KEY_ATTRIBUTES_DESCRIPTION values('CHARACTER_37','BT_SPORT_FLAG','BT Sport Flag',@effective_from,@effective_to,@version,'N')
Insert into CHANNEL_MAP_PROD_SERVICE_KEY_ATTRIBUTES_DESCRIPTION values('CHARACTER_38','CHARACTER_38','',@effective_from,@effective_to,@version,'N')
Insert into CHANNEL_MAP_PROD_SERVICE_KEY_ATTRIBUTES_DESCRIPTION values('CHARACTER_39','CHARACTER_39','',@effective_from,@effective_to,@version,'N')
Insert into CHANNEL_MAP_PROD_SERVICE_KEY_ATTRIBUTES_DESCRIPTION values('CHARACTER_40','CHARACTER_40','',@effective_from,@effective_to,@version,'N')
Insert into CHANNEL_MAP_PROD_SERVICE_KEY_ATTRIBUTES_DESCRIPTION values('CHARACTER_41','CHARACTER_41','',@effective_from,@effective_to,@version,'N')
Insert into CHANNEL_MAP_PROD_SERVICE_KEY_ATTRIBUTES_DESCRIPTION values('CHARACTER_42','CHARACTER_42','',@effective_from,@effective_to,@version,'N')
Insert into CHANNEL_MAP_PROD_SERVICE_KEY_ATTRIBUTES_DESCRIPTION values('CHARACTER_43','CHARACTER_43','',@effective_from,@effective_to,@version,'N')
Insert into CHANNEL_MAP_PROD_SERVICE_KEY_ATTRIBUTES_DESCRIPTION values('CHARACTER_44','CHARACTER_44','',@effective_from,@effective_to,@version,'N')
Insert into CHANNEL_MAP_PROD_SERVICE_KEY_ATTRIBUTES_DESCRIPTION values('CHARACTER_45','CHARACTER_45','',@effective_from,@effective_to,@version,'N')
Insert into CHANNEL_MAP_PROD_SERVICE_KEY_ATTRIBUTES_DESCRIPTION values('CHARACTER_46','CHARACTER_46','',@effective_from,@effective_to,@version,'N')
Insert into CHANNEL_MAP_PROD_SERVICE_KEY_ATTRIBUTES_DESCRIPTION values('CHARACTER_47','CHARACTER_47','',@effective_from,@effective_to,@version,'N')
Insert into CHANNEL_MAP_PROD_SERVICE_KEY_ATTRIBUTES_DESCRIPTION values('CHARACTER_48','CHARACTER_48','',@effective_from,@effective_to,@version,'N')
Insert into CHANNEL_MAP_PROD_SERVICE_KEY_ATTRIBUTES_DESCRIPTION values('CHARACTER_49','CHARACTER_49','',@effective_from,@effective_to,@version,'N')
Insert into CHANNEL_MAP_PROD_SERVICE_KEY_ATTRIBUTES_DESCRIPTION values('CHARACTER_50','CHARACTER_50','',@effective_from,@effective_to,@version,'N')
Insert into CHANNEL_MAP_PROD_SERVICE_KEY_ATTRIBUTES_DESCRIPTION values('CHARACTER_51','CHARACTER_51','',@effective_from,@effective_to,@version,'N')
Insert into CHANNEL_MAP_PROD_SERVICE_KEY_ATTRIBUTES_DESCRIPTION values('CHARACTER_52','CHARACTER_52','',@effective_from,@effective_to,@version,'N')
Insert into CHANNEL_MAP_PROD_SERVICE_KEY_ATTRIBUTES_DESCRIPTION values('CHARACTER_53','CHARACTER_53','',@effective_from,@effective_to,@version,'N')
Insert into CHANNEL_MAP_PROD_SERVICE_KEY_ATTRIBUTES_DESCRIPTION values('CHARACTER_54','CHARACTER_54','',@effective_from,@effective_to,@version,'N')
Insert into CHANNEL_MAP_PROD_SERVICE_KEY_ATTRIBUTES_DESCRIPTION values('CHARACTER_55','CHARACTER_55','',@effective_from,@effective_to,@version,'N')
Insert into CHANNEL_MAP_PROD_SERVICE_KEY_ATTRIBUTES_DESCRIPTION values('CHARACTER_56','CHARACTER_56','',@effective_from,@effective_to,@version,'N')
Insert into CHANNEL_MAP_PROD_SERVICE_KEY_ATTRIBUTES_DESCRIPTION values('CHARACTER_57','CHARACTER_57','',@effective_from,@effective_to,@version,'N')
Insert into CHANNEL_MAP_PROD_SERVICE_KEY_ATTRIBUTES_DESCRIPTION values('CHARACTER_58','CHARACTER_58','',@effective_from,@effective_to,@version,'N')
Insert into CHANNEL_MAP_PROD_SERVICE_KEY_ATTRIBUTES_DESCRIPTION values('CHARACTER_59','CHARACTER_59','',@effective_from,@effective_to,@version,'N')
Insert into CHANNEL_MAP_PROD_SERVICE_KEY_ATTRIBUTES_DESCRIPTION values('CHARACTER_60','CHARACTER_60','',@effective_from,@effective_to,@version,'N')
Insert into CHANNEL_MAP_PROD_SERVICE_KEY_ATTRIBUTES_DESCRIPTION values('CHARACTER_61','CHARACTER_61','',@effective_from,@effective_to,@version,'N')
Insert into CHANNEL_MAP_PROD_SERVICE_KEY_ATTRIBUTES_DESCRIPTION values('CHARACTER_62','CHARACTER_62','',@effective_from,@effective_to,@version,'N')
Insert into CHANNEL_MAP_PROD_SERVICE_KEY_ATTRIBUTES_DESCRIPTION values('CHARACTER_63','CHARACTER_63','',@effective_from,@effective_to,@version,'N')
Insert into CHANNEL_MAP_PROD_SERVICE_KEY_ATTRIBUTES_DESCRIPTION values('CHARACTER_64','CHARACTER_64','',@effective_from,@effective_to,@version,'N')
Insert into CHANNEL_MAP_PROD_SERVICE_KEY_ATTRIBUTES_DESCRIPTION values('CHARACTER_65','CHARACTER_65','',@effective_from,@effective_to,@version,'N')
Insert into CHANNEL_MAP_PROD_SERVICE_KEY_ATTRIBUTES_DESCRIPTION values('CHARACTER_66','CHARACTER_66','',@effective_from,@effective_to,@version,'N')
Insert into CHANNEL_MAP_PROD_SERVICE_KEY_ATTRIBUTES_DESCRIPTION values('CHARACTER_67','CHARACTER_67','',@effective_from,@effective_to,@version,'N')
Insert into CHANNEL_MAP_PROD_SERVICE_KEY_ATTRIBUTES_DESCRIPTION values('CHARACTER_68','CHARACTER_68','',@effective_from,@effective_to,@version,'N')
Insert into CHANNEL_MAP_PROD_SERVICE_KEY_ATTRIBUTES_DESCRIPTION values('CHARACTER_69','CHARACTER_69','',@effective_from,@effective_to,@version,'N')
Insert into CHANNEL_MAP_PROD_SERVICE_KEY_ATTRIBUTES_DESCRIPTION values('CHARACTER_70','CHARACTER_70','',@effective_from,@effective_to,@version,'N')
Insert into CHANNEL_MAP_PROD_SERVICE_KEY_ATTRIBUTES_DESCRIPTION values('CHARACTER_71','CHARACTER_71','',@effective_from,@effective_to,@version,'N')
Insert into CHANNEL_MAP_PROD_SERVICE_KEY_ATTRIBUTES_DESCRIPTION values('CHARACTER_72','CHARACTER_72','',@effective_from,@effective_to,@version,'N')
Insert into CHANNEL_MAP_PROD_SERVICE_KEY_ATTRIBUTES_DESCRIPTION values('CHARACTER_73','CHARACTER_73','',@effective_from,@effective_to,@version,'N')
Insert into CHANNEL_MAP_PROD_SERVICE_KEY_ATTRIBUTES_DESCRIPTION values('CHARACTER_74','CHARACTER_74','',@effective_from,@effective_to,@version,'N')
Insert into CHANNEL_MAP_PROD_SERVICE_KEY_ATTRIBUTES_DESCRIPTION values('CHARACTER_75','CHARACTER_75','',@effective_from,@effective_to,@version,'N')
Insert into CHANNEL_MAP_PROD_SERVICE_KEY_ATTRIBUTES_DESCRIPTION values('CHARACTER_76','CHARACTER_76','',@effective_from,@effective_to,@version,'N')
Insert into CHANNEL_MAP_PROD_SERVICE_KEY_ATTRIBUTES_DESCRIPTION values('CHARACTER_77','CHARACTER_77','',@effective_from,@effective_to,@version,'N')
Insert into CHANNEL_MAP_PROD_SERVICE_KEY_ATTRIBUTES_DESCRIPTION values('CHARACTER_78','CHARACTER_78','',@effective_from,@effective_to,@version,'N')
Insert into CHANNEL_MAP_PROD_SERVICE_KEY_ATTRIBUTES_DESCRIPTION values('CHARACTER_79','CHARACTER_79','',@effective_from,@effective_to,@version,'N')
Insert into CHANNEL_MAP_PROD_SERVICE_KEY_ATTRIBUTES_DESCRIPTION values('CHARACTER_80','CHARACTER_80','',@effective_from,@effective_to,@version,'N')
Insert into CHANNEL_MAP_PROD_SERVICE_KEY_ATTRIBUTES_DESCRIPTION values('CHARACTER_81','CHARACTER_81','',@effective_from,@effective_to,@version,'N')
Insert into CHANNEL_MAP_PROD_SERVICE_KEY_ATTRIBUTES_DESCRIPTION values('CHARACTER_82','CHARACTER_82','',@effective_from,@effective_to,@version,'N')
Insert into CHANNEL_MAP_PROD_SERVICE_KEY_ATTRIBUTES_DESCRIPTION values('CHARACTER_83','CHARACTER_83','',@effective_from,@effective_to,@version,'N')
Insert into CHANNEL_MAP_PROD_SERVICE_KEY_ATTRIBUTES_DESCRIPTION values('CHARACTER_84','CHARACTER_84','',@effective_from,@effective_to,@version,'N')
Insert into CHANNEL_MAP_PROD_SERVICE_KEY_ATTRIBUTES_DESCRIPTION values('CHARACTER_85','CHARACTER_85','',@effective_from,@effective_to,@version,'N')
Insert into CHANNEL_MAP_PROD_SERVICE_KEY_ATTRIBUTES_DESCRIPTION values('CHARACTER_86','CHARACTER_86','',@effective_from,@effective_to,@version,'N')
Insert into CHANNEL_MAP_PROD_SERVICE_KEY_ATTRIBUTES_DESCRIPTION values('CHARACTER_87','CHARACTER_87','',@effective_from,@effective_to,@version,'N')
Insert into CHANNEL_MAP_PROD_SERVICE_KEY_ATTRIBUTES_DESCRIPTION values('CHARACTER_88','CHARACTER_88','',@effective_from,@effective_to,@version,'N')
Insert into CHANNEL_MAP_PROD_SERVICE_KEY_ATTRIBUTES_DESCRIPTION values('CHARACTER_89','CHARACTER_89','',@effective_from,@effective_to,@version,'N')
Insert into CHANNEL_MAP_PROD_SERVICE_KEY_ATTRIBUTES_DESCRIPTION values('CHARACTER_90','CHARACTER_90','',@effective_from,@effective_to,@version,'N')
Insert into CHANNEL_MAP_PROD_SERVICE_KEY_ATTRIBUTES_DESCRIPTION values('CHARACTER_91','CHARACTER_91','',@effective_from,@effective_to,@version,'N')
Insert into CHANNEL_MAP_PROD_SERVICE_KEY_ATTRIBUTES_DESCRIPTION values('CHARACTER_92','CHARACTER_92','',@effective_from,@effective_to,@version,'N')
Insert into CHANNEL_MAP_PROD_SERVICE_KEY_ATTRIBUTES_DESCRIPTION values('CHARACTER_93','CHARACTER_93','',@effective_from,@effective_to,@version,'N')
Insert into CHANNEL_MAP_PROD_SERVICE_KEY_ATTRIBUTES_DESCRIPTION values('CHARACTER_94','CHARACTER_94','',@effective_from,@effective_to,@version,'N')
Insert into CHANNEL_MAP_PROD_SERVICE_KEY_ATTRIBUTES_DESCRIPTION values('CHARACTER_95','CHARACTER_95','',@effective_from,@effective_to,@version,'N')
Insert into CHANNEL_MAP_PROD_SERVICE_KEY_ATTRIBUTES_DESCRIPTION values('CHARACTER_96','CHARACTER_96','',@effective_from,@effective_to,@version,'N')
Insert into CHANNEL_MAP_PROD_SERVICE_KEY_ATTRIBUTES_DESCRIPTION values('CHARACTER_97','CHARACTER_97','',@effective_from,@effective_to,@version,'N')
Insert into CHANNEL_MAP_PROD_SERVICE_KEY_ATTRIBUTES_DESCRIPTION values('CHARACTER_98','CHARACTER_98','',@effective_from,@effective_to,@version,'N')
Insert into CHANNEL_MAP_PROD_SERVICE_KEY_ATTRIBUTES_DESCRIPTION values('CHARACTER_99','CHARACTER_99','',@effective_from,@effective_to,@version,'N')
Insert into CHANNEL_MAP_PROD_SERVICE_KEY_ATTRIBUTES_DESCRIPTION values('CHARACTER_100','CHARACTER_100','',@effective_from,@effective_to,@version,'N')

commit


--------------------------------
-- S2 - Closing Prior CM Version
--------------------------------

-- NYIP!!!

end;
commit;

grant execute on ska_description_fill to vespa_group_low_security;
commit;



