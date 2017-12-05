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

        This script sets the basic tables for the CM Export...

**Tables:
	
		S1 - SERVICE_KEY_CODE_MAPPING
		S2 - SERVICE_KEY_ATTRIBUTES_DESCRIPTION
		S3 - SERVICE_KEY_ATTRIBUTES

**Stats:
	
	1 Minutes run... End-to-End...
--------------------------------------------------------------------------------------------------------------
*/

-------------------------------
--S1 - SERVICE_KEY_CODE_MAPPING
-------------------------------

if object_id ('service_key_code_mapping') is not null
	drop table service_key_code_mapping;
commit;

create table service_key_code_mapping	(

RECORD_TYPE						INTEGER		not null	default 4
,SERVICE_KEY					INTEGER		not null
,TYPE_							VARCHAR(10)	not null
,CODE_1							INTEGER		
,CODE_2							INTEGER		
,CODE_3							INTEGER		
,CODE_4							INTEGER		
,EFFECTIVE_FROM					TIMESTAMP	not null
,EFFECTIVE_TO					TIMESTAMP	not null
,CODE_MAPPING_VERSION_NUMBER	INTEGER		not null
,DUMMY_BARB_CODE				VARCHAR(3)

);
commit;

create hg 	index cm3_hg1 on service_key_code_mapping(service_key);
commit;

grant select on service_key_code_mapping to vespa_group_low_security;
commit;

------------------------------------------
-- S2 - SERVICE_KEY_ATTRIBUTES_DESCRIPTION
------------------------------------------

if object_id ('service_key_attributes_description') is not null
	drop table service_key_attributes_description;
commit;

create table service_key_attributes_description	(

RECORD_TYPE					INTEGER			not null	default 4
,ATTRIBUTE_FIELD			VARCHAR(50)		not null
,ATTRIBUTE_SYSTEM_NAME		VARCHAR(100)	not null
,ATTRIBUTE_FRIENDLY_NAME	VARCHAR(100)	not null
,VERSION_NUMBER				INTEGER			not null

);
commit;

create hg index cm2_hg1 on service_key_attributes_description(attribute_field);
create hg index cm2_hg2 on service_key_attributes_description(attribute_system_name);
commit;

grant select on service_key_attributes_description to vespa_group_low_security;
commit;

------------------------------
-- S3 - SERVICE_KEY_ATTRIBUTES
------------------------------

if object_id ('service_key_attributes') is not null
	drop table service_key_attributes;
commit;

create table service_key_attributes (
    RECORD_TYPE		INTEGER 		not null	default 4
	,SERVICE_KEY	INTEGER			not null
	,PROVIDER_ID    INTEGER
    ,DATE_FROM	    TIMESTAMP		not null
    ,DATE_TO	    TIMESTAMP		not null
    ,VERSION_NUMBER	INTEGER			not null
    ,attribute_1	VARCHAR(200)
    ,attribute_2	VARCHAR(200)
    ,attribute_3	VARCHAR(200)
    ,attribute_4	VARCHAR(200)
    ,attribute_5	VARCHAR(200)
    ,attribute_6	VARCHAR(200)
    ,attribute_7	VARCHAR(200)
    ,attribute_8	VARCHAR(200)
    ,attribute_9	VARCHAR(200)
    ,attribute_10	VARCHAR(200)
    ,attribute_11	VARCHAR(200)
    ,attribute_12	VARCHAR(200)
    ,attribute_13	VARCHAR(200)
    ,attribute_14	VARCHAR(200)
    ,attribute_15	VARCHAR(200)
    ,attribute_16	VARCHAR(200)
    ,attribute_17	VARCHAR(200)
    ,attribute_18	VARCHAR(200)
    ,attribute_19	VARCHAR(200)
    ,attribute_20	VARCHAR(200)
    ,attribute_21	VARCHAR(200)
    ,attribute_22	VARCHAR(200)
    ,attribute_23	VARCHAR(200)
    ,attribute_24	VARCHAR(200)
    ,attribute_25	VARCHAR(200)
    ,attribute_26	VARCHAR(200)
    ,attribute_27	VARCHAR(200)
    ,attribute_28	VARCHAR(200)
    ,attribute_29	VARCHAR(200)
    ,attribute_30	VARCHAR(200)
    ,attribute_31	VARCHAR(200)
    ,attribute_32	VARCHAR(200)
    ,attribute_33	VARCHAR(200)
    ,attribute_34	VARCHAR(200)
    ,attribute_35	VARCHAR(200)
    ,attribute_36	VARCHAR(200)
    ,attribute_37	VARCHAR(200)
    ,attribute_38	VARCHAR(200)
    ,attribute_39	VARCHAR(200)
    ,attribute_40	VARCHAR(200)
    ,attribute_41	VARCHAR(200)
    ,attribute_42	VARCHAR(200)
    ,attribute_43	VARCHAR(200)
    ,attribute_44	VARCHAR(200)
    ,attribute_45	VARCHAR(200)
    ,attribute_46	VARCHAR(200)
    ,attribute_47	VARCHAR(200)
    ,attribute_48	VARCHAR(200)
    ,attribute_49	VARCHAR(200)
    ,attribute_50	VARCHAR(200)
    ,attribute_51	VARCHAR(200)
    ,attribute_52	VARCHAR(200)
    ,attribute_53	VARCHAR(200)
    ,attribute_54	VARCHAR(200)
    ,attribute_55	VARCHAR(200)
    ,attribute_56	VARCHAR(200)
    ,attribute_57	VARCHAR(200)
    ,attribute_58	VARCHAR(200)
    ,attribute_59	VARCHAR(200)
    ,attribute_60	VARCHAR(200)
    ,attribute_61	VARCHAR(200)
    ,attribute_62	VARCHAR(200)
    ,attribute_63	VARCHAR(200)
    ,attribute_64	VARCHAR(200)
    ,attribute_65	VARCHAR(200)
    ,attribute_66	VARCHAR(200)
    ,attribute_67	VARCHAR(200)
    ,attribute_68	VARCHAR(200)
    ,attribute_69	VARCHAR(200)
    ,attribute_70	VARCHAR(200)
    ,attribute_71	VARCHAR(200)
    ,attribute_72	VARCHAR(200)
    ,attribute_73	VARCHAR(200)
    ,attribute_74	VARCHAR(200)
    ,attribute_75	VARCHAR(200)
    ,attribute_76	VARCHAR(200)
    ,attribute_77	VARCHAR(200)
    ,attribute_78	VARCHAR(200)
    ,attribute_79	VARCHAR(200)
    ,attribute_80	VARCHAR(200)
    ,attribute_81	VARCHAR(200)
    ,attribute_82	VARCHAR(200)
    ,attribute_83	VARCHAR(200)
    ,attribute_84	VARCHAR(200)
    ,attribute_85	VARCHAR(200)
    ,attribute_86	VARCHAR(200)
    ,attribute_87	VARCHAR(200)
    ,attribute_88	VARCHAR(200)
    ,attribute_89	VARCHAR(200)
    ,attribute_90	VARCHAR(200)
    ,attribute_91	VARCHAR(200)
    ,attribute_92	VARCHAR(200)
    ,attribute_93	VARCHAR(200)
    ,attribute_94	VARCHAR(200)
    ,attribute_95	VARCHAR(200)
    ,attribute_96	VARCHAR(200)
    ,attribute_97	VARCHAR(200)
    ,attribute_98	VARCHAR(200)
    ,attribute_99	VARCHAR(200)
    ,attribute_100	VARCHAR(200)
);
commit;

create hg 	index cm1_hg1 on service_key_attributes(provider_id);
create hg	index cm1_hg2 on service_key_attributes(service_key);
commit;

grant select on service_key_attributes to vespa_group_low_security;
commit;