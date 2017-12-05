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
        S1 - Assembling SERVICE_KEY_CODE_MAPPING
		S2 - Assembling SERVICE_KEY_ATTRIBUTES_DESCRIPTION
		S3 - Assembling SERVICE_KEY_ATTRIBUTES

**Stats:
	
	1 Minutes run... End-to-End...
--------------------------------------------------------------------------------------------------------------
*/
--------------------------------
-- S0 - Initialising Environment
--------------------------------

create or replace procedure Channel_map_etl
as begin

	truncate table service_key_code_mapping
	truncate table service_key_attributes_description
	truncate table service_key_attributes
	
	commit
	MESSAGE cast(now() as timestamp)||' | @ S0 - Initialising Environment DONE!' TO CLIENT
	
	-------------------------------------------
	-- S1 - Assembling SERVICE_KEY_CODE_MAPPING
	-------------------------------------------

	insert  into service_key_code_mapping
	select  4       as record_type
			,ska.service_key
			,ska.spot_source    as type_
			,case   when ska.spot_source = 'BARB'       then skb.log_station_code
					when ska.spot_source = 'Landmark'   then skl.sare_no
					else null
			end     as code_1
			,case   when ska.spot_source = 'BARB' then skb.sti_code
					else null
			end     as code_2
			,case   when ska.spot_source = 'BARB' then skb.panel_code
					else null
			end     as code_3
			,case   when ska.spot_source = 'BARB' then skb.promo_panel_code
					else null
			end     as code_4
			,ska.effective_from
			,ska.effective_to
			,30     as code_mapping_version_number
			,null   as dummy_barb_code
	from    vespa_analysts.CHANNEL_MAP_DEV_SERVICE_KEY_ATTRIBUTES                       as ska
			left join vespa_analysts.CHANNEL_MAP_DEV_SERVICE_KEY_LANDMARK               as skl
			on  ska.service_key = skl.service_key
			left join vespa_analysts.vespa_analysts.CHANNEL_MAP_DEV_SERVICE_KEY_BARB    as skb
			on  ska.service_key = skb.service_key
	
	commit
	MESSAGE cast(now() as timestamp)||' | @ S1 - Assembling SERVICE_KEY_CODE_MAPPING DONE!' TO CLIENT
			
	-----------------------------------------------------
	-- S2 - Assembling SERVICE_KEY_ATTRIBUTES_DESCRIPTION
	-----------------------------------------------------

	Insert into service_key_attributes_description Values(4,'Attribute_1','FULL_NAME','Full Name',30)
	Insert into service_key_attributes_description Values(4,'Attribute_2','EPG_NUMBER','EPG Number',30)
	Insert into service_key_attributes_description Values(4,'Attribute_3','EPG_NAME','EPG Name',30)
	Insert into service_key_attributes_description Values(4,'Attribute_4','VESPA_NAME','Vespa Name',30)
	Insert into service_key_attributes_description Values(4,'Attribute_5','CHANNEL_NAME','Channel Name',30)
	Insert into service_key_attributes_description Values(4,'Attribute_6','TECHEDGE_NAME','Techedge Name',30)
	Insert into service_key_attributes_description Values(4,'Attribute_7','INFOSYS_NAME','Infosys Name',30)
	Insert into service_key_attributes_description Values(4,'Attribute_8','BARB_REPORTED','Barb Reported',30)
	Insert into service_key_attributes_description Values(4,'Attribute_9','ACTIVE','Active',30)
	Insert into service_key_attributes_description Values(4,'Attribute_10','CHANNEL_OWNER','Channel Owner',30)
	Insert into service_key_attributes_description Values(4,'Attribute_11','OLD_PACKAGING','Old Packaging',30)
	Insert into service_key_attributes_description Values(4,'Attribute_12','NEW_PACKAGING','New Packaging',30)
	Insert into service_key_attributes_description Values(4,'Attribute_13','PAY_FREE_INDICATOR','Pay/Free ',30)
	Insert into service_key_attributes_description Values(4,'Attribute_14','CHANNEL_GENRE','Channel Genre',30)
	Insert into service_key_attributes_description Values(4,'Attribute_15','CHANNEL_TYPE','Channel Type',30)
	Insert into service_key_attributes_description Values(4,'Attribute_16','FORMAT','Format',30)
	Insert into service_key_attributes_description Values(4,'Attribute_17','parent_service_key','Parent Service Key',30)
	Insert into service_key_attributes_description Values(4,'Attribute_18','TIMESHIFT_STATUS','Timeshift',30)
	Insert into service_key_attributes_description Values(4,'Attribute_19','TIMESHIFT_MINUTES','Minutes Shifted',30)
	Insert into service_key_attributes_description Values(4,'Attribute_20','RETAIL','Retail',30)
	Insert into service_key_attributes_description Values(4,'Attribute_21','CHANNEL_REACH','Channel Reach',30)
	Insert into service_key_attributes_description Values(4,'Attribute_22','HD_SWAP_EPG_NUMBER','HD Swap EPG Number',30)
	Insert into service_key_attributes_description Values(4,'Attribute_23','SENSITIVE_CHANNEL','Sensitive Flag',30)
	Insert into service_key_attributes_description Values(4,'Attribute_24','SPOT_SOURCE','Spot Source',30)
	Insert into service_key_attributes_description Values(4,'Attribute_25','PROMO_SOURCE','Promo Source',30)
	Insert into service_key_attributes_description Values(4,'Attribute_26','NOTES','Notes',30)
	Insert into service_key_attributes_description Values(4,'Attribute_27','EFFECTIVE_FROM','Effective From',30)
	Insert into service_key_attributes_description Values(4,'Attribute_28','EFFECTIVE_TO','Effective To',30)
	Insert into service_key_attributes_description Values(4,'Attribute_29','type_id','Type ID',30)
	Insert into service_key_attributes_description Values(4,'Attribute_30','UI_DESCR','UI Description',30)
	Insert into service_key_attributes_description Values(4,'Attribute_31','EPG_CHANNEL','EPG Channel',30)
	Insert into service_key_attributes_description Values(4,'Attribute_32','amend_date','Amended Date',30)
	Insert into service_key_attributes_description Values(4,'Attribute_33','channel_pack','Channel Pack',30)
	Insert into service_key_attributes_description Values(4,'Attribute_34','version','Version',30)
	Insert into service_key_attributes_description Values(4,'Attribute_35','primary_sales_house','Primary Sales House',30)
	Insert into service_key_attributes_description Values(4,'Attribute_36','channel_group','Channel Group',30)

	commit
	MESSAGE cast(now() as timestamp)||' | @ S2 - Assembling SERVICE_KEY_ATTRIBUTES_DESCRIPTION DONE!' TO CLIENT

	-----------------------------------------
	-- S3 - Assembling SERVICE_KEY_ATTRIBUTES
	-----------------------------------------
	declare @sql1       varchar(1000)
	declare @sql2       varchar(2000)
	declare @sql3       varchar(3000)
	declare @attribute  varchar(50)
	declare @sysname    varchar(100)

	set @sql1 = 'Insert into service_key_attributes ('
	set @sql2 = 'Select '

	declare thecursor cursor for
		
		select  attribute_field
				,attribute_system_name
		from    service_key_attributes_description

	for read only

	open thecursor
	fetch next thecursor into @attribute, @sysname

	while   (sqlstate = 0)
	begin

		set @sql1 = @sql1||@attribute||','
		set @sql2 = @sql2||'cast('||@sysname||' as varchar(200)),'
		fetch next thecursor into @attribute,@sysname

	end
	deallocate thecursor

	set @sql1 = @sql1||'service_key,provider_id,date_from,date_to,version_number)'
	set @sql2 = @sql2||'service_key,'''',cast(EFFECTIVE_FROM as varchar(200)),cast(EFFECTIVE_TO as varchar(200)),''30'' from vespa_analysts.channel_map_dev_service_key_attributes'
	set @sql3 = @sql1||' '||@sql2

	execute (@sql3)
	
	MESSAGE cast(now() as timestamp)||' | @ S3 - Assembling SERVICE_KEY_ATTRIBUTES DONE!' TO CLIENT
		
end;

commit;
grant execute on channel_map_etl to vespa_group_low_security;
commit;