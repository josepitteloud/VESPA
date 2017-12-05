/*###############################################################################
# Created on:   25/07/2016
# Created by:   Sebastian Bednaszynski(SBE)
# Description:  Channel Mapping process - VESPA-BARB lookup maintenance
#
#################################################################################
# Dependencies
# ------------------------------------------------------------------------------
# => Tables/objects required:
#
#################################################################################
# Change log
# ------------------------------------------------------------------------------
# 25/07/2016  SBE   Initial version
#
###############################################################################*/


  -- ##############################################################################################################
  -- ##############################################################################################################
  -- ##### BARB - VESPA lookup maintenance                                                                    #####
  -- ##############################################################################################################
  -- ##############################################################################################################
  -- Insert new channels from SI
/*
insert into CM_10_BARB_SI_Channel_Lookup
      (SI_Service_Key, SI_Name, SI_UI_Descr, SI_Channel_Group_Id, SI_Format, SI_Country, SI_Timeshift_Minutes,
       Link_Creation_Date, Link_Expiry_Date, Link_Status)
  select
        si.SI_SERVICE_KEY,
        max(si.NAME),
        max(si.UI_DESCR),
        max(si.CHANNEL_GROUP_ID),
        max(si.xFormat),
        max(si.xCountry),
        max(si.xTimeshift_Minutes),
        today(),
        today(),
        'Inactive'
    from CM_01_Service_Integration_Feed si left join CM_10_BARB_SI_Channel_Lookup bold
          on si.SI_SERVICE_KEY = bold.SI_Service_Key
   where bold.SI_Service_Key is null
     and si.SI_Type <> 'Radio'                                                                                                        -- [!!!] CONFIRM RULES
   group by si.SI_SERVICE_KEY;
commit;


  -- Deactivate links for Sks which are not longer provided in SI extract or BARB side
update CM_10_BARB_SI_Channel_Lookup base
   set base.Link_Expiry_Date  = today(),
       base.Link_Status       = 'Expired'
 where base.SI_Service_Key not in (select
                                         SI_SERVICE_KEY
                                     from CM_01_Service_Integration_Feed);
commit;


  -- Try matching on NAME
update CM_10_BARB_SI_Channel_Lookup base
   set base.BARB_Station_Identifier   = barb.Station_Identifier,
       base.BARB_Station_Name         = barb.Log_Station_Name,
       base.Link_Source               = 'si.Name',
       base.Link_Expiry_Date          = '2999-12-31',
       base.Link_Status               = 'Active'
  from CM_05_BARB_Feed barb
 where trim(lower(replace(base.SI_Name, ' ', ''))) = barb.Station_Identifier
   and base.Link_Source = '???';
commit;


  -- Try matching on UI_DESCR
update CM_10_BARB_SI_Channel_Lookup base
   set base.BARB_Station_Identifier   = barb.Station_Identifier,
       base.BARB_Station_Name         = barb.Log_Station_Name,
       base.Link_Source               = 'si.UI_Descr',
       base.Link_Expiry_Date          = '2999-12-31',
       base.Link_Status               = 'Active'
  from CM_05_BARB_Feed barb
 where trim(lower(replace(base.SI_UI_Descr, ' ', ''))) = barb.Station_Identifier
   and base.Link_Source = '???';
commit;
*/

-- [!!!]





  -- ##############################################################################################################
  -- ##############################################################################################################





























