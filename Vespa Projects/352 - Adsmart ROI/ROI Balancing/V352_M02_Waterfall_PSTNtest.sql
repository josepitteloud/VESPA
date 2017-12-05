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

-----------------------------------------------------------------------------------

**Project Name:                         Panel Balancing
**Analysts:                             Jon Green   (Jonathan.Green@skyiq.co.uk)
                                        Leonardo Ripoli
**Lead(s):                              Hoi Yu Tang (hoiyu.tang@skyiq.co.uk)
**Stakeholder:                          Jose Loureda
**Project Code (Insight Collation):     V306
**SharePoint Folder:                    http://sp-department.bskyb.com/sites/IQSKY/SIG/Insight%20Collation%20Documents/Forms/AllItems.aspx?RootFolder=%2Fsites%2FIQSKY%2FSIG%2FInsight%20Collation%20Documents%2F01%20Analysis%20Requests%2FV306%20-%20Foundation%20-%20Platform%20Maintenance%2FPhase%202%2FPanel%20Balancing

**Business Brief:

Panel Balancing is a regular exercise to ensure that the viewing panel is as
representative of the wider Sky customer base as possible as the latter evolves over
time. Balancing is also crucial in ensuring that key KPI and contractual obligations
of account coverage are maintained.




**Module:                              M02_Waterfall

This module, previously independent, is now integrated within the balancing process (although thois module could be run on its own)
It flags each account on the sky base against several checks.
If an account passes all the tests, it can be added to the panels. There are separate tests for PSTN and broadband.
Tests are flagged 1 for a pass, and 0 for a fail

** Genral tests performed:
  l07_prod_latest_dtv    Accounts must have a TV package
  l08_country            Must live in the UK
  l10_surname            Must have a surname on file
  l11_standard_accounts  Not Staff, etc.
  l13_hibernators        Not on the list of hibernators, maintained by the Decisioning team
  l14_not_vespa_panel    Must not be on a VESPA panel already
  l24_last_callback_dt   There must have been at least one successful Conditional Access callback in the last six months

** Broadband specific tests:
  l20_darwin             All boxes for the account must be Darwin

** PSTN specific tests:
  l22_known_prefix       It must be known whether there is a dialling prefix on the line
  l23_empty_prefix       There must not be a dialling prefix for the line
  l30_ondemand_downloads
  l31_singlebox

0 is fail
1 is pass

*/
  create or replace procedure V352_M02_Waterfall
         @general_schema bit    =       0
        ,@today          date   =       today()
      as begin

                   ---------------------
                   -- M02.0 - Initialise
                   ---------------------

           MESSAGE cast(now() as timestamp)||' | Waterfall M02.0 - Initialising Environment' TO CLIENT

                -- Prepare temp base table
            create table Temp_Waterfall_Base(
                   account_number           varchar(20) default null primary key
                  ,l07_prod_latest_dtv      bit         default 0
                  ,l08_country              bit         default 0
                  ,l10_surname              bit         default 0
                  ,l11_standard_accounts    bit         default 0
                  ,l13a_hibernators_pstn    bit         default 1
                  ,l13b_hibernators_bb      bit         default 1
                  ,l14_not_vespa_panel      bit         default 0
                  ,l20_darwin               varchar(3)  null
                  ,l22_known_prefix         bit         default 1
                  ,l23_empty_prefix         bit         default 1
                  ,l24_last_callback_dt     bit         default 1
                  ,l30_ondemand_downloads   bit         default 1
                  ,l31_singlebox            bit         default 0
                  ,knockout_level           smallint    default 0
                  ,knockout_level_ROI       smallint    default 0
                  ,knockout_level_ROI_PSTN  smallint    default 0
                  ,knockout_reason          varchar(50) null
                  ,knockout_reason_ROI      varchar(50) null
                  ,knockout_reason_ROI_PSTN varchar(50) null
                   )

                if object_id('waterfall_box_base') is not null begin
                    truncate table waterfall_box_base
               end
              else begin
                      create table waterfall_box_base(
                             account_number varchar(30) null
                            ,subscriber_id   int null
                            ,enable          varchar(7) null
                             )
               end

            create table temp_waterfall_box_rules(
                   account_number        varchar(30) null
                  ,subscriber_id         int null
                  ,service_instance_id   varchar(30) null
                  ,darwin                bit default 0
                  ,known_prefix          bit default 0
                  ,empty_prefix          bit default 0
                  ,last_callback_dt      bit default 0
                  ,last_dl_dt            bit default 0
                   )

            commit
            create hg index uhacc on temp_waterfall_box_rules(account_number)
            create hg index hgser on temp_waterfall_box_rules(service_instance_id)

           declare @6months_ago date

                    --------------------------------------
                    -- M02.1 - Begin populating base table
                    --------------------------------------

           MESSAGE cast(now() as timestamp)||' | Waterfall M02.1 - Begin populating base table' TO CLIENT

                -- Update flags for Active, UK, valid surname, and standard accounts
            insert into Temp_Waterfall_Base(
                   account_number
                  ,l07_prod_latest_dtv
                  ,l08_country
                  ,l10_surname
                  ,l11_standard_accounts
                   )
            select sav.account_number
                  ,min(case when sav.PROD_LATEST_DTV_STATUS = 'Active'        then 1 else 0 end) as l07_prod_latest_dtv
                  ,min(case when sav.fin_currency_code = 'EUR'                then 0 else 1 end) as l08_country
                  ,min(case when sav.CB_NAME_SURNAME_soundex IS NOT NULL and
                                 sav.CB_NAME_SURNAME_soundex <> ''            then 1 else 0 end) as l10_surname
                  ,min(case when sav.ACCT_TYPE_code = 'STD'                   then 1 else 0 end) as l11_standard_accounts
              from CUST_SINGLE_ACCOUNT_VIEW as sav
             where sav.CUST_ACTIVE_DTV = 1
          group by sav.account_number

                   --------------------------------------
                   -- M02.2 - Account-based rules
                   --------------------------------------

           MESSAGE cast(now() as timestamp)||' | Waterfall M02.2 - Account-based rules' TO CLIENT

                -- Rule 13 - idenfify hibernator accounts
            update Temp_Waterfall_Base as bas
               set l13a_hibernators_pstn = 0
              from vespa_analysts.panel_exclusions as exc
             where bas.account_number = exc.account_number
               and exclude_from like '%P%'

            update Temp_Waterfall_Base as bas
               set l13b_hibernators_bb = 0
              from vespa_analysts.panel_exclusions as exc
             where bas.account_number = exc.account_number
               and exclude_from like '%B%'
            commit

                -- Rule 14: - identify accounts that are NOT already on a panel

            update temp_Waterfall_Base
               set l14_not_vespa_panel = 1
             where account_number NOT IN (select account_number
                                            from vespa_panel_status
                                           where panel_no in (2, 5, 7, 10, 11, 15)
                                         )

            commit

                   --------------------------------------
                   -- M02.3 - STB-based rules
                   --------------------------------------

           MESSAGE cast(now() as timestamp)||' | Waterfall M02.3 - STB-based rules' TO CLIENT

                -- Identify active STBs
           MESSAGE cast(now() as timestamp)||' | Waterfall M02.3 - STB-based rules - Identify active STBs' TO CLIENT

            insert into temp_waterfall_box_rules(
                   account_number
                  ,service_instance_id
                   )
            select account_number
                  ,service_instance_id
              from cust_set_top_box as stb
             where x_active_box_flag_new = 'Y'

                -- Add subscriber_id to those STBs
           MESSAGE cast(now() as timestamp)||' | Waterfall M02.3 - STB-based rules - Add subscriber_id to those STBs' TO CLIENT

            update temp_waterfall_box_rules as bas
               set bas.subscriber_id = csi.si_external_identifier
              from cust_service_instance as csi
             where csi.src_system_id = bas.service_instance_id
               and effective_to_dt = '9999-09-09'

                -- Get Subscriber Ids allocated to multiple Account Numbers
           MESSAGE cast(now() as timestamp)||' | Waterfall M02.3 - STB-based rules - Identify subscribers allocated to multiple accounts' TO CLIENT

            select subscriber_id
                  ,count(*) as cow
              into temp_Ambiguous_Sub_Ids
              from temp_waterfall_box_rules
          group by subscriber_id
            having cow > 1

                -- Delete these accounts
           MESSAGE cast(now() as timestamp)||' | Waterfall M02.3 - STB-based rules - Removing subscribers allocated to multiple accounts' TO CLIENT

            delete from temp_waterfall_box_rules
             where account_number in (
                      select account_number
                        from temp_Ambiguous_Sub_Ids as amb
                             inner join temp_waterfall_box_rules as box on amb.subscriber_id = box.subscriber_id
                                      )

                -- prefixes - null or empty (Rules 22 & 23)
           MESSAGE cast(now() as timestamp)||' | Waterfall M02.3 - STB-based rules - Identify boxes with NULL or empty dialling prefixes' TO CLIENT

--use this variable to check last callback route from CA list (ROI accounts only)
update temp_waterfall_box_rules as bas
   set bas.known_prefix = 1
  from greenj.ca_roi as car
 where bas.subscriber_id = cast(car.subscriber_id as int)
   and car.rt like 'PSTN%'
   and car.dt > '2016-02-10'

            update temp_waterfall_box_rules as bas
--               set bas.known_prefix       = case when cb.subscriber_id is not null then 1 else 0 end           -- Callback record present for the box
               set bas.empty_prefix       = case when trim(prefix) = '' or prefix is null then 1 else 0 end
              from vespa_analysts.Waterfall_callback_data  cb
             where bas.subscriber_id = cb.subscriber_id
               and cb.callback_seq = 1


                -- Identify active Darwin STBs
           MESSAGE cast(now() as timestamp)||' | Waterfall M02.3 - STB-based rules - Identify active Darwin STBs' TO CLIENT

            update temp_waterfall_box_rules as bas
               set bas.darwin = case when  x_model_number like 'DRX 89%'
                                       or  x_manufacturer = 'Samsung'
                                       or (stb.x_manufacturer = 'Pace' and stb.x_pvr_type = 'PVR4') then 1 else 0 end
              from cust_Set_top_box as stb
             where bas.service_instance_id = stb.service_instance_id
               and x_active_box_flag_new = 'Y'

                -- the prefix issue is now fixed for these 2 boxes
           MESSAGE cast(now() as timestamp)||' | Waterfall M02.3 - STB-based rules - Prefix issue fixed for Darwin' TO CLIENT

            update temp_waterfall_box_rules as bas
               set bas.empty_prefix       = 1
             where darwin = 1

                -- last_callback_dt in the last 6 months (Rule 24)
           MESSAGE cast(now() as timestamp)||' | Waterfall M02.3 - STB-based rules - Calculate last CA callback day' TO CLIENT

                -- Callbacks - fail the test if there were none in the last 6 months
           MESSAGE cast(now() as timestamp)||' | Waterfall M02.3 - STB-based rules - Callbacks - not all missing from the last 6 months' TO CLIENT

               set @6months_ago = dateadd(month, -6, @today)

            select si_external_identifier
                  ,max(last_callback_dt) as dt
              into temp_lastcall
              from cust_stb_callback_summary
          group by si_external_identifier
            having dt > @6months_ago

            commit
            create unique hg index uhsub on temp_lastcall(si_external_identifier)

            update temp_waterfall_box_rules as bas
               set last_callback_dt = 1
              from temp_lastcall as cal
             where bas.subscriber_id = cast(cal.si_external_identifier as int)

                -- On Demand downloads (by box)
           MESSAGE cast(now() as timestamp)||' | Waterfall M02.3 - STB-based rules - Determine most recent OnDemand download by card_id' TO CLIENT

            create table temp_dl_by_box(
                   card_id             varchar(30) null
                  ,service_instance_id varchar(30) null
                   )

            insert into temp_dl_by_box(
                   card_id
                   )
            select card_id
              from temp_waterfall_box_rules as bas
                   inner join CUST_ANYTIME_PLUS_DOWNLOADS as apd on last_modified_dt > @6months_ago
                                                                and bas.account_number = apd.account_number
          group by card_id

            commit
            create unique hg index uhcar on temp_dl_by_box(card_id)

           MESSAGE cast(now() as timestamp)||' | Waterfall M02.3 - STB-based rules - Add service_instance_id' TO CLIENT

            update temp_dl_by_box as bas
               set bas.service_instance_id = cid.service_instance_id
              from cust_card_issue_dim as cid
             where bas.card_id = left(cid.card_id, 8)
               and card_status = 'Enabled'

                -- if there has been an on demand download in the last 6 months (by box)
           MESSAGE cast(now() as timestamp)||' | Waterfall M02.3 - STB-based rules - Flag if downloaded within the last 6 months' TO CLIENT

            update temp_waterfall_box_rules as bas
               set last_dl_dt = 1
              from temp_dl_by_box as dls
             where bas.service_instance_id = dls.service_instance_id


                   ---------------------------------------------------
                   -- M02.4 - Apply box info & apply rules to accounts
                   ---------------------------------------------------

           MESSAGE cast(now() as timestamp)||' | Waterfall M02.4 - Apply box info & apply rules to accounts' TO CLIENT


            update Temp_Waterfall_Base as bas
               set bas.l22_known_prefix = 0         -- ALL boxes must have known prefix information
              from temp_waterfall_box_rules as box
             where bas.account_number = box.account_number
               and box.known_prefix = 0
               and l08_country = 0

            update Temp_Waterfall_Base as bas
               set bas.l23_empty_prefix = 1         -- ALL boxes must have no prefix
              from temp_waterfall_box_rules as box
             where bas.account_number = box.account_number
               and box.empty_prefix = 0

            update Temp_Waterfall_Base as bas
               set bas.l24_last_callback_dt = 0     -- ALL boxes must have a callback
              from temp_waterfall_box_rules as box
             where bas.account_number = box.account_number
               and box.last_callback_dt = 0

            update Temp_Waterfall_Base as bas
               set bas.l30_ondemand_downloads = 0   -- Each box must have an on demand download in the last 6 months
              from temp_waterfall_box_rules as box
             where bas.account_number = box.account_number
               and box.last_dl_dt = 0

                -- Rule 20 - Darwin
            select account_number
                  ,count(1) as boxes
                  ,sum(darwin) as darwin
              into temp_darwin
              from temp_waterfall_box_rules as box
          group by account_number

            update Temp_Waterfall_Base as bas
               set l20_darwin = case when darwin = boxes then 'Yes'
                                     when darwin = 0     then 'No'
                                     else                     'Mix'
                                end
              from temp_darwin as box
             where bas.account_number = box.account_number

                   ---------------------------------------------------
                   -- M02.5 - Calculate Waterfall knockout levels
                   ---------------------------------------------------

           MESSAGE cast(now() as timestamp)||' | Waterfall M02.5 - Calculate Waterfall knockout levels' TO CLIENT

            update temp_Waterfall_Base
               set knockout_level  =      case when l07_prod_latest_dtv        = 0 then 7
                                               when l08_country                = 0 then 8
                                               when l10_surname                = 0 then 10
                                               when l11_standard_accounts      = 0 then 11

                                               when l13b_hibernators_bb        = 0 then 13
                                               when l14_not_vespa_panel        = 0 then 14
                                               when l20_darwin            <> 'Yes' then 20
                                               when l24_last_callback_dt       = 0 then 24
                                               when l30_ondemand_downloads     = 0 then 130
                                               else                                     9999 end -- pass!
                  ,knockout_level_ROI =   case when l07_prod_latest_dtv        = 0 then 7
                                               when l08_country                = 1 then 8
                                               when l10_surname                = 0 then 10
                                               when l11_standard_accounts      = 0 then 11

                                               when l13b_hibernators_bb        = 0 then 13
                                               when l14_not_vespa_panel        = 0 then 14
                                               when l20_darwin            <> 'Yes' then 20
                                               when l24_last_callback_dt       = 0 then 24
                                               when l30_ondemand_downloads     = 0 then 130
                                               else                                     9999 end -- pass!
                  ,knockout_level_ROI_PSTN  = case
                                               when l07_prod_latest_dtv        = 0 then 7
                                               when l08_country                = 1 then 8
                                               when l10_surname                = 0 then 10
                                               when l11_standard_accounts      = 0 then 11

                                               when l14_not_vespa_panel        = 0 then 14
                                               when l24_last_callback_dt       = 0 then 24
                                               when l13a_hibernators_pstn      = 0 then 113
                                               when l22_known_prefix           = 0 then 122
                                               when l23_empty_prefix           = 0 then 123
                                               else                                     9999 end -- pass!

            insert into waterfall_box_base(
                   account_number
                  ,subscriber_id
                  ,enable)
            select bas.account_number
                  ,subscriber_id
                  ,case when last_dl_dt             = 1
                         and l13b_hibernators_bb    = 1 then 'BB'
                        when l13a_hibernators_pstn  = 1
                         and known_prefix           = 1
                         and empty_prefix           = 1 then 'PSTN'
                        else                                 'Neither' end
              from Temp_Waterfall_Base as bas
                   left join temp_waterfall_box_rules as box on bas.account_number = box.account_number
             where l07_prod_latest_dtv    = 1
               and l08_country            = 1
               and l10_surname            = 1
               and l11_standard_accounts  = 1
               and l14_not_vespa_panel    = 1
               and l24_last_callback_dt   = 1
               and knockout_level         < 9999

                -- count boxes that can be pstn/ bb by account
            select account_number
                  ,sum(case when enable = 'Neither' then 1 else 0 end) as neither
                  ,sum(case when enable = 'PSTN'    then 1 else 0 end) as pstn
              into temp_waterfall_box_base_accounts
              from waterfall_box_base
          group by account_number

            commit
            create unique hg index uhacc on temp_waterfall_box_base_accounts(account_number)

            update Temp_Waterfall_Base as bas
               set knockout_level = 9999
              from temp_waterfall_box_base_accounts as box
             where bas.account_number = box.account_number
               and knockout_level = 131
               and neither = 0
               and pstn = 0

            update Temp_Waterfall_Base as bas
               set knockout_reasonb = case knockout_level when 7   then 'DTV account'
                                                          when 8   then 'Country'
                                                          when 10  then 'Surname'
                                                          when 11  then 'Standard_accounts'
                                                          when 13  then 'Hibernators'
                                                          when 14  then 'Not_vespa_panel'
                                                          when 15  then 'Sky_view_panel'
                                                          when 20  then 'Darwin'
                                                          when 24  then 'Last_callback_dt'
                                                          when 130 then 'On Demand downloads'
                                                          else          'Potential BB panellist'
                                        end
                 ,knockout_reason_roi = case knockout_level_roi when 7   then 'DTV account'
                                                                when 8   then 'Country'
                                                                when 10  then 'Surname'
                                                                when 11  then 'Standard_accounts'
                                                                when 13  then 'Hibernators'
                                                                when 14  then 'Not_vespa_panel'
                                                                when 15  then 'Sky_view_panel'
                                                                when 20  then 'Darwin'
                                                                when 24  then 'Last_callback_dt'
                                                                when 130 then 'On Demand downloads'
                                                                else          'Potential ROI panellist'
                                         end
                 ,knockout_reason_roi_pstn = case knockout_level_roi_pstn when 7   then 'DTV account'
                                                                          when 8   then 'Country'
                                                                          when 10  then 'Surname'
                                                                          when 11  then 'Standard_accounts'
                                                                          when 14  then 'Not_vespa_panel'
                                                                          when 15  then 'Sky_view_panel'
                                                                          when 20  then 'Darwin'
                                                                          when 24  then 'Last_callback_dt'
                                                                          when 113 then 'Hibernators'
                                                                          when 122 then 'Prefix information unknown'
                                                                          when 123 then 'Empty prefix'
                                                                          else          'Potential ROI PSTN panellist'
                                          end

                   ---------------------------------------------------
                   -- M02.6 - Save output waterfall_base table
                   ---------------------------------------------------

           MESSAGE cast(now() as timestamp)||' | Waterfall M02.6 - Save output waterfall_base table' TO CLIENT

                if @general_schema = 0 begin

                          -- Create Waterfall_Base table if necessary (for standalone execution of this procedure)
                          if object_id('Waterfall_Base') is null begin
                                create table Waterfall_Base(
                                       account_number           varchar(20) default null primary key
                                      ,l07_prod_latest_dtv      bit         default 0
                                      ,l08_country              bit         default 0
                                      ,l10_surname              bit         default 0
                                      ,l11_standard_accounts    bit         default 0
                                      ,l13a_hibernators_pstn    bit         default 0
                                      ,l13b_hibernators_bb      bit         default 0
                                      ,l14_not_vespa_panel      bit         default 0
                                      ,l15_sky_view_panel       bit         default 0
                                      ,l20_darwin               varchar(3) null
                                      ,l22_known_prefix         bit         default 1
                                      ,l23_empty_prefix         bit         default 1
                                      ,l24_last_callback_dt     bit         default 1
                                      ,l30_ondemand_downloads   bit         default 1
                                      ,knockout_level           smallint    default 0 null
                                      ,knockout_level_ROI       smallint    default 0 null
                                      ,knockout_level_ROI_PSTN  smallint    default 0 null
                                      ,knockout_reason          varchar(50) null
                                      ,knockout_reason_ROI      varchar(50) null
                                      ,knockout_reason_ROI_PSTN varchar(50) null
                                       )
                             commit
                             grant select on waterfall_base to vespa_group_low_security

                         end

                    truncate table waterfall_base
                      commit

                      insert into waterfall_base(
                             account_number
                            ,l07_prod_latest_dtv
                            ,l08_country
                            ,l10_surname
                            ,l11_standard_accounts
                            ,l13a_hibernators_pstn
                            ,l13b_hibernators_bb
                            ,l14_not_vespa_panel
                            ,l20_darwin
                            ,l22_known_prefix
                            ,l23_empty_prefix
                            ,l24_last_callback_dt
                            ,l30_ondemand_downloads
                            ,knockout_level
                            ,knockout_level_ROI
                            ,knockout_level_ROI_PSTN
                            ,knockout_reason
                            ,knockout_reason_ROI
                            ,knockout_reason_ROI_PSTN
                             )
                      select account_number
                            ,l07_prod_latest_dtv
                            ,l08_country
                            ,l10_surname
                            ,l11_standard_accounts
                            ,l13a_hibernators_pstn
                            ,l13b_hibernators_bb
                            ,l14_not_vespa_panel
                            ,l20_darwin
                            ,l22_known_prefix
                            ,l23_empty_prefix
                            ,l24_last_callback_dt
                            ,l30_ondemand_downloads
                            ,knockout_level
                            ,knockout_level_ROI
                            ,knockout_level_ROI_PSTN
                            ,knockout_reason
                            ,knockout_reason_ROI
                            ,knockout_reason_ROI_PSTN
                        from Temp_Waterfall_Base

                             commit

               end
              else begin
                          if object_id('vespa_analysts.Waterfall_Base') is not null begin
                               execute('call dba.sp_drop_table (''vespa_analysts'',''Waterfall_Base'')')
                               execute('call dba.sp_create_table (''vespa_analysts'',''Waterfall_Base'',''
                                             account_number           varchar(20) default null primary key
                                            ,l07_prod_latest_dtv      bit         default 0
                                            ,l08_country              bit         default 0
                                            ,l10_surname              bit         default 0
                                            ,l11_standard_accounts    bit         default 0
                                            ,l13a_hibernators_pstn    bit         default 0
                                            ,l13b_hibernators_bb      bit         default 0
                                            ,l14_not_vespa_panel      bit         default 0
                                            ,l20_darwin               varchar(3) null
                                            ,l22_known_prefix         bit         default 1
                                            ,l23_empty_prefix         bit         default 1
                                            ,l24_last_callback_dt     bit         default 1
                                            ,l30_ondemand_downloads   bit         default 1
                                            ,knockout_level           smallint    default 0 null
                                            ,knockout_level_ROI       smallint    default 0 null
                                            ,knockout_level_ROI_PSTN  smallint    default 0 null
                                            ,knockout_reason          varchar(50) null
                                            ,knockout_reason_ROI      varchar(50) null
                                            ,knockout_reason_ROI_PSTN varchar(50) null''
                                       )')

                                insert into vespa_analysts.waterfall_base(
                                       account_number
                                      ,l07_prod_latest_dtv
                                      ,l08_country
                                      ,l10_surname
                                      ,l11_standard_accounts
                                      ,l13a_hibernators_pstn
                                      ,l13b_hibernators_bb
                                      ,l14_not_vespa_panel
                                      ,l20_darwin
                                      ,l22_known_prefix
                                      ,l23_empty_prefix
                                      ,l24_last_callback_dt
                                      ,l30_ondemand_downloads
                                      ,knockout_level
                                      ,knockout_level_ROI
                                      ,knockout_level_ROI_PSTN
                                      ,knockout_reason
                                      ,knockout_reason_ROI
                                      ,knockout_reason_ROI_PSTN
                                       )
                                select account_number
                                      ,l07_prod_latest_dtv
                                      ,l08_country
                                      ,l10_surname
                                      ,l11_standard_accounts
                                      ,l13a_hibernators_pstn
                                      ,l13b_hibernators_bb
                                      ,l14_not_vespa_panel
                                      ,l20_darwin
                                      ,l22_known_prefix
                                      ,l23_empty_prefix
                                      ,l24_last_callback_dt
                                      ,l30_ondemand_downloads
                                      ,knockout_level
                                      ,knockout_level_ROI
                                      ,knockout_level_ROI_PSTN
                                      ,knockout_reason
                                      ,knockout_reason_ROI
                                      ,knockout_reason_ROI_PSTN
                                  from Temp_Waterfall_Base
                         end
               end
             commit

               drop table temp_Waterfall_Base
               drop table temp_waterfall_box_rules
               drop table temp_Ambiguous_Sub_Ids
               drop table temp_lastcall
               drop table temp_dl_by_box
               drop table temp_darwin
               drop table temp_waterfall_box_base_accounts

            MESSAGE cast(now() as timestamp)||' | Waterfall M02 - DONE' TO CLIENT

     end; -- V352_M02_Waterfall procedure
  commit;

   grant execute on V352_M02_Waterfall to vespa_group_low_security;
 commit;


