--March 2013 converted generic waterfall into procedure in Vespa Analysts
--12/06/13 - Jon - This procedure is not current in Vespa_analysts, due to access issues. In the meantime, this procedure can be created in your own schema.
--09/07/13 - It can now be run from any schema, and will update the vespa_analysts tables.
--07/04/13 - Jon - added split into PSTN, BB and MR. Also tidied up, removed unused tests, and eliminated prefix rule for all Darwin boxes


/*
call dba.sp_drop_table('vespa_analysts', 'Waterfall_Base');
call dba.sp_create_table('vespa_analysts',
                         'Waterfall_Base',
                         'account_number         varchar(20) default null primary key
                         ,l07_prod_latest_dtv    bit         default 0
                         ,l08_country            bit         default 0
                         ,l10_surname            bit         default 0
                         ,l11_standard_accounts  bit         default 0
                         ,l13_hibernators        bit         default 0
                         ,l14_not_vespa_panel    bit         default 0
                         ,l15_sky_view_panel     bit         default 0
                         ,l20_darwin             varchar(3)
                         ,l22_known_prefix       bit         default 1
                         ,l23_empty_prefix       bit         default 1
                         ,l24_last_callback_dt   bit         default 1
                         ,l25_drx595             bit         default 1
                         ,l30_ondemand_downloads bit         default 1
                         ,l31_singlebox          bit         default 0
                         ,knockout_level_PSTN    smallint    default 0
                         ,knockout_level_BB      smallint    default 0
                         ,knockout_level_mix     smallint    default 0
                         ,knockout_reason_PSTN   varchar(50)
                         ,knockout_reason_BB     varchar(50)
                         ,knockout_reason_mix    varchar(50)
');

call dba.sp_drop_table('vespa_analysts','waterfall_box_base');
call dba.sp_create_table('vespa_analysts'
                        ,'waterfall_box_base'
                        ,'account_number varchar(30)
                         ,subscriber_id  int
                         ,enable         varchar(7)
')
                                 */
drop procedure waterfall;
create procedure waterfall as begin

       create table #waterfall_box_rules
             (account_number        varchar(30)
             ,subscriber_id         int
             ,service_instance_id   varchar(30)
             ,darwin                bit default 0
             ,drx595                bit default 0
             ,known_prefix          bit default 0
             ,empty_prefix          bit default 0
             ,last_callback_dt      bit default 0
             ,last_dl_dt            bit default 0)

     truncate table vespa_analysts.waterfall_base
     truncate table vespa_analysts.waterfall_box_base
       commit
       create hg index uhacc on #waterfall_box_rules(account_number)
       create hg index hgser on #waterfall_box_rules(service_instance_id)

       insert into vespa_analysts.waterfall_base
             (account_number
             ,l07_prod_latest_dtv
             ,l08_country
             ,l10_surname
             ,l11_standard_accounts)
       select sav.account_number
             ,min(case when sav.PROD_LATEST_DTV_STATUS = 'Active'        then 1 else 0 end) as l07_prod_latest_dtv
             ,min(case when sav.PTY_COUNTRY_CODE = 'GBR'                 then 1 else 0 end) as l08_country
             ,min(case when sav.CB_NAME_SURNAME IS NOT NULL and
                            sav.CB_NAME_SURNAME <> ''                    then 1 else 0 end) as l10_surname
             ,min(case when sav.ACCT_TYPE_code = 'STD'                   then 1 else 0 end) as l11_standard_accounts
         from sk_prod.CUST_SINGLE_ACCOUNT_VIEW as sav
        where sav.CUST_ACTIVE_DTV = 1
        group by sav.account_number

           -- exclusion due to a misdial
       update vespa_analysts.waterfall_base
          set l11_standard_accounts = 0
        where account_number = '620023218832'


           -- ##### Account based rules #####
           -- Rule 13
       update vespa_analysts.Waterfall_Base
          set l13_hibernators = 1
        where ACCOUNT_NUMBER not IN (select account_number from sk_prod.VESPA_PANEL_HIBERNATIONS)
       commit

           -- Rule 14:
       update vespa_analysts.Waterfall_Base
          set l14_not_vespa_panel = 1
        where ACCOUNT_NUMBER NOT IN (SELECT VESPA_PANEL_STATUS.ACCOUNT_NUMBER FROM sk_prod.VESPA_PANEL_STATUS WHERE VESPA_PANEL_STATUS.PANEL_NO in (5, 6, 7, 11, 12))
       commit

           -- Rule 15:
       update vespa_analysts.Waterfall_Base
          set l15_sky_view_panel = 1
        where ACCOUNT_NUMBER NOT IN (SELECT ACCOUNT_NUMBER FROM sk_prod.VESPA_SKY_VIEW_PANEL)
       commit

           -- ##### Box based rules #####
       insert into #waterfall_box_rules
             (account_number
             ,subscriber_id
             ,service_instance_id)
       select account_number
             ,cast(card_subscriber_id as int) as subscriber_id
             ,service_instance_id
         from sk_prod.cust_card_subscriber_link
        where current_flag = 'Y'
     group by account_number
             ,card_subscriber_id
             ,service_instance_id

           -- Get Subscriber Ids allocated to multiple Account Numbers
       select subscriber_id
             ,count(*) as cow
         into #Ambiguous_Sub_Ids
         from #waterfall_box_rules
     group by subscriber_id
       having cow > 1

           -- Delete these accounts
       delete from #waterfall_box_rules
        where account_number in (select account_number
                                   from #Ambiguous_Sub_Ids as amb
                                        inner join #waterfall_box_rules as box on amb.subscriber_id = box.subscriber_id)

           -- prefixes - null or empty (Rules 22 & 23)
       update #waterfall_box_rules as bas
          set bas.known_prefix       = case when cb.subscriber_id is not null then 1 else 0 end           -- Callback record present for the box
             ,bas.empty_prefix       = case when trim(prefix) = '' or prefix is null then 1 else 0 end
         from vespa_analysts.Waterfall_callback_data  cb
        where bas.subscriber_id = cb.subscriber_id
          and cb.callback_seq = 1

       update #waterfall_box_rules as bas
          set bas.darwin = case when  x_model_number like 'DRX 89%'
                                  or  x_manufacturer = 'Samsung'
                                  or (stb.x_manufacturer = 'Pace' and stb.x_pvr_type = 'PVR4') then 1 else 0 end
             ,bas.drx595 = case when  x_model_number = 'DRX 595' then 0 else 1 end
         from sk_prod.cust_Set_top_box as stb
        where bas.service_instance_id = stb.service_instance_id
          and x_active_box_flag_new = 'Y'

           -- the prefix issue is now fixed for these 2 boxes
       update #waterfall_box_rules as bas
          set bas.known_prefix       = 1
             ,bas.empty_prefix       = 1
        where darwin = 1

           -- last_callback_dt in the last 6 months (Rule 24)
      declare @varLastCallbackDate date
       select @varLastCallbackDate = max(dt)
         from vespa_analysts.Waterfall_callback_data
        where dt < now()

           -- Callbacks - not all missing from the last 6 months
       update #waterfall_box_rules as bas
          set last_callback_dt = 1
         from vespa_analysts.Waterfall_SCMS_callback_data as gol
        where bas.subscriber_id = gol.subscriber_id
          and Missing_Cbcks < Expected_Cbcks

       create table #dl_by_box(
              card_id             varchar(30)
             ,service_instance_id varchar(30)
             ,max_dt              date)

           -- On Demand downloads (by box)
       insert into #dl_by_box(
              card_id
             ,max_dt)
       select card_id
             ,max(last_modified_dt) as max_dt
         from #waterfall_box_rules as bas
              inner join CUST_ANYTIME_PLUS_DOWNLOADS as apd on bas.account_number = apd.account_number
     group by card_id

       commit
       create unique hg index uhcar on #dl_by_box(card_id)

       update #dl_by_box as bas
          set bas.service_instance_id = cid.service_instance_id
         from sk_prod.cust_card_issue_dim as cid
        where bas.card_id = left(cid.card_id, 8)
          and card_status = 'Enabled'

           -- if there has been an on demand download in the last 6 months (by box)
       update #waterfall_box_rules as bas
          set last_dl_dt = 1
         from #dl_by_box as dls
        where bas.service_instance_id = dls.service_instance_id
          and dateadd(month, 6, max_dt) > now()

           -- #### Apply box info & apply rules to accounts ####
       update vespa_analysts.Waterfall_Base as bas
          set bas.l22_known_prefix = 0         -- ALL boxes must have known prefix information
         from #waterfall_box_rules as box
        where bas.account_number = box.account_number
          and box.known_prefix = 0

       update vespa_analysts.Waterfall_Base as bas
          set bas.l23_empty_prefix = 0         -- ALL boxes must have no prefix
         from #waterfall_box_rules as box
        where bas.account_number = box.account_number
          and box.empty_prefix = 0

       update vespa_analysts.Waterfall_Base as bas
          set bas.l24_last_callback_dt = 0     -- ALL boxes must have a callback
         from #waterfall_box_rules as box
        where bas.account_number = box.account_number
          and box.last_callback_dt = 0

       update vespa_analysts.Waterfall_Base as bas
          set bas.l25_drx595 = 0               -- DRX 595 not allowed on BB until R8 come in
         from #waterfall_box_rules as box
        where bas.account_number = box.account_number
          and box.drx595 = 0

       update vespa_analysts.Waterfall_Base as bas
          set bas.l30_ondemand_downloads = 0   -- Each box must have an on demand download in the last 6 months
         from #waterfall_box_rules as box
        where bas.account_number = box.account_number
          and box.last_dl_dt = 0

          -- Rule 31 - Multiroom flag
       select account_number
             ,count(1) as boxes
         into #box_count
         from #waterfall_box_rules
     group by account_number

       update vespa_analysts.Waterfall_Base as bas
          set l31_singlebox = case when boxes > 1 then 0 else 1 end
         from #box_count as box
        where bas.account_number = box.account_number

           -- Rule 20 - Darwin
       select account_number
             ,count(1) as boxes
             ,sum(darwin) as darwin
         into #darwin
         from #waterfall_box_rules as box
     group by account_number

       update vespa_analysts.Waterfall_Base as bas
          set l20_darwin = case when darwin = boxes then 'Yes'
                                when darwin = 0     then 'No'
                                else                     'Mix'
                           end
         from #darwin as box
        where bas.account_number = box.account_number

           -- #### Waterfall results ####
       update vespa_analysts.Waterfall_Base
          set knockout_level_BB  =   case when l07_prod_latest_dtv        = 0 then 7
                                          when l08_country                = 0 then 8
                                          when l10_surname                = 0 then 10
                                          when l11_standard_accounts      = 0 then 11
                                          when l14_not_vespa_panel        = 0 then 14
                                          when l15_sky_view_panel         = 0 then 15

                                          when l20_darwin            <> 'Yes' then 20
                                          when l24_last_callback_dt       = 0 then 24
                                          when l25_drx595                 = 0 then 125
                                          when l30_ondemand_downloads     = 0 then 130
                                          when l31_singlebox              = 0 then 131
                                          else                                     9999 end -- pass!
             ,knockout_level_PSTN  = case when l07_prod_latest_dtv        = 0 then 7
                                          when l08_country                = 0 then 8
                                          when l10_surname                = 0 then 10
                                          when l11_standard_accounts      = 0 then 11
                                          when l14_not_vespa_panel        = 0 then 14
                                          when l15_sky_view_panel         = 0 then 15

                                          when l24_last_callback_dt       = 0 then 24
                                          when l13_hibernators            = 0 then 113
                                          when l22_known_prefix           = 0 then 122
                                          when l23_empty_prefix           = 0 then 123
                                          when l31_singlebox              = 0 then 131
                                          else                                     9999 -- pass!
                                     end

       insert into vespa_analysts.waterfall_box_base(
              account_number
             ,subscriber_id
             ,enable)
       select bas.account_number
             ,subscriber_id
             ,case when drx595           = 1
                    and last_dl_dt       = 1 then 'BB'
                   when l13_hibernators  = 1
                    and known_prefix     = 1
                    and empty_prefix     = 1 then 'PSTN'
                   else                           'Neither' end
         from vespa_analysts.Waterfall_Base as bas
              left join #waterfall_box_rules as box on bas.account_number = box.account_number
        where l07_prod_latest_dtv    = 1
          and l08_country            = 1
          and l10_surname            = 1
          and l11_standard_accounts  = 1
          and l14_not_vespa_panel    = 1
          and l15_sky_view_panel     = 1
          and l24_last_callback_dt   = 1
          and knockout_level_PSTN    < 9999
          and knockout_level_BB      < 9999

           -- count boxes that can be pstn/ bb by account
       select account_number
             ,sum(case when enable = 'Neither' then 1 else 0 end) as neither
             ,sum(case when enable = 'PSTN'    then 1 else 0 end) as pstn
         into #waterfall_box_base_accounts
         from vespa_analysts.waterfall_box_base
     group by account_number

       commit
       create unique hg index uhacc on #waterfall_box_base_accounts(account_number)

       update vespa_analysts.Waterfall_Base as bas
          set knockout_level_bb = 9999
         from #waterfall_box_base_accounts as box
        where bas.account_number = box.account_number
          and knockout_level_BB = 131
          and neither = 0
          and pstn = 0

       update vespa_analysts.Waterfall_Base as bas
          set knockout_level_mix  =  case when l07_prod_latest_dtv        = 0 then 7
                                          when l08_country                = 0 then 8
                                          when l10_surname                = 0 then 10
                                          when l11_standard_accounts      = 0 then 11
                                          when l14_not_vespa_panel        = 0 then 14
                                          when l15_sky_view_panel         = 0 then 15
                                          when l24_last_callback_dt       = 0 then 24

                                          when knockout_level_BB = 9999 and knockout_level_PSTN = 9999 then  99 -- already allocated to PSTN or BB
                                          when neither > 0                                             then 100 -- at least one box can go on neither panel
                                          else                                                             9999 -- pass!
                                     end
         from #waterfall_box_base_accounts as box
        where bas.account_number = box.account_number

       update vespa_analysts.Waterfall_Base as bas
          set knockout_reason_bb = case knockout_level_bb when 7   then 'DTV account'
                                                                        when 8   then 'Country'
                                                                        when 10  then 'Surname'
                                                                        when 11  then 'Standard_accounts'
                                                                        when 14  then 'Not_vespa_panel'
                                                                        when 15  then 'Sky_view_panel'
                                                                        when 20  then 'Darwin'
                                                                        when 24  then 'Last_callback_dt'
                                                                        when 125 then 'DRX 595'
                                                                        when 130 then 'On Demand downloads'
                                                                        when 131 then 'Multiscreen'
                                                                        else          'Potential BB panellist'
                                   end
             ,knockout_reason_pstn = case knockout_level_pstn when 8   then 'Country'
                                                              when 10  then 'Surname'
                                                              when 11  then 'Standard_accounts'
                                                              when 14  then 'Not_vespa_panel'
                                                              when 15  then 'Sky_view_panel'
                                                              when 20  then 'Darwin'
                                                              when 24  then 'Last_callback_dt'
                                                              when 113 then 'Hibernators'
                                                              when 122 then 'Prefix information unknown'
                                                              when 123 then 'Empty prefix'
                                                              else          'Potential PSTN panellist'
                                               end

             ,knockout_reason_mix = case when knockout_level_bb   < 125 or knockout_level_bb   = 9999  then 'Already accounted for'
                                         when knockout_level_pstn < 113 or knockout_level_pstn = 9999 then 'Already accounted for'
                                         when knockout_level_mix  = 100 then 'At least one box cannot go on either panel'
                                         else                                'Potential Mixed panellist'
                                    end

       commit
end;

--BB breakdown by account
  select knockout_level_bb
        ,knockout_reason_bb
        ,count(1)
    from vespa_analysts.Waterfall_Base
group by knockout_level_bb
        ,knockout_reason_bb
order by knockout_level_bb
;

--PSTN breakdown by account
  select max(knockout_level_pstn) as level
        ,knockout_reason_pstn
        ,count(1)
    from vespa_analysts.Waterfall_Base
group by knockout_reason_pstn
order by level
;

--Mix breakdown by account
  select max(knockout_level_mix) as level
        ,knockout_reason_mix
        ,count(1)
    from vespa_analysts.Waterfall_Base
group by knockout_reason_mix
order by level
;





select top 10 * from sk_prod.CUST_ANYTIME_PLUS_DOWNLOADS
select top 10 * from CUST_ANYTIME_PLUS_DOWNLOADS


select top 10 * from sys.sysviews
where viewname='CUST_ANYTIME_PLUS_DOWNLOADS'

create view "sk_prod"."CUST_ANYTIME_PLUS_DOWNLOADS" as select * from "sk_prod_data"."CUST_ANYTIME_PLUS_DOWNLOADS_20140824"

select top 10 * from sk_prod_data.CUST_ANYTIME_PLUS_DOWNLOADS_20140824



  select knockout_level_bb
        ,knockout_reason_bb
        ,knockout_level_pstn
        ,knockout_reason_pstn
        ,count(1)
    from vespa_analysts.Waterfall_Base
group by knockout_level_bb
        ,knockout_reason_bb
        ,knockout_level_pstn
        ,knockout_reason_pstn
order by knockout_level_bb

commit





