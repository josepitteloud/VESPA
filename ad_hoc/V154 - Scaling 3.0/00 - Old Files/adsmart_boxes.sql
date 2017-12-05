--Code to find accounts number and whether they are adsmartable or not
--Looking at 14th July 2013 so profiling date would need to be found
--Find all accounts numbers
begin
        declare @scaling_date date
        set     @scaling_date = '2013-08-08'

        declare @profiling_date date
        select  @profiling_date    = max(profiling_date)
                                        from vespa_analysts.SC2_Sky_base_segment_snapshots
                                        where profiling_date <= @scaling_date

--Create table holding account_numbers for date in question
IF object_id('adsmart_account_numbers') IS NOT NULL DROP TABLE adsmart_account_numbers
         select account_number, expected_boxes
                 into adsmart_account_numbers
                 from vespa_analysts.SC2_Sky_base_segment_snapshots
                 where profiling_date = @profiling_date
commit

create hg index hg_acc_index on adsmart_account_numbers(account_number)
commit

--Create a temp table holding account_numbers and most recent x_model_number and x_description
IF object_id('stb_active') IS NOT NULL DROP TABLE stb_active
select account_number, x_model_number, x_description
        into stb_active from
     (select account_number
            ,x_model_number
            ,x_description
            ,rank () over (partition by service_instance_id order by ph_non_subs_link_sk desc) rank
        from sk_prod.cust_Set_top_box) as sub
 where rank = 1
commit

--Update adsmart_account_numbers to include info from stb_active, namely the model number and description
alter table adsmart_account_numbers
        add (x_model_number     varchar(20)
            ,x_description      varchar(35)
            ,viewing_consent    int
            ,adsmartable_box    int
            ,adsmartable        int)
update      adsmart_account_numbers a
        set a.x_model_number = b.x_model_number
           ,a.x_description = b.x_description
       from stb_active b
      where a.account_number = b.account_number
commit

--Create table with active GB account numbers from sk_prod.CUST_SINGLE_ACCOUNT_VIEW who allow
--data capture. This table is based on code from the waterfall procedure.
IF object_id('sav_account_numbers') IS NOT NULL DROP TABLE sav_account_numbers
     select
            sav.account_number
           ,min(case when sav.PROD_LATEST_DTV_STATUS = 'Active'        then 1 else 0 end) as l07_prod_latest_dtv
           ,min(case when sav.PTY_COUNTRY_CODE = 'GBR'                 then 1 else 0 end) as l08_country
           ,min(case when sav.CUST_VIEWING_DATA_CAPTURE_ALLOWED = 'Y'  then 1 else 0 end) as l09_data_capture
           ,cast(0 as bit)                                                                as l12_TSA_opt_in
       into sav_account_numbers
       from sk_prod.CUST_SINGLE_ACCOUNT_VIEW as sav
      where sav.CUST_ACTIVE_DTV = 1
      group by sav.account_number
     commit

   update sav_account_numbers as bas
      set l09_data_capture = 0
     from vespa_analysts.ConsentIssue_05_Revised_Consent_Info as exc
    where bas.account_number = exc.account_number

   update sav_account_numbers
      set l12_TSA_opt_in = 1
    where ACCOUNT_NUMBER NOT IN
           ( SELECT SAM_REGISTRANT.ACCOUNT_NUMBER
               FROM sk_prod.SAM_REGISTRANT
              WHERE SAM_REGISTRANT.TSA_OPT_IN = 'N'
                AND SAM_REGISTRANT.ACCOUNT_NUMBER IS NOT NULL
           )
    commit

--The rule for viewing consent is that the customer has to allow data to be captured and
--that they have opted in (using the info from sk_prod.SAM_REGISTRANT)
    update      adsmart_account_numbers a
            set a.viewing_consent =
                    (case when b.l09_data_capture = 1 and b.l12_TSA_opt_in = 1 then 1 else 0 end)
           from sav_account_numbers b
          where a.account_number = b.account_number

--Find accounts in adsmart_account_numbers whch have boxes that are adsmartable
    update      adsmart_account_numbers a
            set a.adsmartable_box =
                (case
                 when (x_model_number = 'DRX 890')
                   or (x_model_number = 'TDS850NB')
                   or (x_model_number = 'DRX 895')
                   or (x_description = 'Samsung HD PVR4')
                   or (x_description = 'Samsung HD PVR5')
                   then 1 else 0 end)
--                  when (x_model_number = 'DRX 890' and x_description = 'Amstrad HD PVR5')
--                    or (x_model_number = 'Unknown' and x_description = 'Samsung HD PVR4')
--                    or (x_model_number = 'TDS850NB' and x_description = 'Pace HD PVR4')
--                    or (x_model_number = 'Unknown' and x_description = 'Samsung HD PVR5')
--                    or (x_model_number = 'DRX 895' and x_description = 'Amstrad HD PVR6 (1TB)')
--                    or (x_model_number = 'DRX 895' and x_description = 'Amstrad HD PVR6 (2TB)')
--                    then 1 else 0 end)

--Find the accounts which have adsmartable boxes and have given viewing consent to find account
--which are capable of being adsmartable.
    update      adsmart_account_numbers a
             set a.adsmartable = --coalesce(viewing_consent * adsmartable_box, 0)
                     (case when viewing_consent = 1 and adsmartable_box = 1 then 1 else 0 end)

end

select adsmartable, count(*)
        from adsmart_account_numbers
        group by adsmartable
        order by adsmartable

