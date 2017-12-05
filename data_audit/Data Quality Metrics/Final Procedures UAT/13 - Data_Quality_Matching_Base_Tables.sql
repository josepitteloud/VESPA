IF object_id('Data_Quality_Matching_Rates_TablePreparation') IS not NULL drop procedure Data_Quality_Matching_Rates_TablePreparation

create procedure Data_Quality_Matching_Rates_TablePreparation
@target_date        date = NULL     -- Date of data analyzed or date process run
    ,@CP2_build_ID     bigint = NULL   -- Logger ID (so all builds end up in same queue)
as

declare @skybase_count int
declare @vespa_count int
declare	@postcode_cnt int
declare	@vespa_postcode_cnt int


begin

--Sky Totals

EXECUTE logger_add_event @RunID , 3,'Data_Quality_Matching_Rates_TablePreparation Start',0

    IF object_id('Data_Quality_Match_Rates_Skybase') IS NOT NULL drop table Data_Quality_Match_Rates_Skybase

  SELECT DISTINCT account_number
      ,fin_currency_code currency_code
      ,cb_key_household as household_key
      ,cb_key_individual as individual_key
  INTO Data_Quality_Match_Rates_Skybase
  FROM sk_prod.cust_single_account_view
    where cust_active_dtv = 1     
    and cb_key_household > 0             --UK Only
     AND cb_key_household IS NOT NULL
 
commit

select count(1) into @skybase_count from Data_Quality_Match_Rates_Skybase

    execute logger_add_event @CP2_build_ID, 3, 'SkyBase Total Records', coalesce(@skybase_count, -1)

--vespa Totals

    IF object_id('Data_Quality_Match_Rates_Vespa') IS NOT NULL drop table Data_Quality_Match_Rates_Vespa
   
  SELECT DISTINCT (ve.account_number) account_number
      ,ve.cb_key_individual     AS individual_key
      ,ve.consumerview_cb_row_id
      , sav.cb_key_household    AS household_key
  INTO Data_Quality_Match_Rates_Vespa
  FROM Vespa_Analysts.Vespa_Single_Box_View as ve
  INNER JOIN sk_prod.CUST_SINGLE_ACCOUNT_VIEW as sav 
  ON ve.account_number = sav.account_number
  WHERE panel_id_vespa = 12
    and sav.cust_active_dtv = 1

commit

select count(1) into @vespa_count from Data_Quality_Match_Rates_Vespa

    execute logger_add_event @CP2_build_ID, 3, 'Vespa Total Records', coalesce(@vespa_count, -1)


--sky postcode

    IF object_id('Data_Quality_Match_Rates_SkyPostcode') IS NOT NULL drop table Data_Quality_Match_Rates_SkyPostcode

create table Data_Quality_Match_Rates_SkyPostcode
(cb_address_postcode varchar(20))

insert into Data_Quality_Match_Rates_SkyPostcode
  SELECT distinct 
        trim(replace(SAV.cb_address_postcode,' ','')) cb_address_postcode
  FROM  sk_prod.CUST_SINGLE_ACCOUNT_VIEW as sav
    INNER JOIN Data_Quality_Match_Rates_Skybase   AS sky    ON sky.account_number = sav.account_number
  WHERE   sav.cust_active_dtv = 1

commit

select count(1) into @postcode_cnt from Data_Quality_Match_Rates_SkyPostcode

    execute logger_add_event @CP2_build_ID, 3, 'Sky Base PostCode Total Records', coalesce(@postcode_cnt, -1)


--vespa postcode

    IF object_id('Data_Quality_Match_Rates_VespaPostcode') IS NOT NULL drop table Data_Quality_Match_Rates_VespaPostcode

  CREATE TABLE Data_Quality_Match_Rates_VespaPostcode
  ( cb_address_postcode varchar(20))
   
  INSERT INTO Data_Quality_Match_Rates_VespaPostcode
  SELECT DISTINCT
        TRIM(REPLACE(SAV.cb_address_postcode,' ','')) cb_address_postcode
  FROM  sk_prod.CUST_SINGLE_ACCOUNT_VIEW as sav
    INNER JOIN Data_Quality_Match_Rates_Vespa vespa    ON vespa.account_number = sav.account_number
  WHERE   sav.cust_active_dtv = 1

commit

select count(1) into @vespa_postcode_cnt from Data_Quality_Match_Rates_VespaPostcode

    execute logger_add_event @CP2_build_ID, 3, 'Vespa Base PostCode Total Records', coalesce(@postcode_cnt, -1)






EXECUTE logger_add_event @RunID , 3,'Data_Quality_Matching_Rates_TablePreparation End',0

end

go

