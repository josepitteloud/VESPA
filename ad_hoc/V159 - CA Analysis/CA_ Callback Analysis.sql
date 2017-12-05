
/*
 Analysis for the Conditional Access Team on Call back Analysis
 Period: 15-04-2013 to 26-04-2013
 Author : Patrick Igonor
 Lead: Claudio Lima 
*/

--Tables of interest

select top 10* from vespa_analysts.waterfall_callback_data --Prefix Information
select top 10* from sk_prod.cust_subs_hist --Information on Multi-room, Sky Talk and Sky Packages
select top 10* from sk_prod.Cust_Single_Account_View --Information regarding country
select top 10* from Vespa_analysts.vespa_single_box_view --Information regarding Panel and box type
select top 10* from sk_prod.cust_set_top_box --Information regarding box model, serial number, box type
select top 10* from sk_prod.CUST_SUBSCRIPTIONS --Information regarding call withheld


--Selecting the most current date, the latest call back seqence and the maximum prefix along side other variables of interest
select  account_number
       ,subscriber_id
       ,dt
       ,callback_seq
       ,prefix
       ,rank() over (partition by account_number,subscriber_id order by dt desc, callback_seq desc, prefix desc, row_id desc) as dt_callback_seq
into    dt_callback
from vespa_analysts.waterfall_callback_data
where subscriber_id is not null
and account_number is not null
--43,978,476 Row(s) affected

--Deleting duplicates---

delete from dt_callback where dt_callback_seq >1
--28,239,970 Row(s) affected

--Checks----

select count(*) from
(
select subscriber_id
from dt_callback
group by subscriber_id
having count(*)>1
) t
--0

--Joining the above table to the Customer Single Account Table

select   sav.account_number
        ,dtc.subscriber_id
        ,dtc.prefix
into     SAV_Prefix_Data
from sk_prod.Cust_Single_Account_View sav
inner join dt_callback dtc
on sav.account_number = dtc.account_number
where sav.cust_Active_DTV = 1
and sav.pty_country = 'Great Britain'
----8,932,320 Row(s) affected

----Deduping the duplicates
select   distinct
         account_number
        ,subscriber_id
        ,prefix
into    SAV_Prefix_Data_nodups
from    SAV_Prefix_Data
--8,932,319 Row(s) affected
----------------------------------------------------
--Checks
select count(*) from
(
select subscriber_id
from SAV_Prefix_Data_nodups
group by subscriber_id
having count(*)>1
) t
--0

select * from SAV_Prefix_Data
where subscriber_id in (
select subscriber_id
from SAV_Prefix_Data
group by subscriber_id
having count(*)>1
)
order by account_number, subscriber_id
-------------------------------------------------
--Selecting the subscriber_id and Panel_ID_Vespa from the Single_Box_View table into another table--

select   subscriber_id
        ,Panel_ID_Vespa
into     local_Single_Box_View
from     Vespa_analysts.vespa_single_box_view
where    Status_Vespa = 'Enabled'
--1,610,336 Row(s) affected


--Joining the New table from the single box to the main table (SAV_Info_Pref)
select    SPD.subscriber_id
         ,account_number
         ,prefix
         ,Panel_ID
into      STB_Info_Pref
from SAV_Prefix_Data_nodups SPD
left join local_single_box_view SBV
on SPD.subscriber_id = SBV.subscriber_id
--8,932,319 Row(s) affected


--Selecting the account_number and subscription_sub_type (creating new fields from this) from the cust_subs_hist table into another table

select  account_number
       ,max (case when subscription_sub_type ='DTV Sky+' then 1 else 0 end) as  'Max_SkyPlus'
       ,max (case when subscription_sub_type ='DTV HD' then 1 else 0 end)  as Max_HD
       ,max (case when subscription_sub_type ='DTV Extra Subscription' then 1 else 0 end) as 'Max_Multiroom'
       ,max (case when subscription_sub_type ='DTV Primary Viewing' then 1 else 0 end) as 'Max_DTV'
       ,max (case when subscription_sub_type ='Broadband DSL Line' then 1 else 0 end) as  'Max_Broadband'
       ,max (case when subscription_sub_type ='SKY TALK SELECT' then 1 else 0 end) as 'Max_SkyTalk'
       ,max (case when Subscription_Sub_Type = 'SKY TALK SELECT' and current_product_description like '%Anytime%' then 1 else 0 end) as  'Max_Sky_Anytime'
       ,max (case when Subscription_Sub_Type = 'SKY TALK SELECT' and current_product_description like '%Freetime%' then 1 else 0 end) as 'Max_Sky_Freetime'
       ,max (case when Subscription_Sub_Type = 'SKY TALK SELECT' and current_product_description like '%Weekends%' then 1 else 0 end) as 'Max_Sky_Weekends'
into   local_cust_subs_hist
from sk_prod.cust_subs_hist
where effective_to_dt > today()
group by account_number
--25,286,695 Row(s) affected


--Joining the two tables above (i.e STB_Info_Prefix and local_cust_subs_hist) together based on account_number

select   SPD.account_number
        ,SPD.subscriber_id
        ,SPD.prefix
        ,SPD.Panel_ID
        ,LCS.Max_SkyPlus
        ,LCS.Max_HD
        ,LCS.Max_Multiroom
        ,LCS.Max_DTV
        ,LCS.Max_Broadband
        ,LCS.Max_SKyTalk
        ,LCS.Max_SKy_Anytime
        ,LCS.Max_Sky_Freetime
        ,LCS.Max_Sky_Weekends
into   STB_Cust_History
from STB_Info_Pref SPD
left join local_cust_subs_hist LCS
on SPD.account_number = LCS.account_number
--8,932,319 Row(s) affected


--- Identifying Accounts with Call Hidden Features and putting this into a new table
select   account_number
        ,max (case when ph_subs_subscription_sub_type = 'SKY TALK LR FEATURE'
              and current_product_description = ‘Withhold Number (all calls)’
              and ph_subs_status_code = ‘A’
             then 1 else 0 end) as Sky_Hidden_Feature
into     Hidden_Feature_SkyTalk
from sk_prod.CUST_SUBSCRIPTIONS
group by account_number
--25,232,424 Row(s) affected


--Joining the two tables above together in order to add information regarding the Skytalk hidden feature

select   SCH.account_number
        ,SCH.subscriber_id
        ,SCH.prefix
        ,SCH.Panel_ID
        ,SCH.Max_SkyPlus
        ,SCH.Max_HD
        ,SCH.Max_Multiroom
        ,SCH.Max_DTV
        ,SCH.Max_Broadband
        ,SCH.Max_SKyTalk
        ,SCH.Max_SKy_Anytime
        ,SCH.Max_Sky_Freetime
        ,SCH.Max_Sky_Weekends
        ,HFT.Sky_Hidden_Feature
into    STB_Cust_History_Hidden_Feat
from STB_Cust_History SCH
left join Hidden_Feature_SkyTalk HFT
on SCH.account_number = HFT.account_number
--8,932,319 Row(s) affected

-------------------------------------------------------------------------------
-- Obtain information about which accounts are broadband enabled
-- We're going to do this by identifying accounts that are anytime+ enabled
-- and have at least one STB anytime+ enabled
-------------------------------------------------------------------------------

select * into stb_active
from
(
        select  account_number
                        ,service_instance_id
                        ,active_box_flag
                        ,x_anytime_enabled
                        ,x_anytime_plus_enabled
                        ,rank () over (partition by service_instance_id order by ph_non_subs_link_sk desc) active_flag
from sk_prod.cust_set_top_box
) t
where active_flag = 1

create index stb_active_accnum on stb_active (account_number)
create index stb_active_siid on stb_active (service_instance_id)

select count(*) from stb_active -- 24,304,602

-- Account has anytime+
select distinct stb.account_number
into #account_anytime_plus
from stb_active stb
inner join sk_prod.cust_subs_hist csh
on stb.account_number = csh.account_number
where csh.subscription_sub_type = 'PDL subscriptions'
and csh.status_code = 'AC'
-- 4,454,001 row(s) affected

-- STB is anytime+ enabled
select distinct convert(bigint, card_subscriber_id) as subscriber_id
into #stb_anytime_plus_enabled
from sk_prod.cust_card_subscriber_link c
inner join stb_active d
on c.service_instance_id = d.service_instance_id
where d.x_anytime_plus_enabled = 'Y'
and c.current_flag = 'Y'
-- 10,038,872 row(s) affected

alter table igonorp.STB_Cust_History_Hidden_Feat add Account_anytime_plus smallint
alter table igonorp.STB_Cust_History_Hidden_Feat add STB_anytime_plus_enabled smallint
update igonorp.STB_Cust_History_Hidden_Feat set Account_anytime_plus = 0
update igonorp.STB_Cust_History_Hidden_Feat set STB_anytime_plus_enabled = 0

update igonorp.STB_Cust_History_Hidden_Feat base
set Account_anytime_plus = 1
from #account_anytime_plus acct
where base.account_number = acct.account_number
-- 3,616,726 row(s) updated

update igonorp.STB_Cust_History_Hidden_Feat base
set STB_anytime_plus_enabled = 1
from #stb_anytime_plus_enabled stb
where base.subscriber_id = stb.subscriber_id 
-- 5,273,328 row(s) updated

select count(distinct account_number) 
from igonorp.STB_Cust_History_Hidden_Feat 
where Account_anytime_plus = 1
and STB_anytime_plus_enabled = 1
-- 2,390,069

----------------------------
-- Report for pivot table
----------------------------

select sum(num_hh) from
(
select   --Panel_ID
         prefix
        ,case when prefix is null or trim(prefix)='' then 0 else 1 end as Has_Prefix
        ,Max_Multiroom as Multiroom
        ,Max_SkyTalk as Sky_Talk
        ,Max_Sky_Anytime as Sky_Anytime
        ,Max_Sky_Freetime as Sky_Freetime
        ,Max_Sky_Weekends as Sky_Weekends
        ,Sky_Hidden_Feature
        ,count(*) as Num_HH
from    STB_Cust_History_Hidden_Feat
group by --Panel_ID
        prefix
        ,Has_Prefix
        ,Multiroom
        ,Sky_Talk
        ,Sky_Anytime
        ,Sky_Freetime
        ,Sky_Weekends
        ,Sky_Hidden_Feature
--order by Num_HH
) t


---Selecting Prefixes
select prefix, count(Subscriber_id)as Num_STB
from STB_Cust_History_Hidden_Feat
group by prefix
order by Num_STB desc

--Checks
select count(subscriber_id)
from STB_Cust_History_Hidden_Feat
--8,932,319

 ***-- Granting Priviledges --***


grant all on dt_callback to limac;
grant all on STB_Cust_History_Hidden_Feat to limac;
grant all on Hidden_Feature_SkyTalk to limac;
grant all on local_cust_subs_hist to limac;
grant all on STB_Info_Pref to limac;
grant all on SAV_Prefix_Data to limac;
grant all on SAV_Prefix_Data_nodups to limac;
grant all on Current_Acct_number to limac;
grant all on STB_Cust_History to limac;
grant all on local_Single_Box_View to limac;
grant all on Pre_Mul_Talk_Hidden to limac;

select top 10* from STB_Cust_History_Hidden_Feat
--Checks
select max_sky_anytime,max_sky_freetime,max_sky_weekends,count(distinct account_number)as Num
from (
select account_number,max_sky_anytime,max_sky_freetime,max_sky_weekends, Max_Multiroom,Max_SKyTalk,prefix,case when prefix is null or trim(prefix)='' then 0 else 1 end as Has_Prefix
from STB_Cust_History_Hidden_Feat
) t
where Has_Prefix=1
and Max_Multiroom = 1
and Max_SKyTalk = 1
group by max_sky_anytime,max_sky_freetime,max_sky_weekends
order by max_sky_anytime,max_sky_freetime,max_sky_weekends

------- List of Subscriber_ids for different conditions as stated below-----

select count(distinct subscriber_id)
from STB_Cust_History_Hidden_Feat
where  Max_Multiroom = 1
--4,816,740
and    Max_SKyTalk = 1
--2,440,606
and    prefix = '1470'
--657,604
and    Sky_Hidden_Feature = 0
--624,016
and    STB_anytime_plus_enabled = 0
--335,078



