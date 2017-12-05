
------------------------------------GETTING THE ENTIRE BASE FOR SCORING-------------------

--select count(1) from bcg_base_20121011
--grant all on bcg_base_20121011 to public;
--create base at 18-Oct-2012 at account level
  select distinct account_number
    into glasera.bcg_base_20121011
    from sk_prod.cust_subs_hist
   where subscription_sub_type = 'DTV Primary Viewing'
     and status_code = 'AC'
     and effective_from_dt <= '2012-10-11'
     and effective_to_dt > '2012-10-11'
     and effective_from_dt < effective_to_dt;
--9996116 Row(s) affected

create unique hg index ind_acc on bcg_base_20121011(account_number);

grant select on bcg_base_20121011 to public;


--append target date
alter table bcg_base_20121011
        add target_dt date default null;

update bcg_base_20121011
   set target_dt = '2012-10-11';

--------------------9996116 Row(s) affected


/*
select top 100 * from bcg_base_20120531;
select count(account_number), count(distinct account_number) from bcg_base_20120531;

*/

/*
------------------------------APPEND CHURN EVENTS---------------------------


select  csh.account_number
       ,effective_from_dt as churn_date
       ,case when status_code = 'PO'
             then 'CUSCAN'
             else 'SYSCAN'
         end as churn_type
       ,RANK() OVER (PARTITION BY  csh.account_number
                     ORDER BY  csh.effective_from_dt,csh.cb_row_id) AS churn_rank
       ,1 as churn_flag--Rank to get the first event
  into -----drop table
        #all_churn_records_may
  from sk_prod.cust_subs_hist as csh
    inner join bcg_base_20130418 base
  on base.account_number = csh.account_number
 where subscription_sub_type ='DTV Primary Viewing'     --DTV stack
   and status_code in ('PO','SC')                       --CUSCAN and SYSCAN status codes
   and prev_status_code in ('AC','AB', 'PC')             --Previously ACTIVE
   and status_code_changed = 'Y'
   and effective_from_dt between '2013-05-18' and '2013-08-18'                 --Events after the mailing date
   and effective_from_dt != effective_to_dt
  ;
--------------------309596


delete from #all_churn_records_may     -- deletes all churn records from the temp table except most recent
where churn_rank > 1;
---------------------500




alter table bcg_base_20121130
add (churn_date                 date            default null
    ,churn_type                 varchar(20)     default null
    ,churn_flag                  tinyint        default 0);


update bcg_base_20121130  base
set base.churn_date   =   temp.churn_date
   ,base.churn_type   =   temp.churn_type
   ,base.churn_flag   =   temp.churn_flag
from #all_churn_records_may   temp
where base.account_number  = temp.account_number;


-------------------309096

*/


--append first activation dt
alter table bcg_base_20121011
        add first_activation_dt date default null;


update bcg_base_20121011
   SET  first_activation_dt   = sav.ph_subs_first_activation_dt
  FROM bcg_base_20121011 as base
       INNER JOIN sk_prod.cust_single_account_view AS sav ON base.account_number = sav.account_number;
-----------------9996113 Row(s) affected



--tenure in days
alter table bcg_base_20121011
        add tenure integer default null;

update bcg_base_20121011
   set tenure = target_dt - first_activation_dt;
--------------9996116 Row(s) affected

--tenure group - first activation
alter table bcg_base_20121011
        add tenure_group_firstactive  varchar(50) default '09 Unknown';

update bcg_base_20121011
   set tenure_group_firstactive = case when tenure <=  90  then '01 0-3 months'
                                       when tenure <=  180 then '02 4-6 months'
                                       when tenure <=  365 then '03 6-12 months'
                                       when tenure <=  730 then '04 1-2 years'
                                       when tenure <= 1095 then '05 2-3 Yrs'
                                       when tenure <= 1824 then '06 3-5 Yrs'
                                       when tenure <= 3648 then '07 5-10 Yrs'
                                       when tenure >  3648 then '08 10+ Yrs'
                                            else                '09 Unknown'
                                   end;

------------------9996116 Row(s) affected

/*check

select count(*),tenure_group_firstactive from bcg_base_all group by tenure_group_firstactive;
select count(*),tenure_group_firstactive from bcg_base_all where target_dt = '2012-05-31' group by tenure_group_firstactive;
select count(*),tenure_group_firstactive from bcg_base_all where target_dt = '2012-06-30' group by tenure_group_firstactive;
select count(*),tenure_group_firstactive from bcg_base_all where target_dt = '2012-07-31' group by tenure_group_firstactive;
select count(*),tenure_group_firstactive from bcg_base_all where target_dt = '2012-08-31' group by tenure_group_firstactive;
select count(*),tenure_group_firstactive from bcg_base_all where target_dt = '2012-09-30' group by tenure_group_firstactive;
select count(*),tenure_group_firstactive from bcg_base_all where target_dt = '2012-10-31' group by tenure_group_firstactive;

*/

-------------------------------------ADDING PROFILES------------------------------


--------------------------Product Holding--------------------------


 select base.account_number
        ,max(case when csh.subscription_sub_type ='DTV Extra Subscription'
                       and csh.status_code in  ('AC','AB','PC') then 1 else 0 end) as multiroom
        ,max(case when csh.subscription_sub_type ='DTV HD'
                       and csh.status_code in  ('AC','AB','PC') then 1 else 0 end) as hdtv
        ,max(case when csh.subscription_sub_type ='Broadband DSL Line'
                       and (    status_code in ('AC','AB')
                            or (status_code = 'PC' and prev_status_code not in ('?','RQ','AP','UB','BE','PA') )
                            or (status_code = 'CF' and prev_status_code = 'PC'                                )
                            or (status_code = 'AP' and sale_type = 'SNS Bulk Migration'                       )
                            )                                   then 1 else 0 end) as broadband
        ,max(case when csh.subscription_sub_type = 'SKY TALK SELECT'
                       and (     csh.status_code = 'A'
                            or (csh.status_code = 'FBP' and prev_status_code in ('PC','A'))
                            or (csh.status_code = 'RI'  and prev_status_code in ('FBP','A'))
                            or (csh.status_code = 'PC'  and prev_status_code = 'A')
                            )                                   then 1 else 0 end) as skytalk
        ,max(case when csh.subscription_sub_type = 'SKY TALK LINE RENTAL'
                       and csh.status_code in ('A','CRQ','R')   then 1 else 0 end) as wlr
    into #attachments --drop table #attachments
    from bcg_base_20121011 as base
         inner join sk_prod.cust_subs_hist AS csh on base.account_number = csh.account_number
   where csh.effective_from_dt <= target_dt
     and csh.effective_to_dt    > target_dt
     and csh.subscription_sub_type  in ( 'DTV Primary Viewing'
                                        ,'DTV Extra Subscription'
                                        ,'DTV HD'
                                        ,'Broadband DSL Line'
                                        ,'SKY TALK SELECT'
                                        ,'SKY TALK LINE RENTAL')
     and csh.effective_from_dt <> csh.effective_to_dt
group by base.account_number;
--9996116 Row(s) affected

create hg index ind_acc on #attachments(account_number);

--update base
alter table bcg_base_20121011
        add hd        tinyint default 0
       ,add multiroom tinyint default 0
       ,add broadband tinyint default 0
       ,add talk      tinyint default 0
       ,add wlr       tinyint default 0;

update bcg_base_20121011
   set hd = att.hdtv
      ,multiroom = att.multiroom
      ,broadband = att.broadband
      ,talk = att.skytalk
      ,wlr = att.wlr
  from bcg_base_20121011 as base
       inner join #attachments as att on base.account_number = att.account_number;


--------------------9996116 Row(s) affected

alter table bcg_base_20121011
add product_holding varchar(50) default null;


update bcg_base_20121011
set product_holding =
            CASE WHEN (broadband = 0) AND (talk = 0) AND (wlr = 0) THEN 'TV Only'
                 WHEN (broadband = 1) AND (talk = 0) AND (wlr = 0) THEN 'TV and Broadband'
                 WHEN (broadband = 0) AND (talk = 1) AND (wlr = 0) THEN 'TV and SkyTalk'
                 WHEN (broadband = 1) AND (talk = 1) AND (wlr = 0) THEN 'TV, SkyTalk and Broadband'
                 WHEN (broadband = 1) AND (talk = 0) AND (wlr = 1) THEN 'TV, Broadband and Line Rental'
                 WHEN (broadband = 0) AND (talk = 1) AND (wlr = 1) THEN 'TV, SkyTalk and Line Rental'
                 WHEN (broadband = 1) AND (talk = 1) AND (wlr = 1) THEN 'TV, SkyTalk and Line Rental and Broadband'
                 ELSE '??'
            END;

--------------9996116 Row(s) affected


/*check

select count(*), product_holding from bcg_base_all group by product_holding;
select count(*), product_holding from bcg_base_all where target_dt = '2012-06-30' group by product_holding;
select count(*), product_holding from bcg_base_all where target_dt = '2012-07-31' group by product_holding;
select count(*), product_holding from bcg_base_all where target_dt = '2012-08-31' group by product_holding;
select count(*), product_holding from bcg_base_all where target_dt = '2012-09-30' group by product_holding;
select count(*), product_holding from bcg_base_all where target_dt = '2012-10-31' group by product_holding;

*/
----------------------------------------------AFFLUENCE---------------------------------



ALTER TABLE     bcg_base_20121011

ADD             cb_key_family bigint default null,

ADD             cb_key_individual bigint default null,

ADD             cb_key_household bigint default null;



UPDATE          bcg_base_20121011 base

set             base.cb_key_family = sav.cb_key_family,

                base.cb_key_individual = sav.cb_key_individual,

                base.cb_key_household = sav.cb_key_household

from            sk_prod.cust_single_account_view sav

where           base.account_number = sav.account_number

;
-------------------------9996113 Row(s) affected
commit;



create hg index indx_fam_key on bcg_base_20121011 (cb_key_family);

create hg index indx_ind_key on bcg_base_20121011(cb_key_individual);

create hg index indx_hh_key on bcg_base_20121011(cb_key_household);





SELECT          cb_key_family, cb_key_individual, cb_key_household, cb_row_id

                ,rank() over(partition by cb_key_household  ORDER BY cb_row_id desc) as rank_hh

                ,rank() over(partition by cb_key_family     ORDER BY cb_row_id desc) as rank_fam

                ,rank() over(partition by cb_key_individual ORDER BY cb_row_id desc) as rank_ind

INTO            --drop table
                #cv_keys

FROM            sk_prod.EXPERIAN_CONSUMERVIEW;
-----------------49724314 Row(s) affected


DELETE FROM #cv_keys WHERE rank_hh <> 1 AND rank_fam <> 1 AND rank_ind <> 1;
-----------------1890532 Row(s) affected


--To flag the individual or the family variable



CREATE UNIQUE HG INDEX idx01 ON #cv_keys(cb_row_id);

CREATE        HG INDEX idx02 ON #cv_keys(cb_key_family);

CREATE        HG INDEX idx03 ON #cv_keys(cb_key_individual);

CREATE        LF INDEX idx04 ON #cv_keys(rank_fam);

CREATE        LF INDEX idx05 ON #cv_keys(rank_ind);



UPDATE          #cv_keys

SET             rank_hh = NULL

WHERE           cb_key_household IS NULL

        OR      cb_key_household = 0;

-----------------0 Row(s) affected

UPDATE          #cv_keys

SET             rank_fam = NULL

WHERE           cb_key_family IS NULL

        OR      cb_key_family = 0;

-----------------0 Row(s) affected

UPDATE          #cv_keys

SET             rank_ind = NULL

WHERE           cb_key_individual IS NULL

        OR      cb_key_individual = 0;

-----------------0 Row(s) affected





ALTER TABLE     bcg_base_20121011

ADD(            Affluence VARCHAR(50) default 'Unknown'

);



UPDATE          bcg_base_20121011 base

SET             base.affluence = CASE WHEN cv.h_affluence_v2 in ('00','01','02')   THEN 'Very Low'
                                      WHEN cv.h_affluence_v2 in ('03','04','05')   THEN 'Low'
                                      WHEN cv.h_affluence_v2 in ('06','07','08')   THEN 'Mid Low'
                                      WHEN cv.h_affluence_v2 in ('09','10','11')   THEN 'Mid'
                                      WHEN cv.h_affluence_v2 in ('12','13','14')   THEN 'Mid High'
                                      WHEN cv.h_affluence_v2 in ('15','16','17')   THEN 'High'
                                      WHEN cv.h_affluence_v2 in ('18','19')        THEN 'Very High'
                                      ELSE                                              'Unknown'
                                 END

FROM            bcg_base_20121011 base

       INNER JOIN #cv_keys k

                ON base.cb_key_household = k.cb_key_household

                AND k.rank_hh = 1

        INNER JOIN sk_prod.EXPERIAN_CONSUMERVIEW  AS cv

                ON k.cb_row_id = cv.cb_row_id;

-----------------8731767 Row(s) affected



------------------Premium Holding-------------------



--package
  select base.account_number
        ,cel.prem_sports
        ,cel.prem_movies
        ,variety
        ,knowledge
        ,kids
        ,style_culture
        ,music
        ,news_events
        ,rank() over(partition by base.account_number order by csh.effective_from_dt, csh.cb_row_id desc) as rank
    into ---drop table
         #packages
    from bcg_base_20121011 as base
         inner join sk_prod.cust_subs_hist as csh on csh.account_number = base.account_number
         inner join sk_prod.cust_entitlement_lookup as cel on csh.current_short_description = cel.short_description
   where subscription_sub_type = 'DTV Primary Viewing'
     and status_code in ('AC')
     and effective_from_dt <= target_dt
     and effective_to_dt   >  target_dt
     and effective_from_dt <  effective_to_dt;
--9996694 Row(s) affected

delete
  from #packages
 where rank > 1;
--627 Row(s) affected

--update base
alter table bcg_base_20121011
        add num_mix integer default 0;

update bcg_base_20121011
   set num_mix = variety + knowledge + kids + style_culture + music + news_events
  from bcg_base_20121011 as base
       inner join #packages as tgt on base.account_number = tgt.account_number;
--9996067 Row(s) affected

--check
  select num_mix
        ,count(*)
    from bcg_base_20121011
group by num_mix;


--update base
alter table bcg_base_20121011
        add mix_pack varchar(20) default '06 Ent Pack';

update bcg_base_20121011
   set mix_pack = case when num_mix is null                      or num_mix = 0 then '06 Ent Pack'
                       when (variety = 1  or style_culture = 1) and num_mix = 1 then '06 Ent Pack'
                       when (variety = 1 and style_culture = 1) and num_mix = 2 then '06 Ent Pack'
                       when num_mix > 0                                         then '05 Ent Extra'
                   end
  from bcg_base_20121011 as base
       inner join #packages as tgt on base.account_number = tgt.account_number;
--9996067 Row(s) affected
/*
--check
  select mix_pack
        ,count(*)
    from bcg_base_all
group by mix_pack;
*/

--update base
alter table bcg_base_20121011
        add tv_package varchar(30);

update bcg_base_20121011
   set tv_package = case when tgt.prem_movies = 2 and tgt.prem_sports = 2 then '01 Top Tier'
                         when tgt.prem_movies = 0 and tgt.prem_sports = 2 then '02 Dual Sports'
                         when tgt.prem_movies = 2 and tgt.prem_sports = 0 then '03 Dual Movies'
                         when tgt.prem_movies > 0  or tgt.prem_sports > 0 then '04 Other Prems'
                         when tgt.prem_movies = 0 and tgt.prem_sports = 0 then mix_pack
                     end
  from bcg_base_20121011 as base
       inner join #packages as tgt on base.account_number = tgt.account_number;

--------------------------9996067 Row(s) affected


--------------------Deleting ROI ---------------------

--add roi flag
alter table bcg_base_20121011
        add roi_flag tinyint default 0;

update bcg_base_20121011
   set roi_flag = 1
  from bcg_base_20121011 as base
       inner join sk_prod.cust_single_account_view as sav on base.account_number = sav.account_number
 where pty_country_code = 'IRL';

--------------684098 Row(s) affected




delete from bcg_base_20121011 where roi_flag = 1;
--------------684098 Row(s) affected



----------------------Exclude Staff--------------


DELETE
  FROM bcg_base_20121011   --The target table containing Staff accounts
 WHERE account_number IN (
                           SELECT account_number
                             FROM sk_prod.cust_single_account_view
                            WHERE prod_ph_subs_account_sub_type NOT IN ('Normal','?')
                        );

-----------------44840 Row(s) affected


------------------------------------------ADDING OTHER PROFILES------------------------------





---------------------------Lifestage----------------




ALTER TABLE     bcg_base_20121011

ADD            Lifestage VARCHAR(100) default 'Unknown'

;



UPDATE          bcg_base_20121011 base

SET             base.Lifestage = CASE WHEN cv.h_family_lifestage_2011 = '00' THEN 'Young singles/homesharers'

                                      WHEN cv.h_family_lifestage_2011 = '01' THEN 'Young family no children <18'

                                      WHEN cv.h_family_lifestage_2011 = '02' THEN 'Young family with children <18'

                                      WHEN cv.h_family_lifestage_2011 = '03' THEN 'Young household with children <18'

                                      WHEN cv.h_family_lifestage_2011 = '04' THEN 'Mature singles/homesharers'

                                      WHEN cv.h_family_lifestage_2011 = '05' THEN 'Mature family no children <18'

                                      WHEN cv.h_family_lifestage_2011 = '06' THEN 'Mature family with children <18'

                                      WHEN cv.h_family_lifestage_2011 = '07' THEN 'Mature household with children <18'

                                      WHEN cv.h_family_lifestage_2011 = '08' THEN 'Older single'

                                      WHEN cv.h_family_lifestage_2011 = '09' THEN 'Older family no children <18'

                                      WHEN cv.h_family_lifestage_2011 = '10' THEN 'Older family/household with children <18'

                                      WHEN cv.h_family_lifestage_2011 = '11' THEN 'Elderly single'

                                      WHEN cv.h_family_lifestage_2011 = '12' THEN 'Elderly family no children <18'

                                      ELSE 'Unknown' END



FROM            bcg_base_20121011 base

       INNER JOIN #cv_keys k

                ON base.cb_key_household = k.cb_key_household

                AND k.rank_hh = 1

        INNER JOIN sk_prod.EXPERIAN_CONSUMERVIEW  AS cv

                ON k.cb_row_id = cv.cb_row_id;


-------------------------------8706460 Row(s) affected

/*select count(*), lifestage from bcg_base_20121011 group by lifestage;
*/





----------------------CQM----------------------



--cqm
alter table bcg_base_20121011
        add cqm_score integer default null;

update bcg_base_20121011
   set cqm_score = cqm.model_score
  from bcg_base_20121011 as base
       inner join sk_prod.id_v_universe_all as cqm on base.cb_key_household = cqm.cb_key_household;
--8615668 Row(s) affected



--cqm group
alter table bcg_base_20121011
        add cqm_grp varchar(20) default 'Unknown';

update bcg_base_20121011
   set cqm_grp = case when cqm_score between  1 and  10 then '01 1-10'
                      when cqm_score between  11 and 22 then '02 11-22'
                      when cqm_score between 23 and 29 then '03 23-29'
                      when cqm_score between 30 and 33 then '04 30-33'
                      when cqm_score between 34 and 36 then '05 34-36'
                           else                             'Unknown'
                  end;
------------------9267178 Row(s) affected

/*
--check
  select cqm_grp
        ,count(distinct(account_number))
    from bcg_base_20121130
group by cqm_grp;


select count(account_number), count(distinct account_number) from bcg_base_20121130 ;
select top 100 * from bcg_base_20121130;
*/





------------------------Box Type
--add service instance id
alter table bcg_base_20121011
        add service_instance_id varchar(50);

  select base.account_number
        ,csh.service_instance_id
        ,cb_row_id
        ,rank() over(partition by base.account_number, csh.service_instance_id order by cb_row_id desc) as rank
    into ---drop table
          #serviceid
    from bcg_base_20121011 as base
         inner join sk_prod.cust_subs_hist as csh on base.account_number = csh.account_number
   where subscription_sub_type = 'DTV Primary Viewing'
     and status_code in ('AC','AB','PC')
     and effective_from_dt <= target_dt
     and effective_to_dt > target_dt
     and effective_from_dt < effective_to_dt
group by base.account_number
        ,csh.service_instance_id
        ,cb_row_id;
--9267760 Row(s) affected

create hg index ind_acc on #serviceid(account_number);


delete
  from #serviceid
 where rank > 1;
--565 Row(s) affected

update bcg_base_20121011
   set service_instance_id = tgt.service_instance_id
  from bcg_base_20121011 as base
       inner join #serviceid as tgt on base.account_number = tgt.account_number;
--9267178 Row(s) affected

create hg index ind_sii on bcg_base_20121011(service_instance_id);


alter table bcg_base_20121011
        add box_type varchar(30) default '06 Unknown';

update bcg_base_20121011
   set box_type = case when current_product_description like '%HD%1TB%' then '01 HD 1TB'
                       when x_box_type = 'Sky+HD'                       then '02 Sky+ HD'
                       when x_box_type = 'Basic HD'                     then '03 HD Digibox'
                       when x_box_type = 'Sky+'                         then '04 Sky+ Legacy'
                       when x_box_type = 'Basic'                        then '05 FDB Legacy'
                       when x_box_type is null                          then '06 Unknown'
                            else                                              x_box_type
                   end
  from bcg_base_20121011 as base
       inner join sk_prod.cust_set_top_box as stb on stb.account_number = base.account_number
                                                 and stb.service_instance_id = base.service_instance_id
 where active_box_flag = 'Y'
   and box_installed_dt <= target_dt
   and box_replaced_dt   > target_dt;
--8506876 Row(s) affected


/*
--check
  select box_type, count(*)
    from bcg_base_20121130
group by box_type
order by box_type;
*/



-----------------------Cable Area



--add hh key and postcode
alter table bcg_base_20121011
       --add cb_key_household    bigint      default null
       add cb_address_postcode varchar(20) default null;

update bcg_base_20121011
   set cb_key_household = sav.cb_key_household
      ,cb_address_postcode = sav.cb_address_postcode
  from bcg_base_20121011 as base
       inner join sk_prod.cust_single_account_view as sav on base.account_number = sav.account_number;
--9267175 Row(s) affected


create hg index ind_pc on bcg_base_20121011(cb_address_postcode);

alter table bcg_base_20121011
        add cable  varchar(20) default null;

update bcg_base_20121011
   set cable = case when cable_postcode = 'y' then 'Cable' else 'Non-Cable' end
  from bcg_base_20121011 as base
       left outer join sk_prod.broadband_postcode_exchange as bb
       on replace(base.cb_address_postcode, ' ','') = replace(bb.cb_address_postcode,' ','');

--------------9267178 Row(s) affected


------------------------------------------CONTRIBUTION--------------------------------------

----------------------GETTING TOTAL CONTRIBUTION------
----drop table #Contribution_Base
CREATE TABLE #Contribution_Base (
         id                     bigint          identity  -- a unique ID used for self referencing (enables non unquie account Numbers)
        ,account_number         varchar(20)     not null  -- Account Number for matching
        ,target_date            date            not null  -- date of contribution level for the account
        ,DTV                    bit             default 0 -- Is DTV active
        ,HD                     smallint        default 0 -- how many HD boxes do they have
        ,MR                     smallint        default 0 -- How many MR boxes do they have
        ,SP                     tinyint         default 0 -- Does the account have sky plus functionality?
        ,BB_Pack                varchar(20)     null      -- BASE / MID / MAX / CONN
        ,ST_Pack                varchar(20)     null      -- UNL = Talk Unlimited / FREE = FreeTime
        ,WLR                    bit             default 0 -- Does the customer take WLR?
        ,SVBN                   bit             default 0 -- is the customer on the SVBN network?
        ,Sports                 tinyint         default 0 -- How Many Sports Premiums were suscribed to
        ,Movies                 tinyint         default 0 -- How Many Movies Premiums were suscribed to
        ,DTV_contribution       decimal(8,2)    default 0 -- DTV Contribution level from CEL
        ,xtra_contribution      decimal(8,2)    default 0 -- The combined contributions from attachments
        ,ppv_contribution       decimal(8,2)    default 0 -- Total Contribution from PPV
        ,total_contribution     decimal(8,2)    default 0 -- All the contribution with PPV
        ,no_ppv_contribution    decimal(8,2)    default 0 -- All the contribution none of the ppv
   );

COMMIT;

--Index and grant permissions
CREATE UNIQUE hg INDEX idx1 on #Contribution_Base(id);
CREATE        hg INDEX idx2 on #Contribution_Base(account_number);

COMMIT;
----------------------------------------ADD TARGET ACCOUNTS

--Add you own list of acconts here
--We are adding everyone in Luton for date of running by way of example
--Two step process to remove duplicates



INSERT into #Contribution_Base (account_number, target_date)
SELECT account_number, target_dt
  FROM bcg_base_20121011;
----------------9267178 Row(s) affected

COMMIT;

-------------------------------------Get the DTV Contribution

--Get the contribution levels
 SELECT   base.id
         ,cel.contribution_gbp as contribution
         ,cel.prem_sports
         ,cel.prem_movies
         ,csh.effective_from_dt  --Needed for the rank function
         ,csh.cb_row_id          --Needed for the rank function
         ,RANK() OVER (PARTITION BY  base.id
                                     ORDER BY  csh.effective_from_dt desc
                                              ,csh.cb_row_id         desc
                                 ) AS 'RANK'
    INTO ---drop table
          #DTV
    FROM sk_prod.cust_subs_hist as csh
         inner join #Contribution_Base as base on csh.account_number = base.account_number
         inner join sk_prod.cust_entitlement_lookup as cel  on csh.current_short_description = cel.short_description
   WHERE csh.subscription_sub_type ='DTV Primary Viewing'
     AND csh.status_code in ('AC','AB','PC')
     AND csh.effective_from_dt <= base.target_date
     AND csh.effective_to_dt   >  base.target_date
     AND csh.effective_from_dt <> effective_to_dt
GROUP BY  base.id
         ,csh.account_number
         ,contribution
         ,prem_sports
         ,prem_movies
         ,csh.effective_from_dt
         ,csh.cb_row_id;
----------------------9267712 Row(s) affected
COMMIT;

--Flag the contribution
UPDATE #Contribution_Base
   SET  DTV = 1
       ,DTV_contribution = tgt.contribution
       ,sports = prem_sports
       ,movies = prem_movies
  FROM #Contribution_Base as base
       inner join #DTV as tgt on base.id = tgt.id
 WHERE tgt.rank = 1;
-------------------------9267132 Row(s) affected
COMMIT;



-------------------------------------------------------------PPV

UPDATE #Contribution_Base
   SET  ppv_contribution = tgt.ppv_contribution * 0.16  -- Only a part of the PPV value is actual Contribution
  FROM #Contribution_Base as base
       inner join (
                select  base.id
                       ,sum (charge_amount_incl_tax) as ppv_contribution
                  from sk_prod.CUST_PRODUCT_CHARGES_PPV as ppv
                       inner join #Contribution_Base as base on ppv.account_number = base.account_number
                       inner join sk_prod.cust_single_account_view as sav on ppv.account_number = sav.account_number
                 where ppv.event_dt between dateadd(month,-3,base.target_date) and base.target_date
                   and ppv.ppv_cancelled_dt ='9999-09-09'
                   and ppv.charge_amount_incl_tax > 0
                   and ppv.ppv_service = 'MOVIE'
              group by base.id
       ) as tgt on base.id = tgt.id;
--------------------------------958977 Row(s) affected
COMMIT;


------------------------------------------------FLAG SVBN Accounts

UPDATE #Contribution_Base
   SET SVBN = 1
  FROM #Contribution_Base as base
       INNER JOIN sk_prod.cust_subs_hist as csh on base.account_number = csh.account_number
 WHERE csh.technology_code = 'MPF'                --SBVN Technology Code
   AND csh.effective_from_dt <= base.target_date
   AND csh.effective_to_dt   >  base.target_date
   AND csh.effective_from_dt != effective_to_dt
   AND (      (    csh.subscription_sub_type = 'SKY TALK SELECT'     -- Sky Talk
                       and (     csh.status_code = 'A'
                             or (csh.status_code = 'FBP' and prev_status_code in ('PC','A'))
                             or (csh.status_code = 'RI'  and prev_status_code in ('FBP','A'))
                             or (csh.status_code = 'PC'  and prev_status_code = 'A')))
         OR  (     csh.SUBSCRIPTION_SUB_TYPE ='SKY TALK LINE RENTAL' -- Line Rental
               AND csh.status_code IN  ('A','CRQ')  )
         OR  (     csh.SUBSCRIPTION_SUB_TYPE ='Broadband DSL Line'   -- Broadband
               AND csh.status_code IN  ('AC','AB','PC')  )
       );

COMMIT;
----------------------------------2541204 Row(s) affected

----------------------------------------------Get Attachment Counts

 select     base.id
           ,sum  (case  when    csh.SUBSCRIPTION_SUB_TYPE ='DTV Extra Subscription'
                            and csh.status_code in  ('AC','AB','PC') then 1 else 0  end) as MR
           ,max  (case  when    csh.SUBSCRIPTION_SUB_TYPE ='DTV HD'
                            and csh.status_code in  ('AC','AB','PC') then 1 else 0 end)  as HD
           ,sum  (case  when    csh.SUBSCRIPTION_SUB_TYPE ='DTV Sky+'
                            and csh.status_code in  ('AC','AB','PC') then 1 else 0 end)  as SP




           ,max  (case  when   csh.SUBSCRIPTION_SUB_TYPE ='Broadband DSL Line'
                            and csh.status_code in  ('AC','AB','PC')
                            and csh.current_product_sk=43543                          then 7 -- Sky Fibre Unlimited Pro

                        when    csh.SUBSCRIPTION_SUB_TYPE ='Broadband DSL Line'
                            and csh.status_code in  ('AC','AB','PC')
                            and csh.current_product_sk=43494                          then 6 -- Sky Broadband Unlimited Fibre

                        when    csh.SUBSCRIPTION_SUB_TYPE ='Broadband DSL Line'
                            and csh.status_code in  ('AC','AB','PC')
                            and csh.current_product_sk=43373                          then 5 -- New Unlimited

                        when    csh.SUBSCRIPTION_SUB_TYPE ='Broadband DSL Line'
                            and csh.status_code in  ('AC','AB','PC')
                            and csh.current_product_sk=42128                          then 4 -- Old Unlimited

                        when    csh.SUBSCRIPTION_SUB_TYPE ='Broadband DSL Line'
                            and csh.status_code in  ('AC','AB','PC')
                            and csh.current_product_sk=42129                          then 3 -- Everyday

                        when    csh.SUBSCRIPTION_SUB_TYPE ='Broadband DSL Line'
                            and csh.status_code in  ('AC','AB','PC')
                            and csh.current_product_sk=42130                          then 2 -- Everyday Lite

                        when    csh.SUBSCRIPTION_SUB_TYPE ='Broadband DSL Line'
                            and csh.status_code in  ('AC','AB','PC')
                            and csh.current_product_sk=42131                          then 1 -- BB Connect

                        else                                                               0 -- Nuffing
                     end)  as BB
           ,max  (case  when    csh.subscription_sub_type = 'SKY TALK SELECT'
                            and (csh.status_code = 'A'
                             or (csh.status_code = 'FBP' and prev_status_code in ('PC','A'))
                             or (csh.status_code = 'RI'  and prev_status_code in ('FBP','A'))
                             or (csh.status_code = 'PC'  and prev_status_code = 'A'))
                            and current_product_description like '%Unlimited%'        then 3 -- Unlimited
                        when    csh.subscription_sub_type = 'SKY TALK SELECT'
                            and (csh.status_code = 'A'
                             or (csh.status_code = 'FBP' and prev_status_code in ('PC','A'))
                             or (csh.status_code = 'RI'  and prev_status_code in ('FBP','A'))
                             or (csh.status_code = 'PC'  and prev_status_code = 'A')) then 2 -- Freetime
                        else                                                               0 -- Nuffin
                    end) as ST
           ,max  (case when    csh.SUBSCRIPTION_SUB_TYPE = 'SKY TALK LINE RENTAL'
                            and csh.status_code in  ('A','CRQ','PAX')                 then 1 -- WLR
                        else                                                               0 -- Nuffin
                   end) AS WLR
      into #Attachents---drop table #Attachents
      from sk_prod.cust_subs_hist as csh
           inner join #Contribution_Base as base on csh.account_number = base.account_number
     where csh.effective_from_dt <= base.target_date
       and csh.effective_to_dt    > base.target_date
       and csh.effective_from_dt <> csh.effective_to_dt
       and csh.SUBSCRIPTION_SUB_TYPE in (  'DTV Extra Subscription'
                                          ,'DTV HD','Broadband DSL Line'
                                          ,'SKY TALK SELECT'
                                          ,'SKY TALK LINE RENTAL'
                                          ,'DTV Sky+')
       and csh.status_code in  ('AC','AB','PC','A','L','RI','FBP','CRQ','PAX')
  group by base.id;
------------------------------------------8562434 Row(s) affected
COMMIT;

CREATE UNIQUE HG INDEX idx01 on #Attachents(id);



COMMIT;

UPDATE #Contribution_Base
   SET  HD      = tgt.HD
       ,MR      = tgt.MR
       ,SP      = tgt.SP
       ,WLR     = tgt.WLR
       ,ST_Pack = case when ST = 3 then 'UNL'
                       when ST = 2 then 'FREE'
                       else             null
                    end
       ,BB_Pack = case when BB = 7 then 'FIBRE_PRO'
                       when BB = 6 then 'FIBRE'
                       when BB = 5 then 'NEW_UNL'
                       when BB = 4 then 'OLD_UNL'
                       when BB = 3 then 'ED'
                       when BB = 2 then 'ED_LITE'
                       when BB = 1 then 'CONN'
                       else              null
                    end
  FROM #Contribution_Base as base
       inner join #Attachents as tgt on base.id = tgt.id;
------------------------------------8562434 Row(s) affected
COMMIT;



--------------------------------------------------------Attachment Contribution Calculations

--Check these values are up to date from http://mktskyportal/Campaign%20Handbook/Contribution.aspx

UPDATE #Contribution_Base
   SET xtra_contribution = tgt.HD_Cont + tgt.MR_Cont + tgt.ST_Cont + tgt.WLR_cont + tgt.BB_Cont
  FROM #Contribution_Base as base
       inner join (
            Select id
                   ,7.69 * HD as HD_Cont
                   ,8.26 * MR as MR_Cont
                   ,case when ST_Pack = 'UNL'   and SVBN = 1 then 7.00
                         when ST_Pack = 'UNL'                then 5.77
                         when ST_Pack = 'FREE'  and SVBN = 1 then 5.62
                         when ST_Pack = 'FREE'               then 4.76
                         else                                     0
                     end as ST_Cont
                   ,case when WLR + SVBN = 2 then  2.93
                         when WLR = 1        then -0.38
                         else                      0
                     end as WLR_Cont
                    ,case -- Standalone
                          WHEN DTV = 0 AND SVBN = 1 AND BB_Pack IS NOT NULL             THEN  3.63
                          WHEN DTV = 0 AND SVBN = 0 AND BB_Pack IS NOT NULL             THEN  3.85

                          --SOLUS
                          WHEN WLR = 0 AND ST_Pack IS NULL AND BB_Pack = 'NEW_UNL'      THEN  5.97
                          WHEN WLR = 0 AND ST_Pack IS NULL AND BB_Pack = 'OLD_UNL'      THEN  8.10
                          WHEN WLR = 0 AND ST_Pack IS NULL AND BB_Pack = 'ED'           THEN  4.00
                          WHEN WLR = 0 AND ST_Pack IS NULL AND BB_Pack = 'ED_LITE'      THEN -0.08
                          WHEN WLR = 0 AND ST_Pack IS NULL AND BB_Pack = 'CONN'         THEN  3.00

                          --With NVN (BB + TALK + WLR)
                          WHEN SVBN = 1 AND BB_Pack = 'NEW_UNL'                         THEN  1.50
                          WHEN SVBN = 1 AND BB_Pack = 'OLD_UNL'                         THEN  3.63
                          WHEN SVBN = 1 AND BB_Pack = 'ED'                              THEN -0.30
                          WHEN SVBN = 1 AND BB_Pack = 'ED_LITE'                         THEN -4.55

                          --With No NVN (BB + TALK + WLR)
                          WHEN BB_Pack = 'NEW_UNL'                                      THEN  1.72
                          WHEN BB_Pack = 'OLD_UNL'                                      THEN  3.85
                          WHEN BB_Pack = 'ED'                                           THEN -0.08
                          WHEN BB_Pack = 'ED_LITE'                                      THEN -4.34
                          WHEN BB_Pack = 'CONN'                                         THEN  3.00
                          ELSE                                                                0
                      END AS BB_Cont
              from #Contribution_Base
       )as tgt on base.id = tgt.id;
------------------------------9267178 Row(s) affected
COMMIT;
------------------------------------------------------------Total Contribution

UPDATE  #Contribution_Base
   SET  total_contribution  = DTV_contribution + xtra_contribution + ppv_contribution
       ,no_ppv_contribution = DTV_contribution + xtra_contribution;
-----------------------------9267178 Row(s) affected
COMMIT;

--select top 100 * from #Contribution_Base;
/*
select top 10 * from #Contribution_Base where HD > 0
select top 10 * from #Contribution_Base where SP > 0
select top 10 * from bcg_base_20121130 where Subs_HD1 > 0
select top 10 * from bcg_base_20121130 where Subs_SP1 > 0

*/






alter table bcg_base_20121011
add (total_contribution1                         decimal(10,2)           default 0
    ,no_ppv_contribution1                        decimal(10,2)           default 0
    ,Sports1                                     integer                 default 0
    ,Movies1                                     integer                 default 0
    ,Subs_HD1                                    integer                 default 0
    ,Subs_SP1                                    integer                 default 0
    ,total_contribution_band1                    varchar(25)             default null
    ,box_subscription                           varchar(25)             default null
    ,BB_Pack1                                   varchar(25)             default null);


update bcg_base_20121011
   set box_subscription         = case when Subs_HD1 = 1 then 'HD'
                                       when Subs_SP1 = 1 then 'Sky Plus'
                                  else                       'FDB'
                                  end;





update bcg_base_20121011
   set
   total_contribution1       = tgt.total_contribution
   ,no_ppv_contribution1      = tgt.no_ppv_contribution
   ,Sports1                   = tgt.Sports
   ,Movies1                  = tgt.Movies
   ,Subs_HD1                  = tgt.HD
   ,Subs_SP1                  = tgt.SP
   ,total_contribution_band1  = case when tgt.total_contribution <=  20 then '01 <=£20'
                                       when tgt.total_contribution <=  30 then '02 £20-30'
                                       when tgt.total_contribution <=  40 then '02 £30-40'
                                       when tgt.total_contribution <=  50 then '02 £40-50'
                                       when tgt.total_contribution <=  60 then '02 £50-60'
                                       when tgt.total_contribution <=  70 then '02 £60-70'
                                       when tgt.total_contribution <=  80 then '02 £70-80'
                                       when tgt.total_contribution >   80 then '08 £80+'
                                       else                                '09 Unknown'
                                   end
     -- ,box_subscription         = case when Subs_HD1 = 1 then 'HD'
                                     --  when Subs_SP1 = 1 then 'Sky Plus'
                                 -- else                       'FDB'
                                  --end
      ,BB_Pack1                  = tgt.BB_Pack
 from bcg_base_20121011 as base
      inner join #Contribution_Base as tgt on base.account_number = tgt.account_number;
----------------------9267178 Row(s) affected
COMMIT;




/*

select count(*), sum(churn_flag), churn_type from bcg_base_20121130 group by churn_type order by churn_type
select count(*), box_subscription from bcg_base_20121011 group by box_subscription

*/

-----------------------------Latest Tenure Group-----------------




--tenure group - latest activation
  select base.account_number
        ,max(csh.status_start_dt) as last_activation_dt
    into --drop table
        #latest
    from bcg_base_20121011 as base
         inner join sk_prod.cust_subs_hist as csh on base.account_number = csh.account_number
                                                 and target_dt >= csh.status_start_dt
   where csh.subscription_type = 'DTV PACKAGE'
     and csh.subscription_sub_type = 'DTV Primary Viewing'
     and csh.status_code_changed = 'Y'
     and csh.status_code = 'AC'
     and csh.prev_status_code not in ('PC','AB')
group by base.account_number;
--------------------------9266436 Row(s) affected


alter table bcg_base_20121011
        add latest_activation_dt       date
       ,add tenure_latest              integer     default null
       ,add tenure_group_latestactive  varchar(50) default '09 Unknown';

update bcg_base_20121011
   set latest_activation_dt = tgt.last_activation_dt
  from bcg_base_20121011 as base
       inner join #latest as tgt on base.account_number = tgt.account_number;
-------------------9266436 Row(s) affected

update bcg_base_20121011
   set tenure_latest = target_dt - latest_activation_dt;
---------------------9267178 Row(s) affected


select count(*),tenure_latest from bcg_base_20121011 group by tenure_latest

update bcg_base_20121011
   set tenure_group_latestactive = case when tenure_latest <=  90  then '01 1-3 months'
                                        when tenure_latest <=  180 then '02 4-6 months'
                                        when tenure_latest <=  365 then '03 6-12 months'
                                        when tenure_latest <=  730 then '04 1-2 years'
                                        when tenure_latest <= 1095 then '05 2-3 Yrs'
                                        when tenure_latest <= 1824 then '06 3-5 Yrs'
                                        when tenure_latest <= 3648 then '07 5-10 Yrs'
                                        when tenure_latest >  3648 then '08 10+ Yrs'
                                             else                       '09 Unknown'
                                    end;
---------------------9267178 Row(s) affected


----------------------TAs


select a.account_number
      ,a.target_dt
      ,b.cb_row_id as attempt_row_id
      ,b.attempt_date
      ,b.change_attempt_sk
      ,b.Wh_Attempt_Outcome_Description_1
      ,b.Wh_Attempt_Reason_Description_1
      ,b.order_id
      ,case when b.Wh_Attempt_Outcome_Description_1 in
        ('Turnaround Saved','Legacy Save','Home Move Saved','Home Move Accept Saved','Nursery Saved')
       then 1 else 0 end as saved
into #ta_attempts
from bcg_base_20121011 as a
     left outer join sk_prod.cust_change_attempt as b
  on a.account_number=b.account_number
where b.change_attempt_type='CANCELLATION ATTEMPT'
  and b.attempt_date<= a.target_dt
  and b.created_by_id not in ('dpsbtprd','batchuser')
  and b.Wh_Attempt_Outcome_Description_1 in
('Turnaround Saved','Turnaround Not Saved','Legacy Save','Legacy Fail','Home Move Outcome Not Applicable','Home Move Saved','Home Move Not Saved','Home Move Accept Saved','Nursery Saved', 'Nursery Not Saved')
order by a.account_number,a.target_dt,b.attempt_date,saved desc;
commit;
-----------------21002821 Row(s) affected
create hg index indx on #ta_attempts(account_number);



--ta attempt saved


select distinct account_number
      ,max(case when attempt_date<=target_dt and saved = 1 then attempt_date end) as last_ta_saved_dt
      ,max(case when attempt_date between dateadd(mm,-3,target_dt) and target_dt and saved = 1 then 1 else 0 end) as ta_saved_3m
      ,sum(case when attempt_date between dateadd(mm,-3,target_dt) and target_dt and saved = 1 then 1 else 0 end) as num_ta_saved_3m
      ,max(case when attempt_date between dateadd(mm,-6,target_dt) and target_dt and saved = 1 then 1 else 0 end) as ta_saved_6m
      ,sum(case when attempt_date between dateadd(mm,-2,target_dt) and target_dt and saved = 1 then 1 else 0 end) as num_ta_saved_6m

      ,max(case when attempt_date<=target_dt and saved = 1 then 1 else 0 end) as ta_saved_ever
      ,sum(case when attempt_date<=target_dt and saved = 1 then 1 else 0 end) as num_ta_saved_ever
into ---drop table
   #ta_attempts_saved
from #ta_attempts
group by account_number;
--------------------3385455 Row(s) affected


select count(account_number), count(distinct account_number) from #ta_attempts_saved

commit;
create hg index indx on #ta_attempts_saved (account_number);



alter table bcg_base_20121011
add (ta_saved_3m integer default 0
    ,num_ta_saved_3m integer default 0
    ,ta_saved_6m integer default 0
    ,num_ta_saved_6m integer default 0);

alter table bcg_base_20121011
add (ta_saved_ever integer default 0
   ,num_ta_saved_ever integer default 0);


update bcg_base_20121011 as a
set
a.ta_saved_3m = b.ta_saved_3m
,a.num_ta_saved_3m = b.num_ta_saved_3m
,a.ta_saved_6m = b.ta_saved_6m
,a.num_ta_saved_6m = b.num_ta_saved_6m
,a.ta_saved_ever  =  b.ta_saved_ever
,a.num_ta_saved_ever  = b.num_ta_saved_ever

from #ta_attempts_saved as b
where a.account_number=b.account_number
;
commit;

----------------------3385455 Row(s) affected





--ta attempt not saved
select distinct account_number
      ,max(case when attempt_date<=target_dt and saved = 0 then attempt_date end) as last_ta_notsaved_dt
      ,max(case when attempt_date between dateadd(mm,-3,target_dt) and target_dt and saved = 0 then 1 else 0 end) as ta_notsaved_3m
      ,sum(case when attempt_date between dateadd(mm,-3,target_dt) and target_dt and saved = 0 then 1 else 0 end) as num_ta_notsaved_3m
      ,max(case when attempt_date between dateadd(mm,-6,target_dt) and target_dt and saved = 0 then 1 else 0 end) as ta_notsaved_6m
      ,sum(case when attempt_date between dateadd(mm,-6,target_dt) and target_dt and saved = 0 then 1 else 0 end) as num_ta_notsaved_6m

      ,max(case when attempt_date<=target_dt and saved = 0 then 1 else 0 end) as ta_notsaved_ever
      ,sum(case when attempt_date<=target_dt and saved = 0 then 1 else 0 end) as num_ta_notsaved_ever
into #ta_attempts_notsaved
from #ta_attempts
group by account_number;
---------------3385455 Row(s) affected





commit;
create hg index indx on #ta_attempts_notsaved (account_number);




alter table bcg_base_20121011
add (ta_notsaved_3m integer default 0
    ,num_ta_notsaved_3m integer default 0
    ,ta_notsaved_6m integer default 0
    ,num_ta_notsaved_6m integer default 0
    ,ta_notsaved_ever  integer default 0
    ,num_ta_notsaved_ever integer default 0);


update bcg_base_20121011 as a
set
a.ta_notsaved_3m = b.ta_notsaved_3m
,a.num_ta_notsaved_3m = b.num_ta_notsaved_3m
,a.ta_notsaved_6m = b.ta_notsaved_6m
,a.num_ta_notsaved_6m = b.num_ta_notsaved_6m
,a.ta_notsaved_ever  =  b.ta_notsaved_ever
,a.num_ta_notsaved_ever  = b.num_ta_notsaved_ever

from #ta_attempts_notsaved as b
where a.account_number=b.account_number
;---------------------3385455 Row(s) affected
commit;



-----------------------------------------------------Bills History----------------------------


--get all bill info for accounts in the analysis
SELECT a.account_number
      ,a.target_dt
      ,b.balance_zero_dt
      ,b.payment_due_dt
      ,b.status
      ,b.total_paid_amt
      ,b.bill_period
      ,b.sequence_num
      ,b.payment_method
INTO --drop table
        #temp_bill_history
FROM bcg_base_20121011 as a
left outer join
sk_prod.cust_bills as b
on a.account_number=b.account_number
WHERE b.preparation_date<=a.target_dt
and b.status<>'Unbilled'
and b.payment_due_dt>=target_dt-365
and b.payment_due_dt<'9999-09-09'
and b.total_due_amt>0;
COMMIT;-----------------110670074 Row(s) affected
create hg index idx1 on #temp_bill_history (account_number);

--CHECK SELECT count(*) FROM #temp_bill_history

--obtain latest bill
SELECT account_number
      ,max(sequence_num) as latest_bill_number
INTO --drop table
        #temp_latest_bill_number
FROM #temp_bill_history
GROUP BY account_number
        ,target_dt
;
COMMIT;
create hg index idx1 on #temp_latest_bill_number (account_number);
-----------------------9246099 Row(s) affected
--CHECK SELECT count(*),count(distinct account_number) FROM #temp_latest_bill_number;

/*

select top 100 * from #temp_bill_history;
select top 100 * from #temp_latest_bill_number;

*/




--previous bill behaviour
SELECT a.account_number

      ,sum(case when payment_due_dt between dateadd(mm,-6,a.target_dt) and a.target_dt and balance_zero_dt>payment_due_dt then 1 else 0 end) as late_paid_bills_6m

      ,max(case when b.sequence_num=c.latest_bill_number then payment_method else null end) as payment_method_at_snapshot
      ,max(case when b.sequence_num=c.latest_bill_number and balance_zero_dt>payment_due_dt and a.target_dt>payment_due_dt then 1 else 0 end) as payment_late_at_snapshot
INTO --drop table
        #temp_bill_summary
FROM bcg_base_20121011 as a
left outer join
        #temp_bill_history as b
on a.account_number=b.account_number

left outer join
        #temp_latest_bill_number as c
on b.account_number=c.account_number

GROUP BY a.account_number
       ;
COMMIT;-------------------9267178 Row(s) affected
create hg index idx1 on #temp_bill_summary (account_number);

--CHECK SELECT count(*) FROM #temp_bill_summary


/*
select top 100 * from #temp_bill_summary


select count(*), late_paid_bills_6m from #temp_bill_summary group by late_paid_bills_6m
select count(*), bills_12m from #temp_bill_summary group by bills_12m
select count(*), bills_12m from #temp_bill_summary group by bills_12m
*/


alter table bcg_base_20121011
add     (bills_12m tinyint default 0
        ,late_paid_bills_3m tinyint default 0
        ,late_paid_bills_6m tinyint default 0
        ,payment_method_at_snapshot varchar(16) default null
        ,payment_late_at_snapshot tinyint default 0
        ,first_bill_paid tinyint default 0);

--add to base table
UPDATE bcg_base_20121011 as a
SET
a.late_paid_bills_6m = b.late_paid_bills_6m
,a.payment_method_at_snapshot = b.payment_method_at_snapshot
,a.payment_late_at_snapshot = b.payment_late_at_snapshot
FROM #temp_bill_summary as b
WHERE a.account_number = b.account_number

;
COMMIT;
-------------------9267178 Row(s) affected
---CHECK SELECT late_paid_bills_6m, count(*) FROM bcg_base_20121130 GROUP BY rollup(late_paid_bills_6m)


---------------------------------------Service Calls-------------------------------------------



----------------------------------------------------------------------------------------------------
----- D223 Obtain Customer_Service_Calls
SELECT a.account_number
       ,a.target_dt
      ,cast(response_datetime as date) as call_date
      ,call_category
      ,call_reason

INTO ---drop table
      #temp_merlin_data_cs
FROM bcg_base_20121011 a
     inner join sk_prod.CUST_IS_TREATMENT_OUTCOME b on b.account_number = a.account_number
WHERE call_date between dateadd(mm,-12,target_dt) and target_dt
GROUP BY a.account_number
        ,a.target_dt
        ,call_date
        ,call_category
        ,call_reason;
COMMIT;
--------------------11908636 Row(s) affected
/*
select distinct call_category from #temp_merlin_data_cs;

select call_category, count(*) from #temp_merlin_data_cs group by call_category;

select product_holding, count(*), sum(churn_cuscan) from bcg_base_20121011 group by product_holding

select top 10 * from bcg_base_20121130
select tv_package, count(*), sum(churn_cuscan) from bcg_base_20121011 group by tv_package order by tv_package
select tv_package, count(*), sum(churn_cuscan) from bcg_base_20121011 group by tv_package order by mix_pack

*/


--CHECK SELEC

SELECT account_number
      ,target_dt
      ,max(call_date) as date_last_cs_call  --DP
      ,max(case when (call_date between dateadd(mm,-3,target_dt) and target_dt) then 1 else 0 end) as cs_call_3m
      ,sum(case when (call_date between dateadd(mm,-3,target_dt) and target_dt) then 1 else 0 end) as num_cs_call_3m
      ,max(case when (call_date between dateadd(mm,-6,target_dt) and target_dt) then 1 else 0 end) as cs_call_6m
      ,sum(case when (call_date between dateadd(mm,-6,target_dt) and target_dt) then 1 else 0 end) as num_cs_call_6m
      ,max(case when (call_date<=target_dt) then call_date else null end) as last_cs_call_dt
INTO ---drop table
      #temp_merlin_data_cs2
FROM #temp_merlin_data_cs
GROUP BY account_number
        ,target_dt
;------------------4357628 Row(s) affected
COMMIT;

--add calls to base table


alter table bcg_base_20121011
add (cs_call_3m     integer default 0
    ,num_cs_call_3m integer default 0
    ,cs_call_6m     integer default 0
    ,num_cs_call_6m integer default 0)
;
alter table bcg_base_20121011
add last_cs_call_6months integer default 0;


UPDATE bcg_base_20121011 a
SET


a.cs_call_3m = b.cs_call_3m
,a.num_cs_call_3m = b.num_cs_call_3m
,a.cs_call_6m = b.cs_call_6m
,a.num_cs_call_6m = b.num_cs_call_6m

,a.last_cs_call_6months = case when (case when day(b.target_dt) < day(b.last_cs_call_dt)
                                     then datediff(mm, b.last_cs_call_dt,b.target_dt) - 1
                                     else datediff(mm,b.last_cs_call_dt,b.target_dt) end) <=6 then 1 else 0 end
FROM #temp_merlin_data_cs2 b
WHERE b.account_number = a.account_number
AND   b.target_dt = a.target_dt;

-----------------------4357628 Row(s) affected

-------------------Upgrades---------------------


-------------Upgrades-----------------


select cmsh.account_number
      ,count(cmsh.TypeOfChange) as count_upgrades
      ,sum(case when datediff(dd, cmsh.effective_from_dt, base.target_dt) < 90 then 1 else 0 end) as upgrades_3m
      ,sum(case when datediff(dd, cmsh.effective_from_dt, base.target_dt) < 180 then 1 else 0 end) as upgrades_6m
      into ---drop table
            #upgrades
      from yarlagaddar.View_CUST_MOVIES_SPORTS_DOWNGRADES_UPGRADES_HIST cmsh
      inner join bcg_base_20121011 base
      on base.account_number = cmsh.account_number
      where effective_from_dt <= target_dt
      ---and cmsh.TypeOfChange in ('MU', 'SU')
      group by cmsh.account_number;

--------5566500 Row(s) affected


select top 100 * from #upgrades
select count(account_number), count(distinct account_number) from #upgrades;

alter table bcg_base_20121011
add (count_upgrades integer default 0
    ,upgrades_3m    integer default 0
    ,upgrades_6m    integer default 0);


update bcg_base_20121011 base
set base.count_upgrades = temp.count_upgrades
   ,base.upgrades_3m    = temp.upgrades_3m
   ,base.upgrades_6m    = temp.upgrades_6m
from #upgrades temp
where base.account_number = temp.account_number;

-------------5566500 Row(s) affected



------------------------------NLP----------------------

----------------------------------New Line--------------------



 select  bbs.account_number
         ,bbs.target_dt
         ,cns.status_start_dt
         ,cns.effective_from_dt
         ,cns.current_product_description as NLP_type
         ,rank() over(partition by bbs.account_number, bbs.target_dt, current_product_description ORDER BY effective_from_dt desc, cb_row_id desc) as prank
    INTO ----drop table
         #temp_nlp_sales
    FROM sk_prod.CUST_NON_SUBSCRIPTIONS as cns
         inner join bcg_base_20121011 as bbs on cns.account_number = bbs.account_number
   WHERE current_product_description in ('Additional Phone Line Installation'
                                        ,'New Phone Line Installation'
                                        ,'Phone Line Activation'
                                        ,'Transfer from Unbundled Phone Line')
     and effective_from_dt <= target_dt
     and effective_from_dt < effective_to_dt;
---------------------568357 Row(s) affected
COMMIT;

DELETE FROM  #temp_nlp_sales WHERE prank > 1;
---------------------2911 Row(s) affected


----CHECK: select NLP_type, count(*) FROM #temp_nlp_sales GROUP BY NLP_type



alter table bcg_base_20121011
add     (NLP tinyint default 0
        ,NLP_type varchar(50) default 'No_NLP');


--add data to base table
UPDATE bcg_base_20121011 as a
SET a.NLP=1,
    a.NLP_type=b.NLP_type
FROM #temp_nlp_sales as b
WHERE a.account_number=b.account_number
AND   a.target_dt = b.target_dt;
COMMIT;
--------------------564732 Row(s) affected
----CHECK: select NLP, NLP_type, count(*) FROM bcg_base_20121130 GROUP BY rollup(NLP, NLP_type)


-------------------GET UPDATED VALUE SEGMENTS-------------------------------

-----------------------------ADDING HISTORICAL VALUE SEGMENTS FOR THE TIME PERIOD USED----------------------
-----drop table #value_segments

CREATE TABLE #value_segments(
        id                      BIGINT          IDENTITY PRIMARY KEY
       ,account_number          VARCHAR(20)     NOT NULL
       ,subscription_id         VARCHAR(50)     NULL
       ,target_date             DATE            NOT NULL
       ,segment                 VARCHAR(20)     NULL
       ,first_activation_dt     DATE            NULL
       ,active_days             INTEGER         NULL
       ,CUSCAN_ever             INTEGER         DEFAULT 0
       ,CUSCAN_2Yrs             INTEGER         DEFAULT 0
       ,SYSCAN_ever             INTEGER         DEFAULT 0
       ,SYSCAN_2Yrs             INTEGER         DEFAULT 0
       ,AB_ever                 INTEGER         DEFAULT 0
       ,AB_2Yrs                 INTEGER         DEFAULT 0
       ,PC_ever                 INTEGER         DEFAULT 0
       ,PC_2Yrs                 INTEGER         DEFAULT 0
       ,TA_2yrs                 INTEGER         DEFAULT 0
       ,min_prem_2yrs           INTEGER         DEFAULT 0
       ,max_prem_2yrs           INTEGER         DEFAULT 0
);

CREATE   HG INDEX idx01 ON #value_segments(account_number);
CREATE DATE INDEX idx02 ON #value_segments(target_date);
CREATE   LF INDEX idx03 ON #value_segments(segment);
CREATE   HG INDEX idx04 ON #value_segments(subscription_id);


-------------------------------------------------  02 - Populate table

-- Alter this query to append the accounts and specific dates you want to identify the segments for.

  INSERT INTO #value_segments (account_number, target_date)
  SELECT account_number, target_dt
    FROM bcg_base_20121011
   ;

---------------------------9267178 Row(s) affected

------------------------------------------------ 03 - SAV = First Activation Date & Subscription ID


UPDATE  #value_segments
   SET  first_activation_dt   = sav.ph_subs_first_activation_dt
       ,subscription_id       = sav.prod_ph_subs_subscription_id
  FROM #value_segments AS acc
       INNER JOIN sk_prod.cust_single_account_view AS sav ON acc.account_number = sav.account_number;
------------------------9267175 Row(s) affected

UPDATE #value_segments
   SET active_days = DATEDIFF(day,first_activation_dt,target_date);

------------------------9267178 Row(s) affected

  SELECT account_number, subscription_id, target_date
    INTO #account_list
    FROM #value_segments
;
------------------------9267178 Row(s) affected
CREATE UNIQUE HG INDEX idx01 ON #account_list(account_number);
CREATE HG INDEX idx02 ON #account_list(subscription_id);

--historic status event changes
--drop table #status_events


CREATE TABLE #status_events (
        id                      BIGINT          IDENTITY     PRIMARY KEY
       ,account_number          VARCHAR(20)     NOT NULL
       ,effective_from_dt       DATE            NOT NULL
       ,status_code             VARCHAR(2)      NOT NULL
       ,event_type              VARCHAR(20)     NOT NULL
);

CREATE   HG INDEX idx01 ON #status_events(account_number);
CREATE   LF INDEX idx02 ON #status_events(event_type);
CREATE DATE INDEX idx03 ON #status_events(effective_from_dt);


INSERT INTO #status_events (account_number, effective_from_dt, status_code, event_type)
SELECT  csh.account_number
       ,csh.effective_from_dt
       ,csh.status_code
       ,CASE WHEN status_code = 'PO'              THEN 'CUSCAN'
             WHEN status_code = 'SC'              THEN 'SYSCAN'
             WHEN status_code = 'AB'              THEN 'ACTIVE BLOCK'
             WHEN status_code = 'PC'              THEN 'PENDING CANCEL'
         END AS event_type
  FROM sk_prod.cust_subs_hist AS csh
       INNER JOIN #account_list AS al ON csh.account_number = al.account_number
 WHERE csh.subscription_sub_type = 'DTV Primary Viewing'
   AND csh.status_code_changed = 'Y'
   AND csh.effective_from_dt <= al.target_date
   AND (    (csh.status_code IN ('AB','PC') AND csh.prev_status_code = 'AC')
         OR (csh.status_code IN ('PO','SC') AND csh.prev_status_code IN ('AC','AB','PC'))
       );

--------------------------------8277792 Row(s) affected
-- Update value Segments

UPDATE  #value_segments
   SET  CUSCAN_ever             = tgt.CUSCAN_ever
       ,CUSCAN_2Yrs             = tgt.CUSCAN_2Yrs
       ,SYSCAN_ever             = tgt.SYSCAN_ever
       ,SYSCAN_2Yrs             = tgt.SYSCAN_2Yrs
       ,AB_ever                 = tgt.AB_ever
       ,AB_2Yrs                 = tgt.AB_2Yrs
       ,PC_ever                 = tgt.PC_ever
       ,PC_2Yrs                 = tgt.PC_2Yrs
  FROM #value_segments AS base
       INNER JOIN (
                    SELECT vs.id

                           --CUSCAN
                           ,SUM(CASE WHEN se.status_code = 'PO'
                                      AND  se.effective_from_dt <= vs.target_date
                                     THEN 1 ELSE 0 END) AS CUSCAN_ever
                           ,SUM(CASE WHEN se.status_code = 'PO'
                                      AND se.effective_from_dt BETWEEN DATEADD(year,-2,vs.target_date) AND vs.target_date
                                     THEN 1 ELSE 0 END) AS CUSCAN_2Yrs

                           --SYSCAN
                           ,SUM(CASE WHEN se.status_code = 'SC'
                                      AND  se.effective_from_dt <= vs.target_date
                                     THEN 1 ELSE 0 END) AS SYSCAN_ever
                           ,SUM(CASE WHEN se.status_code = 'SC'
                                      AND se.effective_from_dt BETWEEN DATEADD(year,-2,vs.target_date) AND vs.target_date
                                     THEN 1 ELSE 0 END) AS SYSCAN_2Yrs

                           --Active Block
                           ,SUM(CASE WHEN se.status_code = 'AB'
                                      AND se.effective_from_dt <= vs.target_date
                                     THEN 1 ELSE 0 END) AS AB_ever
                           ,SUM(CASE WHEN se.status_code = 'AB'
                                      AND se.effective_from_dt BETWEEN DATEADD(year,-2,vs.target_date) AND vs.target_date
                                     THEN 1 ELSE 0 END) AS AB_2Yrs

                           --Pending Cancel
                           ,SUM(CASE WHEN se.status_code = 'PC'
                                      AND se.effective_from_dt <= vs.target_date
                                     THEN 1 ELSE 0 END) AS PC_ever
                           ,SUM(CASE WHEN se.status_code = 'PC'
                                      AND se.effective_from_dt BETWEEN DATEADD(year,-2,vs.target_date) AND vs.target_date
                                     THEN 1 ELSE 0 END) AS PC_2Yrs
                      FROM #value_segments AS vs
                           INNER JOIN #status_events AS se ON vs.account_number = se.account_number
                  GROUP BY vs.id
       )AS tgt on base.id = tgt.id;

--------------------------------2179584 Row(s) affected
------------------------------------------------------------ 05 - TA Events


--List all unique days with TA event
SELECT  DISTINCT
        cca.account_number
       ,cca.attempt_date
  INTO ---drop table
        #ta
  FROM sk_prod.cust_change_attempt AS cca
       INNER JOIN #account_list AS al  on cca.account_number = al.account_number
                                      AND cca.subscription_id = al.subscription_id
 WHERE change_attempt_type = 'CANCELLATION ATTEMPT'
   AND created_by_id NOT IN ('dpsbtprd', 'batchuser')
   AND Wh_Attempt_Outcome_Description_1 in ( 'Turnaround Saved'
                                            ,'Legacy Save'
                                            ,'Turnaround Not Saved'
                                            ,'Legacy Fail'
                                            ,'Home Move Saved'
                                            ,'Home Move Not Saved'
                                            ,'Home Move Accept Saved')
   AND cca.attempt_date BETWEEN DATEADD(day,-729,al.target_date) AND al.target_date ;
-------------------------------2198433 Row(s) affected

CREATE HG INDEX idx01 ON #ta(account_number);

-- Update TA flags
UPDATE  #value_segments
   SET  TA_2Yrs = tgt.ta_2Yrs
  FROM #value_segments AS base
       INNER JOIN (
                    SELECT vs.id
                          ,SUM(CASE WHEN ta.attempt_date BETWEEN DATEADD(day,-729,vs.target_date) AND vs.target_date
                                     THEN 1 ELSE 0 END) AS ta_2Yrs
                      FROM #value_segments AS vs
                           INNER JOIN #ta AS ta ON vs.account_number = ta.account_number
                  GROUP BY vs.id
       )AS tgt on base.id = tgt.id;
------------------------------------1582598 Row(s) affected
------------------------------------------------------ 06 - Min Max Premiums

UPDATE  #value_segments
   SET  min_prem_2Yrs = tgt.min_prem_lst_2_yrs
       ,max_prem_2Yrs = tgt.max_prem_lst_2_yrs
  FROM  #value_segments AS acc
        INNER JOIN (
                   SELECT  base.id
                          ,MAX(cel.prem_movies + cel.prem_sports ) as max_prem_lst_2_yrs
                          ,MIN(cel.prem_movies + cel.prem_sports ) as min_prem_lst_2_yrs
                     FROM sk_prod.cust_subs_hist as csh
                          INNER JOIN #value_segments as base on csh.account_number = base.account_number
                          INNER JOIN sk_prod.cust_entitlement_lookup as cel on csh.current_short_description = cel.short_description
                    WHERE csh.subscription_type      =  'DTV PACKAGE'
                      AND csh.subscription_sub_type  =  'DTV Primary Viewing'
                      AND status_code in ('AC','AB','PC')
                      AND ( -- During 2 year Period
                            (    csh.effective_from_dt BETWEEN DATEADD(day,-729,base.target_date) AND base.target_date
                             AND csh.effective_to_dt >= csh.effective_from_dt
                             )
                            OR -- at start of 2 yr period
                            (    csh.effective_from_dt <= DATEADD(day,-729,base.target_date)
                             AND csh.effective_to_dt   > DATEADD(day,-729,base.target_date)  -- limit to report period
                             )
                          )
                  GROUP BY base.id
        )AS tgt ON acc.id = tgt.id;

----------------------------------9267134 Row(s) affected

------------------------------------------------------ 07 - Make Value Segments


UPDATE #value_segments
   SET       segment =     CASE WHEN active_days < 729                            -- All accounts in first 2 Years
                                THEN 'BEDDING IN'

                                WHEN active_days >= 1825                          -- 5 Years
                                 AND CUSCAN_ever + SYSCAN_ever = 0                -- Never Churned
                                 AND AB_ever + PC_ever = 0                        -- Never AB/PC ed
                                 AND ta_2Yrs = 0                                  -- No TA's in last 2 years
                                 AND min_prem_2yrs = 4                            -- Always top tier for last 2 years
                                THEN 'PLATINUM'

                                WHEN active_days >= 1825                          -- 5 Years
                                 AND CUSCAN_ever + SYSCAN_ever = 0                -- Never Churned
                                 AND AB_ever + PC_ever = 0                        -- Never AB/PC ed
                                 AND min_prem_2yrs > 0                            -- Always had Prems in last 2 Years
                                THEN 'GOLD'

                                WHEN CUSCAN_2Yrs + SYSCAN_2Yrs = 0                -- No Churn in last 2 years
                                 AND AB_2Yrs + PC_2Yrs = 0                        -- No AB/PC 's In last 2 Years
                                 AND min_prem_2yrs > 0                            -- Always had Prems in last 2 Years
                                THEN 'SILVER'

                                WHEN CUSCAN_ever + SYSCAN_ever > 0                -- All Churners
                                  OR AB_2Yrs + PC_2Yrs + ta_2Yrs >= 3             -- Blocks , cancels in last 2 years + ta in last 2 years >= 3
                                THEN 'UNSTABLE'

                                WHEN max_prem_2Yrs > 0                            -- Has Had prems in last 2 years
                                THEN 'BRONZE'

                                ELSE 'COPPER'                                        -- everyone else
                            END;



---------------------------9267178 Row(s) affected



alter table bcg_base_20121011
add value_seg_updated varchar(25) default null;


update bcg_base_20121011 base
set base.value_seg_updated = temp.segment
from #value_segments temp
where base.account_number = temp.account_number;
----------------------9267178 Row(s) affected



---------------------------------------------------------------------------------------------------------------------------
--------------------------SET RULES FOR SYSCAN------------------------------------------------------


alter table bcg_base_20121130
drop unstable_syscan_rule

-------------Unstable Segment--------------


alter table bcg_base_20121011
add  (unstable_syscan_rule1                         varchar(20) default 'Unknown'
     ,beddingin_syscan_rule1                        varchar(20) default 'Unknown'
     ,EOC_syscan_rule1                              varchar(20) default 'Unknown'
     ,longtenure_syscan_rule1                       varchar(20) default 'Unknown'
     ,unstable_cuscan_rule1                         varchar(20) default 'Unknown'
     ,beddingin_cuscan_rule1                        varchar(20) default 'Unknown'
     ,EOC_cuscan_rule1                              varchar(20) default 'Unknown'
     ,longtenure_cuscan_rule1                       varchar(20) default 'Unknown');



update bcg_base_20121130
set unstable_syscan_rule1 =  case when value_seg_updated = 'UNSTABLE' then
                                case when cqm_grp in ('01 1-10') and tenure_group_latestactive in ('01 1-3 months','02 4-6 months','03 6-12 months','05 2-3 Yrs') Then 'Rule1'
                                     when cqm_grp in ('01 1-10') and tenure_group_latestactive in ('04 1-2 years','06 3-5 Yrs','08 10+ Yrs') Then 'Rule2'
                                     when cqm_grp in ('01 1-10') and tenure_group_latestactive in ('07 5-10 Yrs') Then 'Rule3'
                                     when cqm_grp in ('02 11-22') and tenure_group_latestactive in ('01 1-3 months','02 4-6 months','03 6-12 months') Then 'Rule4'
                                     when cqm_grp in ('02 11-22') and tenure_group_latestactive in ('04 1-2 years','05 2-3 Yrs') Then 'Rule5'
                                     when cqm_grp in ('02 11-22') and tenure_group_latestactive in ('06 3-5 Yrs','07 5-10 Yrs','08 10+ Yrs') Then 'Rule6'
                                     when cqm_grp in ('03 23-29','04 30-33') and tenure_group_latestactive in ('01 1-3 months','02 4-6 months') Then 'Rule7'
                                     when cqm_grp in ('03 23-29','04 30-33') and tenure_group_latestactive in ('03 6-12 months','04 1-2 years','05 2-3 Yrs') Then 'Rule8'
                                     when cqm_grp in ('03 23-29','04 30-33') and tenure_group_latestactive in ('06 3-5 Yrs','07 5-10 Yrs','08 10+ Yrs') Then 'Rule9'
                                     when cqm_grp in ('05 34-36') and tenure_group_latestactive in ('01 1-3 months','02 4-6 months','03 6-12 months') Then 'Rule10'
                                     when cqm_grp in ('05 34-36') and tenure_group_latestactive in ('04 1-2 years','05 2-3 Yrs') Then 'Rule11'
                                     when cqm_grp in ('05 34-36') and tenure_group_latestactive in ('06 3-5 Yrs','07 5-10 Yrs','08 10+ Yrs') Then 'Rule12'
                                     when cqm_grp in ('Unknown') and Movies1 = 0 or Movies1 = 1 Then 'Rule13'
                                     when cqm_grp in ('Unknown') and Movies1 = 2 Then 'Rule14'
                                     else 'Unknown'
                                     end
                                else 'Unknown'
                                end;


------------------------------9258850


--------------------Bedding In Segment

update bcg_base_20121130
set beddingin_syscan_rule1 =  case when value_seg_updated <> 'UNSTABLE'
                                   and tenure <=  240 then
                                   case when cqm_grp in ('01 1-10') and Box_Subscription in ('FDB','Sky Plus') Then 'Rule1'
                                        when cqm_grp in ('01 1-10') and Box_Subscription in ('HD') Then 'Rule2'
                                        when cqm_grp in ('02 11-22') and affluence in ('High','Mid','Mid High','Mid Low','Very High') and Movies1 = 0 Then 'Rule3'
                                        when cqm_grp in ('02 11-22') and affluence in ('High','Mid','Mid High','Mid Low','Very High') and Movies1 = 1 or Movies1 = 2 Then 'Rule4'
                                        when cqm_grp in ('02 11-22') and affluence in ('Low','Very Low') Then 'Rule5'
                                        when cqm_grp in ('02 11-22') and affluence in ('Unknown') Then 'Rule6'
                                        when cqm_grp in ('03 23-29') and affluence in ('High','Mid','Mid High','Mid Low','Unknown','Very High') Then 'Rule7'
                                        when cqm_grp in ('03 23-29') and affluence in ('Low','Very Low') and tenure_group_latestactive in ('01 1-3 months') Then 'Rule8'
                                        when cqm_grp in ('03 23-29') and affluence in ('Low','Very Low') and tenure_group_latestactive in ('02 4-6 months') Then 'Rule9'
                                        when cqm_grp in ('03 23-29') and affluence in ('Low','Very Low') and tenure_group_latestactive in ('03 6-12 months','07 5-10 Yrs') Then 'Rule10'
                                        when cqm_grp in ('04 30-33') and tenure_group_latestactive in ('01 1-3 months') Then 'Rule11'
                                        when cqm_grp in ('04 30-33') and tenure_group_latestactive in ('02 4-6 months') Then 'Rule12'
                                        when cqm_grp in ('04 30-33') and tenure_group_latestactive in ('03 6-12 months') Then 'Rule13'
                                        when cqm_grp in ('04 30-33') and tenure_group_latestactive in ('07 5-10 Yrs','08 10+ Yrs') Then 'Rule14'
                                        when cqm_grp in ('05 34-36') and tenure_group_latestactive in ('01 1-3 months') Then 'Rule15'
                                        when cqm_grp in ('05 34-36') and tenure_group_latestactive in ('02 4-6 months','03 6-12 months') Then 'Rule16'
                                        when cqm_grp in ('Unknown') and affluence in ('High','Mid','Mid High','Very High') Then 'Rule17'
                                        when cqm_grp in ('Unknown') and affluence in ('Low','Mid Low','Very Low') Then 'Rule18'
                                        when cqm_grp in ('Unknown') and affluence in ('Unknown') Then 'Rule19'
                                        else 'Unknown'

                                     end
                                else 'Unknown'
                                end;

----------------------------------9258850
/*
select count(*) from bcg_base_20121130 where cqm_grp in ('01 1-10') and Box_Subscription in ('HD') and value_seg_updated <> 'UNSTABLE'
                                   and tenure <=  240
select distinct cqm_grp from bcg_base_20121130 where value_seg_updated <> 'UNSTABLE'
                                   and tenure <=  240

select count(*) from bcg_base_20121130  where cqm_grp in ('02 11-22') and affluence in ('High','Mid','Mid High','Mid Low','Very High') and Movies > 0

select distinct Box_subscription from bcg_base_20121130 where value_seg_updated <> 'UNSTABLE'
                                   and tenure <=  240
                                    and cqm_grp in ('01 1-10')

select count(*) from BCG_beddingin_sample2 where cqm_grp in ('01 1-10') and Box_Subscription in ('HD') and value_seg_updated <> 'UNSTABLE'
                                   and tenure <=  240

select distinct box_type from bcg_base_20121130

*/
---------------------End of Contract Segment

update bcg_base_20121130
set EOC_syscan_rule1 =  case when value_seg_updated <> 'UNSTABLE'
                            and tenure between 241 and 630 then
                                   case when cqm_grp in ('01 1-10') and Movies1 = 0 Then 'Rule1'
                                        when cqm_grp in ('01 1-10') and Movies1 = 1 Then 'Rule2'
                                        when cqm_grp in ('01 1-10') and Movies1 = 2 Then 'Rule3'
                                        when cqm_grp in ('02 11-22') and Movies1 = 0 Then 'Rule4'
                                        when cqm_grp in ('02 11-22') and Movies1 = 1 Then 'Rule5'
                                        when cqm_grp in ('02 11-22') and Movies1 = 2 Then 'Rule6'
                                        when cqm_grp in ('03 23-29') and tenure_group_latestactive in ('01 1-3 months','02 4-6 months') Then 'Rule7'
                                        when cqm_grp in ('03 23-29') and tenure_group_latestactive in ('03 6-12 months') Then 'Rule8'
                                        when cqm_grp in ('03 23-29') and tenure_group_latestactive in ('04 1-2 years') Then 'Rule9'
                                        when cqm_grp in ('03 23-29') and tenure_group_latestactive in ('09 Unknown') Then 'Rule10'
                                        when cqm_grp in ('04 30-33') Then 'Rule11'
                                        when cqm_grp in ('05 34-36') Then 'Rule12'
                                        when cqm_grp in ('Unknown') and broadband = 0 Then 'Rule13'
                                        when cqm_grp in ('Unknown') and broadband = 1 Then 'Rule14'
                                        else 'Unknown'


                                     end
                                else 'Unknown'
                                end;

----------------------------------9258850


/*
alter table bcg_base_20121130
drop longtenure_syscan_rule

alter table bcg_base_20121130
add longtenure_syscan_rule varchar(20) default 'Unknown'

*/

------------------------------Long Tenure Segment----------------------



update bcg_base_20121130
set longtenure_syscan_rule1 =  case when value_seg_updated <> 'UNSTABLE'
                            and tenure > 630 then
                                   case when cqm_grp in ('01 1-10') and Box_Subscription in ('FDB') Then 'Rule1'
                                        when cqm_grp in ('01 1-10') and Box_Subscription in ('HD') Then 'Rule2'
                                        when cqm_grp in ('01 1-10') and Box_Subscription in ('Sky Plus') Then 'Rule3'
                                        when cqm_grp in ('02 11-22') and tenure_group_latestactive in ('01 1-3 months','02 4-6 months','03 6-12 months','04 1-2 years') Then 'Rule4'
                                        when cqm_grp in ('02 11-22') and tenure_group_latestactive in ('05 2-3 Yrs') Then 'Rule5'
                                        when cqm_grp in ('02 11-22') and tenure_group_latestactive in ('06 3-5 Yrs') Then 'Rule6'
                                        when cqm_grp in ('02 11-22') and tenure_group_latestactive in ('07 5-10 Yrs','08 10+ Yrs') Then 'Rule7'
                                        when cqm_grp in ('02 11-22') and tenure_group_latestactive in ('09 Unknown') Then 'Rule8'
                                        when cqm_grp in ('03 23-29') and tenure_group_latestactive in ('01 1-3 months','02 4-6 months','03 6-12 months','04 1-2 years','05 2-3 Yrs','06 3-5 Yrs') Then 'Rule9'
                                        when cqm_grp in ('03 23-29') and tenure_group_latestactive in ('07 5-10 Yrs') Then 'Rule10'
                                        when cqm_grp in ('03 23-29') and tenure_group_latestactive in ('08 10+ Yrs') Then 'Rule11'
                                        when cqm_grp in ('03 23-29') and tenure_group_latestactive in ('09 Unknown') Then 'Rule12'
                                        when cqm_grp in ('04 30-33') and tenure_group_latestactive in ('01 1-3 months','02 4-6 months','03 6-12 months','04 1-2 years') Then 'Rule13'
                                        when cqm_grp in ('04 30-33') and tenure_group_latestactive in ('05 2-3 Yrs') Then 'Rule14'
                                        when cqm_grp in ('04 30-33') and tenure_group_latestactive in ('06 3-5 Yrs','07 5-10 Yrs','08 10+ Yrs') Then 'Rule15'
                                        when cqm_grp in ('04 30-33') and tenure_group_latestactive in ('09 Unknown') Then 'Rule16'
                                        when cqm_grp in ('05 34-36') Then 'Rule17'
                                        when cqm_grp in ('Unknown') and Box_Subscription in ('FDB','Sky Plus') Then 'Rule18'
                                        when cqm_grp in ('Unknown') and Box_Subscription in ('HD') Then 'Rule19'
                                        else 'Unknown'



                                     end
                                else 'Unknown'
                                end;
----------------------9258850

/*

select count(*),longtenure_syscan_rule1 from bcg_base_20121130 group by longtenure_syscan_rule1

select count(*),EOC_syscan_rule1 from bcg_base_20121130 group by EOC_syscan_rule1

select count(*),beddingin_syscan_rule1 from bcg_base_20121130 group by beddingin_syscan_rule1

select count(*),unstable_syscan_rule1 from bcg_base_20121130 group by unstable_syscan_rule1

select count(*) from bcg_base_20121011 where value_seg_updated = 'UNSTABLE' and tenure_group_latestactive in ('01 1-3 months','02 4-6 months','03 6-12 months')



select count(*),tenure_group_latestactive from bcg_base_20121011 group by tenure_group_latestactive

select count(*), tenure_group_latestactive from bcg_base_20121011 group by tenure_group_latestactive
select count(*), Box_Subscription1 from bcg_base_20121011 group by Box_Subscription1*/



------------------------------------------------------------------------------------------------------------------------------
---------------------------------------------SET RULES FOR CUSCAN-----------------------------------------------------------

------------------Unstable Segment--------------

update bcg_base_20121011
set unstable_cuscan_rule1 =  case when value_seg_updated = 'UNSTABLE' then
                                case when tenure_group_latestactive in ('01 1-3 months','02 4-6 months','03 6-12 months')
                                and Box_Subscription in ('FDB','Sky Plus') and Sports1 = 0 or Sports1 = 1  then 'Rule1'
                                when tenure_group_latestactive in ('01 1-3 months','02 4-6 months','03 6-12 months')
                                and Box_Subscription in ('FDB','Sky Plus') and Sports1 = 2  then 'Rule2'
                                when tenure_group_latestactive in ('01 1-3 months','02 4-6 months','03 6-12 months')
                                and Box_Subscription in ('HD')  then 'Rule3'
                                when tenure_group_latestactive in ('04 1-2 years','05 2-3 Yrs')
                                and mix_pack in ('05 Ent Extra') and multiroom = 0  then 'Rule4'
                                when tenure_group_latestactive in ('04 1-2 years','05 2-3 Yrs')
                                and mix_pack in ('05 Ent Extra') and multiroom = 1  then 'Rule5'
                                when tenure_group_latestactive in ('04 1-2 years','05 2-3 Yrs')
                                and mix_pack in ('06 Ent Pack') and cs_call_6m <= 0  then 'Rule6'
                                when tenure_group_latestactive in ('04 1-2 years','05 2-3 Yrs')
                                and mix_pack in ('06 Ent Pack') and cs_call_6m > 0  then 'Rule7'
                                when tenure_group_latestactive in ('06 3-5 Yrs')
                                and mix_pack in ('05 Ent Extra') and late_paid_bills_6m <= 0  then 'Rule8'
                                when tenure_group_latestactive in ('06 3-5 Yrs')
                                and mix_pack in ('05 Ent Extra') and late_paid_bills_6m > 0  then 'Rule9'
                                when tenure_group_latestactive in ('06 3-5 Yrs') and mix_pack in ('06 Ent Pack')  then 'Rule10'
                                when tenure_group_latestactive in ('07 5-10 Yrs','08 10+ Yrs')
                                and Box_Subscription in ('FDB','Sky Plus') and broadband = 0  then 'Rule11'
                                when tenure_group_latestactive in ('07 5-10 Yrs','08 10+ Yrs')
                                and Box_Subscription in ('FDB','Sky Plus') and broadband = 1  then 'Rule12'
                                when tenure_group_latestactive in ('07 5-10 Yrs','08 10+ Yrs') and Box_Subscription in ('HD')  then 'Rule13'
                                else 'Unknown'

                                     end
                                else 'Unknown'
                                end;

------------------------9267178 Row(s) affected

---------------------End of Contract Segment

update bcg_base_20121011
set EOC_cuscan_rule1 =  case when value_seg_updated <> 'UNSTABLE'
                            and tenure between 241 and 630 then
                                   case when ta_saved_6m <= 0 and mix_pack in ('05 Ent Extra') and cable in ('Cable') then 'Rule1'
                                        when ta_saved_6m <= 0 and mix_pack in ('05 Ent Extra') and cable in ('Non-Cable') and count_upgrades <= 0 then 'Rule2'
                                        when ta_saved_6m <= 0 and mix_pack in ('05 Ent Extra') and cable in ('Non-Cable') and count_upgrades > 0 then 'Rule3'
                                        when ta_saved_6m <= 0 and mix_pack in ('06 Ent Pack') and cable in ('Cable') and lifestage in
                                        ('Young family no children <18','Young family with children <18','Young household with children <18','Young singles/homesharers') then 'Rule4'
                                        when ta_saved_6m <= 0 and mix_pack in ('06 Ent Pack') and cable in ('Cable') and lifestage in
                                        ('Elderly family no children <18','Elderly single','Mature family no children <18','Mature family with children <18','Mature household with children <18','Mature singles/homesharers','Older family no children <18','Older family/household with children <18','Older single','Unknown') and NLP <= 0 then 'Rule5'
                                        when ta_saved_6m <= 0 and mix_pack in ('06 Ent Pack') and cable in ('Cable') and lifestage in
                                        ('Elderly family no children <18','Elderly single','Mature family no children <18','Mature family with children <18','Mature household with children <18','Mature singles/homesharers','Older family no children <18','Older family/household with children <18','Older single','Unknown') and NLP > 0 then 'Rule6'
                                        when ta_saved_6m <= 0 and mix_pack in ('06 Ent Pack') and cable in ('Non-Cable') then 'Rule7'
                                        when ta_saved_6m > 0 and mix_pack in ('05 Ent Extra') then 'Rule8'
                                        when ta_saved_6m > 0 and mix_pack in ('06 Ent Pack') then 'Rule9'
                                        else 'Unknown'


                                     end
                                else 'Unknown'
                                end;

----------------------------------9267178 Row(s) affected



------------------------------Long Tenure Segment----------------------



update bcg_base_20121011
set longtenure_cuscan_rule1 =  case when value_seg_updated <> 'UNSTABLE'
                            and tenure > 630 then
                                   case when ta_saved_6m <= 0 and tv_package in ('01 Top Tier') or tv_package IS null then 'Rule1'
                                        when ta_saved_6m <= 0 and tv_package in ('02 Dual Sports','03 Dual Movies') then 'Rule2'
                                        when ta_saved_6m <= 0 and tv_package in ('04 Other Prems','05 Ent Extra') then 'Rule3'
                                        when ta_saved_6m <= 0 and tv_package in ('06 Ent Pack') then 'Rule4'
                                        when ta_saved_6m > 0 then 'Rule5'
                                        else 'Unknown'
                                     end
                                else 'Unknown'
                                end;
----------------------9267178 Row(s) affected




select top 100 * from bcg_base_20121130

select count(*),longtenure_cuscan_rule1 from bcg_base_20121011 group by longtenure_cuscan_rule1

select count(*),EOC_cuscan_rule1 from bcg_base_20121011 group by EOC_cuscan_rule1


select count(*), affluence from bcg_base_20121011 group by affluence
select count(*),unstable_cuscan_rule1 from bcg_base_20121011 group by unstable_cuscan_rule1


--------------------------------UPDATED SEGMENTS--------------------------------------------------



alter table bcg_base_20121011
add (updated_unstable_cuscan_seg varchar(25) default 'Unknown'
   ,updated_EOC_cuscan_seg varchar(25) default 'Unknown'
   ,updated_longtenure_cuscan_seg varchar(25) default 'Unknown');




update bcg_base_20121011
set updated_unstable_cuscan_seg = case when value_seg_updated = 'UNSTABLE' then
                                case when tenure_group_latestactive in ('01 1-3 months','02 4-6 months','03 6-12 months')
                                and Box_Subscription in ('FDB','Sky Plus') and Sports1 = 0 or Sports1 = 1 then 'Rule1'

                                when tenure_group_latestactive in ('01 1-3 months','02 4-6 months','03 6-12 months')
                                and Box_Subscription in ('FDB','Sky Plus') and Sports1 = 2 then 'Rule2'

                                when tenure_group_latestactive in ('04 1-2 years')   and mix_pack in ('05 Ent Extra') and multiroom = 0 then 'Rule3'

                                when (tenure_group_latestactive in ('04 1-2 years')
                                or tenure_group_latestactive in ('05 2-3 years') and tenure_group_firstactive in ('05 2-3 years'))
                                and mix_pack in ('06 Ent Pack') and cs_call_6m <= 0 then 'Rule 4'

                                when tenure_group_latestactive in ('04 1-2 years','05 2-3 Yrs')
                                and mix_pack in ('06 Ent Pack') and cs_call_6m > 0 then 'Rule5'

                                when tenure_group_latestactive in ('06 3-5 Yrs') and mix_pack in ('06 Ent Pack') then 'Rule6'
                                else 'Unknown'
                                end
                                else 'Unknown'
                                end;

--------------9267178 Row(s) affected



update bcg_base_20121011
set updated_EOC_cuscan_seg = case when value_seg_updated <> 'UNSTABLE'
                                and tenure between 241 and 630 then
                                   case when  ta_saved_6m <= 0 and mix_pack in ('05 Ent Extra') and cable in ('Cable')
                                        and affluence in ('Very Low', 'Low') then 'Rule1'

                                        when ta_saved_6m <= 0 and mix_pack in ('05 Ent Extra') and cable in ('Non-Cable') and count_upgrades <= 0
                                        and affluence in ('Very Low') then 'Rule2'

                                        when ta_saved_6m <= 0 and mix_pack in ('06 Ent Pack') and cable in ('Cable')
                                        and lifestage in  ('Young family no children <18','Young family with children <18',
                                        'Young household with children <18','Young singles/homesharers') then 'Rule3'

                                        when ta_saved_6m <= 0 and mix_pack in ('06 Ent Pack') and cable in ('Cable') and
                                        lifestage in ('Elderly family no children <18','Elderly single',
                                        'Mature family no children <18','Mature family with children <18','Mature household with children <18',
                                        'Mature singles/homesharers','Older family no children <18','Older family/household with children <18',
                                        'Older single','Unknown') and NLP <= 0 then 'Rule4'

                                        when ta_saved_6m <= 0 and mix_pack in ('06 Ent Pack') and cable in ('Cable') and
                                        lifestage in  ('Elderly family no children <18','Elderly single','Mature family no children <18',
                                        'Mature family with children <18','Mature household with children <18','Mature singles/homesharers',
                                        'Older family no children <18','Older family/household with children <18','Older single','Unknown')
                                        and NLP > 0 then 'Rule5'

                                        when ta_saved_6m > 0 and mix_pack in ('06 Ent Pack') then 'Rule6'
                                        else 'Unknown'
                                        end
                                   else 'Unknown'
                                   end;

--------------9267178 Row(s) affected




update bcg_base_20121011
set updated_longtenure_cuscan_seg =  case when value_seg_updated <> 'UNSTABLE'
                            and tenure > 630 then
                                   case when ta_saved_6m <= 0 and tv_package in ('06 Ent Pack')
                                                then 'Rule1'
                                        when ta_saved_6m > 0 and affluence in ('Very Low', 'Low', 'Mid Low') then 'Rule2'


                                        else 'Unknown'
                                     end
                                else 'Unknown'
                                end;

--------------9267178 Row(s) affected






------------------------------APPEND CHURN EVENTS---------------------------


select  csh.account_number
       ,effective_from_dt as churn_date
       ,case when status_code = 'PO'
             then 'CUSCAN'
             else 'SYSCAN'
         end as churn_type
       ,RANK() OVER (PARTITION BY  csh.account_number
                     ORDER BY  csh.effective_from_dt,csh.cb_row_id) AS churn_rank
       ,1 as churn_flag--Rank to get the first event
  into -----drop table
        #all_churn_records_may
  from sk_prod.cust_subs_hist as csh
    inner join bcg_base_20121130 base
  on base.account_number = csh.account_number
 where subscription_sub_type ='DTV Primary Viewing'     --DTV stack
   and status_code in ('PO','SC')                       --CUSCAN and SYSCAN status codes
   and prev_status_code in ('AC','AB', 'PC')             --Previously ACTIVE
   and status_code_changed = 'Y'
   and effective_from_dt between '2013-04-31' and '2013-03-31'                 --Events after the mailing date
   and effective_from_dt != effective_to_dt
  ;
--------------------309596


delete from #all_churn_records_may     -- deletes all churn records from the temp table except most recent
where churn_rank > 1;
---------------------500


--sybase alter bug
-- select cast(null as date) as churn_date,cast(null as varchar) as churn_type,cast(0 as bit) as churn_flag,* into bcg_base_temp from bcg_base_20121011;
-- drop table bcg_base_20121011;
-- select * into bcg_base_20121011 from bcg_base_temp;
--
alter table bcg_base_20121011
add (churn_date                 date            default null
    ,churn_type                 varchar(20)     default null
    ,churn_flag                  tinyint        default 0);


update bcg_base_20121011  base
set base.churn_date   =   temp.churn_date
   ,base.churn_type   =   temp.churn_type
   ,base.churn_flag   =   temp.churn_flag
from #all_churn_records_may   temp
where base.account_number  = temp.account_number;

-----To sum custcan churn
/*select sum(case when churn_type = 'CUSCAN' then 1 else 0 end) as Cuscan_churn
from maitrap.bcg_base_20121130*/

--select count(*) from maitrap.bcg_base_20121130


------------------------------CREATING SEGMENTS--------------------------------------------




------------------Unstable Segment--------------

update bcg_base_20121130
set unstable_cuscan_rule =  case when value_seg_updated = 'UNSTABLE' then
                                case when tenure_group_latestactive in ('01 1-3 months','02 4-6 months','03 6-12 months')
                                and Box_Subscription in ('FDB','Sky Plus') and Sports1 = 0 or Sports1 = 1  then 'Rule1'

                                when tenure_group_latestactive in ('01 1-3 months','02 4-6 months','03 6-12 months')
                                and Box_Subscription in ('FDB','Sky Plus') and Sports1 = 2  then 'Rule2'

                                when tenure_group_latestactive in ('01 1-3 months','02 4-6 months','03 6-12 months')
                                and Box_Subscription in ('FDB','Sky Plus') then 'Rule3'

                                when tenure_group_latestactive in ('01 1-3 months','02 4-6 months','03 6-12 months')
                                and Box_Subscription in ('HD')  then 'Rule4'

                                when tenure_group_latestactive in ('01 1-3 months','02 4-6 months','03 6-12 months') then 'Rule5'

                                when tenure_group_latestactive in ('04 1-2 years','05 2-3 Yrs')
                                and tv_package in ('01 Top Tier','02 Dual Sports','03 Dual Movies','04 Other Prems')
                                and multiroom = 1  then 'Rule6'

                                when tenure_group_latestactive in ('04 1-2 years','05 2-3 Yrs')
                                and tv_package in ('01 Top Tier','02 Dual Sports','03 Dual Movies','04 Other Prems')
                                 and multiroom = 0  then 'Rule7'

                                 when tenure_group_latestactive in ('04 1-2 years','05 2-3 Yrs')
                                and tv_package in ('01 Top Tier','02 Dual Sports','03 Dual Movies','04 Other Prems') then 'Rule8'

                                when tenure_group_latestactive in ('04 1-2 years','05 2-3 Yrs')
                                and tv_package in ('06 Ent Pack', '05 Ent Extra')  then 'Rule9'

                                when tenure_group_latestactive in ('04 1-2 years','05 2-3 Yrs') then 'Rule10'


                                when tenure_group_latestactive in ('06 3-5 Yrs')
                                and tv_package in ('01 Top Tier','02 Dual Sports','03 Dual Movies','04 Other Prems')
                                 then 'Rule11'



                                when tenure_group_latestactive in ('06 3-5 Yrs') and tv_package in ('06 Ent Pack', '05 Ent Extra')  then 'Rule12'

                                when tenure_group_latestactive in ('06 3-5 Yrs') then 'Rule13'

                                when tenure_group_latestactive in ('07 5-10 Yrs','08 10+ Yrs') then 'Rule14'
                                else 'Unknown'


                                     end
                                else 'Unknown'
                                end;









---------------------End of Contract Segment

update bcg_base_20121130
set EOC_cuscan_rule1 =  case when value_seg_updated <> 'UNSTABLE'
                            and tenure between 241 and 630 then

                                   case when ta_saved_6m <= 0 and tv_package in ('06 Ent Pack','05 Ent Extra') and
                                        and lifestage in
                                        ('Young family no children <18','Young family with children <18','Young household with children <18',
                                        'Young singles/homesharers') and cable in ('Cable') then 'Rule1'


                                        when ta_saved_6m <= 0 and tv_package in ('06 Ent Pack','05 Ent Extra') and
                                        and lifestage in
                                        ('Young family no children <18','Young family with children <18','Young household with children <18',
                                        'Young singles/homesharers') and cable in ('Non-Cable') then 'Rule2'


                                        when ta_saved_6m <= 0 and tv_package in ('06 Ent Pack','05 Ent Extra') and
                                        and lifestage in
                                        ('Young family no children <18','Young family with children <18','Young household with children <18',
                                        'Young singles/homesharers') then 'Rule3'

                                        when ta_saved_6m <= 0 and tv_package in ('06 Ent Pack','05 Ent Extra') and lifestage in
                                        ('Elderly family no children <18','Elderly single','Mature family no children <18',
                                        'Mature family with children <18','Mature household with children <18','Mature singles/homesharers',
                                        'Older family no children <18','Older family/household with children <18','Older single','Unknown')
                                        then 'Rule4'



                                        when ta_saved_6m <= 0 and tv_package in ('06 Ent Pack','05 Ent Extra') then 'Rule5'





                                        when ta_saved_6m <= 0 and tv_package in ('01 Top Tier','02 Dual Sports','03 Dual Movies',
                                        '04 Other Prems') and lifestage in
                                        ('Young family no children <18','Young family with children <18','Young household with children <18',
                                        'Young singles/homesharers') and cable in ('Cable') then 'Rule6'

                                        when ta_saved_6m <= 0 and tv_package in ('01 Top Tier','02 Dual Sports','03 Dual Movies',
                                        '04 Other Prems') and lifestage in
                                        ('Young family no children <18','Young family with children <18','Young household with children <18',
                                        'Young singles/homesharers') and cable in ('Non-Cable') then 'Rule7'

                                         when ta_saved_6m <= 0 and tv_package in ('01 Top Tier','02 Dual Sports','03 Dual Movies',
                                        '04 Other Prems') and lifestage in
                                        ('Young family no children <18','Young family with children <18','Young household with children <18',
                                        'Young singles/homesharers') then 'Rule8'




                                        when ta_saved_6m <= 0 and tv_package in ('01 Top Tier','02 Dual Sports','03 Dual Movies',
                                        '04 Other Prems') and lifestage in
                                        ('Elderly family no children <18','Elderly single','Mature family no children <18',
                                        'Mature family with children <18','Mature household with children <18','Mature singles/homesharers',
                                        'Older family no children <18','Older family/household with children <18','Older single','Unknown')
                                        then 'Rule9'


                                        when ta_saved_6m <= 0 and tv_package in ('01 Top Tier','02 Dual Sports','03 Dual Movies',
                                        '04 Other Prems') then 'Rule10'


                                        when ta_saved_6m <= 0 then 'Rule11'



                                        when ta_saved_6m > 0 and tv_package in ('01 Top Tier','02 Dual Sports','03 Dual Movies',
                                        '04 Other Prems')  then 'Rule12'

                                        when ta_saved_6m > 0 and tv_package in ('06 Ent Pack','05 Ent Extra')  then 'Rule13'

                                        when ta_saved_6m > 0 then 'Rule14'


                                        else 'Unknown'


                                     end
                                else 'Unknown'
                                end;






------------------------------Long Tenure Segment----------------------




update bcg_base_20121130
set longtenure_cuscan_rule1 =  case when value_seg_updated <> 'UNSTABLE'
                            and tenure > 630 then
                                   case when ta_saved_6m <= 0 and tv_package in ('01 Top Tier','02 Dual Sports','03 Dual Movies',
                                        '04 Other Prems')
                                        and affluence in ('Very Low','Low','Mid Low','Unknown')
                                        and product_holding not in ('TV, SkyTalk and Line Rental and Broadband') then 'Rule1'


                                        when ta_saved_6m <= 0 and tv_package in ('01 Top Tier','02 Dual Sports','03 Dual Movies',
                                        '04 Other Prems')
                                        and affluence in ('Very Low','Low','Mid Low','Unknown')
                                        and product_holding in ('TV, SkyTalk and Line Rental and Broadband') then 'Rule2'

                                        when ta_saved_6m <= 0 and tv_package in ('01 Top Tier','02 Dual Sports','03 Dual Movies',
                                        '04 Other Prems')
                                        and affluence in ('Very Low','Low','Mid Low','Unknown') then 'Rule3'


                                        when ta_saved_6m <= 0 and tv_package in ('01 Top Tier','02 Dual Sports','03 Dual Movies',
                                        '04 Other Prems') and affluence in ('Very High','High','Mid High','High')
                                         then 'Rule4'

                                         when ta_saved_6m <= 0 and tv_package in ('01 Top Tier','02 Dual Sports','03 Dual Movies',
                                        '04 Other Prems') then 'Rule5'


                                         when ta_saved_6m <= 0 and tv_package in ('06 Ent Pack','05 Ent Extra')
                                        and affluence in ('Very Low','Low','Mid Low','Unknown')
                                        and product_holding not in ('TV, SkyTalk and Line Rental and Broadband') then 'Rule6'


                                        when ta_saved_6m <= 0 and tv_package in ('06 Ent Pack','05 Ent Extra')
                                        and affluence in ('Very Low','Low','Mid Low','Unknown')
                                        and product_holding in ('TV, SkyTalk and Line Rental and Broadband') then 'Rule7'

                                        when ta_saved_6m <= 0 and tv_package in ('06 Ent Pack','05 Ent Extra')
                                        and affluence in ('Very Low','Low','Mid Low','Unknown') then 'Rule8'


                                        when ta_saved_6m <= 0 and tv_package in ('06 Ent Pack','05 Ent Extra') and
                                        affluence in ('Very High','High','Mid High','High')
                                         then 'Rule9'

                                         when ta_saved_6m <= 0 and tv_package in ('06 Ent Pack','05 Ent Extra')
                                         then 'Rule10'

                                          when ta_saved_6m <= 0 then 'Rule11'

                                        when ta_saved_6m > 0 and tv_package in ('01 Top Tier','02 Dual Sports','03 Dual Movies',
                                        '04 Other Prems') then 'Rule12'

                                        when ta_saved_6m > 0 and tv_package in ('06 Ent Pack','05 Ent Extra') then 'Rule13'

                                        when ta_saved_6m > 0 then 'Rule14'


                                        else 'Unknown'
                                     end
                                else 'Unknown'
                                end;

-------------------------------------------------------------
-- Summarise churn segments in one go
-- Includes a few syntax corrections to the previous rules
-- Added by CLaudio
-------------------------------------------------------------

select
-- Unstable segments
case when value_seg_updated = 'UNSTABLE' then
                                case when tenure_group_latestactive in ('01 1-3 months','02 4-6 months','03 6-12 months')
                                and Box_Subscription in ('FDB','Sky Plus') and Sports1 in (0,1)  then 'Rule1'
                                when tenure_group_latestactive in ('01 1-3 months','02 4-6 months','03 6-12 months')
                                and Box_Subscription in ('FDB','Sky Plus') and Sports1 = 2  then 'Rule2'
                                when tenure_group_latestactive in ('01 1-3 months','02 4-6 months','03 6-12 months')
                                and Box_Subscription in ('FDB','Sky Plus') then 'Rule3'
                                when tenure_group_latestactive in ('01 1-3 months','02 4-6 months','03 6-12 months')
                                and Box_Subscription in ('HD')  then 'Rule4'
                                when tenure_group_latestactive in ('01 1-3 months','02 4-6 months','03 6-12 months') then 'Rule5'
                                when tenure_group_latestactive in ('04 1-2 years','05 2-3 Yrs')
                                and tv_package in ('01 Top Tier','02 Dual Sports','03 Dual Movies','04 Other Prems')
                                and multiroom = 1  then 'Rule6'
                                when tenure_group_latestactive in ('04 1-2 years','05 2-3 Yrs')
                                and tv_package in ('01 Top Tier','02 Dual Sports','03 Dual Movies','04 Other Prems')
                                 and multiroom = 0  then 'Rule7'
                                 when tenure_group_latestactive in ('04 1-2 years','05 2-3 Yrs')
                                and tv_package in ('01 Top Tier','02 Dual Sports','03 Dual Movies','04 Other Prems') then 'Rule8'
                                when tenure_group_latestactive in ('04 1-2 years','05 2-3 Yrs')
                                and tv_package in ('06 Ent Pack', '05 Ent Extra')  then 'Rule9'
                                when tenure_group_latestactive in ('04 1-2 years','05 2-3 Yrs') then 'Rule10'
                                when tenure_group_latestactive in ('06 3-5 Yrs')
                                and tv_package in ('01 Top Tier','02 Dual Sports','03 Dual Movies','04 Other Prems')
                                 then 'Rule11'
                                when tenure_group_latestactive in ('06 3-5 Yrs') and tv_package in ('06 Ent Pack', '05 Ent Extra')  then 'Rule12'
                                when tenure_group_latestactive in ('06 3-5 Yrs') then 'Rule13'
                                when tenure_group_latestactive in ('07 5-10 Yrs','08 10+ Yrs') then 'Rule14'
                                else 'Unknown'
                                     end
                                else 'Unknown'
                                end as unstable
---------------------End of Contract Segment
,case when value_seg_updated <> 'UNSTABLE'
                            and tenure between 241 and 630 then
                                   case when ta_saved_6m <= 0 and tv_package in ('06 Ent Pack','05 Ent Extra')
                                        and lifestage in
                                        ('Young family no children <18','Young family with children <18','Young household with children <18',
                                        'Young singles/homesharers') and cable in ('Cable') then 'Rule1'
                                        when ta_saved_6m <= 0 and tv_package in ('06 Ent Pack','05 Ent Extra')
                                        and lifestage in
                                        ('Young family no children <18','Young family with children <18','Young household with children <18',
                                        'Young singles/homesharers') and cable in ('Non-Cable') then 'Rule2'
                                        when ta_saved_6m <= 0 and tv_package in ('06 Ent Pack','05 Ent Extra')
                                        and lifestage in
                                        ('Young family no children <18','Young family with children <18','Young household with children <18',
                                        'Young singles/homesharers') then 'Rule3'
                                        when ta_saved_6m <= 0 and tv_package in ('06 Ent Pack','05 Ent Extra') and lifestage in
                                        ('Elderly family no children <18','Elderly single','Mature family no children <18',
                                        'Mature family with children <18','Mature household with children <18','Mature singles/homesharers',
                                        'Older family no children <18','Older family/household with children <18','Older single','Unknown')
                                        then 'Rule4'
                                        when ta_saved_6m <= 0 and tv_package in ('06 Ent Pack','05 Ent Extra') then 'Rule5'
                                        when ta_saved_6m <= 0 and tv_package in ('01 Top Tier','02 Dual Sports','03 Dual Movies',
                                        '04 Other Prems') and lifestage in
                                        ('Young family no children <18','Young family with children <18','Young household with children <18',
                                        'Young singles/homesharers') and cable in ('Cable') then 'Rule6'
                                        when ta_saved_6m <= 0 and tv_package in ('01 Top Tier','02 Dual Sports','03 Dual Movies',
                                        '04 Other Prems') and lifestage in
                                        ('Young family no children <18','Young family with children <18','Young household with children <18',
                                        'Young singles/homesharers') and cable in ('Non-Cable') then 'Rule7'
                                         when ta_saved_6m <= 0 and tv_package in ('01 Top Tier','02 Dual Sports','03 Dual Movies',
                                        '04 Other Prems') and lifestage in
                                        ('Young family no children <18','Young family with children <18','Young household with children <18',
                                        'Young singles/homesharers') then 'Rule8'
                                        when ta_saved_6m <= 0 and tv_package in ('01 Top Tier','02 Dual Sports','03 Dual Movies',
                                        '04 Other Prems') and lifestage in
                                        ('Elderly family no children <18','Elderly single','Mature family no children <18',
                                        'Mature family with children <18','Mature household with children <18','Mature singles/homesharers',
                                        'Older family no children <18','Older family/household with children <18','Older single','Unknown')
                                        then 'Rule9'
                                        when ta_saved_6m <= 0 and tv_package in ('01 Top Tier','02 Dual Sports','03 Dual Movies',
                                        '04 Other Prems') then 'Rule10'
                                        when ta_saved_6m <= 0 then 'Rule11'
                                        when ta_saved_6m > 0 and tv_package in ('01 Top Tier','02 Dual Sports','03 Dual Movies',
                                        '04 Other Prems')  then 'Rule12'
                                        when ta_saved_6m > 0 and tv_package in ('06 Ent Pack','05 Ent Extra')  then 'Rule13'
                                        when ta_saved_6m > 0 then 'Rule14'
                                        else 'Unknown'
                                     end
                                else 'Unknown'
                                end as end_of_contract
------------------------------Long Tenure Segment----------------------
,case when value_seg_updated <> 'UNSTABLE'
                            and tenure > 630 then
                                   case when ta_saved_6m <= 0 and tv_package in ('01 Top Tier','02 Dual Sports','03 Dual Movies',
                                        '04 Other Prems')
                                        and affluence in ('Very Low','Low','Mid Low','Unknown')
                                        and product_holding not in ('TV, SkyTalk and Line Rental and Broadband') then 'Rule1'
                                        when ta_saved_6m <= 0 and tv_package in ('01 Top Tier','02 Dual Sports','03 Dual Movies',
                                        '04 Other Prems')
                                        and affluence in ('Very Low','Low','Mid Low','Unknown')
                                        and product_holding in ('TV, SkyTalk and Line Rental and Broadband') then 'Rule2'
                                        when ta_saved_6m <= 0 and tv_package in ('01 Top Tier','02 Dual Sports','03 Dual Movies',
                                        '04 Other Prems')
                                        and affluence in ('Very Low','Low','Mid Low','Unknown') then 'Rule3'
                                        when ta_saved_6m <= 0 and tv_package in ('01 Top Tier','02 Dual Sports','03 Dual Movies',
                                        '04 Other Prems') and affluence in ('Very High','High','Mid High','Mid')
                                         then 'Rule4'
                                         when ta_saved_6m <= 0 and tv_package in ('01 Top Tier','02 Dual Sports','03 Dual Movies',
                                        '04 Other Prems') then 'Rule5'
                                         when ta_saved_6m <= 0 and tv_package in ('06 Ent Pack','05 Ent Extra')
                                        and affluence in ('Very Low','Low','Mid Low','Unknown')
                                        and product_holding not in ('TV, SkyTalk and Line Rental and Broadband') then 'Rule6'
                                        when ta_saved_6m <= 0 and tv_package in ('06 Ent Pack','05 Ent Extra')
                                        and affluence in ('Very Low','Low','Mid Low','Unknown')
                                        and product_holding in ('TV, SkyTalk and Line Rental and Broadband') then 'Rule7'
                                        when ta_saved_6m <= 0 and tv_package in ('06 Ent Pack','05 Ent Extra')
                                        and affluence in ('Very Low','Low','Mid Low','Unknown') then 'Rule8'
                                        when ta_saved_6m <= 0 and tv_package in ('06 Ent Pack','05 Ent Extra') and
                                        affluence in ('Very High','High','Mid High','Mid')
                                         then 'Rule9'
                                         when ta_saved_6m <= 0 and tv_package in ('06 Ent Pack','05 Ent Extra')
                                         then 'Rule10'
                                          when ta_saved_6m <= 0 then 'Rule11'
                                        when ta_saved_6m > 0 and tv_package in ('01 Top Tier','02 Dual Sports','03 Dual Movies',
                                        '04 Other Prems') then 'Rule12'
                                        when ta_saved_6m > 0 and tv_package in ('06 Ent Pack','05 Ent Extra') then 'Rule13'
                                        when ta_saved_6m > 0 then 'Rule14'
                                        else 'Unknown'
                                     end
                                else 'Unknown'
                                end as long_tenure
        ,count(*) as Total_Volume
        ,sum(churn_flag) as Churn_Volume
        ,Churn_Volume*1.0/Total_Volume as Churn_Rate_3M
        ,Churn_Volume*4.0/Total_Volume as Churn_Rate_12M
from bcg_base_20121011
group by unstable
        ,end_of_contract
        ,long_tenure
order by unstable
        ,end_of_contract
        ,long_tenure


select top 10 value_seg_updated,tenure,* from bcg_base_20121011
select count(1),count(distinct account_number) from bcg_base_20121011


  select account_number
         ,case when value_seg_updated = 'UNSTABLE'  then 'Unstable'
               else case when tenure between 241 and 630 then 'End Of Contract'
                         when tenure > 630               then 'Long Tenure'
                    end
          end as cuscan_type
         ,case when value_seg_updated = 'UNSTABLE' then
                    case when tenure_group_latestactive in ('01 1-3 months','02 4-6 months','03 6-12 months')
                         and Box_Subscription in ('FDB','Sky Plus') and Sports1 in (0,1)                      then 1
                         when tenure_group_latestactive in ('01 1-3 months','02 4-6 months','03 6-12 months')
                         and Box_Subscription in ('FDB','Sky Plus') and Sports1 = 2                           then 2
                         when tenure_group_latestactive in ('01 1-3 months','02 4-6 months','03 6-12 months')
                         and Box_Subscription in ('FDB','Sky Plus')                                           then 3
                         when tenure_group_latestactive in ('01 1-3 months','02 4-6 months','03 6-12 months')
                         and Box_Subscription in ('HD')                                                       then 4
                         when tenure_group_latestactive in ('01 1-3 months','02 4-6 months','03 6-12 months') then 5
                         when tenure_group_latestactive in ('04 1-2 years','05 2-3 Yrs')
                         and tv_package in ('01 Top Tier','02 Dual Sports','03 Dual Movies','04 Other Prems')
                         and multiroom = 1                                                                    then 6
                         when tenure_group_latestactive in ('04 1-2 years','05 2-3 Yrs')
                         and tv_package in ('01 Top Tier','02 Dual Sports','03 Dual Movies','04 Other Prems')
                          and multiroom = 0                                                                   then 7
                          when tenure_group_latestactive in ('04 1-2 years','05 2-3 Yrs')
                         and tv_package in ('01 Top Tier','02 Dual Sports','03 Dual Movies','04 Other Prems') then 8
                         when tenure_group_latestactive in ('04 1-2 years','05 2-3 Yrs')
                         and tv_package in ('06 Ent Pack', '05 Ent Extra')                                    then 9
                         when tenure_group_latestactive in ('04 1-2 years','05 2-3 Yrs')                      then 10
                         when tenure_group_latestactive in ('06 3-5 Yrs')
                         and tv_package in ('01 Top Tier','02 Dual Sports','03 Dual Movies','04 Other Prems') then 11
                         when tenure_group_latestactive in ('06 3-5 Yrs') and tv_package in ('06 Ent Pack', '05 Ent Extra')  then 12
                         when tenure_group_latestactive in ('06 3-5 Yrs')                                                    then 13
                         when tenure_group_latestactive in ('07 5-10 Yrs','08 10+ Yrs') then                                      14
                         else 0
                    end
               when value_seg_updated <> 'UNSTABLE' and tenure between 241 and 630 then
                    case when ta_saved_6m <= 0 and tv_package in ('06 Ent Pack','05 Ent Extra')
                                        and lifestage in
                                        ('Young family no children <18','Young family with children <18','Young household with children <18',
                                        'Young singles/homesharers') and cable in ('Cable')                                    then 1
                                        when ta_saved_6m <= 0 and tv_package in ('06 Ent Pack','05 Ent Extra')
                                        and lifestage in
                                        ('Young family no children <18','Young family with children <18','Young household with children <18',
                                        'Young singles/homesharers') and cable in ('Non-Cable')                                then 2
                                        when ta_saved_6m <= 0 and tv_package in ('06 Ent Pack','05 Ent Extra')
                                        and lifestage in
                                        ('Young family no children <18','Young family with children <18','Young household with children <18',
                                        'Young singles/homesharers')                                                            then 3
                                        when ta_saved_6m <= 0 and tv_package in ('06 Ent Pack','05 Ent Extra') and lifestage in
                                        ('Elderly family no children <18','Elderly single','Mature family no children <18',
                                        'Mature family with children <18','Mature household with children <18','Mature singles/homesharers',
                                        'Older family no children <18','Older family/household with children <18','Older single','Unknown')
                                                                                                                                then 4
                                        when ta_saved_6m <= 0 and tv_package in ('06 Ent Pack','05 Ent Extra')                  then 5
                                        when ta_saved_6m <= 0 and tv_package in ('01 Top Tier','02 Dual Sports','03 Dual Movies',
                                        '04 Other Prems') and lifestage in
                                        ('Young family no children <18','Young family with children <18','Young household with children <18',
                                        'Young singles/homesharers') and cable in ('Cable')                                      then 6
                                        when ta_saved_6m <= 0 and tv_package in ('01 Top Tier','02 Dual Sports','03 Dual Movies',
                                        '04 Other Prems') and lifestage in
                                        ('Young family no children <18','Young family with children <18','Young household with children <18',
                                        'Young singles/homesharers') and cable in ('Non-Cable')                                  then 7
                                         when ta_saved_6m <= 0 and tv_package in ('01 Top Tier','02 Dual Sports','03 Dual Movies',
                                        '04 Other Prems') and lifestage in
                                        ('Young family no children <18','Young family with children <18','Young household with children <18',
                                        'Young singles/homesharers')                                                             then 8
                                        when ta_saved_6m <= 0 and tv_package in ('01 Top Tier','02 Dual Sports','03 Dual Movies',
                                        '04 Other Prems') and lifestage in
                                        ('Elderly family no children <18','Elderly single','Mature family no children <18',
                                        'Mature family with children <18','Mature household with children <18','Mature singles/homesharers',
                                        'Older family no children <18','Older family/household with children <18','Older single','Unknown')
                                                                                                                                  then 9
                                        when ta_saved_6m <= 0 and tv_package in ('01 Top Tier','02 Dual Sports','03 Dual Movies',
                                        '04 Other Prems')                                                                         then 10
                                        when ta_saved_6m <= 0                                                                     then 11
                                        when ta_saved_6m > 0 and tv_package in ('01 Top Tier','02 Dual Sports','03 Dual Movies',
                                        '04 Other Prems')                                                                         then 12
                                        when ta_saved_6m > 0 and tv_package in ('06 Ent Pack','05 Ent Extra')                     then 13
                                        when ta_saved_6m > 0                                                                      then 14
                                        else 0
                    end
               else case when value_seg_updated <> 'UNSTABLE' and tenure > 630 then
                              case when ta_saved_6m <= 0 and tv_package in ('01 Top Tier','02 Dual Sports','03 Dual Movies',
                                        '04 Other Prems')
                                        and affluence in ('Very Low','Low','Mid Low','Unknown')
                                        and product_holding not in ('TV, SkyTalk and Line Rental and Broadband')                  then 1
                                        when ta_saved_6m <= 0 and tv_package in ('01 Top Tier','02 Dual Sports','03 Dual Movies',
                                        '04 Other Prems')
                                        and affluence in ('Very Low','Low','Mid Low','Unknown')
                                        and product_holding in ('TV, SkyTalk and Line Rental and Broadband')                      then 2
                                        when ta_saved_6m <= 0 and tv_package in ('01 Top Tier','02 Dual Sports','03 Dual Movies',
                                        '04 Other Prems')
                                        and affluence in ('Very Low','Low','Mid Low','Unknown')                                   then 3
                                        when ta_saved_6m <= 0 and tv_package in ('01 Top Tier','02 Dual Sports','03 Dual Movies',
                                        '04 Other Prems') and affluence in ('Very High','High','Mid High','Mid')                  then 4
                                         when ta_saved_6m <= 0 and tv_package in ('01 Top Tier','02 Dual Sports','03 Dual Movies',
                                        '04 Other Prems')                                                                         then 5
                                         when ta_saved_6m <= 0 and tv_package in ('06 Ent Pack','05 Ent Extra')
                                        and affluence in ('Very Low','Low','Mid Low','Unknown')
                                        and product_holding not in ('TV, SkyTalk and Line Rental and Broadband')                  then 6
                                        when ta_saved_6m <= 0 and tv_package in ('06 Ent Pack','05 Ent Extra')
                                        and affluence in ('Very Low','Low','Mid Low','Unknown')
                                        and product_holding in ('TV, SkyTalk and Line Rental and Broadband')                      then 7
                                        when ta_saved_6m <= 0 and tv_package in ('06 Ent Pack','05 Ent Extra')
                                        and affluence in ('Very Low','Low','Mid Low','Unknown')                                   then 8
                                        when ta_saved_6m <= 0 and tv_package in ('06 Ent Pack','05 Ent Extra') and
                                        affluence in ('Very High','High','Mid High','Mid')                                        then 9
                                         when ta_saved_6m <= 0 and tv_package in ('06 Ent Pack','05 Ent Extra')                   then 10
                                          when ta_saved_6m <= 0                                                                   then 11
                                        when ta_saved_6m > 0 and tv_package in ('01 Top Tier','02 Dual Sports','03 Dual Movies',
                                        '04 Other Prems')                                                                         then 12
                                        when ta_saved_6m > 0 and tv_package in ('06 Ent Pack','05 Ent Extra')                     then 13
                                        when ta_saved_6m > 0                                                                      then 14
                                        else 0
                              end
                         else 0
                    end
          end as rule
        ,case cuscan_type when 'Unstable' then case rule when  1 then 24
                                                         when  2 then 44
                                                         when  3 then 34
                                                         when  4 then 15
                                                         when  5 then 26
                                                         when  6 then 9
                                                         when  7 then 18
                                                         when  8 then 15
                                                         when  9 then 27
                                                         when 10 then 17
                                                         when 11 then 11
                                                         when 12 then 22
                                                         when 13 then 11
                                                         when 14 then 9
                                                 end
                          when 'End Of Contract' then case rule when  1 then 29
                                                                when  2 then 23
                                                                when  3 then 26
                                                                when  4 then 21
                                                                when  5 then 23
                                                                when  6 then 20
                                                                when  7 then 16
                                                                when  8 then 15
                                                                when  9 then 13
                                                                when 10 then 13
                                                                when 11 then 15
                                                                when 12 then 27
                                                                when 13 then 66
                                                                when 14 then 35
                                                      end
                          when 'Long Tenure' then case rule when  1 then 7
                                                            when  2 then 9
                                                            when  3 then 8
                                                            when  4 then 6
                                                            when  5 then 7
                                                            when  6 then 14
                                                            when  7 then 17
                                                            when  8 then 16
                                                            when  9 then 14
                                                            when 10 then 15
                                                            when 11 then 7
                                                            when 12 then 10
                                                            when 13 then 39
                                                            when 14 then 12
                                                   end
         end as churn_rate
        ,case cuscan_type when 'Unstable' then case when rule in (1,2,3,5,9,12) then 'Red'
                                                    when rule in (7,10) then 'Yellow'
                                                    else 'Green'
                                               end
                          when 'End Of Contract' then case when rule in (1,2,3,5,12,13) then 'Red'
                                                           when rule in (4,6,9) then 'Yellow'
                                                           else 'Green'
                                                      end
                          when 'Long Tenure' then case when rule in (6,7,8,9,10,13,14) then 'Red'
                                                       when rule in (2,3,12) then 'Yellow'
                                                       else 'Green'
                                                   end
         end as colour
        ,value_seg_updated
    into atrisk_results_20121011
    from bcg_base_20121011
;
------9267178 Row(s) affected

  select count(distinct account_number)
        ,case when model.value_seg in ('Bedding In','Unstable') then model.value_seg
                                                                else 'Other'
         end as cvs
    from atrisk_results_20121011 as bas
         inner join SK_PROD.CUST_SINGLE_ACCOUNT_VIEW as sav on bas.account_number = sav.account_number


select top 20 * from atrisk_results_20121011

grant all on atrisk_results_20121011 to public;





