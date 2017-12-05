/*
start Date: 15-10-2012
Due Date:   19-10-2012
Analyst: Jonathan Green (started by Angel Donnarumma)
Lead:    Chris Thomas

The intention of this script is to reproduce the same outcome from last time but with amends on below aspects:

1. About 20% of the households with Sky have an unknown package which doesn’t
 seem right as we don’t have any unknown package info when looking by account.
 Possibly the issue is an underlying problem in Olive on package data linking to
 HH as the data for HD, multiroom etc. appears to be fine.
 If there isn’t anything obvious with the code can you please speak to someone in the Sky pillar
 to see how they link to package.  I know they produce regular reports based on accounts so this
 isn’t something new.  Simon Leary maintains Olive so could be another option.
2. The PVR numbers were wrong and the categories need to be corrected. See table below - we included the
 boxtype categories 3, 6, 8, 10 and 11 as PVR = 1 when it should have been all categories except 4 and 12.
 This should be a simple change.
3. A large number of records had no postcodes - can we investigate why this is and whether it’s because
 they are in the Republic of Ireland/Isle of Man/Channel Islands or they are businesses.


***********************************   MILESTONES   *********************************************

• Final population of HH obtained.           (Approach – point 4)
• How the Active and Churned HH were flagged and required fields were filled. (Approach – point 6)
• Count and figures for revising the outcome extracted into the final table.  (Approach – point 8)


***********************************    APPROACH  *********************************************

1) Create the outcome table with the shape mentioned in the business requirement, as shown below:
•             Name
•             Address Line 1
•             Address Line 2
•             Address Line 3
•             Address Line 4
•             Address Line 5
•             Address Line 6
•             Post code
•             Viewing Card No
•             Customer status - whether the household is Sky, ex-Sky, never-Sky (on the agreed date)
•             Multiroom – presence of multiroom in household
•             HD – presence of HD Sky Box in household
•             PVR – presence of PVR Sky Box in household
•             Vespa panel member – to support proposed Phase 1b exercise
•             Package – the package holding in the household

2) Extract all Households from Experian table (sk_prod.EXPERIAN_CONSUMERVIEW) and flag them as such, recent count -> 24,932,745 Distinct HH
                At this point is possible to get at once all Addresses lines shown above in the outcome data structure...
3) Complement HH with those from Single Account View and flag them as well.
4) Check total households = approx. 26 million. If not, add all remaining households and required identifiers from sk_prod.cust_subs_hist
                that are not in base table (flag those that are active and those that are churned).
5) Flag Active and Churned HH.
6) Fill in required 6 variables using definitions in wiki:
                                a.            Sky Variables for Active households
                                b.            Sky Variables for Churned households (e.g. for package, I think I took the last package before they churned – you’ll have to check my code)
                                c.             Third variables for Experian households only.
7) Generate random values for Variables outside the scope.
8) QA table.
9) Export data to CSV


***********************************  SECTIONS   *********************************************

 A: Defining the Base Table:
   A1: Create the Table...
   A2: Defining indexes...

 B: Filling the Base Table:
   B1: Get HH Keys from Experian...
   B2: B2 create distinct version of SAV
   B3: Complement HH Keys with SAV...

 C: Fill in the remaining fields:
   C1: Active
   C2: Vespa
   C3: RoI
   C4: Status
   C5: MR (for active customers)
   C6: HD (for active customers)
   C7: PVR (for active customers)
   C8: Package (for active customers)

 D: Fill in values for ex-customers at the time they left
   D1: Create table of last active dates
   D2: MR (for non-active customers)
   D3: HD (for non-active customers)
   D4: PVR (for non-active customers)
   D5: Package (for non-active customers)

 E: Fill in values for househoilds that have never had sky with random values
   E1: assign random values

 F: Add Viewing card
   F1: add cust_service_instance for primary boxes
   F2: add cust_service_instance for secondary only boxes
   F3: add viewing card info

 G: Output

**********************************************************************************************************************************************************/



--A1: Create the Table...

-- Basically, this table will contain all the necesary to generate the outcome, one shouldn't be
-- generating alter statements, hence every new field will have to be added in this section and
-- re-run the script (Which also should be in sequential order)
drop table data_match;
  create table data_match (
         cb_key_household       bigint not null unique
        ,account_number         varchar(30)
        ,cust_service_instance_id varchar(30)
        ,card_id                bigint
        ,cust_cb_name_fullname  varchar(100)
        ,cb_address_line_1      varchar(60)
        ,cb_address_line_2      varchar(60)
        ,cb_address_line_3      varchar(60)
        ,cb_address_line_4      varchar(60)
        ,cb_address_line_5      varchar(60)
        ,cb_address_line_6      varchar(60)
        ,cb_address_postcode    varchar(60)
        ,sky                    bit     default 0
        ,experian               bit     default 0
        ,active                 bit     default 0
        ,roi                    bit     default 0
        ,vespa                  bit     default 0
        ,status                 tinyint default 0
        ,multiroom              bit     default 0
        ,hd                     bit     default 0
        ,pvr                    bit     default 0
        ,package_type           tinyint default 0
);

create variable @reference_date date;
set @reference_date = '2012-10-18';

--A2: Defining indexes...
commit;


--B1: Get HH Keys from Experian...
  select cb_key_household as hh
        ,max(cb_seq_id) as sq
    into #experian
    from sk_prod.EXPERIAN_CONSUMERVIEW
group by cb_key_household
; --24,932,745

create hg index idx1 on #experian(hh,sq);

insert into data_match(
         cb_key_household
        ,cust_cb_name_fullname
        ,cb_address_line_1
        ,cb_address_line_2
        ,cb_address_line_3
        ,cb_address_line_4
        ,cb_address_line_5
        ,cb_address_line_6
        ,cb_address_postcode
        ,experian
)
  select cb_key_household
        ,cb_name_fullname
        ,cb_address_line_1
        ,cb_address_line_2
        ,cb_address_line_3
        ,cb_address_line_4
        ,cb_address_line_5
        ,cb_address_line_6
        ,cb_address_postcode
        ,1
    from sk_prod.EXPERIAN_CONSUMERVIEW as exp
         inner join          #experian as lkp on exp.cb_key_household = lkp.hh
                                             and exp.cb_seq_id        = lkp.sq
;

commit;

--check for nulls and zeros
select count(1) from data_match where cb_key_household is null;
select count(1) from data_match where cb_key_household=0;
--fine

--B2 create distinct version of SAV
--fill in account number from SAV. SAV has a few dupes, so need to do some fiddling:
  select cb_key_household
        ,account_number
        ,cb_row_id
        ,rank() over (partition by cb_key_household order by cb_row_id desc) as rank
    into #distinct_sav
    from sk_prod.cust_single_Account_view
   where cb_key_household is not null
group by cb_key_household
        ,ACCT_BO_LAST_MODIFIED_DT
        ,account_number
        ,cb_row_id
; --24,474,912

create hg index idx1 on #distinct_sav(cb_key_household,cb_row_id);

  delete from #distinct_sav
   where rank > 1
; --8,329,875

--B3: Complement HH Keys with SAV...
  insert into data_match(cb_key_household
                        ,cust_cb_name_fullname
                        ,cb_address_line_1
                        ,cb_address_line_2
                        ,cb_address_line_3
                        ,cb_address_line_4
                        ,cb_address_line_5
                        ,cb_address_line_6
                        ,cb_address_postcode
                        )
  select sav.cb_key_household
        ,cust_cb_name_fullname
        ,cb_address_line_1
        ,cb_address_line_2
        ,cb_address_line_3
        ,cb_address_line_4
        ,cb_address_line_5
        ,cb_address_line_6
        ,cb_address_postcode
    from sk_prod.cust_single_account_view as sav
         inner join #distinct_sav         as dis on sav.cb_row_id = dis.cb_row_id
   where sav.cb_key_household is not null --there are some this time
     and sav.cb_key_household > 0 --and one of these
     and sav.cb_key_household not in (select cb_key_household from data_match)
; --1,893,794 making a total of 26,826,539 -24mins

--check total
select count(1) from data_match;

  select account_number
        ,cb_key_household
    into #active_dtv
    from sk_prod.cust_subs_hist as csh
   where subscription_sub_type = 'DTV Primary Viewing'
     and status_code in ('AC','AB','PC')
group by account_number
        ,cb_key_household
; --20,709,798

commit;
create hg index idx1 on #active_dtv(cb_key_household);
create hg index idx2 on #active_dtv(account_number);

  update data_match as bas
     set bas.account_number = act.account_number
        ,sky = 1
    from #active_dtv as act
   where bas.cb_key_household = act.cb_key_household
; --15,008,923

--get sky data
--C1 active
  update data_match as bas
     set active = 1
    from sk_prod.cust_subs_hist as csh
   where subscription_sub_type = 'DTV Primary Viewing'
     and bas.cb_key_household = csh.cb_key_household
     and status_code in ('AC','AB','PC')
     and effective_from_dt <= @reference_date
     and effective_to_dt   >  @reference_date
;--9,562,071

--C2 Vespa. I'm putting in current status for now. Need to see whether we have historic data
  update data_match as bas
     set vespa = 1
    from vespa_analysts.vespa_single_box_view as sbv
   where bas.account_number = sbv.account_number
     and panel = 'VESPA'
; --514,895

--C3 RoI
  update data_match as bas
     set roi = 1
    from sk_prod.cust_subs_hist as csh
   where currency_code = 'EUR'
     and bas.cb_key_household = csh.cb_key_household
     and effective_from_dt <= @reference_date
     and effective_to_dt   >  @reference_date
; ---555,790

--C5 MR for active customers
  update data_match as bas
     set multiroom = 1
    from sk_prod.cust_subs_hist as csh
   where subscription_sub_type ='DTV Extra Subscription'
     and bas.cb_key_household = csh.cb_key_household
     and status_code in ('AC','AB','PC')
     and effective_from_dt <= @reference_date
     and effective_to_dt   >  @reference_date
; --2,300,771

--C6 HD for active customers
  update data_match as bas
     set hd = 1
    from sk_prod.cust_subs_hist as csh
   where subscription_sub_type ='DTV HD'
     and bas.cb_key_household = csh.cb_key_household
     and status_code in ('AC','AB','PC')
     and effective_from_dt <= @reference_date
     and effective_to_dt   >  @reference_date
; --4,150,406

--C7 PVR for active customers
  update data_match as bas
     set pvr = 1
    from sk_prod.cust_subs_hist as csh
   where subscription_sub_type in ('DTV Sky+','DTV HD')
     and bas.cb_key_household = csh.cb_key_household
     and status_code in ('AC','AB','PC')
     and effective_from_dt <= @reference_date
     and effective_to_dt   >  @reference_date
; --8,486,144

--C8 Package for active customers
  select cb_key_household
        ,max(cel.prem_sports) as prem_sports
        ,max(cel.prem_movies) as prem_movies
        ,max(case when cel.mixes = 0
                    or (cel.mixes = 1 and (cel.style_culture = 1 or  cel.variety = 1))
                    or (cel.mixes = 2 and  cel.style_culture = 1 and cel.variety = 1) then 0 else 1 end) as mix_type
    into #package
    from sk_prod.cust_subs_hist                     as csh
         inner join sk_prod.cust_entitlement_lookup as cel on csh.current_short_description = cel.short_description
   where csh.subscription_sub_type = 'DTV Primary Viewing'
     and csh.status_code in ('AC','AB','PC')
     and csh.effective_from_dt <= @reference_date
     and csh.effective_to_dt   >  @reference_date
group by cb_key_household
; --9,562,041

  update data_match as bas
     set package_type = case when prem_sports +       prem_movies = 0 and mix_type = 0 then 1
                             when prem_sports +       prem_movies = 0 and mix_type = 1 then 2
                             when prem_sports = 0 and prem_movies = 2                  then 3
                             when prem_sports = 2 and prem_movies = 0                  then 4
                             when prem_sports = 0 and prem_movies = 1                  then 6
                             when prem_sports = 1 and prem_movies = 0                  then 7
                             when prem_sports +       prem_movies = 4                  then 8
                             else                                                           5
                         end
    from #package as pck
   where bas.cb_key_household = pck.cb_key_household
; --9,562,039

-- D: Fill in values for ex-customers at the time they left
--  D1 Find leaving dates
  select csh.cb_key_household
        ,cast(null as date)     as last_date
        ,max(effective_from_dt) as end_date
    into #churned
    from sk_prod.cust_subs_hist as csh
         inner join data_match  as bas on csh.cb_key_household = bas.cb_key_household
   where subscription_sub_type = 'DTV Primary Viewing'
     and effective_from_dt <= @reference_date
     and status_code in ('PO', 'SC')
     and sky = 1
     and active =0
group by csh.cb_key_household
; --5,184,043

 create hg index idx1 on #churned(cb_key_household);

  select csh.cb_key_household
        ,max(effective_to_dt) as last_date
    into #churned_max
    from sk_prod.cust_subs_hist as csh
         inner join #churned    as chn on chn.cb_key_household = csh.cb_key_household
   where csh.status_code in ('AC','AB','PC')
     and effective_to_dt <= end_date
group by csh.cb_key_household
; --5,184,043

 create hg index idx1 on #churned_max(cb_key_household);

  update #churned as chn
     set chn.last_date = cmx.last_date
    from #churned_max as cmx
   where chn.cb_key_household = cmx.cb_key_household
; --5,184,043

--  D2 MR for ex-customers
  update data_match as bas
     set multiroom = 1
    from sk_prod.cust_subs_hist as csh
         inner join #churned    as chn on csh.cb_key_household = chn.cb_key_household
   where subscription_sub_type ='DTV Extra Subscription'
     and bas.cb_key_household = chn.cb_key_household
     and status_code in ('AC','AB','PC')
     and effective_to_dt   =  chn.last_date
; --410,078

--  D3 HD for ex-customers
  update data_match as bas
     set hd = 1
    from sk_prod.cust_subs_hist as csh
         inner join #churned    as chn on csh.cb_key_household = chn.cb_key_household
   where subscription_sub_type ='DTV HD'
     and bas.cb_key_household = chn.cb_key_household
     and status_code in ('AC','AB','PC')
     and effective_to_dt   =  chn.last_date
; --534,358

--D4 PVR for ex-customers
  update data_match as bas
     set pvr = 1
    from sk_prod.cust_subs_hist                     as csh
         inner join #churned                        as chn on csh.cb_key_household          = chn.cb_key_household
         inner join sk_prod.cust_entitlement_lookup as cel on csh.current_short_description = cel.short_description
   where csh.subscription_sub_type = 'DTV Primary Viewing'
     and bas.cb_key_household = chn.cb_key_household
     and status_code in ('AC','AB','PC')
     and effective_to_dt   =  chn.last_date
; --5,194,020

--D5 Package for ex-customers
  select csh.cb_key_household
        ,max(cel.prem_sports) as prem_sports
        ,max(cel.prem_movies) as prem_movies
        ,max(case when cel.mixes = 0
                    or (cel.mixes = 1 and (cel.style_culture = 1 or  cel.variety = 1))
                    or (cel.mixes = 2 and  cel.style_culture = 1 and cel.variety = 1) then 0 else 1 end) as mix_type
    into #package_ex
    from sk_prod.cust_subs_hist                     as csh
         inner join #churned                        as chn on csh.cb_key_household          = chn.cb_key_household
         inner join sk_prod.cust_entitlement_lookup as cel on csh.current_short_description = cel.short_description
   where csh.subscription_sub_type = 'DTV Primary Viewing'
     and csh.status_code in ('AC','AB','PC')
     and effective_to_dt   =  chn.last_date
group by csh.cb_key_household
; --5,183,801

  update data_match as bas
     set package_type = case when prem_sports +       prem_movies = 0 and mix_type = 0 then 1
                             when prem_sports +       prem_movies = 0 and mix_type = 1 then 2
                             when prem_sports = 0 and prem_movies = 2                  then 3
                             when prem_sports = 2 and prem_movies = 0                  then 4
                             when prem_sports = 0 and prem_movies = 1                  then 6
                             when prem_sports = 1 and prem_movies = 0                  then 7
                             when prem_sports +       prem_movies = 4                  then 8
                             else                                                           5
                         end
    from #package_ex as pck
   where bas.cb_key_household = pck.cb_key_household
; --5,194,020

--E1 assign random values to those that have never been customers
--sky=1 includes sky customers that have never had TV, as well as new customers that joined aftre the reference date. These will also be assigned random values.
  update data_match
     set vespa       = cast((rand(number(*))*2) as int)
        ,multiroom   = cast((rand(number(*))*2) as int)
        ,hd          = cast((rand(number(*))*2) as int)
        ,pvr         = cast((rand(number(*))*2) as int)
        ,status      = cast((rand(number(*))*8) as int) +1
        ,package_type= cast((rand(number(*))*7) as int) +1
   where package_type = 0
; --10,681,503

--delete card ids that are duplicates, or of the wreong length
--find duplicate card ids
select * from (select card_id,count(1) as cow from data_match group by card_id having cow>1) as sub

select top 100 * from data_match inner join (select card_id,count(1) as cow from data_match where card_id is not null and card_id>0 group by card_id having cow>1) as sub on data_match.card_id = sub.card_id
order by data_match.card_id

update data_match set card_id = 0 where card_id is null
--there are only 7 pairs, so let's delete them
delete from data_match where cb_key_household in (
99796855689314304
,182440706508324864
,3919986070963355648
,3450340415432032256
,200491404555714560
,7520486365844209664
,2993553732160978944
,88220819844497408
,113924914383159296
,175104019826475008
,7544559277972652032
,6551283361353039872
);


alter table data_match add (new_card varchar(50) default '0')
--then assign random values to all blank card ids

  create variable multiplier bigint;

--loop 2 start
--loop 1 start
                  SET multiplier = DATEPART(millisecond,now())+1;

                  update data_match
                     set new_card = cast(cast(rand(number(*))*multiplier*1000000 as int) as varchar) -- add something so we can recognise them
                   where card_id=0
                     and new_card = '0'
                ;--11,803,354

                  select new_card
                        ,count(1) as cow
                    into #dupes
                    from data_match
                   where card_id = 0
                group by new_card
                  having cow>1
                ;

                  update data_match as bas
                     set new_card = '0'
                    from #dupes as dup
                   where bas.new_card = dup.new_card
                    and card_id = 0
                ;
                 --6437613
                 --149781
                 --4213
                 --134
                 --6
                 --0
                drop table #dupes;
--loop 1 end
                --repeat loop 1 until no dupes

        --check whether there are any duplicates from these random additions
          select count(1)
            from data_match            as a
                 inner join data_match as b on a.new_card = cast(b.card_id as varchar)
           where a.card_id =0
        ;--226817

          update data_match as bas
             set new_card = '0'
            from data_match as joi
           where bas.new_card = cast(joi.card_id as varchar)
             and bas.card_id =0
        ;--
--loop 2 end
        --rerun loop 2 until there are no changes

  update data_match
     set card_id = new_card
   where card_id is null
      or card_id =0
; --11,803,615

--check
select count(1),count(distinct card_id) from data_match;

--C4 Status - this needs to be done after the above
  update data_match
     set status = case when sky    = 0                                            then 1 --never been a customer
                       when active = 1 and experian = 0 and vespa = 1             then 2
                       when active = 1 and experian = 0               and roi = 1 then 3
                       when active = 1 and experian = 0                           then 4
                       when active = 1 and experian = 1 and vespa = 1             then 5
                       when active = 1 and experian = 1                           then 6
                       when                experian = 1                           then 7
                       when                                               roi = 1 then 8
                       else                                                            9
                  end
; --26,826,539

--  F: Add Viewing card
--F1 add cust_service_instance for primary boxes
  update data_match as bas
      set cust_service_instance_id = csi.service_instance_ID
     from sk_prod.cust_service_instance as csi
    where bas.account_number = csi.account_number
      and si_service_instance_type = 'Primary DTV'
      and si_primary_instance_start_dt <= @reference_date
; --15,568,881

--F2 add cust_service_instance for secondary boxes
  update data_match as bas
      set cust_service_instance_id = csi.service_instance_ID
     from sk_prod.cust_service_instance as csi
    where bas.account_number = csi.account_number
      and bas.cust_service_instance_id is null
      and si_primary_instance_start_dt <= @reference_date
; --3,384,239

--F3 add viewing card number
  update data_match as bas
     set bas.card_id = cci.card_id
    from sk_prod.cust_card_issue_dim as cci
   where cci.service_instance_id = bas.cust_service_instance_id
; --12,351,714

--Add any that haven't yet been matched, by account number
  update data_match as bas
     set bas.card_id = cci.card_id
    from sk_prod.cust_card_issue_dim as cci
   where cci.account_number = bas.account_number
     and bas.card_id = 0
; --3,614,451

--  G Output
--G1: Full outpout
SET TEMPORARY OPTION TIMESTAMP_FORMAT='YYYYMMDDHHNNSS';
set temporary option TEMP_EXTRACT_QUOTE = '"';
set temporary option temp_extract_quotes= 'ON';
set temporary option temp_extract_column_delimiter=',';
set temporary option temp_extract_null_as_empty='ON';
set temporary option temp_extract_binary='OFF';
set temporary option temp_extract_name1 = '/ETL013/prod/sky/olive/data/share/clarityq/export/Jon/barb_data_match20121101.txt';
set temporary option temp_extract_name2 ='';
--go

  select 'cust_cb_name_fullname' as cust_cb_name_fullname
        ,'cb_address_line_1'     as cb_address_line_1
        ,'cb_address_line_2'     as cb_address_line_2
        ,'cb_address_line_3'     as cb_address_line_3
        ,'cb_address_line_4'     as cb_address_line_4
        ,'cb_address_line_5'     as cb_address_line_5
        ,'cb_address_line_6'     as cb_address_line_6
        ,'cb_address_postcode'   as cb_address_postcode
        ,'viewing_card_no'       as viewing_card_no
        ,cast('sky_variable_01' as varchar)      as sky_variable_01
        ,'sky_variable_02'       as sky_variable_02
        ,'sky_variable_03'       as sky_variable_03
        ,'sky_variable_04'       as sky_variable_04
        ,'sky_variable_05'       as sky_variable_05
        ,'sky_variable_06'       as sky_variable_06
  union
  select cust_cb_name_fullname
        ,cb_address_line_1
        ,cb_address_line_2
        ,cb_address_line_3
        ,cb_address_line_4
        ,cb_address_line_5
        ,cb_address_line_6
        ,cb_address_postcode
        ,cast(card_id as varchar)     as viewing_card_no
        ,cast(status as varchar)      as sky_variable_01
        ,cast(multiroom as varchar)    as sky_variable_02
        ,cast(hd as varchar)           as sky_variable_03
        ,cast(pvr as varchar)         as sky_variable_04
        ,cast(vespa as varchar)        as sky_variable_05
        ,cast(package_type as varchar) as sky_variable_06
    from data_match
   where sky_variable_01 not in ('3','8')


--go
select count(1),count(distinct card_id) from data_match
where card_id is not null

--G2: counts for pivot table
  select status       as sky_variable_01
        ,multiroom    as sky_variable_02
        ,hd           as sky_variable_03
        ,pvr          as sky_variable_04
        ,vespa        as sky_variable_05
        ,package_type as sky_variable_06
        ,count(1)     as total_hh
    into #counts
    from data_match
group by sky_variable_01
        ,sky_variable_02
        ,sky_variable_03
        ,sky_variable_04
        ,sky_variable_05
        ,sky_variable_06

select * from #counts





alter table data_match rename data_match_20120425;



---
--To load the original csv for comparison:
drop table data_match_sj;
create table data_match_sj
(
cb_address_line_fullname varchar(60)
,cb_address_line_1              varchar(60)
,cb_address_line_2              varchar(60)
,cb_address_line_3              varchar(60)
,cb_address_line_4              varchar(60)
,cb_address_line_5              varchar(60)
,cb_address_line_6              varchar(60)
,cb_address_postcode            varchar(60)
,viewing_card_no                bigint
,sky_variable_01                int
,sky_variable_02                int
,sky_variable_03                int
,sky_variable_04                int
,sky_variable_05                int
,sky_variable_06                int
,sky_variable_07                tinyint
,sky_variable_08                tinyint
,sky_variable_09                tinyint
,sky_variable_10                tinyint
,sky_variable_11                tinyint
,sky_variable_12                tinyint
,sky_variable_13                tinyint
,sky_variable_14                int
,sky_variable_15                int
,sky_variable_16                int
,sky_variable_17                int
,sky_variable_18                int
,sky_variable_19                int
,sky_variable_20                int
,sky_variable_21                int
,sky_variable_22                int
,sky_variable_23                int
,sky_variable_24                int
,sky_variable_25                int
,sky_variable_26                tinyint
,sky_variable_27                tinyint
,sky_variable_28                tinyint
,sky_variable_29                tinyint
,sky_variable_30                tinyint
,sky_variable_31                tinyint
,sky_variable_32                int
,sky_variable_33                int
,sky_variable_34                bigint
,sky_variable_35                tinyint
,sky_variable_36                int
,sky_variable_37                int
,sky_variable_38                int
,sky_variable_39                int
,sky_variable_40                int
,sky_variable_41                int
);

load table data_match_sj
(
cb_address_line_fullname'|',
cb_address_line_1'|',
cb_address_line_2'|',
cb_address_line_3'|',
cb_address_line_4'|',
cb_address_line_5'|',
cb_address_line_6'|',
cb_address_postcode'|',
viewing_card_no'|',
sky_variable_01'|',
sky_variable_02'|',
sky_variable_03'|',
sky_variable_04'|',
sky_variable_05'|',
sky_variable_06'|',
sky_variable_07'|',
sky_variable_08'|',
sky_variable_09'|',
sky_variable_10'|',
sky_variable_11'|',
sky_variable_12'|',
sky_variable_13'|',
sky_variable_14'|',
sky_variable_15'|',
sky_variable_16'|',
sky_variable_17'|',
sky_variable_18'|',
sky_variable_19'|',
sky_variable_20'|',
sky_variable_21'|',
sky_variable_22'|',
sky_variable_23'|',
sky_variable_24'|',
sky_variable_25'|',
sky_variable_26'|',
sky_variable_27'|',
sky_variable_28'|',
sky_variable_29'|',
sky_variable_30'|',
sky_variable_31'|',
sky_variable_32'|',
sky_variable_33'|',
sky_variable_34'|',
sky_variable_35'|',
sky_variable_36'|',
sky_variable_37'|',
sky_variable_39'|',
sky_variable_40'|',
sky_variable_41'\n'
)
from '/ETL013/prod/sky/olive/data/share/clarityq/export/Jon/Barb_versus_vespa_data_match_20120621.txt'
QUOTES ON
ESCAPES OFF
NOTIFY 1000
delimited by '|'
;
--variable 7 on are empty tho.
sky_variable_01=int_household_composition
sky_variable_02=isba_tv_region
sky_variable_03=package
sky_variable_04=tenure
sky_variable_05=mr_boxes
sky_variable_06=boxtype









select 12458348-12458329
select card_id,count(1) as cow from data_match group by card_id having cow>1
card_id cow
0 13
124647231 2
146478292 2
 14335711
148507056 2
187362553 2
143367407 2
208841023 2
169608007 2

select len(cast(card_id as varchar)) as lg,count(1) as cow from data_match group by lg
select top 10 card_id,len(card_id),left(card_id,8) from data_match




