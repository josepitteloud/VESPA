/* ---------------------------------------------------------------

        TOTAL CONTRIBUTION SCRIPT
        -------------------------

        Author  : Nick Leech
        Date    : 1st April 2010
        Version : 1.03


        Usage - Use this script in your own code to obtain the total contribution
                for a list of accounts.

                Simply change the ADD TARGET ACCOUNTS to populate EPL_05_Contribution
                with your own accounts list.  Note the Target date can vary by account
                number.

                The total_contribution includes the PPV contribution  which can make the total
                somewhat volitile.  Use the no_ppv_contribution if you want to exclude ppv.

                MAKE SURE THE ATTACHMENT CONTRIBUTIONS ARE UPTO DATE!
                Check  http://mktskyportal/Campaign%20Handbook/Contribution.aspx


   Version History
   ---------------

        V1.03 01/04/2010 NL  - Altered so that a single account can appear multiple times
                               Sports, Movies, Sky Plus added - not used in contribution but often useful when this scipt is run in conjunttion with other things
        V1.02 17/02/2010 NL  - WLR made independent from Sky Talk
        V1.01 10/11/2009 MR  - amend script to incorporate latest code for SkyTalk as well as some status codes for BB and WLR
        V1.00 31/07/2009 NL  - Initial Version


-------------------------------------------------------------------*/



--------------------------------------Create Working Table
--Create a Base table
CREATE TABLE EPL_05_Contribution(
         id                     bigint          identity  -- a unique ID used for self referencing (enables non unquie account Numbers)
        ,Period                 tinyint         not null
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

--Index and grant permissions
CREATE UNIQUE hg INDEX idx1 on EPL_05_Contribution(id);
CREATE        hg INDEX idx2 on EPL_05_Contribution(account_number);
CREATE        lf INDEX idx3 on EPL_05_Contribution(period);

----------------------------------------ADD TARGET ACCOUNTS

truncate table EPL_05_Contribution;

INSERT into EPL_05_Contribution (period, account_number, target_date)
  select
        Period,
        Account_Number,
        case
          when Latest_Active_Date is null then cast('2014-02-27' as date)
            else Latest_Active_Date - 1
        end
    from EPL_04_Profiling_Variables
   where Period = 1;
commit;


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
    INTO #DTV
    FROM sk_prod.cust_subs_hist as csh
         inner join EPL_05_Contribution as base on csh.account_number = base.account_number
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
commit;



--Flag the contribution
UPDATE EPL_05_Contribution
   SET  DTV = 1
       ,DTV_contribution = tgt.contribution
       ,sports = prem_sports
       ,movies = prem_movies
  FROM EPL_05_Contribution as base
       inner join #DTV as tgt on base.id = tgt.id
 WHERE tgt.rank = 1;
commit;


-------------------------------------------------------------PPV

UPDATE EPL_05_Contribution
   SET  ppv_contribution = tgt.ppv_contribution * 0.16  -- Only a part of the PPV value is actual Contribution
  FROM EPL_05_Contribution as base
       inner join (
                select  base.id
                       ,sum (charge_amount_incl_tax) as ppv_contribution
                  from sk_prod.CUST_PRODUCT_CHARGES_PPV as ppv
                       inner join EPL_05_Contribution as base on ppv.account_number = base.account_number
                       inner join sk_prod.cust_single_account_view as sav on ppv.account_number = sav.account_number
                 where ppv.event_dt between dateadd(month,-3,base.target_date) and base.target_date
                   and ppv.ppv_cancelled_dt ='9999-09-09'
                   and ppv.charge_amount_incl_tax > 0
                   and ppv.ppv_service = 'MOVIE'
              group by base.id
       ) as tgt on base.id = tgt.id;
commit;


------------------------------------------------FLAG SVBN Accounts

UPDATE EPL_05_Contribution
   SET SVBN = 1
  FROM EPL_05_Contribution as base
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
commit;



----------------------------------------------Get Attachment Counts

 select     base.id
           ,sum  (case  when    csh.SUBSCRIPTION_SUB_TYPE ='DTV Extra Subscription'
                            and csh.status_code in  ('AC','AB','PC') then 1 else 0  end) as MR
           ,max  (case  when    csh.SUBSCRIPTION_SUB_TYPE ='DTV HD'
                            and csh.status_code in  ('AC','AB','PC') then 1 else 0 end)  as HD
           ,sum  (case  when    csh.SUBSCRIPTION_SUB_TYPE ='DTV Sky+'
                            and csh.status_code in  ('AC','AB','PC') then 1 else 0 end)  as SP
           ,max  (case  when    csh.SUBSCRIPTION_SUB_TYPE ='Broadband DSL Line'
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
                            and current_product_description like '%Weekend%'          then 4 -- Weekends
                        when    csh.subscription_sub_type = 'SKY TALK SELECT'
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
                            and csh.status_code in  ('A','CRQ','R')                 then 1 -- WLR
                        else                                                               0 -- Nuffin
                   end) AS WLR
      into #Attachents
      from sk_prod.cust_subs_hist as csh
           inner join EPL_05_Contribution as base on csh.account_number = base.account_number
     where csh.effective_from_dt <= base.target_date
       and csh.effective_to_dt    > base.target_date
       and csh.effective_from_dt <> csh.effective_to_dt
       and csh.SUBSCRIPTION_SUB_TYPE in (  'DTV Extra Subscription'
                                          ,'DTV HD','Broadband DSL Line'
                                          ,'SKY TALK SELECT'
                                          ,'SKY TALK LINE RENTAL'
                                          ,'DTV Sky+')
       and csh.status_code in  ('AC','AB','PC','A','L','RI','FBP','CRQ','R')
  group by base.id;
commit;


CREATE UNIQUE HG INDEX idx01 on #Attachents(id);

UPDATE EPL_05_Contribution
   SET  HD      = tgt.HD
       ,MR      = tgt.MR
       ,SP      = tgt.SP
       ,WLR     = tgt.WLR
       ,ST_Pack = case when ST = 4 then 'WKE'
                       when ST = 3 then 'UNL'
                       when ST = 2 then 'FREE'
                       else             null
                    end
       ,BB_Pack = case when BB = 7 then 'FIB_PRO'
                       when BB = 6 then 'FIB'
                       when BB = 5 then 'NEW_UNL'
                       when BB = 4 then 'OLD_UNL'
                       when BB = 3 then 'ED'
                       when BB = 2 then 'ED_LITE'
                       when BB = 1 then 'CONN'
                       else              null
                    end
  FROM EPL_05_Contribution as base
       inner join #Attachents as tgt on base.id = tgt.id;
commit;


--------------------------------------------------------Attachment Contribution Calculations

--Check these values are up to date from http://mktskyportal/Campaign%20Handbook/Contribution.aspx

UPDATE EPL_05_Contribution
   SET xtra_contribution = tgt.HD_Cont + tgt.MR_Cont + tgt.ST_Cont + tgt.WLR_cont + tgt.BB_Cont
  FROM EPL_05_Contribution as base
       inner join (
            Select id
                   ,7.51 * HD as HD_Cont
                   ,8.28 * MR as MR_Cont
                   ,case when ST_Pack = 'WKE'   and SVBN = 1 then 2.99
                         when ST_Pack = 'WKE'                then 3.24
                         when ST_Pack = 'UNL'   and SVBN = 1 then 7.01
                         when ST_Pack = 'UNL'                then 5.79
                         when ST_Pack = 'FREE'  and SVBN = 1 then 2.92
                         when ST_Pack = 'FREE'               then 3.14
                         else                                     0
                     end as ST_Cont
                   ,case when WLR + SVBN = 2 then  5.84
                         when WLR = 1        then  2.98
                         else                      0
                     end as WLR_Cont
                    ,case -- Standalone
                          WHEN DTV = 0 AND SVBN = 1 AND BB_Pack IS NOT NULL             THEN  4.53
                          WHEN DTV = 0 AND SVBN = 0 AND BB_Pack IS NOT NULL             THEN  4.53

                          --SOLUS
--                          WHEN WLR = 0 AND ST_Pack IS NULL AND BB_Pack = 'NEW_UNL'      THEN  5.97
--                          WHEN WLR = 0 AND ST_Pack IS NULL AND BB_Pack = 'OLD_UNL'      THEN  8.10
--                          WHEN WLR = 0 AND ST_Pack IS NULL AND BB_Pack = 'ED'           THEN  4.00
--                          WHEN WLR = 0 AND ST_Pack IS NULL AND BB_Pack = 'ED_LITE'      THEN -0.08
--                          WHEN WLR = 0 AND ST_Pack IS NULL AND BB_Pack = 'CONN'         THEN  3.00

                          --With NVN (BB + TALK + WLR)
                          WHEN SVBN = 1 AND BB_Pack = 'FIB_PRO'                         THEN  4.63
                          WHEN SVBN = 1 AND BB_Pack = 'FIB'                             THEN  4.63
                          WHEN SVBN = 1 AND BB_Pack = 'NEW_UNL'                         THEN  2.42
                          WHEN SVBN = 1 AND BB_Pack = 'OLD_UNL'                         THEN  4.53
                          WHEN SVBN = 1 AND BB_Pack = 'ED'                              THEN  0.31
                          WHEN SVBN = 1 AND BB_Pack = 'ED_LITE'                         THEN -3.90

                          --With No NVN (BB + TALK + WLR)
                          WHEN BB_Pack = 'FIB_PRO'                                      THEN  4.63
                          WHEN BB_Pack = 'FIB'                                          THEN  4.63
                          WHEN BB_Pack = 'NEW_UNL'                                      THEN  2.42
                          WHEN BB_Pack = 'OLD_UNL'                                      THEN  4.52
                          WHEN BB_Pack = 'ED'                                           THEN  0.31
                          WHEN BB_Pack = 'ED_LITE'                                      THEN -3.90
                          WHEN BB_Pack = 'CONN'                                         THEN  1.06

                          ELSE                                                                0
                      END AS BB_Cont
              from EPL_05_Contribution
       )as tgt on base.id = tgt.id;
commit;


------------------------------------------------------------Total Contribution

UPDATE  EPL_05_Contribution
   SET  total_contribution  = DTV_contribution + xtra_contribution + ppv_contribution
       ,no_ppv_contribution = DTV_contribution + xtra_contribution;
commit;




