--Total Boxes
select count (Card_Subscriber_ID)
from vespa_analysts.SIG_SINGLE_BOX_VIEW
where status_vespa = 'Enabled'
and panel = 'VESPA'

--Primary Box Enabled
select count (Card_Subscriber_ID)
from vespa_analysts.SIG_SINGLE_BOX_VIEW
where status_vespa = 'Enabled'
and panel = 'VESPA'
and ps_flag = 'P'

--Primary Box Enabled and Returned Data
select count (Card_Subscriber_ID)
from vespa_analysts.SIG_SINGLE_BOX_VIEW
where status_vespa = 'Enabled'
and panel = 'VESPA'
and ps_flag = 'P'
and Num_logs_sent_30d > 0

--% Primary Boxes Returning Data

--Non-Primary Box Enabled
select count (Card_Subscriber_ID)
from vespa_analysts.SIG_SINGLE_BOX_VIEW
where status_vespa = 'Enabled'
and panel = 'VESPA'
and ps_flag = 'S'

--Non-Primary Box Enabled and Returned Data
select count (Card_Subscriber_ID)
from vespa_analysts.SIG_SINGLE_BOX_VIEW
where status_vespa = 'Enabled'
and panel = 'VESPA'
and ps_flag = 'S'
and Num_logs_sent_30d > 0

--% Non-Primary Boxes Returning Data

--% Boxes Returning Data

--Primary Box - HD with HD Subscription
select count (Card_Subscriber_ID)
from vespa_analysts.SIG_SINGLE_BOX_VIEW as SBV
inner join vespa_analysts.SIG_SINGLE_ACCOUNT_VIEW as SAV
on SBV.account_number = SAV.account_number
where SBV.status_vespa = 'Enabled'
and SBV.panel = 'VESPA'
and SBV.HD_Box_Physical = 1
and SBV.ps_flag = 'P'
and SAV.HD_box_subs = 1

--Primary Box - HD with no HD Subscription
select count (Card_Subscriber_ID)
from vespa_analysts.SIG_SINGLE_BOX_VIEW as SBV
inner join vespa_analysts.SIG_SINGLE_ACCOUNT_VIEW as SAV
on SBV.account_number = SAV.account_number
where SBV.status_vespa = 'Enabled'
and SBV.panel = 'VESPA'
and SBV.HD_Box_Physical = 1
and SBV.ps_flag = 'P'
and SAV.HD_box_subs = 0

--Primary Box - Sky+
select count (Card_Subscriber_ID)
from vespa_analysts.SIG_SINGLE_BOX_VIEW as SBV
inner join vespa_analysts.SIG_SINGLE_ACCOUNT_VIEW as SAV
on SBV.account_number = SAV.account_number
where SBV.status_vespa = 'Enabled'
and SBV.panel = 'VESPA'
and SBV.ps_flag = 'P'
and SAV.box_type_subs in ('F) SkyPlus', 'E) SkyPlus Combi')

--Primary Box - FDB
select count (Card_Subscriber_ID)
from vespa_analysts.SIG_SINGLE_BOX_VIEW as SBV
inner join vespa_analysts.SIG_SINGLE_ACCOUNT_VIEW as SAV
on SBV.account_number = SAV.account_number
where SBV.status_vespa = 'Enabled'
and SBV.panel = 'VESPA'
and SBV.ps_flag = 'P'
and SAV.box_type_subs = 'H) FDB'

--Primary Box - Other/Unknown
select count (Card_Subscriber_ID)
from vespa_analysts.SIG_SINGLE_BOX_VIEW as SBV
inner join vespa_analysts.SIG_SINGLE_ACCOUNT_VIEW as SAV
on SBV.account_number = SAV.account_number
where SBV.status_vespa = 'Enabled'
and SBV.panel = 'VESPA'
and SBV.ps_flag = 'P'
and SAV.box_type_subs in ('G) Multiroom', 'Unknown')


--Secondary Box - HD with HD Subscription
select count (Card_Subscriber_ID)
from vespa_analysts.SIG_SINGLE_BOX_VIEW as SBV
inner join vespa_analysts.SIG_SINGLE_ACCOUNT_VIEW as SAV
on SBV.account_number = SAV.account_number
where SBV.status_vespa = 'Enabled'
and SBV.panel = 'VESPA'
and SBV.HD_Box_Physical = 1
and SBV.ps_flag = 'S'
and SAV.HD_box_subs = 1

--Secondary Box - HD with no HD Subscription
select count (Card_Subscriber_ID)
from vespa_analysts.SIG_SINGLE_BOX_VIEW as SBV
inner join vespa_analysts.SIG_SINGLE_ACCOUNT_VIEW as SAV
on SBV.account_number = SAV.account_number
where SBV.status_vespa = 'Enabled'
and SBV.panel = 'VESPA'
and SBV.HD_Box_Physical = 1
and SBV.ps_flag = 'S'
and SAV.HD_box_subs = 0

--Secondary Box - Sky+
select count (Card_Subscriber_ID)
from vespa_analysts.SIG_SINGLE_BOX_VIEW as SBV
where SBV.status_vespa = 'Enabled'
and SBV.panel = 'VESPA'
and SAV.box_type_subs in ('F) SkyPlus', 'E) SkyPlus Combi')
and SBV.ps_flag = 'S'

--Secondary Box - FDB
select count (Card_Subscriber_ID)
from vespa_analysts.SIG_SINGLE_BOX_VIEW as SBV
inner join vespa_analysts.SIG_SINGLE_ACCOUNT_VIEW as SAV
on SBV.account_number = SAV.account_number
where SBV.status_vespa = 'Enabled'
and SBV.panel = 'VESPA'
and SBV.ps_flag = 'S'
and SAV.box_type_subs = 'H) FDB'

--Secondary Box - Other/Unknown
select count (Card_Subscriber_ID)
from vespa_analysts.SIG_SINGLE_BOX_VIEW as SBV
inner join vespa_analysts.SIG_SINGLE_ACCOUNT_VIEW as SAV
on SBV.account_number = SAV.account_number
where SBV.status_vespa = 'Enabled'
and SBV.panel = 'VESPA'
and SBV.ps_flag = 'S'
and SAV.box_type_subs in ('G) Multiroom', 'Unknown')

--Total Accounts
select count (distinct account_number)
from vespa_analysts.SIG_SINGLE_BOX_VIEW
where status_vespa = 'Enabled'
and panel = 'VESPA'

--Only Primary Box Enabled
select count (SBV.Card_Subscriber_ID)
       from (
             select sum(enabled) as t_enable, account_number
                                                            from (
                                                                  select case when status_vespa = 'Enabled' then 1 else 0 end as enabled
                                                                  ,account_number
                                                                  from vespa_analysts.SIG_SINGLE_BOX_VIEW
                                                                  where panel = 'VESPA'
                                                                  ) as n
             group by account_number
            ) as x
inner join vespa_analysts.SIG_SINGLE_BOX_VIEW as SBV
on sbv.account_number = x.account_number
where t_enable = 1
and SBV.ps_flag = 'P'

--Only Primary Box Enabled and Returned Data
select count (SBV.Card_Subscriber_ID)
       from (
             select sum(enabled) as t_enable, account_number
                                                            from (
                                                                  select case when status_vespa = 'Enabled' then 1 else 0 end as enabled
                                                                  ,account_number
                                                                  from vespa_analysts.SIG_SINGLE_BOX_VIEW
                                                                  where panel = 'VESPA'
                                                                                                                                    ) as n
             group by account_number
            ) as x
inner join vespa_analysts.SIG_SINGLE_BOX_VIEW as SBV
on sbv.account_number = x.account_number
where t_enable = 1
and SBV.ps_flag = 'P'
and Num_logs_sent_30d > 0

--Only Primary Box Enabled - % Returning Data
--Multiple Boxes Enabled
select count (SBV.Card_Subscriber_ID)
       from (
             select sum(enabled) as t_enable, account_number
                                                            from (
                                                                  select case when status_vespa = 'Enabled' then 1 else 0 end as enabled
                                                                  ,account_number
                                                                  from vespa_analysts.SIG_SINGLE_BOX_VIEW
                                                                  where panel = 'VESPA'
                                                                  ) as n
             group by account_number
            ) as x
inner join vespa_analysts.SIG_SINGLE_BOX_VIEW as SBV
on sbv.account_number = x.account_number
where t_enable > 1

--Multiple Boxes Enabled and Data returned from Multiple Boxes
select count (SBV.Card_Subscriber_ID)
       from (
             select sum(enabled) as t_enable, account_number
                                                            from (
                                                                  select case when status_vespa = 'Enabled' then 1 else 0 end as enabled
                                                                  ,account_number
                                                                  from vespa_analysts.SIG_SINGLE_BOX_VIEW
                                                                  where panel = 'VESPA'
                                                                  and Num_logs_sent_7d > 0
                                                                  ) as n
             group by account_number
            ) as x
inner join vespa_analysts.SIG_SINGLE_BOX_VIEW as SBV
on sbv.account_number = x.account_number
where t_enable > 1

--Multiple Boxes Enabled and Data returned from No Boxes
select count (SBV.Card_Subscriber_ID)
       from (
             select sum(enabled) as t_enable, sum (returning) as ret, account_number
                                                            from (
                                                                  select case when status_vespa = 'Enabled' then 1 else 0 end as enabled
                                                                  ,account_number
                                                                  ,case when Num_logs_sent_7d > 0 then 1 else 0 end as returning
                                                                  from vespa_analysts.SIG_SINGLE_BOX_VIEW
                                                                  where panel = 'VESPA'
                                                                                                                                    ) as n
             group by account_number
            ) as x
inner join vespa_analysts.SIG_SINGLE_BOX_VIEW as SBV
on sbv.account_number = x.account_number
where t_enable > 1
and ret = 0

--Only Non-Primary Boxes Enabled
select count (SBV.Card_Subscriber_ID)
       from (
             select sum(enabled) as t_enable, account_number
                                                            from (
                                                                  select case when status_vespa = 'Enabled' then 1 else 0 end as enabled
                                                                  ,account_number
                                                                  from vespa_analysts.SIG_SINGLE_BOX_VIEW
                                                                  where panel = 'VESPA'
                                                                  ) as n
             group by account_number
            ) as x
inner join vespa_analysts.SIG_SINGLE_BOX_VIEW as SBV
on sbv.account_number = x.account_number
where t_enable >= 1
and SBV.ps_flag = 'P'
and status_vespa <> 'Enabled'

--Multiple Boxes Enabled - Multiple Boxes Returning Data %

--Top Tier
select count (distinct SBV.account_number)
from vespa_analysts.SIG_SINGLE_BOX_VIEW as SBV
inner join vespa_analysts.SIG_SINGLE_ACCOUNT_VIEW as SAV
on SBV.account_number = SAV.account_number
where SBV.status_vespa = 'Enabled'
and SBV.panel = 'VESPA'
and prem_sports = 2
and prem_movies = 2

--Dual Sports
select count (distinct SBV.account_number)
from vespa_analysts.SIG_SINGLE_BOX_VIEW as SBV
inner join vespa_analysts.SIG_SINGLE_ACCOUNT_VIEW as SAV
on SBV.account_number = SAV.account_number
where SBV.status_vespa = 'Enabled'
and SBV.panel = 'VESPA'
and prem_sports = 2
and prem_movies = 0

--Dual Movies
select count (distinct SBV.account_number)
from vespa_analysts.SIG_SINGLE_BOX_VIEW as SBV
inner join vespa_analysts.SIG_SINGLE_ACCOUNT_VIEW as SAV
on SBV.account_number = SAV.account_number
where SBV.status_vespa = 'Enabled'
and SBV.panel = 'VESPA'
and prem_sports = 0
and prem_movies = 2

--Dual Sports & Single Movies
select count (distinct SBV.account_number)
from vespa_analysts.SIG_SINGLE_BOX_VIEW as SBV
inner join vespa_analysts.SIG_SINGLE_ACCOUNT_VIEW as SAV
on SBV.account_number = SAV.account_number
where SBV.status_vespa = 'Enabled'
and SBV.panel = 'VESPA'
and prem_sports = 2
and prem_movies = 1

--Dual Movies & Single Sports
select count (distinct SBV.account_number)
from vespa_analysts.SIG_SINGLE_BOX_VIEW as SBV
inner join vespa_analysts.SIG_SINGLE_ACCOUNT_VIEW as SAV
on SBV.account_number = SAV.account_number
where SBV.status_vespa = 'Enabled'
and SBV.panel = 'VESPA'
and prem_sports = 1
and prem_movies = 2

--Single Sports & Single Movies
select count (distinct SBV.account_number)
from vespa_analysts.SIG_SINGLE_BOX_VIEW as SBV
inner join vespa_analysts.SIG_SINGLE_ACCOUNT_VIEW as SAV
on SBV.account_number = SAV.account_number
where SBV.status_vespa = 'Enabled'
and SBV.panel = 'VESPA'
and prem_sports = 1
and prem_movies = 1

--No Prems
select count (distinct SBV.account_number)
from vespa_analysts.SIG_SINGLE_BOX_VIEW as SBV
inner join vespa_analysts.SIG_SINGLE_ACCOUNT_VIEW as SAV
on SBV.account_number = SAV.account_number
where SBV.status_vespa = 'Enabled'
and SBV.panel = 'VESPA'
and prem_sports = 0
and prem_movies = 0

-- Total customers
select count (account_number)
from vespa_analysts.SIG_SINGLE_ACCOUNT_VIEW

-- Personalisation Consent
select count (account_number)
from vespa_analysts.SIG_SINGLE_ACCOUNT_VIEW
where viewing_consent_flag = 'Y'

-- No Personalisation Consent
select count (account_number)
from vespa_analysts.SIG_SINGLE_ACCOUNT_VIEW
where viewing_consent_flag <> 'Y'

-- % Consent
-- AdSmartable Accounts (Sky Base) + Personalisation Consent - Adsmartable
-- AdSmartable Accounts (Sky Base) + Personalisation Consent - Not Adsmartable
-- AdSmartable Accounts (Sky Base) + Personalisation Consent - Total
select count (account_number), adsmart_flag
from vespa_analysts.SIG_SINGLE_ACCOUNT_VIEW
where viewing_consent_flag = 'Y'
group by adsmart_flag

/*
-- AdSmartable Accounts (Sky Base) + Personalisation Consent - Adsmartable
select count (account_number)
from vespa_analysts.SIG_SINGLE_ACCOUNT_VIEW
where viewing_consent_flag = 'Y'
and adsmart_flag = 1

-- AdSmartable Accounts (Sky Base) + Personalisation Consent - Not Adsmartable
select count (account_number)
from vespa_analysts.SIG_SINGLE_ACCOUNT_VIEW
where viewing_consent_flag = 'Y'
and adsmart_flag = 0

-- AdSmartable Accounts (Sky Base) + Personalisation Consent - Total
select count (account_number)
from vespa_analysts.SIG_SINGLE_ACCOUNT_VIEW
where viewing_consent_flag = 'Y'
*/

-- AdSmartable Accounts (VESPA DP) - Adsmartable
-- AdSmartable Accounts (VESPA DP) - Not Adsmartable
-- AdSmartable Accounts (VESPA DP) - Total
select count (distinct SBV.account_number), SAV.adsmart_flag
from vespa_analysts.SIG_SINGLE_ACCOUNT_VIEW as SAV
inner join vespa_analysts.SIG_SINGLE_BOX_VIEW as SBV
on SAV.account_number = SBV.account_number
where viewing_consent_flag = 'Y'
and SBV.panel = 'VESPA'
and SBV.status_vespa = 'Enabled'
group by SAV.adsmart_flag

/*
-- AdSmartable Accounts (VESPA DP) - Adsmartable
select count (distinct SBV.account_number)
from vespa_analysts.SIG_SINGLE_ACCOUNT_VIEW as SAV
inner join vespa_analysts.SIG_SINGLE_BOX_VIEW as SBV
on SAV.account_number = SBV.account_number
where viewing_consent_flag = 'Y'
and SAV.adsmart_flag = 1
and SBV.panel = 'VESPA'
and SBV.status_vespa = 'Enabled'

-- AdSmartable Accounts (VESPA DP) - Not Adsmartable
select count (distinct SBV.account_number)
from vespa_analysts.SIG_SINGLE_ACCOUNT_VIEW as SAV
inner join vespa_analysts.SIG_SINGLE_BOX_VIEW as SBV
on SAV.account_number = SBV.account_number
where viewing_consent_flag = 'Y'
and SAV.adsmart_flag = 0
and SBV.panel = 'VESPA'
and SBV.status_vespa = 'Enabled'

-- AdSmartable Accounts (VESPA DP) - Total
select count (distinct SBV.account_number)
from vespa_analysts.SIG_SINGLE_ACCOUNT_VIEW as SAV
inner join vespa_analysts.SIG_SINGLE_BOX_VIEW as SBV
on SAV.account_number = SBV.account_number
where viewing_consent_flag = 'Y'
and SBV.panel = 'VESPA'
and SBV.status_vespa = 'Enabled'
*/

-- AdSmartable Accounts (VESPA Reporting DP) - Adsmartable
-- AdSmartable Accounts (VESPA Reporting DP) - Not Adsmartable
-- AdSmartable Accounts (VESPA Reporting DP) - Total
select count (distinct SBV.account_number), SAV.adsmart_flag
from vespa_analysts.SIG_SINGLE_ACCOUNT_VIEW as SAV
inner join vespa_analysts.SIG_SINGLE_BOX_VIEW as SBV
on SAV.account_number = SBV.account_number
where viewing_consent_flag = 'Y'
and SBV.panel = 'VESPA'
and SBV.status_vespa = 'Enabled'
and SBV.Num_logs_sent_30d > 0
group by SAV.adsmart_flag

/*
-- AdSmartable Accounts (VESPA Reporting DP) - Adsmartable
select count (distinct SBV.account_number)
from vespa_analysts.SIG_SINGLE_ACCOUNT_VIEW as SAV
inner join vespa_analysts.SIG_SINGLE_BOX_VIEW as SBV
on SAV.account_number = SBV.account_number
where viewing_consent_flag = 'Y'
and SAV.adsmart_flag = 1
and SBV.panel = 'VESPA'
and SBV.status_vespa = 'Enabled'
and SBV.Num_logs_sent_30d > 0

-- AdSmartable Accounts (VESPA Reporting DP) - Not Adsmartable
select count (distinct SBV.account_number)
from vespa_analysts.SIG_SINGLE_ACCOUNT_VIEW as SAV
inner join vespa_analysts.SIG_SINGLE_BOX_VIEW as SBV
on SAV.account_number = SBV.account_number
where viewing_consent_flag = 'Y'
and SAV.adsmart_flag = 0
and SBV.panel = 'VESPA'
and SBV.status_vespa = 'Enabled'
and SBV.Num_logs_sent_30d > 0

-- AdSmartable Accounts (VESPA Reporting DP) - Total
select count (distinct SBV.account_number)
from vespa_analysts.SIG_SINGLE_ACCOUNT_VIEW as SAV
inner join vespa_analysts.SIG_SINGLE_BOX_VIEW as SBV
on SAV.account_number = SBV.account_number
where viewing_consent_flag = 'Y'
and SBV.panel = 'VESPA'
and SBV.status_vespa = 'Enabled'
and SBV.Num_logs_sent_30d > 0
*/

-- Box Type Panel Pivot
select count (distinct SAV.account_number)
       ,box_type_subs
       ,case when SBV.panel = 'VESPA' and SBV.status_vespa = 'Enabled' then 1 else 0 end as DP
from vespa_analysts.SIG_SINGLE_ACCOUNT_VIEW as SAV
inner join vespa_analysts.SIG_SINGLE_BOX_VIEW as SBV
on SAV.account_number = SBV.account_number
group by box_type_subs, DP
order by DP
/*
-- Box Type - Sky Base
select count (account_number), box_type_subs
from vespa_analysts.SIG_SINGLE_ACCOUNT_VIEW
group by box_type_subs

-- Box Type - VESPA DP
select count (distinct SAV.account_number), box_type_subs
from vespa_analysts.SIG_SINGLE_ACCOUNT_VIEW as SAV
inner join vespa_analysts.SIG_SINGLE_BOX_VIEW as SBV
on SAV.account_number = SBV.account_number
where SBV.panel = 'VESPA'
and SBV.status_vespa = 'Enabled'
group by box_type_subs
*/

--Premiums - Sky Base Pivot
select count (account_number)
       ,case when prem_sports is null then 0 else prem_sports end as sports
       ,case when prem_movies is null then 0 else prem_movies end as movies
from vespa_analysts.SIG_SINGLE_ACCOUNT_VIEW
group by sports, movies

--or

select count (account_number)
       ,case when prem_sports = 2 and prem_movies = 2 then 'A) Top Tier'
             when prem_sports = 2 and prem_movies = 0 then 'B) Dual Sports'
             when prem_sports = 0 and prem_movies = 2 then 'C) Dual Movies'
             when prem_sports = 2 and prem_movies = 1 then 'D) Dual Sports & Single Movies'
             when prem_sports = 1 and prem_movies = 2 then 'E) Dual Movies & Single Sports'
             when prem_sports = 1 and prem_movies = 1 then 'F) Single Sports & Single Movies'
             when prem_sports = 1 and prem_movies = 0 then 'G) Single Sports'
             when prem_sports = 0 and prem_movies = 1 then 'H) Single Movies'
             else                                          'I) No Premiums'
             end as Premiums
from vespa_analysts.SIG_SINGLE_ACCOUNT_VIEW
group by Premiums
order by Premiums
/*
--Premiums - Sky Base - Top Tier
select count (account_number)
from vespa_analysts.SIG_SINGLE_ACCOUNT_VIEW
where prem_sports = 2
and prem_movies = 2

--Premiums - Sky Base - Dual Sports
select count (account_number)
from vespa_analysts.SIG_SINGLE_ACCOUNT_VIEW
where prem_sports = 2
and prem_movies = 0

--Premiums - Sky Base - Dual Movies
select count (account_number)
from vespa_analysts.SIG_SINGLE_ACCOUNT_VIEW
where prem_sports = 0
and prem_movies = 2

--Premiums - Sky Base - Dual Sports & Single Movies
select count (account_number)
from vespa_analysts.SIG_SINGLE_ACCOUNT_VIEW
where prem_sports = 2
and prem_movies = 1

--Premiums - Sky Base - Dual Movies & Single Sports
select count (account_number)
from vespa_analysts.SIG_SINGLE_ACCOUNT_VIEW
where prem_sports = 1
and prem_movies = 2

--Premiums - Sky Base - Single Sports & Single Movies
select count (account_number)
from vespa_analysts.SIG_SINGLE_ACCOUNT_VIEW
where prem_sports = 1
and prem_movies = 1

--Premiums - Sky Base - No Prems
select count (account_number)
from vespa_analysts.SIG_SINGLE_ACCOUNT_VIEW
where prem_sports = 0
and prem_movies = 0
*/

-- AdSmartable Boxes + Personalisation Consent by Box Model - Sky Base
select count (Card_Subscriber_ID), box_model
from vespa_analysts.SIG_SINGLE_ACCOUNT_VIEW as SAV
inner join vespa_analysts.SIG_SINGLE_BOX_VIEW as SBV
on SAV.account_number = SBV.account_number
where viewing_consent_flag = 'Y'
and SBV.adsmart_flag = 1
group by box_model


-- AdSmartable Boxes + Personalisation Consent by Box Model - VESPA DP
select count (Card_Subscriber_ID), box_model
from vespa_analysts.SIG_SINGLE_ACCOUNT_VIEW as SAV
inner join vespa_analysts.SIG_SINGLE_BOX_VIEW as SBV
on SAV.account_number = SBV.account_number
where viewing_consent_flag = 'Y'
and SBV.adsmart_flag = 1
and SBV.panel = 'VESPA'
and SBV.status_vespa = 'Enabled'
group by box_model

-- AdSmartable Boxes + Personalisation Consent by Box Model - VESPA DP reporting
select count (Card_Subscriber_ID), box_model
from vespa_analysts.SIG_SINGLE_ACCOUNT_VIEW as SAV
inner join vespa_analysts.SIG_SINGLE_BOX_VIEW as SBV
on SAV.account_number = SBV.account_number
where viewing_consent_flag = 'Y'
and SBV.adsmart_flag = 1
and SBV.panel = 'VESPA'
and SBV.status_vespa = 'Enabled'
and SBV.Num_logs_sent_30d > 0
group by box_model

