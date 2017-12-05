/*-----------------------------------------------------------------------------------------------------------------
        Project:V250 - Surevy Analysis
        Version: 1
        Created: 2014-03-05
        
        Analyst: Dan Barnett
        SK Prod: 5

       Match Viewing Activity to Survey Data

*/------------------------------------------------------------------------------------------------------------------

--dbarnett.v250_sports_rights_survey_responses_winscp

---Create Account Level summary viewing activity that relates to questions asked in Survey---

--Need to aggregate rights data as questionnaire on more generic viewing e.g., UK Club Football
--than individual rights e.g., Premier League

---Add on if someone was asked follow up questions---
--grant all on dbarnett.v250_sports_rights_survey_responses_winscp to public;
---Ask Follow up Questions---
alter table dbarnett.v250_sports_rights_survey_responses_winscp add asked_follow_up_sports_rights_questions tinyint;

update dbarnett.v250_sports_rights_survey_responses_winscp 
set asked_follow_up_sports_rights_questions=case when q20_c ='' then 0 when q20_c in ('1','2','3','4','5') and q21 in ('Nobody in the household watches sports','Someone else is the main sports viewer in the household') then 0 else 1 end
from dbarnett.v250_sports_rights_survey_responses_winscp 
;
commit;
--select count(*) from dbarnett.v250_sports_rights_survey_responses_winscp  where q20_c =''


--drop table #individual_rights_totals_uk_football;
select a.account_number
,annualised_GPLSS_soc_programmes_a*GPLSS_right_broadcast_programmes as total_programmes_GPLSS
,annualised_FLSBBC_soc_programmes_a*FLSBBC_right_broadcast_programmes as total_programmes_FLSBBC
,annualised_FLCCSS_soc_programmes_a*FLCCSS_right_broadcast_programmes as total_programmes_FLCCSS
,annualised_FLOTSS_soc_programmes_a*FLOTSS_right_broadcast_programmes as total_programmes_FLOTSS
,annualised_MOTDBBC_soc_programmes_a*MOTDBBC_right_broadcast_programmes as total_programmes_MOTDBBC
,annualised_FACESPN_soc_programmes_a*FACESPN_right_broadcast_programmes as total_programmes_FACESPN
,annualised_FACITV_soc_programmes_a*FACITV_right_broadcast_programmes as total_programmes_FACITV
,annualised_SFASS_soc_programmes_a*SFASS_right_broadcast_programmes as total_programmes_SFASS
,annualised_SFLESPN_soc_programmes_a*SFLESPN_right_broadcast_programmes as total_programmes_SFLESPN
,annualised_SPFLBTS_soc_programmes_a*SPFLBTS_right_broadcast_programmes as total_programmes_SPFLBTS
,annualised_SPLESPN_soc_programmes_a*SPLESPN_right_broadcast_programmes as total_programmes_SPLESPN
,annualised_SPLSS_soc_programmes_a*SPLSS_right_broadcast_programmes as total_programmes_SPLSS
,annualised_CMSITV_soc_programmes_a*CMSITV_right_broadcast_programmes as total_programmes_CMSITV

,total_programmes_GPLSS
+total_programmes_FLSBBC
+total_programmes_FLCCSS
+total_programmes_FLOTSS
+total_programmes_MOTDBBC
+total_programmes_FACESPN
+total_programmes_FACITV
+total_programmes_SFASS
+total_programmes_SFLESPN
+total_programmes_SPFLBTS
+total_programmes_SPLESPN
+total_programmes_SPLSS
+total_programmes_CMSITV
as uk_football_programmes

,GPLSS_right_broadcast_programmes
+FLSBBC_right_broadcast_programmes
+FLCCSS_right_broadcast_programmes
+FLOTSS_right_broadcast_programmes
+MOTDBBC_right_broadcast_programmes
+FACESPN_right_broadcast_programmes
+FACITV_right_broadcast_programmes
+SFASS_right_broadcast_programmes
+SFLESPN_right_broadcast_programmes
+SPFLBTS_right_broadcast_programmes
+SPLESPN_right_broadcast_programmes
+SPLSS_right_broadcast_programmes
+CMSITV_right_broadcast_programmes
as uk_football_programmes_broadcast

,cast(uk_football_programmes as real)/cast(uk_football_programmes_broadcast as real) as proportion_programmes_uk_football

,annualised_GPLSS_soc_duration_a*GPLSS_right_broadcast_duration as total_duration_GPLSS
,annualised_FLSBBC_soc_duration_a*FLSBBC_right_broadcast_duration as total_duration_FLSBBC
,annualised_FLCCSS_soc_duration_a*FLCCSS_right_broadcast_duration as total_duration_FLCCSS
,annualised_FLOTSS_soc_duration_a*FLOTSS_right_broadcast_duration as total_duration_FLOTSS
,annualised_MOTDBBC_soc_duration_a*MOTDBBC_right_broadcast_duration as total_duration_MOTDBBC
,annualised_FACESPN_soc_duration_a*FACESPN_right_broadcast_duration as total_duration_FACESPN
,annualised_FACITV_soc_duration_a*FACITV_right_broadcast_duration as total_duration_FACITV
,annualised_SFASS_soc_duration_a*SFASS_right_broadcast_duration as total_duration_SFASS
,annualised_SFLESPN_soc_duration_a*SFLESPN_right_broadcast_duration as total_duration_SFLESPN
,annualised_SPFLBTS_soc_duration_a*SPFLBTS_right_broadcast_duration as total_duration_SPFLBTS
,annualised_SPLESPN_soc_duration_a*SPLESPN_right_broadcast_duration as total_duration_SPLESPN
,annualised_SPLSS_soc_duration_a*SPLSS_right_broadcast_duration as total_duration_SPLSS
,annualised_CMSITV_soc_duration_a*CMSITV_right_broadcast_duration as total_duration_CMSITV

,total_duration_GPLSS
+total_duration_FLSBBC
+total_duration_FLCCSS
+total_duration_FLOTSS
+total_duration_MOTDBBC
+total_duration_FACESPN
+total_duration_FACITV
+total_duration_SFASS
+total_duration_SFLESPN
+total_duration_SPFLBTS
+total_duration_SPLESPN
+total_duration_SPLSS
+total_duration_CMSITV
as uk_football_duration

,GPLSS_right_broadcast_duration
+FLSBBC_right_broadcast_duration
+FLCCSS_right_broadcast_duration
+FLOTSS_right_broadcast_duration
+MOTDBBC_right_broadcast_duration
+FACESPN_right_broadcast_duration
+FACITV_right_broadcast_duration
+SFASS_right_broadcast_duration
+SFLESPN_right_broadcast_duration
+SPFLBTS_right_broadcast_duration
+SPLESPN_right_broadcast_duration
+SPLSS_right_broadcast_duration
+CMSITV_right_broadcast_duration
as uk_football_duration_broadcast

,cast(uk_football_duration as real)/cast(uk_football_duration_broadcast as real) as proportion_duration_uk_football
,d.q26_g
,d.q20_c
,d.asked_follow_up_sports_rights_questions
,case when e.sports_premiums>0 then 1 else 0 end as sky_sports_subscriber
into #individual_rights_totals_uk_football
from dbarnett.v250_annualised_activity_table_final_v3 as a
left outer join dbarnett.v250_right_viewable_account_summary as c
on a.account_number = c.account_number
---Add in Survey details--
left outer join dbarnett.v250_sports_rights_survey_responses_winscp as d
on  a.account_number = d.ID_name
left outer join dbarnett.v250_Account_profiling as e
on  a.account_number = e.account_number


;
commit;
--grant all on dbarnett.v250_annualised_activity_table_final_v3 to public;
--grant all on dbarnett.v250_right_viewable_account_summary to public;
--grant all on dbarnett.v250_Account_profiling to public;

select case when proportion_programmes_uk_football=0 then -1  when floor (proportion_programmes_uk_football*100)<1 then 
floor (proportion_programmes_uk_football*1000)/10 else floor(proportion_programmes_uk_football*100) end as prop_programmes_uk_football
,sum(case when q26_g='1 - Definitely don''t want to watch' then 1 else 0 end) as response_01_dont_want_watch
,sum(case when q26_g='2 - Hardly ever watch' then 1 else 0 end) as response_02_hardly_ever_watch
,sum(case when q26_g='3 - Watch once in a while' then 1 else 0 end) as response_03_watch_once_in_a_while
,sum(case when q26_g='4 - If there is nothing better on' then 1 else 0 end) as response_04_watch_if_nothing_better_on
,sum(case when q26_g='5 - I watch the big events' then 1 else 0 end) as response_05_big_events
,sum(case when q26_g='6 - I watch when I can' then 1 else 0 end) as response_06_watch_when_I_can
,sum(case when q26_g='7 - Essential viewing' then 1 else 0 end) as response_07_essential_viewing
,count(*) as accounts
,sum(sky_sports_subscriber) as sky_sports_subscribers
into dbarnett.v250_individual_rights_totals_uk_football_programmes
from #individual_rights_totals_uk_football
where asked_follow_up_sports_rights_questions=1
group by prop_programmes_uk_football
order by prop_programmes_uk_football
;



select case when proportion_duration_uk_football=0 then -1  when floor (proportion_duration_uk_football*100)<1 then 
floor (proportion_duration_uk_football*1000)/10 else floor(proportion_duration_uk_football*100) end as prop_duration_uk_football
,sum(case when q26_g='1 - Definitely don''t want to watch' then 1 else 0 end) as response_01_dont_want_watch
,sum(case when q26_g='2 - Hardly ever watch' then 1 else 0 end) as response_02_hardly_ever_watch
,sum(case when q26_g='3 - Watch once in a while' then 1 else 0 end) as response_03_watch_once_in_a_while
,sum(case when q26_g='4 - If there is nothing better on' then 1 else 0 end) as response_04_watch_if_nothing_better_on
,sum(case when q26_g='5 - I watch the big events' then 1 else 0 end) as response_05_big_events
,sum(case when q26_g='6 - I watch when I can' then 1 else 0 end) as response_06_watch_when_I_can
,sum(case when q26_g='7 - Essential viewing' then 1 else 0 end) as response_07_essential_viewing
,count(*) as accounts
,sum(sky_sports_subscriber) as sky_sports_subscribers

into dbarnett.v250_individual_rights_totals_uk_football_duration
from #individual_rights_totals_uk_football
where asked_follow_up_sports_rights_questions=1
group by prop_duration_uk_football
order by prop_duration_uk_football
;
commit;

--select * from dbarnett.v250_individual_rights_totals_uk_football_programmes;
--select * from dbarnett.v250_individual_rights_totals_uk_football_duration;

/*
select case when proportion_programmes_uk_football=0 then -1  when floor (proportion_programmes_uk_football*100)<1 then 
floor (proportion_programmes_uk_football*1000)/10 else floor(proportion_programmes_uk_football*100) end as prop_programmes_uk_football
,q26_g
,count(*) as accounts
from #individual_rights_totals_uk_football
where asked_follow_up_sports_rights_questions=1
group by q26_g
,prop_programmes_uk_football
order by prop_programmes_uk_football
;

---Repeat for duration--

select case when proportion_duration_uk_football=0 then -1  when floor (proportion_duration_uk_football*100)<1 then 
floor (proportion_duration_uk_football*1000)/10 else floor(proportion_duration_uk_football*100) end as prop_duration_uk_football
,q26_g
,count(*) as accounts
from #individual_rights_totals_uk_football
where asked_follow_up_sports_rights_questions=1
group by q26_g
,prop_duration_uk_football
order by prop_duration_uk_football
;
*/

----Repeat process for Other Rights
--Euro Club Football--

--drop table  #individual_rights_totals_euro_football;
select a.account_number

,annualised_ELBTSP_soc_programmes_a*ELBTSP_right_broadcast_programmes as total_programmes_ELBTSP
,annualised_ELESPN_soc_programmes_a*ELESPN_right_broadcast_programmes as total_programmes_ELESPN
,annualised_ELITV_soc_programmes_a*ELITV_right_broadcast_programmes as total_programmes_ELITV
,annualised_BUNBTS_soc_programmes_a*BUNBTS_right_broadcast_programmes as total_programmes_BUNBTS
,annualised_BUNESPN_soc_programmes_a*BUNESPN_right_broadcast_programmes as total_programmes_BUNESPN
,annualised_SABTS_soc_programmes_a*SABTS_right_broadcast_programmes as total_programmes_SABTS
,annualised_SAESPN_soc_programmes_a*SAESPN_right_broadcast_programmes as total_programmes_SAESPN
,annualised_L1BTS_soc_programmes_a*L1BTS_right_broadcast_programmes as total_programmes_L1BTS
,annualised_L1ESPN_soc_programmes_a*L1ESPN_right_broadcast_programmes as total_programmes_L1ESPN
,annualised_CLASS_soc_programmes_a*CLASS_right_broadcast_programmes as total_programmes_CLASS
,annualised_CHLITV_soc_programmes_a*CHLITV_right_broadcast_programmes as total_programmes_CHLITV



,annualised_ELBTSP_soc_duration_a*ELBTSP_right_broadcast_duration as total_duration_ELBTSP
,annualised_ELESPN_soc_duration_a*ELESPN_right_broadcast_duration as total_duration_ELESPN
,annualised_ELITV_soc_duration_a*ELITV_right_broadcast_duration as total_duration_ELITV
,annualised_BUNBTS_soc_duration_a*BUNBTS_right_broadcast_duration as total_duration_BUNBTS
,annualised_BUNESPN_soc_duration_a*BUNESPN_right_broadcast_duration as total_duration_BUNESPN
,annualised_SABTS_soc_duration_a*SABTS_right_broadcast_duration as total_duration_SABTS
,annualised_SAESPN_soc_duration_a*SAESPN_right_broadcast_duration as total_duration_SAESPN
,annualised_L1BTS_soc_duration_a*L1BTS_right_broadcast_duration as total_duration_L1BTS
,annualised_L1ESPN_soc_duration_a*L1ESPN_right_broadcast_duration as total_duration_L1ESPN
,annualised_CLASS_soc_duration_a*CLASS_right_broadcast_duration as total_duration_CLASS
,annualised_CHLITV_soc_duration_a*CHLITV_right_broadcast_duration as total_duration_CHLITV



,total_programmes_ELBTSP
+total_programmes_ELESPN
+total_programmes_ELITV
+total_programmes_BUNBTS
+total_programmes_BUNESPN
+total_programmes_SABTS
+total_programmes_SAESPN
+total_programmes_L1BTS
+total_programmes_L1ESPN
+total_programmes_CLASS
+total_programmes_CHLITV

as euro_football_programmes

,ELBTSP_right_broadcast_programmes
+ELESPN_right_broadcast_programmes
+ELITV_right_broadcast_programmes
+BUNBTS_right_broadcast_programmes
+BUNESPN_right_broadcast_programmes
+SABTS_right_broadcast_programmes
+SAESPN_right_broadcast_programmes
+L1BTS_right_broadcast_programmes
+L1ESPN_right_broadcast_programmes
+CLASS_right_broadcast_programmes
+CHLITV_right_broadcast_programmes

as euro_football_programmes_broadcast

,cast(euro_football_programmes as real)/cast(euro_football_programmes_broadcast as real) as proportion_programmes_euro_football

,total_duration_ELBTSP
+total_duration_ELESPN
+total_duration_ELITV
+total_duration_BUNBTS
+total_duration_BUNESPN
+total_duration_SABTS
+total_duration_SAESPN
+total_duration_L1BTS
+total_duration_L1ESPN
+total_duration_CLASS
+total_duration_CHLITV

as euro_football_duration

,ELBTSP_right_broadcast_duration
+ELESPN_right_broadcast_duration
+ELITV_right_broadcast_duration
+BUNBTS_right_broadcast_duration
+BUNESPN_right_broadcast_duration
+SABTS_right_broadcast_duration
+SAESPN_right_broadcast_duration
+L1BTS_right_broadcast_duration
+L1ESPN_right_broadcast_duration
+CLASS_right_broadcast_duration
+CHLITV_right_broadcast_duration


as euro_football_duration_broadcast

,cast(euro_football_duration as real)/cast(euro_football_duration_broadcast as real) as proportion_duration_euro_football
,d.q26_h
,d.q20_c
,d.asked_follow_up_sports_rights_questions
,case when e.sports_premiums>0 then 1 else 0 end as sky_sports_subscriber
into #individual_rights_totals_euro_football
from dbarnett.v250_annualised_activity_table_final_v3 as a
left outer join dbarnett.v250_right_viewable_account_summary as c
on a.account_number = c.account_number
---Add in Survey details--
left outer join dbarnett.v250_sports_rights_survey_responses_winscp as d
on  a.account_number = d.ID_name
left outer join dbarnett.v250_Account_profiling as e
on  a.account_number = e.account_number
;
commit;
--drop table dbarnett.v250_individual_rights_totals_euro_football_programmes; drop table dbarnett.v250_individual_rights_totals_euro_football_duration;
--drop table dbarnett.v250_individual_rights_totals_euro_football_programmes;
select case when proportion_programmes_euro_football=0 then -1  when floor (proportion_programmes_euro_football*100)<1 then 
floor (proportion_programmes_euro_football*1000)/10 else floor(proportion_programmes_euro_football*100) end as prop_programmes_euro_football
,sum(case when q26_h='1 - Definitely don''t want to watch' then 1 else 0 end) as response_01_dont_want_watch
,sum(case when q26_h='2 - Hardly ever watch' then 1 else 0 end) as response_02_hardly_ever_watch
,sum(case when q26_h='3 - Watch once in a while' then 1 else 0 end) as response_03_watch_once_in_a_while
,sum(case when q26_h='4 - If there is nothing better on' then 1 else 0 end) as response_04_watch_if_nothing_better_on
,sum(case when q26_h='5 - I watch the big events' then 1 else 0 end) as response_05_big_events
,sum(case when q26_h='6 - I watch when I can' then 1 else 0 end) as response_06_watch_when_I_can
,sum(case when q26_h='7 - Essential viewing' then 1 else 0 end) as response_07_essential_viewing
,count(*) as accounts
,sum(sky_sports_subscriber) as sky_sports_subscribers

into dbarnett.v250_individual_rights_totals_euro_football_programmes
from #individual_rights_totals_euro_football
where asked_follow_up_sports_rights_questions=1
group by prop_programmes_euro_football
order by prop_programmes_euro_football
;

commit;

select case when proportion_duration_euro_football=0 then -1  when floor (proportion_duration_euro_football*100)<1 then 
floor (proportion_duration_euro_football*1000)/10 else floor(proportion_duration_euro_football*100) end as prop_duration_euro_football
,sum(case when q26_h='1 - Definitely don''t want to watch' then 1 else 0 end) as response_01_dont_want_watch
,sum(case when q26_h='2 - Hardly ever watch' then 1 else 0 end) as response_02_hardly_ever_watch
,sum(case when q26_h='3 - Watch once in a while' then 1 else 0 end) as response_03_watch_once_in_a_while
,sum(case when q26_h='4 - If there is nothing better on' then 1 else 0 end) as response_04_watch_if_nothing_better_on
,sum(case when q26_h='5 - I watch the big events' then 1 else 0 end) as response_05_big_events
,sum(case when q26_h='6 - I watch when I can' then 1 else 0 end) as response_06_watch_when_I_can
,sum(case when q26_h='7 - Essential viewing' then 1 else 0 end) as response_07_essential_viewing
,count(*) as accounts
,sum(sky_sports_subscriber) as sky_sports_subscribers

into dbarnett.v250_individual_rights_totals_euro_football_duration
from #individual_rights_totals_euro_football
where asked_follow_up_sports_rights_questions=1
group by prop_duration_euro_football
order by prop_duration_euro_football
;
commit;
--select * from dbarnett.v250_individual_rights_totals_euro_football_programmes;
--select * from dbarnett.v250_individual_rights_totals_euro_football_duration;


--International football
--drop table  #individual_rights_totals_international_football;
select a.account_number

,annualised_AFCEUR_soc_programmes_a*AFCEUR_right_broadcast_programmes as total_programmes_AFCEUR
,annualised_AFCITV_soc_programmes_a*AFCITV_right_broadcast_programmes as total_programmes_AFCITV
,annualised_GENGITV_soc_programmes_a*GENGITV_right_broadcast_programmes as total_programmes_GENGITV
,annualised_IFESPN_soc_programmes_a*IFESPN_right_broadcast_programmes as total_programmes_IFESPN
,annualised_IFBTS_soc_programmes_a*IFBTS_right_broadcast_programmes as total_programmes_IFBTS
,annualised_NIFSS_soc_programmes_a*NIFSS_right_broadcast_programmes as total_programmes_NIFSS
,annualised_SFASS_soc_programmes_a*SFASS_right_broadcast_programmes as total_programmes_SFASS
,annualised_ROISS_soc_programmes_a*ROISS_right_broadcast_programmes as total_programmes_ROISS
,annualised_SP5SS_soc_programmes_a*SP5SS_right_broadcast_programmes as total_programmes_SP5SS
,annualised_WIFSS_soc_programmes_a*WIFSS_right_broadcast_programmes as total_programmes_WIFSS




,annualised_AFCEUR_soc_duration_a*AFCEUR_right_broadcast_duration as total_duration_AFCEUR
,annualised_AFCITV_soc_duration_a*AFCITV_right_broadcast_duration as total_duration_AFCITV
,annualised_GENGITV_soc_duration_a*GENGITV_right_broadcast_duration as total_duration_GENGITV
,annualised_IFESPN_soc_duration_a*IFESPN_right_broadcast_duration as total_duration_IFESPN
,annualised_IFBTS_soc_duration_a*IFBTS_right_broadcast_duration as total_duration_IFBTS
,annualised_NIFSS_soc_duration_a*NIFSS_right_broadcast_duration as total_duration_NIFSS
,annualised_SFASS_soc_duration_a*SFASS_right_broadcast_duration as total_duration_SFASS
,annualised_ROISS_soc_duration_a*ROISS_right_broadcast_duration as total_duration_ROISS
,annualised_SP5SS_soc_duration_a*SP5SS_right_broadcast_duration as total_duration_SP5SS
,annualised_WIFSS_soc_duration_a*WIFSS_right_broadcast_duration as total_duration_WIFSS



,total_programmes_AFCEUR
+total_programmes_AFCITV
+total_programmes_GENGITV
+total_programmes_IFESPN
+total_programmes_IFBTS
+total_programmes_NIFSS
+total_programmes_SFASS
+total_programmes_ROISS
+total_programmes_SP5SS
+total_programmes_WIFSS


as international_football_programmes

,AFCEUR_right_broadcast_programmes
+AFCITV_right_broadcast_programmes
+GENGITV_right_broadcast_programmes
+IFESPN_right_broadcast_programmes
+IFBTS_right_broadcast_programmes
+NIFSS_right_broadcast_programmes
+SFASS_right_broadcast_programmes
+ROISS_right_broadcast_programmes
+SP5SS_right_broadcast_programmes
+WIFSS_right_broadcast_programmes


as international_football_programmes_broadcast

,cast(international_football_programmes as real)/cast(international_football_programmes_broadcast as real) as proportion_programmes_international_football

,total_duration_AFCEUR
+total_duration_AFCITV
+total_duration_GENGITV
+total_duration_IFESPN
+total_duration_IFBTS
+total_duration_NIFSS
+total_duration_SFASS
+total_duration_ROISS
+total_duration_SP5SS
+total_duration_WIFSS

as international_football_duration

,AFCEUR_right_broadcast_duration
+AFCITV_right_broadcast_duration
+GENGITV_right_broadcast_duration
+IFESPN_right_broadcast_duration
+IFBTS_right_broadcast_duration
+NIFSS_right_broadcast_duration
+SFASS_right_broadcast_duration
+ROISS_right_broadcast_duration
+SP5SS_right_broadcast_duration
+WIFSS_right_broadcast_duration

as international_football_duration_broadcast

,cast(international_football_duration as real)/cast(international_football_duration_broadcast as real) as proportion_duration_international_football
,d.q26_i
,d.q20_c
,d.asked_follow_up_sports_rights_questions
,case when e.sports_premiums>0 then 1 else 0 end as sky_sports_subscriber
into #individual_rights_totals_international_football
from dbarnett.v250_annualised_activity_table_final_v3 as a
left outer join dbarnett.v250_right_viewable_account_summary as c
on a.account_number = c.account_number
---Add in Survey details--
left outer join dbarnett.v250_sports_rights_survey_responses_winscp as d
on  a.account_number = d.ID_name
left outer join dbarnett.v250_Account_profiling as e
on  a.account_number = e.account_number
;
commit;

--drop table dbarnett.v250_individual_rights_totals_international_football_programmes;
select case when proportion_programmes_international_football=0 then -1  when floor (proportion_programmes_international_football*100)<1 then 
floor (proportion_programmes_international_football*1000)/10 else floor(proportion_programmes_international_football*100) end as prop_programmes_international_football
,sum(case when q26_i='1 - Definitely don''t want to watch' then 1 else 0 end) as response_01_dont_want_watch
,sum(case when q26_i='2 - Hardly ever watch' then 1 else 0 end) as response_02_hardly_ever_watch
,sum(case when q26_i='3 - Watch once in a while' then 1 else 0 end) as response_03_watch_once_in_a_while
,sum(case when q26_i='4 - If there is nothing better on' then 1 else 0 end) as response_04_watch_if_nothing_better_on
,sum(case when q26_i='5 - I watch the big events' then 1 else 0 end) as response_05_big_events
,sum(case when q26_i='6 - I watch when I can' then 1 else 0 end) as response_06_watch_when_I_can
,sum(case when q26_i='7 - Essential viewing' then 1 else 0 end) as response_07_essential_viewing
,count(*) as accounts
,sum(sky_sports_subscriber) as sky_sports_subscribers

into dbarnett.v250_individual_rights_totals_international_football_programmes
from #individual_rights_totals_international_football
where asked_follow_up_sports_rights_questions=1
group by prop_programmes_international_football
order by prop_programmes_international_football
;

commit;

select case when proportion_duration_international_football=0 then -1  when floor (proportion_duration_international_football*100)<1 then 
floor (proportion_duration_international_football*1000)/10 else floor(proportion_duration_international_football*100) end as prop_duration_international_football
,sum(case when q26_i='1 - Definitely don''t want to watch' then 1 else 0 end) as response_01_dont_want_watch
,sum(case when q26_i='2 - Hardly ever watch' then 1 else 0 end) as response_02_hardly_ever_watch
,sum(case when q26_i='3 - Watch once in a while' then 1 else 0 end) as response_03_watch_once_in_a_while
,sum(case when q26_i='4 - If there is nothing better on' then 1 else 0 end) as response_04_watch_if_nothing_better_on
,sum(case when q26_i='5 - I watch the big events' then 1 else 0 end) as response_05_big_events
,sum(case when q26_i='6 - I watch when I can' then 1 else 0 end) as response_06_watch_when_I_can
,sum(case when q26_i='7 - Essential viewing' then 1 else 0 end) as response_07_essential_viewing
,count(*) as accounts
,sum(sky_sports_subscriber) as sky_sports_subscribers

into dbarnett.v250_individual_rights_totals_international_football_duration
from #individual_rights_totals_international_football
where asked_follow_up_sports_rights_questions=1
group by prop_duration_international_football
order by prop_duration_international_football
;
commit;
--select * from dbarnett.v250_individual_rights_totals_international_football_programmes;
--select * from dbarnett.v250_individual_rights_totals_international_football_duration;


--select distinct q26_g from dbarnett.v250_sports_rights_survey_responses_winscp order by q26_g
--International Rugby
--drop table #individual_rights_totals_international_rugby;
select a.account_number
,annualised_ENRSS_soc_programmes_a*ENRSS_right_broadcast_programmes as total_programmes_ENRSS
,annualised_ORUGESPN_soc_programmes_a*ORUGESPN_right_broadcast_programmes as total_programmes_ORUGESPN
,annualised_RIEBBC_soc_programmes_a*RIEBBC_right_broadcast_programmes as total_programmes_RIEBBC
,annualised_RIIBBC_soc_programmes_a*RIIBBC_right_broadcast_programmes as total_programmes_RIIBBC
,annualised_RISBBC_soc_programmes_a*RISBBC_right_broadcast_programmes as total_programmes_RISBBC
,annualised_RIWBBC_soc_programmes_a*RIWBBC_right_broadcast_programmes as total_programmes_RIWBBC
,annualised_SNRBBC_soc_programmes_a*SNRBBC_right_broadcast_programmes as total_programmes_SNRBBC
,annualised_BILSS_soc_programmes_a*BILSS_right_broadcast_programmes as total_programmes_BILSS


,annualised_ENRSS_soc_duration_a*ENRSS_right_broadcast_duration as total_duration_ENRSS
,annualised_ORUGESPN_soc_duration_a*ORUGESPN_right_broadcast_duration as total_duration_ORUGESPN
,annualised_RIEBBC_soc_duration_a*RIEBBC_right_broadcast_duration as total_duration_RIEBBC
,annualised_RIIBBC_soc_duration_a*RIIBBC_right_broadcast_duration as total_duration_RIIBBC
,annualised_RISBBC_soc_duration_a*RISBBC_right_broadcast_duration as total_duration_RISBBC
,annualised_RIWBBC_soc_duration_a*RIWBBC_right_broadcast_duration as total_duration_RIWBBC
,annualised_SNRBBC_soc_duration_a*SNRBBC_right_broadcast_duration as total_duration_SNRBBC
,annualised_BILSS_soc_duration_a*BILSS_right_broadcast_duration as total_duration_BILSS

,total_programmes_ENRSS
+total_programmes_ORUGESPN
+total_programmes_RIEBBC
+total_programmes_RIIBBC
+total_programmes_RISBBC
+total_programmes_RIWBBC
+total_programmes_SNRBBC
+total_programmes_BILSS

as international_rugby_programmes

,ENRSS_right_broadcast_programmes
+ORUGESPN_right_broadcast_programmes
+RIEBBC_right_broadcast_programmes
+RIIBBC_right_broadcast_programmes
+RISBBC_right_broadcast_programmes
+RIWBBC_right_broadcast_programmes
+SNRBBC_right_broadcast_programmes
+BILSS_right_broadcast_programmes




as international_rugby_programmes_broadcast

,case when international_rugby_programmes_broadcast =0 then 0 else cast(international_rugby_programmes as real)/cast(international_rugby_programmes_broadcast as real) end as proportion_programmes_international_rugby

,total_duration_ENRSS
+total_duration_ORUGESPN
+total_duration_RIEBBC
+total_duration_RIIBBC
+total_duration_RISBBC
+total_duration_RIWBBC
+total_duration_SNRBBC
+total_duration_BILSS


as international_rugby_duration

,ENRSS_right_broadcast_duration
+ORUGESPN_right_broadcast_duration
+RIEBBC_right_broadcast_duration
+RIIBBC_right_broadcast_duration
+RISBBC_right_broadcast_duration
+RIWBBC_right_broadcast_duration
+SNRBBC_right_broadcast_duration
+BILSS_right_broadcast_duration


as international_rugby_duration_broadcast

,case when international_rugby_duration_broadcast =0 then 0 else cast(international_rugby_duration as real)/cast(international_rugby_duration_broadcast as real) end as proportion_duration_international_rugby
,d.q26_n
,d.q20_c
,d.asked_follow_up_sports_rights_questions
,case when e.sports_premiums>0 then 1 else 0 end as sky_sports_subscriber
into #individual_rights_totals_international_rugby
from dbarnett.v250_annualised_activity_table_final_v3 as a
left outer join dbarnett.v250_right_viewable_account_summary as c
on a.account_number = c.account_number
---Add in Survey details--
left outer join dbarnett.v250_sports_rights_survey_responses_winscp as d
on  a.account_number = d.ID_name
left outer join dbarnett.v250_Account_profiling as e
on  a.account_number = e.account_number
;
commit;

--select * from #individual_rights_totals_international_rugby;
--drop table dbarnett.v250_individual_rights_totals_international_rugby_programmes;
select case when proportion_programmes_international_rugby=0 then -1  when floor (proportion_programmes_international_rugby*100)<1 then 
floor (proportion_programmes_international_rugby*1000)/10 else floor(proportion_programmes_international_rugby*100) end as prop_programmes_international_rugby
,sum(case when q26_n='1 - Definitely don''t want to watch' then 1 else 0 end) as response_01_dont_want_watch
,sum(case when q26_n='2 - Hardly ever watch' then 1 else 0 end) as response_02_hardly_ever_watch
,sum(case when q26_n='3 - Watch once in a while' then 1 else 0 end) as response_03_watch_once_in_a_while
,sum(case when q26_n='4 - If there is nothing better on' then 1 else 0 end) as response_04_watch_if_nothing_better_on
,sum(case when q26_n='5 - I watch the big events' then 1 else 0 end) as response_05_big_events
,sum(case when q26_n='6 - I watch when I can' then 1 else 0 end) as response_06_watch_when_I_can
,sum(case when q26_n='7 - Essential viewing' then 1 else 0 end) as response_07_essential_viewing
,count(*) as accounts
,sum(sky_sports_subscriber) as sky_sports_subscribers

into dbarnett.v250_individual_rights_totals_international_rugby_programmes
from #individual_rights_totals_international_rugby
where asked_follow_up_sports_rights_questions=1
group by prop_programmes_international_rugby
order by prop_programmes_international_rugby
;

commit;

select case when proportion_duration_international_rugby=0 then -1  when floor (proportion_duration_international_rugby*100)<1 then 
floor (proportion_duration_international_rugby*1000)/10 else floor(proportion_duration_international_rugby*100) end as prop_duration_international_rugby
,sum(case when q26_n='1 - Definitely don''t want to watch' then 1 else 0 end) as response_01_dont_want_watch
,sum(case when q26_n='2 - Hardly ever watch' then 1 else 0 end) as response_02_hardly_ever_watch
,sum(case when q26_n='3 - Watch once in a while' then 1 else 0 end) as response_03_watch_once_in_a_while
,sum(case when q26_n='4 - If there is nothing better on' then 1 else 0 end) as response_04_watch_if_nothing_better_on
,sum(case when q26_n='5 - I watch the big events' then 1 else 0 end) as response_05_big_events
,sum(case when q26_n='6 - I watch when I can' then 1 else 0 end) as response_06_watch_when_I_can
,sum(case when q26_n='7 - Essential viewing' then 1 else 0 end) as response_07_essential_viewing
,count(*) as accounts
,sum(sky_sports_subscriber) as sky_sports_subscribers

into dbarnett.v250_individual_rights_totals_international_rugby_duration
from #individual_rights_totals_international_rugby
where asked_follow_up_sports_rights_questions=1
group by prop_duration_international_rugby
order by prop_duration_international_rugby
;
commit;

--select * from dbarnett.v250_individual_rights_totals_international_rugby_programmes;
--select * from dbarnett.v250_individual_rights_totals_international_rugby_duration;


--drop table #individual_rights_totals_cricket;
select a.account_number
,annualised_AHCSS_soc_programmes_a*AHCSS_right_broadcast_programmes as total_programmes_AHCSS
,annualised_ICCSS_soc_programmes_a*ICCSS_right_broadcast_programmes as total_programmes_ICCSS
,annualised_ECBHCH5_soc_programmes_a*ECBHCH5_right_broadcast_programmes as total_programmes_ECBHCH5
,annualised_GECRSS_soc_programmes_a*GECRSS_right_broadcast_programmes as total_programmes_GECRSS
,annualised_IHCSS_soc_programmes_a*IHCSS_right_broadcast_programmes as total_programmes_IHCSS
,annualised_SACSS_soc_programmes_a*SACSS_right_broadcast_programmes as total_programmes_SACSS
,annualised_WICCSS_soc_programmes_a*WICCSS_right_broadcast_programmes as total_programmes_WICCSS
,annualised_IPLITV_soc_programmes_a*IPLITV_right_broadcast_programmes as total_programmes_IPLITV






,annualised_AHCSS_soc_duration_a*AHCSS_right_broadcast_duration as total_duration_AHCSS
,annualised_ICCSS_soc_duration_a*ICCSS_right_broadcast_duration as total_duration_ICCSS
,annualised_ECBHCH5_soc_duration_a*ECBHCH5_right_broadcast_duration as total_duration_ECBHCH5
,annualised_GECRSS_soc_duration_a*GECRSS_right_broadcast_duration as total_duration_GECRSS
,annualised_IHCSS_soc_duration_a*IHCSS_right_broadcast_duration as total_duration_IHCSS
,annualised_SACSS_soc_duration_a*SACSS_right_broadcast_duration as total_duration_SACSS
,annualised_WICCSS_soc_duration_a*WICCSS_right_broadcast_duration as total_duration_WICCSS
,annualised_IPLITV_soc_duration_a*IPLITV_right_broadcast_duration as total_duration_IPLITV

,total_programmes_AHCSS
+total_programmes_ICCSS
+total_programmes_ECBHCH5
+total_programmes_GECRSS
+total_programmes_IHCSS
+total_programmes_SACSS
+total_programmes_WICCSS
+total_programmes_IPLITV


as cricket_programmes

,AHCSS_right_broadcast_programmes
+ICCSS_right_broadcast_programmes
+ECBHCH5_right_broadcast_programmes
+GECRSS_right_broadcast_programmes
+IHCSS_right_broadcast_programmes
+SACSS_right_broadcast_programmes
+WICCSS_right_broadcast_programmes
+IPLITV_right_broadcast_programmes





as cricket_programmes_broadcast

,case when cricket_programmes_broadcast =0 then 0 else cast(cricket_programmes as real)/cast(cricket_programmes_broadcast as real) end as proportion_programmes_cricket

,total_duration_AHCSS
+total_duration_ICCSS
+total_duration_ECBHCH5
+total_duration_GECRSS
+total_duration_IHCSS
+total_duration_SACSS
+total_duration_WICCSS
+total_duration_IPLITV



as cricket_duration

,AHCSS_right_broadcast_duration
+ICCSS_right_broadcast_duration
+ECBHCH5_right_broadcast_duration
+GECRSS_right_broadcast_duration
+IHCSS_right_broadcast_duration
+SACSS_right_broadcast_duration
+WICCSS_right_broadcast_duration
+IPLITV_right_broadcast_duration



as cricket_duration_broadcast

,case when cricket_duration_broadcast =0 then 0 else cast(cricket_duration as real)/cast(cricket_duration_broadcast as real) end as proportion_duration_cricket
,d.q26_c
,d.q20_c
,d.asked_follow_up_sports_rights_questions
,case when e.sports_premiums>0 then 1 else 0 end as sky_sports_subscriber
into #individual_rights_totals_cricket
from dbarnett.v250_annualised_activity_table_final_v3 as a
left outer join dbarnett.v250_right_viewable_account_summary as c
on a.account_number = c.account_number
---Add in Survey details--
left outer join dbarnett.v250_sports_rights_survey_responses_winscp as d
on  a.account_number = d.ID_name
left outer join dbarnett.v250_Account_profiling as e
on  a.account_number = e.account_number
;
commit;
--select * from #individual_rights_totals_cricket;
--drop table dbarnett.v250_individual_rights_totals_cricket_programmes;
select case when proportion_programmes_cricket=0 then -1  when floor (proportion_programmes_cricket*100)<1 then 
floor (proportion_programmes_cricket*1000)/10 else floor(proportion_programmes_cricket*100) end as prop_programmes_cricket
,sum(case when q26_c='1 - Definitely don''t want to watch' then 1 else 0 end) as response_01_dont_want_watch
,sum(case when q26_c='2 - Hardly ever watch' then 1 else 0 end) as response_02_hardly_ever_watch
,sum(case when q26_c='3 - Watch once in a while' then 1 else 0 end) as response_03_watch_once_in_a_while
,sum(case when q26_c='4 - If there is nothing better on' then 1 else 0 end) as response_04_watch_if_nothing_better_on
,sum(case when q26_c='5 - I watch the big events' then 1 else 0 end) as response_05_big_events
,sum(case when q26_c='6 - I watch when I can' then 1 else 0 end) as response_06_watch_when_I_can
,sum(case when q26_c='7 - Essential viewing' then 1 else 0 end) as response_07_essential_viewing
,count(*) as accounts
,sum(sky_sports_subscriber) as sky_sports_subscribers

into dbarnett.v250_individual_rights_totals_cricket_programmes
from #individual_rights_totals_cricket
where asked_follow_up_sports_rights_questions=1
group by prop_programmes_cricket
order by prop_programmes_cricket
;

commit;

select case when proportion_duration_cricket=0 then -1  when floor (proportion_duration_cricket*100)<1 then 
floor (proportion_duration_cricket*1000)/10 else floor(proportion_duration_cricket*100) end as prop_duration_cricket
,sum(case when q26_c='1 - Definitely don''t want to watch' then 1 else 0 end) as response_01_dont_want_watch
,sum(case when q26_c='2 - Hardly ever watch' then 1 else 0 end) as response_02_hardly_ever_watch
,sum(case when q26_c='3 - Watch once in a while' then 1 else 0 end) as response_03_watch_once_in_a_while
,sum(case when q26_c='4 - If there is nothing better on' then 1 else 0 end) as response_04_watch_if_nothing_better_on
,sum(case when q26_c='5 - I watch the big events' then 1 else 0 end) as response_05_big_events
,sum(case when q26_c='6 - I watch when I can' then 1 else 0 end) as response_06_watch_when_I_can
,sum(case when q26_c='7 - Essential viewing' then 1 else 0 end) as response_07_essential_viewing
,count(*) as accounts
,sum(sky_sports_subscriber) as sky_sports_subscribers

into dbarnett.v250_individual_rights_totals_cricket_duration
from #individual_rights_totals_cricket
where asked_follow_up_sports_rights_questions=1
group by prop_duration_cricket
order by prop_duration_cricket
;
commit;

--select * from dbarnett.v250_individual_rights_totals_cricket_programmes;
--select * from dbarnett.v250_individual_rights_totals_cricket_duration;


--drop table #individual_rights_totals_motor_sport;
select a.account_number
,annualised_GF1BBC_soc_programmes_a*GF1BBC_right_broadcast_programmes as total_programmes_GF1BBC
,annualised_GF1SS_soc_programmes_a*GF1SS_right_broadcast_programmes as total_programmes_GF1SS
,annualised_MGPBBC_soc_programmes_a*MGPBBC_right_broadcast_programmes as total_programmes_MGPBBC


,annualised_GF1BBC_soc_duration_a*GF1BBC_right_broadcast_duration as total_duration_GF1BBC
,annualised_GF1SS_soc_duration_a*GF1SS_right_broadcast_duration as total_duration_GF1SS
,annualised_MGPBBC_soc_duration_a*MGPBBC_right_broadcast_duration as total_duration_MGPBBC

,total_programmes_GF1BBC
+total_programmes_GF1SS
+total_programmes_MGPBBC



as motor_sport_programmes

,GF1BBC_right_broadcast_programmes
+GF1SS_right_broadcast_programmes
+MGPBBC_right_broadcast_programmes

as motor_sport_programmes_broadcast

,case when motor_sport_programmes_broadcast =0 then 0 else cast(motor_sport_programmes as real)/cast(motor_sport_programmes_broadcast as real) end as proportion_programmes_motor_sport

,total_duration_GF1BBC
+total_duration_GF1SS
+total_duration_MGPBBC

as motor_sport_duration

,GF1BBC_right_broadcast_duration
+GF1SS_right_broadcast_duration
+MGPBBC_right_broadcast_duration

as motor_sport_duration_broadcast

,case when motor_sport_duration_broadcast =0 then 0 else cast(motor_sport_duration as real)/cast(motor_sport_duration_broadcast as real) end as proportion_duration_motor_sport
,d.q26_l
,d.q20_c
,d.asked_follow_up_sports_rights_questions
,case when e.sports_premiums>0 then 1 else 0 end as sky_sports_subscriber
into #individual_rights_totals_motor_sport
from dbarnett.v250_annualised_activity_table_final_v3 as a
left outer join dbarnett.v250_right_viewable_account_summary as c
on a.account_number = c.account_number
---Add in Survey details--
left outer join dbarnett.v250_sports_rights_survey_responses_winscp as d
on  a.account_number = d.ID_name
left outer join dbarnett.v250_Account_profiling as e
on  a.account_number = e.account_number
;
commit;
--select * from #individual_rights_totals_motor_sport;
--drop table dbarnett.v250_individual_rights_totals_motor_sport_programmes;
select case when proportion_programmes_motor_sport=0 then -1  when floor (proportion_programmes_motor_sport*100)<1 then 
floor (proportion_programmes_motor_sport*1000)/10 else floor(proportion_programmes_motor_sport*100) end as prop_programmes_motor_sport
,sum(case when q26_l='1 - Definitely don''t want to watch' then 1 else 0 end) as response_01_dont_want_watch
,sum(case when q26_l='2 - Hardly ever watch' then 1 else 0 end) as response_02_hardly_ever_watch
,sum(case when q26_l='3 - Watch once in a while' then 1 else 0 end) as response_03_watch_once_in_a_while
,sum(case when q26_l='4 - If there is nothing better on' then 1 else 0 end) as response_04_watch_if_nothing_better_on
,sum(case when q26_l='5 - I watch the big events' then 1 else 0 end) as response_05_big_events
,sum(case when q26_l='6 - I watch when I can' then 1 else 0 end) as response_06_watch_when_I_can
,sum(case when q26_l='7 - Essential viewing' then 1 else 0 end) as response_07_essential_viewing
,count(*) as accounts
,sum(sky_sports_subscriber) as sky_sports_subscribers

into dbarnett.v250_individual_rights_totals_motor_sport_programmes
from #individual_rights_totals_motor_sport
where asked_follow_up_sports_rights_questions=1
group by prop_programmes_motor_sport
order by prop_programmes_motor_sport
;

commit;

select case when proportion_duration_motor_sport=0 then -1  when floor (proportion_duration_motor_sport*100)<1 then 
floor (proportion_duration_motor_sport*1000)/10 else floor(proportion_duration_motor_sport*100) end as prop_duration_motor_sport
,sum(case when q26_l='1 - Definitely don''t want to watch' then 1 else 0 end) as response_01_dont_want_watch
,sum(case when q26_l='2 - Hardly ever watch' then 1 else 0 end) as response_02_hardly_ever_watch
,sum(case when q26_l='3 - Watch once in a while' then 1 else 0 end) as response_03_watch_once_in_a_while
,sum(case when q26_l='4 - If there is nothing better on' then 1 else 0 end) as response_04_watch_if_nothing_better_on
,sum(case when q26_l='5 - I watch the big events' then 1 else 0 end) as response_05_big_events
,sum(case when q26_l='6 - I watch when I can' then 1 else 0 end) as response_06_watch_when_I_can
,sum(case when q26_l='7 - Essential viewing' then 1 else 0 end) as response_07_essential_viewing
,count(*) as accounts
,sum(sky_sports_subscriber) as sky_sports_subscribers

into dbarnett.v250_individual_rights_totals_motor_sport_duration
from #individual_rights_totals_motor_sport
where asked_follow_up_sports_rights_questions=1
group by prop_duration_motor_sport
order by prop_duration_motor_sport
;
commit;

--select * from dbarnett.v250_individual_rights_totals_motor_sport_programmes;
--select * from dbarnett.v250_individual_rights_totals_motor_sport_duration;



--drop table #individual_rights_totals_golf;
select a.account_number
,annualised_ATGSS_soc_programmes_a*ATGSS_right_broadcast_programmes as total_programmes_ATGSS
,annualised_BOGSS_soc_programmes_a*BOGSS_right_broadcast_programmes as total_programmes_BOGSS
,annualised_ETGSS_soc_programmes_a*ETGSS_right_broadcast_programmes as total_programmes_ETGSS
,annualised_PGASS_soc_programmes_a*PGASS_right_broadcast_programmes as total_programmes_PGASS
,annualised_SOLSS_soc_programmes_a*SOLSS_right_broadcast_programmes as total_programmes_SOLSS
,annualised_USMGSS_soc_programmes_a*USMGSS_right_broadcast_programmes as total_programmes_USMGSS
,annualised_USOGSS_soc_programmes_a*USOGSS_right_broadcast_programmes as total_programmes_USOGSS
,annualised_USPGASS_soc_programmes_a*USPGASS_right_broadcast_programmes as total_programmes_USPGASS
,annualised_MGBBC_soc_programmes_a*MGBBC_right_broadcast_programmes as total_programmes_MGBBC





,annualised_ATGSS_soc_duration_a*ATGSS_right_broadcast_duration as total_duration_ATGSS
,annualised_BOGSS_soc_duration_a*BOGSS_right_broadcast_duration as total_duration_BOGSS
,annualised_ETGSS_soc_duration_a*ETGSS_right_broadcast_duration as total_duration_ETGSS
,annualised_PGASS_soc_duration_a*PGASS_right_broadcast_duration as total_duration_PGASS
,annualised_SOLSS_soc_duration_a*SOLSS_right_broadcast_duration as total_duration_SOLSS
,annualised_USMGSS_soc_duration_a*USMGSS_right_broadcast_duration as total_duration_USMGSS
,annualised_USOGSS_soc_duration_a*USOGSS_right_broadcast_duration as total_duration_USOGSS
,annualised_USPGASS_soc_duration_a*USPGASS_right_broadcast_duration as total_duration_USPGASS
,annualised_MGBBC_soc_duration_a*MGBBC_right_broadcast_duration as total_duration_MGBBC


,total_programmes_ATGSS
+total_programmes_BOGSS
+total_programmes_ETGSS
+total_programmes_PGASS
+total_programmes_SOLSS
+total_programmes_USMGSS
+total_programmes_USOGSS
+total_programmes_USPGASS
+total_programmes_MGBBC




as golf_programmes

,ATGSS_right_broadcast_programmes
+BOGSS_right_broadcast_programmes
+ETGSS_right_broadcast_programmes
+PGASS_right_broadcast_programmes
+SOLSS_right_broadcast_programmes
+USMGSS_right_broadcast_programmes
+USOGSS_right_broadcast_programmes
+USPGASS_right_broadcast_programmes
+MGBBC_right_broadcast_programmes


as golf_programmes_broadcast

,case when golf_programmes_broadcast =0 then 0 else cast(golf_programmes as real)/cast(golf_programmes_broadcast as real) end as proportion_programmes_golf

,total_duration_ATGSS
+total_duration_BOGSS
+total_duration_ETGSS
+total_duration_PGASS
+total_duration_SOLSS
+total_duration_USMGSS
+total_duration_USOGSS
+total_duration_USPGASS
+total_duration_MGBBC


as golf_duration

,ATGSS_right_broadcast_duration
+BOGSS_right_broadcast_duration
+ETGSS_right_broadcast_duration
+PGASS_right_broadcast_duration
+SOLSS_right_broadcast_duration
+USMGSS_right_broadcast_duration
+USOGSS_right_broadcast_duration
+USPGASS_right_broadcast_duration
+MGBBC_right_broadcast_duration


as golf_duration_broadcast

,case when golf_duration_broadcast =0 then 0 else cast(golf_duration as real)/cast(golf_duration_broadcast as real) end as proportion_duration_golf
,d.q26_j
,d.q20_c
,d.asked_follow_up_sports_rights_questions
,case when e.sports_premiums>0 then 1 else 0 end as sky_sports_subscriber
into #individual_rights_totals_golf
from dbarnett.v250_annualised_activity_table_final_v3 as a
left outer join dbarnett.v250_right_viewable_account_summary as c
on a.account_number = c.account_number
---Add in Survey details--
left outer join dbarnett.v250_sports_rights_survey_responses_winscp as d
on  a.account_number = d.ID_name
left outer join dbarnett.v250_Account_profiling as e
on  a.account_number = e.account_number
;
commit;
--select * from #individual_rights_totals_golf;
--drop table dbarnett.v250_individual_rights_totals_golf_programmes;
select case when proportion_programmes_golf=0 then -1  when floor (proportion_programmes_golf*100)<1 then 
floor (proportion_programmes_golf*1000)/10 else floor(proportion_programmes_golf*100) end as prop_programmes_golf
,sum(case when q26_j='1 - Definitely don''t want to watch' then 1 else 0 end) as response_01_dont_want_watch
,sum(case when q26_j='2 - Hardly ever watch' then 1 else 0 end) as response_02_hardly_ever_watch
,sum(case when q26_j='3 - Watch once in a while' then 1 else 0 end) as response_03_watch_once_in_a_while
,sum(case when q26_j='4 - If there is nothing better on' then 1 else 0 end) as response_04_watch_if_nothing_better_on
,sum(case when q26_j='5 - I watch the big events' then 1 else 0 end) as response_05_big_events
,sum(case when q26_j='6 - I watch when I can' then 1 else 0 end) as response_06_watch_when_I_can
,sum(case when q26_j='7 - Essential viewing' then 1 else 0 end) as response_07_essential_viewing
,count(*) as accounts
,sum(sky_sports_subscriber) as sky_sports_subscribers

into dbarnett.v250_individual_rights_totals_golf_programmes
from #individual_rights_totals_golf
where asked_follow_up_sports_rights_questions=1
group by prop_programmes_golf
order by prop_programmes_golf
;

commit;

select case when proportion_duration_golf=0 then -1  when floor (proportion_duration_golf*100)<1 then 
floor (proportion_duration_golf*1000)/10 else floor(proportion_duration_golf*100) end as prop_duration_golf
,sum(case when q26_j='1 - Definitely don''t want to watch' then 1 else 0 end) as response_01_dont_want_watch
,sum(case when q26_j='2 - Hardly ever watch' then 1 else 0 end) as response_02_hardly_ever_watch
,sum(case when q26_j='3 - Watch once in a while' then 1 else 0 end) as response_03_watch_once_in_a_while
,sum(case when q26_j='4 - If there is nothing better on' then 1 else 0 end) as response_04_watch_if_nothing_better_on
,sum(case when q26_j='5 - I watch the big events' then 1 else 0 end) as response_05_big_events
,sum(case when q26_j='6 - I watch when I can' then 1 else 0 end) as response_06_watch_when_I_can
,sum(case when q26_j='7 - Essential viewing' then 1 else 0 end) as response_07_essential_viewing
,count(*) as accounts
,sum(sky_sports_subscriber) as sky_sports_subscribers

into dbarnett.v250_individual_rights_totals_golf_duration
from #individual_rights_totals_golf
where asked_follow_up_sports_rights_questions=1
group by prop_duration_golf
order by prop_duration_golf
;
commit;

--select * from dbarnett.v250_individual_rights_totals_golf_programmes;
--select * from dbarnett.v250_individual_rights_totals_golf_duration;


---#start

--drop table #individual_rights_totals_athletics;
select a.account_number
,annualised_WACEUR_soc_programmes_a*WACEUR_right_broadcast_programmes as total_programmes_WACEUR
,annualised_WACMR4_soc_programmes_a*WACMR4_right_broadcast_programmes as total_programmes_WACMR4


,annualised_WACEUR_soc_duration_a*WACEUR_right_broadcast_duration as total_duration_WACEUR
,annualised_WACMR4_soc_duration_a*WACMR4_right_broadcast_duration as total_duration_WACMR4


,total_programmes_WACEUR
+total_programmes_WACMR4

as athletics_programmes

,WACEUR_right_broadcast_programmes
+WACMR4_right_broadcast_programmes

as athletics_programmes_broadcast

,case when athletics_programmes_broadcast =0 then 0 else cast(athletics_programmes as real)/cast(athletics_programmes_broadcast as real) end as proportion_programmes_athletics

,total_duration_WACEUR
+total_duration_WACMR4


as athletics_duration

,WACEUR_right_broadcast_duration
+WACMR4_right_broadcast_duration

as athletics_duration_broadcast

,case when athletics_duration_broadcast =0 then 0 else cast(athletics_duration as real)/cast(athletics_duration_broadcast as real) end as proportion_duration_athletics
,d.q26_b
,d.q20_c
,d.asked_follow_up_sports_rights_questions
,case when e.sports_premiums>0 then 1 else 0 end as sky_sports_subscriber
into #individual_rights_totals_athletics
from dbarnett.v250_annualised_activity_table_final_v3 as a
left outer join dbarnett.v250_right_viewable_account_summary as c
on a.account_number = c.account_number
---Add in Survey details--
left outer join dbarnett.v250_sports_rights_survey_responses_winscp as d
on  a.account_number = d.ID_name
left outer join dbarnett.v250_Account_profiling as e
on  a.account_number = e.account_number
;
commit;
--select * from #individual_rights_totals_athletics;
--drop table dbarnett.v250_individual_rights_totals_athletics_programmes;
select case when proportion_programmes_athletics=0 then -1  when floor (proportion_programmes_athletics*100)<1 then 
floor (proportion_programmes_athletics*1000)/10 else floor(proportion_programmes_athletics*100) end as prop_programmes_athletics
,sum(case when q26_b='1 - Definitely don''t want to watch' then 1 else 0 end) as response_01_dont_want_watch
,sum(case when q26_b='2 - Hardly ever watch' then 1 else 0 end) as response_02_hardly_ever_watch
,sum(case when q26_b='3 - Watch once in a while' then 1 else 0 end) as response_03_watch_once_in_a_while
,sum(case when q26_b='4 - If there is nothing better on' then 1 else 0 end) as response_04_watch_if_nothing_better_on
,sum(case when q26_b='5 - I watch the big events' then 1 else 0 end) as response_05_big_events
,sum(case when q26_b='6 - I watch when I can' then 1 else 0 end) as response_06_watch_when_I_can
,sum(case when q26_b='7 - Essential viewing' then 1 else 0 end) as response_07_essential_viewing
,count(*) as accounts
,sum(sky_sports_subscriber) as sky_sports_subscribers

into dbarnett.v250_individual_rights_totals_athletics_programmes
from #individual_rights_totals_athletics
where asked_follow_up_sports_rights_questions=1
group by prop_programmes_athletics
order by prop_programmes_athletics
;

commit;

select case when proportion_duration_athletics=0 then -1  when floor (proportion_duration_athletics*100)<1 then 
floor (proportion_duration_athletics*1000)/10 else floor(proportion_duration_athletics*100) end as prop_duration_athletics
,sum(case when q26_b='1 - Definitely don''t want to watch' then 1 else 0 end) as response_01_dont_want_watch
,sum(case when q26_b='2 - Hardly ever watch' then 1 else 0 end) as response_02_hardly_ever_watch
,sum(case when q26_b='3 - Watch once in a while' then 1 else 0 end) as response_03_watch_once_in_a_while
,sum(case when q26_b='4 - If there is nothing better on' then 1 else 0 end) as response_04_watch_if_nothing_better_on
,sum(case when q26_b='5 - I watch the big events' then 1 else 0 end) as response_05_big_events
,sum(case when q26_b='6 - I watch when I can' then 1 else 0 end) as response_06_watch_when_I_can
,sum(case when q26_b='7 - Essential viewing' then 1 else 0 end) as response_07_essential_viewing
,count(*) as accounts
,sum(sky_sports_subscriber) as sky_sports_subscribers

into dbarnett.v250_individual_rights_totals_athletics_duration
from #individual_rights_totals_athletics
where asked_follow_up_sports_rights_questions=1
group by prop_duration_athletics
order by prop_duration_athletics
;
commit;

--select * from dbarnett.v250_individual_rights_totals_athletics_programmes;
--select * from dbarnett.v250_individual_rights_totals_athletics_duration;





--drop table #individual_rights_totals_club_rugby;
select a.account_number
,annualised_HECSS_soc_programmes_a*HECSS_right_broadcast_programmes as total_programmes_HECSS
,annualised_AVPSS_soc_programmes_a*AVPSS_right_broadcast_programmes as total_programmes_AVPSS
,annualised_RLCCBBC_soc_programmes_a*RLCCBBC_right_broadcast_programmes as total_programmes_RLCCBBC
,annualised_RLGSS_soc_programmes_a*RLGSS_right_broadcast_programmes as total_programmes_RLGSS

,annualised_HECSS_soc_duration_a*HECSS_right_broadcast_duration as total_duration_HECSS
,annualised_AVPSS_soc_duration_a*AVPSS_right_broadcast_duration as total_duration_AVPSS
,annualised_RLCCBBC_soc_duration_a*RLCCBBC_right_broadcast_duration as total_duration_RLCCBBC
,annualised_RLGSS_soc_duration_a*RLGSS_right_broadcast_duration as total_duration_RLGSS

,total_programmes_HECSS
+total_programmes_AVPSS
+total_programmes_RLCCBBC
+total_programmes_RLGSS

as club_rugby_programmes

,HECSS_right_broadcast_programmes
+AVPSS_right_broadcast_programmes
+RLCCBBC_right_broadcast_programmes
+RLGSS_right_broadcast_programmes

as club_rugby_programmes_broadcast

,case when club_rugby_programmes_broadcast =0 then 0 else cast(club_rugby_programmes as real)/cast(club_rugby_programmes_broadcast as real) end as proportion_programmes_club_rugby

,total_duration_HECSS
+total_duration_AVPSS
+total_duration_RLCCBBC
+total_duration_RLGSS


as club_rugby_duration

,HECSS_right_broadcast_duration
+AVPSS_right_broadcast_duration
+RLCCBBC_right_broadcast_duration
+RLGSS_right_broadcast_duration



as club_rugby_duration_broadcast

,case when club_rugby_duration_broadcast =0 then 0 else cast(club_rugby_duration as real)/cast(club_rugby_duration_broadcast as real) end as proportion_duration_club_rugby
,d.q26_m
,d.q20_c
,d.asked_follow_up_sports_rights_questions
,case when e.sports_premiums>0 then 1 else 0 end as sky_sports_subscriber
into #individual_rights_totals_club_rugby
from dbarnett.v250_annualised_activity_table_final_v3 as a
left outer join dbarnett.v250_right_viewable_account_summary as c
on a.account_number = c.account_number
---Add in Survey details--
left outer join dbarnett.v250_sports_rights_survey_responses_winscp as d
on  a.account_number = d.ID_name
left outer join dbarnett.v250_Account_profiling as e
on  a.account_number = e.account_number
;
commit;
--select * from #individual_rights_totals_club_rugby;
--drop table dbarnett.v250_individual_rights_totals_club_rugby_programmes;
select case when proportion_programmes_club_rugby=0 then -1  when floor (proportion_programmes_club_rugby*100)<1 then 
floor (proportion_programmes_club_rugby*1000)/10 else floor(proportion_programmes_club_rugby*100) end as prop_programmes_club_rugby
,sum(case when q26_m='1 - Definitely don''t want to watch' then 1 else 0 end) as response_01_dont_want_watch
,sum(case when q26_m='2 - Hardly ever watch' then 1 else 0 end) as response_02_hardly_ever_watch
,sum(case when q26_m='3 - Watch once in a while' then 1 else 0 end) as response_03_watch_once_in_a_while
,sum(case when q26_m='4 - If there is nothing better on' then 1 else 0 end) as response_04_watch_if_nothing_better_on
,sum(case when q26_m='5 - I watch the big events' then 1 else 0 end) as response_05_big_events
,sum(case when q26_m='6 - I watch when I can' then 1 else 0 end) as response_06_watch_when_I_can
,sum(case when q26_m='7 - Essential viewing' then 1 else 0 end) as response_07_essential_viewing
,count(*) as accounts
,sum(sky_sports_subscriber) as sky_sports_subscribers

into dbarnett.v250_individual_rights_totals_club_rugby_programmes
from #individual_rights_totals_club_rugby
where asked_follow_up_sports_rights_questions=1
group by prop_programmes_club_rugby
order by prop_programmes_club_rugby
;

commit;

select case when proportion_duration_club_rugby=0 then -1  when floor (proportion_duration_club_rugby*100)<1 then 
floor (proportion_duration_club_rugby*1000)/10 else floor(proportion_duration_club_rugby*100) end as prop_duration_club_rugby
,sum(case when q26_m='1 - Definitely don''t want to watch' then 1 else 0 end) as response_01_dont_want_watch
,sum(case when q26_m='2 - Hardly ever watch' then 1 else 0 end) as response_02_hardly_ever_watch
,sum(case when q26_m='3 - Watch once in a while' then 1 else 0 end) as response_03_watch_once_in_a_while
,sum(case when q26_m='4 - If there is nothing better on' then 1 else 0 end) as response_04_watch_if_nothing_better_on
,sum(case when q26_m='5 - I watch the big events' then 1 else 0 end) as response_05_big_events
,sum(case when q26_m='6 - I watch when I can' then 1 else 0 end) as response_06_watch_when_I_can
,sum(case when q26_m='7 - Essential viewing' then 1 else 0 end) as response_07_essential_viewing
,count(*) as accounts
,sum(sky_sports_subscriber) as sky_sports_subscribers

into dbarnett.v250_individual_rights_totals_club_rugby_duration
from #individual_rights_totals_club_rugby
where asked_follow_up_sports_rights_questions=1
group by prop_duration_club_rugby
order by prop_duration_club_rugby
;
commit;

--select * from dbarnett.v250_individual_rights_totals_club_rugby_programmes;
--select * from dbarnett.v250_individual_rights_totals_club_rugby_duration;




--drop table #individual_rights_totals_tennis;
select a.account_number
,annualised_ATPSS_soc_programmes_a*ATPSS_right_broadcast_programmes as total_programmes_ATPSS
,annualised_AOTBBC_soc_programmes_a*AOTBBC_right_broadcast_programmes as total_programmes_AOTBBC
,annualised_AOTEUR_soc_programmes_a*AOTEUR_right_broadcast_programmes as total_programmes_AOTEUR
,annualised_FOTEUR_soc_programmes_a*FOTEUR_right_broadcast_programmes as total_programmes_FOTEUR
,annualised_FOTITV_soc_programmes_a*FOTITV_right_broadcast_programmes as total_programmes_FOTITV
,annualised_USOTSS_soc_programmes_a*USOTSS_right_broadcast_programmes as total_programmes_USOTSS
,annualised_USOTEUR_soc_programmes_a*USOTEUR_right_broadcast_programmes as total_programmes_USOTEUR
,annualised_WIMBBC_soc_programmes_a*WIMBBC_right_broadcast_programmes as total_programmes_WIMBBC






,annualised_ATPSS_soc_duration_a*ATPSS_right_broadcast_duration as total_duration_ATPSS
,annualised_AOTBBC_soc_duration_a*AOTBBC_right_broadcast_duration as total_duration_AOTBBC
,annualised_AOTEUR_soc_duration_a*AOTEUR_right_broadcast_duration as total_duration_AOTEUR
,annualised_FOTEUR_soc_duration_a*FOTEUR_right_broadcast_duration as total_duration_FOTEUR
,annualised_FOTITV_soc_duration_a*FOTITV_right_broadcast_duration as total_duration_FOTITV
,annualised_USOTSS_soc_duration_a*USOTSS_right_broadcast_duration as total_duration_USOTSS
,annualised_USOTEUR_soc_duration_a*USOTEUR_right_broadcast_duration as total_duration_USOTEUR
,annualised_WIMBBC_soc_duration_a*WIMBBC_right_broadcast_duration as total_duration_WIMBBC

,total_programmes_ATPSS
+total_programmes_AOTBBC
+total_programmes_AOTEUR
+total_programmes_FOTEUR
+total_programmes_FOTITV
+total_programmes_USOTSS
+total_programmes_USOTEUR
+total_programmes_WIMBBC


as tennis_programmes

,ATPSS_right_broadcast_programmes
+AOTBBC_right_broadcast_programmes
+AOTEUR_right_broadcast_programmes
+FOTEUR_right_broadcast_programmes
+FOTITV_right_broadcast_programmes
+USOTSS_right_broadcast_programmes
+USOTEUR_right_broadcast_programmes
+WIMBBC_right_broadcast_programmes

as tennis_programmes_broadcast

,case when tennis_programmes_broadcast =0 then 0 else cast(tennis_programmes as real)/cast(tennis_programmes_broadcast as real) end as proportion_programmes_tennis

,total_duration_ATPSS
+total_duration_AOTBBC
+total_duration_AOTEUR
+total_duration_FOTEUR
+total_duration_FOTITV
+total_duration_USOTSS
+total_duration_USOTEUR
+total_duration_WIMBBC

as tennis_duration

,ATPSS_right_broadcast_duration
+AOTBBC_right_broadcast_duration
+AOTEUR_right_broadcast_duration
+FOTEUR_right_broadcast_duration
+FOTITV_right_broadcast_duration
+USOTSS_right_broadcast_duration
+USOTEUR_right_broadcast_duration
+WIMBBC_right_broadcast_duration

as tennis_duration_broadcast

,case when tennis_duration_broadcast =0 then 0 else cast(tennis_duration as real)/cast(tennis_duration_broadcast as real) end as proportion_duration_tennis
,d.q26_p
,d.q20_c
,d.asked_follow_up_sports_rights_questions
,case when e.sports_premiums>0 then 1 else 0 end as sky_sports_subscriber
into #individual_rights_totals_tennis
from dbarnett.v250_annualised_activity_table_final_v3 as a
left outer join dbarnett.v250_right_viewable_account_summary as c
on a.account_number = c.account_number
---Add in Survey details--
left outer join dbarnett.v250_sports_rights_survey_responses_winscp as d
on  a.account_number = d.ID_name
left outer join dbarnett.v250_Account_profiling as e
on  a.account_number = e.account_number
;
commit;
--select * from #individual_rights_totals_tennis;
--drop table dbarnett.v250_individual_rights_totals_tennis_programmes;
select case when proportion_programmes_tennis=0 then -1  when floor (proportion_programmes_tennis*100)<1 then 
floor (proportion_programmes_tennis*1000)/10 else floor(proportion_programmes_tennis*100) end as prop_programmes_tennis
,sum(case when q26_p='1 - Definitely don''t want to watch' then 1 else 0 end) as response_01_dont_want_watch
,sum(case when q26_p='2 - Hardly ever watch' then 1 else 0 end) as response_02_hardly_ever_watch
,sum(case when q26_p='3 - Watch once in a while' then 1 else 0 end) as response_03_watch_once_in_a_while
,sum(case when q26_p='4 - If there is nothing better on' then 1 else 0 end) as response_04_watch_if_nothing_better_on
,sum(case when q26_p='5 - I watch the big events' then 1 else 0 end) as response_05_big_events
,sum(case when q26_p='6 - I watch when I can' then 1 else 0 end) as response_06_watch_when_I_can
,sum(case when q26_p='7 - Essential viewing' then 1 else 0 end) as response_07_essential_viewing
,count(*) as accounts
,sum(sky_sports_subscriber) as sky_sports_subscribers

into dbarnett.v250_individual_rights_totals_tennis_programmes
from #individual_rights_totals_tennis
where asked_follow_up_sports_rights_questions=1
group by prop_programmes_tennis
order by prop_programmes_tennis
;

commit;

select case when proportion_duration_tennis=0 then -1  when floor (proportion_duration_tennis*100)<1 then 
floor (proportion_duration_tennis*1000)/10 else floor(proportion_duration_tennis*100) end as prop_duration_tennis
,sum(case when q26_p='1 - Definitely don''t want to watch' then 1 else 0 end) as response_01_dont_want_watch
,sum(case when q26_p='2 - Hardly ever watch' then 1 else 0 end) as response_02_hardly_ever_watch
,sum(case when q26_p='3 - Watch once in a while' then 1 else 0 end) as response_03_watch_once_in_a_while
,sum(case when q26_p='4 - If there is nothing better on' then 1 else 0 end) as response_04_watch_if_nothing_better_on
,sum(case when q26_p='5 - I watch the big events' then 1 else 0 end) as response_05_big_events
,sum(case when q26_p='6 - I watch when I can' then 1 else 0 end) as response_06_watch_when_I_can
,sum(case when q26_p='7 - Essential viewing' then 1 else 0 end) as response_07_essential_viewing
,count(*) as accounts
,sum(sky_sports_subscriber) as sky_sports_subscribers

into dbarnett.v250_individual_rights_totals_tennis_duration
from #individual_rights_totals_tennis
where asked_follow_up_sports_rights_questions=1
group by prop_duration_tennis
order by prop_duration_tennis
;
commit;

--select * from dbarnett.v250_individual_rights_totals_tennis_programmes;
--select * from dbarnett.v250_individual_rights_totals_tennis_duration;




--drop table #individual_rights_totals_darts;
select a.account_number
,annualised_DRTBBC_soc_programmes_a*DRTBBC_right_broadcast_programmes as total_programmes_DRTBBC
,annualised_PLDSS_soc_programmes_a*PLDSS_right_broadcast_programmes as total_programmes_PLDSS
,annualised_WDCSS_soc_programmes_a*WDCSS_right_broadcast_programmes as total_programmes_WDCSS


,annualised_DRTBBC_soc_duration_a*DRTBBC_right_broadcast_duration as total_duration_DRTBBC
,annualised_PLDSS_soc_duration_a*PLDSS_right_broadcast_duration as total_duration_PLDSS
,annualised_WDCSS_soc_duration_a*WDCSS_right_broadcast_duration as total_duration_WDCSS


,total_programmes_DRTBBC
+total_programmes_PLDSS
+total_programmes_WDCSS

as darts_programmes

,
DRTBBC_right_broadcast_programmes
+PLDSS_right_broadcast_programmes
+WDCSS_right_broadcast_programmes

as darts_programmes_broadcast

,case when darts_programmes_broadcast =0 then 0 else cast(darts_programmes as real)/cast(darts_programmes_broadcast as real) end as proportion_programmes_darts

,total_duration_DRTBBC
+total_duration_PLDSS
+total_duration_WDCSS

as darts_duration

,DRTBBC_right_broadcast_duration
+PLDSS_right_broadcast_duration
+WDCSS_right_broadcast_duration

as darts_duration_broadcast

,case when darts_duration_broadcast =0 then 0 else cast(darts_duration as real)/cast(darts_duration_broadcast as real) end as proportion_duration_darts
,d.q26_e
,d.q20_c
,d.asked_follow_up_sports_rights_questions
,case when e.sports_premiums>0 then 1 else 0 end as sky_sports_subscriber
into #individual_rights_totals_darts
from dbarnett.v250_annualised_activity_table_final_v3 as a
left outer join dbarnett.v250_right_viewable_account_summary as c
on a.account_number = c.account_number
---Add in Survey details--
left outer join dbarnett.v250_sports_rights_survey_responses_winscp as d
on  a.account_number = d.ID_name
left outer join dbarnett.v250_Account_profiling as e
on  a.account_number = e.account_number
;
commit;
--select * from #individual_rights_totals_darts;
--drop table dbarnett.v250_individual_rights_totals_darts_programmes;
select case when proportion_programmes_darts=0 then -1  when floor (proportion_programmes_darts*100)<1 then 
floor (proportion_programmes_darts*1000)/10 else floor(proportion_programmes_darts*100) end as prop_programmes_darts
,sum(case when q26_e='1 - Definitely don''t want to watch' then 1 else 0 end) as response_01_dont_want_watch
,sum(case when q26_e='2 - Hardly ever watch' then 1 else 0 end) as response_02_hardly_ever_watch
,sum(case when q26_e='3 - Watch once in a while' then 1 else 0 end) as response_03_watch_once_in_a_while
,sum(case when q26_e='4 - If there is nothing better on' then 1 else 0 end) as response_04_watch_if_nothing_better_on
,sum(case when q26_e='5 - I watch the big events' then 1 else 0 end) as response_05_big_events
,sum(case when q26_e='6 - I watch when I can' then 1 else 0 end) as response_06_watch_when_I_can
,sum(case when q26_e='7 - Essential viewing' then 1 else 0 end) as response_07_essential_viewing
,count(*) as accounts
,sum(sky_sports_subscriber) as sky_sports_subscribers

into dbarnett.v250_individual_rights_totals_darts_programmes
from #individual_rights_totals_darts
where asked_follow_up_sports_rights_questions=1
group by prop_programmes_darts
order by prop_programmes_darts
;

commit;

select case when proportion_duration_darts=0 then -1  when floor (proportion_duration_darts*100)<1 then 
floor (proportion_duration_darts*1000)/10 else floor(proportion_duration_darts*100) end as prop_duration_darts
,sum(case when q26_e='1 - Definitely don''t want to watch' then 1 else 0 end) as response_01_dont_want_watch
,sum(case when q26_e='2 - Hardly ever watch' then 1 else 0 end) as response_02_hardly_ever_watch
,sum(case when q26_e='3 - Watch once in a while' then 1 else 0 end) as response_03_watch_once_in_a_while
,sum(case when q26_e='4 - If there is nothing better on' then 1 else 0 end) as response_04_watch_if_nothing_better_on
,sum(case when q26_e='5 - I watch the big events' then 1 else 0 end) as response_05_big_events
,sum(case when q26_e='6 - I watch when I can' then 1 else 0 end) as response_06_watch_when_I_can
,sum(case when q26_e='7 - Essential viewing' then 1 else 0 end) as response_07_essential_viewing
,count(*) as accounts
,sum(sky_sports_subscriber) as sky_sports_subscribers

into dbarnett.v250_individual_rights_totals_darts_duration
from #individual_rights_totals_darts
where asked_follow_up_sports_rights_questions=1
group by prop_duration_darts
order by prop_duration_darts
;
commit;

--select * from dbarnett.v250_individual_rights_totals_darts_programmes;
--select * from dbarnett.v250_individual_rights_totals_darts_duration;



--drop table #individual_rights_totals_fighting_sports;
select a.account_number
,annualised_BOXCH5_soc_programmes_a*BOXCH5_right_broadcast_programmes as total_programmes_BOXCH5
,annualised_BOXSS_soc_programmes_a*BOXSS_right_broadcast_programmes as total_programmes_BOXSS
,annualised_BOXITV1_soc_programmes_a*BOXITV1_right_broadcast_programmes as total_programmes_BOXITV1










,annualised_BOXCH5_soc_duration_a*BOXCH5_right_broadcast_duration as total_duration_BOXCH5
,annualised_BOXSS_soc_duration_a*BOXSS_right_broadcast_duration as total_duration_BOXSS
,annualised_BOXITV1_soc_duration_a*BOXITV1_right_broadcast_duration as total_duration_BOXITV1


,total_programmes_BOXCH5
+total_programmes_BOXSS
+total_programmes_BOXITV1


as fighting_sports_programmes

,BOXCH5_right_broadcast_programmes
+BOXSS_right_broadcast_programmes
+BOXITV1_right_broadcast_programmes

as fighting_sports_programmes_broadcast

,case when fighting_sports_programmes_broadcast =0 then 0 else cast(fighting_sports_programmes as real)/cast(fighting_sports_programmes_broadcast as real) end as proportion_programmes_fighting_sports
,total_duration_BOXCH5
+total_duration_BOXSS
+total_duration_BOXITV1

as fighting_sports_duration

,BOXCH5_right_broadcast_duration
+BOXSS_right_broadcast_duration
+BOXITV1_right_broadcast_duration

as fighting_sports_duration_broadcast

,case when fighting_sports_duration_broadcast =0 then 0 else cast(fighting_sports_duration as real)/cast(fighting_sports_duration_broadcast as real) end as proportion_duration_fighting_sports
,d.q26_f
,d.q20_c
,d.asked_follow_up_sports_rights_questions
,case when e.sports_premiums>0 then 1 else 0 end as sky_sports_subscriber
into #individual_rights_totals_fighting_sports
from dbarnett.v250_annualised_activity_table_final_v3 as a
left outer join dbarnett.v250_right_viewable_account_summary as c
on a.account_number = c.account_number
---Add in Survey details--
left outer join dbarnett.v250_sports_rights_survey_responses_winscp as d
on  a.account_number = d.ID_name
left outer join dbarnett.v250_Account_profiling as e
on  a.account_number = e.account_number
;
commit;
--select * from #individual_rights_totals_fighting_sports;
--drop table dbarnett.v250_individual_rights_totals_fighting_sports_programmes;
select case when proportion_programmes_fighting_sports=0 then -1  when floor (proportion_programmes_fighting_sports*100)<1 then 
floor (proportion_programmes_fighting_sports*1000)/10 else floor(proportion_programmes_fighting_sports*100) end as prop_programmes_fighting_sports
,sum(case when q26_f='1 - Definitely don''t want to watch' then 1 else 0 end) as response_01_dont_want_watch
,sum(case when q26_f='2 - Hardly ever watch' then 1 else 0 end) as response_02_hardly_ever_watch
,sum(case when q26_f='3 - Watch once in a while' then 1 else 0 end) as response_03_watch_once_in_a_while
,sum(case when q26_f='4 - If there is nothing better on' then 1 else 0 end) as response_04_watch_if_nothing_better_on
,sum(case when q26_f='5 - I watch the big events' then 1 else 0 end) as response_05_big_events
,sum(case when q26_f='6 - I watch when I can' then 1 else 0 end) as response_06_watch_when_I_can
,sum(case when q26_f='7 - Essential viewing' then 1 else 0 end) as response_07_essential_viewing
,count(*) as accounts
,sum(sky_sports_subscriber) as sky_sports_subscribers

into dbarnett.v250_individual_rights_totals_fighting_sports_programmes
from #individual_rights_totals_fighting_sports
where asked_follow_up_sports_rights_questions=1
group by prop_programmes_fighting_sports
order by prop_programmes_fighting_sports
;

commit;

select case when proportion_duration_fighting_sports=0 then -1  when floor (proportion_duration_fighting_sports*100)<1 then 
floor (proportion_duration_fighting_sports*1000)/10 else floor(proportion_duration_fighting_sports*100) end as prop_duration_fighting_sports
,sum(case when q26_f='1 - Definitely don''t want to watch' then 1 else 0 end) as response_01_dont_want_watch
,sum(case when q26_f='2 - Hardly ever watch' then 1 else 0 end) as response_02_hardly_ever_watch
,sum(case when q26_f='3 - Watch once in a while' then 1 else 0 end) as response_03_watch_once_in_a_while
,sum(case when q26_f='4 - If there is nothing better on' then 1 else 0 end) as response_04_watch_if_nothing_better_on
,sum(case when q26_f='5 - I watch the big events' then 1 else 0 end) as response_05_big_events
,sum(case when q26_f='6 - I watch when I can' then 1 else 0 end) as response_06_watch_when_I_can
,sum(case when q26_f='7 - Essential viewing' then 1 else 0 end) as response_07_essential_viewing
,count(*) as accounts
,sum(sky_sports_subscriber) as sky_sports_subscribers

into dbarnett.v250_individual_rights_totals_fighting_sports_duration
from #individual_rights_totals_fighting_sports
where asked_follow_up_sports_rights_questions=1
group by prop_duration_fighting_sports
order by prop_duration_fighting_sports
;
commit;

--select * from dbarnett.v250_individual_rights_totals_fighting_sports_programmes;
--select * from dbarnett.v250_individual_rights_totals_fighting_sports_duration;



--drop table #individual_rights_totals_cycling;
select a.account_number
,annualised_CLVITV_soc_programmes_a*CLVITV_right_broadcast_programmes as total_programmes_CLVITV
,annualised_CUCISS_soc_programmes_a*CUCISS_right_broadcast_programmes as total_programmes_CUCISS
,annualised_CTBEUR_soc_programmes_a*CTBEUR_right_broadcast_programmes as total_programmes_CTBEUR
,annualised_CTCITV_soc_programmes_a*CTCITV_right_broadcast_programmes as total_programmes_CTCITV
,annualised_TDFEUR_soc_programmes_a*TDFEUR_right_broadcast_programmes as total_programmes_TDFEUR
,annualised_TDFITV_soc_programmes_a*TDFITV_right_broadcast_programmes as total_programmes_TDFITV








,annualised_CLVITV_soc_duration_a*CLVITV_right_broadcast_duration as total_duration_CLVITV
,annualised_CUCISS_soc_duration_a*CUCISS_right_broadcast_duration as total_duration_CUCISS
,annualised_CTBEUR_soc_duration_a*CTBEUR_right_broadcast_duration as total_duration_CTBEUR
,annualised_CTCITV_soc_duration_a*CTCITV_right_broadcast_duration as total_duration_CTCITV
,annualised_TDFEUR_soc_duration_a*TDFEUR_right_broadcast_duration as total_duration_TDFEUR
,annualised_TDFITV_soc_duration_a*TDFITV_right_broadcast_duration as total_duration_TDFITV


,total_programmes_CLVITV
+total_programmes_CUCISS
+total_programmes_CTBEUR
+total_programmes_CTCITV
+total_programmes_TDFEUR
+total_programmes_TDFITV

as cycling_programmes

,CLVITV_right_broadcast_programmes
+CUCISS_right_broadcast_programmes
+CTBEUR_right_broadcast_programmes
+CTCITV_right_broadcast_programmes
+TDFEUR_right_broadcast_programmes
+TDFITV_right_broadcast_programmes



as cycling_programmes_broadcast

,case when cycling_programmes_broadcast =0 then 0 else cast(cycling_programmes as real)/cast(cycling_programmes_broadcast as real) end as proportion_programmes_cycling

,total_duration_CLVITV
+total_duration_CUCISS
+total_duration_CTBEUR
+total_duration_CTCITV
+total_duration_TDFEUR
+total_duration_TDFITV

as cycling_duration

,CLVITV_right_broadcast_duration
+CUCISS_right_broadcast_duration
+CTBEUR_right_broadcast_duration
+CTCITV_right_broadcast_duration
+TDFEUR_right_broadcast_duration
+TDFITV_right_broadcast_duration



as cycling_duration_broadcast

,case when cycling_duration_broadcast =0 then 0 else cast(cycling_duration as real)/cast(cycling_duration_broadcast as real) end as proportion_duration_cycling
,d.q26_d
,d.q20_c
,d.asked_follow_up_sports_rights_questions
,case when e.sports_premiums>0 then 1 else 0 end as sky_sports_subscriber
into #individual_rights_totals_cycling
from dbarnett.v250_annualised_activity_table_final_v3 as a
left outer join dbarnett.v250_right_viewable_account_summary as c
on a.account_number = c.account_number
---Add in Survey details--
left outer join dbarnett.v250_sports_rights_survey_responses_winscp as d
on  a.account_number = d.ID_name
left outer join dbarnett.v250_Account_profiling as e
on  a.account_number = e.account_number
;
commit;
--select * from #individual_rights_totals_cycling;
--drop table dbarnett.v250_individual_rights_totals_cycling_programmes;
select case when proportion_programmes_cycling=0 then -1  when floor (proportion_programmes_cycling*100)<1 then 
floor (proportion_programmes_cycling*1000)/10 else floor(proportion_programmes_cycling*100) end as prop_programmes_cycling
,sum(case when q26_d='1 - Definitely don''t want to watch' then 1 else 0 end) as response_01_dont_want_watch
,sum(case when q26_d='2 - Hardly ever watch' then 1 else 0 end) as response_02_hardly_ever_watch
,sum(case when q26_d='3 - Watch once in a while' then 1 else 0 end) as response_03_watch_once_in_a_while
,sum(case when q26_d='4 - If there is nothing better on' then 1 else 0 end) as response_04_watch_if_nothing_better_on
,sum(case when q26_d='5 - I watch the big events' then 1 else 0 end) as response_05_big_events
,sum(case when q26_d='6 - I watch when I can' then 1 else 0 end) as response_06_watch_when_I_can
,sum(case when q26_d='7 - Essential viewing' then 1 else 0 end) as response_07_essential_viewing
,count(*) as accounts
,sum(sky_sports_subscriber) as sky_sports_subscribers

into dbarnett.v250_individual_rights_totals_cycling_programmes
from #individual_rights_totals_cycling
where asked_follow_up_sports_rights_questions=1
group by prop_programmes_cycling
order by prop_programmes_cycling
;

commit;

select case when proportion_duration_cycling=0 then -1  when floor (proportion_duration_cycling*100)<1 then 
floor (proportion_duration_cycling*1000)/10 else floor(proportion_duration_cycling*100) end as prop_duration_cycling
,sum(case when q26_d='1 - Definitely don''t want to watch' then 1 else 0 end) as response_01_dont_want_watch
,sum(case when q26_d='2 - Hardly ever watch' then 1 else 0 end) as response_02_hardly_ever_watch
,sum(case when q26_d='3 - Watch once in a while' then 1 else 0 end) as response_03_watch_once_in_a_while
,sum(case when q26_d='4 - If there is nothing better on' then 1 else 0 end) as response_04_watch_if_nothing_better_on
,sum(case when q26_d='5 - I watch the big events' then 1 else 0 end) as response_05_big_events
,sum(case when q26_d='6 - I watch when I can' then 1 else 0 end) as response_06_watch_when_I_can
,sum(case when q26_d='7 - Essential viewing' then 1 else 0 end) as response_07_essential_viewing
,count(*) as accounts
,sum(sky_sports_subscriber) as sky_sports_subscribers

into dbarnett.v250_individual_rights_totals_cycling_duration
from #individual_rights_totals_cycling
where asked_follow_up_sports_rights_questions=1
group by prop_duration_cycling
order by prop_duration_cycling
;
commit;

--select * from dbarnett.v250_individual_rights_totals_cycling_programmes;
--select * from dbarnett.v250_individual_rights_totals_cycling_duration;



--drop table #individual_rights_totals_american_sports;
select a.account_number
,annualised_NBASS_soc_programmes_a*NBASS_right_broadcast_programmes as total_programmes_NBASS
,annualised_NFLBBC_soc_programmes_a*NFLBBC_right_broadcast_programmes as total_programmes_NFLBBC
,annualised_NFLCH4_soc_programmes_a*NFLCH4_right_broadcast_programmes as total_programmes_NFLCH4
,annualised_NFLSS_soc_programmes_a*NFLSS_right_broadcast_programmes as total_programmes_NFLSS

,annualised_NBASS_soc_duration_a*NBASS_right_broadcast_duration as total_duration_NBASS
,annualised_NFLBBC_soc_duration_a*NFLBBC_right_broadcast_duration as total_duration_NFLBBC
,annualised_NFLCH4_soc_duration_a*NFLCH4_right_broadcast_duration as total_duration_NFLCH4
,annualised_NFLSS_soc_duration_a*NFLSS_right_broadcast_duration as total_duration_NFLSS


,total_programmes_NBASS
+total_programmes_NFLBBC
+total_programmes_NFLCH4
+total_programmes_NFLSS

as american_sports_programmes

,NBASS_right_broadcast_programmes
+NFLBBC_right_broadcast_programmes
+NFLCH4_right_broadcast_programmes
+NFLSS_right_broadcast_programmes

as american_sports_programmes_broadcast

,case when american_sports_programmes_broadcast =0 then 0 else cast(american_sports_programmes as real)/cast(american_sports_programmes_broadcast as real) end as proportion_programmes_american_sports

,total_duration_NBASS
+total_duration_NFLBBC
+total_duration_NFLCH4
+total_duration_NFLSS
as american_sports_duration

,NBASS_right_broadcast_duration
+NFLBBC_right_broadcast_duration
+NFLCH4_right_broadcast_duration
+NFLSS_right_broadcast_duration

as american_sports_duration_broadcast

,case when american_sports_duration_broadcast =0 then 0 else cast(american_sports_duration as real)/cast(american_sports_duration_broadcast as real) end as proportion_duration_american_sports
,d.q26_a
,d.q20_c
,d.asked_follow_up_sports_rights_questions
,case when e.sports_premiums>0 then 1 else 0 end as sky_sports_subscriber
into #individual_rights_totals_american_sports
from dbarnett.v250_annualised_activity_table_final_v3 as a
left outer join dbarnett.v250_right_viewable_account_summary as c
on a.account_number = c.account_number
---Add in Survey details--
left outer join dbarnett.v250_sports_rights_survey_responses_winscp as d
on  a.account_number = d.ID_name
left outer join dbarnett.v250_Account_profiling as e
on  a.account_number = e.account_number
;
commit;
--select * from #individual_rights_totals_american_sports;
--drop table dbarnett.v250_individual_rights_totals_american_sports_programmes;
select case when proportion_programmes_american_sports=0 then -1  when floor (proportion_programmes_american_sports*100)<1 then 
floor (proportion_programmes_american_sports*1000)/10 else floor(proportion_programmes_american_sports*100) end as prop_programmes_american_sports
,sum(case when q26_a='1 - Definitely don''t want to watch' then 1 else 0 end) as response_01_dont_want_watch
,sum(case when q26_a='2 - Hardly ever watch' then 1 else 0 end) as response_02_hardly_ever_watch
,sum(case when q26_a='3 - Watch once in a while' then 1 else 0 end) as response_03_watch_once_in_a_while
,sum(case when q26_a='4 - If there is nothing better on' then 1 else 0 end) as response_04_watch_if_nothing_better_on
,sum(case when q26_a='5 - I watch the big events' then 1 else 0 end) as response_05_big_events
,sum(case when q26_a='6 - I watch when I can' then 1 else 0 end) as response_06_watch_when_I_can
,sum(case when q26_a='7 - Essential viewing' then 1 else 0 end) as response_07_essential_viewing
,count(*) as accounts
,sum(sky_sports_subscriber) as sky_sports_subscribers

into dbarnett.v250_individual_rights_totals_american_sports_programmes
from #individual_rights_totals_american_sports
where asked_follow_up_sports_rights_questions=1
group by prop_programmes_american_sports
order by prop_programmes_american_sports
;

commit;

select case when proportion_duration_american_sports=0 then -1  when floor (proportion_duration_american_sports*100)<1 then 
floor (proportion_duration_american_sports*1000)/10 else floor(proportion_duration_american_sports*100) end as prop_duration_american_sports
,sum(case when q26_a='1 - Definitely don''t want to watch' then 1 else 0 end) as response_01_dont_want_watch
,sum(case when q26_a='2 - Hardly ever watch' then 1 else 0 end) as response_02_hardly_ever_watch
,sum(case when q26_a='3 - Watch once in a while' then 1 else 0 end) as response_03_watch_once_in_a_while
,sum(case when q26_a='4 - If there is nothing better on' then 1 else 0 end) as response_04_watch_if_nothing_better_on
,sum(case when q26_a='5 - I watch the big events' then 1 else 0 end) as response_05_big_events
,sum(case when q26_a='6 - I watch when I can' then 1 else 0 end) as response_06_watch_when_I_can
,sum(case when q26_a='7 - Essential viewing' then 1 else 0 end) as response_07_essential_viewing
,count(*) as accounts
,sum(sky_sports_subscriber) as sky_sports_subscribers

into dbarnett.v250_individual_rights_totals_american_sports_duration
from #individual_rights_totals_american_sports
where asked_follow_up_sports_rights_questions=1
group by prop_duration_american_sports
order by prop_duration_american_sports
;
commit;

--select * from dbarnett.v250_individual_rights_totals_american_sports_programmes;
--select * from dbarnett.v250_individual_rights_totals_american_sports_duration;



--drop table #individual_rights_totals_snooker_pool;
select a.account_number
,annualised_MRPSS_soc_programmes_a*MRPSS_right_broadcast_programmes as total_programmes_MRPSS
,annualised_MRSSS_soc_programmes_a*MRSSS_right_broadcast_programmes as total_programmes_MRSSS
,annualised_WSCBBC_soc_programmes_a*WSCBBC_right_broadcast_programmes as total_programmes_WSCBBC

,annualised_MRPSS_soc_duration_a*MRPSS_right_broadcast_duration as total_duration_MRPSS
,annualised_MRSSS_soc_duration_a*MRSSS_right_broadcast_duration as total_duration_MRSSS
,annualised_WSCBBC_soc_duration_a*WSCBBC_right_broadcast_duration as total_duration_WSCBBC

,total_programmes_MRPSS
+total_programmes_MRSSS
+total_programmes_WSCBBC


as snooker_pool_programmes

,MRPSS_right_broadcast_programmes
+MRSSS_right_broadcast_programmes
+WSCBBC_right_broadcast_programmes



as snooker_pool_programmes_broadcast

,case when snooker_pool_programmes_broadcast =0 then 0 else cast(snooker_pool_programmes as real)/cast(snooker_pool_programmes_broadcast as real) end as proportion_programmes_snooker_pool

,total_duration_MRPSS
+total_duration_MRSSS
+total_duration_WSCBBC



as snooker_pool_duration

,MRPSS_right_broadcast_duration
+MRSSS_right_broadcast_duration
+WSCBBC_right_broadcast_duration


as snooker_pool_duration_broadcast

,case when snooker_pool_duration_broadcast =0 then 0 else cast(snooker_pool_duration as real)/cast(snooker_pool_duration_broadcast as real) end as proportion_duration_snooker_pool
,d.q26_o
,d.q20_c
,d.asked_follow_up_sports_rights_questions
,case when e.sports_premiums>0 then 1 else 0 end as sky_sports_subscriber
into #individual_rights_totals_snooker_pool
from dbarnett.v250_annualised_activity_table_final_v3 as a
left outer join dbarnett.v250_right_viewable_account_summary as c
on a.account_number = c.account_number
---Add in Survey details--
left outer join dbarnett.v250_sports_rights_survey_responses_winscp as d
on  a.account_number = d.ID_name
left outer join dbarnett.v250_Account_profiling as e
on  a.account_number = e.account_number
;
commit;
--select * from #individual_rights_totals_snooker_pool;
--drop table dbarnett.v250_individual_rights_totals_snooker_pool_programmes;
select case when proportion_programmes_snooker_pool=0 then -1  when floor (proportion_programmes_snooker_pool*100)<1 then 
floor (proportion_programmes_snooker_pool*1000)/10 else floor(proportion_programmes_snooker_pool*100) end as prop_programmes_snooker_pool
,sum(case when q26_o='1 - Definitely don''t want to watch' then 1 else 0 end) as response_01_dont_want_watch
,sum(case when q26_o='2 - Hardly ever watch' then 1 else 0 end) as response_02_hardly_ever_watch
,sum(case when q26_o='3 - Watch once in a while' then 1 else 0 end) as response_03_watch_once_in_a_while
,sum(case when q26_o='4 - If there is nothing better on' then 1 else 0 end) as response_04_watch_if_nothing_better_on
,sum(case when q26_o='5 - I watch the big events' then 1 else 0 end) as response_05_big_events
,sum(case when q26_o='6 - I watch when I can' then 1 else 0 end) as response_06_watch_when_I_can
,sum(case when q26_o='7 - Essential viewing' then 1 else 0 end) as response_07_essential_viewing
,count(*) as accounts
,sum(sky_sports_subscriber) as sky_sports_subscribers

into dbarnett.v250_individual_rights_totals_snooker_pool_programmes
from #individual_rights_totals_snooker_pool
where asked_follow_up_sports_rights_questions=1
group by prop_programmes_snooker_pool
order by prop_programmes_snooker_pool
;

commit;

select case when proportion_duration_snooker_pool=0 then -1  when floor (proportion_duration_snooker_pool*100)<1 then 
floor (proportion_duration_snooker_pool*1000)/10 else floor(proportion_duration_snooker_pool*100) end as prop_duration_snooker_pool
,sum(case when q26_o='1 - Definitely don''t want to watch' then 1 else 0 end) as response_01_dont_want_watch
,sum(case when q26_o='2 - Hardly ever watch' then 1 else 0 end) as response_02_hardly_ever_watch
,sum(case when q26_o='3 - Watch once in a while' then 1 else 0 end) as response_03_watch_once_in_a_while
,sum(case when q26_o='4 - If there is nothing better on' then 1 else 0 end) as response_04_watch_if_nothing_better_on
,sum(case when q26_o='5 - I watch the big events' then 1 else 0 end) as response_05_big_events
,sum(case when q26_o='6 - I watch when I can' then 1 else 0 end) as response_06_watch_when_I_can
,sum(case when q26_o='7 - Essential viewing' then 1 else 0 end) as response_07_essential_viewing
,count(*) as accounts
,sum(sky_sports_subscriber) as sky_sports_subscribers

into dbarnett.v250_individual_rights_totals_snooker_pool_duration
from #individual_rights_totals_snooker_pool
where asked_follow_up_sports_rights_questions=1
group by prop_duration_snooker_pool
order by prop_duration_snooker_pool
;
commit;

--select * from dbarnett.v250_individual_rights_totals_snooker_pool_programmes;
--select * from dbarnett.v250_individual_rights_totals_snooker_pool_duration;




--drop table #individual_rights_totals_horse_racing;
select a.account_number
,annualised_DERCH4_soc_programmes_a*DERCH4_right_broadcast_programmes as total_programmes_DERCH4
,annualised_GDNCH4_soc_programmes_a*GDNCH4_right_broadcast_programmes as total_programmes_GDNCH4
,annualised_OAKCH4_soc_programmes_a*OAKCH4_right_broadcast_programmes as total_programmes_OAKCH4
,annualised_RACSS_soc_programmes_a*RACSS_right_broadcast_programmes as total_programmes_RACSS
,annualised_RACCH4_soc_programmes_a*RACCH4_right_broadcast_programmes as total_programmes_RACCH4

,annualised_DERCH4_soc_duration_a*DERCH4_right_broadcast_duration as total_duration_DERCH4
,annualised_GDNCH4_soc_duration_a*GDNCH4_right_broadcast_duration as total_duration_GDNCH4
,annualised_OAKCH4_soc_duration_a*OAKCH4_right_broadcast_duration as total_duration_OAKCH4
,annualised_RACSS_soc_duration_a*RACSS_right_broadcast_duration as total_duration_RACSS
,annualised_RACCH4_soc_duration_a*RACCH4_right_broadcast_duration as total_duration_RACCH4


,total_programmes_DERCH4
+total_programmes_GDNCH4
+total_programmes_OAKCH4
+total_programmes_RACSS
+total_programmes_RACCH4

as horse_racing_programmes

,DERCH4_right_broadcast_programmes
+GDNCH4_right_broadcast_programmes
+OAKCH4_right_broadcast_programmes
+RACSS_right_broadcast_programmes
+RACCH4_right_broadcast_programmes



as horse_racing_programmes_broadcast

,case when horse_racing_programmes_broadcast =0 then 0 else cast(horse_racing_programmes as real)/cast(horse_racing_programmes_broadcast as real) end as proportion_programmes_horse_racing

,total_duration_DERCH4
+total_duration_GDNCH4
+total_duration_OAKCH4
+total_duration_RACSS
+total_duration_RACCH4

as horse_racing_duration

,DERCH4_right_broadcast_duration
+GDNCH4_right_broadcast_duration
+OAKCH4_right_broadcast_duration
+RACSS_right_broadcast_duration
+RACCH4_right_broadcast_duration



as horse_racing_duration_broadcast

,case when horse_racing_duration_broadcast =0 then 0 else cast(horse_racing_duration as real)/cast(horse_racing_duration_broadcast as real) end as proportion_duration_horse_racing
,d.q26_k
,d.q20_c
,d.asked_follow_up_sports_rights_questions
,case when e.sports_premiums>0 then 1 else 0 end as sky_sports_subscriber
into #individual_rights_totals_horse_racing
from dbarnett.v250_annualised_activity_table_final_v3 as a
left outer join dbarnett.v250_right_viewable_account_summary as c
on a.account_number = c.account_number
---Add in Survey details--
left outer join dbarnett.v250_sports_rights_survey_responses_winscp as d
on  a.account_number = d.ID_name
left outer join dbarnett.v250_Account_profiling as e
on  a.account_number = e.account_number
;
commit;
--select * from #individual_rights_totals_horse_racing;
--drop table dbarnett.v250_individual_rights_totals_horse_racing_programmes;
select case when proportion_programmes_horse_racing=0 then -1  when floor (proportion_programmes_horse_racing*100)<1 then 
floor (proportion_programmes_horse_racing*1000)/10 else floor(proportion_programmes_horse_racing*100) end as prop_programmes_horse_racing
,sum(case when q26_k='1 - Definitely don''t want to watch' then 1 else 0 end) as response_01_dont_want_watch
,sum(case when q26_k='2 - Hardly ever watch' then 1 else 0 end) as response_02_hardly_ever_watch
,sum(case when q26_k='3 - Watch once in a while' then 1 else 0 end) as response_03_watch_once_in_a_while
,sum(case when q26_k='4 - If there is nothing better on' then 1 else 0 end) as response_04_watch_if_nothing_better_on
,sum(case when q26_k='5 - I watch the big events' then 1 else 0 end) as response_05_big_events
,sum(case when q26_k='6 - I watch when I can' then 1 else 0 end) as response_06_watch_when_I_can
,sum(case when q26_k='7 - Essential viewing' then 1 else 0 end) as response_07_essential_viewing
,count(*) as accounts
,sum(sky_sports_subscriber) as sky_sports_subscribers

into dbarnett.v250_individual_rights_totals_horse_racing_programmes
from #individual_rights_totals_horse_racing
where asked_follow_up_sports_rights_questions=1
group by prop_programmes_horse_racing
order by prop_programmes_horse_racing
;

commit;

select case when proportion_duration_horse_racing=0 then -1  when floor (proportion_duration_horse_racing*100)<1 then 
floor (proportion_duration_horse_racing*1000)/10 else floor(proportion_duration_horse_racing*100) end as prop_duration_horse_racing
,sum(case when q26_k='1 - Definitely don''t want to watch' then 1 else 0 end) as response_01_dont_want_watch
,sum(case when q26_k='2 - Hardly ever watch' then 1 else 0 end) as response_02_hardly_ever_watch
,sum(case when q26_k='3 - Watch once in a while' then 1 else 0 end) as response_03_watch_once_in_a_while
,sum(case when q26_k='4 - If there is nothing better on' then 1 else 0 end) as response_04_watch_if_nothing_better_on
,sum(case when q26_k='5 - I watch the big events' then 1 else 0 end) as response_05_big_events
,sum(case when q26_k='6 - I watch when I can' then 1 else 0 end) as response_06_watch_when_I_can
,sum(case when q26_k='7 - Essential viewing' then 1 else 0 end) as response_07_essential_viewing
,count(*) as accounts
,sum(sky_sports_subscriber) as sky_sports_subscribers

into dbarnett.v250_individual_rights_totals_horse_racing_duration
from #individual_rights_totals_horse_racing
where asked_follow_up_sports_rights_questions=1
group by prop_duration_horse_racing
order by prop_duration_horse_racing
;
commit;

--select * from dbarnett.v250_individual_rights_totals_horse_racing_programmes;
--select * from dbarnett.v250_individual_rights_totals_horse_racing_duration;

grant all on dbarnett.v250_individual_rights_totals_uk_football_programmes to public;
grant all on dbarnett.v250_individual_rights_totals_uk_football_duration to public;
grant all on dbarnett.v250_individual_rights_totals_euro_football_programmes to public;
grant all on dbarnett.v250_individual_rights_totals_euro_football_duration to public;
grant all on dbarnett.v250_individual_rights_totals_international_football_programmes to public;
grant all on dbarnett.v250_individual_rights_totals_international_football_duration to public;
grant all on dbarnett.v250_individual_rights_totals_international_rugby_programmes to public;
grant all on dbarnett.v250_individual_rights_totals_international_rugby_duration to public;
grant all on dbarnett.v250_individual_rights_totals_cricket_programmes to public;
grant all on dbarnett.v250_individual_rights_totals_cricket_duration to public;
grant all on dbarnett.v250_individual_rights_totals_motor_sport_programmes to public;
grant all on dbarnett.v250_individual_rights_totals_motor_sport_duration to public;
grant all on dbarnett.v250_individual_rights_totals_golf_programmes to public;
grant all on dbarnett.v250_individual_rights_totals_golf_duration to public;
grant all on dbarnett.v250_individual_rights_totals_athletics_programmes to public;
grant all on dbarnett.v250_individual_rights_totals_athletics_duration to public;
grant all on dbarnett.v250_individual_rights_totals_club_rugby_programmes to public;
grant all on dbarnett.v250_individual_rights_totals_club_rugby_duration to public;
grant all on dbarnett.v250_individual_rights_totals_tennis_programmes to public;
grant all on dbarnett.v250_individual_rights_totals_tennis_duration to public;
grant all on dbarnett.v250_individual_rights_totals_darts_programmes to public;
grant all on dbarnett.v250_individual_rights_totals_darts_duration to public;
grant all on dbarnett.v250_individual_rights_totals_fighting_sports_programmes to public;
grant all on dbarnett.v250_individual_rights_totals_fighting_sports_duration to public;
grant all on dbarnett.v250_individual_rights_totals_cycling_programmes to public;
grant all on dbarnett.v250_individual_rights_totals_cycling_duration to public;
grant all on dbarnett.v250_individual_rights_totals_american_sports_programmes to public;
grant all on dbarnett.v250_individual_rights_totals_american_sports_duration to public;
grant all on dbarnett.v250_individual_rights_totals_snooker_pool_programmes to public;
grant all on dbarnett.v250_individual_rights_totals_snooker_pool_duration to public;
grant all on dbarnett.v250_individual_rights_totals_horse_racing_programmes to public;
grant all on dbarnett.v250_individual_rights_totals_horse_racing_duration to public;
commit;
/*
--Overall
select q26_g
,count(*) as accounts
,sum(proportion_programmes_uk_football) as total_soc_uk_football
from #individual_rights_totals_uk_football
where asked_follow_up_sports_rights_questions=1
group by q26_g
order by q26_g

---Repeat Split by SoC Duration


select round(proportion_duration_uk_football,2) as prop_duration_uk_football
,q26_g
,count(*) as accounts
from #individual_rights_totals_uk_football
where asked_follow_up_sports_rights_questions=1
group by q26_g
,prop_duration_uk_football
order by prop_duration_uk_football


--Overall
select q26_g
,count(*) as accounts
,sum(proportion_duration_uk_football) as total_soc_uk_football
--,sum(sky_sports_subscriber) as ss_sub
from #individual_rights_totals_uk_football
where asked_follow_up_sports_rights_questions=1
group by q26_g
order by q26_g


---Split by Sky Sports/Non Sky Sports Customer--







--select top 500 * from skoczej.v250_cluster_numbers ;
--select count(*) from skoczej.v250_cluster_numbers ;


--select * from #individual_rights_totals_uk_football;

select  case when cast(q20_c as integer)>=6 then 'a) 6-10' else 'b) 0-5' end as interest_sports , q26_g,count(*) 
from #individual_rights_totals_uk_football
where asked_follow_up_sports_rights_questions=1
group by interest_sports , q26_g
order by interest_sports , q26_g;



select  cast(q20_c as integer) as q20c , q26_g,count(*) from #individual_rights_totals_uk_football
where asked_follow_up_sports_rights_questions=1
group by q20c , q26_g
order by q20c , q26_g;






































--select  count(*),sum(asked_follow_up_sports_rights_questions) from dbarnett.v250_sports_rights_survey_responses_winscp where q20_c in ('1','2','3','4','5') 




select top 100 * from dbarnett.v250_annualised_activity_table_final_v3;

commit;

select round(annualised_GPLSS_SOC_Programmes_A,2) as GPL_SOC
,case when q26_g is null then '8 - No Answer' when left(q26_g,1) not in ('1','2','3','4','5','6','7') then '8 - No Answer' else q26_g end as q26_g_response
,count(*)
,sum(account_weight) as accounts

from dbarnett.v250_annualised_activity_table_final_v3 as a
left outer join dbarnett.v250_sports_rights_survey_responses_winscp as b
on a.account_number = b.ID_name
group by GPL_SOC
,q26_g_response
order by GPL_SOC
,q26_g_response
;


---Q26a-Q26p
/*
Q26.a	American sports
Q26.b	Athletics and other Olympic sports
Q26.c	Cycling
Q26.d	Cricket
Q26.e	Darts 
Q26.f	Fighting sports
Q26.g	UK club football
Q26.h	European club football (Champions League, Europa League) 
Q26.i	International football
Q26.j	Golf
Q26.k	Horse racing
Q26.l	Motor sports
Q26.m	UK & European club rugby
Q26.n	International rugby
Q26.o	Snooker/pool
Q26.p	Tennis
*/













































/*
-----Add In Survey Flag---
--Survey Data loaded in using \Git\Vespa\ad_hoc\V250 - Sports Rights Analysis\V250 - Load Survey Data (winscp).sql
--dbarnett.v250_sports_rights_survey_responses_winscp
----Add Response Flag on to Profiling Table--
alter table dbarnett.v250_Account_profiling add survey_responder tinyint;
update dbarnett.v250_Account_profiling
set survey_responder=case when b.ID_name is not null then 1 else 0 end
from dbarnett.v250_Account_profiling as a
left outer join dbarnett.v250_sports_rights_survey_responses_winscp as b
on a.account_number = b.ID_name
;

commit;

alter table dbarnett.v250_Account_profiling add survey_interest_watching_sports tinyint;
update dbarnett.v250_Account_profiling
set survey_interest_watching_sports=case when q20_c in ('6','7','8','9','10') then 1 else 0 end
from dbarnett.v250_Account_profiling as a
left outer join dbarnett.v250_sports_rights_survey_responses_winscp as b
on a.account_number = b.ID_name
;

commit;

--select sum(survey_interest_watching_sports) from dbarnett.v250_Account_profiling





select case when proportion_programmes_euro_football=0 then -1  when floor (proportion_programmes_euro_football*100)<1 then 
floor (proportion_programmes_euro_football*1000)/10 else floor(proportion_programmes_euro_football*100) end as prop_programmes_euro_football
,q26_g
,count(*) as accounts
from #individual_rights_totals_euro_football
where asked_follow_up_sports_rights_questions=1
group by q26_g
,prop_programmes_euro_football
order by prop_programmes_euro_football
;

select case when proportion_duration_euro_football=0 then -1  when floor (proportion_duration_euro_football*100)<1 then 
floor (proportion_duration_euro_football*1000)/10 else floor(proportion_duration_euro_football*100) end as prop_duration_euro_football
,q26_g
,count(*) as accounts
into #individual_rights_totals_euro_football_grouped
from #individual_rights_totals_euro_football
where asked_follow_up_sports_rights_questions=1
group by q26_g
,prop_duration_euro_football
order by prop_duration_euro_football
;


*/








*/