-- All Sports customers
	drop table sports;

create table sports(account_number varchar(20));

insert into sports
  select csh.account_number
    from cust_subs_hist as csh
         inner join cust_entitlement_lookup as cel on csh.current_short_description = cel.short_description
         inner join cust_single_account_view as sav on csh.account_number = sav.account_number
         											and sav.pty_country = 'Great Britain'
                                                    and sav.cust_active_dtv = 1
   where csh.status_code in ('AC')
     and csh.subscription_sub_type ='DTV Primary Viewing'
     and effective_to_dt = '9999-09-09'
     and prem_sports > 0
group by csh.account_number
;

select count() from sports;
count()
4,145,623 -- ALL
count()
3,915,271 -- UK only



-- Current panel membership
select
	panel_no
    ,count()
    ,count(distinct bas.account_number)
from vespa_panel_status bas
    inner join #sports as spo on bas.account_number = spo.account_number
group by panel_no
order by panel_no
;
panel_no	count()	count(distinct VESPA_PANEL_STATUS.account_number)
1	484	484
5	1244351	1244351
6	211365	211365
8	15478	15478
11	169341	169341
12	188833	188833
13	2417	2417
15	62599	62599


select	count()
from vespa_panel_status bas
    inner join #sports as spo on bas.account_number = spo.account_number
;
count()
1,894,868 -- already on a panel
count()
1,892,567 -- already on a UK panel




--BB breakdown by account
  select knockout_level_bb
        ,knockout_reason_bb
        ,count(1)
    from /*vespa_analysts.*/Waterfall_Base as bas
    inner join sports as spo on bas.account_number = spo.account_number
group by knockout_level_bb
        ,knockout_reason_bb
order by knockout_level_bb
;
knockout_level_bb   knockout_reason_bb  count(1)
7   DTV account 5823
8   Country 228913
10  Surname 36
11  Standard_accounts   49043
13  Hibernators 330
14  Not_vespa_panel 1809575
20  Darwin  878082
24  Last_callback_dt    359935
130 On Demand downloads 97360
9999    Potential BB panellist  715,417 -- potential Sports UK BB panellist





--PSTN breakdown by account
  select min(knockout_level_pstn) as level
        ,knockout_reason_pstn
        ,count(1)
    from vespa_analysts.Waterfall_Base as bas
    inner join #sports as spo on bas.account_number = spo.account_number
group by knockout_reason_pstn
order by level
;
level	knockout_reason_pstn	count(1)
7	Potential PSTN panellist	109,350
8	Country	208993
10	Surname	38
11	Standard_accounts	40917
14	Not_vespa_panel	1859414
24	Last_callback_dt	1296520
113	Hibernators	299
122	Prefix information unknown	300747
123	Empty prefix	3638




--Mix breakdown by account
  select max(knockout_level_mix) as level
        ,knockout_reason_mix
        ,count(1)
    from vespa_analysts.Waterfall_Base as bas
    inner join #sports as spo on bas.account_number = spo.account_number
group by knockout_reason_mix
order by level
;
level	knockout_reason_mix	count(1)
9999	Already accounted for	3,819,916






select
		panel_no
    ,	case
    		when spt.account_number is null then 0
            else 								1
        end	as sports_customer
    ,	count()	accounts
from
				vespa_panel_status vps
	left join	#sports           spt	on	vps.account_number = spt.account_number
-- where   spt.account_number is not null
group by panel_no, sports_customer
order by panel_no, sports_customer
;
panel_no	sports_customer	accounts
1	0	4337
1	1	484
5	0	1,778,895 -- select random 715k from this for disablement
5	1	1244290
6	0	261933
6	1	211337
8	0	26602
8	1	15475
11	0	293034
11	1	169326
12	0	204420
12	1	188807
13	0	1446
13	1	2315
15	0	26140
15	1	60533





drop table tmp_panel_5_disablements;
select
		t1.account_number
	,	cast(vps.card_subscriber_id as int) as subscriber_id
    ,	rnk
into	tmp_panel_5_disablements
from
        		(
                    select
                    		*
                    	,	dense_rank() over (order by rnd) rnk
                    from	(
                                select
                                		vps.account_number
                                	,	rand(cast(vps.account_number as bigint) * datepart(us,now())) rnd
                                --     ,   rand(vps.account_number)
                                from
                                				vespa_panel_status	vps
                                	left join	sports           	spt	on	vps.account_number	=	spt.account_number
                                where
                                		vps.panel_no		=	5
                                	and	spt.account_number	is null
                    		)	t0
        		)	t1
	inner join	vespa_subscriber_status		vps	on	t1.account_number	=	vps.account_number
    											and	vps.result			=	'Enabled'
where	t1.rnk <= 1172712--715417
;









select
		b.panel_no
	,	count()
from	tmp_panel_5_disablements	a
inner join	vespa_panel_status b on a.account_number = b.account_number
group by	b.panel_no
order by	b.panel_no
;
-- all existing panellists. good
panel_no	count()
5	715417

select count()
from	tmp_panel_5_disablements	a
inner join	#sports b on a.account_number = b.account_number
;
-- 0 sports customers here. good.






-- select accounts for enablement
drop table tmp_panel_5_sports_enablements;
select	--top 20
		account_number
	,	knockout_level_bb
    ,	knockout_reason_bb
	,	subscriber_id
	,	callback_day
    ,	account_callback_day
	,	case
    		when account_callback_day between 0 and 3	then	'1-3'
    		when account_callback_day between 4 and 6	then	'4-6'
    		when account_callback_day between 8 and 9	then	'7-9'
    		when account_callback_day between 10 and 12	then	'10-12'
    		when account_callback_day > 12				then	'13-28'
            else									 			'1-3'
        end	as	enablement_batch
into    tmp_panel_5_sports_enablements
from	(
            select	--	top 20
            		bas.account_number
            	,	bas.knockout_level_bb
                ,	bas.knockout_reason_bb
            	,	cast(csl.card_subscriber_id as int)	as	subscriber_id
            	,	csi.si_callback_day					as	callback_day
                ,	min(csi.si_callback_day)	over	(partition by bas.account_number)	account_callback_day
            from
            				Waterfall_Base				as	bas
                inner join	sports						as	spo	on	bas.account_number	=	spo.account_number
                left join	CUST_SERVICE_INSTANCE		as	csi	on	bas.account_number	=	csi.account_number
                												and	csi.effective_to_dt	=	'9999-09-09'
            	left join	cust_card_subscriber_link	as	csl	on	bas.account_number		=	csl.account_number
	               												and	csi.service_instance_id	=	csl.service_instance_id
                												and	csl.current_flag		=	'Y'
                                                                and	csl.effective_to_dt		=	'9999-09-09'
            where	bas.knockout_level_bb	in	(
            											9999
                                                    ,	130	-- On Demand downloads
                                                    ,	24	-- CA callback
                                                )
            	and	subscriber_id			is not null
		)	t0
group by	--    callback_day, batch
		account_number
	,	knockout_level_bb
    ,	knockout_reason_bb
	,	subscriber_id
	,	callback_day
    ,	account_callback_day
    ,	enablement_batch
-- order by    callback_day, batch
;


select count() from tmp_panel_5_sports_enablements;--1649520
select count() from tmp_panel_5_sports_enablements_test;--1574012


select *
from tmp_panel_5_sports_enablements
-- where enablement_batch = '1-10'
where enablement_batch = '11-28'
and knockout_level_bb = 24
;




select
		t0.knockout_level_bb
    ,	t0.knockout_reason_bb
	,	t0.enablement_batch
    ,	disable_rnk_to - accounts + 1													as	disable_rnk_from
    ,   sum(t0.accounts)	over	(rows between unbounded preceding and current row)	as  disable_rnk_to
from	(
            select
            		knockout_level_bb
                ,	knockout_reason_bb
            	,	enablement_batch
                ,	count(distinct account_number)	as	accounts
                ,	count(distinct subscriber_id)	as	subs
            from	tmp_panel_5_sports_enablements
            group by
            		knockout_level_bb
                ,	knockout_reason_bb
            	,	enablement_batch
--             order by
--                     knockout_level_bb
--                 ,   knockout_reason_bb
--                 ,   enablement_batch
		)	t0
;


select
		count()
	,	count(distinct account_number)
from	tmp_panel_5_sports_enablements
;


select min(rnk), max(rnk), count(distinct account_number), count(distinct subscriber_id) from tmp_panel_5_disablements where rnk between 381960 and 496862;
select min(rnk), max(rnk), count(distinct account_number), count(distinct subscriber_id) from tmp_panel_5_disablements where rnk between 284600 and 301606;
select min(rnk), max(rnk), count(distinct account_number), count(distinct subscriber_id) from tmp_panel_5_disablements where rnk between 1 and 47544;
select min(rnk), max(rnk), count(distinct account_number), count(distinct subscriber_id) from tmp_panel_5_disablements where rnk between 959061 and 1040710;
select min(rnk), max(rnk), count(distinct account_number), count(distinct subscriber_id) from tmp_panel_5_disablements where rnk between 361718 and 373938;
select min(rnk), max(rnk), count(distinct account_number), count(distinct subscriber_id) from tmp_panel_5_disablements where rnk between 228456 and 261801;
select min(rnk), max(rnk), count(distinct account_number), count(distinct subscriber_id) from tmp_panel_5_disablements where rnk between 1040711 and 1097341;
select min(rnk), max(rnk), count(distinct account_number), count(distinct subscriber_id) from tmp_panel_5_disablements where rnk between 373939 and 381959;
select min(rnk), max(rnk), count(distinct account_number), count(distinct subscriber_id) from tmp_panel_5_disablements where rnk between 261802 and 284599;
select min(rnk), max(rnk), count(distinct account_number), count(distinct subscriber_id) from tmp_panel_5_disablements where rnk between 496863 and 580106;
select min(rnk), max(rnk), count(distinct account_number), count(distinct subscriber_id) from tmp_panel_5_disablements where rnk between 301607 and 312586;
select min(rnk), max(rnk), count(distinct account_number), count(distinct subscriber_id) from tmp_panel_5_disablements where rnk between 47545 and 79690;
select min(rnk), max(rnk), count(distinct account_number), count(distinct subscriber_id) from tmp_panel_5_disablements where rnk between 312587 and 361717;
select min(rnk), max(rnk), count(distinct account_number), count(distinct subscriber_id) from tmp_panel_5_disablements where rnk between 79691 and 228455;
select min(rnk), max(rnk), count(distinct account_number), count(distinct subscriber_id) from tmp_panel_5_disablements where rnk between 580107 and 959060;






select * from tmp_panel_5_disablements where rnk between 457296	and 572235;
select * from tmp_panel_5_sports_enablements where knockout_level_bb = 9999	and enablement_batch	=	'1-3';
select * from tmp_panel_5_disablements where rnk between 359936	and 376942;
select * from tmp_panel_5_sports_enablements where knockout_level_bb = 130	and enablement_batch	=	'1-3';
select * from tmp_panel_5_disablements where rnk between 1	and 122873;
select * from tmp_panel_5_sports_enablements where knockout_level_bb = 24	and enablement_batch	=	'1-3';
select * from tmp_panel_5_disablements where rnk between 1034432	and 1116082;
select * from tmp_panel_5_sports_enablements where knockout_level_bb = 9999	and enablement_batch	=	'4-6';
select * from tmp_panel_5_disablements where rnk between 437054	and 449274;
select * from tmp_panel_5_sports_enablements where knockout_level_bb = 130	and enablement_batch	=	'4-6';
select * from tmp_panel_5_disablements where rnk between 303792	and 337138;
select * from tmp_panel_5_sports_enablements where knockout_level_bb = 24	and enablement_batch	=	'4-6';
select * from tmp_panel_5_disablements where rnk between 1116083	and 1172712;
select * from tmp_panel_5_sports_enablements where knockout_level_bb = 9999	and enablement_batch	=	'7-9';
select * from tmp_panel_5_disablements where rnk between 449275	and 457295;
select * from tmp_panel_5_sports_enablements where knockout_level_bb = 130	and enablement_batch	=	'7-9';
select * from tmp_panel_5_disablements where rnk between 337139	and 359935;
select * from tmp_panel_5_sports_enablements where knockout_level_bb = 24	and enablement_batch	=	'7-9';
select * from tmp_panel_5_disablements where rnk between 572236	and 655478;
select * from tmp_panel_5_sports_enablements where knockout_level_bb = 9999	and enablement_batch	=	'10-12';
select * from tmp_panel_5_disablements where rnk between 376943	and 387922;
select * from tmp_panel_5_sports_enablements where knockout_level_bb = 130	and enablement_batch	=	'10-12';
select * from tmp_panel_5_disablements where rnk between 122874	and 155019;
select * from tmp_panel_5_sports_enablements where knockout_level_bb = 24	and enablement_batch	=	'10-12';
select * from tmp_panel_5_disablements where rnk between 655479	and 1034431;
select * from tmp_panel_5_disablements where rnk between 387923	and 437053;
select * from tmp_panel_5_sports_enablements where knockout_level_bb = 130	and enablement_batch	=	'13-28';
select * from tmp_panel_5_disablements where rnk between 155020	and 303791;
select * from tmp_panel_5_sports_enablements where knockout_level_bb = 24	and enablement_batch	=	'13-28';
select * from tmp_panel_5_sports_enablements where knockout_level_bb = 9999	and enablement_batch	=	'13-28';







select	top 100 *
-- count(), count(distinct account_number), count(distinct subscriber_id)
from tmp_panel_5_sports_enablements
where knockout_level_bb = 24	and enablement_batch	=	'1-3'
;




select * from tmp_panel_5_disablements where rnk between 381960 and 496862;
select * from tmp_panel_5_sports_enablements where knockout_level_bb = 9999 and enablement_batch = '1-3';
select * from tmp_panel_5_disablements where rnk between 284600 and 301606;
select * from tmp_panel_5_disablements where rnk between 1 and 47544;
select * from tmp_panel_5_sports_enablements where knockout_level_bb = 130 and enablement_batch = '1-3';
select * from tmp_panel_5_sports_enablements where knockout_level_bb = 24 and enablement_batch = '1-3';
select * from tmp_panel_5_disablements where rnk between 959061 and 1040710;
select * from tmp_panel_5_sports_enablements where knockout_level_bb = 9999 and enablement_batch = '4-6';
select * from tmp_panel_5_disablements where rnk between 361718 and 373938;
select * from tmp_panel_5_sports_enablements where knockout_level_bb = 130 and enablement_batch = '4-6';
select * from tmp_panel_5_disablements where rnk between 228456 and 261801;
select * from tmp_panel_5_sports_enablements where knockout_level_bb = 24 and enablement_batch = '4-6';
select * from tmp_panel_5_disablements where rnk between 1040711 and 1097341;
select * from tmp_panel_5_sports_enablements where knockout_level_bb = 9999 and enablement_batch = '7-9';
select * from tmp_panel_5_disablements where rnk between 373939 and 381959;
select * from tmp_panel_5_sports_enablements where knockout_level_bb = 130 and enablement_batch = '7-9';
select * from tmp_panel_5_disablements where rnk between 261802 and 284599;
select * from tmp_panel_5_sports_enablements where knockout_level_bb = 24 and enablement_batch = '7-9';
select * from tmp_panel_5_disablements where rnk between 496863 and 580106;
select * from tmp_panel_5_sports_enablements where knockout_level_bb = 9999 and enablement_batch = '10-12';
select * from tmp_panel_5_disablements where rnk between 301607 and 312586;
select * from tmp_panel_5_sports_enablements where knockout_level_bb = 130 and enablement_batch = '10-12';
select * from tmp_panel_5_disablements where rnk between 47545 and 79690;
select * from tmp_panel_5_sports_enablements where knockout_level_bb = 24 and enablement_batch = '10-12';
select * from tmp_panel_5_disablements where rnk between 580107 and 959060;
select * from tmp_panel_5_disablements where rnk between 312587 and 361717;
select * from tmp_panel_5_sports_enablements where knockout_level_bb = 130 and enablement_batch = '13-28';
select * from tmp_panel_5_disablements where rnk between 79691 and 228455;
select * from tmp_panel_5_sports_enablements where knockout_level_bb = 24 and enablement_batch = '13-28';
select * from tmp_panel_5_sports_enablements where knockout_level_bb = 9999 and enablement_batch = '13-28';



