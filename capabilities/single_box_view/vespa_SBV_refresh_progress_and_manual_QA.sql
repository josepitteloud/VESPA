/******************************************************************************
**
** Project Vespa: Single box view
**                  - Progress tracking, manual QA
**
******************************************************************************/

-- For checkign the progress: it's done when the last item says "Single box view: refresh complete!"
EXECUTE citeam.logger_get_latest_job_events 'VespaSingleBoxView', 3;
-- Blanks are fine, large numbers are good, but 0's and -1's in the
-- integer column are a bit of an issue and should be checked out.

-- Are there any associated QA errors?
EXECUTE citeam.logger_get_latest_job_events 'VespaSingleBoxView', 2;
-- Just the usual: *. 32 subscribers with multiple accounts (supressed from SBV)
--                 *. 34k boxes without subscriber IDs and not linking to Olive
--                 *. 38k boxes without P / S decision
-- Might help to try to pull the service instance ID of the daily tables? but then, also
-- might not. Nothing we can do about any of these guys, so, meh.

-- Manual runs must go as below
-- EXECUTE citeam.logger_get_latest_job_events 'SBV test ***', 3;
-- EXECUTE citeam.logger_get_latest_job_events 'SBV test ***', 2;
-- Replacing "***" for the last letter of your DB username + first two letters of your DB username 
-- IE.: usernema = angeld -> *** being replace by DAN

/****************** Box Enablement QA ******************/

select in_vespa_panel, alternate_panel_6, alternate_panel_7, Is_Sky_View_candidate, Is_Sky_View_Selected, count(1) as hits
from vespa_single_box_view
group by in_vespa_panel, alternate_panel_6, alternate_panel_7, Is_Sky_View_candidate, Is_Sky_View_Selected
order by in_vespa_panel, alternate_panel_6, alternate_panel_7, Is_Sky_View_candidate, Is_Sky_View_Selected;

/* REDEFINED TO ABOVE...
select Open_loop_enabled, Closed_loop_enabled, Is_Sky_View_candidate, Is_Sky_View_Selected, count(1) as hits
from vespa_single_box_view
group by Open_loop_enabled, Closed_loop_enabled, Is_Sky_View_candidate, Is_Sky_View_Selected
order by Open_loop_enabled, Closed_loop_enabled, Is_Sky_View_candidate, Is_Sky_View_Selected;
*/

/* 22 Feb Test RS session:
0	0	0	0	6217    <- Why are these here? Previously reporting people who have been deselected / disabled?
0	0	0	1	2148    <- New additions to Sky View Selected
0	0	1	0	100452  <- Unused Sky View candidates
0	0	1	1	30238   <- Mostly Sky View Panel
1	0	0	0	21350   <- Open loop not yet completed enablement
1	0	0	1	431     <- Uh-oh. Requesting Vespa enablement for boxes that are Sky panel?
1	1	0	0	618220  <-- Good Vespa panel closed loop guys
*/
/* Live build of 28 Feb:
0	0	0	0	7476
0	0	0	1	2148
0	0	1	0	100490
0	0	1	1	30247
1	0	0	0	21322
1	0	0	1	431
1	1	0	0	616949
*/
/* Live build 6 March: numbers still comparable
0	0	0	0	8514
0	0	0	1	2151
0	0	1	0	100549
0	0	1	1	30247
1	0	0	0	21276
1	0	0	1	428
1	1	0	0	615958
*/
/* Build of 28th March: More of the same
0	0	0	0	12968
0	0	0	1	2157
0	0	1	0	100731
0	0	1	1	30247
1	0	0	0	21219
1	0	0	1	422
1	1	0	0	611561
*/
/* Build of 15 May 2012: wait! Where did the Vespa panel go? there's hardly any closed loop enablement at all...
0	0	0	0	91573
0	0	0	1	2172
0	0	1	0	101265
0	0	1	1	30247
1	0	0	0	557629
1	0	0	1	407
1	1	0	0	1950
*/
/* Build of 22 May: mostly more sensible
0	0	0	0	92389
0	0	0	1	2172
0	0	1	0	101334
0	0	1	1	30247
1	0	0	0	148361  <- There are still a lot of guys in here; probably all those Trumped boxes in the next QA section...
1	0	0	1	405
1	1	0	0	410402  <- decent numbers on Vespa panel now, at least in comparison to last week
1	1	0	1	2       <- WTF? Boxes on Sky View and Vespa at the same time? (Investigate Trumped...)
*/

/****************** Panel ID QA ******************/

select Panel_ID_Vespa, status_vespa, count(1) as boxes
from vespa_single_box_view
where Is_Sky_View_candidate = 0 and Is_Sky_View_Selected = 0
group by Panel_ID_Vespa, status_vespa
order by Panel_ID_Vespa, status_vespa 

/* REDEFINED TO ABOVE...
select Panel_ID_4_cells_confirm, Status_Vespa, count(1) as boxes
from vespa_single_box_view
where Is_Sky_View_candidate = 0 and Is_Sky_View_Selected = 0 -- Don't care about Sky View boxes in this view
group by Panel_ID_4_cells_confirm, Status_Vespa
order by Panel_ID_4_cells_confirm, Status_Vespa;

/* So there's still a lot of stuff floating around as of 8 Feb 2012, CCN not processed yet...
0       DisableFailed   1
0       DisablePending  900
0       Disabled        71045
0       EnableFailed    120
0       EnablePending   314974
0       Enabled 585934
0       Trumped 36
1       DisablePending  70707
1       Disabled        19
1       EnableFailed    49
1       EnablePending   224542
1       Enabled 337914
1       Trumped 50
*/
/* The RS testing instance on 22 Feb 2012:
0		                    225     <- Old boxes that are in the log snapshot but are no longer active
0	    DisableRequested	5918    <- shouldn't be here long
0	    Enabled	            3
1		                    21421   <- Some of these are reporting, some are not, there's a mix of panel IDs in here too :/
1	    Enabled	            618220  <- good
*/
/* Of 28 Feb:
0	        	185
0	Disabled	5918
0	Enabled	    3
1		        21421
1	Enabled	    618220
*/
/* As of 6th of March;
0		        186
0	Disabled	5918
0	Enabled	    3
1   		    21421
1	Enabled	    618220
*/
/* 13th MArch: DisablePending is back!
Panel_ID_4_cells_confirm	Status_Vespa	boxes
0		            183
0	DisablePending	3107
0	Disabled	    6276
0	Enabled	        3
1                   21421
1	Enabled	        614755
*/
/* Build of 28th March: again, similar
0		        186
0	Disabled	9479
0	Enabled	    3
1		        21421
1	Enabled	    614659
*/
/* Build of 15th May: barely anything enabled, things mostly look EnablePending...
0		179
0	DisablePending	6
0	Disabled	81783
0	EnablePending	5563
0	Enabled	2
0	Trumped	15
1		21386
1	DisablePending	5279
1	EnablePending	536594
1	Enabled	298
1	Trumped	47
*/
-- So... 2012-05-15 didn't end up being a sensible build at all... not a good example to compare
/* Build of 22 May: Wow, that's a lot of Trumped. What's going on?
0		179
0	DisablePending	11
0	Disabled	81783
0	EnablePending	3738
0	Enabled	1822
0	Trumped	15
1		21386
1	DisablePending	5343
1	EnablePending	368926
1	Enabled	40333
1	Trumped	127616
*/

/****************** Primary / Secondary box flag QA ******************/

select PS_Olive, PS_Vespa, PS_inferred_primary, PS_flag, PS_source
        ,count(1) as hits
        ,sum(case when account_number is not null then 1 else 0 end) as valid_account_numbers
        ,count(distinct account_number) as distinct_account_numbers
from vespa_single_box_view
group by PS_Olive, PS_Vespa, PS_inferred_primary, PS_flag, PS_source
order by PS_Olive, PS_Vespa, PS_inferred_primary, PS_flag, PS_source;
/* Wait, so, there are 24962 accounts which don't have MR subscriptions going by SAV? Really?
P	P	0	P	Both agree	301367	301367	301367
P	U	0	P	Olive	    148292	148292	148291
S	S	0	S	Both agree	75337	75337	69564
S	U	0	S	Olive	    124488	124488	106776
U	P	0	P	Vespa	    205	205	205
U	S	0	S	Vespa	    164	164	163
U	U	0	U	Both agree	26934	26934	24962
U	U	1	P	Inferred	128	128	128
*/ 
/* RS Test build of the 22nd:
P	P	0	P	Both agree	343735	343735	343735
P	U	0	P	Olive	    183553	183553	183552
S	S	0	S	Both agree	77068	77068	71284
S	U	0	S	Olive	    140416	140416	120264
U	P	0	P	Vespa	    207	    207	    207
U	S	0	S	Vespa	    170	    170	    169
U	U	0	U	Olive	    32646	32646	30129
U	U	1	P	Inferred	1261	1261	1261
*/
/* 28th Feb:
P	P	0	P	Both agree	346986	346986	346986
P	U	0	P	Olive	    180255	180255	180254
S	S	0	S	Both agree	78260	78260	72324
S	U	0	S	Olive	    139253	139253	119298
U	P	0	P	Vespa	    214	    214	    214
U	S	0	S	Vespa	    174	    174	    173
U	U	0	U	Olive	    32647	32647	30131
U	U	1	P	Inferred	1274	1274	1274
*/
/* 6th March: numbers still not particuarly good, but w/e
P	P	0	P	Both agree	349566	349566	349566
P	U	0	P	Olive	    177632	177632	177631
S	S	0	S	Both agree	79163	79163	73116
S	U	0	S	Olive	    138389	138389	118596
U	P	0	P	Vespa	    226	    226	    226
U	S	0	S	Vespa	    180	    180	    178
U	U	0	U	Olive	    32676	32676	30147
U	U	1	P	Inferred	1291	1291	1291
*/
/* So now, by the 13th of March, we have collissions in definitions. Awesome. Because P/S wasn't already complicated enough.
P	P	0	P	Both agree	350934	350934	350934
P	S	0	!	Collision!	7	7	7
P	U	0	P	Olive	    176255	176255	176254
S	P	0	!	Collision!	10	10	10
S	S	0	S	Both agree	79797	79797	73675
S	U	0	S	Olive	    137744	137744	118042
U	P	0	P	Vespa	    222	222	222
U	S	0	S	Vespa	    204	204	202
U	U	0	U	Olive	    32652	32652	30123
U	U	1	P	Inferred	1297	1297	1297

*/
/* March 28th: no more claches between Vespa and Olive DBs, that's good.
P	P	0	P	Both agree	354643	354643	354643
P	U	0	P	Olive	    172435	172435	172434
S	S	0	S	Both agree	81348	81348	75034
S	U	0	S	Olive	    136345	136345	116903
U	P	0	P	Vespa	    240	    240	    240
U	S	0	S	Vespa	    233	    233	    232
U	U	0	U	Olive	    32684	32684	30137
U	U	1	P	Inferred	1377	1377	1377
*/
/* May 5th: heh, this is the build where everything fell off the Vespa panel
P	P	0	P	Both agree	18645	18645	18645
P	U	0	P	Olive	    508562	508562	508560
S	S	0	S	Both agree	5794	5794	5175
S	U	0	S	Olive	    215711	215711	177089
U	P	0	P	Vespa	    23	    23	    23
U	S	0	S	Vespa	    21	    21	    21
U	U	0	U	Olive	    34841	34841	31991
U	U	1	P	Inferred	1646	1646	1646
*/
/* May 22nd build figures:
P	P	0	P	Both agree	352782	352782	352782
P	U	0	P	Olive	    174380	174380	174379
S	S	0	S	Both agree	83485	83485	76694
S	U	0	S	Olive	    138257	138257	118205
U	P	0	P	Vespa	    268	    268	    268
U	S	0	S	Vespa	    284	    284	    281
U	U	0	U	Olive	    34315	34315	31523
U	U	1	P	Inferred	1541	1541	1541
*/


/****************** Enablement date QA ******************/

select Enablement_date, Enablement_date_source, count(1) as hits
from vespa_single_box_view
group by Enablement_date, Enablement_date_source
order by  Enablement_date, Enablement_date_source;
/* Feb 28 build: still no major changes, no new enablement lumps
<null>      <null>		103602      <- these guys are Sky View Candidates that are not Selected?
2011-05-12	historic	7022
2011-05-18	historic	7090
2011-05-19	historic	7075
2011-05-20	historic	7198
2011-05-25	historic	7204
2011-06-02	historic	14717
2011-06-22	historic	20133
2011-06-24	historic	1298
2011-07-11	historic	17921
2011-07-27	historic	5098
2011-07-28	historic	1668
2011-08-04	historic	1420
2011-08-08	historic	1555
2011-08-10	historic	1538
2011-08-11	historic	1743
2011-08-13	historic	3333
2011-08-14	historic	1722
2011-08-15	historic	1707
2011-08-16	historic	1582
2011-08-17	historic	1445
2011-10-17	writeback	1
2011-10-20	writeback	19
2011-11-08	historic	9017
2011-11-08	writeback	74785
2011-11-15	historic	66777
2011-11-15	writeback	29
2011-11-18	historic	19877
2011-11-18	writeback	59
2011-11-24	historic	112541
2011-11-24	writeback	217468
2012-01-26	historic	29577
2012-01-26	writeback	14
2012-02-20	vss_created_dt	2
2012-02-21	Sky View	32826
*/
-- 6th March: of note: no new enablement dates, but the NULL guys have increased by 60 :-/
/* So now we have null boxes that are not Sky View, because those are flagged separately...
		4641
2011-05-10	Sky View	100551
2011-05-12	historic	7022
2011-05-18	historic	7090
2011-05-19	historic	7075
2011-05-20	historic	7198
2011-05-25	historic	7203
2011-06-02	historic	14716
2011-06-22	historic	20133
2011-06-24	historic	1298
2011-07-11	historic	17921
2011-07-27	historic	5098
2011-07-28	historic	1668
2011-08-04	historic	1420
2011-08-08	historic	1555
2011-08-10	historic	1538
2011-08-11	historic	1743
2011-08-13	historic	3333
2011-08-14	historic	1722
2011-08-15	historic	1707
2011-08-16	historic	1582
2011-08-17	historic	1445
2011-10-17	writeback	1
2011-10-20	writeback	18
2011-11-08	historic	9017
2011-11-08	writeback	74416
2011-11-15	historic	66777
2011-11-15	writeback	29
2011-11-18	historic	19877
2011-11-18	writeback	59
2011-11-24	historic	112541
2011-11-24	writeback	216309
2012-01-26	historic	29577
2012-01-26	writeback	14
2012-02-20	vss_created_dt	2
2012-02-21	Sky View	32826
*/

/* March 28th: Some of the request dates came back? Okay. We changed the duplication
** treatment from the historic table but apparently there's stuff in the request date
** too now, so whatever.
		179
2011-05-10	Sky View	100731
2011-05-12	historic	305
2011-05-12	vss_request_dt	6660
2011-05-18	historic	301
2011-05-18	vss_request_dt	6710
2011-05-19	historic	312
2011-05-19	vss_request_dt	6684
2011-05-20	historic	280
2011-05-20	vss_request_dt	6826
2011-05-25	historic	259
2011-05-25	vss_request_dt	6870
2011-06-02	historic	566
2011-06-02	vss_request_dt	13971
2011-06-21	vss_request_dt	17
2011-06-22	historic	804
2011-06-22	vss_request_dt	19073
2011-06-24	historic	41
2011-06-24	vss_request_dt	1256
2011-07-11	historic	654
2011-07-11	vss_request_dt	17034
2011-07-27	historic	173
2011-07-27	vss_request_dt	4951
2011-07-28	historic	44
2011-07-28	vss_request_dt	1600
2011-08-04	historic	42
2011-08-04	vss_request_dt	1408
2011-08-08	historic	46
2011-08-08	vss_request_dt	1495
2011-08-10	historic	40
2011-08-10	vss_request_dt	1529
2011-08-11	historic	40
2011-08-11	vss_request_dt	1687
2011-08-13	historic	84
2011-08-13	vss_request_dt	3215
2011-08-14	historic	51
2011-08-14	vss_request_dt	1647
2011-08-15	historic	41
2011-08-15	vss_request_dt	1655
2011-08-16	historic	50
2011-08-16	vss_request_dt	1533
2011-08-17	historic	38
2011-08-17	vss_request_dt	1381
2011-09-06	vss_request_dt	102
2011-09-30	vss_request_dt	137
2011-10-20	writeback	1
2011-11-08	historic	157
2011-11-08	vss_request_dt	75173
2011-11-08	writeback	7972
2011-11-15	historic	2358
2011-11-15	vss_request_dt	63423
2011-11-15	writeback	27
2011-11-18	historic	745
2011-11-18	vss_request_dt	18842
2011-11-18	writeback	47
2011-11-24	historic	3378
2011-11-24	vss_request_dt	107693
2011-11-24	writeback	215764
2012-01-26	historic	522
2012-01-26	vss_request_dt	28426
2012-01-26	writeback	14
2012-02-21	Sky View	32826
2012-02-21	historic	5918
2012-03-09	historic	358
2012-03-12	historic	3107
2012-03-23	historic	32
*/

/* May 5th: this date is not so broken (though the rest of the panel flags are)
		5426
2011-05-10	Sky View	101265
2011-05-12	historic	6948
2011-05-12	vss_request_dt	3
2011-05-18	historic	7011
2011-05-18	vss_request_dt	7
2011-05-19	historic	7008
2011-05-19	vss_request_dt	3
2011-05-20	historic	7111
2011-05-20	vss_request_dt	4
2011-05-25	historic	7142
2011-05-25	vss_request_dt	4
2011-06-02	historic	14563
2011-06-02	vss_request_dt	8
2011-06-21	historic	17
2011-06-22	historic	19909
2011-06-22	vss_request_dt	13
2011-06-24	historic	1312
2011-07-11	historic	17734
2011-07-11	vss_request_dt	10
2011-07-27	historic	5140
2011-07-27	vss_request_dt	1
2011-07-28	historic	1643
2011-08-04	historic	1454
2011-08-08	historic	1540
2011-08-08	vss_request_dt	7
2011-08-10	historic	1576
2011-08-11	historic	1728
2011-08-11	vss_request_dt	1
2011-08-13	historic	3308
2011-08-13	vss_request_dt	2
2011-08-14	historic	1701
2011-08-15	historic	1701
2011-08-15	vss_request_dt	1
2011-08-16	historic	1581
2011-08-17	historic	1428
2011-09-06	historic	102
2011-09-30	historic	137
2011-10-17	writeback	1
2011-10-20	writeback	1
2011-11-08	historic	75285
2011-11-08	vss_request_dt	45
2011-11-08	writeback	2671
2011-11-15	historic	65747
2011-11-15	vss_request_dt	28
2011-11-15	writeback	29
2011-11-18	historic	19571
2011-11-18	vss_request_dt	14
2011-11-18	writeback	59
2011-11-24	historic	110987
2011-11-24	vss_request_dt	42
2011-11-24	writeback	7522
2012-01-26	historic	28924
2012-01-26	vss_request_dt	17
2012-01-26	writeback	14
2012-02-21	Sky View	32826
2012-02-21	historic	5918
2012-03-09	historic	358
2012-03-12	historic	3107
2012-03-23	historic	32
2012-03-28	historic	1385
2012-04-04	historic	441
2012-04-06	historic	50
2012-04-10	historic	21798
2012-04-13	historic	40
2012-04-19	historic	907
2012-04-25	historic	94
2012-05-04	historic	864
2012-05-14	historic	187917
*/
/* Build from 22nd of May:
		3707
2011-05-10	Sky View	101334
2011-05-12	historic	6557
2011-05-12	vss_request_dt	3
2011-05-18	historic	6840
2011-05-18	vss_request_dt	7
2011-05-19	historic	6797
2011-05-19	vss_request_dt	3
2011-05-20	historic	6936
2011-05-20	vss_request_dt	4
2011-05-25	historic	6935
2011-05-25	vss_request_dt	4
2011-06-02	historic	13680
2011-06-02	vss_request_dt	8
2011-06-21	historic	16
2011-06-22	historic	18216
2011-06-22	vss_request_dt	13
2011-06-24	historic	1281
2011-07-11	historic	15723
2011-07-11	vss_request_dt	10
2011-07-27	historic	4427
2011-07-27	vss_request_dt	1
2011-07-28	historic	1589
2011-08-04	historic	1396
2011-08-08	historic	1471
2011-08-08	vss_request_dt	7
2011-08-10	historic	1517
2011-08-11	historic	1655
2011-08-11	vss_request_dt	1
2011-08-13	historic	3153
2011-08-13	vss_request_dt	2
2011-08-14	historic	1589
2011-08-15	historic	1621
2011-08-15	vss_request_dt	1
2011-08-16	historic	1483
2011-08-17	historic	1312
2011-09-06	historic	99
2011-09-30	historic	130
2011-10-17	writeback	1
2011-10-20	writeback	1
2011-11-08	historic	70720
2011-11-08	vss_request_dt	45
2011-11-08	writeback	2671
2011-11-15	historic	60704
2011-11-15	vss_request_dt	28
2011-11-15	writeback	29
2011-11-18	historic	18367
2011-11-18	vss_request_dt	14
2011-11-18	writeback	59
2011-11-24	historic	104304
2011-11-24	vss_request_dt	42
2011-11-24	writeback	7514
2012-01-26	historic	27648
2012-01-26	vss_request_dt	17
2012-01-26	writeback	13
2012-02-21	Sky View	32826
2012-02-21	historic	5918
2012-03-09	historic	358
2012-03-12	historic	3107
2012-03-23	historic	32
2012-03-28	historic	1385
2012-04-04	historic	441
2012-04-06	historic	50
2012-04-10	historic	21798
2012-04-13	historic	40
2012-04-19	historic	907
2012-04-25	historic	94
2012-05-04	historic	864
2012-05-14	historic	173921
2012-05-15	historic	5
2012-05-15	vss_request_dt	9360 <- these later request_dt sources might be migrations to 12 that look like enablements, which could be the source of the reporting quality metrics inconsistency if they were returning data before this date on panel 4...
2012-05-16	historic	1
2012-05-16	vss_request_dt	4654
2012-05-17	vss_request_dt	4263
2012-05-18	historic	2
2012-05-18	vss_request_dt	4200
2012-05-21	historic	1
2012-05-21	vss_request_dt	14545
2012-05-22	historic	2
2012-05-22	vss_request_dt	4833
*/

/****************** Sky View panel QA ******************/

-- So: are there things returning data that aren't on either the main enablement list
-- (Which is months old) or the boxes-selected-for-data-return list (which came into
-- being in early Feb 2012)?
select Is_Sky_View_candidate, Is_Sky_View_Selected, Panel_ID_Vespa, count(1) as boxes
from vespa_single_box_view
group by Is_Sky_View_candidate, Is_Sky_View_Selected, Panel_ID_Vespa
order by Is_Sky_View_candidate, Is_Sky_View_Selected, Panel_ID_Vespa;
/* 8 Feb 2012 gives us:
0	0		1213944
0	0	1	445         <- Boxes that have unexpectedly reported to Sky View
0	0	3	2
0	0	4	391802
0	0	5	98
0	1		751         <- non-reporting boxes that are selected but not in the old Sky View enablement list
0	1	1	1828        <- Reporting boxes that are on the selection list but not on the months old enablement list
1	0		100026
1	0	1	346
1	1		8622        <- non-reporting boxes that are on all the Sky View lists
1	1	1	21610       <- well behaved reporting Sky View boxes
*/
/* RS Test build 22 Feb 2012
0	0		234013
0	0	1	413
0	0	3	2
0	0	4	411241
0	0	5	114
0	0	9	4
0	1		657
0	1	1	1922
1	0		100075
1	0	1	377
1	1		7803
1	1	1	22435
*/
/* Live build 28 Feb
0	0		229745
0	0	1	380
0	0	3	2
0	0	4	415502
0	0	5	116
0	0	9	2
0	1		631
0	1	1	1948
1	0		100112
1	0	1	378
1	1		7624
1	1	1	22623
*/
/* 6 March build: again messy, but nothing surprising
0	0		226404
0	0	1	380
0	0	3	2
0	0	4	418832
0	0	5	128
0	0	9	2
0	1		613
0	1	1	1966
1	0		100157
1	0	1	391
1	0	3	1
1	1		7476
1	1	1	22771
*/
/* 28th March: Oh hey panel 12 is onboard. Good thing we're checking all of this stuff.
0	0		217849
0	0	1	380
0	0	3	1
0	0	4	427357
0	0	5	157
0	0	9	2
0	0	12	2
0	1		560
0	1	1	2019
1	0		100301
1	0	1	429
1	0	3	1
1	1		7066
1	1	1	23181
*/
/* May 5th: fortunately the Sky View panel is mostly good...
0	0		214246
0	0	1	380
0	0	3	1
0	0	4	436326
0	0	5	172
0	0	9	2
0	0	12	25
0	1		492
0	1	1	2087
1	0		100745
1	0	1	518
1	0	3	1
1	0	4	1
1	1		6496
1	1	1	23751
*/
/* May 22nd: 
0	0		213105
0	0	1	379
0	0	3	1
0	0	4	394705
0	0	5	178
0	0	9	2
0	0	12	42782
0	1		482
0	1	1	2097
1	0		100811
1	0	1	521
1	0	3	1
1	0	4	1
1	1		6431
1	1	1	23816
*/

/****************** Canonical panel flag QA ******************/

select panel, count(1) as hits
from vespa_single_box_view
group by panel order by panel;
/* That's pretty much what we expected, though the CLASH! items are a sadface.
	132740
CLASH!	131
SKYVIEW	32826
VESPA	613425
*/
/* March 28th: more of the same.
	134918
CLASH!	144
SKYVIEW	32826
VESPA	611417
*/
/* May 5th: Vespa Panel Fully Broken
	750467
CLASH!	30
SKYVIEW	32826
VESPA	1920
*/
/* May 22nd: more of the same, except... Vespa has fallen off by a *lot*
	342084
CLASH!	124
SKYVIEW	32824
VESPA	410280
*/