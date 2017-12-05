*------------------------------------------------------------*;
* EM SCORE CODE;
*------------------------------------------------------------*;
*------------------------------------------------------------*;
* TOOL: Input Data Source;
* TYPE: SAMPLE;
* NODE: Ids2;
*------------------------------------------------------------*;
*------------------------------------------------------------*;
* TOOL: Filtering;
* TYPE: MODIFY;
* NODE: Filter;
*------------------------------------------------------------*;


length _FILTERFMT1  $200;
drop _FILTERFMT1 ;
_FILTERFMT1= put(Dtv_Package,$10.);
if
_FILTERFMT1 not in ( 'A.Box Sets' , 'D.Sky Q')
and
( Age eq . or (7.3391589426<=Age) and (Age<=97.467509652))
and ( BB_Last_Activation eq . or (-1841.16321<=BB_Last_Activation) and (BB_Last_Activation<=4660.5989725))
and ( Broadband_Average_Demand eq . or (-300.6871677<=Broadband_Average_Demand) and (Broadband_Average_Demand<=557.24860481))
and ( Curr_Offer_Actual_End_DTV eq . or (-11554.70802<=Curr_Offer_Actual_End_DTV) and (Curr_Offer_Actual_End_DTV<=10182.032492))
and ( Curr_Offer_Start_DTV eq . or (-438.7731593<=Curr_Offer_Start_DTV) and (Curr_Offer_Start_DTV<=801.68839199))
and ( DTV_1st_Activation eq . or (-3244.488654<=DTV_1st_Activation) and (DTV_1st_Activation<=9731.6661113))
and ( DTV_Curr_Contract_Intended_End eq . or (-687.5075384<=DTV_Curr_Contract_Intended_End) and (DTV_Curr_Contract_Intended_End<=222.80186676))
and ( DTV_CusCan_Churns_Ever eq . or (-1.480995986<=DTV_CusCan_Churns_Ever) and (DTV_CusCan_Churns_Ever<=1.7952436341))
and ( DTV_Last_Activation eq . or (-3590.516936<=DTV_Last_Activation) and (DTV_Last_Activation<=9429.9654991))
and ( DTV_Last_Active_Block eq . or (-3210.544238<=DTV_Last_Active_Block) and (DTV_Last_Active_Block<=6093.3294626))
and ( DTV_Last_Pending_Cancel eq . or (-3141.448377<=DTV_Last_Pending_Cancel) and (DTV_Last_Pending_Cancel<=6025.8851735))
and ( DTV_Last_cuscan_churn eq . or (-3257.128281<=DTV_Last_cuscan_churn) and (DTV_Last_cuscan_churn<=6753.4703724))
and ( DTV_Pending_cancels_ever eq . or (-1.971389684<=DTV_Pending_cancels_ever) and (DTV_Pending_cancels_ever<=2.487007324))
and ( DTV_SysCan_Churns_Ever eq . or (-1.173659145<=DTV_SysCan_Churns_Ever) and (DTV_SysCan_Churns_Ever<=1.3134548574))
and ( LAST_movies_downgrade eq . or (-98.6330199<=LAST_movies_downgrade) and (LAST_movies_downgrade<=212.24042597))
and ( LAST_sports_downgrade eq . or (-103.9698843<=LAST_sports_downgrade) and (LAST_sports_downgrade<=221.75818049))
and ( NTV_Ents_Last_30D eq . or (-0.268260737<=NTV_Ents_Last_30D) and (NTV_Ents_Last_30D<=0.2854408141))
and ( NTV_Ents_Last_90D eq . or (-0.289038967<=NTV_Ents_Last_90D) and (NTV_Ents_Last_90D<=0.3091178286))
and ( OD_Last_12M eq . or (-1151.002432<=OD_Last_12M) and (OD_Last_12M<=1851.4670588))
and ( OD_Last_3M eq . or (-345.6112169<=OD_Last_3M) and (OD_Last_3M<=551.41509832))
and ( OD_Months_since_Last eq . or (-5.434146278<=OD_Months_since_Last) and (OD_Months_since_Last<=6.5860508637))
and ( Prev_Offer_Amount_DTV eq . or (-39.01294832<=Prev_Offer_Amount_DTV) and (Prev_Offer_Amount_DTV<=13.135125671))
and ( Superfast_Available_End_2016 eq . or (0.2256737761<=Superfast_Available_End_2016) and (Superfast_Available_End_2016<=1.5456892913))
and ( Superfast_Available_End_2017 eq . or (0.4610070147<=Superfast_Available_End_2017) and (Superfast_Available_End_2017<=1.3824943999))
and ( Throughput_Speed eq . or (-13.02442062<=Throughput_Speed) and (Throughput_Speed<=48.573791366))
and ( _1st_TA_nonsave_x eq . or (-2072.622259<=_1st_TA_nonsave_x) and (_1st_TA_nonsave_x<=4646.2678872))
and ( _1st_TA_save_x eq . or (-2070.35495<=_1st_TA_save_x) and (_1st_TA_save_x<=4998.3647624))
and ( _1st_TA_x eq . or (-2084.519013<=_1st_TA_x) and (_1st_TA_x<=5060.7056982))
and ( h_income_value eq . or (-58106.18116<=h_income_value) and (h_income_value<=152677.91433))
and ( last_TA_nonsave_x eq . or (-2045.897482<=last_TA_nonsave_x) and (last_TA_nonsave_x<=4075.7408455))
and ( last_TA_save_x eq . or (-1957.15935<=last_TA_save_x) and (last_TA_save_x<=3492.1512831))
and ( last_TA_x eq . or (-1955.377277<=last_TA_x) and (last_TA_x<=3418.3026304))
and ( max_speed_uplift eq . or (-53.50433958<=max_speed_uplift) and (max_speed_uplift<=75.760318221))
and ( num_sports_events eq . or (-2.445788372<=num_sports_events) and (num_sports_events<=2.9425221924))
and ( skyfibre_enabled_perc eq . or (5.7716929465<=skyfibre_enabled_perc) and (skyfibre_enabled_perc<=174.7871682))
and ( skyfibre_planned_perc eq . or (15.862274603<=skyfibre_planned_perc) and (skyfibre_planned_perc<=168.42312742))
then do;
if M_FILTER eq . then M_FILTER = 0;
else M_FILTER = M_FILTER + 0;
end;
else M_FILTER = 1;
label M_FILTER = 'Filtered Indicator';
*------------------------------------------------------------*;
* TOOL: Extension Class;
* TYPE: HPDM;
* NODE: HPPart4;
*------------------------------------------------------------*;
*------------------------------------------------------------*;
* TOOL: Transform;
* TYPE: MODIFY;
* NODE: Trans3;
*------------------------------------------------------------*;
*------------------------------------------------------------*;
* Computed Code;
*------------------------------------------------------------*;
*------------------------------------------------------------*;
* TRANSFORM: Age , (Age + 1)**2;
*------------------------------------------------------------*;
label SQR_Age = 'Transformed: Age';
if Age eq . then SQR_Age = .;
else do;
SQR_Age = (Age + 1)**2;
end;
*------------------------------------------------------------*;
* TRANSFORM: BB_Last_Activation , Optimal Binning(4);
*------------------------------------------------------------*;
label OPT_BB_Last_Activation = 'Transformed: BB_Last_Activation';
length OPT_BB_Last_Activation $36;
if (BB_Last_Activation eq .) then OPT_BB_Last_Activation="03:3216.5-3311.5, MISSING";
else
if (BB_Last_Activation < 63.5) then
OPT_BB_Last_Activation = "01:low-63.5";
else
if (BB_Last_Activation >= 63.5 and BB_Last_Activation < 3216.5) then
OPT_BB_Last_Activation = "02:63.5-3216.5";
else
if (BB_Last_Activation >= 3216.5 and BB_Last_Activation < 3311.5) then
OPT_BB_Last_Activation = "03:3216.5-3311.5, MISSING";
else
if (BB_Last_Activation >= 3311.5) then
OPT_BB_Last_Activation = "04:3311.5-high";
*------------------------------------------------------------*;
* TRANSFORM: Broadband_Average_Demand , (Broadband_Average_Demand + 1)**2;
*------------------------------------------------------------*;
label SQR_Broadband_Average_Demand = 'Transformed: Broadband_Average_Demand';
if Broadband_Average_Demand eq . then SQR_Broadband_Average_Demand = .;
else do;
SQR_Broadband_Average_Demand = (Broadband_Average_Demand + 1)**2;
end;
*------------------------------------------------------------*;
* TRANSFORM: Curr_Offer_Actual_End_DTV , Optimal Binning(4);
*------------------------------------------------------------*;
label OPT_Curr_Offer_Actual_End_DTV = 'Transformed: Curr_Offer_Actual_End_DTV';
length OPT_Curr_Offer_Actual_End_DTV $36;
if (Curr_Offer_Actual_End_DTV eq .) then OPT_Curr_Offer_Actual_End_DTV="03:-326.5--34.5, MISSING";
else
if (Curr_Offer_Actual_End_DTV < -549.5) then
OPT_Curr_Offer_Actual_End_DTV = "01:low--549.5";
else
if (Curr_Offer_Actual_End_DTV >= -549.5 and Curr_Offer_Actual_End_DTV < -326.5) then
OPT_Curr_Offer_Actual_End_DTV = "02:-549.5--326.5";
else
if (Curr_Offer_Actual_End_DTV >= -326.5 and Curr_Offer_Actual_End_DTV < -34.5) then
OPT_Curr_Offer_Actual_End_DTV = "03:-326.5--34.5, MISSING";
else
if (Curr_Offer_Actual_End_DTV >= -34.5) then
OPT_Curr_Offer_Actual_End_DTV = "04:-34.5-high";
*------------------------------------------------------------*;
* TRANSFORM: Curr_Offer_Start_DTV , Optimal Binning(4);
*------------------------------------------------------------*;
label OPT_Curr_Offer_Start_DTV = 'Transformed: Curr_Offer_Start_DTV';
length OPT_Curr_Offer_Start_DTV $36;
if (Curr_Offer_Start_DTV eq .) then OPT_Curr_Offer_Start_DTV="04:375.5-high, MISSING";
else
if (Curr_Offer_Start_DTV < 19.5) then
OPT_Curr_Offer_Start_DTV = "01:low-19.5";
else
if (Curr_Offer_Start_DTV >= 19.5 and Curr_Offer_Start_DTV < 319.5) then
OPT_Curr_Offer_Start_DTV = "02:19.5-319.5";
else
if (Curr_Offer_Start_DTV >= 319.5 and Curr_Offer_Start_DTV < 375.5) then
OPT_Curr_Offer_Start_DTV = "03:319.5-375.5";
else
if (Curr_Offer_Start_DTV >= 375.5) then
OPT_Curr_Offer_Start_DTV = "04:375.5-high, MISSING";
*------------------------------------------------------------*;
* TRANSFORM: DTV_1st_Activation , Optimal Binning(4);
*------------------------------------------------------------*;
label OPT_DTV_1st_Activation = 'Transformed: DTV_1st_Activation';
length OPT_DTV_1st_Activation $36;
if (DTV_1st_Activation eq .) then OPT_DTV_1st_Activation="02:41.5-high, MISSING";
else
if (DTV_1st_Activation < 41.5) then
OPT_DTV_1st_Activation = "01:low-41.5";
else
if (DTV_1st_Activation >= 41.5) then
OPT_DTV_1st_Activation = "02:41.5-high, MISSING";
*------------------------------------------------------------*;
* TRANSFORM: DTV_Curr_Contract_Intended_End , Optimal Binning(4);
*------------------------------------------------------------*;
label OPT_DTV_Curr_Contract_Intended_E = 'Transformed: DTV_Curr_Contract_Intended_End';
length OPT_DTV_Curr_Contract_Intended_E $36;
if (DTV_Curr_Contract_Intended_End eq .) then OPT_DTV_Curr_Contract_Intended_E="03:-331.5--58.5, MISSING";
else
if (DTV_Curr_Contract_Intended_End < -552.5) then
OPT_DTV_Curr_Contract_Intended_E = "01:low--552.5";
else
if (DTV_Curr_Contract_Intended_End >= -552.5 and DTV_Curr_Contract_Intended_End < -331.5) then
OPT_DTV_Curr_Contract_Intended_E = "02:-552.5--331.5";
else
if (DTV_Curr_Contract_Intended_End >= -331.5 and DTV_Curr_Contract_Intended_End < -58.5) then
OPT_DTV_Curr_Contract_Intended_E = "03:-331.5--58.5, MISSING";
else
if (DTV_Curr_Contract_Intended_End >= -58.5) then
OPT_DTV_Curr_Contract_Intended_E = "04:-58.5-high";
*------------------------------------------------------------*;
* TRANSFORM: DTV_Curr_Contract_Start , Optimal Binning(4);
*------------------------------------------------------------*;
label OPT_DTV_Curr_Contract_Start = 'Transformed: DTV_Curr_Contract_Start';
length OPT_DTV_Curr_Contract_Start $36;
if (DTV_Curr_Contract_Start eq .) then OPT_DTV_Curr_Contract_Start="03:40.5-307.5, MISSING";
else
if (DTV_Curr_Contract_Start < 0.5) then
OPT_DTV_Curr_Contract_Start = "01:low-0.5";
else
if (DTV_Curr_Contract_Start >= 0.5 and DTV_Curr_Contract_Start < 40.5) then
OPT_DTV_Curr_Contract_Start = "02:0.5-40.5";
else
if (DTV_Curr_Contract_Start >= 40.5 and DTV_Curr_Contract_Start < 307.5) then
OPT_DTV_Curr_Contract_Start = "03:40.5-307.5, MISSING";
else
if (DTV_Curr_Contract_Start >= 307.5) then
OPT_DTV_Curr_Contract_Start = "04:307.5-high";
*------------------------------------------------------------*;
* TRANSFORM: DTV_Last_Activation , Optimal Binning(4);
*------------------------------------------------------------*;
label OPT_DTV_Last_Activation = 'Transformed: DTV_Last_Activation';
length OPT_DTV_Last_Activation $36;
if (DTV_Last_Activation eq .) then OPT_DTV_Last_Activation="02:48.5-high, MISSING";
else
if (DTV_Last_Activation < 48.5) then
OPT_DTV_Last_Activation = "01:low-48.5";
else
if (DTV_Last_Activation >= 48.5) then
OPT_DTV_Last_Activation = "02:48.5-high, MISSING";
*------------------------------------------------------------*;
* TRANSFORM: DTV_Last_Active_Block , Optimal Binning(4);
*------------------------------------------------------------*;
label OPT_DTV_Last_Active_Block = 'Transformed: DTV_Last_Active_Block';
length OPT_DTV_Last_Active_Block $36;
if (DTV_Last_Active_Block eq .) then OPT_DTV_Last_Active_Block="02:596-high, MISSING";
else
if (DTV_Last_Active_Block < 596) then
OPT_DTV_Last_Active_Block = "01:low-596";
else
if (DTV_Last_Active_Block >= 596) then
OPT_DTV_Last_Active_Block = "02:596-high, MISSING";
*------------------------------------------------------------*;
* TRANSFORM: DTV_Last_Pending_Cancel , Optimal Binning(4);
*------------------------------------------------------------*;
label OPT_DTV_Last_Pending_Cancel = 'Transformed: DTV_Last_Pending_Cancel';
length OPT_DTV_Last_Pending_Cancel $36;
if (DTV_Last_Pending_Cancel eq .) then OPT_DTV_Last_Pending_Cancel="03:2697.5-high, MISSING";
else
if (DTV_Last_Pending_Cancel < 263.5) then
OPT_DTV_Last_Pending_Cancel = "01:low-263.5";
else
if (DTV_Last_Pending_Cancel >= 263.5 and DTV_Last_Pending_Cancel < 2697.5) then
OPT_DTV_Last_Pending_Cancel = "02:263.5-2697.5";
else
if (DTV_Last_Pending_Cancel >= 2697.5) then
OPT_DTV_Last_Pending_Cancel = "03:2697.5-high, MISSING";
*------------------------------------------------------------*;
* TRANSFORM: DTV_Last_cuscan_churn , Optimal Binning(4);
*------------------------------------------------------------*;
label OPT_DTV_Last_cuscan_churn = 'Transformed: DTV_Last_cuscan_churn';
length OPT_DTV_Last_cuscan_churn $36;
if (DTV_Last_cuscan_churn eq .) then OPT_DTV_Last_cuscan_churn="02:230.5-high, MISSING";
else
if (DTV_Last_cuscan_churn < 230.5) then
OPT_DTV_Last_cuscan_churn = "01:low-230.5";
else
if (DTV_Last_cuscan_churn >= 230.5) then
OPT_DTV_Last_cuscan_churn = "02:230.5-high, MISSING";
*------------------------------------------------------------*;
* TRANSFORM: DTV_Pending_cancels_ever , 1 / (DTV_Pending_cancels_ever + 1);
*------------------------------------------------------------*;
label INV_DTV_Pending_cancels_ever = 'Transformed: DTV_Pending_cancels_ever';
if DTV_Pending_cancels_ever eq . then INV_DTV_Pending_cancels_ever = .;
else if DTV_Pending_cancels_ever + 1 ne 0 then INV_DTV_Pending_cancels_ever = 1 / (DTV_Pending_cancels_ever + 1);
else INV_DTV_Pending_cancels_ever = .;
*------------------------------------------------------------*;
* TRANSFORM: OD_Last_12M , Optimal Binning(4);
*------------------------------------------------------------*;
label OPT_OD_Last_12M = 'Transformed: OD_Last_12M';
length OPT_OD_Last_12M $36;
if (OD_Last_12M eq .) then OPT_OD_Last_12M="_MISSING_";
else
if (OD_Last_12M < 169.5) then
OPT_OD_Last_12M = "01:low-169.5";
else
if (OD_Last_12M >= 169.5 and OD_Last_12M < 921) then
OPT_OD_Last_12M = "02:169.5-921";
else
if (OD_Last_12M >= 921) then
OPT_OD_Last_12M = "03:921-high";
*------------------------------------------------------------*;
* TRANSFORM: OD_Last_3M , Optimal Binning(4);
*------------------------------------------------------------*;
label OPT_OD_Last_3M = 'Transformed: OD_Last_3M';
length OPT_OD_Last_3M $36;
if (OD_Last_3M eq .) then OPT_OD_Last_3M="_MISSING_";
else
if (OD_Last_3M < 37.5) then
OPT_OD_Last_3M = "01:low-37.5";
else
if (OD_Last_3M >= 37.5 and OD_Last_3M < 153.5) then
OPT_OD_Last_3M = "02:37.5-153.5";
else
if (OD_Last_3M >= 153.5) then
OPT_OD_Last_3M = "03:153.5-high";
*------------------------------------------------------------*;
* TRANSFORM: OD_Months_since_Last , Optimal Binning(4);
*------------------------------------------------------------*;
label OPT_OD_Months_since_Last = 'Transformed: OD_Months_since_Last';
length OPT_OD_Months_since_Last $36;
if (OD_Months_since_Last eq .) then OPT_OD_Months_since_Last="_MISSING_";
else
if (OD_Months_since_Last < 0.5) then
OPT_OD_Months_since_Last = "01:low-0.5";
else
if (OD_Months_since_Last >= 0.5) then
OPT_OD_Months_since_Last = "02:0.5-high";
*------------------------------------------------------------*;
* TRANSFORM: Prev_Offer_Amount_DTV , Optimal Binning(4);
*------------------------------------------------------------*;
label OPT_Prev_Offer_Amount_DTV = 'Transformed: Prev_Offer_Amount_DTV';
length OPT_Prev_Offer_Amount_DTV $36;
if (Prev_Offer_Amount_DTV eq .) then OPT_Prev_Offer_Amount_DTV="02:-10.945--10.59, MISSING";
else
if (Prev_Offer_Amount_DTV < -10.945) then
OPT_Prev_Offer_Amount_DTV = "01:low--10.945";
else
if (Prev_Offer_Amount_DTV >= -10.945 and Prev_Offer_Amount_DTV < -10.59) then
OPT_Prev_Offer_Amount_DTV = "02:-10.945--10.59, MISSING";
else
if (Prev_Offer_Amount_DTV >= -10.59 and Prev_Offer_Amount_DTV < -2.3) then
OPT_Prev_Offer_Amount_DTV = "03:-10.59--2.3";
else
if (Prev_Offer_Amount_DTV >= -2.3) then
OPT_Prev_Offer_Amount_DTV = "04:-2.3-high";
*------------------------------------------------------------*;
* TRANSFORM: Superfast_Available_End_2016 , exp(Superfast_Available_End_2016 );
*------------------------------------------------------------*;
label EXP_Superfast_Available_End_2016 = 'Transformed: Superfast_Available_End_2016';
if Superfast_Available_End_2016 eq . then EXP_Superfast_Available_End_2016 = .;
else do;
EXP_Superfast_Available_End_2016 = exp(Superfast_Available_End_2016 );
end;
*------------------------------------------------------------*;
* TRANSFORM: Superfast_Available_End_2017 , exp(Superfast_Available_End_2017 );
*------------------------------------------------------------*;
label EXP_Superfast_Available_End_2017 = 'Transformed: Superfast_Available_End_2017';
if Superfast_Available_End_2017 eq . then EXP_Superfast_Available_End_2017 = .;
else do;
EXP_Superfast_Available_End_2017 = exp(Superfast_Available_End_2017 );
end;
*------------------------------------------------------------*;
* TRANSFORM: Throughput_Speed , Sqrt(Throughput_Speed + 1);
*------------------------------------------------------------*;
label SQRT_Throughput_Speed = 'Transformed: Throughput_Speed';
if Throughput_Speed eq . then SQRT_Throughput_Speed = .;
else do;
if Throughput_Speed + 1 >= 0 then SQRT_Throughput_Speed = Sqrt(Throughput_Speed + 1);
else SQRT_Throughput_Speed = .;
end;
*------------------------------------------------------------*;
* TRANSFORM: h_income_value , Optimal Binning(4);
*------------------------------------------------------------*;
label OPT_h_income_value = 'Transformed: h_income_value';
length OPT_h_income_value $36;
if (h_income_value eq .) then OPT_h_income_value="01:low-29430, MISSING";
else
if (h_income_value < 29430) then
OPT_h_income_value = "01:low-29430, MISSING";
else
if (h_income_value >= 29430) then
OPT_h_income_value = "02:29430-high";
*------------------------------------------------------------*;
* TRANSFORM: max_speed_uplift , log(max_speed_uplift + 1);
*------------------------------------------------------------*;
label LOG_max_speed_uplift = 'Transformed: max_speed_uplift';
if max_speed_uplift eq . then LOG_max_speed_uplift = .;
else do;
if max_speed_uplift + 1 > 0 then LOG_max_speed_uplift = log(max_speed_uplift + 1);
else LOG_max_speed_uplift = .;
end;
*------------------------------------------------------------*;
* TRANSFORM: num_sports_events , Optimal Binning(4);
*------------------------------------------------------------*;
label OPT_num_sports_events = 'Transformed: num_sports_events';
length OPT_num_sports_events $36;
if (num_sports_events eq .) then OPT_num_sports_events="01:low-0.5, MISSING";
else
if (num_sports_events < 0.5) then
OPT_num_sports_events = "01:low-0.5, MISSING";
else
if (num_sports_events >= 0.5 and num_sports_events < 1.5) then
OPT_num_sports_events = "02:0.5-1.5";
else
if (num_sports_events >= 1.5) then
OPT_num_sports_events = "03:1.5-high";
*------------------------------------------------------------*;
* TRANSFORM: skyfibre_enabled_perc , exp(skyfibre_enabled_perc );
*------------------------------------------------------------*;
label EXP_skyfibre_enabled_perc = 'Transformed: skyfibre_enabled_perc';
if skyfibre_enabled_perc eq . then EXP_skyfibre_enabled_perc = .;
else do;
EXP_skyfibre_enabled_perc = exp(skyfibre_enabled_perc );
end;
*------------------------------------------------------------*;
* TRANSFORM: skyfibre_planned_perc , exp(skyfibre_planned_perc );
*------------------------------------------------------------*;
label EXP_skyfibre_planned_perc = 'Transformed: skyfibre_planned_perc';
if skyfibre_planned_perc eq . then EXP_skyfibre_planned_perc = .;
else do;
EXP_skyfibre_planned_perc = exp(skyfibre_planned_perc );
end;
*------------------------------------------------------------*;
* TRANSFORM: LAST_movies_downgrade , Optimal Binning(4);
*------------------------------------------------------------*;
label OPT_LAST_movies_downgrade = 'Transformed: LAST_movies_downgrade';
length OPT_LAST_movies_downgrade $36;
if (LAST_movies_downgrade eq .) then OPT_LAST_movies_downgrade="03:37.5-high, MISSING";
else
if (LAST_movies_downgrade < 1.5) then
OPT_LAST_movies_downgrade = "01:low-1.5";
else
if (LAST_movies_downgrade >= 1.5 and LAST_movies_downgrade < 37.5) then
OPT_LAST_movies_downgrade = "02:1.5-37.5";
else
if (LAST_movies_downgrade >= 37.5) then
OPT_LAST_movies_downgrade = "03:37.5-high, MISSING";
*------------------------------------------------------------*;
* TRANSFORM: LAST_sports_downgrade , Optimal Binning(4);
*------------------------------------------------------------*;
label OPT_LAST_sports_downgrade = 'Transformed: LAST_sports_downgrade';
length OPT_LAST_sports_downgrade $36;
if (LAST_sports_downgrade eq .) then OPT_LAST_sports_downgrade="02:14.5-high, MISSING";
else
if (LAST_sports_downgrade < 14.5) then
OPT_LAST_sports_downgrade = "01:low-14.5";
else
if (LAST_sports_downgrade >= 14.5) then
OPT_LAST_sports_downgrade = "02:14.5-high, MISSING";
*------------------------------------------------------------*;
* TOOL: Extension Class;
* TYPE: MODIFY;
* NODE: Repl2;
*------------------------------------------------------------*;

* ;
* Defining New Variables;
* ;
Length REP_ADSL_Enabled $1;
Label REP_ADSL_Enabled='Replacement: ADSL_Enabled';
format REP_ADSL_Enabled $1.;
REP_ADSL_Enabled= ADSL_Enabled;
Length REP_BB_3rdParty_PL_Entry_Ever_b $3;
Label REP_BB_3rdParty_PL_Entry_Ever_b='Replacement: BB_3rdParty_PL_Entry_Ever_b';
format REP_BB_3rdParty_PL_Entry_Ever_b $3.;
REP_BB_3rdParty_PL_Entry_Ever_b= BB_3rdParty_PL_Entry_Ever_b;
Length REP_BB_3rdParty_PL_Entry_Last_18 $3;
Label REP_BB_3rdParty_PL_Entry_Last_18='Replacement: BB_3rdParty_PL_Entry_Last_180D_b';
format REP_BB_3rdParty_PL_Entry_Last_18 $3.;
REP_BB_3rdParty_PL_Entry_Last_18= BB_3rdParty_PL_Entry_Last_180D_b;
Length REP_BB_3rdParty_PL_Entry_Last_1Y $3;
Label REP_BB_3rdParty_PL_Entry_Last_1Y='Replacement: BB_3rdParty_PL_Entry_Last_1Yr_b';
format REP_BB_3rdParty_PL_Entry_Last_1Y $3.;
REP_BB_3rdParty_PL_Entry_Last_1Y= BB_3rdParty_PL_Entry_Last_1Yr_b;
Length REP_BB_3rdParty_PL_Entry_Last_3Y $3;
Label REP_BB_3rdParty_PL_Entry_Last_3Y='Replacement: BB_3rdParty_PL_Entry_Last_3Yr_b';
format REP_BB_3rdParty_PL_Entry_Last_3Y $3.;
REP_BB_3rdParty_PL_Entry_Last_3Y= BB_3rdParty_PL_Entry_Last_3Yr_b;
Length REP_BB_3rdParty_PL_Entry_Last_5Y $3;
Label REP_BB_3rdParty_PL_Entry_Last_5Y='Replacement: BB_3rdParty_PL_Entry_Last_5Yr_b';
format REP_BB_3rdParty_PL_Entry_Last_5Y $3.;
REP_BB_3rdParty_PL_Entry_Last_5Y= BB_3rdParty_PL_Entry_Last_5Yr_b;
Label REP_BB_Active='Replacement: BB_Active';
REP_BB_Active= BB_Active;
Length REP_BB_Churns_Ever_b $3;
Label REP_BB_Churns_Ever_b='Replacement: BB_Churns_Ever_b';
format REP_BB_Churns_Ever_b $3.;
REP_BB_Churns_Ever_b= BB_Churns_Ever_b;
Length REP_BB_Churns_Last_180D_b $3;
Label REP_BB_Churns_Last_180D_b='Replacement: BB_Churns_Last_180D_b';
format REP_BB_Churns_Last_180D_b $3.;
REP_BB_Churns_Last_180D_b= BB_Churns_Last_180D_b;
Length REP_BB_Churns_Last_1Yr_b $3;
Label REP_BB_Churns_Last_1Yr_b='Replacement: BB_Churns_Last_1Yr_b';
format REP_BB_Churns_Last_1Yr_b $3.;
REP_BB_Churns_Last_1Yr_b= BB_Churns_Last_1Yr_b;
Length REP_BB_Churns_Last_3Yr_b $3;
Label REP_BB_Churns_Last_3Yr_b='Replacement: BB_Churns_Last_3Yr_b';
format REP_BB_Churns_Last_3Yr_b $3.;
REP_BB_Churns_Last_3Yr_b= BB_Churns_Last_3Yr_b;
Length REP_BB_Churns_Last_5Yr_b $3;
Label REP_BB_Churns_Last_5Yr_b='Replacement: BB_Churns_Last_5Yr_b';
format REP_BB_Churns_Last_5Yr_b $3.;
REP_BB_Churns_Last_5Yr_b= BB_Churns_Last_5Yr_b;
Length REP_BB_Churns_Last_90D_b $3;
Label REP_BB_Churns_Last_90D_b='Replacement: BB_Churns_Last_90D_b';
format REP_BB_Churns_Last_90D_b $3.;
REP_BB_Churns_Last_90D_b= BB_Churns_Last_90D_b;
Length REP_BB_CusCan_PL_Entry_Ever_b $3;
Label REP_BB_CusCan_PL_Entry_Ever_b='Replacement: BB_CusCan_PL_Entry_Ever_b';
format REP_BB_CusCan_PL_Entry_Ever_b $3.;
REP_BB_CusCan_PL_Entry_Ever_b= BB_CusCan_PL_Entry_Ever_b;
Length REP_BB_CusCan_PL_Entry_Last_180D $3;
Label REP_BB_CusCan_PL_Entry_Last_180D='Replacement: BB_CusCan_PL_Entry_Last_180D_b';
format REP_BB_CusCan_PL_Entry_Last_180D $3.;
REP_BB_CusCan_PL_Entry_Last_180D= BB_CusCan_PL_Entry_Last_180D_b;
Length REP_BB_CusCan_PL_Entry_Last_1Yr_ $3;
Label REP_BB_CusCan_PL_Entry_Last_1Yr_='Replacement: BB_CusCan_PL_Entry_Last_1Yr_b';
format REP_BB_CusCan_PL_Entry_Last_1Yr_ $3.;
REP_BB_CusCan_PL_Entry_Last_1Yr_= BB_CusCan_PL_Entry_Last_1Yr_b;
Length REP_BB_CusCan_PL_Entry_Last_3Yr_ $3;
Label REP_BB_CusCan_PL_Entry_Last_3Yr_='Replacement: BB_CusCan_PL_Entry_Last_3Yr_b';
format REP_BB_CusCan_PL_Entry_Last_3Yr_ $3.;
REP_BB_CusCan_PL_Entry_Last_3Yr_= BB_CusCan_PL_Entry_Last_3Yr_b;
Length REP_BB_CusCan_PL_Entry_Last_5Yr_ $3;
Label REP_BB_CusCan_PL_Entry_Last_5Yr_='Replacement: BB_CusCan_PL_Entry_Last_5Yr_b';
format REP_BB_CusCan_PL_Entry_Last_5Yr_ $3.;
REP_BB_CusCan_PL_Entry_Last_5Yr_= BB_CusCan_PL_Entry_Last_5Yr_b;
Length REP_BB_HomeMove_PL_Entry_Ever_b $3;
Label REP_BB_HomeMove_PL_Entry_Ever_b='Replacement: BB_HomeMove_PL_Entry_Ever_b';
format REP_BB_HomeMove_PL_Entry_Ever_b $3.;
REP_BB_HomeMove_PL_Entry_Ever_b= BB_HomeMove_PL_Entry_Ever_b;
Length REP_BB_HomeMove_PL_Entry_Last_1Y $3;
Label REP_BB_HomeMove_PL_Entry_Last_1Y='Replacement: BB_HomeMove_PL_Entry_Last_1Yr_b';
format REP_BB_HomeMove_PL_Entry_Last_1Y $3.;
REP_BB_HomeMove_PL_Entry_Last_1Y= BB_HomeMove_PL_Entry_Last_1Yr_b;
Length REP_BB_HomeMove_PL_Entry_Last_3Y $3;
Label REP_BB_HomeMove_PL_Entry_Last_3Y='Replacement: BB_HomeMove_PL_Entry_Last_3Yr_b';
format REP_BB_HomeMove_PL_Entry_Last_3Y $3.;
REP_BB_HomeMove_PL_Entry_Last_3Y= BB_HomeMove_PL_Entry_Last_3Yr_b;
Length REP_BB_HomeMove_PL_Entry_Last_5Y $3;
Label REP_BB_HomeMove_PL_Entry_Last_5Y='Replacement: BB_HomeMove_PL_Entry_Last_5Yr_b';
format REP_BB_HomeMove_PL_Entry_Last_5Y $3.;
REP_BB_HomeMove_PL_Entry_Last_5Y= BB_HomeMove_PL_Entry_Last_5Yr_b;
Length REP_BB_Product_Holding $80;
Label REP_BB_Product_Holding='Replacement: BB_Product_Holding';
format REP_BB_Product_Holding $80.;
REP_BB_Product_Holding= BB_Product_Holding;
Length REP_BB_Provider $20;
Label REP_BB_Provider='Replacement: BB_Provider';
format REP_BB_Provider $20.;
REP_BB_Provider= BB_Provider;
Length REP_BB_Status_Code $10;
Label REP_BB_Status_Code='Replacement: BB_Status_Code';
format REP_BB_Status_Code $10.;
REP_BB_Status_Code= BB_Status_Code;
Length REP_BB_SysCan_PL_Entry_Ever_b $3;
Label REP_BB_SysCan_PL_Entry_Ever_b='Replacement: BB_SysCan_PL_Entry_Ever_b';
format REP_BB_SysCan_PL_Entry_Ever_b $3.;
REP_BB_SysCan_PL_Entry_Ever_b= BB_SysCan_PL_Entry_Ever_b;
Length REP_BB_SysCan_PL_Entry_Last_180D $3;
Label REP_BB_SysCan_PL_Entry_Last_180D='Replacement: BB_SysCan_PL_Entry_Last_180D_b';
format REP_BB_SysCan_PL_Entry_Last_180D $3.;
REP_BB_SysCan_PL_Entry_Last_180D= BB_SysCan_PL_Entry_Last_180D_b;
Length REP_BB_SysCan_PL_Entry_Last_1Yr_ $3;
Label REP_BB_SysCan_PL_Entry_Last_1Yr_='Replacement: BB_SysCan_PL_Entry_Last_1Yr_b';
format REP_BB_SysCan_PL_Entry_Last_1Yr_ $3.;
REP_BB_SysCan_PL_Entry_Last_1Yr_= BB_SysCan_PL_Entry_Last_1Yr_b;
Length REP_BB_SysCan_PL_Entry_Last_3Yr_ $3;
Label REP_BB_SysCan_PL_Entry_Last_3Yr_='Replacement: BB_SysCan_PL_Entry_Last_3Yr_b';
format REP_BB_SysCan_PL_Entry_Last_3Yr_ $3.;
REP_BB_SysCan_PL_Entry_Last_3Yr_= BB_SysCan_PL_Entry_Last_3Yr_b;
Length REP_BB_SysCan_PL_Entry_Last_5Yr_ $3;
Label REP_BB_SysCan_PL_Entry_Last_5Yr_='Replacement: BB_SysCan_PL_Entry_Last_5Yr_b';
format REP_BB_SysCan_PL_Entry_Last_5Yr_ $3.;
REP_BB_SysCan_PL_Entry_Last_5Yr_= BB_SysCan_PL_Entry_Last_5Yr_b;
Length REP_BB_SysCan_PL_Entry_Last_90D_ $3;
Label REP_BB_SysCan_PL_Entry_Last_90D_='Replacement: BB_SysCan_PL_Entry_Last_90D_b';
format REP_BB_SysCan_PL_Entry_Last_90D_ $3.;
REP_BB_SysCan_PL_Entry_Last_90D_= BB_SysCan_PL_Entry_Last_90D_b;
Length REP_BB_contract_segment $23;
Label REP_BB_contract_segment='Replacement: BB_contract_segment';
format REP_BB_contract_segment $23.;
REP_BB_contract_segment= BB_contract_segment;
Length REP_Curr_Offer_Amount_DTV_b $17;
Label REP_Curr_Offer_Amount_DTV_b='Replacement: Curr_Offer_Amount_DTV_b';
format REP_Curr_Offer_Amount_DTV_b $17.;
REP_Curr_Offer_Amount_DTV_b= Curr_Offer_Amount_DTV_b;
Length REP_Curr_Offer_Amount_DTV_flag $15;
Label REP_Curr_Offer_Amount_DTV_flag='Replacement: Curr_Offer_Amount_DTV_flag';
format REP_Curr_Offer_Amount_DTV_flag $15.;
REP_Curr_Offer_Amount_DTV_flag= Curr_Offer_Amount_DTV_flag;
Length REP_Curr_Offer_Length_DTV_b $18;
Label REP_Curr_Offer_Length_DTV_b='Replacement: Curr_Offer_Length_DTV_b';
format REP_Curr_Offer_Length_DTV_b $18.;
REP_Curr_Offer_Length_DTV_b= Curr_Offer_Length_DTV_b;
Length REP_DTV_Active_Blocks_Ever_b $3;
Label REP_DTV_Active_Blocks_Ever_b='Replacement: DTV_Active_Blocks_Ever_b';
format REP_DTV_Active_Blocks_Ever_b $3.;
REP_DTV_Active_Blocks_Ever_b= DTV_Active_Blocks_Ever_b;
Length REP_DTV_Active_Blocks_Last_180D_ $3;
Label REP_DTV_Active_Blocks_Last_180D_='Replacement: DTV_Active_Blocks_Last_180D_b';
format REP_DTV_Active_Blocks_Last_180D_ $3.;
REP_DTV_Active_Blocks_Last_180D_= DTV_Active_Blocks_Last_180D_b;
Length REP_DTV_Active_Blocks_Last_1Yr_b $3;
Label REP_DTV_Active_Blocks_Last_1Yr_b='Replacement: DTV_Active_Blocks_Last_1Yr_b';
format REP_DTV_Active_Blocks_Last_1Yr_b $3.;
REP_DTV_Active_Blocks_Last_1Yr_b= DTV_Active_Blocks_Last_1Yr_b;
Length REP_DTV_Active_Blocks_Last_3Yr_b $3;
Label REP_DTV_Active_Blocks_Last_3Yr_b='Replacement: DTV_Active_Blocks_Last_3Yr_b';
format REP_DTV_Active_Blocks_Last_3Yr_b $3.;
REP_DTV_Active_Blocks_Last_3Yr_b= DTV_Active_Blocks_Last_3Yr_b;
Length REP_DTV_Active_Blocks_Last_5Yr_b $3;
Label REP_DTV_Active_Blocks_Last_5Yr_b='Replacement: DTV_Active_Blocks_Last_5Yr_b';
format REP_DTV_Active_Blocks_Last_5Yr_b $3.;
REP_DTV_Active_Blocks_Last_5Yr_b= DTV_Active_Blocks_Last_5Yr_b;
Length REP_DTV_Active_Blocks_Last_90D_b $3;
Label REP_DTV_Active_Blocks_Last_90D_b='Replacement: DTV_Active_Blocks_Last_90D_b';
format REP_DTV_Active_Blocks_Last_90D_b $3.;
REP_DTV_Active_Blocks_Last_90D_b= DTV_Active_Blocks_Last_90D_b;
Length REP_DTV_Churns_Ever_b $3;
Label REP_DTV_Churns_Ever_b='Replacement: DTV_Churns_Ever_b';
format REP_DTV_Churns_Ever_b $3.;
REP_DTV_Churns_Ever_b= DTV_Churns_Ever_b;
Length REP_DTV_Churns_Last_1Yr_b $3;
Label REP_DTV_Churns_Last_1Yr_b='Replacement: DTV_Churns_Last_1Yr_b';
format REP_DTV_Churns_Last_1Yr_b $3.;
REP_DTV_Churns_Last_1Yr_b= DTV_Churns_Last_1Yr_b;
Length REP_DTV_Churns_Last_3Yr_b $3;
Label REP_DTV_Churns_Last_3Yr_b='Replacement: DTV_Churns_Last_3Yr_b';
format REP_DTV_Churns_Last_3Yr_b $3.;
REP_DTV_Churns_Last_3Yr_b= DTV_Churns_Last_3Yr_b;
Length REP_DTV_Churns_Last_5Yr_b $3;
Label REP_DTV_Churns_Last_5Yr_b='Replacement: DTV_Churns_Last_5Yr_b';
format REP_DTV_Churns_Last_5Yr_b $3.;
REP_DTV_Churns_Last_5Yr_b= DTV_Churns_Last_5Yr_b;
Length REP_DTV_CusCan_Churns_Last_180D_ $3;
Label REP_DTV_CusCan_Churns_Last_180D_='Replacement: DTV_CusCan_Churns_Last_180D_b';
format REP_DTV_CusCan_Churns_Last_180D_ $3.;
REP_DTV_CusCan_Churns_Last_180D_= DTV_CusCan_Churns_Last_180D_b;
Length REP_DTV_CusCan_Churns_Last_1Yr_b $3;
Label REP_DTV_CusCan_Churns_Last_1Yr_b='Replacement: DTV_CusCan_Churns_Last_1Yr_b';
format REP_DTV_CusCan_Churns_Last_1Yr_b $3.;
REP_DTV_CusCan_Churns_Last_1Yr_b= DTV_CusCan_Churns_Last_1Yr_b;
Length REP_DTV_CusCan_Churns_Last_3Yr_b $3;
Label REP_DTV_CusCan_Churns_Last_3Yr_b='Replacement: DTV_CusCan_Churns_Last_3Yr_b';
format REP_DTV_CusCan_Churns_Last_3Yr_b $3.;
REP_DTV_CusCan_Churns_Last_3Yr_b= DTV_CusCan_Churns_Last_3Yr_b;
Length REP_DTV_CusCan_Churns_Last_5Yr_b $3;
Label REP_DTV_CusCan_Churns_Last_5Yr_b='Replacement: DTV_CusCan_Churns_Last_5Yr_b';
format REP_DTV_CusCan_Churns_Last_5Yr_b $3.;
REP_DTV_CusCan_Churns_Last_5Yr_b= DTV_CusCan_Churns_Last_5Yr_b;
Length REP_DTV_PO_Cancellations_Ever_b $3;
Label REP_DTV_PO_Cancellations_Ever_b='Replacement: DTV_PO_Cancellations_Ever_b';
format REP_DTV_PO_Cancellations_Ever_b $3.;
REP_DTV_PO_Cancellations_Ever_b= DTV_PO_Cancellations_Ever_b;
Length REP_DTV_PO_Cancellations_Last_1Y $3;
Label REP_DTV_PO_Cancellations_Last_1Y='Replacement: DTV_PO_Cancellations_Last_1Yr_b';
format REP_DTV_PO_Cancellations_Last_1Y $3.;
REP_DTV_PO_Cancellations_Last_1Y= DTV_PO_Cancellations_Last_1Yr_b;
Length REP_DTV_PO_Cancellations_Last_3Y $3;
Label REP_DTV_PO_Cancellations_Last_3Y='Replacement: DTV_PO_Cancellations_Last_3Yr_b';
format REP_DTV_PO_Cancellations_Last_3Y $3.;
REP_DTV_PO_Cancellations_Last_3Y= DTV_PO_Cancellations_Last_3Yr_b;
Length REP_DTV_PO_Cancellations_Last_5Y $3;
Label REP_DTV_PO_Cancellations_Last_5Y='Replacement: DTV_PO_Cancellations_Last_5Yr_b';
format REP_DTV_PO_Cancellations_Last_5Y $3.;
REP_DTV_PO_Cancellations_Last_5Y= DTV_PO_Cancellations_Last_5Yr_b;
Length REP_DTV_Pending_Cancels_Last_180 $3;
Label REP_DTV_Pending_Cancels_Last_180='Replacement: DTV_Pending_Cancels_Last_180D_b';
format REP_DTV_Pending_Cancels_Last_180 $3.;
REP_DTV_Pending_Cancels_Last_180= DTV_Pending_Cancels_Last_180D_b;
Length REP_DTV_Pending_Cancels_Last_1Yr $3;
Label REP_DTV_Pending_Cancels_Last_1Yr='Replacement: DTV_Pending_Cancels_Last_1Yr_b';
format REP_DTV_Pending_Cancels_Last_1Yr $3.;
REP_DTV_Pending_Cancels_Last_1Yr= DTV_Pending_Cancels_Last_1Yr_b;
Length REP_DTV_Pending_Cancels_Last_30D $3;
Label REP_DTV_Pending_Cancels_Last_30D='Replacement: DTV_Pending_Cancels_Last_30D_b';
format REP_DTV_Pending_Cancels_Last_30D $3.;
REP_DTV_Pending_Cancels_Last_30D= DTV_Pending_Cancels_Last_30D_b;
Length REP_DTV_Pending_Cancels_Last_3Yr $3;
Label REP_DTV_Pending_Cancels_Last_3Yr='Replacement: DTV_Pending_Cancels_Last_3Yr_b';
format REP_DTV_Pending_Cancels_Last_3Yr $3.;
REP_DTV_Pending_Cancels_Last_3Yr= DTV_Pending_Cancels_Last_3Yr_b;
Length REP_DTV_Pending_Cancels_Last_5Yr $3;
Label REP_DTV_Pending_Cancels_Last_5Yr='Replacement: DTV_Pending_Cancels_Last_5Yr_b';
format REP_DTV_Pending_Cancels_Last_5Yr $3.;
REP_DTV_Pending_Cancels_Last_5Yr= DTV_Pending_Cancels_Last_5Yr_b;
Length REP_DTV_Pending_Cancels_Last_90D $3;
Label REP_DTV_Pending_Cancels_Last_90D='Replacement: DTV_Pending_Cancels_Last_90D_b';
format REP_DTV_Pending_Cancels_Last_90D $3.;
REP_DTV_Pending_Cancels_Last_90D= DTV_Pending_Cancels_Last_90D_b;
Length REP_DTV_SysCan_Churns_In_Last_90 $3;
Label REP_DTV_SysCan_Churns_In_Last_90='Replacement: DTV_SysCan_Churns_In_Last_90D_b';
format REP_DTV_SysCan_Churns_In_Last_90 $3.;
REP_DTV_SysCan_Churns_In_Last_90= DTV_SysCan_Churns_In_Last_90D_b;
Length REP_DTV_SysCan_Churns_Last_180D_ $3;
Label REP_DTV_SysCan_Churns_Last_180D_='Replacement: DTV_SysCan_Churns_Last_180D_b';
format REP_DTV_SysCan_Churns_Last_180D_ $3.;
REP_DTV_SysCan_Churns_Last_180D_= DTV_SysCan_Churns_Last_180D_b;
Length REP_DTV_SysCan_Churns_Last_1Yr_b $3;
Label REP_DTV_SysCan_Churns_Last_1Yr_b='Replacement: DTV_SysCan_Churns_Last_1Yr_b';
format REP_DTV_SysCan_Churns_Last_1Yr_b $3.;
REP_DTV_SysCan_Churns_Last_1Yr_b= DTV_SysCan_Churns_Last_1Yr_b;
Length REP_DTV_SysCan_Churns_Last_30D_b $3;
Label REP_DTV_SysCan_Churns_Last_30D_b='Replacement: DTV_SysCan_Churns_Last_30D_b';
format REP_DTV_SysCan_Churns_Last_30D_b $3.;
REP_DTV_SysCan_Churns_Last_30D_b= DTV_SysCan_Churns_Last_30D_b;
Length REP_DTV_SysCan_Churns_Last_3Yr_b $3;
Label REP_DTV_SysCan_Churns_Last_3Yr_b='Replacement: DTV_SysCan_Churns_Last_3Yr_b';
format REP_DTV_SysCan_Churns_Last_3Yr_b $3.;
REP_DTV_SysCan_Churns_Last_3Yr_b= DTV_SysCan_Churns_Last_3Yr_b;
Length REP_DTV_SysCan_Churns_Last_5Yr_b $3;
Label REP_DTV_SysCan_Churns_Last_5Yr_b='Replacement: DTV_SysCan_Churns_Last_5Yr_b';
format REP_DTV_SysCan_Churns_Last_5Yr_b $3.;
REP_DTV_SysCan_Churns_Last_5Yr_b= DTV_SysCan_Churns_Last_5Yr_b;
Length REP_DTV_contract_segment $23;
Label REP_DTV_contract_segment='Replacement: DTV_contract_segment';
format REP_DTV_contract_segment $23.;
REP_DTV_contract_segment= DTV_contract_segment;
Length REP_DTV_product_holding_recode $40;
Label REP_DTV_product_holding_recode='Replacement: DTV_product_holding_recode';
format REP_DTV_product_holding_recode $40.;
REP_DTV_product_holding_recode= DTV_product_holding_recode;
Length REP_Dtv_Package $10;
Label REP_Dtv_Package='Replacement: Dtv_Package';
format REP_Dtv_Package $10.;
REP_Dtv_Package= Dtv_Package;
Length REP_Home_Owner_Status $30;
Label REP_Home_Owner_Status='Replacement: Home_Owner_Status';
format REP_Home_Owner_Status $30.;
REP_Home_Owner_Status= Home_Owner_Status;
Length REP_Movies_Tenure $20;
Label REP_Movies_Tenure='Replacement: Movies_Tenure';
format REP_Movies_Tenure $20.;
REP_Movies_Tenure= Movies_Tenure;
Length REP_OPT_BB_Last_Activation $36;
Label REP_OPT_BB_Last_Activation='Replacement: Transformed: BB_Last_Activation';
REP_OPT_BB_Last_Activation= OPT_BB_Last_Activation;
Length REP_OPT_Curr_Offer_Actual_End_DT $36;
Label REP_OPT_Curr_Offer_Actual_End_DT='Replacement: Transformed: Curr_Offer_Actual_End_DTV';
REP_OPT_Curr_Offer_Actual_End_DT= OPT_Curr_Offer_Actual_End_DTV;
Length REP_OPT_Curr_Offer_Start_DTV $36;
Label REP_OPT_Curr_Offer_Start_DTV='Replacement: Transformed: Curr_Offer_Start_DTV';
REP_OPT_Curr_Offer_Start_DTV= OPT_Curr_Offer_Start_DTV;
Length REP_OPT_DTV_1st_Activation $36;
Label REP_OPT_DTV_1st_Activation='Replacement: Transformed: DTV_1st_Activation';
REP_OPT_DTV_1st_Activation= OPT_DTV_1st_Activation;
Length REP_OPT_DTV_Curr_Contract_Intend $36;
Label REP_OPT_DTV_Curr_Contract_Intend='Replacement: Transformed: DTV_Curr_Contract_Intended_End';
REP_OPT_DTV_Curr_Contract_Intend= OPT_DTV_Curr_Contract_Intended_E;
Length REP_OPT_DTV_Last_Activation $36;
Label REP_OPT_DTV_Last_Activation='Replacement: Transformed: DTV_Last_Activation';
REP_OPT_DTV_Last_Activation= OPT_DTV_Last_Activation;
Length REP_OPT_DTV_Last_Active_Block $36;
Label REP_OPT_DTV_Last_Active_Block='Replacement: Transformed: DTV_Last_Active_Block';
REP_OPT_DTV_Last_Active_Block= OPT_DTV_Last_Active_Block;
Length REP_OPT_DTV_Last_Pending_Cancel $36;
Label REP_OPT_DTV_Last_Pending_Cancel='Replacement: Transformed: DTV_Last_Pending_Cancel';
REP_OPT_DTV_Last_Pending_Cancel= OPT_DTV_Last_Pending_Cancel;
Length REP_OPT_DTV_Last_cuscan_churn $36;
Label REP_OPT_DTV_Last_cuscan_churn='Replacement: Transformed: DTV_Last_cuscan_churn';
REP_OPT_DTV_Last_cuscan_churn= OPT_DTV_Last_cuscan_churn;
Length REP_OPT_LAST_movies_downgrade $36;
Label REP_OPT_LAST_movies_downgrade='Replacement: Transformed: LAST_movies_downgrade';
REP_OPT_LAST_movies_downgrade= OPT_LAST_movies_downgrade;
Length REP_OPT_LAST_sports_downgrade $36;
Label REP_OPT_LAST_sports_downgrade='Replacement: Transformed: LAST_sports_downgrade';
REP_OPT_LAST_sports_downgrade= OPT_LAST_sports_downgrade;
Length REP_OPT_OD_Last_12M $36;
Label REP_OPT_OD_Last_12M='Replacement: Transformed: OD_Last_12M';
REP_OPT_OD_Last_12M= OPT_OD_Last_12M;
Length REP_OPT_OD_Last_3M $36;
Label REP_OPT_OD_Last_3M='Replacement: Transformed: OD_Last_3M';
REP_OPT_OD_Last_3M= OPT_OD_Last_3M;
Length REP_OPT_OD_Months_since_Last $36;
Label REP_OPT_OD_Months_since_Last='Replacement: Transformed: OD_Months_since_Last';
REP_OPT_OD_Months_since_Last= OPT_OD_Months_since_Last;
Length REP_OPT_Prev_Offer_Amount_DTV $36;
Label REP_OPT_Prev_Offer_Amount_DTV='Replacement: Transformed: Prev_Offer_Amount_DTV';
REP_OPT_Prev_Offer_Amount_DTV= OPT_Prev_Offer_Amount_DTV;
Length REP_OPT_h_income_value $36;
Label REP_OPT_h_income_value='Replacement: Transformed: h_income_value';
REP_OPT_h_income_value= OPT_h_income_value;
Length REP_OPT_num_sports_events $36;
Label REP_OPT_num_sports_events='Replacement: Transformed: num_sports_events';
REP_OPT_num_sports_events= OPT_num_sports_events;
Length REP_Offers_Applied_Lst_12M_DTV_b $3;
Label REP_Offers_Applied_Lst_12M_DTV_b='Replacement: Offers_Applied_Lst_12M_DTV_b';
format REP_Offers_Applied_Lst_12M_DTV_b $3.;
REP_Offers_Applied_Lst_12M_DTV_b= Offers_Applied_Lst_12M_DTV_b;
Length REP_Offers_Applied_Lst_24M_DTV_b $3;
Label REP_Offers_Applied_Lst_24M_DTV_b='Replacement: Offers_Applied_Lst_24M_DTV_b';
format REP_Offers_Applied_Lst_24M_DTV_b $3.;
REP_Offers_Applied_Lst_24M_DTV_b= Offers_Applied_Lst_24M_DTV_b;
Length REP_Offers_Applied_Lst_30D_DTV_b $3;
Label REP_Offers_Applied_Lst_30D_DTV_b='Replacement: Offers_Applied_Lst_30D_DTV_b';
format REP_Offers_Applied_Lst_30D_DTV_b $3.;
REP_Offers_Applied_Lst_30D_DTV_b= Offers_Applied_Lst_30D_DTV_b;
Length REP_Offers_Applied_Lst_36M_DTV_b $3;
Label REP_Offers_Applied_Lst_36M_DTV_b='Replacement: Offers_Applied_Lst_36M_DTV_b';
format REP_Offers_Applied_Lst_36M_DTV_b $3.;
REP_Offers_Applied_Lst_36M_DTV_b= Offers_Applied_Lst_36M_DTV_b;
Length REP_Offers_Applied_Lst_90D_DTV_b $3;
Label REP_Offers_Applied_Lst_90D_DTV_b='Replacement: Offers_Applied_Lst_90D_DTV_b';
format REP_Offers_Applied_Lst_90D_DTV_b $3.;
REP_Offers_Applied_Lst_90D_DTV_b= Offers_Applied_Lst_90D_DTV_b;
Length REP_Prev_Offer_Amount_DTV_b $17;
Label REP_Prev_Offer_Amount_DTV_b='Replacement: Prev_Offer_Amount_DTV_b';
format REP_Prev_Offer_Amount_DTV_b $17.;
REP_Prev_Offer_Amount_DTV_b= Prev_Offer_Amount_DTV_b;
Length REP_Prev_Offer_Amount_DTV_flag $15;
Label REP_Prev_Offer_Amount_DTV_flag='Replacement: Prev_Offer_Amount_DTV_flag';
format REP_Prev_Offer_Amount_DTV_flag $15.;
REP_Prev_Offer_Amount_DTV_flag= Prev_Offer_Amount_DTV_flag;
Length REP_Prev_Offer_Length_DTV_b $17;
Label REP_Prev_Offer_Length_DTV_b='Replacement: Prev_Offer_Length_DTV_b';
format REP_Prev_Offer_Length_DTV_b $17.;
REP_Prev_Offer_Length_DTV_b= Prev_Offer_Length_DTV_b;
Length REP_Sports_Tenure $20;
Label REP_Sports_Tenure='Replacement: Sports_Tenure';
format REP_Sports_Tenure $20.;
REP_Sports_Tenure= Sports_Tenure;
Length REP_TA_nonsaves_in_last_12m_b $3;
Label REP_TA_nonsaves_in_last_12m_b='Replacement: TA_nonsaves_in_last_12m_b';
format REP_TA_nonsaves_in_last_12m_b $3.;
REP_TA_nonsaves_in_last_12m_b= TA_nonsaves_in_last_12m_b;
Length REP_TA_nonsaves_in_last_24m_b $3;
Label REP_TA_nonsaves_in_last_24m_b='Replacement: TA_nonsaves_in_last_24m_b';
format REP_TA_nonsaves_in_last_24m_b $3.;
REP_TA_nonsaves_in_last_24m_b= TA_nonsaves_in_last_24m_b;
Length REP_TA_nonsaves_in_last_36m_b $3;
Label REP_TA_nonsaves_in_last_36m_b='Replacement: TA_nonsaves_in_last_36m_b';
format REP_TA_nonsaves_in_last_36m_b $3.;
REP_TA_nonsaves_in_last_36m_b= TA_nonsaves_in_last_36m_b;
Length REP_TA_saves_in_last_12m_b $3;
Label REP_TA_saves_in_last_12m_b='Replacement: TA_saves_in_last_12m_b';
format REP_TA_saves_in_last_12m_b $3.;
REP_TA_saves_in_last_12m_b= TA_saves_in_last_12m_b;
Length REP_TA_saves_in_last_14d_b $3;
Label REP_TA_saves_in_last_14d_b='Replacement: TA_saves_in_last_14d_b';
format REP_TA_saves_in_last_14d_b $3.;
REP_TA_saves_in_last_14d_b= TA_saves_in_last_14d_b;
Length REP_TA_saves_in_last_24m_b $3;
Label REP_TA_saves_in_last_24m_b='Replacement: TA_saves_in_last_24m_b';
format REP_TA_saves_in_last_24m_b $3.;
REP_TA_saves_in_last_24m_b= TA_saves_in_last_24m_b;
Length REP_TA_saves_in_last_30d_b $3;
Label REP_TA_saves_in_last_30d_b='Replacement: TA_saves_in_last_30d_b';
format REP_TA_saves_in_last_30d_b $3.;
REP_TA_saves_in_last_30d_b= TA_saves_in_last_30d_b;
Length REP_TA_saves_in_last_36m_b $3;
Label REP_TA_saves_in_last_36m_b='Replacement: TA_saves_in_last_36m_b';
format REP_TA_saves_in_last_36m_b $3.;
REP_TA_saves_in_last_36m_b= TA_saves_in_last_36m_b;
Length REP_TA_saves_in_last_60d_b $3;
Label REP_TA_saves_in_last_60d_b='Replacement: TA_saves_in_last_60d_b';
format REP_TA_saves_in_last_60d_b $3.;
REP_TA_saves_in_last_60d_b= TA_saves_in_last_60d_b;
Length REP_TA_saves_in_last_90d_b $3;
Label REP_TA_saves_in_last_90d_b='Replacement: TA_saves_in_last_90d_b';
format REP_TA_saves_in_last_90d_b $3.;
REP_TA_saves_in_last_90d_b= TA_saves_in_last_90d_b;
Length REP_TAs_in_last_12m_b $3;
Label REP_TAs_in_last_12m_b='Replacement: TAs_in_last_12m_b';
format REP_TAs_in_last_12m_b $3.;
REP_TAs_in_last_12m_b= TAs_in_last_12m_b;
Length REP_TAs_in_last_14d_b $3;
Label REP_TAs_in_last_14d_b='Replacement: TAs_in_last_14d_b';
format REP_TAs_in_last_14d_b $3.;
REP_TAs_in_last_14d_b= TAs_in_last_14d_b;
Length REP_TAs_in_last_24m_b $3;
Label REP_TAs_in_last_24m_b='Replacement: TAs_in_last_24m_b';
format REP_TAs_in_last_24m_b $3.;
REP_TAs_in_last_24m_b= TAs_in_last_24m_b;
Length REP_TAs_in_last_30d_b $3;
Label REP_TAs_in_last_30d_b='Replacement: TAs_in_last_30d_b';
format REP_TAs_in_last_30d_b $3.;
REP_TAs_in_last_30d_b= TAs_in_last_30d_b;
Length REP_TAs_in_last_36m_b $3;
Label REP_TAs_in_last_36m_b='Replacement: TAs_in_last_36m_b';
format REP_TAs_in_last_36m_b $3.;
REP_TAs_in_last_36m_b= TAs_in_last_36m_b;
Length REP_TAs_in_last_60d_b $3;
Label REP_TAs_in_last_60d_b='Replacement: TAs_in_last_60d_b';
format REP_TAs_in_last_60d_b $3.;
REP_TAs_in_last_60d_b= TAs_in_last_60d_b;
Length REP_TAs_in_last_90d_b $3;
Label REP_TAs_in_last_90d_b='Replacement: TAs_in_last_90d_b';
format REP_TAs_in_last_90d_b $3.;
REP_TAs_in_last_90d_b= TAs_in_last_90d_b;
Length REP__1st_TA_b $12;
Label REP__1st_TA_b='Replacement: _1st_TA_b';
format REP__1st_TA_b $12.;
REP__1st_TA_b= _1st_TA_b;
Length REP__1st_TA_nonsave_b $12;
Label REP__1st_TA_nonsave_b='Replacement: _1st_TA_nonsave_b';
format REP__1st_TA_nonsave_b $12.;
REP__1st_TA_nonsave_b= _1st_TA_nonsave_b;
Length REP__1st_TA_reason_flag $15;
Label REP__1st_TA_reason_flag='Replacement: _1st_TA_reason_flag';
format REP__1st_TA_reason_flag $15.;
REP__1st_TA_reason_flag= _1st_TA_reason_flag;
Length REP__1st_TA_save_b $12;
Label REP__1st_TA_save_b='Replacement: _1st_TA_save_b';
format REP__1st_TA_save_b $12.;
REP__1st_TA_save_b= _1st_TA_save_b;
Length REP_bb_last_tenure $12;
Label REP_bb_last_tenure='Replacement: bb_last_tenure';
format REP_bb_last_tenure $12.;
REP_bb_last_tenure= bb_last_tenure;
Length REP_dtv_1st_tenure $12;
Label REP_dtv_1st_tenure='Replacement: dtv_1st_tenure';
format REP_dtv_1st_tenure $12.;
REP_dtv_1st_tenure= dtv_1st_tenure;
Length REP_dtv_last_tenure $12;
Label REP_dtv_last_tenure='Replacement: dtv_last_tenure';
format REP_dtv_last_tenure $12.;
REP_dtv_last_tenure= dtv_last_tenure;
Length REP_financial_strategy $50;
Label REP_financial_strategy='Replacement: financial_strategy';
format REP_financial_strategy $50.;
REP_financial_strategy= financial_strategy;
Length REP_h_family_lifestage $50;
Label REP_h_family_lifestage='Replacement: h_family_lifestage';
format REP_h_family_lifestage $50.;
REP_h_family_lifestage= h_family_lifestage;
Length REP_h_household_composition $50;
Label REP_h_household_composition='Replacement: h_household_composition';
format REP_h_household_composition $50.;
REP_h_household_composition= h_household_composition;
Length REP_h_mosaic_group $50;
Label REP_h_mosaic_group='Replacement: h_mosaic_group';
format REP_h_mosaic_group $50.;
REP_h_mosaic_group= h_mosaic_group;
Length REP_h_number_of_adults_b $7;
Label REP_h_number_of_adults_b='Replacement: h_number_of_adults_b';
format REP_h_number_of_adults_b $7.;
REP_h_number_of_adults_b= h_number_of_adults_b;
Length REP_h_number_of_bedrooms_b $7;
Label REP_h_number_of_bedrooms_b='Replacement: h_number_of_bedrooms_b';
format REP_h_number_of_bedrooms_b $7.;
REP_h_number_of_bedrooms_b= h_number_of_bedrooms_b;
Length REP_h_number_of_children_in_hous $7;
Label REP_h_number_of_children_in_hous='Replacement: h_number_of_children_in_house_b';
format REP_h_number_of_children_in_hous $7.;
REP_h_number_of_children_in_hous= h_number_of_children_in_house_b;
Length REP_h_presence_of_child_aged_0_4 $10;
Label REP_h_presence_of_child_aged_0_4='Replacement: h_presence_of_child_aged_0_4';
format REP_h_presence_of_child_aged_0_4 $10.;
REP_h_presence_of_child_aged_0_4= h_presence_of_child_aged_0_4;
Length REP_h_presence_of_child_aged_12_ $10;
Label REP_h_presence_of_child_aged_12_='Replacement: h_presence_of_child_aged_12_17';
format REP_h_presence_of_child_aged_12_ $10.;
REP_h_presence_of_child_aged_12_= h_presence_of_child_aged_12_17;
Length REP_h_presence_of_child_aged_5_1 $10;
Label REP_h_presence_of_child_aged_5_1='Replacement: h_presence_of_child_aged_5_11';
format REP_h_presence_of_child_aged_5_1 $10.;
REP_h_presence_of_child_aged_5_1= h_presence_of_child_aged_5_11;
Length REP_h_presence_of_young_person_a $10;
Label REP_h_presence_of_young_person_a='Replacement: h_presence_of_young_person_at_ad';
format REP_h_presence_of_young_person_a $10.;
REP_h_presence_of_young_person_a= h_presence_of_young_person_at_ad;
Length REP_h_property_type $50;
Label REP_h_property_type='Replacement: h_property_type';
format REP_h_property_type $50.;
REP_h_property_type= h_property_type;
Length REP_h_residence_type $50;
Label REP_h_residence_type='Replacement: h_residence_type';
format REP_h_residence_type $50.;
REP_h_residence_type= h_residence_type;
Length REP_last_TA_b $12;
Label REP_last_TA_b='Replacement: last_TA_b';
format REP_last_TA_b $12.;
REP_last_TA_b= last_TA_b;
Length REP_last_TA_nonsave_b $12;
Label REP_last_TA_nonsave_b='Replacement: last_TA_nonsave_b';
format REP_last_TA_nonsave_b $12.;
REP_last_TA_nonsave_b= last_TA_nonsave_b;
Length REP_last_TA_reason_flag $15;
Label REP_last_TA_reason_flag='Replacement: last_TA_reason_flag';
format REP_last_TA_reason_flag $15.;
REP_last_TA_reason_flag= last_TA_reason_flag;
Length REP_last_TA_save_b $12;
Label REP_last_TA_save_b='Replacement: last_TA_save_b';
format REP_last_TA_save_b $12.;
REP_last_TA_save_b= last_TA_save_b;
Length REP_p_true_touch_group $50;
Label REP_p_true_touch_group='Replacement: p_true_touch_group';
format REP_p_true_touch_group $50.;
REP_p_true_touch_group= p_true_touch_group;
Length REP_p_true_touch_type $2;
Label REP_p_true_touch_type='Replacement: p_true_touch_type';
format REP_p_true_touch_type $2.;
REP_p_true_touch_type= p_true_touch_type;
Length REP_skyfibre_enabled $1;
Label REP_skyfibre_enabled='Replacement: skyfibre_enabled';
format REP_skyfibre_enabled $1.;
REP_skyfibre_enabled= skyfibre_enabled;
* ;
* Replace Unknown Class Levels ;
* ;
length _UFORMAT200 $200;
drop   _UFORMAT200;
_UFORMAT200 = " ";
*;
_UFORMAT200 = strip(put(ADSL_Enabled,$1.));
if ^(_UFORMAT200 in(
"Y", "U", "N"
, "" )) then
REP_ADSL_Enabled= "";
*;
_UFORMAT200 = strip(put(BB_3rdParty_PL_Entry_Ever_b,$3.));
if ^(_UFORMAT200 in(
"0", "1"
, "" )) then
REP_BB_3rdParty_PL_Entry_Ever_b= "";
*;
_UFORMAT200 = strip(put(BB_3rdParty_PL_Entry_Last_180D_b,$3.));
if ^(_UFORMAT200 in(
"0", "1"
, "" )) then
REP_BB_3rdParty_PL_Entry_Last_18= "";
*;
_UFORMAT200 = strip(put(BB_3rdParty_PL_Entry_Last_1Yr_b,$3.));
if ^(_UFORMAT200 in(
"0", "1"
, "" )) then
REP_BB_3rdParty_PL_Entry_Last_1Y= "";
*;
_UFORMAT200 = strip(put(BB_3rdParty_PL_Entry_Last_3Yr_b,$3.));
if ^(_UFORMAT200 in(
"0", "1"
, "" )) then
REP_BB_3rdParty_PL_Entry_Last_3Y= "";
*;
_UFORMAT200 = strip(put(BB_3rdParty_PL_Entry_Last_5Yr_b,$3.));
if ^(_UFORMAT200 in(
"0", "1"
, "" )) then
REP_BB_3rdParty_PL_Entry_Last_5Y= "";
*;
if (
BB_Active ne 1 and
BB_Active ne 0 and
BB_Active ne . ) then
REP_BB_Active= .;
*;
_UFORMAT200 = strip(put(BB_Churns_Ever_b,$3.));
if ^(_UFORMAT200 in(
"0", "1"
, "" )) then
REP_BB_Churns_Ever_b= "";
*;
_UFORMAT200 = strip(put(BB_Churns_Last_180D_b,$3.));
if ^(_UFORMAT200 in(
"0", "1"
, "" )) then
REP_BB_Churns_Last_180D_b= "";
*;
_UFORMAT200 = strip(put(BB_Churns_Last_1Yr_b,$3.));
if ^(_UFORMAT200 in(
"0", "1"
, "" )) then
REP_BB_Churns_Last_1Yr_b= "";
*;
_UFORMAT200 = strip(put(BB_Churns_Last_3Yr_b,$3.));
if ^(_UFORMAT200 in(
"0", "1"
, "" )) then
REP_BB_Churns_Last_3Yr_b= "";
*;
_UFORMAT200 = strip(put(BB_Churns_Last_5Yr_b,$3.));
if ^(_UFORMAT200 in(
"0", "1"
, "" )) then
REP_BB_Churns_Last_5Yr_b= "";
*;
_UFORMAT200 = strip(put(BB_Churns_Last_90D_b,$3.));
if ^(_UFORMAT200 in(
"0", "1"
, "" )) then
REP_BB_Churns_Last_90D_b= "";
*;
_UFORMAT200 = strip(put(BB_CusCan_PL_Entry_Ever_b,$3.));
if ^(_UFORMAT200 in(
"0", "1"
, "" )) then
REP_BB_CusCan_PL_Entry_Ever_b= "";
*;
_UFORMAT200 = strip(put(BB_CusCan_PL_Entry_Last_180D_b,$3.));
if ^(_UFORMAT200 in(
"0", "1"
, "" )) then
REP_BB_CusCan_PL_Entry_Last_180D= "";
*;
_UFORMAT200 = strip(put(BB_CusCan_PL_Entry_Last_1Yr_b,$3.));
if ^(_UFORMAT200 in(
"0", "1"
, "" )) then
REP_BB_CusCan_PL_Entry_Last_1Yr_= "";
*;
_UFORMAT200 = strip(put(BB_CusCan_PL_Entry_Last_3Yr_b,$3.));
if ^(_UFORMAT200 in(
"0", "1"
, "" )) then
REP_BB_CusCan_PL_Entry_Last_3Yr_= "";
*;
_UFORMAT200 = strip(put(BB_CusCan_PL_Entry_Last_5Yr_b,$3.));
if ^(_UFORMAT200 in(
"0", "1"
, "" )) then
REP_BB_CusCan_PL_Entry_Last_5Yr_= "";
*;
_UFORMAT200 = strip(put(BB_HomeMove_PL_Entry_Ever_b,$3.));
if ^(_UFORMAT200 in(
"0", "1"
, "" )) then
REP_BB_HomeMove_PL_Entry_Ever_b= "";
*;
_UFORMAT200 = strip(put(BB_HomeMove_PL_Entry_Last_1Yr_b,$3.));
if ^(_UFORMAT200 in(
"0", "1"
, "" )) then
REP_BB_HomeMove_PL_Entry_Last_1Y= "";
*;
_UFORMAT200 = strip(put(BB_HomeMove_PL_Entry_Last_3Yr_b,$3.));
if ^(_UFORMAT200 in(
"0", "1"
, "" )) then
REP_BB_HomeMove_PL_Entry_Last_3Y= "";
*;
_UFORMAT200 = strip(put(BB_HomeMove_PL_Entry_Last_5Yr_b,$3.));
if ^(_UFORMAT200 in(
"0", "1"
, "" )) then
REP_BB_HomeMove_PL_Entry_Last_5Y= "";
*;
_UFORMAT200 = strip(put(BB_Product_Holding,$80.));
if ^(_UFORMAT200 in(
"", "Unlimited (Legacy)", "Unlimited Fibre", "Unlimited", "Sky Broadband Lite"
, "Fibre Max", "Sky Fibre", "Connect", "Fibre Lite", "Fibre Unlimited Pro"
, "12GB", "Unlimited Pro", "Everyday"
)) then
REP_BB_Product_Holding= "Other";
*;
_UFORMAT200 = strip(put(BB_Provider,$20.));
if ^(_UFORMAT200 in(
"BskyB", "Unknown", "bt", "talkta", "none", "virgin", "plusne", "vodafo"
, "telefo", "easyne", "h3g", "janet"
, "" )) then
REP_BB_Provider= "Other";
*;
_UFORMAT200 = strip(put(BB_Status_Code,$10.));
if ^(_UFORMAT200 in(
"AC", "", "PC", "BCRQ", "AB", "PT"
)) then
REP_BB_Status_Code= "";
*;
_UFORMAT200 = strip(put(BB_SysCan_PL_Entry_Ever_b,$3.));
if ^(_UFORMAT200 in(
"0", "1"
, "" )) then
REP_BB_SysCan_PL_Entry_Ever_b= "";
*;
_UFORMAT200 = strip(put(BB_SysCan_PL_Entry_Last_180D_b,$3.));
if ^(_UFORMAT200 in(
"0", "1"
, "" )) then
REP_BB_SysCan_PL_Entry_Last_180D= "";
*;
_UFORMAT200 = strip(put(BB_SysCan_PL_Entry_Last_1Yr_b,$3.));
if ^(_UFORMAT200 in(
"0", "1"
, "" )) then
REP_BB_SysCan_PL_Entry_Last_1Yr_= "";
*;
_UFORMAT200 = strip(put(BB_SysCan_PL_Entry_Last_3Yr_b,$3.));
if ^(_UFORMAT200 in(
"0", "1"
, "" )) then
REP_BB_SysCan_PL_Entry_Last_3Yr_= "";
*;
_UFORMAT200 = strip(put(BB_SysCan_PL_Entry_Last_5Yr_b,$3.));
if ^(_UFORMAT200 in(
"0", "1"
, "" )) then
REP_BB_SysCan_PL_Entry_Last_5Yr_= "";
*;
_UFORMAT200 = strip(put(BB_SysCan_PL_Entry_Last_90D_b,$3.));
if ^(_UFORMAT200 in(
"0", "1"
, "" )) then
REP_BB_SysCan_PL_Entry_Last_90D_= "";
*;
_UFORMAT200 = strip(put(BB_contract_segment,$23.));
if ^(_UFORMAT200 in(
"A.NeverOnContract", "B.ExpiredContract(>6M)", "F.ExpiringContract(>6M)"
, "C.ExpiredContract(<6M)", "E.ExpiringContract(<6M)"
, "D.ExpiringContract(<3M)"
, "" )) then
REP_BB_contract_segment= "";
*;
_UFORMAT200 = strip(put(Curr_Offer_Amount_DTV_b,$17.));
if ^(_UFORMAT200 in(
"A.No offer", "B.Less than 20", "C.Greater than 20"
, "" )) then
REP_Curr_Offer_Amount_DTV_b= "";
*;
_UFORMAT200 = strip(put(Curr_Offer_Amount_DTV_flag,$15.));
if ^(_UFORMAT200 in(
"A.Curr no offer", "B.Curr on offer"
, "" )) then
REP_Curr_Offer_Amount_DTV_flag= "";
*;
_UFORMAT200 = strip(put(Curr_Offer_Length_DTV_b,$18.));
if ^(_UFORMAT200 in(
"A.No offer", "B.Less than 12M", "C.Greater than 12M"
, "" )) then
REP_Curr_Offer_Length_DTV_b= "";
*;
_UFORMAT200 = strip(put(DTV_Active_Blocks_Ever_b,$3.));
if ^(_UFORMAT200 in(
"0", "1"
, "" )) then
REP_DTV_Active_Blocks_Ever_b= "";
*;
_UFORMAT200 = strip(put(DTV_Active_Blocks_Last_180D_b,$3.));
if ^(_UFORMAT200 in(
"0", "1"
, "" )) then
REP_DTV_Active_Blocks_Last_180D_= "";
*;
_UFORMAT200 = strip(put(DTV_Active_Blocks_Last_1Yr_b,$3.));
if ^(_UFORMAT200 in(
"0", "1"
, "" )) then
REP_DTV_Active_Blocks_Last_1Yr_b= "";
*;
_UFORMAT200 = strip(put(DTV_Active_Blocks_Last_3Yr_b,$3.));
if ^(_UFORMAT200 in(
"0", "1"
, "" )) then
REP_DTV_Active_Blocks_Last_3Yr_b= "";
*;
_UFORMAT200 = strip(put(DTV_Active_Blocks_Last_5Yr_b,$3.));
if ^(_UFORMAT200 in(
"0", "1"
, "" )) then
REP_DTV_Active_Blocks_Last_5Yr_b= "";
*;
_UFORMAT200 = strip(put(DTV_Active_Blocks_Last_90D_b,$3.));
if ^(_UFORMAT200 in(
"0", "1"
, "" )) then
REP_DTV_Active_Blocks_Last_90D_b= "";
*;
_UFORMAT200 = strip(put(DTV_Churns_Ever_b,$3.));
if ^(_UFORMAT200 in(
"0", "1"
, "" )) then
REP_DTV_Churns_Ever_b= "";
*;
_UFORMAT200 = strip(put(DTV_Churns_Last_1Yr_b,$3.));
if ^(_UFORMAT200 in(
"0", "1"
, "" )) then
REP_DTV_Churns_Last_1Yr_b= "";
*;
_UFORMAT200 = strip(put(DTV_Churns_Last_3Yr_b,$3.));
if ^(_UFORMAT200 in(
"0", "1"
, "" )) then
REP_DTV_Churns_Last_3Yr_b= "";
*;
_UFORMAT200 = strip(put(DTV_Churns_Last_5Yr_b,$3.));
if ^(_UFORMAT200 in(
"0", "1"
, "" )) then
REP_DTV_Churns_Last_5Yr_b= "";
*;
_UFORMAT200 = strip(put(DTV_CusCan_Churns_Last_180D_b,$3.));
if ^(_UFORMAT200 in(
"0", "1"
, "" )) then
REP_DTV_CusCan_Churns_Last_180D_= "";
*;
_UFORMAT200 = strip(put(DTV_CusCan_Churns_Last_1Yr_b,$3.));
if ^(_UFORMAT200 in(
"0", "1"
, "" )) then
REP_DTV_CusCan_Churns_Last_1Yr_b= "";
*;
_UFORMAT200 = strip(put(DTV_CusCan_Churns_Last_3Yr_b,$3.));
if ^(_UFORMAT200 in(
"0", "1"
, "" )) then
REP_DTV_CusCan_Churns_Last_3Yr_b= "";
*;
_UFORMAT200 = strip(put(DTV_CusCan_Churns_Last_5Yr_b,$3.));
if ^(_UFORMAT200 in(
"0", "1"
, "" )) then
REP_DTV_CusCan_Churns_Last_5Yr_b= "";
*;
_UFORMAT200 = strip(put(DTV_PO_Cancellations_Ever_b,$3.));
if ^(_UFORMAT200 in(
"0", "1"
, "" )) then
REP_DTV_PO_Cancellations_Ever_b= "";
*;
_UFORMAT200 = strip(put(DTV_PO_Cancellations_Last_1Yr_b,$3.));
if ^(_UFORMAT200 in(
"0", "1"
, "" )) then
REP_DTV_PO_Cancellations_Last_1Y= "";
*;
_UFORMAT200 = strip(put(DTV_PO_Cancellations_Last_3Yr_b,$3.));
if ^(_UFORMAT200 in(
"0", "1"
, "" )) then
REP_DTV_PO_Cancellations_Last_3Y= "";
*;
_UFORMAT200 = strip(put(DTV_PO_Cancellations_Last_5Yr_b,$3.));
if ^(_UFORMAT200 in(
"0", "1"
, "" )) then
REP_DTV_PO_Cancellations_Last_5Y= "";
*;
_UFORMAT200 = strip(put(DTV_Pending_Cancels_Last_180D_b,$3.));
if ^(_UFORMAT200 in(
"0", "1"
, "" )) then
REP_DTV_Pending_Cancels_Last_180= "";
*;
_UFORMAT200 = strip(put(DTV_Pending_Cancels_Last_1Yr_b,$3.));
if ^(_UFORMAT200 in(
"0", "1"
, "" )) then
REP_DTV_Pending_Cancels_Last_1Yr= "";
*;
_UFORMAT200 = strip(put(DTV_Pending_Cancels_Last_30D_b,$3.));
if ^(_UFORMAT200 in(
"0", "1"
, "" )) then
REP_DTV_Pending_Cancels_Last_30D= "";
*;
_UFORMAT200 = strip(put(DTV_Pending_Cancels_Last_3Yr_b,$3.));
if ^(_UFORMAT200 in(
"0", "1"
, "" )) then
REP_DTV_Pending_Cancels_Last_3Yr= "";
*;
_UFORMAT200 = strip(put(DTV_Pending_Cancels_Last_5Yr_b,$3.));
if ^(_UFORMAT200 in(
"0", "1"
, "" )) then
REP_DTV_Pending_Cancels_Last_5Yr= "";
*;
_UFORMAT200 = strip(put(DTV_Pending_Cancels_Last_90D_b,$3.));
if ^(_UFORMAT200 in(
"0", "1"
, "" )) then
REP_DTV_Pending_Cancels_Last_90D= "";
*;
_UFORMAT200 = strip(put(DTV_SysCan_Churns_In_Last_90D_b,$3.));
if ^(_UFORMAT200 in(
"0", "1"
, "" )) then
REP_DTV_SysCan_Churns_In_Last_90= "";
*;
_UFORMAT200 = strip(put(DTV_SysCan_Churns_Last_180D_b,$3.));
if ^(_UFORMAT200 in(
"0", "1"
, "" )) then
REP_DTV_SysCan_Churns_Last_180D_= "";
*;
_UFORMAT200 = strip(put(DTV_SysCan_Churns_Last_1Yr_b,$3.));
if ^(_UFORMAT200 in(
"0", "1"
, "" )) then
REP_DTV_SysCan_Churns_Last_1Yr_b= "";
*;
_UFORMAT200 = strip(put(DTV_SysCan_Churns_Last_30D_b,$3.));
if ^(_UFORMAT200 in(
"0", "1"
, "" )) then
REP_DTV_SysCan_Churns_Last_30D_b= "";
*;
_UFORMAT200 = strip(put(DTV_SysCan_Churns_Last_3Yr_b,$3.));
if ^(_UFORMAT200 in(
"0", "1"
, "" )) then
REP_DTV_SysCan_Churns_Last_3Yr_b= "";
*;
_UFORMAT200 = strip(put(DTV_SysCan_Churns_Last_5Yr_b,$3.));
if ^(_UFORMAT200 in(
"0", "1"
, "" )) then
REP_DTV_SysCan_Churns_Last_5Yr_b= "";
*;
_UFORMAT200 = strip(put(DTV_contract_segment,$23.));
if ^(_UFORMAT200 in(
"B.ExpiredContract(>6M)", "F.ExpiringContract(>6M)", "C.ExpiredContract(<6M)"
, "E.ExpiringContract(<6M)", "D.ExpiringContract(<3M)", "A.NeverOnContract"
, "" )) then
REP_DTV_contract_segment= "";
*;
_UFORMAT200 = strip(put(DTV_product_holding_recode,$40.));
if ^(_UFORMAT200 in(
"Variety", "Variety with Sports & Cinema", "Original", "Variety with Sports"
, "Variety with Cinema", "Original with Sports", "Original with Cinema"
, "Original with Sports & Cinema"
, "" )) then
REP_DTV_product_holding_recode= "";
*;
_UFORMAT200 = strip(put(Dtv_Package,$10.));
if ^(_UFORMAT200 in(
"B.Variety", "C.Original"
, "" )) then
REP_Dtv_Package= "";
*;
_UFORMAT200 = strip(put(Home_Owner_Status,$30.));
if ^(_UFORMAT200 in(
"Owner", "Council Rent", "Private Rent", "UNKNOWN"
, "" )) then
REP_Home_Owner_Status= "";
*;
_UFORMAT200 = strip(put(Movies_Tenure,$20.));
if ^(_UFORMAT200 in(
"", "D.10+ Yrs", "A.<2 Yrs", "C.<10 Yrs", "B.<5 Yrs"
)) then
REP_Movies_Tenure= "";
*;
_UFORMAT200 = strip(OPT_BB_Last_Activation);
if ^(_UFORMAT200 in(
"02:63.5-3216.5", "03:3216.5-3311.5, MISSING", "04:3311.5-high", "01:low-63.5"
, "" )) then
REP_OPT_BB_Last_Activation= "";
*;
_UFORMAT200 = strip(OPT_Curr_Offer_Actual_End_DTV);
if ^(_UFORMAT200 in(
"03:-326.5--34.5, MISSING", "02:-549.5--326.5", "04:-34.5-high"
, "01:low--549.5"
, "" )) then
REP_OPT_Curr_Offer_Actual_End_DT= "";
*;
_UFORMAT200 = strip(OPT_Curr_Offer_Start_DTV);
if ^(_UFORMAT200 in(
"04:375.5-high, MISSING", "02:19.5-319.5", "01:low-19.5", "03:319.5-375.5"
, "" )) then
REP_OPT_Curr_Offer_Start_DTV= "";
*;
_UFORMAT200 = strip(OPT_DTV_1st_Activation);
if ^(_UFORMAT200 in(
"02:41.5-high, MISSING", "01:low-41.5"
, "" )) then
REP_OPT_DTV_1st_Activation= "";
*;
_UFORMAT200 = strip(OPT_DTV_Curr_Contract_Intended_E);
if ^(_UFORMAT200 in(
"03:-331.5--58.5, MISSING", "02:-552.5--331.5", "04:-58.5-high"
, "01:low--552.5"
, "" )) then
REP_OPT_DTV_Curr_Contract_Intend= "";
*;
_UFORMAT200 = strip(OPT_DTV_Last_Activation);
if ^(_UFORMAT200 in(
"02:48.5-high, MISSING", "01:low-48.5"
, "" )) then
REP_OPT_DTV_Last_Activation= "";
*;
_UFORMAT200 = strip(OPT_DTV_Last_Active_Block);
if ^(_UFORMAT200 in(
"02:596-high, MISSING", "01:low-596"
, "" )) then
REP_OPT_DTV_Last_Active_Block= "";
*;
_UFORMAT200 = strip(OPT_DTV_Last_Pending_Cancel);
if ^(_UFORMAT200 in(
"03:2697.5-high, MISSING", "02:263.5-2697.5", "01:low-263.5"
, "" )) then
REP_OPT_DTV_Last_Pending_Cancel= "";
*;
_UFORMAT200 = strip(OPT_DTV_Last_cuscan_churn);
if ^(_UFORMAT200 in(
"02:230.5-high, MISSING", "01:low-230.5"
, "" )) then
REP_OPT_DTV_Last_cuscan_churn= "";
*;
_UFORMAT200 = strip(OPT_LAST_movies_downgrade);
if ^(_UFORMAT200 in(
"03:37.5-high, MISSING", "02:1.5-37.5", "01:low-1.5"
, "" )) then
REP_OPT_LAST_movies_downgrade= "";
*;
_UFORMAT200 = strip(OPT_LAST_sports_downgrade);
if ^(_UFORMAT200 in(
"02:14.5-high, MISSING", "01:low-14.5"
, "" )) then
REP_OPT_LAST_sports_downgrade= "";
*;
_UFORMAT200 = strip(OPT_OD_Last_12M);
if ^(_UFORMAT200 in(
"01:low-169.5", "02:169.5-921", "_MISSING_", "03:921-high"
, "" )) then
REP_OPT_OD_Last_12M= "";
*;
_UFORMAT200 = strip(OPT_OD_Last_3M);
if ^(_UFORMAT200 in(
"01:low-37.5", "02:37.5-153.5", "_MISSING_", "03:153.5-high"
, "" )) then
REP_OPT_OD_Last_3M= "";
*;
_UFORMAT200 = strip(OPT_OD_Months_since_Last);
if ^(_UFORMAT200 in(
"01:low-0.5", "_MISSING_", "02:0.5-high"
, "" )) then
REP_OPT_OD_Months_since_Last= "";
*;
_UFORMAT200 = strip(OPT_Prev_Offer_Amount_DTV);
if ^(_UFORMAT200 in(
"02:-10.945--10.59, MISSING", "03:-10.59--2.3", "01:low--10.945"
, "04:-2.3-high"
, "" )) then
REP_OPT_Prev_Offer_Amount_DTV= "";
*;
_UFORMAT200 = strip(OPT_h_income_value);
if ^(_UFORMAT200 in(
"02:29430-high", "01:low-29430, MISSING"
, "" )) then
REP_OPT_h_income_value= "";
*;
_UFORMAT200 = strip(OPT_num_sports_events);
if ^(_UFORMAT200 in(
"01:low-0.5, MISSING", "02:0.5-1.5", "03:1.5-high"
, "" )) then
REP_OPT_num_sports_events= "";
*;
_UFORMAT200 = strip(put(Offers_Applied_Lst_12M_DTV_b,$3.));
if ^(_UFORMAT200 in(
"0", "1"
, "" )) then
REP_Offers_Applied_Lst_12M_DTV_b= "";
*;
_UFORMAT200 = strip(put(Offers_Applied_Lst_24M_DTV_b,$3.));
if ^(_UFORMAT200 in(
"1", "0"
, "" )) then
REP_Offers_Applied_Lst_24M_DTV_b= "";
*;
_UFORMAT200 = strip(put(Offers_Applied_Lst_30D_DTV_b,$3.));
if ^(_UFORMAT200 in(
"0", "1"
, "" )) then
REP_Offers_Applied_Lst_30D_DTV_b= "";
*;
_UFORMAT200 = strip(put(Offers_Applied_Lst_36M_DTV_b,$3.));
if ^(_UFORMAT200 in(
"1", "0"
, "" )) then
REP_Offers_Applied_Lst_36M_DTV_b= "";
*;
_UFORMAT200 = strip(put(Offers_Applied_Lst_90D_DTV_b,$3.));
if ^(_UFORMAT200 in(
"0", "1"
, "" )) then
REP_Offers_Applied_Lst_90D_DTV_b= "";
*;
_UFORMAT200 = strip(put(Prev_Offer_Amount_DTV_b,$17.));
if ^(_UFORMAT200 in(
"B.Less than 20", "A.No offer", "C.Greater than 20"
, "" )) then
REP_Prev_Offer_Amount_DTV_b= "";
*;
_UFORMAT200 = strip(put(Prev_Offer_Amount_DTV_flag,$15.));
if ^(_UFORMAT200 in(
"B.Prev on offer", "A.Prev no offer"
, "" )) then
REP_Prev_Offer_Amount_DTV_flag= "";
*;
_UFORMAT200 = strip(put(Prev_Offer_Length_DTV_b,$17.));
if ^(_UFORMAT200 in(
"B.Less than 9M", "A.No offer", "C.Greater than 9M"
, "" )) then
REP_Prev_Offer_Length_DTV_b= "";
*;
_UFORMAT200 = strip(put(Sports_Tenure,$20.));
if ^(_UFORMAT200 in(
"", "D.10+ Yrs", "C.<10 Yrs", "A.<2 Yrs", "B.<5 Yrs"
)) then
REP_Sports_Tenure= "";
*;
_UFORMAT200 = strip(put(TA_nonsaves_in_last_12m_b,$3.));
if ^(_UFORMAT200 in(
"0", "1"
, "" )) then
REP_TA_nonsaves_in_last_12m_b= "";
*;
_UFORMAT200 = strip(put(TA_nonsaves_in_last_24m_b,$3.));
if ^(_UFORMAT200 in(
"0", "1"
, "" )) then
REP_TA_nonsaves_in_last_24m_b= "";
*;
_UFORMAT200 = strip(put(TA_nonsaves_in_last_36m_b,$3.));
if ^(_UFORMAT200 in(
"0", "1"
, "" )) then
REP_TA_nonsaves_in_last_36m_b= "";
*;
_UFORMAT200 = strip(put(TA_saves_in_last_12m_b,$3.));
if ^(_UFORMAT200 in(
"0", "1"
, "" )) then
REP_TA_saves_in_last_12m_b= "";
*;
_UFORMAT200 = strip(put(TA_saves_in_last_14d_b,$3.));
if ^(_UFORMAT200 in(
"0", "1"
, "" )) then
REP_TA_saves_in_last_14d_b= "";
*;
_UFORMAT200 = strip(put(TA_saves_in_last_24m_b,$3.));
if ^(_UFORMAT200 in(
"0", "1"
, "" )) then
REP_TA_saves_in_last_24m_b= "";
*;
_UFORMAT200 = strip(put(TA_saves_in_last_30d_b,$3.));
if ^(_UFORMAT200 in(
"0", "1"
, "" )) then
REP_TA_saves_in_last_30d_b= "";
*;
_UFORMAT200 = strip(put(TA_saves_in_last_36m_b,$3.));
if ^(_UFORMAT200 in(
"0", "1"
, "" )) then
REP_TA_saves_in_last_36m_b= "";
*;
_UFORMAT200 = strip(put(TA_saves_in_last_60d_b,$3.));
if ^(_UFORMAT200 in(
"0", "1"
, "" )) then
REP_TA_saves_in_last_60d_b= "";
*;
_UFORMAT200 = strip(put(TA_saves_in_last_90d_b,$3.));
if ^(_UFORMAT200 in(
"0", "1"
, "" )) then
REP_TA_saves_in_last_90d_b= "";
*;
_UFORMAT200 = strip(put(TAs_in_last_12m_b,$3.));
if ^(_UFORMAT200 in(
"0", "1"
, "" )) then
REP_TAs_in_last_12m_b= "";
*;
_UFORMAT200 = strip(put(TAs_in_last_14d_b,$3.));
if ^(_UFORMAT200 in(
"0", "1"
, "" )) then
REP_TAs_in_last_14d_b= "";
*;
_UFORMAT200 = strip(put(TAs_in_last_24m_b,$3.));
if ^(_UFORMAT200 in(
"0", "1"
, "" )) then
REP_TAs_in_last_24m_b= "";
*;
_UFORMAT200 = strip(put(TAs_in_last_30d_b,$3.));
if ^(_UFORMAT200 in(
"0", "1"
, "" )) then
REP_TAs_in_last_30d_b= "";
*;
_UFORMAT200 = strip(put(TAs_in_last_36m_b,$3.));
if ^(_UFORMAT200 in(
"0", "1"
, "" )) then
REP_TAs_in_last_36m_b= "";
*;
_UFORMAT200 = strip(put(TAs_in_last_60d_b,$3.));
if ^(_UFORMAT200 in(
"0", "1"
, "" )) then
REP_TAs_in_last_60d_b= "";
*;
_UFORMAT200 = strip(put(TAs_in_last_90d_b,$3.));
if ^(_UFORMAT200 in(
"0", "1"
, "" )) then
REP_TAs_in_last_90d_b= "";
*;
_UFORMAT200 = strip(put(_1st_TA_b,$12.));
if ^(_UFORMAT200 in(
"J.Other", "F.3-4 Years", "H.7-10 Years", "G.5-6 Years", "E.1-2 Years"
, "D.<1 Year", "I.11+ Years", "C.<6 Months", "B.<3 Months", "A.<1 Month"
, "" )) then
REP__1st_TA_b= "";
*;
_UFORMAT200 = strip(put(_1st_TA_nonsave_b,$12.));
if ^(_UFORMAT200 in(
"J.Other", "F.3-4 Years", "E.1-2 Years", "D.<1 Year", "H.7-10 Years"
, "G.5-6 Years", "C.<6 Months", "B.<3 Months", "I.11+ Years", "A.<1 Month"
, "" )) then
REP__1st_TA_nonsave_b= "";
*;
_UFORMAT200 = strip(put(_1st_TA_reason_flag,$15.));
if ^(_UFORMAT200 in(
"No reason given", "Reason given"
, "" )) then
REP__1st_TA_reason_flag= "";
*;
_UFORMAT200 = strip(put(_1st_TA_save_b,$12.));
if ^(_UFORMAT200 in(
"J.Other", "F.3-4 Years", "H.7-10 Years", "G.5-6 Years", "E.1-2 Years"
, "D.<1 Year", "C.<6 Months", "I.11+ Years", "B.<3 Months", "A.<1 Month"
, "" )) then
REP__1st_TA_save_b= "";
*;
_UFORMAT200 = strip(put(bb_last_tenure,$12.));
if ^(_UFORMAT200 in(
"I.Other", "G.7-10 Years", "E.3-4 Years", "F.5-6 Years", "D.1-2 Years"
, "C.<1 Year", "A.<3 Months", "B.<6 Months", "H.11+ Years"
, "" )) then
REP_bb_last_tenure= "";
*;
_UFORMAT200 = strip(put(dtv_1st_tenure,$12.));
if ^(_UFORMAT200 in(
"H.11+ Years", "G.7-10 Years", "F.5-6 Years", "E.3-4 Years", "D.1-2 Years"
, "C.<1 Year", "A.<3 Months", "B.<6 Months", "I.Other"
, "" )) then
REP_dtv_1st_tenure= "";
*;
_UFORMAT200 = strip(put(dtv_last_tenure,$12.));
if ^(_UFORMAT200 in(
"H.11+ Years", "G.7-10 Years", "F.5-6 Years", "E.3-4 Years", "D.1-2 Years"
, "C.<1 Year", "A.<3 Months", "B.<6 Months", "I.Other"
, "" )) then
REP_dtv_last_tenure= "";
*;
_UFORMAT200 = strip(put(financial_strategy,$50.));
if ^(_UFORMAT200 in(
"Balancing Budgets", "Established Reserves", "Sunset Security"
, "Consolidating Assets", "Growing Rewards", "Unallocated", "Single Endeavours"
, "Traditional Thrift", "Stretched Finances", "Bright Futures"
, "Seasoned Economy", "Family Interest", "Platinum Pensions"
, "Accumulated Wealth", "Young Essentials"
, "" )) then
REP_financial_strategy= "";
*;
_UFORMAT200 = strip(put(h_family_lifestage,$50.));
if ^(_UFORMAT200 in(
"Older family/household with children <18", "Older family no children <18"
, "Elderly family no children <18", "Older single", "Elderly single"
, "Mature family with children <18", "Unclassified"
, "Young singles/homesharers", "Mature singles/homesharers"
, "Young family with children <18", "Mature household with children <18"
, "Young family no children <18", "Mature family no children <18"
, "Young household with children <18"
, "" )) then
REP_h_family_lifestage= "";
*;
_UFORMAT200 = strip(put(h_household_composition,$50.));
if ^(_UFORMAT200 in(
"Families", "Single Female", "Extended Family", "Single Male", "Pseudo Family"
, "Extended Household", "Unclassified", "Mixed Homesharers"
, "Female Homesharers", "Abbreviated Female Families", "Male Homesharers"
, "Abbreviated Male Families", "Multi-occupancy Dwelling"
, "" )) then
REP_h_household_composition= "";
*;
_UFORMAT200 = strip(put(h_mosaic_group,$50.));
if ^(_UFORMAT200 in(
"Suburban Mindsets", "Professional Rewards", "Small Town Diversity"
, "Ex-Council Community", "Industrial Heritage", "Careers and Kids"
, "Unclassified", "Terraced Melting Pot", "New Homemakers", "Liberal Opinions"
, "Claimant Cultures", "Alpha Territory", "Active Retirement", "Rural Solitude"
, "Upper Floor Living", "Elderly Needs"
, "" )) then
REP_h_mosaic_group= "";
*;
_UFORMAT200 = strip(put(h_number_of_adults_b,$7.));
if ^(_UFORMAT200 in(
"2", "1", "3", "4", "Unknown", "5", "6+"
, "" )) then
REP_h_number_of_adults_b= "";
*;
_UFORMAT200 = strip(put(h_number_of_bedrooms_b,$7.));
if ^(_UFORMAT200 in(
"3", "4-5", "2", "Unknown", "1"
, "" )) then
REP_h_number_of_bedrooms_b= "";
*;
_UFORMAT200 = strip(put(h_number_of_children_in_house_b,$7.));
if ^(_UFORMAT200 in(
"0", "1", "2", "Unknown", "3", "4"
, "" )) then
REP_h_number_of_children_in_hous= "";
*;
_UFORMAT200 = strip(put(h_presence_of_child_aged_0_4,$10.));
if ^(_UFORMAT200 in(
"No", "Yes", "Unknown"
, "" )) then
REP_h_presence_of_child_aged_0_4= "";
*;
_UFORMAT200 = strip(put(h_presence_of_child_aged_12_17,$10.));
if ^(_UFORMAT200 in(
"No", "Yes", "Unknown"
, "" )) then
REP_h_presence_of_child_aged_12_= "";
*;
_UFORMAT200 = strip(put(h_presence_of_child_aged_5_11,$10.));
if ^(_UFORMAT200 in(
"No", "Yes", "Unknown"
, "" )) then
REP_h_presence_of_child_aged_5_1= "";
*;
_UFORMAT200 = strip(put(h_presence_of_young_person_at_ad,$10.));
if ^(_UFORMAT200 in(
"No", "Yes", "Unknown"
, "" )) then
REP_h_presence_of_young_person_a= "";
*;
_UFORMAT200 = strip(put(h_property_type,$50.));
if ^(_UFORMAT200 in(
"Other Type", "Purpose Built Flats", "Unknown", "Named Building"
, "Converted Flats", "Farm"
, "" )) then
REP_h_property_type= "";
*;
_UFORMAT200 = strip(put(h_residence_type,$50.));
if ^(_UFORMAT200 in(
"Semi-detached", "Terraced", "Detached", "Flat", "Bungalow", "Unknown"
, "" )) then
REP_h_residence_type= "";
*;
_UFORMAT200 = strip(put(last_TA_b,$12.));
if ^(_UFORMAT200 in(
"J.Other", "D.<1 Year", "E.1-2 Years", "F.3-4 Years", "C.<6 Months"
, "B.<3 Months", "G.5-6 Years", "H.7-10 Years", "A.<1 Month"
, "" )) then
REP_last_TA_b= "";
*;
_UFORMAT200 = strip(put(last_TA_nonsave_b,$12.));
if ^(_UFORMAT200 in(
"J.Other", "E.1-2 Years", "D.<1 Year", "F.3-4 Years", "H.7-10 Years"
, "G.5-6 Years", "C.<6 Months", "B.<3 Months", "I.11+ Years", "A.<1 Month"
, "" )) then
REP_last_TA_nonsave_b= "";
*;
_UFORMAT200 = strip(put(last_TA_reason_flag,$15.));
if ^(_UFORMAT200 in(
"No reason given", "Reason given"
, "" )) then
REP_last_TA_reason_flag= "";
*;
_UFORMAT200 = strip(put(last_TA_save_b,$12.));
if ^(_UFORMAT200 in(
"J.Other", "E.1-2 Years", "D.<1 Year", "F.3-4 Years", "C.<6 Months"
, "B.<3 Months", "G.5-6 Years", "H.7-10 Years", "A.<1 Month"
, "" )) then
REP_last_TA_save_b= "";
*;
_UFORMAT200 = strip(put(p_true_touch_group,$50.));
if ^(_UFORMAT200 in(
"Cyber Tourist", "Traditional Approach", "Experienced Netizen"
, "Modern Media Margins", "New Tech Novices", "Unknown", "Digital Culture"
, "" )) then
REP_p_true_touch_group= "";
*;
_UFORMAT200 = strip(put(p_true_touch_type,$2.));
if ^(_UFORMAT200 in(
"6", "2", "22", "7", "", "18", "5", "3", "17", "15", "8", "16", "19", "14"
, "21", "20", "1", "12", "9", "11", "4", "13", "10", "99"
)) then
REP_p_true_touch_type= "";
*;
_UFORMAT200 = strip(put(skyfibre_enabled,$1.));
if ^(_UFORMAT200 in(
"Y", ""
)) then
REP_skyfibre_enabled= "";

* ;
* Replace Specific Class Levels ;
* ;
length _UFormat200 $200;
drop   _UFORMAT200;
_UFORMAT200 = " ";
* ;
* Variable: BB_Product_Holding;
* ;
_UFORMAT200 = strip(
put(BB_Product_Holding,$80.));
if _UFORMAT200 =  "" then
REP_BB_Product_Holding="NO BB";
else
if _UFORMAT200 =  "Unlimited (Legacy)" then
REP_BB_Product_Holding="Unlimited";
else
if _UFORMAT200 =  "Unlimited Fibre" then
REP_BB_Product_Holding="Fibre";
else
if _UFORMAT200 =  "Unlimited" then
REP_BB_Product_Holding="Unlimited";
else
if _UFORMAT200 =  "Sky Broadband Lite" then
REP_BB_Product_Holding="Other";
else
if _UFORMAT200 =  "Fibre Max" then
REP_BB_Product_Holding="Fibre";
else
if _UFORMAT200 =  "Sky Fibre" then
REP_BB_Product_Holding="Fibre";
else
if _UFORMAT200 =  "Connect" then
REP_BB_Product_Holding="Other";
else
if _UFORMAT200 =  "Fibre Lite" then
REP_BB_Product_Holding="Fibre";
else
if _UFORMAT200 =  "Fibre Unlimited Pro" then
REP_BB_Product_Holding="Fibre";
else
if _UFORMAT200 =  "12GB" then
REP_BB_Product_Holding="Other";
else
if _UFORMAT200 =  "Unlimited Pro" then
REP_BB_Product_Holding="Unlimited";
else
if _UFORMAT200 =  "Everyday" then
REP_BB_Product_Holding="Other";
* ;
* Variable: BB_Provider;
* ;
_UFORMAT200 = strip(
put(BB_Provider,$20.));
if _UFORMAT200 =  "plusne" then
REP_BB_Provider="Other";
else
if _UFORMAT200 =  "vodafo" then
REP_BB_Provider="Other";
else
if _UFORMAT200 =  "telefo" then
REP_BB_Provider="Other";
else
if _UFORMAT200 =  "easyne" then
REP_BB_Provider="Other";
else
if _UFORMAT200 =  "h3g" then
REP_BB_Provider="Other";
* ;
* Variable: Movies_Tenure;
* ;
_UFORMAT200 = strip(
put(Movies_Tenure,$20.));
if _UFORMAT200 =  "" then
REP_Movies_Tenure="NO movies ever";
* ;
* Variable: h_household_composition;
* ;
_UFORMAT200 = strip(
put(h_household_composition,$50.));
if _UFORMAT200 =  "Families" then
REP_h_household_composition="Families";
else
if _UFORMAT200 =  "Single Female" then
REP_h_household_composition="Singles & Sharers";
else
if _UFORMAT200 =  "Extended Family" then
REP_h_household_composition="Families";
else
if _UFORMAT200 =  "Single Male" then
REP_h_household_composition="Singles & Sharers";
else
if _UFORMAT200 =  "Pseudo Family" then
REP_h_household_composition="Families";
else
if _UFORMAT200 =  "Extended Household" then
REP_h_household_composition="Families";
else
if _UFORMAT200 =  "Unclassified" then
REP_h_household_composition="Other";
else
if _UFORMAT200 =  "Mixed Homesharers" then
REP_h_household_composition="Singles & Sharers";
else
if _UFORMAT200 =  "Female Homesharers" then
REP_h_household_composition="Singles & Sharers";
else
if _UFORMAT200 =  "Abbreviated Female Families" then
REP_h_household_composition="Families";
else
if _UFORMAT200 =  "Male Homesharers" then
REP_h_household_composition="Singles & Sharers";
else
if _UFORMAT200 =  "Abbreviated Male Families" then
REP_h_household_composition="Families";
else
if _UFORMAT200 =  "Multi-occupancy Dwelling" then
REP_h_household_composition="Families";
*------------------------------------------------------------*;
* TOOL: SASHELP.EMCORE.EMCODETOOL.CLASS;
* TYPE: MODEL;
* NODE: Dec;
*------------------------------------------------------------*;
*------------------------------------------------------------*;
*Posterior Probabilities: REP_Up_Box_Sets;
*------------------------------------------------------------*;
Label P_REP_Up_Box_Sets1='Predicted: REP_Up_Box_Sets=1';
P_REP_Up_Box_Sets1 = 0.3932823701;
Label P_REP_Up_Box_Sets0='Predicted: REP_Up_Box_Sets=0';
P_REP_Up_Box_Sets0 = 0.6067176299;
*------------------------------------------------------------*;
*Posterior Probabilities: ;
*------------------------------------------------------------*;
*------------------------------------------------------------*;
*Computing Classification Vars: REP_Up_Box_Sets;
*------------------------------------------------------------*;
length I_REP_Up_Box_Sets $12;
label  I_REP_Up_Box_Sets = 'Into: REP_Up_Box_Sets';
length _format200 $200;
drop _format200;
_format200= ' ' ;
length _p_ 8;
_p_= 0 ;
drop _p_ ;
if P_REP_Up_Box_Sets1 - _p_ > 1e-8 then do ;
   _p_= P_REP_Up_Box_Sets1 ;
   _format200='1';
end;
if P_REP_Up_Box_Sets0 - _p_ > 1e-8 then do ;
   _p_= P_REP_Up_Box_Sets0 ;
   _format200='0';
end;
I_REP_Up_Box_Sets=dmnorm(_format200,32); ;
length U_REP_Up_Box_Sets 8;
label U_REP_Up_Box_Sets = 'Unnormalized Into: REP_Up_Box_Sets';
if I_REP_Up_Box_Sets='1' then
U_REP_Up_Box_Sets=1;
if I_REP_Up_Box_Sets='0' then
U_REP_Up_Box_Sets=0;
*------------------------------------------------------------*;
* Decision Score Code: REP_Up_Box_Sets;
*------------------------------------------------------------*;
*** Warning Variable;
length _warn_ $4;
label _warn_ = 'Warnings';
drop _decwarn; _decwarn = 0;

*** Check Posterior Probabilities;
if not (n( P_REP_Up_Box_Sets1 ) & (0 <= P_REP_Up_Box_Sets1 )
      & n( P_REP_Up_Box_Sets0 ) & (0 <= P_REP_Up_Box_Sets0 )
   ) then do;
   _decwarn = 1;
   substr(_warn_,3,1) = 'P';
   P_REP_Up_Box_Sets1 = .;
   P_REP_Up_Box_Sets0 = .;
   goto DECdemi;
end;
else if not (P_REP_Up_Box_Sets1 <= 1
      & P_REP_Up_Box_Sets0 <= 1
   ) then do;
   substr(_warn_,3,1) = 'P';
end;

*** Update Posterior Probabilities;
P_REP_Up_Box_Sets1 = P_REP_Up_Box_Sets1 * 0.005 / 0.39328167954171;
P_REP_Up_Box_Sets0 = P_REP_Up_Box_Sets0 * 0.995 / 0.60671832045828;
drop _sum; _sum = P_REP_Up_Box_Sets1 + P_REP_Up_Box_Sets0 ;
if _sum > 4.135903E-25 then do;
   P_REP_Up_Box_Sets1 = P_REP_Up_Box_Sets1 / _sum;
   P_REP_Up_Box_Sets0 = P_REP_Up_Box_Sets0 / _sum;
end;

*** Find Category with Maximum Posterior;
label I_REP_Up_Box_Sets = 'Into: REP_Up_Box_Sets' ;
length I_REP_Up_Box_Sets $ 12;
I_REP_Up_Box_Sets = '1' ;
drop _sum; _sum = P_REP_Up_Box_Sets1 + 4.547474E-13;
if _sum < P_REP_Up_Box_Sets0 then do;
   _sum = P_REP_Up_Box_Sets0 + 4.547474E-13;
   I_REP_Up_Box_Sets = '0' ;
end;

DECdemi:;


*** End Decision Processing ;
*------------------------------------------------------------*;
*Computing Classification Vars: REP_Up_Box_Sets;
*------------------------------------------------------------*;
length I_REP_Up_Box_Sets $12;
label  I_REP_Up_Box_Sets = 'Into: REP_Up_Box_Sets';
length _format200 $200;
drop _format200;
_format200= ' ' ;
length _p_ 8;
_p_= 0 ;
drop _p_ ;
if P_REP_Up_Box_Sets1 - _p_ > 1e-8 then do ;
   _p_= P_REP_Up_Box_Sets1 ;
   _format200='1';
end;
if P_REP_Up_Box_Sets0 - _p_ > 1e-8 then do ;
   _p_= P_REP_Up_Box_Sets0 ;
   _format200='0';
end;
I_REP_Up_Box_Sets=dmnorm(_format200,32); ;
length U_REP_Up_Box_Sets 8;
label U_REP_Up_Box_Sets = 'Unnormalized Into: REP_Up_Box_Sets';
if I_REP_Up_Box_Sets='1' then
U_REP_Up_Box_Sets=1;
if I_REP_Up_Box_Sets='0' then
U_REP_Up_Box_Sets=0;
*------------------------------------------------------------*;
* TOOL: Extension Class;
* TYPE: MODEL;
* NODE: HPReg12;
*------------------------------------------------------------*;
*****************************************;
** SAS Scoring Code for PROC Hplogistic;
*****************************************;

length _WARN_ $4;
label _WARN_ = 'Warning' ;
_WARN_ = '';
drop _LMR_IMPUTE;
_LMR_IMPUTE = 0;
length I_REP_Up_Box_Sets $ 12;
label I_REP_Up_Box_Sets = 'Into: REP_Up_Box_Sets' ;
label U_REP_Up_Box_Sets = 'Unnormalized Into: REP_Up_Box_Sets' ;

label P_REP_Up_Box_Sets1 = 'Predicted: REP_Up_Box_Sets=1' ;
label P_REP_Up_Box_Sets0 = 'Predicted: REP_Up_Box_Sets=0' ;

drop _LMR_BAD;
_LMR_BAD=0;

*** Generate design variables for REP_Offers_Applied_Lst_36M_DTV_b;
drop _63_0 _63_1 ;
_63_0= 0;
_63_1= 0;
length _st3 $ 3; drop _st3;
_st3 = put(REP_Offers_Applied_Lst_36M_DTV_b, $3.0);
call dmnorm(_st3, 3);
if _st3 = '0'  then do;
   _63_0 = 1;
end;
else if _st3 = '1'  then do;
   _63_1 = 1;
end;
else do;
   _63_0 = .;
   _63_1 = .;
   substr(_warn_,2,1) = 'U';
   _LMR_IMPUTE = 1;
end;

*** Generate design variables for REP_BB_Provider;
drop _70_0 _70_1 _70_2 _70_3 _70_4 _70_5 _70_6 _70_7 ;
_70_0= 0;
_70_1= 0;
_70_2= 0;
_70_3= 0;
_70_4= 0;
_70_5= 0;
_70_6= 0;
_70_7= 0;
length _st20 $ 20; drop _st20;
_st20 = put(REP_BB_Provider, $20.0);
call dmnorm(_st20, 20);
_dm_find = 0; drop _dm_find;
if _st20 <= 'NONE'  then do;
   if _st20 <= 'BT'  then do;
      if _st20 = 'BSKYB'  then do;
         _70_0 = 1;
         _dm_find = 1;
      end;
      else do;
         if _st20 = 'BT'  then do;
            _70_1 = 1;
            _dm_find = 1;
         end;
      end;
   end;
   else do;
      if _st20 = 'JANET'  then do;
         _70_2 = 1;
         _dm_find = 1;
      end;
      else do;
         if _st20 = 'NONE'  then do;
            _70_3 = 1;
            _dm_find = 1;
         end;
      end;
   end;
end;
else do;
   if _st20 <= 'TALKTA'  then do;
      if _st20 = 'OTHER'  then do;
         _70_4 = 1;
         _dm_find = 1;
      end;
      else do;
         if _st20 = 'TALKTA'  then do;
            _70_5 = 1;
            _dm_find = 1;
         end;
      end;
   end;
   else do;
      if _st20 = 'UNKNOWN'  then do;
         _70_6 = 1;
         _dm_find = 1;
      end;
      else do;
         if _st20 = 'VIRGIN'  then do;
            _70_7 = 1;
            _dm_find = 1;
         end;
      end;
   end;
end;
if not _dm_find then do;
   _70_0 = .;
   _70_1 = .;
   _70_2 = .;
   _70_3 = .;
   _70_4 = .;
   _70_5 = .;
   _70_6 = .;
   _70_7 = .;
   substr(_warn_,2,1) = 'U';
   _LMR_IMPUTE = 1;
end;

*** Generate design variables for REP_Curr_Offer_Amount_DTV_b;
drop _73_0 _73_1 _73_2 ;
_73_0= 0;
_73_1= 0;
_73_2= 0;
length _st17 $ 17; drop _st17;
_st17 = put(REP_Curr_Offer_Amount_DTV_b, $17.0);
call dmnorm(_st17, 17);
if _st17 = 'A.NO OFFER'  then do;
   _73_0 = 1;
end;
else if _st17 = 'B.LESS THAN 20'  then do;
   _73_1 = 1;
end;
else if _st17 = 'C.GREATER THAN 20'  then do;
   _73_2 = 1;
end;
else do;
   _73_0 = .;
   _73_1 = .;
   _73_2 = .;
   substr(_warn_,2,1) = 'U';
   _LMR_IMPUTE = 1;
end;

*** Generate design variables for REP_DTV_product_holding_recode;
drop _75_0 _75_1 _75_2 _75_3 _75_4 _75_5 _75_6 _75_7 ;
_75_0= 0;
_75_1= 0;
_75_2= 0;
_75_3= 0;
_75_4= 0;
_75_5= 0;
_75_6= 0;
_75_7= 0;
length _st32 $ 32; drop _st32;
_st32 = put(REP_DTV_product_holding_recode, $40.0);
call dmnorm(_st32, 32);
_dm_find = 0; drop _dm_find;
if _st32 <= 'ORIGINAL WITH SPORTS & CINEMA'  then do;
   if _st32 <= 'ORIGINAL WITH CINEMA'  then do;
      if _st32 = 'ORIGINAL'  then do;
         _75_0 = 1;
         _dm_find = 1;
      end;
      else do;
         if _st32 = 'ORIGINAL WITH CINEMA'  then do;
            _75_1 = 1;
            _dm_find = 1;
         end;
      end;
   end;
   else do;
      if _st32 = 'ORIGINAL WITH SPORTS'  then do;
         _75_2 = 1;
         _dm_find = 1;
      end;
      else do;
         if _st32 = 'ORIGINAL WITH SPORTS & CINEMA'  then do;
            _75_3 = 1;
            _dm_find = 1;
         end;
      end;
   end;
end;
else do;
   if _st32 <= 'VARIETY WITH CINEMA'  then do;
      if _st32 = 'VARIETY'  then do;
         _75_4 = 1;
         _dm_find = 1;
      end;
      else do;
         if _st32 = 'VARIETY WITH CINEMA'  then do;
            _75_5 = 1;
            _dm_find = 1;
         end;
      end;
   end;
   else do;
      if _st32 = 'VARIETY WITH SPORTS'  then do;
         _75_6 = 1;
         _dm_find = 1;
      end;
      else do;
         if _st32 = 'VARIETY WITH SPORTS & CINEMA'  then do;
            _75_7 = 1;
            _dm_find = 1;
         end;
      end;
   end;
end;
if not _dm_find then do;
   _75_0 = .;
   _75_1 = .;
   _75_2 = .;
   _75_3 = .;
   _75_4 = .;
   _75_5 = .;
   _75_6 = .;
   _75_7 = .;
   substr(_warn_,2,1) = 'U';
   _LMR_IMPUTE = 1;
end;

*** Generate design variables for REP_OPT_Curr_Offer_Actual_End_DT;
drop _79_0 _79_1 _79_2 _79_3 ;
_79_0= 0;
_79_1= 0;
_79_2= 0;
_79_3= 0;
length _st32 $ 32; drop _st32;
_st32 = put(REP_OPT_Curr_Offer_Actual_End_DT, $32.);
call dmnorm(_st32, 32);
if _st32 = '01:LOW--549.5'  then do;
   _79_0 = 1;
end;
else if _st32 = '02:-549.5--326.5'  then do;
   _79_1 = 1;
end;
else if _st32 = '03:-326.5--34.5, MISSING'  then do;
   _79_2 = 1;
end;
else if _st32 = '04:-34.5-HIGH'  then do;
   _79_3 = 1;
end;
else do;
   _79_0 = .;
   _79_1 = .;
   _79_2 = .;
   _79_3 = .;
   substr(_warn_,2,1) = 'U';
   _LMR_IMPUTE = 1;
end;

*** Generate design variables for REP_OPT_DTV_Curr_Contract_Intend;
drop _82_0 _82_1 _82_2 _82_3 ;
_82_0= 0;
_82_1= 0;
_82_2= 0;
_82_3= 0;
length _st32 $ 32; drop _st32;
_st32 = put(REP_OPT_DTV_Curr_Contract_Intend, $32.);
call dmnorm(_st32, 32);
if _st32 = '01:LOW--552.5'  then do;
   _82_0 = 1;
end;
else if _st32 = '02:-552.5--331.5'  then do;
   _82_1 = 1;
end;
else if _st32 = '03:-331.5--58.5, MISSING'  then do;
   _82_2 = 1;
end;
else if _st32 = '04:-58.5-HIGH'  then do;
   _82_3 = 1;
end;
else do;
   _82_0 = .;
   _82_1 = .;
   _82_2 = .;
   _82_3 = .;
   substr(_warn_,2,1) = 'U';
   _LMR_IMPUTE = 1;
end;

*** Generate design variables for REP_OPT_DTV_Last_Pending_Cancel;
drop _85_0 _85_1 _85_2 ;
_85_0= 0;
_85_1= 0;
_85_2= 0;
length _st32 $ 32; drop _st32;
_st32 = put(REP_OPT_DTV_Last_Pending_Cancel, $32.);
call dmnorm(_st32, 32);
if _st32 = '01:LOW-263.5'  then do;
   _85_0 = 1;
end;
else if _st32 = '02:263.5-2697.5'  then do;
   _85_1 = 1;
end;
else if _st32 = '03:2697.5-HIGH, MISSING'  then do;
   _85_2 = 1;
end;
else do;
   _85_0 = .;
   _85_1 = .;
   _85_2 = .;
   substr(_warn_,2,1) = 'U';
   _LMR_IMPUTE = 1;
end;

*** Generate design variables for REP_OPT_LAST_movies_downgrade;
drop _87_0 _87_1 _87_2 ;
_87_0= 0;
_87_1= 0;
_87_2= 0;
length _st32 $ 32; drop _st32;
_st32 = put(REP_OPT_LAST_movies_downgrade, $32.);
call dmnorm(_st32, 32);
if _st32 = '01:LOW-1.5'  then do;
   _87_0 = 1;
end;
else if _st32 = '02:1.5-37.5'  then do;
   _87_1 = 1;
end;
else if _st32 = '03:37.5-HIGH, MISSING'  then do;
   _87_2 = 1;
end;
else do;
   _87_0 = .;
   _87_1 = .;
   _87_2 = .;
   substr(_warn_,2,1) = 'U';
   _LMR_IMPUTE = 1;
end;

*** Generate design variables for REP_OPT_OD_Last_3M;
drop _90_0 _90_1 _90_2 _90_3 ;
_90_0= 0;
_90_1= 0;
_90_2= 0;
_90_3= 0;
length _st32 $ 32; drop _st32;
_st32 = put(REP_OPT_OD_Last_3M, $32.);
call dmnorm(_st32, 32);
if _st32 = '01:LOW-37.5'  then do;
   _90_0 = 1;
end;
else if _st32 = '02:37.5-153.5'  then do;
   _90_1 = 1;
end;
else if _st32 = '03:153.5-HIGH'  then do;
   _90_2 = 1;
end;
else if _st32 = '_MISSING_'  then do;
   _90_3 = 1;
end;
else do;
   _90_0 = .;
   _90_1 = .;
   _90_2 = .;
   _90_3 = .;
   substr(_warn_,2,1) = 'U';
   _LMR_IMPUTE = 1;
end;

*** Generate design variables for REP_dtv_last_tenure;
drop _120_0 _120_1 _120_2 _120_3 _120_4 _120_5 _120_6 _120_7 _120_8 ;
_120_0= 0;
_120_1= 0;
_120_2= 0;
_120_3= 0;
_120_4= 0;
_120_5= 0;
_120_6= 0;
_120_7= 0;
_120_8= 0;
length _st12 $ 12; drop _st12;
_st12 = put(REP_dtv_last_tenure, $12.0);
call dmnorm(_st12, 12);
_dm_find = 0; drop _dm_find;
if _st12 <= 'E.3-4 YEARS'  then do;
   if _st12 <= 'C.<1 YEAR'  then do;
      if _st12 <= 'B.<6 MONTHS'  then do;
         if _st12 = 'A.<3 MONTHS'  then do;
            _120_0 = 1;
            _dm_find = 1;
         end;
         else do;
            if _st12 = 'B.<6 MONTHS'  then do;
               _120_1 = 1;
               _dm_find = 1;
            end;
         end;
      end;
      else do;
         if _st12 = 'C.<1 YEAR'  then do;
            _120_2 = 1;
            _dm_find = 1;
         end;
      end;
   end;
   else do;
      if _st12 = 'D.1-2 YEARS'  then do;
         _120_3 = 1;
         _dm_find = 1;
      end;
      else do;
         if _st12 = 'E.3-4 YEARS'  then do;
            _120_4 = 1;
            _dm_find = 1;
         end;
      end;
   end;
end;
else do;
   if _st12 <= 'G.7-10 YEARS'  then do;
      if _st12 = 'F.5-6 YEARS'  then do;
         _120_5 = 1;
         _dm_find = 1;
      end;
      else do;
         if _st12 = 'G.7-10 YEARS'  then do;
            _120_6 = 1;
            _dm_find = 1;
         end;
      end;
   end;
   else do;
      if _st12 = 'H.11+ YEARS'  then do;
         _120_7 = 1;
         _dm_find = 1;
      end;
      else do;
         if _st12 = 'I.OTHER'  then do;
            _120_8 = 1;
            _dm_find = 1;
         end;
      end;
   end;
end;
if not _dm_find then do;
   _120_0 = .;
   _120_1 = .;
   _120_2 = .;
   _120_3 = .;
   _120_4 = .;
   _120_5 = .;
   _120_6 = .;
   _120_7 = .;
   _120_8 = .;
   substr(_warn_,2,1) = 'U';
   _LMR_IMPUTE = 1;
end;

*** Generate design variables for REP_last_TA_b;
drop _134_0 _134_1 _134_2 _134_3 _134_4 _134_5 _134_6 _134_7 _134_8 ;
_134_0= 0;
_134_1= 0;
_134_2= 0;
_134_3= 0;
_134_4= 0;
_134_5= 0;
_134_6= 0;
_134_7= 0;
_134_8= 0;
length _st12 $ 12; drop _st12;
_st12 = put(REP_last_TA_b, $12.0);
call dmnorm(_st12, 12);
_dm_find = 0; drop _dm_find;
if _st12 <= 'E.1-2 YEARS'  then do;
   if _st12 <= 'C.<6 MONTHS'  then do;
      if _st12 <= 'B.<3 MONTHS'  then do;
         if _st12 = 'A.<1 MONTH'  then do;
            _134_0 = 1;
            _dm_find = 1;
         end;
         else do;
            if _st12 = 'B.<3 MONTHS'  then do;
               _134_1 = 1;
               _dm_find = 1;
            end;
         end;
      end;
      else do;
         if _st12 = 'C.<6 MONTHS'  then do;
            _134_2 = 1;
            _dm_find = 1;
         end;
      end;
   end;
   else do;
      if _st12 = 'D.<1 YEAR'  then do;
         _134_3 = 1;
         _dm_find = 1;
      end;
      else do;
         if _st12 = 'E.1-2 YEARS'  then do;
            _134_4 = 1;
            _dm_find = 1;
         end;
      end;
   end;
end;
else do;
   if _st12 <= 'G.5-6 YEARS'  then do;
      if _st12 = 'F.3-4 YEARS'  then do;
         _134_5 = 1;
         _dm_find = 1;
      end;
      else do;
         if _st12 = 'G.5-6 YEARS'  then do;
            _134_6 = 1;
            _dm_find = 1;
         end;
      end;
   end;
   else do;
      if _st12 = 'H.7-10 YEARS'  then do;
         _134_7 = 1;
         _dm_find = 1;
      end;
      else do;
         if _st12 = 'J.OTHER'  then do;
            _134_8 = 1;
            _dm_find = 1;
         end;
      end;
   end;
end;
if not _dm_find then do;
   _134_0 = .;
   _134_1 = .;
   _134_2 = .;
   _134_3 = .;
   _134_4 = .;
   _134_5 = .;
   _134_6 = .;
   _134_7 = .;
   _134_8 = .;
   substr(_warn_,2,1) = 'U';
   _LMR_IMPUTE = 1;
end;

*** If missing or invalid inputs, use averages;
if _LMR_IMPUTE > 0 then do;
   _P0 = 0.39545678569828;
   _P1 = 0.60454321430171;
   goto HPREG12SKIP_000;
end;

*** Compute Linear Predictors;
drop _LP0;
_LP0 = 0;

*** Effect: REP_Offers_Applied_Lst_36M_DTV_b;
_LP0 = _LP0 + (-0.30725082931669) * _63_0;
*** Effect: REP_BB_Provider;
_LP0 = _LP0 + (0.24685953997427) * _70_0;
_LP0 = _LP0 + (-0.07521014103985) * _70_1;
_LP0 = _LP0 + (-6.42663567576873) * _70_2;
_LP0 = _LP0 + (-0.04963382685107) * _70_3;
_LP0 = _LP0 + (-0.15252139253191) * _70_4;
_LP0 = _LP0 + (-0.14885315002681) * _70_5;
_LP0 = _LP0 + (0.81706237421557) * _70_6;
*** Effect: REP_Curr_Offer_Amount_DTV_b;
_LP0 = _LP0 + (0.18987617603448) * _73_0;
_LP0 = _LP0 + (-0.59613897729618) * _73_1;
*** Effect: REP_DTV_product_holding_recode;
_LP0 = _LP0 + (-1.32751109352867) * _75_0;
_LP0 = _LP0 + (-0.58469864775641) * _75_1;
_LP0 = _LP0 + (-1.03727224322076) * _75_2;
_LP0 = _LP0 + (-0.57421163482099) * _75_3;
_LP0 = _LP0 + (-0.45516813553604) * _75_4;
_LP0 = _LP0 + (0.04600355037998) * _75_5;
_LP0 = _LP0 + (-0.2636298815082) * _75_6;
*** Effect: REP_OPT_Curr_Offer_Actual_End_DT;
_LP0 = _LP0 + (-2.00581421285431) * _79_0;
_LP0 = _LP0 + (-1.64856036565945) * _79_1;
_LP0 = _LP0 + (-1.8057087916959) * _79_2;
*** Effect: REP_OPT_DTV_Curr_Contract_Intend;
_LP0 = _LP0 + (4.058675094075) * _82_0;
_LP0 = _LP0 + (0.23486957056676) * _82_1;
_LP0 = _LP0 + (-0.19580815037334) * _82_2;
*** Effect: REP_OPT_DTV_Last_Pending_Cancel;
_LP0 = _LP0 + (0.59387574990832) * _85_0;
_LP0 = _LP0 + (0.22107164325682) * _85_1;
*** Effect: REP_OPT_LAST_movies_downgrade;
_LP0 = _LP0 + (0.8196411657616) * _87_0;
_LP0 = _LP0 + (0.22448021811972) * _87_1;
*** Effect: REP_OPT_OD_Last_3M;
_LP0 = _LP0 + (1.66747997658086) * _90_0;
_LP0 = _LP0 + (1.90851565512711) * _90_1;
_LP0 = _LP0 + (2.09292486557066) * _90_2;
*** Effect: REP_dtv_last_tenure;
_LP0 = _LP0 + (1.22804627804272) * _120_0;
_LP0 = _LP0 + (0.69428083290115) * _120_1;
_LP0 = _LP0 + (0.32681855388425) * _120_2;
_LP0 = _LP0 + (0.0989011596008) * _120_3;
_LP0 = _LP0 + (-0.11883671409798) * _120_4;
_LP0 = _LP0 + (-0.09611633294901) * _120_5;
_LP0 = _LP0 + (-0.01234295551804) * _120_6;
_LP0 = _LP0 + (-0.06183946318514) * _120_7;
*** Effect: REP_last_TA_b;
_LP0 = _LP0 + (0.59958859942125) * _134_0;
_LP0 = _LP0 + (0.45829468814024) * _134_1;
_LP0 = _LP0 + (0.29793577389919) * _134_2;
_LP0 = _LP0 + (0.19847374633319) * _134_3;
_LP0 = _LP0 + (0.32011111656456) * _134_4;
_LP0 = _LP0 + (0.21710812361775) * _134_5;
_LP0 = _LP0 + (0.22074175038315) * _134_6;
_LP0 = _LP0 + (0.17924251298161) * _134_7;

*** Predicted values;
drop _MAXP _IY _P0 _P1;
_TEMP = 0.00113275144562  + _LP0;
if (_TEMP < 0) then do;
   _TEMP = exp(_TEMP);
   _P0 = _TEMP / (1 + _TEMP);
end;
else _P0 = 1 / (1 + exp(-_TEMP));
_P1 = 1.0 - _P0;
HPREG12SKIP_000:
P_REP_Up_Box_Sets1 = _P0;
_MAXP = _P0;
_IY = 1;
P_REP_Up_Box_Sets0 = _P1;
if (_P1 >  _MAXP + 1E-8) then do;
   _MAXP = _P1;
   _IY = 2;
end;
select( _IY );
   when (1) do;
      I_REP_Up_Box_Sets = '1' ;
      U_REP_Up_Box_Sets = 1;
   end;
   when (2) do;
      I_REP_Up_Box_Sets = '0' ;
      U_REP_Up_Box_Sets = 0;
   end;
   otherwise do;
      I_REP_Up_Box_Sets = '';
      U_REP_Up_Box_Sets = .;
   end;
end;
drop _TEMP;
*------------------------------------------------------------*;
* Decision Score Code: REP_Up_Box_Sets;
*------------------------------------------------------------*;
*** Warning Variable;
length _warn_ $4;
label _warn_ = 'Warnings';
drop _decwarn; _decwarn = 0;

*** Check Posterior Probabilities;
if not (n( P_REP_Up_Box_Sets1 ) & (0 <= P_REP_Up_Box_Sets1 )
      & n( P_REP_Up_Box_Sets0 ) & (0 <= P_REP_Up_Box_Sets0 )
   ) then do;
   _decwarn = 1;
   substr(_warn_,3,1) = 'P';
   P_REP_Up_Box_Sets1 = .;
   P_REP_Up_Box_Sets0 = .;
   goto HPREG12demi;
end;
else if not (P_REP_Up_Box_Sets1 <= 1
      & P_REP_Up_Box_Sets0 <= 1
   ) then do;
   substr(_warn_,3,1) = 'P';
end;

*** Update Posterior Probabilities;
P_REP_Up_Box_Sets1 = P_REP_Up_Box_Sets1 * 0.005 / 0.39328167954171;
P_REP_Up_Box_Sets0 = P_REP_Up_Box_Sets0 * 0.995 / 0.60671832045828;
drop _sum; _sum = P_REP_Up_Box_Sets1 + P_REP_Up_Box_Sets0 ;
if _sum > 4.135903E-25 then do;
   P_REP_Up_Box_Sets1 = P_REP_Up_Box_Sets1 / _sum;
   P_REP_Up_Box_Sets0 = P_REP_Up_Box_Sets0 / _sum;
end;

*** Find Category with Maximum Posterior;
label I_REP_Up_Box_Sets = 'Into: REP_Up_Box_Sets' ;
length I_REP_Up_Box_Sets $ 12;
I_REP_Up_Box_Sets = '1' ;
drop _sum; _sum = P_REP_Up_Box_Sets1 + 4.547474E-13;
if _sum < P_REP_Up_Box_Sets0 then do;
   _sum = P_REP_Up_Box_Sets0 + 4.547474E-13;
   I_REP_Up_Box_Sets = '0' ;
end;

HPREG12demi:;


*** End Decision Processing ;
*------------------------------------------------------------*;
*Computing Classification Vars: REP_Up_Box_Sets;
*------------------------------------------------------------*;
length I_REP_Up_Box_Sets $12;
label  I_REP_Up_Box_Sets = 'Into: REP_Up_Box_Sets';
length _format200 $200;
drop _format200;
_format200= ' ' ;
length _p_ 8;
_p_= 0 ;
drop _p_ ;
if P_REP_Up_Box_Sets1 - _p_ > 1e-8 then do ;
   _p_= P_REP_Up_Box_Sets1 ;
   _format200='1';
end;
if P_REP_Up_Box_Sets0 - _p_ > 1e-8 then do ;
   _p_= P_REP_Up_Box_Sets0 ;
   _format200='0';
end;
I_REP_Up_Box_Sets=dmnorm(_format200,32); ;
length U_REP_Up_Box_Sets 8;
label U_REP_Up_Box_Sets = 'Unnormalized Into: REP_Up_Box_Sets';
if I_REP_Up_Box_Sets='1' then
U_REP_Up_Box_Sets=1;
if I_REP_Up_Box_Sets='0' then
U_REP_Up_Box_Sets=0;
*------------------------------------------------------------*;
* TOOL: Model Compare Class;
* TYPE: ASSESS;
* NODE: MdlComp4;
*------------------------------------------------------------*;
if (P_REP_Up_Box_Sets1 ge 0.0135075840798) then do;
b_REP_Up_Box_Sets = 1;
end;
else
if (P_REP_Up_Box_Sets1 ge 0.00996440374504) then do;
b_REP_Up_Box_Sets = 2;
end;
else
if (P_REP_Up_Box_Sets1 ge 0.00815789116998) then do;
b_REP_Up_Box_Sets = 3;
end;
else
if (P_REP_Up_Box_Sets1 ge 0.00710868757562) then do;
b_REP_Up_Box_Sets = 4;
end;
else
if (P_REP_Up_Box_Sets1 ge 0.0061837382436) then do;
b_REP_Up_Box_Sets = 5;
end;
else
if (P_REP_Up_Box_Sets1 ge 0.00557317262885) then do;
b_REP_Up_Box_Sets = 6;
end;
else
if (P_REP_Up_Box_Sets1 ge 0.0048981916687) then do;
b_REP_Up_Box_Sets = 7;
end;
else
if (P_REP_Up_Box_Sets1 ge 0.0043671678181) then do;
b_REP_Up_Box_Sets = 8;
end;
else
if (P_REP_Up_Box_Sets1 ge 0.00393095030291) then do;
b_REP_Up_Box_Sets = 9;
end;
else
if (P_REP_Up_Box_Sets1 ge 0.00343963093361) then do;
b_REP_Up_Box_Sets = 10;
end;
else
if (P_REP_Up_Box_Sets1 ge 0.00298945889866) then do;
b_REP_Up_Box_Sets = 11;
end;
else
if (P_REP_Up_Box_Sets1 ge 0.00257535156865) then do;
b_REP_Up_Box_Sets = 12;
end;
else
if (P_REP_Up_Box_Sets1 ge 0.00216885764603) then do;
b_REP_Up_Box_Sets = 13;
end;
else
if (P_REP_Up_Box_Sets1 ge 0.00198073968229) then do;
b_REP_Up_Box_Sets = 14;
end;
else
if (P_REP_Up_Box_Sets1 ge 0.00161937771217) then do;
b_REP_Up_Box_Sets = 15;
end;
else
if (P_REP_Up_Box_Sets1 ge 0.00147833811315) then do;
b_REP_Up_Box_Sets = 16;
end;
else
if (P_REP_Up_Box_Sets1 ge 0.00125737567767) then do;
b_REP_Up_Box_Sets = 17;
end;
else
if (P_REP_Up_Box_Sets1 ge 0.00099593471276) then do;
b_REP_Up_Box_Sets = 18;
end;
else
if (P_REP_Up_Box_Sets1 ge 0.0006582067133) then do;
b_REP_Up_Box_Sets = 19;
end;
else
do;
b_REP_Up_Box_Sets = 20;
end;
*------------------------------------------------------------*;
* TOOL: Score Node;
* TYPE: ASSESS;
* NODE: Score;
*------------------------------------------------------------*;
*------------------------------------------------------------*;
* Score: Creating Fixed Names;
*------------------------------------------------------------*;
LABEL EM_SEGMENT = 'Segment';
EM_SEGMENT = b_REP_Up_Box_Sets;
LABEL EM_EVENTPROBABILITY = 'Probability for level 1 of REP_Up_Box_Sets';
EM_EVENTPROBABILITY = P_REP_Up_Box_Sets1;
LABEL EM_PROBABILITY = 'Probability of Classification';
EM_PROBABILITY =
max(
P_REP_Up_Box_Sets1
,
P_REP_Up_Box_Sets0
);
LENGTH EM_CLASSIFICATION $%dmnorlen;
LABEL EM_CLASSIFICATION = "Prediction for REP_Up_Box_Sets";
EM_CLASSIFICATION = I_REP_Up_Box_Sets;
