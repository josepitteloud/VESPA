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
label OPT_DTV_1st_Activation = 'Transformed: DTV_1st_Activation';
length OPT_DTV_1st_Activation $36;
if (DTV_1st_Activation eq .) then OPT_DTV_1st_Activation="02:41.5-high, MISSING";
else
if (DTV_1st_Activation < 41.5) then
OPT_DTV_1st_Activation = "01:low-41.5";
else
if (DTV_1st_Activation >= 41.5) then
OPT_DTV_1st_Activation = "02:41.5-high, MISSING";
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
label OPT_DTV_Last_Activation = 'Transformed: DTV_Last_Activation';
length OPT_DTV_Last_Activation $36;
if (DTV_Last_Activation eq .) then OPT_DTV_Last_Activation="02:48.5-high, MISSING";
else
if (DTV_Last_Activation < 48.5) then
OPT_DTV_Last_Activation = "01:low-48.5";
else
if (DTV_Last_Activation >= 48.5) then
OPT_DTV_Last_Activation = "02:48.5-high, MISSING";
label OPT_DTV_Last_Active_Block = 'Transformed: DTV_Last_Active_Block';
length OPT_DTV_Last_Active_Block $36;
if (DTV_Last_Active_Block eq .) then OPT_DTV_Last_Active_Block="02:596-high, MISSING";
else
if (DTV_Last_Active_Block < 596) then
OPT_DTV_Last_Active_Block = "01:low-596";
else
if (DTV_Last_Active_Block >= 596) then
OPT_DTV_Last_Active_Block = "02:596-high, MISSING";
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
label OPT_DTV_Last_cuscan_churn = 'Transformed: DTV_Last_cuscan_churn';
length OPT_DTV_Last_cuscan_churn $36;
if (DTV_Last_cuscan_churn eq .) then OPT_DTV_Last_cuscan_churn="02:230.5-high, MISSING";
else
if (DTV_Last_cuscan_churn < 230.5) then
OPT_DTV_Last_cuscan_churn = "01:low-230.5";
else
if (DTV_Last_cuscan_churn >= 230.5) then
OPT_DTV_Last_cuscan_churn = "02:230.5-high, MISSING";
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
label OPT_LAST_sports_downgrade = 'Transformed: LAST_sports_downgrade';
length OPT_LAST_sports_downgrade $36;
if (LAST_sports_downgrade eq .) then OPT_LAST_sports_downgrade="02:14.5-high, MISSING";
else
if (LAST_sports_downgrade < 14.5) then
OPT_LAST_sports_downgrade = "01:low-14.5";
else
if (LAST_sports_downgrade >= 14.5) then
OPT_LAST_sports_downgrade = "02:14.5-high, MISSING";
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
label OPT_OD_Months_since_Last = 'Transformed: OD_Months_since_Last';
length OPT_OD_Months_since_Last $36;
if (OD_Months_since_Last eq .) then OPT_OD_Months_since_Last="_MISSING_";
else
if (OD_Months_since_Last < 0.5) then
OPT_OD_Months_since_Last = "01:low-0.5";
else
if (OD_Months_since_Last >= 0.5) then
OPT_OD_Months_since_Last = "02:0.5-high";
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
label OPT_h_income_value = 'Transformed: h_income_value';
length OPT_h_income_value $36;
if (h_income_value eq .) then OPT_h_income_value="01:low-29430, MISSING";
else
if (h_income_value < 29430) then
OPT_h_income_value = "01:low-29430, MISSING";
else
if (h_income_value >= 29430) then
OPT_h_income_value = "02:29430-high";
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
* TOOL: Extension Class;
* TYPE: MODIFY;
* NODE: Repl2;
*------------------------------------------------------------*;
length _UFormat200 $200;
drop   _UFORMAT200;
_UFORMAT200 = " ";

* ;
* Defining: REP_BB_Provider;
* ;
Length REP_BB_Provider$20;
Label REP_BB_Provider='Replacement: BB_Provider';
format REP_BB_Provider $20.;
REP_BB_Provider=BB_Provider;
*;
_UFORMAT200 = strip(put(BB_Provider,$20.));
if ^(_UFORMAT200 in(
"BskyB", "Unknown", "bt", "talkta", "none", "virgin", "plusne", "vodafo"
, "telefo", "easyne", "h3g", "janet"
, "" )) then
REP_BB_Provider= "Other";
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
* Defining: REP_Curr_Offer_Amount_DTV_b;
* ;
Length REP_Curr_Offer_Amount_DTV_b$17;
Label REP_Curr_Offer_Amount_DTV_b='Replacement: Curr_Offer_Amount_DTV_b';
format REP_Curr_Offer_Amount_DTV_b $17.;
REP_Curr_Offer_Amount_DTV_b=Curr_Offer_Amount_DTV_b;
*;
_UFORMAT200 = strip(put(Curr_Offer_Amount_DTV_b,$17.));
if ^(_UFORMAT200 in(
"A.No offer", "B.Less than 20", "C.Greater than 20"
, "" )) then
REP_Curr_Offer_Amount_DTV_b= "";

* ;
* Defining: REP_DTV_product_holding_recode;
* ;
Length REP_DTV_product_holding_recode$40;
Label REP_DTV_product_holding_recode='Replacement: DTV_product_holding_recode';
format REP_DTV_product_holding_recode $40.;
REP_DTV_product_holding_recode=DTV_product_holding_recode;
*;
_UFORMAT200 = strip(put(DTV_product_holding_recode,$40.));
if ^(_UFORMAT200 in(
"Variety", "Variety with Sports & Cinema", "Original", "Variety with Sports"
, "Variety with Cinema", "Original with Sports", "Original with Cinema"
, "Original with Sports & Cinema"
, "" )) then
REP_DTV_product_holding_recode= "";

* ;
* Defining: REP_OPT_Curr_Offer_Actual_End_DT;
* ;
Length REP_OPT_Curr_Offer_Actual_End_DT$36;
Label REP_OPT_Curr_Offer_Actual_End_DT='Replacement: Transformed: Curr_Offer_Actual_End_DTV';
REP_OPT_Curr_Offer_Actual_End_DT=OPT_Curr_Offer_Actual_End_DTV;
*;
_UFORMAT200 = strip(OPT_Curr_Offer_Actual_End_DTV);
if ^(_UFORMAT200 in(
"03:-326.5--34.5, MISSING", "02:-549.5--326.5", "04:-34.5-high"
, "01:low--549.5"
, "" )) then
REP_OPT_Curr_Offer_Actual_End_DT= "";

* ;
* Defining: REP_OPT_DTV_Curr_Contract_Intend;
* ;
Length REP_OPT_DTV_Curr_Contract_Intend$36;
Label REP_OPT_DTV_Curr_Contract_Intend='Replacement: Transformed: DTV_Curr_Contract_Intended_End';
REP_OPT_DTV_Curr_Contract_Intend=OPT_DTV_Curr_Contract_Intended_E;
*;
_UFORMAT200 = strip(OPT_DTV_Curr_Contract_Intended_E);
if ^(_UFORMAT200 in(
"03:-331.5--58.5, MISSING", "02:-552.5--331.5", "04:-58.5-high"
, "01:low--552.5"
, "" )) then
REP_OPT_DTV_Curr_Contract_Intend= "";

* ;
* Defining: REP_OPT_DTV_Last_Pending_Cancel;
* ;
Length REP_OPT_DTV_Last_Pending_Cancel$36;
Label REP_OPT_DTV_Last_Pending_Cancel='Replacement: Transformed: DTV_Last_Pending_Cancel';
REP_OPT_DTV_Last_Pending_Cancel=OPT_DTV_Last_Pending_Cancel;
*;
_UFORMAT200 = strip(OPT_DTV_Last_Pending_Cancel);
if ^(_UFORMAT200 in(
"03:2697.5-high, MISSING", "02:263.5-2697.5", "01:low-263.5"
, "" )) then
REP_OPT_DTV_Last_Pending_Cancel= "";

* ;
* Defining: REP_OPT_LAST_movies_downgrade;
* ;
Length REP_OPT_LAST_movies_downgrade$36;
Label REP_OPT_LAST_movies_downgrade='Replacement: Transformed: LAST_movies_downgrade';
REP_OPT_LAST_movies_downgrade=OPT_LAST_movies_downgrade;
*;
_UFORMAT200 = strip(OPT_LAST_movies_downgrade);
if ^(_UFORMAT200 in(
"03:37.5-high, MISSING", "02:1.5-37.5", "01:low-1.5"
, "" )) then
REP_OPT_LAST_movies_downgrade= "";

* ;
* Defining: REP_OPT_OD_Last_3M;
* ;
Length REP_OPT_OD_Last_3M$36;
Label REP_OPT_OD_Last_3M='Replacement: Transformed: OD_Last_3M';
REP_OPT_OD_Last_3M=OPT_OD_Last_3M;
*;
_UFORMAT200 = strip(OPT_OD_Last_3M);
if ^(_UFORMAT200 in(
"01:low-37.5", "02:37.5-153.5", "_MISSING_", "03:153.5-high"
, "" )) then
REP_OPT_OD_Last_3M= "";

* ;
* Defining: REP_Offers_Applied_Lst_36M_DTV_b;
* ;
Length REP_Offers_Applied_Lst_36M_DTV_b$3;
Label REP_Offers_Applied_Lst_36M_DTV_b='Replacement: Offers_Applied_Lst_36M_DTV_b';
format REP_Offers_Applied_Lst_36M_DTV_b $3.;
REP_Offers_Applied_Lst_36M_DTV_b=Offers_Applied_Lst_36M_DTV_b;
*;
_UFORMAT200 = strip(put(Offers_Applied_Lst_36M_DTV_b,$3.));
if ^(_UFORMAT200 in(
"1", "0"
, "" )) then
REP_Offers_Applied_Lst_36M_DTV_b= "";

* ;
* Defining: REP_dtv_last_tenure;
* ;
Length REP_dtv_last_tenure$12;
Label REP_dtv_last_tenure='Replacement: dtv_last_tenure';
format REP_dtv_last_tenure $12.;
REP_dtv_last_tenure=dtv_last_tenure;
*;
_UFORMAT200 = strip(put(dtv_last_tenure,$12.));
if ^(_UFORMAT200 in(
"H.11+ Years", "G.7-10 Years", "F.5-6 Years", "E.3-4 Years", "D.1-2 Years"
, "C.<1 Year", "A.<3 Months", "B.<6 Months", "I.Other"
, "" )) then
REP_dtv_last_tenure= "";

* ;
* Defining: REP_last_TA_b;
* ;
Length REP_last_TA_b$12;
Label REP_last_TA_b='Replacement: last_TA_b';
format REP_last_TA_b $12.;
REP_last_TA_b=last_TA_b;
*;
_UFORMAT200 = strip(put(last_TA_b,$12.));
if ^(_UFORMAT200 in(
"J.Other", "D.<1 Year", "E.1-2 Years", "F.3-4 Years", "C.<6 Months"
, "B.<3 Months", "G.5-6 Years", "H.7-10 Years", "A.<1 Month"
, "" )) then
REP_last_TA_b= "";
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
