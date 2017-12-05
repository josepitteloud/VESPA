------------------------------------
--     PART iii - Create Probs    --
------------------------------------
--1. Set Scoring Month and Mail/No Mail Multiplier;
Create Variable @TCount Integer;
Create Variable @Month tinyint;
Create Variable @Rate decimal(4,2);

Set @Month = (select max(substring(cast(observation_dt as varchar(20)),6,2)) from mms_2017_06); select @month;
--Set @Month      = 1;                       Select @Month;
Set @Rate       = 1.2;                      Select @Rate;

--2.  CHANGE INPUT TABLES
--simmonsr.planning_201612
--simmonsr.dm_em_base_201612


--3. SUBMIT REMAINING CODE

-- SCORING
alter table mms_2017_06
 add prob_BT_VIEWER                     decimal(20,10)  null,

 add prob_fp_basic_to_TT                decimal(20,10)  null,
 add prob_op_basic_to_TT                decimal(20,10)  null,
 add prob_uplift_basic_to_TT            decimal(20,10)  null,
 add prob_resp_basic_to_TT              decimal(20,10)  null,

 add prob_fp_DM_to_TT                   decimal(20,10)  null,
 add prob_op_DM_to_TT                   decimal(20,10)  null,
 add prob_uplift_DM_to_TT               decimal(20,10)  null,
 add prob_resp_DM_to_TT                 decimal(20,10)  null,

 add prob_fp_DS_to_TT                   decimal(20,10)  null,
 add prob_op_DS_to_TT                   decimal(20,10)  null,
 add prob_uplift_DS_to_TT               decimal(20,10)  null,
 add prob_resp_DS_to_TT                 decimal(20,10)  null,

 add prob_fp_sports                     decimal(20,10)  null,
 add prob_op_sports                     decimal(20,10)  null,
 add prob_uplift_sports                 decimal(20,10)  null,
 add prob_resp_sports                   decimal(20,10)  null,

 add prob_fp_movies                     decimal(20,10)  null,
 add prob_op_movies                     decimal(20,10)  null,
 add prob_uplift_movies                 decimal(20,10)  null,
 add prob_resp_movies                   decimal(20,10)  null,

 add prob_fp_family                     decimal(20,10)  null,
 add prob_op_family                     decimal(20,10)  null,
 add prob_uplift_family                 decimal(20,10)  null,
 add prob_resp_family                   decimal(20,10)  null,

 add prob_fp_Multiscreen                decimal(20,10)  null,
 add prob_op_Multiscreen                decimal(20,10)  null,
 add prob_uplift_Multiscreen            decimal(20,10)  null,
 add prob_resp_Multiscreen              decimal(20,10)  null,

 add bb_offer_prob                      decimal(20,10)  null,
 add bb_full_prob                       decimal(20,10)  null,
 add bb_uplift_prob                     decimal(20,10)  null,
 add bb_resp_prob                       decimal(20,10)  null,

 add f_up_offer_prob                    decimal(20,10)  null,
 add f_up_full_prob                     decimal(20,10)  null,
 add f_up_uplift_prob                   decimal(20,10)  null,
 add f_up_resp_prob                     decimal(20,10)  null,

 add f_re_offer_prob                    decimal(20,10)  null,
 add f_re_full_prob                     decimal(20,10)  null,
 add f_re_uplift_prob                   decimal(20,10)  null,
 add f_re_resp_prob                     decimal(20,10)  null,

 add sge_offer_prob                     decimal(20,10)  null,
 add sge_full_prob                      decimal(20,10)  null,
 add sge_uplift_prob                    decimal(20,10)  null,
 add sge_resp_prob                      decimal(20,10)  null;



Create Variable @Exp_NonMail_Rate decimal(20,10);
Create Variable @Exp_Mail_Rate decimal(20,10);
Create Variable @Mail_Uplift_Factor decimal(20,10);
Create Variable @NonMail_Uplift_Factor decimal(20,10);

SET OPTION query_temp_space_limit =0;
-------------------------------------------------------------------------------------
-- 1.  SPORTS
Set @Exp_NonMail_Rate = (select score2 from uplift_seasonality where model_name='SPORTS_FULL_PRICE_FROM_BASIC' and month=@month);
Set @Exp_Mail_Rate    = @Exp_NonMail_Rate*@Rate;

Select @Exp_NonMail_Rate;
Select @Exp_Mail_Rate;

select account_number,
       sports,
       movies,
       sp_fp_Score as fp_Score,   -- CHANGE
       sp_op_Score as op_score,   -- CHANGE
       Cast(0.0000000000000 as decimal(20,10)) as Unscaled_Prob_NonMailed,
       Cast(0.0000000000000 as decimal(20,10)) as Unscaled_Prob_Mailed,
       Cast(0.0000000000000 as decimal(20,10)) as Prob_NonMailed,
       Cast(0.0000000000000 as decimal(20,10)) as Prob_Mailed,
       Cast(0.0000000000000 as decimal(20,10)) as Prob_Uplift,
       Cast(0.0000000000000 as decimal(20,10)) as Prob_Response

into #scoring
from mms_2017_06
--Where (movies=0 or movies=null)
--  and (sports=0 or sports=null)
;

update #scoring
 set  Unscaled_Prob_NonMailed = (exp(fp_Score)/(1+exp(fp_Score))),
      Unscaled_Prob_Mailed    = (exp(op_Score)/(1+exp(op_Score)));

Set @NonMail_Uplift_Factor = @Exp_NonMail_Rate / (select avg(Unscaled_Prob_NonMailed) from #scoring Where (movies=0 or movies=null) and (sports=0 or sports=null));
Select @NonMail_Uplift_Factor as NonMail_Uplift_Factor;

Set @Mail_Uplift_Factor = @Exp_Mail_Rate / (select avg(Unscaled_Prob_Mailed) from #scoring Where (movies=0 or movies=null) and (sports=0 or sports=null));
Select @Mail_Uplift_Factor as Mail_Uplift_Factor;

update #scoring
 set    Prob_NonMailed = Unscaled_Prob_NonMailed*@NonMail_Uplift_Factor*.9,
        Prob_Mailed    = Unscaled_Prob_Mailed*@Mail_Uplift_Factor*.9;

update #scoring
 set    Prob_Uplift    = case when (Prob_Mailed - Prob_NonMailed) > 0                      then (Prob_Mailed - Prob_NonMailed)
                              when (Prob_Mailed-Prob_NonMailed)   > (0-Prob_NonMailed*0.1) then .000001
                                                                                           else (Prob_Mailed-Prob_NonMailed) end;
update #scoring
 set Prob_Response  = case when Prob_Uplift < 0 then Prob_NonMailed else (Prob_Uplift + Prob_NonMailed) end;

update mms_2017_06 a
set     a.prob_fp_sports        = b.Prob_NonMailed,             -- CHANGE
        a.prob_op_sports        = b.Prob_Mailed,                -- CHANGE
        a.prob_uplift_sports    = b.Prob_Uplift,                -- CHANGE
        a.prob_resp_sports      = b.Prob_Response               -- CHANGE
from #scoring b
 where a.account_number=b.account_number;

Select Avg(prob_fp_sports)/.9 as FP from mms_2017_06 Where (movies=0 or movies=null) and (sports=0 or sports=null);;
Select Avg(prob_Op_sports)/.9 as FP from mms_2017_06 Where (movies=0 or movies=null) and (sports=0 or sports=null);;
-------------------------------------------------------------------------

-------------------------------------------------------------------------------------
-- 2.  MOVIES
Set @Exp_NonMail_Rate = (select score2 from uplift_seasonality where model_name='MOVIES_FULL_PRICE_FROM_BASIC' and month=@month);
Set @Exp_Mail_Rate    = @Exp_NonMail_Rate*@Rate;

Select @Exp_NonMail_Rate;
Select @Exp_Mail_Rate;

DROP TABLE #scoring;

select account_number,
       sports,
       movies,
       mo_fp_Score as fp_Score,   -- CHANGE
       mo_op_Score as op_score,   -- CHANGE
       Cast(0.0000000000000 as decimal(20,10)) as Unscaled_Prob_NonMailed,
       Cast(0.0000000000000 as decimal(20,10)) as Unscaled_Prob_Mailed,
       Cast(0.0000000000000 as decimal(20,10)) as Prob_NonMailed,
       Cast(0.0000000000000 as decimal(20,10)) as Prob_Mailed,
       Cast(0.0000000000000 as decimal(20,10)) as Prob_Uplift,
       Cast(0.0000000000000 as decimal(20,10)) as Prob_Response

into #scoring
from mms_2017_06;

update #scoring
 set  Unscaled_Prob_NonMailed = (exp(fp_Score)/(1+exp(fp_Score))),
      Unscaled_Prob_Mailed    = (exp(op_Score)/(1+exp(op_Score)));

Set @NonMail_Uplift_Factor = @Exp_NonMail_Rate / (select avg(Unscaled_Prob_NonMailed) from #scoring Where (movies=0 or movies=null) and (sports=0 or sports=null));
Select @NonMail_Uplift_Factor as NonMail_Uplift_Factor;

Set @Mail_Uplift_Factor = @Exp_Mail_Rate / (select avg(Unscaled_Prob_Mailed) from #scoring Where (movies=0 or movies=null) and (sports=0 or sports=null));
Select @Mail_Uplift_Factor as Mail_Uplift_Factor;

update #scoring
 set    Prob_NonMailed = Unscaled_Prob_NonMailed*@NonMail_Uplift_Factor*.9,
        Prob_Mailed    = Unscaled_Prob_Mailed*@Mail_Uplift_Factor*.9;

update #scoring
 set    Prob_Uplift    = case when (Prob_Mailed - Prob_NonMailed) > 0                     then (Prob_Mailed - Prob_NonMailed)
                              when (Prob_Mailed-Prob_NonMailed)  > (0-Prob_NonMailed*0.1) then .000001
                                                                                          else (Prob_Mailed-Prob_NonMailed) end;
update #scoring
 set Prob_Response  = case when Prob_Uplift < 0 then Prob_NonMailed else (Prob_Uplift + Prob_NonMailed) end;


update mms_2017_06 a
set     a.prob_fp_movies        = b.Prob_NonMailed,             -- CHANGE
        a.prob_op_movies        = b.Prob_Mailed,                -- CHANGE
        a.prob_uplift_movies    = b.Prob_Uplift,                -- CHANGE
        a.prob_resp_movies      = b.Prob_Response               -- CHANGE
from #scoring b
 where a.account_number=b.account_number;

Select Avg(prob_fp_movies) as FP from mms_2017_06 where Movies is null or Movies = 0;;
Select Avg(prob_op_movies) as OP from mms_2017_06 where Movies is null or Movies = 0;;

-------------------------------------------------------------------------------------


-------------------------------------------------------------------------------------
-- 3  Basic -> Top Tier
Set @Exp_NonMail_Rate = (select score2 from uplift_seasonality where model_name='TOPTIER_FULL_PRICE_FROM_BASIC' and month=@month);
Set @Exp_Mail_Rate    = @Exp_NonMail_Rate*@Rate;

Select @Exp_NonMail_Rate;
Select @Exp_Mail_Rate;

DROP TABLE #scoring;

select account_number,
       sports,
       movies,
       tt_fp_Score as fp_Score,   -- CHANGE
       tt_op_Score as op_score,   -- CHANGE
       Cast(0.0000000000000 as decimal(20,10)) as Unscaled_Prob_NonMailed,
       Cast(0.0000000000000 as decimal(20,10)) as Unscaled_Prob_Mailed,
       Cast(0.0000000000000 as decimal(20,10)) as Prob_NonMailed,
       Cast(0.0000000000000 as decimal(20,10)) as Prob_Mailed,
       Cast(0.0000000000000 as decimal(20,10)) as Prob_Uplift,
       Cast(0.0000000000000 as decimal(20,10)) as Prob_Response

into #scoring
from mms_2017_06;


update #scoring
 set  Unscaled_Prob_NonMailed = (exp(fp_Score)/(1+exp(fp_Score))),
      Unscaled_Prob_Mailed    = (exp(op_Score)/(1+exp(op_Score)));

Set @NonMail_Uplift_Factor = @Exp_NonMail_Rate / (select avg(Unscaled_Prob_NonMailed) from #scoring Where (movies=0 or movies=null)   and (sports=0 or sports=null));
Select @NonMail_Uplift_Factor as NonMail_Uplift_Factor;

Set @Mail_Uplift_Factor = @Exp_Mail_Rate / (select avg(Unscaled_Prob_Mailed) from #scoring Where (movies=0 or movies=null)   and (sports=0 or sports=null));
Select @Mail_Uplift_Factor as Mail_Uplift_Factor;

update #scoring
 set    Prob_NonMailed = Unscaled_Prob_NonMailed*@NonMail_Uplift_Factor*.9,
        Prob_Mailed    = Unscaled_Prob_Mailed*@Mail_Uplift_Factor*.9;

update #scoring
 set    Prob_Uplift    = case when (Prob_Mailed - Prob_NonMailed) > 0                     then (Prob_Mailed - Prob_NonMailed)
                              when (Prob_Mailed-Prob_NonMailed)  > (0-Prob_NonMailed*0.1) then .000001
                                                                                          else (Prob_Mailed-Prob_NonMailed) end;
update #scoring
 set Prob_Response  = case when Prob_Uplift < 0 then Prob_NonMailed else (Prob_Uplift + Prob_NonMailed) end;


update mms_2017_06 a
set     a.prob_fp_basic_to_tt         = b.Prob_NonMailed,             -- CHANGE
        a.prob_op_basic_to_tt         = b.Prob_Mailed,                -- CHANGE
        a.prob_uplift_basic_to_tt     = b.Prob_Uplift,                -- CHANGE
        a.prob_resp_basic_to_tt       = b.Prob_Response               -- CHANGE
from #scoring b
 where a.account_number=b.account_number
;

Select Avg(prob_fp_basic_to_tt) as FP from mms_2017_06 where (sports is null or sports = 0) and (Movies is null or Movies = 0);;
Select Avg(prob_op_basic_to_tt) as OP from mms_2017_06 where (sports is null or sports = 0) and (Movies is null or Movies = 0);;

-------------------------------------------------------------------------


-------------------------------------------------------------------------------------
-- 4.  TT (DS -> TT)
Set @Exp_NonMail_Rate = (select score2 from uplift_seasonality where model_name='TOPTIER_FULL_PRICE_FROM_DS' and month=@month);
Set @Exp_Mail_Rate    = @Exp_NonMail_Rate*@Rate;

Select @Exp_NonMail_Rate;
Select @Exp_Mail_Rate;

DROP TABLE #scoring;

select account_number,
       sports,
       movies,
       TTDS_fp_Score as fp_Score,   -- CHANGE
       TTDS_op_Score as op_score,   -- CHANGE
       Cast(0.0000000000000 as decimal(20,10)) as Unscaled_Prob_NonMailed,
       Cast(0.0000000000000 as decimal(20,10)) as Unscaled_Prob_Mailed,
       Cast(0.0000000000000 as decimal(20,10)) as Prob_NonMailed,
       Cast(0.0000000000000 as decimal(20,10)) as Prob_Mailed,
       Cast(0.0000000000000 as decimal(20,10)) as Prob_Uplift,
       Cast(0.0000000000000 as decimal(20,10)) as Prob_Response

into #scoring
from mms_2017_06;


update #scoring
 set  Unscaled_Prob_NonMailed = (exp(fp_Score)/(1+exp(fp_Score))),
      Unscaled_Prob_Mailed    = (exp(op_Score)/(1+exp(op_Score)));

Set @NonMail_Uplift_Factor = @Exp_NonMail_Rate / (select avg(Unscaled_Prob_NonMailed) from #scoring Where (movies=0 or movies=null)   and (sports=2));
Select @NonMail_Uplift_Factor as NonMail_Uplift_Factor;

Set @Mail_Uplift_Factor = @Exp_Mail_Rate / (select avg(Unscaled_Prob_Mailed) from #scoring Where (movies=0 or movies=null)   and (sports=2));
Select @Mail_Uplift_Factor as Mail_Uplift_Factor;

update #scoring
 set    Prob_NonMailed = Unscaled_Prob_NonMailed*@NonMail_Uplift_Factor*.9,
        Prob_Mailed    = Unscaled_Prob_Mailed*@Mail_Uplift_Factor*.9;

update #scoring
 set    Prob_Uplift    = case when (Prob_Mailed - Prob_NonMailed) > 0                     then (Prob_Mailed - Prob_NonMailed)
                              when (Prob_Mailed-Prob_NonMailed)  > (0-Prob_NonMailed*0.1) then .000001
                                                                                          else (Prob_Mailed-Prob_NonMailed) end;
update #scoring
 set Prob_Response  = case when Prob_Uplift < 0 then Prob_NonMailed else (Prob_Uplift + Prob_NonMailed) end;


update mms_2017_06 a
set     a.prob_fp_DS_to_TT         = b.Prob_NonMailed,             -- CHANGE
        a.prob_op_DS_to_TT         = b.Prob_Mailed,                -- CHANGE
        a.prob_uplift_DS_to_TT     = b.Prob_Uplift,                -- CHANGE
        a.prob_resp_DS_to_TT       = b.Prob_Response               -- CHANGE
from #scoring b
 where a.account_number=b.account_number;

Select Avg(prob_fp_DS_to_TT) as FP from mms_2017_06 where sports= 2 and (Movies is null or Movies = 0);;
Select Avg(prob_op_DS_to_TT) as OP from mms_2017_06 where sports= 2 and (Movies is null or Movies = 0);;
-------------------------------------------------------------------------

-------------------------------------------------------------------------------------
-- 5.  TT (DM -> TT)
Set @Exp_NonMail_Rate = (select score2 from uplift_seasonality where model_name='TOPTIER_FULL_PRICE_FROM_DM' and month=@month);
Set @Exp_Mail_Rate    = @Exp_NonMail_Rate*@Rate;

Select @Exp_NonMail_Rate;
Select @Exp_Mail_Rate;

DROP TABLE #scoring;

select account_number,
       sports,
       movies,
       ttdm_fp_Score as fp_Score,   -- CHANGE
       ttdm_op_Score as op_score,   -- CHANGE
       Cast(0.0000000000000 as decimal(20,10)) as Unscaled_Prob_NonMailed,
       Cast(0.0000000000000 as decimal(20,10)) as Unscaled_Prob_Mailed,
       Cast(0.0000000000000 as decimal(20,10)) as Prob_NonMailed,
       Cast(0.0000000000000 as decimal(20,10)) as Prob_Mailed,
       Cast(0.0000000000000 as decimal(20,10)) as Prob_Uplift,
       Cast(0.0000000000000 as decimal(20,10)) as Prob_Response

into #scoring
from mms_2017_06;

update #scoring
 set  Unscaled_Prob_NonMailed = (exp(fp_Score)/(1+exp(fp_Score))),
      Unscaled_Prob_Mailed    = (exp(op_Score)/(1+exp(op_Score)));

Set @NonMail_Uplift_Factor = @Exp_NonMail_Rate / (select avg(Unscaled_Prob_NonMailed) from #scoring Where (sports=0 or sports=null) and (Movies=2));
Select @NonMail_Uplift_Factor as NonMail_Uplift_Factor;

Set @Mail_Uplift_Factor = @Exp_Mail_Rate / (select avg(Unscaled_Prob_Mailed) from #scoring Where (sports=0 or sports=null) and (Movies=2));
Select @Mail_Uplift_Factor as Mail_Uplift_Factor;

update #scoring
 set    Prob_NonMailed = Unscaled_Prob_NonMailed*@NonMail_Uplift_Factor*.9,
        Prob_Mailed    = Unscaled_Prob_Mailed*@Mail_Uplift_Factor*.9;

update #scoring
 set    Prob_Uplift    = case when (Prob_Mailed - Prob_NonMailed) > 0                     then (Prob_Mailed - Prob_NonMailed)
                              when (Prob_Mailed-Prob_NonMailed)  > (0-Prob_NonMailed*0.1) then .000001
                                                                                          else (Prob_Mailed-Prob_NonMailed) end;
update #scoring
 set Prob_Response  = case when Prob_Uplift < 0 then Prob_NonMailed else (Prob_Uplift + Prob_NonMailed) end;


update mms_2017_06 a
set     a.prob_fp_DM_to_TT         = b.Prob_NonMailed,             -- CHANGE
        a.prob_op_DM_to_TT         = b.Prob_Mailed,                -- CHANGE
        a.prob_uplift_DM_to_TT     = b.Prob_Uplift,                -- CHANGE
        a.prob_resp_DM_to_TT       = b.Prob_Response               -- CHANGE
from #scoring b
 where a.account_number=b.account_number;

Select Avg(prob_fp_DM_to_TT)/.9 as FP from mms_2017_06 where Movies= 2 and (sports is null or sports = 0);;
Select Avg(prob_op_DM_to_TT)/.9 as OP from mms_2017_06 where Movies= 2 and (sports is null or sports = 0);;

-------------------------------------------------------------------------

-------------------------------------------------------------------------------------
-- 6.  BBU
Set @Exp_NonMail_Rate = (select score2 from uplift_seasonality where model_name='BBU_FULL_PRICE' and month=@month);
Set @Exp_Mail_Rate    = @Exp_NonMail_Rate*@Rate;

Select @Exp_NonMail_Rate;
Select @Exp_Mail_Rate;

DROP TABLE #scoring;

select account_number,
       broadband,
       bb_fp_Score as fp_Score,   -- CHANGE
       bb_op_Score as op_score,   -- CHANGE
       Cast(0.0000000000000 as decimal(20,10)) as Unscaled_Prob_NonMailed,
       Cast(0.0000000000000 as decimal(20,10)) as Unscaled_Prob_Mailed,
       Cast(0.0000000000000 as decimal(20,10)) as Prob_NonMailed,
       Cast(0.0000000000000 as decimal(20,10)) as Prob_Mailed,
       Cast(0.0000000000000 as decimal(20,10)) as Prob_Uplift,
       Cast(0.0000000000000 as decimal(20,10)) as Prob_Response

into #scoring
from mms_2017_06;

update #scoring
 set  Unscaled_Prob_NonMailed = (exp(fp_Score)/(1+exp(fp_Score))),
      Unscaled_Prob_Mailed    = (exp(op_Score)/(1+exp(op_Score)));

Set @NonMail_Uplift_Factor = @Exp_NonMail_Rate / (select avg(Unscaled_Prob_NonMailed) from #scoring Where broadband = 0);
Select @NonMail_Uplift_Factor as NonMail_Uplift_Factor;

Set @Mail_Uplift_Factor = @Exp_Mail_Rate / (select avg(Unscaled_Prob_Mailed) from #scoring Where broadband = 0);
Select @Mail_Uplift_Factor as Mail_Uplift_Factor;

update #scoring
 set    Prob_NonMailed = Unscaled_Prob_NonMailed*@NonMail_Uplift_Factor*.9,
        Prob_Mailed    = Unscaled_Prob_Mailed*@Mail_Uplift_Factor*.9;

update #scoring
 set    Prob_Uplift    = case when (Prob_Mailed - Prob_NonMailed) > 0                     then (Prob_Mailed - Prob_NonMailed)
                              when (Prob_Mailed-Prob_NonMailed)  > (0-Prob_NonMailed*0.1) then .000001
                                                                                          else (Prob_Mailed-Prob_NonMailed) end;
update #scoring
 set Prob_Response  = case when Prob_Uplift < 0 then Prob_NonMailed else (Prob_Uplift + Prob_NonMailed) end;


update mms_2017_06 a
set     a.bb_full_prob          = b.Prob_NonMailed,             -- CHANGE
        a.bb_offer_prob         = b.Prob_Mailed,                -- CHANGE
        a.bb_uplift_prob        = b.Prob_Uplift,                -- CHANGE
        a.bb_resp_prob          = b.Prob_Response               -- CHANGE
from #scoring b
 where a.account_number=b.account_number;

Select Avg(bb_full_prob)/.9 as FP from mms_2017_06 where broadband=0;
Select Avg(bb_offer_prob)/.9 as OP from mms_2017_06 where broadband=0;

-------------------------------------------------------------------------


-------------------------------------------------------------------------------------
-- 7.  F_UP
Set @Exp_NonMail_Rate = (select score2 from uplift_seasonality where model_name='BBFIBRE_FULL_PRICE' and month=@month);
Set @Exp_Mail_Rate    = @Exp_NonMail_Rate*@Rate;

Select @Exp_NonMail_Rate;
Select @Exp_Mail_Rate;

DROP TABLE #scoring;

select account_number,
       broadband,
       bf_fp_Score as fp_Score,   -- CHANGE
       bf_op_Score as op_score,   -- CHANGE
       Cast(0.0000000000000 as decimal(20,10)) as Unscaled_Prob_NonMailed,
       Cast(0.0000000000000 as decimal(20,10)) as Unscaled_Prob_Mailed,
       Cast(0.0000000000000 as decimal(20,10)) as Prob_NonMailed,
       Cast(0.0000000000000 as decimal(20,10)) as Prob_Mailed,
       Cast(0.0000000000000 as decimal(20,10)) as Prob_Uplift,
       Cast(0.0000000000000 as decimal(20,10)) as Prob_Response

into #scoring
from mms_2017_06;

update #scoring
 set  Unscaled_Prob_NonMailed = (exp(fp_Score)/(1+exp(fp_Score))),
      Unscaled_Prob_Mailed    = (exp(op_Score)/(1+exp(op_Score)));

Set @NonMail_Uplift_Factor = @Exp_NonMail_Rate / (select avg(Unscaled_Prob_NonMailed) from #scoring Where broadband = 0);
Select @NonMail_Uplift_Factor as NonMail_Uplift_Factor;

Set @Mail_Uplift_Factor = @Exp_Mail_Rate / (select avg(Unscaled_Prob_Mailed) from #scoring Where broadband = 0);
Select @Mail_Uplift_Factor as Mail_Uplift_Factor;

update #scoring
 set    Prob_NonMailed = Unscaled_Prob_NonMailed*@NonMail_Uplift_Factor*.9,
        Prob_Mailed    = Unscaled_Prob_Mailed*@Mail_Uplift_Factor*.9;

update #scoring
 set    Prob_Uplift    = case when (Prob_Mailed - Prob_NonMailed) > 0                     then (Prob_Mailed - Prob_NonMailed)
                              when (Prob_Mailed-Prob_NonMailed)  > (0-Prob_NonMailed*0.1) then .000001
                                                                                          else (Prob_Mailed-Prob_NonMailed) end;
update #scoring
 set Prob_Response  = case when Prob_Uplift < 0 then Prob_NonMailed else (Prob_Uplift + Prob_NonMailed) end;


update mms_2017_06 a
set     a.f_up_full_prob          = b.Prob_NonMailed,             -- CHANGE
        a.f_up_offer_prob         = b.Prob_Mailed,                -- CHANGE
        a.f_up_uplift_prob        = b.Prob_Uplift,                -- CHANGE
        a.f_up_resp_prob          = b.Prob_Response               -- CHANGE
from #scoring b
 where a.account_number=b.account_number;

Select Avg(f_up_full_prob)/.9 as FP from mms_2017_06 Where broadband = 0;
Select Avg(f_up_offer_prob)/.9 as OP from mms_2017_06 Where broadband = 0;

-------------------------------------------------------------------------

-------------------------------------------------------------------------------------
-- 8.  F REGRADE
Set @Exp_NonMail_Rate = (select score2 from uplift_seasonality where model_name='FIBRE_RE_FULL_PRICE' and month=@month);
Set @Exp_Mail_Rate    = @Exp_NonMail_Rate*@Rate;

Select @Exp_NonMail_Rate;
Select @Exp_Mail_Rate;

DROP TABLE #scoring;

select account_number,
       broadband,
       bb_type,
       fr_fp_Score as fp_Score,   -- CHANGE
       fr_op_Score as op_score,   -- CHANGE
       Cast(0.0000000000000 as decimal(20,10)) as Unscaled_Prob_NonMailed,
       Cast(0.0000000000000 as decimal(20,10)) as Unscaled_Prob_Mailed,
       Cast(0.0000000000000 as decimal(20,10)) as Prob_NonMailed,
       Cast(0.0000000000000 as decimal(20,10)) as Prob_Mailed,
       Cast(0.0000000000000 as decimal(20,10)) as Prob_Uplift,
       Cast(0.0000000000000 as decimal(20,10)) as Prob_Response

into #scoring
from mms_2017_06;

update #scoring
 set  Unscaled_Prob_NonMailed = (exp(fp_Score)/(1+exp(fp_Score))),
      Unscaled_Prob_Mailed    = (exp(op_Score)/(1+exp(op_Score)));

Set @NonMail_Uplift_Factor = @Exp_NonMail_Rate / (select avg(Unscaled_Prob_NonMailed) from #scoring where broadband=1 and upper(bb_type) not like '%FIBRE%');
Select @NonMail_Uplift_Factor as NonMail_Uplift_Factor;

Set @Mail_Uplift_Factor = @Exp_Mail_Rate / (select avg(Unscaled_Prob_Mailed) from #scoring where broadband=1 and upper(bb_type) not like '%FIBRE%');
Select @Mail_Uplift_Factor as Mail_Uplift_Factor;

update #scoring
 set    Prob_NonMailed = Unscaled_Prob_NonMailed*@NonMail_Uplift_Factor*.9,
        Prob_Mailed    = Unscaled_Prob_Mailed*@Mail_Uplift_Factor*.9;

update #scoring
 set    Prob_Uplift    = case when (Prob_Mailed - Prob_NonMailed) > 0                     then (Prob_Mailed - Prob_NonMailed)
                              when (Prob_Mailed-Prob_NonMailed)  > (0-Prob_NonMailed*0.1) then .000001
                                                                                          else (Prob_Mailed-Prob_NonMailed) end;
update #scoring
 set Prob_Response  = case when Prob_Uplift < 0 then Prob_NonMailed else (Prob_Uplift + Prob_NonMailed) end;


update mms_2017_06 a
set     a.f_re_full_prob          = b.Prob_NonMailed,             -- CHANGE
        a.f_re_offer_prob         = b.Prob_Mailed,                -- CHANGE
        a.f_re_uplift_prob        = b.Prob_Uplift,                -- CHANGE
        a.f_re_resp_prob          = b.Prob_Response               -- CHANGE
from #scoring b
 where a.account_number=b.account_number;

Select Avg(f_re_full_prob)/.9  as FP from mms_2017_06 where broadband=1 and upper(bb_type) not like '%FIBRE%';
Select Avg(f_re_offer_prob)/.9 as OP from mms_2017_06 where broadband=1 and upper(bb_type) not like '%FIBRE%';
-------------------------------------------------------------------------

-------------------------------------------------------------------------------------
-- 9.  SGE
Set @Exp_NonMail_Rate = (select score2 from uplift_seasonality where model_name='SKYGOEXTRA_FULL_PRICE' and month=@month);
Set @Exp_Mail_Rate    = @Exp_NonMail_Rate*@Rate;

Select @Exp_NonMail_Rate;
Select @Exp_Mail_Rate;

DROP TABLE #scoring;

select account_number,
       skygoextra,
       sge_fp_Score as fp_Score,   -- CHANGE
       sge_op_Score as op_score,   -- CHANGE
       Cast(0.0000000000000 as decimal(20,10)) as Unscaled_Prob_NonMailed,
       Cast(0.0000000000000 as decimal(20,10)) as Unscaled_Prob_Mailed,
       Cast(0.0000000000000 as decimal(20,10)) as Prob_NonMailed,
       Cast(0.0000000000000 as decimal(20,10)) as Prob_Mailed,
       Cast(0.0000000000000 as decimal(20,10)) as Prob_Uplift,
       Cast(0.0000000000000 as decimal(20,10)) as Prob_Response

into #scoring
from mms_2017_06;

update #scoring
 set  Unscaled_Prob_NonMailed = (exp(fp_Score)/(1+exp(fp_Score))),
      Unscaled_Prob_Mailed    = (exp(op_Score)/(1+exp(op_Score)));

Set @NonMail_Uplift_Factor = @Exp_NonMail_Rate / (select avg(Unscaled_Prob_NonMailed) from #scoring where skygoextra=0);
Select @NonMail_Uplift_Factor as NonMail_Uplift_Factor;

Set @Mail_Uplift_Factor = @Exp_Mail_Rate / (select avg(Unscaled_Prob_Mailed) from #scoring where skygoextra=0);
Select @Mail_Uplift_Factor as Mail_Uplift_Factor;

update #scoring
 set    Prob_NonMailed = Unscaled_Prob_NonMailed*@NonMail_Uplift_Factor*.9,
        Prob_Mailed    = Unscaled_Prob_Mailed*@Mail_Uplift_Factor*.9;

update #scoring
 set    Prob_Uplift    = case when (Prob_Mailed - Prob_NonMailed) > 0                     then (Prob_Mailed - Prob_NonMailed)
                              when (Prob_Mailed-Prob_NonMailed)  > (0-Prob_NonMailed*0.1) then .000001
                                                                                          else (Prob_Mailed-Prob_NonMailed) end;
update #scoring
 set Prob_Response  = case when Prob_Uplift < 0 then Prob_NonMailed else (Prob_Uplift + Prob_NonMailed) end;


update mms_2017_06 a
set     a.sge_full_prob          = b.Prob_NonMailed,             -- CHANGE
        a.sge_offer_prob         = b.Prob_Mailed,                -- CHANGE
        a.sge_uplift_prob        = b.Prob_Uplift,                -- CHANGE
        a.sge_resp_prob          = b.Prob_Response               -- CHANGE
from #scoring b
 where a.account_number=b.account_number;

Select Avg(sge_full_prob)/.9  as FP from mms_2017_06 where skygoextra=0;
Select Avg(sge_offer_prob)/.9 as OP from mms_2017_06 where skygoextra=0;
-------------------------------------------------------------------------

-------------------------------------------------------------------------------------
-- 10. MULTISCREEN
Set @Exp_NonMail_Rate = (select score2 from uplift_seasonality where model_name='MULTISCREEN_FULL_PRICE' and month=@month);
Set @Exp_Mail_Rate    = @Exp_NonMail_Rate*@Rate;

Select @Exp_NonMail_Rate;
Select @Exp_Mail_Rate;

DROP TABLE #scoring;

select account_number,
       mr,
       ms_fp_Score as fp_Score,   -- CHANGE
       ms_op_Score as op_score,   -- CHANGE
       Cast(0.0000000000000 as decimal(20,10)) as Unscaled_Prob_NonMailed,
       Cast(0.0000000000000 as decimal(20,10)) as Unscaled_Prob_Mailed,
       Cast(0.0000000000000 as decimal(20,10)) as Prob_NonMailed,
       Cast(0.0000000000000 as decimal(20,10)) as Prob_Mailed,
       Cast(0.0000000000000 as decimal(20,10)) as Prob_Uplift,
       Cast(0.0000000000000 as decimal(20,10)) as Prob_Response

into --drop table
        #scoring
from mms_2017_06;

update #scoring
 set  Unscaled_Prob_NonMailed = (exp(fp_Score)/(1+exp(fp_Score))),
      Unscaled_Prob_Mailed    = (exp(op_Score)/(1+exp(op_Score)));

Set @NonMail_Uplift_Factor = @Exp_NonMail_Rate / (select avg(Unscaled_Prob_NonMailed) from #scoring where mr=0);
Select @NonMail_Uplift_Factor as NonMail_Uplift_Factor;

Set @Mail_Uplift_Factor = @Exp_Mail_Rate / (select avg(Unscaled_Prob_Mailed) from #scoring where mr=0);
Select @Mail_Uplift_Factor as Mail_Uplift_Factor;

update #scoring
 set    Prob_NonMailed = Unscaled_Prob_NonMailed*@NonMail_Uplift_Factor*.9,
        Prob_Mailed    = Unscaled_Prob_Mailed*@Mail_Uplift_Factor*.9;

update #scoring
 set    Prob_Uplift    = case when (Prob_Mailed - Prob_NonMailed) > 0                     then (Prob_Mailed - Prob_NonMailed)
                              when (Prob_Mailed-Prob_NonMailed)  > (0-Prob_NonMailed*0.1) then .000001
                                                                                          else (Prob_Mailed-Prob_NonMailed) end;
update #scoring
 set Prob_Response  = case when Prob_Uplift < 0 then Prob_NonMailed else (Prob_Uplift + Prob_NonMailed) end;


update mms_2017_06 a
set     a.prob_fp_multiscreen        = b.Prob_NonMailed,             -- CHANGE
        a.prob_op_multiscreen        = b.Prob_Mailed,                -- CHANGE
        a.prob_uplift_multiscreen    = b.Prob_Uplift,                -- CHANGE
        a.prob_resp_multiscreen      = b.Prob_Response               -- CHANGE
from #scoring b
 where a.account_number=b.account_number;

Select Avg(prob_fp_multiscreen)/.9  as FP from mms_2017_06 where mr=0;
Select Avg(prob_op_multiscreen)/.9  as OP from mms_2017_06 where mr=0;

-------------------------------------------------------------------------------------

-------------------------------------------------------------------------------------
-- 11. FAMILY
Set @Exp_NonMail_Rate = (select score2 from uplift_seasonality where model_name='FAMILY_FULL_PRICE' and month=@month);
Set @Exp_Mail_Rate    = @Exp_NonMail_Rate*@Rate;

Select @Exp_NonMail_Rate;
Select @Exp_Mail_Rate;

DROP TABLE #scoring;

select account_number,
       package_desc,
       FAMILY_fp_Score as fp_Score,   -- CHANGE
       FAMILY_op_Score as op_score,   -- CHANGE
       Cast(0.0000000000000 as decimal(20,10)) as Unscaled_Prob_NonMailed,
       Cast(0.0000000000000 as decimal(20,10)) as Unscaled_Prob_Mailed,
       Cast(0.0000000000000 as decimal(20,10)) as Prob_NonMailed,
       Cast(0.0000000000000 as decimal(20,10)) as Prob_Mailed,
       Cast(0.0000000000000 as decimal(20,10)) as Prob_Uplift,
       Cast(0.0000000000000 as decimal(20,10)) as Prob_Response

into --drop table
        #scoring
from mms_2017_06;

update #scoring
 set  Unscaled_Prob_NonMailed = (exp(fp_Score)/(1+exp(fp_Score))),
      Unscaled_Prob_Mailed    = (exp(op_Score)/(1+exp(op_Score)));

Set @NonMail_Uplift_Factor = @Exp_NonMail_Rate / (select avg(Unscaled_Prob_NonMailed) from #scoring where package_desc in('Original','Variety'));
Select @NonMail_Uplift_Factor as NonMail_Uplift_Factor;

Set @Mail_Uplift_Factor = @Exp_Mail_Rate / (select avg(Unscaled_Prob_Mailed) from #scoring where package_desc in('Original','Variety'));
Select @Mail_Uplift_Factor as Mail_Uplift_Factor;

update #scoring
 set    Prob_NonMailed = Unscaled_Prob_NonMailed*@NonMail_Uplift_Factor*.9,
        Prob_Mailed    = Unscaled_Prob_Mailed*@Mail_Uplift_Factor*.9;

update #scoring
 set    Prob_Uplift    = case when (Prob_Mailed - Prob_NonMailed) > 0                     then (Prob_Mailed - Prob_NonMailed)
                              when (Prob_Mailed-Prob_NonMailed)  > (0-Prob_NonMailed*0.1) then .000001
                                                                                          else (Prob_Mailed-Prob_NonMailed) end;
update #scoring
 set Prob_Response  = case when Prob_Uplift < 0 then Prob_NonMailed else (Prob_Uplift + Prob_NonMailed) end;


update mms_2017_06 a
set     a.prob_fp_family        = b.Prob_NonMailed,             -- CHANGE
        a.prob_op_family        = b.Prob_Mailed,                -- CHANGE
        a.prob_uplift_family    = b.Prob_Uplift,                -- CHANGE
        a.prob_resp_family      = b.Prob_Response               -- CHANGE
from #scoring b
 where a.account_number=b.account_number;

Select Avg(prob_fp_family)/.9  as FP from mms_2017_06 where package_desc in('Original','Variety');
Select Avg(prob_op_family)/.9  as OP from mms_2017_06 where package_desc in('Original','Variety');

-- 12 BT VIEWER MODEL
update mms_2017_06
 set  prob_BT_VIEWER = (exp(bt_fp_score)/(1+exp(bt_fp_score)));

-------------------------------------------------------------------------

-- COMBINE CONTACTABLE BASE WITH MODEL SCORES
SELECT
     a.account_number,
     b.observation_dt as obs_pt,-- CHECK
     a.sports,
     a.movies,
     a.sports_eli as sports_eligible,
     a.movies_eli as movies_eligible,
     a.tt_eli as tt_eligible,
     a.bb_eli as bb_eligible,
     a.F_up_eli as F_up_eligible,
     a.f_re_eli as f_re_eligible,
     a.family_eli as family_eligible,
     a.MS_eli as MS_eligible,
     a.SGE_eli as SGE_eligible,
     a.channel,
     b.dtv,
     b.Email_Open_Propensity,
     b.Ratio_Email_Open,
     b.Modeled_channel_Preference,
     cast(000.0000000 as decimal(20,10)) as DM_Channel_model,
     cast(000.0000000 as DECIMAL(20,20)) as rand_num,
     case when a.sports=2 and a.movies=0  then 1 else 0 end as Movies_DS_eligible,
     case when a.sports=0 and a.movies=2  then 1 else 0 end as Sports_DM_eligible,

     a.offers_dtv as exclude_dtv2,
     a.offers_comms as exclude_bb2,

     b.prob_FP_Sports,
     b.Prob_OP_Sports,
     b.Prob_Uplift_Sports,
     b.prob_resp_sports,
     cast(null as tinyint) as decile_FP_Sports,
     cast(null as tinyint) as decile_OP_Sports,
     cast(null as tinyint) as decile_Uplift_Sports,
     cast(null as tinyint) as decile_resp_sports,

     b.prob_FP_Movies,
     b.Prob_OP_Movies,
     b.Prob_Uplift_Movies,
     b.Prob_resp_Movies,
     cast(null as tinyint) as decile_FP_Movies,
     cast(null as tinyint) as decile_OP_Movies,
     cast(null as tinyint) as decile_Uplift_Movies,
     cast(null as tinyint) as decile_resp_Movies,

     b.prob_FP_basic_to_TT,
     b.Prob_OP_basic_to_TT,
     b.Prob_Uplift_basic_to_TT,
     b.Prob_resp_basic_to_TT,
     cast(null as tinyint) as decile_FP_basic_to_TT,
     cast(null as tinyint) as decile_OP_basic_to_TT,
     cast(null as tinyint) as decile_Uplift_basic_to_TT,
     cast(null as tinyint) as decile_resp_basic_to_TT,

     b.prob_FP_DM_to_TT,
     b.Prob_OP_DM_to_TT,
     b.Prob_Uplift_DM_to_TT,
     b.Prob_resp_DM_to_TT,
     cast(null as tinyint) as decile_FP_DM_to_TT,
     cast(null as tinyint) as decile_OP_DM_to_TT,
     cast(null as tinyint) as decile_Uplift_DM_to_TT,
     cast(null as tinyint) as decile_resp_DM_to_TT,

     b.prob_FP_DS_to_TT,
     b.Prob_OP_DS_to_TT,
     b.Prob_Uplift_DS_to_TT,
     b.Prob_resp_DS_to_TT,
     cast(null as tinyint) as decile_FP_DS_to_TT,
     cast(null as tinyint) as decile_OP_DS_to_TT,
     cast(null as tinyint) as decile_Uplift_DS_to_TT,
     cast(null as tinyint) as decile_resp_DS_to_TT,

     b.prob_FP_Family,
     b.Prob_OP_Family,
     b.Prob_Uplift_Family,
     b.prob_resp_Family,
     cast(null as tinyint) as decile_FP_Family,
     cast(null as tinyint) as decile_OP_Family,
     cast(null as tinyint) as decile_Uplift_Family,
     cast(null as tinyint) as decile_resp_Family,

     b.prob_FP_Multiscreen,
     b.Prob_OP_Multiscreen,
     b.Prob_Uplift_Multiscreen,
     b.Prob_resp_Multiscreen,
     cast(null as tinyint) as decile_FP_Multiscreen,
     cast(null as tinyint) as decile_OP_Multiscreen,
     cast(null as tinyint) as decile_Uplift_Multiscreen,
     cast(null as tinyint) as decile_resp_Multiscreen,

     b.bb_offer_prob,
     b.bb_full_prob,
     b.bb_uplift_prob,
     b.bb_resp_prob,
     cast(null as tinyint) as bb_offer_decile,
     cast(null as tinyint) as bb_full_decile,
     cast(null as tinyint) as bb_uplift_decile,
     cast(null as tinyint) as bb_resp_decile,

     b.f_up_offer_prob,
     b.f_up_full_prob,
     b.f_up_uplift_prob,
     b.f_up_resp_prob,
     cast(null as tinyint) as f_up_offer_decile,
     cast(null as tinyint) as f_up_full_decile,
     cast(null as tinyint) as f_up_uplift_decile,
     cast(null as tinyint) as f_up_resp_decile,

     b.f_re_offer_prob,
     b.f_re_full_prob,
     b.f_re_uplift_prob,
     b.f_re_resp_prob,
     cast(null as tinyint) as f_re_offer_decile,
     cast(null as tinyint) as f_re_full_decile,
     cast(null as tinyint) as f_re_uplift_decile,
     cast(null as tinyint) as f_re_resp_decile,

     b.sge_offer_prob,
     b.sge_full_prob,
     b.sge_uplift_prob,
     b.sge_resp_prob,
     cast(null as tinyint) as sge_offer_decile,
     cast(null as tinyint) as sge_full_decile,
     cast(null as tinyint) as sge_uplift_decile,
     cast(null as tinyint) as sge_resp_decile,

     b.prob_BT_VIEWER,
     cast(null as tinyint) as bt_View_decile

into --drop table
        planning_201706
from dm_em_base_201706 a
 inner join mms_2017_06 b
  on a.account_number=b.account_number;

Update planning_201706
Set Modeled_channel_Preference = case
when Channel in( 'EM Only     ','EM, OBTM    ')                                         then 'EM'
when Channel in( 'DM, EM      ','DM, EM, OBTM') and Email_Open_Propensity = 'High/Med'  then 'EM'
when Channel in( 'DM, EM      ','DM, EM, OBTM') and Email_Open_Propensity = 'Low'       then 'DM'
when Channel in( 'DM Only','DM, OBTM    ')                                              then 'DM'
End;

CREATE VARIABLE multiplier bigint; --Has to be a bigint if you are dealing with millions of records.
SET multiplier = DATEPART(millisecond,now())+1; -- pretty random number between 1 and 1000

UPDATE planning_201706
SET rand_num = rand(number(*)*multiplier); --The Number(*) function just gives a sequential number.
  --6403684 Row(s) affected


--===============================================================================
--===============================================================================
--===============================================================================


-- 1. Movies

If Object_ID('#scored2') is not NUll then DROP TABLE #scored2 end if;
Select *
into #scored2
from planning_201706;
--Where (movies=0 or movies=null)
 -- and (sports=0 or sports=null);  -- CHANGE

SELECT @TCount = COUNT(account_number) FROM #scored2;
SELECT @TCount;

If Object_ID('#scored3') is not NUll then DROP TABLE #scored3 end if;
Select *
  ,ROW_NUMBER() OVER(ORDER BY prob_fp_movies desc)      AS Rank_fp      -- CHANGE
  ,ROW_NUMBER() OVER(ORDER BY prob_Op_movies desc)      AS Rank_Op      -- CHANGE
  ,ROW_NUMBER() OVER(ORDER BY prob_Uplift_movies desc)  AS Rank_Uplift  -- CHANGE
  ,ROW_NUMBER() OVER(ORDER BY prob_resp_movies desc)    AS Rank_resp    -- CHANGE
into #scored3
from #scored2;

If Object_ID('#scored4') is not NUll then DROP TABLE #scored4 end if;
Select *
  ,case when Rank_fp between 0                   and (1 * (@TCount /10)) then 1
        When Rank_fp Between (1 * (@TCount /10)) and (2 * (@TCount /10)) THEN 2
        When Rank_fp Between (2 * (@TCount /10)) and (3 * (@TCount /10)) THEN 3
        When Rank_fp Between (3 * (@TCount /10)) and (4 * (@TCount /10)) THEN 4
        When Rank_fp Between (4 * (@TCount /10)) and (5 * (@TCount /10)) THEN 5
        When Rank_fp Between (5 * (@TCount /10)) and (6 * (@TCount /10)) THEN 6
        When Rank_fp Between (6 * (@TCount /10)) and (7 * (@TCount /10)) THEN 7
        When Rank_fp Between (7 * (@TCount /10)) and (8 * (@TCount /10)) THEN 8
        When Rank_fp Between (8 * (@TCount /10)) and (9 * (@TCount /10)) THEN 9
        When Rank_fp Between (9 * (@TCount /10)) and (10* (@TCount /10)) THEN 10
   end as Decile_FP

  ,case when Rank_OP between 0                   and (1 * (@TCount /10)) then 1
        When Rank_OP Between (1 * (@TCount /10)) and (2 * (@TCount /10)) THEN 2
        When Rank_OP Between (2 * (@TCount /10)) and (3 * (@TCount /10)) THEN 3
        When Rank_OP Between (3 * (@TCount /10)) and (4 * (@TCount /10)) THEN 4
        When Rank_OP Between (4 * (@TCount /10)) and (5 * (@TCount /10)) THEN 5
        When Rank_OP Between (5 * (@TCount /10)) and (6 * (@TCount /10)) THEN 6
        When Rank_OP Between (6 * (@TCount /10)) and (7 * (@TCount /10)) THEN 7
        When Rank_OP Between (7 * (@TCount /10)) and (8 * (@TCount /10)) THEN 8
        When Rank_OP Between (8 * (@TCount /10)) and (9 * (@TCount /10)) THEN 9
        When Rank_OP Between (9 * (@TCount /10)) and (10* (@TCount /10)) THEN 10
   end as Decile_OP

  ,case when Rank_Uplift between 0                   and (1 * (@TCount /10)) then 1
        When Rank_Uplift Between (1 * (@TCount /10)) and (2 * (@TCount /10)) THEN 2
        When Rank_Uplift Between (2 * (@TCount /10)) and (3 * (@TCount /10)) THEN 3
        When Rank_Uplift Between (3 * (@TCount /10)) and (4 * (@TCount /10)) THEN 4
        When Rank_Uplift Between (4 * (@TCount /10)) and (5 * (@TCount /10)) THEN 5
        When Rank_Uplift Between (5 * (@TCount /10)) and (6 * (@TCount /10)) THEN 6
        When Rank_Uplift Between (6 * (@TCount /10)) and (7 * (@TCount /10)) THEN 7
        When Rank_Uplift Between (7 * (@TCount /10)) and (8 * (@TCount /10)) THEN 8
        When Rank_Uplift Between (8 * (@TCount /10)) and (9 * (@TCount /10)) THEN 9
        When Rank_Uplift Between (9 * (@TCount /10)) and (10* (@TCount /10)) THEN 10
   end as Decile_Uplift

  ,case when rank_resp between 0                   and (1 * (@TCount /10)) then 1
        When rank_resp Between (1 * (@TCount /10)) and (2 * (@TCount /10)) THEN 2
        When rank_resp Between (2 * (@TCount /10)) and (3 * (@TCount /10)) THEN 3
        When rank_resp Between (3 * (@TCount /10)) and (4 * (@TCount /10)) THEN 4
        When rank_resp Between (4 * (@TCount /10)) and (5 * (@TCount /10)) THEN 5
        When rank_resp Between (5 * (@TCount /10)) and (6 * (@TCount /10)) THEN 6
        When rank_resp Between (6 * (@TCount /10)) and (7 * (@TCount /10)) THEN 7
        When rank_resp Between (7 * (@TCount /10)) and (8 * (@TCount /10)) THEN 8
        When rank_resp Between (8 * (@TCount /10)) and (9 * (@TCount /10)) THEN 9
        When rank_resp Between (9 * (@TCount /10)) and (10* (@TCount /10)) THEN 10
   end as Decile_resp

into #scored4
from #scored3;

update planning_201706 a
 set a.decile_FP_Movies         = b.decile_fp,          -- CHANGE
     a.decile_OP_Movies         = b.decile_op,          -- CHANGE
     a.decile_Uplift_Movies     = b.decile_uplift,      -- CHANGE
     a.decile_resp_Movies       = b.decile_resp         -- CHANGE
 from #scored4 b
   where a.account_number=b.account_number;

drop table #scored2;
drop table #scored3;
drop table #scored4;

------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------


-- 2. SPORTS
Select *
into #scored2
from planning_201706;
--Where (movies=0 or movies=null)
--  and (sports=0 or sports=null); -- CHANGE

SELECT @TCount = COUNT(account_number) FROM #scored2;
SELECT @TCount;

If Object_ID('#scored3') is not NUll then DROP TABLE #scored3 end if;
Select *
  ,ROW_NUMBER() OVER(ORDER BY prob_fp_sports desc)      AS Rank_fp      -- CHANGE
  ,ROW_NUMBER() OVER(ORDER BY prob_Op_sports desc)      AS Rank_Op      -- CHANGE
  ,ROW_NUMBER() OVER(ORDER BY prob_Uplift_sports desc)  AS Rank_Uplift  -- CHANGE
  ,ROW_NUMBER() OVER(ORDER BY prob_resp_sports desc)    AS Rank_resp    -- CHANGE
into #scored3
from #scored2;

If Object_ID('#scored4') is not NUll then DROP TABLE #scored4 end if;
Select *
  ,case when Rank_fp between 0                   and (1 * (@TCount /10)) then 1
        When Rank_fp Between (1 * (@TCount /10)) and (2 * (@TCount /10)) THEN 2
        When Rank_fp Between (2 * (@TCount /10)) and (3 * (@TCount /10)) THEN 3
        When Rank_fp Between (3 * (@TCount /10)) and (4 * (@TCount /10)) THEN 4
        When Rank_fp Between (4 * (@TCount /10)) and (5 * (@TCount /10)) THEN 5
        When Rank_fp Between (5 * (@TCount /10)) and (6 * (@TCount /10)) THEN 6
        When Rank_fp Between (6 * (@TCount /10)) and (7 * (@TCount /10)) THEN 7
        When Rank_fp Between (7 * (@TCount /10)) and (8 * (@TCount /10)) THEN 8
        When Rank_fp Between (8 * (@TCount /10)) and (9 * (@TCount /10)) THEN 9
        When Rank_fp Between (9 * (@TCount /10)) and (10* (@TCount /10)) THEN 10
   end as Decile_FP

  ,case when Rank_OP between 0                   and (1 * (@TCount /10)) then 1
        When Rank_OP Between (1 * (@TCount /10)) and (2 * (@TCount /10)) THEN 2
        When Rank_OP Between (2 * (@TCount /10)) and (3 * (@TCount /10)) THEN 3
        When Rank_OP Between (3 * (@TCount /10)) and (4 * (@TCount /10)) THEN 4
        When Rank_OP Between (4 * (@TCount /10)) and (5 * (@TCount /10)) THEN 5
        When Rank_OP Between (5 * (@TCount /10)) and (6 * (@TCount /10)) THEN 6
        When Rank_OP Between (6 * (@TCount /10)) and (7 * (@TCount /10)) THEN 7
        When Rank_OP Between (7 * (@TCount /10)) and (8 * (@TCount /10)) THEN 8
        When Rank_OP Between (8 * (@TCount /10)) and (9 * (@TCount /10)) THEN 9
        When Rank_OP Between (9 * (@TCount /10)) and (10* (@TCount /10)) THEN 10
   end as Decile_OP

  ,case when Rank_Uplift between 0                   and (1 * (@TCount /10)) then 1
        When Rank_Uplift Between (1 * (@TCount /10)) and (2 * (@TCount /10)) THEN 2
        When Rank_Uplift Between (2 * (@TCount /10)) and (3 * (@TCount /10)) THEN 3
        When Rank_Uplift Between (3 * (@TCount /10)) and (4 * (@TCount /10)) THEN 4
        When Rank_Uplift Between (4 * (@TCount /10)) and (5 * (@TCount /10)) THEN 5
        When Rank_Uplift Between (5 * (@TCount /10)) and (6 * (@TCount /10)) THEN 6
        When Rank_Uplift Between (6 * (@TCount /10)) and (7 * (@TCount /10)) THEN 7
        When Rank_Uplift Between (7 * (@TCount /10)) and (8 * (@TCount /10)) THEN 8
        When Rank_Uplift Between (8 * (@TCount /10)) and (9 * (@TCount /10)) THEN 9
        When Rank_Uplift Between (9 * (@TCount /10)) and (10* (@TCount /10)) THEN 10
   end as Decile_Uplift

  ,case when rank_resp between 0                   and (1 * (@TCount /10)) then 1
        When rank_resp Between (1 * (@TCount /10)) and (2 * (@TCount /10)) THEN 2
        When rank_resp Between (2 * (@TCount /10)) and (3 * (@TCount /10)) THEN 3
        When rank_resp Between (3 * (@TCount /10)) and (4 * (@TCount /10)) THEN 4
        When rank_resp Between (4 * (@TCount /10)) and (5 * (@TCount /10)) THEN 5
        When rank_resp Between (5 * (@TCount /10)) and (6 * (@TCount /10)) THEN 6
        When rank_resp Between (6 * (@TCount /10)) and (7 * (@TCount /10)) THEN 7
        When rank_resp Between (7 * (@TCount /10)) and (8 * (@TCount /10)) THEN 8
        When rank_resp Between (8 * (@TCount /10)) and (9 * (@TCount /10)) THEN 9
        When rank_resp Between (9 * (@TCount /10)) and (10* (@TCount /10)) THEN 10
   end as Decile_resp

into #scored4
from #scored3;

update planning_201706 a
 set a.decile_FP_sports         = b.decile_fp,          -- CHANGE
     a.decile_OP_sports         = b.decile_op,          -- CHANGE
     a.decile_Uplift_sports     = b.decile_uplift,      -- CHANGE
     a.decile_resp_sports       = b.decile_resp         -- CHANGE
 from #scored4 b
   where a.account_number=b.account_number;

drop table #scored2;
drop table #scored3;
drop table #scored4;

-- 3. TT (BASIC -> TT)
Select *
into #scored2
from planning_201706;
--Where (movies=0 or movies=null)
 -- and (sports=0 or sports=null);-- CHANGE

SELECT @TCount = COUNT(account_number) FROM #scored2;
SELECT @TCount;

If Object_ID('#scored3') is not NUll then DROP TABLE #scored3 end if;
Select *
  ,ROW_NUMBER() OVER(ORDER BY prob_fp_basic_to_TT desc)      AS Rank_fp      -- CHANGE
  ,ROW_NUMBER() OVER(ORDER BY prob_Op_basic_to_TT desc)      AS Rank_Op      -- CHANGE
  ,ROW_NUMBER() OVER(ORDER BY prob_Uplift_basic_to_TT desc)  AS Rank_Uplift  -- CHANGE
  ,ROW_NUMBER() OVER(ORDER BY prob_resp_basic_to_TT desc)    AS Rank_resp    -- CHANGE
into #scored3
from #scored2;

If Object_ID('#scored4') is not NUll then DROP TABLE #scored4 end if;
Select *
  ,case when Rank_fp between 0                   and (1 * (@TCount /10)) then 1
        When Rank_fp Between (1 * (@TCount /10)) and (2 * (@TCount /10)) THEN 2
        When Rank_fp Between (2 * (@TCount /10)) and (3 * (@TCount /10)) THEN 3
        When Rank_fp Between (3 * (@TCount /10)) and (4 * (@TCount /10)) THEN 4
        When Rank_fp Between (4 * (@TCount /10)) and (5 * (@TCount /10)) THEN 5
        When Rank_fp Between (5 * (@TCount /10)) and (6 * (@TCount /10)) THEN 6
        When Rank_fp Between (6 * (@TCount /10)) and (7 * (@TCount /10)) THEN 7
        When Rank_fp Between (7 * (@TCount /10)) and (8 * (@TCount /10)) THEN 8
        When Rank_fp Between (8 * (@TCount /10)) and (9 * (@TCount /10)) THEN 9
        When Rank_fp Between (9 * (@TCount /10)) and (10* (@TCount /10)) THEN 10
   end as Decile_FP

  ,case when Rank_OP between 0                   and (1 * (@TCount /10)) then 1
        When Rank_OP Between (1 * (@TCount /10)) and (2 * (@TCount /10)) THEN 2
        When Rank_OP Between (2 * (@TCount /10)) and (3 * (@TCount /10)) THEN 3
        When Rank_OP Between (3 * (@TCount /10)) and (4 * (@TCount /10)) THEN 4
        When Rank_OP Between (4 * (@TCount /10)) and (5 * (@TCount /10)) THEN 5
        When Rank_OP Between (5 * (@TCount /10)) and (6 * (@TCount /10)) THEN 6
        When Rank_OP Between (6 * (@TCount /10)) and (7 * (@TCount /10)) THEN 7
        When Rank_OP Between (7 * (@TCount /10)) and (8 * (@TCount /10)) THEN 8
        When Rank_OP Between (8 * (@TCount /10)) and (9 * (@TCount /10)) THEN 9
        When Rank_OP Between (9 * (@TCount /10)) and (10* (@TCount /10)) THEN 10
   end as Decile_OP

  ,case when Rank_Uplift between 0                   and (1 * (@TCount /10)) then 1
        When Rank_Uplift Between (1 * (@TCount /10)) and (2 * (@TCount /10)) THEN 2
        When Rank_Uplift Between (2 * (@TCount /10)) and (3 * (@TCount /10)) THEN 3
        When Rank_Uplift Between (3 * (@TCount /10)) and (4 * (@TCount /10)) THEN 4
        When Rank_Uplift Between (4 * (@TCount /10)) and (5 * (@TCount /10)) THEN 5
        When Rank_Uplift Between (5 * (@TCount /10)) and (6 * (@TCount /10)) THEN 6
        When Rank_Uplift Between (6 * (@TCount /10)) and (7 * (@TCount /10)) THEN 7
        When Rank_Uplift Between (7 * (@TCount /10)) and (8 * (@TCount /10)) THEN 8
        When Rank_Uplift Between (8 * (@TCount /10)) and (9 * (@TCount /10)) THEN 9
        When Rank_Uplift Between (9 * (@TCount /10)) and (10* (@TCount /10)) THEN 10
   end as Decile_Uplift

  ,case when rank_resp between 0                   and (1 * (@TCount /10)) then 1
        When rank_resp Between (1 * (@TCount /10)) and (2 * (@TCount /10)) THEN 2
        When rank_resp Between (2 * (@TCount /10)) and (3 * (@TCount /10)) THEN 3
        When rank_resp Between (3 * (@TCount /10)) and (4 * (@TCount /10)) THEN 4
        When rank_resp Between (4 * (@TCount /10)) and (5 * (@TCount /10)) THEN 5
        When rank_resp Between (5 * (@TCount /10)) and (6 * (@TCount /10)) THEN 6
        When rank_resp Between (6 * (@TCount /10)) and (7 * (@TCount /10)) THEN 7
        When rank_resp Between (7 * (@TCount /10)) and (8 * (@TCount /10)) THEN 8
        When rank_resp Between (8 * (@TCount /10)) and (9 * (@TCount /10)) THEN 9
        When rank_resp Between (9 * (@TCount /10)) and (10* (@TCount /10)) THEN 10
   end as Decile_resp

into #scored4
from #scored3;

update planning_201706 a
 set a.decile_FP_basic_to_TT         = b.decile_fp,          -- CHANGE
     a.decile_OP_basic_to_TT         = b.decile_op,          -- CHANGE
     a.decile_Uplift_basic_to_TT     = b.decile_uplift,      -- CHANGE
     a.decile_resp_basic_to_TT      = b.decile_resp         -- CHANGE
 from #scored4 b
   where a.account_number=b.account_number;

drop table #scored2;
drop table #scored3;
drop table #scored4;

-- 4. TT (DS -> TT)
Select *
into #scored2
from planning_201706;
--Where (movies=0 or movies=null)
  --and (sports=2);-- CHANGE

SELECT @TCount = COUNT(account_number) FROM #scored2;
SELECT @TCount;

If Object_ID('#scored3') is not NUll then DROP TABLE #scored3 end if;
Select *
  ,ROW_NUMBER() OVER(ORDER BY prob_fp_DS_to_TT desc)      AS Rank_fp      -- CHANGE
  ,ROW_NUMBER() OVER(ORDER BY prob_Op_DS_to_TT desc)      AS Rank_Op      -- CHANGE
  ,ROW_NUMBER() OVER(ORDER BY prob_Uplift_DS_to_TT desc)  AS Rank_Uplift  -- CHANGE
  ,ROW_NUMBER() OVER(ORDER BY prob_resp_DS_to_TT desc)    AS Rank_resp    -- CHANGE
into #scored3
from #scored2;

If Object_ID('#scored4') is not NUll then DROP TABLE #scored4 end if;
Select *
  ,case when Rank_fp between 0                   and (1 * (@TCount /10)) then 1
        When Rank_fp Between (1 * (@TCount /10)) and (2 * (@TCount /10)) THEN 2
        When Rank_fp Between (2 * (@TCount /10)) and (3 * (@TCount /10)) THEN 3
        When Rank_fp Between (3 * (@TCount /10)) and (4 * (@TCount /10)) THEN 4
        When Rank_fp Between (4 * (@TCount /10)) and (5 * (@TCount /10)) THEN 5
        When Rank_fp Between (5 * (@TCount /10)) and (6 * (@TCount /10)) THEN 6
        When Rank_fp Between (6 * (@TCount /10)) and (7 * (@TCount /10)) THEN 7
        When Rank_fp Between (7 * (@TCount /10)) and (8 * (@TCount /10)) THEN 8
        When Rank_fp Between (8 * (@TCount /10)) and (9 * (@TCount /10)) THEN 9
        When Rank_fp Between (9 * (@TCount /10)) and (10* (@TCount /10)) THEN 10
   end as Decile_FP

  ,case when Rank_OP between 0                   and (1 * (@TCount /10)) then 1
        When Rank_OP Between (1 * (@TCount /10)) and (2 * (@TCount /10)) THEN 2
        When Rank_OP Between (2 * (@TCount /10)) and (3 * (@TCount /10)) THEN 3
        When Rank_OP Between (3 * (@TCount /10)) and (4 * (@TCount /10)) THEN 4
        When Rank_OP Between (4 * (@TCount /10)) and (5 * (@TCount /10)) THEN 5
        When Rank_OP Between (5 * (@TCount /10)) and (6 * (@TCount /10)) THEN 6
        When Rank_OP Between (6 * (@TCount /10)) and (7 * (@TCount /10)) THEN 7
        When Rank_OP Between (7 * (@TCount /10)) and (8 * (@TCount /10)) THEN 8
        When Rank_OP Between (8 * (@TCount /10)) and (9 * (@TCount /10)) THEN 9
        When Rank_OP Between (9 * (@TCount /10)) and (10* (@TCount /10)) THEN 10
   end as Decile_OP

  ,case when Rank_Uplift between 0                   and (1 * (@TCount /10)) then 1
        When Rank_Uplift Between (1 * (@TCount /10)) and (2 * (@TCount /10)) THEN 2
        When Rank_Uplift Between (2 * (@TCount /10)) and (3 * (@TCount /10)) THEN 3
        When Rank_Uplift Between (3 * (@TCount /10)) and (4 * (@TCount /10)) THEN 4
        When Rank_Uplift Between (4 * (@TCount /10)) and (5 * (@TCount /10)) THEN 5
        When Rank_Uplift Between (5 * (@TCount /10)) and (6 * (@TCount /10)) THEN 6
        When Rank_Uplift Between (6 * (@TCount /10)) and (7 * (@TCount /10)) THEN 7
        When Rank_Uplift Between (7 * (@TCount /10)) and (8 * (@TCount /10)) THEN 8
        When Rank_Uplift Between (8 * (@TCount /10)) and (9 * (@TCount /10)) THEN 9
        When Rank_Uplift Between (9 * (@TCount /10)) and (10* (@TCount /10)) THEN 10
   end as Decile_Uplift

  ,case when rank_resp between 0                   and (1 * (@TCount /10)) then 1
        When rank_resp Between (1 * (@TCount /10)) and (2 * (@TCount /10)) THEN 2
        When rank_resp Between (2 * (@TCount /10)) and (3 * (@TCount /10)) THEN 3
        When rank_resp Between (3 * (@TCount /10)) and (4 * (@TCount /10)) THEN 4
        When rank_resp Between (4 * (@TCount /10)) and (5 * (@TCount /10)) THEN 5
        When rank_resp Between (5 * (@TCount /10)) and (6 * (@TCount /10)) THEN 6
        When rank_resp Between (6 * (@TCount /10)) and (7 * (@TCount /10)) THEN 7
        When rank_resp Between (7 * (@TCount /10)) and (8 * (@TCount /10)) THEN 8
        When rank_resp Between (8 * (@TCount /10)) and (9 * (@TCount /10)) THEN 9
        When rank_resp Between (9 * (@TCount /10)) and (10* (@TCount /10)) THEN 10
   end as Decile_resp

into #scored4
from #scored3;

update planning_201706 a
 set a.decile_FP_DS_to_TT         = b.decile_fp,          -- CHANGE
     a.decile_OP_DS_to_TT         = b.decile_op,          -- CHANGE
     a.decile_Uplift_DS_to_TT     = b.decile_uplift,      -- CHANGE
     a.decile_resp_DS_to_TT      = b.decile_resp         -- CHANGE
 from #scored4 b
   where a.account_number=b.account_number;

drop table #scored2;
drop table #scored3;
drop table #scored4;

-- 5. TT (DM -> TT)
Select *
into #scored2
from planning_201706;
--Where (sports=0 or sports=null)
  --and (movies=2);-- CHANGE

SELECT @TCount = COUNT(account_number) FROM #scored2;
SELECT @TCount;

If Object_ID('#scored3') is not NUll then DROP TABLE #scored3 end if;
Select *
  ,ROW_NUMBER() OVER(ORDER BY prob_fp_DM_to_TT desc)      AS Rank_fp      -- CHANGE
  ,ROW_NUMBER() OVER(ORDER BY prob_Op_DM_to_TT desc)      AS Rank_Op      -- CHANGE
  ,ROW_NUMBER() OVER(ORDER BY prob_Uplift_DM_to_TT desc)  AS Rank_Uplift  -- CHANGE
  ,ROW_NUMBER() OVER(ORDER BY prob_resp_DM_to_TT desc)    AS Rank_resp    -- CHANGE
into #scored3
from #scored2;

If Object_ID('#scored4') is not NUll then DROP TABLE #scored4 end if;
Select *
  ,case when Rank_fp between 0                   and (1 * (@TCount /10)) then 1
        When Rank_fp Between (1 * (@TCount /10)) and (2 * (@TCount /10)) THEN 2
        When Rank_fp Between (2 * (@TCount /10)) and (3 * (@TCount /10)) THEN 3
        When Rank_fp Between (3 * (@TCount /10)) and (4 * (@TCount /10)) THEN 4
        When Rank_fp Between (4 * (@TCount /10)) and (5 * (@TCount /10)) THEN 5
        When Rank_fp Between (5 * (@TCount /10)) and (6 * (@TCount /10)) THEN 6
        When Rank_fp Between (6 * (@TCount /10)) and (7 * (@TCount /10)) THEN 7
        When Rank_fp Between (7 * (@TCount /10)) and (8 * (@TCount /10)) THEN 8
        When Rank_fp Between (8 * (@TCount /10)) and (9 * (@TCount /10)) THEN 9
        When Rank_fp Between (9 * (@TCount /10)) and (10* (@TCount /10)) THEN 10
   end as Decile_FP

  ,case when Rank_OP between 0                   and (1 * (@TCount /10)) then 1
        When Rank_OP Between (1 * (@TCount /10)) and (2 * (@TCount /10)) THEN 2
        When Rank_OP Between (2 * (@TCount /10)) and (3 * (@TCount /10)) THEN 3
        When Rank_OP Between (3 * (@TCount /10)) and (4 * (@TCount /10)) THEN 4
        When Rank_OP Between (4 * (@TCount /10)) and (5 * (@TCount /10)) THEN 5
        When Rank_OP Between (5 * (@TCount /10)) and (6 * (@TCount /10)) THEN 6
        When Rank_OP Between (6 * (@TCount /10)) and (7 * (@TCount /10)) THEN 7
        When Rank_OP Between (7 * (@TCount /10)) and (8 * (@TCount /10)) THEN 8
        When Rank_OP Between (8 * (@TCount /10)) and (9 * (@TCount /10)) THEN 9
        When Rank_OP Between (9 * (@TCount /10)) and (10* (@TCount /10)) THEN 10
   end as Decile_OP

  ,case when Rank_Uplift between 0                   and (1 * (@TCount /10)) then 1
        When Rank_Uplift Between (1 * (@TCount /10)) and (2 * (@TCount /10)) THEN 2
        When Rank_Uplift Between (2 * (@TCount /10)) and (3 * (@TCount /10)) THEN 3
        When Rank_Uplift Between (3 * (@TCount /10)) and (4 * (@TCount /10)) THEN 4
        When Rank_Uplift Between (4 * (@TCount /10)) and (5 * (@TCount /10)) THEN 5
        When Rank_Uplift Between (5 * (@TCount /10)) and (6 * (@TCount /10)) THEN 6
        When Rank_Uplift Between (6 * (@TCount /10)) and (7 * (@TCount /10)) THEN 7
        When Rank_Uplift Between (7 * (@TCount /10)) and (8 * (@TCount /10)) THEN 8
        When Rank_Uplift Between (8 * (@TCount /10)) and (9 * (@TCount /10)) THEN 9
        When Rank_Uplift Between (9 * (@TCount /10)) and (10* (@TCount /10)) THEN 10
   end as Decile_Uplift

  ,case when rank_resp between 0                   and (1 * (@TCount /10)) then 1
        When rank_resp Between (1 * (@TCount /10)) and (2 * (@TCount /10)) THEN 2
        When rank_resp Between (2 * (@TCount /10)) and (3 * (@TCount /10)) THEN 3
        When rank_resp Between (3 * (@TCount /10)) and (4 * (@TCount /10)) THEN 4
        When rank_resp Between (4 * (@TCount /10)) and (5 * (@TCount /10)) THEN 5
        When rank_resp Between (5 * (@TCount /10)) and (6 * (@TCount /10)) THEN 6
        When rank_resp Between (6 * (@TCount /10)) and (7 * (@TCount /10)) THEN 7
        When rank_resp Between (7 * (@TCount /10)) and (8 * (@TCount /10)) THEN 8
        When rank_resp Between (8 * (@TCount /10)) and (9 * (@TCount /10)) THEN 9
        When rank_resp Between (9 * (@TCount /10)) and (10* (@TCount /10)) THEN 10
   end as Decile_resp

into #scored4
from #scored3;

update planning_201706 a
 set a.decile_FP_DM_to_TT         = b.decile_fp,          -- CHANGE
     a.decile_OP_DM_to_TT         = b.decile_op,          -- CHANGE
     a.decile_Uplift_DM_to_TT     = b.decile_uplift,      -- CHANGE
     a.decile_resp_DM_to_TT      = b.decile_resp         -- CHANGE
 from #scored4 b
   where a.account_number=b.account_number;

drop table #scored2;
drop table #scored3;
drop table #scored4;

-- 6. BBU
Select *
into #scored2
from planning_201706;
--Where bb_eligible=1;-- CHANGE

SELECT @TCount = COUNT(account_number) FROM #scored2;
SELECT @TCount;

If Object_ID('#scored3') is not NUll then DROP TABLE #scored3 end if;
Select *
  ,ROW_NUMBER() OVER(ORDER BY bb_full_prob desc)      AS Rank_fp       -- CHANGE
  ,ROW_NUMBER() OVER(ORDER BY bb_offer_prob desc)      AS Rank_Op      -- CHANGE
  ,ROW_NUMBER() OVER(ORDER BY bb_uplift_prob desc)  AS Rank_Uplift     -- CHANGE
  ,ROW_NUMBER() OVER(ORDER BY bb_resp_prob desc)    AS Rank_resp       -- CHANGE
into #scored3
from #scored2;

If Object_ID('#scored4') is not NUll then DROP TABLE #scored4 end if;
Select *
  ,case when Rank_fp between 0                   and (1 * (@TCount /10)) then 1
        When Rank_fp Between (1 * (@TCount /10)) and (2 * (@TCount /10)) THEN 2
        When Rank_fp Between (2 * (@TCount /10)) and (3 * (@TCount /10)) THEN 3
        When Rank_fp Between (3 * (@TCount /10)) and (4 * (@TCount /10)) THEN 4
        When Rank_fp Between (4 * (@TCount /10)) and (5 * (@TCount /10)) THEN 5
        When Rank_fp Between (5 * (@TCount /10)) and (6 * (@TCount /10)) THEN 6
        When Rank_fp Between (6 * (@TCount /10)) and (7 * (@TCount /10)) THEN 7
        When Rank_fp Between (7 * (@TCount /10)) and (8 * (@TCount /10)) THEN 8
        When Rank_fp Between (8 * (@TCount /10)) and (9 * (@TCount /10)) THEN 9
        When Rank_fp Between (9 * (@TCount /10)) and (10* (@TCount /10)) THEN 10
   end as Decile_FP

  ,case when Rank_OP between 0                   and (1 * (@TCount /10)) then 1
        When Rank_OP Between (1 * (@TCount /10)) and (2 * (@TCount /10)) THEN 2
        When Rank_OP Between (2 * (@TCount /10)) and (3 * (@TCount /10)) THEN 3
        When Rank_OP Between (3 * (@TCount /10)) and (4 * (@TCount /10)) THEN 4
        When Rank_OP Between (4 * (@TCount /10)) and (5 * (@TCount /10)) THEN 5
        When Rank_OP Between (5 * (@TCount /10)) and (6 * (@TCount /10)) THEN 6
        When Rank_OP Between (6 * (@TCount /10)) and (7 * (@TCount /10)) THEN 7
        When Rank_OP Between (7 * (@TCount /10)) and (8 * (@TCount /10)) THEN 8
        When Rank_OP Between (8 * (@TCount /10)) and (9 * (@TCount /10)) THEN 9
        When Rank_OP Between (9 * (@TCount /10)) and (10* (@TCount /10)) THEN 10
   end as Decile_OP

  ,case when Rank_Uplift between 0                   and (1 * (@TCount /10)) then 1
        When Rank_Uplift Between (1 * (@TCount /10)) and (2 * (@TCount /10)) THEN 2
        When Rank_Uplift Between (2 * (@TCount /10)) and (3 * (@TCount /10)) THEN 3
        When Rank_Uplift Between (3 * (@TCount /10)) and (4 * (@TCount /10)) THEN 4
        When Rank_Uplift Between (4 * (@TCount /10)) and (5 * (@TCount /10)) THEN 5
        When Rank_Uplift Between (5 * (@TCount /10)) and (6 * (@TCount /10)) THEN 6
        When Rank_Uplift Between (6 * (@TCount /10)) and (7 * (@TCount /10)) THEN 7
        When Rank_Uplift Between (7 * (@TCount /10)) and (8 * (@TCount /10)) THEN 8
        When Rank_Uplift Between (8 * (@TCount /10)) and (9 * (@TCount /10)) THEN 9
        When Rank_Uplift Between (9 * (@TCount /10)) and (10* (@TCount /10)) THEN 10
   end as Decile_Uplift

  ,case when rank_resp between 0                   and (1 * (@TCount /10)) then 1
        When rank_resp Between (1 * (@TCount /10)) and (2 * (@TCount /10)) THEN 2
        When rank_resp Between (2 * (@TCount /10)) and (3 * (@TCount /10)) THEN 3
        When rank_resp Between (3 * (@TCount /10)) and (4 * (@TCount /10)) THEN 4
        When rank_resp Between (4 * (@TCount /10)) and (5 * (@TCount /10)) THEN 5
        When rank_resp Between (5 * (@TCount /10)) and (6 * (@TCount /10)) THEN 6
        When rank_resp Between (6 * (@TCount /10)) and (7 * (@TCount /10)) THEN 7
        When rank_resp Between (7 * (@TCount /10)) and (8 * (@TCount /10)) THEN 8
        When rank_resp Between (8 * (@TCount /10)) and (9 * (@TCount /10)) THEN 9
        When rank_resp Between (9 * (@TCount /10)) and (10* (@TCount /10)) THEN 10
   end as Decile_resp

into #scored4
from #scored3;

update planning_201706 a
 set a.bb_full_decile          = b.decile_fp,          -- CHANGE
     a.bb_offer_decile         = b.decile_op,          -- CHANGE
     a.bb_uplift_decile        = b.decile_uplift,      -- CHANGE
     a.bb_resp_decile          = b.decile_resp         -- CHANGE
 from #scored4 b
   where a.account_number=b.account_number;

drop table #scored2;
drop table #scored3;
drop table #scored4;

-- 7. FIBRE UPGRADE
Select *
into #scored2
from planning_201706;
--Where f_up_eligible=1;-- CHANGE

SELECT @TCount = COUNT(account_number) FROM #scored2;
SELECT @TCount;

If Object_ID('#scored3') is not NUll then DROP TABLE #scored3 end if;
Select *
  ,ROW_NUMBER() OVER(ORDER BY f_up_full_prob desc)      AS Rank_fp       -- CHANGE
  ,ROW_NUMBER() OVER(ORDER BY f_up_offer_prob desc)      AS Rank_Op      -- CHANGE
  ,ROW_NUMBER() OVER(ORDER BY f_up_uplift_prob desc)  AS Rank_Uplift     -- CHANGE
  ,ROW_NUMBER() OVER(ORDER BY f_up_resp_prob desc)    AS Rank_resp       -- CHANGE
into #scored3
from #scored2;

If Object_ID('#scored4') is not NUll then DROP TABLE #scored4 end if;
Select *
  ,case when Rank_fp between 0                   and (1 * (@TCount /10)) then 1
        When Rank_fp Between (1 * (@TCount /10)) and (2 * (@TCount /10)) THEN 2
        When Rank_fp Between (2 * (@TCount /10)) and (3 * (@TCount /10)) THEN 3
        When Rank_fp Between (3 * (@TCount /10)) and (4 * (@TCount /10)) THEN 4
        When Rank_fp Between (4 * (@TCount /10)) and (5 * (@TCount /10)) THEN 5
        When Rank_fp Between (5 * (@TCount /10)) and (6 * (@TCount /10)) THEN 6
        When Rank_fp Between (6 * (@TCount /10)) and (7 * (@TCount /10)) THEN 7
        When Rank_fp Between (7 * (@TCount /10)) and (8 * (@TCount /10)) THEN 8
        When Rank_fp Between (8 * (@TCount /10)) and (9 * (@TCount /10)) THEN 9
        When Rank_fp Between (9 * (@TCount /10)) and (10* (@TCount /10)) THEN 10
   end as Decile_FP

  ,case when Rank_OP between 0                   and (1 * (@TCount /10)) then 1
        When Rank_OP Between (1 * (@TCount /10)) and (2 * (@TCount /10)) THEN 2
        When Rank_OP Between (2 * (@TCount /10)) and (3 * (@TCount /10)) THEN 3
        When Rank_OP Between (3 * (@TCount /10)) and (4 * (@TCount /10)) THEN 4
        When Rank_OP Between (4 * (@TCount /10)) and (5 * (@TCount /10)) THEN 5
        When Rank_OP Between (5 * (@TCount /10)) and (6 * (@TCount /10)) THEN 6
        When Rank_OP Between (6 * (@TCount /10)) and (7 * (@TCount /10)) THEN 7
        When Rank_OP Between (7 * (@TCount /10)) and (8 * (@TCount /10)) THEN 8
        When Rank_OP Between (8 * (@TCount /10)) and (9 * (@TCount /10)) THEN 9
        When Rank_OP Between (9 * (@TCount /10)) and (10* (@TCount /10)) THEN 10
   end as Decile_OP

  ,case when Rank_Uplift between 0                   and (1 * (@TCount /10)) then 1
        When Rank_Uplift Between (1 * (@TCount /10)) and (2 * (@TCount /10)) THEN 2
        When Rank_Uplift Between (2 * (@TCount /10)) and (3 * (@TCount /10)) THEN 3
        When Rank_Uplift Between (3 * (@TCount /10)) and (4 * (@TCount /10)) THEN 4
        When Rank_Uplift Between (4 * (@TCount /10)) and (5 * (@TCount /10)) THEN 5
        When Rank_Uplift Between (5 * (@TCount /10)) and (6 * (@TCount /10)) THEN 6
        When Rank_Uplift Between (6 * (@TCount /10)) and (7 * (@TCount /10)) THEN 7
        When Rank_Uplift Between (7 * (@TCount /10)) and (8 * (@TCount /10)) THEN 8
        When Rank_Uplift Between (8 * (@TCount /10)) and (9 * (@TCount /10)) THEN 9
        When Rank_Uplift Between (9 * (@TCount /10)) and (10* (@TCount /10)) THEN 10
   end as Decile_Uplift

  ,case when rank_resp between 0                   and (1 * (@TCount /10)) then 1
        When rank_resp Between (1 * (@TCount /10)) and (2 * (@TCount /10)) THEN 2
        When rank_resp Between (2 * (@TCount /10)) and (3 * (@TCount /10)) THEN 3
        When rank_resp Between (3 * (@TCount /10)) and (4 * (@TCount /10)) THEN 4
        When rank_resp Between (4 * (@TCount /10)) and (5 * (@TCount /10)) THEN 5
        When rank_resp Between (5 * (@TCount /10)) and (6 * (@TCount /10)) THEN 6
        When rank_resp Between (6 * (@TCount /10)) and (7 * (@TCount /10)) THEN 7
        When rank_resp Between (7 * (@TCount /10)) and (8 * (@TCount /10)) THEN 8
        When rank_resp Between (8 * (@TCount /10)) and (9 * (@TCount /10)) THEN 9
        When rank_resp Between (9 * (@TCount /10)) and (10* (@TCount /10)) THEN 10
   end as Decile_resp

into #scored4
from #scored3;

update planning_201706 a
 set a.f_up_full_decile          = b.decile_fp,          -- CHANGE
     a.f_up_offer_decile         = b.decile_op,          -- CHANGE
     a.f_up_uplift_decile        = b.decile_uplift,      -- CHANGE
     a.f_up_resp_decile          = b.decile_resp         -- CHANGE
 from #scored4 b
   where a.account_number=b.account_number;

drop table #scored2;
drop table #scored3;
drop table #scored4;

-- 8. FIBRE REGRADE
Select *
into #scored2
from planning_201706;
--Where f_re_eligible=1;-- CHANGE

SELECT @TCount = COUNT(account_number) FROM #scored2;
SELECT @TCount;

If Object_ID('#scored3') is not NUll then DROP TABLE #scored3 end if;
Select *
  ,ROW_NUMBER() OVER(ORDER BY f_re_full_prob desc)      AS Rank_fp       -- CHANGE
  ,ROW_NUMBER() OVER(ORDER BY f_re_offer_prob desc)      AS Rank_Op      -- CHANGE
  ,ROW_NUMBER() OVER(ORDER BY f_re_uplift_prob desc)  AS Rank_Uplift     -- CHANGE
  ,ROW_NUMBER() OVER(ORDER BY f_re_resp_prob desc)    AS Rank_resp       -- CHANGE
into #scored3
from #scored2;

If Object_ID('#scored4') is not NUll then DROP TABLE #scored4 end if;
Select *
  ,case when Rank_fp between 0                   and (1 * (@TCount /10)) then 1
        When Rank_fp Between (1 * (@TCount /10)) and (2 * (@TCount /10)) THEN 2
        When Rank_fp Between (2 * (@TCount /10)) and (3 * (@TCount /10)) THEN 3
        When Rank_fp Between (3 * (@TCount /10)) and (4 * (@TCount /10)) THEN 4
        When Rank_fp Between (4 * (@TCount /10)) and (5 * (@TCount /10)) THEN 5
        When Rank_fp Between (5 * (@TCount /10)) and (6 * (@TCount /10)) THEN 6
        When Rank_fp Between (6 * (@TCount /10)) and (7 * (@TCount /10)) THEN 7
        When Rank_fp Between (7 * (@TCount /10)) and (8 * (@TCount /10)) THEN 8
        When Rank_fp Between (8 * (@TCount /10)) and (9 * (@TCount /10)) THEN 9
        When Rank_fp Between (9 * (@TCount /10)) and (10* (@TCount /10)) THEN 10
   end as Decile_FP

  ,case when Rank_OP between 0                   and (1 * (@TCount /10)) then 1
        When Rank_OP Between (1 * (@TCount /10)) and (2 * (@TCount /10)) THEN 2
        When Rank_OP Between (2 * (@TCount /10)) and (3 * (@TCount /10)) THEN 3
        When Rank_OP Between (3 * (@TCount /10)) and (4 * (@TCount /10)) THEN 4
        When Rank_OP Between (4 * (@TCount /10)) and (5 * (@TCount /10)) THEN 5
        When Rank_OP Between (5 * (@TCount /10)) and (6 * (@TCount /10)) THEN 6
        When Rank_OP Between (6 * (@TCount /10)) and (7 * (@TCount /10)) THEN 7
        When Rank_OP Between (7 * (@TCount /10)) and (8 * (@TCount /10)) THEN 8
        When Rank_OP Between (8 * (@TCount /10)) and (9 * (@TCount /10)) THEN 9
        When Rank_OP Between (9 * (@TCount /10)) and (10* (@TCount /10)) THEN 10
   end as Decile_OP

  ,case when Rank_Uplift between 0                   and (1 * (@TCount /10)) then 1
        When Rank_Uplift Between (1 * (@TCount /10)) and (2 * (@TCount /10)) THEN 2
        When Rank_Uplift Between (2 * (@TCount /10)) and (3 * (@TCount /10)) THEN 3
        When Rank_Uplift Between (3 * (@TCount /10)) and (4 * (@TCount /10)) THEN 4
        When Rank_Uplift Between (4 * (@TCount /10)) and (5 * (@TCount /10)) THEN 5
        When Rank_Uplift Between (5 * (@TCount /10)) and (6 * (@TCount /10)) THEN 6
        When Rank_Uplift Between (6 * (@TCount /10)) and (7 * (@TCount /10)) THEN 7
        When Rank_Uplift Between (7 * (@TCount /10)) and (8 * (@TCount /10)) THEN 8
        When Rank_Uplift Between (8 * (@TCount /10)) and (9 * (@TCount /10)) THEN 9
        When Rank_Uplift Between (9 * (@TCount /10)) and (10* (@TCount /10)) THEN 10
   end as Decile_Uplift

  ,case when rank_resp between 0                   and (1 * (@TCount /10)) then 1
        When rank_resp Between (1 * (@TCount /10)) and (2 * (@TCount /10)) THEN 2
        When rank_resp Between (2 * (@TCount /10)) and (3 * (@TCount /10)) THEN 3
        When rank_resp Between (3 * (@TCount /10)) and (4 * (@TCount /10)) THEN 4
        When rank_resp Between (4 * (@TCount /10)) and (5 * (@TCount /10)) THEN 5
        When rank_resp Between (5 * (@TCount /10)) and (6 * (@TCount /10)) THEN 6
        When rank_resp Between (6 * (@TCount /10)) and (7 * (@TCount /10)) THEN 7
        When rank_resp Between (7 * (@TCount /10)) and (8 * (@TCount /10)) THEN 8
        When rank_resp Between (8 * (@TCount /10)) and (9 * (@TCount /10)) THEN 9
        When rank_resp Between (9 * (@TCount /10)) and (10* (@TCount /10)) THEN 10
   end as Decile_resp

into #scored4
from #scored3;

update planning_201706 a
 set a.f_re_full_decile          = b.decile_fp,          -- CHANGE
     a.f_re_offer_decile         = b.decile_op,          -- CHANGE
     a.f_re_uplift_decile        = b.decile_uplift,      -- CHANGE
     a.f_re_resp_decile          = b.decile_resp         -- CHANGE
 from #scored4 b
   where a.account_number=b.account_number;

drop table #scored2;
drop table #scored3;
drop table #scored4;

-- 9. SKY GO EXTRA
Select *
into #scored2
from planning_201706;
--Where sge_eligible=1;-- CHANGE

SELECT @TCount = COUNT(account_number) FROM #scored2;
SELECT @TCount;

If Object_ID('#scored3') is not NUll then DROP TABLE #scored3 end if;
Select *
  ,ROW_NUMBER() OVER(ORDER BY sge_full_prob desc)    AS Rank_fp       -- CHANGE
  ,ROW_NUMBER() OVER(ORDER BY sge_offer_prob desc)   AS Rank_Op      -- CHANGE
  ,ROW_NUMBER() OVER(ORDER BY sge_uplift_prob desc)  AS Rank_Uplift     -- CHANGE
  ,ROW_NUMBER() OVER(ORDER BY sge_resp_prob desc)    AS Rank_resp       -- CHANGE
into #scored3
from #scored2;

If Object_ID('#scored4') is not NUll then DROP TABLE #scored4 end if;
Select *
  ,case when Rank_fp between 0                   and (1 * (@TCount /10)) then 1
        When Rank_fp Between (1 * (@TCount /10)) and (2 * (@TCount /10)) THEN 2
        When Rank_fp Between (2 * (@TCount /10)) and (3 * (@TCount /10)) THEN 3
        When Rank_fp Between (3 * (@TCount /10)) and (4 * (@TCount /10)) THEN 4
        When Rank_fp Between (4 * (@TCount /10)) and (5 * (@TCount /10)) THEN 5
        When Rank_fp Between (5 * (@TCount /10)) and (6 * (@TCount /10)) THEN 6
        When Rank_fp Between (6 * (@TCount /10)) and (7 * (@TCount /10)) THEN 7
        When Rank_fp Between (7 * (@TCount /10)) and (8 * (@TCount /10)) THEN 8
        When Rank_fp Between (8 * (@TCount /10)) and (9 * (@TCount /10)) THEN 9
        When Rank_fp Between (9 * (@TCount /10)) and (10* (@TCount /10)) THEN 10
   end as Decile_FP

  ,case when Rank_OP between 0                   and (1 * (@TCount /10)) then 1
        When Rank_OP Between (1 * (@TCount /10)) and (2 * (@TCount /10)) THEN 2
        When Rank_OP Between (2 * (@TCount /10)) and (3 * (@TCount /10)) THEN 3
        When Rank_OP Between (3 * (@TCount /10)) and (4 * (@TCount /10)) THEN 4
        When Rank_OP Between (4 * (@TCount /10)) and (5 * (@TCount /10)) THEN 5
        When Rank_OP Between (5 * (@TCount /10)) and (6 * (@TCount /10)) THEN 6
        When Rank_OP Between (6 * (@TCount /10)) and (7 * (@TCount /10)) THEN 7
        When Rank_OP Between (7 * (@TCount /10)) and (8 * (@TCount /10)) THEN 8
        When Rank_OP Between (8 * (@TCount /10)) and (9 * (@TCount /10)) THEN 9
        When Rank_OP Between (9 * (@TCount /10)) and (10* (@TCount /10)) THEN 10
   end as Decile_OP

  ,case when Rank_Uplift between 0                   and (1 * (@TCount /10)) then 1
        When Rank_Uplift Between (1 * (@TCount /10)) and (2 * (@TCount /10)) THEN 2
        When Rank_Uplift Between (2 * (@TCount /10)) and (3 * (@TCount /10)) THEN 3
        When Rank_Uplift Between (3 * (@TCount /10)) and (4 * (@TCount /10)) THEN 4
        When Rank_Uplift Between (4 * (@TCount /10)) and (5 * (@TCount /10)) THEN 5
        When Rank_Uplift Between (5 * (@TCount /10)) and (6 * (@TCount /10)) THEN 6
        When Rank_Uplift Between (6 * (@TCount /10)) and (7 * (@TCount /10)) THEN 7
        When Rank_Uplift Between (7 * (@TCount /10)) and (8 * (@TCount /10)) THEN 8
        When Rank_Uplift Between (8 * (@TCount /10)) and (9 * (@TCount /10)) THEN 9
        When Rank_Uplift Between (9 * (@TCount /10)) and (10* (@TCount /10)) THEN 10
   end as Decile_Uplift

  ,case when rank_resp between 0                   and (1 * (@TCount /10)) then 1
        When rank_resp Between (1 * (@TCount /10)) and (2 * (@TCount /10)) THEN 2
        When rank_resp Between (2 * (@TCount /10)) and (3 * (@TCount /10)) THEN 3
        When rank_resp Between (3 * (@TCount /10)) and (4 * (@TCount /10)) THEN 4
        When rank_resp Between (4 * (@TCount /10)) and (5 * (@TCount /10)) THEN 5
        When rank_resp Between (5 * (@TCount /10)) and (6 * (@TCount /10)) THEN 6
        When rank_resp Between (6 * (@TCount /10)) and (7 * (@TCount /10)) THEN 7
        When rank_resp Between (7 * (@TCount /10)) and (8 * (@TCount /10)) THEN 8
        When rank_resp Between (8 * (@TCount /10)) and (9 * (@TCount /10)) THEN 9
        When rank_resp Between (9 * (@TCount /10)) and (10* (@TCount /10)) THEN 10
   end as Decile_resp

into #scored4
from #scored3;

update planning_201706 a
 set a.sge_full_decile          = b.decile_fp,          -- CHANGE
     a.sge_offer_decile         = b.decile_op,          -- CHANGE
     a.sge_uplift_decile        = b.decile_uplift,      -- CHANGE
     a.sge_resp_decile          = b.decile_resp         -- CHANGE
 from #scored4 b
   where a.account_number=b.account_number;

drop table #scored2;
drop table #scored3;
drop table #scored4;


-- 10. FAMILY
Select *
into #scored2
from planning_201706;
--Where sge_eligible=1;-- CHANGE

SELECT @TCount = COUNT(account_number) FROM #scored2;
SELECT @TCount;

If Object_ID('#scored3') is not NUll then DROP TABLE #scored3 end if;
Select *
  ,ROW_NUMBER() OVER(ORDER BY prob_fp_family desc)      AS Rank_fp       -- CHANGE
  ,ROW_NUMBER() OVER(ORDER BY prob_op_family desc)      AS Rank_Op      -- CHANGE
  ,ROW_NUMBER() OVER(ORDER BY prob_uplift_family desc)  AS Rank_Uplift     -- CHANGE
  ,ROW_NUMBER() OVER(ORDER BY prob_resp_family desc)    AS Rank_resp       -- CHANGE
into #scored3
from #scored2;

If Object_ID('#scored4') is not NUll then DROP TABLE #scored4 end if;
Select *
  ,case when Rank_fp between 0                   and (1 * (@TCount /10)) then 1
        When Rank_fp Between (1 * (@TCount /10)) and (2 * (@TCount /10)) THEN 2
        When Rank_fp Between (2 * (@TCount /10)) and (3 * (@TCount /10)) THEN 3
        When Rank_fp Between (3 * (@TCount /10)) and (4 * (@TCount /10)) THEN 4
        When Rank_fp Between (4 * (@TCount /10)) and (5 * (@TCount /10)) THEN 5
        When Rank_fp Between (5 * (@TCount /10)) and (6 * (@TCount /10)) THEN 6
        When Rank_fp Between (6 * (@TCount /10)) and (7 * (@TCount /10)) THEN 7
        When Rank_fp Between (7 * (@TCount /10)) and (8 * (@TCount /10)) THEN 8
        When Rank_fp Between (8 * (@TCount /10)) and (9 * (@TCount /10)) THEN 9
        When Rank_fp Between (9 * (@TCount /10)) and (10* (@TCount /10)) THEN 10
   end as Decile_FP

  ,case when Rank_OP between 0                   and (1 * (@TCount /10)) then 1
        When Rank_OP Between (1 * (@TCount /10)) and (2 * (@TCount /10)) THEN 2
        When Rank_OP Between (2 * (@TCount /10)) and (3 * (@TCount /10)) THEN 3
        When Rank_OP Between (3 * (@TCount /10)) and (4 * (@TCount /10)) THEN 4
        When Rank_OP Between (4 * (@TCount /10)) and (5 * (@TCount /10)) THEN 5
        When Rank_OP Between (5 * (@TCount /10)) and (6 * (@TCount /10)) THEN 6
        When Rank_OP Between (6 * (@TCount /10)) and (7 * (@TCount /10)) THEN 7
        When Rank_OP Between (7 * (@TCount /10)) and (8 * (@TCount /10)) THEN 8
        When Rank_OP Between (8 * (@TCount /10)) and (9 * (@TCount /10)) THEN 9
        When Rank_OP Between (9 * (@TCount /10)) and (10* (@TCount /10)) THEN 10
   end as Decile_OP

  ,case when Rank_Uplift between 0                   and (1 * (@TCount /10)) then 1
        When Rank_Uplift Between (1 * (@TCount /10)) and (2 * (@TCount /10)) THEN 2
        When Rank_Uplift Between (2 * (@TCount /10)) and (3 * (@TCount /10)) THEN 3
        When Rank_Uplift Between (3 * (@TCount /10)) and (4 * (@TCount /10)) THEN 4
        When Rank_Uplift Between (4 * (@TCount /10)) and (5 * (@TCount /10)) THEN 5
        When Rank_Uplift Between (5 * (@TCount /10)) and (6 * (@TCount /10)) THEN 6
        When Rank_Uplift Between (6 * (@TCount /10)) and (7 * (@TCount /10)) THEN 7
        When Rank_Uplift Between (7 * (@TCount /10)) and (8 * (@TCount /10)) THEN 8
        When Rank_Uplift Between (8 * (@TCount /10)) and (9 * (@TCount /10)) THEN 9
        When Rank_Uplift Between (9 * (@TCount /10)) and (10* (@TCount /10)) THEN 10
   end as Decile_Uplift

  ,case when rank_resp between 0                   and (1 * (@TCount /10)) then 1
        When rank_resp Between (1 * (@TCount /10)) and (2 * (@TCount /10)) THEN 2
        When rank_resp Between (2 * (@TCount /10)) and (3 * (@TCount /10)) THEN 3
        When rank_resp Between (3 * (@TCount /10)) and (4 * (@TCount /10)) THEN 4
        When rank_resp Between (4 * (@TCount /10)) and (5 * (@TCount /10)) THEN 5
        When rank_resp Between (5 * (@TCount /10)) and (6 * (@TCount /10)) THEN 6
        When rank_resp Between (6 * (@TCount /10)) and (7 * (@TCount /10)) THEN 7
        When rank_resp Between (7 * (@TCount /10)) and (8 * (@TCount /10)) THEN 8
        When rank_resp Between (8 * (@TCount /10)) and (9 * (@TCount /10)) THEN 9
        When rank_resp Between (9 * (@TCount /10)) and (10* (@TCount /10)) THEN 10
   end as Decile_resp

into #scored4
from #scored3;

update planning_201706 a
 set a.decile_fp_family            = b.decile_fp,          -- CHANGE
     a.decile_op_family            = b.decile_op,          -- CHANGE
     a.decile_uplift_family        = b.decile_uplift,      -- CHANGE
     a.decile_resp_family          = b.decile_resp         -- CHANGE
 from #scored4 b
   where a.account_number=b.account_number;

drop table #scored2;
drop table #scored3;
drop table #scored4;


-- 11. MULTISCREEN
Select *
into #scored2
from planning_201706;
--Where sge_eligible=1;-- CHANGE

SELECT @TCount = COUNT(account_number) FROM #scored2;
SELECT @TCount;

If Object_ID('#scored3') is not NUll then DROP TABLE #scored3 end if;
Select *
  ,ROW_NUMBER() OVER(ORDER BY prob_fp_multiscreen desc)      AS Rank_fp       -- CHANGE
  ,ROW_NUMBER() OVER(ORDER BY prob_op_multiscreen desc)      AS Rank_Op      -- CHANGE
  ,ROW_NUMBER() OVER(ORDER BY prob_uplift_multiscreen desc)  AS Rank_Uplift     -- CHANGE
  ,ROW_NUMBER() OVER(ORDER BY prob_resp_multiscreen desc)    AS Rank_resp       -- CHANGE
into #scored3
from #scored2;

If Object_ID('#scored4') is not NUll then DROP TABLE #scored4 end if;
Select *
  ,case when Rank_fp between 0                   and (1 * (@TCount /10)) then 1
        When Rank_fp Between (1 * (@TCount /10)) and (2 * (@TCount /10)) THEN 2
        When Rank_fp Between (2 * (@TCount /10)) and (3 * (@TCount /10)) THEN 3
        When Rank_fp Between (3 * (@TCount /10)) and (4 * (@TCount /10)) THEN 4
        When Rank_fp Between (4 * (@TCount /10)) and (5 * (@TCount /10)) THEN 5
        When Rank_fp Between (5 * (@TCount /10)) and (6 * (@TCount /10)) THEN 6
        When Rank_fp Between (6 * (@TCount /10)) and (7 * (@TCount /10)) THEN 7
        When Rank_fp Between (7 * (@TCount /10)) and (8 * (@TCount /10)) THEN 8
        When Rank_fp Between (8 * (@TCount /10)) and (9 * (@TCount /10)) THEN 9
        When Rank_fp Between (9 * (@TCount /10)) and (10* (@TCount /10)) THEN 10
   end as Decile_FP

  ,case when Rank_OP between 0                   and (1 * (@TCount /10)) then 1
        When Rank_OP Between (1 * (@TCount /10)) and (2 * (@TCount /10)) THEN 2
        When Rank_OP Between (2 * (@TCount /10)) and (3 * (@TCount /10)) THEN 3
        When Rank_OP Between (3 * (@TCount /10)) and (4 * (@TCount /10)) THEN 4
        When Rank_OP Between (4 * (@TCount /10)) and (5 * (@TCount /10)) THEN 5
        When Rank_OP Between (5 * (@TCount /10)) and (6 * (@TCount /10)) THEN 6
        When Rank_OP Between (6 * (@TCount /10)) and (7 * (@TCount /10)) THEN 7
        When Rank_OP Between (7 * (@TCount /10)) and (8 * (@TCount /10)) THEN 8
        When Rank_OP Between (8 * (@TCount /10)) and (9 * (@TCount /10)) THEN 9
        When Rank_OP Between (9 * (@TCount /10)) and (10* (@TCount /10)) THEN 10
   end as Decile_OP

  ,case when Rank_Uplift between 0                   and (1 * (@TCount /10)) then 1
        When Rank_Uplift Between (1 * (@TCount /10)) and (2 * (@TCount /10)) THEN 2
        When Rank_Uplift Between (2 * (@TCount /10)) and (3 * (@TCount /10)) THEN 3
        When Rank_Uplift Between (3 * (@TCount /10)) and (4 * (@TCount /10)) THEN 4
        When Rank_Uplift Between (4 * (@TCount /10)) and (5 * (@TCount /10)) THEN 5
        When Rank_Uplift Between (5 * (@TCount /10)) and (6 * (@TCount /10)) THEN 6
        When Rank_Uplift Between (6 * (@TCount /10)) and (7 * (@TCount /10)) THEN 7
        When Rank_Uplift Between (7 * (@TCount /10)) and (8 * (@TCount /10)) THEN 8
        When Rank_Uplift Between (8 * (@TCount /10)) and (9 * (@TCount /10)) THEN 9
        When Rank_Uplift Between (9 * (@TCount /10)) and (10* (@TCount /10)) THEN 10
   end as Decile_Uplift

  ,case when rank_resp between 0                   and (1 * (@TCount /10)) then 1
        When rank_resp Between (1 * (@TCount /10)) and (2 * (@TCount /10)) THEN 2
        When rank_resp Between (2 * (@TCount /10)) and (3 * (@TCount /10)) THEN 3
        When rank_resp Between (3 * (@TCount /10)) and (4 * (@TCount /10)) THEN 4
        When rank_resp Between (4 * (@TCount /10)) and (5 * (@TCount /10)) THEN 5
        When rank_resp Between (5 * (@TCount /10)) and (6 * (@TCount /10)) THEN 6
        When rank_resp Between (6 * (@TCount /10)) and (7 * (@TCount /10)) THEN 7
        When rank_resp Between (7 * (@TCount /10)) and (8 * (@TCount /10)) THEN 8
        When rank_resp Between (8 * (@TCount /10)) and (9 * (@TCount /10)) THEN 9
        When rank_resp Between (9 * (@TCount /10)) and (10* (@TCount /10)) THEN 10
   end as Decile_resp

into #scored4
from #scored3;

update planning_201706 a
 set a.decile_fp_multiscreen            = b.decile_fp,          -- CHANGE
     a.decile_op_multiscreen            = b.decile_op,          -- CHANGE
     a.decile_uplift_multiscreen        = b.decile_uplift,      -- CHANGE
     a.decile_resp_multiscreen          = b.decile_resp         -- CHANGE
 from #scored4 b
   where a.account_number=b.account_number;

drop table #scored2;
drop table #scored3;
drop table #scored4;

-- 11. BT VIEWER
Select *
into #scored2
from planning_201706
Where dtv=1;-- CHANGE

SELECT @TCount = COUNT(account_number) FROM #scored2;
SELECT @TCount;

If Object_ID('#scored3') is not NUll then DROP TABLE #scored3 end if;
Select *
  ,ROW_NUMBER() OVER(ORDER BY prob_BT_VIEWER desc)      AS Rank_fp       -- CHANGE

into #scored3
from #scored2;

If Object_ID('#scored4') is not NUll then DROP TABLE #scored4 end if;
Select *
  ,case when Rank_fp between 0                   and (1 * (@TCount /10)) then 1
        When Rank_fp Between (1 * (@TCount /10)) and (2 * (@TCount /10)) THEN 2
        When Rank_fp Between (2 * (@TCount /10)) and (3 * (@TCount /10)) THEN 3
        When Rank_fp Between (3 * (@TCount /10)) and (4 * (@TCount /10)) THEN 4
        When Rank_fp Between (4 * (@TCount /10)) and (5 * (@TCount /10)) THEN 5
        When Rank_fp Between (5 * (@TCount /10)) and (6 * (@TCount /10)) THEN 6
        When Rank_fp Between (6 * (@TCount /10)) and (7 * (@TCount /10)) THEN 7
        When Rank_fp Between (7 * (@TCount /10)) and (8 * (@TCount /10)) THEN 8
        When Rank_fp Between (8 * (@TCount /10)) and (9 * (@TCount /10)) THEN 9
        When Rank_fp Between (9 * (@TCount /10)) and (10* (@TCount /10)) THEN 10
   end as Decile_FP


into #scored4
from #scored3;

update planning_201706 a
 set a.bt_View_decile            = case when b.account_number is not null then b.decile_fp else 99 end          -- CHANGE
 from #scored4 b
   where a.account_number=b.account_number;

update planning_201706 a
 set a.bt_View_decile = case when a.bt_View_decile is null then 99 else a.bt_View_decile end;

select bt_View_decile, avg(prob_BT_VIEWER), count(*) from planning_201706 group by bt_View_decile;

select top 100 * from planning_201706
