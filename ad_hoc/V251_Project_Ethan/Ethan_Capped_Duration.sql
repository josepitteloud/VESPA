NOTE: I only kept the last table so as to save space---This output is for called data only....(Remember to filter out duration >6 ok...


--Getting sum of duration across  account_number, subscriber_id, live_recorded and day
select  account_number
       ,subscriber_id
       ,live_recorded
       ,date(EVENT_START_DATE_TIME_UTC) as Day
       ,sum(Capped_Duration)as Capped_duration
into  drop table Account_Subscriber_duration_C
from VEA_06_12_Dec2014_Viewing_Events
where Program_Order = 1 and EVENT_START_DATE_TIME_UTC <= EVENT_END_DATE_TIME_UTC and duration > 6
group by account_number
        ,subscriber_id
        ,live_recorded
        ,Day
--6,736,095 Row(s) affected
;

--Joining the above table Account_Subscriber_duration to All_TV_Viewers table
select ATV.account_number
      ,ATV.Multi_Room
      ,AD.subscriber_id
      ,AD.live_recorded
      ,AD.Day
      ,AD.Capped_duration
into   drop table All_TV_Viewers_Account_Sub_Duration_C
from All_TV_Viewers ATV
left join Account_Subscriber_duration_C AD
on  ATV.account_number = AD.account_number
--15,625,796 Row(s) affected

;
--Bringing in the PS flag from the Single_box_View table
select ATV.account_number
      ,ATV.subscriber_id
      ,ATV.Multi_Room
      ,ATV.live_recorded
      ,ATV.Day
      ,ATV.Capped_duration
      ,NV.PS_flag
into  drop table All_TV_Viewers_Account_Sub_Duration_PS_C
from All_TV_Viewers_Account_Sub_Duration_C ATV
inner join Vespa_analysts.vespa_single_box_view NV
on  ATV.subscriber_id = NV.subscriber_id
--6,720,940 Row(s) affected

;
--Getting the duration into minutes
select account_number
      ,subscriber_id
      ,Multi_Room
      ,live_recorded
      ,Capped_duration
      ,day
      ,Capped_duration / 60 as dur_Mins
into drop table All_TV_Viewers_Account_Sub_Duration_PS_Mins_C
from All_TV_Viewers_Account_Sub_Duration_PS_C

;
--Putting the results into bands--

select account_number
      ,subscriber_id
      ,Multi_Room
      ,live_recorded
      ,Capped_duration
      ,day
      ,dur_Mins
      ,case when dur_Mins =  0  then '0'
            when dur_Mins between   0 and  60    then '0-60'
            when dur_Mins between  61 and 120    then '61-120'
            when dur_Mins between 121 and 180    then '121-180'
            when dur_Mins between 181 and 240    then '181-240'
            when dur_Mins between 241 and 300    then '241-300'
            when dur_Mins between 301 and 360    then '301-360'
            when dur_Mins between 361 and 420    then '361-420'
            when dur_Mins between 421 and 480    then '421-480'
            when dur_Mins between 481 and 540    then '481-540'
            when dur_Mins between 541 and 600    then '541-600'
            when dur_Mins between 601 and 660    then '601-660'
            when dur_Mins between 661 and 720    then '661-720'
            when dur_Mins between 721 and 780    then '721-780'
            when dur_Mins between 781 and 840    then '781-840'
            when dur_Mins between 841 and 900    then '841-900'
            when dur_Mins between 901 and 960    then '901-960'
            when dur_Mins between 961 and 1020   then '961-1020'
            when dur_Mins between 1021 and 1080  then '1021-1080'
            when dur_Mins between 1081 and 1140  then '1081-1140'
            when dur_Mins between 1141 and 1200  then '1141-1200'
            when dur_Mins between 1201 and 1260  then '1201-1260'
            when dur_Mins between 1261 and 1320  then '1261-1320'
            when dur_Mins between 1321 and 1380  then '1321-1380'
            when dur_Mins between 1381 and 1440  then '1381-1440'
            when dur_Mins between 1441 and 1500  then '1441-1500'
            when dur_Mins >= 1501                 then '1501+'
            else null
      end as Dur_Intervals_Mins
into   drop table All_TV_Viewers_Duration_C
from    All_TV_Viewers_Account_Sub_Duration_PS_Mins_C
--6,720,940 Row(s) affected
;
Alter table All_TV_Viewers_Duration_C add Weights bigint;

--Updating the above table --
update All_TV_Viewers_Duration_C
set ATV.Weights = SC.scaling_weighting
        from  vespa_analysts.vespa_household_weighting SC
    inner join All_TV_Viewers_Duration_C as ATV
    on SC.account_number = ATV.account_number
    where SC.scaling_date = '2014-01-09'
--6,464,224 Row(s) affected



select top 10* from All_TV_Viewers_Duration_C

select case when duration > 0 then 1
            when duration is null or duration = 0 then 0
            end as Duration
       ,case when scaling_weighting is null then 0 else 1 end as Scaling
       ,count(*)
from Final_Viewing_Weights
group by duration, scaling
