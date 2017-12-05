
-- ##### Check volumes of accounts by package when sum for individual genres < total for package

  -- Entertainment package
drop table missing_aggr_ent;
select
    a.Account_Number,
    a.total_aggr_viewing_dur,
    b.total_aggr_viewing_dur2
  into --drop table
       missing_aggr_ent
  from (select Account_Number,total_aggr_viewing_dur from VAggr_03_Account_Metrics_PH2_GENRES where aggregation_id = 2 and account_status = 1) a,
       (select Account_Number,sum(total_aggr_viewing_dur) as total_aggr_viewing_dur2 from VAggr_03_Account_Metrics_PH2_GENRES where aggregation_id between 10 and 17 group by Account_Number) b
 where a.Account_Number = b.Account_Number
 --  and a.account_number = '621090487359'
   and a.total_aggr_viewing_dur > b.total_aggr_viewing_dur2;
commit;
create unique hg index ix on missing_aggr_ent(account_Number);
select count(*) from missing_aggr_ent;
select * from missing_aggr_ent;


  -- Entertainment Extra package
drop table missing_aggr_ent_ext;
select
    a.Account_Number,
    a.total_aggr_viewing_dur,
    b.total_aggr_viewing_dur2
  into --drop table
       missing_aggr_ent_ext
  from (select Account_Number,total_aggr_viewing_dur from VAggr_03_Account_Metrics_PH2_GENRES where aggregation_id = 3 and account_status = 1) a,
       (select Account_Number,sum(total_aggr_viewing_dur) as total_aggr_viewing_dur2 from VAggr_03_Account_Metrics_PH2_GENRES where aggregation_id between 20 and 27 group by Account_Number) b
 where a.Account_Number = b.Account_Number
 --  and a.account_number = '621090487359'
   and a.total_aggr_viewing_dur > b.total_aggr_viewing_dur2;
commit;
create unique hg index ix on missing_aggr_ent_ext(account_Number);
select count(*) from missing_aggr_ent_ext;
select * from missing_aggr_ent_ext;

  -- Entertainment Extra+ package
drop table missing_aggr_ent_ext_plus;
select
    a.Account_Number,
    a.total_aggr_viewing_dur,
    b.total_aggr_viewing_dur2
  into --drop table
       missing_aggr_ent_ext_plus
  from (select Account_Number,total_aggr_viewing_dur from VAggr_03_Account_Metrics_PH2_GENRES where aggregation_id = 4 and account_status = 1) a,
       (select Account_Number,sum(total_aggr_viewing_dur) as total_aggr_viewing_dur2 from VAggr_03_Account_Metrics_PH2_GENRES where aggregation_id between 30 and 37 group by Account_Number) b
 where a.Account_Number = b.Account_Number
 --  and a.account_number = '621090487359'
   and a.total_aggr_viewing_dur > b.total_aggr_viewing_dur2;
commit;
create unique hg index ix on missing_aggr_ent_ext_plus(account_Number);
select count(*) from missing_aggr_ent_ext_plus;
select * from missing_aggr_ent_ext_plus;

  -- Movies package
drop table missing_aggr_movies;
select
    a.Account_Number,
    a.total_aggr_viewing_dur,
    b.total_aggr_viewing_dur2
  into --drop table
       missing_aggr_movies
  from (select Account_Number,total_aggr_viewing_dur from VAggr_03_Account_Metrics_PH2_GENRES where aggregation_id = 5 and account_status = 1) a,
       (select Account_Number,sum(total_aggr_viewing_dur) as total_aggr_viewing_dur2 from VAggr_03_Account_Metrics_PH2_GENRES where aggregation_id between 40 and 45 group by Account_Number) b
 where a.Account_Number = b.Account_Number
 --  and a.account_number = '621090487359'
   and a.total_aggr_viewing_dur > b.total_aggr_viewing_dur2;
commit;
create unique hg index ix on missing_aggr_movies(account_Number);
select count(*) from missing_aggr_movies;
select * from missing_aggr_movies;

  -- Sports package
drop table missing_aggr_sports;
select
    a.Account_Number,
    a.total_aggr_viewing_dur,
    b.total_aggr_viewing_dur2
  into --drop table
       missing_aggr_sports
  from (select Account_Number,total_aggr_viewing_dur from VAggr_03_Account_Metrics_PH2_GENRES where aggregation_id = 6 and account_status = 1) a,
       (select Account_Number,sum(total_aggr_viewing_dur) as total_aggr_viewing_dur2 from VAggr_03_Account_Metrics_PH2_GENRES where aggregation_id between 50 and 58 group by Account_Number) b
 where a.Account_Number = b.Account_Number
 --  and a.account_number = '621090487359'
   and a.total_aggr_viewing_dur > b.total_aggr_viewing_dur2;
commit;
create unique hg index ix on missing_aggr_sports(account_Number);
select count(*) from missing_aggr_sports;
select * from missing_aggr_sports;







-- ##### Review genres & sub-genres in the source table that are not genre-flagged
drop table missing_aggr_pks;
select pk_viewing_prog_instance_Fact, 1 as dummy
  into missing_aggr_pks
  from VAggr_02_Viewing_Events_PH2_GENRES
 where F_Genre_Children + F_Genre_Movies + F_Genre_News_Documentaries + F_Genre_Sports + F_Genre_Action_SciFi +
       F_Genre_Arts_Lifestyle + F_Genre_Comedy_GameShows + F_Genre_Drama_Crime + F_Genre_Action_Adventure + F_Genre_Comedy +
       F_Genre_Drama_Romance + F_Genre_Family + F_Genre_Horror_Thriller + F_Genre_SciFi_Fantasy + F_Genre_American +
       F_Genre_Boxing_Wrestling + F_Genre_Cricket + F_Genre_Football + F_Genre_Golf + F_Genre_Motor_Extreme +
       F_Genre_Rugby + F_Genre_Tennis + F_Genre_Niche_Sport = 0;
commit;
create unique hg index ix on missing_aggr_pks(pk_viewing_prog_instance_Fact);
select * from missing_aggr_pks

drop table missing_aggr_source;
select a.*
  into missing_aggr_source
  from VAggr_02_Viewing_Events_201305 a,
       missing_aggr_pks b
 where a.pk_viewing_prog_instance_Fact = b.pk_viewing_prog_instance_Fact;
commit;

  -- Overall
select genre_description, sub_genre_description, count(*) as cnt from missing_aggr_source group by genre_description, sub_genre_description;

  -- Entertainment package
select genre_description, sub_genre_description, count(*) as cnt from missing_aggr_source where account_number in (select account_number from missing_aggr_ent) group by genre_description, sub_genre_description;

  -- Entertainment Extra package
select genre_description, sub_genre_description, count(*) as cnt from missing_aggr_source where account_number in (select account_number from missing_aggr_ent_ext) group by genre_description, sub_genre_description;

  -- Entertainment Extra+ package
select genre_description, sub_genre_description, count(*) as cnt from missing_aggr_source where account_number in (select account_number from missing_aggr_ent_ext_plus) group by genre_description, sub_genre_description;

  -- Movies package
select genre_description, sub_genre_description, count(*) as cnt from missing_aggr_source where account_number in (select account_number from missing_aggr_movies) group by genre_description, sub_genre_description;

  -- Sports package
select genre_description, sub_genre_description, count(*) as cnt from missing_aggr_source where account_number in (select account_number from missing_aggr_sports) group by genre_description, sub_genre_description;






/*
-- ##### Reset statuses for all aggregations #####

update VAggr_03_Account_Metrics_PH2_GENRES a
   set Account_Status = case
                          when acc.Ent_DTV_Pack_Ent = 0 or acc.Movmt_DTV_Pack_Ent = 1 then -3                               -- Not eligible
                            else 1
                        end
  from VESPA_Shared.Aggr_Account_Attributes acc
 where acc.Account_Number = a.Account_Number
   and acc.period_key = 5
   and a.Aggregation_Id = 2;
commit;

update VAggr_03_Account_Metrics_PH2_GENRES a
   set Account_Status = case
                          when acc.Ent_DTV_Pack_Ent_Extra = 0 or acc.Movmt_DTV_Pack_Ent_Extra = 1 then -3                   -- Not eligible
                            else 1
                        end
  from VESPA_Shared.Aggr_Account_Attributes acc
 where acc.Account_Number = a.Account_Number
   and acc.period_key = 5
   and a.Aggregation_Id = 3;
commit;

update VAggr_03_Account_Metrics_PH2_GENRES a
   set Account_Status = case
                          when acc.Ent_DTV_Pack_Ent_Extra_Plus = 0 or acc.Movmt_DTV_Pack_Ent_Extra_Plus = 1 then -3         -- Not eligible
                            else 1
                        end
  from VESPA_Shared.Aggr_Account_Attributes acc
 where acc.Account_Number = a.Account_Number
   and acc.period_key = 5
   and a.Aggregation_Id = 4;
commit;

update VAggr_03_Account_Metrics_PH2_GENRES a
   set Account_Status = case
                          when acc.Ent_DTV_Prem_Movies = 0 or acc.Movmt_DTV_Prem_Movies = 1 then -3                         -- Not eligible
                            else 1
                        end
  from VESPA_Shared.Aggr_Account_Attributes acc
 where acc.Account_Number = a.Account_Number
   and acc.period_key = 5
   and a.Aggregation_Id = 5;
commit;

update VAggr_03_Account_Metrics_PH2_GENRES a
   set Account_Status = case
                          when acc.Ent_DTV_Prem_Sports = 0 or acc.Movmt_DTV_Prem_Sports = 1 then -3                         -- Not eligible
                            else 1
                        end
  from VESPA_Shared.Aggr_Account_Attributes acc
 where acc.Account_Number = a.Account_Number
   and acc.period_key = 5
   and a.Aggregation_Id = 6;
commit;


update VAggr_03_Account_Metrics_PH2_GENRES a
   set Account_Status = case
                          when acc.Ent_DTV_Pack_Ent = 0 or acc.Movmt_DTV_Pack_Ent = 1 then -3                               -- Not eligible
                            else 1
                        end
  from VESPA_Shared.Aggr_Account_Attributes acc
 where acc.Account_Number = a.Account_Number
   and acc.period_key = 5
   and a.Aggregation_Id between 10 and 17;
commit;

update VAggr_03_Account_Metrics_PH2_GENRES a
   set Account_Status = case
                          when acc.Ent_DTV_Pack_Ent_Extra = 0 or acc.Movmt_DTV_Pack_Ent_Extra = 1 then -3                   -- Not eligible
                            else 1
                        end
  from VESPA_Shared.Aggr_Account_Attributes acc
 where acc.Account_Number = a.Account_Number
   and acc.period_key = 5
   and a.Aggregation_Id between 20 and 27;
commit;

update VAggr_03_Account_Metrics_PH2_GENRES a
   set Account_Status = case
                          when acc.Ent_DTV_Pack_Ent_Extra_Plus = 0 or acc.Movmt_DTV_Pack_Ent_Extra_Plus = 1 then -3         -- Not eligible
                            else 1
                        end
  from VESPA_Shared.Aggr_Account_Attributes acc
 where acc.Account_Number = a.Account_Number
   and acc.period_key = 5
   and a.Aggregation_Id between 30 and 37;
commit;

update VAggr_03_Account_Metrics_PH2_GENRES a
   set Account_Status = case
                          when acc.Ent_DTV_Prem_Movies = 0 or acc.Movmt_DTV_Prem_Movies = 1 then -3                         -- Not eligible
                            else 1
                        end
  from VESPA_Shared.Aggr_Account_Attributes acc
 where acc.Account_Number = a.Account_Number
   and acc.period_key = 5
   and a.Aggregation_Id between 40 and 45;
commit;

update VAggr_03_Account_Metrics_PH2_GENRES a
   set Account_Status = case
                          when acc.Ent_DTV_Prem_Sports = 0 or acc.Movmt_DTV_Prem_Sports = 1 then -3                         -- Not eligible
                            else 1
                        end
  from VESPA_Shared.Aggr_Account_Attributes acc
 where acc.Account_Number = a.Account_Number
   and acc.period_key = 5
   and a.Aggregation_Id between 50 and 58;
commit;

select
      Aggregation_Id,
      Accout_Status,
      min(Ent_DTV_Pack_Ent) as Mn1,
      max(Ent_DTV_Pack_Ent) as Mx1,
      min(Ent_DTV_Pack_Ent_Extra) as Mn2,
      max(Ent_DTV_Pack_Ent_Extra) as Mx2,
      min(Ent_DTV_Pack_Ent_Extra_Plus) as Mn3,
      max(Ent_DTV_Pack_Ent_Extra_Plus) as Mx3,
      min(Ent_DTV_Prem_Movies) as Mn4,
      max(Ent_DTV_Prem_Movies) as Mx4,
      min(Ent_DTV_Prem_Sports) as Mn5,
      max(Ent_DTV_Prem_Sports) as Mx5
  from VAggr_03_Account_Metrics_PH2_GENRES a,
       VESPA_Shared.Aggr_Account_Attributes acc
 where acc.Account_Number = a.Account_Number
   and acc.period_key = 5
 group by Aggregation_Id, Accout_Status;
*/
















