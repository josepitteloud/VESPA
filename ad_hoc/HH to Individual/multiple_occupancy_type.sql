--Find no. of accounts in the Sky base which have the following household composition
--which we later break down by age
--(HHcomposition code in brackets afterwards)
--      male homesharers        (06)
--      female homesharers      (07)

begin
        declare @scaling_date date
        set     @scaling_date = '2013-07-14'

        declare @profiling_date date
        select  @profiling_date    = max(profiling_date)
                                        from vespa_analysts.SC2_Sky_base_segment_snapshots
                                        where profiling_date <= @scaling_date

--First of all, using the column cb_key_individual to link the tables
--cust_single_account_view and experian_consumerview find the common
--account_numbers and store cb_key_household, cb_key_individual and p_actual_age
--Note that there are approximately 1.8 million cb_key_individual records which
--are duplicates (relating to about 3.7 million rows in the table)
--To remove duplicates we have only included those cb_key_individual with count > 1
-- IF object_id('accounts_p_actual_age') IS NOT NULL DROP TABLE accounts_p_actual_age
-- select account_number, a.cb_key_household, a.cb_key_individual, p_actual_age
--         into accounts_p_actual_age
--         from sk_prod.cust_single_account_view a
--   inner join (SELECT cb_key_household, cb_key_individual, MAX(p_actual_age) as p_actual_age
--                 FROM sk_prod.EXPERIAN_CONSUMERVIEW
--             GROUP BY cb_key_household, cb_key_individual
--               ) as  b
--           on a.cb_key_individual = b.cb_key_individual
-- commit

--Create tables containing all of the accounts numbers for each household composition of interest
IF object_id('accounts_multiple_occupancy_single_sex') IS NOT NULL DROP TABLE accounts_multiple_occupancy_single_sex
select       account_number, hhcomposition
        into accounts_multiple_occupancy_single_sex
        from vespa_analysts.SC2_Sky_base_segment_snapshots a
  inner join vespa_analysts.SC2_Segments_Lookup_v2_1 b
          on a.scaling_segment_id = b.scaling_segment_id
       where profiling_date = @profiling_date
         and a.scaling_segment_id in
                (select scaling_segment_id
                        from vespa_analysts.SC2_Segments_Lookup_v2_1
                       where hhcomposition = '06'
                          or hhcomposition = '07')
commit

if           object_id('accounts_female_sharers') is not null drop table accounts_female_sharers
select       cb_key_household
            ,cb_key_individual
            ,cb_name_fullname
            ,cb_name_gender
            ,cb_address_line_1
            ,cb_address_line_2
            ,cb_address_line_3
            ,cb_address_line_4
            ,cb_address_line_5
            ,cb_address_line_6
            ,p_actual_age
            ,h_number_of_adults
            ,h_number_of_children_in_household_2011
        into accounts_female_sharers
        from sk_prod.EXPERIAN_CONSUMERVIEW
       where cb_key_individual in
                (select cb_key_individual
                        from sk_prod.cust_single_account_view
                       where account_number in
                             (select account_number
                                from accounts_multiple_occupancy_single_sex
                               where hhcomposition = '07'))
              order by cb_key_household
commit

--Update table to include age bands
alter table  accounts_female_sharers add (age_band varchar(8))

--Set age to BARB age bands
update accounts_female_sharers
        set age_band = (case
                when p_actual_age between  4 and  9 then   '4-9'
                when p_actual_age between 10 and 15 then '10-15'
                when p_actual_age between 16 and 19 then '16-19'
                when p_actual_age between 20 and 24 then '20-24'
                when p_actual_age between 25 and 34 then '25-34'
                when p_actual_age between 35 and 44 then '35-44'
                when p_actual_age between 45 and 64 then '45-64'
                else '65+' end)
commit

if           object_id('accounts_male_sharers') is not null drop table accounts_male_sharers
select       cb_key_household
            ,cb_key_individual
            ,cb_name_fullname
            ,cb_name_gender
            ,cb_address_line_1
            ,cb_address_line_2
            ,cb_address_line_3
            ,cb_address_line_4
            ,cb_address_line_5
            ,cb_address_line_6
            ,p_actual_age
            ,h_number_of_adults
            ,h_number_of_children_in_household_2011
        into accounts_male_sharers
        from sk_prod.EXPERIAN_CONSUMERVIEW
       where cb_key_individual in
                (select cb_key_individual
                        from sk_prod.cust_single_account_view
                       where account_number in
                             (select account_number
                                from accounts_multiple_occupancy_single_sex
                               where hhcomposition = '06'))
              order by cb_key_household
commit

--Update table to include age bands
alter table  accounts_male_sharers add (age_band varchar(8))

--Set age to BARB age bands
update accounts_male_sharers
        set age_band = (case
                when p_actual_age between  4 and  9 then   '4-9'
                when p_actual_age between 10 and 15 then '10-15'
                when p_actual_age between 16 and 19 then '16-19'
                when p_actual_age between 20 and 24 then '20-24'
                when p_actual_age between 25 and 34 then '25-34'
                when p_actual_age between 35 and 44 then '35-44'
                when p_actual_age between 45 and 64 then '45-64'
                else '65+' end)
commit

--Link tables with single_account_view to add account_number
if           object_id('account_no_f_sharers') is not null drop table account_no_f_sharers
select       a.account_number, b.*
        into account_no_f_sharers
        from sk_prod.cust_single_account_view a
  right join accounts_female_sharers b
          on a.cb_key_individual = b.cb_key_individual
       where h_number_of_children_in_household_2011 = '0'
          or h_number_of_children_in_household_2011 = 'U'
    order by account_number
commit

--Update table to include further info about hhcomposition, namely
--(no. of individual keys = no. of adults) > 1  -> 1
--(no. of individual keys = no. of adults) = 1  -> 2
--(no. of individual keys = 1 < no. of adults)  -> 3
--(no. of individual keys > 1 < no. of adults)  -> 4
alter table  account_no_f_sharers add (hh_individual_key int)

select cb_key_household, h_number_of_adults, count(distinct cb_key_individual) as c
        into #temp_f_sharers_1
        from account_no_f_sharers
       where h_number_of_adults > 1
    group by cb_key_household, h_number_of_adults
      having count(distinct cb_key_individual) = h_number_of_adults
select cb_key_household, h_number_of_adults, count(distinct cb_key_individual) as c
        into #temp_f_sharers_2
        from account_no_f_sharers
       where h_number_of_adults = 1
    group by cb_key_household, h_number_of_adults
      having count(distinct cb_key_individual) = h_number_of_adults
select cb_key_household, h_number_of_adults, count(distinct cb_key_individual) as c
        into #temp_f_sharers_3
        from account_no_f_sharers
    group by cb_key_household, h_number_of_adults
      having count(distinct cb_key_individual) < h_number_of_adults and count(distinct cb_key_individual) = 1
select cb_key_household, h_number_of_adults, count(distinct cb_key_individual) as c
        into #temp_f_sharers_4
        from account_no_f_sharers
    group by cb_key_household, h_number_of_adults
      having count(distinct cb_key_individual) < h_number_of_adults and count(distinct cb_key_individual) > 1

update account_no_f_sharers a
        set hh_individual_key = 1
        from #temp_f_sharers_1 b
        where a.cb_key_household = b.cb_key_household
update account_no_f_sharers a
        set hh_individual_key = 2
        from #temp_f_sharers_2 b
        where a.cb_key_household = b.cb_key_household
update account_no_f_sharers a
        set hh_individual_key = 3
        from #temp_f_sharers_3 b
        where a.cb_key_household = b.cb_key_household
update account_no_f_sharers a
        set hh_individual_key = 4
        from #temp_f_sharers_4 b
        where a.cb_key_household = b.cb_key_household
commit

--Repeat the above for male homesharers
if           object_id('account_no_m_sharers') is not null drop table account_no_m_sharers
select       a.account_number, b.*
        into account_no_m_sharers
        from sk_prod.cust_single_account_view a
  right join accounts_male_sharers b
          on a.cb_key_individual = b.cb_key_individual
       where h_number_of_children_in_household_2011 = '0'
          or h_number_of_children_in_household_2011 = 'U'
    order by account_number
commit

--Update table to include further info about hhcomposition, namely
--(no. of individual keys = no. of adults) > 1  -> 1
--(no. of individual keys = no. of adults) = 1  -> 2
--(no. of individual keys = 1 < no. of adults)  -> 3
--(no. of individual keys > 1 < no. of adults)  -> 4
alter table  account_no_m_sharers add (hh_individual_key int)

select cb_key_household, h_number_of_adults, count(distinct cb_key_individual) as c
        into #temp_m_sharers_1
        from account_no_m_sharers
       where h_number_of_adults > 1
    group by cb_key_household, h_number_of_adults
      having count(distinct cb_key_individual) = h_number_of_adults
select cb_key_household, h_number_of_adults, count(distinct cb_key_individual) as c
        into #temp_m_sharers_2
        from account_no_m_sharers
       where h_number_of_adults = 1
    group by cb_key_household, h_number_of_adults
      having count(distinct cb_key_individual) = h_number_of_adults
select cb_key_household, h_number_of_adults, count(distinct cb_key_individual) as c
        into #temp_m_sharers_3
        from account_no_m_sharers
    group by cb_key_household, h_number_of_adults
      having count(distinct cb_key_individual) < h_number_of_adults and count(distinct cb_key_individual) = 1
select cb_key_household, h_number_of_adults, count(distinct cb_key_individual) as c
        into #temp_m_sharers_4
        from account_no_m_sharers
    group by cb_key_household, h_number_of_adults
      having count(distinct cb_key_individual) < h_number_of_adults and count(distinct cb_key_individual) > 1

update account_no_m_sharers a
        set hh_individual_key = 1
        from #temp_m_sharers_1 b
        where a.cb_key_household = b.cb_key_household
update account_no_m_sharers a
        set hh_individual_key = 2
        from #temp_m_sharers_2 b
        where a.cb_key_household = b.cb_key_household
update account_no_m_sharers a
        set hh_individual_key = 3
        from #temp_m_sharers_3 b
        where a.cb_key_household = b.cb_key_household
update account_no_m_sharers a
        set hh_individual_key = 4
        from #temp_m_sharers_4 b
        where a.cb_key_household = b.cb_key_household
commit

select hh_individual_key, count(*) from account_no_m_sharers group by hh_individual_key
select hh_individual_key, count(*) from account_no_f_sharers group by hh_individual_key

grant all on account_no_m_sharers to public
grant all on account_no_f_sharers to public
end

--Counts for each of the four groupings we have for female house sharers
begin

--Find female homesharers where no_of_individual_keys = no_of_adults > 1 where all residents are in the same age band
select m1, m2, count(*) from(
    select       cb_key_household, MIN(age_band) as m1, MAX(age_band) as m2
            from account_no_f_sharers
           where h_number_of_adults > 1
             and cb_key_household in (select cb_key_household from (
                          select cb_key_household, h_number_of_adults, count(distinct cb_key_individual) as c
                                    from account_no_f_sharers
                                group by cb_key_household, h_number_of_adults
                                  having count(distinct cb_key_individual) = h_number_of_adults) as sub1)
        group by cb_key_household) as sub2
--    order by age_band
where m1 = m2
group by m1, m2
order by m1, m2
commit
--Find male homesharers where no_of_individual_keys = no_of_adults > 1 where all residents are in the same age band
select m1, m2, count(*) from(
    select       cb_key_household, MIN(age_band) as m1, MAX(age_band) as m2
            from account_no_m_sharers
           where h_number_of_adults > 1
             and cb_key_household in (select cb_key_household from (
                          select cb_key_household, h_number_of_adults, count(distinct cb_key_individual) as c
                                    from account_no_m_sharers
                                group by cb_key_household, h_number_of_adults
                                  having count(distinct cb_key_individual) = h_number_of_adults) as sub1)
        group by cb_key_household) as sub2
--    order by age_band
where m1 = m2
group by m1, m2
order by m1, m2
commit

--Find female homesharers where no_of_adults = no_of_individual_keys = 1
select       age_band, count(*)
        from account_no_f_sharers
       where h_number_of_adults = 1
         and cb_key_household in (select cb_key_household from (
                      select cb_key_household, count(distinct cb_key_individual) as c
                                from account_no_f_sharers
                            group by cb_key_household
                              having count(distinct cb_key_individual) = 1) as sub1)
    group by age_band
    order by age_band
commit
--Find male homesharers where no_of_individual_keys = no_of_adults = 1
select       age_band, count(*)
        from account_no_m_sharers
       where h_number_of_adults = 1
         and cb_key_household in (select cb_key_household from (
                      select cb_key_household, count(distinct cb_key_individual) as c
                                from account_no_m_sharers
                            group by cb_key_household
                              having count(distinct cb_key_individual) = 1) as sub1)
    group by age_band
    order by age_band
commit

--Find female homesharers where (no_of_individual_keys=1) < no_of_adults
select       age_band, count(*)
        from account_no_f_sharers
       where h_number_of_adults > 1
         and cb_key_household in (select cb_key_household from (
                      select cb_key_household, count(distinct cb_key_individual) as c
                                from account_no_f_sharers
                            group by cb_key_household
                              having count(distinct cb_key_individual) = 1) as sub1)
        group by age_band
        order by age_band
commit
--Find male homesharers where (no_of_individual_keys=1) < no_of_adults
select       age_band, count(*)
        from account_no_m_sharers
       where h_number_of_adults > 1
         and cb_key_household in (select cb_key_household from (
                      select cb_key_household, count(distinct cb_key_individual) as c
                                from account_no_m_sharers
                            group by cb_key_household
                              having count(distinct cb_key_individual) = 1) as sub1)
    group by age_band
    order by age_band
commit

--Find female homesharers where (no_of_individual_keys>1) < no_of_adults where all residents are in the same age band
  select   m1, m2, count(*) from(
    select       cb_key_household, MIN(age_band) as m1, MAX(age_band) as m2
            from account_no_f_sharers
           where h_number_of_adults > 1
             and cb_key_household in (select cb_key_household from (
                          select cb_key_household, h_number_of_adults, count(distinct cb_key_individual) as c
                                    from account_no_f_sharers
                                group by cb_key_household, h_number_of_adults
                                  having count(distinct cb_key_individual) > 1 and count(distinct cb_key_individual) < h_number_of_adults) as sub1)
            group by cb_key_household) as sub2
--    order by age_band
   where   m1 = m2
group by   m1, m2
order by   m1, m2
commit
--Find male homesharers where (no_of_individual_keys>1) < no_of_adults where all residents are in the same age band
  select   m1, m2, count(*) from(
    select       cb_key_household, MIN(age_band) as m1, MAX(age_band) as m2
            from account_no_m_sharers
           where h_number_of_adults > 1
             and cb_key_household in (select cb_key_household from (
                          select cb_key_household, h_number_of_adults, count(distinct cb_key_individual) as c
                                    from account_no_m_sharers
                                group by cb_key_household, h_number_of_adults
                                  having count(distinct cb_key_individual) > 1 and count(distinct cb_key_individual) < h_number_of_adults) as sub1)
            group by cb_key_household) as sub2
--    order by age_band
   where   m1 = m2
group by   m1, m2
order by   m1, m2
commit


-- select h_number_of_adults, count(*)
--         from accounts_female_sharers
--         group by h_number_of_adults
--         order by h_number_of_adults
-- 
-- begin
-- select count(*), count(distinct account_number)
--         from account_no_m_sharers
--         where cb_key_household in (select cb_key_household from (
--                                           select cb_key_household, count(distinct cb_key_individual) as c
--                                                     from account_no_m_sharers
--                                                 group by cb_key_household
--                                                   having count(distinct cb_key_individual) > 1) as sub1)
-- commit
-- end
-- begin
-- select count(*), count(distinct account_number)
--         from account_no_f_sharers
--         where cb_key_household in (select cb_key_household from (
--                                           select cb_key_household, count(distinct cb_key_individual) as c
--                                                     from account_no_f_sharers
--                                                 group by cb_key_household
--                                                   having count(distinct cb_key_individual) > 1) as sub1)
-- --      order by cb_key_household
-- commit
-- end
-- select top 20 * from sk_prod_experian_consumerview
-- begin
-- select m1 as age_band, count(*) as households, sum(individuals) as total_people from (
-- select cb_key_household, MIN(age_band) as m1, MAX(age_band) as m2, same = case when m1 = m2 then 1 else 0 end, count(*) as individuals
--         from test_table_f
--         where cb_key_household in (select cb_key_household from (
--                                           select cb_key_household, count(distinct cb_key_individual) as c
--                                                     from test_table_f
--                                                 group by cb_key_household
--                                                   having count(distinct cb_key_individual) > 1) as sub1)
--     group by cb_key_household) as sub1
-- --     order by same desc
--     where same = 1
--     group by age_band
--     order by age_band
-- commit
-- end
-- begin
-- select m1 as age_band, count(*) as households, sum(individuals) as total_people from (
-- select cb_key_household, MIN(age_band) as m1, MAX(age_band) as m2, same = case when m1 = m2 then 1 else 0 end, count(*) as individuals
--         from test_table_m
--         where cb_key_household in (select cb_key_household from (
--                                           select cb_key_household, count(distinct cb_key_individual) as c
--                                                     from test_table_m
--                                                 group by cb_key_household
--                                                   having count(distinct cb_key_individual) > 1) as sub1)
--     group by cb_key_household) as sub1
-- --     order by same desc
--     where same = 1
--     group by age_band
--     order by age_band
-- commit
-- end
-- 
-- select count(*) from accounts_female_sharers
--         where lower(cb_address_line_1) not like '%flat%'
--           and lower(cb_address_line_1) not like '%apartment%'
--      order by cb_address_line_2
-- select top 20 * from accounts_female_sharers order by cb_key_household
-- 
--  select top 20 * from sk_prod.experian_consumerview
-- 
-- 
-- select count(cb_key_household), count(distinct cb_key_household) from (
-- select cb_key_household, count(distinct cb_key_individual) as c from test_table group by cb_key_household having count(distinct cb_key_individual) > 1) as sub1
-- 
-- select count(account_number), count(distinct account_number)
--         from sk_prod.cust_single_account_view
--        where cb_key_individual in
--                 (select cb_key_individual from accounts_male_sharers)
-- 
-- begin
-- select  count(*) from (
--         select cb_key_individual, count(*) as c
--                 from sk_prod.cust_single_account_view
--             group by cb_key_individual
--               having count(cb_key_individual) > 1
--         ) as sub1
-- commit
-- end
-- 
-- 
-- --Add household composition, age and a BARB age band column
-- alter table accounts_single_occupancy_type add (age int, age_band varchar(8))
-- 
-- --Take actual age from accounts_p_actual_age table
-- update accounts_single_occupancy_type a
--         set age = p_actual_age
--        from accounts_p_actual_age b
--       where a.account_number = b.account_number
-- 
-- --Set age to BARB age bands
-- update accounts_single_occupancy_type
--         set age_band = (case
--                 when age between  4 and  9 then   '4-9'
--                 when age between 10 and 15 then '10-15'
--                 when age between 16 and 19 then '16-19'
--                 when age between 20 and 24 then '20-24'
--                 when age between 25 and 34 then '25-34'
--                 when age between 35 and 44 then '35-44'
--                 when age between 45 and 64 then '45-64'
--                 else '65+' end)
-- 
-- select hhcomposition, age_band, count(*) from accounts_single_occupancy_type group by hhcomposition, age_band order by hhcomposition, age_band
-- end
-- 
-- --Find no. of accounts in the Vespa which have the following household composition
-- --which we later break down by age. This part of the code excludes any account with
-- --a repeated cb_key_individual
-- --(HHcomposition code in brackets afterwards)
-- --      single male             (04)
-- --      single female           (05)
-- --      male homesharers        (06)
-- --      female homesharers      (07)
-- 
-- begin
--         declare @scaling_date date
--         set     @scaling_date = '2013-07-14'
-- 
--         declare @profiling_date date
--         select  @profiling_date    = max(profiling_date)
--                                         from vespa_analysts.SC2_Sky_base_segment_snapshots
--                                         where profiling_date <= @scaling_date
-- 
-- --First of all, using the column cb_key_individual to link the tables
-- --cust_single_account_view and experian_consumerview find the common
-- --account_numbers in vespa and store cb_key_household, cb_key_individual and p_actual_age
-- --Note that there are approximately 1.8 million cb_key_individual records which
-- --are duplicates (relating to about 3.7 million rows in the table)
-- --To remove duplicates we have only included those cb_key_individual with count > 1
-- IF object_id('vespa_p_actual_age') IS NOT NULL DROP TABLE vespa_p_actual_age
-- select account_number, a.cb_key_household, a.cb_key_individual, p_actual_age
--         into vespa_p_actual_age
--         from sk_prod.cust_single_account_view a
--   inner join (SELECT cb_key_household, cb_key_individual, MAX(p_actual_age) as p_actual_age
--                 FROM sk_prod.EXPERIAN_CONSUMERVIEW
--             GROUP BY cb_key_household, cb_key_individual
--               having count(cb_key_individual) = 1
--               ) as  b
--           on a.cb_key_individual = b.cb_key_individual
--        where account_number is not null
--          and account_number in
--                 (select account_number
--                     from Vespa_Analysts.Vespa_Single_Box_View sbv
--                    where cast(sbv.subscriber_id as int) in
--                             ( select cast(subscriber_id as int)
--                                 from vespa_analysts.alt_panel_data
--                                where panel = 12
--                                  and dt = @scaling_date))
-- commit
-- 
-- --Create tables containing all of the accounts numbers for each household composition of interest
-- IF object_id('vespa_single_occupancy_type') IS NOT NULL DROP TABLE vespa_single_occupancy_type
-- -- select       *
-- --         into vespa_single_occupancy_type
-- --         from accounts_single_occupancy_type
-- --        where account_number in
-- --                 (select account_number
-- --                     from Vespa_Analysts.Vespa_Single_Box_View sbv
-- --               inner join vespa_analysts.alt_panel_data alt
-- --                       on cast(sbv.subscriber_id as int) = cast(alt.subscriber_id as int)
-- --                    where alt.panel = 12
-- --                      and dt = @scaling_date)
-- 
-- select       account_number, hhcomposition
--         into vespa_single_occupancy_type
--         from vespa_analysts.SC2_Sky_base_segment_snapshots a
--   inner join vespa_analysts.SC2_Segments_Lookup_v2_1 b
--           on a.scaling_segment_id = b.scaling_segment_id
--        where profiling_date = @profiling_date
--          and a.scaling_segment_id in
--                 (select scaling_segment_id
--                         from vespa_analysts.SC2_Segments_Lookup_v2_1
--                        where hhcomposition = '04'
--                           or hhcomposition = '05'
--                           or hhcomposition = '06'
--                           or hhcomposition = '07')
--          and account_number in (select account_number from vespa_p_actual_age)
-- --                     Vespa_Analysts.Vespa_Single_Box_View sbv
-- --                    where cast(sbv.subscriber_id as int) in
-- --                             ( select cast(subscriber_id as int)
-- --                                 from vespa_analysts.alt_panel_data
-- --                                where panel = 12
-- --                                  and dt = @scaling_date))
-- commit
-- 
-- --Add household composition, age and a BARB age band column
-- alter table vespa_single_occupancy_type add (age int, age_band varchar(8))
-- 
-- --Take actual age from vespa_p_actual_age table
-- update vespa_single_occupancy_type a
--         set age = p_actual_age
--        from vespa_p_actual_age b
--       where a.account_number = b.account_number
-- 
-- --Set age to BARB age bands
-- update vespa_single_occupancy_type
--         set age_band = (case
--                 when age between  4 and  9 then   '4-9'
--                 when age between 10 and 15 then '10-15'
--                 when age between 16 and 19 then '16-19'
--                 when age between 20 and 24 then '20-24'
--                 when age between 25 and 34 then '25-34'
--                 when age between 35 and 44 then '35-44'
--                 when age between 45 and 64 then '45-64'
--                 else '65+' end)
-- 
-- select hhcomposition, age_band, count(*) from vespa_single_occupancy_type group by hhcomposition, age_band order by hhcomposition, age_band
-- end
-- 
-- --Find no. of accounts in the Vespa which have the following household composition
-- --which we later break down by age. This part of the code deals exclkusively with
-- --those accounts that have a repeated cb_key_individual
-- --(HHcomposition code in brackets afterwards)
-- --      single male             (04)
-- --      single female           (05)
-- --      male homesharers        (06)
-- --      female homesharers      (07)
-- 
-- --      single male             (04)
-- --      single female           (05)
-- --      male homesharers        (06)
-- --      female homesharers      (07)
-- 
-- begin
--         declare @scaling_date date
--         set     @scaling_date = '2013-07-14'
-- 
--         declare @profiling_date date
--         select  @profiling_date    = max(profiling_date)
--                                         from vespa_analysts.SC2_Sky_base_segment_snapshots
--                                         where profiling_date <= @scaling_date
-- 
-- -- --First of all, using the column cb_key_individual to link the tables
-- -- --cust_single_account_view and experian_consumerview find the common
-- -- --account_numbers and p_actual_age
-- -- --Note that there are approximately 1.8 million cb_key_individual records which
-- -- --are duplicates (relating to about 3.7 million rows in the table)
-- -- --To deal exclusively with the duplicates we have only included those cb_key_individual
-- -- --with counts greater than one
-- IF object_id('vespa_rep_p_actual_age') IS NOT NULL DROP TABLE vespa_rep_p_actual_age
-- select account_number, a.cb_key_household, a.cb_key_individual, p_actual_age
--         into vespa_rep_p_actual_age
--         from sk_prod.cust_single_account_view a
--   inner join (SELECT cb_key_household, cb_key_individual, MAX(p_actual_age) as p_actual_age
--                 FROM sk_prod.EXPERIAN_CONSUMERVIEW
--              GROUP BY cb_key_household, cb_key_individual
--               having count(cb_key_individual) > 1
--               ) as  b
--           on a.cb_key_individual = b.cb_key_individual
--        where account_number is not null
--          and account_number in
--                 (select account_number
--                     from Vespa_Analysts.Vespa_Single_Box_View sbv
--                    where cast(sbv.subscriber_id as int) in
--                             ( select cast(subscriber_id as int)
--                                 from vespa_analysts.alt_panel_data
--                                where panel = 12
--                                  and dt = @scaling_date))
-- 
-- commit
-- 
-- --Create tables containing all of the accounts numbers for each household composition of interest
-- IF object_id('vespa_rep_single_occupancy_type') IS NOT NULL DROP TABLE vespa_rep_single_occupancy_type
-- select       account_number, hhcomposition
--         into vespa_rep_single_occupancy_type
--         from vespa_analysts.SC2_Sky_base_segment_snapshots a
--   inner join vespa_analysts.SC2_Segments_Lookup_v2_1 b
--           on a.scaling_segment_id = b.scaling_segment_id
--        where profiling_date = @profiling_date
--          and a.scaling_segment_id in
--                 (select scaling_segment_id
--                         from vespa_analysts.SC2_Segments_Lookup_v2_1
--                        where hhcomposition = '04'
--                           or hhcomposition = '05')
--          and account_number in (select account_number from vespa_rep_p_actual_age)
-- --         and account_number in
-- --                 (select account_number
-- --                     from Vespa_Analysts.Vespa_Single_Box_View sbv
-- --                    where cast(sbv.subscriber_id as int) in
-- --                             ( select cast(subscriber_id as int)
-- --                                 from vespa_analysts.alt_panel_data
-- --                                where panel = 12
-- --                                  and dt = @scaling_date))
-- commit
-- 
-- --Add household composition, age and a BARB age band column
-- alter table vespa_rep_single_occupancy_type add (age int, age_band varchar(8))
-- 
-- --Take actual age from vespa_p_actual_age table
-- update      vespa_rep_single_occupancy_type a
--         set age = p_actual_age
--        from vespa_rep_p_actual_age b
--       where a.account_number = b.account_number
-- 
-- --Set age to BARB age bands
-- update vespa_rep_single_occupancy_type
--         set age_band = (case
--                 when age between  4 and  9 then   '4-9'
--                 when age between 10 and 15 then '10-15'
--                 when age between 16 and 19 then '16-19'
--                 when age between 20 and 24 then '20-24'
--                 when age between 25 and 34 then '25-34'
--                 when age between 35 and 44 then '35-44'
--                 when age between 45 and 64 then '45-64'
--                 else '65+' end)
-- 
-- select hhcomposition, age_band, count(*) from vespa_rep_single_occupancy_type group by hhcomposition, age_band order by hhcomposition, age_band
-- end
-- 
--
