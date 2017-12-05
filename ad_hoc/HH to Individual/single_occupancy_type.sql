--Find no. of accounts in the SKy base which have the following household composition
--which we later break down by age
--(HHcomposition code in brackets afterwards)
--      single male             (04)
--      single female           (05)
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
IF object_id('accounts_p_actual_age') IS NOT NULL DROP TABLE accounts_p_actual_age
select account_number, a.cb_key_household, a.cb_key_individual, p_actual_age
        into accounts_p_actual_age
        from sk_prod.cust_single_account_view a
  inner join (SELECT cb_key_household, cb_key_individual, MAX(p_actual_age) as p_actual_age
                FROM sk_prod.EXPERIAN_CONSUMERVIEW
            GROUP BY cb_key_household, cb_key_individual
              ) as  b
          on a.cb_key_individual = b.cb_key_individual
commit

--Create tables containing all of the accounts numbers for each household composition of interest
IF object_id('accounts_single_occupancy_type') IS NOT NULL DROP TABLE accounts_single_occupancy_type
select       account_number, hhcomposition
        into accounts_single_occupancy_type
        from vespa_analysts.SC2_Sky_base_segment_snapshots a
  inner join vespa_analysts.SC2_Segments_Lookup_v2_1 b
          on a.scaling_segment_id = b.scaling_segment_id
       where profiling_date = @profiling_date
         and a.scaling_segment_id in
                (select scaling_segment_id
                        from vespa_analysts.SC2_Segments_Lookup_v2_1
                       where hhcomposition = '04'
                          or hhcomposition = '05'
                          or hhcomposition = '06'
                          or hhcomposition = '07')
commit

--Add household composition, age and a BARB age band column
alter table accounts_single_occupancy_type add (age int, age_band varchar(8))

--Take actual age from accounts_p_actual_age table
update accounts_single_occupancy_type a
        set age = p_actual_age
       from accounts_p_actual_age b
      where a.account_number = b.account_number

--Set age to BARB age bands
update accounts_single_occupancy_type
        set age_band = (case
                when age between  4 and  9 then   '4-9'
                when age between 10 and 15 then '10-15'
                when age between 16 and 19 then '16-19'
                when age between 20 and 24 then '20-24'
                when age between 25 and 34 then '25-34'
                when age between 35 and 44 then '35-44'
                when age between 45 and 64 then '45-64'
                else '65+' end)

select hhcomposition, age_band, count(*) from accounts_single_occupancy_type group by hhcomposition, age_band order by hhcomposition, age_band
end

--Find no. of accounts in the Vespa which have the following household composition
--which we later break down by age. This part of the code excludes any account with
--a repeated cb_key_individual
--(HHcomposition code in brackets afterwards)
--      single male             (04)
--      single female           (05)
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
--account_numbers in vespa and store cb_key_household, cb_key_individual and p_actual_age
--Note that there are approximately 1.8 million cb_key_individual records which
--are duplicates (relating to about 3.7 million rows in the table)
--To remove duplicates we have only included those cb_key_individual with count > 1
IF object_id('vespa_p_actual_age') IS NOT NULL DROP TABLE vespa_p_actual_age
select account_number, a.cb_key_household, a.cb_key_individual, p_actual_age
        into vespa_p_actual_age
        from sk_prod.cust_single_account_view a
  inner join (SELECT cb_key_household, cb_key_individual, MAX(p_actual_age) as p_actual_age
                FROM sk_prod.EXPERIAN_CONSUMERVIEW
            GROUP BY cb_key_household, cb_key_individual
              having count(cb_key_individual) = 1
              ) as  b
          on a.cb_key_individual = b.cb_key_individual
       where account_number is not null
         and account_number in
                (select account_number
                    from Vespa_Analysts.Vespa_Single_Box_View sbv
                   where cast(sbv.subscriber_id as int) in
                            ( select cast(subscriber_id as int)
                                from vespa_analysts.alt_panel_data
                               where panel = 12
                                 and dt = @scaling_date))
commit

--Create tables containing all of the accounts numbers for each household composition of interest
IF object_id('vespa_single_occupancy_type') IS NOT NULL DROP TABLE vespa_single_occupancy_type
-- select       *
--         into vespa_single_occupancy_type
--         from accounts_single_occupancy_type
--        where account_number in
--                 (select account_number
--                     from Vespa_Analysts.Vespa_Single_Box_View sbv
--               inner join vespa_analysts.alt_panel_data alt
--                       on cast(sbv.subscriber_id as int) = cast(alt.subscriber_id as int)
--                    where alt.panel = 12
--                      and dt = @scaling_date)

select       account_number, hhcomposition
        into vespa_single_occupancy_type
        from vespa_analysts.SC2_Sky_base_segment_snapshots a
  inner join vespa_analysts.SC2_Segments_Lookup_v2_1 b
          on a.scaling_segment_id = b.scaling_segment_id
       where profiling_date = @profiling_date
         and a.scaling_segment_id in
                (select scaling_segment_id
                        from vespa_analysts.SC2_Segments_Lookup_v2_1
                       where hhcomposition = '04'
                          or hhcomposition = '05'
                          or hhcomposition = '06'
                          or hhcomposition = '07')
         and account_number in (select account_number from vespa_p_actual_age)
--                     Vespa_Analysts.Vespa_Single_Box_View sbv
--                    where cast(sbv.subscriber_id as int) in
--                             ( select cast(subscriber_id as int)
--                                 from vespa_analysts.alt_panel_data
--                                where panel = 12
--                                  and dt = @scaling_date))
commit

--Add household composition, age and a BARB age band column
alter table vespa_single_occupancy_type add (age int, age_band varchar(8))

--Take actual age from vespa_p_actual_age table
update vespa_single_occupancy_type a
        set age = p_actual_age
       from vespa_p_actual_age b
      where a.account_number = b.account_number

--Set age to BARB age bands
update vespa_single_occupancy_type
        set age_band = (case
                when age between  4 and  9 then   '4-9'
                when age between 10 and 15 then '10-15'
                when age between 16 and 19 then '16-19'
                when age between 20 and 24 then '20-24'
                when age between 25 and 34 then '25-34'
                when age between 35 and 44 then '35-44'
                when age between 45 and 64 then '45-64'
                else '65+' end)

select hhcomposition, age_band, count(*) from vespa_single_occupancy_type group by hhcomposition, age_band order by hhcomposition, age_band
end

--Find no. of accounts in the Vespa which have the following household composition
--which we later break down by age. This part of the code deals exclkusively with
--those accounts that have a repeated cb_key_individual
--(HHcomposition code in brackets afterwards)
--      single male             (04)
--      single female           (05)
--      male homesharers        (06)
--      female homesharers      (07)

--      single male             (04)
--      single female           (05)
--      male homesharers        (06)
--      female homesharers      (07)

begin
        declare @scaling_date date
        set     @scaling_date = '2013-07-14'

        declare @profiling_date date
        select  @profiling_date    = max(profiling_date)
                                        from vespa_analysts.SC2_Sky_base_segment_snapshots
                                        where profiling_date <= @scaling_date

-- --First of all, using the column cb_key_individual to link the tables
-- --cust_single_account_view and experian_consumerview find the common
-- --account_numbers and p_actual_age
-- --Note that there are approximately 1.8 million cb_key_individual records which
-- --are duplicates (relating to about 3.7 million rows in the table)
-- --To deal exclusively with the duplicates we have only included those cb_key_individual
-- --with counts greater than one
IF object_id('vespa_rep_p_actual_age') IS NOT NULL DROP TABLE vespa_rep_p_actual_age
select account_number, a.cb_key_household, a.cb_key_individual, p_actual_age
        into vespa_rep_p_actual_age
        from sk_prod.cust_single_account_view a
  inner join (SELECT cb_key_household, cb_key_individual, MAX(p_actual_age) as p_actual_age
                FROM sk_prod.EXPERIAN_CONSUMERVIEW
             GROUP BY cb_key_household, cb_key_individual
              having count(cb_key_individual) > 1
              ) as  b
          on a.cb_key_individual = b.cb_key_individual
       where account_number is not null
         and account_number in
                (select account_number
                    from Vespa_Analysts.Vespa_Single_Box_View sbv
                   where cast(sbv.subscriber_id as int) in
                            ( select cast(subscriber_id as int)
                                from vespa_analysts.alt_panel_data
                               where panel = 12
                                 and dt = @scaling_date))

commit

--Create tables containing all of the accounts numbers for each household composition of interest
IF object_id('vespa_rep_single_occupancy_type') IS NOT NULL DROP TABLE vespa_rep_single_occupancy_type
select       account_number, hhcomposition
        into vespa_rep_single_occupancy_type
        from vespa_analysts.SC2_Sky_base_segment_snapshots a
  inner join vespa_analysts.SC2_Segments_Lookup_v2_1 b
          on a.scaling_segment_id = b.scaling_segment_id
       where profiling_date = @profiling_date
         and a.scaling_segment_id in
                (select scaling_segment_id
                        from vespa_analysts.SC2_Segments_Lookup_v2_1
                       where hhcomposition = '04'
                          or hhcomposition = '05')
         and account_number in (select account_number from vespa_rep_p_actual_age)
--         and account_number in
--                 (select account_number
--                     from Vespa_Analysts.Vespa_Single_Box_View sbv
--                    where cast(sbv.subscriber_id as int) in
--                             ( select cast(subscriber_id as int)
--                                 from vespa_analysts.alt_panel_data
--                                where panel = 12
--                                  and dt = @scaling_date))
commit

--Add household composition, age and a BARB age band column
alter table vespa_rep_single_occupancy_type add (age int, age_band varchar(8))

--Take actual age from vespa_p_actual_age table
update      vespa_rep_single_occupancy_type a
        set age = p_actual_age
       from vespa_rep_p_actual_age b
      where a.account_number = b.account_number

--Set age to BARB age bands
update vespa_rep_single_occupancy_type
        set age_band = (case
                when age between  4 and  9 then   '4-9'
                when age between 10 and 15 then '10-15'
                when age between 16 and 19 then '16-19'
                when age between 20 and 24 then '20-24'
                when age between 25 and 34 then '25-34'
                when age between 35 and 44 then '35-44'
                when age between 45 and 64 then '45-64'
                else '65+' end)

select hhcomposition, age_band, count(*) from vespa_rep_single_occupancy_type group by hhcomposition, age_band order by hhcomposition, age_band
end


