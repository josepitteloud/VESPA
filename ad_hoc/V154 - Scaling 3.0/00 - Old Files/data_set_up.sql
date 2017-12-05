-- Attempt to coerce account info into a table from which we can use PCA / MCA
-- or some other multivariate technique
begin
        declare @scaling_date date
        set     @scaling_date = '2013-07-14'

        declare @profiling_date date
        select  @profiling_date    = max(profiling_date)
                                        from vespa_analysts.SC2_Sky_base_segment_snapshots
                                        where profiling_date <= @scaling_date

--Create table holding account_numbers for date in question
IF object_id('V154_account_numbers') IS NOT NULL DROP TABLE V154_account_numbers
         select account_number, scaling_segment_id
                 into V154_account_numbers
                 from vespa_analysts.SC2_Sky_base_segment_snapshots
                 where profiling_date = @profiling_date
commit

create hg index hg_acc_index on V154_account_numbers(account_number)
commit

-- Join the current account numbers with the values of the variables for each scaling segment
IF object_id('V154_account_numbers_variables') IS NOT NULL DROP TABLE V154_account_numbers_variables
         select  a.account_number
                ,a.scaling_segment_id
                ,universe
                ,isba_tv_region
                ,hhcomposition
                ,tenure
                ,package
                ,boxtype
                 into V154_account_numbers_variables
                 from V154_account_numbers a
           inner join vespa_analysts.SC2_Segments_Lookup_v2_1 b
                   on a.scaling_segment_id = b.scaling_segment_id
commit

-- Add adsmartable variable to the table and join to created table
-- The difference between adsmartable and adsmartable_capable is that
-- adsmartable are people who have adsmart capable boxes and have given
-- viewing consent
alter table V154_account_numbers_variables
        add (adsmartable        int default 0
            ,adsmartable_no     int)
commit

alter table V154_account_numbers_variables
        add (adsmartable_capable        int default 0
            ,adsmartable_capable_no     int)
commit

update      V154_account_numbers_variables
        set adsmartable = 1
      where account_number in (
     select account_number
       from adsmart_account_numbers
      where flag = 1)
commit

update      V154_account_numbers_variables
        set adsmartable_capable = 1
      where account_number in (
     select account_number
       from adsmart_capable_account_numbers
      where flag = 1)
commit

-- add columns to V154_account_numbers_variables and convert values to numbers
-- note the numbers are in the order that they appear in on the Vespa panel management forms
alter table V154_account_numbers_variables
        add (universe_no        int
            ,isba_tv_region_no  int
            ,hhcomposition_no   int
            ,tenure_no          int
            ,package_no         int
            ,boxtype_no         int)
commit

update V154_account_numbers_variables a
        set universe_no = (case
                when universe = 'A) Single box HH' then 1
                when universe = 'B) Multiple box HH' then 2
                else 0 end)
commit

update V154_account_numbers_variables a
        set isba_tv_region_no = (case
                when isba_tv_region = 'Border' then 1
                when isba_tv_region = 'Central Scotland' then 2
                when isba_tv_region = 'East Of England' then 3
                when isba_tv_region = 'HTV Wales' then 4
                when isba_tv_region = 'HTV West' then 5
                when isba_tv_region = 'London' then 6
                when isba_tv_region = 'Meridian (exc. Chann' then 7
                when isba_tv_region = 'Midlands' then 8
                when isba_tv_region = 'North East' then 9
                when isba_tv_region = 'North Scotland' then 10
                when isba_tv_region = 'North West' then 11
                when isba_tv_region = 'Not Defined' then 12
                when isba_tv_region = 'South West' then 13
                when isba_tv_region = 'Ulster' then 14
                when isba_tv_region = 'Yorkshire' then 15
                else 0 end)
commit

update V154_account_numbers_variables a
        set hhcomposition_no = (case
                when hhcomposition = '00' then 1
                when hhcomposition = '01' then 2
                when hhcomposition = '02' then 3
                when hhcomposition = '03' then 4
                when hhcomposition = '04' then 5
                when hhcomposition = '05' then 6
                when hhcomposition = '06' then 7
                when hhcomposition = '07' then 8
                when hhcomposition = '08' then 9
                when hhcomposition = '09' then 10
                when hhcomposition = '10' then 11
                when hhcomposition = '11' then 12
                when hhcomposition = 'U' then 13
                else 0 end)
commit

update V154_account_numbers_variables a
        set tenure_no = (case
                when tenure = 'A) 0-2 Years' then 1
                when tenure = 'B) 3-10 Years' then 2
                when tenure = 'C) 10 Years+' then 3
                when tenure = 'D) Unknown' then 4
                else 0 end)
commit

update V154_account_numbers_variables a
        set package_no = (case
                when package = 'Basic - Ent' then 1
                when package = 'Basic - Ent Extra' then 2
                when package = 'Dual Movies' then 3
                when package = 'Dual Sports' then 4
                when package = 'Other Premiums' then 5
                when package = 'Single Movies' then 6
                when package = 'Single Sports' then 7
                when package = 'Top Tier' then 8
                else 0 end)
commit

update V154_account_numbers_variables a
        set boxtype_no = (case
                when boxtype = 'A) HDx & No_secondary_box' then 1
                when boxtype = 'B) HD & No_secondary_box' then 2
                when boxtype = 'C) Skyplus & No_secondary_box' then 3
                when boxtype = 'D) FDB & No_secondary_box' then 4
                when boxtype = 'E) HD & HD' then 5
                when boxtype = 'F) HD & Skyplus' then 6
                when boxtype = 'G) HD & FDB' then 7
                when boxtype = 'H) HDx & HDx' then 8
                when boxtype = 'I) HDx & Skyplus' then 9
                when boxtype = 'J) HDx & FDB' then 10
                when boxtype = 'K) Skyplus & Skyplus' then 11
                when boxtype = 'L) Skyplus & FDB' then 12
                when boxtype = 'M) FDB & FDB' then 13
                else 0 end)
commit

--For the moment keep adsmartable_no the same as adsmartable since it is the only either/or variable
update V154_account_numbers_variables
        set adsmartable_no = adsmartable
commit

--Update 17/09/13
--As we altered the interpretation of adsmart capable and adsmartable boxes to make them as
--consistent as possible (using the code utilised in the weekly reports) we add these updated values
--to the table V154_account_numbers_variables. We have not updated the latter code as it was not considered
--a priority sinece we are not doing any more PCA work.
alter table V154_account_numbers_variables
        add (updated_adsmart_capable    tinyint default 0
            ,updated_adsmartable        tinyint default 0)
commit
update      V154_account_numbers_variables
        set updated_adsmart_capable = 1
      where account_number in (
     select account_number
       from adsmart_capable_account_numbers
      where flag = 1)
commit
update      V154_account_numbers_variables
        set updated_adsmartable = 1
      where account_number in (
     select account_number
       from adsmart_account_numbers
      where flag = 1)
commit

-- declare  @avg_universe_no double
-- set      @avg_universe_no = (select avg(universe_no) from V154_account_numbers_variables)
-- declare  @avg_isba_tv_region_no double
-- set      @avg_isba_tv_region_no = (select avg(isba_tv_region_no) from V154_account_numbers_variables)
-- declare  @avg_hhcomposition_no double
-- set      @avg_hhcomposition_no = (select avg(hhcomposition_no) from V154_account_numbers_variables)
-- declare  @avg_tenure_no double
-- set      @avg_tenure_no = (select avg(tenure_no) from V154_account_numbers_variables)
-- declare  @avg_package_no double
-- set      @avg_package_no = (select avg(package_no) from V154_account_numbers_variables)
-- declare  @avg_boxtype_no double
-- set      @avg_boxtype_no = (select avg(boxtype_no) from V154_account_numbers_variables)
-- declare  @avg_adsmartable_no double
-- set      @avg_adsmartable_no = (select avg(adsmartable_no) from V154_account_numbers_variables)
-- commit
--
-- IF object_id('V154_account_data') IS NOT NULL DROP TABLE drop table V154_account_data
--          select  account_number
--                 ,universe_no - @avg_universe_no as universe_no
--                 ,isba_tv_region_no - @avg_isba_tv_region_no as isba_tv_region_no
--                 ,hhcomposition_no - @avg_hhcomposition_no as hhcomposition_no
--                 ,tenure_no - @avg_tenure_no as tenure_no
--                 ,package_no - @avg_package_no as package_no
--                 ,boxtype_no - @avg_boxtype_no as boxtype_no
--                 ,adsmartable_no - @avg_adsmartable_no as adsmartable_no
--                  into V154_account_data
--                  from V154_account_numbers_variables
-- commit

--Create an X'X matrix. Rather than create a 7 x 7 (symmetric) matrix we shall enter the
--indices in the first two columns as the data will later be entered into R.
--Used R to create all of the insert into statement
IF object_id('V154_X_data') IS NOT NULL DROP TABLE V154_X_data
create table V154_X_data (
         i       int
        ,j       int
        ,value   double
)

insert into V154_X_data select 1, 1, COVAR_SAMP(universe_no, universe_no) from V154_account_numbers_variables
insert into V154_X_data select 2, 1, COVAR_SAMP(isba_tv_region_no, universe_no) from V154_account_numbers_variables
insert into V154_X_data select 2, 2, COVAR_SAMP(isba_tv_region_no, isba_tv_region_no) from V154_account_numbers_variables
insert into V154_X_data select 3, 1, COVAR_SAMP(hhcomposition_no, universe_no) from V154_account_numbers_variables
insert into V154_X_data select 3, 2, COVAR_SAMP(hhcomposition_no, isba_tv_region_no) from V154_account_numbers_variables
insert into V154_X_data select 3, 3, COVAR_SAMP(hhcomposition_no, hhcomposition_no) from V154_account_numbers_variables
insert into V154_X_data select 4, 1, COVAR_SAMP(tenure_no, universe_no) from V154_account_numbers_variables
insert into V154_X_data select 4, 2, COVAR_SAMP(tenure_no, isba_tv_region_no) from V154_account_numbers_variables
insert into V154_X_data select 4, 3, COVAR_SAMP(tenure_no, hhcomposition_no) from V154_account_numbers_variables
insert into V154_X_data select 4, 4, COVAR_SAMP(tenure_no, tenure_no) from V154_account_numbers_variables
insert into V154_X_data select 5, 1, COVAR_SAMP(package_no, universe_no) from V154_account_numbers_variables
insert into V154_X_data select 5, 2, COVAR_SAMP(package_no, isba_tv_region_no) from V154_account_numbers_variables
insert into V154_X_data select 5, 3, COVAR_SAMP(package_no, hhcomposition_no) from V154_account_numbers_variables
insert into V154_X_data select 5, 4, COVAR_SAMP(package_no, tenure_no) from V154_account_numbers_variables
insert into V154_X_data select 5, 5, COVAR_SAMP(package_no, package_no) from V154_account_numbers_variables
insert into V154_X_data select 6, 1, COVAR_SAMP(boxtype_no, universe_no) from V154_account_numbers_variables
insert into V154_X_data select 6, 2, COVAR_SAMP(boxtype_no, isba_tv_region_no) from V154_account_numbers_variables
insert into V154_X_data select 6, 3, COVAR_SAMP(boxtype_no, hhcomposition_no) from V154_account_numbers_variables
insert into V154_X_data select 6, 4, COVAR_SAMP(boxtype_no, tenure_no) from V154_account_numbers_variables
insert into V154_X_data select 6, 5, COVAR_SAMP(boxtype_no, package_no) from V154_account_numbers_variables
insert into V154_X_data select 6, 6, COVAR_SAMP(boxtype_no, boxtype_no) from V154_account_numbers_variables
insert into V154_X_data select 7, 1, COVAR_SAMP(adsmartable_no, universe_no) from V154_account_numbers_variables
insert into V154_X_data select 7, 2, COVAR_SAMP(adsmartable_no, isba_tv_region_no) from V154_account_numbers_variables
insert into V154_X_data select 7, 3, COVAR_SAMP(adsmartable_no, hhcomposition_no) from V154_account_numbers_variables
insert into V154_X_data select 7, 4, COVAR_SAMP(adsmartable_no, tenure_no) from V154_account_numbers_variables
insert into V154_X_data select 7, 5, COVAR_SAMP(adsmartable_no, package_no) from V154_account_numbers_variables
insert into V154_X_data select 7, 6, COVAR_SAMP(adsmartable_no, boxtype_no) from V154_account_numbers_variables
insert into V154_X_data select 7, 7, COVAR_SAMP(adsmartable_no, adsmartable_no) from V154_account_numbers_variables
commit
select top 20 * from V154_account_numbers_variables

select corr(boxtype_no, adsmartable_no) from V154_account_numbers_variables
select corr(boxtype_no, adsmartable_capable_no) from V154_account_numbers_variables

grant all on V154_X_data to public
grant all on V154_account_numbers_variables to public
grant all on V154_account_data to public
commit

--Create table holding the means of all of the variables. Required when recreating
--covariance matrix in R
IF object_id('V154_X_mean_n') IS NOT NULL DROP TABLE V154_X_mean_n
select avg(universe_no) as avg_uni_no
      ,avg(isba_tv_region_no) as avg_region_no
      ,avg(hhcomposition_no) as avg_hh_no
      ,avg(tenure_no) as avg_tenure_no
      ,avg(package_no) as avg_package_no
      ,avg(boxtype_no) as avg_box_no
      ,avg(adsmartable_no) as avg_adsmart_no
      ,count(*) as n
       into V154_X_mean_n
       from V154_account_numbers_variables
commit

--Create a table of weighted V154_X_data, i.e. the values in V154_X_data, but with a 'weights' column
--telling us how many times they are repeated.
IF object_id('V154_X_weighted_data') IS NOT NULL DROP TABLE V154_X_weighted_data
select  universe
       ,isba_tv_region
       ,hhcomposition
       ,tenure
       ,package
       ,boxtype
       ,adsmartable
       ,universe_no
       ,isba_tv_region_no
       ,hhcomposition_no
       ,tenure_no
       ,package_no
       ,boxtype_no
       ,adsmartable_no
       ,count(*) as weights
        into V154_X_weighted_data
        from V154_account_numbers_variables
    group by    universe
               ,isba_tv_region
               ,hhcomposition
               ,tenure
               ,package
               ,boxtype
               ,adsmartable
               ,universe_no
               ,isba_tv_region_no
               ,hhcomposition_no
               ,tenure_no
               ,package_no
               ,boxtype_no
               ,adsmartable_no
commit

grant all on V154_X_mean_n to public
grant all on V154_X_weighted_data to public
commit
end

select adsmartable, count(*) from V154_account_numbers_variables group by adsmartable


--Extra code when we separate out adsmartable and non-adsmartable data
begin
select *
        into #temp_V154_adsmartable
        from V154_account_numbers_variables
       where adsmartable = 1
commit

-- declare  @avg_universe_no double
-- set      @avg_universe_no = (select avg(universe_no) from #temp_V154_adsmartable)
-- declare  @avg_isba_tv_region_no double
-- set      @avg_isba_tv_region_no = (select avg(isba_tv_region_no) from #temp_V154_adsmartable)
-- declare  @avg_hhcomposition_no double
-- set      @avg_hhcomposition_no = (select avg(hhcomposition_no) from #temp_V154_adsmartable)
-- declare  @avg_tenure_no double
-- set      @avg_tenure_no = (select avg(tenure_no) from #temp_V154_adsmartable)
-- declare  @avg_package_no double
-- set      @avg_package_no = (select avg(package_no) from #temp_V154_adsmartable)
-- declare  @avg_boxtype_no double
-- set      @avg_boxtype_no = (select avg(boxtype_no) from #temp_V154_adsmartable)
-- commit
--
-- IF object_id('#temp_V154_account') IS NOT NULL DROP TABLE #temp_V154_account
--          select  account_number
--                 ,universe_no - @avg_universe_no as universe_no
--                 ,isba_tv_region_no - @avg_isba_tv_region_no as isba_tv_region_no
--                 ,hhcomposition_no - @avg_hhcomposition_no as hhcomposition_no
--                 ,tenure_no - @avg_tenure_no as tenure_no
--                 ,package_no - @avg_package_no as package_no
--                 ,boxtype_no - @avg_boxtype_no as boxtype_no
--                  into #temp_V154_account
--                  from V154_account_numbers_variables
-- commit
-- 
--Create an X'X matrix. Rather than create a 6 x 6 (symmetric) matrix we shall enter the
--indices in the first two columns as the data will later be entered into R.
--Used R to create all of the insert into statement
IF object_id('temp_V154_adsmartable_data') IS NOT NULL DROP TABLE temp_V154_adsmartable_data
create table temp_V154_adsmartable_data (
         i       int
        ,j       int
        ,value   double
)

insert into temp_V154_adsmartable_data select 1, 1, COVAR_SAMP(universe_no, universe_no) from #temp_V154_adsmartable
insert into temp_V154_adsmartable_data select 2, 1, COVAR_SAMP(isba_tv_region_no, universe_no) from #temp_V154_adsmartable
insert into temp_V154_adsmartable_data select 2, 2, COVAR_SAMP(isba_tv_region_no, isba_tv_region_no) from #temp_V154_adsmartable
insert into temp_V154_adsmartable_data select 3, 1, COVAR_SAMP(hhcomposition_no, universe_no) from #temp_V154_adsmartable
insert into temp_V154_adsmartable_data select 3, 2, COVAR_SAMP(hhcomposition_no, isba_tv_region_no) from #temp_V154_adsmartable
insert into temp_V154_adsmartable_data select 3, 3, COVAR_SAMP(hhcomposition_no, hhcomposition_no) from #temp_V154_adsmartable
insert into temp_V154_adsmartable_data select 4, 1, COVAR_SAMP(tenure_no, universe_no) from #temp_V154_adsmartable
insert into temp_V154_adsmartable_data select 4, 2, COVAR_SAMP(tenure_no, isba_tv_region_no) from #temp_V154_adsmartable
insert into temp_V154_adsmartable_data select 4, 3, COVAR_SAMP(tenure_no, hhcomposition_no) from #temp_V154_adsmartable
insert into temp_V154_adsmartable_data select 4, 4, COVAR_SAMP(tenure_no, tenure_no) from #temp_V154_adsmartable
insert into temp_V154_adsmartable_data select 5, 1, COVAR_SAMP(package_no, universe_no) from #temp_V154_adsmartable
insert into temp_V154_adsmartable_data select 5, 2, COVAR_SAMP(package_no, isba_tv_region_no) from #temp_V154_adsmartable
insert into temp_V154_adsmartable_data select 5, 3, COVAR_SAMP(package_no, hhcomposition_no) from #temp_V154_adsmartable
insert into temp_V154_adsmartable_data select 5, 4, COVAR_SAMP(package_no, tenure_no) from #temp_V154_adsmartable
insert into temp_V154_adsmartable_data select 5, 5, COVAR_SAMP(package_no, package_no) from #temp_V154_adsmartable
insert into temp_V154_adsmartable_data select 6, 1, COVAR_SAMP(boxtype_no, universe_no) from #temp_V154_adsmartable
insert into temp_V154_adsmartable_data select 6, 2, COVAR_SAMP(boxtype_no, isba_tv_region_no) from #temp_V154_adsmartable
insert into temp_V154_adsmartable_data select 6, 3, COVAR_SAMP(boxtype_no, hhcomposition_no) from #temp_V154_adsmartable
insert into temp_V154_adsmartable_data select 6, 4, COVAR_SAMP(boxtype_no, tenure_no) from #temp_V154_adsmartable
insert into temp_V154_adsmartable_data select 6, 5, COVAR_SAMP(boxtype_no, package_no) from #temp_V154_adsmartable
insert into temp_V154_adsmartable_data select 6, 6, COVAR_SAMP(boxtype_no, boxtype_no) from #temp_V154_adsmartable
commit
end

begin
select *
        into #temp_V154_non_adsmartable
        from V154_account_numbers_variables
       where adsmartable = 0
commit

-- declare  @avg_universe_no double
-- set      @avg_universe_no = (select avg(universe_no) from #temp_V154_non_adsmartable)
-- declare  @avg_isba_tv_region_no double
-- set      @avg_isba_tv_region_no = (select avg(isba_tv_region_no) from #temp_V154_non_adsmartable)
-- declare  @avg_hhcomposition_no double
-- set      @avg_hhcomposition_no = (select avg(hhcomposition_no) from #temp_V154_non_adsmartable)
-- declare  @avg_tenure_no double
-- set      @avg_tenure_no = (select avg(tenure_no) from #temp_V154_non_adsmartable)
-- declare  @avg_package_no double
-- set      @avg_package_no = (select avg(package_no) from #temp_V154_non_adsmartable)
-- declare  @avg_boxtype_no double
-- set      @avg_boxtype_no = (select avg(boxtype_no) from #temp_V154_non_adsmartable)
-- commit
--
-- IF object_id('#temp_V154_adsmartable') IS NOT NULL DROP TABLE #temp_V154_adsmartable
--          select  account_number
--                 ,universe_no - @avg_universe_no as universe_no
--                 ,isba_tv_region_no - @avg_isba_tv_region_no as isba_tv_region_no
--                 ,hhcomposition_no - @avg_hhcomposition_no as hhcomposition_no
--                 ,tenure_no - @avg_tenure_no as tenure_no
--                 ,package_no - @avg_package_no as package_no
--                 ,boxtype_no - @avg_boxtype_no as boxtype_no
--                  into #temp_V154_adsmartable
--                  from V154_account_numbers_variables
-- commit
-- 
--Create an X'X matrix. Rather than create a 6 x 6 (symmetric) matrix we shall enter the
--indices in the first two columns as the data will later be entered into R.
--Used R to create all of the insert into statement
IF object_id('temp_V154_non_adsmartable_data') IS NOT NULL DROP TABLE temp_V154_non_adsmartable_data
create table temp_V154_non_adsmartable_data (
         i       int
        ,j       int
        ,value   double
)

insert into temp_V154_non_adsmartable_data select 1, 1, COVAR_SAMP(universe_no, universe_no) from #temp_V154_non_adsmartable
insert into temp_V154_non_adsmartable_data select 2, 1, COVAR_SAMP(isba_tv_region_no, universe_no) from #temp_V154_non_adsmartable
insert into temp_V154_non_adsmartable_data select 2, 2, COVAR_SAMP(isba_tv_region_no, isba_tv_region_no) from #temp_V154_non_adsmartable
insert into temp_V154_non_adsmartable_data select 3, 1, COVAR_SAMP(hhcomposition_no, universe_no) from #temp_V154_non_adsmartable
insert into temp_V154_non_adsmartable_data select 3, 2, COVAR_SAMP(hhcomposition_no, isba_tv_region_no) from #temp_V154_non_adsmartable
insert into temp_V154_non_adsmartable_data select 3, 3, COVAR_SAMP(hhcomposition_no, hhcomposition_no) from #temp_V154_non_adsmartable
insert into temp_V154_non_adsmartable_data select 4, 1, COVAR_SAMP(tenure_no, universe_no) from #temp_V154_non_adsmartable
insert into temp_V154_non_adsmartable_data select 4, 2, COVAR_SAMP(tenure_no, isba_tv_region_no) from #temp_V154_non_adsmartable
insert into temp_V154_non_adsmartable_data select 4, 3, COVAR_SAMP(tenure_no, hhcomposition_no) from #temp_V154_non_adsmartable
insert into temp_V154_non_adsmartable_data select 4, 4, COVAR_SAMP(tenure_no, tenure_no) from #temp_V154_non_adsmartable
insert into temp_V154_non_adsmartable_data select 5, 1, COVAR_SAMP(package_no, universe_no) from #temp_V154_non_adsmartable
insert into temp_V154_non_adsmartable_data select 5, 2, COVAR_SAMP(package_no, isba_tv_region_no) from #temp_V154_non_adsmartable
insert into temp_V154_non_adsmartable_data select 5, 3, COVAR_SAMP(package_no, hhcomposition_no) from #temp_V154_non_adsmartable
insert into temp_V154_non_adsmartable_data select 5, 4, COVAR_SAMP(package_no, tenure_no) from #temp_V154_non_adsmartable
insert into temp_V154_non_adsmartable_data select 5, 5, COVAR_SAMP(package_no, package_no) from #temp_V154_non_adsmartable
insert into temp_V154_non_adsmartable_data select 6, 1, COVAR_SAMP(boxtype_no, universe_no) from #temp_V154_non_adsmartable
insert into temp_V154_non_adsmartable_data select 6, 2, COVAR_SAMP(boxtype_no, isba_tv_region_no) from #temp_V154_non_adsmartable
insert into temp_V154_non_adsmartable_data select 6, 3, COVAR_SAMP(boxtype_no, hhcomposition_no) from #temp_V154_non_adsmartable
insert into temp_V154_non_adsmartable_data select 6, 4, COVAR_SAMP(boxtype_no, tenure_no) from #temp_V154_non_adsmartable
insert into temp_V154_non_adsmartable_data select 6, 5, COVAR_SAMP(boxtype_no, package_no) from #temp_V154_non_adsmartable
insert into temp_V154_non_adsmartable_data select 6, 6, COVAR_SAMP(boxtype_no, boxtype_no) from #temp_V154_non_adsmartable
commit
end

grant all on V154_account_numbers_variables to public
grant SELECT on V154_X_data to public
grant SELECT on temp_V154_adsmartable_data to public
grant SELECT on temp_V154_non_adsmartable_data to public
