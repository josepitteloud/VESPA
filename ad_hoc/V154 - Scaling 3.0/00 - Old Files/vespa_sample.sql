--Code to sample vespa accounts number and treat them as a proxy 'No Consent'
--group.
--Looking at 14th July 2013 so profiling date would need to be found
begin
        declare @scaling_date date
        set     @scaling_date = '2013-07-14'

--         declare @profiling_date date
--         select  @profiling_date    = max(dt)
--                                         from vespa_analysts.alt_panel_data
--                                         where dt <= @scaling_date
commit

    --Obtain subscriber_ids from alt_panel_data
    IF object_id('vespa_account_numbers') IS NOT NULL DROP TABLE vespa_account_numbers
    SELECT          intr.account_number
                   ,scaling_segment_id
            into    vespa_account_numbers
            from    vespa_analysts.SC2_intervals intr
      inner join (
                    select       sbv.account_number
                            from vespa_analysts.alt_panel_data alt
                      inner join Vespa_Analysts.Vespa_Single_Box_View sbv
                              on cast(alt.subscriber_id as int) = cast(sbv.subscriber_id as int)
                           where alt.panel = 12
                             and dt = @scaling_date) as sub1
              on intr.account_number = sub1.account_number
           where reporting_starts <= @scaling_date
             and reporting_ends   >= @scaling_date
         order by scaling_segment_id, intr.account_number
    commit

    --Create a new table with info from V154_account_numbers_variables, ignoring universe as this will be removed
    IF object_id('V154_accounts_without_universe') IS NOT NULL DROP TABLE V154_accounts_without_universe
    select       account_number
                ,isba_tv_region
                ,hhcomposition
                ,tenure
                ,package
                ,boxtype
                ,adsmartable
                ,adsmartable_capable
        into V154_accounts_without_universe
        from V154_account_numbers_variables
    commit

    --Drop from V154_accounts_without_universe where boxes are adsmartable but not adsmartable_capable
    delete from V154_accounts_without_universe
        where adsmartable = 1 and adsmartable_capable = 0
    commit

    --Variables used to decide which group of vespa the account is in
    declare @adsmartable            int
    declare @adsmartable_capable    int
    declare @adsmartable_ratio      real

    set     @adsmartable         = (select sum(adsmartable_no) from V154_account_numbers_variables)
    set     @adsmartable_capable = (select sum(adsmartable_capable_no) from V154_account_numbers_variables)
    set     @adsmartable_ratio   = 1.0 - 1.0*@adsmartable/@adsmartable_capable
    commit

    --Add columns to indicate if the account is in vespa or not, and see which of the adsmartable
    --universes the account is in.
    --The adsmartable universe will be assigned as follows:
    -- 0 - non-vespa
    -- 1 - in vespa but not adsmartable
    -- 2 - in vespa but considered to be 'non-consenting'
    -- 3 - in vespa with viewing consent
    alter table V154_accounts_without_universe
        add (vespa_indicator        int default 0
            ,vespa_non_adsmartable  int default 0
            ,vespa_no_consent       int default 0
            ,vespa_consent          int default 0
            ,adsmartable_universe   varchar(40)
    update      V154_accounts_without_universe
        set     vespa_indicator = 1
        where   account_number in (select account_number from vespa_account_numbers)
    update      V154_accounts_without_universe
        set     vespa_non_adsmartable = 1
        where   vespa_indicator = 1 and adsmartable = 0
    --Add a random number to the V154_accounts_without_universe table, which will be used to decide
    --which stays in the vespa and which goes into the proxy' no consent' panel
    alter table V154_accounts_without_universe add random_number real
    update V154_accounts_without_universe
        set random_number =  RAND(NUMBER(*)*(DATEPART(MS,NOW())+1))
      where vespa_indicator = 1
    create hg index indx_temp_rand on V154_accounts_without_universe(random_number)
    commit
    update      V154_accounts_without_universe
        set     vespa_no_consent = 1
        where   vespa_indicator = 1
        and     adsmartable = 1
        and     random_number < @adsmartable_ratio
    update      V154_accounts_without_universe
        set     vespa_consent = 1
        where   vespa_indicator = 1
        and     adsmartable = 1
        and     random_number >= @adsmartable_ratio
    commit

    --Final column adsmartable_indicator indicates if the account is in one of the following three states
    --0 Sky base, non-vespa
    --1 vespa non-adsmartable
    --2 vespa panellists, considered non-adsmartable for this exercise
    --3 vespa panellists and adsmartable
    update      V154_accounts_without_universe
        set     adsmartable_universe = (case
                        when adsmartable = 0 and adsmartable_capable = 0 then 'Not adsmartable'
                        when (adsmartable = 0 and adsmartable_capable = 1
                          or  adsmartable = 1 and adsmartable_capable = 1 and vespa_no_consent = 1) then 'Adsmartable but no consent'
                        when adsmartable = 1 and adsmartable_capable = 1 then 'Adsmartable with consent'
                end)
        from V154_accounts_without_universe
    commit

    --Create a new table with aggregated info from V154_accounts_without_universe
    IF object_id('V154_accounts_aggregated') IS NOT NULL DROP TABLE V154_accounts_aggregated
    CREATE TABLE V154_accounts_aggregated (
         updated_scaling_segment INT IDENTITY PRIMARY KEY
        ,isba_tv_region varchar(25)
        ,hhcomposition  varchar(5)
        ,tenure         varchar(25)
        ,package        varchar(25)
        ,boxtype        varchar(25)
        ,adsmartable_universe   varchar(40)
        ,sky_base_accounts      int
        ,vespa_accounts         int
--         ,vespa_non_adsmartable  int
--         ,vespa_adsmartable_but_no_consent int
--         ,vespa_adsmartable      int
     )
     commit

     --Insert into V154_accounts_aggregated count of sky base customers
     --Keeping order the same as in vespa_analysts.SC2_Segments_Lookup_v2_1
     insert into V154_accounts_aggregated (
         isba_tv_region
        ,hhcomposition
        ,tenure
        ,package
        ,boxtype
        ,adsmartable_universe
        ,sky_base_accounts
        ,vespa_accounts
--         ,vespa_non_adsmartable
--         ,vespa_adsmartable_but_no_consent
--         ,vespa_adsmartable
     )
     select distinct
         isba_tv_region
        ,hhcomposition
        ,tenure
        ,package
        ,boxtype
        ,adsmartable_universe
        ,count(*)
        ,sum(vespa_indicator)
--         ,sum(vespa_non_adsmartable)
--         ,sum(vespa_no_consent)
--         ,sum(vespa_consent)
        from V154_accounts_without_universe
        group by
                 adsmartable_universe
                ,isba_tv_region
                ,boxtype
                ,hhcomposition
                ,tenure
                ,package
        order by
                 adsmartable_universe
                ,package
                ,tenure
                ,hhcomposition
                ,boxtype
                ,isba_tv_region
     commit

end


