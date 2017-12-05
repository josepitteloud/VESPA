--Code to sample vespa accounts number and treat them as a proxy 'No Consent'
--group.
--Plan is to find the proportion within each scaling segment who do not give
--viewing consent and set the proportion within vespa to be the same. E.g. if 10%
--of the Sky Base in scaling segment 1 do not give viewing consent then 10% of the
--vespa panellists in scaling segment 1 should be considered 'No Consent' viewers.

--Looking at 14th July 2013 so profiling date would need to be found.
begin
        declare @scaling_date date
        set     @scaling_date = '2013-07-14'
commit
end
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

    --Create a new table with info from V154_account_numbers_variables, this table will be updated later
    IF object_id('V154_accounts_proxy_consent') IS NOT NULL DROP TABLE V154_accounts_proxy_consent
    select       account_number
                ,isba_tv_region
                ,hhcomposition
                ,tenure
                ,package
                ,boxtype
                ,updated_adsmart_capable as adsmart_capable
                ,updated_adsmartable as adsmartable
        into V154_accounts_proxy_consent
        from V154_account_numbers_variables
    commit

    --Create a new table with updated segments (e.g. old universe removed, sky base universe added)
    IF object_id('V154_updated_segments') IS NOT NULL DROP TABLE V154_updated_segments
    CREATE TABLE V154_updated_segments (
         updated_scaling_segment_id INT IDENTITY PRIMARY KEY
        ,isba_tv_region varchar(25)
        ,hhcomposition  varchar(5)
        ,tenure         varchar(25)
        ,package        varchar(25)
        ,boxtype        varchar(25)
        ,adsmartable_universe   varchar(40)
     )
     commit

     --Insert into V154_updated_segments segments from vespa_analysts.SC2_Segments_Lookup_v2_1
     --Keeping the same order as the segments in vespa_analysts.SC2_Segments_Lookup_v2_1, however
     --we only create the 'adsmartable_universe' here (i.e. is it adsmartable or not). If we were
     --to create the sky_base_universe segment here, then we would have zeroes or ones when we
     --calculate the proportion of those who have given viewing consent in each segment
     insert into V154_updated_segments (
         isba_tv_region
        ,hhcomposition
        ,tenure
        ,package
        ,boxtype
        ,adsmartable_universe
     )
     select distinct
         isba_tv_region
        ,hhcomposition
        ,tenure
        ,package
        ,boxtype
        ,'Adsmartable'
        from vespa_analysts.SC2_Segments_Lookup_v2_1
        order by
                 package
                ,tenure
                ,hhcomposition
                ,boxtype
                ,isba_tv_region
     commit
     insert into V154_updated_segments (
         isba_tv_region
        ,hhcomposition
        ,tenure
        ,package
        ,boxtype
        ,adsmartable_universe
     )
     select distinct
         isba_tv_region
        ,hhcomposition
        ,tenure
        ,package
        ,boxtype
        ,'Not adsmartable'
        from vespa_analysts.SC2_Segments_Lookup_v2_1
        order by
                 package
                ,tenure
                ,hhcomposition
                ,boxtype
                ,isba_tv_region
     commit

     --Update V154_accounts_proxy_consent to include sky_base_universe1
     alter table V154_accounts_proxy_consent add adsmartable_universe varchar(40)
     update      V154_accounts_proxy_consent
        set      adsmartable_universe = case
                        when adsmart_capable = 1 then 'Adsmartable'
                        when adsmart_capable = 0 then 'Not adsmartable'
                        else 'Unknown' end

     alter table V154_accounts_proxy_consent add sky_base_universe varchar(40)
     update      V154_accounts_proxy_consent
        set      sky_base_universe = case
                        when adsmart_capable = 1 and adsmartable = 1 then 'Adsmartable with consent'
                        when adsmart_capable = 1 and adsmartable = 0 then 'Adsmartable but no consent'
                        when adsmart_capable = 0 and adsmartable = 0 then 'Not adsmartable'
                        else 'Unknown' end
     commit

     --Update V154_accounts_proxy_consent to include (intermediate) scaling segment ids
     --Note, have to use substring for boxtype as, for some reason, boxtype 'C)...' doesn't link across tables
     alter table V154_accounts_proxy_consent add updated_scaling_segment_id integer
     update      V154_accounts_proxy_consent pr
        set      pr.updated_scaling_segment_id = seg.updated_scaling_segment_id
        from     V154_updated_segments seg
        where    seg.isba_tv_region = pr.isba_tv_region
        and      seg.hhcomposition = pr.hhcomposition
        and      seg.tenure = pr.tenure
        and      seg.package = pr.package
        and      substring(seg.boxtype, 1, 4) = substring(pr.boxtype, 1, 4)
        and      seg.adsmartable_universe = pr.adsmartable_universe
     commit

    --Create a table calculating the proportion of adsmart capable boxes that are actually adsmartable
    --in each scaling segment (i.e. those that have given viewing consent).
    IF object_id('V154_adsmartable_by_scaling_segments') IS NOT NULL DROP TABLE V154_adsmartable_by_scaling_segments
    select   updated_scaling_segment_id
            ,sum(adsmart_capable) as no_of_adsmart_capable_accounts
            ,sum(adsmartable) as no_of_adsmart_accounts
            ,case when no_of_adsmart_capable_accounts > 0 then 1.0*no_of_adsmart_accounts/no_of_adsmart_capable_accounts else 0 end as adsmartable_ratio
        into V154_adsmartable_by_scaling_segments
        from V154_accounts_proxy_consent
    group by updated_scaling_segment_id
    commit

    --Add columns to indicate if the account is in vespa or not, and see which of the vespa
    --universes the account is in. Use the random number and adsmartable_ratio values for this.
    --The vespa universes will be assigned as follows:
    -- 0 - non-vespa
    -- 1 - in vespa but not adsmartable
    -- 2 - in vespa but considered to be 'non-consenting'
    -- 3 - in vespa with viewing consent
    alter table V154_accounts_proxy_consent
        add (random_number          real
            ,vespa_indicator        int default 0
            ,vespa_universe         varchar(40) default 'Non-vespa')
    update      V154_accounts_proxy_consent
        set     vespa_indicator = 1
        where   account_number in (select account_number from vespa_account_numbers)
    update      V154_accounts_proxy_consent
        set     vespa_universe = 'Vespa not adsmartable'
        where   vespa_indicator = 1 and adsmartable = 0
    update      V154_accounts_proxy_consent
        set     vespa_universe = 'Vespa adsmartable'
        where   vespa_indicator = 1 and adsmartable = 1
    commit
    update V154_accounts_proxy_consent
        set random_number =  RAND(NUMBER(*)*(DATEPART(MS,NOW())+1))
      where vespa_indicator = 1
    create hg index indx_temp_rand on V154_accounts_proxy_consent(random_number)
    commit
    update      V154_accounts_proxy_consent
        set     vespa_universe = 'Vespa but no consent'
        from    V154_accounts_proxy_consent             pr
  inner join    V154_adsmartable_by_scaling_segments    ad
        on      pr.updated_scaling_segment_id = ad.updated_scaling_segment_id
        where   vespa_indicator = 1
        and     adsmartable = 1
        and     random_number >= adsmartable_ratio
    commit

    --Ensure that people who did not give viewing consent are not included in the vespa accounts
    --Normally this should be the case, but there are instances of when people have requested
    --that viewing consent not be given after the date we are investigating so they would show
    --up as 'Adsmartable but no consent' and 'Vespa ...'.
    update      V154_accounts_proxy_consent
        set     vespa_universe = 'Non-vespa'
        where   vespa_indicator = 1 and sky_base_universe = 'Adsmartable but no consent'
    commit

    --Also update vespa_indicator to ensure theya re not counted in table V154_accounts_aggregated
    update      V154_accounts_proxy_consent
        set     vespa_indicator = 0
        where   vespa_universe = 'Non-vespa' and sky_base_universe = 'Adsmartable but no consent'
    commit

    --Tidy up of table to remove columns no longer required
    alter table V154_accounts_proxy_consent drop adsmart_capable
    alter table V154_accounts_proxy_consent drop adsmartable
    alter table V154_accounts_proxy_consent drop adsmartable_universe
    alter table V154_accounts_proxy_consent drop updated_scaling_segment_id
    alter table V154_accounts_proxy_consent drop random_number

    --Need to create another table of updated segments, though this time we shall split the
    --universe into the sky_base_universe
    --Create a new table with updated segments (e.g. old universe removed, sky base universe added)
    IF object_id('V154_updated_segments') IS NOT NULL DROP TABLE V154_updated_segments
    CREATE TABLE V154_updated_segments (
         updated_scaling_segment_id INT IDENTITY PRIMARY KEY
        ,isba_tv_region varchar(25)
        ,hhcomposition  varchar(5)
        ,tenure         varchar(25)
        ,package        varchar(25)
        ,boxtype        varchar(25)
        ,sky_base_universe   varchar(40)
     )
     commit

     --Insert into V154_updated_segments segments from vespa_analysts.SC2_Segments_Lookup_v2_1
     --Keeping the same order as the segments in vespa_analysts.SC2_Segments_Lookup_v2_1, however
     --we only keep the
     insert into V154_updated_segments (
         isba_tv_region
        ,hhcomposition
        ,tenure
        ,package
        ,boxtype
        ,sky_base_universe
     )
     select distinct
         isba_tv_region
        ,hhcomposition
        ,tenure
        ,package
        ,boxtype
        ,'Adsmartable but no consent'
        from vespa_analysts.SC2_Segments_Lookup_v2_1
        order by
                 package
                ,tenure
                ,hhcomposition
                ,boxtype
                ,isba_tv_region
     commit
     insert into V154_updated_segments (
         isba_tv_region
        ,hhcomposition
        ,tenure
        ,package
        ,boxtype
        ,sky_base_universe
     )
     select distinct
         isba_tv_region
        ,hhcomposition
        ,tenure
        ,package
        ,boxtype
        ,'Adsmartable with consent'
        from vespa_analysts.SC2_Segments_Lookup_v2_1
        order by
                 package
                ,tenure
                ,hhcomposition
                ,boxtype
                ,isba_tv_region
     commit
     insert into V154_updated_segments (
         isba_tv_region
        ,hhcomposition
        ,tenure
        ,package
        ,boxtype
        ,sky_base_universe
     )
     select distinct
         isba_tv_region
        ,hhcomposition
        ,tenure
        ,package
        ,boxtype
        ,'Not adsmartable'
        from vespa_analysts.SC2_Segments_Lookup_v2_1
        order by
                 package
                ,tenure
                ,hhcomposition
                ,boxtype
                ,isba_tv_region
     commit

     --Update V154_accounts_proxy_consent to include (latest) scaling segment ids which now use sky_base_universe
     --Note, have to use substring for boxtype as, for some reason, boxtype 'C)...' doesn't link across tables
     alter table V154_accounts_proxy_consent add updated_scaling_segment_id integer
     update      V154_accounts_proxy_consent pr
        set      pr.updated_scaling_segment_id = seg.updated_scaling_segment_id
        from     V154_updated_segments seg
        where    seg.isba_tv_region = pr.isba_tv_region
        and      seg.hhcomposition = pr.hhcomposition
        and      seg.tenure = pr.tenure
        and      seg.package = pr.package
        and      substring(seg.boxtype, 1, 4) = substring(pr.boxtype, 1, 4)
        and      seg.sky_base_universe = pr.sky_base_universe
     commit

     --Update V154_accounts_proxy_consent to include an alternative set up for the sky_base_universe
     --At present we were to split this according to the current definition, no vespa accounts would
     --appear for 'Adsmartable but no consent', so we need to say that those in the vespa_universe
     --with the values 'Vespa but no consent' are read as 'Adsmartable but no consent' in our
     --alternative sky_base_universe
     alter table V154_accounts_proxy_consent add weighting_universe varchar(40)
     update      V154_accounts_proxy_consent
        set      weighting_universe = sky_base_universe
     update      V154_accounts_proxy_consent
        set      weighting_universe = 'Adsmartable but no consent'
        where    vespa_universe = 'Vespa but no consent'
     commit

    --Create a new table with aggregated info from V154_accounts_proxy_consent
    IF object_id('V154_accounts_aggregated') IS NOT NULL DROP TABLE V154_accounts_aggregated
    CREATE TABLE V154_accounts_aggregated (
         updated_scaling_segment INT IDENTITY PRIMARY KEY
        ,isba_tv_region varchar(25)
        ,hhcomposition  varchar(5)
        ,tenure         varchar(25)
        ,package        varchar(25)
        ,boxtype        varchar(25)
        ,sky_base_universe   varchar(40)
        ,sky_base_accounts      int
        ,vespa_accounts         int
     )
     commit

     --Insert into V154_accounts_aggregated count of sky base customers
     --Keeping order the same as in vespa_analysts.SC2_Segments_Lookup_v2_1
     --Note that we have to group the population by sky_base_universe, but
     --group the vespa panellists by weighting_universe. This is so that
     --accounts inthe 'Vespa but not adsmartable' group don't get counted in
     --the population.
     insert into V154_accounts_aggregated (
         isba_tv_region
        ,hhcomposition
        ,tenure
        ,package
        ,boxtype
        ,sky_base_universe
        ,sky_base_accounts
        ,vespa_accounts
     )
     select distinct
         isba_tv_region
        ,hhcomposition
        ,tenure
        ,package
        ,boxtype
        ,sky_base_universe
        ,count(*)
        ,0
        from V154_accounts_proxy_consent
        group by
                 sky_base_universe
                ,isba_tv_region
                ,boxtype
                ,hhcomposition
                ,tenure
                ,package
        order by
                 sky_base_universe
                ,package
                ,tenure
                ,hhcomposition
                ,boxtype
                ,isba_tv_region
     commit

    IF object_id('temp_vespa_aggregated') IS NOT NULL DROP TABLE temp_vespa_aggregated
    select distinct
         isba_tv_region
        ,hhcomposition
        ,tenure
        ,package
        ,boxtype
        ,weighting_universe as sky_base_universe
        ,sum(vespa_indicator) as vespa_accounts
        into temp_vespa_aggregated
        from V154_accounts_proxy_consent
        group by
                 weighting_universe
                ,isba_tv_region
                ,boxtype
                ,hhcomposition
                ,tenure
                ,package

    update  V154_accounts_aggregated agg
        set agg.vespa_accounts = tva.vespa_accounts
       from temp_vespa_aggregated tva
      where agg.sky_base_universe = tva.sky_base_universe
        and agg.isba_tv_region = tva.isba_tv_region
        and agg.boxtype = tva.boxtype
        and agg.hhcomposition = tva.hhcomposition
        and agg.tenure = tva.tenure
        and agg.package = tva.package
end

