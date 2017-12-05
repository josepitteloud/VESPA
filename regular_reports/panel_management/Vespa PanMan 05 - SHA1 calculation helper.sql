-- OK, so we're trying to calculate a lot of hashes in the panel management report, but
-- as part of the next step we're also going to want to try to process and rank over the
-- whole Sky base! that's going to take ages, so we're going to pull some tricks and try
-- calculating batches of these things overnight, maybe until we have the whole active
-- DTV base hashed. All these results will go straight into the hash archive described
-- in the 00 table creation script.

-- Okay, something is borked in this despite it working outside of a proc. See ##

if object_id('vespa_analysts.PanMan_cache_more_hashes') is not null
   drop procedure vespa_analysts.PanMan_cache_more_hashes;
create procedure vespa_analysts.PanMan_cache_more_hashes
    @hashes_to_calculate            int = 800000
    -- OK, so this parameter is still valid, though for technical reasons it doesn't
    -- actually do anything now... oh well.
as
begin

    declare @Hashing_logging_ID     integer
    DECLARE @QA_catcher             integer
    EXECUTE citeam.logger_create_run 'VespaHashCache', 'Cache a SHA1 on ' || convert(varchar(10),today(),123), @Hashing_logging_ID output

    declare @weird_parameter_workaround integer
    
    set @weird_parameter_workaround = @hashes_to_calculate
    
    -- First off, grab all the active account numbers from SAV:
    select distinct account_number
        ,convert(bit, 1) as keeper
    into #hash_ables
    from sk_prod.cust_single_account_view
    where cust_active_dtv = 1

    commit
    create unique index fake_pk on #hash_ables (account_number)
    commit

    set @QA_catcher = -1
    select @QA_catcher = count(1) from #hash_ables
    
    execute citeam.logger_add_event @Hashing_logging_ID, 3, 'A01: Active customers identified.', coalesce(@QA_catcher, -1)
    commit
    
    -- Clip out anything that we already have:
    update #hash_ables
    set keeper = 0
    from #hash_ables
    inner join Vespa_PanMan_SHA1_archive as sha
    on #hash_ables.account_number = sha.account_number

    commit
    delete from #hash_ables
    where keeper = 0
    commit

    set @QA_catcher = -1
    select @QA_catcher = count(1) from #hash_ables
    
    execute citeam.logger_add_event @Hashing_logging_ID, 3, 'A02: Unhashed accounts found.', coalesce(@QA_catcher, -1)
    commit
    
    -- now grab a sample of those (500k?) to process now:
    --select top @weird_parameter_workaround account_number
    select top 800000 account_number
        ,convert(varchar(40), null) as accno_sha1
    into #hash_targets
    from #hash_ables
    -- ## ^^ okay, so you can TOP by a variable when just running code, but doesn't work
    -- in a procedure? that's inconvenient.    

    commit
    drop table #hash_ables
    create unique index fake_pk on #hash_targets (account_number)
    commit

    set @QA_catcher = -1
    select @QA_catcher = count(1) from #hash_targets
    
    execute citeam.logger_add_event @Hashing_logging_ID, 3, 'A03: Hash targets selected.', coalesce(@QA_catcher, -1)
    commit    
    
    -- 500k is to many hashes to attempt to calculate in one go, but we can break it into some
    -- smaller clumps (4k) and iterate over those, it seems to work okay.

    -- For holding each batch of 4k items...
    create table #hash_cache (
        account_number          varchar(20)
    )
    
    -- For storing all the results before they go back into the main table
    create table #hash_results (
        account_number          varchar(20)     primary key
        ,accno_sha1             varchar(40)
    )
    
    commit

    declare @t integer
    set @t = 0

    while @t <= @QA_catcher / 4000
    begin

        insert into #hash_cache
        select top 4000 account_number
        from #hash_targets
        where accno_sha1 is null
        
        commit
    
        insert into #hash_results
        select
            account_number
            ,hash(account_number, 'SHA1') as accno_SHA1
        from #hash_cache

        update #hash_targets
        set accno_SHA1 = res.accno_SHA1
        from #hash_targets
        inner join #hash_results as res
        on #hash_targets.account_number = res.account_number
        
        commit
        delete from #hash_results
        delete from #hash_cache
        commit

        set @t = @t + 1

        if mod(@t, 10) = 0
            -- Ping the logger only every 40k of account numbers hashed
            execute citeam.logger_add_event @Hashing_logging_ID, 3, 'A04: Hash batch processed.', @t/10
        commit
        
    end
    
    set @QA_catcher = -1
    select @QA_catcher = count(1)
    from #hash_targets
    
    execute citeam.logger_add_event @Hashing_logging_ID, 3, 'A05: All hashes calculated.', coalesce(@QA_catcher, -1)
    commit
    
    set @QA_catcher = -1
    select @QA_catcher = count(1)
    from #hash_targets
    where accno_SHA1 is null
    
    if @QA_catcher is null or @QA_catcher <> 0
        execute citeam.logger_add_event @Hashing_logging_ID, 2, 'A05: NULL hashes Present!', coalesce(@QA_catcher, -1)
    commit
    
    -- Now dump them into the hash pool:
    insert into Vespa_PanMan_SHA1_archive
    select * from #hash_targets

    commit
    
    drop table #hash_cache
    drop table #hash_results
    drop table #hash_targets
    
    set @QA_catcher = -1
    select @QA_catcher = count(1)
    from Vespa_PanMan_SHA1_archive
    
    execute citeam.logger_add_event @Hashing_logging_ID, 3, 'A06: Hashes now in pool.', coalesce(@QA_catcher, -1)
    commit
    
end; -- of "PanMan_cache_more_hashes" procedure

grant execute on PanMan_cache_more_hashes to stafforr, CITeam;

-- oh, and we'll schedule a build of that for every Thursday night (refer to crs_task_listings.sql in the Customer Group repo)



