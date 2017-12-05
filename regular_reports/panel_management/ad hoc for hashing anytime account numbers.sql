-- So it takes a bit less than 30 sec to build 5000 hashes? and we have 1.1m to
-- build? So a bit less than two hours, that's okay.

if object_id('stafforr.PanMan_cache_more_hashes') is not null
   drop procedure stafforr.PanMan_cache_more_hashes;
create procedure stafforr.PanMan_cache_more_hashes
    @Hashing_logging_ID      integer
as
begin

    DECLARE @QA_catcher             integer

    select top 4000 account_number
    into #hash_targets
    from V035_Prioritised_household_enablements
    where accno_sha1 is null
    
    set @QA_catcher = -1
    select @QA_catcher = count(1) from #hash_targets
    
    execute citeam.logger_add_event @Hashing_logging_ID, 3, 'A01: Unhashed accounts found.', coalesce(@QA_catcher, -1)
    commit

    select
        account_number
        ,hash(account_number, 'SHA1') as accno_SHA1
    into #hash_results
    from #hash_targets

    commit    
    create unique index fake_pk on #hash_results (account_number)
    commit

    set @QA_catcher = -1
    select @QA_catcher = count(1) from #hash_results
    
    execute citeam.logger_add_event @Hashing_logging_ID, 3, 'A02: Hashes calculated.', coalesce(@QA_catcher, -1)
    commit

    -- Now dump them into the hash pool:
    update V035_Prioritised_household_enablements
    set V035_Prioritised_household_enablements.accno_sha1 = t.accno_sha1
    from V035_Prioritised_household_enablements
    inner join #hash_results as t
    on V035_Prioritised_household_enablements.account_number = t.account_number

    drop table #hash_results
    drop table #hash_targets
    
    set @QA_catcher = -1
    select @QA_catcher = count(1)
    from V035_Prioritised_household_enablements
    where accno_sha1 is not null
    
    execute citeam.logger_add_event @Hashing_logging_ID, 3, 'A03: Hashes now in pool.', coalesce(@QA_catcher, -1)
    commit
end;


-- And the aciton loop:
create variable @Hashing_logging_ID      integer;
EXECUTE citeam.logger_create_run 'VeshaHashCache', 'Cache a SHA1 on ' || convert(varchar(10),today(),123), @Hashing_logging_ID output;

create variable @t integer;
create variable @loop_bound integer;

set @t = 0;

select @loop_bound = count(1) / 4000
from V035_Prioritised_household_enablements
where accno_SHA1 is null;

while @t <= @loop_bound
begin
    execute stafforr.PanMan_cache_more_hashes @Hashing_logging_ID
    commit
    @t = @t + 1
end;
