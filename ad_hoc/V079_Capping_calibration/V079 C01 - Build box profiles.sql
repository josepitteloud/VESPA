/******************************************************************************
** Capping calibration exercise - ONE-OFF script
**
******************************************************************************/

    -- ###############################################################################
    -- ##### Run statements (structures & procedure must be created first)       #####
    -- ###############################################################################
execute V079_Profile_Boxes '2012-05-16', '2012-05-16';
execute citeam.logger_get_latest_job_events 'V079 BP', 4;





    -- ###############################################################################
    -- ##### Structures                                                          #####
    -- ###############################################################################
if object_id('V079_relevant_boxes') is not null then drop table V079_relevant_boxes endif;
create table V079_relevant_boxes (
    account_number                      varchar(20)
    ,subscriber_id                      bigint
    ,service_instance_id                varchar(50)
);
create index idx1 on V079_relevant_boxes(account_number);
create index idx2 on V079_relevant_boxes(subscriber_id);
create index idx3 on V079_relevant_boxes(service_instance_id);
commit;


if object_id('V079_box_lookup') is not null then drop table V079_box_lookup endif;
create table V079_box_lookup (
    subscriber_id                       bigint          primary key
    ,account_number                     varchar(20)     not null
    ,service_instance_id                varchar(50)
    ,PS_flag                            varchar(1)      default 'U'
);
create index idx2 on V079_box_lookup (account_number);
create index idx3 on V079_box_lookup (service_instance_id);
commit;



    -- ###############################################################################
    -- ##### Procedure                                                           #####
    -- ###############################################################################
if object_id('V079_Profile_Boxes') is not null then drop procedure V079_Profile_Boxes endif;

create procedure V079_Profile_Boxes
    @varPeriodStart           date = NULL,
    @varPeriodEnd             date = NULL
as
begin


    declare @varRunId                 bigint
    declare @varSql                   varchar(15000)
    declare @varScanningDay           date

    commit
    execute citeam.logger_create_run 'V079 BP', dateformat(@varPeriodStart, 'dd/mm/yyyy') || '-' || dateformat(@varPeriodEnd, 'dd/mm/yyyy'), @varRunId output
    commit


    DELETE FROM V079_box_lookup
    DELETE FROM V079_relevant_boxes

    set @varSql = 'insert into V079_relevant_boxes
                    select distinct account_number
                        ,subscriber_id
                        ,service_instance_id
                    from sk_prod.VESPA_STB_PROG_EVENTS_##^^*^*##
                    where panel_id in (4,5,12)
                  '
    set @varScanningDay = @varPeriodStart

    while @varScanningDay <= @varPeriodEnd
      begin

            EXECUTE(replace(@varSql,'##^^*^*##', dateformat(@varScanningDay, 'yyyymmdd')))

            commit
            EXECUTE citeam.logger_add_event @varRunId, 3, '--- Day processed: ' || dateformat(@varScanningDay, 'dd/mm/yyyy') || ' ---'
            commit

            set @varScanningDay = dateadd(day, 1, @varScanningDay)
            commit
        end

    commit
    EXECUTE citeam.logger_add_event @varRunId, 3, '(all days have been processed)'
    commit

    -- We also need to populate the V079_box_lookup table:
    insert into V079_box_lookup (
        subscriber_id
        ,account_number
        ,service_instance_id
    )
    select
        subscriber_id
        ,min(account_number)
        ,min(service_instance_id)
    from V079_relevant_boxes
    where subscriber_id is not null
      and account_number is not null
    group by subscriber_id

    commit
    EXECUTE citeam.logger_add_event @varRunId, 3, '(box lookup built)'
    commit


    -------------------------------------------------------------------------------------------------
    -- J02) PRIMARY & SECONDARY BOX FLAGS
    -------------------------------------------------------------------------------------------------
    -- For pulling stuff out of the customer database: we would join on service instance ID,
    -- except that it's not indexed in cust_subs_hist. So instead we pull out everything for
    -- these accounts, and then join back on service instance ID later.
    select distinct account_number
    into #CP2_deduplicated_accounts
    from V079_relevant_boxes

    commit
    create unique index fake_pk on #CP2_deduplicated_accounts (account_number)
    commit

    -- OK, now we can go get get P/S flgs:

    select distinct
        --da.account_number,        -- we're joining back in on service_instance_id, so we don't need account_number
        csh.service_instance_id,
        case
            when csh.subscription_sub_type = 'DTV Primary Viewing' then 'P'
            when csh.subscription_sub_type = 'DTV Extra Subscription' then 'S'
        end as PS_flag
    into #all_PS_flags
    from #CP2_deduplicated_accounts as da
    inner join sk_prod.cust_subs_hist as csh
    on da.account_number = csh.account_number
    where csh.SUBSCRIPTION_SUB_TYPE in ('DTV Primary Viewing','DTV Extra Subscription')
    and csh.status_code in ('AC','AB','PC')
    and csh.effective_from_dt <= @varPeriodStart
    and csh.effective_to_dt   >  @varPeriodEnd

    -- ^^ This guy, on the test build (300k distinct accounts) took 8 minutes. That's managable.

    commit

    -- OK, so building P/S off what's active on the Thursday could cause issues with
    -- recent activators not having subscriptions which give them flags, but I'm okay
    -- with there being a few 'U' entries for recent joiners to Sky for the first week
    -- they're on the Vespa panel. It's not about recently joining Vespa, it's about
    -- recently joining Sky, so it shouldn't be much of an issue at all.

    -- Index *should* be unique, but might not be if there are conflicts in Olive. So,
    -- more QA, check that these are actually unique.
    create index idx1 on #all_PS_flags (service_instance_id)
    commit

    update V079_box_lookup
    set V079_box_lookup.PS_flag = apsf.PS_flag
    from V079_box_lookup
    inner join #all_PS_flags as apsf
    on V079_box_lookup.service_instance_id = apsf.service_instance_id

    commit
    drop table #CP2_deduplicated_accounts
    drop table #all_PS_flags
    commit
    -- Need some QA on the these numbers, including warning about guys still flagged
    -- as 'U', but the process all seems okay.


    commit
    EXECUTE citeam.logger_add_event @varRunId, 3, '(derived P/S per box)'
    commit


end; -- procedure CP2_Profile_Boxes

commit;








