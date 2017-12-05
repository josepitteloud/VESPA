


1. build list of account types from SAV
        - creates table: V239_SAV_account_type_tbl
        - dependancy: CUST_SINGLE_ACCOUNT_VIEW

2. Update the account type table, setting:
        - exclude_flag (types of account to exclude from the import, usually monitoring & test/dev accounts)
        - aggregate_flag (where we want to mask the account ID as staff or VIP etc..)
          set by indicating normal accounts for inclusion
        - normal_flag (accounts we will report on normally)

3. create summary of the accounts, by joining the account information with handling information
   {there is no plan to make this run historically}

4. Create a summary of available data by file date
        - create list of all possible dates from the 1st August 2014 (the first load date for this data)
        - insert rows into table VESPA_Comscore_audit_run_tbl where data has not been processed
        - insert count information into audit table  (raw data rows, rejects, accounts)
        - calculate expected volume of data using an exponentialy weighted average

5. Build Universe information
        - asset counts
        - first and last data per account
        - count of records recevied within certain periods

6. Check that it's ok to continue processing the day's data. If not halt the build process and raise error.


7. extract the day's period keys from VESPA_CALENDAR according to the local day

8. extract the required fields from the day's data (from COMSCORE_UNION_VIEW)
        - handling DQ error codes with conversions
        - inserting a unique ID that will also order the events

9. Form the context of each event by creating the event transitions
        - using functions lag() and lead()
        - Identify certain events (including 'orphaned events'), content starts,
          and event series start with flags

10. Identify the context of the Orphaned events, and events that signify the end of content or event series.
    Also apply fixes to the assetID and accumulated playing time (ns_st_pt) if required

11. Apply midnight cross-over rules to extend viewing to/from midnight, add local-time fields.
    Remove orphaned-end events

12. Build an aggregate table using new_event_series = 1, end_event_series = 1, continuing_content_from = 1,
    content_continuing_to = 1. Incorporate additional lead/lag context/transistions for requiring fields

13. Compress the prepared start/end event rows into a single 'aggregated' row.
    Apply rules for allocating the event's play duration

14. Prepare and execute dynamic SQL to insert day's basic aggregates into the appropriate monthly table

15. Update basic stats from the generated aggregates

16. Create channel mapping information for conversion of linear viewing events into programme instances

17. Over-write any manual channel mappings if the mapping doesn't already exist

18. Split the linear viewing events into programme instances according to the identified broadcasts
    from the vespa_programme_schedule

19. Record statistics that describe the split of linear viewing events into programme instances

20. Scan the day's records and make corrections to missing or incorrect data

21. Refresh the final union view  -- VESPA_Comscore_SkyGo_Union_View

22. Log the completion of the build


--working out panelID

select top 20
    panel_no
  , account_number
    , card_subscriber_id
from sk_prod.vespa_subscriber_status
where
    result = 'Enabled'
  and panel_no in (11,12)


select top 10 *
 from sk_prod.vespa_subscriber_status


commit
select *
  from VESPA_Comscore_audit_run_tbl

  update VESPA_Comscore_audit_run_tbl
  set processed = 0
  where ts_utc_date > '2014-08-12'
  commit

select top 1000 *
  from VESPA_Comscore_universe_tbl


select top 1000 *
  from VESPA_Comscore_audit_run_tbl

commit
select top 1000 *
from VESPA_Comscore_SAV_summary_tbl

select top 1000 *
from V239_comscore_event_tmp3


                barbera.V239_Comscore_SAV_summary_tbl

this is the raw data feed from Comscore (holding SkyGo viewing information):

                COMSCORE_201408

And the current event view that analysts will use once in production:

                barbera.VESPA_SkyGo_Comscore_View   [this is currently built for the 4th August 2014]


I also have some stats that are created for each of the days when we receive data:

select *
  from VESPA_Comscore_audit_run_tbl r,
       VESPA_Comscore_audit_stats_tbl s
where r.ts_utc_date = s.ts_utc_date

this shows 310,102 IDs for the 4th August. (samprofiles)


-- raw dat
select top 100 *
from COMSCORE_201408


select top 1000 *
from barbera.VESPA_SkyGo_Comscore_View
where sam_profileid is not null



-- count 30,863,435
select count(distinct samprofileid)
from sk_prod.SAM_REGISTRANT

--count 18,472,385
select count(distinct sam_profileid)
select top 10 *
  from barbera.V239_Comscore_SAV_summary_tbl

--count 1,816,220
select count(distinct sam_profileid)
  from COMSCORE_201408


--count 18,413,958
select count(distinct sam_profileid)
  from barbera.V239_Comscore_SAV_summary_tbl
where normal_flag = 1

--count 255,630  (for 4th August)
select count(distinct sam_profileid)
  from barbera.VESPA_SkyGo_Comscore_View


the last figure shows 255,630 from an original 310,102. I haven’t checked if this is as a result of excluding them using:

select *
  from barbera.V239_Comscore_SAV_summary_tbl
where exclude_flag = 1



select top 10 *
from  sk_prod.SAM_REGISTRANT
where

select distinct sam_profileid, null
  into Comscore_id_tmp
   from COMSCORE_201408 c
where  cast(c.ns_utc as date) = '2014-08-04'

--COUNT(1) 310,103
SELECT COUNT(1)
FROM Comscore_id_tmp

--COUNT(1) 309,653
select COUNT(1) --top 10 *
  from Comscore_id_tmp c, sk_prod.SAM_REGISTRANT r
 where c.sam_profileid = r.samprofileid


select *
into sam_registrant_missing_IDs
  from Comscore_id_tmp c
 where c.SAM_PROFILEID NOT IN (SELECT r.samprofileid FROM sk_prod.SAM_REGISTRANT r where samprofileid is not null)

commit

grant select on sam_registrant_missing_IDs to sleary

--where account numbers are null in Sam_registrant
select c.sam_profileid, null
into sam_registrant_missing_Accounts
  from Comscore_id_tmp c,sk_prod.SAM_REGISTRANT r
 where c.sam_profileid = r.samprofileid
   and r.account_number is null

grant select on sam_registrant_missing_Accounts to sleary

commit





