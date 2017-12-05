/******************************************************************************
**
** PROJECT VESPA: MEASUREMENT ERRORS
**
** Refer to
**      http://rtci/Lists/RTCI%20IC%20dev/DispForm.aspx?ID=136&Source=http%3A%2F%2Frtci%2FLists%2FRTCI%2520IC%2520dev%2Factive.aspx
**
** This script takes the information from the 'MEERR_' SQL metric pages and
** creates a table called MEERR_all_viewing_table to store all of the relevant
** viewing information required. It is similar to MEERR_prog_viewing_table,
** however it is not restricted to just viewers of a particular show.
**
** Also created are tables MEERR_strata_popn_size and MEERR_strata_sample_size
** which contain the strata sample sizes and strata population sizes respectively.
**
******************************************************************************/
-- drop procedure
--         MEERR_viewing_table

create procedure
        MEERR_viewing_table
        (
             @viewing_table     varchar(60),
             @broadcast_start   datetime,
             @broadcast_end     datetime
        )
as begin

--Create table containing all live viewers of programme of interest.
if object_id('MEERR_all_viewing_table') is not null drop table MEERR_all_viewing_table
EXECUTE('
    SELECT   pk_viewing_prog_instance_fact
            ,account_number
            ,subscriber_id
            ,programme_name
            ,event_start_date_time_utc
            ,event_end_date_time_utc
            ,instance_start_date_time_utc
            ,case when (capping_end_date_time_utc is null or instance_end_date_time_utc < capping_end_date_time_utc)
                then instance_end_date_time_utc else capping_end_date_time_utc end as instance_end_date_time_utc
            INTO MEERR_all_viewing_table
            FROM '||@viewing_table||'
            WHERE panel_id = 12
                    AND EVENT_START_DATE_TIME_UTC         <= @broadcast_end
                    AND EVENT_END_DATE_TIME_UTC           >= @broadcast_start
                    AND type_of_viewing_event             <> ''Non viewing event''
                    AND UPPER(live_recorded)              = ''LIVE''
                    AND (capping_end_date_time_utc IS NULL
                        OR capping_end_date_time_utc > @broadcast_start)
                    AND subscriber_id       IS NOT NULL
                    AND account_number      IS NOT NULL
                    AND account_number      NOT IN (SELECT account_number
                                            FROM vespa_analysts.accounts_to_exclude)')
commit

end
