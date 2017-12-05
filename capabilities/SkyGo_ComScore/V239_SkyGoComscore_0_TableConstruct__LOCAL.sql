/*
                         $$$
                        I$$$
                        I$$$
               $$$$$$$$ I$$$    $$$$$      $$$     ZDD    DDDDDDD.
             ,$$$$$$$$  I$$$   $$$$$$$    $$$      ODD  ODDDZ 7DDDD
             ?$$$,      I$$$ $$$$. $$$$  $$$=      ODD  DDD     NDD
              $$$$$$$$= I$$$$$$$    $$$$.$$$       ODD +DD$     +DD$
                  :$$$$~I$$$ $$$$    $$$$$$        ODD  DDN     NDD.
               ,.   $$$+I$$$  $$$$    $$$$=        ODD  NDDN   NDDN
              $$$$$$$$$ I$$$   $$$$   .$$$         ODD   ZDDDDDDDN
                                      $$$           .      $DDZ
                                     $$$                  ,NDDDDDDD
                                    $$$?

                      CUSTOMER INTELLIGENCE SERVICES


        V239 - Oneview Comscore (SkyGo) - Aggregated Event Table Construction


###############################################################################
# Created on:   06/08/2014
# Created by:   Alan Barber (ABA)
# Description:  Construct table required for the final output
#
# List of steps:
# --------------
#
# code_location_A01
#
#
#################################################################################
# Dependencies
# ------------------------------------------------------------------------------
# => Tables/objects required:
#     - none
#
#################################################################################
# Change log
# ------------------------------------------------------------------------------
# 06/08/2014  ABA   Initial version
# 11/11/2014  ABA   Added run_datetime to VESPA_Comscore_SkyGo_linear_channel_mapping_audit_tbl
#                   so that proc can be run and logged multiple times on the same data date
#
###############################################################################*/



/* code_location_A01 *************************************************************************
 *****                                                                                      **
 *****            Create account information table                                             **
 *********************************************************************************************/


IF object_id('barbera.VESPA_Comscore_SkyGo_SAV_account_type_tbl') IS NOT NULL
        BEGIN
                DROP TABLE barbera.VESPA_Comscore_SkyGo_SAV_account_type_tbl
        END

CREATE TABLE barbera.VESPA_Comscore_SkyGo_SAV_account_type_tbl (
        uniqid                  INTEGER         NOT NULL,
        acct_type_code          VARCHAR(8)      NULL,
        acct_sub_type           VARCHAR(64)     NULL,
        exclude_flag            BIT             NULL, --0
        aggregate_flag          BIT             NULL, --0
        normal_flag             BIT             NULL, --0
        check_flag              BIT             NULL, --0
        primary key (uniqid)
)
create index VESPA_SAV_account_type_idx on barbera.VESPA_Comscore_SkyGo_SAV_account_type_tbl(acct_type_code)
create index VESPA_SAV_account_sub_idx on barbera.VESPA_Comscore_SkyGo_SAV_account_type_tbl(acct_sub_type)
commit



/* code_location_A02 *************************************************************************
 *****                                                                                      **
 *****            Create summary table for accounts, and SamProfileIDs                      **
 *********************************************************************************************/

IF object_id('barbera.VESPA_Comscore_SkyGo_SAV_summary_tbl') IS NOT NULL
        BEGIN
                DROP TABLE barbera.VESPA_Comscore_SkyGo_SAV_summary_tbl
        END

CREATE TABLE barbera.VESPA_Comscore_SkyGo_SAV_summary_tbl (
        uniqid                  INTEGER         NOT NULL,
        account_number          VARCHAR(14)     NULL,
        sam_profileid           BIGINT          NULL,
        cb_key_household        BIGINT          NULL,
        type_id                 INTEGER         NOT NULL,
        acct_type_code          VARCHAR(8)      NULL,
        acct_sub_type           VARCHAR(64)     NULL,
        exclude_flag            BIT             default 0, --NULL, --0
        aggregate_flag          BIT             default 0, --NULL, --0
        normal_flag             BIT             default 0, --NULL, --0
        primary key (uniqid)
)
create HG index VESPA_Comscore_SAV_summary_ac_idx on barbera.VESPA_Comscore_SkyGo_SAV_summary_tbl(account_number)
create index VESPA_Comscore_SAV_summary_sam_idx on barbera.VESPA_Comscore_SkyGo_SAV_summary_tbl(sam_profileid)

commit




/* code_location_B01 *************************************************************************
 *****                                                                                      **
 *****            Create processing audit table                                             **
 *********************************************************************************************/

IF object_id('barbera.VESPA_Comscore_SkyGo_audit_run_tbl') IS NOT NULL
        BEGIN
                DROP TABLE barbera.VESPA_Comscore_SkyGo_audit_run_tbl
        END

CREATE TABLE barbera.VESPA_Comscore_SkyGo_audit_run_tbl (
        data_date               DATE            NOT NULL,
        QA_status               VARCHAR(12)     'unknown',
        build_started           BIT             0,
        basic_view_created      BIT             0,
        channels_mapped         BIT             0,
        linear_events_split     BIT             0,
        build_completed         BIT             0,
        build_success           BIT             0,
        build_completed_date    DATETIME        cast('0001-01-01 00:00:00' as datetime),
        build_duration_secs     INTEGER         -99,
        primary key (data_date)
)
commit

IF object_id('barbera.VESPA_Comscore_SkyGo_audit_stats_tbl') IS NOT NULL
        BEGIN
                DROP TABLE barbera.VESPA_Comscore_SkyGo_audit_stats_tbl
        END

CREATE TABLE barbera.VESPA_Comscore_SkyGo_audit_stats_tbl (
        data_date               DATE            NOT NULL,
        samprofiles             INTEGER         -99,
        sam_trigger_level       INTEGER         -99,
        imported_rows           INTEGER         -99,
        rejected_rows           INTEGER         -99,
        status                  VARCHAR(24)     'unknown',
        aggr_vod_events         INTEGER         -99,
        aggr_dvod_events        INTEGER         -99,
        aggr_lin_events         INTEGER         -99,
        linear_event_instances  INTEGER         -99,      -- linear events split into programme instances
        aggr_lin_not_split      INTEGER         -99,      -- linear events not split into programme instances
        primary key (data_date)
)
commit


IF object_id('barbera.VESPA_Comscore_SkyGo_linear_channel_mapping_audit_tbl') IS NOT NULL
        BEGIN
                DROP TABLE barbera.VESPA_Comscore_SkyGo_linear_channel_mapping_audit_tbl
        END

CREATE TABLE barbera.VESPA_Comscore_SkyGo_linear_channel_mapping_audit_tbl(
       data_date_local          DATE            NOT NULL,
       run_datetime             DATETIME        NOT NULL,
       station_name             VARCHAR(48)     NOT NULL,
       events_mapped            INTEGER         NULL,
       accounts_mapped          INTEGER         NULL,
       events_not_mapped        INTEGER         NULL,
       accounts_not_mapped      INTEGER         NULL,
       event_unknown_errors     INTEGER         NULL,
       account_unknown_errors   INTEGER         NULL,
       total_events             INTEGER         NULL,
       total_accounts           INTEGER         NULL
)




/* code_location_B01 ***********************************************************
 *****                                                                        **
 *****          Create logging table                                          **
 *****                                                                        **
 *******************************************************************************/

IF object_id('barbera.VESPA_Comscore_SkyGo_log_tbl') IS NOT NULL
        BEGIN
                DROP TABLE barbera.VESPA_Comscore_SkyGo_log_tbl
        END

CREATE TABLE barbera.VESPA_Comscore_SkyGo_log_tbl (
        log_datetime            DATETIME        NOT NULL,
        status                  VARCHAR(300)    'unknown',
        primary key (log_datetime)
)
commit



CREATE or REPLACE procedure barbera.VESPA_Comscore_SkyGo_log(
                        IN @status VARCHAR(300)
                  )AS
  BEGIN
    INSERT into barbera.VESPA_Comscore_SkyGo_log_tbl values(now(), @status)
    commit
  END






/* code_location_C01 ***********************************************************
 *****                                                                        **
 *****          Create final results table - this is now managed              **
 *****          within the main code                                          **
 *******************************************************************************/


/*
IF object_id('VESPA_SKYGO_VIEWING_201408') IS NOT NULL
        BEGIN
                DROP VIEW VESPA_SKYGO_VIEWING_201408
        END

CREATE VIEW VESPA_SKYGO_VIEWING_201408 (
        account_number          VARCHAR(20)     NOT NULL,
        samprofile_id           INTEGER         NOT NULL,
        viewing_event_start     DATETIME        NOT NULL,
        viewing_event_end       DATETIME        NOT NULL,
        stream_context          VARCHAR(10)     NOT NULL,
        vod_asset_id            VARCHAR(48)     DEFAULT NULL,
        ad_asset_id             VARCHAR(24)     DEFAULT NULL,
        channel_key             INTEGER         DEFAULT NULL,
        episode_name            VARCHAR(128)    DEFAULT NULL,
        program_name            VARCHAR(255)    DEFAULT NULL,
        station_name            VARCHAR(64)     DEFAULT NULL,
        genre                   VARCHAR(128)    DEFAULT NULL,
        ad_flag                 BIT             NOT NULL,
        device_name             VARCHAR(50)     NOT NULL,
        platform_name           VARCHAR(24)     NOT NULL,
        platform_version        VARCHAR(24)     NOT NULL,
        connection_type         VARCHAR(8)      NOT NULL,
        event_duration          INTEGER         NOT NULL,
        content_duration        INTEGER         NOT NULL,
        percentage_viewed       FLOAT           NOT NULL,
        buffering_duration      INTEGER         NOT NULL,
        raw_events              SMALLINT        NOT NULL,
        pause_events            SMALLINT        NOT NULL,
        play_events             SMALLINT
)

commit;
*/
