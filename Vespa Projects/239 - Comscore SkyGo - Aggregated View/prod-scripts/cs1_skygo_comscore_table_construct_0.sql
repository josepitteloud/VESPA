/*
###############################################################################
# Created on:   06/08/2014
# Created from COMSCORE:   Alan Barber (ABA)
# Created in Olive : Mohammed Rafi
# Description:  Construct table required for the final output
#
#################################################################################
# Dependencies
# ------------------------------------------------------------------------------
# => Tables/objects required:
#     - none
#################################################################################
# Change log
# ------------------------------------------------------------------------------
# 06/08/2014  ABA   Initial version
# 11/11/2014  ABA   Added run_datetime to ${CBAF_DB_DATA_SCHEMA}.VESPA_Comscore_SkyGo_linear_channel_mapping_audit_tbl
#                   so that proc can be run and logged multiple times on the same data date
###############################################################################*/

/* code_location_A01 *************************************************************************
 *****                                                                                      **
 *****            Create account information table                                             **
 *********************************************************************************************/


IF object_id('${CBAF_DB_DATA_SCHEMA}.VESPA_Comscore_SkyGo_SAV_account_type_tbl') IS NOT NULL
        BEGIN
                DROP TABLE ${CBAF_DB_DATA_SCHEMA}.VESPA_Comscore_SkyGo_SAV_account_type_tbl
        END

GO

CREATE TABLE ${CBAF_DB_DATA_SCHEMA}.VESPA_Comscore_SkyGo_SAV_account_type_tbl (
        uniqid                  INTEGER         NOT NULL,
        acct_type_code          VARCHAR(8)      default NULL,
        acct_sub_type           VARCHAR(64)     default NULL,
        exclude_flag            BIT             default 0,
        aggregate_flag          BIT             default 0,
        normal_flag             BIT             default 0,
        check_flag              BIT             default 0,
        primary key (uniqid)
)
GO

create index VESPA_SAV_account_type_idx on ${CBAF_DB_DATA_SCHEMA}.VESPA_Comscore_SkyGo_SAV_account_type_tbl(acct_type_code)
GO

create index VESPA_SAV_account_sub_idx on ${CBAF_DB_DATA_SCHEMA}.VESPA_Comscore_SkyGo_SAV_account_type_tbl(acct_sub_type)
GO



/* code_location_A02 *************************************************************************
 *****                                                                                      **
 *****            Create summary table for accounts, and SamProfileIDs                      **
 *********************************************************************************************/

IF object_id('${CBAF_DB_DATA_SCHEMA}.VESPA_Comscore_SkyGo_SAV_summary_tbl') IS NOT NULL
        BEGIN
                DROP TABLE ${CBAF_DB_DATA_SCHEMA}.VESPA_Comscore_SkyGo_SAV_summary_tbl
        END
GO

CREATE TABLE ${CBAF_DB_DATA_SCHEMA}.VESPA_Comscore_SkyGo_SAV_summary_tbl (
        uniqid                  INTEGER         NOT NULL,
        account_number          VARCHAR(14)     default NULL,
        sam_profileid           BIGINT          default NULL,
        cb_key_household        BIGINT          default NULL,
        type_id                 INTEGER         NOT NULL,
        acct_type_code          VARCHAR(8)      default NULL,
        acct_sub_type           VARCHAR(64)     default NULL,
        exclude_flag            BIT             default 0,
        aggregate_flag          BIT             default 0,
        normal_flag             BIT             default 0,
        primary key (uniqid)
)
GO

create HG index VESPA_Comscore_SAV_summary_ac_idx on ${CBAF_DB_DATA_SCHEMA}.VESPA_Comscore_SkyGo_SAV_summary_tbl(account_number)
GO

create index VESPA_Comscore_SAV_summary_sam_idx on ${CBAF_DB_DATA_SCHEMA}.VESPA_Comscore_SkyGo_SAV_summary_tbl(sam_profileid)
GO




/* code_location_B01 *************************************************************************
 *****                                                                                      **
 *****            Create processing audit table                                             **
 *********************************************************************************************/

IF object_id('${CBAF_DB_DATA_SCHEMA}.VESPA_Comscore_SkyGo_audit_run_tbl') IS NOT NULL
        BEGIN
                DROP TABLE ${CBAF_DB_DATA_SCHEMA}.VESPA_Comscore_SkyGo_audit_run_tbl
        END
GO

CREATE TABLE ${CBAF_DB_DATA_SCHEMA}.VESPA_Comscore_SkyGo_audit_run_tbl (
        data_date               DATE            NOT NULL,
        QA_status               VARCHAR(12)     default 'unknown',
        basic_view_created      BIT             default 0,
        channels_mapped         BIT             default 0,
        linear_events_split     BIT             default 0,
        primary key (data_date)
)
GO

IF object_id('${CBAF_DB_DATA_SCHEMA}.VESPA_Comscore_SkyGo_audit_stats_tbl') IS NOT NULL
        BEGIN
                DROP TABLE ${CBAF_DB_DATA_SCHEMA}.VESPA_Comscore_SkyGo_audit_stats_tbl
        END
GO

CREATE TABLE ${CBAF_DB_DATA_SCHEMA}.VESPA_Comscore_SkyGo_audit_stats_tbl (
        data_date               DATE            NOT NULL,
        samprofiles             INTEGER         default -99,
        sam_trigger_level       INTEGER         default -99,
        imported_rows           INTEGER         default -99,
        rejected_rows           INTEGER         default -99,
        status                  VARCHAR(24)     default 'unknown',
        aggr_vod_events         INTEGER         default -99,
        aggr_dvod_events        INTEGER         default -99,
        aggr_lin_events         INTEGER         default -99,
        linear_event_instances  INTEGER         default -99,      -- linear events split into programme instances
        aggr_lin_not_split      INTEGER         default -99,      -- linear events not split into programme instances
        primary key (data_date)
)
GO


IF object_id('${CBAF_DB_DATA_SCHEMA}.VESPA_Comscore_SkyGo_linear_channel_mapping_audit_tbl') IS NOT NULL
        BEGIN
                DROP TABLE ${CBAF_DB_DATA_SCHEMA}.VESPA_Comscore_SkyGo_linear_channel_mapping_audit_tbl
        END
GO

CREATE TABLE ${CBAF_DB_DATA_SCHEMA}.VESPA_Comscore_SkyGo_linear_channel_mapping_audit_tbl(
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
GO



/* code_location_B01 ***********************************************************
 *****                                                                        **
 *****          Create logging table                                          **
 *****                                                                        **
 *******************************************************************************/

IF object_id('${CBAF_DB_DATA_SCHEMA}.VESPA_Comscore_SkyGo_log_tbl') IS NOT NULL
        BEGIN
                DROP TABLE ${CBAF_DB_DATA_SCHEMA}.VESPA_Comscore_SkyGo_log_tbl
        END

GO

CREATE TABLE ${CBAF_DB_DATA_SCHEMA}.VESPA_Comscore_SkyGo_log_tbl (
        log_datetime            DATETIME        NOT NULL,
        status                  VARCHAR(300)    default 'unknown',
        primary key (log_datetime)
)
GO

CREATE or REPLACE procedure ${CBAF_DB_DATA_SCHEMA}.VESPA_Comscore_SkyGo_log(
                        IN @status VARCHAR(300)
                  )AS
  BEGIN
    INSERT into ${CBAF_DB_DATA_SCHEMA}.VESPA_Comscore_SkyGo_log_tbl values(now(), @status)
  END
GO

