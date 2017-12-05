




CREATE or replace procedure SEG01_SETUP_TABLES_proc(
                ) AS
BEGIN

/**********************
 **   code_location_A01
 **********************/

    exec seg01_log 'SEG01_SETUP_TABLES_proc<'||now()||'>'


    --create logging table and proceedure

    IF object_id('SEG01_log_tbl') IS NOT NULL
        BEGIN
            DROP TABLE SEG01_log_tbl
        END

    create table SEG01_log_tbl(
        out              VARCHAR(150)    NOT NULL
    )

    exec seg01_log '    created logging table<SEG01_log_tbl>'
    commit

/*
select *
from SEG01_log_tbl

truncate table SEG01_log_tbl
commit
*/

-- OFFICIAL START



/****************************************************************************
 **    CREATE REQUIRED TABLES for STRUCTURE
 ****************************************************************************/


--need to create a data dictionary for the segmentation meta data
    IF object_id('SEG01_Segment_Dictionary_Tag_Types_tbl') IS NOT NULL
        BEGIN
            DROP TABLE SEG01_Segment_Dictionary_Tag_Types_tbl
        END

    create table SEG01_Segment_Dictionary_Tag_Types_tbl(
        uniqid                 BIGINT          NOT NULL identity,
        tag_name               VARCHAR(255)    NOT NULL,
        database_name          VARCHAR(24)     NOT NULL,
        schema_name            VARCHAR(24)     NOT NULL,
        table_name             VARCHAR(48)     DEFAULT NULL,
        table_proxy            VARCHAR(48)     DEFAULT NULL,
        col_name               VARCHAR(42)     NOT NULL,
        tag_type               VARCHAR(24)     NOT NULL
    )

    exec seg01_log '    created <SEG01_Segment_Dictionary_Tag_Types_tbl>'
    commit


    IF object_id('SEG01_Segment_Dictionary_tbl') IS NOT NULL
        BEGIN
            DROP TABLE SEG01_Segment_Dictionary_tbl
        END

    create table SEG01_Segment_Dictionary_tbl(
        tag_type_uid           BIGINT          NOT NULL,
        tag_value_uid          BIGINT          NOT NULL,
        tag_value              VARCHAR(150)    NOT NULL
    )

    exec seg01_log '    created <SEG01_Segment_Dictionary_tbl>'
    commit


    IF object_id('SEG01_Table_Association_tbl') IS NOT NULL
        BEGIN
            DROP TABLE SEG01_Table_Association_tbl
        END

    create table SEG01_Table_Association_tbl(
        uniqid                 BIGINT          NOT NULL identity,
        database_name          VARCHAR(24)     NOT NULL,
        schema_name            VARCHAR(24)     NOT NULL,
        table_name             VARCHAR(24)     NOT NULL,
        col_name               VARCHAR(42)     NOT NULL,
        association_name       VARCHAR(42)     NOT NULL  -- this will usually be the column name
    )

    exec seg01_log '    created <SEG01_Table_Association_tbl>'
    commit


    IF object_id('SEG01_Tag_Self_Aware_tbl') IS NOT NULL
        BEGIN
            DROP TABLE SEG01_Tag_Self_Aware_tbl
        END

    create table SEG01_Tag_Self_Aware_tbl(
        uniqid                 BIGINT          NOT NULL identity,
        tag_name               VARCHAR(255)    NOT NULL,
        database_name          VARCHAR(24)     NOT NULL,
        schema_name            VARCHAR(24)     NOT NULL,
        table_name             VARCHAR(24)     NOT NULL,
        table_proxy            VARCHAR(24)     NOT NULL,
        col_name               VARCHAR(42)     NOT NULL,
        aware_type             VARCHAR(24)     NOT NULL
    )

    exec seg01_log '    created <SEG01_Tag_Self_Aware_tbl>'
    commit


    IF object_id('SEG01_temporalid_lookup_tbl') IS NOT NULL
        BEGIN
            DROP TABLE SEG01_temporalid_lookup_tbl
        END
    create table SEG01_temporalid_lookup_tbl(
      uniqid                 BIGINT          NOT NULL identity,
      allocated_date         DATETIME        NOT NULL,
      create_date            DATETIME        NULL
    )
    exec seg01_log '    created <SEG01_temporalid_lookup_tbl>'
    commit


    IF object_id('SEG01_segmentid_lookup_tbl') IS NOT NULL
        BEGIN
            DROP TABLE SEG01_segmentid_lookup_tbl
        END
    create table SEG01_segmentid_lookup_tbl(
      uniqid                 BIGINT          NOT NULL identity,
      allocated_date         DATETIME        NOT NULL,
      create_date            DATETIME        NULL
    )
    exec seg01_log '    created <SEG01_segmentid_lookup_tbl>'
    commit


    IF object_id('SEG01_trunkid_lookup_tbl') IS NOT NULL
        BEGIN
            DROP TABLE SEG01_trunkid_lookup_tbl
        END

    create table SEG01_trunkid_lookup_tbl(
      uniqid                 BIGINT          NOT NULL identity,
      allocated_date         DATETIME        NOT NULL,
      create_date            DATETIME        NULL
    )
    exec seg01_log '    created <SEG01_trunkid_lookup_tbl>'
    commit


    IF object_id('SEG01_ruleid_lookup_tbl') IS NOT NULL
        BEGIN
            DROP TABLE SEG01_ruleid_lookup_tbl
        END

    create table SEG01_ruleid_lookup_tbl(
      uniqid                 BIGINT          NOT NULL identity,
      allocated_date         DATETIME        NOT NULL,
      create_date            DATETIME        NULL
    )
    exec seg01_log '    created <SEG01_ruleid_lookup_tbl>'
    commit


    IF object_id('SEG01_filterid_lookup_tbl') IS NOT NULL
        BEGIN
            DROP TABLE SEG01_filterid_lookup_tbl
        END
    create table SEG01_filterid_lookup_tbl(
      uniqid                 BIGINT          NOT NULL identity,
      allocated_date         DATETIME        NOT NULL,
      create_date            DATETIME        NULL
    )
    exec seg01_log '    created <SEG01_filterid_lookup_tbl>'


    IF object_id('SEG01_leafid_lookup_tbl') IS NOT NULL
        BEGIN
            DROP TABLE SEG01_leafid_lookup_tbl
        END
    create table SEG01_leafid_lookup_tbl(
      uniqid                 BIGINT          NOT NULL identity,
      allocated_date         DATETIME        NOT NULL,
      create_date            DATETIME        NULL
    )
    exec seg01_log '    created <SEG01_leafid_lookup_tbl>'
    commit


--make table that hold which roots/trunks we wish to combine
    IF object_id('SEG01_trunk_filter_defn_tbl') IS NOT NULL
        BEGIN
            DROP TABLE SEG01_trunk_filter_defn_tbl
        END
    create table SEG01_trunk_filter_defn_tbl(
        uniqid                  bigint      not null  identity,
        filter_id               bigint      not null,
        aggregation_id          bigint      not null,           --root_id/trunk_id
        aggregation_type        integer     not null,           --root[0]/trunk[1]
        temporal_id             bigint      not null,
        datetime_type_field     integer     not null,
        filter_type             integer     not null            --AND[0]/OR[1]  (type of join)
    )
    exec seg01_log '    created <SEG01_trunk_filter_defn_tbl>'
    commit


    --make table that holds which roots/trunk aggregates we wish to combine to make LEAFs
    IF object_id('SEG01_leaf_aggregation_defn_tbl') IS NOT NULL
        BEGIN
            DROP TABLE SEG01_leaf_aggregation_defn_tbl
        END
    create table SEG01_leaf_aggregation_defn_tbl(
        uniqid                  bigint      not null  identity,
        leaf_id                 bigint      not null,
        aggregation_id          bigint      not null,           --root_id/trunk_id/leaf_id
        aggregation_type        integer     not null,           --root[0]/trunk[1]/leaf[3]
       -- temporal_id             bigint      not null,
       -- datetime_type_field     integer     not null,
        operator_type           integer     not null            --AND[0]/OR[1]/MULTIPLY[2]/DIVIDE[3]  (type of join)
    )
    exec seg01_log '    created <SEG01_leaf_aggregation_defn_tbl>'
    commit


    --to store root time-aggregation information
    IF object_id('SEG01_root_temporal_tbl') IS NOT NULL
        BEGIN
            DROP TABLE SEG01_root_temporal_tbl
        END
    create table SEG01_root_temporal_tbl(
        pk_viewing_prog_instance_fact  BIGINT      NOT NULL,
        account_number                 varchar(16) NOT NULL,
        temporal_library_id            BIGINT      NOT NULL,
        max_from_date                  datetime    NOT NULL, --not required
        min_to_date                    datetime    NOT NULL, --not required
        total_duration                 bigint      NOT NULL,
        period_duration                bigint      NOT NULL,
        PRIMARY KEY (pk_viewing_prog_instance_fact, temporal_library_id)
    )
    exec seg01_log '    created <SEG01_root_temporal_tbl>'
    commit

    --create some indexes
    --create unique index SEG01_temporal_root_agg_pk_tempid_idx on SEG01_root_temporal_tbl (pk_viewing_prog_instance_fact asc, temporal_library_id asc)
    create clustered index SEG01_temporal_root_agg_tempid_ac_idx on SEG01_root_temporal_tbl (temporal_library_id asc, account_number asc)
    create index SEG01_temporal_root_agg_pk_idx on SEG01_root_temporal_tbl (pk_viewing_prog_instance_fact asc)
    exec seg01_log '    created INDEXES <SEG01_root_temporal_tbl>'
    commit


    -- Build-schedule for the root aggregations
     IF object_id('SEG01_root_build_schedule_tbl') IS NOT NULL
       BEGIN
         DROP TABLE SEG01_root_build_schedule_tbl
       END
    create table SEG01_root_build_schedule_tbl(
        uniqid             BIGINT           NOT NULL identity,
        attribute          VARCHAR(255)     NOT NULL,
        operator           VARCHAR(12)      NOT NULL,
        condition          VARCHAR(124)     NOT NULL
    )
    exec seg01_log '    created <SEG01_root_build_schedule_tbl>'
    commit


    -- Build-schedule for the root aggregations with temporal component
    IF object_id('SEG01_root_temporal_build_schedule_tbl') IS NOT NULL
        BEGIN
            DROP TABLE SEG01_root_temporal_build_schedule_tbl
        END
    create table SEG01_root_temporal_build_schedule_tbl(
        uniqid             BIGINT           NOT NULL identity,
        root_schedule_id   BIGINT           NOT NULL,
        temporal_id        BIGINT           NOT NULL
    )
    exec seg01_log '    created <SEG01_root_temporal_build_schedule_tbl>'
    commit


    -- Audit the build process for root aggregations
    IF object_id('SEG01_root_temporal_build_audit_tbl') IS NOT NULL
        BEGIN
            DROP TABLE SEG01_root_temporal_build_audit_tbl
        END
    create table SEG01_root_temporal_build_audit_tbl(
        uniqid             BIGINT           NOT NULL identity,
        root_schedule_id   BIGINT           NOT NULL,
        temporal_id        BIGINT           NOT NULL,
        last_built         DATETIME         NOT NULL
    )
    exec seg01_log '    created <SEG01_root_temporal_build_audit_tbl>'
    commit


    IF object_id('SEG01_root_segment_desc_tbl') IS NOT NULL
        BEGIN
            DROP TABLE SEG01_root_segment_desc_tbl
        END
    create table SEG01_root_segment_desc_tbl(
       segment_id             BIGINT          NOT NULL,
       tag_id                 BIGINT          NOT NULL,
       tag_name               VARCHAR(255)    NOT NULL,
       temporal_id            BIGINT          NOT NULL,
       operator               VARCHAR(6)      NOT NULL,
       condition              VARCHAR(12)     NOT NULL
    )
    exec seg01_log '    created <SEG01_root_segment_desc_tbl>'
    commit


    IF object_id('SEG01_root_segment_tbl') IS NOT NULL
        BEGIN
            DROP TABLE SEG01_root_segment_tbl
        END
    create table SEG01_root_segment_tbl(
        pk_viewing_prog_instance_fact  BIGINT  NOT NULL,
        segment_id                     INTEGER NOT NULL
    )
    exec seg01_log '    created <SEG01_root_segment_tbl>'

    create unique index SEG01_root_segment_pk_aggid_idx on SEG01_root_segment_tbl (pk_viewing_prog_instance_fact asc, segment_id asc)
    create index SEG01_root_segment_pk_idx on SEG01_root_segment_tbl (pk_viewing_prog_instance_fact asc)
    create index SEG01_root_segment_aggid_idx on SEG01_root_segment_tbl (segment_id asc)
    exec seg01_log '    created INDEXES <SEG01_root_segment_tbl>'
    commit


    -- used to store a list of events tables
    IF object_id('SEG01_viewed_dp_event_table_summary_tbl') IS NOT NULL
        BEGIN
            DROP TABLE SEG01_viewed_dp_event_table_summary_tbl
        END
    CREATE TABLE SEG01_viewed_dp_event_table_summary_tbl(
        uniqid                         BIGINT         NOT NULL identity,
        schema_name                    VARCHAR(24)    NOT NULL,
        table_name                     VARCHAR(52)    NOT NULL,
        min_viewed_datetime            DATETIME       DEFAULT NULL,
        max_viewed_datetime            DATETIME       DEFAULT NULL,
        min_broadcast_datetime         DATETIME       DEFAULT NULL,
        max_broadcast_datetime         DATETIME       DEFAULT NULL,
        min_event_datetime             DATETIME       DEFAULT NULL,
        max_event_datetime             DATETIME       DEFAULT NULL
    )
    exec seg01_log '    created <SEG01_viewed_dp_event_table_summary_tbl>'
    commit


    /** this allows you to say - [hh, 3, '09:00:00 0..'] which means 9am for 3hours */
    IF object_id('SEG01_temporal_period_tbl') IS NOT NULL
        BEGIN
            DROP TABLE SEG01_temporal_period_tbl
        END
    create table SEG01_temporal_period_tbl(
        uniqid                    BIGINT         NOT NULL identity,
        date_part_abbreviation    VARCHAR(3)     NOT NULL, -- depicts the unit of time we're dealing with
        quantity                  BIGINT         NOT NULL,
        period_start              TIME           NOT NULL, -- based on time of day
        period                    BIGINT         NOT NULL  -- number of seconds in the period - this is ignored now, due to differences in months
    )
    exec seg01_log '    created <SEG01_temporal_period_tbl>'
    commit


    /* use this as a mapping table, so that more than one period type can be
       combined into a more complex temporal period
        - usage is now: one period for the recurrence, and a second for the period
          within the recurrence we are interested in basing the aggregation on
     */
    IF object_id('SEG01_temporal_definition_tbl') IS NOT NULL
        BEGIN
            DROP TABLE SEG01_temporal_definition_tbl
        END
    create table SEG01_temporal_definition_tbl
    (    uniqid                    BIGINT         NOT NULL,
         period_id                 BIGINT         NOT NULL,
         period_lag                BIGINT         NOT NULL,
         period_type               bit            NOT NULL -- [0]Occurrence/[1]Period
    )
    exec seg01_log '    created <SEG01_temporal_definition_tbl>'
    commit


    IF object_id('SEG01_temporal_library_tbl') IS NOT NULL
        BEGIN
            DROP TABLE SEG01_temporal_library_tbl
        END
    create table SEG01_temporal_library_tbl(
         uniqid                         BIGINT         NOT NULL identity,
       --  temporal_id                    BIGINT         NOT NULL, -- temporal descriptor
         definition_id                  BIGINT         NOT NULL,
         recurrence_id                  BIGINT         NOT NULL,
         period_id                      BIGINT         NOT NULL,
         recurrence_start_datetime      DATETIME       NOT NULL,
         recurrence_period              BIGINT         NOT NULL,
         period_lag                     BIGINT         NOT NULL,
         period_start_datetime          DATETIME       NOT NULL,
         period_duration                BIGINT         NOT NULL,
         period_end_datetime            DATETIME       NOT NULL,
         recurrence_end_datetime        DATETIME       NOT NULL
    )
    exec seg01_log '    created <SEG01_temporal_library_tbl>'
    commit


    IF object_id('SEG01_metric_library_tbl') IS NOT NULL
        BEGIN
            DROP TABLE SEG01_metric_library_tbl
        END
    create table SEG01_metric_library_tbl(
        uniqid            BIGINT          NOT NULL identity,
        metric_name       VARCHAR(24)     NOT NULL,
        calculation       VARCHAR(64)     NOT NULL,
        over_group        VARCHAR(24)     NOT NULL
    )
    exec seg01_log '    created <SEG01_metric_library_tbl>'
    commit


    --create table for metric build schedule
    IF object_id('SEG01_metric_build_schedule_tbl') IS NOT NULL
        BEGIN
            DROP TABLE SEG01_metric_build_schedule_tbl
        END
    create table SEG01_metric_build_schedule_tbl(
        uniqid                BIGINT      NOT NULL identity,
        metric_schedule_id    BIGINT      NOT NULL,
        metric_id             BIGINT      NOT NULL
    )
    exec seg01_log '    created <SEG01_metric_build_schedule_tbl>'
    commit


--we extend the dictionary capaibilty by creating a bolt-on table that holds the state of the tag (discrete/continuous)
    IF object_id('SEG01_Segment_Dictionary_Tag_States_tbl') IS NOT NULL
        BEGIN
            DROP TABLE SEG01_Segment_Dictionary_Tag_States_tbl
        END
    create table SEG01_Segment_Dictionary_Tag_States_tbl(
        uniqid                 BIGINT          NOT NULL identity,
        tag_type_uid           BIGINT          NOT NULL,
        variable_state         VARCHAR(24)     NOT NULL --to identify the variable as continuous/discrete
    )
    exec seg01_log '    created <SEG01_Segment_Dictionary_Tag_States_tbl>'
    commit


    /***********************************************************************************
     ** Build the primary aggregation table to store all the results at account level
     ***********************************************************************************/

    IF object_id('SEG01_tmp_event_table_log_tbl') IS NOT NULL
        BEGIN
            DROP TABLE SEG01_tmp_event_table_log_tbl
        END
    create table SEG01_tmp_event_table_log_tbl (
        table_name             varchar(128)    NOT NULL,
        filter_id                bigint          NOT NULL,
        tags_required          varchar(500)    NOT NULL
    )
    exec seg01_log '    created <SEG01_tmp_event_table_log_tbl>'
    commit


    -- IS THE TABLE NECESSARY?  as we could just use the universal one and map to individual account_numbers??
    IF object_id('SEG01_root_account_aggregate_tbl') IS NOT NULL
        BEGIN
            DROP TABLE SEG01_root_account_aggregate_tbl
        END
    --this table should actually have an ID for the aggregate!!???
    create table SEG01_root_account_aggregate_tbl (
            account_number      varchar(24)   NULL,
            aggregate_id        bigint        NOT NULL,
            universe_id         bigint        NOT NULL,
            root_id             bigint        NOT NULL,
            temporal_id         bigint        NOT NULL,
            metric_id           bigint        NOT NULL,
            aggregation_value   double        NOT NULL
    )
    create hg index SEG01_root_ac_agg_ac_idx on SEG01_root_account_aggregate_tbl(account_number)
    create hg index SEG01_root_ac_agg_aggid_idx on SEG01_root_account_aggregate_tbl(aggregate_id)
    exec seg01_log '    created <SEG01_root_account_aggregate_tbl>'
    commit


    --create an audit like table, where an ID can be assigned for the build
    IF object_id('SEG01_root_aggregation_built_tbl') IS NOT NULL
        BEGIN
            DROP TABLE SEG01_root_aggregation_built_tbl
        END
    create table SEG01_root_aggregation_built_tbl(
        uniqid          bigint  NOT NULL    identity,
        universe_id     bigint  NOT NULL,
        root_id         bigint  NOT NULL,
        temporal_id     bigint  NOT NULL,
        metric_id       bigint  NOT NULL
    )
    exec seg01_log '    created <SEG01_root_aggregation_built_tbl>'
    commit

    --select top 100 *
    -- from SEG01_root_account_aggregate_tbl


    IF object_id('SEG01_universe_tbl') IS NOT NULL
        BEGIN
            DROP TABLE SEG01_universe_tbl
        END
    create table SEG01_universe_tbl(
        uniqid           bigint        NOT NULL, --universe_id
        account_number   varchar(24)   NOT NULL
    )
    exec seg01_log '    created <SEG01_universe_tbl>'
    commit

    --is this table required? -- NOT CURRENTLY BEING USED....
    IF object_id('SEG01_universal_aggregtate_tbl') IS NOT NULL
        BEGIN
            DROP TABLE SEG01_universal_aggregtate_tbl
        END
    create table SEG01_universal_aggregtate_tbl(
        uniqid              bigint        NOT NULL   identity,
        universe_id         bigint        NOT NULL,
        aggregation_id      bigint        NOT NULL,
        temporal_id         bigint        NOT NULL,
        metric_id           bigint        NOT NULL,
        aggregation_value   double        NOT NULL
    )
    exec seg01_log '    created <SEG01_universal_aggregtate_tbl>'
    commit


    ------ NOT CURRENTLY USED.....
    IF object_id('SEG01_trunk_aggregation_desc_tbl') IS NOT NULL
        BEGIN
            DROP TABLE SEG01_trunk_aggregation_desc_tbl
        END
    create table SEG01_trunk_aggregation_desc_tbl(
        uniqid          bigint  NOT NULL    identity,
        trunk_id        bigint  NOT NULL,
        universe_id     bigint  NOT NULL,
        root_id         bigint  DEFAULT NULL,  --will either be a root_id or base_/trunk_id in this table
        base_trunk_id   bigint  DEFAULT NULL,
        temporal_id     bigint  NOT NULL,
        metric_id       bigint  NOT NULL,
        rule_id         bigint  NOT NULL
    )
    exec seg01_log '    created <SEG01_trunk_aggregation_desc_tbl>'
    commit

    /*
    select *
    from SEG01_trunk_aggregation_desc_tbl
    */


    IF object_id('SEG01_trunk_aggregation_results_tbl') IS NOT NULL
        BEGIN
            DROP TABLE SEG01_trunk_aggregation_results_tbl
        END
    create table SEG01_trunk_aggregation_results_tbl(
        account_number    varchar(24)     NOT NULL,
        trunk_id          bigint          NOT NULL,
        filter_id         bigint          NOT NULL,
        metric_id         bigint          NOT NULL,
        aggregation_value double          NOT NULL
    )
    create hg index SEG01_trunk_agg_res_ac_idx on SEG01_trunk_aggregation_results_tbl(account_number)
    create hg index SEG01_trunk_agg_res_leafid_idx on SEG01_trunk_aggregation_results_tbl(trunk_id)
    exec seg01_log '    created <SEG01_trunk_aggregation_results_tbl>'
    commit


    IF object_id('SEG01_leaf_aggregation_results_tbl') IS NOT NULL
        BEGIN
            DROP TABLE SEG01_leaf_aggregation_results_tbl
        END
    create table SEG01_leaf_aggregation_results_tbl(
        account_number    varchar(24)     NOT NULL,
        leaf_id           bigint          NOT NULL,
        construct_id      bigint          NOT NULL,
        aggregation_value double          NOT NULL
    )
    create hg index SEG01_leaf_agg_res_ac_idx on SEG01_leaf_aggregation_results_tbl(account_number)
    create hg index SEG01_leaf_agg_res_leafid_idx on SEG01_leaf_aggregation_results_tbl(leaf_id)
    exec seg01_log '    created <SEG01_leaf_aggregation_results_tbl>'
    commit


    IF object_id('SEG01_leaf_aggregation_desc_tbl') IS NOT NULL
        BEGIN
            DROP TABLE SEG01_leaf_aggregation_desc_tbl
        END
    create table SEG01_leaf_aggregation_desc_tbl(
        uniqid            bigint          NOT NULL identity,
        leaf_id           bigint          NOT NULL,
        aggregation_id    bigint          NOT NULL,
        aggregation_type  integer         NOT NULL,
        operator_type     integer         NOT NULL,
        operator_type_str varchar(1)      NOT NULL,
        construct_id      bigint          NOT NULL--not sure this is required, but should represent the metric (for example share, or duration)
    )
    create hg index SEG01_leaf_agg_desc_aggid_idx on SEG01_leaf_aggregation_desc_tbl(aggregation_id)
    create hg index SEG01_leaf_agg_desc_aggtype_idx on SEG01_leaf_aggregation_desc_tbl(aggregation_type)
    create hg index SEG01_leaf_agg_desc_leafid_idx on SEG01_leaf_aggregation_desc_tbl(leaf_id)
    exec seg01_log '    created SEG01_leaf_aggregation_desc_tbl'
    commit

select *
from SEG01_leaf_aggregation_desc_tbl


END;



----------------------------- ###################################################
------------------------------    E N D    T A B L E    C R E A T I O N     ------
------------------------------- ###################################################




