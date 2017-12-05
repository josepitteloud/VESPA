--drop UAT Tables

if object_id('data_quality_adsmart_campaign_data_audit') is not null drop table Data_Quality_Vespa_STB_Checks
commit

if object_id('data_quality_adsmart_hh_data_audit') is not null drop table data_quality_adsmart_hh_data_audit commit

if object_id('data_quality_adsmart_segment_data_audit') is not null drop table data_quality_adsmart_segment_data_audit commit

if object_id ('data_quality_channel_check') is not null drop table data_quality_channel_check commit

if object_id ('data_quality_check_details') is not null drop table data_quality_check_details commit

if object_id ('data_quality_check_type') is not null drop table data_quality_check_type commit

if object_id ('data_quality_columns') is not null drop table data_quality_columns commit

if object_id ('data_quality_dp_data_audit') is not null drop table data_quality_dp_data_audit commit

if object_id ('data_quality_results') is not null drop table data_quality_results commit

if object_id ('data_quality_run_group') is not null drop table data_quality_run_group commit

if object_id ('data_quality_scaling_accounts') is not null drop table data_quality_scaling_accounts commit

if object_id ('data_quality_sky_base_upscale') is not null drop table data_quality_sky_base_upscale commit

if object_id ('data_quality_vespa_metrics') is not null drop table data_quality_vespa_metrics commit

if object_id ('data_quality_vespa_repository') is not null drop table data_quality_vespa_repository commit

if object_id ('data_quality_vespa_repository_reporting') is not null drop table data_quality_vespa_repository_reporting commit

if object_id ('DATA_QUALITY_ADSMART_SLOT_DATA_AUDIT') is not null drop table DATA_QUALITY_ADSMART_SLOT_DATA_AUDIT commit

if object_id ('DATA_QUALITY_LINEAR_SLOT_CAMPAIGN_DATA_AUDIT') is not null drop table DATA_QUALITY_LINEAR_SLOT_CAMPAIGN_DATA_AUDIT commit


-------------------CREATE UAT TABLES -------------------------------

CREATE TABLE data_quality_adsmart_campaign_data_audit (
        ADSMART_CAMPAIGN_KEY bigint NOT NULL DEFAULT NULL,
        AGENCY_KEY         bigint DEFAULT NULL,
        SEGMENT_KEY        bigint DEFAULT NULL,
        campaign_actual_impressions numeric(13, 6) DEFAULT NULL,
        campaign_actual_impressions_day_one_weighted numeric(13, 6) DEFAULT NULL,
        campaign_actual_serves int DEFAULT NULL,
        campaign_days_run  smallint DEFAULT NULL,
        campaign_percentage_campaign_run numeric(5, 2) DEFAULT NULL,
        campaign_percentage_target_achieved numeric(5, 2) DEFAULT NULL,
        campaign_Sample_impressions int DEFAULT NULL,
        campaign_sample_serves numeric(13, 6) DEFAULT NULL,
        campaign_segment_measurement_panel int DEFAULT NULL,
        campaign_Segment_universe_size_day_one_weighted numeric(13, 6) DEFAULT NULL,
        campaign_target_impressions int DEFAULT NULL,
        campaign_tracking_index numeric(5, 2) DEFAULT NULL,
        adsmart_campaign_code int DEFAULT NULL,
        adsmart_campaign_active_length int DEFAULT NULL,
        adsmart_campaign_budget numeric(38, 5) DEFAULT NULL,
        adsmart_campaign_start_date varchar(8) DEFAULT NULL,
        adsmart_campaign_end_date varchar(8) DEFAULT NULL,
        adsmart_campaign_status varchar(1) DEFAULT NULL,
        break_spacing      int DEFAULT NULL,
        daily_business_pvr_cap int DEFAULT NULL,
        daily_tech_pvr_cap int DEFAULT NULL,
        total_business_pvr_cap int DEFAULT NULL,
        total_tech_pvr_cap int DEFAULT NULL,
        segment_status     varchar(50) DEFAULT NULL,
        segment_description varchar(1000) DEFAULT NULL
)
GO




CREATE TABLE data_quality_adsmart_hh_data_audit (
        account_number     varchar(20) DEFAULT NULL,
        household_key      bigint DEFAULT NULL,
        segment_date_key   int DEFAULT NULL,
        segment_key        bigint DEFAULT NULL,
        segment_status     varchar(50) DEFAULT NULL,
        segment_description varchar(1000) DEFAULT NULL,
        hh_has_adsmart_stb varchar(8) DEFAULT NULL,
        no_of_adsmart_stb  smallint DEFAULT NULL
)
GO





CREATE TABLE data_quality_adsmart_segment_data_audit (
        segment_key        bigint DEFAULT NULL,
        segment_date_key   int DEFAULT NULL,
        measurement_panel_billable_customer_accounts int DEFAULT NULL,
        measurement_panel_dth_active_viewing_cards int DEFAULT NULL,
        universe_size_billable_customer_accounts int DEFAULT NULL,
        universe_size_dth_active_viewing_cards int DEFAULT NULL,
        universe_size_weighted_billable_customer_accounts numeric(13, 6) DEFAULT NULL,
        universe_size_weighted_dth_active_viewing_cards numeric(13, 6) DEFAULT NULL,
        segment_status     varchar(50) DEFAULT NULL,
        segment_description varchar(1000) DEFAULT NULL
)
GO





CREATE TABLE data_quality_channel_check (
        pk_dq_chan_chk     int ,
        service_key        int DEFAULT NULL,
        channel_name       varchar(100) DEFAULT NULL,
        viewing_data_date  timestamp DEFAULT NULL,
        live_recorded      varchar(8) DEFAULT NULL,
        num_of_instances   int DEFAULT NULL,
        dq_run_id          int DEFAULT NULL,
        load_timestamp     timestamp DEFAULT timestamp
)
GO




CREATE TABLE data_quality_check_details (
        dq_check_detail_id bigint ,
        dq_col_id          bigint NOT NULL DEFAULT NULL,
        dq_sched_run_id    bigint NOT NULL DEFAULT NULL,
        dq_check_type_Id   bigint NOT NULL DEFAULT NULL,
        expected_value     varchar(20) DEFAULT NULL,
        metric_benchmark   numeric(16, 3) DEFAULT NULL,
        metric_tolerance_amber numeric(6, 3) DEFAULT NULL,
        metric_tolerance_red numeric(6, 3) DEFAULT NULL,
        unknown_value      varchar(20) DEFAULT NULL,
        load_timestamp     timestamp DEFAULT NULL,
        modified_date      timestamp DEFAULT timestamp,
        metric_short_name  varchar(200) DEFAULT NULL,
        exception_value    varchar(255) DEFAULT NULL,
        notnull_col_checks varchar(1000) DEFAULT NULL,
        sql_where_clause   varchar(2000) DEFAULT NULL,
        fk_dq_col_id       int DEFAULT NULL
)
GO


CREATE TABLE data_quality_check_type (
        dq_check_type_Id   bigint ,
        dq_check_type      varchar(200) DEFAULT NULL,
        load_timestamp     timestamp DEFAULT NULL,
        modified_date      timestamp DEFAULT timestamp
)
GO


CREATE TABLE data_quality_columns (
        dq_col_id          bigint ,
        creator            varchar(50) DEFAULT NULL,
        table_name         varchar(200) NOT NULL DEFAULT NULL,
        column_name        varchar(200) NOT NULL DEFAULT NULL,
        column_type        varchar(50) NOT NULL DEFAULT NULL,
        column_length      int NOT NULL DEFAULT NULL,
        load_timestamp     timestamp DEFAULT NULL,
        modified_date      timestamp DEFAULT timestamp
)
GO

ALTER TABLE kinnairt.data_quality_columns ADD PRIMARY KEY (table_name, column_name)
GO


CREATE TABLE data_quality_dp_data_audit (
        viewing_date       date DEFAULT NULL,
        pk_viewing_prog_instance_fact bigint DEFAULT NULL,
        cb_change_date     date DEFAULT NULL,
        dk_barb_min_start_datehour_dim int DEFAULT NULL,
        dk_barb_min_start_time_dim int DEFAULT NULL,
        dk_barb_min_end_datehour_dim int DEFAULT NULL,
        dk_barb_min_end_time_dim int DEFAULT NULL,
        dk_channel_dim     int DEFAULT NULL,
        dk_event_start_datehour_dim int DEFAULT NULL,
        dk_event_start_time_dim int DEFAULT NULL,
        dk_event_end_datehour_dim int DEFAULT NULL,
        dk_event_end_time_dim int DEFAULT NULL,
        dk_instance_start_datehour_dim int DEFAULT NULL,
        dk_instance_start_time_dim int DEFAULT NULL,
        dk_instance_end_datehour_dim int DEFAULT NULL,
        dk_instance_end_time_dim int DEFAULT NULL,
        dk_programme_dim   bigint DEFAULT NULL,
        dk_programme_instance_dim bigint DEFAULT NULL,
        dk_viewing_event_dim bigint DEFAULT NULL,
        genre_description  varchar(20) DEFAULT NULL,
        sub_genre_description varchar(20) DEFAULT NULL,
        service_type       bigint DEFAULT NULL,
        service_type_description varchar(40) DEFAULT NULL,
        type_of_viewing_event varchar(40) DEFAULT NULL,
        account_number     varchar(20) DEFAULT NULL,
        panel_id           tinyint DEFAULT NULL,
        live_recorded      varchar(8) DEFAULT NULL,
        barb_min_start_date_time_utc timestamp DEFAULT NULL,
        barb_min_end_date_time_utc timestamp DEFAULT NULL,
        event_start_date_time_utc timestamp DEFAULT NULL,
        event_end_date_time_utc timestamp DEFAULT NULL,
        instance_start_date_time_utc timestamp DEFAULT NULL,
        instance_end_date_time_utc timestamp DEFAULT NULL,
        dk_capping_end_datehour_dim int DEFAULT NULL,
        dk_capping_end_time_dim int DEFAULT NULL,
        capping_end_date_time_utc timestamp DEFAULT NULL,
        log_start_date_time_utc timestamp DEFAULT NULL,
        duration           int DEFAULT NULL,
        subscriber_id      numeric(9, 0) DEFAULT NULL,
        log_received_start_date_time_utc timestamp DEFAULT NULL,
        capped_full_flag   bit NOT NULL DEFAULT NULL,
        capped_partial_flag bit NOT NULL DEFAULT NULL,
        service_key        int DEFAULT NULL
)
GO


CREATE TABLE data_quality_results (
        dq_res_id          bigint ,
        dq_check_detail_id bigint NOT NULL DEFAULT NULL,
        dq_run_id          bigint NOT NULL DEFAULT NULL,
        result             bigint DEFAULT NULL,
        RAG_STATUS         varchar(5) DEFAULT NULL,
        sql_processed      varchar(8000) DEFAULT NULL,
        date_period        date DEFAULT NULL,
        data_total         bigint DEFAULT NULL,
        logger_id          bigint DEFAULT NULL,
        data_date          date DEFAULT NULL,
        load_timestamp     timestamp DEFAULT NULL,
        modified_date      timestamp DEFAULT timestamp,
        result_text        varchar(255) DEFAULT NULL
)
GO


CREATE TABLE data_quality_run_group (
        dq_run_id          bigint ,
        run_type           varchar(100) DEFAULT NULL,
        load_timestamp     timestamp DEFAULT NULL,
        modified_date      timestamp DEFAULT timestamp
)
GO



CREATE TABLE data_quality_scaling_accounts (
        account_number     varchar(20) DEFAULT NULL
)
GO


CREATE TABLE data_quality_sky_base_upscale (
        sky_base_upscale_total bigint DEFAULT NULL,
        event_date         date DEFAULT NULL
)
GO

CREATE TABLE data_quality_vespa_metrics (
        dq_vm_id           int ,
        metric_short_name  varchar(200) DEFAULT NULL,
        metric_description varchar(1000) DEFAULT NULL,
        metric_benchmark   numeric(16, 3) DEFAULT NULL,
        metric_tolerance_amber numeric(6, 3) DEFAULT NULL,
        metric_tolerance_red numeric(6, 3) DEFAULT NULL,
        load_timestamp     timestamp DEFAULT NULL,
        modified_date      timestamp DEFAULT timestamp,
        metric_grouping    varchar(30) DEFAULT NULL,
        current_flag       int DEFAULT 1
)
GO


CREATE TABLE data_quality_vespa_repository (
        dq_vr_id           bigint ,
        dq_run_id          bigint NOT NULL DEFAULT NULL,
        viewing_data_date  date DEFAULT NULL,
        dq_vm_id           bigint NOT NULL DEFAULT NULL,
        metric_result      numeric(16, 3) DEFAULT NULL,
        metric_tolerance_amber numeric(6, 3) DEFAULT NULL,
        metric_tolerance_red numeric(6, 3) DEFAULT NULL,
        metric_rag         varchar(8) DEFAULT NULL,
        load_timestamp     timestamp DEFAULT NULL,
        modified_date      timestamp DEFAULT timestamp
)
GO

CREATE TABLE data_quality_vespa_repository_reporting (
        dq_vr_id           bigint DEFAULT NULL,
        dq_run_id          bigint NOT NULL DEFAULT NULL,
        viewing_data_date  date DEFAULT NULL,
        dq_vm_id           bigint NOT NULL DEFAULT NULL,
        metric_result      numeric(16, 3) DEFAULT NULL,
        metric_tolerance_amber numeric(6, 3) DEFAULT NULL,
        metric_tolerance_red numeric(6, 3) DEFAULT NULL,
        metric_rag         varchar(8) DEFAULT NULL,
        load_timestamp     timestamp DEFAULT NULL,
        modified_date      timestamp DEFAULT NULL
)
GO



CREATE TABLE DATA_QUALITY_ADSMART_SLOT_DATA_AUDIT (
        fact_viewing_slot_instance_key bigint NOT NULL DEFAULT NULL,
        adsmart_campaign_key bigint DEFAULT NULL,
        agency_key         bigint DEFAULT NULL,
        broadcast_channel_key bigint DEFAULT NULL,
        broadcast_start_date_key int DEFAULT NULL,
        broadcast_start_time_key int DEFAULT NULL,
        preceding_programme_schedule_key bigint DEFAULT NULL,
        succeeding_programme_schedule_key bigint DEFAULT NULL,
        segment_key        bigint DEFAULT NULL,
        slot_copy_key      bigint DEFAULT NULL,
        slot_reference_key bigint DEFAULT NULL,
        time_shift_key     smallint DEFAULT NULL,
        viewed_start_date_key int DEFAULT NULL,
        viewed_start_time_key int DEFAULT NULL,
        actual_impacts     smallint DEFAULT NULL,
        actual_impressions numeric(13, 6) DEFAULT NULL,
        actual_impressions_day_one_weighted numeric(13, 6) DEFAULT NULL,
        actual_serves      int DEFAULT NULL,
        actual_weight      numeric(13, 6) DEFAULT NULL,
        sample_impressions int DEFAULT NULL,
        viewed_duration    int DEFAULT NULL,
	household_key	   bigint DEFAULT NULL,
        viewing_type       varchar(10) DEFAULT NULL,
        timeshift_band     varchar(20) DEFAULT NULL,
        elapsed_days       varchar(5) DEFAULT NULL,
        elapsed_hours      varchar(5) DEFAULT NULL,
        elapsed_hours_total varchar(5) DEFAULT NULL,
        advertiser_code    varchar(32) DEFAULT NULL,
        advertiser_name    varchar(50) DEFAULT NULL,
        barb_sales_house_id int DEFAULT NULL,
        buyer_code         varchar(32) DEFAULT NULL,
        sales_house_name   varchar(30) DEFAULT NULL,
        sales_house_short_name varchar(10) DEFAULT NULL,
        buyer_name         varchar(50) DEFAULT NULL,
        channel_format     varchar(200) DEFAULT NULL,
        media_adsmart_status varchar(20) DEFAULT NULL,
        vespa_channel_name varchar(200) DEFAULT NULL,
        preceding_programme_broadcast_start_date_key int DEFAULT NULL,
        preceding_programme_broadcast_start_time_key int DEFAULT NULL,
        preceding_programme_broadcast_end_date_key int DEFAULT NULL,
        preceding_programme_broadcast_end_time_key int DEFAULT NULL,
        succ_programme_broadcast_start_date_key int DEFAULT NULL,
        succ_programme_broadcast_start_time_key int DEFAULT NULL,
        succ_programme_broadcast_end_date_key int DEFAULT NULL,
        succ_programme_broadcast_end_time_key int DEFAULT NULL,
        segment_id         int DEFAULT NULL,
        segment_name       varchar(6) DEFAULT NULL,
        segment_status     varchar(50) DEFAULT NULL,
        segment_description varchar(1000) DEFAULT NULL,
        slot_copy_duration_seconds int DEFAULT NULL,
        slot_type          varchar(15) DEFAULT NULL,
        product_code       varchar(32) DEFAULT NULL,
        product_name       varchar(100) DEFAULT NULL,
        slot_reference_slot_type varchar(15) DEFAULT NULL,
        slot_sub_type      varchar(15) DEFAULT NULL,
        slot_duration_seconds int DEFAULT NULL,
        slot_duration_reported_Seconds int DEFAULT NULL,
        spot_position_in_break varchar(15) DEFAULT NULL,
        slot_type_position int DEFAULT NULL,
        slot_type_total_position int DEFAULT NULL,
        break_position     varchar(15) DEFAULT NULL,
        adsmart_action     varchar(30) DEFAULT NULL,
        adsmart_priority   int DEFAULT NULL,
        adsmart_status     varchar(20) DEFAULT NULL,
        adsmart_total_priority int DEFAULT NULL,
        broadcast_start_utc_time time DEFAULT NULL,
        start_broadcast_time varchar(6) DEFAULT NULL,
        start_spot_standard_daypart_uk varchar(255) DEFAULT NULL,
        viewing_start_utc_time time DEFAULT NULL,
        viewing_start_broadcast_time varchar(6) DEFAULT NULL,
        viewing_start_spot_standard_daypart_uk varchar(255) DEFAULT NULL,
        broadcast_start_datehour_utc timestamp DEFAULT NULL,
        broadcast_start_day_date date DEFAULT NULL,
        broadcast_start_weekday varchar(3) DEFAULT NULL,
        broadcast_start_day_in_month tinyint DEFAULT NULL,
        broadcast_start_day_in_week tinyint DEFAULT NULL,
        broadcast_start_day_long varchar(20) DEFAULT NULL,
        utc_start_day_date date DEFAULT NULL,
        utc_start_weekday  varchar(3) DEFAULT NULL,
        utc_start_day_in_month tinyint DEFAULT NULL,
        utc_start_day_in_week tinyint DEFAULT NULL,
        utc_start_day_long varchar(20) DEFAULT NULL,
        viewing_start_datehour_utc timestamp DEFAULT NULL,
        viewing_start_day_date date DEFAULT NULL,
        viewing_start_weekday varchar(3) DEFAULT NULL,
        viewing_start_day_in_month tinyint DEFAULT NULL,
        viewing_start_day_in_week tinyint DEFAULT NULL,
        viewing_start_day_long varchar(20) DEFAULT NULL,
        utc_viewing_start_day_date date DEFAULT NULL,
        utc_viewing_start_weekday varchar(3) DEFAULT NULL,
        utc_viewing_start_day_in_month tinyint DEFAULT NULL,
        utc_viewing_start_day_in_week tinyint DEFAULT NULL,
        utc_viewing_start_day_long varchar(20) DEFAULT NULL
)
GO


CREATE TABLE DATA_QUALITY_LINEAR_SLOT_CAMPAIGN_DATA_AUDIT (
        slot_instance_key  bigint NOT NULL DEFAULT NULL,
        channel_key        int NOT NULL DEFAULT NULL,
        slot_start_date_key int NOT NULL DEFAULT NULL,
        slot_start_time_key int NOT NULL DEFAULT NULL,
        slot_end_date_key  int NOT NULL DEFAULT NULL,
        slot_end_time_key  int NOT NULL DEFAULT NULL,
        slot_instance_position bigint NOT NULL DEFAULT NULL,
        slot_instance_total_position bigint NOT NULL DEFAULT NULL,
        slot_type_position bigint NOT NULL DEFAULT NULL,
        slot_type_total_position bigint NOT NULL DEFAULT NULL,
        slot_key           bigint NOT NULL DEFAULT NULL,
        buyer_key          bigint NOT NULL DEFAULT NULL,
        advertiser_key     bigint NOT NULL DEFAULT NULL,
        sales_house_key    bigint NOT NULL DEFAULT NULL,
        spot_position_key  int NOT NULL DEFAULT NULL,
        media_code         varchar(255) DEFAULT NULL,
        slot_type          varchar(15) DEFAULT NULL,
        slot_name          varchar(255) DEFAULT NULL,
        slot_duration      int DEFAULT NULL,
        clearcast_commercial_no varchar(15) DEFAULT NULL,
        product_code       varchar(50) DEFAULT NULL,
        product_name       varchar(255) DEFAULT NULL,
        previous_programme_key bigint NOT NULL DEFAULT NULL,
        prev_prog_schedule_key bigint NOT NULL DEFAULT NULL,
        next_programme_key bigint NOT NULL DEFAULT NULL,
        next_prog_schedule_key bigint NOT NULL DEFAULT NULL,
        prev_broadcast_start_date date DEFAULT NULL,
        prev_broadcast_end_date date DEFAULT NULL,
        next_broadcast_start_date date DEFAULT NULL,
        next_broadcast_end_date date DEFAULT NULL,
        prev_broadcast_start_time time DEFAULT NULL,
        prev_broadcast_end_time time DEFAULT NULL,
        next_broadcast_start_time time DEFAULT NULL,
        next_broadcast_end_time time DEFAULT NULL,
        slot_start_date    date DEFAULT NULL,
        slot_end_date      date DEFAULT NULL,
        slot_start_time    time DEFAULT NULL,
        slot_end_time      time DEFAULT NULL,
        advertiser_code    varchar(10) DEFAULT NULL,
        advertiser_name    varchar(255) DEFAULT NULL,
        buyer_code         varchar(10) DEFAULT NULL,
        buyer_source       varchar(255) DEFAULT NULL,
        buyer_name         varchar(255) DEFAULT NULL,
        buyer_type         varchar(10) DEFAULT NULL,
        spot_type          varchar(30) DEFAULT NULL,
        break_position     varchar(10) DEFAULT NULL,
        spot_position_in_break varchar(30) DEFAULT NULL,
        barb_sales_house_id varchar(100) DEFAULT NULL,
        media_sales_house_name varchar(255) DEFAULT NULL,
        media_sales_house_short_name varchar(255) DEFAULT NULL
)
GO

