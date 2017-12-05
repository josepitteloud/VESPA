setuser sk_vespa_dq


CREATE SEQUENCE dq_respository_Seq
START WITH 100
INCREMENT BY 1
NO MAXVALUE
NO CYCLE
CACHE 15;

CREATE TABLE "DATA_QUALITY_ADSMART_SLOT_DATA_AUDIT" (
        "fact_viewing_slot_instance_key" bigint NOT NULL DEFAULT NULL,
        "adsmart_campaign_key" bigint DEFAULT NULL,
        "agency_key"         bigint DEFAULT NULL,
        "broadcast_channel_key" bigint DEFAULT NULL,
        "broadcast_start_date_key" int DEFAULT NULL,
        "broadcast_start_time_key" int DEFAULT NULL,
        "preceding_programme_schedule_key" bigint DEFAULT NULL,
        "succeeding_programme_schedule_key" bigint DEFAULT NULL,
        "segment_key"        bigint DEFAULT NULL,
        "slot_copy_key"      bigint DEFAULT NULL,
        "slot_reference_key" bigint DEFAULT NULL,
        "time_shift_key"     smallint DEFAULT NULL,
        "viewed_start_date_key" int DEFAULT NULL,
        "viewed_start_time_key" int DEFAULT NULL,
        "actual_impacts"     smallint DEFAULT NULL,
        "actual_impressions" numeric(13, 6) DEFAULT NULL,
        "actual_impressions_day_one_weighted" numeric(13, 6) DEFAULT NULL,
        "actual_serves"      int DEFAULT NULL,
        "actual_weight"      numeric(13, 6) DEFAULT NULL,
        "sample_impressions" int DEFAULT NULL,
        "viewed_duration"    int DEFAULT NULL,
        "household_key"      bigint DEFAULT NULL,
        "viewing_type"       varchar(10) DEFAULT NULL,
        "timeshift_band"     varchar(20) DEFAULT NULL,
        "elapsed_days"       varchar(5) DEFAULT NULL,
        "elapsed_hours"      varchar(5) DEFAULT NULL,
        "elapsed_hours_total" varchar(5) DEFAULT NULL,
        "advertiser_code"    varchar(32) DEFAULT NULL,
        "advertiser_name"    varchar(50) DEFAULT NULL,
        "barb_sales_house_id" int DEFAULT NULL,
        "buyer_code"         varchar(32) DEFAULT NULL,
        "sales_house_name"   varchar(30) DEFAULT NULL,
        "sales_house_short_name" varchar(10) DEFAULT NULL,
        "buyer_name"         varchar(50) DEFAULT NULL,
        "channel_format"     varchar(200) DEFAULT NULL,
        "media_adsmart_status" varchar(20) DEFAULT NULL,
        "vespa_channel_name" varchar(200) DEFAULT NULL,
        "preceding_programme_broadcast_start_date_key" int DEFAULT NULL,
        "preceding_programme_broadcast_start_time_key" int DEFAULT NULL,
        "preceding_programme_broadcast_end_date_key" int DEFAULT NULL,
        "preceding_programme_broadcast_end_time_key" int DEFAULT NULL,
        "succ_programme_broadcast_start_date_key" int DEFAULT NULL,
        "succ_programme_broadcast_start_time_key" int DEFAULT NULL,
        "succ_programme_broadcast_end_date_key" int DEFAULT NULL,
        "succ_programme_broadcast_end_time_key" int DEFAULT NULL,
        "segment_id"         int DEFAULT NULL,
        "segment_name"       varchar(6) DEFAULT NULL,
        "segment_status"     varchar(50) DEFAULT NULL,
        "segment_description" varchar(1000) DEFAULT NULL,
        "slot_copy_duration_seconds" int DEFAULT NULL,
        "slot_type"          varchar(15) DEFAULT NULL,
        "product_code"       varchar(32) DEFAULT NULL,
        "product_name"       varchar(100) DEFAULT NULL,
        "slot_reference_slot_type" varchar(15) DEFAULT NULL,
        "slot_sub_type"      varchar(15) DEFAULT NULL,
        "slot_duration_seconds" int DEFAULT NULL,
        "slot_duration_reported_Seconds" int DEFAULT NULL,
        "spot_position_in_break" varchar(15) DEFAULT NULL,
        "slot_type_position" int DEFAULT NULL,
        "slot_type_total_position" int DEFAULT NULL,
        "break_position"     varchar(15) DEFAULT NULL,
        "adsmart_action"     varchar(30) DEFAULT NULL,
        "adsmart_priority"   int DEFAULT NULL,
        "adsmart_status"     varchar(20) DEFAULT NULL,
        "adsmart_total_priority" int DEFAULT NULL,
        "broadcast_start_utc_time" time DEFAULT NULL,
        "start_broadcast_time" varchar(6) DEFAULT NULL,
        "start_spot_standard_daypart_uk" varchar(255) DEFAULT NULL,
        "viewing_start_utc_time" time DEFAULT NULL,
        "viewing_start_broadcast_time" varchar(6) DEFAULT NULL,
        "viewing_start_spot_standard_daypart_uk" varchar(255) DEFAULT NULL,
        "broadcast_start_datehour_utc" timestamp DEFAULT NULL,
        "broadcast_start_day_date" date DEFAULT NULL,
        "broadcast_start_weekday" varchar(3) DEFAULT NULL,
        "broadcast_start_day_in_month" tinyint DEFAULT NULL,
        "broadcast_start_day_in_week" tinyint DEFAULT NULL,
        "broadcast_start_day_long" varchar(20) DEFAULT NULL,
        "utc_start_day_date" date DEFAULT NULL,
        "utc_start_weekday"  varchar(3) DEFAULT NULL,
        "utc_start_day_in_month" tinyint DEFAULT NULL,
        "utc_start_day_in_week" tinyint DEFAULT NULL,
        "utc_start_day_long" varchar(20) DEFAULT NULL,
        "viewing_start_datehour_utc" timestamp DEFAULT NULL,
        "viewing_start_day_date" date DEFAULT NULL,
        "viewing_start_weekday" varchar(3) DEFAULT NULL,
        "viewing_start_day_in_month" tinyint DEFAULT NULL,
        "viewing_start_day_in_week" tinyint DEFAULT NULL,
        "viewing_start_day_long" varchar(20) DEFAULT NULL,
        "utc_viewing_start_day_date" date DEFAULT NULL,
        "utc_viewing_start_weekday" varchar(3) DEFAULT NULL,
        "utc_viewing_start_day_in_month" tinyint DEFAULT NULL,
        "utc_viewing_start_day_in_week" tinyint DEFAULT NULL,
        "utc_viewing_start_day_long" varchar(20) DEFAULT NULL,
        "local_start_day_date" date DEFAULT NULL,
        "local_start_weekday" varchar(3) DEFAULT NULL,
        "local_start_day_in_month" int DEFAULT NULL,
        "local_start_day_in_week" int DEFAULT NULL,
        "local_start_day_long" varchar(10) DEFAULT NULL
)
GO




CREATE TABLE "DATA_QUALITY_ADSMART_SLOT_DATA_AUDIT_TOTAL" (
        "fact_viewing_slot_instance_key" bigint NOT NULL DEFAULT NULL,
        "adsmart_campaign_key" bigint DEFAULT NULL,
        "agency_key"         bigint DEFAULT NULL,
        "broadcast_channel_key" bigint DEFAULT NULL,
        "broadcast_start_date_key" int DEFAULT NULL,
        "broadcast_start_time_key" int DEFAULT NULL,
        "preceding_programme_schedule_key" bigint DEFAULT NULL,
        "succeeding_programme_schedule_key" bigint DEFAULT NULL,
        "segment_key"        bigint DEFAULT NULL,
        "slot_copy_key"      bigint DEFAULT NULL,
        "slot_reference_key" bigint DEFAULT NULL,
        "time_shift_key"     smallint DEFAULT NULL,
        "viewed_start_date_key" int DEFAULT NULL,
        "viewed_start_time_key" int DEFAULT NULL,
        "actual_impacts"     smallint DEFAULT NULL,
        "actual_impressions" numeric(13, 6) DEFAULT NULL,
        "actual_impressions_day_one_weighted" numeric(13, 6) DEFAULT NULL,
        "actual_serves"      int DEFAULT NULL,
        "actual_weight"      numeric(13, 6) DEFAULT NULL,
        "sample_impressions" int DEFAULT NULL,
        "viewed_duration"    int DEFAULT NULL,
        "household_key"      bigint DEFAULT NULL,
        "viewing_type"       varchar(10) DEFAULT NULL,
        "timeshift_band"     varchar(20) DEFAULT NULL,
        "elapsed_days"       varchar(5) DEFAULT NULL,
        "elapsed_hours"      varchar(5) DEFAULT NULL,
        "elapsed_hours_total" varchar(5) DEFAULT NULL,
        "advertiser_code"    varchar(32) DEFAULT NULL,
        "advertiser_name"    varchar(50) DEFAULT NULL,
        "barb_sales_house_id" int DEFAULT NULL,
        "buyer_code"         varchar(32) DEFAULT NULL,
        "sales_house_name"   varchar(30) DEFAULT NULL,
        "sales_house_short_name" varchar(10) DEFAULT NULL,
        "buyer_name"         varchar(50) DEFAULT NULL,
        "channel_format"     varchar(200) DEFAULT NULL,
        "media_adsmart_status" varchar(20) DEFAULT NULL,
        "vespa_channel_name" varchar(200) DEFAULT NULL,
        "preceding_programme_broadcast_start_date_key" int DEFAULT NULL,
        "preceding_programme_broadcast_start_time_key" int DEFAULT NULL,
        "preceding_programme_broadcast_end_date_key" int DEFAULT NULL,
        "preceding_programme_broadcast_end_time_key" int DEFAULT NULL,
        "succ_programme_broadcast_start_date_key" int DEFAULT NULL,
        "succ_programme_broadcast_start_time_key" int DEFAULT NULL,
        "succ_programme_broadcast_end_date_key" int DEFAULT NULL,
        "succ_programme_broadcast_end_time_key" int DEFAULT NULL,
        "segment_id"         int DEFAULT NULL,
        "segment_name"       varchar(6) DEFAULT NULL,
        "segment_status"     varchar(50) DEFAULT NULL,
        "segment_description" varchar(1000) DEFAULT NULL,
        "slot_copy_duration_seconds" int DEFAULT NULL,
        "slot_type"          varchar(15) DEFAULT NULL,
        "product_code"       varchar(32) DEFAULT NULL,
        "product_name"       varchar(100) DEFAULT NULL,
        "slot_reference_slot_type" varchar(15) DEFAULT NULL,
        "slot_sub_type"      varchar(15) DEFAULT NULL,
        "slot_duration_seconds" int DEFAULT NULL,
        "slot_duration_reported_Seconds" int DEFAULT NULL,
        "spot_position_in_break" varchar(15) DEFAULT NULL,
        "slot_type_position" int DEFAULT NULL,
        "slot_type_total_position" int DEFAULT NULL,
        "break_position"     varchar(15) DEFAULT NULL,
        "adsmart_action"     varchar(30) DEFAULT NULL,
        "adsmart_priority"   int DEFAULT NULL,
        "adsmart_status"     varchar(20) DEFAULT NULL,
        "adsmart_total_priority" int DEFAULT NULL,
        "broadcast_start_utc_time" time DEFAULT NULL,
        "start_broadcast_time" varchar(6) DEFAULT NULL,
        "start_spot_standard_daypart_uk" varchar(255) DEFAULT NULL,
        "viewing_start_utc_time" time DEFAULT NULL,
        "viewing_start_broadcast_time" varchar(6) DEFAULT NULL,
        "viewing_start_spot_standard_daypart_uk" varchar(255) DEFAULT NULL,
        "broadcast_start_datehour_utc" timestamp DEFAULT NULL,
        "broadcast_start_day_date" date DEFAULT NULL,
        "broadcast_start_weekday" varchar(3) DEFAULT NULL,
        "broadcast_start_day_in_month" tinyint DEFAULT NULL,
        "broadcast_start_day_in_week" tinyint DEFAULT NULL,
        "broadcast_start_day_long" varchar(20) DEFAULT NULL,
        "utc_start_day_date" date DEFAULT NULL,
        "utc_start_weekday"  varchar(3) DEFAULT NULL,
        "utc_start_day_in_month" tinyint DEFAULT NULL,
        "utc_start_day_in_week" tinyint DEFAULT NULL,
        "utc_start_day_long" varchar(20) DEFAULT NULL,
        "viewing_start_datehour_utc" timestamp DEFAULT NULL,
        "viewing_start_day_date" date DEFAULT NULL,
        "viewing_start_weekday" varchar(3) DEFAULT NULL,
        "viewing_start_day_in_month" tinyint DEFAULT NULL,
        "viewing_start_day_in_week" tinyint DEFAULT NULL,
        "viewing_start_day_long" varchar(20) DEFAULT NULL,
        "utc_viewing_start_day_date" date DEFAULT NULL,
        "utc_viewing_start_weekday" varchar(3) DEFAULT NULL,
        "utc_viewing_start_day_in_month" tinyint DEFAULT NULL,
        "utc_viewing_start_day_in_week" tinyint DEFAULT NULL,
        "utc_viewing_start_day_long" varchar(20) DEFAULT NULL
)
GO



CREATE TABLE "DATA_QUALITY_LINEAR_SLOT_CAMPAIGN_DATA_AUDIT" (
        "slot_instance_key"  bigint NOT NULL DEFAULT NULL,
        "channel_key"        int NOT NULL DEFAULT NULL,
        "slot_start_date_key" int NOT NULL DEFAULT NULL,
        "slot_start_time_key" int NOT NULL DEFAULT NULL,
        "slot_end_date_key"  int NOT NULL DEFAULT NULL,
        "slot_end_time_key"  int NOT NULL DEFAULT NULL,
        "slot_instance_position" bigint NOT NULL DEFAULT NULL,
        "slot_instance_total_position" bigint NOT NULL DEFAULT NULL,
        "slot_type_position" bigint NOT NULL DEFAULT NULL,
        "slot_type_total_position" bigint NOT NULL DEFAULT NULL,
        "slot_key"           bigint NOT NULL DEFAULT NULL,
        "buyer_key"          bigint NOT NULL DEFAULT NULL,
        "advertiser_key"     bigint NOT NULL DEFAULT NULL,
        "sales_house_key"    bigint NOT NULL DEFAULT NULL,
        "spot_position_key"  int NOT NULL DEFAULT NULL,
        "media_code"         varchar(255) DEFAULT NULL,
        "slot_type"          varchar(15) DEFAULT NULL,
        "slot_name"          varchar(255) DEFAULT NULL,
        "slot_duration"      int DEFAULT NULL,
        "clearcast_commercial_no" varchar(15) DEFAULT NULL,
        "product_code"       varchar(50) DEFAULT NULL,
        "product_name"       varchar(255) DEFAULT NULL,
        "previous_programme_key" bigint NOT NULL DEFAULT NULL,
        "prev_prog_schedule_key" bigint NOT NULL DEFAULT NULL,
        "next_programme_key" bigint NOT NULL DEFAULT NULL,
        "next_prog_schedule_key" bigint NOT NULL DEFAULT NULL,
        "prev_broadcast_start_date" date DEFAULT NULL,
        "prev_broadcast_end_date" date DEFAULT NULL,
        "next_broadcast_start_date" date DEFAULT NULL,
        "next_broadcast_end_date" date DEFAULT NULL,
        "prev_broadcast_start_time" time DEFAULT NULL,
        "prev_broadcast_end_time" time DEFAULT NULL,
        "next_broadcast_start_time" time DEFAULT NULL,
        "next_broadcast_end_time" time DEFAULT NULL,
        "slot_start_date"    date DEFAULT NULL,
        "slot_end_date"      date DEFAULT NULL,
        "slot_start_time"    time DEFAULT NULL,
        "slot_end_time"      time DEFAULT NULL,
        "advertiser_code"    varchar(10) DEFAULT NULL,
        "advertiser_name"    varchar(255) DEFAULT NULL,
        "buyer_code"         varchar(10) DEFAULT NULL,
        "buyer_source"       varchar(255) DEFAULT NULL,
        "buyer_name"         varchar(255) DEFAULT NULL,
        "buyer_type"         varchar(10) DEFAULT NULL,
        "spot_type"          varchar(30) DEFAULT NULL,
        "break_position"     varchar(10) DEFAULT NULL,
        "spot_position_in_break" varchar(30) DEFAULT NULL,
        "barb_sales_house_id" varchar(100) DEFAULT NULL,
        "media_sales_house_name" varchar(255) DEFAULT NULL,
        "media_sales_house_short_name" varchar(255) DEFAULT NULL
)
GO


CREATE TABLE "DATA_QUALITY_SLOT_DATA_AUDIT" (
        "SLOT_DATA_KEY"      bigint NOT NULL DEFAULT NULL,
        "VIEWED_START_DATE_KEY" int DEFAULT NULL,
        "IMPACTS"            smallint DEFAULT NULL,
        "RECORD_DATE"        timestamp DEFAULT NULL,
        "HOUSEHOLD_KEY"      bigint DEFAULT NULL,
        "IMPACT_DAY"         timestamp NOT NULL DEFAULT NULL,
        "slot_instance_key"  bigint DEFAULT NULL,
        "channel_key"        int DEFAULT NULL,
        "slot_start_date_key" int DEFAULT NULL,
        "slot_start_time_key" int DEFAULT NULL,
        "slot_end_date_key"  int DEFAULT NULL,
        "slot_end_time_key"  int DEFAULT NULL,
        "previous_programme_key" bigint DEFAULT NULL,
        "next_programme_key" bigint DEFAULT NULL,
        "prev_prog_schedule_key" bigint DEFAULT NULL,
        "next_prog_schedule_key" bigint DEFAULT NULL,
        "prev_broadcast_start_date" date DEFAULT NULL,
        "next_broadcast_start_date" date DEFAULT NULL,
        "prev_broadcast_start_time" time DEFAULT NULL,
        "next_broadcast_start_time" time DEFAULT NULL,
        "slot_start_date"    date DEFAULT NULL,
        "slot_end_date"      date DEFAULT NULL,
        "slot_start_time"    time DEFAULT NULL,
        "slot_end_time"      time DEFAULT NULL,
        "scaling_factor"     double DEFAULT NULL,
        "prev_broadcast_end_time" time DEFAULT NULL,
        "next_broadcast_end_time" time DEFAULT NULL,
        "prev_broadcast_end_date" date DEFAULT NULL,
        "next_broadcast_end_date" date DEFAULT NULL,
        "slot_key"           bigint DEFAULT NULL,
        "viewed_duration"    numeric(15, 0) DEFAULT NULL,
        "viewed_start_time_key" int DEFAULT NULL,
        "time_shift_key"     int DEFAULT NULL,
        "advertiser_code"    varchar(10) DEFAULT NULL,
        "advertiser_name"    varchar(255) DEFAULT NULL,
        "buyer_code"         varchar(10) DEFAULT NULL,
        "buyer_name"         varchar(255) DEFAULT NULL,
        "buyer_source"       varchar(255) DEFAULT NULL,
        "buyer_type"         varchar(10) DEFAULT NULL,
        "barb_sales_house_id" varchar(100) DEFAULT NULL,
        "media_sales_house_name" varchar(255) DEFAULT NULL,
        "media_sales_house_short_name" varchar(255) DEFAULT NULL,
        "break_position"     varchar(10) DEFAULT NULL,
        "spot_position_in_break" varchar(30) DEFAULT NULL,
        "spot_type"          varchar(30) DEFAULT NULL
)
GO



CREATE TABLE "data_quality_adsmart_campaign_data_audit" (
        "ADSMART_CAMPAIGN_KEY" bigint NOT NULL DEFAULT NULL,
        "AGENCY_KEY"         bigint DEFAULT NULL,
        "SEGMENT_KEY"        bigint DEFAULT NULL,
        "campaign_actual_impressions" numeric(13, 6) DEFAULT NULL,
        "campaign_actual_impressions_day_one_weighted" numeric(13, 6) DEFAULT NULL,
        "campaign_actual_serves" int DEFAULT NULL,
        "campaign_days_run"  smallint DEFAULT NULL,
        "campaign_percentage_campaign_run" numeric(5, 2) DEFAULT NULL,
        "campaign_percentage_target_achieved" numeric(5, 2) DEFAULT NULL,
        "campaign_Sample_impressions" int DEFAULT NULL,
        "campaign_sample_serves" numeric(13, 6) DEFAULT NULL,
        "campaign_segment_measurement_panel" int DEFAULT NULL,
        "campaign_Segment_universe_size_day_one_weighted" numeric(13, 6) DEFAULT NULL,
        "campaign_target_impressions" int DEFAULT NULL,
        "campaign_tracking_index" numeric(5, 2) DEFAULT NULL,
        "adsmart_campaign_code" int DEFAULT NULL,
        "adsmart_campaign_active_length" int DEFAULT NULL,
        "adsmart_campaign_budget" numeric(38, 5) DEFAULT NULL,
        "adsmart_campaign_start_date" varchar(8) DEFAULT NULL,
        "adsmart_campaign_end_date" varchar(8) DEFAULT NULL,
        "adsmart_campaign_status" varchar(1) DEFAULT NULL,
        "break_spacing"      int DEFAULT NULL,
        "daily_business_pvr_cap" int DEFAULT NULL,
        "daily_tech_pvr_cap" int DEFAULT NULL,
        "total_business_pvr_cap" int DEFAULT NULL,
        "total_tech_pvr_cap" int DEFAULT NULL,
        "segment_status"     varchar(50) DEFAULT NULL,
        "segment_description" varchar(1000) DEFAULT NULL
)
GO




CREATE TABLE "data_quality_adsmart_hh_data_audit" (
        "account_number"     varchar(20) DEFAULT NULL,
        "household_key"      bigint DEFAULT NULL,
        "segment_date_key"   int DEFAULT NULL,
        "segment_key"        bigint DEFAULT NULL,
        "segment_status"     varchar(50) DEFAULT NULL,
        "segment_description" varchar(1000) DEFAULT NULL,
        "hh_has_adsmart_stb" varchar(8) DEFAULT NULL,
        "no_of_adsmart_stb"  smallint DEFAULT NULL
)
GO



CREATE TABLE "data_quality_adsmart_segment_data_audit" (
        "segment_key"        bigint DEFAULT NULL,
        "segment_date_key"   int DEFAULT NULL,
        "measurement_panel_billable_customer_accounts" int DEFAULT NULL,
        "measurement_panel_dth_active_viewing_cards" int DEFAULT NULL,
        "universe_size_billable_customer_accounts" int DEFAULT NULL,
        "universe_size_dth_active_viewing_cards" int DEFAULT NULL,
        "universe_size_weighted_billable_customer_accounts" numeric(13, 6) DEFAULT NULL,
        "universe_size_weighted_dth_active_viewing_cards" numeric(13, 6) DEFAULT NULL,
        "segment_status"     varchar(50) DEFAULT NULL,
        "segment_description" varchar(1000) DEFAULT NULL
)
GO


CREATE TABLE "data_quality_channel_check" (
        "pk_dq_chan_chk"     int DEFAULT autoincrement,
        "service_key"        int DEFAULT NULL,
        "channel_name"       varchar(100) DEFAULT NULL,
        "viewing_data_date"  timestamp DEFAULT NULL,
        "live_recorded"      varchar(8) DEFAULT NULL,
        "num_of_instances"   int DEFAULT NULL,
        "dq_run_id"          int DEFAULT NULL,
        "load_timestamp"     timestamp DEFAULT timestamp
)
GO


CREATE TABLE "data_quality_channel_issues_list" (
        "service_key"        bigint NOT NULL DEFAULT NULL,
        "event_date"         varchar(8) DEFAULT NULL,
        "channel_name"       varchar(100) DEFAULT NULL,
        "playback_type"      varchar(8) DEFAULT NULL,
        "instances"          int DEFAULT NULL,
        "month"              int DEFAULT NULL,
        "year"               int DEFAULT NULL
)
GO



CREATE TABLE "data_quality_channel_service_key_list" (
        "dq_cskl_id"         bigint DEFAULT autoincrement,
        "service_key"        bigint NOT NULL DEFAULT NULL,
        "load_timestamp"     timestamp DEFAULT NULL,
        "modified_date"      timestamp DEFAULT timestamp,
        "current_flag"       bit NOT NULL DEFAULT 1
)
GO


CREATE TABLE "data_quality_check_details" (
        "dq_check_detail_id" bigint PRIMARY KEY DEFAULT autoincrement,
        "dq_col_id"          bigint NOT NULL DEFAULT NULL,
        "dq_sched_run_id"    bigint NOT NULL DEFAULT NULL,
        "dq_check_type_Id"   bigint NOT NULL DEFAULT NULL,
        "expected_value"     varchar(20) DEFAULT NULL,
        "metric_benchmark"   numeric(16, 3) DEFAULT NULL,
        "metric_tolerance_amber" numeric(6, 3) DEFAULT NULL,
        "metric_tolerance_red" numeric(6, 3) DEFAULT NULL,
        "unknown_value"      varchar(20) DEFAULT NULL,
        "load_timestamp"     timestamp DEFAULT NULL,
        "modified_date"      timestamp DEFAULT timestamp,
        "metric_short_name"  varchar(200) DEFAULT NULL,
        "exception_value"    varchar(255) DEFAULT NULL,
        "notnull_col_checks" varchar(1000) DEFAULT NULL,
        "sql_where_clause"   varchar(2000) DEFAULT NULL,
        "fk_dq_col_id"       int DEFAULT NULL
)
GO



CREATE TABLE "data_quality_check_type" (
        "dq_check_type_Id"   bigint DEFAULT autoincrement,
        "dq_check_type"      varchar(200) DEFAULT NULL,
        "load_timestamp"     timestamp DEFAULT NULL,
        "modified_date"      timestamp DEFAULT timestamp
)
GO


CREATE TABLE "data_quality_columns" (
        "dq_col_id"          bigint DEFAULT autoincrement,
        "creator"            varchar(50) DEFAULT NULL,
        "table_name"         varchar(200) NOT NULL DEFAULT NULL,
        "column_name"        varchar(200) NOT NULL DEFAULT NULL,
        "column_type"        varchar(50) NOT NULL DEFAULT NULL,
        "column_length"      int NOT NULL DEFAULT NULL,
        "load_timestamp"     timestamp DEFAULT NULL,
        "modified_date"      timestamp DEFAULT timestamp
)
GO

ALTER TABLE kinnairt.data_quality_columns ADD PRIMARY KEY (table_name, column_name)
GO


CREATE TABLE "data_quality_dp_data_audit" (
        "viewing_date"       date DEFAULT NULL,
        "pk_viewing_prog_instance_fact" bigint DEFAULT NULL,
        "cb_change_date"     date DEFAULT NULL,
        "dk_barb_min_start_datehour_dim" int DEFAULT NULL,
        "dk_barb_min_start_time_dim" int DEFAULT NULL,
        "dk_barb_min_end_datehour_dim" int DEFAULT NULL,
        "dk_barb_min_end_time_dim" int DEFAULT NULL,
        "dk_channel_dim"     int DEFAULT NULL,
        "dk_event_start_datehour_dim" int DEFAULT NULL,
        "dk_event_start_time_dim" int DEFAULT NULL,
        "dk_event_end_datehour_dim" int DEFAULT NULL,
        "dk_event_end_time_dim" int DEFAULT NULL,
        "dk_instance_start_datehour_dim" int DEFAULT NULL,
        "dk_instance_start_time_dim" int DEFAULT NULL,
        "dk_instance_end_datehour_dim" int DEFAULT NULL,
        "dk_instance_end_time_dim" int DEFAULT NULL,
        "dk_programme_dim"   bigint DEFAULT NULL,
        "dk_programme_instance_dim" bigint DEFAULT NULL,
        "dk_viewing_event_dim" bigint DEFAULT NULL,
        "genre_description"  varchar(20) DEFAULT NULL,
        "sub_genre_description" varchar(20) DEFAULT NULL,
        "service_type"       bigint DEFAULT NULL,
        "service_type_description" varchar(40) DEFAULT NULL,
        "type_of_viewing_event" varchar(40) DEFAULT NULL,
        "account_number"     varchar(20) DEFAULT NULL,
        "panel_id"           tinyint DEFAULT NULL,
        "live_recorded"      varchar(8) DEFAULT NULL,
        "barb_min_start_date_time_utc" timestamp DEFAULT NULL,
        "barb_min_end_date_time_utc" timestamp DEFAULT NULL,
        "event_start_date_time_utc" timestamp DEFAULT NULL,
        "event_end_date_time_utc" timestamp DEFAULT NULL,
        "instance_start_date_time_utc" timestamp DEFAULT NULL,
        "instance_end_date_time_utc" timestamp DEFAULT NULL,
        "dk_capping_end_datehour_dim" int DEFAULT NULL,
        "dk_capping_end_time_dim" int DEFAULT NULL,
        "capping_end_date_time_utc" timestamp DEFAULT NULL,
        "log_start_date_time_utc" timestamp DEFAULT NULL,
        "duration"           int DEFAULT NULL,
        "subscriber_id"      numeric(9, 0) DEFAULT NULL,
        "log_received_start_date_time_utc" timestamp DEFAULT NULL,
        "capped_full_flag"   bit NOT NULL DEFAULT NULL,
        "capped_partial_flag" bit NOT NULL DEFAULT NULL,
        "service_key"        int DEFAULT NULL
)
GO



CREATE TABLE "data_quality_homebase_channels" (
        "service_key"        int DEFAULT NULL,
        "hd_sd"              varchar(2) DEFAULT NULL,
        "channel_name"       varchar(100) DEFAULT NULL
)
GO


CREATE TABLE "data_quality_regression_reports_repository" (
        "dq_rrr_id"          bigint DEFAULT autoincrement,
        "report_date"        date DEFAULT NULL,
        "run_id"             bigint DEFAULT NULL,
        "report_type"        varchar(50) DEFAULT NULL,
        "metric_id"          varchar(10) DEFAULT NULL,
        "metric_result"      numeric(16, 2) DEFAULT NULL,
        "metric_threshold"   numeric(16, 2) DEFAULT NULL,
        "metric_tolerance_amber" numeric(6, 2) DEFAULT NULL,
        "metric_tolerance_red" numeric(6, 2) DEFAULT NULL,
        "rag_status"         varchar(5) DEFAULT NULL,
        "load_timestamp"     timestamp DEFAULT "today"(),
        "modified_date"      timestamp DEFAULT "today"()
)
GO


CREATE TABLE "data_quality_regression_thresholds" (
        "dq_rt_id"           bigint DEFAULT autoincrement,
        "report_type"        varchar(50) DEFAULT NULL,
        "metric_id"          varchar(10) DEFAULT NULL,
        "metric_details"     varchar(2000) DEFAULT NULL,
        "metric_threshold"   numeric(16, 2) DEFAULT NULL,
        "metric_tolerance_amber" numeric(6, 2) DEFAULT NULL,
        "metric_tolerance_red" numeric(6, 2) DEFAULT NULL,
        "load_timestamp"     timestamp DEFAULT "today"(),
        "modified_date"      timestamp DEFAULT "today"()
)
GO


CREATE TABLE "data_quality_results" (
        "dq_res_id"          bigint DEFAULT autoincrement,
        "dq_check_detail_id" bigint NOT NULL DEFAULT NULL,
        "dq_run_id"          bigint NOT NULL DEFAULT NULL,
        "result"             bigint DEFAULT NULL,
        "RAG_STATUS"         varchar(5) DEFAULT NULL,
        "sql_processed"      varchar(8000) DEFAULT NULL,
        "date_period"        date DEFAULT NULL,
        "data_total"         bigint DEFAULT NULL,
        "logger_id"          bigint DEFAULT NULL,
        "data_date"          date DEFAULT NULL,
        "load_timestamp"     timestamp DEFAULT NULL,
        "modified_date"      timestamp DEFAULT timestamp,
        "result_text"        varchar(255) DEFAULT NULL
)
GO


CREATE TABLE "data_quality_run_group" (
        "dq_run_id"          bigint PRIMARY KEY DEFAULT autoincrement,
        "run_type"           varchar(100) DEFAULT NULL,
        "load_timestamp"     timestamp DEFAULT NULL,
        "modified_date"      timestamp DEFAULT timestamp
)
GO


CREATE TABLE "data_quality_scaling_accounts" (
        "account_number"     varchar(20) DEFAULT NULL
)
GO


CREATE TABLE "data_quality_scaling_table_checks" (
        "scaling_date"       date DEFAULT NULL,
        "scaling_universe_key" varchar(100) DEFAULT NULL,
        "weight_scaled_pre_uplift" numeric(21, 6) DEFAULT NULL,
        "adsmart_scaling_weight" numeric(21, 6) DEFAULT NULL,
        "calculated_Scaling_Weight" numeric(21, 6) DEFAULT NULL,
        "distinct_accounts"  int NOT NULL DEFAULT NULL,
        "distinct_households" int DEFAULT NULL,
        "unknown_household_key" int DEFAULT NULL,
        "weight_sample_value" int DEFAULT NULL,
        "HH_COMPOSITION"     varchar(100) DEFAULT NULL,
        "TV_REGION"          varchar(100) DEFAULT NULL,
        "DTV_PACKAGE"        varchar(100) DEFAULT NULL,
        "TENURE"             varchar(100) DEFAULT NULL,
        "HD_NON_HD"          varchar(100) DEFAULT NULL,
        "SINGLE_MULTI_BOX"   varchar(100) DEFAULT NULL,
        "PVR_NON_PVR"        varchar(100) DEFAULT NULL
)
GO


CREATE TABLE "data_quality_sky_base_upscale" (
        "sky_base_upscale_total" bigint DEFAULT NULL,
        "event_date"         date DEFAULT NULL
)
GO


CREATE TABLE "data_quality_slots_daily_reporting" (
        "dq_sdr_id"          bigint DEFAULT autoincrement,
        "date_type"          varchar(20) DEFAULT NULL,
        "batch_date"         date DEFAULT NULL,
        "date_value"         date DEFAULT NULL,
        "slots_totals"       int DEFAULT NULL,
        "actual_impressions" numeric(20, 4) DEFAULT NULL,
        "segments_totals"    int DEFAULT NULL,
        "households_totals"  int DEFAULT NULL,
        "campaigns_totals"   int DEFAULT NULL,
        "load_timestamp"     timestamp DEFAULT NULL
)
GO

CREATE TABLE "data_quality_vespa_metrics" (
        "dq_vm_id"           int DEFAULT autoincrement,
        "metric_short_name"  varchar(200) DEFAULT NULL,
        "metric_description" varchar(1000) DEFAULT NULL,
        "metric_benchmark"   numeric(16, 3) DEFAULT NULL,
        "metric_tolerance_amber" numeric(6, 3) DEFAULT NULL,
        "metric_tolerance_red" numeric(6, 3) DEFAULT NULL,
        "load_timestamp"     timestamp DEFAULT NULL,
        "modified_date"      timestamp DEFAULT timestamp,
        "metric_grouping"    varchar(30) DEFAULT NULL,
        "current_flag"       int DEFAULT 1,
        "report_sub_header"  varchar(20) DEFAULT NULL
)
GO

CREATE TABLE "data_quality_vespa_repository" (
        "dq_vr_id"           bigint DEFAULT autoincrement,
        "dq_run_id"          bigint NOT NULL DEFAULT NULL,
        "viewing_data_date"  date DEFAULT NULL,
        "dq_vm_id"           bigint NOT NULL DEFAULT NULL,
        "metric_result"      numeric(16, 3) DEFAULT NULL,
        "metric_tolerance_amber" numeric(6, 3) DEFAULT NULL,
        "metric_tolerance_red" numeric(6, 3) DEFAULT NULL,
        "metric_rag"         varchar(8) DEFAULT NULL,
        "load_timestamp"     timestamp DEFAULT NULL,
        "modified_date"      timestamp DEFAULT timestamp
)
GO


CREATE TABLE "data_quality_vespa_repository_reporting" (
        "dq_vr_id"           bigint DEFAULT NULL,
        "dq_run_id"          bigint NOT NULL DEFAULT NULL,
        "viewing_data_date"  date DEFAULT NULL,
        "dq_vm_id"           bigint NOT NULL DEFAULT NULL,
        "metric_result"      numeric(16, 3) DEFAULT NULL,
        "metric_tolerance_amber" numeric(6, 3) DEFAULT NULL,
        "metric_tolerance_red" numeric(6, 3) DEFAULT NULL,
        "metric_rag"         varchar(8) DEFAULT NULL,
        "load_timestamp"     timestamp DEFAULT NULL,
        "modified_date"      timestamp DEFAULT NULL
)
GO



insert into DATA_QUALITY_ADSMART_SLOT_DATA_AUDIT_TOTAL
select * from kinnairt.DATA_QUALITY_ADSMART_SLOT_DATA_AUDIT_TOTAL

insert into DATA_QUALITY_LINEAR_SLOT_CAMPAIGN_DATA_AUDIT
select * from kinnairt.DATA_QUALITY_LINEAR_SLOT_CAMPAIGN_DATA_AUDIT

insert into  DATA_QUALITY_SLOT_DATA_AUDIT
select * from kinnairt.DATA_QUALITY_SLOT_DATA_AUDIT

insert into  data_quality_adsmart_campaign_data_audit
select * from kinnairt.data_quality_adsmart_campaign_data_audit

insert into data_quality_adsmart_hh_data_audit
select * from kinnairt.data_quality_adsmart_hh_data_audit

insert into data_quality_adsmart_segment_data_audit
insert into data_quality_channel_issues_list
select * from kinnairt.data_quality_channel_issues_list

insert into data_quality_dp_data_audit
select * from kinnairt.data_quality_dp_data_audit

insert into data_quality_homebase_channels
select * from kinnairt.data_quality_homebase_channels

insert into data_quality_scaling_accounts
select * from kinnairt.data_quality_scaling_accounts

insert into data_quality_scaling_table_checks
select * from kinnairt.data_quality_scaling_table_checks


insert into data_quality_sky_base_upscale
select * from kinnairt.data_quality_sky_base_upscale


insert into data_quality_vespa_repository_reporting
select * from kinnairt.data_quality_vespa_repository_reporting

commit

set identity_insert sales_daily on
insert sales_daily (syb_identity, stor_id) 
values (101, "1349")




SET TEMPORARY OPTION IDENTITY_INSERT = 'data_quality_channel_check'

insert into data_quality_channel_check
select * from kinnairt.data_quality_channel_check;

commit;


SET TEMPORARY OPTION IDENTITY_INSERT = '';
SET TEMPORARY OPTION IDENTITY_INSERT = 'data_quality_channel_service_key_list';
insert into data_quality_channel_service_key_list
select * from kinnairt.data_quality_channel_service_key_list;

commit;

SET TEMPORARY OPTION IDENTITY_INSERT = '';
SET TEMPORARY OPTION IDENTITY_INSERT = 'data_quality_check_details';
insert into data_quality_check_details
select * from kinnairt.data_quality_check_details;

commit;




SET TEMPORARY OPTION IDENTITY_INSERT = '';
SET TEMPORARY OPTION IDENTITY_INSERT = 'data_quality_check_type';
insert into data_quality_check_type
select * from kinnairt.data_quality_check_type;

commit;

SET TEMPORARY OPTION IDENTITY_INSERT = '';
SET TEMPORARY OPTION IDENTITY_INSERT = 'data_quality_columns';
insert into data_quality_columns
select * from kinnairt.data_quality_columns;

commit;

SET TEMPORARY OPTION IDENTITY_INSERT = '';
SET TEMPORARY OPTION IDENTITY_INSERT = 'data_quality_regression_reports_repository';
insert into data_quality_regression_reports_repository
select * from kinnairt.data_quality_regression_reports_repository;

commit;

SET TEMPORARY OPTION IDENTITY_INSERT = '';
SET TEMPORARY OPTION IDENTITY_INSERT = 'data_quality_regression_thresholds';
insert into data_quality_regression_thresholds
select * from kinnairt.data_quality_regression_thresholds;

commit;

SET TEMPORARY OPTION IDENTITY_INSERT = '';
SET TEMPORARY OPTION IDENTITY_INSERT = 'data_quality_results';
insert into data_quality_results
select * from kinnairt.data_quality_results;

commit;

SET TEMPORARY OPTION IDENTITY_INSERT = '';
SET TEMPORARY OPTION IDENTITY_INSERT = 'data_quality_run_group';
insert into data_quality_run_group
select * from kinnairt.data_quality_run_group;

commit;

SET TEMPORARY OPTION IDENTITY_INSERT = '';
SET TEMPORARY OPTION IDENTITY_INSERT = 'data_quality_slots_daily_reporting';
insert into data_quality_slots_daily_reporting
select * from kinnairt.data_quality_slots_daily_reporting;

commit;

SET TEMPORARY OPTION IDENTITY_INSERT = '';
SET TEMPORARY OPTION IDENTITY_INSERT = 'data_quality_vespa_metrics';
insert into data_quality_vespa_metrics
select * from kinnairt.data_quality_vespa_metrics;

commit;

SET TEMPORARY OPTION IDENTITY_INSERT = '';
SET TEMPORARY OPTION IDENTITY_INSERT = 'data_quality_vespa_repository';
insert into data_quality_vespa_repository
select * from kinnairt.data_quality_vespa_repository;

commit;



----if need to replace the z_logger_events to make the process work run the below code-------------------

truncate table z_logger_events

commit

SET TEMPORARY OPTION IDENTITY_INSERT = '';
SET TEMPORARY OPTION IDENTITY_INSERT = 'z_logger_events';
insert into z_logger_events
select * from kinnairt.z_logger_events

commit



---update data quality columns table so that it goes to sk_vespa_dq schema----

update data_quality_columns
set creator = 'sk_vespa_dq'
where creator = 'kinnairt'

commit
