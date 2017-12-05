
CREATE or replace procedure SEG01_insert_trunk_filter_defn_tbl(
                 in @_filter_id              bigint,
                 in @_aggregation_id         bigint,
                 in @_aggregation_type       integer,
                 in @_temporal_id            bigint,
                 in @_datetime_type_field    integer,
                 in @_filter_type            integer,
                 in @_auto_commit            bit -- [0]false, [1]true
                ) AS
BEGIN


INSERT into SEG01_trunk_filter_defn_tbl(filter_id, aggregation_id, aggregation_type, temporal_id, datetime_type_field, filter_type)
    values(@_filter_id, @_aggregation_id, @_aggregation_type, @_temporal_id, @_datetime_type_field, @_filter_type)

if (@_auto_commit = 1) commit


END;

