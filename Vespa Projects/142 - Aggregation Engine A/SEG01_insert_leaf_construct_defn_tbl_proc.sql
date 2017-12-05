
CREATE or replace procedure SEG01_insert_leaf_construct_defn_tbl(
                 in @_leaf_id              bigint,
                 in @_aggregation_id         bigint,
                 in @_aggregation_type       integer,
                -- in @_temporal_id            bigint,
                -- in @_datetime_type_field    integer,
                 in @_operator               integer, --[0]+, [1]-, [2]*, [3]/
                 in @_auto_commit            bit -- [0]false, [1]true
                ) AS
BEGIN

exec seg01_log 'SEG01_insert_leaf_construct_defn_tbl<'||@_leaf_id||', '||@_aggregation_id||', '||@_aggregation_type||', '||@_operator||'>'

execute(' INSERT into SEG01_leaf_aggregation_defn_tbl(leaf_id, aggregation_id, aggregation_type, operator_type) '||
        '    values('||@_leaf_id||', '||@_aggregation_id||', '||@_aggregation_type||', '||@_operator||')')

if (@_auto_commit = 1) commit


END;



--test this


declare @_aggregation_id         bigint
declare @_aggregation_type       integer
declare @_operator               integer --[0]+, [1]-, [2]*, [3]/
declare @_auto_commit            bit -- [0]false, [1]true
DECLARE @leaf_id bigint

exec SEG01_assign_leafid @leaf_id
--select @leaf_id


SET @_aggregation_id         = 7
SET @_aggregation_type       = 1
SET @_operator               = 0--[0]+, [1]-, [2]*, [3]/
SET @_auto_commit            = 1-- [0]false, [1]true

execute(' INSERT into SEG01_leaf_aggregation_defn_tbl(leaf_id, aggregation_id, aggregation_type, operator_type) '||
        '    values('||@leaf_id||', '||@_aggregation_id||', '||@_aggregation_type||', '||@_operator||')')

if (@_auto_commit = 1) commit


