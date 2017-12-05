
select *
from SEG01_pk_tmp



select top 1000 *
  from SEG01_combined_events_20131022141730_8tmp


--execute SEG01_add_to_metric_library  'Distinct Programmes',  'count(distinct dk_programme_instance_dim)',  'account_number'



select top 1000 *
  from SEG01_metric_library_tbl


---->   RUN FOR BOTH METRICS

DECLARE @table_name     varchar(200)
SET @table_name = 'SEG01_combined_events_20131022141730_8tmp'

-- metric
DECLARE @metric     varchar(200)
SET @metric = 'sum(period_duration)'                          -- Metric 1
--SET @metric = 'count(distinct dk_programme_instance_dim)'   -- Metric 2


DECLARE @metric_id  bigint
 SELECT @metric_id = uniqid
   from SEG01_metric_library_tbl
  where calculation = @metric


DECLARE  @trunk_id   bigint
exec SEG01_assign_trunk_aggregation_id  @trunk_id


-- group by....  should insert trunk_id in here too..
DECLARE @group_by   varchar(200)
SET @group_by = 'account_number, '||@trunk_id||', filter_id, '||@metric_id||' '



DECLARE @xsql varchar(500)
    SET @xsql = '   select '||@group_by||', '||@metric||' '||
                '     into #seg01_trunk_aggregate_tmp '||
                '     from '||@table_name||' '||
                ' group by '||@group_by||' '

execute(@xsql)


insert into SEG01_trunk_aggregation_results_tbl
  select *
    from #seg01_trunk_aggregate_tmp

commit

--451779 Row(s) affected
--451779 Row(s) affected



select top 1000 *
from SEG01_trunk_aggregation_results_tbl

select top 1000 *
from SEG01_metric_library_tbl

select top 10 *
  from seg01_trunk_filter_defn_tbl

select top 1000 *
from SEG01_root_aggregation_built_tbl



-- *****************************************************
--  AT THIS POINT - we're kinda into LEAF AGGREGATIONS **
-- *****************************************************

select top 10 *
  from seg01_trunk_filter_defn_tbl


--need a leaf aggregation defn tbl
select top 1000 *
from SEG01_leaf_aggregation_defn_tbl
--uniqid, leaf_id, step_id, trunk_id, comb_type,

--need a proc to produce the leaf definition


/*--rule example...
  leaf_1, step_1, trunk_id_7, null
  leaf_1, step_2, trunk_id_6, '/'
*/


--do an average per programme
SELECT d1.account_number, /*leaf_id,*/ d2.value/d1.value as average_duration
  FROM
    (select account_number, trunk_id, value
       from SEG01_trunk_aggregation_results_tbl
      where trunk_id = 6) d1, --freq.
    (select account_number, trunk_id, value
       from SEG01_trunk_aggregation_results_tbl
      where trunk_id = 7) d2 --duration
 WHERE d1.account_number = d2.account_number



--build leaf:
-- 1. Assign ID
-- 2. Create build definition
-- 3. Build Leaf aggregation

DECLARE @leaf_id bigint
execute SEG01_assign_leafid @leaf_id

/*
    aggregation_id          bigint      not null,           --root_id/trunk_id/leaf_id
    aggregation_type        integer     not null,           --root[0]/trunk[1]/leaf[3]
    operator_type             integer     not null            --AND[0]/OR[1]/MULTIPLY[2]/DIVIDE[3]
*/


--don't think we need temporal info here... as this will be taken from the trunk/root aggregate info..
-- but we should define a metric for the result???  -no, I think this should reside in the aggregation results tables
INSERT into SEG01_leaf_aggregation_defn_tbl(leaf_id,
                                            aggregation_id,
                                            aggregation_type,
                                            operator_type)
  values(@leaf_id, /*trunk_id*/6, /*trunk_type*/1, 0)
commit

select top 1000 *
  from SEG01_leaf_aggregation_defn_tbl



select distinct aggregation_id
  from SEG01_combined_events_20131018152411_8tmp






