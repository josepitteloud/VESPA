

/************************************************************************
 **
 **  Calculates the leaf aggregation according to the rules defined in
 **    the table SEG01_leaf_aggregation_defn_tbl
 **
 ************************************************************************/


CREATE or replace procedure SEG01_create_leaf_aggregation(
                    @leaf_id             bigint
                ) AS
BEGIN

  exec seg01_log 'SEG01_create_leaf_aggregation<'||now()||'>'

  declare @aggregation_type   integer
  declare @aggregation_id     bigint
  declare @operator_type      integer
  declare @construct_id       bigint        --what's this for?
  declare @max_rank           integer
  declare @rank               integer
  declare @table_name         varchar(48)
  declare @operator_type_str  varchar(1)
  declare @aggid_field_str    varchar(48)



  --work out what the construct_id should be for the rule that defines this leaf
  --left out for now... construct_id should represent the metric used (or combined) in generating the aggregation_value
  SET @construct_id = -1
  /*
  select *
  from SEG01_leaf_aggregation_defn_tbl a
  */


  --drop tables in preparation for fresh run of procedure
  IF object_id('SEG01_leaf_build_schedule_tmp') IS NOT NULL
    BEGIN
      DROP TABLE SEG01_leaf_build_schedule_tmp
    END
  IF object_id('SEG01_leaf_calculation_tmp') IS NOT NULL
    BEGIN
      DROP TABLE SEG01_leaf_calculation_tmp
    END
  IF object_id('SEG01_handle_calc_tmp') IS NOT NULL
    BEGIN
      DROP TABLE SEG01_handle_calc_tmp
    END


  select a.*, dense_rank() over(order by uniqid asc) rank
    into SEG01_leaf_build_schedule_tmp
    from SEG01_leaf_aggregation_defn_tbl a
   where leaf_id = @leaf_id
  order by uniqid


  SET @rank = 1

  select @max_rank = max(rank)
    from SEG01_leaf_build_schedule_tmp



  while(@rank<=@max_rank)
    BEGIN

        exec SEG01_log '    Starting aggregation run '||@rank

        select @aggregation_type = aggregation_type,
               @aggregation_id = aggregation_id,
               @operator_type = operator_type
          from SEG01_leaf_build_schedule_tmp
         where rank = @rank

        exec SEG01_log '      > aggregation_id<'||@aggregation_id||'>'
        exec SEG01_log '      > aggregation_type<'||@aggregation_type||'>'
        exec SEG01_log '      > operator_type<'||@operator_type||'>'
        --do some magic


        -- 1. aggregation_type [0]root, [1]trunk, [2]branch, [3]leaf
             if(@aggregation_type=0)
                BEGIN
                  SET @table_name = 'SEG01_root_account_aggregate_tbl'
                  SET @aggid_field_str = 'aggregate_id'
                END
        else if(@aggregation_type=1)
                BEGIN
                  SET @table_name = 'SEG01_trunk_aggregation_results_tbl'
                  SET @aggid_field_str = 'trunk_id'
                END
        else if(@aggregation_type=2)
                BEGIN
                  SET @table_name = 'SEG01_branch_aggregation_results_tbl'
                  SET @aggid_field_str = ''
                END
        else if(@aggregation_type=3)
                BEGIN
                  SET @table_name = 'SEG01_leaf_aggregation_results_tbl'
                  SET @aggid_field_str = 'leaf_id'
                END


        -- 2. identify operator type:  [0]+, [1]-, [2]*, [3]/
        --construct query... lets do this one step at a time...
        -- will be slower than all in one go.. but easier to code in the first instance
        -- build up the results slowly
             if(@operator_type=0) begin SET @operator_type_str = '+' end
        else if(@operator_type=1) begin SET @operator_type_str = '-' end
        else if(@operator_type=2) begin SET @operator_type_str = '*' end
        else if(@operator_type=3) begin SET @operator_type_str = '/' end


        --if first-time, then create table - else operate on table
        if (@rank = 1)
            BEGIN
                --exec SEG01_log '     rank = 1<'||@rank||'>'

                execute(' select account_number, '||@leaf_id||' leaf_id, '||@aggregation_id||' as aggregation_id, '||@aggregation_type||' as aggregation_type, '||@construct_id||' as construct_id, aggregation_value '||
                        '   into SEG01_leaf_calculation_tmp '||
                        '   from '||@table_name||' b'||
                        '  where b.'||@aggid_field_str||' = '||@aggregation_id)

                --add index
                create unique index SEG01_leaf_calculation_ac_idx on SEG01_leaf_calculation_tmp(account_number)
                commit

                -- also create audit information at this point
                INSERT into SEG01_leaf_aggregation_desc_tbl(leaf_id, aggregation_id, aggregation_type, operator_type, operator_type_str, construct_id) values(@leaf_id, @aggregation_id, @aggregation_type, null, null, @construct_id)
                commit
            END
        else
            BEGIN
                -- IT'S SLOW (DON'T KNOW HOW SLOW COMPARED - BUT IT WORKS!!
                --exec SEG01_log '     rank greater than 1<'||@rank||'>'

                --lets make a table of accounts and values - assigned by the aggregation_type field
                execute(' select b.account_number, b.aggregation_value '||
                        '   into #SEG01_ac_value_tmp '||
                        '   from '||@table_name||' b '||
                        '  where b.'||@aggid_field_str||' = '||@aggregation_id)

                if(@operator_type=0)
                  BEGIN
                    select a.account_number, handle_calc(a.aggregation_value, '+', b.aggregation_value) new_value
                      into SEG01_handle_calc_tmp
                      from SEG01_leaf_calculation_tmp a, #SEG01_ac_value_tmp b
                     where a.account_number = b.account_number
                  END
                else if(@operator_type=1)
                  BEGIN
                    select a.account_number, handle_calc(a.aggregation_value, '-', b.aggregation_value) new_value
                      into SEG01_handle_calc_tmp
                      from SEG01_leaf_calculation_tmp a, #SEG01_ac_value_tmp b
                     where a.account_number = b.account_number
                  END
                else if(@operator_type=2)
                  BEGIN
                    select a.account_number, handle_calc(a.aggregation_value, '*', b.aggregation_value) new_value
                      into SEG01_handle_calc_tmp
                      from SEG01_leaf_calculation_tmp a, #SEG01_ac_value_tmp b
                     where a.account_number = b.account_number
                  END
                else if(@operator_type=3)
                  BEGIN
                    select a.account_number, handle_calc(a.aggregation_value, '/', b.aggregation_value) new_value
                      into SEG01_handle_calc_tmp
                      from SEG01_leaf_calculation_tmp a, #SEG01_ac_value_tmp b
                     where a.account_number = b.account_number
                  END

                UPDATE SEG01_leaf_calculation_tmp c
                   SET c.aggregation_value = b.new_value
                  from SEG01_handle_calc_tmp b
                 where c.account_number = b.account_number
                   and c.aggregation_id = @aggregation_id
                   and c.aggregation_type = @aggregation_type
                commit

                drop table SEG01_handle_calc_tmp
                commit

                -- also create audit information at this point
                INSERT into SEG01_leaf_aggregation_desc_tbl(leaf_id, aggregation_id, aggregation_type, operator_type, operator_type_str, construct_id) values(@leaf_id, @aggregation_id, @aggregation_type, @operator_type, @operator_type_str, @construct_id)
                commit
            END

        exec SEG01_log '    Run aggregation '||@rank

        SET @rank = @rank + 1
    END
END;
