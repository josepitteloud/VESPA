  create or replace procedure V352_M08_Metrics
         @r_type  bit = 0
        ,@today  date = today()
        ,@country bit = 0
      as begin

          truncate table panbal_metrics

            create table #PanBal_all_aggregated_results(
                   aggregation_variable                              int null
                  ,variable_value                                    varchar(60) default null
                  ,Sky_Base_Households                               int null
                  ,Panel_Households                                  decimal(10,2) null
                  ,Good_Household_Index                              double default 0 null
                  ,GHIplus1                                          double default 0 null
                  ,GHIminus1                                         double default 0 null
                  ,incr_diff                                         double default 0 null
                  ,decr_diff                                         double default 0 null
                   )

            commit
            create lf index lfagg on #PanBal_all_aggregated_results(aggregation_variable)
            create lf index lfvar on #PanBal_all_aggregated_results(variable_value)

            create table #panel_households(
                   aggregation_variable                              int null
                  ,Panel_Households                                  decimal(10,2) null
                  ,sky_base_households                               int null
                   )

            create table #PanBal_Scaling_Segment_Profiling (
                   segment_id                                        int null
                  ,Sky_Base_Households                               int null
                  ,Panel_households                                  decimal(10,2) default 0 null
                  ,actual_households                                 int null
                  ,primary key (segment_id)
                   )

            create table #panel_segmentation(
                   segment_id                                        int null
                  ,Panel_Households                                  decimal(10,2) null
                  ,actual_households                                 int null
                   )

            create table temp_PanBal_panel(
                   account_number                                    varchar(30) null
                  ,segment_id                                        int null
                   )

                -- declarations
           declare @total_sky_base  int
           declare @cow             int

            insert into #PanBal_Scaling_Segment_Profiling
                  (segment_id
                  ,Sky_Base_Households
                  )
            select segment_id
                  ,sum(case when (fin_currency_code = 'GBP' and @country = 0) or (fin_currency_code = 'EUR' and @country = 1) then 1 else 0 end) as Sky_Base_Households
              from PanBal_segment_snapshots as seg
                   inner join cust_single_account_view as sav on seg.account_number = sav.account_number
          group by segment_id

            select @total_sky_base = sum(Sky_Base_Households) from #PanBal_Scaling_Segment_Profiling

            insert into temp_PanBal_panel(
                   account_number
                  ,segment_id
                   )
            select account_number
                  ,segment_id
              from panbal_sav
             where (panel in (10, 11, 12) and @country = 0)
                or (panel in (2, 7) and @country = 1)

                if @r_type = 1 begin
                      insert into temp_PanBal_panel(account_number)
                      select account_number
                        from panbal_amends where movement in ('Account to add to secondary panels, eventually for primary panels'
                                                             ,'Account to add to primary panels from secondary panels'
                                                             )

                      delete from temp_PanBal_panel
                       where account_number in (select account_number
                                                  from panbal_amends
                                                 where movement = 'Account to remove from primary panels'
                                               )

                      update temp_PanBal_panel as bas
                         set bas.segment_id = snp.segment_id
                        from panbal_segment_snapshots as snp
                       where bas.account_number = snp.account_number

               end

            insert into #panel_segmentation(segment_id
                                           ,Panel_Households
                                           ,actual_households
                                           )
            select bas.segment_id
                  ,sum(case when sav.rq is null then 0
                                                       when sav.rq > 1 then 1
                                                       else sav.rq
                                                   end
                      )
                  ,count(1)
              from temp_PanBal_panel as bas
                   inner join panbal_sav as sav on bas.account_number = sav.account_number
          group by bas.segment_id

            update #PanBal_Scaling_Segment_Profiling as bas
               set Panel_Households     = seg.Panel_Households
                  ,actual_Households    = seg.actual_Households
              from #panel_segmentation as seg
             where bas.segment_id       = seg.segment_id

             if @r_type = 0 begin
                      insert into panbal_metrics(metric
                                                ,value
                                                )
                      select case when @country = 1 then 'ROI ' end || 'Actual / ' || var.aggregation_variable || ' / ' || value as metric
                            ,sum(actual_households) as valu
                        from #PanBal_Scaling_Segment_Profiling as ssp
                             inner join panbal_segments_lookup_normalised as ssl on ssp.segment_id = ssl.segment_id
                             left  join panbal_variables                  as var on ssl.aggregation_variable = var.id
                       where var.aggregation_variable is not null
                    group by metric
                      having valu > 0
               end

            insert into #PanBal_all_aggregated_results(
                   aggregation_variable
                  ,variable_value
                  ,Sky_Base_Households
                  ,Panel_Households
                   )
            select ssl.aggregation_variable
                  ,ssl.value
                  ,sum(sky_base_households)
                  ,sum(Panel_households)
              from #PanBal_Scaling_Segment_Profiling as ssp
                   inner join panbal_segments_lookup_normalised as ssl on ssp.segment_id = ssl.segment_id
          group by ssl.aggregation_variable
                  ,ssl.value

                -- insert panel aggregated results into metrics table
            insert into panbal_metrics(metric
                                      ,value
                                      )
            select case when @country = 1 then 'ROI ' end || case when @r_type = 1 then 'Proposed' else 'Current' end || ' Panel / ' || var.aggregation_variable || ' / ' || value as metric
                  ,sum(panel_households) as valu
              from #PanBal_Scaling_Segment_Profiling as ssp
                   inner join panbal_segments_lookup_normalised as ssl on ssp.segment_id = ssl.segment_id
                   left  join panbal_variables                  as var on ssl.aggregation_variable = var.id
          group by metric
            having valu > 0

            select account_number
              into #country
              from cust_single_account_view
             where ((@country = 1 and fin_currency_code = 'EUR')
                or  (@country = 0 and fin_currency_code = 'GBP')
                   )

                -- insert sky base aggregated results into metrics table
            insert into panbal_metrics(metric
                                      ,value
                                      )
            select case when @country = 1 then 'ROI ' end || 'Sky Base' || ' / ' || var.aggregation_variable || ' / ' || value as metric
                  ,count() as valu
              from PanBal_Segment_snapshots as snp
                   inner join #country                          as cou on snp.account_number       = cou.account_number
                   inner join panbal_segments_lookup_normalised as ssl on snp.segment_id           = ssl.segment_id
                   inner join panbal_variables                  as var on ssl.aggregation_variable = var.id
          group by metric
            having valu > 0

                -- if any values are Unknown, then we don't want to balance these
            delete from #PanBal_all_aggregated_results
             where variable_value in ('Non-scalable', 'NS', 'U', 'Not Defined', 'D) Unknown', 'Unknown', 'No Panel', 'ROI Not Defined')
                or sky_base_households < 1000
                or sky_base_households is null

          truncate table #panel_households

            insert into #panel_households(
                   aggregation_variable
                  ,Panel_Households
                  ,sky_base_households
                   )
            select aggregation_variable
                  ,sum(panel_households)
                  ,sum(sky_base_households)
              from #PanBal_all_aggregated_results
          group by aggregation_variable

                -- index value for each variable value
            update #PanBal_all_aggregated_results as bas
               set Good_Household_Index = case when hsh.Panel_Households = 0 then 0 else 100.0 *  bas.Panel_households * hsh.Sky_Base_Households / bas.Sky_Base_Households /  hsh.Panel_Households end
              from #panel_households as hsh
             where bas.aggregation_variable = hsh.aggregation_variable

                -- insert traffic lights into metric table
            insert into panbal_metrics(metric
                                      ,value
                                      )
            select case when @country = 1 then 'ROI ' end || case when @r_type = 1 then 'Proposed / ' else 'Current / ' end || cast(var.aggregation_variable as varchar(20))
                  ,sqrt(avg((coalesce(Good_Household_Index, 100) - 100) * (coalesce(Good_Household_Index, 100) - 100))) as imbalance_rating
              from panbal_variables as var
                   left join #PanBal_all_aggregated_results as bas on bas.aggregation_variable = var.id
          group by var.aggregation_variable

            declare @scaling_day date

             select @scaling_day = max(adjusted_event_start_date_vespa)
               from viq_viewing_data_scaling

             select sum(calculated_scaling_weight * calculated_scaling_weight) as large
                   ,sum(calculated_scaling_weight)                             as small
                   ,count(*)                                                   as total_accounts
                   ,adjusted_event_start_date_vespa
               into temp_ess2
               from viq_viewing_data_scaling
              where adjusted_event_start_date_vespa = @scaling_day
           group by adjusted_event_start_date_vespa

                -- insert ESS from VIQ into metrics table
            insert into panbal_metrics(metric,value)
            select case when @country = 1 then 'ROI ' end || case when @r_type = 1 then 'Proposed' else 'Current' end || ' / ESS from VIQ'
                  ,(small * small) / large
              from temp_ess2

                -- insert scaled accounts count into metrics
            insert into panbal_metrics(metric,value)
            select 'Scaled Accounts'
                  ,count(account_number)
              from viq_viewing_data_scaling
             where adjusted_event_start_date_vespa = (select max(adjusted_event_start_date_vespa) from viq_viewing_data_scaling)

              -- insert primary panel accounts count into metrics
            insert into panbal_metrics(metric,value)
            select case when @country = 1 then 'ROI ' end || case when @r_type = 1 then 'Proposed' else 'Current' end || ' / Primary panel accounts'
                  ,count(account_number)
              from temp_PanBal_panel

                -- insert primary panel box count into metrics
            insert into panbal_metrics(metric,value)
            select case when @country = 1 then 'ROI ' end || case when @r_type = 1 then 'Proposed' else 'Current' end || ' / Primary panel boxes'
                  ,sum(boxes)
              from temp_PanBal_panel as bas
                   inner join panbal_sav as sav on bas.account_number = sav.account_number

            insert into temp_PanBal_panel(
                   account_number
                   )
            select bas.account_number
              from panbal_sav as bas
                   left join temp_PanBal_panel as pan on bas.account_number = pan.account_number
             where panel in (5, 6)
               and pan.account_number is null

                if @r_type = 1 begin
                      insert into temp_PanBal_panel(
                             account_number
                             )
                      select ame.account_number
                        from panbal_amends                        as ame
                             left join temp_PanBal_panel              as bas on ame.account_number = bas.account_number
                       where bas.account_number is null
                         and movement in ('Account to remove from primary panels'
                                         ,'Account to add to secondary panels as segment backup'
                                         ,'Account to add to secondary panels for virtual panel req.'
                                         ,'Account to add to secondary panels for TA coverage'
                                         )
               end
                if @country = 0 begin
                          -- insert combined panel box count into metrics
                      insert into panbal_metrics(metric, value)
                      select case when @r_type = 1 then 'Proposed' else 'Current' end || ' / Combined panels boxes'
                            ,sum(boxes)
                        from temp_PanBal_panel as bas
                             inner join panbal_sav as sav on bas.account_number = sav.account_number

                          -- insert combined accounts count into metrics
                      insert into panbal_metrics(metric, value)
                      select case when @r_type = 1 then 'Proposed' else 'Current' end || ' / Combined panels accounts'
                            ,count(account_number)
                        from temp_PanBal_panel

                          -- insert virtual panel count into metrics
                      insert into panbal_metrics(metric, value)
                      select 'Virtual Panel Boxes'
                            ,sum(boxes)
                        from temp_PanBal_panel         as bas
                             inner join panbal_sav as sav on bas.account_number = sav.account_number
                       where vp1 = 1

                          -- insert TA propensity coverage into metrics
                      select @cow = sum(ta_propensity)
                        from panbal_sav as sav
                             left join temp_PanBal_panel as pan on sav.account_number = pan.account_number

                      insert into panbal_metrics(metric, value)
                      select 'TA coverage'
                            ,@cow / sum(ta_propensity)
                        from vespa_analysts.SkyBase_TA_scores

                          -- insert TA propensity coverage from returning accounts into metrics
                      select @cow = sum(ta_propensity)
                        from panbal_sav as sav
                             left join temp_PanBal_panel as pan on sav.account_number = pan.account_number
                       where rq >= 0.5

                          -- insert TA propensity coverage from returning accounts into metrics
                      select @cow = sum(ta_propensity)
                        from panbal_sav as sav
                             left join temp_PanBal_panel as pan on sav.account_number = pan.account_number
                       where rq >= 0.5

                      insert into panbal_metrics(metric, value)
                      select 'TA coverage from returning accounts'
                            ,@cow / sum(ta_propensity) from vespa_analysts.SkyBase_TA_scores
               end

            insert into panbal_metrics_hist(
                   metric
                  ,value
                  ,dt
                   )
            select metric
                  ,value
                  ,@today
              from panbal_metrics

              drop table temp_PanBal_panel
              drop table temp_ess2

     end; --V352_M08_Metrics
  commit;
  grant execute on V352_M08_Metrics to vespa_group_low_security;
  commit;




