--calculate effective sample size for the current panel
drop procedure PanBal_ess;
create procedure PanBal_ess as begin
       select account_number
         into #sbv
         from vespa_analysts.vespa_single_box_view
        where status_vespa ='Enabled'
          and panel='VESPA'
     group by account_number

     --create variable @max_date date
      declare @max_date date
     
       select @max_date = max(adjusted_event_start_date_vespa) from sk_prod.VIQ_VIEWING_DATA_SCALING

       select sum(calculated_scaling_weight * calculated_scaling_weight) as large
             ,sum(calculated_scaling_weight) as small
         into #ess1
         from sk_prod.viq_viewing_data_scaling as scl
              inner join #sbv on scl.account_number = #sbv.account_number
        where adjusted_event_start_date_vespa = @max_date

     select (small * small)/large from #ess1
end;
