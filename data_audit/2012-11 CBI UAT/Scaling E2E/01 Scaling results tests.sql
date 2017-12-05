select
      adjusted_event_start_date_vespa as scaling_date,
      pty_country_code,
      max(calculated_scaling_weight) as x_max_weight,
      avg(calculated_scaling_weight) as x_av_weight,
      sum(calculated_scaling_weight) as x_sum_of_weights,
      count(distinct a.account_number) as x_vespa_panel
  into --drop table
       e2e_tests_scaling
  from sk_prod.VIQ_VIEWING_DATA_SCALING a,
       sk_prod.CUST_SINGLE_ACCOUNT_VIEW b
 where a.account_number = b.account_number
 group by adjusted_event_start_date_vespa, pty_country_code;
commit;



select
      va.scaling_date,

      max(va.max_weight) as max_weight,
      max(case when sk.pty_country_code = 'GBR' then sk.x_max_weight else 0 end) as x_max_weight_GBR,
      max(case when sk.pty_country_code = 'IRL' then sk.x_max_weight else 0 end) as x_max_weight_IRL,

      max(va.av_weight) as av_weight,
      max(case when sk.pty_country_code = 'GBR' then sk.x_av_weight else 0 end) as x_av_weight_GBR,
      max(case when sk.pty_country_code = 'IRL' then sk.x_av_weight else 0 end) as x_av_weight_IRL,

      max(va.sum_of_weights) as sum_of_weights,
      max(case when sk.pty_country_code = 'GBR' then sk.x_sum_of_weights else 0 end) as x_sum_of_weights_GBR,
      max(case when sk.pty_country_code = 'IRL' then sk.x_sum_of_weights else 0 end) as x_sum_of_weights_IRL,

      max(va.vespa_panel) as vespa_panel,
      max(case when sk.pty_country_code = 'GBR' then sk.x_vespa_panel else 0 end) as x_vespa_panel_GBR,
      max(case when sk.pty_country_code = 'IRL' then sk.x_vespa_panel else 0 end) as x_vespa_panel_IRL

  from vespa_analysts.SC2_metrics as va,
       e2e_tests_scaling sk
 where va.scaling_date = sk.scaling_date
 group by va.scaling_date;


















