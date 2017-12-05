  select *
     		 ,skygo_distinct_activitydate_last180days - skygo_distinct_activitydate_last90days  AS SkyGo_Logins_180to90
								,skygo_distinct_activitydate_last270days - skygo_distinct_activitydate_last180days AS SkyGo_Logins_270to180
				into #skygo1
    from TA_MODELING_RAW_DATA
				
  select *
		      ,log(skygo_distinct_activitydate_last90days) as skygo_distinct_activitydate_last90days_log
        ,log(SkyGo_Logins_180to90) as SkyGo_Logins_180to90_log
        ,log(SkyGo_Logins_270to180) as SkyGo_Logins_270to180_log
				into #skygo2
				from #skygo1
   where skygo_distinct_activitydate_last90days <> 0				
			
		select snapshot_date
		      ,avg(skygo_distinct_activitydate_last90days_log)   as skygo_distinct_activitydate_last90days_log_mean
								,stdev(skygo_distinct_activitydate_last90days_log) as skygo_distinct_activitydate_last90days_log_stdev
		      ,avg(SkyGo_Logins_180to90_log)   as SkyGo_Logins_180to90_log_mean
		      ,stdev(SkyGo_Logins_180to90_log)   as SkyGo_Logins_180to90_log_stdev
		      ,avg(SkyGo_Logins_270to180_log)   as SkyGo_Logins_270to180_log_mean
		      ,stdev(SkyGo_Logins_270to180_log)   as SkyGo_Logins_270to180_log_stdev
    into #skygo3
    from #skygo2
group by snapshot_date

  select sk2.*
        ,skygo_distinct_activitydate_last90days_log_mean
        ,skygo_distinct_activitydate_last90days_log_stdev
				    ,SkyGo_Logins_180to90_log_mean
				    ,SkyGo_Logins_180to90_log_stdev
				    ,SkyGo_Logins_270to180_log_mean
				    ,SkyGo_Logins_270to180_log_stdev
								,case when skygo_distinct_activitydate_last90days_Log is null then 0
								      when skygo_distinct_activitydate_last90days_Log < skygo_distinct_activitydate_last90days_Log_Mean - (0.5 * skygo_distinct_activitydate_last90days_Log_StDev) then 1
              when (skygo_distinct_activitydate_last90days_Log >= skygo_distinct_activitydate_last90days_Log_Mean - (0.5 * skygo_distinct_activitydate_last90days_Log_StDev)) and (skygo_distinct_activitydate_last90days_Log <= skygo_distinct_activitydate_last90days_Log_Mean + (0.5 * skygo_distinct_activitydate_last90days_Log_StDev)) then 2
														when (skygo_distinct_activitydate_last90days_Log >  skygo_distinct_activitydate_last90days_Log_Mean + (0.5 * skygo_distinct_activitydate_last90days_Log_StDev)) and (skygo_distinct_activitydate_last90days_Log <= skygo_distinct_activitydate_last90days_Log_Mean + (1   * skygo_distinct_activitydate_last90days_Log_StDev)) then 3
														else 4
								  end as Last_90_Days_Sky_Go_Distinct_Logins_Category
								,case when SkyGo_Logins_180to90_Log is null then 0
								      when SkyGo_Logins_180to90_Log  < SkyGo_Logins_180to90_Log_Mean  - (0.5 * SkyGo_Logins_180to90_Log_StDev) then 1
              when (SkyGo_Logins_180to90_Log >= SkyGo_Logins_180to90_Log_Mean - (0.5 * SkyGo_Logins_180to90_Log_StDev)) and (SkyGo_Logins_180to90_Log <= SkyGo_Logins_180to90_Log_Mean + (0.5 * SkyGo_Logins_180to90_Log_SDev)) then 2
														when (SkyGo_Logins_180to90_Log > SkyGo_Logins_180to90_Log_Mean  + (0.5 * SkyGo_Logins_180to90_Log_StDev)) and (SkyGo_Logins_180to90_Log <= SkyGo_Logins_180to90_Log_Mean + (1   * SkyGo_Logins_180to90_Log_SDev)) then 3
														else 4
								  end as 90_180_Days_Sky_Go_Distinct_Logins_Category
								,case when SkyGo_Logins_270to180_Log is null then 0
								      when SkyGo_Logins_270to180_Log  < SkyGo_Logins_270to180_Log_Mean  - (0.5 * SkyGo_Logins_270to180_Log_StDev) then 1
              when (SkyGo_Logins_270to180_Log >= SkyGo_Logins_270to180_Log_Mean - (0.5 * SkyGo_Logins_270to180_Log_StDev)) and (SkyGo_Logins_270to180_Log <= SkyGo_Logins_270to180_Log_Mean + (0.5 * SkyGo_Logins_270to180_Log_SDev)) then 2
														when (SkyGo_Logins_270to180_Log > SkyGo_Logins_270to180_Log_Mean  + (0.5 * SkyGo_Logins_270to180_Log_StDev)) and (SkyGo_Logins_270to180_Log <= SkyGo_Logins_270to180_Log_Mean + (1   * SkyGo_Logins_270to180_Log_SDev)) then 3
														else 4
								  end as 180_270_Days_Sky_Go_Distinct_Logins_Category
								,case when Last_90_Days_Sky_Go_Distinct_Logins_Category = 0 and  [90_180_Days_SkyGo_Distinct_Logins_Category] = 0 and  [180_270_Days_SkyGo_Distinct_Logins_Category] = 0 then 'Never Used'
								      when Last_90_Days_Sky_Go_Distinct_Logins_Category - (('90_180_Days_SkyGo_Distinct_Logins_Category'+'180_270_Days_SkyGo_Distinct_Logins_Category')/2) > 0 and  Last_90_Days_Sky_Go_Distinct_Logins_Category - (('90_180_Days_SkyGo_Distinct_Logins_Category'+'180_270_Days_SkyGo_Distinct_Logins_Category')/2) <= 1 then 'Slightly Increasing'
														when Last_90_Days_Sky_Go_Distinct_Logins_Category - (('90_180_Days_SkyGo_Distinct_Logins_Category'+'180_270_Days_SkyGo_Distinct_Logins_Category')/2) > 1 then 'Heavily Ucreasing'
														when Last_90_Days_Sky_Go_Distinct_Logins_Category - (('90_180_Days_SkyGo_Distinct_Logins_Category'+'180_270_Days_SkyGo_Distinct_Logins_Category')/2) < 0 and  Last_90_Days_Sky_Go_Distinct_Logins_Category - (('90_180_Days_SkyGo_Distinct_Logins_Category'+'180_270_Days_SkyGo_Distinct_Logins_Category')/2) >= -1 then 'Slightly Increasing'
														when Last_90_Days_Sky_Go_Distinct_Logins_Category - (('90_180_Days_SkyGo_Distinct_Logins_Category'+'180_270_Days_SkyGo_Distinct_Logins_Category')/2) < -1 then 'Heavily Increasing'
														else 'Constant'
								  end as 180_270_Days_Sky_Go_Distinct_Logins_Category
				into #skygo4
    from #skygo2 as sk2 
         inner join #skygo3 as sk3 on sk2.snapshot_date = sk3.snapshot_date				
								
								
								