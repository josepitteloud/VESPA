declare @Sky_Base decimal(8,1), @Panel_Base decimal(8,1), @todt date, @todtMinus1 date, @todtMinus2 date, @todtMinus3 date, @HLAvgReturners integer, @HLBalanceIndex decimal(16,6)

declare @var_count      tinyint
declare @thevariable    varchar(20)

-- Populate the variables
select @Sky_Base   =    count(distinct account_number)
                        from    PanBal_segment_snapshots

select @Panel_Base =    count(distinct account_number)
                        from    vespa_analysts.SIG_SINGLE_ACCOUNT_VIEW
                        where   panel in ('VESPA', 'VESPA11')
                        and     status_vespa = 'Enabled'

select  @todt = case    when datepart(weekday,today()) = 7 then today()
                                else (today() - datepart(weekday,today()))
                        end

select  @todtMinus1 = @todt - 7
select  @todtMinus2 = @todtMinus1 - 7
select  @todtMinus3 = @todtMinus2 - 7

select  @var_count = min(id) from panbal_variables

Message now()||' | Building SWRQ' to client

-- Scaling Weight Reporting Quality population
select  case when dial.dt <= @todt       and dial.dt > @todtMinus1   then @todt
             when dial.dt <= @todtMinus1 and dial.dt > @todtMinus2   then @todtMinus1
             when dial.dt <= @todtMinus2 and dial.dt > @todtMinus3   then @todtMinus2
             when dial.dt <= @todtMinus3 and dial.dt > @todtMinus3-7 then @todtMinus3
             end dt
        ,acview.account_number
into    #SWRQ
from    panel_data         as dial
        inner join acview  as acview                    --*
        on  dial.account_number = acview.account_number
        and dial.dialling_b >= acview.num_boxes -- This is the condition that flags whether an account returned data or not
where   dt > @todtMinus3-7

commit
create hg index idx_accountnumber on #SWRQ(account_number)
commit

Message now()||' | Building SWRQ DONE' to client

Message now()||' | Building PanBaseHH' to client

-- Vespa panel households population
select  account_number
into    #PanBaseHH
from    vespa_analysts.SIG_SINGLE_ACCOUNT_VIEW
where   panel in ('VESPA', 'VESPA11')
and     status_vespa = 'Enabled'

commit
create hg index idx_accountnumber on #PanBaseHH(account_number)
commit

Message now()||' | Building PanBaseHH DONE' to client

-- Variable to get average returning households at high level. - 277k for Universe

Message now()||' | Calculating @HLAvgReturners' to client

select  @HLAvgReturners = avg(AvgRet)
from	(
			select  swrq.dt
					,count(swrq.account_number) / 7 as AvgRet
			from    PanBal_segment_snapshots as pss
					left join #SWRQ as swrq
					on swrq.account_number = pss.account_number
			group   by swrq.dt
		) 	as HLReturners

Message now()||' | Calculating @HLAvgReturners DONE' to client

Message now()||' | Looping' to client

while @var_count <= (select max(id) from panbal_variables)
begin

    select  @thevariable = aggregation_variable from panbal_variables where id = @var_count

    Message now()||' | Looping for '|| @thevariable to client
    
    -- Varaible to get the the Balance Index at high level.
    select  @HLBalanceIndex = min(Balance_Index)
    from(
    		select  psl.value
    				,count(pss.account_number) as Sky_Base_Households
    				,count(distinct pbhh.account_number) as Panel_base_Households
    				,sqrt(
    						avg(
    								(
    										(
    												(Panel_base_Households * @Sky_Base
    																				/ Sky_Base_Households
    																										/ @Panel_Base
    												) * 100
    										) - 100
    								) * (
    										(
    												(Panel_base_Households * @Sky_Base
    																				/ Sky_Base_Households
    																										/ @Panel_Base
    												) * 100
    										) - 100
    									)
    						   ) over(partition by Part)
    					  ) as Balance_Index --formula to get high level balance index
    				,'Partition' as Part
    		from    PanBal_segment_snapshots as pss
    				inner join PanBal_segments_lookup_normalised as psl             --*
    				on  pss.segment_id = psl.segment_id
                    and psl.aggregation_variable = @var_count
    				left join #PanBaseHH as pbhh
    				on  pbhh.account_number = pss.account_number
    		group   by  psl.value
    	)HLBalanceIndex
    
    Message now()||' | Calculating STEP 1 DONE' to client

    -- To get low level sub variable metrics
    select  psl.value
            ,count(pss.account_number) as Sky_Base_Households
            ,count(distinct pbhh.account_number) as Panel_base_Households
            ,cast(0 as integer) Returning_Households
            ,(Panel_base_Households * @Sky_Base
                                            / Sky_Base_Households
                                                                    / @Panel_Base
             ) * 100 as Balance_Index
    into    #TempLowLevel
    from    PanBal_segment_snapshots as pss
            inner join PanBal_segments_lookup_normalised as psl             --*
            on	pss.segment_id = psl.segment_id
    		and	psl.aggregation_variable = @var_count
            left join #PanBaseHH as pbhh
            on	pbhh.account_number = pss.account_number
    group   by 	psl.value
    
    Message now()||' | Calculating STEP 2 DONE' to client

    -- To get average returning households at low level sub variables
    select  value
            ,cast(avg(AvgRet) as integer)	as AvgReturners
    into    #TempAvgRetLL
    from    (
    			select  psl.value
    					,count(swrq.account_number) / 7 AvgRet
    			from    PanBal_segment_snapshots 						as pss
    					inner join PanBal_segments_lookup_normalised	as psl
    					on	pss.segment_id = psl.segment_id
    					and	psl.aggregation_variable = @var_count
    					left join #SWRQ 								as swrq
    					on	swrq.account_number = pss.account_number
    			group   by	psl.value
    						,swrq.dt
                having  AvgRet > 0
    		) 	as LLAvgReturners
    group	by	value
    
    Message now()||' | Calculating STEP 3 DONE' to client

    --To update low level average Returning households.
    update  #TempLowLevel as tll
    set     tll.Returning_Households = tarll.AvgReturners
    from    #TempAvgRetLL as tarll
    where   tll.value = tarll.value
    and     tarll.AvgReturners is not null
    
    Message now()||' | Calculating STEP 4 DONE' to client

    -- To get variables with Sky base, Panel base at high level unioned with low level split per sub variable with Balance indices
    
    insert into xdash_overview_variables2 (  variable_value
                                            ,sky_base_households
                                            ,panel_base_households
                                            ,avg_returning_households
                                            ,balance_index
                                          )
    select  @var_count||' - '||@thevariable as variable_value
            ,count(pss.account_number) as Sky_Base_Households
            ,count(distinct pbhh.account_number) as Panel_base_Households
            ,@HLAvgReturners as Returning_Households
            ,@HLBalanceIndex as Balance_Index
    from    PanBal_segment_snapshots as pss
            left join #PanBaseHH as pbhh
            on pbhh.account_number = pss.account_number
    UNION
    select	*
    from 	#TempLowLevel
    where   trim(value) <> ''
    commit

    Message now()||' | Calculating STEP 5 DONE' to client    

    drop table #TempLowLevel
    drop table #TempAvgRetLL
    commit
    
    set @var_count = @var_count + 1

    Message now()||' | Looping through '|| @thevariable ||' DONE' to client    

end


