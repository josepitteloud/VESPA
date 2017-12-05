
-----------------
-- List of tables
-----------------

-- rules 		(table for SVOD)
-- rules_4live 	(table for live)



select  sg_userid
        ,ns_st_ci
		,ns_st_ep
        ,event_start
        ,event_end
        ,starting_action
        ,ending_action
from    (
			-- Once identified when events start and end... applying lead/lag to the event type. Compacting at a single row per event...
            select  sg_userid
                    ,ns_st_ci
					,ns_st_ep
                    ,dt         as event_start
                    ,dt_after   as event_end
                    ,ns_st_ev   as starting_action
                    ,min(ns_st_ev) over (
                                            partition by    sg_userid
                                                            ,ns_st_ci
                                            order by        event_start
                                            rows between    1 following and 1 following
                                        )   ending_action
                    ,start_flag
            from    (
						-- Applying rules to the Dataset...
                        select  sg_userid
                                ,ns_st_ci
								,ns_st_ep
                                ,min(thedate) over  (
                                                        partition by    sg_userid
                                                                        ,ns_st_ci
                                                        order by        thedate
                                                        rows between    1 preceding and 1 preceding
                                                    )   as dt_before
                                ,thedate    as dt
                                ,min(thedate) over  (
                                                        partition by    sg_userid
                                                                        ,ns_st_ci
                                                        order by        thedate
                                                        rows between    1 following and 1 following
                                                    )   as dt_after
                                ,min(trim(ns_st_ev)) over   (
                                                                partition by    sg_userid
                                                                                ,ns_st_ci
                                                                order by        thedate
                                                                rows between    1 preceding and 1 preceding
                                                            )   as prev_ev
                                ,ns_st_ev
                                ,min(trim(ns_st_ev)) over   (
                                                                partition by    sg_userid
                                                                                ,ns_st_ci
                                                                order by        thedate
                                                                rows between    1 following and 1 following
                                                            )   as next_ev
															
								-- RULES TO FLAG WHERE AN EVENT STARTS...
                                ,case   when trim(ns_st_ev) = 'play' and dt_before is null                          then 1 
                                        when trim(prev_ev) = 'end' and trim(ns_st_ev) = 'play' then 1
                                        when trim(ns_st_ev) = 'play' and datediff(ss,dt_before,dt) > 6              then 1
                                        else 0
                                end     as start_flag
								
								-- RULES TO FLAG WHERE AN EVENT ENDS...
                                ,case   when trim(ns_st_ev) = 'end'     then 1
                                        when trim(ns_st_ev) = 'pause' and trim(next_ev) = 'play' and datediff(ss,dt,dt_after) > 6 then 1
                                        else 0
                                end     as end_flag
								
                        from    rules -- [SWITCH]
                    )   as base
            where   (start_flag = 1 or end_flag = 1)
            and     next_ev is not null
        )   as final_stage
where   start_flag = 1