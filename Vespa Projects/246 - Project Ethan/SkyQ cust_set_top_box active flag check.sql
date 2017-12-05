select 
		case 
			when (has_Q_silver = 1 and has_Q = 0 and has_Q_mini = 0) then	'Q silver only' 
			when (has_Q_silver = 0 and has_Q = 1 and has_Q_mini = 0) then	'Q standard only' 
			when (has_Q_silver = 0 and has_Q = 0 and has_Q_mini = 1) then	'Q mini only (not expected)' 
			when (has_Q_silver = 1 and has_Q = 1 and has_Q_mini = 0) then	'Q silver plus standard (not expected)' 
			when (has_Q_silver = 1 and has_Q = 0 and has_Q_mini = 1) then	'Q silver plus mini' 
			when (has_Q_silver = 0 and has_Q = 1 and has_Q_mini = 1) then	'Q standard plus mini (not expected)' 
			when (has_Q_silver = 1 and has_Q = 1 and has_Q_mini = 1) then	'Q silver plus Q standard plus mini (not expected)' 
			when (has_Q_silver = 0 and has_Q = 0 and has_Q_mini = 0) then	'No Q STBs (not expected)' 
			else															NULL 
		end stb_combinations 
	,	count() accounts 
from	( 
			select 
					clc.account_number 
				,	max ( 
							case left(stb.x_decoder_nds_number_prefix_4,3) 
								when '32B' then	1 
								else			0 
							end 
						)	has_Q_silver 
				,	max ( 
							case left(stb.x_decoder_nds_number_prefix_4,3) 
								when '32C' then 1 
								else			0 
							end 
							) has_Q 
				,	max ( 
							case left(stb.x_decoder_nds_number_prefix_4,3) 
								when '32D' then 1 
								else			0 
							end 
						)	has_Q_mini 
			from 
							/*sk_prodreg.*/cust_latest_contacts clc 
				inner join	/*sk_prodreg.*/cust_set_top_box stb 	on 	clc.account_number = stb.account_number 
																and left(stb.x_decoder_nds_number_prefix_4,2) = '32' 
																and stb.x_active_box_flag_new = 'Y' 
			where clc.ethan_customer_flag = 1 
			group by clc.account_number 
		) t0 
group by stb_combinations 
order by stb_combinations 
; 




/*
select 
		account_number
	,	case 
			when (has_Q_silver = 1 and has_Q = 0 and has_Q_mini = 0) then	'Q silver only' 
			when (has_Q_silver = 0 and has_Q = 1 and has_Q_mini = 0) then	'Q standard only' 
			when (has_Q_silver = 0 and has_Q = 0 and has_Q_mini = 1) then	'Q mini only (not expected)' 
			when (has_Q_silver = 1 and has_Q = 1 and has_Q_mini = 0) then	'Q silver plus standard (not expected)' 
			when (has_Q_silver = 1 and has_Q = 0 and has_Q_mini = 1) then	'Q silver plus mini' 
			when (has_Q_silver = 0 and has_Q = 1 and has_Q_mini = 1) then	'Q standard plus mini (not expected)' 
			when (has_Q_silver = 1 and has_Q = 1 and has_Q_mini = 1) then	'Q silver plus Q standard plus mini (not expected)' 
			when (has_Q_silver = 0 and has_Q = 0 and has_Q_mini = 0) then	'No Q STBs (not expected)' 
			else															NULL 
		end stb_combinations 
from	( 
			select 
					clc.account_number 
				,	max ( 
							case left(stb.x_decoder_nds_number_prefix_4,3) 
								when '32B' then	1 
								else			0 
							end 
						)	has_Q_silver 
				,	max ( 
							case left(stb.x_decoder_nds_number_prefix_4,3) 
								when '32C' then 1 
								else			0 
							end 
							) has_Q 
				,	max ( 
							case left(stb.x_decoder_nds_number_prefix_4,3) 
								when '32D' then 1 
								else			0 
							end 
						)	has_Q_mini 
			from 
							sk_prodreg.cust_latest_contacts clc 
				inner join	sk_prodreg.cust_set_top_box stb 	on 	clc.account_number = stb.account_number 
																and left(stb.x_decoder_nds_number_prefix_4,2) = '32' 
																and stb.x_active_box_flag_new = 'Y' 
			where clc.ethan_customer_flag = 1 
			group by clc.account_number 
		) t0 
where	stb_combinations	like	'%(not expected)'
order by account_number
; 

*/
