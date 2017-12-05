/*
#                                            '#                                 
#                                            '#                                 
#                                            '#                                 
#                                            '#                                 
#                                            '#                                 
#                                            '#                                 
#              '##                           '#                                 
#              ###                           '#                                 
#             .###                           '#                                 
#             .###                           '#                                 
#     .:::.   .###       ::         ..       '#       .                   ,:,   
#   ######### .###     #####       ###.      '#      '##  ########`     ########
#  ########## .###    ######+     ####       '#      '##  #########'   ########'
# ;#########  .###   +#######     ###;       '#      '##  ###    ###.  ##       
# ####        .###  '#### ####   '###        '#      '##  ###     ###  ##       
# '####+.     .### ;####  +###:  ###+        '#      '##  ###      ##  ###`     
#  ########+  .###,####    #### .###         '#      '##  ###      ##. ;#####,  
#  `######### .###`####    `########         '#      '##  ###      ##.  `######`
#     :######`.### +###.    #######          '#      '##  ###      ##      .####
#         ###'.###  ####     ######          '#      '##  ###     ;##         ##
#  `'':..+###:.###  .####    ,####`          '#      '##  ###    `##+         ##
#  ########## .###   ####.    ####           '#      '##  ###   +###   ;,    +##
#  #########, .###    ####    ###:           '#      '##  #########    ########+
#  #######;   .##:     ###+  '###            '#      '##  '######      ;######, 
#                            ###'            '#                                 
#                           ;###             '#                                 
#                           ####             '#                                 
#                          :###              '#                                 
#                                            '#                                 
#                                            '#                                 
#                                            '#                                 
#                                            '#                                 
#                                            '#                                 
#                                            '#                                 
# ------------------------------------------------------------------------------
# ------------------------------------------------------------------------------
# 246 - Sky Q platform cumulative daily uptake.sql
# 2017-01-06
# 
# Environment:
# SQL script to be run on Olive
# 
# 
# Function:
# Calculate uptake of Sky Q platform from the beginning of 2016
# 
# Dependencies:
# None
# 
# ------------------------------------------------------------------------------
# 
*/


-- Get all Sky Q STBs and their installation/replacement dates
drop table #tmp;
select
        account_number
    ,   decoder_nds_number
    ,   box_installed_dt
    ,   box_replaced_dt
into    #tmp
from  cust_set_top_box
where
        decoder_nds_number  like    '32%'
    and account_type        =       'Standard'
    and account_sub_type    =       'Normal'
;

create hg index idx1 on #tmp(account_number);
create hg index idx2 on #tmp(decoder_nds_number);
create date index idx3 on #tmp(box_installed_dt);
create date index idx4 on #tmp(box_replaced_dt);


-- Create date vector
drop table #tmp1;
select  utc_day_date
into    #tmp1
from    sk_prod.VIQ_DATE
where   utc_day_date    between '2016-01-01'
                        and     today()
group by    utc_day_date
;
create date index idx1 on #tmp1(utc_day_date);


-- Cumulative unique STBs and accounts by date
select
		a.utc_day_date
	,	count(distinct b.account_number)		accounts
	,	count(distinct b.decoder_nds_number)	STBs
from
				#tmp1	a
	left join	#tmp	b	on	a.utc_day_date	between	b.box_installed_dt
												and		b.box_replaced_dt
group by	a.utc_day_date
order by	a.utc_day_date
;


-- Cumulative unique STBs by model and date
select
		a.utc_day_date
	,	case	left(b.decoder_nds_number,3)
			when	'32B'	then	'Sky Q Silver'
			when	'32C'	then	'Sky Q'
			when	'32D'	then	'Sky Q Mini'
		end		STB_type
	,	count(distinct b.decoder_nds_number)	STBs
from
				#tmp1	a
	left join	#tmp	b	on	a.utc_day_date	between	b.box_installed_dt
												and		b.box_replaced_dt
group by
		a.utc_day_date
	,	STB_type
order by
		a.utc_day_date
	,	STB_type
;



-- Cumulative unique STBs and accounts by date (single-query version for Tableau custom-SQL data source)
select
		a.utc_day_date
	,	count(distinct b.account_number)		accounts
	,	count(distinct b.decoder_nds_number)	STBs
from
				(	-- Create date vector
					select  utc_day_date
					from    sk_prod.VIQ_DATE
					where   utc_day_date    between '2016-01-01'
					                        and     today()
					group by    utc_day_date
				)	a
	left join	(
					select
					        account_number
					    ,   decoder_nds_number
					    ,   box_installed_dt
					    ,   box_replaced_dt
					into    #tmp
					from  cust_set_top_box
					where
					        decoder_nds_number  like    '32%'
					    and account_type        =       'Standard'
					    and account_sub_type    =       'Normal'
				)	b	on	a.utc_day_date	between	b.box_installed_dt
												and	b.box_replaced_dt
group by	a.utc_day_date
order by	a.utc_day_date
;