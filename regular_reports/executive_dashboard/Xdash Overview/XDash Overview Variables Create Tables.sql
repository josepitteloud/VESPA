
-- creating a variable table to store metrics and calculations...

if object_id('xdash_overview_variables') is not null
	drop table xdash_overview_variables
	
commit
	
create table xdash_overview_variables(
	aggregation_variable		varchar(50)
	,categories                 varchar(50)
	,sky_base_households        integer
	,panel_base_households      integer
	,avg_returning_households   integer
	,balance_index              decimal(16,6)
)

commit

commit
create lf index idx_variablevalue on xdash_overview_variables(categories)
grant select on xdash_overview_variables to vespa_group_low_security
commit


-----------------------------------------------------------------[Patch meanwhile panbal project not deployed in VA]

if object_id('category_lookup') is not null
	drop table category_lookup
	
commit

create table category_lookup(
	aggregation_index	tinyint
	,category_techname	varchar(5)
	,friendlyname		varchar(30)
)

commit
create lf index lf1 on category_lookup(aggregation_index)
create lf index lf2	on category_lookup(category_techname)
grant select on categoriy_lookup to vespa_group_low_security
commit

if object_id('panbal_segments_lookup_normalised') is not null 
	drop table panbal_segments_lookup_normalised
	
commit

create table panbal_segments_lookup_normalised(
	segment_id          	bigint
	,aggregation_variable   tinyint
	,value                  varchar(40)
	,curr                   bit default 0
)

commit
create hg index hg1 on panbal_segments_lookup_normalised(segment_id)
create hg index hg2 on panbal_segments_lookup_normalised(aggregation_variable)
create hg index hg3 on panbal_segments_lookup_normalised(value)
grant select on panbal_segments_lookup_normalised to vespa_group_low_security
commit


if object_id('PanBal_segment_snapshots') is not null 
	drop table PanBal_segment_snapshots
	
commit

create table PanBal_segment_snapshots(
	   account_number varchar(30)
	  ,segment_id     int
)

commit
grant select on PanBal_segment_snapshots to vespa_group_low_security
create unique hg index uhacc on PanBal_segment_snapshots(account_number)
commit

if object_id('panbal_variables') is not null 
	drop table panbal_variables
	
commit
            
create table panbal_variables(
	   id					int
	  ,aggregation_variable	varchar(30)
)

commit
grant select on panbal_variables to vespa_group_low_security
create lf index lfid1 on panbal_variables(id)
commit