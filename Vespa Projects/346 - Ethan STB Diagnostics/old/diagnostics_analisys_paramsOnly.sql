
/* parameters list with occurrences(nr of times they have been reported) and number of different values */
/* put result in a table */
/*
IF OBJECT_ID('paramsListAndOccurrences') IS NOT NULL
DROP table paramsListAndOccurrences
;

select parameter_name
,count() as nrOfOccurrences
,count(distinct parameter_value) as distinctValues
,min(cast(parameter_value as int)) as minVal
,max(cast(parameter_value as int)) as maxVal
,median(cast(parameter_value as int)) as medianVal
,avg(cast(parameter_value as int)) as avgVal
,stddev(cast(parameter_value as int)) as stdDevVal
,cast(NULL as varchar(5000)) as listOfStringValues
,row_number() over (order by nrOfOccurrences desc) as row_id
into paramsListAndOccurrences
from
et_technical
group by parameter_name
order by parameter_name
;
*/
create or replace variable @max_cnt int;
create or replace variable @cnt int;
create or replace variable @max_inner_cnt int;
create or replace variable @inner_cnt int;
create or replace variable @param_name varchar(500);
create or replace variable @param_listString varchar(10000);
create or replace variable @max_nr_of_string_parameters int; -- maximum number of items in the comma separated list of parameters

set @max_nr_of_string_parameters=100000000; -- no more than 50 items in the list of parameters

IF OBJECT_ID('param_names_tab') IS NOT NULL
DROP table param_names_tab
IF OBJECT_ID('param_names_tab_with_rownumber') IS NOT NULL
DROP table param_names_tab_with_rownumber

create table param_names_tab(
param_name varchar(1000)
);

insert into param_names_tab
select distinct parameter_name
from paramsListAndOccurrences
where minVal is NULL
;

select param_name
,row_number() over(order by param_name) as row_id
into param_names_tab_with_rownumber
from param_names_tab
;
-- select top 346 * from param_names_tab _with_rownumber
set @max_cnt=(select max(row_id) from param_names_tab_with_rownumber);
set @cnt=1;

while @cnt<=@max_cnt
begin
set @param_name=(select param_name from param_names_tab_with_rownumber where row_id=@cnt)
commit


IF OBJECT_ID('param_values_tab') IS NOT NULL
DROP table param_values_tab
IF OBJECT_ID('param_values_tab_with_rownumber') IS NOT NULL
DROP table param_values_tab_with_rownumber

create table param_values_tab(
param_val varchar(1000)
)
commit

insert into param_values_tab
select distinct parameter_value
from et_technical
where parameter_name=@param_name
commit
-- select * from param_values_tab
select param_val
,row_number() over(order by param_val) as row_id
into param_values_tab_with_rownumber
from param_values_tab
commit
-- select * from param_values_tab_with_rownumber
-- select @param_name
set @inner_cnt=1
commit
set @param_listString=''
commit
set @max_inner_cnt=(select max(row_id) from param_values_tab_with_rownumber)
commit
-- select @max_inner_cnt
while (@inner_cnt<=@max_inner_cnt) and (@inner_cnt<=@max_nr_of_string_parameters)
begin -- take the list from the data
if @param_listString!=''
set @param_listString = @param_listString || ',' || (select param_val FROM param_values_tab_with_rownumber where row_id=@inner_cnt)
else
set @param_listString = (select param_val FROM param_values_tab_with_rownumber where row_id=@inner_cnt)

commit
set @inner_cnt=@inner_cnt+1
commit
end
commit

update paramsListAndOccurrences
set listOfStringValues=@param_listString
where
parameter_name=@param_name
commit
--select @param_listString
set @cnt=@cnt+1
commit

end

/*

select @cnt

select @param_name

select GROUP_CONCAT(distinct parameter_name ORDER BY parameter_name ASC )
FROM paramsListAndOccurrences
where
minVal is null
and
distinctValues=2


SELECT 1 AS a, GROUP_CONCAT(distinct parameter_name ORDER BY parameter_name ASC SEPARATOR ', ') AS names
FROM et_technical
where
minVal is null
and
distinctValues=2
GROUP BY a


SELECT Stuff(
  (SELECT N', ' + Name FROM Names FOR XML PATH(''),TYPE)
  .value('text()[1]','nvarchar(max)'),1,2,N'')


-- some shit

select count(), max(distinctValues) as nrOfVals
from
paramsListAndOccurrences
where
minVal is null
and
distinctValues=
;

select count(), max(distinctValues) as nrOfCazPArams
from
paramsListAndOccurrences
where
minVal is null
and
distinctValues>50
;

select distinct parameter_name
from
paramsListAndOccurrences
where
minVal is null
and
distinctValues>50
;

select top 100 *
from
paramsListAndOccurrences


create or replace variable @stella varchar(100);

set @stella=(select 'a' + ',','c','d')

select top 10 parameter_name+','
from
paramsListAndOccurrences
*/
