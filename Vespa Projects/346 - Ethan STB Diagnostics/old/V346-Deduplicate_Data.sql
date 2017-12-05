

select
min(reference_date) as reference_date
,id
,tstamp
,parameter_name
,parameter_value
,count() as nrOfDups
into #et_dedup
from
et_technical
group by id,tstamp,parameter_name,parameter_value
having nrOfDups>1
;

/*
-- if we are interested in knowing how many duplicated records there ar eper day/model

select reference_date
, substring(id,1,3) as model
,count() as nrPerModel
from #et_dedup
group by reference_date, model
*/

delete from et_technical
from
et_technical et
inner join
#et_dedup dup
on
et.id=dup.id
and
et.tstamp=dup.tstamp
and
et.parameter_name=dup.parameter_name
and
et.parameter_value=dup.parameter_value
;

insert into
et_technical(
reference_date
,id
,tstamp
,parameter_name
,parameter_value)
select
reference_date
,id
,tstamp
,parameter_name
,parameter_value
from
#et_dedup
;

