/*-----------------------------------------------------------------------------------------------------------------
        Project:V250 - Sports right Analysis Profiling
        Part - Part 09 Create Deduped Viewing Dataset
        
        Analyst: Dan Barnett
        SK Prod: 5

        Although viewing data comes from daily/monthly viewing tables at 1 record per programme, same programme can be viewed
        over multiple days so this part of code is to create a total at 1 record per programme per account
*/------------------------------------------------------------------------------------------------------------------


----Create Deduped version of Viewing Data (Has some dupes where same prog watched multiple days)

select account_number
,dk_programme_instance_dim
,sum(viewing_duration) as viewing_duration_total
,sum(viewing_events) as viewing_events_total
into dbarnett.v250_all_sports_programmes_viewed_deduped
from dbarnett.v250_all_sports_programmes_viewed
group by account_number
,dk_programme_instance_dim
;
commit;

alter table dbarnett.v250_all_sports_programmes_viewed_deduped add service_key bigint;
alter table dbarnett.v250_all_sports_programmes_viewed_deduped add sub_genre_description varchar(40);
alter table dbarnett.v250_all_sports_programmes_viewed_deduped add broadcast_date date;
--select top 100 * from sk_prod.Vespa_programme_schedule
update dbarnett.v250_all_sports_programmes_viewed_deduped
set service_key=b.service_key
,sub_genre_description=b.sub_genre_description
,broadcast_date=cast(b.broadcast_start_date_time_local as date)
from dbarnett.v250_all_sports_programmes_viewed_deduped as a
left outer join  sk_prod.Vespa_programme_schedule as b
on a.dk_programme_instance_dim=b.dk_programme_instance_dim
;
commit;



--select max(len( sub_genre_description))  from  sk_prod.Vespa_programme_schedule
--Add Channel Name--
--alter table dbarnett.v250_all_sports_programmes_viewed_deduped add channel_name varchar(40);

update dbarnett.v250_all_sports_programmes_viewed_deduped
set channel_name=b.channel_name
from dbarnett.v250_all_sports_programmes_viewed_deduped as a
left outer join  v250_channel_to_service_key_lookup_deduped as b
on a.service_key=b.service_key
;
commit;
--select * from dbarnett.v250_all_sports_programmes_viewed_deduped where channel_name is null
---Group Channels Together e.g., Sky Sports---

update dbarnett.v250_all_sports_programmes_viewed_deduped
set channel_name= case when channel_name in ('BBC 1','BBC 2','BBC HD','BBC Three') then 'BBC'
 when channel_name in ('BT Sport 1','BT Sport 2') then 'BT Sport'
when channel_name in ('Eurosport','Eurosport UK','Eurosport2','Eurosport2 UK') then 'Eurosport'
 when channel_name in ('Challenge','Channel 4','Channel 5','ESPN','ITV1','ITV4') then channel_name
 when channel_name in ('ESPN Classic','ESPN America') then 'ESPN'
when channel_name in ('Sky Sports 1','Sky Sports 2','Sky Sports 3','Sky Sports 4','Sky Sports F1') then 'Sky Sports'
when channel_name in ('Sky1','Sky2') then 'Sky 1 and Sky 2'

 else 'Other' end
from dbarnett.v250_all_sports_programmes_viewed_deduped 
commit;



