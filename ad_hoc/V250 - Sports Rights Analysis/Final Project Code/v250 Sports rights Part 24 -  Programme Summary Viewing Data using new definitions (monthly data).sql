
commit;
---Get Summary by Account---
--drop table dbarnett.v250_prog_watched_from_daily_data;
--drop table dbarnett.v250_programme_summary_201301;
select account_number
,service_key
,dk_programme_instance_dim
,programme_instance_duration
,sum(case when a.capped_partial_flag = 1 then datediff(second, a.instance_start_date_time_utc, a.capping_end_date_time_utc)
     else datediff(second, a.instance_start_date_time_utc, a.instance_end_date_time_utc)
     end)  as viewing_duration
into dbarnett.v250_programme_summary_201301
from  sk_prod.vespa_dp_prog_viewed_201301 as a
where capped_full_flag = 0 -- only those instances that have not been fully capped
                           and instance_start_date_time_utc < instance_end_date_time_utc              -- Remove 0sec instances
                           and (reported_playback_speed is null or reported_playback_speed = 2) -- Live or Recorded Records
                           and account_number is not null --remove instances we do not know the account for
                           and subscriber_id is not null --remove instances we do not know the subscriber_id for
                           and (type_of_viewing_event in ('HD Viewing Event', 'Sky+ time-shifted viewing event', 'TV Channel Viewing')-- limit to keep out 
                            --interactive viewing and other service viewing event i.e. where it could not identify viewing event type it was
                            or (type_of_viewing_event = 'Other Service Viewing Event' 
                            and service_type_description in ('NVOD service','High Definition TV test service','Digital TV channel')))
                           and capping_end_date_time_utc is not null and duration>=180

group by account_number
,service_key
,dk_programme_instance_dim
,programme_instance_duration
;
commit;
CREATE HG INDEX idx1 ON dbarnett.v250_programme_summary_201301(account_number);
commit;
CREATE HG INDEX idx2 ON dbarnett.v250_programme_summary_201301(service_key);
--select top 100 * from dbarnett.v250_programme_summary_201301;
--drop table dbarnett.v250_prog_watched_from_201301_data;
select account_number 

,sum(case when pay=1 and viewing_duration>=180 
then 1 else 0 end) as fta_programmes_03min_plus
,sum(case when pay=1 and viewing_duration>=600 
then 1 else 0 end) as fta_programmes_10min_plus
,sum(case when pay=1 and viewing_duration/cast(programme_instance_duration as real)>=0.6 then 1
when pay=1 and viewing_duration>=3600 and programme_instance_duration >=5400 then 1 else 0 end)
as fta_programmes_60pc_or_1hr
 

,sum(case when pay=1 and sky_channel=1 and grouped_channel not in ('Sky Movies Channels','Sky Sports Channels','Sky Box Office') and viewing_duration>=180 
then 1 else 0 end) as sky_pay_basic_programmes_03min_plus
,sum(case when pay=1 and sky_channel=1 and grouped_channel not in ('Sky Movies Channels','Sky Sports Channels','Sky Box Office') and viewing_duration>=600 
then 1 else 0 end) as sky_pay_basic_programmes_10min_plus
,sum(case when pay=1 and sky_channel=1 and grouped_channel not in ('Sky Movies Channels','Sky Sports Channels','Sky Box Office') and viewing_duration/cast(programme_instance_duration as real)>=0.6 then 1
when pay=1 and sky_channel=1 and grouped_channel not in ('Sky Movies Channels','Sky Sports Channels','Sky Box Office') and viewing_duration>=3600 and programme_instance_duration >=5400 then 1 else 0 end)
as sky_pay_basic_programmes_60pc_or_1hr    
         


,sum(case when pay=1 and sky_channel=0 and grouped_channel not in ('Sky Movies Channels','Sky Sports Channels') and viewing_duration>=180 
 and b.channel_name not in ('BT Sport 1','BT Sport 2','ESPN','ESPN America','ESPN Classic','Eurosport','Eurosport UK','Eurosport2','Eurosport2 UK') then 1 else 0 end) as third_party_pay_basic_programmes_03min_plus
,sum(case when pay=1 and sky_channel=0 and grouped_channel not in ('Sky Movies Channels','Sky Sports Channels') and viewing_duration>=600 
 and b.channel_name not in ('BT Sport 1','BT Sport 2','ESPN','ESPN America','ESPN Classic','Eurosport','Eurosport UK','Eurosport2','Eurosport2 UK') then 1 else 0 end) as third_party_pay_basic_programmes_10min_plus
,sum(case when pay=1 and sky_channel=0 and grouped_channel not in ('Sky Movies Channels','Sky Sports Channels') and viewing_duration/cast(programme_instance_duration as real)>=0.6  and b.channel_name not in ('BT Sport 1','BT Sport 2','ESPN','ESPN America','ESPN Classic','Eurosport','Eurosport UK','Eurosport2','Eurosport2 UK') then 1
when pay=1 and sky_channel=0 and grouped_channel not in ('Sky Movies Channels','Sky Sports Channels') and viewing_duration>=3600
 and programme_instance_duration >=5400  
and b.channel_name not in ('BT Sport 1','BT Sport 2','ESPN','ESPN America','ESPN Classic','Eurosport','Eurosport UK','Eurosport2','Eurosport2 UK')
 then 1 else 0 end)
as third_party_pay_basic_programmes_60pc_or_1hr    
         

,sum(case when pay=1 and grouped_channel  in ('Sky Movies Channels') and viewing_duration>=180 
then 1 else 0 end) as sky_movies_programmes_03min_plus
,sum(case when pay=1 and grouped_channel  in ('Sky Movies Channels') and viewing_duration>=600 
then 1 else 0 end) as sky_movies_programmes_10min_plus
,sum(case when pay=1 and grouped_channel  in ('Sky Movies Channels') and viewing_duration/cast(programme_instance_duration as real)>=0.6 then 1
when pay=1  and grouped_channel  in ('Sky Movies Channels') and viewing_duration>=3600 and programme_instance_duration >=5400 then 1 else 0 end)
as sky_movies_programmes_60pc_or_1hr  



,sum(case when pay=1 
    and (b.channel_name in ('BT Sport 1','BT Sport 2','ESPN','ESPN America','ESPN Classic','Eurosport','Eurosport UK','Eurosport2','Eurosport2 UK') 
       or   grouped_channel = 'Sky Box Office')

and viewing_duration>=180 
then 1 else 0 end) as other_programmes_03min_plus
,sum(case when pay=1  and viewing_duration>=600 
 and (b.channel_name in ('BT Sport 1','BT Sport 2','ESPN','ESPN America','ESPN Classic','Eurosport','Eurosport UK','Eurosport2','Eurosport2 UK') 
       or   grouped_channel = 'Sky Box Office') then 1 else 0 end) as other_programmes_10min_plus

,sum(case when pay=1  and viewing_duration/cast(programme_instance_duration as real)>=0.6
 and (b.channel_name in ('BT Sport 1','BT Sport 2','ESPN','ESPN America','ESPN Classic','Eurosport','Eurosport UK','Eurosport2','Eurosport2 UK') 
       or   grouped_channel = 'Sky Box Office')
then 1
when pay=1 and viewing_duration>=3600 and programme_instance_duration >=5400 
and (b.channel_name in ('BT Sport 1','BT Sport 2','ESPN','ESPN America','ESPN Classic','Eurosport','Eurosport UK','Eurosport2','Eurosport2 UK') 
       or   grouped_channel = 'Sky Box Office') then 1 else 0 end)
as other_programmes_60pc_or_1hr  


into dbarnett.v250_prog_watched_from_201301_data
from  dbarnett.v250_programme_summary_201301 as a
--from dbarnett.v250_viewing_by_account_and_channel as a  Changed to use new data created in Part 22b
left outer join v250_channel_to_service_key_lookup_deduped as b
on a.service_key=b.service_key
--left outer join sk_prod.Vespa_programme_schedule as c
--ON a.dk_programme_instance_dim = c.pk_programme_instance_dim
group by account_number
;
commit;
--select top 100 * from dbarnett.v250_prog_watched_from_201301_data;
drop table dbarnett.v250_programme_summary_201301;
commit;


----repeat for Feb---
--drop table dbarnett.v250_programme_summary_201302;
select account_number
,service_key
,dk_programme_instance_dim
,programme_instance_duration
,sum(case when a.capped_partial_flag = 1 then datediff(second, a.instance_start_date_time_utc, a.capping_end_date_time_utc)
     else datediff(second, a.instance_start_date_time_utc, a.instance_end_date_time_utc)
     end)  as viewing_duration
into dbarnett.v250_programme_summary_201302
from  sk_prod.vespa_dp_prog_viewed_201302 as a
where capped_full_flag = 0 -- only those instances that have not been fully capped
                           and instance_start_date_time_utc < instance_end_date_time_utc              -- Remove 0sec instances
                           and (reported_playback_speed is null or reported_playback_speed = 2) -- Live or Recorded Records
                           and account_number is not null --remove instances we do not know the account for
                           and subscriber_id is not null --remove instances we do not know the subscriber_id for
                           and (type_of_viewing_event in ('HD Viewing Event', 'Sky+ time-shifted viewing event', 'TV Channel Viewing')-- limit to keep out 
                            --interactive viewing and other service viewing event i.e. where it could not identify viewing event type it was
                            or (type_of_viewing_event = 'Other Service Viewing Event' 
                            and service_type_description in ('NVOD service','High Definition TV test service','Digital TV channel')))
                           and capping_end_date_time_utc is not null and duration>=180

group by account_number
,service_key
,dk_programme_instance_dim
,programme_instance_duration
;
commit;
CREATE HG INDEX idx1 ON dbarnett.v250_programme_summary_201302(account_number);
commit;
CREATE HG INDEX idx2 ON dbarnett.v250_programme_summary_201302(service_key);
--select top 100 * from dbarnett.v250_programme_summary_201302;
--drop table dbarnett.v250_prog_watched_from_201302_data;
select account_number 

,sum(case when pay=1 and viewing_duration>=180 
then 1 else 0 end) as fta_programmes_03min_plus
,sum(case when pay=1 and viewing_duration>=600 
then 1 else 0 end) as fta_programmes_10min_plus
,sum(case when pay=1 and viewing_duration/cast(programme_instance_duration as real)>=0.6 then 1
when pay=1 and viewing_duration>=3600 and programme_instance_duration >=5400 then 1 else 0 end)
as fta_programmes_60pc_or_1hr
 

,sum(case when pay=1 and sky_channel=1 and grouped_channel not in ('Sky Movies Channels','Sky Sports Channels','Sky Box Office') and viewing_duration>=180 
then 1 else 0 end) as sky_pay_basic_programmes_03min_plus
,sum(case when pay=1 and sky_channel=1 and grouped_channel not in ('Sky Movies Channels','Sky Sports Channels','Sky Box Office') and viewing_duration>=600 
then 1 else 0 end) as sky_pay_basic_programmes_10min_plus
,sum(case when pay=1 and sky_channel=1 and grouped_channel not in ('Sky Movies Channels','Sky Sports Channels','Sky Box Office') and viewing_duration/cast(programme_instance_duration as real)>=0.6 then 1
when pay=1 and sky_channel=1 and grouped_channel not in ('Sky Movies Channels','Sky Sports Channels','Sky Box Office') and viewing_duration>=3600 and programme_instance_duration >=5400 then 1 else 0 end)
as sky_pay_basic_programmes_60pc_or_1hr    
         


,sum(case when pay=1 and sky_channel=0 and grouped_channel not in ('Sky Movies Channels','Sky Sports Channels') and viewing_duration>=180 
 and b.channel_name not in ('BT Sport 1','BT Sport 2','ESPN','ESPN America','ESPN Classic','Eurosport','Eurosport UK','Eurosport2','Eurosport2 UK') then 1 else 0 end) as third_party_pay_basic_programmes_03min_plus
,sum(case when pay=1 and sky_channel=0 and grouped_channel not in ('Sky Movies Channels','Sky Sports Channels') and viewing_duration>=600 
 and b.channel_name not in ('BT Sport 1','BT Sport 2','ESPN','ESPN America','ESPN Classic','Eurosport','Eurosport UK','Eurosport2','Eurosport2 UK') then 1 else 0 end) as third_party_pay_basic_programmes_10min_plus
,sum(case when pay=1 and sky_channel=0 and grouped_channel not in ('Sky Movies Channels','Sky Sports Channels') and viewing_duration/cast(programme_instance_duration as real)>=0.6  and b.channel_name not in ('BT Sport 1','BT Sport 2','ESPN','ESPN America','ESPN Classic','Eurosport','Eurosport UK','Eurosport2','Eurosport2 UK') then 1
when pay=1 and sky_channel=0 and grouped_channel not in ('Sky Movies Channels','Sky Sports Channels') and viewing_duration>=3600
 and programme_instance_duration >=5400  
and b.channel_name not in ('BT Sport 1','BT Sport 2','ESPN','ESPN America','ESPN Classic','Eurosport','Eurosport UK','Eurosport2','Eurosport2 UK')
 then 1 else 0 end)
as third_party_pay_basic_programmes_60pc_or_1hr    
         

,sum(case when pay=1 and grouped_channel  in ('Sky Movies Channels') and viewing_duration>=180 
then 1 else 0 end) as sky_movies_programmes_03min_plus
,sum(case when pay=1 and grouped_channel  in ('Sky Movies Channels') and viewing_duration>=600 
then 1 else 0 end) as sky_movies_programmes_10min_plus
,sum(case when pay=1 and grouped_channel  in ('Sky Movies Channels') and viewing_duration/cast(programme_instance_duration as real)>=0.6 then 1
when pay=1  and grouped_channel  in ('Sky Movies Channels') and viewing_duration>=3600 and programme_instance_duration >=5400 then 1 else 0 end)
as sky_movies_programmes_60pc_or_1hr  



,sum(case when pay=1 
    and (b.channel_name in ('BT Sport 1','BT Sport 2','ESPN','ESPN America','ESPN Classic','Eurosport','Eurosport UK','Eurosport2','Eurosport2 UK') 
       or   grouped_channel = 'Sky Box Office')

and viewing_duration>=180 
then 1 else 0 end) as other_programmes_03min_plus
,sum(case when pay=1  and viewing_duration>=600 
 and (b.channel_name in ('BT Sport 1','BT Sport 2','ESPN','ESPN America','ESPN Classic','Eurosport','Eurosport UK','Eurosport2','Eurosport2 UK') 
       or   grouped_channel = 'Sky Box Office') then 1 else 0 end) as other_programmes_10min_plus

,sum(case when pay=1  and viewing_duration/cast(programme_instance_duration as real)>=0.6
 and (b.channel_name in ('BT Sport 1','BT Sport 2','ESPN','ESPN America','ESPN Classic','Eurosport','Eurosport UK','Eurosport2','Eurosport2 UK') 
       or   grouped_channel = 'Sky Box Office')
then 1
when pay=1 and viewing_duration>=3600 and programme_instance_duration >=5400 
and (b.channel_name in ('BT Sport 1','BT Sport 2','ESPN','ESPN America','ESPN Classic','Eurosport','Eurosport UK','Eurosport2','Eurosport2 UK') 
       or   grouped_channel = 'Sky Box Office') then 1 else 0 end)
as other_programmes_60pc_or_1hr  


into dbarnett.v250_prog_watched_from_201302_data
from  dbarnett.v250_programme_summary_201302 as a
--from dbarnett.v250_viewing_by_account_and_channel as a  Changed to use new data created in Part 22b
left outer join v250_channel_to_service_key_lookup_deduped as b
on a.service_key=b.service_key
--left outer join sk_prod.Vespa_programme_schedule as c
--ON a.dk_programme_instance_dim = c.pk_programme_instance_dim
group by account_number
;
commit;
--select top 100 * from dbarnett.v250_prog_watched_from_201302_data;
drop table dbarnett.v250_programme_summary_201302;
commit;

----repeat for May---
--drop table dbarnett.v250_programme_summary_201305;
select account_number
,service_key
,dk_programme_instance_dim
,programme_instance_duration
,sum(case when a.capped_partial_flag = 1 then datediff(second, a.instance_start_date_time_utc, a.capping_end_date_time_utc)
     else datediff(second, a.instance_start_date_time_utc, a.instance_end_date_time_utc)
     end)  as viewing_duration
into dbarnett.v250_programme_summary_201305
from  sk_prod.vespa_dp_prog_viewed_201305 as a
where capped_full_flag = 0 -- only those instances that have not been fully capped
                           and instance_start_date_time_utc < instance_end_date_time_utc              -- Remove 0sec instances
                           and (reported_playback_speed is null or reported_playback_speed = 2) -- Live or Recorded Records
                           and account_number is not null --remove instances we do not know the account for
                           and subscriber_id is not null --remove instances we do not know the subscriber_id for
                           and (type_of_viewing_event in ('HD Viewing Event', 'Sky+ time-shifted viewing event', 'TV Channel Viewing')-- limit to keep out 
                            --interactive viewing and other service viewing event i.e. where it could not identify viewing event type it was
                            or (type_of_viewing_event = 'Other Service Viewing Event' 
                            and service_type_description in ('NVOD service','High Definition TV test service','Digital TV channel')))
                           and capping_end_date_time_utc is not null and duration>=180

group by account_number
,service_key
,dk_programme_instance_dim
,programme_instance_duration
;
commit;
CREATE HG INDEX idx1 ON dbarnett.v250_programme_summary_201305(account_number);
commit;
CREATE HG INDEX idx2 ON dbarnett.v250_programme_summary_201305(service_key);
--select top 100 * from dbarnett.v250_programme_summary_201305;
--drop table dbarnett.v250_prog_watched_from_201305_data;
select account_number 

,sum(case when pay=1 and viewing_duration>=180 
then 1 else 0 end) as fta_programmes_03min_plus
,sum(case when pay=1 and viewing_duration>=600 
then 1 else 0 end) as fta_programmes_10min_plus
,sum(case when pay=1 and viewing_duration/cast(programme_instance_duration as real)>=0.6 then 1
when pay=1 and viewing_duration>=3600 and programme_instance_duration >=5400 then 1 else 0 end)
as fta_programmes_60pc_or_1hr
 

,sum(case when pay=1 and sky_channel=1 and grouped_channel not in ('Sky Movies Channels','Sky Sports Channels','Sky Box Office') and viewing_duration>=180 
then 1 else 0 end) as sky_pay_basic_programmes_03min_plus
,sum(case when pay=1 and sky_channel=1 and grouped_channel not in ('Sky Movies Channels','Sky Sports Channels','Sky Box Office') and viewing_duration>=600 
then 1 else 0 end) as sky_pay_basic_programmes_10min_plus
,sum(case when pay=1 and sky_channel=1 and grouped_channel not in ('Sky Movies Channels','Sky Sports Channels','Sky Box Office') and viewing_duration/cast(programme_instance_duration as real)>=0.6 then 1
when pay=1 and sky_channel=1 and grouped_channel not in ('Sky Movies Channels','Sky Sports Channels','Sky Box Office') and viewing_duration>=3600 and programme_instance_duration >=5400 then 1 else 0 end)
as sky_pay_basic_programmes_60pc_or_1hr    
         


,sum(case when pay=1 and sky_channel=0 and grouped_channel not in ('Sky Movies Channels','Sky Sports Channels') and viewing_duration>=180 
 and b.channel_name not in ('BT Sport 1','BT Sport 2','ESPN','ESPN America','ESPN Classic','Eurosport','Eurosport UK','Eurosport2','Eurosport2 UK') then 1 else 0 end) as third_party_pay_basic_programmes_03min_plus
,sum(case when pay=1 and sky_channel=0 and grouped_channel not in ('Sky Movies Channels','Sky Sports Channels') and viewing_duration>=600 
 and b.channel_name not in ('BT Sport 1','BT Sport 2','ESPN','ESPN America','ESPN Classic','Eurosport','Eurosport UK','Eurosport2','Eurosport2 UK') then 1 else 0 end) as third_party_pay_basic_programmes_10min_plus
,sum(case when pay=1 and sky_channel=0 and grouped_channel not in ('Sky Movies Channels','Sky Sports Channels') and viewing_duration/cast(programme_instance_duration as real)>=0.6  and b.channel_name not in ('BT Sport 1','BT Sport 2','ESPN','ESPN America','ESPN Classic','Eurosport','Eurosport UK','Eurosport2','Eurosport2 UK') then 1
when pay=1 and sky_channel=0 and grouped_channel not in ('Sky Movies Channels','Sky Sports Channels') and viewing_duration>=3600
 and programme_instance_duration >=5400  
and b.channel_name not in ('BT Sport 1','BT Sport 2','ESPN','ESPN America','ESPN Classic','Eurosport','Eurosport UK','Eurosport2','Eurosport2 UK')
 then 1 else 0 end)
as third_party_pay_basic_programmes_60pc_or_1hr    
         

,sum(case when pay=1 and grouped_channel  in ('Sky Movies Channels') and viewing_duration>=180 
then 1 else 0 end) as sky_movies_programmes_03min_plus
,sum(case when pay=1 and grouped_channel  in ('Sky Movies Channels') and viewing_duration>=600 
then 1 else 0 end) as sky_movies_programmes_10min_plus
,sum(case when pay=1 and grouped_channel  in ('Sky Movies Channels') and viewing_duration/cast(programme_instance_duration as real)>=0.6 then 1
when pay=1  and grouped_channel  in ('Sky Movies Channels') and viewing_duration>=3600 and programme_instance_duration >=5400 then 1 else 0 end)
as sky_movies_programmes_60pc_or_1hr  



,sum(case when pay=1 
    and (b.channel_name in ('BT Sport 1','BT Sport 2','ESPN','ESPN America','ESPN Classic','Eurosport','Eurosport UK','Eurosport2','Eurosport2 UK') 
       or   grouped_channel = 'Sky Box Office')

and viewing_duration>=180 
then 1 else 0 end) as other_programmes_03min_plus
,sum(case when pay=1  and viewing_duration>=600 
 and (b.channel_name in ('BT Sport 1','BT Sport 2','ESPN','ESPN America','ESPN Classic','Eurosport','Eurosport UK','Eurosport2','Eurosport2 UK') 
       or   grouped_channel = 'Sky Box Office') then 1 else 0 end) as other_programmes_10min_plus

,sum(case when pay=1  and viewing_duration/cast(programme_instance_duration as real)>=0.6
 and (b.channel_name in ('BT Sport 1','BT Sport 2','ESPN','ESPN America','ESPN Classic','Eurosport','Eurosport UK','Eurosport2','Eurosport2 UK') 
       or   grouped_channel = 'Sky Box Office')
then 1
when pay=1 and viewing_duration>=3600 and programme_instance_duration >=5400 
and (b.channel_name in ('BT Sport 1','BT Sport 2','ESPN','ESPN America','ESPN Classic','Eurosport','Eurosport UK','Eurosport2','Eurosport2 UK') 
       or   grouped_channel = 'Sky Box Office') then 1 else 0 end)
as other_programmes_60pc_or_1hr  


into dbarnett.v250_prog_watched_from_201305_data
from  dbarnett.v250_programme_summary_201305 as a
--from dbarnett.v250_viewing_by_account_and_channel as a  Changed to use new data created in Part 22b
left outer join v250_channel_to_service_key_lookup_deduped as b
on a.service_key=b.service_key
--left outer join sk_prod.Vespa_programme_schedule as c
--ON a.dk_programme_instance_dim = c.pk_programme_instance_dim
group by account_number
;
commit;
--select top 100 * from dbarnett.v250_prog_watched_from_201305_data;
drop table dbarnett.v250_programme_summary_201305;
commit;





----repeat for July---
--drop table dbarnett.v250_programme_summary_201307;
select account_number
,service_key
,dk_programme_instance_dim
,programme_instance_duration
,sum(case when a.capped_partial_flag = 1 then datediff(second, a.instance_start_date_time_utc, a.capping_end_date_time_utc)
     else datediff(second, a.instance_start_date_time_utc, a.instance_end_date_time_utc)
     end)  as viewing_duration
into dbarnett.v250_programme_summary_201307
from  sk_prod.vespa_dp_prog_viewed_201307 as a
where capped_full_flag = 0 -- only those instances that have not been fully capped
                           and instance_start_date_time_utc < instance_end_date_time_utc              -- Remove 0sec instances
                           and (reported_playback_speed is null or reported_playback_speed = 2) -- Live or Recorded Records
                           and account_number is not null --remove instances we do not know the account for
                           and subscriber_id is not null --remove instances we do not know the subscriber_id for
                           and (type_of_viewing_event in ('HD Viewing Event', 'Sky+ time-shifted viewing event', 'TV Channel Viewing')-- limit to keep out 
                            --interactive viewing and other service viewing event i.e. where it could not identify viewing event type it was
                            or (type_of_viewing_event = 'Other Service Viewing Event' 
                            and service_type_description in ('NVOD service','High Definition TV test service','Digital TV channel')))
                           and capping_end_date_time_utc is not null and duration>=180

group by account_number
,service_key
,dk_programme_instance_dim
,programme_instance_duration
;
commit;
CREATE HG INDEX idx1 ON dbarnett.v250_programme_summary_201307(account_number);
commit;
CREATE HG INDEX idx2 ON dbarnett.v250_programme_summary_201307(service_key);


--drop table dbarnett.v250_prog_watched_from_201307_data;
select account_number 

,sum(case when pay=1 and viewing_duration>=180 
then 1 else 0 end) as fta_programmes_03min_plus
,sum(case when pay=1 and viewing_duration>=600 
then 1 else 0 end) as fta_programmes_10min_plus
,sum(case when pay=1 and viewing_duration/cast(programme_instance_duration as real)>=0.6 then 1
when pay=1 and viewing_duration>=3600 and programme_instance_duration >=5400 then 1 else 0 end)
as fta_programmes_60pc_or_1hr
 

,sum(case when pay=1 and sky_channel=1 and grouped_channel not in ('Sky Movies Channels','Sky Sports Channels','Sky Box Office') and viewing_duration>=180 
then 1 else 0 end) as sky_pay_basic_programmes_03min_plus
,sum(case when pay=1 and sky_channel=1 and grouped_channel not in ('Sky Movies Channels','Sky Sports Channels','Sky Box Office') and viewing_duration>=600 
then 1 else 0 end) as sky_pay_basic_programmes_10min_plus
,sum(case when pay=1 and sky_channel=1 and grouped_channel not in ('Sky Movies Channels','Sky Sports Channels','Sky Box Office') and viewing_duration/cast(programme_instance_duration as real)>=0.6 then 1
when pay=1 and sky_channel=1 and grouped_channel not in ('Sky Movies Channels','Sky Sports Channels','Sky Box Office') and viewing_duration>=3600 and programme_instance_duration >=5400 then 1 else 0 end)
as sky_pay_basic_programmes_60pc_or_1hr    
         


,sum(case when pay=1 and sky_channel=0 and grouped_channel not in ('Sky Movies Channels','Sky Sports Channels') and viewing_duration>=180 
 and b.channel_name not in ('BT Sport 1','BT Sport 2','ESPN','ESPN America','ESPN Classic','Eurosport','Eurosport UK','Eurosport2','Eurosport2 UK') then 1 else 0 end) as third_party_pay_basic_programmes_03min_plus
,sum(case when pay=1 and sky_channel=0 and grouped_channel not in ('Sky Movies Channels','Sky Sports Channels') and viewing_duration>=600 
 and b.channel_name not in ('BT Sport 1','BT Sport 2','ESPN','ESPN America','ESPN Classic','Eurosport','Eurosport UK','Eurosport2','Eurosport2 UK') then 1 else 0 end) as third_party_pay_basic_programmes_10min_plus
,sum(case when pay=1 and sky_channel=0 and grouped_channel not in ('Sky Movies Channels','Sky Sports Channels') and viewing_duration/cast(programme_instance_duration as real)>=0.6  and b.channel_name not in ('BT Sport 1','BT Sport 2','ESPN','ESPN America','ESPN Classic','Eurosport','Eurosport UK','Eurosport2','Eurosport2 UK') then 1
when pay=1 and sky_channel=0 and grouped_channel not in ('Sky Movies Channels','Sky Sports Channels') and viewing_duration>=3600
 and programme_instance_duration >=5400  
and b.channel_name not in ('BT Sport 1','BT Sport 2','ESPN','ESPN America','ESPN Classic','Eurosport','Eurosport UK','Eurosport2','Eurosport2 UK')
 then 1 else 0 end)
as third_party_pay_basic_programmes_60pc_or_1hr    
         

,sum(case when pay=1 and grouped_channel  in ('Sky Movies Channels') and viewing_duration>=180 
then 1 else 0 end) as sky_movies_programmes_03min_plus
,sum(case when pay=1 and grouped_channel  in ('Sky Movies Channels') and viewing_duration>=600 
then 1 else 0 end) as sky_movies_programmes_10min_plus
,sum(case when pay=1 and grouped_channel  in ('Sky Movies Channels') and viewing_duration/cast(programme_instance_duration as real)>=0.6 then 1
when pay=1  and grouped_channel  in ('Sky Movies Channels') and viewing_duration>=3600 and programme_instance_duration >=5400 then 1 else 0 end)
as sky_movies_programmes_60pc_or_1hr  



,sum(case when pay=1 
    and (b.channel_name in ('BT Sport 1','BT Sport 2','ESPN','ESPN America','ESPN Classic','Eurosport','Eurosport UK','Eurosport2','Eurosport2 UK') 
       or   grouped_channel = 'Sky Box Office')

and viewing_duration>=180 
then 1 else 0 end) as other_programmes_03min_plus
,sum(case when pay=1  and viewing_duration>=600 
 and (b.channel_name in ('BT Sport 1','BT Sport 2','ESPN','ESPN America','ESPN Classic','Eurosport','Eurosport UK','Eurosport2','Eurosport2 UK') 
       or   grouped_channel = 'Sky Box Office') then 1 else 0 end) as other_programmes_10min_plus

,sum(case when pay=1  and viewing_duration/cast(programme_instance_duration as real)>=0.6
 and (b.channel_name in ('BT Sport 1','BT Sport 2','ESPN','ESPN America','ESPN Classic','Eurosport','Eurosport UK','Eurosport2','Eurosport2 UK') 
       or   grouped_channel = 'Sky Box Office')
then 1
when pay=1 and viewing_duration>=3600 and programme_instance_duration >=5400 
and (b.channel_name in ('BT Sport 1','BT Sport 2','ESPN','ESPN America','ESPN Classic','Eurosport','Eurosport UK','Eurosport2','Eurosport2 UK') 
       or   grouped_channel = 'Sky Box Office') then 1 else 0 end)
as other_programmes_60pc_or_1hr  


into dbarnett.v250_prog_watched_from_201307_data
from  dbarnett.v250_programme_summary_201307 as a
--from dbarnett.v250_viewing_by_account_and_channel as a  Changed to use new data created in Part 22b
left outer join v250_channel_to_service_key_lookup_deduped as b
on a.service_key=b.service_key
--left outer join sk_prod.Vespa_programme_schedule as c
--ON a.dk_programme_instance_dim = c.pk_programme_instance_dim
group by account_number
;
commit;
--select top 100 * from dbarnett.v250_prog_watched_from_201307_data;
drop table dbarnett.v250_programme_summary_201307;
commit;





----repeat for August---
--drop table dbarnett.v250_programme_summary_201308;
select account_number
,service_key
,dk_programme_instance_dim
,programme_instance_duration
,sum(case when a.capped_partial_flag = 1 then datediff(second, a.instance_start_date_time_utc, a.capping_end_date_time_utc)
     else datediff(second, a.instance_start_date_time_utc, a.instance_end_date_time_utc)
     end)  as viewing_duration
into dbarnett.v250_programme_summary_201308
from  sk_prod.vespa_dp_prog_viewed_201308 as a
where capped_full_flag = 0 -- only those instances that have not been fully capped
                           and instance_start_date_time_utc < instance_end_date_time_utc              -- Remove 0sec instances
                           and (reported_playback_speed is null or reported_playback_speed = 2) -- Live or Recorded Records
                           and account_number is not null --remove instances we do not know the account for
                           and subscriber_id is not null --remove instances we do not know the subscriber_id for
                           and (type_of_viewing_event in ('HD Viewing Event', 'Sky+ time-shifted viewing event', 'TV Channel Viewing')-- limit to keep out 
                            --interactive viewing and other service viewing event i.e. where it could not identify viewing event type it was
                            or (type_of_viewing_event = 'Other Service Viewing Event' 
                            and service_type_description in ('NVOD service','High Definition TV test service','Digital TV channel')))
                           and capping_end_date_time_utc is not null and duration>=180

group by account_number
,service_key
,dk_programme_instance_dim
,programme_instance_duration
;
commit;
CREATE HG INDEX idx1 ON dbarnett.v250_programme_summary_201308(account_number);
commit;
CREATE HG INDEX idx2 ON dbarnett.v250_programme_summary_201308(service_key);


--drop table dbarnett.v250_prog_watched_from_201308_data;
select account_number 

,sum(case when pay=1 and viewing_duration>=180 
then 1 else 0 end) as fta_programmes_03min_plus
,sum(case when pay=1 and viewing_duration>=600 
then 1 else 0 end) as fta_programmes_10min_plus
,sum(case when pay=1 and viewing_duration/cast(programme_instance_duration as real)>=0.6 then 1
when pay=1 and viewing_duration>=3600 and programme_instance_duration >=5400 then 1 else 0 end)
as fta_programmes_60pc_or_1hr
 

,sum(case when pay=1 and sky_channel=1 and grouped_channel not in ('Sky Movies Channels','Sky Sports Channels','Sky Box Office') and viewing_duration>=180 
then 1 else 0 end) as sky_pay_basic_programmes_03min_plus
,sum(case when pay=1 and sky_channel=1 and grouped_channel not in ('Sky Movies Channels','Sky Sports Channels','Sky Box Office') and viewing_duration>=600 
then 1 else 0 end) as sky_pay_basic_programmes_10min_plus
,sum(case when pay=1 and sky_channel=1 and grouped_channel not in ('Sky Movies Channels','Sky Sports Channels','Sky Box Office') and viewing_duration/cast(programme_instance_duration as real)>=0.6 then 1
when pay=1 and sky_channel=1 and grouped_channel not in ('Sky Movies Channels','Sky Sports Channels','Sky Box Office') and viewing_duration>=3600 and programme_instance_duration >=5400 then 1 else 0 end)
as sky_pay_basic_programmes_60pc_or_1hr    
         


,sum(case when pay=1 and sky_channel=0 and grouped_channel not in ('Sky Movies Channels','Sky Sports Channels') and viewing_duration>=180 
 and b.channel_name not in ('BT Sport 1','BT Sport 2','ESPN','ESPN America','ESPN Classic','Eurosport','Eurosport UK','Eurosport2','Eurosport2 UK') then 1 else 0 end) as third_party_pay_basic_programmes_03min_plus
,sum(case when pay=1 and sky_channel=0 and grouped_channel not in ('Sky Movies Channels','Sky Sports Channels') and viewing_duration>=600 
 and b.channel_name not in ('BT Sport 1','BT Sport 2','ESPN','ESPN America','ESPN Classic','Eurosport','Eurosport UK','Eurosport2','Eurosport2 UK') then 1 else 0 end) as third_party_pay_basic_programmes_10min_plus
,sum(case when pay=1 and sky_channel=0 and grouped_channel not in ('Sky Movies Channels','Sky Sports Channels') and viewing_duration/cast(programme_instance_duration as real)>=0.6  and b.channel_name not in ('BT Sport 1','BT Sport 2','ESPN','ESPN America','ESPN Classic','Eurosport','Eurosport UK','Eurosport2','Eurosport2 UK') then 1
when pay=1 and sky_channel=0 and grouped_channel not in ('Sky Movies Channels','Sky Sports Channels') and viewing_duration>=3600
 and programme_instance_duration >=5400  
and b.channel_name not in ('BT Sport 1','BT Sport 2','ESPN','ESPN America','ESPN Classic','Eurosport','Eurosport UK','Eurosport2','Eurosport2 UK')
 then 1 else 0 end)
as third_party_pay_basic_programmes_60pc_or_1hr    
         

,sum(case when pay=1 and grouped_channel  in ('Sky Movies Channels') and viewing_duration>=180 
then 1 else 0 end) as sky_movies_programmes_03min_plus
,sum(case when pay=1 and grouped_channel  in ('Sky Movies Channels') and viewing_duration>=600 
then 1 else 0 end) as sky_movies_programmes_10min_plus
,sum(case when pay=1 and grouped_channel  in ('Sky Movies Channels') and viewing_duration/cast(programme_instance_duration as real)>=0.6 then 1
when pay=1  and grouped_channel  in ('Sky Movies Channels') and viewing_duration>=3600 and programme_instance_duration >=5400 then 1 else 0 end)
as sky_movies_programmes_60pc_or_1hr  



,sum(case when pay=1 
    and (b.channel_name in ('BT Sport 1','BT Sport 2','ESPN','ESPN America','ESPN Classic','Eurosport','Eurosport UK','Eurosport2','Eurosport2 UK') 
       or   grouped_channel = 'Sky Box Office')

and viewing_duration>=180 
then 1 else 0 end) as other_programmes_03min_plus
,sum(case when pay=1  and viewing_duration>=600 
 and (b.channel_name in ('BT Sport 1','BT Sport 2','ESPN','ESPN America','ESPN Classic','Eurosport','Eurosport UK','Eurosport2','Eurosport2 UK') 
       or   grouped_channel = 'Sky Box Office') then 1 else 0 end) as other_programmes_10min_plus

,sum(case when pay=1  and viewing_duration/cast(programme_instance_duration as real)>=0.6
 and (b.channel_name in ('BT Sport 1','BT Sport 2','ESPN','ESPN America','ESPN Classic','Eurosport','Eurosport UK','Eurosport2','Eurosport2 UK') 
       or   grouped_channel = 'Sky Box Office')
then 1
when pay=1 and viewing_duration>=3600 and programme_instance_duration >=5400 
and (b.channel_name in ('BT Sport 1','BT Sport 2','ESPN','ESPN America','ESPN Classic','Eurosport','Eurosport UK','Eurosport2','Eurosport2 UK') 
       or   grouped_channel = 'Sky Box Office') then 1 else 0 end)
as other_programmes_60pc_or_1hr  


into dbarnett.v250_prog_watched_from_201308_data
from  dbarnett.v250_programme_summary_201308 as a
--from dbarnett.v250_viewing_by_account_and_channel as a  Changed to use new data created in Part 22b
left outer join v250_channel_to_service_key_lookup_deduped as b
on a.service_key=b.service_key
--left outer join sk_prod.Vespa_programme_schedule as c
--ON a.dk_programme_instance_dim = c.pk_programme_instance_dim
group by account_number
;
commit;
--select top 100 * from dbarnett.v250_prog_watched_from_201308_data;
drop table dbarnett.v250_programme_summary_201308;
commit;








--select count(*) from dbarnett.v250_programme_summary_201308;