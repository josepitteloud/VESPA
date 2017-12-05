/*-----------------------------------------------------------------------------------------------------------------
        Project:V250 - Sports right Analysis Profiling
        Part - Part 05 Create Grouped Rights details for key rights e.g., Premier League
        
        Analyst: Dan Barnett
        SK Prod: 5

        

*/------------------------------------------------------------------------------------------------------------------
--alter table dbarnett.v250_sports_rights_epg_data_for_analysis delete analysis_right_grouped ;
alter table dbarnett.v250_sports_rights_epg_data_for_analysis add analysis_right_grouped varchar(80);

update dbarnett.v250_sports_rights_epg_data_for_analysis
set analysis_right_grouped = 
case when analysis_right_new in ('ECB Test Cricket Sky Sports'
,'ECB non-Test Cricket Sky Sports')
then 'ECB Cricket Sky Sports'
when analysis_right_new in (
'F1 (Practice Live)- BBC'
,'F1 (Qualifying Live)- BBC'
,'F1 (Race Live)- BBC'
,'F1 (non-Live)- BBC')
then 'F1 - BBC'
when analysis_right_new in 
('Formula One 2012-2018 - (Practice Live) Sky Sports'
,'Formula One 2012-2018 - (Qualifying Live) Sky Sports'
,'Formula One 2012-2018 - (Race Live) Sky Sports'
,'Formula One 2012-2018 - (non-Live) Sky Sports')
then 'F1 - Sky Sports'

when analysis_right_new in (
'Premier League Football - Sky Sports (MNF)'
,'Premier League Football - Sky Sports (Match Choice)'
,'Premier League Football - Sky Sports (Sat Lunchtime)'
,'Premier League Football - Sky Sports (Sat Night Live)'
,'Premier League Football - Sky Sports (Sun 4pm)'
,'Premier League Football - Sky Sports (Sun Lunchtime)'
,'Premier League Football - Sky Sports (non Live)'
,'Premier League Football - Sky Sports (other Live)')
then 'Premier League Football - Sky Sports'
when 
analysis_right_new
in ('England Friendlies (Football) - ITV'
,'England World Cup Qualifying (Away) - ITV'
,'England World Cup Qualifying (Home) - ITV')
then 'England Football Internationals - ITV'

WHEN analysis_right_new
in ('UEFA Champions League -  Sky Sports (Tue)'
,'UEFA Champions League -  Sky Sports (Wed)'
,'UEFA Champions League -  Sky Sports (non Live)'
,'UEFA Champions League -  Sky Sports (other Live)')
then 'UEFA Champions League -  Sky Sports'
else analysis_right_new end
from dbarnett.v250_sports_rights_epg_data_for_analysis
;
commit;