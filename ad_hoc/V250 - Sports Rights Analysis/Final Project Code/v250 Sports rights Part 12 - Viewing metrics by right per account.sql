/*-----------------------------------------------------------------------------------------------------------------
        Project:V250 - Sports right Analysis Profiling
        Part - Part 12 Viewing summary
        
        Analyst: Dan Barnett
        SK Prod: 5
        Create Summary viewing per right for each account also with Live/Non Live version as well as overall

*/------------------------------------------------------------------------------------------------------------------
commit;
CREATE HG INDEX idx1 ON dbarnett.v250_sports_rights_viewed_by_right_overall (account_number);
CREATE HG INDEX idx2 ON dbarnett.v250_sports_rights_viewed_by_right_overall (analysis_right);
commit;
----Convert Activity to one record per account--
--20140212
--dbarnett.v250_sports_rights_viewed_by_right_overall
--Part 1 - Overall
drop table dbarnett.v250_unannualised_right_activity;
select account_number

,sum(case when analysis_right ='Africa Cup of Nations - Eurosport' then broadcast_days_viewed else 0 end) 
as AFCEUR_Broadcast_Days_Viewed
,sum(case when analysis_right ='Africa Cup of Nations - ITV' then broadcast_days_viewed else 0 end) 
as AFCITV_Broadcast_Days_Viewed
,sum(case when analysis_right ='Americas Cup - BBC' then broadcast_days_viewed else 0 end) 
as AMCBBC_Broadcast_Days_Viewed
,sum(case when analysis_right ='Asian Tour Golf 2012-2013 Sky Sports' then broadcast_days_viewed else 0 end) 
as ATGSS_Broadcast_Days_Viewed
,sum(case when analysis_right ='ATP Tennis Masters Series  2011-2013 Sky Sports' then broadcast_days_viewed else 0 end) 
as ATPSS_Broadcast_Days_Viewed
,sum(case when analysis_right ='Australia Home Cricket  2012-2016 Sky Sports' then broadcast_days_viewed else 0 end) 
as AHCSS_Broadcast_Days_Viewed
,sum(case when analysis_right ='Australian Football - BT Sport' then broadcast_days_viewed else 0 end) 
as AUFBTS_Broadcast_Days_Viewed
,sum(case when analysis_right ='Australian Open Tennis - BBC' then broadcast_days_viewed else 0 end) 
as AOTBBC_Broadcast_Days_Viewed
,sum(case when analysis_right ='Australian Open Tennis - Eurosport' then broadcast_days_viewed else 0 end) 
as AOTEUR_Broadcast_Days_Viewed
,sum(case when analysis_right ='Aviva Premiership - ESPN' then broadcast_days_viewed else 0 end) 
as AVPSS_Broadcast_Days_Viewed
,sum(case when analysis_right ='BBC American Football' then broadcast_days_viewed else 0 end) 
as AFBBC_Broadcast_Days_Viewed
,sum(case when analysis_right ='BBC Athletics' then broadcast_days_viewed else 0 end) 
as ATHBBC_Broadcast_Days_Viewed
,sum(case when analysis_right ='BBC Boxing' then broadcast_days_viewed else 0 end) 
as BOXBBC_Broadcast_Days_Viewed
,sum(case when analysis_right ='BBC Darts' then broadcast_days_viewed else 0 end) 
as DRTBBC_Broadcast_Days_Viewed
,sum(case when analysis_right ='BBC Equestrian' then broadcast_days_viewed else 0 end) 
as EQUBBC_Broadcast_Days_Viewed
,sum(case when analysis_right ='BBC Football' then broadcast_days_viewed else 0 end) 
as FOOTBBC_Broadcast_Days_Viewed
,sum(case when analysis_right ='BBC Golf' then broadcast_days_viewed else 0 end) 
as GOLFBBC_Broadcast_Days_Viewed
,sum(case when analysis_right ='BBC Motor Sport' then broadcast_days_viewed else 0 end) 
as MSPBBC_Broadcast_Days_Viewed
,sum(case when analysis_right ='BBC Rugby' then broadcast_days_viewed else 0 end) 
as RUGBBC_Broadcast_Days_Viewed
,sum(case when analysis_right ='BBC Snooker/Pool' then broadcast_days_viewed else 0 end) 
as SNPBBC_Broadcast_Days_Viewed
,sum(case when analysis_right ='BBC Tennis' then broadcast_days_viewed else 0 end) 
as TENBBC_Broadcast_Days_Viewed
,sum(case when analysis_right ='BBC Unknown' then broadcast_days_viewed else 0 end) 
as UNKBBC_Broadcast_Days_Viewed
,sum(case when analysis_right ='BBC Watersports' then broadcast_days_viewed else 0 end) 
as WATBBC_Broadcast_Days_Viewed
,sum(case when analysis_right ='BBC Wintersports' then broadcast_days_viewed else 0 end) 
as WINBBC_Broadcast_Days_Viewed
,sum(case when analysis_right ='Boxing  - Channel 5' then broadcast_days_viewed else 0 end) 
as BOXCH5_Broadcast_Days_Viewed
,sum(case when analysis_right ='Boxing - Matchroom 2012-2014 Sky Sports' then broadcast_days_viewed else 0 end) 
as BOXMSS_Broadcast_Days_Viewed
,sum(case when analysis_right ='Brazil Football - BT Sport' then broadcast_days_viewed else 0 end) 
as BFTBTS_Broadcast_Days_Viewed
,sum(case when analysis_right ='British & Irish Lions 2013 - Sky Sports' then broadcast_days_viewed else 0 end) 
as BILSS_Broadcast_Days_Viewed
,sum(case when analysis_right ='British Open Golf - BBC' then broadcast_days_viewed else 0 end) 
as BOGSS_Broadcast_Days_Viewed
,sum(case when analysis_right ='BT Sport American Football' then broadcast_days_viewed else 0 end) 
as AFBTS_Broadcast_Days_Viewed
,sum(case when analysis_right ='BT Sport Athletics' then broadcast_days_viewed else 0 end) 
as ATHBTS_Broadcast_Days_Viewed
,sum(case when analysis_right ='BT Sport Baseball' then broadcast_days_viewed else 0 end) 
as BASEBTS_Broadcast_Days_Viewed
,sum(case when analysis_right ='BT Sport Basketball' then broadcast_days_viewed else 0 end) 
as BASKBTS_Broadcast_Days_Viewed
,sum(case when analysis_right ='BT Sport Boxing' then broadcast_days_viewed else 0 end) 
as BOXBTS_Broadcast_Days_Viewed
,sum(case when analysis_right ='BT Sport Cricket' then broadcast_days_viewed else 0 end) 
as CRIBTS_Broadcast_Days_Viewed
,sum(case when analysis_right ='BT Sport Equestrian' then broadcast_days_viewed else 0 end) 
as EQUBTS_Broadcast_Days_Viewed
,sum(case when analysis_right ='BT Sport Extreme' then broadcast_days_viewed else 0 end) 
as EXTBTS_Broadcast_Days_Viewed
,sum(case when analysis_right ='BT Sport Football' then broadcast_days_viewed else 0 end) 
as FOOTBTS_Broadcast_Days_Viewed
,sum(case when analysis_right ='BT Sport Motor Sport' then broadcast_days_viewed else 0 end) 
as MSPBTS_Broadcast_Days_Viewed
,sum(case when analysis_right ='BT Sport Rugby' then broadcast_days_viewed else 0 end) 
as RUGBTS_Broadcast_Days_Viewed
,sum(case when analysis_right ='BT Sport Tennis' then broadcast_days_viewed else 0 end) 
as TENBTS_Broadcast_Days_Viewed
,sum(case when analysis_right ='BT Sport Unknown' then broadcast_days_viewed else 0 end) 
as UNKBTS_Broadcast_Days_Viewed
,sum(case when analysis_right ='BT Sport Wintersports' then broadcast_days_viewed else 0 end) 
as WINBTS_Broadcast_Days_Viewed
,sum(case when analysis_right ='Bundesliga - BT Sport' then broadcast_days_viewed else 0 end) 
as BUNBTS_Broadcast_Days_Viewed
,sum(case when analysis_right ='Bundesliga- ESPN' then broadcast_days_viewed else 0 end) 
as BUNESPN_Broadcast_Days_Viewed
,sum(case when analysis_right ='Challenge Darts' then broadcast_days_viewed else 0 end) 
as DRTCHA_Broadcast_Days_Viewed
,sum(case when analysis_right ='Challenge Extreme' then broadcast_days_viewed else 0 end) 
as EXTCHA_Broadcast_Days_Viewed
,sum(case when analysis_right ='Challenge Unknown' then broadcast_days_viewed else 0 end) 
as UNKCHA_Broadcast_Days_Viewed
,sum(case when analysis_right ='Challenge Wrestling' then broadcast_days_viewed else 0 end) 
as WRECHA_Broadcast_Days_Viewed
,sum(case when analysis_right ='Champions League - ITV' then broadcast_days_viewed else 0 end) 
as CHLITV_Broadcast_Days_Viewed
,sum(case when analysis_right ='Champions Trophy - I C C Cricket Deal  2012-2015 Sky Sports' then broadcast_days_viewed else 0 end) 
as ICCSS_Broadcast_Days_Viewed
,sum(case when analysis_right ='Channel 4 American Football' then broadcast_days_viewed else 0 end) 
as AMCH4_Broadcast_Days_Viewed
,sum(case when analysis_right ='Channel 4 Athletics' then broadcast_days_viewed else 0 end) 
as ATHCH4_Broadcast_Days_Viewed
,sum(case when analysis_right ='Channel 4 Boxing' then broadcast_days_viewed else 0 end) 
as BOXCH4_Broadcast_Days_Viewed
,sum(case when analysis_right ='Channel 4 Equestrian' then broadcast_days_viewed else 0 end) 
as EQUCH4_Broadcast_Days_Viewed
,sum(case when analysis_right ='Channel 4 Extreme' then broadcast_days_viewed else 0 end) 
as EXTCH4_Broadcast_Days_Viewed
,sum(case when analysis_right ='Channel 4 Football' then broadcast_days_viewed else 0 end) 
as FOOTCH4_Broadcast_Days_Viewed
,sum(case when analysis_right ='Channel 4 Motor Sport' then broadcast_days_viewed else 0 end) 
as MSPCH4_Broadcast_Days_Viewed
,sum(case when analysis_right ='Channel 4 Racing' then broadcast_days_viewed else 0 end) 
as RACCH4_Broadcast_Days_Viewed
,sum(case when analysis_right ='Channel 4 Unknown' then broadcast_days_viewed else 0 end) 
as UNKCH4_Broadcast_Days_Viewed
,sum(case when analysis_right ='Channel 4 Watersports' then broadcast_days_viewed else 0 end) 
as WATCH4_Broadcast_Days_Viewed
,sum(case when analysis_right ='Channel 4 Wintersports' then broadcast_days_viewed else 0 end) 
as WINCH4_Broadcast_Days_Viewed
,sum(case when analysis_right ='Channel 5 Athletics' then broadcast_days_viewed else 0 end) 
as ATHCH5_Broadcast_Days_Viewed
,sum(case when analysis_right ='Channel 5 Boxing' then broadcast_days_viewed else 0 end) 
as BOXOCH5_Broadcast_Days_Viewed
,sum(case when analysis_right ='Channel 5 Cricket' then broadcast_days_viewed else 0 end) 
as CRICH5_Broadcast_Days_Viewed
,sum(case when analysis_right ='Channel 5 Motor Sport' then broadcast_days_viewed else 0 end) 
as MSPCH5_Broadcast_Days_Viewed
,sum(case when analysis_right ='Channel 5 Unknown' then broadcast_days_viewed else 0 end) 
as UNKCH5_Broadcast_Days_Viewed
,sum(case when analysis_right ='Channel 5 Wrestling' then broadcast_days_viewed else 0 end) 
as WRECH5_Broadcast_Days_Viewed
,sum(case when analysis_right ='Cheltenham Festival - Channel 4' then broadcast_days_viewed else 0 end) 
as CHELCH4_Broadcast_Days_Viewed
,sum(case when analysis_right ='Community Shield - ITV' then broadcast_days_viewed else 0 end) 
as CMSITV_Broadcast_Days_Viewed
,sum(case when analysis_right ='Confederations Cup - BBC' then broadcast_days_viewed else 0 end) 
as CONCBBC_Broadcast_Days_Viewed
,sum(case when analysis_right ='Conference - BT Sport' then broadcast_days_viewed else 0 end) 
as CONFBTS_Broadcast_Days_Viewed
,sum(case when analysis_right ='Cycling - La Vuelta ITV' then broadcast_days_viewed else 0 end) 
as CLVITV_Broadcast_Days_Viewed
,sum(case when analysis_right ='Cycling - U C I World Tour Sky Sports' then broadcast_days_viewed else 0 end) 
as CUCISS_Broadcast_Days_Viewed
,sum(case when analysis_right ='Cycling Tour of Britain - Eurosport' then broadcast_days_viewed else 0 end) 
as CTBEUR_Broadcast_Days_Viewed
,sum(case when analysis_right ='Cycling: tour of britain ITV4' then broadcast_days_viewed else 0 end) 
as CTCITV_Broadcast_Days_Viewed
,sum(case when analysis_right ='Derby - Channel 4' then broadcast_days_viewed else 0 end) 
as DERCH4_Broadcast_Days_Viewed
,sum(case when analysis_right ='ECB (highlights) - Channel 5' then broadcast_days_viewed else 0 end) 
as ECBHCH5_Broadcast_Days_Viewed
,sum(case when analysis_right ='ECB Cricket Sky Sports' then broadcast_days_viewed else 0 end) 
as GECRSS_Broadcast_Days_Viewed
,sum(case when analysis_right ='ECB non-Test Cricket Sky Sports' then broadcast_days_viewed else 0 end) 
as ECBNSS_Broadcast_Days_Viewed
,sum(case when analysis_right ='ECB Test Cricket Sky Sports' then broadcast_days_viewed else 0 end) 
as ECBTSS_Broadcast_Days_Viewed
,sum(case when analysis_right ='England Football Internationals - ITV' then broadcast_days_viewed else 0 end) 
as GENGITV_Broadcast_Days_Viewed
,sum(case when analysis_right ='England Friendlies (Football) - ITV' then broadcast_days_viewed else 0 end) 
as EFRITV_Broadcast_Days_Viewed
,sum(case when analysis_right ='England Rugby Internationals 2010-2015 - Sky Sports' then broadcast_days_viewed else 0 end) 
as ENRSS_Broadcast_Days_Viewed
,sum(case when analysis_right ='England World Cup Qualifying (Away) - ITV' then broadcast_days_viewed else 0 end) 
as EWQAITV_Broadcast_Days_Viewed
,sum(case when analysis_right ='England World Cup Qualifying (Home) - ITV' then broadcast_days_viewed else 0 end) 
as EWQHITV_Broadcast_Days_Viewed
,sum(case when analysis_right ='ESPN American Football' then broadcast_days_viewed else 0 end) 
as AMESPN_Broadcast_Days_Viewed
,sum(case when analysis_right ='ESPN Athletics' then broadcast_days_viewed else 0 end) 
as ATHESPN_Broadcast_Days_Viewed
,sum(case when analysis_right ='ESPN Baseball' then broadcast_days_viewed else 0 end) 
as BASEESPN_Broadcast_Days_Viewed
,sum(case when analysis_right ='ESPN Basketball' then broadcast_days_viewed else 0 end) 
as BASKESPN_Broadcast_Days_Viewed
,sum(case when analysis_right ='ESPN Boxing' then broadcast_days_viewed else 0 end) 
as BOXESPN_Broadcast_Days_Viewed
,sum(case when analysis_right ='ESPN Cricket' then broadcast_days_viewed else 0 end) 
as CRIESPN_Broadcast_Days_Viewed
,sum(case when analysis_right ='ESPN Darts' then broadcast_days_viewed else 0 end) 
as DARTESPN_Broadcast_Days_Viewed
,sum(case when analysis_right ='ESPN Equestrian' then broadcast_days_viewed else 0 end) 
as EQUESPN_Broadcast_Days_Viewed
,sum(case when analysis_right ='ESPN Extreme' then broadcast_days_viewed else 0 end) 
as EXTESPN_Broadcast_Days_Viewed
,sum(case when analysis_right ='ESPN Football' then broadcast_days_viewed else 0 end) 
as FOOTESPN_Broadcast_Days_Viewed
,sum(case when analysis_right ='ESPN Golf' then broadcast_days_viewed else 0 end) 
as GOLFESPN_Broadcast_Days_Viewed
,sum(case when analysis_right ='ESPN Ice Hockey' then broadcast_days_viewed else 0 end) 
as IHESPN_Broadcast_Days_Viewed
,sum(case when analysis_right ='ESPN Motor Sport' then broadcast_days_viewed else 0 end) 
as MSPESPN_Broadcast_Days_Viewed
,sum(case when analysis_right ='ESPN Racing' then broadcast_days_viewed else 0 end) 
as RACESPN_Broadcast_Days_Viewed
,sum(case when analysis_right ='ESPN Rugby' then broadcast_days_viewed else 0 end) 
as RUGESPN_Broadcast_Days_Viewed
,sum(case when analysis_right ='ESPN Snooker/Pool' then broadcast_days_viewed else 0 end) 
as SNPESPN_Broadcast_Days_Viewed
,sum(case when analysis_right ='ESPN Tennis' then broadcast_days_viewed else 0 end) 
as TENESPN_Broadcast_Days_Viewed
,sum(case when analysis_right ='ESPN Unknown' then broadcast_days_viewed else 0 end) 
as UNKESPN_Broadcast_Days_Viewed
,sum(case when analysis_right ='ESPN Watersports' then broadcast_days_viewed else 0 end) 
as WATESPN_Broadcast_Days_Viewed
,sum(case when analysis_right ='ESPN Wintersports' then broadcast_days_viewed else 0 end) 
as WINESPN_Broadcast_Days_Viewed
,sum(case when analysis_right ='ESPN Wrestling' then broadcast_days_viewed else 0 end) 
as WREESPN_Broadcast_Days_Viewed
,sum(case when analysis_right ='Europa League - BT Sport' then broadcast_days_viewed else 0 end) 
as ELBTSP_Broadcast_Days_Viewed
,sum(case when analysis_right ='Europa League - ESPN' then broadcast_days_viewed else 0 end) 
as ELESPN_Broadcast_Days_Viewed
,sum(case when analysis_right ='Europa League - ITV' then broadcast_days_viewed else 0 end) 
as ELITV_Broadcast_Days_Viewed
,sum(case when analysis_right ='European Tour Golf - Sky Sports' then broadcast_days_viewed else 0 end) 
as ETGSS_Broadcast_Days_Viewed
,sum(case when analysis_right ='Eurosport American Football' then broadcast_days_viewed else 0 end) 
as AMEUR_Broadcast_Days_Viewed
,sum(case when analysis_right ='Eurosport Athletics' then broadcast_days_viewed else 0 end) 
as ATHEUR_Broadcast_Days_Viewed
,sum(case when analysis_right ='Eurosport Baseball' then broadcast_days_viewed else 0 end) 
as BASEEUR_Broadcast_Days_Viewed
,sum(case when analysis_right ='Eurosport Basketball' then broadcast_days_viewed else 0 end) 
as BASKEUR_Broadcast_Days_Viewed
,sum(case when analysis_right ='Eurosport Boxing' then broadcast_days_viewed else 0 end) 
as BOXEUR_Broadcast_Days_Viewed
,sum(case when analysis_right ='Eurosport Cricket' then broadcast_days_viewed else 0 end) 
as CRIEUR_Broadcast_Days_Viewed
,sum(case when analysis_right ='Eurosport Darts' then broadcast_days_viewed else 0 end) 
as DARTEUR_Broadcast_Days_Viewed
,sum(case when analysis_right ='Eurosport Equestrian' then broadcast_days_viewed else 0 end) 
as EQUEUR_Broadcast_Days_Viewed
,sum(case when analysis_right ='Eurosport Extreme' then broadcast_days_viewed else 0 end) 
as EXTEUR_Broadcast_Days_Viewed
,sum(case when analysis_right ='Eurosport Football' then broadcast_days_viewed else 0 end) 
as FOOTEUR_Broadcast_Days_Viewed
,sum(case when analysis_right ='Eurosport Golf' then broadcast_days_viewed else 0 end) 
as GOLFEUR_Broadcast_Days_Viewed
,sum(case when analysis_right ='Eurosport Ice Hockey' then broadcast_days_viewed else 0 end) 
as IHEUR_Broadcast_Days_Viewed
,sum(case when analysis_right ='Eurosport Motor Sport' then broadcast_days_viewed else 0 end) 
as MSPEUR_Broadcast_Days_Viewed
,sum(case when analysis_right ='Eurosport Racing' then broadcast_days_viewed else 0 end) 
as RACEUR_Broadcast_Days_Viewed
,sum(case when analysis_right ='Eurosport Snooker/Pool' then broadcast_days_viewed else 0 end) 
as SNPEUR_Broadcast_Days_Viewed
,sum(case when analysis_right ='Eurosport Tennis' then broadcast_days_viewed else 0 end) 
as TENEUR_Broadcast_Days_Viewed
,sum(case when analysis_right ='Eurosport Unknown' then broadcast_days_viewed else 0 end) 
as UNKEUR_Broadcast_Days_Viewed
,sum(case when analysis_right ='Eurosport Watersports' then broadcast_days_viewed else 0 end) 
as WATEUR_Broadcast_Days_Viewed
,sum(case when analysis_right ='Eurosport Wintersports' then broadcast_days_viewed else 0 end) 
as WINEUR_Broadcast_Days_Viewed
,sum(case when analysis_right ='F1 - BBC' then broadcast_days_viewed else 0 end) 
as GF1BBC_Broadcast_Days_Viewed
,sum(case when analysis_right ='F1 - Sky Sports' then broadcast_days_viewed else 0 end) 
as GF1SS_Broadcast_Days_Viewed
,sum(case when analysis_right ='F1 (non-Live)- BBC' then broadcast_days_viewed else 0 end) 
as F1NBBC_Broadcast_Days_Viewed
,sum(case when analysis_right ='F1 (Practice Live)- BBC' then broadcast_days_viewed else 0 end) 
as F1PBBC_Broadcast_Days_Viewed
,sum(case when analysis_right ='F1 (Qualifying Live)- BBC' then broadcast_days_viewed else 0 end) 
as F1QBBC_Broadcast_Days_Viewed
,sum(case when analysis_right ='F1 (Race Live)- BBC' then broadcast_days_viewed else 0 end) 
as F1RBBC_Broadcast_Days_Viewed
,sum(case when analysis_right ='FA Cup - ESPN' then broadcast_days_viewed else 0 end) 
as FACESPN_Broadcast_Days_Viewed
,sum(case when analysis_right ='FA Cup - ITV' then broadcast_days_viewed else 0 end) 
as FACITV_Broadcast_Days_Viewed
,sum(case when analysis_right ='Football League - Seasons 2012/13 To 14/15 Sky Sports - Capital 1 Cup' then broadcast_days_viewed else 0 end) 
as FLCCSS_Broadcast_Days_Viewed
,sum(case when analysis_right ='Football League - Seasons 2012/13 To 14/15 Sky Sports - others' then broadcast_days_viewed else 0 end) 
as FLOTSS_Broadcast_Days_Viewed
,sum(case when analysis_right ='Formula One 2012-2018 - (non-Live) Sky Sports' then broadcast_days_viewed else 0 end) 
as F1NSS_Broadcast_Days_Viewed
,sum(case when analysis_right ='Formula One 2012-2018 - (Practice Live) Sky Sports' then broadcast_days_viewed else 0 end) 
as F1PSS_Broadcast_Days_Viewed
,sum(case when analysis_right ='Formula One 2012-2018 - (Qualifying Live) Sky Sports' then broadcast_days_viewed else 0 end) 
as F1QSS_Broadcast_Days_Viewed
,sum(case when analysis_right ='Formula One 2012-2018 - (Race Live) Sky Sports' then broadcast_days_viewed else 0 end) 
as F1RSS_Broadcast_Days_Viewed
,sum(case when analysis_right ='French Open Tennis - Eurosport' then broadcast_days_viewed else 0 end) 
as FOTEUR_Broadcast_Days_Viewed
,sum(case when analysis_right ='French Open Tennis - ITV' then broadcast_days_viewed else 0 end) 
as FOTITV_Broadcast_Days_Viewed
,sum(case when analysis_right ='Grand National - Channel 4' then broadcast_days_viewed else 0 end) 
as GDNCH4_Broadcast_Days_Viewed
,sum(case when analysis_right ='Heineken Cup 2010/11 To 2013/14- Sky Sports' then broadcast_days_viewed else 0 end) 
as HECSS_Broadcast_Days_Viewed
,sum(case when analysis_right ='I R B Sevens 2008/09 -2014/15 - Sky Sports' then broadcast_days_viewed else 0 end) 
as IRBSS_Broadcast_Days_Viewed
,sum(case when analysis_right ='IAAF World Athletics Championship - Eurosport' then broadcast_days_viewed else 0 end) 
as WACEUR_Broadcast_Days_Viewed
,sum(case when analysis_right ='India Home Cricket 2012-2018 Sky Sports' then broadcast_days_viewed else 0 end) 
as IHCSS_Broadcast_Days_Viewed
,sum(case when analysis_right ='India Premier League - ITV' then broadcast_days_viewed else 0 end) 
as IPLITV_Broadcast_Days_Viewed
,sum(case when analysis_right ='International Freindlies - ESPN' then broadcast_days_viewed else 0 end) 
as IFESPN_Broadcast_Days_Viewed
,sum(case when analysis_right ='International Friendlies - BT Sport' then broadcast_days_viewed else 0 end) 
as IFBTS_Broadcast_Days_Viewed
,sum(case when analysis_right ='ITV1 Boxing' then broadcast_days_viewed else 0 end) 
as BOXITV1_Broadcast_Days_Viewed
,sum(case when analysis_right ='ITV1 Football' then broadcast_days_viewed else 0 end) 
as FOOTITV1_Broadcast_Days_Viewed
,sum(case when analysis_right ='ITV1 Motor Sport' then broadcast_days_viewed else 0 end) 
as MOTSITV1_Broadcast_Days_Viewed
,sum(case when analysis_right ='ITV1 Rugby' then broadcast_days_viewed else 0 end) 
as RUGITV1_Broadcast_Days_Viewed
,sum(case when analysis_right ='ITV1 Snooker/Pool' then broadcast_days_viewed else 0 end) 
as SNPITV1_Broadcast_Days_Viewed
,sum(case when analysis_right ='ITV1 Unknown' then broadcast_days_viewed else 0 end) 
as UNKITV1_Broadcast_Days_Viewed
,sum(case when analysis_right ='ITV4 Boxing' then broadcast_days_viewed else 0 end) 
as BOXITV4_Broadcast_Days_Viewed
,sum(case when analysis_right ='ITV4 Cricket' then broadcast_days_viewed else 0 end) 
as CRIITV4_Broadcast_Days_Viewed
,sum(case when analysis_right ='ITV4 Darts' then broadcast_days_viewed else 0 end) 
as DARTITV4_Broadcast_Days_Viewed
,sum(case when analysis_right ='ITV4 Extreme' then broadcast_days_viewed else 0 end) 
as EXTITV4_Broadcast_Days_Viewed
,sum(case when analysis_right ='ITV4 Football' then broadcast_days_viewed else 0 end) 
as FOOTITV4_Broadcast_Days_Viewed
,sum(case when analysis_right ='ITV4 Motor Sport' then broadcast_days_viewed else 0 end) 
as MSPITV4_Broadcast_Days_Viewed
,sum(case when analysis_right ='ITV4 Rugby' then broadcast_days_viewed else 0 end) 
as RUGITV4_Broadcast_Days_Viewed
,sum(case when analysis_right ='ITV4 Snooker/Pool' then broadcast_days_viewed else 0 end) 
as SNPITV4_Broadcast_Days_Viewed
,sum(case when analysis_right ='ITV4 Tennis' then broadcast_days_viewed else 0 end) 
as TENITV4_Broadcast_Days_Viewed
,sum(case when analysis_right ='ITV4 Unknown' then broadcast_days_viewed else 0 end) 
as UNKITV4_Broadcast_Days_Viewed
,sum(case when analysis_right ='Ligue 1 - BT Sport' then broadcast_days_viewed else 0 end) 
as L1BTS_Broadcast_Days_Viewed
,sum(case when analysis_right ='Ligue 1 - ESPN' then broadcast_days_viewed else 0 end) 
as L1ESPN_Broadcast_Days_Viewed
,sum(case when analysis_right ='Match of the day - BBC' then broadcast_days_viewed else 0 end) 
as MOTDBBC_Broadcast_Days_Viewed
,sum(case when analysis_right ='Matchroom Output Deal  2010-2012 (Other)- Sky Sports' then broadcast_days_viewed else 0 end) 
as MROSS_Broadcast_Days_Viewed
,sum(case when analysis_right ='Matchroom Output Deal  2010-2012 (Pool)- Sky Sports' then broadcast_days_viewed else 0 end) 
as MRPSS_Broadcast_Days_Viewed
,sum(case when analysis_right ='Matchroom Output Deal  2010-2012 (Snooker)- Sky Sports' then broadcast_days_viewed else 0 end) 
as MRSSS_Broadcast_Days_Viewed
,sum(case when analysis_right ='Moto GP BBC' then broadcast_days_viewed else 0 end) 
as MGPBBC_Broadcast_Days_Viewed
,sum(case when analysis_right ='NBA - Sky Sports' then broadcast_days_viewed else 0 end) 
as NBASS_Broadcast_Days_Viewed
,sum(case when analysis_right ='NFL - BBC' then broadcast_days_viewed else 0 end) 
as NFLBBC_Broadcast_Days_Viewed
,sum(case when analysis_right ='NFL - Channel 4' then broadcast_days_viewed else 0 end) 
as NFLCH4_Broadcast_Days_Viewed
,sum(case when analysis_right ='NFL Seasons 2012-2015 - Sky Sports' then broadcast_days_viewed else 0 end) 
as NFLSS_Broadcast_Days_Viewed
,sum(case when analysis_right ='Northern Ireland Football Association 2012-2014 - Sky Sports' then broadcast_days_viewed else 0 end) 
as NIFSS_Broadcast_Days_Viewed
,sum(case when analysis_right ='Oaks - Channel 4' then broadcast_days_viewed else 0 end) 
as OAKCH4_Broadcast_Days_Viewed
,sum(case when analysis_right ='Other American Football' then broadcast_days_viewed else 0 end) 
as AMOTH_Broadcast_Days_Viewed
,sum(case when analysis_right ='Other Athletics' then broadcast_days_viewed else 0 end) 
as ATHOTH_Broadcast_Days_Viewed
,sum(case when analysis_right ='Other Baseball' then broadcast_days_viewed else 0 end) 
as BASEOTH_Broadcast_Days_Viewed
,sum(case when analysis_right ='Other Basketball' then broadcast_days_viewed else 0 end) 
as BASKOTH_Broadcast_Days_Viewed
,sum(case when analysis_right ='Other Boxing' then broadcast_days_viewed else 0 end) 
as BOXOTH_Broadcast_Days_Viewed
,sum(case when analysis_right ='Other Cricket' then broadcast_days_viewed else 0 end) 
as CRIOTH_Broadcast_Days_Viewed
,sum(case when analysis_right ='Other Darts' then broadcast_days_viewed else 0 end) 
as DARTOTH_Broadcast_Days_Viewed
,sum(case when analysis_right ='Other Equestrian' then broadcast_days_viewed else 0 end) 
as EQUOTH_Broadcast_Days_Viewed
,sum(case when analysis_right ='Other Extreme' then broadcast_days_viewed else 0 end) 
as EXTOTH_Broadcast_Days_Viewed
,sum(case when analysis_right ='Other Fishing' then broadcast_days_viewed else 0 end) 
as FSHOTH_Broadcast_Days_Viewed
,sum(case when analysis_right ='Other Football' then broadcast_days_viewed else 0 end) 
as FOOTOTH_Broadcast_Days_Viewed
,sum(case when analysis_right ='Other Golf' then broadcast_days_viewed else 0 end) 
as GOLFOTH_Broadcast_Days_Viewed
,sum(case when analysis_right ='Other Ice Hockey' then broadcast_days_viewed else 0 end) 
as IHOTH_Broadcast_Days_Viewed
,sum(case when analysis_right ='Other Motor Sport' then broadcast_days_viewed else 0 end) 
as MSPOTH_Broadcast_Days_Viewed
,sum(case when analysis_right ='Other Racing' then broadcast_days_viewed else 0 end) 
as RACOTH_Broadcast_Days_Viewed
,sum(case when analysis_right ='Other Rugby' then broadcast_days_viewed else 0 end) 
as RUGOTH_Broadcast_Days_Viewed
,sum(case when analysis_right ='Other Rugby Internationals - ESPN' then broadcast_days_viewed else 0 end) 
as ORUGESPN_Broadcast_Days_Viewed
,sum(case when analysis_right ='Other Snooker/Pool' then broadcast_days_viewed else 0 end) 
as OTHSNP_Broadcast_Days_Viewed
,sum(case when analysis_right ='Other Tennis' then broadcast_days_viewed else 0 end) 
as OTHTEN_Broadcast_Days_Viewed
,sum(case when analysis_right ='Other Unknown' then broadcast_days_viewed else 0 end) 
as OTHUNK_Broadcast_Days_Viewed
,sum(case when analysis_right ='Other Watersports' then broadcast_days_viewed else 0 end) 
as OTHWAT_Broadcast_Days_Viewed
,sum(case when analysis_right ='Other Wintersports' then broadcast_days_viewed else 0 end) 
as OTHWIN_Broadcast_Days_Viewed
,sum(case when analysis_right ='Other Wrestling' then broadcast_days_viewed else 0 end) 
as OTHWRE_Broadcast_Days_Viewed
,sum(case when analysis_right ='PGA Tour Golf 2010-2017 - Sky Sports' then broadcast_days_viewed else 0 end) 
as PGASS_Broadcast_Days_Viewed
,sum(case when analysis_right ='Premier League - BT Sport' then broadcast_days_viewed else 0 end) 
as PLBTS_Broadcast_Days_Viewed
,sum(case when analysis_right ='Premier League - ESPN' then broadcast_days_viewed else 0 end) 
as PLESPN_Broadcast_Days_Viewed
,sum(case when analysis_right ='Premier League Darts 2010-2013 - Sky Sports' then broadcast_days_viewed else 0 end) 
as PLDSS_Broadcast_Days_Viewed
,sum(case when analysis_right ='Premier League Football - Sky Sports' then broadcast_days_viewed else 0 end) 
as GPLSS_Broadcast_Days_Viewed
,sum(case when analysis_right ='Premier League Football - Sky Sports (Match Choice)' then broadcast_days_viewed else 0 end) 
as PLMCSS_Broadcast_Days_Viewed
,sum(case when analysis_right ='Premier League Football - Sky Sports (MNF)' then broadcast_days_viewed else 0 end) 
as PLMNFSS_Broadcast_Days_Viewed
,sum(case when analysis_right ='Premier League Football - Sky Sports (non Live)' then broadcast_days_viewed else 0 end) 
as PLNLSS_Broadcast_Days_Viewed
,sum(case when analysis_right ='Premier League Football - Sky Sports (other Live)' then broadcast_days_viewed else 0 end) 
as PLOLSS_Broadcast_Days_Viewed
,sum(case when analysis_right ='Premier League Football - Sky Sports (Sat Lunchtime)' then broadcast_days_viewed else 0 end) 
as PLSLSS_Broadcast_Days_Viewed
,sum(case when analysis_right ='Premier League Football - Sky Sports (Sat Night Live)' then broadcast_days_viewed else 0 end) 
as PLSNSS_Broadcast_Days_Viewed
,sum(case when analysis_right ='Premier League Football - Sky Sports (Sun 4pm)' then broadcast_days_viewed else 0 end) 
as PLS4SS_Broadcast_Days_Viewed
,sum(case when analysis_right ='Premier League Football - Sky Sports (Sun Lunchtime)' then broadcast_days_viewed else 0 end) 
as PLSULSS_Broadcast_Days_Viewed
,sum(case when analysis_right ='Premiership Rugby - Sky Sports' then broadcast_days_viewed else 0 end) 
as PRUSS_Broadcast_Days_Viewed
,sum(case when analysis_right ='Republic Of Ireland FA - 2010/11 To 2013/14 - Sky Sports' then broadcast_days_viewed else 0 end) 
as ROISS_Broadcast_Days_Viewed
,sum(case when analysis_right ='Royal Ascot - Channel 4' then broadcast_days_viewed else 0 end) 
as RASCH4_Broadcast_Days_Viewed
,sum(case when analysis_right ='Rugby Internationals (England) - BBC' then broadcast_days_viewed else 0 end) 
as RIEBBC_Broadcast_Days_Viewed
,sum(case when analysis_right ='Rugby Internationals (Ireland) - BBC' then broadcast_days_viewed else 0 end) 
as RIIBBC_Broadcast_Days_Viewed
,sum(case when analysis_right ='Rugby Internationals (Scotland) - BBC' then broadcast_days_viewed else 0 end) 
as RISBBC_Broadcast_Days_Viewed
,sum(case when analysis_right ='Rugby Internationals (Wales) - BBC' then broadcast_days_viewed else 0 end) 
as RIWBBC_Broadcast_Days_Viewed
,sum(case when analysis_right ='Rugby League  Challenge Cup- BBC' then broadcast_days_viewed else 0 end) 
as RLCCBBC_Broadcast_Days_Viewed
,sum(case when analysis_right ='Rugby League - Sky Sports' then broadcast_days_viewed else 0 end) 
as RLGSS_Broadcast_Days_Viewed
,sum(case when analysis_right ='Rugby League  World Cup- BBC' then broadcast_days_viewed else 0 end) 
as RLWCBBC_Broadcast_Days_Viewed
,sum(case when analysis_right ='Sanzar Rugby Union 2011 - 2015 - Sky Sports' then broadcast_days_viewed else 0 end) 
as SARUSS_Broadcast_Days_Viewed
,sum(case when analysis_right ='Scottish FA Deal - 2010/11 To 2013/14 - Sky Sports' then broadcast_days_viewed else 0 end) 
as SFASS_Broadcast_Days_Viewed
,sum(case when analysis_right ='Serie A - BT Sport' then broadcast_days_viewed else 0 end) 
as SABTS_Broadcast_Days_Viewed
,sum(case when analysis_right ='Serie A - ESPN' then broadcast_days_viewed else 0 end) 
as SAESPN_Broadcast_Days_Viewed
,sum(case when analysis_right ='SFL - ESPN' then broadcast_days_viewed else 0 end) 
as SFLESPN_Broadcast_Days_Viewed
,sum(case when analysis_right ='Six Nations - BBC' then broadcast_days_viewed else 0 end) 
as SNRBBC_Broadcast_Days_Viewed
,sum(case when analysis_right ='Sky 1 and Sky 2 Boxing' then broadcast_days_viewed else 0 end) 
as BOXS12_Broadcast_Days_Viewed
,sum(case when analysis_right ='Sky 1 and Sky 2 Football' then broadcast_days_viewed else 0 end) 
as FOOTS12_Broadcast_Days_Viewed
,sum(case when analysis_right ='Sky 1 and Sky 2 Motor Sport' then broadcast_days_viewed else 0 end) 
as MSPS12_Broadcast_Days_Viewed
,sum(case when analysis_right ='Sky 1 and Sky 2 Unknown' then broadcast_days_viewed else 0 end) 
as UNKS12_Broadcast_Days_Viewed
,sum(case when analysis_right ='Sky 1 and Sky 2 Wrestling' then broadcast_days_viewed else 0 end) 
as WRES12_Broadcast_Days_Viewed
,sum(case when analysis_right ='Sky Sports American Football' then broadcast_days_viewed else 0 end) 
as AMSS_Broadcast_Days_Viewed
,sum(case when analysis_right ='Sky Sports Athletics' then broadcast_days_viewed else 0 end) 
as ATHSS_Broadcast_Days_Viewed
,sum(case when analysis_right ='Sky Sports Baseball' then broadcast_days_viewed else 0 end) 
as BASESS_Broadcast_Days_Viewed
,sum(case when analysis_right ='Sky Sports Basketball' then broadcast_days_viewed else 0 end) 
as BASKSS_Broadcast_Days_Viewed
,sum(case when analysis_right ='Sky Sports Boxing' then broadcast_days_viewed else 0 end) 
as BOXSS_Broadcast_Days_Viewed
,sum(case when analysis_right ='Sky Sports Cricket' then broadcast_days_viewed else 0 end) 
as CRISS_Broadcast_Days_Viewed
,sum(case when analysis_right ='Sky Sports Darts' then broadcast_days_viewed else 0 end) 
as DARTSS_Broadcast_Days_Viewed
,sum(case when analysis_right ='Sky Sports Equestrian' then broadcast_days_viewed else 0 end) 
as EQUSS_Broadcast_Days_Viewed
,sum(case when analysis_right ='Sky Sports Extreme' then broadcast_days_viewed else 0 end) 
as EXTSS_Broadcast_Days_Viewed
,sum(case when analysis_right ='Sky Sports Fishing' then broadcast_days_viewed else 0 end) 
as FISHSS_Broadcast_Days_Viewed
,sum(case when analysis_right ='Sky Sports Football' then broadcast_days_viewed else 0 end) 
as FOOTSS_Broadcast_Days_Viewed
,sum(case when analysis_right ='Sky Sports Golf' then broadcast_days_viewed else 0 end) 
as GOLFSS_Broadcast_Days_Viewed
,sum(case when analysis_right ='Sky Sports Ice Hockey' then broadcast_days_viewed else 0 end) 
as IHSS_Broadcast_Days_Viewed
,sum(case when analysis_right ='Sky Sports Motor Sport' then broadcast_days_viewed else 0 end) 
as MSPSS_Broadcast_Days_Viewed
,sum(case when analysis_right ='Sky Sports Racing' then broadcast_days_viewed else 0 end) 
as RACSS_Broadcast_Days_Viewed
,sum(case when analysis_right ='Sky Sports Rugby' then broadcast_days_viewed else 0 end) 
as RUGSS_Broadcast_Days_Viewed
,sum(case when analysis_right ='Sky Sports Snooker/Pool' then broadcast_days_viewed else 0 end) 
as SNPSS_Broadcast_Days_Viewed
,sum(case when analysis_right ='Sky Sports Tennis' then broadcast_days_viewed else 0 end) 
as TENSS_Broadcast_Days_Viewed
,sum(case when analysis_right ='Sky Sports Unknown' then broadcast_days_viewed else 0 end) 
as UNKSS_Broadcast_Days_Viewed
,sum(case when analysis_right ='Sky Sports Watersports' then broadcast_days_viewed else 0 end) 
as WATSS_Broadcast_Days_Viewed
,sum(case when analysis_right ='Sky Sports Wintersports' then broadcast_days_viewed else 0 end) 
as WINSS_Broadcast_Days_Viewed
,sum(case when analysis_right ='Sky Sports Wrestling' then broadcast_days_viewed else 0 end) 
as WRESS_Broadcast_Days_Viewed
,sum(case when analysis_right ='Solheim Cup 2013 Ladies Golf - Sky Sports' then broadcast_days_viewed else 0 end) 
as SOLSS_Broadcast_Days_Viewed
,sum(case when analysis_right ='South African Home Cricket  2012-2020 Sky Sports' then broadcast_days_viewed else 0 end) 
as SACSS_Broadcast_Days_Viewed
,sum(case when analysis_right ='Spanish Football 2012/13 To 14/15 - Sky Sports' then broadcast_days_viewed else 0 end) 
as SPFSS_Broadcast_Days_Viewed
,sum(case when analysis_right ='SPFL - BT Sport' then broadcast_days_viewed else 0 end) 
as SPFLBTS_Broadcast_Days_Viewed
,sum(case when analysis_right ='SPL - ESPN' then broadcast_days_viewed else 0 end) 
as SPLESPN_Broadcast_Days_Viewed
,sum(case when analysis_right ='SPL - Sky Sports' then broadcast_days_viewed else 0 end) 
as SPLSS_Broadcast_Days_Viewed
,sum(case when analysis_right ='Sportfive World Cup 2014 Qualifers - Sky Sports' then broadcast_days_viewed else 0 end) 
as SP5SS_Broadcast_Days_Viewed
,sum(case when analysis_right ='The boat race - BBC' then broadcast_days_viewed else 0 end) 
as BTRBBC_Broadcast_Days_Viewed
,sum(case when analysis_right ='The football league show - BBC' then broadcast_days_viewed else 0 end) 
as FLSBBC_Broadcast_Days_Viewed
,sum(case when analysis_right ='The Masters Golf - BBC' then broadcast_days_viewed else 0 end) 
as MGBBC_Broadcast_Days_Viewed
,sum(case when analysis_right ='TNA Wrestling Challenge' then broadcast_days_viewed else 0 end) 
as TNACHA_Broadcast_Days_Viewed
,sum(case when analysis_right ='Tour de France - Eurosport' then broadcast_days_viewed else 0 end) 
as TDFEUR_Broadcast_Days_Viewed
,sum(case when analysis_right ='Tour de France - ITV' then broadcast_days_viewed else 0 end) 
as TDFITV_Broadcast_Days_Viewed
,sum(case when analysis_right ='U S Masters Golf 2011-2014 Sky Sports' then broadcast_days_viewed else 0 end) 
as USMGSS_Broadcast_Days_Viewed
,sum(case when analysis_right ='U.S Open Tennis 2013-2015 Sky Sports' then broadcast_days_viewed else 0 end) 
as USOTSS_Broadcast_Days_Viewed
,sum(case when analysis_right ='U.S Open/Womens/Seniors/Amateur Golf Sky Sports' then broadcast_days_viewed else 0 end) 
as USOGSS_Broadcast_Days_Viewed
,sum(case when analysis_right ='UEFA Champions League -  Sky Sports' then broadcast_days_viewed else 0 end) 
as CLASS_Broadcast_Days_Viewed
,sum(case when analysis_right ='UEFA Champions League -  Sky Sports (non Live)' then broadcast_days_viewed else 0 end) 
as CLNSS_Broadcast_Days_Viewed
,sum(case when analysis_right ='UEFA Champions League -  Sky Sports (other Live)' then broadcast_days_viewed else 0 end) 
as CLOSS_Broadcast_Days_Viewed
,sum(case when analysis_right ='UEFA Champions League -  Sky Sports (Tue)' then broadcast_days_viewed else 0 end) 
as CLTSS_Broadcast_Days_Viewed
,sum(case when analysis_right ='UEFA Champions League -  Sky Sports (Wed)' then broadcast_days_viewed else 0 end) 
as CLWSS_Broadcast_Days_Viewed
,sum(case when analysis_right ='US Open Tennis - Eurosport' then broadcast_days_viewed else 0 end) 
as USOTEUR_Broadcast_Days_Viewed
,sum(case when analysis_right ='USA Football - BT Sport' then broadcast_days_viewed else 0 end) 
as USFBTS_Broadcast_Days_Viewed
,sum(case when analysis_right ='USPGA Championship (2007-2016) Sky Sports' then broadcast_days_viewed else 0 end) 
as USPGASS_Broadcast_Days_Viewed
,sum(case when analysis_right ='WCQ - ESPN' then broadcast_days_viewed else 0 end) 
as WCQESPN_Broadcast_Days_Viewed
,sum(case when analysis_right ='Welsh Home International Football 2012-2014 Sky Sports' then broadcast_days_viewed else 0 end) 
as WIFSS_Broadcast_Days_Viewed
,sum(case when analysis_right ='West Indies Home Cricket 2013-2020 Sky Sports' then broadcast_days_viewed else 0 end) 
as WICSS_Broadcast_Days_Viewed
,sum(case when analysis_right ='Wimbledon - BBC' then broadcast_days_viewed else 0 end) 
as WIMBBC_Broadcast_Days_Viewed
,sum(case when analysis_right ='Womens Cricket - I C C Cricket Deal  2012-2015 Sky Sports' then broadcast_days_viewed else 0 end) 
as WICCSS_Broadcast_Days_Viewed
,sum(case when analysis_right ='World Athletics Championship - More 4' then broadcast_days_viewed else 0 end) 
as WACMR4_Broadcast_Days_Viewed
,sum(case when analysis_right ='World Club Championship - BBC' then broadcast_days_viewed else 0 end) 
as WCLBBBC_Broadcast_Days_Viewed
,sum(case when analysis_right ='World Cup Qualifiers - BT Sport' then broadcast_days_viewed else 0 end) 
as WCQBTS_Broadcast_Days_Viewed
,sum(case when analysis_right ='World Darts Championship 2009-2012 Sky Sports' then broadcast_days_viewed else 0 end) 
as WDCSS_Broadcast_Days_Viewed
,sum(case when analysis_right ='World snooker championship - BBC' then broadcast_days_viewed else 0 end) 
as WSCBBC_Broadcast_Days_Viewed
,sum(case when analysis_right ='WWE Sky 1 and 2' then broadcast_days_viewed else 0 end) 
as WWES12_Broadcast_Days_Viewed
,sum(case when analysis_right ='WWE Sky Sports' then broadcast_days_viewed else 0 end) 
as WWESS_Broadcast_Days_Viewed
,sum(case when analysis_right ='Africa Cup of Nations - Eurosport' then total_duration_viewed_seconds else 0 end) 
as AFCEUR_Total_Seconds_Viewed
,sum(case when analysis_right ='Africa Cup of Nations - ITV' then total_duration_viewed_seconds else 0 end) 
as AFCITV_Total_Seconds_Viewed
,sum(case when analysis_right ='Americas Cup - BBC' then total_duration_viewed_seconds else 0 end) 
as AMCBBC_Total_Seconds_Viewed
,sum(case when analysis_right ='Asian Tour Golf 2012-2013 Sky Sports' then total_duration_viewed_seconds else 0 end) 
as ATGSS_Total_Seconds_Viewed
,sum(case when analysis_right ='ATP Tennis Masters Series  2011-2013 Sky Sports' then total_duration_viewed_seconds else 0 end) 
as ATPSS_Total_Seconds_Viewed
,sum(case when analysis_right ='Australia Home Cricket  2012-2016 Sky Sports' then total_duration_viewed_seconds else 0 end) 
as AHCSS_Total_Seconds_Viewed
,sum(case when analysis_right ='Australian Football - BT Sport' then total_duration_viewed_seconds else 0 end) 
as AUFBTS_Total_Seconds_Viewed
,sum(case when analysis_right ='Australian Open Tennis - BBC' then total_duration_viewed_seconds else 0 end) 
as AOTBBC_Total_Seconds_Viewed
,sum(case when analysis_right ='Australian Open Tennis - Eurosport' then total_duration_viewed_seconds else 0 end) 
as AOTEUR_Total_Seconds_Viewed
,sum(case when analysis_right ='Aviva Premiership - ESPN' then total_duration_viewed_seconds else 0 end) 
as AVPSS_Total_Seconds_Viewed
,sum(case when analysis_right ='BBC American Football' then total_duration_viewed_seconds else 0 end) 
as AFBBC_Total_Seconds_Viewed
,sum(case when analysis_right ='BBC Athletics' then total_duration_viewed_seconds else 0 end) 
as ATHBBC_Total_Seconds_Viewed
,sum(case when analysis_right ='BBC Boxing' then total_duration_viewed_seconds else 0 end) 
as BOXBBC_Total_Seconds_Viewed
,sum(case when analysis_right ='BBC Darts' then total_duration_viewed_seconds else 0 end) 
as DRTBBC_Total_Seconds_Viewed
,sum(case when analysis_right ='BBC Equestrian' then total_duration_viewed_seconds else 0 end) 
as EQUBBC_Total_Seconds_Viewed
,sum(case when analysis_right ='BBC Football' then total_duration_viewed_seconds else 0 end) 
as FOOTBBC_Total_Seconds_Viewed
,sum(case when analysis_right ='BBC Golf' then total_duration_viewed_seconds else 0 end) 
as GOLFBBC_Total_Seconds_Viewed
,sum(case when analysis_right ='BBC Motor Sport' then total_duration_viewed_seconds else 0 end) 
as MSPBBC_Total_Seconds_Viewed
,sum(case when analysis_right ='BBC Rugby' then total_duration_viewed_seconds else 0 end) 
as RUGBBC_Total_Seconds_Viewed
,sum(case when analysis_right ='BBC Snooker/Pool' then total_duration_viewed_seconds else 0 end) 
as SNPBBC_Total_Seconds_Viewed
,sum(case when analysis_right ='BBC Tennis' then total_duration_viewed_seconds else 0 end) 
as TENBBC_Total_Seconds_Viewed
,sum(case when analysis_right ='BBC Unknown' then total_duration_viewed_seconds else 0 end) 
as UNKBBC_Total_Seconds_Viewed
,sum(case when analysis_right ='BBC Watersports' then total_duration_viewed_seconds else 0 end) 
as WATBBC_Total_Seconds_Viewed
,sum(case when analysis_right ='BBC Wintersports' then total_duration_viewed_seconds else 0 end) 
as WINBBC_Total_Seconds_Viewed
,sum(case when analysis_right ='Boxing  - Channel 5' then total_duration_viewed_seconds else 0 end) 
as BOXCH5_Total_Seconds_Viewed
,sum(case when analysis_right ='Boxing - Matchroom 2012-2014 Sky Sports' then total_duration_viewed_seconds else 0 end) 
as BOXMSS_Total_Seconds_Viewed
,sum(case when analysis_right ='Brazil Football - BT Sport' then total_duration_viewed_seconds else 0 end) 
as BFTBTS_Total_Seconds_Viewed
,sum(case when analysis_right ='British & Irish Lions 2013 - Sky Sports' then total_duration_viewed_seconds else 0 end) 
as BILSS_Total_Seconds_Viewed
,sum(case when analysis_right ='British Open Golf - BBC' then total_duration_viewed_seconds else 0 end) 
as BOGSS_Total_Seconds_Viewed
,sum(case when analysis_right ='BT Sport American Football' then total_duration_viewed_seconds else 0 end) 
as AFBTS_Total_Seconds_Viewed
,sum(case when analysis_right ='BT Sport Athletics' then total_duration_viewed_seconds else 0 end) 
as ATHBTS_Total_Seconds_Viewed
,sum(case when analysis_right ='BT Sport Baseball' then total_duration_viewed_seconds else 0 end) 
as BASEBTS_Total_Seconds_Viewed
,sum(case when analysis_right ='BT Sport Basketball' then total_duration_viewed_seconds else 0 end) 
as BASKBTS_Total_Seconds_Viewed
,sum(case when analysis_right ='BT Sport Boxing' then total_duration_viewed_seconds else 0 end) 
as BOXBTS_Total_Seconds_Viewed
,sum(case when analysis_right ='BT Sport Cricket' then total_duration_viewed_seconds else 0 end) 
as CRIBTS_Total_Seconds_Viewed
,sum(case when analysis_right ='BT Sport Equestrian' then total_duration_viewed_seconds else 0 end) 
as EQUBTS_Total_Seconds_Viewed
,sum(case when analysis_right ='BT Sport Extreme' then total_duration_viewed_seconds else 0 end) 
as EXTBTS_Total_Seconds_Viewed
,sum(case when analysis_right ='BT Sport Football' then total_duration_viewed_seconds else 0 end) 
as FOOTBTS_Total_Seconds_Viewed
,sum(case when analysis_right ='BT Sport Motor Sport' then total_duration_viewed_seconds else 0 end) 
as MSPBTS_Total_Seconds_Viewed
,sum(case when analysis_right ='BT Sport Rugby' then total_duration_viewed_seconds else 0 end) 
as RUGBTS_Total_Seconds_Viewed
,sum(case when analysis_right ='BT Sport Tennis' then total_duration_viewed_seconds else 0 end) 
as TENBTS_Total_Seconds_Viewed
,sum(case when analysis_right ='BT Sport Unknown' then total_duration_viewed_seconds else 0 end) 
as UNKBTS_Total_Seconds_Viewed
,sum(case when analysis_right ='BT Sport Wintersports' then total_duration_viewed_seconds else 0 end) 
as WINBTS_Total_Seconds_Viewed
,sum(case when analysis_right ='Bundesliga - BT Sport' then total_duration_viewed_seconds else 0 end) 
as BUNBTS_Total_Seconds_Viewed
,sum(case when analysis_right ='Bundesliga- ESPN' then total_duration_viewed_seconds else 0 end) 
as BUNESPN_Total_Seconds_Viewed
,sum(case when analysis_right ='Challenge Darts' then total_duration_viewed_seconds else 0 end) 
as DRTCHA_Total_Seconds_Viewed
,sum(case when analysis_right ='Challenge Extreme' then total_duration_viewed_seconds else 0 end) 
as EXTCHA_Total_Seconds_Viewed
,sum(case when analysis_right ='Challenge Unknown' then total_duration_viewed_seconds else 0 end) 
as UNKCHA_Total_Seconds_Viewed
,sum(case when analysis_right ='Challenge Wrestling' then total_duration_viewed_seconds else 0 end) 
as WRECHA_Total_Seconds_Viewed
,sum(case when analysis_right ='Champions League - ITV' then total_duration_viewed_seconds else 0 end) 
as CHLITV_Total_Seconds_Viewed
,sum(case when analysis_right ='Champions Trophy - I C C Cricket Deal  2012-2015 Sky Sports' then total_duration_viewed_seconds else 0 end) 
as ICCSS_Total_Seconds_Viewed
,sum(case when analysis_right ='Channel 4 American Football' then total_duration_viewed_seconds else 0 end) 
as AMCH4_Total_Seconds_Viewed
,sum(case when analysis_right ='Channel 4 Athletics' then total_duration_viewed_seconds else 0 end) 
as ATHCH4_Total_Seconds_Viewed
,sum(case when analysis_right ='Channel 4 Boxing' then total_duration_viewed_seconds else 0 end) 
as BOXCH4_Total_Seconds_Viewed
,sum(case when analysis_right ='Channel 4 Equestrian' then total_duration_viewed_seconds else 0 end) 
as EQUCH4_Total_Seconds_Viewed
,sum(case when analysis_right ='Channel 4 Extreme' then total_duration_viewed_seconds else 0 end) 
as EXTCH4_Total_Seconds_Viewed
,sum(case when analysis_right ='Channel 4 Football' then total_duration_viewed_seconds else 0 end) 
as FOOTCH4_Total_Seconds_Viewed
,sum(case when analysis_right ='Channel 4 Motor Sport' then total_duration_viewed_seconds else 0 end) 
as MSPCH4_Total_Seconds_Viewed
,sum(case when analysis_right ='Channel 4 Racing' then total_duration_viewed_seconds else 0 end) 
as RACCH4_Total_Seconds_Viewed
,sum(case when analysis_right ='Channel 4 Unknown' then total_duration_viewed_seconds else 0 end) 
as UNKCH4_Total_Seconds_Viewed
,sum(case when analysis_right ='Channel 4 Watersports' then total_duration_viewed_seconds else 0 end) 
as WATCH4_Total_Seconds_Viewed
,sum(case when analysis_right ='Channel 4 Wintersports' then total_duration_viewed_seconds else 0 end) 
as WINCH4_Total_Seconds_Viewed
,sum(case when analysis_right ='Channel 5 Athletics' then total_duration_viewed_seconds else 0 end) 
as ATHCH5_Total_Seconds_Viewed
,sum(case when analysis_right ='Channel 5 Boxing' then total_duration_viewed_seconds else 0 end) 
as BOXOCH5_Total_Seconds_Viewed
,sum(case when analysis_right ='Channel 5 Cricket' then total_duration_viewed_seconds else 0 end) 
as CRICH5_Total_Seconds_Viewed
,sum(case when analysis_right ='Channel 5 Motor Sport' then total_duration_viewed_seconds else 0 end) 
as MSPCH5_Total_Seconds_Viewed
,sum(case when analysis_right ='Channel 5 Unknown' then total_duration_viewed_seconds else 0 end) 
as UNKCH5_Total_Seconds_Viewed
,sum(case when analysis_right ='Channel 5 Wrestling' then total_duration_viewed_seconds else 0 end) 
as WRECH5_Total_Seconds_Viewed
,sum(case when analysis_right ='Cheltenham Festival - Channel 4' then total_duration_viewed_seconds else 0 end) 
as CHELCH4_Total_Seconds_Viewed
,sum(case when analysis_right ='Community Shield - ITV' then total_duration_viewed_seconds else 0 end) 
as CMSITV_Total_Seconds_Viewed
,sum(case when analysis_right ='Confederations Cup - BBC' then total_duration_viewed_seconds else 0 end) 
as CONCBBC_Total_Seconds_Viewed
,sum(case when analysis_right ='Conference - BT Sport' then total_duration_viewed_seconds else 0 end) 
as CONFBTS_Total_Seconds_Viewed
,sum(case when analysis_right ='Cycling - La Vuelta ITV' then total_duration_viewed_seconds else 0 end) 
as CLVITV_Total_Seconds_Viewed
,sum(case when analysis_right ='Cycling - U C I World Tour Sky Sports' then total_duration_viewed_seconds else 0 end) 
as CUCISS_Total_Seconds_Viewed
,sum(case when analysis_right ='Cycling Tour of Britain - Eurosport' then total_duration_viewed_seconds else 0 end) 
as CTBEUR_Total_Seconds_Viewed
,sum(case when analysis_right ='Cycling: tour of britain ITV4' then total_duration_viewed_seconds else 0 end) 
as CTCITV_Total_Seconds_Viewed
,sum(case when analysis_right ='Derby - Channel 4' then total_duration_viewed_seconds else 0 end) 
as DERCH4_Total_Seconds_Viewed
,sum(case when analysis_right ='ECB (highlights) - Channel 5' then total_duration_viewed_seconds else 0 end) 
as ECBHCH5_Total_Seconds_Viewed
,sum(case when analysis_right ='ECB Cricket Sky Sports' then total_duration_viewed_seconds else 0 end) 
as GECRSS_Total_Seconds_Viewed
,sum(case when analysis_right ='ECB non-Test Cricket Sky Sports' then total_duration_viewed_seconds else 0 end) 
as ECBNSS_Total_Seconds_Viewed
,sum(case when analysis_right ='ECB Test Cricket Sky Sports' then total_duration_viewed_seconds else 0 end) 
as ECBTSS_Total_Seconds_Viewed
,sum(case when analysis_right ='England Football Internationals - ITV' then total_duration_viewed_seconds else 0 end) 
as GENGITV_Total_Seconds_Viewed
,sum(case when analysis_right ='England Friendlies (Football) - ITV' then total_duration_viewed_seconds else 0 end) 
as EFRITV_Total_Seconds_Viewed
,sum(case when analysis_right ='England Rugby Internationals 2010-2015 - Sky Sports' then total_duration_viewed_seconds else 0 end) 
as ENRSS_Total_Seconds_Viewed
,sum(case when analysis_right ='England World Cup Qualifying (Away) - ITV' then total_duration_viewed_seconds else 0 end) 
as EWQAITV_Total_Seconds_Viewed
,sum(case when analysis_right ='England World Cup Qualifying (Home) - ITV' then total_duration_viewed_seconds else 0 end) 
as EWQHITV_Total_Seconds_Viewed
,sum(case when analysis_right ='ESPN American Football' then total_duration_viewed_seconds else 0 end) 
as AMESPN_Total_Seconds_Viewed
,sum(case when analysis_right ='ESPN Athletics' then total_duration_viewed_seconds else 0 end) 
as ATHESPN_Total_Seconds_Viewed
,sum(case when analysis_right ='ESPN Baseball' then total_duration_viewed_seconds else 0 end) 
as BASEESPN_Total_Seconds_Viewed
,sum(case when analysis_right ='ESPN Basketball' then total_duration_viewed_seconds else 0 end) 
as BASKESPN_Total_Seconds_Viewed
,sum(case when analysis_right ='ESPN Boxing' then total_duration_viewed_seconds else 0 end) 
as BOXESPN_Total_Seconds_Viewed
,sum(case when analysis_right ='ESPN Cricket' then total_duration_viewed_seconds else 0 end) 
as CRIESPN_Total_Seconds_Viewed
,sum(case when analysis_right ='ESPN Darts' then total_duration_viewed_seconds else 0 end) 
as DARTESPN_Total_Seconds_Viewed
,sum(case when analysis_right ='ESPN Equestrian' then total_duration_viewed_seconds else 0 end) 
as EQUESPN_Total_Seconds_Viewed
,sum(case when analysis_right ='ESPN Extreme' then total_duration_viewed_seconds else 0 end) 
as EXTESPN_Total_Seconds_Viewed
,sum(case when analysis_right ='ESPN Football' then total_duration_viewed_seconds else 0 end) 
as FOOTESPN_Total_Seconds_Viewed
,sum(case when analysis_right ='ESPN Golf' then total_duration_viewed_seconds else 0 end) 
as GOLFESPN_Total_Seconds_Viewed
,sum(case when analysis_right ='ESPN Ice Hockey' then total_duration_viewed_seconds else 0 end) 
as IHESPN_Total_Seconds_Viewed
,sum(case when analysis_right ='ESPN Motor Sport' then total_duration_viewed_seconds else 0 end) 
as MSPESPN_Total_Seconds_Viewed
,sum(case when analysis_right ='ESPN Racing' then total_duration_viewed_seconds else 0 end) 
as RACESPN_Total_Seconds_Viewed
,sum(case when analysis_right ='ESPN Rugby' then total_duration_viewed_seconds else 0 end) 
as RUGESPN_Total_Seconds_Viewed
,sum(case when analysis_right ='ESPN Snooker/Pool' then total_duration_viewed_seconds else 0 end) 
as SNPESPN_Total_Seconds_Viewed
,sum(case when analysis_right ='ESPN Tennis' then total_duration_viewed_seconds else 0 end) 
as TENESPN_Total_Seconds_Viewed
,sum(case when analysis_right ='ESPN Unknown' then total_duration_viewed_seconds else 0 end) 
as UNKESPN_Total_Seconds_Viewed
,sum(case when analysis_right ='ESPN Watersports' then total_duration_viewed_seconds else 0 end) 
as WATESPN_Total_Seconds_Viewed
,sum(case when analysis_right ='ESPN Wintersports' then total_duration_viewed_seconds else 0 end) 
as WINESPN_Total_Seconds_Viewed
,sum(case when analysis_right ='ESPN Wrestling' then total_duration_viewed_seconds else 0 end) 
as WREESPN_Total_Seconds_Viewed
,sum(case when analysis_right ='Europa League - BT Sport' then total_duration_viewed_seconds else 0 end) 
as ELBTSP_Total_Seconds_Viewed
,sum(case when analysis_right ='Europa League - ESPN' then total_duration_viewed_seconds else 0 end) 
as ELESPN_Total_Seconds_Viewed
,sum(case when analysis_right ='Europa League - ITV' then total_duration_viewed_seconds else 0 end) 
as ELITV_Total_Seconds_Viewed
,sum(case when analysis_right ='European Tour Golf - Sky Sports' then total_duration_viewed_seconds else 0 end) 
as ETGSS_Total_Seconds_Viewed
,sum(case when analysis_right ='Eurosport American Football' then total_duration_viewed_seconds else 0 end) 
as AMEUR_Total_Seconds_Viewed
,sum(case when analysis_right ='Eurosport Athletics' then total_duration_viewed_seconds else 0 end) 
as ATHEUR_Total_Seconds_Viewed
,sum(case when analysis_right ='Eurosport Baseball' then total_duration_viewed_seconds else 0 end) 
as BASEEUR_Total_Seconds_Viewed
,sum(case when analysis_right ='Eurosport Basketball' then total_duration_viewed_seconds else 0 end) 
as BASKEUR_Total_Seconds_Viewed
,sum(case when analysis_right ='Eurosport Boxing' then total_duration_viewed_seconds else 0 end) 
as BOXEUR_Total_Seconds_Viewed
,sum(case when analysis_right ='Eurosport Cricket' then total_duration_viewed_seconds else 0 end) 
as CRIEUR_Total_Seconds_Viewed
,sum(case when analysis_right ='Eurosport Darts' then total_duration_viewed_seconds else 0 end) 
as DARTEUR_Total_Seconds_Viewed
,sum(case when analysis_right ='Eurosport Equestrian' then total_duration_viewed_seconds else 0 end) 
as EQUEUR_Total_Seconds_Viewed
,sum(case when analysis_right ='Eurosport Extreme' then total_duration_viewed_seconds else 0 end) 
as EXTEUR_Total_Seconds_Viewed
,sum(case when analysis_right ='Eurosport Football' then total_duration_viewed_seconds else 0 end) 
as FOOTEUR_Total_Seconds_Viewed
,sum(case when analysis_right ='Eurosport Golf' then total_duration_viewed_seconds else 0 end) 
as GOLFEUR_Total_Seconds_Viewed
,sum(case when analysis_right ='Eurosport Ice Hockey' then total_duration_viewed_seconds else 0 end) 
as IHEUR_Total_Seconds_Viewed
,sum(case when analysis_right ='Eurosport Motor Sport' then total_duration_viewed_seconds else 0 end) 
as MSPEUR_Total_Seconds_Viewed
,sum(case when analysis_right ='Eurosport Racing' then total_duration_viewed_seconds else 0 end) 
as RACEUR_Total_Seconds_Viewed
,sum(case when analysis_right ='Eurosport Snooker/Pool' then total_duration_viewed_seconds else 0 end) 
as SNPEUR_Total_Seconds_Viewed
,sum(case when analysis_right ='Eurosport Tennis' then total_duration_viewed_seconds else 0 end) 
as TENEUR_Total_Seconds_Viewed
,sum(case when analysis_right ='Eurosport Unknown' then total_duration_viewed_seconds else 0 end) 
as UNKEUR_Total_Seconds_Viewed
,sum(case when analysis_right ='Eurosport Watersports' then total_duration_viewed_seconds else 0 end) 
as WATEUR_Total_Seconds_Viewed
,sum(case when analysis_right ='Eurosport Wintersports' then total_duration_viewed_seconds else 0 end) 
as WINEUR_Total_Seconds_Viewed
,sum(case when analysis_right ='F1 - BBC' then total_duration_viewed_seconds else 0 end) 
as GF1BBC_Total_Seconds_Viewed
,sum(case when analysis_right ='F1 - Sky Sports' then total_duration_viewed_seconds else 0 end) 
as GF1SS_Total_Seconds_Viewed
,sum(case when analysis_right ='F1 (non-Live)- BBC' then total_duration_viewed_seconds else 0 end) 
as F1NBBC_Total_Seconds_Viewed
,sum(case when analysis_right ='F1 (Practice Live)- BBC' then total_duration_viewed_seconds else 0 end) 
as F1PBBC_Total_Seconds_Viewed
,sum(case when analysis_right ='F1 (Qualifying Live)- BBC' then total_duration_viewed_seconds else 0 end) 
as F1QBBC_Total_Seconds_Viewed
,sum(case when analysis_right ='F1 (Race Live)- BBC' then total_duration_viewed_seconds else 0 end) 
as F1RBBC_Total_Seconds_Viewed
,sum(case when analysis_right ='FA Cup - ESPN' then total_duration_viewed_seconds else 0 end) 
as FACESPN_Total_Seconds_Viewed
,sum(case when analysis_right ='FA Cup - ITV' then total_duration_viewed_seconds else 0 end) 
as FACITV_Total_Seconds_Viewed
,sum(case when analysis_right ='Football League - Seasons 2012/13 To 14/15 Sky Sports - Capital 1 Cup' then total_duration_viewed_seconds else 0 end) 
as FLCCSS_Total_Seconds_Viewed
,sum(case when analysis_right ='Football League - Seasons 2012/13 To 14/15 Sky Sports - others' then total_duration_viewed_seconds else 0 end) 
as FLOTSS_Total_Seconds_Viewed
,sum(case when analysis_right ='Formula One 2012-2018 - (non-Live) Sky Sports' then total_duration_viewed_seconds else 0 end) 
as F1NSS_Total_Seconds_Viewed
,sum(case when analysis_right ='Formula One 2012-2018 - (Practice Live) Sky Sports' then total_duration_viewed_seconds else 0 end) 
as F1PSS_Total_Seconds_Viewed
,sum(case when analysis_right ='Formula One 2012-2018 - (Qualifying Live) Sky Sports' then total_duration_viewed_seconds else 0 end) 
as F1QSS_Total_Seconds_Viewed
,sum(case when analysis_right ='Formula One 2012-2018 - (Race Live) Sky Sports' then total_duration_viewed_seconds else 0 end) 
as F1RSS_Total_Seconds_Viewed
,sum(case when analysis_right ='French Open Tennis - Eurosport' then total_duration_viewed_seconds else 0 end) 
as FOTEUR_Total_Seconds_Viewed
,sum(case when analysis_right ='French Open Tennis - ITV' then total_duration_viewed_seconds else 0 end) 
as FOTITV_Total_Seconds_Viewed
,sum(case when analysis_right ='Grand National - Channel 4' then total_duration_viewed_seconds else 0 end) 
as GDNCH4_Total_Seconds_Viewed
,sum(case when analysis_right ='Heineken Cup 2010/11 To 2013/14- Sky Sports' then total_duration_viewed_seconds else 0 end) 
as HECSS_Total_Seconds_Viewed
,sum(case when analysis_right ='I R B Sevens 2008/09 -2014/15 - Sky Sports' then total_duration_viewed_seconds else 0 end) 
as IRBSS_Total_Seconds_Viewed
,sum(case when analysis_right ='IAAF World Athletics Championship - Eurosport' then total_duration_viewed_seconds else 0 end) 
as WACEUR_Total_Seconds_Viewed
,sum(case when analysis_right ='India Home Cricket 2012-2018 Sky Sports' then total_duration_viewed_seconds else 0 end) 
as IHCSS_Total_Seconds_Viewed
,sum(case when analysis_right ='India Premier League - ITV' then total_duration_viewed_seconds else 0 end) 
as IPLITV_Total_Seconds_Viewed
,sum(case when analysis_right ='International Freindlies - ESPN' then total_duration_viewed_seconds else 0 end) 
as IFESPN_Total_Seconds_Viewed
,sum(case when analysis_right ='International Friendlies - BT Sport' then total_duration_viewed_seconds else 0 end) 
as IFBTS_Total_Seconds_Viewed
,sum(case when analysis_right ='ITV1 Boxing' then total_duration_viewed_seconds else 0 end) 
as BOXITV1_Total_Seconds_Viewed
,sum(case when analysis_right ='ITV1 Football' then total_duration_viewed_seconds else 0 end) 
as FOOTITV1_Total_Seconds_Viewed
,sum(case when analysis_right ='ITV1 Motor Sport' then total_duration_viewed_seconds else 0 end) 
as MOTSITV1_Total_Seconds_Viewed
,sum(case when analysis_right ='ITV1 Rugby' then total_duration_viewed_seconds else 0 end) 
as RUGITV1_Total_Seconds_Viewed
,sum(case when analysis_right ='ITV1 Snooker/Pool' then total_duration_viewed_seconds else 0 end) 
as SNPITV1_Total_Seconds_Viewed
,sum(case when analysis_right ='ITV1 Unknown' then total_duration_viewed_seconds else 0 end) 
as UNKITV1_Total_Seconds_Viewed
,sum(case when analysis_right ='ITV4 Boxing' then total_duration_viewed_seconds else 0 end) 
as BOXITV4_Total_Seconds_Viewed
,sum(case when analysis_right ='ITV4 Cricket' then total_duration_viewed_seconds else 0 end) 
as CRIITV4_Total_Seconds_Viewed
,sum(case when analysis_right ='ITV4 Darts' then total_duration_viewed_seconds else 0 end) 
as DARTITV4_Total_Seconds_Viewed
,sum(case when analysis_right ='ITV4 Extreme' then total_duration_viewed_seconds else 0 end) 
as EXTITV4_Total_Seconds_Viewed
,sum(case when analysis_right ='ITV4 Football' then total_duration_viewed_seconds else 0 end) 
as FOOTITV4_Total_Seconds_Viewed
,sum(case when analysis_right ='ITV4 Motor Sport' then total_duration_viewed_seconds else 0 end) 
as MSPITV4_Total_Seconds_Viewed
,sum(case when analysis_right ='ITV4 Rugby' then total_duration_viewed_seconds else 0 end) 
as RUGITV4_Total_Seconds_Viewed
,sum(case when analysis_right ='ITV4 Snooker/Pool' then total_duration_viewed_seconds else 0 end) 
as SNPITV4_Total_Seconds_Viewed
,sum(case when analysis_right ='ITV4 Tennis' then total_duration_viewed_seconds else 0 end) 
as TENITV4_Total_Seconds_Viewed
,sum(case when analysis_right ='ITV4 Unknown' then total_duration_viewed_seconds else 0 end) 
as UNKITV4_Total_Seconds_Viewed
,sum(case when analysis_right ='Ligue 1 - BT Sport' then total_duration_viewed_seconds else 0 end) 
as L1BTS_Total_Seconds_Viewed
,sum(case when analysis_right ='Ligue 1 - ESPN' then total_duration_viewed_seconds else 0 end) 
as L1ESPN_Total_Seconds_Viewed
,sum(case when analysis_right ='Match of the day - BBC' then total_duration_viewed_seconds else 0 end) 
as MOTDBBC_Total_Seconds_Viewed
,sum(case when analysis_right ='Matchroom Output Deal  2010-2012 (Other)- Sky Sports' then total_duration_viewed_seconds else 0 end) 
as MROSS_Total_Seconds_Viewed
,sum(case when analysis_right ='Matchroom Output Deal  2010-2012 (Pool)- Sky Sports' then total_duration_viewed_seconds else 0 end) 
as MRPSS_Total_Seconds_Viewed
,sum(case when analysis_right ='Matchroom Output Deal  2010-2012 (Snooker)- Sky Sports' then total_duration_viewed_seconds else 0 end) 
as MRSSS_Total_Seconds_Viewed
,sum(case when analysis_right ='Moto GP BBC' then total_duration_viewed_seconds else 0 end) 
as MGPBBC_Total_Seconds_Viewed
,sum(case when analysis_right ='NBA - Sky Sports' then total_duration_viewed_seconds else 0 end) 
as NBASS_Total_Seconds_Viewed
,sum(case when analysis_right ='NFL - BBC' then total_duration_viewed_seconds else 0 end) 
as NFLBBC_Total_Seconds_Viewed
,sum(case when analysis_right ='NFL - Channel 4' then total_duration_viewed_seconds else 0 end) 
as NFLCH4_Total_Seconds_Viewed
,sum(case when analysis_right ='NFL Seasons 2012-2015 - Sky Sports' then total_duration_viewed_seconds else 0 end) 
as NFLSS_Total_Seconds_Viewed
,sum(case when analysis_right ='Northern Ireland Football Association 2012-2014 - Sky Sports' then total_duration_viewed_seconds else 0 end) 
as NIFSS_Total_Seconds_Viewed
,sum(case when analysis_right ='Oaks - Channel 4' then total_duration_viewed_seconds else 0 end) 
as OAKCH4_Total_Seconds_Viewed
,sum(case when analysis_right ='Other American Football' then total_duration_viewed_seconds else 0 end) 
as AMOTH_Total_Seconds_Viewed
,sum(case when analysis_right ='Other Athletics' then total_duration_viewed_seconds else 0 end) 
as ATHOTH_Total_Seconds_Viewed
,sum(case when analysis_right ='Other Baseball' then total_duration_viewed_seconds else 0 end) 
as BASEOTH_Total_Seconds_Viewed
,sum(case when analysis_right ='Other Basketball' then total_duration_viewed_seconds else 0 end) 
as BASKOTH_Total_Seconds_Viewed
,sum(case when analysis_right ='Other Boxing' then total_duration_viewed_seconds else 0 end) 
as BOXOTH_Total_Seconds_Viewed
,sum(case when analysis_right ='Other Cricket' then total_duration_viewed_seconds else 0 end) 
as CRIOTH_Total_Seconds_Viewed
,sum(case when analysis_right ='Other Darts' then total_duration_viewed_seconds else 0 end) 
as DARTOTH_Total_Seconds_Viewed
,sum(case when analysis_right ='Other Equestrian' then total_duration_viewed_seconds else 0 end) 
as EQUOTH_Total_Seconds_Viewed
,sum(case when analysis_right ='Other Extreme' then total_duration_viewed_seconds else 0 end) 
as EXTOTH_Total_Seconds_Viewed
,sum(case when analysis_right ='Other Fishing' then total_duration_viewed_seconds else 0 end) 
as FSHOTH_Total_Seconds_Viewed
,sum(case when analysis_right ='Other Football' then total_duration_viewed_seconds else 0 end) 
as FOOTOTH_Total_Seconds_Viewed
,sum(case when analysis_right ='Other Golf' then total_duration_viewed_seconds else 0 end) 
as GOLFOTH_Total_Seconds_Viewed
,sum(case when analysis_right ='Other Ice Hockey' then total_duration_viewed_seconds else 0 end) 
as IHOTH_Total_Seconds_Viewed
,sum(case when analysis_right ='Other Motor Sport' then total_duration_viewed_seconds else 0 end) 
as MSPOTH_Total_Seconds_Viewed
,sum(case when analysis_right ='Other Racing' then total_duration_viewed_seconds else 0 end) 
as RACOTH_Total_Seconds_Viewed
,sum(case when analysis_right ='Other Rugby' then total_duration_viewed_seconds else 0 end) 
as RUGOTH_Total_Seconds_Viewed
,sum(case when analysis_right ='Other Rugby Internationals - ESPN' then total_duration_viewed_seconds else 0 end) 
as ORUGESPN_Total_Seconds_Viewed
,sum(case when analysis_right ='Other Snooker/Pool' then total_duration_viewed_seconds else 0 end) 
as OTHSNP_Total_Seconds_Viewed
,sum(case when analysis_right ='Other Tennis' then total_duration_viewed_seconds else 0 end) 
as OTHTEN_Total_Seconds_Viewed
,sum(case when analysis_right ='Other Unknown' then total_duration_viewed_seconds else 0 end) 
as OTHUNK_Total_Seconds_Viewed
,sum(case when analysis_right ='Other Watersports' then total_duration_viewed_seconds else 0 end) 
as OTHWAT_Total_Seconds_Viewed
,sum(case when analysis_right ='Other Wintersports' then total_duration_viewed_seconds else 0 end) 
as OTHWIN_Total_Seconds_Viewed
,sum(case when analysis_right ='Other Wrestling' then total_duration_viewed_seconds else 0 end) 
as OTHWRE_Total_Seconds_Viewed
,sum(case when analysis_right ='PGA Tour Golf 2010-2017 - Sky Sports' then total_duration_viewed_seconds else 0 end) 
as PGASS_Total_Seconds_Viewed
,sum(case when analysis_right ='Premier League - BT Sport' then total_duration_viewed_seconds else 0 end) 
as PLBTS_Total_Seconds_Viewed
,sum(case when analysis_right ='Premier League - ESPN' then total_duration_viewed_seconds else 0 end) 
as PLESPN_Total_Seconds_Viewed
,sum(case when analysis_right ='Premier League Darts 2010-2013 - Sky Sports' then total_duration_viewed_seconds else 0 end) 
as PLDSS_Total_Seconds_Viewed
,sum(case when analysis_right ='Premier League Football - Sky Sports' then total_duration_viewed_seconds else 0 end) 
as GPLSS_Total_Seconds_Viewed
,sum(case when analysis_right ='Premier League Football - Sky Sports (Match Choice)' then total_duration_viewed_seconds else 0 end) 
as PLMCSS_Total_Seconds_Viewed
,sum(case when analysis_right ='Premier League Football - Sky Sports (MNF)' then total_duration_viewed_seconds else 0 end) 
as PLMNFSS_Total_Seconds_Viewed
,sum(case when analysis_right ='Premier League Football - Sky Sports (non Live)' then total_duration_viewed_seconds else 0 end) 
as PLNLSS_Total_Seconds_Viewed
,sum(case when analysis_right ='Premier League Football - Sky Sports (other Live)' then total_duration_viewed_seconds else 0 end) 
as PLOLSS_Total_Seconds_Viewed
,sum(case when analysis_right ='Premier League Football - Sky Sports (Sat Lunchtime)' then total_duration_viewed_seconds else 0 end) 
as PLSLSS_Total_Seconds_Viewed
,sum(case when analysis_right ='Premier League Football - Sky Sports (Sat Night Live)' then total_duration_viewed_seconds else 0 end) 
as PLSNSS_Total_Seconds_Viewed
,sum(case when analysis_right ='Premier League Football - Sky Sports (Sun 4pm)' then total_duration_viewed_seconds else 0 end) 
as PLS4SS_Total_Seconds_Viewed
,sum(case when analysis_right ='Premier League Football - Sky Sports (Sun Lunchtime)' then total_duration_viewed_seconds else 0 end) 
as PLSULSS_Total_Seconds_Viewed
,sum(case when analysis_right ='Premiership Rugby - Sky Sports' then total_duration_viewed_seconds else 0 end) 
as PRUSS_Total_Seconds_Viewed
,sum(case when analysis_right ='Republic Of Ireland FA - 2010/11 To 2013/14 - Sky Sports' then total_duration_viewed_seconds else 0 end) 
as ROISS_Total_Seconds_Viewed
,sum(case when analysis_right ='Royal Ascot - Channel 4' then total_duration_viewed_seconds else 0 end) 
as RASCH4_Total_Seconds_Viewed
,sum(case when analysis_right ='Rugby Internationals (England) - BBC' then total_duration_viewed_seconds else 0 end) 
as RIEBBC_Total_Seconds_Viewed
,sum(case when analysis_right ='Rugby Internationals (Ireland) - BBC' then total_duration_viewed_seconds else 0 end) 
as RIIBBC_Total_Seconds_Viewed
,sum(case when analysis_right ='Rugby Internationals (Scotland) - BBC' then total_duration_viewed_seconds else 0 end) 
as RISBBC_Total_Seconds_Viewed
,sum(case when analysis_right ='Rugby Internationals (Wales) - BBC' then total_duration_viewed_seconds else 0 end) 
as RIWBBC_Total_Seconds_Viewed
,sum(case when analysis_right ='Rugby League  Challenge Cup- BBC' then total_duration_viewed_seconds else 0 end) 
as RLCCBBC_Total_Seconds_Viewed
,sum(case when analysis_right ='Rugby League - Sky Sports' then total_duration_viewed_seconds else 0 end) 
as RLGSS_Total_Seconds_Viewed
,sum(case when analysis_right ='Rugby League  World Cup- BBC' then total_duration_viewed_seconds else 0 end) 
as RLWCBBC_Total_Seconds_Viewed
,sum(case when analysis_right ='Sanzar Rugby Union 2011 - 2015 - Sky Sports' then total_duration_viewed_seconds else 0 end) 
as SARUSS_Total_Seconds_Viewed
,sum(case when analysis_right ='Scottish FA Deal - 2010/11 To 2013/14 - Sky Sports' then total_duration_viewed_seconds else 0 end) 
as SFASS_Total_Seconds_Viewed
,sum(case when analysis_right ='Serie A - BT Sport' then total_duration_viewed_seconds else 0 end) 
as SABTS_Total_Seconds_Viewed
,sum(case when analysis_right ='Serie A - ESPN' then total_duration_viewed_seconds else 0 end) 
as SAESPN_Total_Seconds_Viewed
,sum(case when analysis_right ='SFL - ESPN' then total_duration_viewed_seconds else 0 end) 
as SFLESPN_Total_Seconds_Viewed
,sum(case when analysis_right ='Six Nations - BBC' then total_duration_viewed_seconds else 0 end) 
as SNRBBC_Total_Seconds_Viewed
,sum(case when analysis_right ='Sky 1 and Sky 2 Boxing' then total_duration_viewed_seconds else 0 end) 
as BOXS12_Total_Seconds_Viewed
,sum(case when analysis_right ='Sky 1 and Sky 2 Football' then total_duration_viewed_seconds else 0 end) 
as FOOTS12_Total_Seconds_Viewed
,sum(case when analysis_right ='Sky 1 and Sky 2 Motor Sport' then total_duration_viewed_seconds else 0 end) 
as MSPS12_Total_Seconds_Viewed
,sum(case when analysis_right ='Sky 1 and Sky 2 Unknown' then total_duration_viewed_seconds else 0 end) 
as UNKS12_Total_Seconds_Viewed
,sum(case when analysis_right ='Sky 1 and Sky 2 Wrestling' then total_duration_viewed_seconds else 0 end) 
as WRES12_Total_Seconds_Viewed
,sum(case when analysis_right ='Sky Sports American Football' then total_duration_viewed_seconds else 0 end) 
as AMSS_Total_Seconds_Viewed
,sum(case when analysis_right ='Sky Sports Athletics' then total_duration_viewed_seconds else 0 end) 
as ATHSS_Total_Seconds_Viewed
,sum(case when analysis_right ='Sky Sports Baseball' then total_duration_viewed_seconds else 0 end) 
as BASESS_Total_Seconds_Viewed
,sum(case when analysis_right ='Sky Sports Basketball' then total_duration_viewed_seconds else 0 end) 
as BASKSS_Total_Seconds_Viewed
,sum(case when analysis_right ='Sky Sports Boxing' then total_duration_viewed_seconds else 0 end) 
as BOXSS_Total_Seconds_Viewed
,sum(case when analysis_right ='Sky Sports Cricket' then total_duration_viewed_seconds else 0 end) 
as CRISS_Total_Seconds_Viewed
,sum(case when analysis_right ='Sky Sports Darts' then total_duration_viewed_seconds else 0 end) 
as DARTSS_Total_Seconds_Viewed
,sum(case when analysis_right ='Sky Sports Equestrian' then total_duration_viewed_seconds else 0 end) 
as EQUSS_Total_Seconds_Viewed
,sum(case when analysis_right ='Sky Sports Extreme' then total_duration_viewed_seconds else 0 end) 
as EXTSS_Total_Seconds_Viewed
,sum(case when analysis_right ='Sky Sports Fishing' then total_duration_viewed_seconds else 0 end) 
as FISHSS_Total_Seconds_Viewed
,sum(case when analysis_right ='Sky Sports Football' then total_duration_viewed_seconds else 0 end) 
as FOOTSS_Total_Seconds_Viewed
,sum(case when analysis_right ='Sky Sports Golf' then total_duration_viewed_seconds else 0 end) 
as GOLFSS_Total_Seconds_Viewed
,sum(case when analysis_right ='Sky Sports Ice Hockey' then total_duration_viewed_seconds else 0 end) 
as IHSS_Total_Seconds_Viewed
,sum(case when analysis_right ='Sky Sports Motor Sport' then total_duration_viewed_seconds else 0 end) 
as MSPSS_Total_Seconds_Viewed
,sum(case when analysis_right ='Sky Sports Racing' then total_duration_viewed_seconds else 0 end) 
as RACSS_Total_Seconds_Viewed
,sum(case when analysis_right ='Sky Sports Rugby' then total_duration_viewed_seconds else 0 end) 
as RUGSS_Total_Seconds_Viewed
,sum(case when analysis_right ='Sky Sports Snooker/Pool' then total_duration_viewed_seconds else 0 end) 
as SNPSS_Total_Seconds_Viewed
,sum(case when analysis_right ='Sky Sports Tennis' then total_duration_viewed_seconds else 0 end) 
as TENSS_Total_Seconds_Viewed
,sum(case when analysis_right ='Sky Sports Unknown' then total_duration_viewed_seconds else 0 end) 
as UNKSS_Total_Seconds_Viewed
,sum(case when analysis_right ='Sky Sports Watersports' then total_duration_viewed_seconds else 0 end) 
as WATSS_Total_Seconds_Viewed
,sum(case when analysis_right ='Sky Sports Wintersports' then total_duration_viewed_seconds else 0 end) 
as WINSS_Total_Seconds_Viewed
,sum(case when analysis_right ='Sky Sports Wrestling' then total_duration_viewed_seconds else 0 end) 
as WRESS_Total_Seconds_Viewed
,sum(case when analysis_right ='Solheim Cup 2013 Ladies Golf - Sky Sports' then total_duration_viewed_seconds else 0 end) 
as SOLSS_Total_Seconds_Viewed
,sum(case when analysis_right ='South African Home Cricket  2012-2020 Sky Sports' then total_duration_viewed_seconds else 0 end) 
as SACSS_Total_Seconds_Viewed
,sum(case when analysis_right ='Spanish Football 2012/13 To 14/15 - Sky Sports' then total_duration_viewed_seconds else 0 end) 
as SPFSS_Total_Seconds_Viewed
,sum(case when analysis_right ='SPFL - BT Sport' then total_duration_viewed_seconds else 0 end) 
as SPFLBTS_Total_Seconds_Viewed
,sum(case when analysis_right ='SPL - ESPN' then total_duration_viewed_seconds else 0 end) 
as SPLESPN_Total_Seconds_Viewed
,sum(case when analysis_right ='SPL - Sky Sports' then total_duration_viewed_seconds else 0 end) 
as SPLSS_Total_Seconds_Viewed
,sum(case when analysis_right ='Sportfive World Cup 2014 Qualifers - Sky Sports' then total_duration_viewed_seconds else 0 end) 
as SP5SS_Total_Seconds_Viewed
,sum(case when analysis_right ='The boat race - BBC' then total_duration_viewed_seconds else 0 end) 
as BTRBBC_Total_Seconds_Viewed
,sum(case when analysis_right ='The football league show - BBC' then total_duration_viewed_seconds else 0 end) 
as FLSBBC_Total_Seconds_Viewed
,sum(case when analysis_right ='The Masters Golf - BBC' then total_duration_viewed_seconds else 0 end) 
as MGBBC_Total_Seconds_Viewed
,sum(case when analysis_right ='TNA Wrestling Challenge' then total_duration_viewed_seconds else 0 end) 
as TNACHA_Total_Seconds_Viewed
,sum(case when analysis_right ='Tour de France - Eurosport' then total_duration_viewed_seconds else 0 end) 
as TDFEUR_Total_Seconds_Viewed
,sum(case when analysis_right ='Tour de France - ITV' then total_duration_viewed_seconds else 0 end) 
as TDFITV_Total_Seconds_Viewed
,sum(case when analysis_right ='U S Masters Golf 2011-2014 Sky Sports' then total_duration_viewed_seconds else 0 end) 
as USMGSS_Total_Seconds_Viewed
,sum(case when analysis_right ='U.S Open Tennis 2013-2015 Sky Sports' then total_duration_viewed_seconds else 0 end) 
as USOTSS_Total_Seconds_Viewed
,sum(case when analysis_right ='U.S Open/Womens/Seniors/Amateur Golf Sky Sports' then total_duration_viewed_seconds else 0 end) 
as USOGSS_Total_Seconds_Viewed
,sum(case when analysis_right ='UEFA Champions League -  Sky Sports' then total_duration_viewed_seconds else 0 end) 
as CLASS_Total_Seconds_Viewed
,sum(case when analysis_right ='UEFA Champions League -  Sky Sports (non Live)' then total_duration_viewed_seconds else 0 end) 
as CLNSS_Total_Seconds_Viewed
,sum(case when analysis_right ='UEFA Champions League -  Sky Sports (other Live)' then total_duration_viewed_seconds else 0 end) 
as CLOSS_Total_Seconds_Viewed
,sum(case when analysis_right ='UEFA Champions League -  Sky Sports (Tue)' then total_duration_viewed_seconds else 0 end) 
as CLTSS_Total_Seconds_Viewed
,sum(case when analysis_right ='UEFA Champions League -  Sky Sports (Wed)' then total_duration_viewed_seconds else 0 end) 
as CLWSS_Total_Seconds_Viewed
,sum(case when analysis_right ='US Open Tennis - Eurosport' then total_duration_viewed_seconds else 0 end) 
as USOTEUR_Total_Seconds_Viewed
,sum(case when analysis_right ='USA Football - BT Sport' then total_duration_viewed_seconds else 0 end) 
as USFBTS_Total_Seconds_Viewed
,sum(case when analysis_right ='USPGA Championship (2007-2016) Sky Sports' then total_duration_viewed_seconds else 0 end) 
as USPGASS_Total_Seconds_Viewed
,sum(case when analysis_right ='WCQ - ESPN' then total_duration_viewed_seconds else 0 end) 
as WCQESPN_Total_Seconds_Viewed
,sum(case when analysis_right ='Welsh Home International Football 2012-2014 Sky Sports' then total_duration_viewed_seconds else 0 end) 
as WIFSS_Total_Seconds_Viewed
,sum(case when analysis_right ='West Indies Home Cricket 2013-2020 Sky Sports' then total_duration_viewed_seconds else 0 end) 
as WICSS_Total_Seconds_Viewed
,sum(case when analysis_right ='Wimbledon - BBC' then total_duration_viewed_seconds else 0 end) 
as WIMBBC_Total_Seconds_Viewed
,sum(case when analysis_right ='Womens Cricket - I C C Cricket Deal  2012-2015 Sky Sports' then total_duration_viewed_seconds else 0 end) 
as WICCSS_Total_Seconds_Viewed
,sum(case when analysis_right ='World Athletics Championship - More 4' then total_duration_viewed_seconds else 0 end) 
as WACMR4_Total_Seconds_Viewed
,sum(case when analysis_right ='World Club Championship - BBC' then total_duration_viewed_seconds else 0 end) 
as WCLBBBC_Total_Seconds_Viewed
,sum(case when analysis_right ='World Cup Qualifiers - BT Sport' then total_duration_viewed_seconds else 0 end) 
as WCQBTS_Total_Seconds_Viewed
,sum(case when analysis_right ='World Darts Championship 2009-2012 Sky Sports' then total_duration_viewed_seconds else 0 end) 
as WDCSS_Total_Seconds_Viewed
,sum(case when analysis_right ='World snooker championship - BBC' then total_duration_viewed_seconds else 0 end) 
as WSCBBC_Total_Seconds_Viewed
,sum(case when analysis_right ='WWE Sky 1 and 2' then total_duration_viewed_seconds else 0 end) 
as WWES12_Total_Seconds_Viewed
,sum(case when analysis_right ='WWE Sky Sports' then total_duration_viewed_seconds else 0 end) 
as WWESS_Total_Seconds_Viewed
,sum(case when analysis_right ='Africa Cup of Nations - Eurosport' then total_programmes_viewed_over_threshold else 0 end) 
as AFCEUR_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='Africa Cup of Nations - ITV' then total_programmes_viewed_over_threshold else 0 end) 
as AFCITV_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='Americas Cup - BBC' then total_programmes_viewed_over_threshold else 0 end) 
as AMCBBC_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='Asian Tour Golf 2012-2013 Sky Sports' then total_programmes_viewed_over_threshold else 0 end) 
as ATGSS_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='ATP Tennis Masters Series  2011-2013 Sky Sports' then total_programmes_viewed_over_threshold else 0 end) 
as ATPSS_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='Australia Home Cricket  2012-2016 Sky Sports' then total_programmes_viewed_over_threshold else 0 end) 
as AHCSS_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='Australian Football - BT Sport' then total_programmes_viewed_over_threshold else 0 end) 
as AUFBTS_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='Australian Open Tennis - BBC' then total_programmes_viewed_over_threshold else 0 end) 
as AOTBBC_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='Australian Open Tennis - Eurosport' then total_programmes_viewed_over_threshold else 0 end) 
as AOTEUR_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='Aviva Premiership - ESPN' then total_programmes_viewed_over_threshold else 0 end) 
as AVPSS_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='BBC American Football' then total_programmes_viewed_over_threshold else 0 end) 
as AFBBC_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='BBC Athletics' then total_programmes_viewed_over_threshold else 0 end) 
as ATHBBC_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='BBC Boxing' then total_programmes_viewed_over_threshold else 0 end) 
as BOXBBC_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='BBC Darts' then total_programmes_viewed_over_threshold else 0 end) 
as DRTBBC_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='BBC Equestrian' then total_programmes_viewed_over_threshold else 0 end) 
as EQUBBC_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='BBC Football' then total_programmes_viewed_over_threshold else 0 end) 
as FOOTBBC_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='BBC Golf' then total_programmes_viewed_over_threshold else 0 end) 
as GOLFBBC_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='BBC Motor Sport' then total_programmes_viewed_over_threshold else 0 end) 
as MSPBBC_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='BBC Rugby' then total_programmes_viewed_over_threshold else 0 end) 
as RUGBBC_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='BBC Snooker/Pool' then total_programmes_viewed_over_threshold else 0 end) 
as SNPBBC_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='BBC Tennis' then total_programmes_viewed_over_threshold else 0 end) 
as TENBBC_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='BBC Unknown' then total_programmes_viewed_over_threshold else 0 end) 
as UNKBBC_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='BBC Watersports' then total_programmes_viewed_over_threshold else 0 end) 
as WATBBC_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='BBC Wintersports' then total_programmes_viewed_over_threshold else 0 end) 
as WINBBC_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='Boxing  - Channel 5' then total_programmes_viewed_over_threshold else 0 end) 
as BOXCH5_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='Boxing - Matchroom 2012-2014 Sky Sports' then total_programmes_viewed_over_threshold else 0 end) 
as BOXMSS_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='Brazil Football - BT Sport' then total_programmes_viewed_over_threshold else 0 end) 
as BFTBTS_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='British & Irish Lions 2013 - Sky Sports' then total_programmes_viewed_over_threshold else 0 end) 
as BILSS_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='British Open Golf - BBC' then total_programmes_viewed_over_threshold else 0 end) 
as BOGSS_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='BT Sport American Football' then total_programmes_viewed_over_threshold else 0 end) 
as AFBTS_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='BT Sport Athletics' then total_programmes_viewed_over_threshold else 0 end) 
as ATHBTS_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='BT Sport Baseball' then total_programmes_viewed_over_threshold else 0 end) 
as BASEBTS_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='BT Sport Basketball' then total_programmes_viewed_over_threshold else 0 end) 
as BASKBTS_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='BT Sport Boxing' then total_programmes_viewed_over_threshold else 0 end) 
as BOXBTS_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='BT Sport Cricket' then total_programmes_viewed_over_threshold else 0 end) 
as CRIBTS_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='BT Sport Equestrian' then total_programmes_viewed_over_threshold else 0 end) 
as EQUBTS_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='BT Sport Extreme' then total_programmes_viewed_over_threshold else 0 end) 
as EXTBTS_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='BT Sport Football' then total_programmes_viewed_over_threshold else 0 end) 
as FOOTBTS_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='BT Sport Motor Sport' then total_programmes_viewed_over_threshold else 0 end) 
as MSPBTS_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='BT Sport Rugby' then total_programmes_viewed_over_threshold else 0 end) 
as RUGBTS_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='BT Sport Tennis' then total_programmes_viewed_over_threshold else 0 end) 
as TENBTS_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='BT Sport Unknown' then total_programmes_viewed_over_threshold else 0 end) 
as UNKBTS_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='BT Sport Wintersports' then total_programmes_viewed_over_threshold else 0 end) 
as WINBTS_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='Bundesliga - BT Sport' then total_programmes_viewed_over_threshold else 0 end) 
as BUNBTS_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='Bundesliga- ESPN' then total_programmes_viewed_over_threshold else 0 end) 
as BUNESPN_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='Challenge Darts' then total_programmes_viewed_over_threshold else 0 end) 
as DRTCHA_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='Challenge Extreme' then total_programmes_viewed_over_threshold else 0 end) 
as EXTCHA_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='Challenge Unknown' then total_programmes_viewed_over_threshold else 0 end) 
as UNKCHA_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='Challenge Wrestling' then total_programmes_viewed_over_threshold else 0 end) 
as WRECHA_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='Champions League - ITV' then total_programmes_viewed_over_threshold else 0 end) 
as CHLITV_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='Champions Trophy - I C C Cricket Deal  2012-2015 Sky Sports' then total_programmes_viewed_over_threshold else 0 end) 
as ICCSS_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='Channel 4 American Football' then total_programmes_viewed_over_threshold else 0 end) 
as AMCH4_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='Channel 4 Athletics' then total_programmes_viewed_over_threshold else 0 end) 
as ATHCH4_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='Channel 4 Boxing' then total_programmes_viewed_over_threshold else 0 end) 
as BOXCH4_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='Channel 4 Equestrian' then total_programmes_viewed_over_threshold else 0 end) 
as EQUCH4_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='Channel 4 Extreme' then total_programmes_viewed_over_threshold else 0 end) 
as EXTCH4_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='Channel 4 Football' then total_programmes_viewed_over_threshold else 0 end) 
as FOOTCH4_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='Channel 4 Motor Sport' then total_programmes_viewed_over_threshold else 0 end) 
as MSPCH4_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='Channel 4 Racing' then total_programmes_viewed_over_threshold else 0 end) 
as RACCH4_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='Channel 4 Unknown' then total_programmes_viewed_over_threshold else 0 end) 
as UNKCH4_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='Channel 4 Watersports' then total_programmes_viewed_over_threshold else 0 end) 
as WATCH4_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='Channel 4 Wintersports' then total_programmes_viewed_over_threshold else 0 end) 
as WINCH4_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='Channel 5 Athletics' then total_programmes_viewed_over_threshold else 0 end) 
as ATHCH5_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='Channel 5 Boxing' then total_programmes_viewed_over_threshold else 0 end) 
as BOXOCH5_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='Channel 5 Cricket' then total_programmes_viewed_over_threshold else 0 end) 
as CRICH5_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='Channel 5 Motor Sport' then total_programmes_viewed_over_threshold else 0 end) 
as MSPCH5_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='Channel 5 Unknown' then total_programmes_viewed_over_threshold else 0 end) 
as UNKCH5_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='Channel 5 Wrestling' then total_programmes_viewed_over_threshold else 0 end) 
as WRECH5_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='Cheltenham Festival - Channel 4' then total_programmes_viewed_over_threshold else 0 end) 
as CHELCH4_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='Community Shield - ITV' then total_programmes_viewed_over_threshold else 0 end) 
as CMSITV_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='Confederations Cup - BBC' then total_programmes_viewed_over_threshold else 0 end) 
as CONCBBC_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='Conference - BT Sport' then total_programmes_viewed_over_threshold else 0 end) 
as CONFBTS_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='Cycling - La Vuelta ITV' then total_programmes_viewed_over_threshold else 0 end) 
as CLVITV_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='Cycling - U C I World Tour Sky Sports' then total_programmes_viewed_over_threshold else 0 end) 
as CUCISS_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='Cycling Tour of Britain - Eurosport' then total_programmes_viewed_over_threshold else 0 end) 
as CTBEUR_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='Cycling: tour of britain ITV4' then total_programmes_viewed_over_threshold else 0 end) 
as CTCITV_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='Derby - Channel 4' then total_programmes_viewed_over_threshold else 0 end) 
as DERCH4_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='ECB (highlights) - Channel 5' then total_programmes_viewed_over_threshold else 0 end) 
as ECBHCH5_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='ECB Cricket Sky Sports' then total_programmes_viewed_over_threshold else 0 end) 
as GECRSS_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='ECB non-Test Cricket Sky Sports' then total_programmes_viewed_over_threshold else 0 end) 
as ECBNSS_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='ECB Test Cricket Sky Sports' then total_programmes_viewed_over_threshold else 0 end) 
as ECBTSS_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='England Football Internationals - ITV' then total_programmes_viewed_over_threshold else 0 end) 
as GENGITV_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='England Friendlies (Football) - ITV' then total_programmes_viewed_over_threshold else 0 end) 
as EFRITV_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='England Rugby Internationals 2010-2015 - Sky Sports' then total_programmes_viewed_over_threshold else 0 end) 
as ENRSS_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='England World Cup Qualifying (Away) - ITV' then total_programmes_viewed_over_threshold else 0 end) 
as EWQAITV_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='England World Cup Qualifying (Home) - ITV' then total_programmes_viewed_over_threshold else 0 end) 
as EWQHITV_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='ESPN American Football' then total_programmes_viewed_over_threshold else 0 end) 
as AMESPN_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='ESPN Athletics' then total_programmes_viewed_over_threshold else 0 end) 
as ATHESPN_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='ESPN Baseball' then total_programmes_viewed_over_threshold else 0 end) 
as BASEESPN_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='ESPN Basketball' then total_programmes_viewed_over_threshold else 0 end) 
as BASKESPN_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='ESPN Boxing' then total_programmes_viewed_over_threshold else 0 end) 
as BOXESPN_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='ESPN Cricket' then total_programmes_viewed_over_threshold else 0 end) 
as CRIESPN_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='ESPN Darts' then total_programmes_viewed_over_threshold else 0 end) 
as DARTESPN_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='ESPN Equestrian' then total_programmes_viewed_over_threshold else 0 end) 
as EQUESPN_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='ESPN Extreme' then total_programmes_viewed_over_threshold else 0 end) 
as EXTESPN_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='ESPN Football' then total_programmes_viewed_over_threshold else 0 end) 
as FOOTESPN_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='ESPN Golf' then total_programmes_viewed_over_threshold else 0 end) 
as GOLFESPN_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='ESPN Ice Hockey' then total_programmes_viewed_over_threshold else 0 end) 
as IHESPN_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='ESPN Motor Sport' then total_programmes_viewed_over_threshold else 0 end) 
as MSPESPN_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='ESPN Racing' then total_programmes_viewed_over_threshold else 0 end) 
as RACESPN_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='ESPN Rugby' then total_programmes_viewed_over_threshold else 0 end) 
as RUGESPN_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='ESPN Snooker/Pool' then total_programmes_viewed_over_threshold else 0 end) 
as SNPESPN_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='ESPN Tennis' then total_programmes_viewed_over_threshold else 0 end) 
as TENESPN_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='ESPN Unknown' then total_programmes_viewed_over_threshold else 0 end) 
as UNKESPN_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='ESPN Watersports' then total_programmes_viewed_over_threshold else 0 end) 
as WATESPN_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='ESPN Wintersports' then total_programmes_viewed_over_threshold else 0 end) 
as WINESPN_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='ESPN Wrestling' then total_programmes_viewed_over_threshold else 0 end) 
as WREESPN_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='Europa League - BT Sport' then total_programmes_viewed_over_threshold else 0 end) 
as ELBTSP_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='Europa League - ESPN' then total_programmes_viewed_over_threshold else 0 end) 
as ELESPN_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='Europa League - ITV' then total_programmes_viewed_over_threshold else 0 end) 
as ELITV_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='European Tour Golf - Sky Sports' then total_programmes_viewed_over_threshold else 0 end) 
as ETGSS_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='Eurosport American Football' then total_programmes_viewed_over_threshold else 0 end) 
as AMEUR_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='Eurosport Athletics' then total_programmes_viewed_over_threshold else 0 end) 
as ATHEUR_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='Eurosport Baseball' then total_programmes_viewed_over_threshold else 0 end) 
as BASEEUR_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='Eurosport Basketball' then total_programmes_viewed_over_threshold else 0 end) 
as BASKEUR_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='Eurosport Boxing' then total_programmes_viewed_over_threshold else 0 end) 
as BOXEUR_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='Eurosport Cricket' then total_programmes_viewed_over_threshold else 0 end) 
as CRIEUR_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='Eurosport Darts' then total_programmes_viewed_over_threshold else 0 end) 
as DARTEUR_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='Eurosport Equestrian' then total_programmes_viewed_over_threshold else 0 end) 
as EQUEUR_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='Eurosport Extreme' then total_programmes_viewed_over_threshold else 0 end) 
as EXTEUR_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='Eurosport Football' then total_programmes_viewed_over_threshold else 0 end) 
as FOOTEUR_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='Eurosport Golf' then total_programmes_viewed_over_threshold else 0 end) 
as GOLFEUR_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='Eurosport Ice Hockey' then total_programmes_viewed_over_threshold else 0 end) 
as IHEUR_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='Eurosport Motor Sport' then total_programmes_viewed_over_threshold else 0 end) 
as MSPEUR_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='Eurosport Racing' then total_programmes_viewed_over_threshold else 0 end) 
as RACEUR_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='Eurosport Snooker/Pool' then total_programmes_viewed_over_threshold else 0 end) 
as SNPEUR_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='Eurosport Tennis' then total_programmes_viewed_over_threshold else 0 end) 
as TENEUR_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='Eurosport Unknown' then total_programmes_viewed_over_threshold else 0 end) 
as UNKEUR_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='Eurosport Watersports' then total_programmes_viewed_over_threshold else 0 end) 
as WATEUR_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='Eurosport Wintersports' then total_programmes_viewed_over_threshold else 0 end) 
as WINEUR_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='F1 - BBC' then total_programmes_viewed_over_threshold else 0 end) 
as GF1BBC_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='F1 - Sky Sports' then total_programmes_viewed_over_threshold else 0 end) 
as GF1SS_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='F1 (non-Live)- BBC' then total_programmes_viewed_over_threshold else 0 end) 
as F1NBBC_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='F1 (Practice Live)- BBC' then total_programmes_viewed_over_threshold else 0 end) 
as F1PBBC_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='F1 (Qualifying Live)- BBC' then total_programmes_viewed_over_threshold else 0 end) 
as F1QBBC_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='F1 (Race Live)- BBC' then total_programmes_viewed_over_threshold else 0 end) 
as F1RBBC_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='FA Cup - ESPN' then total_programmes_viewed_over_threshold else 0 end) 
as FACESPN_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='FA Cup - ITV' then total_programmes_viewed_over_threshold else 0 end) 
as FACITV_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='Football League - Seasons 2012/13 To 14/15 Sky Sports - Capital 1 Cup' then total_programmes_viewed_over_threshold else 0 end) 
as FLCCSS_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='Football League - Seasons 2012/13 To 14/15 Sky Sports - others' then total_programmes_viewed_over_threshold else 0 end) 
as FLOTSS_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='Formula One 2012-2018 - (non-Live) Sky Sports' then total_programmes_viewed_over_threshold else 0 end) 
as F1NSS_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='Formula One 2012-2018 - (Practice Live) Sky Sports' then total_programmes_viewed_over_threshold else 0 end) 
as F1PSS_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='Formula One 2012-2018 - (Qualifying Live) Sky Sports' then total_programmes_viewed_over_threshold else 0 end) 
as F1QSS_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='Formula One 2012-2018 - (Race Live) Sky Sports' then total_programmes_viewed_over_threshold else 0 end) 
as F1RSS_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='French Open Tennis - Eurosport' then total_programmes_viewed_over_threshold else 0 end) 
as FOTEUR_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='French Open Tennis - ITV' then total_programmes_viewed_over_threshold else 0 end) 
as FOTITV_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='Grand National - Channel 4' then total_programmes_viewed_over_threshold else 0 end) 
as GDNCH4_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='Heineken Cup 2010/11 To 2013/14- Sky Sports' then total_programmes_viewed_over_threshold else 0 end) 
as HECSS_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='I R B Sevens 2008/09 -2014/15 - Sky Sports' then total_programmes_viewed_over_threshold else 0 end) 
as IRBSS_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='IAAF World Athletics Championship - Eurosport' then total_programmes_viewed_over_threshold else 0 end) 
as WACEUR_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='India Home Cricket 2012-2018 Sky Sports' then total_programmes_viewed_over_threshold else 0 end) 
as IHCSS_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='India Premier League - ITV' then total_programmes_viewed_over_threshold else 0 end) 
as IPLITV_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='International Freindlies - ESPN' then total_programmes_viewed_over_threshold else 0 end) 
as IFESPN_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='International Friendlies - BT Sport' then total_programmes_viewed_over_threshold else 0 end) 
as IFBTS_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='ITV1 Boxing' then total_programmes_viewed_over_threshold else 0 end) 
as BOXITV1_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='ITV1 Football' then total_programmes_viewed_over_threshold else 0 end) 
as FOOTITV1_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='ITV1 Motor Sport' then total_programmes_viewed_over_threshold else 0 end) 
as MOTSITV1_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='ITV1 Rugby' then total_programmes_viewed_over_threshold else 0 end) 
as RUGITV1_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='ITV1 Snooker/Pool' then total_programmes_viewed_over_threshold else 0 end) 
as SNPITV1_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='ITV1 Unknown' then total_programmes_viewed_over_threshold else 0 end) 
as UNKITV1_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='ITV4 Boxing' then total_programmes_viewed_over_threshold else 0 end) 
as BOXITV4_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='ITV4 Cricket' then total_programmes_viewed_over_threshold else 0 end) 
as CRIITV4_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='ITV4 Darts' then total_programmes_viewed_over_threshold else 0 end) 
as DARTITV4_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='ITV4 Extreme' then total_programmes_viewed_over_threshold else 0 end) 
as EXTITV4_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='ITV4 Football' then total_programmes_viewed_over_threshold else 0 end) 
as FOOTITV4_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='ITV4 Motor Sport' then total_programmes_viewed_over_threshold else 0 end) 
as MSPITV4_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='ITV4 Rugby' then total_programmes_viewed_over_threshold else 0 end) 
as RUGITV4_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='ITV4 Snooker/Pool' then total_programmes_viewed_over_threshold else 0 end) 
as SNPITV4_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='ITV4 Tennis' then total_programmes_viewed_over_threshold else 0 end) 
as TENITV4_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='ITV4 Unknown' then total_programmes_viewed_over_threshold else 0 end) 
as UNKITV4_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='Ligue 1 - BT Sport' then total_programmes_viewed_over_threshold else 0 end) 
as L1BTS_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='Ligue 1 - ESPN' then total_programmes_viewed_over_threshold else 0 end) 
as L1ESPN_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='Match of the day - BBC' then total_programmes_viewed_over_threshold else 0 end) 
as MOTDBBC_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='Matchroom Output Deal  2010-2012 (Other)- Sky Sports' then total_programmes_viewed_over_threshold else 0 end) 
as MROSS_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='Matchroom Output Deal  2010-2012 (Pool)- Sky Sports' then total_programmes_viewed_over_threshold else 0 end) 
as MRPSS_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='Matchroom Output Deal  2010-2012 (Snooker)- Sky Sports' then total_programmes_viewed_over_threshold else 0 end) 
as MRSSS_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='Moto GP BBC' then total_programmes_viewed_over_threshold else 0 end) 
as MGPBBC_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='NBA - Sky Sports' then total_programmes_viewed_over_threshold else 0 end) 
as NBASS_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='NFL - BBC' then total_programmes_viewed_over_threshold else 0 end) 
as NFLBBC_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='NFL - Channel 4' then total_programmes_viewed_over_threshold else 0 end) 
as NFLCH4_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='NFL Seasons 2012-2015 - Sky Sports' then total_programmes_viewed_over_threshold else 0 end) 
as NFLSS_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='Northern Ireland Football Association 2012-2014 - Sky Sports' then total_programmes_viewed_over_threshold else 0 end) 
as NIFSS_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='Oaks - Channel 4' then total_programmes_viewed_over_threshold else 0 end) 
as OAKCH4_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='Other American Football' then total_programmes_viewed_over_threshold else 0 end) 
as AMOTH_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='Other Athletics' then total_programmes_viewed_over_threshold else 0 end) 
as ATHOTH_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='Other Baseball' then total_programmes_viewed_over_threshold else 0 end) 
as BASEOTH_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='Other Basketball' then total_programmes_viewed_over_threshold else 0 end) 
as BASKOTH_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='Other Boxing' then total_programmes_viewed_over_threshold else 0 end) 
as BOXOTH_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='Other Cricket' then total_programmes_viewed_over_threshold else 0 end) 
as CRIOTH_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='Other Darts' then total_programmes_viewed_over_threshold else 0 end) 
as DARTOTH_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='Other Equestrian' then total_programmes_viewed_over_threshold else 0 end) 
as EQUOTH_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='Other Extreme' then total_programmes_viewed_over_threshold else 0 end) 
as EXTOTH_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='Other Fishing' then total_programmes_viewed_over_threshold else 0 end) 
as FSHOTH_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='Other Football' then total_programmes_viewed_over_threshold else 0 end) 
as FOOTOTH_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='Other Golf' then total_programmes_viewed_over_threshold else 0 end) 
as GOLFOTH_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='Other Ice Hockey' then total_programmes_viewed_over_threshold else 0 end) 
as IHOTH_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='Other Motor Sport' then total_programmes_viewed_over_threshold else 0 end) 
as MSPOTH_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='Other Racing' then total_programmes_viewed_over_threshold else 0 end) 
as RACOTH_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='Other Rugby' then total_programmes_viewed_over_threshold else 0 end) 
as RUGOTH_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='Other Rugby Internationals - ESPN' then total_programmes_viewed_over_threshold else 0 end) 
as ORUGESPN_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='Other Snooker/Pool' then total_programmes_viewed_over_threshold else 0 end) 
as OTHSNP_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='Other Tennis' then total_programmes_viewed_over_threshold else 0 end) 
as OTHTEN_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='Other Unknown' then total_programmes_viewed_over_threshold else 0 end) 
as OTHUNK_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='Other Watersports' then total_programmes_viewed_over_threshold else 0 end) 
as OTHWAT_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='Other Wintersports' then total_programmes_viewed_over_threshold else 0 end) 
as OTHWIN_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='Other Wrestling' then total_programmes_viewed_over_threshold else 0 end) 
as OTHWRE_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='PGA Tour Golf 2010-2017 - Sky Sports' then total_programmes_viewed_over_threshold else 0 end) 
as PGASS_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='Premier League - BT Sport' then total_programmes_viewed_over_threshold else 0 end) 
as PLBTS_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='Premier League - ESPN' then total_programmes_viewed_over_threshold else 0 end) 
as PLESPN_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='Premier League Darts 2010-2013 - Sky Sports' then total_programmes_viewed_over_threshold else 0 end) 
as PLDSS_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='Premier League Football - Sky Sports' then total_programmes_viewed_over_threshold else 0 end) 
as GPLSS_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='Premier League Football - Sky Sports (Match Choice)' then total_programmes_viewed_over_threshold else 0 end) 
as PLMCSS_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='Premier League Football - Sky Sports (MNF)' then total_programmes_viewed_over_threshold else 0 end) 
as PLMNFSS_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='Premier League Football - Sky Sports (non Live)' then total_programmes_viewed_over_threshold else 0 end) 
as PLNLSS_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='Premier League Football - Sky Sports (other Live)' then total_programmes_viewed_over_threshold else 0 end) 
as PLOLSS_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='Premier League Football - Sky Sports (Sat Lunchtime)' then total_programmes_viewed_over_threshold else 0 end) 
as PLSLSS_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='Premier League Football - Sky Sports (Sat Night Live)' then total_programmes_viewed_over_threshold else 0 end) 
as PLSNSS_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='Premier League Football - Sky Sports (Sun 4pm)' then total_programmes_viewed_over_threshold else 0 end) 
as PLS4SS_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='Premier League Football - Sky Sports (Sun Lunchtime)' then total_programmes_viewed_over_threshold else 0 end) 
as PLSULSS_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='Premiership Rugby - Sky Sports' then total_programmes_viewed_over_threshold else 0 end) 
as PRUSS_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='Republic Of Ireland FA - 2010/11 To 2013/14 - Sky Sports' then total_programmes_viewed_over_threshold else 0 end) 
as ROISS_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='Royal Ascot - Channel 4' then total_programmes_viewed_over_threshold else 0 end) 
as RASCH4_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='Rugby Internationals (England) - BBC' then total_programmes_viewed_over_threshold else 0 end) 
as RIEBBC_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='Rugby Internationals (Ireland) - BBC' then total_programmes_viewed_over_threshold else 0 end) 
as RIIBBC_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='Rugby Internationals (Scotland) - BBC' then total_programmes_viewed_over_threshold else 0 end) 
as RISBBC_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='Rugby Internationals (Wales) - BBC' then total_programmes_viewed_over_threshold else 0 end) 
as RIWBBC_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='Rugby League  Challenge Cup- BBC' then total_programmes_viewed_over_threshold else 0 end) 
as RLCCBBC_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='Rugby League - Sky Sports' then total_programmes_viewed_over_threshold else 0 end) 
as RLGSS_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='Rugby League  World Cup- BBC' then total_programmes_viewed_over_threshold else 0 end) 
as RLWCBBC_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='Sanzar Rugby Union 2011 - 2015 - Sky Sports' then total_programmes_viewed_over_threshold else 0 end) 
as SARUSS_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='Scottish FA Deal - 2010/11 To 2013/14 - Sky Sports' then total_programmes_viewed_over_threshold else 0 end) 
as SFASS_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='Serie A - BT Sport' then total_programmes_viewed_over_threshold else 0 end) 
as SABTS_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='Serie A - ESPN' then total_programmes_viewed_over_threshold else 0 end) 
as SAESPN_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='SFL - ESPN' then total_programmes_viewed_over_threshold else 0 end) 
as SFLESPN_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='Six Nations - BBC' then total_programmes_viewed_over_threshold else 0 end) 
as SNRBBC_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='Sky 1 and Sky 2 Boxing' then total_programmes_viewed_over_threshold else 0 end) 
as BOXS12_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='Sky 1 and Sky 2 Football' then total_programmes_viewed_over_threshold else 0 end) 
as FOOTS12_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='Sky 1 and Sky 2 Motor Sport' then total_programmes_viewed_over_threshold else 0 end) 
as MSPS12_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='Sky 1 and Sky 2 Unknown' then total_programmes_viewed_over_threshold else 0 end) 
as UNKS12_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='Sky 1 and Sky 2 Wrestling' then total_programmes_viewed_over_threshold else 0 end) 
as WRES12_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='Sky Sports American Football' then total_programmes_viewed_over_threshold else 0 end) 
as AMSS_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='Sky Sports Athletics' then total_programmes_viewed_over_threshold else 0 end) 
as ATHSS_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='Sky Sports Baseball' then total_programmes_viewed_over_threshold else 0 end) 
as BASESS_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='Sky Sports Basketball' then total_programmes_viewed_over_threshold else 0 end) 
as BASKSS_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='Sky Sports Boxing' then total_programmes_viewed_over_threshold else 0 end) 
as BOXSS_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='Sky Sports Cricket' then total_programmes_viewed_over_threshold else 0 end) 
as CRISS_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='Sky Sports Darts' then total_programmes_viewed_over_threshold else 0 end) 
as DARTSS_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='Sky Sports Equestrian' then total_programmes_viewed_over_threshold else 0 end) 
as EQUSS_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='Sky Sports Extreme' then total_programmes_viewed_over_threshold else 0 end) 
as EXTSS_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='Sky Sports Fishing' then total_programmes_viewed_over_threshold else 0 end) 
as FISHSS_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='Sky Sports Football' then total_programmes_viewed_over_threshold else 0 end) 
as FOOTSS_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='Sky Sports Golf' then total_programmes_viewed_over_threshold else 0 end) 
as GOLFSS_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='Sky Sports Ice Hockey' then total_programmes_viewed_over_threshold else 0 end) 
as IHSS_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='Sky Sports Motor Sport' then total_programmes_viewed_over_threshold else 0 end) 
as MSPSS_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='Sky Sports Racing' then total_programmes_viewed_over_threshold else 0 end) 
as RACSS_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='Sky Sports Rugby' then total_programmes_viewed_over_threshold else 0 end) 
as RUGSS_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='Sky Sports Snooker/Pool' then total_programmes_viewed_over_threshold else 0 end) 
as SNPSS_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='Sky Sports Tennis' then total_programmes_viewed_over_threshold else 0 end) 
as TENSS_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='Sky Sports Unknown' then total_programmes_viewed_over_threshold else 0 end) 
as UNKSS_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='Sky Sports Watersports' then total_programmes_viewed_over_threshold else 0 end) 
as WATSS_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='Sky Sports Wintersports' then total_programmes_viewed_over_threshold else 0 end) 
as WINSS_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='Sky Sports Wrestling' then total_programmes_viewed_over_threshold else 0 end) 
as WRESS_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='Solheim Cup 2013 Ladies Golf - Sky Sports' then total_programmes_viewed_over_threshold else 0 end) 
as SOLSS_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='South African Home Cricket  2012-2020 Sky Sports' then total_programmes_viewed_over_threshold else 0 end) 
as SACSS_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='Spanish Football 2012/13 To 14/15 - Sky Sports' then total_programmes_viewed_over_threshold else 0 end) 
as SPFSS_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='SPFL - BT Sport' then total_programmes_viewed_over_threshold else 0 end) 
as SPFLBTS_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='SPL - ESPN' then total_programmes_viewed_over_threshold else 0 end) 
as SPLESPN_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='SPL - Sky Sports' then total_programmes_viewed_over_threshold else 0 end) 
as SPLSS_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='Sportfive World Cup 2014 Qualifers - Sky Sports' then total_programmes_viewed_over_threshold else 0 end) 
as SP5SS_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='The boat race - BBC' then total_programmes_viewed_over_threshold else 0 end) 
as BTRBBC_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='The football league show - BBC' then total_programmes_viewed_over_threshold else 0 end) 
as FLSBBC_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='The Masters Golf - BBC' then total_programmes_viewed_over_threshold else 0 end) 
as MGBBC_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='TNA Wrestling Challenge' then total_programmes_viewed_over_threshold else 0 end) 
as TNACHA_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='Tour de France - Eurosport' then total_programmes_viewed_over_threshold else 0 end) 
as TDFEUR_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='Tour de France - ITV' then total_programmes_viewed_over_threshold else 0 end) 
as TDFITV_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='U S Masters Golf 2011-2014 Sky Sports' then total_programmes_viewed_over_threshold else 0 end) 
as USMGSS_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='U.S Open Tennis 2013-2015 Sky Sports' then total_programmes_viewed_over_threshold else 0 end) 
as USOTSS_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='U.S Open/Womens/Seniors/Amateur Golf Sky Sports' then total_programmes_viewed_over_threshold else 0 end) 
as USOGSS_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='UEFA Champions League -  Sky Sports' then total_programmes_viewed_over_threshold else 0 end) 
as CLASS_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='UEFA Champions League -  Sky Sports (non Live)' then total_programmes_viewed_over_threshold else 0 end) 
as CLNSS_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='UEFA Champions League -  Sky Sports (other Live)' then total_programmes_viewed_over_threshold else 0 end) 
as CLOSS_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='UEFA Champions League -  Sky Sports (Tue)' then total_programmes_viewed_over_threshold else 0 end) 
as CLTSS_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='UEFA Champions League -  Sky Sports (Wed)' then total_programmes_viewed_over_threshold else 0 end) 
as CLWSS_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='US Open Tennis - Eurosport' then total_programmes_viewed_over_threshold else 0 end) 
as USOTEUR_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='USA Football - BT Sport' then total_programmes_viewed_over_threshold else 0 end) 
as USFBTS_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='USPGA Championship (2007-2016) Sky Sports' then total_programmes_viewed_over_threshold else 0 end) 
as USPGASS_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='WCQ - ESPN' then total_programmes_viewed_over_threshold else 0 end) 
as WCQESPN_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='Welsh Home International Football 2012-2014 Sky Sports' then total_programmes_viewed_over_threshold else 0 end) 
as WIFSS_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='West Indies Home Cricket 2013-2020 Sky Sports' then total_programmes_viewed_over_threshold else 0 end) 
as WICSS_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='Wimbledon - BBC' then total_programmes_viewed_over_threshold else 0 end) 
as WIMBBC_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='Womens Cricket - I C C Cricket Deal  2012-2015 Sky Sports' then total_programmes_viewed_over_threshold else 0 end) 
as WICCSS_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='World Athletics Championship - More 4' then total_programmes_viewed_over_threshold else 0 end) 
as WACMR4_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='World Club Championship - BBC' then total_programmes_viewed_over_threshold else 0 end) 
as WCLBBBC_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='World Cup Qualifiers - BT Sport' then total_programmes_viewed_over_threshold else 0 end) 
as WCQBTS_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='World Darts Championship 2009-2012 Sky Sports' then total_programmes_viewed_over_threshold else 0 end) 
as WDCSS_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='World snooker championship - BBC' then total_programmes_viewed_over_threshold else 0 end) 
as WSCBBC_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='WWE Sky 1 and 2' then total_programmes_viewed_over_threshold else 0 end) 
as WWES12_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='WWE Sky Sports' then total_programmes_viewed_over_threshold else 0 end) 
as WWESS_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='Africa Cup of Nations - Eurosport' then number_of_events_viewed else 0 end) 
as AFCEUR_Total_Viewing_Events
,sum(case when analysis_right ='Africa Cup of Nations - ITV' then number_of_events_viewed else 0 end) 
as AFCITV_Total_Viewing_Events
,sum(case when analysis_right ='Americas Cup - BBC' then number_of_events_viewed else 0 end) 
as AMCBBC_Total_Viewing_Events
,sum(case when analysis_right ='Asian Tour Golf 2012-2013 Sky Sports' then number_of_events_viewed else 0 end) 
as ATGSS_Total_Viewing_Events
,sum(case when analysis_right ='ATP Tennis Masters Series  2011-2013 Sky Sports' then number_of_events_viewed else 0 end) 
as ATPSS_Total_Viewing_Events
,sum(case when analysis_right ='Australia Home Cricket  2012-2016 Sky Sports' then number_of_events_viewed else 0 end) 
as AHCSS_Total_Viewing_Events
,sum(case when analysis_right ='Australian Football - BT Sport' then number_of_events_viewed else 0 end) 
as AUFBTS_Total_Viewing_Events
,sum(case when analysis_right ='Australian Open Tennis - BBC' then number_of_events_viewed else 0 end) 
as AOTBBC_Total_Viewing_Events
,sum(case when analysis_right ='Australian Open Tennis - Eurosport' then number_of_events_viewed else 0 end) 
as AOTEUR_Total_Viewing_Events
,sum(case when analysis_right ='Aviva Premiership - ESPN' then number_of_events_viewed else 0 end) 
as AVPSS_Total_Viewing_Events
,sum(case when analysis_right ='BBC American Football' then number_of_events_viewed else 0 end) 
as AFBBC_Total_Viewing_Events
,sum(case when analysis_right ='BBC Athletics' then number_of_events_viewed else 0 end) 
as ATHBBC_Total_Viewing_Events
,sum(case when analysis_right ='BBC Boxing' then number_of_events_viewed else 0 end) 
as BOXBBC_Total_Viewing_Events
,sum(case when analysis_right ='BBC Darts' then number_of_events_viewed else 0 end) 
as DRTBBC_Total_Viewing_Events
,sum(case when analysis_right ='BBC Equestrian' then number_of_events_viewed else 0 end) 
as EQUBBC_Total_Viewing_Events
,sum(case when analysis_right ='BBC Football' then number_of_events_viewed else 0 end) 
as FOOTBBC_Total_Viewing_Events
,sum(case when analysis_right ='BBC Golf' then number_of_events_viewed else 0 end) 
as GOLFBBC_Total_Viewing_Events
,sum(case when analysis_right ='BBC Motor Sport' then number_of_events_viewed else 0 end) 
as MSPBBC_Total_Viewing_Events
,sum(case when analysis_right ='BBC Rugby' then number_of_events_viewed else 0 end) 
as RUGBBC_Total_Viewing_Events
,sum(case when analysis_right ='BBC Snooker/Pool' then number_of_events_viewed else 0 end) 
as SNPBBC_Total_Viewing_Events
,sum(case when analysis_right ='BBC Tennis' then number_of_events_viewed else 0 end) 
as TENBBC_Total_Viewing_Events
,sum(case when analysis_right ='BBC Unknown' then number_of_events_viewed else 0 end) 
as UNKBBC_Total_Viewing_Events
,sum(case when analysis_right ='BBC Watersports' then number_of_events_viewed else 0 end) 
as WATBBC_Total_Viewing_Events
,sum(case when analysis_right ='BBC Wintersports' then number_of_events_viewed else 0 end) 
as WINBBC_Total_Viewing_Events
,sum(case when analysis_right ='Boxing  - Channel 5' then number_of_events_viewed else 0 end) 
as BOXCH5_Total_Viewing_Events
,sum(case when analysis_right ='Boxing - Matchroom 2012-2014 Sky Sports' then number_of_events_viewed else 0 end) 
as BOXMSS_Total_Viewing_Events
,sum(case when analysis_right ='Brazil Football - BT Sport' then number_of_events_viewed else 0 end) 
as BFTBTS_Total_Viewing_Events
,sum(case when analysis_right ='British & Irish Lions 2013 - Sky Sports' then number_of_events_viewed else 0 end) 
as BILSS_Total_Viewing_Events
,sum(case when analysis_right ='British Open Golf - BBC' then number_of_events_viewed else 0 end) 
as BOGSS_Total_Viewing_Events
,sum(case when analysis_right ='BT Sport American Football' then number_of_events_viewed else 0 end) 
as AFBTS_Total_Viewing_Events
,sum(case when analysis_right ='BT Sport Athletics' then number_of_events_viewed else 0 end) 
as ATHBTS_Total_Viewing_Events
,sum(case when analysis_right ='BT Sport Baseball' then number_of_events_viewed else 0 end) 
as BASEBTS_Total_Viewing_Events
,sum(case when analysis_right ='BT Sport Basketball' then number_of_events_viewed else 0 end) 
as BASKBTS_Total_Viewing_Events
,sum(case when analysis_right ='BT Sport Boxing' then number_of_events_viewed else 0 end) 
as BOXBTS_Total_Viewing_Events
,sum(case when analysis_right ='BT Sport Cricket' then number_of_events_viewed else 0 end) 
as CRIBTS_Total_Viewing_Events
,sum(case when analysis_right ='BT Sport Equestrian' then number_of_events_viewed else 0 end) 
as EQUBTS_Total_Viewing_Events
,sum(case when analysis_right ='BT Sport Extreme' then number_of_events_viewed else 0 end) 
as EXTBTS_Total_Viewing_Events
,sum(case when analysis_right ='BT Sport Football' then number_of_events_viewed else 0 end) 
as FOOTBTS_Total_Viewing_Events
,sum(case when analysis_right ='BT Sport Motor Sport' then number_of_events_viewed else 0 end) 
as MSPBTS_Total_Viewing_Events
,sum(case when analysis_right ='BT Sport Rugby' then number_of_events_viewed else 0 end) 
as RUGBTS_Total_Viewing_Events
,sum(case when analysis_right ='BT Sport Tennis' then number_of_events_viewed else 0 end) 
as TENBTS_Total_Viewing_Events
,sum(case when analysis_right ='BT Sport Unknown' then number_of_events_viewed else 0 end) 
as UNKBTS_Total_Viewing_Events
,sum(case when analysis_right ='BT Sport Wintersports' then number_of_events_viewed else 0 end) 
as WINBTS_Total_Viewing_Events
,sum(case when analysis_right ='Bundesliga - BT Sport' then number_of_events_viewed else 0 end) 
as BUNBTS_Total_Viewing_Events
,sum(case when analysis_right ='Bundesliga- ESPN' then number_of_events_viewed else 0 end) 
as BUNESPN_Total_Viewing_Events
,sum(case when analysis_right ='Challenge Darts' then number_of_events_viewed else 0 end) 
as DRTCHA_Total_Viewing_Events
,sum(case when analysis_right ='Challenge Extreme' then number_of_events_viewed else 0 end) 
as EXTCHA_Total_Viewing_Events
,sum(case when analysis_right ='Challenge Unknown' then number_of_events_viewed else 0 end) 
as UNKCHA_Total_Viewing_Events
,sum(case when analysis_right ='Challenge Wrestling' then number_of_events_viewed else 0 end) 
as WRECHA_Total_Viewing_Events
,sum(case when analysis_right ='Champions League - ITV' then number_of_events_viewed else 0 end) 
as CHLITV_Total_Viewing_Events
,sum(case when analysis_right ='Champions Trophy - I C C Cricket Deal  2012-2015 Sky Sports' then number_of_events_viewed else 0 end) 
as ICCSS_Total_Viewing_Events
,sum(case when analysis_right ='Channel 4 American Football' then number_of_events_viewed else 0 end) 
as AMCH4_Total_Viewing_Events
,sum(case when analysis_right ='Channel 4 Athletics' then number_of_events_viewed else 0 end) 
as ATHCH4_Total_Viewing_Events
,sum(case when analysis_right ='Channel 4 Boxing' then number_of_events_viewed else 0 end) 
as BOXCH4_Total_Viewing_Events
,sum(case when analysis_right ='Channel 4 Equestrian' then number_of_events_viewed else 0 end) 
as EQUCH4_Total_Viewing_Events
,sum(case when analysis_right ='Channel 4 Extreme' then number_of_events_viewed else 0 end) 
as EXTCH4_Total_Viewing_Events
,sum(case when analysis_right ='Channel 4 Football' then number_of_events_viewed else 0 end) 
as FOOTCH4_Total_Viewing_Events
,sum(case when analysis_right ='Channel 4 Motor Sport' then number_of_events_viewed else 0 end) 
as MSPCH4_Total_Viewing_Events
,sum(case when analysis_right ='Channel 4 Racing' then number_of_events_viewed else 0 end) 
as RACCH4_Total_Viewing_Events
,sum(case when analysis_right ='Channel 4 Unknown' then number_of_events_viewed else 0 end) 
as UNKCH4_Total_Viewing_Events
,sum(case when analysis_right ='Channel 4 Watersports' then number_of_events_viewed else 0 end) 
as WATCH4_Total_Viewing_Events
,sum(case when analysis_right ='Channel 4 Wintersports' then number_of_events_viewed else 0 end) 
as WINCH4_Total_Viewing_Events
,sum(case when analysis_right ='Channel 5 Athletics' then number_of_events_viewed else 0 end) 
as ATHCH5_Total_Viewing_Events
,sum(case when analysis_right ='Channel 5 Boxing' then number_of_events_viewed else 0 end) 
as BOXOCH5_Total_Viewing_Events
,sum(case when analysis_right ='Channel 5 Cricket' then number_of_events_viewed else 0 end) 
as CRICH5_Total_Viewing_Events
,sum(case when analysis_right ='Channel 5 Motor Sport' then number_of_events_viewed else 0 end) 
as MSPCH5_Total_Viewing_Events
,sum(case when analysis_right ='Channel 5 Unknown' then number_of_events_viewed else 0 end) 
as UNKCH5_Total_Viewing_Events
,sum(case when analysis_right ='Channel 5 Wrestling' then number_of_events_viewed else 0 end) 
as WRECH5_Total_Viewing_Events
,sum(case when analysis_right ='Cheltenham Festival - Channel 4' then number_of_events_viewed else 0 end) 
as CHELCH4_Total_Viewing_Events
,sum(case when analysis_right ='Community Shield - ITV' then number_of_events_viewed else 0 end) 
as CMSITV_Total_Viewing_Events
,sum(case when analysis_right ='Confederations Cup - BBC' then number_of_events_viewed else 0 end) 
as CONCBBC_Total_Viewing_Events
,sum(case when analysis_right ='Conference - BT Sport' then number_of_events_viewed else 0 end) 
as CONFBTS_Total_Viewing_Events
,sum(case when analysis_right ='Cycling - La Vuelta ITV' then number_of_events_viewed else 0 end) 
as CLVITV_Total_Viewing_Events
,sum(case when analysis_right ='Cycling - U C I World Tour Sky Sports' then number_of_events_viewed else 0 end) 
as CUCISS_Total_Viewing_Events
,sum(case when analysis_right ='Cycling Tour of Britain - Eurosport' then number_of_events_viewed else 0 end) 
as CTBEUR_Total_Viewing_Events
,sum(case when analysis_right ='Cycling: tour of britain ITV4' then number_of_events_viewed else 0 end) 
as CTCITV_Total_Viewing_Events
,sum(case when analysis_right ='Derby - Channel 4' then number_of_events_viewed else 0 end) 
as DERCH4_Total_Viewing_Events
,sum(case when analysis_right ='ECB (highlights) - Channel 5' then number_of_events_viewed else 0 end) 
as ECBHCH5_Total_Viewing_Events
,sum(case when analysis_right ='ECB Cricket Sky Sports' then number_of_events_viewed else 0 end) 
as GECRSS_Total_Viewing_Events
,sum(case when analysis_right ='ECB non-Test Cricket Sky Sports' then number_of_events_viewed else 0 end) 
as ECBNSS_Total_Viewing_Events
,sum(case when analysis_right ='ECB Test Cricket Sky Sports' then number_of_events_viewed else 0 end) 
as ECBTSS_Total_Viewing_Events
,sum(case when analysis_right ='England Football Internationals - ITV' then number_of_events_viewed else 0 end) 
as GENGITV_Total_Viewing_Events
,sum(case when analysis_right ='England Friendlies (Football) - ITV' then number_of_events_viewed else 0 end) 
as EFRITV_Total_Viewing_Events
,sum(case when analysis_right ='England Rugby Internationals 2010-2015 - Sky Sports' then number_of_events_viewed else 0 end) 
as ENRSS_Total_Viewing_Events
,sum(case when analysis_right ='England World Cup Qualifying (Away) - ITV' then number_of_events_viewed else 0 end) 
as EWQAITV_Total_Viewing_Events
,sum(case when analysis_right ='England World Cup Qualifying (Home) - ITV' then number_of_events_viewed else 0 end) 
as EWQHITV_Total_Viewing_Events
,sum(case when analysis_right ='ESPN American Football' then number_of_events_viewed else 0 end) 
as AMESPN_Total_Viewing_Events
,sum(case when analysis_right ='ESPN Athletics' then number_of_events_viewed else 0 end) 
as ATHESPN_Total_Viewing_Events
,sum(case when analysis_right ='ESPN Baseball' then number_of_events_viewed else 0 end) 
as BASEESPN_Total_Viewing_Events
,sum(case when analysis_right ='ESPN Basketball' then number_of_events_viewed else 0 end) 
as BASKESPN_Total_Viewing_Events
,sum(case when analysis_right ='ESPN Boxing' then number_of_events_viewed else 0 end) 
as BOXESPN_Total_Viewing_Events
,sum(case when analysis_right ='ESPN Cricket' then number_of_events_viewed else 0 end) 
as CRIESPN_Total_Viewing_Events
,sum(case when analysis_right ='ESPN Darts' then number_of_events_viewed else 0 end) 
as DARTESPN_Total_Viewing_Events
,sum(case when analysis_right ='ESPN Equestrian' then number_of_events_viewed else 0 end) 
as EQUESPN_Total_Viewing_Events
,sum(case when analysis_right ='ESPN Extreme' then number_of_events_viewed else 0 end) 
as EXTESPN_Total_Viewing_Events
,sum(case when analysis_right ='ESPN Football' then number_of_events_viewed else 0 end) 
as FOOTESPN_Total_Viewing_Events
,sum(case when analysis_right ='ESPN Golf' then number_of_events_viewed else 0 end) 
as GOLFESPN_Total_Viewing_Events
,sum(case when analysis_right ='ESPN Ice Hockey' then number_of_events_viewed else 0 end) 
as IHESPN_Total_Viewing_Events
,sum(case when analysis_right ='ESPN Motor Sport' then number_of_events_viewed else 0 end) 
as MSPESPN_Total_Viewing_Events
,sum(case when analysis_right ='ESPN Racing' then number_of_events_viewed else 0 end) 
as RACESPN_Total_Viewing_Events
,sum(case when analysis_right ='ESPN Rugby' then number_of_events_viewed else 0 end) 
as RUGESPN_Total_Viewing_Events
,sum(case when analysis_right ='ESPN Snooker/Pool' then number_of_events_viewed else 0 end) 
as SNPESPN_Total_Viewing_Events
,sum(case when analysis_right ='ESPN Tennis' then number_of_events_viewed else 0 end) 
as TENESPN_Total_Viewing_Events
,sum(case when analysis_right ='ESPN Unknown' then number_of_events_viewed else 0 end) 
as UNKESPN_Total_Viewing_Events
,sum(case when analysis_right ='ESPN Watersports' then number_of_events_viewed else 0 end) 
as WATESPN_Total_Viewing_Events
,sum(case when analysis_right ='ESPN Wintersports' then number_of_events_viewed else 0 end) 
as WINESPN_Total_Viewing_Events
,sum(case when analysis_right ='ESPN Wrestling' then number_of_events_viewed else 0 end) 
as WREESPN_Total_Viewing_Events
,sum(case when analysis_right ='Europa League - BT Sport' then number_of_events_viewed else 0 end) 
as ELBTSP_Total_Viewing_Events
,sum(case when analysis_right ='Europa League - ESPN' then number_of_events_viewed else 0 end) 
as ELESPN_Total_Viewing_Events
,sum(case when analysis_right ='Europa League - ITV' then number_of_events_viewed else 0 end) 
as ELITV_Total_Viewing_Events
,sum(case when analysis_right ='European Tour Golf - Sky Sports' then number_of_events_viewed else 0 end) 
as ETGSS_Total_Viewing_Events
,sum(case when analysis_right ='Eurosport American Football' then number_of_events_viewed else 0 end) 
as AMEUR_Total_Viewing_Events
,sum(case when analysis_right ='Eurosport Athletics' then number_of_events_viewed else 0 end) 
as ATHEUR_Total_Viewing_Events
,sum(case when analysis_right ='Eurosport Baseball' then number_of_events_viewed else 0 end) 
as BASEEUR_Total_Viewing_Events
,sum(case when analysis_right ='Eurosport Basketball' then number_of_events_viewed else 0 end) 
as BASKEUR_Total_Viewing_Events
,sum(case when analysis_right ='Eurosport Boxing' then number_of_events_viewed else 0 end) 
as BOXEUR_Total_Viewing_Events
,sum(case when analysis_right ='Eurosport Cricket' then number_of_events_viewed else 0 end) 
as CRIEUR_Total_Viewing_Events
,sum(case when analysis_right ='Eurosport Darts' then number_of_events_viewed else 0 end) 
as DARTEUR_Total_Viewing_Events
,sum(case when analysis_right ='Eurosport Equestrian' then number_of_events_viewed else 0 end) 
as EQUEUR_Total_Viewing_Events
,sum(case when analysis_right ='Eurosport Extreme' then number_of_events_viewed else 0 end) 
as EXTEUR_Total_Viewing_Events
,sum(case when analysis_right ='Eurosport Football' then number_of_events_viewed else 0 end) 
as FOOTEUR_Total_Viewing_Events
,sum(case when analysis_right ='Eurosport Golf' then number_of_events_viewed else 0 end) 
as GOLFEUR_Total_Viewing_Events
,sum(case when analysis_right ='Eurosport Ice Hockey' then number_of_events_viewed else 0 end) 
as IHEUR_Total_Viewing_Events
,sum(case when analysis_right ='Eurosport Motor Sport' then number_of_events_viewed else 0 end) 
as MSPEUR_Total_Viewing_Events
,sum(case when analysis_right ='Eurosport Racing' then number_of_events_viewed else 0 end) 
as RACEUR_Total_Viewing_Events
,sum(case when analysis_right ='Eurosport Snooker/Pool' then number_of_events_viewed else 0 end) 
as SNPEUR_Total_Viewing_Events
,sum(case when analysis_right ='Eurosport Tennis' then number_of_events_viewed else 0 end) 
as TENEUR_Total_Viewing_Events
,sum(case when analysis_right ='Eurosport Unknown' then number_of_events_viewed else 0 end) 
as UNKEUR_Total_Viewing_Events
,sum(case when analysis_right ='Eurosport Watersports' then number_of_events_viewed else 0 end) 
as WATEUR_Total_Viewing_Events
,sum(case when analysis_right ='Eurosport Wintersports' then number_of_events_viewed else 0 end) 
as WINEUR_Total_Viewing_Events
,sum(case when analysis_right ='F1 - BBC' then number_of_events_viewed else 0 end) 
as GF1BBC_Total_Viewing_Events
,sum(case when analysis_right ='F1 - Sky Sports' then number_of_events_viewed else 0 end) 
as GF1SS_Total_Viewing_Events
,sum(case when analysis_right ='F1 (non-Live)- BBC' then number_of_events_viewed else 0 end) 
as F1NBBC_Total_Viewing_Events
,sum(case when analysis_right ='F1 (Practice Live)- BBC' then number_of_events_viewed else 0 end) 
as F1PBBC_Total_Viewing_Events
,sum(case when analysis_right ='F1 (Qualifying Live)- BBC' then number_of_events_viewed else 0 end) 
as F1QBBC_Total_Viewing_Events
,sum(case when analysis_right ='F1 (Race Live)- BBC' then number_of_events_viewed else 0 end) 
as F1RBBC_Total_Viewing_Events
,sum(case when analysis_right ='FA Cup - ESPN' then number_of_events_viewed else 0 end) 
as FACESPN_Total_Viewing_Events
,sum(case when analysis_right ='FA Cup - ITV' then number_of_events_viewed else 0 end) 
as FACITV_Total_Viewing_Events
,sum(case when analysis_right ='Football League - Seasons 2012/13 To 14/15 Sky Sports - Capital 1 Cup' then number_of_events_viewed else 0 end) 
as FLCCSS_Total_Viewing_Events
,sum(case when analysis_right ='Football League - Seasons 2012/13 To 14/15 Sky Sports - others' then number_of_events_viewed else 0 end) 
as FLOTSS_Total_Viewing_Events
,sum(case when analysis_right ='Formula One 2012-2018 - (non-Live) Sky Sports' then number_of_events_viewed else 0 end) 
as F1NSS_Total_Viewing_Events
,sum(case when analysis_right ='Formula One 2012-2018 - (Practice Live) Sky Sports' then number_of_events_viewed else 0 end) 
as F1PSS_Total_Viewing_Events
,sum(case when analysis_right ='Formula One 2012-2018 - (Qualifying Live) Sky Sports' then number_of_events_viewed else 0 end) 
as F1QSS_Total_Viewing_Events
,sum(case when analysis_right ='Formula One 2012-2018 - (Race Live) Sky Sports' then number_of_events_viewed else 0 end) 
as F1RSS_Total_Viewing_Events
,sum(case when analysis_right ='French Open Tennis - Eurosport' then number_of_events_viewed else 0 end) 
as FOTEUR_Total_Viewing_Events
,sum(case when analysis_right ='French Open Tennis - ITV' then number_of_events_viewed else 0 end) 
as FOTITV_Total_Viewing_Events
,sum(case when analysis_right ='Grand National - Channel 4' then number_of_events_viewed else 0 end) 
as GDNCH4_Total_Viewing_Events
,sum(case when analysis_right ='Heineken Cup 2010/11 To 2013/14- Sky Sports' then number_of_events_viewed else 0 end) 
as HECSS_Total_Viewing_Events
,sum(case when analysis_right ='I R B Sevens 2008/09 -2014/15 - Sky Sports' then number_of_events_viewed else 0 end) 
as IRBSS_Total_Viewing_Events
,sum(case when analysis_right ='IAAF World Athletics Championship - Eurosport' then number_of_events_viewed else 0 end) 
as WACEUR_Total_Viewing_Events
,sum(case when analysis_right ='India Home Cricket 2012-2018 Sky Sports' then number_of_events_viewed else 0 end) 
as IHCSS_Total_Viewing_Events
,sum(case when analysis_right ='India Premier League - ITV' then number_of_events_viewed else 0 end) 
as IPLITV_Total_Viewing_Events
,sum(case when analysis_right ='International Freindlies - ESPN' then number_of_events_viewed else 0 end) 
as IFESPN_Total_Viewing_Events
,sum(case when analysis_right ='International Friendlies - BT Sport' then number_of_events_viewed else 0 end) 
as IFBTS_Total_Viewing_Events
,sum(case when analysis_right ='ITV1 Boxing' then number_of_events_viewed else 0 end) 
as BOXITV1_Total_Viewing_Events
,sum(case when analysis_right ='ITV1 Football' then number_of_events_viewed else 0 end) 
as FOOTITV1_Total_Viewing_Events
,sum(case when analysis_right ='ITV1 Motor Sport' then number_of_events_viewed else 0 end) 
as MOTSITV1_Total_Viewing_Events
,sum(case when analysis_right ='ITV1 Rugby' then number_of_events_viewed else 0 end) 
as RUGITV1_Total_Viewing_Events
,sum(case when analysis_right ='ITV1 Snooker/Pool' then number_of_events_viewed else 0 end) 
as SNPITV1_Total_Viewing_Events
,sum(case when analysis_right ='ITV1 Unknown' then number_of_events_viewed else 0 end) 
as UNKITV1_Total_Viewing_Events
,sum(case when analysis_right ='ITV4 Boxing' then number_of_events_viewed else 0 end) 
as BOXITV4_Total_Viewing_Events
,sum(case when analysis_right ='ITV4 Cricket' then number_of_events_viewed else 0 end) 
as CRIITV4_Total_Viewing_Events
,sum(case when analysis_right ='ITV4 Darts' then number_of_events_viewed else 0 end) 
as DARTITV4_Total_Viewing_Events
,sum(case when analysis_right ='ITV4 Extreme' then number_of_events_viewed else 0 end) 
as EXTITV4_Total_Viewing_Events
,sum(case when analysis_right ='ITV4 Football' then number_of_events_viewed else 0 end) 
as FOOTITV4_Total_Viewing_Events
,sum(case when analysis_right ='ITV4 Motor Sport' then number_of_events_viewed else 0 end) 
as MSPITV4_Total_Viewing_Events
,sum(case when analysis_right ='ITV4 Rugby' then number_of_events_viewed else 0 end) 
as RUGITV4_Total_Viewing_Events
,sum(case when analysis_right ='ITV4 Snooker/Pool' then number_of_events_viewed else 0 end) 
as SNPITV4_Total_Viewing_Events
,sum(case when analysis_right ='ITV4 Tennis' then number_of_events_viewed else 0 end) 
as TENITV4_Total_Viewing_Events
,sum(case when analysis_right ='ITV4 Unknown' then number_of_events_viewed else 0 end) 
as UNKITV4_Total_Viewing_Events
,sum(case when analysis_right ='Ligue 1 - BT Sport' then number_of_events_viewed else 0 end) 
as L1BTS_Total_Viewing_Events
,sum(case when analysis_right ='Ligue 1 - ESPN' then number_of_events_viewed else 0 end) 
as L1ESPN_Total_Viewing_Events
,sum(case when analysis_right ='Match of the day - BBC' then number_of_events_viewed else 0 end) 
as MOTDBBC_Total_Viewing_Events
,sum(case when analysis_right ='Matchroom Output Deal  2010-2012 (Other)- Sky Sports' then number_of_events_viewed else 0 end) 
as MROSS_Total_Viewing_Events
,sum(case when analysis_right ='Matchroom Output Deal  2010-2012 (Pool)- Sky Sports' then number_of_events_viewed else 0 end) 
as MRPSS_Total_Viewing_Events
,sum(case when analysis_right ='Matchroom Output Deal  2010-2012 (Snooker)- Sky Sports' then number_of_events_viewed else 0 end) 
as MRSSS_Total_Viewing_Events
,sum(case when analysis_right ='Moto GP BBC' then number_of_events_viewed else 0 end) 
as MGPBBC_Total_Viewing_Events
,sum(case when analysis_right ='NBA - Sky Sports' then number_of_events_viewed else 0 end) 
as NBASS_Total_Viewing_Events
,sum(case when analysis_right ='NFL - BBC' then number_of_events_viewed else 0 end) 
as NFLBBC_Total_Viewing_Events
,sum(case when analysis_right ='NFL - Channel 4' then number_of_events_viewed else 0 end) 
as NFLCH4_Total_Viewing_Events
,sum(case when analysis_right ='NFL Seasons 2012-2015 - Sky Sports' then number_of_events_viewed else 0 end) 
as NFLSS_Total_Viewing_Events
,sum(case when analysis_right ='Northern Ireland Football Association 2012-2014 - Sky Sports' then number_of_events_viewed else 0 end) 
as NIFSS_Total_Viewing_Events
,sum(case when analysis_right ='Oaks - Channel 4' then number_of_events_viewed else 0 end) 
as OAKCH4_Total_Viewing_Events
,sum(case when analysis_right ='Other American Football' then number_of_events_viewed else 0 end) 
as AMOTH_Total_Viewing_Events
,sum(case when analysis_right ='Other Athletics' then number_of_events_viewed else 0 end) 
as ATHOTH_Total_Viewing_Events
,sum(case when analysis_right ='Other Baseball' then number_of_events_viewed else 0 end) 
as BASEOTH_Total_Viewing_Events
,sum(case when analysis_right ='Other Basketball' then number_of_events_viewed else 0 end) 
as BASKOTH_Total_Viewing_Events
,sum(case when analysis_right ='Other Boxing' then number_of_events_viewed else 0 end) 
as BOXOTH_Total_Viewing_Events
,sum(case when analysis_right ='Other Cricket' then number_of_events_viewed else 0 end) 
as CRIOTH_Total_Viewing_Events
,sum(case when analysis_right ='Other Darts' then number_of_events_viewed else 0 end) 
as DARTOTH_Total_Viewing_Events
,sum(case when analysis_right ='Other Equestrian' then number_of_events_viewed else 0 end) 
as EQUOTH_Total_Viewing_Events
,sum(case when analysis_right ='Other Extreme' then number_of_events_viewed else 0 end) 
as EXTOTH_Total_Viewing_Events
,sum(case when analysis_right ='Other Fishing' then number_of_events_viewed else 0 end) 
as FSHOTH_Total_Viewing_Events
,sum(case when analysis_right ='Other Football' then number_of_events_viewed else 0 end) 
as FOOTOTH_Total_Viewing_Events
,sum(case when analysis_right ='Other Golf' then number_of_events_viewed else 0 end) 
as GOLFOTH_Total_Viewing_Events
,sum(case when analysis_right ='Other Ice Hockey' then number_of_events_viewed else 0 end) 
as IHOTH_Total_Viewing_Events
,sum(case when analysis_right ='Other Motor Sport' then number_of_events_viewed else 0 end) 
as MSPOTH_Total_Viewing_Events
,sum(case when analysis_right ='Other Racing' then number_of_events_viewed else 0 end) 
as RACOTH_Total_Viewing_Events
,sum(case when analysis_right ='Other Rugby' then number_of_events_viewed else 0 end) 
as RUGOTH_Total_Viewing_Events
,sum(case when analysis_right ='Other Rugby Internationals - ESPN' then number_of_events_viewed else 0 end) 
as ORUGESPN_Total_Viewing_Events
,sum(case when analysis_right ='Other Snooker/Pool' then number_of_events_viewed else 0 end) 
as OTHSNP_Total_Viewing_Events
,sum(case when analysis_right ='Other Tennis' then number_of_events_viewed else 0 end) 
as OTHTEN_Total_Viewing_Events
,sum(case when analysis_right ='Other Unknown' then number_of_events_viewed else 0 end) 
as OTHUNK_Total_Viewing_Events
,sum(case when analysis_right ='Other Watersports' then number_of_events_viewed else 0 end) 
as OTHWAT_Total_Viewing_Events
,sum(case when analysis_right ='Other Wintersports' then number_of_events_viewed else 0 end) 
as OTHWIN_Total_Viewing_Events
,sum(case when analysis_right ='Other Wrestling' then number_of_events_viewed else 0 end) 
as OTHWRE_Total_Viewing_Events
,sum(case when analysis_right ='PGA Tour Golf 2010-2017 - Sky Sports' then number_of_events_viewed else 0 end) 
as PGASS_Total_Viewing_Events
,sum(case when analysis_right ='Premier League - BT Sport' then number_of_events_viewed else 0 end) 
as PLBTS_Total_Viewing_Events
,sum(case when analysis_right ='Premier League - ESPN' then number_of_events_viewed else 0 end) 
as PLESPN_Total_Viewing_Events
,sum(case when analysis_right ='Premier League Darts 2010-2013 - Sky Sports' then number_of_events_viewed else 0 end) 
as PLDSS_Total_Viewing_Events
,sum(case when analysis_right ='Premier League Football - Sky Sports' then number_of_events_viewed else 0 end) 
as GPLSS_Total_Viewing_Events
,sum(case when analysis_right ='Premier League Football - Sky Sports (Match Choice)' then number_of_events_viewed else 0 end) 
as PLMCSS_Total_Viewing_Events
,sum(case when analysis_right ='Premier League Football - Sky Sports (MNF)' then number_of_events_viewed else 0 end) 
as PLMNFSS_Total_Viewing_Events
,sum(case when analysis_right ='Premier League Football - Sky Sports (non Live)' then number_of_events_viewed else 0 end) 
as PLNLSS_Total_Viewing_Events
,sum(case when analysis_right ='Premier League Football - Sky Sports (other Live)' then number_of_events_viewed else 0 end) 
as PLOLSS_Total_Viewing_Events
,sum(case when analysis_right ='Premier League Football - Sky Sports (Sat Lunchtime)' then number_of_events_viewed else 0 end) 
as PLSLSS_Total_Viewing_Events
,sum(case when analysis_right ='Premier League Football - Sky Sports (Sat Night Live)' then number_of_events_viewed else 0 end) 
as PLSNSS_Total_Viewing_Events
,sum(case when analysis_right ='Premier League Football - Sky Sports (Sun 4pm)' then number_of_events_viewed else 0 end) 
as PLS4SS_Total_Viewing_Events
,sum(case when analysis_right ='Premier League Football - Sky Sports (Sun Lunchtime)' then number_of_events_viewed else 0 end) 
as PLSULSS_Total_Viewing_Events
,sum(case when analysis_right ='Premiership Rugby - Sky Sports' then number_of_events_viewed else 0 end) 
as PRUSS_Total_Viewing_Events
,sum(case when analysis_right ='Republic Of Ireland FA - 2010/11 To 2013/14 - Sky Sports' then number_of_events_viewed else 0 end) 
as ROISS_Total_Viewing_Events
,sum(case when analysis_right ='Royal Ascot - Channel 4' then number_of_events_viewed else 0 end) 
as RASCH4_Total_Viewing_Events
,sum(case when analysis_right ='Rugby Internationals (England) - BBC' then number_of_events_viewed else 0 end) 
as RIEBBC_Total_Viewing_Events
,sum(case when analysis_right ='Rugby Internationals (Ireland) - BBC' then number_of_events_viewed else 0 end) 
as RIIBBC_Total_Viewing_Events
,sum(case when analysis_right ='Rugby Internationals (Scotland) - BBC' then number_of_events_viewed else 0 end) 
as RISBBC_Total_Viewing_Events
,sum(case when analysis_right ='Rugby Internationals (Wales) - BBC' then number_of_events_viewed else 0 end) 
as RIWBBC_Total_Viewing_Events
,sum(case when analysis_right ='Rugby League  Challenge Cup- BBC' then number_of_events_viewed else 0 end) 
as RLCCBBC_Total_Viewing_Events
,sum(case when analysis_right ='Rugby League - Sky Sports' then number_of_events_viewed else 0 end) 
as RLGSS_Total_Viewing_Events
,sum(case when analysis_right ='Rugby League  World Cup- BBC' then number_of_events_viewed else 0 end) 
as RLWCBBC_Total_Viewing_Events
,sum(case when analysis_right ='Sanzar Rugby Union 2011 - 2015 - Sky Sports' then number_of_events_viewed else 0 end) 
as SARUSS_Total_Viewing_Events
,sum(case when analysis_right ='Scottish FA Deal - 2010/11 To 2013/14 - Sky Sports' then number_of_events_viewed else 0 end) 
as SFASS_Total_Viewing_Events
,sum(case when analysis_right ='Serie A - BT Sport' then number_of_events_viewed else 0 end) 
as SABTS_Total_Viewing_Events
,sum(case when analysis_right ='Serie A - ESPN' then number_of_events_viewed else 0 end) 
as SAESPN_Total_Viewing_Events
,sum(case when analysis_right ='SFL - ESPN' then number_of_events_viewed else 0 end) 
as SFLESPN_Total_Viewing_Events
,sum(case when analysis_right ='Six Nations - BBC' then number_of_events_viewed else 0 end) 
as SNRBBC_Total_Viewing_Events
,sum(case when analysis_right ='Sky 1 and Sky 2 Boxing' then number_of_events_viewed else 0 end) 
as BOXS12_Total_Viewing_Events
,sum(case when analysis_right ='Sky 1 and Sky 2 Football' then number_of_events_viewed else 0 end) 
as FOOTS12_Total_Viewing_Events
,sum(case when analysis_right ='Sky 1 and Sky 2 Motor Sport' then number_of_events_viewed else 0 end) 
as MSPS12_Total_Viewing_Events
,sum(case when analysis_right ='Sky 1 and Sky 2 Unknown' then number_of_events_viewed else 0 end) 
as UNKS12_Total_Viewing_Events
,sum(case when analysis_right ='Sky 1 and Sky 2 Wrestling' then number_of_events_viewed else 0 end) 
as WRES12_Total_Viewing_Events
,sum(case when analysis_right ='Sky Sports American Football' then number_of_events_viewed else 0 end) 
as AMSS_Total_Viewing_Events
,sum(case when analysis_right ='Sky Sports Athletics' then number_of_events_viewed else 0 end) 
as ATHSS_Total_Viewing_Events
,sum(case when analysis_right ='Sky Sports Baseball' then number_of_events_viewed else 0 end) 
as BASESS_Total_Viewing_Events
,sum(case when analysis_right ='Sky Sports Basketball' then number_of_events_viewed else 0 end) 
as BASKSS_Total_Viewing_Events
,sum(case when analysis_right ='Sky Sports Boxing' then number_of_events_viewed else 0 end) 
as BOXSS_Total_Viewing_Events
,sum(case when analysis_right ='Sky Sports Cricket' then number_of_events_viewed else 0 end) 
as CRISS_Total_Viewing_Events
,sum(case when analysis_right ='Sky Sports Darts' then number_of_events_viewed else 0 end) 
as DARTSS_Total_Viewing_Events
,sum(case when analysis_right ='Sky Sports Equestrian' then number_of_events_viewed else 0 end) 
as EQUSS_Total_Viewing_Events
,sum(case when analysis_right ='Sky Sports Extreme' then number_of_events_viewed else 0 end) 
as EXTSS_Total_Viewing_Events
,sum(case when analysis_right ='Sky Sports Fishing' then number_of_events_viewed else 0 end) 
as FISHSS_Total_Viewing_Events
,sum(case when analysis_right ='Sky Sports Football' then number_of_events_viewed else 0 end) 
as FOOTSS_Total_Viewing_Events
,sum(case when analysis_right ='Sky Sports Golf' then number_of_events_viewed else 0 end) 
as GOLFSS_Total_Viewing_Events
,sum(case when analysis_right ='Sky Sports Ice Hockey' then number_of_events_viewed else 0 end) 
as IHSS_Total_Viewing_Events
,sum(case when analysis_right ='Sky Sports Motor Sport' then number_of_events_viewed else 0 end) 
as MSPSS_Total_Viewing_Events
,sum(case when analysis_right ='Sky Sports Racing' then number_of_events_viewed else 0 end) 
as RACSS_Total_Viewing_Events
,sum(case when analysis_right ='Sky Sports Rugby' then number_of_events_viewed else 0 end) 
as RUGSS_Total_Viewing_Events
,sum(case when analysis_right ='Sky Sports Snooker/Pool' then number_of_events_viewed else 0 end) 
as SNPSS_Total_Viewing_Events
,sum(case when analysis_right ='Sky Sports Tennis' then number_of_events_viewed else 0 end) 
as TENSS_Total_Viewing_Events
,sum(case when analysis_right ='Sky Sports Unknown' then number_of_events_viewed else 0 end) 
as UNKSS_Total_Viewing_Events
,sum(case when analysis_right ='Sky Sports Watersports' then number_of_events_viewed else 0 end) 
as WATSS_Total_Viewing_Events
,sum(case when analysis_right ='Sky Sports Wintersports' then number_of_events_viewed else 0 end) 
as WINSS_Total_Viewing_Events
,sum(case when analysis_right ='Sky Sports Wrestling' then number_of_events_viewed else 0 end) 
as WRESS_Total_Viewing_Events
,sum(case when analysis_right ='Solheim Cup 2013 Ladies Golf - Sky Sports' then number_of_events_viewed else 0 end) 
as SOLSS_Total_Viewing_Events
,sum(case when analysis_right ='South African Home Cricket  2012-2020 Sky Sports' then number_of_events_viewed else 0 end) 
as SACSS_Total_Viewing_Events
,sum(case when analysis_right ='Spanish Football 2012/13 To 14/15 - Sky Sports' then number_of_events_viewed else 0 end) 
as SPFSS_Total_Viewing_Events
,sum(case when analysis_right ='SPFL - BT Sport' then number_of_events_viewed else 0 end) 
as SPFLBTS_Total_Viewing_Events
,sum(case when analysis_right ='SPL - ESPN' then number_of_events_viewed else 0 end) 
as SPLESPN_Total_Viewing_Events
,sum(case when analysis_right ='SPL - Sky Sports' then number_of_events_viewed else 0 end) 
as SPLSS_Total_Viewing_Events
,sum(case when analysis_right ='Sportfive World Cup 2014 Qualifers - Sky Sports' then number_of_events_viewed else 0 end) 
as SP5SS_Total_Viewing_Events
,sum(case when analysis_right ='The boat race - BBC' then number_of_events_viewed else 0 end) 
as BTRBBC_Total_Viewing_Events
,sum(case when analysis_right ='The football league show - BBC' then number_of_events_viewed else 0 end) 
as FLSBBC_Total_Viewing_Events
,sum(case when analysis_right ='The Masters Golf - BBC' then number_of_events_viewed else 0 end) 
as MGBBC_Total_Viewing_Events
,sum(case when analysis_right ='TNA Wrestling Challenge' then number_of_events_viewed else 0 end) 
as TNACHA_Total_Viewing_Events
,sum(case when analysis_right ='Tour de France - Eurosport' then number_of_events_viewed else 0 end) 
as TDFEUR_Total_Viewing_Events
,sum(case when analysis_right ='Tour de France - ITV' then number_of_events_viewed else 0 end) 
as TDFITV_Total_Viewing_Events
,sum(case when analysis_right ='U S Masters Golf 2011-2014 Sky Sports' then number_of_events_viewed else 0 end) 
as USMGSS_Total_Viewing_Events
,sum(case when analysis_right ='U.S Open Tennis 2013-2015 Sky Sports' then number_of_events_viewed else 0 end) 
as USOTSS_Total_Viewing_Events
,sum(case when analysis_right ='U.S Open/Womens/Seniors/Amateur Golf Sky Sports' then number_of_events_viewed else 0 end) 
as USOGSS_Total_Viewing_Events
,sum(case when analysis_right ='UEFA Champions League -  Sky Sports' then number_of_events_viewed else 0 end) 
as CLASS_Total_Viewing_Events
,sum(case when analysis_right ='UEFA Champions League -  Sky Sports (non Live)' then number_of_events_viewed else 0 end) 
as CLNSS_Total_Viewing_Events
,sum(case when analysis_right ='UEFA Champions League -  Sky Sports (other Live)' then number_of_events_viewed else 0 end) 
as CLOSS_Total_Viewing_Events
,sum(case when analysis_right ='UEFA Champions League -  Sky Sports (Tue)' then number_of_events_viewed else 0 end) 
as CLTSS_Total_Viewing_Events
,sum(case when analysis_right ='UEFA Champions League -  Sky Sports (Wed)' then number_of_events_viewed else 0 end) 
as CLWSS_Total_Viewing_Events
,sum(case when analysis_right ='US Open Tennis - Eurosport' then number_of_events_viewed else 0 end) 
as USOTEUR_Total_Viewing_Events
,sum(case when analysis_right ='USA Football - BT Sport' then number_of_events_viewed else 0 end) 
as USFBTS_Total_Viewing_Events
,sum(case when analysis_right ='USPGA Championship (2007-2016) Sky Sports' then number_of_events_viewed else 0 end) 
as USPGASS_Total_Viewing_Events
,sum(case when analysis_right ='WCQ - ESPN' then number_of_events_viewed else 0 end) 
as WCQESPN_Total_Viewing_Events
,sum(case when analysis_right ='Welsh Home International Football 2012-2014 Sky Sports' then number_of_events_viewed else 0 end) 
as WIFSS_Total_Viewing_Events
,sum(case when analysis_right ='West Indies Home Cricket 2013-2020 Sky Sports' then number_of_events_viewed else 0 end) 
as WICSS_Total_Viewing_Events
,sum(case when analysis_right ='Wimbledon - BBC' then number_of_events_viewed else 0 end) 
as WIMBBC_Total_Viewing_Events
,sum(case when analysis_right ='Womens Cricket - I C C Cricket Deal  2012-2015 Sky Sports' then number_of_events_viewed else 0 end) 
as WICCSS_Total_Viewing_Events
,sum(case when analysis_right ='World Athletics Championship - More 4' then number_of_events_viewed else 0 end) 
as WACMR4_Total_Viewing_Events
,sum(case when analysis_right ='World Club Championship - BBC' then number_of_events_viewed else 0 end) 
as WCLBBBC_Total_Viewing_Events
,sum(case when analysis_right ='World Cup Qualifiers - BT Sport' then number_of_events_viewed else 0 end) 
as WCQBTS_Total_Viewing_Events
,sum(case when analysis_right ='World Darts Championship 2009-2012 Sky Sports' then number_of_events_viewed else 0 end) 
as WDCSS_Total_Viewing_Events
,sum(case when analysis_right ='World snooker championship - BBC' then number_of_events_viewed else 0 end) 
as WSCBBC_Total_Viewing_Events
,sum(case when analysis_right ='WWE Sky 1 and 2' then number_of_events_viewed else 0 end) 
as WWES12_Total_Viewing_Events
,sum(case when analysis_right ='WWE Sky Sports' then number_of_events_viewed else 0 end) 
as WWESS_Total_Viewing_Events

into dbarnett.v250_unannualised_right_activity
from dbarnett.v250_sports_rights_viewed_by_right_overall
group by account_number
;
commit;
--select top 100 * from dbarnett.v250_unannualised_right_activity;

---repeat for Live/Non Live Splits---
---pt2
commit;
CREATE HG INDEX idx1 ON dbarnett.v250_sports_rights_viewed_by_right_and_live_status (account_number);
CREATE HG INDEX idx2 ON dbarnett.v250_sports_rights_viewed_by_right_and_live_status(analysis_right);
commit;
drop table dbarnett.v250_unannualised_right_activity_by_live_non_live;
--select top 100 * from dbarnett.v250_unannualised_right_activity_by_live_non_live;
--livenonlive
select account_number

,sum(case when live=1 and analysis_right ='Africa Cup of Nations - Eurosport' then broadcast_days_viewed else 0 end) 
as AFCEUR_Broadcast_Days_Viewed_LIVE
,sum(case when live=1 and analysis_right ='Africa Cup of Nations - ITV' then broadcast_days_viewed else 0 end) 
as AFCITV_Broadcast_Days_Viewed_LIVE
,sum(case when live=1 and analysis_right ='Americas Cup - BBC' then broadcast_days_viewed else 0 end) 
as AMCBBC_Broadcast_Days_Viewed_LIVE
,sum(case when live=1 and analysis_right ='Asian Tour Golf 2012-2013 Sky Sports' then broadcast_days_viewed else 0 end) 
as ATGSS_Broadcast_Days_Viewed_LIVE
,sum(case when live=1 and analysis_right ='ATP Tennis Masters Series  2011-2013 Sky Sports' then broadcast_days_viewed else 0 end) 
as ATPSS_Broadcast_Days_Viewed_LIVE
,sum(case when live=1 and analysis_right ='Australia Home Cricket  2012-2016 Sky Sports' then broadcast_days_viewed else 0 end) 
as AHCSS_Broadcast_Days_Viewed_LIVE
,sum(case when live=1 and analysis_right ='Australian Football - BT Sport' then broadcast_days_viewed else 0 end) 
as AUFBTS_Broadcast_Days_Viewed_LIVE
,sum(case when live=1 and analysis_right ='Australian Open Tennis - BBC' then broadcast_days_viewed else 0 end) 
as AOTBBC_Broadcast_Days_Viewed_LIVE
,sum(case when live=1 and analysis_right ='Australian Open Tennis - Eurosport' then broadcast_days_viewed else 0 end) 
as AOTEUR_Broadcast_Days_Viewed_LIVE
,sum(case when live=1 and analysis_right ='Aviva Premiership - ESPN' then broadcast_days_viewed else 0 end) 
as AVPSS_Broadcast_Days_Viewed_LIVE
,sum(case when live=1 and analysis_right ='BBC American Football' then broadcast_days_viewed else 0 end) 
as AFBBC_Broadcast_Days_Viewed_LIVE
,sum(case when live=1 and analysis_right ='BBC Athletics' then broadcast_days_viewed else 0 end) 
as ATHBBC_Broadcast_Days_Viewed_LIVE
,sum(case when live=1 and analysis_right ='BBC Boxing' then broadcast_days_viewed else 0 end) 
as BOXBBC_Broadcast_Days_Viewed_LIVE
,sum(case when live=1 and analysis_right ='BBC Darts' then broadcast_days_viewed else 0 end) 
as DRTBBC_Broadcast_Days_Viewed_LIVE
,sum(case when live=1 and analysis_right ='BBC Equestrian' then broadcast_days_viewed else 0 end) 
as EQUBBC_Broadcast_Days_Viewed_LIVE
,sum(case when live=1 and analysis_right ='BBC Football' then broadcast_days_viewed else 0 end) 
as FOOTBBC_Broadcast_Days_Viewed_LIVE
,sum(case when live=1 and analysis_right ='BBC Golf' then broadcast_days_viewed else 0 end) 
as GOLFBBC_Broadcast_Days_Viewed_LIVE
,sum(case when live=1 and analysis_right ='BBC Motor Sport' then broadcast_days_viewed else 0 end) 
as MSPBBC_Broadcast_Days_Viewed_LIVE
,sum(case when live=1 and analysis_right ='BBC Rugby' then broadcast_days_viewed else 0 end) 
as RUGBBC_Broadcast_Days_Viewed_LIVE
,sum(case when live=1 and analysis_right ='BBC Snooker/Pool' then broadcast_days_viewed else 0 end) 
as SNPBBC_Broadcast_Days_Viewed_LIVE
,sum(case when live=1 and analysis_right ='BBC Tennis' then broadcast_days_viewed else 0 end) 
as TENBBC_Broadcast_Days_Viewed_LIVE
,sum(case when live=1 and analysis_right ='BBC Unknown' then broadcast_days_viewed else 0 end) 
as UNKBBC_Broadcast_Days_Viewed_LIVE
,sum(case when live=1 and analysis_right ='BBC Watersports' then broadcast_days_viewed else 0 end) 
as WATBBC_Broadcast_Days_Viewed_LIVE
,sum(case when live=1 and analysis_right ='BBC Wintersports' then broadcast_days_viewed else 0 end) 
as WINBBC_Broadcast_Days_Viewed_LIVE
,sum(case when live=1 and analysis_right ='Boxing  - Channel 5' then broadcast_days_viewed else 0 end) 
as BOXCH5_Broadcast_Days_Viewed_LIVE
,sum(case when live=1 and analysis_right ='Boxing - Matchroom 2012-2014 Sky Sports' then broadcast_days_viewed else 0 end) 
as BOXMSS_Broadcast_Days_Viewed_LIVE
,sum(case when live=1 and analysis_right ='Brazil Football - BT Sport' then broadcast_days_viewed else 0 end) 
as BFTBTS_Broadcast_Days_Viewed_LIVE
,sum(case when live=1 and analysis_right ='British & Irish Lions 2013 - Sky Sports' then broadcast_days_viewed else 0 end) 
as BILSS_Broadcast_Days_Viewed_LIVE
,sum(case when live=1 and analysis_right ='British Open Golf - BBC' then broadcast_days_viewed else 0 end) 
as BOGSS_Broadcast_Days_Viewed_LIVE
,sum(case when live=1 and analysis_right ='BT Sport American Football' then broadcast_days_viewed else 0 end) 
as AFBTS_Broadcast_Days_Viewed_LIVE
,sum(case when live=1 and analysis_right ='BT Sport Athletics' then broadcast_days_viewed else 0 end) 
as ATHBTS_Broadcast_Days_Viewed_LIVE
,sum(case when live=1 and analysis_right ='BT Sport Baseball' then broadcast_days_viewed else 0 end) 
as BASEBTS_Broadcast_Days_Viewed_LIVE
,sum(case when live=1 and analysis_right ='BT Sport Basketball' then broadcast_days_viewed else 0 end) 
as BASKBTS_Broadcast_Days_Viewed_LIVE
,sum(case when live=1 and analysis_right ='BT Sport Boxing' then broadcast_days_viewed else 0 end) 
as BOXBTS_Broadcast_Days_Viewed_LIVE
,sum(case when live=1 and analysis_right ='BT Sport Cricket' then broadcast_days_viewed else 0 end) 
as CRIBTS_Broadcast_Days_Viewed_LIVE
,sum(case when live=1 and analysis_right ='BT Sport Equestrian' then broadcast_days_viewed else 0 end) 
as EQUBTS_Broadcast_Days_Viewed_LIVE
,sum(case when live=1 and analysis_right ='BT Sport Extreme' then broadcast_days_viewed else 0 end) 
as EXTBTS_Broadcast_Days_Viewed_LIVE
,sum(case when live=1 and analysis_right ='BT Sport Football' then broadcast_days_viewed else 0 end) 
as FOOTBTS_Broadcast_Days_Viewed_LIVE
,sum(case when live=1 and analysis_right ='BT Sport Motor Sport' then broadcast_days_viewed else 0 end) 
as MSPBTS_Broadcast_Days_Viewed_LIVE
,sum(case when live=1 and analysis_right ='BT Sport Rugby' then broadcast_days_viewed else 0 end) 
as RUGBTS_Broadcast_Days_Viewed_LIVE
,sum(case when live=1 and analysis_right ='BT Sport Tennis' then broadcast_days_viewed else 0 end) 
as TENBTS_Broadcast_Days_Viewed_LIVE
,sum(case when live=1 and analysis_right ='BT Sport Unknown' then broadcast_days_viewed else 0 end) 
as UNKBTS_Broadcast_Days_Viewed_LIVE
,sum(case when live=1 and analysis_right ='BT Sport Wintersports' then broadcast_days_viewed else 0 end) 
as WINBTS_Broadcast_Days_Viewed_LIVE
,sum(case when live=1 and analysis_right ='Bundesliga - BT Sport' then broadcast_days_viewed else 0 end) 
as BUNBTS_Broadcast_Days_Viewed_LIVE
,sum(case when live=1 and analysis_right ='Bundesliga- ESPN' then broadcast_days_viewed else 0 end) 
as BUNESPN_Broadcast_Days_Viewed_LIVE
,sum(case when live=1 and analysis_right ='Challenge Darts' then broadcast_days_viewed else 0 end) 
as DRTCHA_Broadcast_Days_Viewed_LIVE
,sum(case when live=1 and analysis_right ='Challenge Extreme' then broadcast_days_viewed else 0 end) 
as EXTCHA_Broadcast_Days_Viewed_LIVE
,sum(case when live=1 and analysis_right ='Challenge Unknown' then broadcast_days_viewed else 0 end) 
as UNKCHA_Broadcast_Days_Viewed_LIVE
,sum(case when live=1 and analysis_right ='Challenge Wrestling' then broadcast_days_viewed else 0 end) 
as WRECHA_Broadcast_Days_Viewed_LIVE
,sum(case when live=1 and analysis_right ='Champions League - ITV' then broadcast_days_viewed else 0 end) 
as CHLITV_Broadcast_Days_Viewed_LIVE
,sum(case when live=1 and analysis_right ='Champions Trophy - I C C Cricket Deal  2012-2015 Sky Sports' then broadcast_days_viewed else 0 end) 
as ICCSS_Broadcast_Days_Viewed_LIVE
,sum(case when live=1 and analysis_right ='Channel 4 American Football' then broadcast_days_viewed else 0 end) 
as AMCH4_Broadcast_Days_Viewed_LIVE
,sum(case when live=1 and analysis_right ='Channel 4 Athletics' then broadcast_days_viewed else 0 end) 
as ATHCH4_Broadcast_Days_Viewed_LIVE
,sum(case when live=1 and analysis_right ='Channel 4 Boxing' then broadcast_days_viewed else 0 end) 
as BOXCH4_Broadcast_Days_Viewed_LIVE
,sum(case when live=1 and analysis_right ='Channel 4 Equestrian' then broadcast_days_viewed else 0 end) 
as EQUCH4_Broadcast_Days_Viewed_LIVE
,sum(case when live=1 and analysis_right ='Channel 4 Extreme' then broadcast_days_viewed else 0 end) 
as EXTCH4_Broadcast_Days_Viewed_LIVE
,sum(case when live=1 and analysis_right ='Channel 4 Football' then broadcast_days_viewed else 0 end) 
as FOOTCH4_Broadcast_Days_Viewed_LIVE
,sum(case when live=1 and analysis_right ='Channel 4 Motor Sport' then broadcast_days_viewed else 0 end) 
as MSPCH4_Broadcast_Days_Viewed_LIVE
,sum(case when live=1 and analysis_right ='Channel 4 Racing' then broadcast_days_viewed else 0 end) 
as RACCH4_Broadcast_Days_Viewed_LIVE
,sum(case when live=1 and analysis_right ='Channel 4 Unknown' then broadcast_days_viewed else 0 end) 
as UNKCH4_Broadcast_Days_Viewed_LIVE
,sum(case when live=1 and analysis_right ='Channel 4 Watersports' then broadcast_days_viewed else 0 end) 
as WATCH4_Broadcast_Days_Viewed_LIVE
,sum(case when live=1 and analysis_right ='Channel 4 Wintersports' then broadcast_days_viewed else 0 end) 
as WINCH4_Broadcast_Days_Viewed_LIVE
,sum(case when live=1 and analysis_right ='Channel 5 Athletics' then broadcast_days_viewed else 0 end) 
as ATHCH5_Broadcast_Days_Viewed_LIVE
,sum(case when live=1 and analysis_right ='Channel 5 Boxing' then broadcast_days_viewed else 0 end) 
as BOXOCH5_Broadcast_Days_Viewed_LIVE
,sum(case when live=1 and analysis_right ='Channel 5 Cricket' then broadcast_days_viewed else 0 end) 
as CRICH5_Broadcast_Days_Viewed_LIVE
,sum(case when live=1 and analysis_right ='Channel 5 Motor Sport' then broadcast_days_viewed else 0 end) 
as MSPCH5_Broadcast_Days_Viewed_LIVE
,sum(case when live=1 and analysis_right ='Channel 5 Unknown' then broadcast_days_viewed else 0 end) 
as UNKCH5_Broadcast_Days_Viewed_LIVE
,sum(case when live=1 and analysis_right ='Channel 5 Wrestling' then broadcast_days_viewed else 0 end) 
as WRECH5_Broadcast_Days_Viewed_LIVE
,sum(case when live=1 and analysis_right ='Cheltenham Festival - Channel 4' then broadcast_days_viewed else 0 end) 
as CHELCH4_Broadcast_Days_Viewed_LIVE
,sum(case when live=1 and analysis_right ='Community Shield - ITV' then broadcast_days_viewed else 0 end) 
as CMSITV_Broadcast_Days_Viewed_LIVE
,sum(case when live=1 and analysis_right ='Confederations Cup - BBC' then broadcast_days_viewed else 0 end) 
as CONCBBC_Broadcast_Days_Viewed_LIVE
,sum(case when live=1 and analysis_right ='Conference - BT Sport' then broadcast_days_viewed else 0 end) 
as CONFBTS_Broadcast_Days_Viewed_LIVE
,sum(case when live=1 and analysis_right ='Cycling - La Vuelta ITV' then broadcast_days_viewed else 0 end) 
as CLVITV_Broadcast_Days_Viewed_LIVE
,sum(case when live=1 and analysis_right ='Cycling - U C I World Tour Sky Sports' then broadcast_days_viewed else 0 end) 
as CUCISS_Broadcast_Days_Viewed_LIVE
,sum(case when live=1 and analysis_right ='Cycling Tour of Britain - Eurosport' then broadcast_days_viewed else 0 end) 
as CTBEUR_Broadcast_Days_Viewed_LIVE
,sum(case when live=1 and analysis_right ='Cycling: tour of britain ITV4' then broadcast_days_viewed else 0 end) 
as CTCITV_Broadcast_Days_Viewed_LIVE
,sum(case when live=1 and analysis_right ='Derby - Channel 4' then broadcast_days_viewed else 0 end) 
as DERCH4_Broadcast_Days_Viewed_LIVE
,sum(case when live=1 and analysis_right ='ECB (highlights) - Channel 5' then broadcast_days_viewed else 0 end) 
as ECBHCH5_Broadcast_Days_Viewed_LIVE
,sum(case when live=1 and analysis_right ='ECB Cricket Sky Sports' then broadcast_days_viewed else 0 end) 
as GECRSS_Broadcast_Days_Viewed_LIVE
,sum(case when live=1 and analysis_right ='ECB non-Test Cricket Sky Sports' then broadcast_days_viewed else 0 end) 
as ECBNSS_Broadcast_Days_Viewed_LIVE
,sum(case when live=1 and analysis_right ='ECB Test Cricket Sky Sports' then broadcast_days_viewed else 0 end) 
as ECBTSS_Broadcast_Days_Viewed_LIVE
,sum(case when live=1 and analysis_right ='England Football Internationals - ITV' then broadcast_days_viewed else 0 end) 
as GENGITV_Broadcast_Days_Viewed_LIVE
,sum(case when live=1 and analysis_right ='England Friendlies (Football) - ITV' then broadcast_days_viewed else 0 end) 
as EFRITV_Broadcast_Days_Viewed_LIVE
,sum(case when live=1 and analysis_right ='England Rugby Internationals 2010-2015 - Sky Sports' then broadcast_days_viewed else 0 end) 
as ENRSS_Broadcast_Days_Viewed_LIVE
,sum(case when live=1 and analysis_right ='England World Cup Qualifying (Away) - ITV' then broadcast_days_viewed else 0 end) 
as EWQAITV_Broadcast_Days_Viewed_LIVE
,sum(case when live=1 and analysis_right ='England World Cup Qualifying (Home) - ITV' then broadcast_days_viewed else 0 end) 
as EWQHITV_Broadcast_Days_Viewed_LIVE
,sum(case when live=1 and analysis_right ='ESPN American Football' then broadcast_days_viewed else 0 end) 
as AMESPN_Broadcast_Days_Viewed_LIVE
,sum(case when live=1 and analysis_right ='ESPN Athletics' then broadcast_days_viewed else 0 end) 
as ATHESPN_Broadcast_Days_Viewed_LIVE
,sum(case when live=1 and analysis_right ='ESPN Baseball' then broadcast_days_viewed else 0 end) 
as BASEESPN_Broadcast_Days_Viewed_LIVE
,sum(case when live=1 and analysis_right ='ESPN Basketball' then broadcast_days_viewed else 0 end) 
as BASKESPN_Broadcast_Days_Viewed_LIVE
,sum(case when live=1 and analysis_right ='ESPN Boxing' then broadcast_days_viewed else 0 end) 
as BOXESPN_Broadcast_Days_Viewed_LIVE
,sum(case when live=1 and analysis_right ='ESPN Cricket' then broadcast_days_viewed else 0 end) 
as CRIESPN_Broadcast_Days_Viewed_LIVE
,sum(case when live=1 and analysis_right ='ESPN Darts' then broadcast_days_viewed else 0 end) 
as DARTESPN_Broadcast_Days_Viewed_LIVE
,sum(case when live=1 and analysis_right ='ESPN Equestrian' then broadcast_days_viewed else 0 end) 
as EQUESPN_Broadcast_Days_Viewed_LIVE
,sum(case when live=1 and analysis_right ='ESPN Extreme' then broadcast_days_viewed else 0 end) 
as EXTESPN_Broadcast_Days_Viewed_LIVE
,sum(case when live=1 and analysis_right ='ESPN Football' then broadcast_days_viewed else 0 end) 
as FOOTESPN_Broadcast_Days_Viewed_LIVE
,sum(case when live=1 and analysis_right ='ESPN Golf' then broadcast_days_viewed else 0 end) 
as GOLFESPN_Broadcast_Days_Viewed_LIVE
,sum(case when live=1 and analysis_right ='ESPN Ice Hockey' then broadcast_days_viewed else 0 end) 
as IHESPN_Broadcast_Days_Viewed_LIVE
,sum(case when live=1 and analysis_right ='ESPN Motor Sport' then broadcast_days_viewed else 0 end) 
as MSPESPN_Broadcast_Days_Viewed_LIVE
,sum(case when live=1 and analysis_right ='ESPN Racing' then broadcast_days_viewed else 0 end) 
as RACESPN_Broadcast_Days_Viewed_LIVE
,sum(case when live=1 and analysis_right ='ESPN Rugby' then broadcast_days_viewed else 0 end) 
as RUGESPN_Broadcast_Days_Viewed_LIVE
,sum(case when live=1 and analysis_right ='ESPN Snooker/Pool' then broadcast_days_viewed else 0 end) 
as SNPESPN_Broadcast_Days_Viewed_LIVE
,sum(case when live=1 and analysis_right ='ESPN Tennis' then broadcast_days_viewed else 0 end) 
as TENESPN_Broadcast_Days_Viewed_LIVE
,sum(case when live=1 and analysis_right ='ESPN Unknown' then broadcast_days_viewed else 0 end) 
as UNKESPN_Broadcast_Days_Viewed_LIVE
,sum(case when live=1 and analysis_right ='ESPN Watersports' then broadcast_days_viewed else 0 end) 
as WATESPN_Broadcast_Days_Viewed_LIVE
,sum(case when live=1 and analysis_right ='ESPN Wintersports' then broadcast_days_viewed else 0 end) 
as WINESPN_Broadcast_Days_Viewed_LIVE
,sum(case when live=1 and analysis_right ='ESPN Wrestling' then broadcast_days_viewed else 0 end) 
as WREESPN_Broadcast_Days_Viewed_LIVE
,sum(case when live=1 and analysis_right ='Europa League - BT Sport' then broadcast_days_viewed else 0 end) 
as ELBTSP_Broadcast_Days_Viewed_LIVE
,sum(case when live=1 and analysis_right ='Europa League - ESPN' then broadcast_days_viewed else 0 end) 
as ELESPN_Broadcast_Days_Viewed_LIVE
,sum(case when live=1 and analysis_right ='Europa League - ITV' then broadcast_days_viewed else 0 end) 
as ELITV_Broadcast_Days_Viewed_LIVE
,sum(case when live=1 and analysis_right ='European Tour Golf - Sky Sports' then broadcast_days_viewed else 0 end) 
as ETGSS_Broadcast_Days_Viewed_LIVE
,sum(case when live=1 and analysis_right ='Eurosport American Football' then broadcast_days_viewed else 0 end) 
as AMEUR_Broadcast_Days_Viewed_LIVE
,sum(case when live=1 and analysis_right ='Eurosport Athletics' then broadcast_days_viewed else 0 end) 
as ATHEUR_Broadcast_Days_Viewed_LIVE
,sum(case when live=1 and analysis_right ='Eurosport Baseball' then broadcast_days_viewed else 0 end) 
as BASEEUR_Broadcast_Days_Viewed_LIVE
,sum(case when live=1 and analysis_right ='Eurosport Basketball' then broadcast_days_viewed else 0 end) 
as BASKEUR_Broadcast_Days_Viewed_LIVE
,sum(case when live=1 and analysis_right ='Eurosport Boxing' then broadcast_days_viewed else 0 end) 
as BOXEUR_Broadcast_Days_Viewed_LIVE
,sum(case when live=1 and analysis_right ='Eurosport Cricket' then broadcast_days_viewed else 0 end) 
as CRIEUR_Broadcast_Days_Viewed_LIVE
,sum(case when live=1 and analysis_right ='Eurosport Darts' then broadcast_days_viewed else 0 end) 
as DARTEUR_Broadcast_Days_Viewed_LIVE
,sum(case when live=1 and analysis_right ='Eurosport Equestrian' then broadcast_days_viewed else 0 end) 
as EQUEUR_Broadcast_Days_Viewed_LIVE
,sum(case when live=1 and analysis_right ='Eurosport Extreme' then broadcast_days_viewed else 0 end) 
as EXTEUR_Broadcast_Days_Viewed_LIVE
,sum(case when live=1 and analysis_right ='Eurosport Football' then broadcast_days_viewed else 0 end) 
as FOOTEUR_Broadcast_Days_Viewed_LIVE
,sum(case when live=1 and analysis_right ='Eurosport Golf' then broadcast_days_viewed else 0 end) 
as GOLFEUR_Broadcast_Days_Viewed_LIVE
,sum(case when live=1 and analysis_right ='Eurosport Ice Hockey' then broadcast_days_viewed else 0 end) 
as IHEUR_Broadcast_Days_Viewed_LIVE
,sum(case when live=1 and analysis_right ='Eurosport Motor Sport' then broadcast_days_viewed else 0 end) 
as MSPEUR_Broadcast_Days_Viewed_LIVE
,sum(case when live=1 and analysis_right ='Eurosport Racing' then broadcast_days_viewed else 0 end) 
as RACEUR_Broadcast_Days_Viewed_LIVE
,sum(case when live=1 and analysis_right ='Eurosport Snooker/Pool' then broadcast_days_viewed else 0 end) 
as SNPEUR_Broadcast_Days_Viewed_LIVE
,sum(case when live=1 and analysis_right ='Eurosport Tennis' then broadcast_days_viewed else 0 end) 
as TENEUR_Broadcast_Days_Viewed_LIVE
,sum(case when live=1 and analysis_right ='Eurosport Unknown' then broadcast_days_viewed else 0 end) 
as UNKEUR_Broadcast_Days_Viewed_LIVE
,sum(case when live=1 and analysis_right ='Eurosport Watersports' then broadcast_days_viewed else 0 end) 
as WATEUR_Broadcast_Days_Viewed_LIVE
,sum(case when live=1 and analysis_right ='Eurosport Wintersports' then broadcast_days_viewed else 0 end) 
as WINEUR_Broadcast_Days_Viewed_LIVE
,sum(case when live=1 and analysis_right ='F1 - BBC' then broadcast_days_viewed else 0 end) 
as GF1BBC_Broadcast_Days_Viewed_LIVE
,sum(case when live=1 and analysis_right ='F1 - Sky Sports' then broadcast_days_viewed else 0 end) 
as GF1SS_Broadcast_Days_Viewed_LIVE
,sum(case when live=1 and analysis_right ='F1 (non-Live)- BBC' then broadcast_days_viewed else 0 end) 
as F1NBBC_Broadcast_Days_Viewed_LIVE
,sum(case when live=1 and analysis_right ='F1 (Practice Live)- BBC' then broadcast_days_viewed else 0 end) 
as F1PBBC_Broadcast_Days_Viewed_LIVE
,sum(case when live=1 and analysis_right ='F1 (Qualifying Live)- BBC' then broadcast_days_viewed else 0 end) 
as F1QBBC_Broadcast_Days_Viewed_LIVE
,sum(case when live=1 and analysis_right ='F1 (Race Live)- BBC' then broadcast_days_viewed else 0 end) 
as F1RBBC_Broadcast_Days_Viewed_LIVE
,sum(case when live=1 and analysis_right ='FA Cup - ESPN' then broadcast_days_viewed else 0 end) 
as FACESPN_Broadcast_Days_Viewed_LIVE
,sum(case when live=1 and analysis_right ='FA Cup - ITV' then broadcast_days_viewed else 0 end) 
as FACITV_Broadcast_Days_Viewed_LIVE
,sum(case when live=1 and analysis_right ='Football League - Seasons 2012/13 To 14/15 Sky Sports - Capital 1 Cup' then broadcast_days_viewed else 0 end) 
as FLCCSS_Broadcast_Days_Viewed_LIVE
,sum(case when live=1 and analysis_right ='Football League - Seasons 2012/13 To 14/15 Sky Sports - others' then broadcast_days_viewed else 0 end) 
as FLOTSS_Broadcast_Days_Viewed_LIVE
,sum(case when live=1 and analysis_right ='Formula One 2012-2018 - (non-Live) Sky Sports' then broadcast_days_viewed else 0 end) 
as F1NSS_Broadcast_Days_Viewed_LIVE
,sum(case when live=1 and analysis_right ='Formula One 2012-2018 - (Practice Live) Sky Sports' then broadcast_days_viewed else 0 end) 
as F1PSS_Broadcast_Days_Viewed_LIVE
,sum(case when live=1 and analysis_right ='Formula One 2012-2018 - (Qualifying Live) Sky Sports' then broadcast_days_viewed else 0 end) 
as F1QSS_Broadcast_Days_Viewed_LIVE
,sum(case when live=1 and analysis_right ='Formula One 2012-2018 - (Race Live) Sky Sports' then broadcast_days_viewed else 0 end) 
as F1RSS_Broadcast_Days_Viewed_LIVE
,sum(case when live=1 and analysis_right ='French Open Tennis - Eurosport' then broadcast_days_viewed else 0 end) 
as FOTEUR_Broadcast_Days_Viewed_LIVE
,sum(case when live=1 and analysis_right ='French Open Tennis - ITV' then broadcast_days_viewed else 0 end) 
as FOTITV_Broadcast_Days_Viewed_LIVE
,sum(case when live=1 and analysis_right ='Grand National - Channel 4' then broadcast_days_viewed else 0 end) 
as GDNCH4_Broadcast_Days_Viewed_LIVE
,sum(case when live=1 and analysis_right ='Heineken Cup 2010/11 To 2013/14- Sky Sports' then broadcast_days_viewed else 0 end) 
as HECSS_Broadcast_Days_Viewed_LIVE
,sum(case when live=1 and analysis_right ='I R B Sevens 2008/09 -2014/15 - Sky Sports' then broadcast_days_viewed else 0 end) 
as IRBSS_Broadcast_Days_Viewed_LIVE
,sum(case when live=1 and analysis_right ='IAAF World Athletics Championship - Eurosport' then broadcast_days_viewed else 0 end) 
as WACEUR_Broadcast_Days_Viewed_LIVE
,sum(case when live=1 and analysis_right ='India Home Cricket 2012-2018 Sky Sports' then broadcast_days_viewed else 0 end) 
as IHCSS_Broadcast_Days_Viewed_LIVE
,sum(case when live=1 and analysis_right ='India Premier League - ITV' then broadcast_days_viewed else 0 end) 
as IPLITV_Broadcast_Days_Viewed_LIVE
,sum(case when live=1 and analysis_right ='International Freindlies - ESPN' then broadcast_days_viewed else 0 end) 
as IFESPN_Broadcast_Days_Viewed_LIVE
,sum(case when live=1 and analysis_right ='International Friendlies - BT Sport' then broadcast_days_viewed else 0 end) 
as IFBTS_Broadcast_Days_Viewed_LIVE
,sum(case when live=1 and analysis_right ='ITV1 Boxing' then broadcast_days_viewed else 0 end) 
as BOXITV1_Broadcast_Days_Viewed_LIVE
,sum(case when live=1 and analysis_right ='ITV1 Football' then broadcast_days_viewed else 0 end) 
as FOOTITV1_Broadcast_Days_Viewed_LIVE
,sum(case when live=1 and analysis_right ='ITV1 Motor Sport' then broadcast_days_viewed else 0 end) 
as MOTSITV1_Broadcast_Days_Viewed_LIVE
,sum(case when live=1 and analysis_right ='ITV1 Rugby' then broadcast_days_viewed else 0 end) 
as RUGITV1_Broadcast_Days_Viewed_LIVE
,sum(case when live=1 and analysis_right ='ITV1 Snooker/Pool' then broadcast_days_viewed else 0 end) 
as SNPITV1_Broadcast_Days_Viewed_LIVE
,sum(case when live=1 and analysis_right ='ITV1 Unknown' then broadcast_days_viewed else 0 end) 
as UNKITV1_Broadcast_Days_Viewed_LIVE
,sum(case when live=1 and analysis_right ='ITV4 Boxing' then broadcast_days_viewed else 0 end) 
as BOXITV4_Broadcast_Days_Viewed_LIVE
,sum(case when live=1 and analysis_right ='ITV4 Cricket' then broadcast_days_viewed else 0 end) 
as CRIITV4_Broadcast_Days_Viewed_LIVE
,sum(case when live=1 and analysis_right ='ITV4 Darts' then broadcast_days_viewed else 0 end) 
as DARTITV4_Broadcast_Days_Viewed_LIVE
,sum(case when live=1 and analysis_right ='ITV4 Extreme' then broadcast_days_viewed else 0 end) 
as EXTITV4_Broadcast_Days_Viewed_LIVE
,sum(case when live=1 and analysis_right ='ITV4 Football' then broadcast_days_viewed else 0 end) 
as FOOTITV4_Broadcast_Days_Viewed_LIVE
,sum(case when live=1 and analysis_right ='ITV4 Motor Sport' then broadcast_days_viewed else 0 end) 
as MSPITV4_Broadcast_Days_Viewed_LIVE
,sum(case when live=1 and analysis_right ='ITV4 Rugby' then broadcast_days_viewed else 0 end) 
as RUGITV4_Broadcast_Days_Viewed_LIVE
,sum(case when live=1 and analysis_right ='ITV4 Snooker/Pool' then broadcast_days_viewed else 0 end) 
as SNPITV4_Broadcast_Days_Viewed_LIVE
,sum(case when live=1 and analysis_right ='ITV4 Tennis' then broadcast_days_viewed else 0 end) 
as TENITV4_Broadcast_Days_Viewed_LIVE
,sum(case when live=1 and analysis_right ='ITV4 Unknown' then broadcast_days_viewed else 0 end) 
as UNKITV4_Broadcast_Days_Viewed_LIVE
,sum(case when live=1 and analysis_right ='Ligue 1 - BT Sport' then broadcast_days_viewed else 0 end) 
as L1BTS_Broadcast_Days_Viewed_LIVE
,sum(case when live=1 and analysis_right ='Ligue 1 - ESPN' then broadcast_days_viewed else 0 end) 
as L1ESPN_Broadcast_Days_Viewed_LIVE
,sum(case when live=1 and analysis_right ='Match of the day - BBC' then broadcast_days_viewed else 0 end) 
as MOTDBBC_Broadcast_Days_Viewed_LIVE
,sum(case when live=1 and analysis_right ='Matchroom Output Deal  2010-2012 (Other)- Sky Sports' then broadcast_days_viewed else 0 end) 
as MROSS_Broadcast_Days_Viewed_LIVE
,sum(case when live=1 and analysis_right ='Matchroom Output Deal  2010-2012 (Pool)- Sky Sports' then broadcast_days_viewed else 0 end) 
as MRPSS_Broadcast_Days_Viewed_LIVE
,sum(case when live=1 and analysis_right ='Matchroom Output Deal  2010-2012 (Snooker)- Sky Sports' then broadcast_days_viewed else 0 end) 
as MRSSS_Broadcast_Days_Viewed_LIVE
,sum(case when live=1 and analysis_right ='Moto GP BBC' then broadcast_days_viewed else 0 end) 
as MGPBBC_Broadcast_Days_Viewed_LIVE
,sum(case when live=1 and analysis_right ='NBA - Sky Sports' then broadcast_days_viewed else 0 end) 
as NBASS_Broadcast_Days_Viewed_LIVE
,sum(case when live=1 and analysis_right ='NFL - BBC' then broadcast_days_viewed else 0 end) 
as NFLBBC_Broadcast_Days_Viewed_LIVE
,sum(case when live=1 and analysis_right ='NFL - Channel 4' then broadcast_days_viewed else 0 end) 
as NFLCH4_Broadcast_Days_Viewed_LIVE
,sum(case when live=1 and analysis_right ='NFL Seasons 2012-2015 - Sky Sports' then broadcast_days_viewed else 0 end) 
as NFLSS_Broadcast_Days_Viewed_LIVE
,sum(case when live=1 and analysis_right ='Northern Ireland Football Association 2012-2014 - Sky Sports' then broadcast_days_viewed else 0 end) 
as NIFSS_Broadcast_Days_Viewed_LIVE
,sum(case when live=1 and analysis_right ='Oaks - Channel 4' then broadcast_days_viewed else 0 end) 
as OAKCH4_Broadcast_Days_Viewed_LIVE
,sum(case when live=1 and analysis_right ='Other American Football' then broadcast_days_viewed else 0 end) 
as AMOTH_Broadcast_Days_Viewed_LIVE
,sum(case when live=1 and analysis_right ='Other Athletics' then broadcast_days_viewed else 0 end) 
as ATHOTH_Broadcast_Days_Viewed_LIVE
,sum(case when live=1 and analysis_right ='Other Baseball' then broadcast_days_viewed else 0 end) 
as BASEOTH_Broadcast_Days_Viewed_LIVE
,sum(case when live=1 and analysis_right ='Other Basketball' then broadcast_days_viewed else 0 end) 
as BASKOTH_Broadcast_Days_Viewed_LIVE
,sum(case when live=1 and analysis_right ='Other Boxing' then broadcast_days_viewed else 0 end) 
as BOXOTH_Broadcast_Days_Viewed_LIVE
,sum(case when live=1 and analysis_right ='Other Cricket' then broadcast_days_viewed else 0 end) 
as CRIOTH_Broadcast_Days_Viewed_LIVE
,sum(case when live=1 and analysis_right ='Other Darts' then broadcast_days_viewed else 0 end) 
as DARTOTH_Broadcast_Days_Viewed_LIVE
,sum(case when live=1 and analysis_right ='Other Equestrian' then broadcast_days_viewed else 0 end) 
as EQUOTH_Broadcast_Days_Viewed_LIVE
,sum(case when live=1 and analysis_right ='Other Extreme' then broadcast_days_viewed else 0 end) 
as EXTOTH_Broadcast_Days_Viewed_LIVE
,sum(case when live=1 and analysis_right ='Other Fishing' then broadcast_days_viewed else 0 end) 
as FSHOTH_Broadcast_Days_Viewed_LIVE
,sum(case when live=1 and analysis_right ='Other Football' then broadcast_days_viewed else 0 end) 
as FOOTOTH_Broadcast_Days_Viewed_LIVE
,sum(case when live=1 and analysis_right ='Other Golf' then broadcast_days_viewed else 0 end) 
as GOLFOTH_Broadcast_Days_Viewed_LIVE
,sum(case when live=1 and analysis_right ='Other Ice Hockey' then broadcast_days_viewed else 0 end) 
as IHOTH_Broadcast_Days_Viewed_LIVE
,sum(case when live=1 and analysis_right ='Other Motor Sport' then broadcast_days_viewed else 0 end) 
as MSPOTH_Broadcast_Days_Viewed_LIVE
,sum(case when live=1 and analysis_right ='Other Racing' then broadcast_days_viewed else 0 end) 
as RACOTH_Broadcast_Days_Viewed_LIVE
,sum(case when live=1 and analysis_right ='Other Rugby' then broadcast_days_viewed else 0 end) 
as RUGOTH_Broadcast_Days_Viewed_LIVE
,sum(case when live=1 and analysis_right ='Other Rugby Internationals - ESPN' then broadcast_days_viewed else 0 end) 
as ORUGESPN_Broadcast_Days_Viewed_LIVE
,sum(case when live=1 and analysis_right ='Other Snooker/Pool' then broadcast_days_viewed else 0 end) 
as OTHSNP_Broadcast_Days_Viewed_LIVE
,sum(case when live=1 and analysis_right ='Other Tennis' then broadcast_days_viewed else 0 end) 
as OTHTEN_Broadcast_Days_Viewed_LIVE
,sum(case when live=1 and analysis_right ='Other Unknown' then broadcast_days_viewed else 0 end) 
as OTHUNK_Broadcast_Days_Viewed_LIVE
,sum(case when live=1 and analysis_right ='Other Watersports' then broadcast_days_viewed else 0 end) 
as OTHWAT_Broadcast_Days_Viewed_LIVE
,sum(case when live=1 and analysis_right ='Other Wintersports' then broadcast_days_viewed else 0 end) 
as OTHWIN_Broadcast_Days_Viewed_LIVE
,sum(case when live=1 and analysis_right ='Other Wrestling' then broadcast_days_viewed else 0 end) 
as OTHWRE_Broadcast_Days_Viewed_LIVE
,sum(case when live=1 and analysis_right ='PGA Tour Golf 2010-2017 - Sky Sports' then broadcast_days_viewed else 0 end) 
as PGASS_Broadcast_Days_Viewed_LIVE
,sum(case when live=1 and analysis_right ='Premier League - BT Sport' then broadcast_days_viewed else 0 end) 
as PLBTS_Broadcast_Days_Viewed_LIVE
,sum(case when live=1 and analysis_right ='Premier League - ESPN' then broadcast_days_viewed else 0 end) 
as PLESPN_Broadcast_Days_Viewed_LIVE
,sum(case when live=1 and analysis_right ='Premier League Darts 2010-2013 - Sky Sports' then broadcast_days_viewed else 0 end) 
as PLDSS_Broadcast_Days_Viewed_LIVE
,sum(case when live=1 and analysis_right ='Premier League Football - Sky Sports' then broadcast_days_viewed else 0 end) 
as GPLSS_Broadcast_Days_Viewed_LIVE
,sum(case when live=1 and analysis_right ='Premier League Football - Sky Sports (Match Choice)' then broadcast_days_viewed else 0 end) 
as PLMCSS_Broadcast_Days_Viewed_LIVE
,sum(case when live=1 and analysis_right ='Premier League Football - Sky Sports (MNF)' then broadcast_days_viewed else 0 end) 
as PLMNFSS_Broadcast_Days_Viewed_LIVE
,sum(case when live=1 and analysis_right ='Premier League Football - Sky Sports (non Live)' then broadcast_days_viewed else 0 end) 
as PLNLSS_Broadcast_Days_Viewed_LIVE
,sum(case when live=1 and analysis_right ='Premier League Football - Sky Sports (other Live)' then broadcast_days_viewed else 0 end) 
as PLOLSS_Broadcast_Days_Viewed_LIVE
,sum(case when live=1 and analysis_right ='Premier League Football - Sky Sports (Sat Lunchtime)' then broadcast_days_viewed else 0 end) 
as PLSLSS_Broadcast_Days_Viewed_LIVE
,sum(case when live=1 and analysis_right ='Premier League Football - Sky Sports (Sat Night Live)' then broadcast_days_viewed else 0 end) 
as PLSNSS_Broadcast_Days_Viewed_LIVE
,sum(case when live=1 and analysis_right ='Premier League Football - Sky Sports (Sun 4pm)' then broadcast_days_viewed else 0 end) 
as PLS4SS_Broadcast_Days_Viewed_LIVE
,sum(case when live=1 and analysis_right ='Premier League Football - Sky Sports (Sun Lunchtime)' then broadcast_days_viewed else 0 end) 
as PLSULSS_Broadcast_Days_Viewed_LIVE
,sum(case when live=1 and analysis_right ='Premiership Rugby - Sky Sports' then broadcast_days_viewed else 0 end) 
as PRUSS_Broadcast_Days_Viewed_LIVE
,sum(case when live=1 and analysis_right ='Republic Of Ireland FA - 2010/11 To 2013/14 - Sky Sports' then broadcast_days_viewed else 0 end) 
as ROISS_Broadcast_Days_Viewed_LIVE
,sum(case when live=1 and analysis_right ='Royal Ascot - Channel 4' then broadcast_days_viewed else 0 end) 
as RASCH4_Broadcast_Days_Viewed_LIVE
,sum(case when live=1 and analysis_right ='Rugby Internationals (England) - BBC' then broadcast_days_viewed else 0 end) 
as RIEBBC_Broadcast_Days_Viewed_LIVE
,sum(case when live=1 and analysis_right ='Rugby Internationals (Ireland) - BBC' then broadcast_days_viewed else 0 end) 
as RIIBBC_Broadcast_Days_Viewed_LIVE
,sum(case when live=1 and analysis_right ='Rugby Internationals (Scotland) - BBC' then broadcast_days_viewed else 0 end) 
as RISBBC_Broadcast_Days_Viewed_LIVE
,sum(case when live=1 and analysis_right ='Rugby Internationals (Wales) - BBC' then broadcast_days_viewed else 0 end) 
as RIWBBC_Broadcast_Days_Viewed_LIVE
,sum(case when live=1 and analysis_right ='Rugby League  Challenge Cup- BBC' then broadcast_days_viewed else 0 end) 
as RLCCBBC_Broadcast_Days_Viewed_LIVE
,sum(case when live=1 and analysis_right ='Rugby League - Sky Sports' then broadcast_days_viewed else 0 end) 
as RLGSS_Broadcast_Days_Viewed_LIVE
,sum(case when live=1 and analysis_right ='Rugby League  World Cup- BBC' then broadcast_days_viewed else 0 end) 
as RLWCBBC_Broadcast_Days_Viewed_LIVE
,sum(case when live=1 and analysis_right ='Sanzar Rugby Union 2011 - 2015 - Sky Sports' then broadcast_days_viewed else 0 end) 
as SARUSS_Broadcast_Days_Viewed_LIVE
,sum(case when live=1 and analysis_right ='Scottish FA Deal - 2010/11 To 2013/14 - Sky Sports' then broadcast_days_viewed else 0 end) 
as SFASS_Broadcast_Days_Viewed_LIVE
,sum(case when live=1 and analysis_right ='Serie A - BT Sport' then broadcast_days_viewed else 0 end) 
as SABTS_Broadcast_Days_Viewed_LIVE
,sum(case when live=1 and analysis_right ='Serie A - ESPN' then broadcast_days_viewed else 0 end) 
as SAESPN_Broadcast_Days_Viewed_LIVE
,sum(case when live=1 and analysis_right ='SFL - ESPN' then broadcast_days_viewed else 0 end) 
as SFLESPN_Broadcast_Days_Viewed_LIVE
,sum(case when live=1 and analysis_right ='Six Nations - BBC' then broadcast_days_viewed else 0 end) 
as SNRBBC_Broadcast_Days_Viewed_LIVE
,sum(case when live=1 and analysis_right ='Sky 1 and Sky 2 Boxing' then broadcast_days_viewed else 0 end) 
as BOXS12_Broadcast_Days_Viewed_LIVE
,sum(case when live=1 and analysis_right ='Sky 1 and Sky 2 Football' then broadcast_days_viewed else 0 end) 
as FOOTS12_Broadcast_Days_Viewed_LIVE
,sum(case when live=1 and analysis_right ='Sky 1 and Sky 2 Motor Sport' then broadcast_days_viewed else 0 end) 
as MSPS12_Broadcast_Days_Viewed_LIVE
,sum(case when live=1 and analysis_right ='Sky 1 and Sky 2 Unknown' then broadcast_days_viewed else 0 end) 
as UNKS12_Broadcast_Days_Viewed_LIVE
,sum(case when live=1 and analysis_right ='Sky 1 and Sky 2 Wrestling' then broadcast_days_viewed else 0 end) 
as WRES12_Broadcast_Days_Viewed_LIVE
,sum(case when live=1 and analysis_right ='Sky Sports American Football' then broadcast_days_viewed else 0 end) 
as AMSS_Broadcast_Days_Viewed_LIVE
,sum(case when live=1 and analysis_right ='Sky Sports Athletics' then broadcast_days_viewed else 0 end) 
as ATHSS_Broadcast_Days_Viewed_LIVE
,sum(case when live=1 and analysis_right ='Sky Sports Baseball' then broadcast_days_viewed else 0 end) 
as BASESS_Broadcast_Days_Viewed_LIVE
,sum(case when live=1 and analysis_right ='Sky Sports Basketball' then broadcast_days_viewed else 0 end) 
as BASKSS_Broadcast_Days_Viewed_LIVE
,sum(case when live=1 and analysis_right ='Sky Sports Boxing' then broadcast_days_viewed else 0 end) 
as BOXSS_Broadcast_Days_Viewed_LIVE
,sum(case when live=1 and analysis_right ='Sky Sports Cricket' then broadcast_days_viewed else 0 end) 
as CRISS_Broadcast_Days_Viewed_LIVE
,sum(case when live=1 and analysis_right ='Sky Sports Darts' then broadcast_days_viewed else 0 end) 
as DARTSS_Broadcast_Days_Viewed_LIVE
,sum(case when live=1 and analysis_right ='Sky Sports Equestrian' then broadcast_days_viewed else 0 end) 
as EQUSS_Broadcast_Days_Viewed_LIVE
,sum(case when live=1 and analysis_right ='Sky Sports Extreme' then broadcast_days_viewed else 0 end) 
as EXTSS_Broadcast_Days_Viewed_LIVE
,sum(case when live=1 and analysis_right ='Sky Sports Fishing' then broadcast_days_viewed else 0 end) 
as FISHSS_Broadcast_Days_Viewed_LIVE
,sum(case when live=1 and analysis_right ='Sky Sports Football' then broadcast_days_viewed else 0 end) 
as FOOTSS_Broadcast_Days_Viewed_LIVE
,sum(case when live=1 and analysis_right ='Sky Sports Golf' then broadcast_days_viewed else 0 end) 
as GOLFSS_Broadcast_Days_Viewed_LIVE
,sum(case when live=1 and analysis_right ='Sky Sports Ice Hockey' then broadcast_days_viewed else 0 end) 
as IHSS_Broadcast_Days_Viewed_LIVE
,sum(case when live=1 and analysis_right ='Sky Sports Motor Sport' then broadcast_days_viewed else 0 end) 
as MSPSS_Broadcast_Days_Viewed_LIVE
,sum(case when live=1 and analysis_right ='Sky Sports Racing' then broadcast_days_viewed else 0 end) 
as RACSS_Broadcast_Days_Viewed_LIVE
,sum(case when live=1 and analysis_right ='Sky Sports Rugby' then broadcast_days_viewed else 0 end) 
as RUGSS_Broadcast_Days_Viewed_LIVE
,sum(case when live=1 and analysis_right ='Sky Sports Snooker/Pool' then broadcast_days_viewed else 0 end) 
as SNPSS_Broadcast_Days_Viewed_LIVE
,sum(case when live=1 and analysis_right ='Sky Sports Tennis' then broadcast_days_viewed else 0 end) 
as TENSS_Broadcast_Days_Viewed_LIVE
,sum(case when live=1 and analysis_right ='Sky Sports Unknown' then broadcast_days_viewed else 0 end) 
as UNKSS_Broadcast_Days_Viewed_LIVE
,sum(case when live=1 and analysis_right ='Sky Sports Watersports' then broadcast_days_viewed else 0 end) 
as WATSS_Broadcast_Days_Viewed_LIVE
,sum(case when live=1 and analysis_right ='Sky Sports Wintersports' then broadcast_days_viewed else 0 end) 
as WINSS_Broadcast_Days_Viewed_LIVE
,sum(case when live=1 and analysis_right ='Sky Sports Wrestling' then broadcast_days_viewed else 0 end) 
as WRESS_Broadcast_Days_Viewed_LIVE
,sum(case when live=1 and analysis_right ='Solheim Cup 2013 Ladies Golf - Sky Sports' then broadcast_days_viewed else 0 end) 
as SOLSS_Broadcast_Days_Viewed_LIVE
,sum(case when live=1 and analysis_right ='South African Home Cricket  2012-2020 Sky Sports' then broadcast_days_viewed else 0 end) 
as SACSS_Broadcast_Days_Viewed_LIVE
,sum(case when live=1 and analysis_right ='Spanish Football 2012/13 To 14/15 - Sky Sports' then broadcast_days_viewed else 0 end) 
as SPFSS_Broadcast_Days_Viewed_LIVE
,sum(case when live=1 and analysis_right ='SPFL - BT Sport' then broadcast_days_viewed else 0 end) 
as SPFLBTS_Broadcast_Days_Viewed_LIVE
,sum(case when live=1 and analysis_right ='SPL - ESPN' then broadcast_days_viewed else 0 end) 
as SPLESPN_Broadcast_Days_Viewed_LIVE
,sum(case when live=1 and analysis_right ='SPL - Sky Sports' then broadcast_days_viewed else 0 end) 
as SPLSS_Broadcast_Days_Viewed_LIVE
,sum(case when live=1 and analysis_right ='Sportfive World Cup 2014 Qualifers - Sky Sports' then broadcast_days_viewed else 0 end) 
as SP5SS_Broadcast_Days_Viewed_LIVE
,sum(case when live=1 and analysis_right ='The boat race - BBC' then broadcast_days_viewed else 0 end) 
as BTRBBC_Broadcast_Days_Viewed_LIVE
,sum(case when live=1 and analysis_right ='The football league show - BBC' then broadcast_days_viewed else 0 end) 
as FLSBBC_Broadcast_Days_Viewed_LIVE
,sum(case when live=1 and analysis_right ='The Masters Golf - BBC' then broadcast_days_viewed else 0 end) 
as MGBBC_Broadcast_Days_Viewed_LIVE
,sum(case when live=1 and analysis_right ='TNA Wrestling Challenge' then broadcast_days_viewed else 0 end) 
as TNACHA_Broadcast_Days_Viewed_LIVE
,sum(case when live=1 and analysis_right ='Tour de France - Eurosport' then broadcast_days_viewed else 0 end) 
as TDFEUR_Broadcast_Days_Viewed_LIVE
,sum(case when live=1 and analysis_right ='Tour de France - ITV' then broadcast_days_viewed else 0 end) 
as TDFITV_Broadcast_Days_Viewed_LIVE
,sum(case when live=1 and analysis_right ='U S Masters Golf 2011-2014 Sky Sports' then broadcast_days_viewed else 0 end) 
as USMGSS_Broadcast_Days_Viewed_LIVE
,sum(case when live=1 and analysis_right ='U.S Open Tennis 2013-2015 Sky Sports' then broadcast_days_viewed else 0 end) 
as USOTSS_Broadcast_Days_Viewed_LIVE
,sum(case when live=1 and analysis_right ='U.S Open/Womens/Seniors/Amateur Golf Sky Sports' then broadcast_days_viewed else 0 end) 
as USOGSS_Broadcast_Days_Viewed_LIVE
,sum(case when live=1 and analysis_right ='UEFA Champions League -  Sky Sports' then broadcast_days_viewed else 0 end) 
as CLASS_Broadcast_Days_Viewed_LIVE
,sum(case when live=1 and analysis_right ='UEFA Champions League -  Sky Sports (non Live)' then broadcast_days_viewed else 0 end) 
as CLNSS_Broadcast_Days_Viewed_LIVE
,sum(case when live=1 and analysis_right ='UEFA Champions League -  Sky Sports (other Live)' then broadcast_days_viewed else 0 end) 
as CLOSS_Broadcast_Days_Viewed_LIVE
,sum(case when live=1 and analysis_right ='UEFA Champions League -  Sky Sports (Tue)' then broadcast_days_viewed else 0 end) 
as CLTSS_Broadcast_Days_Viewed_LIVE
,sum(case when live=1 and analysis_right ='UEFA Champions League -  Sky Sports (Wed)' then broadcast_days_viewed else 0 end) 
as CLWSS_Broadcast_Days_Viewed_LIVE
,sum(case when live=1 and analysis_right ='US Open Tennis - Eurosport' then broadcast_days_viewed else 0 end) 
as USOTEUR_Broadcast_Days_Viewed_LIVE
,sum(case when live=1 and analysis_right ='USA Football - BT Sport' then broadcast_days_viewed else 0 end) 
as USFBTS_Broadcast_Days_Viewed_LIVE
,sum(case when live=1 and analysis_right ='USPGA Championship (2007-2016) Sky Sports' then broadcast_days_viewed else 0 end) 
as USPGASS_Broadcast_Days_Viewed_LIVE
,sum(case when live=1 and analysis_right ='WCQ - ESPN' then broadcast_days_viewed else 0 end) 
as WCQESPN_Broadcast_Days_Viewed_LIVE
,sum(case when live=1 and analysis_right ='Welsh Home International Football 2012-2014 Sky Sports' then broadcast_days_viewed else 0 end) 
as WIFSS_Broadcast_Days_Viewed_LIVE
,sum(case when live=1 and analysis_right ='West Indies Home Cricket 2013-2020 Sky Sports' then broadcast_days_viewed else 0 end) 
as WICSS_Broadcast_Days_Viewed_LIVE
,sum(case when live=1 and analysis_right ='Wimbledon - BBC' then broadcast_days_viewed else 0 end) 
as WIMBBC_Broadcast_Days_Viewed_LIVE
,sum(case when live=1 and analysis_right ='Womens Cricket - I C C Cricket Deal  2012-2015 Sky Sports' then broadcast_days_viewed else 0 end) 
as WICCSS_Broadcast_Days_Viewed_LIVE
,sum(case when live=1 and analysis_right ='World Athletics Championship - More 4' then broadcast_days_viewed else 0 end) 
as WACMR4_Broadcast_Days_Viewed_LIVE
,sum(case when live=1 and analysis_right ='World Club Championship - BBC' then broadcast_days_viewed else 0 end) 
as WCLBBBC_Broadcast_Days_Viewed_LIVE
,sum(case when live=1 and analysis_right ='World Cup Qualifiers - BT Sport' then broadcast_days_viewed else 0 end) 
as WCQBTS_Broadcast_Days_Viewed_LIVE
,sum(case when live=1 and analysis_right ='World Darts Championship 2009-2012 Sky Sports' then broadcast_days_viewed else 0 end) 
as WDCSS_Broadcast_Days_Viewed_LIVE
,sum(case when live=1 and analysis_right ='World snooker championship - BBC' then broadcast_days_viewed else 0 end) 
as WSCBBC_Broadcast_Days_Viewed_LIVE
,sum(case when live=1 and analysis_right ='WWE Sky 1 and 2' then broadcast_days_viewed else 0 end) 
as WWES12_Broadcast_Days_Viewed_LIVE
,sum(case when live=1 and analysis_right ='WWE Sky Sports' then broadcast_days_viewed else 0 end) 
as WWESS_Broadcast_Days_Viewed_LIVE
,sum(case when live=0 and analysis_right ='Africa Cup of Nations - Eurosport' then broadcast_days_viewed else 0 end) 
as AFCEUR_Broadcast_Days_ViewedNon_LIVE
,sum(case when live=0 and analysis_right ='Africa Cup of Nations - ITV' then broadcast_days_viewed else 0 end) 
as AFCITV_Broadcast_Days_ViewedNon_LIVE
,sum(case when live=0 and analysis_right ='Americas Cup - BBC' then broadcast_days_viewed else 0 end) 
as AMCBBC_Broadcast_Days_ViewedNon_LIVE
,sum(case when live=0 and analysis_right ='Asian Tour Golf 2012-2013 Sky Sports' then broadcast_days_viewed else 0 end) 
as ATGSS_Broadcast_Days_ViewedNon_LIVE
,sum(case when live=0 and analysis_right ='ATP Tennis Masters Series  2011-2013 Sky Sports' then broadcast_days_viewed else 0 end) 
as ATPSS_Broadcast_Days_ViewedNon_LIVE
,sum(case when live=0 and analysis_right ='Australia Home Cricket  2012-2016 Sky Sports' then broadcast_days_viewed else 0 end) 
as AHCSS_Broadcast_Days_ViewedNon_LIVE
,sum(case when live=0 and analysis_right ='Australian Football - BT Sport' then broadcast_days_viewed else 0 end) 
as AUFBTS_Broadcast_Days_ViewedNon_LIVE
,sum(case when live=0 and analysis_right ='Australian Open Tennis - BBC' then broadcast_days_viewed else 0 end) 
as AOTBBC_Broadcast_Days_ViewedNon_LIVE
,sum(case when live=0 and analysis_right ='Australian Open Tennis - Eurosport' then broadcast_days_viewed else 0 end) 
as AOTEUR_Broadcast_Days_ViewedNon_LIVE
,sum(case when live=0 and analysis_right ='Aviva Premiership - ESPN' then broadcast_days_viewed else 0 end) 
as AVPSS_Broadcast_Days_ViewedNon_LIVE
,sum(case when live=0 and analysis_right ='BBC American Football' then broadcast_days_viewed else 0 end) 
as AFBBC_Broadcast_Days_ViewedNon_LIVE
,sum(case when live=0 and analysis_right ='BBC Athletics' then broadcast_days_viewed else 0 end) 
as ATHBBC_Broadcast_Days_ViewedNon_LIVE
,sum(case when live=0 and analysis_right ='BBC Boxing' then broadcast_days_viewed else 0 end) 
as BOXBBC_Broadcast_Days_ViewedNon_LIVE
,sum(case when live=0 and analysis_right ='BBC Darts' then broadcast_days_viewed else 0 end) 
as DRTBBC_Broadcast_Days_ViewedNon_LIVE
,sum(case when live=0 and analysis_right ='BBC Equestrian' then broadcast_days_viewed else 0 end) 
as EQUBBC_Broadcast_Days_ViewedNon_LIVE
,sum(case when live=0 and analysis_right ='BBC Football' then broadcast_days_viewed else 0 end) 
as FOOTBBC_Broadcast_Days_ViewedNon_LIVE
,sum(case when live=0 and analysis_right ='BBC Golf' then broadcast_days_viewed else 0 end) 
as GOLFBBC_Broadcast_Days_ViewedNon_LIVE
,sum(case when live=0 and analysis_right ='BBC Motor Sport' then broadcast_days_viewed else 0 end) 
as MSPBBC_Broadcast_Days_ViewedNon_LIVE
,sum(case when live=0 and analysis_right ='BBC Rugby' then broadcast_days_viewed else 0 end) 
as RUGBBC_Broadcast_Days_ViewedNon_LIVE
,sum(case when live=0 and analysis_right ='BBC Snooker/Pool' then broadcast_days_viewed else 0 end) 
as SNPBBC_Broadcast_Days_ViewedNon_LIVE
,sum(case when live=0 and analysis_right ='BBC Tennis' then broadcast_days_viewed else 0 end) 
as TENBBC_Broadcast_Days_ViewedNon_LIVE
,sum(case when live=0 and analysis_right ='BBC Unknown' then broadcast_days_viewed else 0 end) 
as UNKBBC_Broadcast_Days_ViewedNon_LIVE
,sum(case when live=0 and analysis_right ='BBC Watersports' then broadcast_days_viewed else 0 end) 
as WATBBC_Broadcast_Days_ViewedNon_LIVE
,sum(case when live=0 and analysis_right ='BBC Wintersports' then broadcast_days_viewed else 0 end) 
as WINBBC_Broadcast_Days_ViewedNon_LIVE
,sum(case when live=0 and analysis_right ='Boxing  - Channel 5' then broadcast_days_viewed else 0 end) 
as BOXCH5_Broadcast_Days_ViewedNon_LIVE
,sum(case when live=0 and analysis_right ='Boxing - Matchroom 2012-2014 Sky Sports' then broadcast_days_viewed else 0 end) 
as BOXMSS_Broadcast_Days_ViewedNon_LIVE
,sum(case when live=0 and analysis_right ='Brazil Football - BT Sport' then broadcast_days_viewed else 0 end) 
as BFTBTS_Broadcast_Days_ViewedNon_LIVE
,sum(case when live=0 and analysis_right ='British & Irish Lions 2013 - Sky Sports' then broadcast_days_viewed else 0 end) 
as BILSS_Broadcast_Days_ViewedNon_LIVE
,sum(case when live=0 and analysis_right ='British Open Golf - BBC' then broadcast_days_viewed else 0 end) 
as BOGSS_Broadcast_Days_ViewedNon_LIVE
,sum(case when live=0 and analysis_right ='BT Sport American Football' then broadcast_days_viewed else 0 end) 
as AFBTS_Broadcast_Days_ViewedNon_LIVE
,sum(case when live=0 and analysis_right ='BT Sport Athletics' then broadcast_days_viewed else 0 end) 
as ATHBTS_Broadcast_Days_ViewedNon_LIVE
,sum(case when live=0 and analysis_right ='BT Sport Baseball' then broadcast_days_viewed else 0 end) 
as BASEBTS_Broadcast_Days_ViewedNon_LIVE
,sum(case when live=0 and analysis_right ='BT Sport Basketball' then broadcast_days_viewed else 0 end) 
as BASKBTS_Broadcast_Days_ViewedNon_LIVE
,sum(case when live=0 and analysis_right ='BT Sport Boxing' then broadcast_days_viewed else 0 end) 
as BOXBTS_Broadcast_Days_ViewedNon_LIVE
,sum(case when live=0 and analysis_right ='BT Sport Cricket' then broadcast_days_viewed else 0 end) 
as CRIBTS_Broadcast_Days_ViewedNon_LIVE
,sum(case when live=0 and analysis_right ='BT Sport Equestrian' then broadcast_days_viewed else 0 end) 
as EQUBTS_Broadcast_Days_ViewedNon_LIVE
,sum(case when live=0 and analysis_right ='BT Sport Extreme' then broadcast_days_viewed else 0 end) 
as EXTBTS_Broadcast_Days_ViewedNon_LIVE
,sum(case when live=0 and analysis_right ='BT Sport Football' then broadcast_days_viewed else 0 end) 
as FOOTBTS_Broadcast_Days_ViewedNon_LIVE
,sum(case when live=0 and analysis_right ='BT Sport Motor Sport' then broadcast_days_viewed else 0 end) 
as MSPBTS_Broadcast_Days_ViewedNon_LIVE
,sum(case when live=0 and analysis_right ='BT Sport Rugby' then broadcast_days_viewed else 0 end) 
as RUGBTS_Broadcast_Days_ViewedNon_LIVE
,sum(case when live=0 and analysis_right ='BT Sport Tennis' then broadcast_days_viewed else 0 end) 
as TENBTS_Broadcast_Days_ViewedNon_LIVE
,sum(case when live=0 and analysis_right ='BT Sport Unknown' then broadcast_days_viewed else 0 end) 
as UNKBTS_Broadcast_Days_ViewedNon_LIVE
,sum(case when live=0 and analysis_right ='BT Sport Wintersports' then broadcast_days_viewed else 0 end) 
as WINBTS_Broadcast_Days_ViewedNon_LIVE
,sum(case when live=0 and analysis_right ='Bundesliga - BT Sport' then broadcast_days_viewed else 0 end) 
as BUNBTS_Broadcast_Days_ViewedNon_LIVE
,sum(case when live=0 and analysis_right ='Bundesliga- ESPN' then broadcast_days_viewed else 0 end) 
as BUNESPN_Broadcast_Days_ViewedNon_LIVE
,sum(case when live=0 and analysis_right ='Challenge Darts' then broadcast_days_viewed else 0 end) 
as DRTCHA_Broadcast_Days_ViewedNon_LIVE
,sum(case when live=0 and analysis_right ='Challenge Extreme' then broadcast_days_viewed else 0 end) 
as EXTCHA_Broadcast_Days_ViewedNon_LIVE
,sum(case when live=0 and analysis_right ='Challenge Unknown' then broadcast_days_viewed else 0 end) 
as UNKCHA_Broadcast_Days_ViewedNon_LIVE
,sum(case when live=0 and analysis_right ='Challenge Wrestling' then broadcast_days_viewed else 0 end) 
as WRECHA_Broadcast_Days_ViewedNon_LIVE
,sum(case when live=0 and analysis_right ='Champions League - ITV' then broadcast_days_viewed else 0 end) 
as CHLITV_Broadcast_Days_ViewedNon_LIVE
,sum(case when live=0 and analysis_right ='Champions Trophy - I C C Cricket Deal  2012-2015 Sky Sports' then broadcast_days_viewed else 0 end) 
as ICCSS_Broadcast_Days_ViewedNon_LIVE
,sum(case when live=0 and analysis_right ='Channel 4 American Football' then broadcast_days_viewed else 0 end) 
as AMCH4_Broadcast_Days_ViewedNon_LIVE
,sum(case when live=0 and analysis_right ='Channel 4 Athletics' then broadcast_days_viewed else 0 end) 
as ATHCH4_Broadcast_Days_ViewedNon_LIVE
,sum(case when live=0 and analysis_right ='Channel 4 Boxing' then broadcast_days_viewed else 0 end) 
as BOXCH4_Broadcast_Days_ViewedNon_LIVE
,sum(case when live=0 and analysis_right ='Channel 4 Equestrian' then broadcast_days_viewed else 0 end) 
as EQUCH4_Broadcast_Days_ViewedNon_LIVE
,sum(case when live=0 and analysis_right ='Channel 4 Extreme' then broadcast_days_viewed else 0 end) 
as EXTCH4_Broadcast_Days_ViewedNon_LIVE
,sum(case when live=0 and analysis_right ='Channel 4 Football' then broadcast_days_viewed else 0 end) 
as FOOTCH4_Broadcast_Days_ViewedNon_LIVE
,sum(case when live=0 and analysis_right ='Channel 4 Motor Sport' then broadcast_days_viewed else 0 end) 
as MSPCH4_Broadcast_Days_ViewedNon_LIVE
,sum(case when live=0 and analysis_right ='Channel 4 Racing' then broadcast_days_viewed else 0 end) 
as RACCH4_Broadcast_Days_ViewedNon_LIVE
,sum(case when live=0 and analysis_right ='Channel 4 Unknown' then broadcast_days_viewed else 0 end) 
as UNKCH4_Broadcast_Days_ViewedNon_LIVE
,sum(case when live=0 and analysis_right ='Channel 4 Watersports' then broadcast_days_viewed else 0 end) 
as WATCH4_Broadcast_Days_ViewedNon_LIVE
,sum(case when live=0 and analysis_right ='Channel 4 Wintersports' then broadcast_days_viewed else 0 end) 
as WINCH4_Broadcast_Days_ViewedNon_LIVE
,sum(case when live=0 and analysis_right ='Channel 5 Athletics' then broadcast_days_viewed else 0 end) 
as ATHCH5_Broadcast_Days_ViewedNon_LIVE
,sum(case when live=0 and analysis_right ='Channel 5 Boxing' then broadcast_days_viewed else 0 end) 
as BOXOCH5_Broadcast_Days_ViewedNon_LIVE
,sum(case when live=0 and analysis_right ='Channel 5 Cricket' then broadcast_days_viewed else 0 end) 
as CRICH5_Broadcast_Days_ViewedNon_LIVE
,sum(case when live=0 and analysis_right ='Channel 5 Motor Sport' then broadcast_days_viewed else 0 end) 
as MSPCH5_Broadcast_Days_ViewedNon_LIVE
,sum(case when live=0 and analysis_right ='Channel 5 Unknown' then broadcast_days_viewed else 0 end) 
as UNKCH5_Broadcast_Days_ViewedNon_LIVE
,sum(case when live=0 and analysis_right ='Channel 5 Wrestling' then broadcast_days_viewed else 0 end) 
as WRECH5_Broadcast_Days_ViewedNon_LIVE
,sum(case when live=0 and analysis_right ='Cheltenham Festival - Channel 4' then broadcast_days_viewed else 0 end) 
as CHELCH4_Broadcast_Days_ViewedNon_LIVE
,sum(case when live=0 and analysis_right ='Community Shield - ITV' then broadcast_days_viewed else 0 end) 
as CMSITV_Broadcast_Days_ViewedNon_LIVE
,sum(case when live=0 and analysis_right ='Confederations Cup - BBC' then broadcast_days_viewed else 0 end) 
as CONCBBC_Broadcast_Days_ViewedNon_LIVE
,sum(case when live=0 and analysis_right ='Conference - BT Sport' then broadcast_days_viewed else 0 end) 
as CONFBTS_Broadcast_Days_ViewedNon_LIVE
,sum(case when live=0 and analysis_right ='Cycling - La Vuelta ITV' then broadcast_days_viewed else 0 end) 
as CLVITV_Broadcast_Days_ViewedNon_LIVE
,sum(case when live=0 and analysis_right ='Cycling - U C I World Tour Sky Sports' then broadcast_days_viewed else 0 end) 
as CUCISS_Broadcast_Days_ViewedNon_LIVE
,sum(case when live=0 and analysis_right ='Cycling Tour of Britain - Eurosport' then broadcast_days_viewed else 0 end) 
as CTBEUR_Broadcast_Days_ViewedNon_LIVE
,sum(case when live=0 and analysis_right ='Cycling: tour of britain ITV4' then broadcast_days_viewed else 0 end) 
as CTCITV_Broadcast_Days_ViewedNon_LIVE
,sum(case when live=0 and analysis_right ='Derby - Channel 4' then broadcast_days_viewed else 0 end) 
as DERCH4_Broadcast_Days_ViewedNon_LIVE
,sum(case when live=0 and analysis_right ='ECB (highlights) - Channel 5' then broadcast_days_viewed else 0 end) 
as ECBHCH5_Broadcast_Days_ViewedNon_LIVE
,sum(case when live=0 and analysis_right ='ECB Cricket Sky Sports' then broadcast_days_viewed else 0 end) 
as GECRSS_Broadcast_Days_ViewedNon_LIVE
,sum(case when live=0 and analysis_right ='ECB non-Test Cricket Sky Sports' then broadcast_days_viewed else 0 end) 
as ECBNSS_Broadcast_Days_ViewedNon_LIVE
,sum(case when live=0 and analysis_right ='ECB Test Cricket Sky Sports' then broadcast_days_viewed else 0 end) 
as ECBTSS_Broadcast_Days_ViewedNon_LIVE
,sum(case when live=0 and analysis_right ='England Football Internationals - ITV' then broadcast_days_viewed else 0 end) 
as GENGITV_Broadcast_Days_ViewedNon_LIVE
,sum(case when live=0 and analysis_right ='England Friendlies (Football) - ITV' then broadcast_days_viewed else 0 end) 
as EFRITV_Broadcast_Days_ViewedNon_LIVE
,sum(case when live=0 and analysis_right ='England Rugby Internationals 2010-2015 - Sky Sports' then broadcast_days_viewed else 0 end) 
as ENRSS_Broadcast_Days_ViewedNon_LIVE
,sum(case when live=0 and analysis_right ='England World Cup Qualifying (Away) - ITV' then broadcast_days_viewed else 0 end) 
as EWQAITV_Broadcast_Days_ViewedNon_LIVE
,sum(case when live=0 and analysis_right ='England World Cup Qualifying (Home) - ITV' then broadcast_days_viewed else 0 end) 
as EWQHITV_Broadcast_Days_ViewedNon_LIVE
,sum(case when live=0 and analysis_right ='ESPN American Football' then broadcast_days_viewed else 0 end) 
as AMESPN_Broadcast_Days_ViewedNon_LIVE
,sum(case when live=0 and analysis_right ='ESPN Athletics' then broadcast_days_viewed else 0 end) 
as ATHESPN_Broadcast_Days_ViewedNon_LIVE
,sum(case when live=0 and analysis_right ='ESPN Baseball' then broadcast_days_viewed else 0 end) 
as BASEESPN_Broadcast_Days_ViewedNon_LIVE
,sum(case when live=0 and analysis_right ='ESPN Basketball' then broadcast_days_viewed else 0 end) 
as BASKESPN_Broadcast_Days_ViewedNon_LIVE
,sum(case when live=0 and analysis_right ='ESPN Boxing' then broadcast_days_viewed else 0 end) 
as BOXESPN_Broadcast_Days_ViewedNon_LIVE
,sum(case when live=0 and analysis_right ='ESPN Cricket' then broadcast_days_viewed else 0 end) 
as CRIESPN_Broadcast_Days_ViewedNon_LIVE
,sum(case when live=0 and analysis_right ='ESPN Darts' then broadcast_days_viewed else 0 end) 
as DARTESPN_Broadcast_Days_ViewedNon_LIVE
,sum(case when live=0 and analysis_right ='ESPN Equestrian' then broadcast_days_viewed else 0 end) 
as EQUESPN_Broadcast_Days_ViewedNon_LIVE
,sum(case when live=0 and analysis_right ='ESPN Extreme' then broadcast_days_viewed else 0 end) 
as EXTESPN_Broadcast_Days_ViewedNon_LIVE
,sum(case when live=0 and analysis_right ='ESPN Football' then broadcast_days_viewed else 0 end) 
as FOOTESPN_Broadcast_Days_ViewedNon_LIVE
,sum(case when live=0 and analysis_right ='ESPN Golf' then broadcast_days_viewed else 0 end) 
as GOLFESPN_Broadcast_Days_ViewedNon_LIVE
,sum(case when live=0 and analysis_right ='ESPN Ice Hockey' then broadcast_days_viewed else 0 end) 
as IHESPN_Broadcast_Days_ViewedNon_LIVE
,sum(case when live=0 and analysis_right ='ESPN Motor Sport' then broadcast_days_viewed else 0 end) 
as MSPESPN_Broadcast_Days_ViewedNon_LIVE
,sum(case when live=0 and analysis_right ='ESPN Racing' then broadcast_days_viewed else 0 end) 
as RACESPN_Broadcast_Days_ViewedNon_LIVE
,sum(case when live=0 and analysis_right ='ESPN Rugby' then broadcast_days_viewed else 0 end) 
as RUGESPN_Broadcast_Days_ViewedNon_LIVE
,sum(case when live=0 and analysis_right ='ESPN Snooker/Pool' then broadcast_days_viewed else 0 end) 
as SNPESPN_Broadcast_Days_ViewedNon_LIVE
,sum(case when live=0 and analysis_right ='ESPN Tennis' then broadcast_days_viewed else 0 end) 
as TENESPN_Broadcast_Days_ViewedNon_LIVE
,sum(case when live=0 and analysis_right ='ESPN Unknown' then broadcast_days_viewed else 0 end) 
as UNKESPN_Broadcast_Days_ViewedNon_LIVE
,sum(case when live=0 and analysis_right ='ESPN Watersports' then broadcast_days_viewed else 0 end) 
as WATESPN_Broadcast_Days_ViewedNon_LIVE
,sum(case when live=0 and analysis_right ='ESPN Wintersports' then broadcast_days_viewed else 0 end) 
as WINESPN_Broadcast_Days_ViewedNon_LIVE
,sum(case when live=0 and analysis_right ='ESPN Wrestling' then broadcast_days_viewed else 0 end) 
as WREESPN_Broadcast_Days_ViewedNon_LIVE
,sum(case when live=0 and analysis_right ='Europa League - BT Sport' then broadcast_days_viewed else 0 end) 
as ELBTSP_Broadcast_Days_ViewedNon_LIVE
,sum(case when live=0 and analysis_right ='Europa League - ESPN' then broadcast_days_viewed else 0 end) 
as ELESPN_Broadcast_Days_ViewedNon_LIVE
,sum(case when live=0 and analysis_right ='Europa League - ITV' then broadcast_days_viewed else 0 end) 
as ELITV_Broadcast_Days_ViewedNon_LIVE
,sum(case when live=0 and analysis_right ='European Tour Golf - Sky Sports' then broadcast_days_viewed else 0 end) 
as ETGSS_Broadcast_Days_ViewedNon_LIVE
,sum(case when live=0 and analysis_right ='Eurosport American Football' then broadcast_days_viewed else 0 end) 
as AMEUR_Broadcast_Days_ViewedNon_LIVE
,sum(case when live=0 and analysis_right ='Eurosport Athletics' then broadcast_days_viewed else 0 end) 
as ATHEUR_Broadcast_Days_ViewedNon_LIVE
,sum(case when live=0 and analysis_right ='Eurosport Baseball' then broadcast_days_viewed else 0 end) 
as BASEEUR_Broadcast_Days_ViewedNon_LIVE
,sum(case when live=0 and analysis_right ='Eurosport Basketball' then broadcast_days_viewed else 0 end) 
as BASKEUR_Broadcast_Days_ViewedNon_LIVE
,sum(case when live=0 and analysis_right ='Eurosport Boxing' then broadcast_days_viewed else 0 end) 
as BOXEUR_Broadcast_Days_ViewedNon_LIVE
,sum(case when live=0 and analysis_right ='Eurosport Cricket' then broadcast_days_viewed else 0 end) 
as CRIEUR_Broadcast_Days_ViewedNon_LIVE
,sum(case when live=0 and analysis_right ='Eurosport Darts' then broadcast_days_viewed else 0 end) 
as DARTEUR_Broadcast_Days_ViewedNon_LIVE
,sum(case when live=0 and analysis_right ='Eurosport Equestrian' then broadcast_days_viewed else 0 end) 
as EQUEUR_Broadcast_Days_ViewedNon_LIVE
,sum(case when live=0 and analysis_right ='Eurosport Extreme' then broadcast_days_viewed else 0 end) 
as EXTEUR_Broadcast_Days_ViewedNon_LIVE
,sum(case when live=0 and analysis_right ='Eurosport Football' then broadcast_days_viewed else 0 end) 
as FOOTEUR_Broadcast_Days_ViewedNon_LIVE
,sum(case when live=0 and analysis_right ='Eurosport Golf' then broadcast_days_viewed else 0 end) 
as GOLFEUR_Broadcast_Days_ViewedNon_LIVE
,sum(case when live=0 and analysis_right ='Eurosport Ice Hockey' then broadcast_days_viewed else 0 end) 
as IHEUR_Broadcast_Days_ViewedNon_LIVE
,sum(case when live=0 and analysis_right ='Eurosport Motor Sport' then broadcast_days_viewed else 0 end) 
as MSPEUR_Broadcast_Days_ViewedNon_LIVE
,sum(case when live=0 and analysis_right ='Eurosport Racing' then broadcast_days_viewed else 0 end) 
as RACEUR_Broadcast_Days_ViewedNon_LIVE
,sum(case when live=0 and analysis_right ='Eurosport Snooker/Pool' then broadcast_days_viewed else 0 end) 
as SNPEUR_Broadcast_Days_ViewedNon_LIVE
,sum(case when live=0 and analysis_right ='Eurosport Tennis' then broadcast_days_viewed else 0 end) 
as TENEUR_Broadcast_Days_ViewedNon_LIVE
,sum(case when live=0 and analysis_right ='Eurosport Unknown' then broadcast_days_viewed else 0 end) 
as UNKEUR_Broadcast_Days_ViewedNon_LIVE
,sum(case when live=0 and analysis_right ='Eurosport Watersports' then broadcast_days_viewed else 0 end) 
as WATEUR_Broadcast_Days_ViewedNon_LIVE
,sum(case when live=0 and analysis_right ='Eurosport Wintersports' then broadcast_days_viewed else 0 end) 
as WINEUR_Broadcast_Days_ViewedNon_LIVE
,sum(case when live=0 and analysis_right ='F1 - BBC' then broadcast_days_viewed else 0 end) 
as GF1BBC_Broadcast_Days_ViewedNon_LIVE
,sum(case when live=0 and analysis_right ='F1 - Sky Sports' then broadcast_days_viewed else 0 end) 
as GF1SS_Broadcast_Days_ViewedNon_LIVE
,sum(case when live=0 and analysis_right ='F1 (non-Live)- BBC' then broadcast_days_viewed else 0 end) 
as F1NBBC_Broadcast_Days_ViewedNon_LIVE
,sum(case when live=0 and analysis_right ='F1 (Practice Live)- BBC' then broadcast_days_viewed else 0 end) 
as F1PBBC_Broadcast_Days_ViewedNon_LIVE
,sum(case when live=0 and analysis_right ='F1 (Qualifying Live)- BBC' then broadcast_days_viewed else 0 end) 
as F1QBBC_Broadcast_Days_ViewedNon_LIVE
,sum(case when live=0 and analysis_right ='F1 (Race Live)- BBC' then broadcast_days_viewed else 0 end) 
as F1RBBC_Broadcast_Days_ViewedNon_LIVE
,sum(case when live=0 and analysis_right ='FA Cup - ESPN' then broadcast_days_viewed else 0 end) 
as FACESPN_Broadcast_Days_ViewedNon_LIVE
,sum(case when live=0 and analysis_right ='FA Cup - ITV' then broadcast_days_viewed else 0 end) 
as FACITV_Broadcast_Days_ViewedNon_LIVE
,sum(case when live=0 and analysis_right ='Football League - Seasons 2012/13 To 14/15 Sky Sports - Capital 1 Cup' then broadcast_days_viewed else 0 end) 
as FLCCSS_Broadcast_Days_ViewedNon_LIVE
,sum(case when live=0 and analysis_right ='Football League - Seasons 2012/13 To 14/15 Sky Sports - others' then broadcast_days_viewed else 0 end) 
as FLOTSS_Broadcast_Days_ViewedNon_LIVE
,sum(case when live=0 and analysis_right ='Formula One 2012-2018 - (non-Live) Sky Sports' then broadcast_days_viewed else 0 end) 
as F1NSS_Broadcast_Days_ViewedNon_LIVE
,sum(case when live=0 and analysis_right ='Formula One 2012-2018 - (Practice Live) Sky Sports' then broadcast_days_viewed else 0 end) 
as F1PSS_Broadcast_Days_ViewedNon_LIVE
,sum(case when live=0 and analysis_right ='Formula One 2012-2018 - (Qualifying Live) Sky Sports' then broadcast_days_viewed else 0 end) 
as F1QSS_Broadcast_Days_ViewedNon_LIVE
,sum(case when live=0 and analysis_right ='Formula One 2012-2018 - (Race Live) Sky Sports' then broadcast_days_viewed else 0 end) 
as F1RSS_Broadcast_Days_ViewedNon_LIVE
,sum(case when live=0 and analysis_right ='French Open Tennis - Eurosport' then broadcast_days_viewed else 0 end) 
as FOTEUR_Broadcast_Days_ViewedNon_LIVE
,sum(case when live=0 and analysis_right ='French Open Tennis - ITV' then broadcast_days_viewed else 0 end) 
as FOTITV_Broadcast_Days_ViewedNon_LIVE
,sum(case when live=0 and analysis_right ='Grand National - Channel 4' then broadcast_days_viewed else 0 end) 
as GDNCH4_Broadcast_Days_ViewedNon_LIVE
,sum(case when live=0 and analysis_right ='Heineken Cup 2010/11 To 2013/14- Sky Sports' then broadcast_days_viewed else 0 end) 
as HECSS_Broadcast_Days_ViewedNon_LIVE
,sum(case when live=0 and analysis_right ='I R B Sevens 2008/09 -2014/15 - Sky Sports' then broadcast_days_viewed else 0 end) 
as IRBSS_Broadcast_Days_ViewedNon_LIVE
,sum(case when live=0 and analysis_right ='IAAF World Athletics Championship - Eurosport' then broadcast_days_viewed else 0 end) 
as WACEUR_Broadcast_Days_ViewedNon_LIVE
,sum(case when live=0 and analysis_right ='India Home Cricket 2012-2018 Sky Sports' then broadcast_days_viewed else 0 end) 
as IHCSS_Broadcast_Days_ViewedNon_LIVE
,sum(case when live=0 and analysis_right ='India Premier League - ITV' then broadcast_days_viewed else 0 end) 
as IPLITV_Broadcast_Days_ViewedNon_LIVE
,sum(case when live=0 and analysis_right ='International Freindlies - ESPN' then broadcast_days_viewed else 0 end) 
as IFESPN_Broadcast_Days_ViewedNon_LIVE
,sum(case when live=0 and analysis_right ='International Friendlies - BT Sport' then broadcast_days_viewed else 0 end) 
as IFBTS_Broadcast_Days_ViewedNon_LIVE
,sum(case when live=0 and analysis_right ='ITV1 Boxing' then broadcast_days_viewed else 0 end) 
as BOXITV1_Broadcast_Days_ViewedNon_LIVE
,sum(case when live=0 and analysis_right ='ITV1 Football' then broadcast_days_viewed else 0 end) 
as FOOTITV1_Broadcast_Days_ViewedNon_LIVE
,sum(case when live=0 and analysis_right ='ITV1 Motor Sport' then broadcast_days_viewed else 0 end) 
as MOTSITV1_Broadcast_Days_ViewedNon_LIVE
,sum(case when live=0 and analysis_right ='ITV1 Rugby' then broadcast_days_viewed else 0 end) 
as RUGITV1_Broadcast_Days_ViewedNon_LIVE
,sum(case when live=0 and analysis_right ='ITV1 Snooker/Pool' then broadcast_days_viewed else 0 end) 
as SNPITV1_Broadcast_Days_ViewedNon_LIVE
,sum(case when live=0 and analysis_right ='ITV1 Unknown' then broadcast_days_viewed else 0 end) 
as UNKITV1_Broadcast_Days_ViewedNon_LIVE
,sum(case when live=0 and analysis_right ='ITV4 Boxing' then broadcast_days_viewed else 0 end) 
as BOXITV4_Broadcast_Days_ViewedNon_LIVE
,sum(case when live=0 and analysis_right ='ITV4 Cricket' then broadcast_days_viewed else 0 end) 
as CRIITV4_Broadcast_Days_ViewedNon_LIVE
,sum(case when live=0 and analysis_right ='ITV4 Darts' then broadcast_days_viewed else 0 end) 
as DARTITV4_Broadcast_Days_ViewedNon_LIVE
,sum(case when live=0 and analysis_right ='ITV4 Extreme' then broadcast_days_viewed else 0 end) 
as EXTITV4_Broadcast_Days_ViewedNon_LIVE
,sum(case when live=0 and analysis_right ='ITV4 Football' then broadcast_days_viewed else 0 end) 
as FOOTITV4_Broadcast_Days_ViewedNon_LIVE
,sum(case when live=0 and analysis_right ='ITV4 Motor Sport' then broadcast_days_viewed else 0 end) 
as MSPITV4_Broadcast_Days_ViewedNon_LIVE
,sum(case when live=0 and analysis_right ='ITV4 Rugby' then broadcast_days_viewed else 0 end) 
as RUGITV4_Broadcast_Days_ViewedNon_LIVE
,sum(case when live=0 and analysis_right ='ITV4 Snooker/Pool' then broadcast_days_viewed else 0 end) 
as SNPITV4_Broadcast_Days_ViewedNon_LIVE
,sum(case when live=0 and analysis_right ='ITV4 Tennis' then broadcast_days_viewed else 0 end) 
as TENITV4_Broadcast_Days_ViewedNon_LIVE
,sum(case when live=0 and analysis_right ='ITV4 Unknown' then broadcast_days_viewed else 0 end) 
as UNKITV4_Broadcast_Days_ViewedNon_LIVE
,sum(case when live=0 and analysis_right ='Ligue 1 - BT Sport' then broadcast_days_viewed else 0 end) 
as L1BTS_Broadcast_Days_ViewedNon_LIVE
,sum(case when live=0 and analysis_right ='Ligue 1 - ESPN' then broadcast_days_viewed else 0 end) 
as L1ESPN_Broadcast_Days_ViewedNon_LIVE
,sum(case when live=0 and analysis_right ='Match of the day - BBC' then broadcast_days_viewed else 0 end) 
as MOTDBBC_Broadcast_Days_ViewedNon_LIVE
,sum(case when live=0 and analysis_right ='Matchroom Output Deal  2010-2012 (Other)- Sky Sports' then broadcast_days_viewed else 0 end) 
as MROSS_Broadcast_Days_ViewedNon_LIVE
,sum(case when live=0 and analysis_right ='Matchroom Output Deal  2010-2012 (Pool)- Sky Sports' then broadcast_days_viewed else 0 end) 
as MRPSS_Broadcast_Days_ViewedNon_LIVE
,sum(case when live=0 and analysis_right ='Matchroom Output Deal  2010-2012 (Snooker)- Sky Sports' then broadcast_days_viewed else 0 end) 
as MRSSS_Broadcast_Days_ViewedNon_LIVE
,sum(case when live=0 and analysis_right ='Moto GP BBC' then broadcast_days_viewed else 0 end) 
as MGPBBC_Broadcast_Days_ViewedNon_LIVE
,sum(case when live=0 and analysis_right ='NBA - Sky Sports' then broadcast_days_viewed else 0 end) 
as NBASS_Broadcast_Days_ViewedNon_LIVE
,sum(case when live=0 and analysis_right ='NFL - BBC' then broadcast_days_viewed else 0 end) 
as NFLBBC_Broadcast_Days_ViewedNon_LIVE
,sum(case when live=0 and analysis_right ='NFL - Channel 4' then broadcast_days_viewed else 0 end) 
as NFLCH4_Broadcast_Days_ViewedNon_LIVE
,sum(case when live=0 and analysis_right ='NFL Seasons 2012-2015 - Sky Sports' then broadcast_days_viewed else 0 end) 
as NFLSS_Broadcast_Days_ViewedNon_LIVE
,sum(case when live=0 and analysis_right ='Northern Ireland Football Association 2012-2014 - Sky Sports' then broadcast_days_viewed else 0 end) 
as NIFSS_Broadcast_Days_ViewedNon_LIVE
,sum(case when live=0 and analysis_right ='Oaks - Channel 4' then broadcast_days_viewed else 0 end) 
as OAKCH4_Broadcast_Days_ViewedNon_LIVE
,sum(case when live=0 and analysis_right ='Other American Football' then broadcast_days_viewed else 0 end) 
as AMOTH_Broadcast_Days_ViewedNon_LIVE
,sum(case when live=0 and analysis_right ='Other Athletics' then broadcast_days_viewed else 0 end) 
as ATHOTH_Broadcast_Days_ViewedNon_LIVE
,sum(case when live=0 and analysis_right ='Other Baseball' then broadcast_days_viewed else 0 end) 
as BASEOTH_Broadcast_Days_ViewedNon_LIVE
,sum(case when live=0 and analysis_right ='Other Basketball' then broadcast_days_viewed else 0 end) 
as BASKOTH_Broadcast_Days_ViewedNon_LIVE
,sum(case when live=0 and analysis_right ='Other Boxing' then broadcast_days_viewed else 0 end) 
as BOXOTH_Broadcast_Days_ViewedNon_LIVE
,sum(case when live=0 and analysis_right ='Other Cricket' then broadcast_days_viewed else 0 end) 
as CRIOTH_Broadcast_Days_ViewedNon_LIVE
,sum(case when live=0 and analysis_right ='Other Darts' then broadcast_days_viewed else 0 end) 
as DARTOTH_Broadcast_Days_ViewedNon_LIVE
,sum(case when live=0 and analysis_right ='Other Equestrian' then broadcast_days_viewed else 0 end) 
as EQUOTH_Broadcast_Days_ViewedNon_LIVE
,sum(case when live=0 and analysis_right ='Other Extreme' then broadcast_days_viewed else 0 end) 
as EXTOTH_Broadcast_Days_ViewedNon_LIVE
,sum(case when live=0 and analysis_right ='Other Fishing' then broadcast_days_viewed else 0 end) 
as FSHOTH_Broadcast_Days_ViewedNon_LIVE
,sum(case when live=0 and analysis_right ='Other Football' then broadcast_days_viewed else 0 end) 
as FOOTOTH_Broadcast_Days_ViewedNon_LIVE
,sum(case when live=0 and analysis_right ='Other Golf' then broadcast_days_viewed else 0 end) 
as GOLFOTH_Broadcast_Days_ViewedNon_LIVE
,sum(case when live=0 and analysis_right ='Other Ice Hockey' then broadcast_days_viewed else 0 end) 
as IHOTH_Broadcast_Days_ViewedNon_LIVE
,sum(case when live=0 and analysis_right ='Other Motor Sport' then broadcast_days_viewed else 0 end) 
as MSPOTH_Broadcast_Days_ViewedNon_LIVE
,sum(case when live=0 and analysis_right ='Other Racing' then broadcast_days_viewed else 0 end) 
as RACOTH_Broadcast_Days_ViewedNon_LIVE
,sum(case when live=0 and analysis_right ='Other Rugby' then broadcast_days_viewed else 0 end) 
as RUGOTH_Broadcast_Days_ViewedNon_LIVE
,sum(case when live=0 and analysis_right ='Other Rugby Internationals - ESPN' then broadcast_days_viewed else 0 end) 
as ORUGESPN_Broadcast_Days_ViewedNon_LIVE
,sum(case when live=0 and analysis_right ='Other Snooker/Pool' then broadcast_days_viewed else 0 end) 
as OTHSNP_Broadcast_Days_ViewedNon_LIVE
,sum(case when live=0 and analysis_right ='Other Tennis' then broadcast_days_viewed else 0 end) 
as OTHTEN_Broadcast_Days_ViewedNon_LIVE
,sum(case when live=0 and analysis_right ='Other Unknown' then broadcast_days_viewed else 0 end) 
as OTHUNK_Broadcast_Days_ViewedNon_LIVE
,sum(case when live=0 and analysis_right ='Other Watersports' then broadcast_days_viewed else 0 end) 
as OTHWAT_Broadcast_Days_ViewedNon_LIVE
,sum(case when live=0 and analysis_right ='Other Wintersports' then broadcast_days_viewed else 0 end) 
as OTHWIN_Broadcast_Days_ViewedNon_LIVE
,sum(case when live=0 and analysis_right ='Other Wrestling' then broadcast_days_viewed else 0 end) 
as OTHWRE_Broadcast_Days_ViewedNon_LIVE
,sum(case when live=0 and analysis_right ='PGA Tour Golf 2010-2017 - Sky Sports' then broadcast_days_viewed else 0 end) 
as PGASS_Broadcast_Days_ViewedNon_LIVE
,sum(case when live=0 and analysis_right ='Premier League - BT Sport' then broadcast_days_viewed else 0 end) 
as PLBTS_Broadcast_Days_ViewedNon_LIVE
,sum(case when live=0 and analysis_right ='Premier League - ESPN' then broadcast_days_viewed else 0 end) 
as PLESPN_Broadcast_Days_ViewedNon_LIVE
,sum(case when live=0 and analysis_right ='Premier League Darts 2010-2013 - Sky Sports' then broadcast_days_viewed else 0 end) 
as PLDSS_Broadcast_Days_ViewedNon_LIVE
,sum(case when live=0 and analysis_right ='Premier League Football - Sky Sports' then broadcast_days_viewed else 0 end) 
as GPLSS_Broadcast_Days_ViewedNon_LIVE
,sum(case when live=0 and analysis_right ='Premier League Football - Sky Sports (Match Choice)' then broadcast_days_viewed else 0 end) 
as PLMCSS_Broadcast_Days_ViewedNon_LIVE
,sum(case when live=0 and analysis_right ='Premier League Football - Sky Sports (MNF)' then broadcast_days_viewed else 0 end) 
as PLMNFSS_Broadcast_Days_ViewedNon_LIVE
,sum(case when live=0 and analysis_right ='Premier League Football - Sky Sports (non Live)' then broadcast_days_viewed else 0 end) 
as PLNLSS_Broadcast_Days_ViewedNon_LIVE
,sum(case when live=0 and analysis_right ='Premier League Football - Sky Sports (other Live)' then broadcast_days_viewed else 0 end) 
as PLOLSS_Broadcast_Days_ViewedNon_LIVE
,sum(case when live=0 and analysis_right ='Premier League Football - Sky Sports (Sat Lunchtime)' then broadcast_days_viewed else 0 end) 
as PLSLSS_Broadcast_Days_ViewedNon_LIVE
,sum(case when live=0 and analysis_right ='Premier League Football - Sky Sports (Sat Night Live)' then broadcast_days_viewed else 0 end) 
as PLSNSS_Broadcast_Days_ViewedNon_LIVE
,sum(case when live=0 and analysis_right ='Premier League Football - Sky Sports (Sun 4pm)' then broadcast_days_viewed else 0 end) 
as PLS4SS_Broadcast_Days_ViewedNon_LIVE
,sum(case when live=0 and analysis_right ='Premier League Football - Sky Sports (Sun Lunchtime)' then broadcast_days_viewed else 0 end) 
as PLSULSS_Broadcast_Days_ViewedNon_LIVE
,sum(case when live=0 and analysis_right ='Premiership Rugby - Sky Sports' then broadcast_days_viewed else 0 end) 
as PRUSS_Broadcast_Days_ViewedNon_LIVE
,sum(case when live=0 and analysis_right ='Republic Of Ireland FA - 2010/11 To 2013/14 - Sky Sports' then broadcast_days_viewed else 0 end) 
as ROISS_Broadcast_Days_ViewedNon_LIVE
,sum(case when live=0 and analysis_right ='Royal Ascot - Channel 4' then broadcast_days_viewed else 0 end) 
as RASCH4_Broadcast_Days_ViewedNon_LIVE
,sum(case when live=0 and analysis_right ='Rugby Internationals (England) - BBC' then broadcast_days_viewed else 0 end) 
as RIEBBC_Broadcast_Days_ViewedNon_LIVE
,sum(case when live=0 and analysis_right ='Rugby Internationals (Ireland) - BBC' then broadcast_days_viewed else 0 end) 
as RIIBBC_Broadcast_Days_ViewedNon_LIVE
,sum(case when live=0 and analysis_right ='Rugby Internationals (Scotland) - BBC' then broadcast_days_viewed else 0 end) 
as RISBBC_Broadcast_Days_ViewedNon_LIVE
,sum(case when live=0 and analysis_right ='Rugby Internationals (Wales) - BBC' then broadcast_days_viewed else 0 end) 
as RIWBBC_Broadcast_Days_ViewedNon_LIVE
,sum(case when live=0 and analysis_right ='Rugby League  Challenge Cup- BBC' then broadcast_days_viewed else 0 end) 
as RLCCBBC_Broadcast_Days_ViewedNon_LIVE
,sum(case when live=0 and analysis_right ='Rugby League - Sky Sports' then broadcast_days_viewed else 0 end) 
as RLGSS_Broadcast_Days_ViewedNon_LIVE
,sum(case when live=0 and analysis_right ='Rugby League  World Cup- BBC' then broadcast_days_viewed else 0 end) 
as RLWCBBC_Broadcast_Days_ViewedNon_LIVE
,sum(case when live=0 and analysis_right ='Sanzar Rugby Union 2011 - 2015 - Sky Sports' then broadcast_days_viewed else 0 end) 
as SARUSS_Broadcast_Days_ViewedNon_LIVE
,sum(case when live=0 and analysis_right ='Scottish FA Deal - 2010/11 To 2013/14 - Sky Sports' then broadcast_days_viewed else 0 end) 
as SFASS_Broadcast_Days_ViewedNon_LIVE
,sum(case when live=0 and analysis_right ='Serie A - BT Sport' then broadcast_days_viewed else 0 end) 
as SABTS_Broadcast_Days_ViewedNon_LIVE
,sum(case when live=0 and analysis_right ='Serie A - ESPN' then broadcast_days_viewed else 0 end) 
as SAESPN_Broadcast_Days_ViewedNon_LIVE
,sum(case when live=0 and analysis_right ='SFL - ESPN' then broadcast_days_viewed else 0 end) 
as SFLESPN_Broadcast_Days_ViewedNon_LIVE
,sum(case when live=0 and analysis_right ='Six Nations - BBC' then broadcast_days_viewed else 0 end) 
as SNRBBC_Broadcast_Days_ViewedNon_LIVE
,sum(case when live=0 and analysis_right ='Sky 1 and Sky 2 Boxing' then broadcast_days_viewed else 0 end) 
as BOXS12_Broadcast_Days_ViewedNon_LIVE
,sum(case when live=0 and analysis_right ='Sky 1 and Sky 2 Football' then broadcast_days_viewed else 0 end) 
as FOOTS12_Broadcast_Days_ViewedNon_LIVE
,sum(case when live=0 and analysis_right ='Sky 1 and Sky 2 Motor Sport' then broadcast_days_viewed else 0 end) 
as MSPS12_Broadcast_Days_ViewedNon_LIVE
,sum(case when live=0 and analysis_right ='Sky 1 and Sky 2 Unknown' then broadcast_days_viewed else 0 end) 
as UNKS12_Broadcast_Days_ViewedNon_LIVE
,sum(case when live=0 and analysis_right ='Sky 1 and Sky 2 Wrestling' then broadcast_days_viewed else 0 end) 
as WRES12_Broadcast_Days_ViewedNon_LIVE
,sum(case when live=0 and analysis_right ='Sky Sports American Football' then broadcast_days_viewed else 0 end) 
as AMSS_Broadcast_Days_ViewedNon_LIVE
,sum(case when live=0 and analysis_right ='Sky Sports Athletics' then broadcast_days_viewed else 0 end) 
as ATHSS_Broadcast_Days_ViewedNon_LIVE
,sum(case when live=0 and analysis_right ='Sky Sports Baseball' then broadcast_days_viewed else 0 end) 
as BASESS_Broadcast_Days_ViewedNon_LIVE
,sum(case when live=0 and analysis_right ='Sky Sports Basketball' then broadcast_days_viewed else 0 end) 
as BASKSS_Broadcast_Days_ViewedNon_LIVE
,sum(case when live=0 and analysis_right ='Sky Sports Boxing' then broadcast_days_viewed else 0 end) 
as BOXSS_Broadcast_Days_ViewedNon_LIVE
,sum(case when live=0 and analysis_right ='Sky Sports Cricket' then broadcast_days_viewed else 0 end) 
as CRISS_Broadcast_Days_ViewedNon_LIVE
,sum(case when live=0 and analysis_right ='Sky Sports Darts' then broadcast_days_viewed else 0 end) 
as DARTSS_Broadcast_Days_ViewedNon_LIVE
,sum(case when live=0 and analysis_right ='Sky Sports Equestrian' then broadcast_days_viewed else 0 end) 
as EQUSS_Broadcast_Days_ViewedNon_LIVE
,sum(case when live=0 and analysis_right ='Sky Sports Extreme' then broadcast_days_viewed else 0 end) 
as EXTSS_Broadcast_Days_ViewedNon_LIVE
,sum(case when live=0 and analysis_right ='Sky Sports Fishing' then broadcast_days_viewed else 0 end) 
as FISHSS_Broadcast_Days_ViewedNon_LIVE
,sum(case when live=0 and analysis_right ='Sky Sports Football' then broadcast_days_viewed else 0 end) 
as FOOTSS_Broadcast_Days_ViewedNon_LIVE
,sum(case when live=0 and analysis_right ='Sky Sports Golf' then broadcast_days_viewed else 0 end) 
as GOLFSS_Broadcast_Days_ViewedNon_LIVE
,sum(case when live=0 and analysis_right ='Sky Sports Ice Hockey' then broadcast_days_viewed else 0 end) 
as IHSS_Broadcast_Days_ViewedNon_LIVE
,sum(case when live=0 and analysis_right ='Sky Sports Motor Sport' then broadcast_days_viewed else 0 end) 
as MSPSS_Broadcast_Days_ViewedNon_LIVE
,sum(case when live=0 and analysis_right ='Sky Sports Racing' then broadcast_days_viewed else 0 end) 
as RACSS_Broadcast_Days_ViewedNon_LIVE
,sum(case when live=0 and analysis_right ='Sky Sports Rugby' then broadcast_days_viewed else 0 end) 
as RUGSS_Broadcast_Days_ViewedNon_LIVE
,sum(case when live=0 and analysis_right ='Sky Sports Snooker/Pool' then broadcast_days_viewed else 0 end) 
as SNPSS_Broadcast_Days_ViewedNon_LIVE
,sum(case when live=0 and analysis_right ='Sky Sports Tennis' then broadcast_days_viewed else 0 end) 
as TENSS_Broadcast_Days_ViewedNon_LIVE
,sum(case when live=0 and analysis_right ='Sky Sports Unknown' then broadcast_days_viewed else 0 end) 
as UNKSS_Broadcast_Days_ViewedNon_LIVE
,sum(case when live=0 and analysis_right ='Sky Sports Watersports' then broadcast_days_viewed else 0 end) 
as WATSS_Broadcast_Days_ViewedNon_LIVE
,sum(case when live=0 and analysis_right ='Sky Sports Wintersports' then broadcast_days_viewed else 0 end) 
as WINSS_Broadcast_Days_ViewedNon_LIVE
,sum(case when live=0 and analysis_right ='Sky Sports Wrestling' then broadcast_days_viewed else 0 end) 
as WRESS_Broadcast_Days_ViewedNon_LIVE
,sum(case when live=0 and analysis_right ='Solheim Cup 2013 Ladies Golf - Sky Sports' then broadcast_days_viewed else 0 end) 
as SOLSS_Broadcast_Days_ViewedNon_LIVE
,sum(case when live=0 and analysis_right ='South African Home Cricket  2012-2020 Sky Sports' then broadcast_days_viewed else 0 end) 
as SACSS_Broadcast_Days_ViewedNon_LIVE
,sum(case when live=0 and analysis_right ='Spanish Football 2012/13 To 14/15 - Sky Sports' then broadcast_days_viewed else 0 end) 
as SPFSS_Broadcast_Days_ViewedNon_LIVE
,sum(case when live=0 and analysis_right ='SPFL - BT Sport' then broadcast_days_viewed else 0 end) 
as SPFLBTS_Broadcast_Days_ViewedNon_LIVE
,sum(case when live=0 and analysis_right ='SPL - ESPN' then broadcast_days_viewed else 0 end) 
as SPLESPN_Broadcast_Days_ViewedNon_LIVE
,sum(case when live=0 and analysis_right ='SPL - Sky Sports' then broadcast_days_viewed else 0 end) 
as SPLSS_Broadcast_Days_ViewedNon_LIVE
,sum(case when live=0 and analysis_right ='Sportfive World Cup 2014 Qualifers - Sky Sports' then broadcast_days_viewed else 0 end) 
as SP5SS_Broadcast_Days_ViewedNon_LIVE
,sum(case when live=0 and analysis_right ='The boat race - BBC' then broadcast_days_viewed else 0 end) 
as BTRBBC_Broadcast_Days_ViewedNon_LIVE
,sum(case when live=0 and analysis_right ='The football league show - BBC' then broadcast_days_viewed else 0 end) 
as FLSBBC_Broadcast_Days_ViewedNon_LIVE
,sum(case when live=0 and analysis_right ='The Masters Golf - BBC' then broadcast_days_viewed else 0 end) 
as MGBBC_Broadcast_Days_ViewedNon_LIVE
,sum(case when live=0 and analysis_right ='TNA Wrestling Challenge' then broadcast_days_viewed else 0 end) 
as TNACHA_Broadcast_Days_ViewedNon_LIVE
,sum(case when live=0 and analysis_right ='Tour de France - Eurosport' then broadcast_days_viewed else 0 end) 
as TDFEUR_Broadcast_Days_ViewedNon_LIVE
,sum(case when live=0 and analysis_right ='Tour de France - ITV' then broadcast_days_viewed else 0 end) 
as TDFITV_Broadcast_Days_ViewedNon_LIVE
,sum(case when live=0 and analysis_right ='U S Masters Golf 2011-2014 Sky Sports' then broadcast_days_viewed else 0 end) 
as USMGSS_Broadcast_Days_ViewedNon_LIVE
,sum(case when live=0 and analysis_right ='U.S Open Tennis 2013-2015 Sky Sports' then broadcast_days_viewed else 0 end) 
as USOTSS_Broadcast_Days_ViewedNon_LIVE
,sum(case when live=0 and analysis_right ='U.S Open/Womens/Seniors/Amateur Golf Sky Sports' then broadcast_days_viewed else 0 end) 
as USOGSS_Broadcast_Days_ViewedNon_LIVE
,sum(case when live=0 and analysis_right ='UEFA Champions League -  Sky Sports' then broadcast_days_viewed else 0 end) 
as CLASS_Broadcast_Days_ViewedNon_LIVE
,sum(case when live=0 and analysis_right ='UEFA Champions League -  Sky Sports (non Live)' then broadcast_days_viewed else 0 end) 
as CLNSS_Broadcast_Days_ViewedNon_LIVE
,sum(case when live=0 and analysis_right ='UEFA Champions League -  Sky Sports (other Live)' then broadcast_days_viewed else 0 end) 
as CLOSS_Broadcast_Days_ViewedNon_LIVE
,sum(case when live=0 and analysis_right ='UEFA Champions League -  Sky Sports (Tue)' then broadcast_days_viewed else 0 end) 
as CLTSS_Broadcast_Days_ViewedNon_LIVE
,sum(case when live=0 and analysis_right ='UEFA Champions League -  Sky Sports (Wed)' then broadcast_days_viewed else 0 end) 
as CLWSS_Broadcast_Days_ViewedNon_LIVE
,sum(case when live=0 and analysis_right ='US Open Tennis - Eurosport' then broadcast_days_viewed else 0 end) 
as USOTEUR_Broadcast_Days_ViewedNon_LIVE
,sum(case when live=0 and analysis_right ='USA Football - BT Sport' then broadcast_days_viewed else 0 end) 
as USFBTS_Broadcast_Days_ViewedNon_LIVE
,sum(case when live=0 and analysis_right ='USPGA Championship (2007-2016) Sky Sports' then broadcast_days_viewed else 0 end) 
as USPGASS_Broadcast_Days_ViewedNon_LIVE
,sum(case when live=0 and analysis_right ='WCQ - ESPN' then broadcast_days_viewed else 0 end) 
as WCQESPN_Broadcast_Days_ViewedNon_LIVE
,sum(case when live=0 and analysis_right ='Welsh Home International Football 2012-2014 Sky Sports' then broadcast_days_viewed else 0 end) 
as WIFSS_Broadcast_Days_ViewedNon_LIVE
,sum(case when live=0 and analysis_right ='West Indies Home Cricket 2013-2020 Sky Sports' then broadcast_days_viewed else 0 end) 
as WICSS_Broadcast_Days_ViewedNon_LIVE
,sum(case when live=0 and analysis_right ='Wimbledon - BBC' then broadcast_days_viewed else 0 end) 
as WIMBBC_Broadcast_Days_ViewedNon_LIVE
,sum(case when live=0 and analysis_right ='Womens Cricket - I C C Cricket Deal  2012-2015 Sky Sports' then broadcast_days_viewed else 0 end) 
as WICCSS_Broadcast_Days_ViewedNon_LIVE
,sum(case when live=0 and analysis_right ='World Athletics Championship - More 4' then broadcast_days_viewed else 0 end) 
as WACMR4_Broadcast_Days_ViewedNon_LIVE
,sum(case when live=0 and analysis_right ='World Club Championship - BBC' then broadcast_days_viewed else 0 end) 
as WCLBBBC_Broadcast_Days_ViewedNon_LIVE
,sum(case when live=0 and analysis_right ='World Cup Qualifiers - BT Sport' then broadcast_days_viewed else 0 end) 
as WCQBTS_Broadcast_Days_ViewedNon_LIVE
,sum(case when live=0 and analysis_right ='World Darts Championship 2009-2012 Sky Sports' then broadcast_days_viewed else 0 end) 
as WDCSS_Broadcast_Days_ViewedNon_LIVE
,sum(case when live=0 and analysis_right ='World snooker championship - BBC' then broadcast_days_viewed else 0 end) 
as WSCBBC_Broadcast_Days_ViewedNon_LIVE
,sum(case when live=0 and analysis_right ='WWE Sky 1 and 2' then broadcast_days_viewed else 0 end) 
as WWES12_Broadcast_Days_ViewedNon_LIVE
,sum(case when live=0 and analysis_right ='WWE Sky Sports' then broadcast_days_viewed else 0 end) 
as WWESS_Broadcast_Days_ViewedNon_LIVE
,sum(case when live=1 and analysis_right ='Africa Cup of Nations - Eurosport' then total_duration_viewed_seconds else 0 end) 
as AFCEUR_Total_Seconds_Viewed_LIVE
,sum(case when live=1 and analysis_right ='Africa Cup of Nations - ITV' then total_duration_viewed_seconds else 0 end) 
as AFCITV_Total_Seconds_Viewed_LIVE
,sum(case when live=1 and analysis_right ='Americas Cup - BBC' then total_duration_viewed_seconds else 0 end) 
as AMCBBC_Total_Seconds_Viewed_LIVE
,sum(case when live=1 and analysis_right ='Asian Tour Golf 2012-2013 Sky Sports' then total_duration_viewed_seconds else 0 end) 
as ATGSS_Total_Seconds_Viewed_LIVE
,sum(case when live=1 and analysis_right ='ATP Tennis Masters Series  2011-2013 Sky Sports' then total_duration_viewed_seconds else 0 end) 
as ATPSS_Total_Seconds_Viewed_LIVE
,sum(case when live=1 and analysis_right ='Australia Home Cricket  2012-2016 Sky Sports' then total_duration_viewed_seconds else 0 end) 
as AHCSS_Total_Seconds_Viewed_LIVE
,sum(case when live=1 and analysis_right ='Australian Football - BT Sport' then total_duration_viewed_seconds else 0 end) 
as AUFBTS_Total_Seconds_Viewed_LIVE
,sum(case when live=1 and analysis_right ='Australian Open Tennis - BBC' then total_duration_viewed_seconds else 0 end) 
as AOTBBC_Total_Seconds_Viewed_LIVE
,sum(case when live=1 and analysis_right ='Australian Open Tennis - Eurosport' then total_duration_viewed_seconds else 0 end) 
as AOTEUR_Total_Seconds_Viewed_LIVE
,sum(case when live=1 and analysis_right ='Aviva Premiership - ESPN' then total_duration_viewed_seconds else 0 end) 
as AVPSS_Total_Seconds_Viewed_LIVE
,sum(case when live=1 and analysis_right ='BBC American Football' then total_duration_viewed_seconds else 0 end) 
as AFBBC_Total_Seconds_Viewed_LIVE
,sum(case when live=1 and analysis_right ='BBC Athletics' then total_duration_viewed_seconds else 0 end) 
as ATHBBC_Total_Seconds_Viewed_LIVE
,sum(case when live=1 and analysis_right ='BBC Boxing' then total_duration_viewed_seconds else 0 end) 
as BOXBBC_Total_Seconds_Viewed_LIVE
,sum(case when live=1 and analysis_right ='BBC Darts' then total_duration_viewed_seconds else 0 end) 
as DRTBBC_Total_Seconds_Viewed_LIVE
,sum(case when live=1 and analysis_right ='BBC Equestrian' then total_duration_viewed_seconds else 0 end) 
as EQUBBC_Total_Seconds_Viewed_LIVE
,sum(case when live=1 and analysis_right ='BBC Football' then total_duration_viewed_seconds else 0 end) 
as FOOTBBC_Total_Seconds_Viewed_LIVE
,sum(case when live=1 and analysis_right ='BBC Golf' then total_duration_viewed_seconds else 0 end) 
as GOLFBBC_Total_Seconds_Viewed_LIVE
,sum(case when live=1 and analysis_right ='BBC Motor Sport' then total_duration_viewed_seconds else 0 end) 
as MSPBBC_Total_Seconds_Viewed_LIVE
,sum(case when live=1 and analysis_right ='BBC Rugby' then total_duration_viewed_seconds else 0 end) 
as RUGBBC_Total_Seconds_Viewed_LIVE
,sum(case when live=1 and analysis_right ='BBC Snooker/Pool' then total_duration_viewed_seconds else 0 end) 
as SNPBBC_Total_Seconds_Viewed_LIVE
,sum(case when live=1 and analysis_right ='BBC Tennis' then total_duration_viewed_seconds else 0 end) 
as TENBBC_Total_Seconds_Viewed_LIVE
,sum(case when live=1 and analysis_right ='BBC Unknown' then total_duration_viewed_seconds else 0 end) 
as UNKBBC_Total_Seconds_Viewed_LIVE
,sum(case when live=1 and analysis_right ='BBC Watersports' then total_duration_viewed_seconds else 0 end) 
as WATBBC_Total_Seconds_Viewed_LIVE
,sum(case when live=1 and analysis_right ='BBC Wintersports' then total_duration_viewed_seconds else 0 end) 
as WINBBC_Total_Seconds_Viewed_LIVE
,sum(case when live=1 and analysis_right ='Boxing  - Channel 5' then total_duration_viewed_seconds else 0 end) 
as BOXCH5_Total_Seconds_Viewed_LIVE
,sum(case when live=1 and analysis_right ='Boxing - Matchroom 2012-2014 Sky Sports' then total_duration_viewed_seconds else 0 end) 
as BOXMSS_Total_Seconds_Viewed_LIVE
,sum(case when live=1 and analysis_right ='Brazil Football - BT Sport' then total_duration_viewed_seconds else 0 end) 
as BFTBTS_Total_Seconds_Viewed_LIVE
,sum(case when live=1 and analysis_right ='British & Irish Lions 2013 - Sky Sports' then total_duration_viewed_seconds else 0 end) 
as BILSS_Total_Seconds_Viewed_LIVE
,sum(case when live=1 and analysis_right ='British Open Golf - BBC' then total_duration_viewed_seconds else 0 end) 
as BOGSS_Total_Seconds_Viewed_LIVE
,sum(case when live=1 and analysis_right ='BT Sport American Football' then total_duration_viewed_seconds else 0 end) 
as AFBTS_Total_Seconds_Viewed_LIVE
,sum(case when live=1 and analysis_right ='BT Sport Athletics' then total_duration_viewed_seconds else 0 end) 
as ATHBTS_Total_Seconds_Viewed_LIVE
,sum(case when live=1 and analysis_right ='BT Sport Baseball' then total_duration_viewed_seconds else 0 end) 
as BASEBTS_Total_Seconds_Viewed_LIVE
,sum(case when live=1 and analysis_right ='BT Sport Basketball' then total_duration_viewed_seconds else 0 end) 
as BASKBTS_Total_Seconds_Viewed_LIVE
,sum(case when live=1 and analysis_right ='BT Sport Boxing' then total_duration_viewed_seconds else 0 end) 
as BOXBTS_Total_Seconds_Viewed_LIVE
,sum(case when live=1 and analysis_right ='BT Sport Cricket' then total_duration_viewed_seconds else 0 end) 
as CRIBTS_Total_Seconds_Viewed_LIVE
,sum(case when live=1 and analysis_right ='BT Sport Equestrian' then total_duration_viewed_seconds else 0 end) 
as EQUBTS_Total_Seconds_Viewed_LIVE
,sum(case when live=1 and analysis_right ='BT Sport Extreme' then total_duration_viewed_seconds else 0 end) 
as EXTBTS_Total_Seconds_Viewed_LIVE
,sum(case when live=1 and analysis_right ='BT Sport Football' then total_duration_viewed_seconds else 0 end) 
as FOOTBTS_Total_Seconds_Viewed_LIVE
,sum(case when live=1 and analysis_right ='BT Sport Motor Sport' then total_duration_viewed_seconds else 0 end) 
as MSPBTS_Total_Seconds_Viewed_LIVE
,sum(case when live=1 and analysis_right ='BT Sport Rugby' then total_duration_viewed_seconds else 0 end) 
as RUGBTS_Total_Seconds_Viewed_LIVE
,sum(case when live=1 and analysis_right ='BT Sport Tennis' then total_duration_viewed_seconds else 0 end) 
as TENBTS_Total_Seconds_Viewed_LIVE
,sum(case when live=1 and analysis_right ='BT Sport Unknown' then total_duration_viewed_seconds else 0 end) 
as UNKBTS_Total_Seconds_Viewed_LIVE
,sum(case when live=1 and analysis_right ='BT Sport Wintersports' then total_duration_viewed_seconds else 0 end) 
as WINBTS_Total_Seconds_Viewed_LIVE
,sum(case when live=1 and analysis_right ='Bundesliga - BT Sport' then total_duration_viewed_seconds else 0 end) 
as BUNBTS_Total_Seconds_Viewed_LIVE
,sum(case when live=1 and analysis_right ='Bundesliga- ESPN' then total_duration_viewed_seconds else 0 end) 
as BUNESPN_Total_Seconds_Viewed_LIVE
,sum(case when live=1 and analysis_right ='Challenge Darts' then total_duration_viewed_seconds else 0 end) 
as DRTCHA_Total_Seconds_Viewed_LIVE
,sum(case when live=1 and analysis_right ='Challenge Extreme' then total_duration_viewed_seconds else 0 end) 
as EXTCHA_Total_Seconds_Viewed_LIVE
,sum(case when live=1 and analysis_right ='Challenge Unknown' then total_duration_viewed_seconds else 0 end) 
as UNKCHA_Total_Seconds_Viewed_LIVE
,sum(case when live=1 and analysis_right ='Challenge Wrestling' then total_duration_viewed_seconds else 0 end) 
as WRECHA_Total_Seconds_Viewed_LIVE
,sum(case when live=1 and analysis_right ='Champions League - ITV' then total_duration_viewed_seconds else 0 end) 
as CHLITV_Total_Seconds_Viewed_LIVE
,sum(case when live=1 and analysis_right ='Champions Trophy - I C C Cricket Deal  2012-2015 Sky Sports' then total_duration_viewed_seconds else 0 end) 
as ICCSS_Total_Seconds_Viewed_LIVE
,sum(case when live=1 and analysis_right ='Channel 4 American Football' then total_duration_viewed_seconds else 0 end) 
as AMCH4_Total_Seconds_Viewed_LIVE
,sum(case when live=1 and analysis_right ='Channel 4 Athletics' then total_duration_viewed_seconds else 0 end) 
as ATHCH4_Total_Seconds_Viewed_LIVE
,sum(case when live=1 and analysis_right ='Channel 4 Boxing' then total_duration_viewed_seconds else 0 end) 
as BOXCH4_Total_Seconds_Viewed_LIVE
,sum(case when live=1 and analysis_right ='Channel 4 Equestrian' then total_duration_viewed_seconds else 0 end) 
as EQUCH4_Total_Seconds_Viewed_LIVE
,sum(case when live=1 and analysis_right ='Channel 4 Extreme' then total_duration_viewed_seconds else 0 end) 
as EXTCH4_Total_Seconds_Viewed_LIVE
,sum(case when live=1 and analysis_right ='Channel 4 Football' then total_duration_viewed_seconds else 0 end) 
as FOOTCH4_Total_Seconds_Viewed_LIVE
,sum(case when live=1 and analysis_right ='Channel 4 Motor Sport' then total_duration_viewed_seconds else 0 end) 
as MSPCH4_Total_Seconds_Viewed_LIVE
,sum(case when live=1 and analysis_right ='Channel 4 Racing' then total_duration_viewed_seconds else 0 end) 
as RACCH4_Total_Seconds_Viewed_LIVE
,sum(case when live=1 and analysis_right ='Channel 4 Unknown' then total_duration_viewed_seconds else 0 end) 
as UNKCH4_Total_Seconds_Viewed_LIVE
,sum(case when live=1 and analysis_right ='Channel 4 Watersports' then total_duration_viewed_seconds else 0 end) 
as WATCH4_Total_Seconds_Viewed_LIVE
,sum(case when live=1 and analysis_right ='Channel 4 Wintersports' then total_duration_viewed_seconds else 0 end) 
as WINCH4_Total_Seconds_Viewed_LIVE
,sum(case when live=1 and analysis_right ='Channel 5 Athletics' then total_duration_viewed_seconds else 0 end) 
as ATHCH5_Total_Seconds_Viewed_LIVE
,sum(case when live=1 and analysis_right ='Channel 5 Boxing' then total_duration_viewed_seconds else 0 end) 
as BOXOCH5_Total_Seconds_Viewed_LIVE
,sum(case when live=1 and analysis_right ='Channel 5 Cricket' then total_duration_viewed_seconds else 0 end) 
as CRICH5_Total_Seconds_Viewed_LIVE
,sum(case when live=1 and analysis_right ='Channel 5 Motor Sport' then total_duration_viewed_seconds else 0 end) 
as MSPCH5_Total_Seconds_Viewed_LIVE
,sum(case when live=1 and analysis_right ='Channel 5 Unknown' then total_duration_viewed_seconds else 0 end) 
as UNKCH5_Total_Seconds_Viewed_LIVE
,sum(case when live=1 and analysis_right ='Channel 5 Wrestling' then total_duration_viewed_seconds else 0 end) 
as WRECH5_Total_Seconds_Viewed_LIVE
,sum(case when live=1 and analysis_right ='Cheltenham Festival - Channel 4' then total_duration_viewed_seconds else 0 end) 
as CHELCH4_Total_Seconds_Viewed_LIVE
,sum(case when live=1 and analysis_right ='Community Shield - ITV' then total_duration_viewed_seconds else 0 end) 
as CMSITV_Total_Seconds_Viewed_LIVE
,sum(case when live=1 and analysis_right ='Confederations Cup - BBC' then total_duration_viewed_seconds else 0 end) 
as CONCBBC_Total_Seconds_Viewed_LIVE
,sum(case when live=1 and analysis_right ='Conference - BT Sport' then total_duration_viewed_seconds else 0 end) 
as CONFBTS_Total_Seconds_Viewed_LIVE
,sum(case when live=1 and analysis_right ='Cycling - La Vuelta ITV' then total_duration_viewed_seconds else 0 end) 
as CLVITV_Total_Seconds_Viewed_LIVE
,sum(case when live=1 and analysis_right ='Cycling - U C I World Tour Sky Sports' then total_duration_viewed_seconds else 0 end) 
as CUCISS_Total_Seconds_Viewed_LIVE
,sum(case when live=1 and analysis_right ='Cycling Tour of Britain - Eurosport' then total_duration_viewed_seconds else 0 end) 
as CTBEUR_Total_Seconds_Viewed_LIVE
,sum(case when live=1 and analysis_right ='Cycling: tour of britain ITV4' then total_duration_viewed_seconds else 0 end) 
as CTCITV_Total_Seconds_Viewed_LIVE
,sum(case when live=1 and analysis_right ='Derby - Channel 4' then total_duration_viewed_seconds else 0 end) 
as DERCH4_Total_Seconds_Viewed_LIVE
,sum(case when live=1 and analysis_right ='ECB (highlights) - Channel 5' then total_duration_viewed_seconds else 0 end) 
as ECBHCH5_Total_Seconds_Viewed_LIVE
,sum(case when live=1 and analysis_right ='ECB Cricket Sky Sports' then total_duration_viewed_seconds else 0 end) 
as GECRSS_Total_Seconds_Viewed_LIVE
,sum(case when live=1 and analysis_right ='ECB non-Test Cricket Sky Sports' then total_duration_viewed_seconds else 0 end) 
as ECBNSS_Total_Seconds_Viewed_LIVE
,sum(case when live=1 and analysis_right ='ECB Test Cricket Sky Sports' then total_duration_viewed_seconds else 0 end) 
as ECBTSS_Total_Seconds_Viewed_LIVE
,sum(case when live=1 and analysis_right ='England Football Internationals - ITV' then total_duration_viewed_seconds else 0 end) 
as GENGITV_Total_Seconds_Viewed_LIVE
,sum(case when live=1 and analysis_right ='England Friendlies (Football) - ITV' then total_duration_viewed_seconds else 0 end) 
as EFRITV_Total_Seconds_Viewed_LIVE
,sum(case when live=1 and analysis_right ='England Rugby Internationals 2010-2015 - Sky Sports' then total_duration_viewed_seconds else 0 end) 
as ENRSS_Total_Seconds_Viewed_LIVE
,sum(case when live=1 and analysis_right ='England World Cup Qualifying (Away) - ITV' then total_duration_viewed_seconds else 0 end) 
as EWQAITV_Total_Seconds_Viewed_LIVE
,sum(case when live=1 and analysis_right ='England World Cup Qualifying (Home) - ITV' then total_duration_viewed_seconds else 0 end) 
as EWQHITV_Total_Seconds_Viewed_LIVE
,sum(case when live=1 and analysis_right ='ESPN American Football' then total_duration_viewed_seconds else 0 end) 
as AMESPN_Total_Seconds_Viewed_LIVE
,sum(case when live=1 and analysis_right ='ESPN Athletics' then total_duration_viewed_seconds else 0 end) 
as ATHESPN_Total_Seconds_Viewed_LIVE
,sum(case when live=1 and analysis_right ='ESPN Baseball' then total_duration_viewed_seconds else 0 end) 
as BASEESPN_Total_Seconds_Viewed_LIVE
,sum(case when live=1 and analysis_right ='ESPN Basketball' then total_duration_viewed_seconds else 0 end) 
as BASKESPN_Total_Seconds_Viewed_LIVE
,sum(case when live=1 and analysis_right ='ESPN Boxing' then total_duration_viewed_seconds else 0 end) 
as BOXESPN_Total_Seconds_Viewed_LIVE
,sum(case when live=1 and analysis_right ='ESPN Cricket' then total_duration_viewed_seconds else 0 end) 
as CRIESPN_Total_Seconds_Viewed_LIVE
,sum(case when live=1 and analysis_right ='ESPN Darts' then total_duration_viewed_seconds else 0 end) 
as DARTESPN_Total_Seconds_Viewed_LIVE
,sum(case when live=1 and analysis_right ='ESPN Equestrian' then total_duration_viewed_seconds else 0 end) 
as EQUESPN_Total_Seconds_Viewed_LIVE
,sum(case when live=1 and analysis_right ='ESPN Extreme' then total_duration_viewed_seconds else 0 end) 
as EXTESPN_Total_Seconds_Viewed_LIVE
,sum(case when live=1 and analysis_right ='ESPN Football' then total_duration_viewed_seconds else 0 end) 
as FOOTESPN_Total_Seconds_Viewed_LIVE
,sum(case when live=1 and analysis_right ='ESPN Golf' then total_duration_viewed_seconds else 0 end) 
as GOLFESPN_Total_Seconds_Viewed_LIVE
,sum(case when live=1 and analysis_right ='ESPN Ice Hockey' then total_duration_viewed_seconds else 0 end) 
as IHESPN_Total_Seconds_Viewed_LIVE
,sum(case when live=1 and analysis_right ='ESPN Motor Sport' then total_duration_viewed_seconds else 0 end) 
as MSPESPN_Total_Seconds_Viewed_LIVE
,sum(case when live=1 and analysis_right ='ESPN Racing' then total_duration_viewed_seconds else 0 end) 
as RACESPN_Total_Seconds_Viewed_LIVE
,sum(case when live=1 and analysis_right ='ESPN Rugby' then total_duration_viewed_seconds else 0 end) 
as RUGESPN_Total_Seconds_Viewed_LIVE
,sum(case when live=1 and analysis_right ='ESPN Snooker/Pool' then total_duration_viewed_seconds else 0 end) 
as SNPESPN_Total_Seconds_Viewed_LIVE
,sum(case when live=1 and analysis_right ='ESPN Tennis' then total_duration_viewed_seconds else 0 end) 
as TENESPN_Total_Seconds_Viewed_LIVE
,sum(case when live=1 and analysis_right ='ESPN Unknown' then total_duration_viewed_seconds else 0 end) 
as UNKESPN_Total_Seconds_Viewed_LIVE
,sum(case when live=1 and analysis_right ='ESPN Watersports' then total_duration_viewed_seconds else 0 end) 
as WATESPN_Total_Seconds_Viewed_LIVE
,sum(case when live=1 and analysis_right ='ESPN Wintersports' then total_duration_viewed_seconds else 0 end) 
as WINESPN_Total_Seconds_Viewed_LIVE
,sum(case when live=1 and analysis_right ='ESPN Wrestling' then total_duration_viewed_seconds else 0 end) 
as WREESPN_Total_Seconds_Viewed_LIVE
,sum(case when live=1 and analysis_right ='Europa League - BT Sport' then total_duration_viewed_seconds else 0 end) 
as ELBTSP_Total_Seconds_Viewed_LIVE
,sum(case when live=1 and analysis_right ='Europa League - ESPN' then total_duration_viewed_seconds else 0 end) 
as ELESPN_Total_Seconds_Viewed_LIVE
,sum(case when live=1 and analysis_right ='Europa League - ITV' then total_duration_viewed_seconds else 0 end) 
as ELITV_Total_Seconds_Viewed_LIVE
,sum(case when live=1 and analysis_right ='European Tour Golf - Sky Sports' then total_duration_viewed_seconds else 0 end) 
as ETGSS_Total_Seconds_Viewed_LIVE
,sum(case when live=1 and analysis_right ='Eurosport American Football' then total_duration_viewed_seconds else 0 end) 
as AMEUR_Total_Seconds_Viewed_LIVE
,sum(case when live=1 and analysis_right ='Eurosport Athletics' then total_duration_viewed_seconds else 0 end) 
as ATHEUR_Total_Seconds_Viewed_LIVE
,sum(case when live=1 and analysis_right ='Eurosport Baseball' then total_duration_viewed_seconds else 0 end) 
as BASEEUR_Total_Seconds_Viewed_LIVE
,sum(case when live=1 and analysis_right ='Eurosport Basketball' then total_duration_viewed_seconds else 0 end) 
as BASKEUR_Total_Seconds_Viewed_LIVE
,sum(case when live=1 and analysis_right ='Eurosport Boxing' then total_duration_viewed_seconds else 0 end) 
as BOXEUR_Total_Seconds_Viewed_LIVE
,sum(case when live=1 and analysis_right ='Eurosport Cricket' then total_duration_viewed_seconds else 0 end) 
as CRIEUR_Total_Seconds_Viewed_LIVE
,sum(case when live=1 and analysis_right ='Eurosport Darts' then total_duration_viewed_seconds else 0 end) 
as DARTEUR_Total_Seconds_Viewed_LIVE
,sum(case when live=1 and analysis_right ='Eurosport Equestrian' then total_duration_viewed_seconds else 0 end) 
as EQUEUR_Total_Seconds_Viewed_LIVE
,sum(case when live=1 and analysis_right ='Eurosport Extreme' then total_duration_viewed_seconds else 0 end) 
as EXTEUR_Total_Seconds_Viewed_LIVE
,sum(case when live=1 and analysis_right ='Eurosport Football' then total_duration_viewed_seconds else 0 end) 
as FOOTEUR_Total_Seconds_Viewed_LIVE
,sum(case when live=1 and analysis_right ='Eurosport Golf' then total_duration_viewed_seconds else 0 end) 
as GOLFEUR_Total_Seconds_Viewed_LIVE
,sum(case when live=1 and analysis_right ='Eurosport Ice Hockey' then total_duration_viewed_seconds else 0 end) 
as IHEUR_Total_Seconds_Viewed_LIVE
,sum(case when live=1 and analysis_right ='Eurosport Motor Sport' then total_duration_viewed_seconds else 0 end) 
as MSPEUR_Total_Seconds_Viewed_LIVE
,sum(case when live=1 and analysis_right ='Eurosport Racing' then total_duration_viewed_seconds else 0 end) 
as RACEUR_Total_Seconds_Viewed_LIVE
,sum(case when live=1 and analysis_right ='Eurosport Snooker/Pool' then total_duration_viewed_seconds else 0 end) 
as SNPEUR_Total_Seconds_Viewed_LIVE
,sum(case when live=1 and analysis_right ='Eurosport Tennis' then total_duration_viewed_seconds else 0 end) 
as TENEUR_Total_Seconds_Viewed_LIVE
,sum(case when live=1 and analysis_right ='Eurosport Unknown' then total_duration_viewed_seconds else 0 end) 
as UNKEUR_Total_Seconds_Viewed_LIVE
,sum(case when live=1 and analysis_right ='Eurosport Watersports' then total_duration_viewed_seconds else 0 end) 
as WATEUR_Total_Seconds_Viewed_LIVE
,sum(case when live=1 and analysis_right ='Eurosport Wintersports' then total_duration_viewed_seconds else 0 end) 
as WINEUR_Total_Seconds_Viewed_LIVE
,sum(case when live=1 and analysis_right ='F1 - BBC' then total_duration_viewed_seconds else 0 end) 
as GF1BBC_Total_Seconds_Viewed_LIVE
,sum(case when live=1 and analysis_right ='F1 - Sky Sports' then total_duration_viewed_seconds else 0 end) 
as GF1SS_Total_Seconds_Viewed_LIVE
,sum(case when live=1 and analysis_right ='F1 (non-Live)- BBC' then total_duration_viewed_seconds else 0 end) 
as F1NBBC_Total_Seconds_Viewed_LIVE
,sum(case when live=1 and analysis_right ='F1 (Practice Live)- BBC' then total_duration_viewed_seconds else 0 end) 
as F1PBBC_Total_Seconds_Viewed_LIVE
,sum(case when live=1 and analysis_right ='F1 (Qualifying Live)- BBC' then total_duration_viewed_seconds else 0 end) 
as F1QBBC_Total_Seconds_Viewed_LIVE
,sum(case when live=1 and analysis_right ='F1 (Race Live)- BBC' then total_duration_viewed_seconds else 0 end) 
as F1RBBC_Total_Seconds_Viewed_LIVE
,sum(case when live=1 and analysis_right ='FA Cup - ESPN' then total_duration_viewed_seconds else 0 end) 
as FACESPN_Total_Seconds_Viewed_LIVE
,sum(case when live=1 and analysis_right ='FA Cup - ITV' then total_duration_viewed_seconds else 0 end) 
as FACITV_Total_Seconds_Viewed_LIVE
,sum(case when live=1 and analysis_right ='Football League - Seasons 2012/13 To 14/15 Sky Sports - Capital 1 Cup' then total_duration_viewed_seconds else 0 end) 
as FLCCSS_Total_Seconds_Viewed_LIVE
,sum(case when live=1 and analysis_right ='Football League - Seasons 2012/13 To 14/15 Sky Sports - others' then total_duration_viewed_seconds else 0 end) 
as FLOTSS_Total_Seconds_Viewed_LIVE
,sum(case when live=1 and analysis_right ='Formula One 2012-2018 - (non-Live) Sky Sports' then total_duration_viewed_seconds else 0 end) 
as F1NSS_Total_Seconds_Viewed_LIVE
,sum(case when live=1 and analysis_right ='Formula One 2012-2018 - (Practice Live) Sky Sports' then total_duration_viewed_seconds else 0 end) 
as F1PSS_Total_Seconds_Viewed_LIVE
,sum(case when live=1 and analysis_right ='Formula One 2012-2018 - (Qualifying Live) Sky Sports' then total_duration_viewed_seconds else 0 end) 
as F1QSS_Total_Seconds_Viewed_LIVE
,sum(case when live=1 and analysis_right ='Formula One 2012-2018 - (Race Live) Sky Sports' then total_duration_viewed_seconds else 0 end) 
as F1RSS_Total_Seconds_Viewed_LIVE
,sum(case when live=1 and analysis_right ='French Open Tennis - Eurosport' then total_duration_viewed_seconds else 0 end) 
as FOTEUR_Total_Seconds_Viewed_LIVE
,sum(case when live=1 and analysis_right ='French Open Tennis - ITV' then total_duration_viewed_seconds else 0 end) 
as FOTITV_Total_Seconds_Viewed_LIVE
,sum(case when live=1 and analysis_right ='Grand National - Channel 4' then total_duration_viewed_seconds else 0 end) 
as GDNCH4_Total_Seconds_Viewed_LIVE
,sum(case when live=1 and analysis_right ='Heineken Cup 2010/11 To 2013/14- Sky Sports' then total_duration_viewed_seconds else 0 end) 
as HECSS_Total_Seconds_Viewed_LIVE
,sum(case when live=1 and analysis_right ='I R B Sevens 2008/09 -2014/15 - Sky Sports' then total_duration_viewed_seconds else 0 end) 
as IRBSS_Total_Seconds_Viewed_LIVE
,sum(case when live=1 and analysis_right ='IAAF World Athletics Championship - Eurosport' then total_duration_viewed_seconds else 0 end) 
as WACEUR_Total_Seconds_Viewed_LIVE
,sum(case when live=1 and analysis_right ='India Home Cricket 2012-2018 Sky Sports' then total_duration_viewed_seconds else 0 end) 
as IHCSS_Total_Seconds_Viewed_LIVE
,sum(case when live=1 and analysis_right ='India Premier League - ITV' then total_duration_viewed_seconds else 0 end) 
as IPLITV_Total_Seconds_Viewed_LIVE
,sum(case when live=1 and analysis_right ='International Freindlies - ESPN' then total_duration_viewed_seconds else 0 end) 
as IFESPN_Total_Seconds_Viewed_LIVE
,sum(case when live=1 and analysis_right ='International Friendlies - BT Sport' then total_duration_viewed_seconds else 0 end) 
as IFBTS_Total_Seconds_Viewed_LIVE
,sum(case when live=1 and analysis_right ='ITV1 Boxing' then total_duration_viewed_seconds else 0 end) 
as BOXITV1_Total_Seconds_Viewed_LIVE
,sum(case when live=1 and analysis_right ='ITV1 Football' then total_duration_viewed_seconds else 0 end) 
as FOOTITV1_Total_Seconds_Viewed_LIVE
,sum(case when live=1 and analysis_right ='ITV1 Motor Sport' then total_duration_viewed_seconds else 0 end) 
as MOTSITV1_Total_Seconds_Viewed_LIVE
,sum(case when live=1 and analysis_right ='ITV1 Rugby' then total_duration_viewed_seconds else 0 end) 
as RUGITV1_Total_Seconds_Viewed_LIVE
,sum(case when live=1 and analysis_right ='ITV1 Snooker/Pool' then total_duration_viewed_seconds else 0 end) 
as SNPITV1_Total_Seconds_Viewed_LIVE
,sum(case when live=1 and analysis_right ='ITV1 Unknown' then total_duration_viewed_seconds else 0 end) 
as UNKITV1_Total_Seconds_Viewed_LIVE
,sum(case when live=1 and analysis_right ='ITV4 Boxing' then total_duration_viewed_seconds else 0 end) 
as BOXITV4_Total_Seconds_Viewed_LIVE
,sum(case when live=1 and analysis_right ='ITV4 Cricket' then total_duration_viewed_seconds else 0 end) 
as CRIITV4_Total_Seconds_Viewed_LIVE
,sum(case when live=1 and analysis_right ='ITV4 Darts' then total_duration_viewed_seconds else 0 end) 
as DARTITV4_Total_Seconds_Viewed_LIVE
,sum(case when live=1 and analysis_right ='ITV4 Extreme' then total_duration_viewed_seconds else 0 end) 
as EXTITV4_Total_Seconds_Viewed_LIVE
,sum(case when live=1 and analysis_right ='ITV4 Football' then total_duration_viewed_seconds else 0 end) 
as FOOTITV4_Total_Seconds_Viewed_LIVE
,sum(case when live=1 and analysis_right ='ITV4 Motor Sport' then total_duration_viewed_seconds else 0 end) 
as MSPITV4_Total_Seconds_Viewed_LIVE
,sum(case when live=1 and analysis_right ='ITV4 Rugby' then total_duration_viewed_seconds else 0 end) 
as RUGITV4_Total_Seconds_Viewed_LIVE
,sum(case when live=1 and analysis_right ='ITV4 Snooker/Pool' then total_duration_viewed_seconds else 0 end) 
as SNPITV4_Total_Seconds_Viewed_LIVE
,sum(case when live=1 and analysis_right ='ITV4 Tennis' then total_duration_viewed_seconds else 0 end) 
as TENITV4_Total_Seconds_Viewed_LIVE
,sum(case when live=1 and analysis_right ='ITV4 Unknown' then total_duration_viewed_seconds else 0 end) 
as UNKITV4_Total_Seconds_Viewed_LIVE
,sum(case when live=1 and analysis_right ='Ligue 1 - BT Sport' then total_duration_viewed_seconds else 0 end) 
as L1BTS_Total_Seconds_Viewed_LIVE
,sum(case when live=1 and analysis_right ='Ligue 1 - ESPN' then total_duration_viewed_seconds else 0 end) 
as L1ESPN_Total_Seconds_Viewed_LIVE
,sum(case when live=1 and analysis_right ='Match of the day - BBC' then total_duration_viewed_seconds else 0 end) 
as MOTDBBC_Total_Seconds_Viewed_LIVE
,sum(case when live=1 and analysis_right ='Matchroom Output Deal  2010-2012 (Other)- Sky Sports' then total_duration_viewed_seconds else 0 end) 
as MROSS_Total_Seconds_Viewed_LIVE
,sum(case when live=1 and analysis_right ='Matchroom Output Deal  2010-2012 (Pool)- Sky Sports' then total_duration_viewed_seconds else 0 end) 
as MRPSS_Total_Seconds_Viewed_LIVE
,sum(case when live=1 and analysis_right ='Matchroom Output Deal  2010-2012 (Snooker)- Sky Sports' then total_duration_viewed_seconds else 0 end) 
as MRSSS_Total_Seconds_Viewed_LIVE
,sum(case when live=1 and analysis_right ='Moto GP BBC' then total_duration_viewed_seconds else 0 end) 
as MGPBBC_Total_Seconds_Viewed_LIVE
,sum(case when live=1 and analysis_right ='NBA - Sky Sports' then total_duration_viewed_seconds else 0 end) 
as NBASS_Total_Seconds_Viewed_LIVE
,sum(case when live=1 and analysis_right ='NFL - BBC' then total_duration_viewed_seconds else 0 end) 
as NFLBBC_Total_Seconds_Viewed_LIVE
,sum(case when live=1 and analysis_right ='NFL - Channel 4' then total_duration_viewed_seconds else 0 end) 
as NFLCH4_Total_Seconds_Viewed_LIVE
,sum(case when live=1 and analysis_right ='NFL Seasons 2012-2015 - Sky Sports' then total_duration_viewed_seconds else 0 end) 
as NFLSS_Total_Seconds_Viewed_LIVE
,sum(case when live=1 and analysis_right ='Northern Ireland Football Association 2012-2014 - Sky Sports' then total_duration_viewed_seconds else 0 end) 
as NIFSS_Total_Seconds_Viewed_LIVE
,sum(case when live=1 and analysis_right ='Oaks - Channel 4' then total_duration_viewed_seconds else 0 end) 
as OAKCH4_Total_Seconds_Viewed_LIVE
,sum(case when live=1 and analysis_right ='Other American Football' then total_duration_viewed_seconds else 0 end) 
as AMOTH_Total_Seconds_Viewed_LIVE
,sum(case when live=1 and analysis_right ='Other Athletics' then total_duration_viewed_seconds else 0 end) 
as ATHOTH_Total_Seconds_Viewed_LIVE
,sum(case when live=1 and analysis_right ='Other Baseball' then total_duration_viewed_seconds else 0 end) 
as BASEOTH_Total_Seconds_Viewed_LIVE
,sum(case when live=1 and analysis_right ='Other Basketball' then total_duration_viewed_seconds else 0 end) 
as BASKOTH_Total_Seconds_Viewed_LIVE
,sum(case when live=1 and analysis_right ='Other Boxing' then total_duration_viewed_seconds else 0 end) 
as BOXOTH_Total_Seconds_Viewed_LIVE
,sum(case when live=1 and analysis_right ='Other Cricket' then total_duration_viewed_seconds else 0 end) 
as CRIOTH_Total_Seconds_Viewed_LIVE
,sum(case when live=1 and analysis_right ='Other Darts' then total_duration_viewed_seconds else 0 end) 
as DARTOTH_Total_Seconds_Viewed_LIVE
,sum(case when live=1 and analysis_right ='Other Equestrian' then total_duration_viewed_seconds else 0 end) 
as EQUOTH_Total_Seconds_Viewed_LIVE
,sum(case when live=1 and analysis_right ='Other Extreme' then total_duration_viewed_seconds else 0 end) 
as EXTOTH_Total_Seconds_Viewed_LIVE
,sum(case when live=1 and analysis_right ='Other Fishing' then total_duration_viewed_seconds else 0 end) 
as FSHOTH_Total_Seconds_Viewed_LIVE
,sum(case when live=1 and analysis_right ='Other Football' then total_duration_viewed_seconds else 0 end) 
as FOOTOTH_Total_Seconds_Viewed_LIVE
,sum(case when live=1 and analysis_right ='Other Golf' then total_duration_viewed_seconds else 0 end) 
as GOLFOTH_Total_Seconds_Viewed_LIVE
,sum(case when live=1 and analysis_right ='Other Ice Hockey' then total_duration_viewed_seconds else 0 end) 
as IHOTH_Total_Seconds_Viewed_LIVE
,sum(case when live=1 and analysis_right ='Other Motor Sport' then total_duration_viewed_seconds else 0 end) 
as MSPOTH_Total_Seconds_Viewed_LIVE
,sum(case when live=1 and analysis_right ='Other Racing' then total_duration_viewed_seconds else 0 end) 
as RACOTH_Total_Seconds_Viewed_LIVE
,sum(case when live=1 and analysis_right ='Other Rugby' then total_duration_viewed_seconds else 0 end) 
as RUGOTH_Total_Seconds_Viewed_LIVE
,sum(case when live=1 and analysis_right ='Other Rugby Internationals - ESPN' then total_duration_viewed_seconds else 0 end) 
as ORUGESPN_Total_Seconds_Viewed_LIVE
,sum(case when live=1 and analysis_right ='Other Snooker/Pool' then total_duration_viewed_seconds else 0 end) 
as OTHSNP_Total_Seconds_Viewed_LIVE
,sum(case when live=1 and analysis_right ='Other Tennis' then total_duration_viewed_seconds else 0 end) 
as OTHTEN_Total_Seconds_Viewed_LIVE
,sum(case when live=1 and analysis_right ='Other Unknown' then total_duration_viewed_seconds else 0 end) 
as OTHUNK_Total_Seconds_Viewed_LIVE
,sum(case when live=1 and analysis_right ='Other Watersports' then total_duration_viewed_seconds else 0 end) 
as OTHWAT_Total_Seconds_Viewed_LIVE
,sum(case when live=1 and analysis_right ='Other Wintersports' then total_duration_viewed_seconds else 0 end) 
as OTHWIN_Total_Seconds_Viewed_LIVE
,sum(case when live=1 and analysis_right ='Other Wrestling' then total_duration_viewed_seconds else 0 end) 
as OTHWRE_Total_Seconds_Viewed_LIVE
,sum(case when live=1 and analysis_right ='PGA Tour Golf 2010-2017 - Sky Sports' then total_duration_viewed_seconds else 0 end) 
as PGASS_Total_Seconds_Viewed_LIVE
,sum(case when live=1 and analysis_right ='Premier League - BT Sport' then total_duration_viewed_seconds else 0 end) 
as PLBTS_Total_Seconds_Viewed_LIVE
,sum(case when live=1 and analysis_right ='Premier League - ESPN' then total_duration_viewed_seconds else 0 end) 
as PLESPN_Total_Seconds_Viewed_LIVE
,sum(case when live=1 and analysis_right ='Premier League Darts 2010-2013 - Sky Sports' then total_duration_viewed_seconds else 0 end) 
as PLDSS_Total_Seconds_Viewed_LIVE
,sum(case when live=1 and analysis_right ='Premier League Football - Sky Sports' then total_duration_viewed_seconds else 0 end) 
as GPLSS_Total_Seconds_Viewed_LIVE
,sum(case when live=1 and analysis_right ='Premier League Football - Sky Sports (Match Choice)' then total_duration_viewed_seconds else 0 end) 
as PLMCSS_Total_Seconds_Viewed_LIVE
,sum(case when live=1 and analysis_right ='Premier League Football - Sky Sports (MNF)' then total_duration_viewed_seconds else 0 end) 
as PLMNFSS_Total_Seconds_Viewed_LIVE
,sum(case when live=1 and analysis_right ='Premier League Football - Sky Sports (non Live)' then total_duration_viewed_seconds else 0 end) 
as PLNLSS_Total_Seconds_Viewed_LIVE
,sum(case when live=1 and analysis_right ='Premier League Football - Sky Sports (other Live)' then total_duration_viewed_seconds else 0 end) 
as PLOLSS_Total_Seconds_Viewed_LIVE
,sum(case when live=1 and analysis_right ='Premier League Football - Sky Sports (Sat Lunchtime)' then total_duration_viewed_seconds else 0 end) 
as PLSLSS_Total_Seconds_Viewed_LIVE
,sum(case when live=1 and analysis_right ='Premier League Football - Sky Sports (Sat Night Live)' then total_duration_viewed_seconds else 0 end) 
as PLSNSS_Total_Seconds_Viewed_LIVE
,sum(case when live=1 and analysis_right ='Premier League Football - Sky Sports (Sun 4pm)' then total_duration_viewed_seconds else 0 end) 
as PLS4SS_Total_Seconds_Viewed_LIVE
,sum(case when live=1 and analysis_right ='Premier League Football - Sky Sports (Sun Lunchtime)' then total_duration_viewed_seconds else 0 end) 
as PLSULSS_Total_Seconds_Viewed_LIVE
,sum(case when live=1 and analysis_right ='Premiership Rugby - Sky Sports' then total_duration_viewed_seconds else 0 end) 
as PRUSS_Total_Seconds_Viewed_LIVE
,sum(case when live=1 and analysis_right ='Republic Of Ireland FA - 2010/11 To 2013/14 - Sky Sports' then total_duration_viewed_seconds else 0 end) 
as ROISS_Total_Seconds_Viewed_LIVE
,sum(case when live=1 and analysis_right ='Royal Ascot - Channel 4' then total_duration_viewed_seconds else 0 end) 
as RASCH4_Total_Seconds_Viewed_LIVE
,sum(case when live=1 and analysis_right ='Rugby Internationals (England) - BBC' then total_duration_viewed_seconds else 0 end) 
as RIEBBC_Total_Seconds_Viewed_LIVE
,sum(case when live=1 and analysis_right ='Rugby Internationals (Ireland) - BBC' then total_duration_viewed_seconds else 0 end) 
as RIIBBC_Total_Seconds_Viewed_LIVE
,sum(case when live=1 and analysis_right ='Rugby Internationals (Scotland) - BBC' then total_duration_viewed_seconds else 0 end) 
as RISBBC_Total_Seconds_Viewed_LIVE
,sum(case when live=1 and analysis_right ='Rugby Internationals (Wales) - BBC' then total_duration_viewed_seconds else 0 end) 
as RIWBBC_Total_Seconds_Viewed_LIVE
,sum(case when live=1 and analysis_right ='Rugby League  Challenge Cup- BBC' then total_duration_viewed_seconds else 0 end) 
as RLCCBBC_Total_Seconds_Viewed_LIVE
,sum(case when live=1 and analysis_right ='Rugby League - Sky Sports' then total_duration_viewed_seconds else 0 end) 
as RLGSS_Total_Seconds_Viewed_LIVE
,sum(case when live=1 and analysis_right ='Rugby League  World Cup- BBC' then total_duration_viewed_seconds else 0 end) 
as RLWCBBC_Total_Seconds_Viewed_LIVE
,sum(case when live=1 and analysis_right ='Sanzar Rugby Union 2011 - 2015 - Sky Sports' then total_duration_viewed_seconds else 0 end) 
as SARUSS_Total_Seconds_Viewed_LIVE
,sum(case when live=1 and analysis_right ='Scottish FA Deal - 2010/11 To 2013/14 - Sky Sports' then total_duration_viewed_seconds else 0 end) 
as SFASS_Total_Seconds_Viewed_LIVE
,sum(case when live=1 and analysis_right ='Serie A - BT Sport' then total_duration_viewed_seconds else 0 end) 
as SABTS_Total_Seconds_Viewed_LIVE
,sum(case when live=1 and analysis_right ='Serie A - ESPN' then total_duration_viewed_seconds else 0 end) 
as SAESPN_Total_Seconds_Viewed_LIVE
,sum(case when live=1 and analysis_right ='SFL - ESPN' then total_duration_viewed_seconds else 0 end) 
as SFLESPN_Total_Seconds_Viewed_LIVE
,sum(case when live=1 and analysis_right ='Six Nations - BBC' then total_duration_viewed_seconds else 0 end) 
as SNRBBC_Total_Seconds_Viewed_LIVE
,sum(case when live=1 and analysis_right ='Sky 1 and Sky 2 Boxing' then total_duration_viewed_seconds else 0 end) 
as BOXS12_Total_Seconds_Viewed_LIVE
,sum(case when live=1 and analysis_right ='Sky 1 and Sky 2 Football' then total_duration_viewed_seconds else 0 end) 
as FOOTS12_Total_Seconds_Viewed_LIVE
,sum(case when live=1 and analysis_right ='Sky 1 and Sky 2 Motor Sport' then total_duration_viewed_seconds else 0 end) 
as MSPS12_Total_Seconds_Viewed_LIVE
,sum(case when live=1 and analysis_right ='Sky 1 and Sky 2 Unknown' then total_duration_viewed_seconds else 0 end) 
as UNKS12_Total_Seconds_Viewed_LIVE
,sum(case when live=1 and analysis_right ='Sky 1 and Sky 2 Wrestling' then total_duration_viewed_seconds else 0 end) 
as WRES12_Total_Seconds_Viewed_LIVE
,sum(case when live=1 and analysis_right ='Sky Sports American Football' then total_duration_viewed_seconds else 0 end) 
as AMSS_Total_Seconds_Viewed_LIVE
,sum(case when live=1 and analysis_right ='Sky Sports Athletics' then total_duration_viewed_seconds else 0 end) 
as ATHSS_Total_Seconds_Viewed_LIVE
,sum(case when live=1 and analysis_right ='Sky Sports Baseball' then total_duration_viewed_seconds else 0 end) 
as BASESS_Total_Seconds_Viewed_LIVE
,sum(case when live=1 and analysis_right ='Sky Sports Basketball' then total_duration_viewed_seconds else 0 end) 
as BASKSS_Total_Seconds_Viewed_LIVE
,sum(case when live=1 and analysis_right ='Sky Sports Boxing' then total_duration_viewed_seconds else 0 end) 
as BOXSS_Total_Seconds_Viewed_LIVE
,sum(case when live=1 and analysis_right ='Sky Sports Cricket' then total_duration_viewed_seconds else 0 end) 
as CRISS_Total_Seconds_Viewed_LIVE
,sum(case when live=1 and analysis_right ='Sky Sports Darts' then total_duration_viewed_seconds else 0 end) 
as DARTSS_Total_Seconds_Viewed_LIVE
,sum(case when live=1 and analysis_right ='Sky Sports Equestrian' then total_duration_viewed_seconds else 0 end) 
as EQUSS_Total_Seconds_Viewed_LIVE
,sum(case when live=1 and analysis_right ='Sky Sports Extreme' then total_duration_viewed_seconds else 0 end) 
as EXTSS_Total_Seconds_Viewed_LIVE
,sum(case when live=1 and analysis_right ='Sky Sports Fishing' then total_duration_viewed_seconds else 0 end) 
as FISHSS_Total_Seconds_Viewed_LIVE
,sum(case when live=1 and analysis_right ='Sky Sports Football' then total_duration_viewed_seconds else 0 end) 
as FOOTSS_Total_Seconds_Viewed_LIVE
,sum(case when live=1 and analysis_right ='Sky Sports Golf' then total_duration_viewed_seconds else 0 end) 
as GOLFSS_Total_Seconds_Viewed_LIVE
,sum(case when live=1 and analysis_right ='Sky Sports Ice Hockey' then total_duration_viewed_seconds else 0 end) 
as IHSS_Total_Seconds_Viewed_LIVE
,sum(case when live=1 and analysis_right ='Sky Sports Motor Sport' then total_duration_viewed_seconds else 0 end) 
as MSPSS_Total_Seconds_Viewed_LIVE
,sum(case when live=1 and analysis_right ='Sky Sports Racing' then total_duration_viewed_seconds else 0 end) 
as RACSS_Total_Seconds_Viewed_LIVE
,sum(case when live=1 and analysis_right ='Sky Sports Rugby' then total_duration_viewed_seconds else 0 end) 
as RUGSS_Total_Seconds_Viewed_LIVE
,sum(case when live=1 and analysis_right ='Sky Sports Snooker/Pool' then total_duration_viewed_seconds else 0 end) 
as SNPSS_Total_Seconds_Viewed_LIVE
,sum(case when live=1 and analysis_right ='Sky Sports Tennis' then total_duration_viewed_seconds else 0 end) 
as TENSS_Total_Seconds_Viewed_LIVE
,sum(case when live=1 and analysis_right ='Sky Sports Unknown' then total_duration_viewed_seconds else 0 end) 
as UNKSS_Total_Seconds_Viewed_LIVE
,sum(case when live=1 and analysis_right ='Sky Sports Watersports' then total_duration_viewed_seconds else 0 end) 
as WATSS_Total_Seconds_Viewed_LIVE
,sum(case when live=1 and analysis_right ='Sky Sports Wintersports' then total_duration_viewed_seconds else 0 end) 
as WINSS_Total_Seconds_Viewed_LIVE
,sum(case when live=1 and analysis_right ='Sky Sports Wrestling' then total_duration_viewed_seconds else 0 end) 
as WRESS_Total_Seconds_Viewed_LIVE
,sum(case when live=1 and analysis_right ='Solheim Cup 2013 Ladies Golf - Sky Sports' then total_duration_viewed_seconds else 0 end) 
as SOLSS_Total_Seconds_Viewed_LIVE
,sum(case when live=1 and analysis_right ='South African Home Cricket  2012-2020 Sky Sports' then total_duration_viewed_seconds else 0 end) 
as SACSS_Total_Seconds_Viewed_LIVE
,sum(case when live=1 and analysis_right ='Spanish Football 2012/13 To 14/15 - Sky Sports' then total_duration_viewed_seconds else 0 end) 
as SPFSS_Total_Seconds_Viewed_LIVE
,sum(case when live=1 and analysis_right ='SPFL - BT Sport' then total_duration_viewed_seconds else 0 end) 
as SPFLBTS_Total_Seconds_Viewed_LIVE
,sum(case when live=1 and analysis_right ='SPL - ESPN' then total_duration_viewed_seconds else 0 end) 
as SPLESPN_Total_Seconds_Viewed_LIVE
,sum(case when live=1 and analysis_right ='SPL - Sky Sports' then total_duration_viewed_seconds else 0 end) 
as SPLSS_Total_Seconds_Viewed_LIVE
,sum(case when live=1 and analysis_right ='Sportfive World Cup 2014 Qualifers - Sky Sports' then total_duration_viewed_seconds else 0 end) 
as SP5SS_Total_Seconds_Viewed_LIVE
,sum(case when live=1 and analysis_right ='The boat race - BBC' then total_duration_viewed_seconds else 0 end) 
as BTRBBC_Total_Seconds_Viewed_LIVE
,sum(case when live=1 and analysis_right ='The football league show - BBC' then total_duration_viewed_seconds else 0 end) 
as FLSBBC_Total_Seconds_Viewed_LIVE
,sum(case when live=1 and analysis_right ='The Masters Golf - BBC' then total_duration_viewed_seconds else 0 end) 
as MGBBC_Total_Seconds_Viewed_LIVE
,sum(case when live=1 and analysis_right ='TNA Wrestling Challenge' then total_duration_viewed_seconds else 0 end) 
as TNACHA_Total_Seconds_Viewed_LIVE
,sum(case when live=1 and analysis_right ='Tour de France - Eurosport' then total_duration_viewed_seconds else 0 end) 
as TDFEUR_Total_Seconds_Viewed_LIVE
,sum(case when live=1 and analysis_right ='Tour de France - ITV' then total_duration_viewed_seconds else 0 end) 
as TDFITV_Total_Seconds_Viewed_LIVE
,sum(case when live=1 and analysis_right ='U S Masters Golf 2011-2014 Sky Sports' then total_duration_viewed_seconds else 0 end) 
as USMGSS_Total_Seconds_Viewed_LIVE
,sum(case when live=1 and analysis_right ='U.S Open Tennis 2013-2015 Sky Sports' then total_duration_viewed_seconds else 0 end) 
as USOTSS_Total_Seconds_Viewed_LIVE
,sum(case when live=1 and analysis_right ='U.S Open/Womens/Seniors/Amateur Golf Sky Sports' then total_duration_viewed_seconds else 0 end) 
as USOGSS_Total_Seconds_Viewed_LIVE
,sum(case when live=1 and analysis_right ='UEFA Champions League -  Sky Sports' then total_duration_viewed_seconds else 0 end) 
as CLASS_Total_Seconds_Viewed_LIVE
,sum(case when live=1 and analysis_right ='UEFA Champions League -  Sky Sports (non Live)' then total_duration_viewed_seconds else 0 end) 
as CLNSS_Total_Seconds_Viewed_LIVE
,sum(case when live=1 and analysis_right ='UEFA Champions League -  Sky Sports (other Live)' then total_duration_viewed_seconds else 0 end) 
as CLOSS_Total_Seconds_Viewed_LIVE
,sum(case when live=1 and analysis_right ='UEFA Champions League -  Sky Sports (Tue)' then total_duration_viewed_seconds else 0 end) 
as CLTSS_Total_Seconds_Viewed_LIVE
,sum(case when live=1 and analysis_right ='UEFA Champions League -  Sky Sports (Wed)' then total_duration_viewed_seconds else 0 end) 
as CLWSS_Total_Seconds_Viewed_LIVE
,sum(case when live=1 and analysis_right ='US Open Tennis - Eurosport' then total_duration_viewed_seconds else 0 end) 
as USOTEUR_Total_Seconds_Viewed_LIVE
,sum(case when live=1 and analysis_right ='USA Football - BT Sport' then total_duration_viewed_seconds else 0 end) 
as USFBTS_Total_Seconds_Viewed_LIVE
,sum(case when live=1 and analysis_right ='USPGA Championship (2007-2016) Sky Sports' then total_duration_viewed_seconds else 0 end) 
as USPGASS_Total_Seconds_Viewed_LIVE
,sum(case when live=1 and analysis_right ='WCQ - ESPN' then total_duration_viewed_seconds else 0 end) 
as WCQESPN_Total_Seconds_Viewed_LIVE
,sum(case when live=1 and analysis_right ='Welsh Home International Football 2012-2014 Sky Sports' then total_duration_viewed_seconds else 0 end) 
as WIFSS_Total_Seconds_Viewed_LIVE
,sum(case when live=1 and analysis_right ='West Indies Home Cricket 2013-2020 Sky Sports' then total_duration_viewed_seconds else 0 end) 
as WICSS_Total_Seconds_Viewed_LIVE
,sum(case when live=1 and analysis_right ='Wimbledon - BBC' then total_duration_viewed_seconds else 0 end) 
as WIMBBC_Total_Seconds_Viewed_LIVE
,sum(case when live=1 and analysis_right ='Womens Cricket - I C C Cricket Deal  2012-2015 Sky Sports' then total_duration_viewed_seconds else 0 end) 
as WICCSS_Total_Seconds_Viewed_LIVE
,sum(case when live=1 and analysis_right ='World Athletics Championship - More 4' then total_duration_viewed_seconds else 0 end) 
as WACMR4_Total_Seconds_Viewed_LIVE
,sum(case when live=1 and analysis_right ='World Club Championship - BBC' then total_duration_viewed_seconds else 0 end) 
as WCLBBBC_Total_Seconds_Viewed_LIVE
,sum(case when live=1 and analysis_right ='World Cup Qualifiers - BT Sport' then total_duration_viewed_seconds else 0 end) 
as WCQBTS_Total_Seconds_Viewed_LIVE
,sum(case when live=1 and analysis_right ='World Darts Championship 2009-2012 Sky Sports' then total_duration_viewed_seconds else 0 end) 
as WDCSS_Total_Seconds_Viewed_LIVE
,sum(case when live=1 and analysis_right ='World snooker championship - BBC' then total_duration_viewed_seconds else 0 end) 
as WSCBBC_Total_Seconds_Viewed_LIVE
,sum(case when live=1 and analysis_right ='WWE Sky 1 and 2' then total_duration_viewed_seconds else 0 end) 
as WWES12_Total_Seconds_Viewed_LIVE
,sum(case when live=1 and analysis_right ='WWE Sky Sports' then total_duration_viewed_seconds else 0 end) 
as WWESS_Total_Seconds_Viewed_LIVE
,sum(case when live=0 and analysis_right ='Africa Cup of Nations - Eurosport' then total_duration_viewed_seconds else 0 end) 
as AFCEUR_Total_Seconds_ViewedNon_LIVE
,sum(case when live=0 and analysis_right ='Africa Cup of Nations - ITV' then total_duration_viewed_seconds else 0 end) 
as AFCITV_Total_Seconds_ViewedNon_LIVE
,sum(case when live=0 and analysis_right ='Americas Cup - BBC' then total_duration_viewed_seconds else 0 end) 
as AMCBBC_Total_Seconds_ViewedNon_LIVE
,sum(case when live=0 and analysis_right ='Asian Tour Golf 2012-2013 Sky Sports' then total_duration_viewed_seconds else 0 end) 
as ATGSS_Total_Seconds_ViewedNon_LIVE
,sum(case when live=0 and analysis_right ='ATP Tennis Masters Series  2011-2013 Sky Sports' then total_duration_viewed_seconds else 0 end) 
as ATPSS_Total_Seconds_ViewedNon_LIVE
,sum(case when live=0 and analysis_right ='Australia Home Cricket  2012-2016 Sky Sports' then total_duration_viewed_seconds else 0 end) 
as AHCSS_Total_Seconds_ViewedNon_LIVE
,sum(case when live=0 and analysis_right ='Australian Football - BT Sport' then total_duration_viewed_seconds else 0 end) 
as AUFBTS_Total_Seconds_ViewedNon_LIVE
,sum(case when live=0 and analysis_right ='Australian Open Tennis - BBC' then total_duration_viewed_seconds else 0 end) 
as AOTBBC_Total_Seconds_ViewedNon_LIVE
,sum(case when live=0 and analysis_right ='Australian Open Tennis - Eurosport' then total_duration_viewed_seconds else 0 end) 
as AOTEUR_Total_Seconds_ViewedNon_LIVE
,sum(case when live=0 and analysis_right ='Aviva Premiership - ESPN' then total_duration_viewed_seconds else 0 end) 
as AVPSS_Total_Seconds_ViewedNon_LIVE
,sum(case when live=0 and analysis_right ='BBC American Football' then total_duration_viewed_seconds else 0 end) 
as AFBBC_Total_Seconds_ViewedNon_LIVE
,sum(case when live=0 and analysis_right ='BBC Athletics' then total_duration_viewed_seconds else 0 end) 
as ATHBBC_Total_Seconds_ViewedNon_LIVE
,sum(case when live=0 and analysis_right ='BBC Boxing' then total_duration_viewed_seconds else 0 end) 
as BOXBBC_Total_Seconds_ViewedNon_LIVE
,sum(case when live=0 and analysis_right ='BBC Darts' then total_duration_viewed_seconds else 0 end) 
as DRTBBC_Total_Seconds_ViewedNon_LIVE
,sum(case when live=0 and analysis_right ='BBC Equestrian' then total_duration_viewed_seconds else 0 end) 
as EQUBBC_Total_Seconds_ViewedNon_LIVE
,sum(case when live=0 and analysis_right ='BBC Football' then total_duration_viewed_seconds else 0 end) 
as FOOTBBC_Total_Seconds_ViewedNon_LIVE
,sum(case when live=0 and analysis_right ='BBC Golf' then total_duration_viewed_seconds else 0 end) 
as GOLFBBC_Total_Seconds_ViewedNon_LIVE
,sum(case when live=0 and analysis_right ='BBC Motor Sport' then total_duration_viewed_seconds else 0 end) 
as MSPBBC_Total_Seconds_ViewedNon_LIVE
,sum(case when live=0 and analysis_right ='BBC Rugby' then total_duration_viewed_seconds else 0 end) 
as RUGBBC_Total_Seconds_ViewedNon_LIVE
,sum(case when live=0 and analysis_right ='BBC Snooker/Pool' then total_duration_viewed_seconds else 0 end) 
as SNPBBC_Total_Seconds_ViewedNon_LIVE
,sum(case when live=0 and analysis_right ='BBC Tennis' then total_duration_viewed_seconds else 0 end) 
as TENBBC_Total_Seconds_ViewedNon_LIVE
,sum(case when live=0 and analysis_right ='BBC Unknown' then total_duration_viewed_seconds else 0 end) 
as UNKBBC_Total_Seconds_ViewedNon_LIVE
,sum(case when live=0 and analysis_right ='BBC Watersports' then total_duration_viewed_seconds else 0 end) 
as WATBBC_Total_Seconds_ViewedNon_LIVE
,sum(case when live=0 and analysis_right ='BBC Wintersports' then total_duration_viewed_seconds else 0 end) 
as WINBBC_Total_Seconds_ViewedNon_LIVE
,sum(case when live=0 and analysis_right ='Boxing  - Channel 5' then total_duration_viewed_seconds else 0 end) 
as BOXCH5_Total_Seconds_ViewedNon_LIVE
,sum(case when live=0 and analysis_right ='Boxing - Matchroom 2012-2014 Sky Sports' then total_duration_viewed_seconds else 0 end) 
as BOXMSS_Total_Seconds_ViewedNon_LIVE
,sum(case when live=0 and analysis_right ='Brazil Football - BT Sport' then total_duration_viewed_seconds else 0 end) 
as BFTBTS_Total_Seconds_ViewedNon_LIVE
,sum(case when live=0 and analysis_right ='British & Irish Lions 2013 - Sky Sports' then total_duration_viewed_seconds else 0 end) 
as BILSS_Total_Seconds_ViewedNon_LIVE
,sum(case when live=0 and analysis_right ='British Open Golf - BBC' then total_duration_viewed_seconds else 0 end) 
as BOGSS_Total_Seconds_ViewedNon_LIVE
,sum(case when live=0 and analysis_right ='BT Sport American Football' then total_duration_viewed_seconds else 0 end) 
as AFBTS_Total_Seconds_ViewedNon_LIVE
,sum(case when live=0 and analysis_right ='BT Sport Athletics' then total_duration_viewed_seconds else 0 end) 
as ATHBTS_Total_Seconds_ViewedNon_LIVE
,sum(case when live=0 and analysis_right ='BT Sport Baseball' then total_duration_viewed_seconds else 0 end) 
as BASEBTS_Total_Seconds_ViewedNon_LIVE
,sum(case when live=0 and analysis_right ='BT Sport Basketball' then total_duration_viewed_seconds else 0 end) 
as BASKBTS_Total_Seconds_ViewedNon_LIVE
,sum(case when live=0 and analysis_right ='BT Sport Boxing' then total_duration_viewed_seconds else 0 end) 
as BOXBTS_Total_Seconds_ViewedNon_LIVE
,sum(case when live=0 and analysis_right ='BT Sport Cricket' then total_duration_viewed_seconds else 0 end) 
as CRIBTS_Total_Seconds_ViewedNon_LIVE
,sum(case when live=0 and analysis_right ='BT Sport Equestrian' then total_duration_viewed_seconds else 0 end) 
as EQUBTS_Total_Seconds_ViewedNon_LIVE
,sum(case when live=0 and analysis_right ='BT Sport Extreme' then total_duration_viewed_seconds else 0 end) 
as EXTBTS_Total_Seconds_ViewedNon_LIVE
,sum(case when live=0 and analysis_right ='BT Sport Football' then total_duration_viewed_seconds else 0 end) 
as FOOTBTS_Total_Seconds_ViewedNon_LIVE
,sum(case when live=0 and analysis_right ='BT Sport Motor Sport' then total_duration_viewed_seconds else 0 end) 
as MSPBTS_Total_Seconds_ViewedNon_LIVE
,sum(case when live=0 and analysis_right ='BT Sport Rugby' then total_duration_viewed_seconds else 0 end) 
as RUGBTS_Total_Seconds_ViewedNon_LIVE
,sum(case when live=0 and analysis_right ='BT Sport Tennis' then total_duration_viewed_seconds else 0 end) 
as TENBTS_Total_Seconds_ViewedNon_LIVE
,sum(case when live=0 and analysis_right ='BT Sport Unknown' then total_duration_viewed_seconds else 0 end) 
as UNKBTS_Total_Seconds_ViewedNon_LIVE
,sum(case when live=0 and analysis_right ='BT Sport Wintersports' then total_duration_viewed_seconds else 0 end) 
as WINBTS_Total_Seconds_ViewedNon_LIVE
,sum(case when live=0 and analysis_right ='Bundesliga - BT Sport' then total_duration_viewed_seconds else 0 end) 
as BUNBTS_Total_Seconds_ViewedNon_LIVE
,sum(case when live=0 and analysis_right ='Bundesliga- ESPN' then total_duration_viewed_seconds else 0 end) 
as BUNESPN_Total_Seconds_ViewedNon_LIVE
,sum(case when live=0 and analysis_right ='Challenge Darts' then total_duration_viewed_seconds else 0 end) 
as DRTCHA_Total_Seconds_ViewedNon_LIVE
,sum(case when live=0 and analysis_right ='Challenge Extreme' then total_duration_viewed_seconds else 0 end) 
as EXTCHA_Total_Seconds_ViewedNon_LIVE
,sum(case when live=0 and analysis_right ='Challenge Unknown' then total_duration_viewed_seconds else 0 end) 
as UNKCHA_Total_Seconds_ViewedNon_LIVE
,sum(case when live=0 and analysis_right ='Challenge Wrestling' then total_duration_viewed_seconds else 0 end) 
as WRECHA_Total_Seconds_ViewedNon_LIVE
,sum(case when live=0 and analysis_right ='Champions League - ITV' then total_duration_viewed_seconds else 0 end) 
as CHLITV_Total_Seconds_ViewedNon_LIVE
,sum(case when live=0 and analysis_right ='Champions Trophy - I C C Cricket Deal  2012-2015 Sky Sports' then total_duration_viewed_seconds else 0 end) 
as ICCSS_Total_Seconds_ViewedNon_LIVE
,sum(case when live=0 and analysis_right ='Channel 4 American Football' then total_duration_viewed_seconds else 0 end) 
as AMCH4_Total_Seconds_ViewedNon_LIVE
,sum(case when live=0 and analysis_right ='Channel 4 Athletics' then total_duration_viewed_seconds else 0 end) 
as ATHCH4_Total_Seconds_ViewedNon_LIVE
,sum(case when live=0 and analysis_right ='Channel 4 Boxing' then total_duration_viewed_seconds else 0 end) 
as BOXCH4_Total_Seconds_ViewedNon_LIVE
,sum(case when live=0 and analysis_right ='Channel 4 Equestrian' then total_duration_viewed_seconds else 0 end) 
as EQUCH4_Total_Seconds_ViewedNon_LIVE
,sum(case when live=0 and analysis_right ='Channel 4 Extreme' then total_duration_viewed_seconds else 0 end) 
as EXTCH4_Total_Seconds_ViewedNon_LIVE
,sum(case when live=0 and analysis_right ='Channel 4 Football' then total_duration_viewed_seconds else 0 end) 
as FOOTCH4_Total_Seconds_ViewedNon_LIVE
,sum(case when live=0 and analysis_right ='Channel 4 Motor Sport' then total_duration_viewed_seconds else 0 end) 
as MSPCH4_Total_Seconds_ViewedNon_LIVE
,sum(case when live=0 and analysis_right ='Channel 4 Racing' then total_duration_viewed_seconds else 0 end) 
as RACCH4_Total_Seconds_ViewedNon_LIVE
,sum(case when live=0 and analysis_right ='Channel 4 Unknown' then total_duration_viewed_seconds else 0 end) 
as UNKCH4_Total_Seconds_ViewedNon_LIVE
,sum(case when live=0 and analysis_right ='Channel 4 Watersports' then total_duration_viewed_seconds else 0 end) 
as WATCH4_Total_Seconds_ViewedNon_LIVE
,sum(case when live=0 and analysis_right ='Channel 4 Wintersports' then total_duration_viewed_seconds else 0 end) 
as WINCH4_Total_Seconds_ViewedNon_LIVE
,sum(case when live=0 and analysis_right ='Channel 5 Athletics' then total_duration_viewed_seconds else 0 end) 
as ATHCH5_Total_Seconds_ViewedNon_LIVE
,sum(case when live=0 and analysis_right ='Channel 5 Boxing' then total_duration_viewed_seconds else 0 end) 
as BOXOCH5_Total_Seconds_ViewedNon_LIVE
,sum(case when live=0 and analysis_right ='Channel 5 Cricket' then total_duration_viewed_seconds else 0 end) 
as CRICH5_Total_Seconds_ViewedNon_LIVE
,sum(case when live=0 and analysis_right ='Channel 5 Motor Sport' then total_duration_viewed_seconds else 0 end) 
as MSPCH5_Total_Seconds_ViewedNon_LIVE
,sum(case when live=0 and analysis_right ='Channel 5 Unknown' then total_duration_viewed_seconds else 0 end) 
as UNKCH5_Total_Seconds_ViewedNon_LIVE
,sum(case when live=0 and analysis_right ='Channel 5 Wrestling' then total_duration_viewed_seconds else 0 end) 
as WRECH5_Total_Seconds_ViewedNon_LIVE
,sum(case when live=0 and analysis_right ='Cheltenham Festival - Channel 4' then total_duration_viewed_seconds else 0 end) 
as CHELCH4_Total_Seconds_ViewedNon_LIVE
,sum(case when live=0 and analysis_right ='Community Shield - ITV' then total_duration_viewed_seconds else 0 end) 
as CMSITV_Total_Seconds_ViewedNon_LIVE
,sum(case when live=0 and analysis_right ='Confederations Cup - BBC' then total_duration_viewed_seconds else 0 end) 
as CONCBBC_Total_Seconds_ViewedNon_LIVE
,sum(case when live=0 and analysis_right ='Conference - BT Sport' then total_duration_viewed_seconds else 0 end) 
as CONFBTS_Total_Seconds_ViewedNon_LIVE
,sum(case when live=0 and analysis_right ='Cycling - La Vuelta ITV' then total_duration_viewed_seconds else 0 end) 
as CLVITV_Total_Seconds_ViewedNon_LIVE
,sum(case when live=0 and analysis_right ='Cycling - U C I World Tour Sky Sports' then total_duration_viewed_seconds else 0 end) 
as CUCISS_Total_Seconds_ViewedNon_LIVE
,sum(case when live=0 and analysis_right ='Cycling Tour of Britain - Eurosport' then total_duration_viewed_seconds else 0 end) 
as CTBEUR_Total_Seconds_ViewedNon_LIVE
,sum(case when live=0 and analysis_right ='Cycling: tour of britain ITV4' then total_duration_viewed_seconds else 0 end) 
as CTCITV_Total_Seconds_ViewedNon_LIVE
,sum(case when live=0 and analysis_right ='Derby - Channel 4' then total_duration_viewed_seconds else 0 end) 
as DERCH4_Total_Seconds_ViewedNon_LIVE
,sum(case when live=0 and analysis_right ='ECB (highlights) - Channel 5' then total_duration_viewed_seconds else 0 end) 
as ECBHCH5_Total_Seconds_ViewedNon_LIVE
,sum(case when live=0 and analysis_right ='ECB Cricket Sky Sports' then total_duration_viewed_seconds else 0 end) 
as GECRSS_Total_Seconds_ViewedNon_LIVE
,sum(case when live=0 and analysis_right ='ECB non-Test Cricket Sky Sports' then total_duration_viewed_seconds else 0 end) 
as ECBNSS_Total_Seconds_ViewedNon_LIVE
,sum(case when live=0 and analysis_right ='ECB Test Cricket Sky Sports' then total_duration_viewed_seconds else 0 end) 
as ECBTSS_Total_Seconds_ViewedNon_LIVE
,sum(case when live=0 and analysis_right ='England Football Internationals - ITV' then total_duration_viewed_seconds else 0 end) 
as GENGITV_Total_Seconds_ViewedNon_LIVE
,sum(case when live=0 and analysis_right ='England Friendlies (Football) - ITV' then total_duration_viewed_seconds else 0 end) 
as EFRITV_Total_Seconds_ViewedNon_LIVE
,sum(case when live=0 and analysis_right ='England Rugby Internationals 2010-2015 - Sky Sports' then total_duration_viewed_seconds else 0 end) 
as ENRSS_Total_Seconds_ViewedNon_LIVE
,sum(case when live=0 and analysis_right ='England World Cup Qualifying (Away) - ITV' then total_duration_viewed_seconds else 0 end) 
as EWQAITV_Total_Seconds_ViewedNon_LIVE
,sum(case when live=0 and analysis_right ='England World Cup Qualifying (Home) - ITV' then total_duration_viewed_seconds else 0 end) 
as EWQHITV_Total_Seconds_ViewedNon_LIVE
,sum(case when live=0 and analysis_right ='ESPN American Football' then total_duration_viewed_seconds else 0 end) 
as AMESPN_Total_Seconds_ViewedNon_LIVE
,sum(case when live=0 and analysis_right ='ESPN Athletics' then total_duration_viewed_seconds else 0 end) 
as ATHESPN_Total_Seconds_ViewedNon_LIVE
,sum(case when live=0 and analysis_right ='ESPN Baseball' then total_duration_viewed_seconds else 0 end) 
as BASEESPN_Total_Seconds_ViewedNon_LIVE
,sum(case when live=0 and analysis_right ='ESPN Basketball' then total_duration_viewed_seconds else 0 end) 
as BASKESPN_Total_Seconds_ViewedNon_LIVE
,sum(case when live=0 and analysis_right ='ESPN Boxing' then total_duration_viewed_seconds else 0 end) 
as BOXESPN_Total_Seconds_ViewedNon_LIVE
,sum(case when live=0 and analysis_right ='ESPN Cricket' then total_duration_viewed_seconds else 0 end) 
as CRIESPN_Total_Seconds_ViewedNon_LIVE
,sum(case when live=0 and analysis_right ='ESPN Darts' then total_duration_viewed_seconds else 0 end) 
as DARTESPN_Total_Seconds_ViewedNon_LIVE
,sum(case when live=0 and analysis_right ='ESPN Equestrian' then total_duration_viewed_seconds else 0 end) 
as EQUESPN_Total_Seconds_ViewedNon_LIVE
,sum(case when live=0 and analysis_right ='ESPN Extreme' then total_duration_viewed_seconds else 0 end) 
as EXTESPN_Total_Seconds_ViewedNon_LIVE
,sum(case when live=0 and analysis_right ='ESPN Football' then total_duration_viewed_seconds else 0 end) 
as FOOTESPN_Total_Seconds_ViewedNon_LIVE
,sum(case when live=0 and analysis_right ='ESPN Golf' then total_duration_viewed_seconds else 0 end) 
as GOLFESPN_Total_Seconds_ViewedNon_LIVE
,sum(case when live=0 and analysis_right ='ESPN Ice Hockey' then total_duration_viewed_seconds else 0 end) 
as IHESPN_Total_Seconds_ViewedNon_LIVE
,sum(case when live=0 and analysis_right ='ESPN Motor Sport' then total_duration_viewed_seconds else 0 end) 
as MSPESPN_Total_Seconds_ViewedNon_LIVE
,sum(case when live=0 and analysis_right ='ESPN Racing' then total_duration_viewed_seconds else 0 end) 
as RACESPN_Total_Seconds_ViewedNon_LIVE
,sum(case when live=0 and analysis_right ='ESPN Rugby' then total_duration_viewed_seconds else 0 end) 
as RUGESPN_Total_Seconds_ViewedNon_LIVE
,sum(case when live=0 and analysis_right ='ESPN Snooker/Pool' then total_duration_viewed_seconds else 0 end) 
as SNPESPN_Total_Seconds_ViewedNon_LIVE
,sum(case when live=0 and analysis_right ='ESPN Tennis' then total_duration_viewed_seconds else 0 end) 
as TENESPN_Total_Seconds_ViewedNon_LIVE
,sum(case when live=0 and analysis_right ='ESPN Unknown' then total_duration_viewed_seconds else 0 end) 
as UNKESPN_Total_Seconds_ViewedNon_LIVE
,sum(case when live=0 and analysis_right ='ESPN Watersports' then total_duration_viewed_seconds else 0 end) 
as WATESPN_Total_Seconds_ViewedNon_LIVE
,sum(case when live=0 and analysis_right ='ESPN Wintersports' then total_duration_viewed_seconds else 0 end) 
as WINESPN_Total_Seconds_ViewedNon_LIVE
,sum(case when live=0 and analysis_right ='ESPN Wrestling' then total_duration_viewed_seconds else 0 end) 
as WREESPN_Total_Seconds_ViewedNon_LIVE
,sum(case when live=0 and analysis_right ='Europa League - BT Sport' then total_duration_viewed_seconds else 0 end) 
as ELBTSP_Total_Seconds_ViewedNon_LIVE
,sum(case when live=0 and analysis_right ='Europa League - ESPN' then total_duration_viewed_seconds else 0 end) 
as ELESPN_Total_Seconds_ViewedNon_LIVE
,sum(case when live=0 and analysis_right ='Europa League - ITV' then total_duration_viewed_seconds else 0 end) 
as ELITV_Total_Seconds_ViewedNon_LIVE
,sum(case when live=0 and analysis_right ='European Tour Golf - Sky Sports' then total_duration_viewed_seconds else 0 end) 
as ETGSS_Total_Seconds_ViewedNon_LIVE
,sum(case when live=0 and analysis_right ='Eurosport American Football' then total_duration_viewed_seconds else 0 end) 
as AMEUR_Total_Seconds_ViewedNon_LIVE
,sum(case when live=0 and analysis_right ='Eurosport Athletics' then total_duration_viewed_seconds else 0 end) 
as ATHEUR_Total_Seconds_ViewedNon_LIVE
,sum(case when live=0 and analysis_right ='Eurosport Baseball' then total_duration_viewed_seconds else 0 end) 
as BASEEUR_Total_Seconds_ViewedNon_LIVE
,sum(case when live=0 and analysis_right ='Eurosport Basketball' then total_duration_viewed_seconds else 0 end) 
as BASKEUR_Total_Seconds_ViewedNon_LIVE
,sum(case when live=0 and analysis_right ='Eurosport Boxing' then total_duration_viewed_seconds else 0 end) 
as BOXEUR_Total_Seconds_ViewedNon_LIVE
,sum(case when live=0 and analysis_right ='Eurosport Cricket' then total_duration_viewed_seconds else 0 end) 
as CRIEUR_Total_Seconds_ViewedNon_LIVE
,sum(case when live=0 and analysis_right ='Eurosport Darts' then total_duration_viewed_seconds else 0 end) 
as DARTEUR_Total_Seconds_ViewedNon_LIVE
,sum(case when live=0 and analysis_right ='Eurosport Equestrian' then total_duration_viewed_seconds else 0 end) 
as EQUEUR_Total_Seconds_ViewedNon_LIVE
,sum(case when live=0 and analysis_right ='Eurosport Extreme' then total_duration_viewed_seconds else 0 end) 
as EXTEUR_Total_Seconds_ViewedNon_LIVE
,sum(case when live=0 and analysis_right ='Eurosport Football' then total_duration_viewed_seconds else 0 end) 
as FOOTEUR_Total_Seconds_ViewedNon_LIVE
,sum(case when live=0 and analysis_right ='Eurosport Golf' then total_duration_viewed_seconds else 0 end) 
as GOLFEUR_Total_Seconds_ViewedNon_LIVE
,sum(case when live=0 and analysis_right ='Eurosport Ice Hockey' then total_duration_viewed_seconds else 0 end) 
as IHEUR_Total_Seconds_ViewedNon_LIVE
,sum(case when live=0 and analysis_right ='Eurosport Motor Sport' then total_duration_viewed_seconds else 0 end) 
as MSPEUR_Total_Seconds_ViewedNon_LIVE
,sum(case when live=0 and analysis_right ='Eurosport Racing' then total_duration_viewed_seconds else 0 end) 
as RACEUR_Total_Seconds_ViewedNon_LIVE
,sum(case when live=0 and analysis_right ='Eurosport Snooker/Pool' then total_duration_viewed_seconds else 0 end) 
as SNPEUR_Total_Seconds_ViewedNon_LIVE
,sum(case when live=0 and analysis_right ='Eurosport Tennis' then total_duration_viewed_seconds else 0 end) 
as TENEUR_Total_Seconds_ViewedNon_LIVE
,sum(case when live=0 and analysis_right ='Eurosport Unknown' then total_duration_viewed_seconds else 0 end) 
as UNKEUR_Total_Seconds_ViewedNon_LIVE
,sum(case when live=0 and analysis_right ='Eurosport Watersports' then total_duration_viewed_seconds else 0 end) 
as WATEUR_Total_Seconds_ViewedNon_LIVE
,sum(case when live=0 and analysis_right ='Eurosport Wintersports' then total_duration_viewed_seconds else 0 end) 
as WINEUR_Total_Seconds_ViewedNon_LIVE
,sum(case when live=0 and analysis_right ='F1 - BBC' then total_duration_viewed_seconds else 0 end) 
as GF1BBC_Total_Seconds_ViewedNon_LIVE
,sum(case when live=0 and analysis_right ='F1 - Sky Sports' then total_duration_viewed_seconds else 0 end) 
as GF1SS_Total_Seconds_ViewedNon_LIVE
,sum(case when live=0 and analysis_right ='F1 (non-Live)- BBC' then total_duration_viewed_seconds else 0 end) 
as F1NBBC_Total_Seconds_ViewedNon_LIVE
,sum(case when live=0 and analysis_right ='F1 (Practice Live)- BBC' then total_duration_viewed_seconds else 0 end) 
as F1PBBC_Total_Seconds_ViewedNon_LIVE
,sum(case when live=0 and analysis_right ='F1 (Qualifying Live)- BBC' then total_duration_viewed_seconds else 0 end) 
as F1QBBC_Total_Seconds_ViewedNon_LIVE
,sum(case when live=0 and analysis_right ='F1 (Race Live)- BBC' then total_duration_viewed_seconds else 0 end) 
as F1RBBC_Total_Seconds_ViewedNon_LIVE
,sum(case when live=0 and analysis_right ='FA Cup - ESPN' then total_duration_viewed_seconds else 0 end) 
as FACESPN_Total_Seconds_ViewedNon_LIVE
,sum(case when live=0 and analysis_right ='FA Cup - ITV' then total_duration_viewed_seconds else 0 end) 
as FACITV_Total_Seconds_ViewedNon_LIVE
,sum(case when live=0 and analysis_right ='Football League - Seasons 2012/13 To 14/15 Sky Sports - Capital 1 Cup' then total_duration_viewed_seconds else 0 end) 
as FLCCSS_Total_Seconds_ViewedNon_LIVE
,sum(case when live=0 and analysis_right ='Football League - Seasons 2012/13 To 14/15 Sky Sports - others' then total_duration_viewed_seconds else 0 end) 
as FLOTSS_Total_Seconds_ViewedNon_LIVE
,sum(case when live=0 and analysis_right ='Formula One 2012-2018 - (non-Live) Sky Sports' then total_duration_viewed_seconds else 0 end) 
as F1NSS_Total_Seconds_ViewedNon_LIVE
,sum(case when live=0 and analysis_right ='Formula One 2012-2018 - (Practice Live) Sky Sports' then total_duration_viewed_seconds else 0 end) 
as F1PSS_Total_Seconds_ViewedNon_LIVE
,sum(case when live=0 and analysis_right ='Formula One 2012-2018 - (Qualifying Live) Sky Sports' then total_duration_viewed_seconds else 0 end) 
as F1QSS_Total_Seconds_ViewedNon_LIVE
,sum(case when live=0 and analysis_right ='Formula One 2012-2018 - (Race Live) Sky Sports' then total_duration_viewed_seconds else 0 end) 
as F1RSS_Total_Seconds_ViewedNon_LIVE
,sum(case when live=0 and analysis_right ='French Open Tennis - Eurosport' then total_duration_viewed_seconds else 0 end) 
as FOTEUR_Total_Seconds_ViewedNon_LIVE
,sum(case when live=0 and analysis_right ='French Open Tennis - ITV' then total_duration_viewed_seconds else 0 end) 
as FOTITV_Total_Seconds_ViewedNon_LIVE
,sum(case when live=0 and analysis_right ='Grand National - Channel 4' then total_duration_viewed_seconds else 0 end) 
as GDNCH4_Total_Seconds_ViewedNon_LIVE
,sum(case when live=0 and analysis_right ='Heineken Cup 2010/11 To 2013/14- Sky Sports' then total_duration_viewed_seconds else 0 end) 
as HECSS_Total_Seconds_ViewedNon_LIVE
,sum(case when live=0 and analysis_right ='I R B Sevens 2008/09 -2014/15 - Sky Sports' then total_duration_viewed_seconds else 0 end) 
as IRBSS_Total_Seconds_ViewedNon_LIVE
,sum(case when live=0 and analysis_right ='IAAF World Athletics Championship - Eurosport' then total_duration_viewed_seconds else 0 end) 
as WACEUR_Total_Seconds_ViewedNon_LIVE
,sum(case when live=0 and analysis_right ='India Home Cricket 2012-2018 Sky Sports' then total_duration_viewed_seconds else 0 end) 
as IHCSS_Total_Seconds_ViewedNon_LIVE
,sum(case when live=0 and analysis_right ='India Premier League - ITV' then total_duration_viewed_seconds else 0 end) 
as IPLITV_Total_Seconds_ViewedNon_LIVE
,sum(case when live=0 and analysis_right ='International Freindlies - ESPN' then total_duration_viewed_seconds else 0 end) 
as IFESPN_Total_Seconds_ViewedNon_LIVE
,sum(case when live=0 and analysis_right ='International Friendlies - BT Sport' then total_duration_viewed_seconds else 0 end) 
as IFBTS_Total_Seconds_ViewedNon_LIVE
,sum(case when live=0 and analysis_right ='ITV1 Boxing' then total_duration_viewed_seconds else 0 end) 
as BOXITV1_Total_Seconds_ViewedNon_LIVE
,sum(case when live=0 and analysis_right ='ITV1 Football' then total_duration_viewed_seconds else 0 end) 
as FOOTITV1_Total_Seconds_ViewedNon_LIVE
,sum(case when live=0 and analysis_right ='ITV1 Motor Sport' then total_duration_viewed_seconds else 0 end) 
as MOTSITV1_Total_Seconds_ViewedNon_LIVE
,sum(case when live=0 and analysis_right ='ITV1 Rugby' then total_duration_viewed_seconds else 0 end) 
as RUGITV1_Total_Seconds_ViewedNon_LIVE
,sum(case when live=0 and analysis_right ='ITV1 Snooker/Pool' then total_duration_viewed_seconds else 0 end) 
as SNPITV1_Total_Seconds_ViewedNon_LIVE
,sum(case when live=0 and analysis_right ='ITV1 Unknown' then total_duration_viewed_seconds else 0 end) 
as UNKITV1_Total_Seconds_ViewedNon_LIVE
,sum(case when live=0 and analysis_right ='ITV4 Boxing' then total_duration_viewed_seconds else 0 end) 
as BOXITV4_Total_Seconds_ViewedNon_LIVE
,sum(case when live=0 and analysis_right ='ITV4 Cricket' then total_duration_viewed_seconds else 0 end) 
as CRIITV4_Total_Seconds_ViewedNon_LIVE
,sum(case when live=0 and analysis_right ='ITV4 Darts' then total_duration_viewed_seconds else 0 end) 
as DARTITV4_Total_Seconds_ViewedNon_LIVE
,sum(case when live=0 and analysis_right ='ITV4 Extreme' then total_duration_viewed_seconds else 0 end) 
as EXTITV4_Total_Seconds_ViewedNon_LIVE
,sum(case when live=0 and analysis_right ='ITV4 Football' then total_duration_viewed_seconds else 0 end) 
as FOOTITV4_Total_Seconds_ViewedNon_LIVE
,sum(case when live=0 and analysis_right ='ITV4 Motor Sport' then total_duration_viewed_seconds else 0 end) 
as MSPITV4_Total_Seconds_ViewedNon_LIVE
,sum(case when live=0 and analysis_right ='ITV4 Rugby' then total_duration_viewed_seconds else 0 end) 
as RUGITV4_Total_Seconds_ViewedNon_LIVE
,sum(case when live=0 and analysis_right ='ITV4 Snooker/Pool' then total_duration_viewed_seconds else 0 end) 
as SNPITV4_Total_Seconds_ViewedNon_LIVE
,sum(case when live=0 and analysis_right ='ITV4 Tennis' then total_duration_viewed_seconds else 0 end) 
as TENITV4_Total_Seconds_ViewedNon_LIVE
,sum(case when live=0 and analysis_right ='ITV4 Unknown' then total_duration_viewed_seconds else 0 end) 
as UNKITV4_Total_Seconds_ViewedNon_LIVE
,sum(case when live=0 and analysis_right ='Ligue 1 - BT Sport' then total_duration_viewed_seconds else 0 end) 
as L1BTS_Total_Seconds_ViewedNon_LIVE
,sum(case when live=0 and analysis_right ='Ligue 1 - ESPN' then total_duration_viewed_seconds else 0 end) 
as L1ESPN_Total_Seconds_ViewedNon_LIVE
,sum(case when live=0 and analysis_right ='Match of the day - BBC' then total_duration_viewed_seconds else 0 end) 
as MOTDBBC_Total_Seconds_ViewedNon_LIVE
,sum(case when live=0 and analysis_right ='Matchroom Output Deal  2010-2012 (Other)- Sky Sports' then total_duration_viewed_seconds else 0 end) 
as MROSS_Total_Seconds_ViewedNon_LIVE
,sum(case when live=0 and analysis_right ='Matchroom Output Deal  2010-2012 (Pool)- Sky Sports' then total_duration_viewed_seconds else 0 end) 
as MRPSS_Total_Seconds_ViewedNon_LIVE
,sum(case when live=0 and analysis_right ='Matchroom Output Deal  2010-2012 (Snooker)- Sky Sports' then total_duration_viewed_seconds else 0 end) 
as MRSSS_Total_Seconds_ViewedNon_LIVE
,sum(case when live=0 and analysis_right ='Moto GP BBC' then total_duration_viewed_seconds else 0 end) 
as MGPBBC_Total_Seconds_ViewedNon_LIVE
,sum(case when live=0 and analysis_right ='NBA - Sky Sports' then total_duration_viewed_seconds else 0 end) 
as NBASS_Total_Seconds_ViewedNon_LIVE
,sum(case when live=0 and analysis_right ='NFL - BBC' then total_duration_viewed_seconds else 0 end) 
as NFLBBC_Total_Seconds_ViewedNon_LIVE
,sum(case when live=0 and analysis_right ='NFL - Channel 4' then total_duration_viewed_seconds else 0 end) 
as NFLCH4_Total_Seconds_ViewedNon_LIVE
,sum(case when live=0 and analysis_right ='NFL Seasons 2012-2015 - Sky Sports' then total_duration_viewed_seconds else 0 end) 
as NFLSS_Total_Seconds_ViewedNon_LIVE
,sum(case when live=0 and analysis_right ='Northern Ireland Football Association 2012-2014 - Sky Sports' then total_duration_viewed_seconds else 0 end) 
as NIFSS_Total_Seconds_ViewedNon_LIVE
,sum(case when live=0 and analysis_right ='Oaks - Channel 4' then total_duration_viewed_seconds else 0 end) 
as OAKCH4_Total_Seconds_ViewedNon_LIVE
,sum(case when live=0 and analysis_right ='Other American Football' then total_duration_viewed_seconds else 0 end) 
as AMOTH_Total_Seconds_ViewedNon_LIVE
,sum(case when live=0 and analysis_right ='Other Athletics' then total_duration_viewed_seconds else 0 end) 
as ATHOTH_Total_Seconds_ViewedNon_LIVE
,sum(case when live=0 and analysis_right ='Other Baseball' then total_duration_viewed_seconds else 0 end) 
as BASEOTH_Total_Seconds_ViewedNon_LIVE
,sum(case when live=0 and analysis_right ='Other Basketball' then total_duration_viewed_seconds else 0 end) 
as BASKOTH_Total_Seconds_ViewedNon_LIVE
,sum(case when live=0 and analysis_right ='Other Boxing' then total_duration_viewed_seconds else 0 end) 
as BOXOTH_Total_Seconds_ViewedNon_LIVE
,sum(case when live=0 and analysis_right ='Other Cricket' then total_duration_viewed_seconds else 0 end) 
as CRIOTH_Total_Seconds_ViewedNon_LIVE
,sum(case when live=0 and analysis_right ='Other Darts' then total_duration_viewed_seconds else 0 end) 
as DARTOTH_Total_Seconds_ViewedNon_LIVE
,sum(case when live=0 and analysis_right ='Other Equestrian' then total_duration_viewed_seconds else 0 end) 
as EQUOTH_Total_Seconds_ViewedNon_LIVE
,sum(case when live=0 and analysis_right ='Other Extreme' then total_duration_viewed_seconds else 0 end) 
as EXTOTH_Total_Seconds_ViewedNon_LIVE
,sum(case when live=0 and analysis_right ='Other Fishing' then total_duration_viewed_seconds else 0 end) 
as FSHOTH_Total_Seconds_ViewedNon_LIVE
,sum(case when live=0 and analysis_right ='Other Football' then total_duration_viewed_seconds else 0 end) 
as FOOTOTH_Total_Seconds_ViewedNon_LIVE
,sum(case when live=0 and analysis_right ='Other Golf' then total_duration_viewed_seconds else 0 end) 
as GOLFOTH_Total_Seconds_ViewedNon_LIVE
,sum(case when live=0 and analysis_right ='Other Ice Hockey' then total_duration_viewed_seconds else 0 end) 
as IHOTH_Total_Seconds_ViewedNon_LIVE
,sum(case when live=0 and analysis_right ='Other Motor Sport' then total_duration_viewed_seconds else 0 end) 
as MSPOTH_Total_Seconds_ViewedNon_LIVE
,sum(case when live=0 and analysis_right ='Other Racing' then total_duration_viewed_seconds else 0 end) 
as RACOTH_Total_Seconds_ViewedNon_LIVE
,sum(case when live=0 and analysis_right ='Other Rugby' then total_duration_viewed_seconds else 0 end) 
as RUGOTH_Total_Seconds_ViewedNon_LIVE
,sum(case when live=0 and analysis_right ='Other Rugby Internationals - ESPN' then total_duration_viewed_seconds else 0 end) 
as ORUGESPN_Total_Seconds_ViewedNon_LIVE
,sum(case when live=0 and analysis_right ='Other Snooker/Pool' then total_duration_viewed_seconds else 0 end) 
as OTHSNP_Total_Seconds_ViewedNon_LIVE
,sum(case when live=0 and analysis_right ='Other Tennis' then total_duration_viewed_seconds else 0 end) 
as OTHTEN_Total_Seconds_ViewedNon_LIVE
,sum(case when live=0 and analysis_right ='Other Unknown' then total_duration_viewed_seconds else 0 end) 
as OTHUNK_Total_Seconds_ViewedNon_LIVE
,sum(case when live=0 and analysis_right ='Other Watersports' then total_duration_viewed_seconds else 0 end) 
as OTHWAT_Total_Seconds_ViewedNon_LIVE
,sum(case when live=0 and analysis_right ='Other Wintersports' then total_duration_viewed_seconds else 0 end) 
as OTHWIN_Total_Seconds_ViewedNon_LIVE
,sum(case when live=0 and analysis_right ='Other Wrestling' then total_duration_viewed_seconds else 0 end) 
as OTHWRE_Total_Seconds_ViewedNon_LIVE
,sum(case when live=0 and analysis_right ='PGA Tour Golf 2010-2017 - Sky Sports' then total_duration_viewed_seconds else 0 end) 
as PGASS_Total_Seconds_ViewedNon_LIVE
,sum(case when live=0 and analysis_right ='Premier League - BT Sport' then total_duration_viewed_seconds else 0 end) 
as PLBTS_Total_Seconds_ViewedNon_LIVE
,sum(case when live=0 and analysis_right ='Premier League - ESPN' then total_duration_viewed_seconds else 0 end) 
as PLESPN_Total_Seconds_ViewedNon_LIVE
,sum(case when live=0 and analysis_right ='Premier League Darts 2010-2013 - Sky Sports' then total_duration_viewed_seconds else 0 end) 
as PLDSS_Total_Seconds_ViewedNon_LIVE
,sum(case when live=0 and analysis_right ='Premier League Football - Sky Sports' then total_duration_viewed_seconds else 0 end) 
as GPLSS_Total_Seconds_ViewedNon_LIVE
,sum(case when live=0 and analysis_right ='Premier League Football - Sky Sports (Match Choice)' then total_duration_viewed_seconds else 0 end) 
as PLMCSS_Total_Seconds_ViewedNon_LIVE
,sum(case when live=0 and analysis_right ='Premier League Football - Sky Sports (MNF)' then total_duration_viewed_seconds else 0 end) 
as PLMNFSS_Total_Seconds_ViewedNon_LIVE
,sum(case when live=0 and analysis_right ='Premier League Football - Sky Sports (non Live)' then total_duration_viewed_seconds else 0 end) 
as PLNLSS_Total_Seconds_ViewedNon_LIVE
,sum(case when live=0 and analysis_right ='Premier League Football - Sky Sports (other Live)' then total_duration_viewed_seconds else 0 end) 
as PLOLSS_Total_Seconds_ViewedNon_LIVE
,sum(case when live=0 and analysis_right ='Premier League Football - Sky Sports (Sat Lunchtime)' then total_duration_viewed_seconds else 0 end) 
as PLSLSS_Total_Seconds_ViewedNon_LIVE
,sum(case when live=0 and analysis_right ='Premier League Football - Sky Sports (Sat Night Live)' then total_duration_viewed_seconds else 0 end) 
as PLSNSS_Total_Seconds_ViewedNon_LIVE
,sum(case when live=0 and analysis_right ='Premier League Football - Sky Sports (Sun 4pm)' then total_duration_viewed_seconds else 0 end) 
as PLS4SS_Total_Seconds_ViewedNon_LIVE
,sum(case when live=0 and analysis_right ='Premier League Football - Sky Sports (Sun Lunchtime)' then total_duration_viewed_seconds else 0 end) 
as PLSULSS_Total_Seconds_ViewedNon_LIVE
,sum(case when live=0 and analysis_right ='Premiership Rugby - Sky Sports' then total_duration_viewed_seconds else 0 end) 
as PRUSS_Total_Seconds_ViewedNon_LIVE
,sum(case when live=0 and analysis_right ='Republic Of Ireland FA - 2010/11 To 2013/14 - Sky Sports' then total_duration_viewed_seconds else 0 end) 
as ROISS_Total_Seconds_ViewedNon_LIVE
,sum(case when live=0 and analysis_right ='Royal Ascot - Channel 4' then total_duration_viewed_seconds else 0 end) 
as RASCH4_Total_Seconds_ViewedNon_LIVE
,sum(case when live=0 and analysis_right ='Rugby Internationals (England) - BBC' then total_duration_viewed_seconds else 0 end) 
as RIEBBC_Total_Seconds_ViewedNon_LIVE
,sum(case when live=0 and analysis_right ='Rugby Internationals (Ireland) - BBC' then total_duration_viewed_seconds else 0 end) 
as RIIBBC_Total_Seconds_ViewedNon_LIVE
,sum(case when live=0 and analysis_right ='Rugby Internationals (Scotland) - BBC' then total_duration_viewed_seconds else 0 end) 
as RISBBC_Total_Seconds_ViewedNon_LIVE
,sum(case when live=0 and analysis_right ='Rugby Internationals (Wales) - BBC' then total_duration_viewed_seconds else 0 end) 
as RIWBBC_Total_Seconds_ViewedNon_LIVE
,sum(case when live=0 and analysis_right ='Rugby League  Challenge Cup- BBC' then total_duration_viewed_seconds else 0 end) 
as RLCCBBC_Total_Seconds_ViewedNon_LIVE
,sum(case when live=0 and analysis_right ='Rugby League - Sky Sports' then total_duration_viewed_seconds else 0 end) 
as RLGSS_Total_Seconds_ViewedNon_LIVE
,sum(case when live=0 and analysis_right ='Rugby League  World Cup- BBC' then total_duration_viewed_seconds else 0 end) 
as RLWCBBC_Total_Seconds_ViewedNon_LIVE
,sum(case when live=0 and analysis_right ='Sanzar Rugby Union 2011 - 2015 - Sky Sports' then total_duration_viewed_seconds else 0 end) 
as SARUSS_Total_Seconds_ViewedNon_LIVE
,sum(case when live=0 and analysis_right ='Scottish FA Deal - 2010/11 To 2013/14 - Sky Sports' then total_duration_viewed_seconds else 0 end) 
as SFASS_Total_Seconds_ViewedNon_LIVE
,sum(case when live=0 and analysis_right ='Serie A - BT Sport' then total_duration_viewed_seconds else 0 end) 
as SABTS_Total_Seconds_ViewedNon_LIVE
,sum(case when live=0 and analysis_right ='Serie A - ESPN' then total_duration_viewed_seconds else 0 end) 
as SAESPN_Total_Seconds_ViewedNon_LIVE
,sum(case when live=0 and analysis_right ='SFL - ESPN' then total_duration_viewed_seconds else 0 end) 
as SFLESPN_Total_Seconds_ViewedNon_LIVE
,sum(case when live=0 and analysis_right ='Six Nations - BBC' then total_duration_viewed_seconds else 0 end) 
as SNRBBC_Total_Seconds_ViewedNon_LIVE
,sum(case when live=0 and analysis_right ='Sky 1 and Sky 2 Boxing' then total_duration_viewed_seconds else 0 end) 
as BOXS12_Total_Seconds_ViewedNon_LIVE
,sum(case when live=0 and analysis_right ='Sky 1 and Sky 2 Football' then total_duration_viewed_seconds else 0 end) 
as FOOTS12_Total_Seconds_ViewedNon_LIVE
,sum(case when live=0 and analysis_right ='Sky 1 and Sky 2 Motor Sport' then total_duration_viewed_seconds else 0 end) 
as MSPS12_Total_Seconds_ViewedNon_LIVE
,sum(case when live=0 and analysis_right ='Sky 1 and Sky 2 Unknown' then total_duration_viewed_seconds else 0 end) 
as UNKS12_Total_Seconds_ViewedNon_LIVE
,sum(case when live=0 and analysis_right ='Sky 1 and Sky 2 Wrestling' then total_duration_viewed_seconds else 0 end) 
as WRES12_Total_Seconds_ViewedNon_LIVE
,sum(case when live=0 and analysis_right ='Sky Sports American Football' then total_duration_viewed_seconds else 0 end) 
as AMSS_Total_Seconds_ViewedNon_LIVE
,sum(case when live=0 and analysis_right ='Sky Sports Athletics' then total_duration_viewed_seconds else 0 end) 
as ATHSS_Total_Seconds_ViewedNon_LIVE
,sum(case when live=0 and analysis_right ='Sky Sports Baseball' then total_duration_viewed_seconds else 0 end) 
as BASESS_Total_Seconds_ViewedNon_LIVE
,sum(case when live=0 and analysis_right ='Sky Sports Basketball' then total_duration_viewed_seconds else 0 end) 
as BASKSS_Total_Seconds_ViewedNon_LIVE
,sum(case when live=0 and analysis_right ='Sky Sports Boxing' then total_duration_viewed_seconds else 0 end) 
as BOXSS_Total_Seconds_ViewedNon_LIVE
,sum(case when live=0 and analysis_right ='Sky Sports Cricket' then total_duration_viewed_seconds else 0 end) 
as CRISS_Total_Seconds_ViewedNon_LIVE
,sum(case when live=0 and analysis_right ='Sky Sports Darts' then total_duration_viewed_seconds else 0 end) 
as DARTSS_Total_Seconds_ViewedNon_LIVE
,sum(case when live=0 and analysis_right ='Sky Sports Equestrian' then total_duration_viewed_seconds else 0 end) 
as EQUSS_Total_Seconds_ViewedNon_LIVE
,sum(case when live=0 and analysis_right ='Sky Sports Extreme' then total_duration_viewed_seconds else 0 end) 
as EXTSS_Total_Seconds_ViewedNon_LIVE
,sum(case when live=0 and analysis_right ='Sky Sports Fishing' then total_duration_viewed_seconds else 0 end) 
as FISHSS_Total_Seconds_ViewedNon_LIVE
,sum(case when live=0 and analysis_right ='Sky Sports Football' then total_duration_viewed_seconds else 0 end) 
as FOOTSS_Total_Seconds_ViewedNon_LIVE
,sum(case when live=0 and analysis_right ='Sky Sports Golf' then total_duration_viewed_seconds else 0 end) 
as GOLFSS_Total_Seconds_ViewedNon_LIVE
,sum(case when live=0 and analysis_right ='Sky Sports Ice Hockey' then total_duration_viewed_seconds else 0 end) 
as IHSS_Total_Seconds_ViewedNon_LIVE
,sum(case when live=0 and analysis_right ='Sky Sports Motor Sport' then total_duration_viewed_seconds else 0 end) 
as MSPSS_Total_Seconds_ViewedNon_LIVE
,sum(case when live=0 and analysis_right ='Sky Sports Racing' then total_duration_viewed_seconds else 0 end) 
as RACSS_Total_Seconds_ViewedNon_LIVE
,sum(case when live=0 and analysis_right ='Sky Sports Rugby' then total_duration_viewed_seconds else 0 end) 
as RUGSS_Total_Seconds_ViewedNon_LIVE
,sum(case when live=0 and analysis_right ='Sky Sports Snooker/Pool' then total_duration_viewed_seconds else 0 end) 
as SNPSS_Total_Seconds_ViewedNon_LIVE
,sum(case when live=0 and analysis_right ='Sky Sports Tennis' then total_duration_viewed_seconds else 0 end) 
as TENSS_Total_Seconds_ViewedNon_LIVE
,sum(case when live=0 and analysis_right ='Sky Sports Unknown' then total_duration_viewed_seconds else 0 end) 
as UNKSS_Total_Seconds_ViewedNon_LIVE
,sum(case when live=0 and analysis_right ='Sky Sports Watersports' then total_duration_viewed_seconds else 0 end) 
as WATSS_Total_Seconds_ViewedNon_LIVE
,sum(case when live=0 and analysis_right ='Sky Sports Wintersports' then total_duration_viewed_seconds else 0 end) 
as WINSS_Total_Seconds_ViewedNon_LIVE
,sum(case when live=0 and analysis_right ='Sky Sports Wrestling' then total_duration_viewed_seconds else 0 end) 
as WRESS_Total_Seconds_ViewedNon_LIVE
,sum(case when live=0 and analysis_right ='Solheim Cup 2013 Ladies Golf - Sky Sports' then total_duration_viewed_seconds else 0 end) 
as SOLSS_Total_Seconds_ViewedNon_LIVE
,sum(case when live=0 and analysis_right ='South African Home Cricket  2012-2020 Sky Sports' then total_duration_viewed_seconds else 0 end) 
as SACSS_Total_Seconds_ViewedNon_LIVE
,sum(case when live=0 and analysis_right ='Spanish Football 2012/13 To 14/15 - Sky Sports' then total_duration_viewed_seconds else 0 end) 
as SPFSS_Total_Seconds_ViewedNon_LIVE
,sum(case when live=0 and analysis_right ='SPFL - BT Sport' then total_duration_viewed_seconds else 0 end) 
as SPFLBTS_Total_Seconds_ViewedNon_LIVE
,sum(case when live=0 and analysis_right ='SPL - ESPN' then total_duration_viewed_seconds else 0 end) 
as SPLESPN_Total_Seconds_ViewedNon_LIVE
,sum(case when live=0 and analysis_right ='SPL - Sky Sports' then total_duration_viewed_seconds else 0 end) 
as SPLSS_Total_Seconds_ViewedNon_LIVE
,sum(case when live=0 and analysis_right ='Sportfive World Cup 2014 Qualifers - Sky Sports' then total_duration_viewed_seconds else 0 end) 
as SP5SS_Total_Seconds_ViewedNon_LIVE
,sum(case when live=0 and analysis_right ='The boat race - BBC' then total_duration_viewed_seconds else 0 end) 
as BTRBBC_Total_Seconds_ViewedNon_LIVE
,sum(case when live=0 and analysis_right ='The football league show - BBC' then total_duration_viewed_seconds else 0 end) 
as FLSBBC_Total_Seconds_ViewedNon_LIVE
,sum(case when live=0 and analysis_right ='The Masters Golf - BBC' then total_duration_viewed_seconds else 0 end) 
as MGBBC_Total_Seconds_ViewedNon_LIVE
,sum(case when live=0 and analysis_right ='TNA Wrestling Challenge' then total_duration_viewed_seconds else 0 end) 
as TNACHA_Total_Seconds_ViewedNon_LIVE
,sum(case when live=0 and analysis_right ='Tour de France - Eurosport' then total_duration_viewed_seconds else 0 end) 
as TDFEUR_Total_Seconds_ViewedNon_LIVE
,sum(case when live=0 and analysis_right ='Tour de France - ITV' then total_duration_viewed_seconds else 0 end) 
as TDFITV_Total_Seconds_ViewedNon_LIVE
,sum(case when live=0 and analysis_right ='U S Masters Golf 2011-2014 Sky Sports' then total_duration_viewed_seconds else 0 end) 
as USMGSS_Total_Seconds_ViewedNon_LIVE
,sum(case when live=0 and analysis_right ='U.S Open Tennis 2013-2015 Sky Sports' then total_duration_viewed_seconds else 0 end) 
as USOTSS_Total_Seconds_ViewedNon_LIVE
,sum(case when live=0 and analysis_right ='U.S Open/Womens/Seniors/Amateur Golf Sky Sports' then total_duration_viewed_seconds else 0 end) 
as USOGSS_Total_Seconds_ViewedNon_LIVE
,sum(case when live=0 and analysis_right ='UEFA Champions League -  Sky Sports' then total_duration_viewed_seconds else 0 end) 
as CLASS_Total_Seconds_ViewedNon_LIVE
,sum(case when live=0 and analysis_right ='UEFA Champions League -  Sky Sports (non Live)' then total_duration_viewed_seconds else 0 end) 
as CLNSS_Total_Seconds_ViewedNon_LIVE
,sum(case when live=0 and analysis_right ='UEFA Champions League -  Sky Sports (other Live)' then total_duration_viewed_seconds else 0 end) 
as CLOSS_Total_Seconds_ViewedNon_LIVE
,sum(case when live=0 and analysis_right ='UEFA Champions League -  Sky Sports (Tue)' then total_duration_viewed_seconds else 0 end) 
as CLTSS_Total_Seconds_ViewedNon_LIVE
,sum(case when live=0 and analysis_right ='UEFA Champions League -  Sky Sports (Wed)' then total_duration_viewed_seconds else 0 end) 
as CLWSS_Total_Seconds_ViewedNon_LIVE
,sum(case when live=0 and analysis_right ='US Open Tennis - Eurosport' then total_duration_viewed_seconds else 0 end) 
as USOTEUR_Total_Seconds_ViewedNon_LIVE
,sum(case when live=0 and analysis_right ='USA Football - BT Sport' then total_duration_viewed_seconds else 0 end) 
as USFBTS_Total_Seconds_ViewedNon_LIVE
,sum(case when live=0 and analysis_right ='USPGA Championship (2007-2016) Sky Sports' then total_duration_viewed_seconds else 0 end) 
as USPGASS_Total_Seconds_ViewedNon_LIVE
,sum(case when live=0 and analysis_right ='WCQ - ESPN' then total_duration_viewed_seconds else 0 end) 
as WCQESPN_Total_Seconds_ViewedNon_LIVE
,sum(case when live=0 and analysis_right ='Welsh Home International Football 2012-2014 Sky Sports' then total_duration_viewed_seconds else 0 end) 
as WIFSS_Total_Seconds_ViewedNon_LIVE
,sum(case when live=0 and analysis_right ='West Indies Home Cricket 2013-2020 Sky Sports' then total_duration_viewed_seconds else 0 end) 
as WICSS_Total_Seconds_ViewedNon_LIVE
,sum(case when live=0 and analysis_right ='Wimbledon - BBC' then total_duration_viewed_seconds else 0 end) 
as WIMBBC_Total_Seconds_ViewedNon_LIVE
,sum(case when live=0 and analysis_right ='Womens Cricket - I C C Cricket Deal  2012-2015 Sky Sports' then total_duration_viewed_seconds else 0 end) 
as WICCSS_Total_Seconds_ViewedNon_LIVE
,sum(case when live=0 and analysis_right ='World Athletics Championship - More 4' then total_duration_viewed_seconds else 0 end) 
as WACMR4_Total_Seconds_ViewedNon_LIVE
,sum(case when live=0 and analysis_right ='World Club Championship - BBC' then total_duration_viewed_seconds else 0 end) 
as WCLBBBC_Total_Seconds_ViewedNon_LIVE
,sum(case when live=0 and analysis_right ='World Cup Qualifiers - BT Sport' then total_duration_viewed_seconds else 0 end) 
as WCQBTS_Total_Seconds_ViewedNon_LIVE
,sum(case when live=0 and analysis_right ='World Darts Championship 2009-2012 Sky Sports' then total_duration_viewed_seconds else 0 end) 
as WDCSS_Total_Seconds_ViewedNon_LIVE
,sum(case when live=0 and analysis_right ='World snooker championship - BBC' then total_duration_viewed_seconds else 0 end) 
as WSCBBC_Total_Seconds_ViewedNon_LIVE
,sum(case when live=0 and analysis_right ='WWE Sky 1 and 2' then total_duration_viewed_seconds else 0 end) 
as WWES12_Total_Seconds_ViewedNon_LIVE
,sum(case when live=0 and analysis_right ='WWE Sky Sports' then total_duration_viewed_seconds else 0 end) 
as WWESS_Total_Seconds_ViewedNon_LIVE
,sum(case when live=1 and analysis_right ='Africa Cup of Nations - Eurosport' then total_programmes_viewed_over_threshold else 0 end) 
as AFCEUR_Programmes_Viewed_over_threshold_LIVE
,sum(case when live=1 and analysis_right ='Africa Cup of Nations - ITV' then total_programmes_viewed_over_threshold else 0 end) 
as AFCITV_Programmes_Viewed_over_threshold_LIVE
,sum(case when live=1 and analysis_right ='Americas Cup - BBC' then total_programmes_viewed_over_threshold else 0 end) 
as AMCBBC_Programmes_Viewed_over_threshold_LIVE
,sum(case when live=1 and analysis_right ='Asian Tour Golf 2012-2013 Sky Sports' then total_programmes_viewed_over_threshold else 0 end) 
as ATGSS_Programmes_Viewed_over_threshold_LIVE
,sum(case when live=1 and analysis_right ='ATP Tennis Masters Series  2011-2013 Sky Sports' then total_programmes_viewed_over_threshold else 0 end) 
as ATPSS_Programmes_Viewed_over_threshold_LIVE
,sum(case when live=1 and analysis_right ='Australia Home Cricket  2012-2016 Sky Sports' then total_programmes_viewed_over_threshold else 0 end) 
as AHCSS_Programmes_Viewed_over_threshold_LIVE
,sum(case when live=1 and analysis_right ='Australian Football - BT Sport' then total_programmes_viewed_over_threshold else 0 end) 
as AUFBTS_Programmes_Viewed_over_threshold_LIVE
,sum(case when live=1 and analysis_right ='Australian Open Tennis - BBC' then total_programmes_viewed_over_threshold else 0 end) 
as AOTBBC_Programmes_Viewed_over_threshold_LIVE
,sum(case when live=1 and analysis_right ='Australian Open Tennis - Eurosport' then total_programmes_viewed_over_threshold else 0 end) 
as AOTEUR_Programmes_Viewed_over_threshold_LIVE
,sum(case when live=1 and analysis_right ='Aviva Premiership - ESPN' then total_programmes_viewed_over_threshold else 0 end) 
as AVPSS_Programmes_Viewed_over_threshold_LIVE
,sum(case when live=1 and analysis_right ='BBC American Football' then total_programmes_viewed_over_threshold else 0 end) 
as AFBBC_Programmes_Viewed_over_threshold_LIVE
,sum(case when live=1 and analysis_right ='BBC Athletics' then total_programmes_viewed_over_threshold else 0 end) 
as ATHBBC_Programmes_Viewed_over_threshold_LIVE
,sum(case when live=1 and analysis_right ='BBC Boxing' then total_programmes_viewed_over_threshold else 0 end) 
as BOXBBC_Programmes_Viewed_over_threshold_LIVE
,sum(case when live=1 and analysis_right ='BBC Darts' then total_programmes_viewed_over_threshold else 0 end) 
as DRTBBC_Programmes_Viewed_over_threshold_LIVE
,sum(case when live=1 and analysis_right ='BBC Equestrian' then total_programmes_viewed_over_threshold else 0 end) 
as EQUBBC_Programmes_Viewed_over_threshold_LIVE
,sum(case when live=1 and analysis_right ='BBC Football' then total_programmes_viewed_over_threshold else 0 end) 
as FOOTBBC_Programmes_Viewed_over_threshold_LIVE
,sum(case when live=1 and analysis_right ='BBC Golf' then total_programmes_viewed_over_threshold else 0 end) 
as GOLFBBC_Programmes_Viewed_over_threshold_LIVE
,sum(case when live=1 and analysis_right ='BBC Motor Sport' then total_programmes_viewed_over_threshold else 0 end) 
as MSPBBC_Programmes_Viewed_over_threshold_LIVE
,sum(case when live=1 and analysis_right ='BBC Rugby' then total_programmes_viewed_over_threshold else 0 end) 
as RUGBBC_Programmes_Viewed_over_threshold_LIVE
,sum(case when live=1 and analysis_right ='BBC Snooker/Pool' then total_programmes_viewed_over_threshold else 0 end) 
as SNPBBC_Programmes_Viewed_over_threshold_LIVE
,sum(case when live=1 and analysis_right ='BBC Tennis' then total_programmes_viewed_over_threshold else 0 end) 
as TENBBC_Programmes_Viewed_over_threshold_LIVE
,sum(case when live=1 and analysis_right ='BBC Unknown' then total_programmes_viewed_over_threshold else 0 end) 
as UNKBBC_Programmes_Viewed_over_threshold_LIVE
,sum(case when live=1 and analysis_right ='BBC Watersports' then total_programmes_viewed_over_threshold else 0 end) 
as WATBBC_Programmes_Viewed_over_threshold_LIVE
,sum(case when live=1 and analysis_right ='BBC Wintersports' then total_programmes_viewed_over_threshold else 0 end) 
as WINBBC_Programmes_Viewed_over_threshold_LIVE
,sum(case when live=1 and analysis_right ='Boxing  - Channel 5' then total_programmes_viewed_over_threshold else 0 end) 
as BOXCH5_Programmes_Viewed_over_threshold_LIVE
,sum(case when live=1 and analysis_right ='Boxing - Matchroom 2012-2014 Sky Sports' then total_programmes_viewed_over_threshold else 0 end) 
as BOXMSS_Programmes_Viewed_over_threshold_LIVE
,sum(case when live=1 and analysis_right ='Brazil Football - BT Sport' then total_programmes_viewed_over_threshold else 0 end) 
as BFTBTS_Programmes_Viewed_over_threshold_LIVE
,sum(case when live=1 and analysis_right ='British & Irish Lions 2013 - Sky Sports' then total_programmes_viewed_over_threshold else 0 end) 
as BILSS_Programmes_Viewed_over_threshold_LIVE
,sum(case when live=1 and analysis_right ='British Open Golf - BBC' then total_programmes_viewed_over_threshold else 0 end) 
as BOGSS_Programmes_Viewed_over_threshold_LIVE
,sum(case when live=1 and analysis_right ='BT Sport American Football' then total_programmes_viewed_over_threshold else 0 end) 
as AFBTS_Programmes_Viewed_over_threshold_LIVE
,sum(case when live=1 and analysis_right ='BT Sport Athletics' then total_programmes_viewed_over_threshold else 0 end) 
as ATHBTS_Programmes_Viewed_over_threshold_LIVE
,sum(case when live=1 and analysis_right ='BT Sport Baseball' then total_programmes_viewed_over_threshold else 0 end) 
as BASEBTS_Programmes_Viewed_over_threshold_LIVE
,sum(case when live=1 and analysis_right ='BT Sport Basketball' then total_programmes_viewed_over_threshold else 0 end) 
as BASKBTS_Programmes_Viewed_over_threshold_LIVE
,sum(case when live=1 and analysis_right ='BT Sport Boxing' then total_programmes_viewed_over_threshold else 0 end) 
as BOXBTS_Programmes_Viewed_over_threshold_LIVE
,sum(case when live=1 and analysis_right ='BT Sport Cricket' then total_programmes_viewed_over_threshold else 0 end) 
as CRIBTS_Programmes_Viewed_over_threshold_LIVE
,sum(case when live=1 and analysis_right ='BT Sport Equestrian' then total_programmes_viewed_over_threshold else 0 end) 
as EQUBTS_Programmes_Viewed_over_threshold_LIVE
,sum(case when live=1 and analysis_right ='BT Sport Extreme' then total_programmes_viewed_over_threshold else 0 end) 
as EXTBTS_Programmes_Viewed_over_threshold_LIVE
,sum(case when live=1 and analysis_right ='BT Sport Football' then total_programmes_viewed_over_threshold else 0 end) 
as FOOTBTS_Programmes_Viewed_over_threshold_LIVE
,sum(case when live=1 and analysis_right ='BT Sport Motor Sport' then total_programmes_viewed_over_threshold else 0 end) 
as MSPBTS_Programmes_Viewed_over_threshold_LIVE
,sum(case when live=1 and analysis_right ='BT Sport Rugby' then total_programmes_viewed_over_threshold else 0 end) 
as RUGBTS_Programmes_Viewed_over_threshold_LIVE
,sum(case when live=1 and analysis_right ='BT Sport Tennis' then total_programmes_viewed_over_threshold else 0 end) 
as TENBTS_Programmes_Viewed_over_threshold_LIVE
,sum(case when live=1 and analysis_right ='BT Sport Unknown' then total_programmes_viewed_over_threshold else 0 end) 
as UNKBTS_Programmes_Viewed_over_threshold_LIVE
,sum(case when live=1 and analysis_right ='BT Sport Wintersports' then total_programmes_viewed_over_threshold else 0 end) 
as WINBTS_Programmes_Viewed_over_threshold_LIVE
,sum(case when live=1 and analysis_right ='Bundesliga - BT Sport' then total_programmes_viewed_over_threshold else 0 end) 
as BUNBTS_Programmes_Viewed_over_threshold_LIVE
,sum(case when live=1 and analysis_right ='Bundesliga- ESPN' then total_programmes_viewed_over_threshold else 0 end) 
as BUNESPN_Programmes_Viewed_over_threshold_LIVE
,sum(case when live=1 and analysis_right ='Challenge Darts' then total_programmes_viewed_over_threshold else 0 end) 
as DRTCHA_Programmes_Viewed_over_threshold_LIVE
,sum(case when live=1 and analysis_right ='Challenge Extreme' then total_programmes_viewed_over_threshold else 0 end) 
as EXTCHA_Programmes_Viewed_over_threshold_LIVE
,sum(case when live=1 and analysis_right ='Challenge Unknown' then total_programmes_viewed_over_threshold else 0 end) 
as UNKCHA_Programmes_Viewed_over_threshold_LIVE
,sum(case when live=1 and analysis_right ='Challenge Wrestling' then total_programmes_viewed_over_threshold else 0 end) 
as WRECHA_Programmes_Viewed_over_threshold_LIVE
,sum(case when live=1 and analysis_right ='Champions League - ITV' then total_programmes_viewed_over_threshold else 0 end) 
as CHLITV_Programmes_Viewed_over_threshold_LIVE
,sum(case when live=1 and analysis_right ='Champions Trophy - I C C Cricket Deal  2012-2015 Sky Sports' then total_programmes_viewed_over_threshold else 0 end) 
as ICCSS_Programmes_Viewed_over_threshold_LIVE
,sum(case when live=1 and analysis_right ='Channel 4 American Football' then total_programmes_viewed_over_threshold else 0 end) 
as AMCH4_Programmes_Viewed_over_threshold_LIVE
,sum(case when live=1 and analysis_right ='Channel 4 Athletics' then total_programmes_viewed_over_threshold else 0 end) 
as ATHCH4_Programmes_Viewed_over_threshold_LIVE
,sum(case when live=1 and analysis_right ='Channel 4 Boxing' then total_programmes_viewed_over_threshold else 0 end) 
as BOXCH4_Programmes_Viewed_over_threshold_LIVE
,sum(case when live=1 and analysis_right ='Channel 4 Equestrian' then total_programmes_viewed_over_threshold else 0 end) 
as EQUCH4_Programmes_Viewed_over_threshold_LIVE
,sum(case when live=1 and analysis_right ='Channel 4 Extreme' then total_programmes_viewed_over_threshold else 0 end) 
as EXTCH4_Programmes_Viewed_over_threshold_LIVE
,sum(case when live=1 and analysis_right ='Channel 4 Football' then total_programmes_viewed_over_threshold else 0 end) 
as FOOTCH4_Programmes_Viewed_over_threshold_LIVE
,sum(case when live=1 and analysis_right ='Channel 4 Motor Sport' then total_programmes_viewed_over_threshold else 0 end) 
as MSPCH4_Programmes_Viewed_over_threshold_LIVE
,sum(case when live=1 and analysis_right ='Channel 4 Racing' then total_programmes_viewed_over_threshold else 0 end) 
as RACCH4_Programmes_Viewed_over_threshold_LIVE
,sum(case when live=1 and analysis_right ='Channel 4 Unknown' then total_programmes_viewed_over_threshold else 0 end) 
as UNKCH4_Programmes_Viewed_over_threshold_LIVE
,sum(case when live=1 and analysis_right ='Channel 4 Watersports' then total_programmes_viewed_over_threshold else 0 end) 
as WATCH4_Programmes_Viewed_over_threshold_LIVE
,sum(case when live=1 and analysis_right ='Channel 4 Wintersports' then total_programmes_viewed_over_threshold else 0 end) 
as WINCH4_Programmes_Viewed_over_threshold_LIVE
,sum(case when live=1 and analysis_right ='Channel 5 Athletics' then total_programmes_viewed_over_threshold else 0 end) 
as ATHCH5_Programmes_Viewed_over_threshold_LIVE
,sum(case when live=1 and analysis_right ='Channel 5 Boxing' then total_programmes_viewed_over_threshold else 0 end) 
as BOXOCH5_Programmes_Viewed_over_threshold_LIVE
,sum(case when live=1 and analysis_right ='Channel 5 Cricket' then total_programmes_viewed_over_threshold else 0 end) 
as CRICH5_Programmes_Viewed_over_threshold_LIVE
,sum(case when live=1 and analysis_right ='Channel 5 Motor Sport' then total_programmes_viewed_over_threshold else 0 end) 
as MSPCH5_Programmes_Viewed_over_threshold_LIVE
,sum(case when live=1 and analysis_right ='Channel 5 Unknown' then total_programmes_viewed_over_threshold else 0 end) 
as UNKCH5_Programmes_Viewed_over_threshold_LIVE
,sum(case when live=1 and analysis_right ='Channel 5 Wrestling' then total_programmes_viewed_over_threshold else 0 end) 
as WRECH5_Programmes_Viewed_over_threshold_LIVE
,sum(case when live=1 and analysis_right ='Cheltenham Festival - Channel 4' then total_programmes_viewed_over_threshold else 0 end) 
as CHELCH4_Programmes_Viewed_over_threshold_LIVE
,sum(case when live=1 and analysis_right ='Community Shield - ITV' then total_programmes_viewed_over_threshold else 0 end) 
as CMSITV_Programmes_Viewed_over_threshold_LIVE
,sum(case when live=1 and analysis_right ='Confederations Cup - BBC' then total_programmes_viewed_over_threshold else 0 end) 
as CONCBBC_Programmes_Viewed_over_threshold_LIVE
,sum(case when live=1 and analysis_right ='Conference - BT Sport' then total_programmes_viewed_over_threshold else 0 end) 
as CONFBTS_Programmes_Viewed_over_threshold_LIVE
,sum(case when live=1 and analysis_right ='Cycling - La Vuelta ITV' then total_programmes_viewed_over_threshold else 0 end) 
as CLVITV_Programmes_Viewed_over_threshold_LIVE
,sum(case when live=1 and analysis_right ='Cycling - U C I World Tour Sky Sports' then total_programmes_viewed_over_threshold else 0 end) 
as CUCISS_Programmes_Viewed_over_threshold_LIVE
,sum(case when live=1 and analysis_right ='Cycling Tour of Britain - Eurosport' then total_programmes_viewed_over_threshold else 0 end) 
as CTBEUR_Programmes_Viewed_over_threshold_LIVE
,sum(case when live=1 and analysis_right ='Cycling: tour of britain ITV4' then total_programmes_viewed_over_threshold else 0 end) 
as CTCITV_Programmes_Viewed_over_threshold_LIVE
,sum(case when live=1 and analysis_right ='Derby - Channel 4' then total_programmes_viewed_over_threshold else 0 end) 
as DERCH4_Programmes_Viewed_over_threshold_LIVE
,sum(case when live=1 and analysis_right ='ECB (highlights) - Channel 5' then total_programmes_viewed_over_threshold else 0 end) 
as ECBHCH5_Programmes_Viewed_over_threshold_LIVE
,sum(case when live=1 and analysis_right ='ECB Cricket Sky Sports' then total_programmes_viewed_over_threshold else 0 end) 
as GECRSS_Programmes_Viewed_over_threshold_LIVE
,sum(case when live=1 and analysis_right ='ECB non-Test Cricket Sky Sports' then total_programmes_viewed_over_threshold else 0 end) 
as ECBNSS_Programmes_Viewed_over_threshold_LIVE
,sum(case when live=1 and analysis_right ='ECB Test Cricket Sky Sports' then total_programmes_viewed_over_threshold else 0 end) 
as ECBTSS_Programmes_Viewed_over_threshold_LIVE
,sum(case when live=1 and analysis_right ='England Football Internationals - ITV' then total_programmes_viewed_over_threshold else 0 end) 
as GENGITV_Programmes_Viewed_over_threshold_LIVE
,sum(case when live=1 and analysis_right ='England Friendlies (Football) - ITV' then total_programmes_viewed_over_threshold else 0 end) 
as EFRITV_Programmes_Viewed_over_threshold_LIVE
,sum(case when live=1 and analysis_right ='England Rugby Internationals 2010-2015 - Sky Sports' then total_programmes_viewed_over_threshold else 0 end) 
as ENRSS_Programmes_Viewed_over_threshold_LIVE
,sum(case when live=1 and analysis_right ='England World Cup Qualifying (Away) - ITV' then total_programmes_viewed_over_threshold else 0 end) 
as EWQAITV_Programmes_Viewed_over_threshold_LIVE
,sum(case when live=1 and analysis_right ='England World Cup Qualifying (Home) - ITV' then total_programmes_viewed_over_threshold else 0 end) 
as EWQHITV_Programmes_Viewed_over_threshold_LIVE
,sum(case when live=1 and analysis_right ='ESPN American Football' then total_programmes_viewed_over_threshold else 0 end) 
as AMESPN_Programmes_Viewed_over_threshold_LIVE
,sum(case when live=1 and analysis_right ='ESPN Athletics' then total_programmes_viewed_over_threshold else 0 end) 
as ATHESPN_Programmes_Viewed_over_threshold_LIVE
,sum(case when live=1 and analysis_right ='ESPN Baseball' then total_programmes_viewed_over_threshold else 0 end) 
as BASEESPN_Programmes_Viewed_over_threshold_LIVE
,sum(case when live=1 and analysis_right ='ESPN Basketball' then total_programmes_viewed_over_threshold else 0 end) 
as BASKESPN_Programmes_Viewed_over_threshold_LIVE
,sum(case when live=1 and analysis_right ='ESPN Boxing' then total_programmes_viewed_over_threshold else 0 end) 
as BOXESPN_Programmes_Viewed_over_threshold_LIVE
,sum(case when live=1 and analysis_right ='ESPN Cricket' then total_programmes_viewed_over_threshold else 0 end) 
as CRIESPN_Programmes_Viewed_over_threshold_LIVE
,sum(case when live=1 and analysis_right ='ESPN Darts' then total_programmes_viewed_over_threshold else 0 end) 
as DARTESPN_Programmes_Viewed_over_threshold_LIVE
,sum(case when live=1 and analysis_right ='ESPN Equestrian' then total_programmes_viewed_over_threshold else 0 end) 
as EQUESPN_Programmes_Viewed_over_threshold_LIVE
,sum(case when live=1 and analysis_right ='ESPN Extreme' then total_programmes_viewed_over_threshold else 0 end) 
as EXTESPN_Programmes_Viewed_over_threshold_LIVE
,sum(case when live=1 and analysis_right ='ESPN Football' then total_programmes_viewed_over_threshold else 0 end) 
as FOOTESPN_Programmes_Viewed_over_threshold_LIVE
,sum(case when live=1 and analysis_right ='ESPN Golf' then total_programmes_viewed_over_threshold else 0 end) 
as GOLFESPN_Programmes_Viewed_over_threshold_LIVE
,sum(case when live=1 and analysis_right ='ESPN Ice Hockey' then total_programmes_viewed_over_threshold else 0 end) 
as IHESPN_Programmes_Viewed_over_threshold_LIVE
,sum(case when live=1 and analysis_right ='ESPN Motor Sport' then total_programmes_viewed_over_threshold else 0 end) 
as MSPESPN_Programmes_Viewed_over_threshold_LIVE
,sum(case when live=1 and analysis_right ='ESPN Racing' then total_programmes_viewed_over_threshold else 0 end) 
as RACESPN_Programmes_Viewed_over_threshold_LIVE
,sum(case when live=1 and analysis_right ='ESPN Rugby' then total_programmes_viewed_over_threshold else 0 end) 
as RUGESPN_Programmes_Viewed_over_threshold_LIVE
,sum(case when live=1 and analysis_right ='ESPN Snooker/Pool' then total_programmes_viewed_over_threshold else 0 end) 
as SNPESPN_Programmes_Viewed_over_threshold_LIVE
,sum(case when live=1 and analysis_right ='ESPN Tennis' then total_programmes_viewed_over_threshold else 0 end) 
as TENESPN_Programmes_Viewed_over_threshold_LIVE
,sum(case when live=1 and analysis_right ='ESPN Unknown' then total_programmes_viewed_over_threshold else 0 end) 
as UNKESPN_Programmes_Viewed_over_threshold_LIVE
,sum(case when live=1 and analysis_right ='ESPN Watersports' then total_programmes_viewed_over_threshold else 0 end) 
as WATESPN_Programmes_Viewed_over_threshold_LIVE
,sum(case when live=1 and analysis_right ='ESPN Wintersports' then total_programmes_viewed_over_threshold else 0 end) 
as WINESPN_Programmes_Viewed_over_threshold_LIVE
,sum(case when live=1 and analysis_right ='ESPN Wrestling' then total_programmes_viewed_over_threshold else 0 end) 
as WREESPN_Programmes_Viewed_over_threshold_LIVE
,sum(case when live=1 and analysis_right ='Europa League - BT Sport' then total_programmes_viewed_over_threshold else 0 end) 
as ELBTSP_Programmes_Viewed_over_threshold_LIVE
,sum(case when live=1 and analysis_right ='Europa League - ESPN' then total_programmes_viewed_over_threshold else 0 end) 
as ELESPN_Programmes_Viewed_over_threshold_LIVE
,sum(case when live=1 and analysis_right ='Europa League - ITV' then total_programmes_viewed_over_threshold else 0 end) 
as ELITV_Programmes_Viewed_over_threshold_LIVE
,sum(case when live=1 and analysis_right ='European Tour Golf - Sky Sports' then total_programmes_viewed_over_threshold else 0 end) 
as ETGSS_Programmes_Viewed_over_threshold_LIVE
,sum(case when live=1 and analysis_right ='Eurosport American Football' then total_programmes_viewed_over_threshold else 0 end) 
as AMEUR_Programmes_Viewed_over_threshold_LIVE
,sum(case when live=1 and analysis_right ='Eurosport Athletics' then total_programmes_viewed_over_threshold else 0 end) 
as ATHEUR_Programmes_Viewed_over_threshold_LIVE
,sum(case when live=1 and analysis_right ='Eurosport Baseball' then total_programmes_viewed_over_threshold else 0 end) 
as BASEEUR_Programmes_Viewed_over_threshold_LIVE
,sum(case when live=1 and analysis_right ='Eurosport Basketball' then total_programmes_viewed_over_threshold else 0 end) 
as BASKEUR_Programmes_Viewed_over_threshold_LIVE
,sum(case when live=1 and analysis_right ='Eurosport Boxing' then total_programmes_viewed_over_threshold else 0 end) 
as BOXEUR_Programmes_Viewed_over_threshold_LIVE
,sum(case when live=1 and analysis_right ='Eurosport Cricket' then total_programmes_viewed_over_threshold else 0 end) 
as CRIEUR_Programmes_Viewed_over_threshold_LIVE
,sum(case when live=1 and analysis_right ='Eurosport Darts' then total_programmes_viewed_over_threshold else 0 end) 
as DARTEUR_Programmes_Viewed_over_threshold_LIVE
,sum(case when live=1 and analysis_right ='Eurosport Equestrian' then total_programmes_viewed_over_threshold else 0 end) 
as EQUEUR_Programmes_Viewed_over_threshold_LIVE
,sum(case when live=1 and analysis_right ='Eurosport Extreme' then total_programmes_viewed_over_threshold else 0 end) 
as EXTEUR_Programmes_Viewed_over_threshold_LIVE
,sum(case when live=1 and analysis_right ='Eurosport Football' then total_programmes_viewed_over_threshold else 0 end) 
as FOOTEUR_Programmes_Viewed_over_threshold_LIVE
,sum(case when live=1 and analysis_right ='Eurosport Golf' then total_programmes_viewed_over_threshold else 0 end) 
as GOLFEUR_Programmes_Viewed_over_threshold_LIVE
,sum(case when live=1 and analysis_right ='Eurosport Ice Hockey' then total_programmes_viewed_over_threshold else 0 end) 
as IHEUR_Programmes_Viewed_over_threshold_LIVE
,sum(case when live=1 and analysis_right ='Eurosport Motor Sport' then total_programmes_viewed_over_threshold else 0 end) 
as MSPEUR_Programmes_Viewed_over_threshold_LIVE
,sum(case when live=1 and analysis_right ='Eurosport Racing' then total_programmes_viewed_over_threshold else 0 end) 
as RACEUR_Programmes_Viewed_over_threshold_LIVE
,sum(case when live=1 and analysis_right ='Eurosport Snooker/Pool' then total_programmes_viewed_over_threshold else 0 end) 
as SNPEUR_Programmes_Viewed_over_threshold_LIVE
,sum(case when live=1 and analysis_right ='Eurosport Tennis' then total_programmes_viewed_over_threshold else 0 end) 
as TENEUR_Programmes_Viewed_over_threshold_LIVE
,sum(case when live=1 and analysis_right ='Eurosport Unknown' then total_programmes_viewed_over_threshold else 0 end) 
as UNKEUR_Programmes_Viewed_over_threshold_LIVE
,sum(case when live=1 and analysis_right ='Eurosport Watersports' then total_programmes_viewed_over_threshold else 0 end) 
as WATEUR_Programmes_Viewed_over_threshold_LIVE
,sum(case when live=1 and analysis_right ='Eurosport Wintersports' then total_programmes_viewed_over_threshold else 0 end) 
as WINEUR_Programmes_Viewed_over_threshold_LIVE
,sum(case when live=1 and analysis_right ='F1 - BBC' then total_programmes_viewed_over_threshold else 0 end) 
as GF1BBC_Programmes_Viewed_over_threshold_LIVE
,sum(case when live=1 and analysis_right ='F1 - Sky Sports' then total_programmes_viewed_over_threshold else 0 end) 
as GF1SS_Programmes_Viewed_over_threshold_LIVE
,sum(case when live=1 and analysis_right ='F1 (non-Live)- BBC' then total_programmes_viewed_over_threshold else 0 end) 
as F1NBBC_Programmes_Viewed_over_threshold_LIVE
,sum(case when live=1 and analysis_right ='F1 (Practice Live)- BBC' then total_programmes_viewed_over_threshold else 0 end) 
as F1PBBC_Programmes_Viewed_over_threshold_LIVE
,sum(case when live=1 and analysis_right ='F1 (Qualifying Live)- BBC' then total_programmes_viewed_over_threshold else 0 end) 
as F1QBBC_Programmes_Viewed_over_threshold_LIVE
,sum(case when live=1 and analysis_right ='F1 (Race Live)- BBC' then total_programmes_viewed_over_threshold else 0 end) 
as F1RBBC_Programmes_Viewed_over_threshold_LIVE
,sum(case when live=1 and analysis_right ='FA Cup - ESPN' then total_programmes_viewed_over_threshold else 0 end) 
as FACESPN_Programmes_Viewed_over_threshold_LIVE
,sum(case when live=1 and analysis_right ='FA Cup - ITV' then total_programmes_viewed_over_threshold else 0 end) 
as FACITV_Programmes_Viewed_over_threshold_LIVE
,sum(case when live=1 and analysis_right ='Football League - Seasons 2012/13 To 14/15 Sky Sports - Capital 1 Cup' then total_programmes_viewed_over_threshold else 0 end) 
as FLCCSS_Programmes_Viewed_over_threshold_LIVE
,sum(case when live=1 and analysis_right ='Football League - Seasons 2012/13 To 14/15 Sky Sports - others' then total_programmes_viewed_over_threshold else 0 end) 
as FLOTSS_Programmes_Viewed_over_threshold_LIVE
,sum(case when live=1 and analysis_right ='Formula One 2012-2018 - (non-Live) Sky Sports' then total_programmes_viewed_over_threshold else 0 end) 
as F1NSS_Programmes_Viewed_over_threshold_LIVE
,sum(case when live=1 and analysis_right ='Formula One 2012-2018 - (Practice Live) Sky Sports' then total_programmes_viewed_over_threshold else 0 end) 
as F1PSS_Programmes_Viewed_over_threshold_LIVE
,sum(case when live=1 and analysis_right ='Formula One 2012-2018 - (Qualifying Live) Sky Sports' then total_programmes_viewed_over_threshold else 0 end) 
as F1QSS_Programmes_Viewed_over_threshold_LIVE
,sum(case when live=1 and analysis_right ='Formula One 2012-2018 - (Race Live) Sky Sports' then total_programmes_viewed_over_threshold else 0 end) 
as F1RSS_Programmes_Viewed_over_threshold_LIVE
,sum(case when live=1 and analysis_right ='French Open Tennis - Eurosport' then total_programmes_viewed_over_threshold else 0 end) 
as FOTEUR_Programmes_Viewed_over_threshold_LIVE
,sum(case when live=1 and analysis_right ='French Open Tennis - ITV' then total_programmes_viewed_over_threshold else 0 end) 
as FOTITV_Programmes_Viewed_over_threshold_LIVE
,sum(case when live=1 and analysis_right ='Grand National - Channel 4' then total_programmes_viewed_over_threshold else 0 end) 
as GDNCH4_Programmes_Viewed_over_threshold_LIVE
,sum(case when live=1 and analysis_right ='Heineken Cup 2010/11 To 2013/14- Sky Sports' then total_programmes_viewed_over_threshold else 0 end) 
as HECSS_Programmes_Viewed_over_threshold_LIVE
,sum(case when live=1 and analysis_right ='I R B Sevens 2008/09 -2014/15 - Sky Sports' then total_programmes_viewed_over_threshold else 0 end) 
as IRBSS_Programmes_Viewed_over_threshold_LIVE
,sum(case when live=1 and analysis_right ='IAAF World Athletics Championship - Eurosport' then total_programmes_viewed_over_threshold else 0 end) 
as WACEUR_Programmes_Viewed_over_threshold_LIVE
,sum(case when live=1 and analysis_right ='India Home Cricket 2012-2018 Sky Sports' then total_programmes_viewed_over_threshold else 0 end) 
as IHCSS_Programmes_Viewed_over_threshold_LIVE
,sum(case when live=1 and analysis_right ='India Premier League - ITV' then total_programmes_viewed_over_threshold else 0 end) 
as IPLITV_Programmes_Viewed_over_threshold_LIVE
,sum(case when live=1 and analysis_right ='International Freindlies - ESPN' then total_programmes_viewed_over_threshold else 0 end) 
as IFESPN_Programmes_Viewed_over_threshold_LIVE
,sum(case when live=1 and analysis_right ='International Friendlies - BT Sport' then total_programmes_viewed_over_threshold else 0 end) 
as IFBTS_Programmes_Viewed_over_threshold_LIVE
,sum(case when live=1 and analysis_right ='ITV1 Boxing' then total_programmes_viewed_over_threshold else 0 end) 
as BOXITV1_Programmes_Viewed_over_threshold_LIVE
,sum(case when live=1 and analysis_right ='ITV1 Football' then total_programmes_viewed_over_threshold else 0 end) 
as FOOTITV1_Programmes_Viewed_over_threshold_LIVE
,sum(case when live=1 and analysis_right ='ITV1 Motor Sport' then total_programmes_viewed_over_threshold else 0 end) 
as MOTSITV1_Programmes_Viewed_over_threshold_LIVE
,sum(case when live=1 and analysis_right ='ITV1 Rugby' then total_programmes_viewed_over_threshold else 0 end) 
as RUGITV1_Programmes_Viewed_over_threshold_LIVE
,sum(case when live=1 and analysis_right ='ITV1 Snooker/Pool' then total_programmes_viewed_over_threshold else 0 end) 
as SNPITV1_Programmes_Viewed_over_threshold_LIVE
,sum(case when live=1 and analysis_right ='ITV1 Unknown' then total_programmes_viewed_over_threshold else 0 end) 
as UNKITV1_Programmes_Viewed_over_threshold_LIVE
,sum(case when live=1 and analysis_right ='ITV4 Boxing' then total_programmes_viewed_over_threshold else 0 end) 
as BOXITV4_Programmes_Viewed_over_threshold_LIVE
,sum(case when live=1 and analysis_right ='ITV4 Cricket' then total_programmes_viewed_over_threshold else 0 end) 
as CRIITV4_Programmes_Viewed_over_threshold_LIVE
,sum(case when live=1 and analysis_right ='ITV4 Darts' then total_programmes_viewed_over_threshold else 0 end) 
as DARTITV4_Programmes_Viewed_over_threshold_LIVE
,sum(case when live=1 and analysis_right ='ITV4 Extreme' then total_programmes_viewed_over_threshold else 0 end) 
as EXTITV4_Programmes_Viewed_over_threshold_LIVE
,sum(case when live=1 and analysis_right ='ITV4 Football' then total_programmes_viewed_over_threshold else 0 end) 
as FOOTITV4_Programmes_Viewed_over_threshold_LIVE
,sum(case when live=1 and analysis_right ='ITV4 Motor Sport' then total_programmes_viewed_over_threshold else 0 end) 
as MSPITV4_Programmes_Viewed_over_threshold_LIVE
,sum(case when live=1 and analysis_right ='ITV4 Rugby' then total_programmes_viewed_over_threshold else 0 end) 
as RUGITV4_Programmes_Viewed_over_threshold_LIVE
,sum(case when live=1 and analysis_right ='ITV4 Snooker/Pool' then total_programmes_viewed_over_threshold else 0 end) 
as SNPITV4_Programmes_Viewed_over_threshold_LIVE
,sum(case when live=1 and analysis_right ='ITV4 Tennis' then total_programmes_viewed_over_threshold else 0 end) 
as TENITV4_Programmes_Viewed_over_threshold_LIVE
,sum(case when live=1 and analysis_right ='ITV4 Unknown' then total_programmes_viewed_over_threshold else 0 end) 
as UNKITV4_Programmes_Viewed_over_threshold_LIVE
,sum(case when live=1 and analysis_right ='Ligue 1 - BT Sport' then total_programmes_viewed_over_threshold else 0 end) 
as L1BTS_Programmes_Viewed_over_threshold_LIVE
,sum(case when live=1 and analysis_right ='Ligue 1 - ESPN' then total_programmes_viewed_over_threshold else 0 end) 
as L1ESPN_Programmes_Viewed_over_threshold_LIVE
,sum(case when live=1 and analysis_right ='Match of the day - BBC' then total_programmes_viewed_over_threshold else 0 end) 
as MOTDBBC_Programmes_Viewed_over_threshold_LIVE
,sum(case when live=1 and analysis_right ='Matchroom Output Deal  2010-2012 (Other)- Sky Sports' then total_programmes_viewed_over_threshold else 0 end) 
as MROSS_Programmes_Viewed_over_threshold_LIVE
,sum(case when live=1 and analysis_right ='Matchroom Output Deal  2010-2012 (Pool)- Sky Sports' then total_programmes_viewed_over_threshold else 0 end) 
as MRPSS_Programmes_Viewed_over_threshold_LIVE
,sum(case when live=1 and analysis_right ='Matchroom Output Deal  2010-2012 (Snooker)- Sky Sports' then total_programmes_viewed_over_threshold else 0 end) 
as MRSSS_Programmes_Viewed_over_threshold_LIVE
,sum(case when live=1 and analysis_right ='Moto GP BBC' then total_programmes_viewed_over_threshold else 0 end) 
as MGPBBC_Programmes_Viewed_over_threshold_LIVE
,sum(case when live=1 and analysis_right ='NBA - Sky Sports' then total_programmes_viewed_over_threshold else 0 end) 
as NBASS_Programmes_Viewed_over_threshold_LIVE
,sum(case when live=1 and analysis_right ='NFL - BBC' then total_programmes_viewed_over_threshold else 0 end) 
as NFLBBC_Programmes_Viewed_over_threshold_LIVE
,sum(case when live=1 and analysis_right ='NFL - Channel 4' then total_programmes_viewed_over_threshold else 0 end) 
as NFLCH4_Programmes_Viewed_over_threshold_LIVE
,sum(case when live=1 and analysis_right ='NFL Seasons 2012-2015 - Sky Sports' then total_programmes_viewed_over_threshold else 0 end) 
as NFLSS_Programmes_Viewed_over_threshold_LIVE
,sum(case when live=1 and analysis_right ='Northern Ireland Football Association 2012-2014 - Sky Sports' then total_programmes_viewed_over_threshold else 0 end) 
as NIFSS_Programmes_Viewed_over_threshold_LIVE
,sum(case when live=1 and analysis_right ='Oaks - Channel 4' then total_programmes_viewed_over_threshold else 0 end) 
as OAKCH4_Programmes_Viewed_over_threshold_LIVE
,sum(case when live=1 and analysis_right ='Other American Football' then total_programmes_viewed_over_threshold else 0 end) 
as AMOTH_Programmes_Viewed_over_threshold_LIVE
,sum(case when live=1 and analysis_right ='Other Athletics' then total_programmes_viewed_over_threshold else 0 end) 
as ATHOTH_Programmes_Viewed_over_threshold_LIVE
,sum(case when live=1 and analysis_right ='Other Baseball' then total_programmes_viewed_over_threshold else 0 end) 
as BASEOTH_Programmes_Viewed_over_threshold_LIVE
,sum(case when live=1 and analysis_right ='Other Basketball' then total_programmes_viewed_over_threshold else 0 end) 
as BASKOTH_Programmes_Viewed_over_threshold_LIVE
,sum(case when live=1 and analysis_right ='Other Boxing' then total_programmes_viewed_over_threshold else 0 end) 
as BOXOTH_Programmes_Viewed_over_threshold_LIVE
,sum(case when live=1 and analysis_right ='Other Cricket' then total_programmes_viewed_over_threshold else 0 end) 
as CRIOTH_Programmes_Viewed_over_threshold_LIVE
,sum(case when live=1 and analysis_right ='Other Darts' then total_programmes_viewed_over_threshold else 0 end) 
as DARTOTH_Programmes_Viewed_over_threshold_LIVE
,sum(case when live=1 and analysis_right ='Other Equestrian' then total_programmes_viewed_over_threshold else 0 end) 
as EQUOTH_Programmes_Viewed_over_threshold_LIVE
,sum(case when live=1 and analysis_right ='Other Extreme' then total_programmes_viewed_over_threshold else 0 end) 
as EXTOTH_Programmes_Viewed_over_threshold_LIVE
,sum(case when live=1 and analysis_right ='Other Fishing' then total_programmes_viewed_over_threshold else 0 end) 
as FSHOTH_Programmes_Viewed_over_threshold_LIVE
,sum(case when live=1 and analysis_right ='Other Football' then total_programmes_viewed_over_threshold else 0 end) 
as FOOTOTH_Programmes_Viewed_over_threshold_LIVE
,sum(case when live=1 and analysis_right ='Other Golf' then total_programmes_viewed_over_threshold else 0 end) 
as GOLFOTH_Programmes_Viewed_over_threshold_LIVE
,sum(case when live=1 and analysis_right ='Other Ice Hockey' then total_programmes_viewed_over_threshold else 0 end) 
as IHOTH_Programmes_Viewed_over_threshold_LIVE
,sum(case when live=1 and analysis_right ='Other Motor Sport' then total_programmes_viewed_over_threshold else 0 end) 
as MSPOTH_Programmes_Viewed_over_threshold_LIVE
,sum(case when live=1 and analysis_right ='Other Racing' then total_programmes_viewed_over_threshold else 0 end) 
as RACOTH_Programmes_Viewed_over_threshold_LIVE
,sum(case when live=1 and analysis_right ='Other Rugby' then total_programmes_viewed_over_threshold else 0 end) 
as RUGOTH_Programmes_Viewed_over_threshold_LIVE
,sum(case when live=1 and analysis_right ='Other Rugby Internationals - ESPN' then total_programmes_viewed_over_threshold else 0 end) 
as ORUGESPN_Programmes_Viewed_over_threshold_LIVE
,sum(case when live=1 and analysis_right ='Other Snooker/Pool' then total_programmes_viewed_over_threshold else 0 end) 
as OTHSNP_Programmes_Viewed_over_threshold_LIVE
,sum(case when live=1 and analysis_right ='Other Tennis' then total_programmes_viewed_over_threshold else 0 end) 
as OTHTEN_Programmes_Viewed_over_threshold_LIVE
,sum(case when live=1 and analysis_right ='Other Unknown' then total_programmes_viewed_over_threshold else 0 end) 
as OTHUNK_Programmes_Viewed_over_threshold_LIVE
,sum(case when live=1 and analysis_right ='Other Watersports' then total_programmes_viewed_over_threshold else 0 end) 
as OTHWAT_Programmes_Viewed_over_threshold_LIVE
,sum(case when live=1 and analysis_right ='Other Wintersports' then total_programmes_viewed_over_threshold else 0 end) 
as OTHWIN_Programmes_Viewed_over_threshold_LIVE
,sum(case when live=1 and analysis_right ='Other Wrestling' then total_programmes_viewed_over_threshold else 0 end) 
as OTHWRE_Programmes_Viewed_over_threshold_LIVE
,sum(case when live=1 and analysis_right ='PGA Tour Golf 2010-2017 - Sky Sports' then total_programmes_viewed_over_threshold else 0 end) 
as PGASS_Programmes_Viewed_over_threshold_LIVE
,sum(case when live=1 and analysis_right ='Premier League - BT Sport' then total_programmes_viewed_over_threshold else 0 end) 
as PLBTS_Programmes_Viewed_over_threshold_LIVE
,sum(case when live=1 and analysis_right ='Premier League - ESPN' then total_programmes_viewed_over_threshold else 0 end) 
as PLESPN_Programmes_Viewed_over_threshold_LIVE
,sum(case when live=1 and analysis_right ='Premier League Darts 2010-2013 - Sky Sports' then total_programmes_viewed_over_threshold else 0 end) 
as PLDSS_Programmes_Viewed_over_threshold_LIVE
,sum(case when live=1 and analysis_right ='Premier League Football - Sky Sports' then total_programmes_viewed_over_threshold else 0 end) 
as GPLSS_Programmes_Viewed_over_threshold_LIVE
,sum(case when live=1 and analysis_right ='Premier League Football - Sky Sports (Match Choice)' then total_programmes_viewed_over_threshold else 0 end) 
as PLMCSS_Programmes_Viewed_over_threshold_LIVE
,sum(case when live=1 and analysis_right ='Premier League Football - Sky Sports (MNF)' then total_programmes_viewed_over_threshold else 0 end) 
as PLMNFSS_Programmes_Viewed_over_threshold_LIVE
,sum(case when live=1 and analysis_right ='Premier League Football - Sky Sports (non Live)' then total_programmes_viewed_over_threshold else 0 end) 
as PLNLSS_Programmes_Viewed_over_threshold_LIVE
,sum(case when live=1 and analysis_right ='Premier League Football - Sky Sports (other Live)' then total_programmes_viewed_over_threshold else 0 end) 
as PLOLSS_Programmes_Viewed_over_threshold_LIVE
,sum(case when live=1 and analysis_right ='Premier League Football - Sky Sports (Sat Lunchtime)' then total_programmes_viewed_over_threshold else 0 end) 
as PLSLSS_Programmes_Viewed_over_threshold_LIVE
,sum(case when live=1 and analysis_right ='Premier League Football - Sky Sports (Sat Night Live)' then total_programmes_viewed_over_threshold else 0 end) 
as PLSNSS_Programmes_Viewed_over_threshold_LIVE
,sum(case when live=1 and analysis_right ='Premier League Football - Sky Sports (Sun 4pm)' then total_programmes_viewed_over_threshold else 0 end) 
as PLS4SS_Programmes_Viewed_over_threshold_LIVE
,sum(case when live=1 and analysis_right ='Premier League Football - Sky Sports (Sun Lunchtime)' then total_programmes_viewed_over_threshold else 0 end) 
as PLSULSS_Programmes_Viewed_over_threshold_LIVE
,sum(case when live=1 and analysis_right ='Premiership Rugby - Sky Sports' then total_programmes_viewed_over_threshold else 0 end) 
as PRUSS_Programmes_Viewed_over_threshold_LIVE
,sum(case when live=1 and analysis_right ='Republic Of Ireland FA - 2010/11 To 2013/14 - Sky Sports' then total_programmes_viewed_over_threshold else 0 end) 
as ROISS_Programmes_Viewed_over_threshold_LIVE
,sum(case when live=1 and analysis_right ='Royal Ascot - Channel 4' then total_programmes_viewed_over_threshold else 0 end) 
as RASCH4_Programmes_Viewed_over_threshold_LIVE
,sum(case when live=1 and analysis_right ='Rugby Internationals (England) - BBC' then total_programmes_viewed_over_threshold else 0 end) 
as RIEBBC_Programmes_Viewed_over_threshold_LIVE
,sum(case when live=1 and analysis_right ='Rugby Internationals (Ireland) - BBC' then total_programmes_viewed_over_threshold else 0 end) 
as RIIBBC_Programmes_Viewed_over_threshold_LIVE
,sum(case when live=1 and analysis_right ='Rugby Internationals (Scotland) - BBC' then total_programmes_viewed_over_threshold else 0 end) 
as RISBBC_Programmes_Viewed_over_threshold_LIVE
,sum(case when live=1 and analysis_right ='Rugby Internationals (Wales) - BBC' then total_programmes_viewed_over_threshold else 0 end) 
as RIWBBC_Programmes_Viewed_over_threshold_LIVE
,sum(case when live=1 and analysis_right ='Rugby League  Challenge Cup- BBC' then total_programmes_viewed_over_threshold else 0 end) 
as RLCCBBC_Programmes_Viewed_over_threshold_LIVE
,sum(case when live=1 and analysis_right ='Rugby League - Sky Sports' then total_programmes_viewed_over_threshold else 0 end) 
as RLGSS_Programmes_Viewed_over_threshold_LIVE
,sum(case when live=1 and analysis_right ='Rugby League  World Cup- BBC' then total_programmes_viewed_over_threshold else 0 end) 
as RLWCBBC_Programmes_Viewed_over_threshold_LIVE
,sum(case when live=1 and analysis_right ='Sanzar Rugby Union 2011 - 2015 - Sky Sports' then total_programmes_viewed_over_threshold else 0 end) 
as SARUSS_Programmes_Viewed_over_threshold_LIVE
,sum(case when live=1 and analysis_right ='Scottish FA Deal - 2010/11 To 2013/14 - Sky Sports' then total_programmes_viewed_over_threshold else 0 end) 
as SFASS_Programmes_Viewed_over_threshold_LIVE
,sum(case when live=1 and analysis_right ='Serie A - BT Sport' then total_programmes_viewed_over_threshold else 0 end) 
as SABTS_Programmes_Viewed_over_threshold_LIVE
,sum(case when live=1 and analysis_right ='Serie A - ESPN' then total_programmes_viewed_over_threshold else 0 end) 
as SAESPN_Programmes_Viewed_over_threshold_LIVE
,sum(case when live=1 and analysis_right ='SFL - ESPN' then total_programmes_viewed_over_threshold else 0 end) 
as SFLESPN_Programmes_Viewed_over_threshold_LIVE
,sum(case when live=1 and analysis_right ='Six Nations - BBC' then total_programmes_viewed_over_threshold else 0 end) 
as SNRBBC_Programmes_Viewed_over_threshold_LIVE
,sum(case when live=1 and analysis_right ='Sky 1 and Sky 2 Boxing' then total_programmes_viewed_over_threshold else 0 end) 
as BOXS12_Programmes_Viewed_over_threshold_LIVE
,sum(case when live=1 and analysis_right ='Sky 1 and Sky 2 Football' then total_programmes_viewed_over_threshold else 0 end) 
as FOOTS12_Programmes_Viewed_over_threshold_LIVE
,sum(case when live=1 and analysis_right ='Sky 1 and Sky 2 Motor Sport' then total_programmes_viewed_over_threshold else 0 end) 
as MSPS12_Programmes_Viewed_over_threshold_LIVE
,sum(case when live=1 and analysis_right ='Sky 1 and Sky 2 Unknown' then total_programmes_viewed_over_threshold else 0 end) 
as UNKS12_Programmes_Viewed_over_threshold_LIVE
,sum(case when live=1 and analysis_right ='Sky 1 and Sky 2 Wrestling' then total_programmes_viewed_over_threshold else 0 end) 
as WRES12_Programmes_Viewed_over_threshold_LIVE
,sum(case when live=1 and analysis_right ='Sky Sports American Football' then total_programmes_viewed_over_threshold else 0 end) 
as AMSS_Programmes_Viewed_over_threshold_LIVE
,sum(case when live=1 and analysis_right ='Sky Sports Athletics' then total_programmes_viewed_over_threshold else 0 end) 
as ATHSS_Programmes_Viewed_over_threshold_LIVE
,sum(case when live=1 and analysis_right ='Sky Sports Baseball' then total_programmes_viewed_over_threshold else 0 end) 
as BASESS_Programmes_Viewed_over_threshold_LIVE
,sum(case when live=1 and analysis_right ='Sky Sports Basketball' then total_programmes_viewed_over_threshold else 0 end) 
as BASKSS_Programmes_Viewed_over_threshold_LIVE
,sum(case when live=1 and analysis_right ='Sky Sports Boxing' then total_programmes_viewed_over_threshold else 0 end) 
as BOXSS_Programmes_Viewed_over_threshold_LIVE
,sum(case when live=1 and analysis_right ='Sky Sports Cricket' then total_programmes_viewed_over_threshold else 0 end) 
as CRISS_Programmes_Viewed_over_threshold_LIVE
,sum(case when live=1 and analysis_right ='Sky Sports Darts' then total_programmes_viewed_over_threshold else 0 end) 
as DARTSS_Programmes_Viewed_over_threshold_LIVE
,sum(case when live=1 and analysis_right ='Sky Sports Equestrian' then total_programmes_viewed_over_threshold else 0 end) 
as EQUSS_Programmes_Viewed_over_threshold_LIVE
,sum(case when live=1 and analysis_right ='Sky Sports Extreme' then total_programmes_viewed_over_threshold else 0 end) 
as EXTSS_Programmes_Viewed_over_threshold_LIVE
,sum(case when live=1 and analysis_right ='Sky Sports Fishing' then total_programmes_viewed_over_threshold else 0 end) 
as FISHSS_Programmes_Viewed_over_threshold_LIVE
,sum(case when live=1 and analysis_right ='Sky Sports Football' then total_programmes_viewed_over_threshold else 0 end) 
as FOOTSS_Programmes_Viewed_over_threshold_LIVE
,sum(case when live=1 and analysis_right ='Sky Sports Golf' then total_programmes_viewed_over_threshold else 0 end) 
as GOLFSS_Programmes_Viewed_over_threshold_LIVE
,sum(case when live=1 and analysis_right ='Sky Sports Ice Hockey' then total_programmes_viewed_over_threshold else 0 end) 
as IHSS_Programmes_Viewed_over_threshold_LIVE
,sum(case when live=1 and analysis_right ='Sky Sports Motor Sport' then total_programmes_viewed_over_threshold else 0 end) 
as MSPSS_Programmes_Viewed_over_threshold_LIVE
,sum(case when live=1 and analysis_right ='Sky Sports Racing' then total_programmes_viewed_over_threshold else 0 end) 
as RACSS_Programmes_Viewed_over_threshold_LIVE
,sum(case when live=1 and analysis_right ='Sky Sports Rugby' then total_programmes_viewed_over_threshold else 0 end) 
as RUGSS_Programmes_Viewed_over_threshold_LIVE
,sum(case when live=1 and analysis_right ='Sky Sports Snooker/Pool' then total_programmes_viewed_over_threshold else 0 end) 
as SNPSS_Programmes_Viewed_over_threshold_LIVE
,sum(case when live=1 and analysis_right ='Sky Sports Tennis' then total_programmes_viewed_over_threshold else 0 end) 
as TENSS_Programmes_Viewed_over_threshold_LIVE
,sum(case when live=1 and analysis_right ='Sky Sports Unknown' then total_programmes_viewed_over_threshold else 0 end) 
as UNKSS_Programmes_Viewed_over_threshold_LIVE
,sum(case when live=1 and analysis_right ='Sky Sports Watersports' then total_programmes_viewed_over_threshold else 0 end) 
as WATSS_Programmes_Viewed_over_threshold_LIVE
,sum(case when live=1 and analysis_right ='Sky Sports Wintersports' then total_programmes_viewed_over_threshold else 0 end) 
as WINSS_Programmes_Viewed_over_threshold_LIVE
,sum(case when live=1 and analysis_right ='Sky Sports Wrestling' then total_programmes_viewed_over_threshold else 0 end) 
as WRESS_Programmes_Viewed_over_threshold_LIVE
,sum(case when live=1 and analysis_right ='Solheim Cup 2013 Ladies Golf - Sky Sports' then total_programmes_viewed_over_threshold else 0 end) 
as SOLSS_Programmes_Viewed_over_threshold_LIVE
,sum(case when live=1 and analysis_right ='South African Home Cricket  2012-2020 Sky Sports' then total_programmes_viewed_over_threshold else 0 end) 
as SACSS_Programmes_Viewed_over_threshold_LIVE
,sum(case when live=1 and analysis_right ='Spanish Football 2012/13 To 14/15 - Sky Sports' then total_programmes_viewed_over_threshold else 0 end) 
as SPFSS_Programmes_Viewed_over_threshold_LIVE
,sum(case when live=1 and analysis_right ='SPFL - BT Sport' then total_programmes_viewed_over_threshold else 0 end) 
as SPFLBTS_Programmes_Viewed_over_threshold_LIVE
,sum(case when live=1 and analysis_right ='SPL - ESPN' then total_programmes_viewed_over_threshold else 0 end) 
as SPLESPN_Programmes_Viewed_over_threshold_LIVE
,sum(case when live=1 and analysis_right ='SPL - Sky Sports' then total_programmes_viewed_over_threshold else 0 end) 
as SPLSS_Programmes_Viewed_over_threshold_LIVE
,sum(case when live=1 and analysis_right ='Sportfive World Cup 2014 Qualifers - Sky Sports' then total_programmes_viewed_over_threshold else 0 end) 
as SP5SS_Programmes_Viewed_over_threshold_LIVE
,sum(case when live=1 and analysis_right ='The boat race - BBC' then total_programmes_viewed_over_threshold else 0 end) 
as BTRBBC_Programmes_Viewed_over_threshold_LIVE
,sum(case when live=1 and analysis_right ='The football league show - BBC' then total_programmes_viewed_over_threshold else 0 end) 
as FLSBBC_Programmes_Viewed_over_threshold_LIVE
,sum(case when live=1 and analysis_right ='The Masters Golf - BBC' then total_programmes_viewed_over_threshold else 0 end) 
as MGBBC_Programmes_Viewed_over_threshold_LIVE
,sum(case when live=1 and analysis_right ='TNA Wrestling Challenge' then total_programmes_viewed_over_threshold else 0 end) 
as TNACHA_Programmes_Viewed_over_threshold_LIVE
,sum(case when live=1 and analysis_right ='Tour de France - Eurosport' then total_programmes_viewed_over_threshold else 0 end) 
as TDFEUR_Programmes_Viewed_over_threshold_LIVE
,sum(case when live=1 and analysis_right ='Tour de France - ITV' then total_programmes_viewed_over_threshold else 0 end) 
as TDFITV_Programmes_Viewed_over_threshold_LIVE
,sum(case when live=1 and analysis_right ='U S Masters Golf 2011-2014 Sky Sports' then total_programmes_viewed_over_threshold else 0 end) 
as USMGSS_Programmes_Viewed_over_threshold_LIVE
,sum(case when live=1 and analysis_right ='U.S Open Tennis 2013-2015 Sky Sports' then total_programmes_viewed_over_threshold else 0 end) 
as USOTSS_Programmes_Viewed_over_threshold_LIVE
,sum(case when live=1 and analysis_right ='U.S Open/Womens/Seniors/Amateur Golf Sky Sports' then total_programmes_viewed_over_threshold else 0 end) 
as USOGSS_Programmes_Viewed_over_threshold_LIVE
,sum(case when live=1 and analysis_right ='UEFA Champions League -  Sky Sports' then total_programmes_viewed_over_threshold else 0 end) 
as CLASS_Programmes_Viewed_over_threshold_LIVE
,sum(case when live=1 and analysis_right ='UEFA Champions League -  Sky Sports (non Live)' then total_programmes_viewed_over_threshold else 0 end) 
as CLNSS_Programmes_Viewed_over_threshold_LIVE
,sum(case when live=1 and analysis_right ='UEFA Champions League -  Sky Sports (other Live)' then total_programmes_viewed_over_threshold else 0 end) 
as CLOSS_Programmes_Viewed_over_threshold_LIVE
,sum(case when live=1 and analysis_right ='UEFA Champions League -  Sky Sports (Tue)' then total_programmes_viewed_over_threshold else 0 end) 
as CLTSS_Programmes_Viewed_over_threshold_LIVE
,sum(case when live=1 and analysis_right ='UEFA Champions League -  Sky Sports (Wed)' then total_programmes_viewed_over_threshold else 0 end) 
as CLWSS_Programmes_Viewed_over_threshold_LIVE
,sum(case when live=1 and analysis_right ='US Open Tennis - Eurosport' then total_programmes_viewed_over_threshold else 0 end) 
as USOTEUR_Programmes_Viewed_over_threshold_LIVE
,sum(case when live=1 and analysis_right ='USA Football - BT Sport' then total_programmes_viewed_over_threshold else 0 end) 
as USFBTS_Programmes_Viewed_over_threshold_LIVE
,sum(case when live=1 and analysis_right ='USPGA Championship (2007-2016) Sky Sports' then total_programmes_viewed_over_threshold else 0 end) 
as USPGASS_Programmes_Viewed_over_threshold_LIVE
,sum(case when live=1 and analysis_right ='WCQ - ESPN' then total_programmes_viewed_over_threshold else 0 end) 
as WCQESPN_Programmes_Viewed_over_threshold_LIVE
,sum(case when live=1 and analysis_right ='Welsh Home International Football 2012-2014 Sky Sports' then total_programmes_viewed_over_threshold else 0 end) 
as WIFSS_Programmes_Viewed_over_threshold_LIVE
,sum(case when live=1 and analysis_right ='West Indies Home Cricket 2013-2020 Sky Sports' then total_programmes_viewed_over_threshold else 0 end) 
as WICSS_Programmes_Viewed_over_threshold_LIVE
,sum(case when live=1 and analysis_right ='Wimbledon - BBC' then total_programmes_viewed_over_threshold else 0 end) 
as WIMBBC_Programmes_Viewed_over_threshold_LIVE
,sum(case when live=1 and analysis_right ='Womens Cricket - I C C Cricket Deal  2012-2015 Sky Sports' then total_programmes_viewed_over_threshold else 0 end) 
as WICCSS_Programmes_Viewed_over_threshold_LIVE
,sum(case when live=1 and analysis_right ='World Athletics Championship - More 4' then total_programmes_viewed_over_threshold else 0 end) 
as WACMR4_Programmes_Viewed_over_threshold_LIVE
,sum(case when live=1 and analysis_right ='World Club Championship - BBC' then total_programmes_viewed_over_threshold else 0 end) 
as WCLBBBC_Programmes_Viewed_over_threshold_LIVE
,sum(case when live=1 and analysis_right ='World Cup Qualifiers - BT Sport' then total_programmes_viewed_over_threshold else 0 end) 
as WCQBTS_Programmes_Viewed_over_threshold_LIVE
,sum(case when live=1 and analysis_right ='World Darts Championship 2009-2012 Sky Sports' then total_programmes_viewed_over_threshold else 0 end) 
as WDCSS_Programmes_Viewed_over_threshold_LIVE
,sum(case when live=1 and analysis_right ='World snooker championship - BBC' then total_programmes_viewed_over_threshold else 0 end) 
as WSCBBC_Programmes_Viewed_over_threshold_LIVE
,sum(case when live=1 and analysis_right ='WWE Sky 1 and 2' then total_programmes_viewed_over_threshold else 0 end) 
as WWES12_Programmes_Viewed_over_threshold_LIVE
,sum(case when live=1 and analysis_right ='WWE Sky Sports' then total_programmes_viewed_over_threshold else 0 end) 
as WWESS_Programmes_Viewed_over_threshold_LIVE
,sum(case when live=0 and analysis_right ='Africa Cup of Nations - Eurosport' then total_programmes_viewed_over_threshold else 0 end) 
as AFCEUR_Programmes_Viewed_over_thresholdNon_LIVE
,sum(case when live=0 and analysis_right ='Africa Cup of Nations - ITV' then total_programmes_viewed_over_threshold else 0 end) 
as AFCITV_Programmes_Viewed_over_thresholdNon_LIVE
,sum(case when live=0 and analysis_right ='Americas Cup - BBC' then total_programmes_viewed_over_threshold else 0 end) 
as AMCBBC_Programmes_Viewed_over_thresholdNon_LIVE
,sum(case when live=0 and analysis_right ='Asian Tour Golf 2012-2013 Sky Sports' then total_programmes_viewed_over_threshold else 0 end) 
as ATGSS_Programmes_Viewed_over_thresholdNon_LIVE
,sum(case when live=0 and analysis_right ='ATP Tennis Masters Series  2011-2013 Sky Sports' then total_programmes_viewed_over_threshold else 0 end) 
as ATPSS_Programmes_Viewed_over_thresholdNon_LIVE
,sum(case when live=0 and analysis_right ='Australia Home Cricket  2012-2016 Sky Sports' then total_programmes_viewed_over_threshold else 0 end) 
as AHCSS_Programmes_Viewed_over_thresholdNon_LIVE
,sum(case when live=0 and analysis_right ='Australian Football - BT Sport' then total_programmes_viewed_over_threshold else 0 end) 
as AUFBTS_Programmes_Viewed_over_thresholdNon_LIVE
,sum(case when live=0 and analysis_right ='Australian Open Tennis - BBC' then total_programmes_viewed_over_threshold else 0 end) 
as AOTBBC_Programmes_Viewed_over_thresholdNon_LIVE
,sum(case when live=0 and analysis_right ='Australian Open Tennis - Eurosport' then total_programmes_viewed_over_threshold else 0 end) 
as AOTEUR_Programmes_Viewed_over_thresholdNon_LIVE
,sum(case when live=0 and analysis_right ='Aviva Premiership - ESPN' then total_programmes_viewed_over_threshold else 0 end) 
as AVPSS_Programmes_Viewed_over_thresholdNon_LIVE
,sum(case when live=0 and analysis_right ='BBC American Football' then total_programmes_viewed_over_threshold else 0 end) 
as AFBBC_Programmes_Viewed_over_thresholdNon_LIVE
,sum(case when live=0 and analysis_right ='BBC Athletics' then total_programmes_viewed_over_threshold else 0 end) 
as ATHBBC_Programmes_Viewed_over_thresholdNon_LIVE
,sum(case when live=0 and analysis_right ='BBC Boxing' then total_programmes_viewed_over_threshold else 0 end) 
as BOXBBC_Programmes_Viewed_over_thresholdNon_LIVE
,sum(case when live=0 and analysis_right ='BBC Darts' then total_programmes_viewed_over_threshold else 0 end) 
as DRTBBC_Programmes_Viewed_over_thresholdNon_LIVE
,sum(case when live=0 and analysis_right ='BBC Equestrian' then total_programmes_viewed_over_threshold else 0 end) 
as EQUBBC_Programmes_Viewed_over_thresholdNon_LIVE
,sum(case when live=0 and analysis_right ='BBC Football' then total_programmes_viewed_over_threshold else 0 end) 
as FOOTBBC_Programmes_Viewed_over_thresholdNon_LIVE
,sum(case when live=0 and analysis_right ='BBC Golf' then total_programmes_viewed_over_threshold else 0 end) 
as GOLFBBC_Programmes_Viewed_over_thresholdNon_LIVE
,sum(case when live=0 and analysis_right ='BBC Motor Sport' then total_programmes_viewed_over_threshold else 0 end) 
as MSPBBC_Programmes_Viewed_over_thresholdNon_LIVE
,sum(case when live=0 and analysis_right ='BBC Rugby' then total_programmes_viewed_over_threshold else 0 end) 
as RUGBBC_Programmes_Viewed_over_thresholdNon_LIVE
,sum(case when live=0 and analysis_right ='BBC Snooker/Pool' then total_programmes_viewed_over_threshold else 0 end) 
as SNPBBC_Programmes_Viewed_over_thresholdNon_LIVE
,sum(case when live=0 and analysis_right ='BBC Tennis' then total_programmes_viewed_over_threshold else 0 end) 
as TENBBC_Programmes_Viewed_over_thresholdNon_LIVE
,sum(case when live=0 and analysis_right ='BBC Unknown' then total_programmes_viewed_over_threshold else 0 end) 
as UNKBBC_Programmes_Viewed_over_thresholdNon_LIVE
,sum(case when live=0 and analysis_right ='BBC Watersports' then total_programmes_viewed_over_threshold else 0 end) 
as WATBBC_Programmes_Viewed_over_thresholdNon_LIVE
,sum(case when live=0 and analysis_right ='BBC Wintersports' then total_programmes_viewed_over_threshold else 0 end) 
as WINBBC_Programmes_Viewed_over_thresholdNon_LIVE
,sum(case when live=0 and analysis_right ='Boxing  - Channel 5' then total_programmes_viewed_over_threshold else 0 end) 
as BOXCH5_Programmes_Viewed_over_thresholdNon_LIVE
,sum(case when live=0 and analysis_right ='Boxing - Matchroom 2012-2014 Sky Sports' then total_programmes_viewed_over_threshold else 0 end) 
as BOXMSS_Programmes_Viewed_over_thresholdNon_LIVE
,sum(case when live=0 and analysis_right ='Brazil Football - BT Sport' then total_programmes_viewed_over_threshold else 0 end) 
as BFTBTS_Programmes_Viewed_over_thresholdNon_LIVE
,sum(case when live=0 and analysis_right ='British & Irish Lions 2013 - Sky Sports' then total_programmes_viewed_over_threshold else 0 end) 
as BILSS_Programmes_Viewed_over_thresholdNon_LIVE
,sum(case when live=0 and analysis_right ='British Open Golf - BBC' then total_programmes_viewed_over_threshold else 0 end) 
as BOGSS_Programmes_Viewed_over_thresholdNon_LIVE
,sum(case when live=0 and analysis_right ='BT Sport American Football' then total_programmes_viewed_over_threshold else 0 end) 
as AFBTS_Programmes_Viewed_over_thresholdNon_LIVE
,sum(case when live=0 and analysis_right ='BT Sport Athletics' then total_programmes_viewed_over_threshold else 0 end) 
as ATHBTS_Programmes_Viewed_over_thresholdNon_LIVE
,sum(case when live=0 and analysis_right ='BT Sport Baseball' then total_programmes_viewed_over_threshold else 0 end) 
as BASEBTS_Programmes_Viewed_over_thresholdNon_LIVE
,sum(case when live=0 and analysis_right ='BT Sport Basketball' then total_programmes_viewed_over_threshold else 0 end) 
as BASKBTS_Programmes_Viewed_over_thresholdNon_LIVE
,sum(case when live=0 and analysis_right ='BT Sport Boxing' then total_programmes_viewed_over_threshold else 0 end) 
as BOXBTS_Programmes_Viewed_over_thresholdNon_LIVE
,sum(case when live=0 and analysis_right ='BT Sport Cricket' then total_programmes_viewed_over_threshold else 0 end) 
as CRIBTS_Programmes_Viewed_over_thresholdNon_LIVE
,sum(case when live=0 and analysis_right ='BT Sport Equestrian' then total_programmes_viewed_over_threshold else 0 end) 
as EQUBTS_Programmes_Viewed_over_thresholdNon_LIVE
,sum(case when live=0 and analysis_right ='BT Sport Extreme' then total_programmes_viewed_over_threshold else 0 end) 
as EXTBTS_Programmes_Viewed_over_thresholdNon_LIVE
,sum(case when live=0 and analysis_right ='BT Sport Football' then total_programmes_viewed_over_threshold else 0 end) 
as FOOTBTS_Programmes_Viewed_over_thresholdNon_LIVE
,sum(case when live=0 and analysis_right ='BT Sport Motor Sport' then total_programmes_viewed_over_threshold else 0 end) 
as MSPBTS_Programmes_Viewed_over_thresholdNon_LIVE
,sum(case when live=0 and analysis_right ='BT Sport Rugby' then total_programmes_viewed_over_threshold else 0 end) 
as RUGBTS_Programmes_Viewed_over_thresholdNon_LIVE
,sum(case when live=0 and analysis_right ='BT Sport Tennis' then total_programmes_viewed_over_threshold else 0 end) 
as TENBTS_Programmes_Viewed_over_thresholdNon_LIVE
,sum(case when live=0 and analysis_right ='BT Sport Unknown' then total_programmes_viewed_over_threshold else 0 end) 
as UNKBTS_Programmes_Viewed_over_thresholdNon_LIVE
,sum(case when live=0 and analysis_right ='BT Sport Wintersports' then total_programmes_viewed_over_threshold else 0 end) 
as WINBTS_Programmes_Viewed_over_thresholdNon_LIVE
,sum(case when live=0 and analysis_right ='Bundesliga - BT Sport' then total_programmes_viewed_over_threshold else 0 end) 
as BUNBTS_Programmes_Viewed_over_thresholdNon_LIVE
,sum(case when live=0 and analysis_right ='Bundesliga- ESPN' then total_programmes_viewed_over_threshold else 0 end) 
as BUNESPN_Programmes_Viewed_over_thresholdNon_LIVE
,sum(case when live=0 and analysis_right ='Challenge Darts' then total_programmes_viewed_over_threshold else 0 end) 
as DRTCHA_Programmes_Viewed_over_thresholdNon_LIVE
,sum(case when live=0 and analysis_right ='Challenge Extreme' then total_programmes_viewed_over_threshold else 0 end) 
as EXTCHA_Programmes_Viewed_over_thresholdNon_LIVE
,sum(case when live=0 and analysis_right ='Challenge Unknown' then total_programmes_viewed_over_threshold else 0 end) 
as UNKCHA_Programmes_Viewed_over_thresholdNon_LIVE
,sum(case when live=0 and analysis_right ='Challenge Wrestling' then total_programmes_viewed_over_threshold else 0 end) 
as WRECHA_Programmes_Viewed_over_thresholdNon_LIVE
,sum(case when live=0 and analysis_right ='Champions League - ITV' then total_programmes_viewed_over_threshold else 0 end) 
as CHLITV_Programmes_Viewed_over_thresholdNon_LIVE
,sum(case when live=0 and analysis_right ='Champions Trophy - I C C Cricket Deal  2012-2015 Sky Sports' then total_programmes_viewed_over_threshold else 0 end) 
as ICCSS_Programmes_Viewed_over_thresholdNon_LIVE
,sum(case when live=0 and analysis_right ='Channel 4 American Football' then total_programmes_viewed_over_threshold else 0 end) 
as AMCH4_Programmes_Viewed_over_thresholdNon_LIVE
,sum(case when live=0 and analysis_right ='Channel 4 Athletics' then total_programmes_viewed_over_threshold else 0 end) 
as ATHCH4_Programmes_Viewed_over_thresholdNon_LIVE
,sum(case when live=0 and analysis_right ='Channel 4 Boxing' then total_programmes_viewed_over_threshold else 0 end) 
as BOXCH4_Programmes_Viewed_over_thresholdNon_LIVE
,sum(case when live=0 and analysis_right ='Channel 4 Equestrian' then total_programmes_viewed_over_threshold else 0 end) 
as EQUCH4_Programmes_Viewed_over_thresholdNon_LIVE
,sum(case when live=0 and analysis_right ='Channel 4 Extreme' then total_programmes_viewed_over_threshold else 0 end) 
as EXTCH4_Programmes_Viewed_over_thresholdNon_LIVE
,sum(case when live=0 and analysis_right ='Channel 4 Football' then total_programmes_viewed_over_threshold else 0 end) 
as FOOTCH4_Programmes_Viewed_over_thresholdNon_LIVE
,sum(case when live=0 and analysis_right ='Channel 4 Motor Sport' then total_programmes_viewed_over_threshold else 0 end) 
as MSPCH4_Programmes_Viewed_over_thresholdNon_LIVE
,sum(case when live=0 and analysis_right ='Channel 4 Racing' then total_programmes_viewed_over_threshold else 0 end) 
as RACCH4_Programmes_Viewed_over_thresholdNon_LIVE
,sum(case when live=0 and analysis_right ='Channel 4 Unknown' then total_programmes_viewed_over_threshold else 0 end) 
as UNKCH4_Programmes_Viewed_over_thresholdNon_LIVE
,sum(case when live=0 and analysis_right ='Channel 4 Watersports' then total_programmes_viewed_over_threshold else 0 end) 
as WATCH4_Programmes_Viewed_over_thresholdNon_LIVE
,sum(case when live=0 and analysis_right ='Channel 4 Wintersports' then total_programmes_viewed_over_threshold else 0 end) 
as WINCH4_Programmes_Viewed_over_thresholdNon_LIVE
,sum(case when live=0 and analysis_right ='Channel 5 Athletics' then total_programmes_viewed_over_threshold else 0 end) 
as ATHCH5_Programmes_Viewed_over_thresholdNon_LIVE
,sum(case when live=0 and analysis_right ='Channel 5 Boxing' then total_programmes_viewed_over_threshold else 0 end) 
as BOXOCH5_Programmes_Viewed_over_thresholdNon_LIVE
,sum(case when live=0 and analysis_right ='Channel 5 Cricket' then total_programmes_viewed_over_threshold else 0 end) 
as CRICH5_Programmes_Viewed_over_thresholdNon_LIVE
,sum(case when live=0 and analysis_right ='Channel 5 Motor Sport' then total_programmes_viewed_over_threshold else 0 end) 
as MSPCH5_Programmes_Viewed_over_thresholdNon_LIVE
,sum(case when live=0 and analysis_right ='Channel 5 Unknown' then total_programmes_viewed_over_threshold else 0 end) 
as UNKCH5_Programmes_Viewed_over_thresholdNon_LIVE
,sum(case when live=0 and analysis_right ='Channel 5 Wrestling' then total_programmes_viewed_over_threshold else 0 end) 
as WRECH5_Programmes_Viewed_over_thresholdNon_LIVE
,sum(case when live=0 and analysis_right ='Cheltenham Festival - Channel 4' then total_programmes_viewed_over_threshold else 0 end) 
as CHELCH4_Programmes_Viewed_over_thresholdNon_LIVE
,sum(case when live=0 and analysis_right ='Community Shield - ITV' then total_programmes_viewed_over_threshold else 0 end) 
as CMSITV_Programmes_Viewed_over_thresholdNon_LIVE
,sum(case when live=0 and analysis_right ='Confederations Cup - BBC' then total_programmes_viewed_over_threshold else 0 end) 
as CONCBBC_Programmes_Viewed_over_thresholdNon_LIVE
,sum(case when live=0 and analysis_right ='Conference - BT Sport' then total_programmes_viewed_over_threshold else 0 end) 
as CONFBTS_Programmes_Viewed_over_thresholdNon_LIVE
,sum(case when live=0 and analysis_right ='Cycling - La Vuelta ITV' then total_programmes_viewed_over_threshold else 0 end) 
as CLVITV_Programmes_Viewed_over_thresholdNon_LIVE
,sum(case when live=0 and analysis_right ='Cycling - U C I World Tour Sky Sports' then total_programmes_viewed_over_threshold else 0 end) 
as CUCISS_Programmes_Viewed_over_thresholdNon_LIVE
,sum(case when live=0 and analysis_right ='Cycling Tour of Britain - Eurosport' then total_programmes_viewed_over_threshold else 0 end) 
as CTBEUR_Programmes_Viewed_over_thresholdNon_LIVE
,sum(case when live=0 and analysis_right ='Cycling: tour of britain ITV4' then total_programmes_viewed_over_threshold else 0 end) 
as CTCITV_Programmes_Viewed_over_thresholdNon_LIVE
,sum(case when live=0 and analysis_right ='Derby - Channel 4' then total_programmes_viewed_over_threshold else 0 end) 
as DERCH4_Programmes_Viewed_over_thresholdNon_LIVE
,sum(case when live=0 and analysis_right ='ECB (highlights) - Channel 5' then total_programmes_viewed_over_threshold else 0 end) 
as ECBHCH5_Programmes_Viewed_over_thresholdNon_LIVE
,sum(case when live=0 and analysis_right ='ECB Cricket Sky Sports' then total_programmes_viewed_over_threshold else 0 end) 
as GECRSS_Programmes_Viewed_over_thresholdNon_LIVE
,sum(case when live=0 and analysis_right ='ECB non-Test Cricket Sky Sports' then total_programmes_viewed_over_threshold else 0 end) 
as ECBNSS_Programmes_Viewed_over_thresholdNon_LIVE
,sum(case when live=0 and analysis_right ='ECB Test Cricket Sky Sports' then total_programmes_viewed_over_threshold else 0 end) 
as ECBTSS_Programmes_Viewed_over_thresholdNon_LIVE
,sum(case when live=0 and analysis_right ='England Football Internationals - ITV' then total_programmes_viewed_over_threshold else 0 end) 
as GENGITV_Programmes_Viewed_over_thresholdNon_LIVE
,sum(case when live=0 and analysis_right ='England Friendlies (Football) - ITV' then total_programmes_viewed_over_threshold else 0 end) 
as EFRITV_Programmes_Viewed_over_thresholdNon_LIVE
,sum(case when live=0 and analysis_right ='England Rugby Internationals 2010-2015 - Sky Sports' then total_programmes_viewed_over_threshold else 0 end) 
as ENRSS_Programmes_Viewed_over_thresholdNon_LIVE
,sum(case when live=0 and analysis_right ='England World Cup Qualifying (Away) - ITV' then total_programmes_viewed_over_threshold else 0 end) 
as EWQAITV_Programmes_Viewed_over_thresholdNon_LIVE
,sum(case when live=0 and analysis_right ='England World Cup Qualifying (Home) - ITV' then total_programmes_viewed_over_threshold else 0 end) 
as EWQHITV_Programmes_Viewed_over_thresholdNon_LIVE
,sum(case when live=0 and analysis_right ='ESPN American Football' then total_programmes_viewed_over_threshold else 0 end) 
as AMESPN_Programmes_Viewed_over_thresholdNon_LIVE
,sum(case when live=0 and analysis_right ='ESPN Athletics' then total_programmes_viewed_over_threshold else 0 end) 
as ATHESPN_Programmes_Viewed_over_thresholdNon_LIVE
,sum(case when live=0 and analysis_right ='ESPN Baseball' then total_programmes_viewed_over_threshold else 0 end) 
as BASEESPN_Programmes_Viewed_over_thresholdNon_LIVE
,sum(case when live=0 and analysis_right ='ESPN Basketball' then total_programmes_viewed_over_threshold else 0 end) 
as BASKESPN_Programmes_Viewed_over_thresholdNon_LIVE
,sum(case when live=0 and analysis_right ='ESPN Boxing' then total_programmes_viewed_over_threshold else 0 end) 
as BOXESPN_Programmes_Viewed_over_thresholdNon_LIVE
,sum(case when live=0 and analysis_right ='ESPN Cricket' then total_programmes_viewed_over_threshold else 0 end) 
as CRIESPN_Programmes_Viewed_over_thresholdNon_LIVE
,sum(case when live=0 and analysis_right ='ESPN Darts' then total_programmes_viewed_over_threshold else 0 end) 
as DARTESPN_Programmes_Viewed_over_thresholdNon_LIVE
,sum(case when live=0 and analysis_right ='ESPN Equestrian' then total_programmes_viewed_over_threshold else 0 end) 
as EQUESPN_Programmes_Viewed_over_thresholdNon_LIVE
,sum(case when live=0 and analysis_right ='ESPN Extreme' then total_programmes_viewed_over_threshold else 0 end) 
as EXTESPN_Programmes_Viewed_over_thresholdNon_LIVE
,sum(case when live=0 and analysis_right ='ESPN Football' then total_programmes_viewed_over_threshold else 0 end) 
as FOOTESPN_Programmes_Viewed_over_thresholdNon_LIVE
,sum(case when live=0 and analysis_right ='ESPN Golf' then total_programmes_viewed_over_threshold else 0 end) 
as GOLFESPN_Programmes_Viewed_over_thresholdNon_LIVE
,sum(case when live=0 and analysis_right ='ESPN Ice Hockey' then total_programmes_viewed_over_threshold else 0 end) 
as IHESPN_Programmes_Viewed_over_thresholdNon_LIVE
,sum(case when live=0 and analysis_right ='ESPN Motor Sport' then total_programmes_viewed_over_threshold else 0 end) 
as MSPESPN_Programmes_Viewed_over_thresholdNon_LIVE
,sum(case when live=0 and analysis_right ='ESPN Racing' then total_programmes_viewed_over_threshold else 0 end) 
as RACESPN_Programmes_Viewed_over_thresholdNon_LIVE
,sum(case when live=0 and analysis_right ='ESPN Rugby' then total_programmes_viewed_over_threshold else 0 end) 
as RUGESPN_Programmes_Viewed_over_thresholdNon_LIVE
,sum(case when live=0 and analysis_right ='ESPN Snooker/Pool' then total_programmes_viewed_over_threshold else 0 end) 
as SNPESPN_Programmes_Viewed_over_thresholdNon_LIVE
,sum(case when live=0 and analysis_right ='ESPN Tennis' then total_programmes_viewed_over_threshold else 0 end) 
as TENESPN_Programmes_Viewed_over_thresholdNon_LIVE
,sum(case when live=0 and analysis_right ='ESPN Unknown' then total_programmes_viewed_over_threshold else 0 end) 
as UNKESPN_Programmes_Viewed_over_thresholdNon_LIVE
,sum(case when live=0 and analysis_right ='ESPN Watersports' then total_programmes_viewed_over_threshold else 0 end) 
as WATESPN_Programmes_Viewed_over_thresholdNon_LIVE
,sum(case when live=0 and analysis_right ='ESPN Wintersports' then total_programmes_viewed_over_threshold else 0 end) 
as WINESPN_Programmes_Viewed_over_thresholdNon_LIVE
,sum(case when live=0 and analysis_right ='ESPN Wrestling' then total_programmes_viewed_over_threshold else 0 end) 
as WREESPN_Programmes_Viewed_over_thresholdNon_LIVE
,sum(case when live=0 and analysis_right ='Europa League - BT Sport' then total_programmes_viewed_over_threshold else 0 end) 
as ELBTSP_Programmes_Viewed_over_thresholdNon_LIVE
,sum(case when live=0 and analysis_right ='Europa League - ESPN' then total_programmes_viewed_over_threshold else 0 end) 
as ELESPN_Programmes_Viewed_over_thresholdNon_LIVE
,sum(case when live=0 and analysis_right ='Europa League - ITV' then total_programmes_viewed_over_threshold else 0 end) 
as ELITV_Programmes_Viewed_over_thresholdNon_LIVE
,sum(case when live=0 and analysis_right ='European Tour Golf - Sky Sports' then total_programmes_viewed_over_threshold else 0 end) 
as ETGSS_Programmes_Viewed_over_thresholdNon_LIVE
,sum(case when live=0 and analysis_right ='Eurosport American Football' then total_programmes_viewed_over_threshold else 0 end) 
as AMEUR_Programmes_Viewed_over_thresholdNon_LIVE
,sum(case when live=0 and analysis_right ='Eurosport Athletics' then total_programmes_viewed_over_threshold else 0 end) 
as ATHEUR_Programmes_Viewed_over_thresholdNon_LIVE
,sum(case when live=0 and analysis_right ='Eurosport Baseball' then total_programmes_viewed_over_threshold else 0 end) 
as BASEEUR_Programmes_Viewed_over_thresholdNon_LIVE
,sum(case when live=0 and analysis_right ='Eurosport Basketball' then total_programmes_viewed_over_threshold else 0 end) 
as BASKEUR_Programmes_Viewed_over_thresholdNon_LIVE
,sum(case when live=0 and analysis_right ='Eurosport Boxing' then total_programmes_viewed_over_threshold else 0 end) 
as BOXEUR_Programmes_Viewed_over_thresholdNon_LIVE
,sum(case when live=0 and analysis_right ='Eurosport Cricket' then total_programmes_viewed_over_threshold else 0 end) 
as CRIEUR_Programmes_Viewed_over_thresholdNon_LIVE
,sum(case when live=0 and analysis_right ='Eurosport Darts' then total_programmes_viewed_over_threshold else 0 end) 
as DARTEUR_Programmes_Viewed_over_thresholdNon_LIVE
,sum(case when live=0 and analysis_right ='Eurosport Equestrian' then total_programmes_viewed_over_threshold else 0 end) 
as EQUEUR_Programmes_Viewed_over_thresholdNon_LIVE
,sum(case when live=0 and analysis_right ='Eurosport Extreme' then total_programmes_viewed_over_threshold else 0 end) 
as EXTEUR_Programmes_Viewed_over_thresholdNon_LIVE
,sum(case when live=0 and analysis_right ='Eurosport Football' then total_programmes_viewed_over_threshold else 0 end) 
as FOOTEUR_Programmes_Viewed_over_thresholdNon_LIVE
,sum(case when live=0 and analysis_right ='Eurosport Golf' then total_programmes_viewed_over_threshold else 0 end) 
as GOLFEUR_Programmes_Viewed_over_thresholdNon_LIVE
,sum(case when live=0 and analysis_right ='Eurosport Ice Hockey' then total_programmes_viewed_over_threshold else 0 end) 
as IHEUR_Programmes_Viewed_over_thresholdNon_LIVE
,sum(case when live=0 and analysis_right ='Eurosport Motor Sport' then total_programmes_viewed_over_threshold else 0 end) 
as MSPEUR_Programmes_Viewed_over_thresholdNon_LIVE
,sum(case when live=0 and analysis_right ='Eurosport Racing' then total_programmes_viewed_over_threshold else 0 end) 
as RACEUR_Programmes_Viewed_over_thresholdNon_LIVE
,sum(case when live=0 and analysis_right ='Eurosport Snooker/Pool' then total_programmes_viewed_over_threshold else 0 end) 
as SNPEUR_Programmes_Viewed_over_thresholdNon_LIVE
,sum(case when live=0 and analysis_right ='Eurosport Tennis' then total_programmes_viewed_over_threshold else 0 end) 
as TENEUR_Programmes_Viewed_over_thresholdNon_LIVE
,sum(case when live=0 and analysis_right ='Eurosport Unknown' then total_programmes_viewed_over_threshold else 0 end) 
as UNKEUR_Programmes_Viewed_over_thresholdNon_LIVE
,sum(case when live=0 and analysis_right ='Eurosport Watersports' then total_programmes_viewed_over_threshold else 0 end) 
as WATEUR_Programmes_Viewed_over_thresholdNon_LIVE
,sum(case when live=0 and analysis_right ='Eurosport Wintersports' then total_programmes_viewed_over_threshold else 0 end) 
as WINEUR_Programmes_Viewed_over_thresholdNon_LIVE
,sum(case when live=0 and analysis_right ='F1 - BBC' then total_programmes_viewed_over_threshold else 0 end) 
as GF1BBC_Programmes_Viewed_over_thresholdNon_LIVE
,sum(case when live=0 and analysis_right ='F1 - Sky Sports' then total_programmes_viewed_over_threshold else 0 end) 
as GF1SS_Programmes_Viewed_over_thresholdNon_LIVE
,sum(case when live=0 and analysis_right ='F1 (non-Live)- BBC' then total_programmes_viewed_over_threshold else 0 end) 
as F1NBBC_Programmes_Viewed_over_thresholdNon_LIVE
,sum(case when live=0 and analysis_right ='F1 (Practice Live)- BBC' then total_programmes_viewed_over_threshold else 0 end) 
as F1PBBC_Programmes_Viewed_over_thresholdNon_LIVE
,sum(case when live=0 and analysis_right ='F1 (Qualifying Live)- BBC' then total_programmes_viewed_over_threshold else 0 end) 
as F1QBBC_Programmes_Viewed_over_thresholdNon_LIVE
,sum(case when live=0 and analysis_right ='F1 (Race Live)- BBC' then total_programmes_viewed_over_threshold else 0 end) 
as F1RBBC_Programmes_Viewed_over_thresholdNon_LIVE
,sum(case when live=0 and analysis_right ='FA Cup - ESPN' then total_programmes_viewed_over_threshold else 0 end) 
as FACESPN_Programmes_Viewed_over_thresholdNon_LIVE
,sum(case when live=0 and analysis_right ='FA Cup - ITV' then total_programmes_viewed_over_threshold else 0 end) 
as FACITV_Programmes_Viewed_over_thresholdNon_LIVE
,sum(case when live=0 and analysis_right ='Football League - Seasons 2012/13 To 14/15 Sky Sports - Capital 1 Cup' then total_programmes_viewed_over_threshold else 0 end) 
as FLCCSS_Programmes_Viewed_over_thresholdNon_LIVE
,sum(case when live=0 and analysis_right ='Football League - Seasons 2012/13 To 14/15 Sky Sports - others' then total_programmes_viewed_over_threshold else 0 end) 
as FLOTSS_Programmes_Viewed_over_thresholdNon_LIVE
,sum(case when live=0 and analysis_right ='Formula One 2012-2018 - (non-Live) Sky Sports' then total_programmes_viewed_over_threshold else 0 end) 
as F1NSS_Programmes_Viewed_over_thresholdNon_LIVE
,sum(case when live=0 and analysis_right ='Formula One 2012-2018 - (Practice Live) Sky Sports' then total_programmes_viewed_over_threshold else 0 end) 
as F1PSS_Programmes_Viewed_over_thresholdNon_LIVE
,sum(case when live=0 and analysis_right ='Formula One 2012-2018 - (Qualifying Live) Sky Sports' then total_programmes_viewed_over_threshold else 0 end) 
as F1QSS_Programmes_Viewed_over_thresholdNon_LIVE
,sum(case when live=0 and analysis_right ='Formula One 2012-2018 - (Race Live) Sky Sports' then total_programmes_viewed_over_threshold else 0 end) 
as F1RSS_Programmes_Viewed_over_thresholdNon_LIVE
,sum(case when live=0 and analysis_right ='French Open Tennis - Eurosport' then total_programmes_viewed_over_threshold else 0 end) 
as FOTEUR_Programmes_Viewed_over_thresholdNon_LIVE
,sum(case when live=0 and analysis_right ='French Open Tennis - ITV' then total_programmes_viewed_over_threshold else 0 end) 
as FOTITV_Programmes_Viewed_over_thresholdNon_LIVE
,sum(case when live=0 and analysis_right ='Grand National - Channel 4' then total_programmes_viewed_over_threshold else 0 end) 
as GDNCH4_Programmes_Viewed_over_thresholdNon_LIVE
,sum(case when live=0 and analysis_right ='Heineken Cup 2010/11 To 2013/14- Sky Sports' then total_programmes_viewed_over_threshold else 0 end) 
as HECSS_Programmes_Viewed_over_thresholdNon_LIVE
,sum(case when live=0 and analysis_right ='I R B Sevens 2008/09 -2014/15 - Sky Sports' then total_programmes_viewed_over_threshold else 0 end) 
as IRBSS_Programmes_Viewed_over_thresholdNon_LIVE
,sum(case when live=0 and analysis_right ='IAAF World Athletics Championship - Eurosport' then total_programmes_viewed_over_threshold else 0 end) 
as WACEUR_Programmes_Viewed_over_thresholdNon_LIVE
,sum(case when live=0 and analysis_right ='India Home Cricket 2012-2018 Sky Sports' then total_programmes_viewed_over_threshold else 0 end) 
as IHCSS_Programmes_Viewed_over_thresholdNon_LIVE
,sum(case when live=0 and analysis_right ='India Premier League - ITV' then total_programmes_viewed_over_threshold else 0 end) 
as IPLITV_Programmes_Viewed_over_thresholdNon_LIVE
,sum(case when live=0 and analysis_right ='International Freindlies - ESPN' then total_programmes_viewed_over_threshold else 0 end) 
as IFESPN_Programmes_Viewed_over_thresholdNon_LIVE
,sum(case when live=0 and analysis_right ='International Friendlies - BT Sport' then total_programmes_viewed_over_threshold else 0 end) 
as IFBTS_Programmes_Viewed_over_thresholdNon_LIVE
,sum(case when live=0 and analysis_right ='ITV1 Boxing' then total_programmes_viewed_over_threshold else 0 end) 
as BOXITV1_Programmes_Viewed_over_thresholdNon_LIVE
,sum(case when live=0 and analysis_right ='ITV1 Football' then total_programmes_viewed_over_threshold else 0 end) 
as FOOTITV1_Programmes_Viewed_over_thresholdNon_LIVE
,sum(case when live=0 and analysis_right ='ITV1 Motor Sport' then total_programmes_viewed_over_threshold else 0 end) 
as MOTSITV1_Programmes_Viewed_over_thresholdNon_LIVE
,sum(case when live=0 and analysis_right ='ITV1 Rugby' then total_programmes_viewed_over_threshold else 0 end) 
as RUGITV1_Programmes_Viewed_over_thresholdNon_LIVE
,sum(case when live=0 and analysis_right ='ITV1 Snooker/Pool' then total_programmes_viewed_over_threshold else 0 end) 
as SNPITV1_Programmes_Viewed_over_thresholdNon_LIVE
,sum(case when live=0 and analysis_right ='ITV1 Unknown' then total_programmes_viewed_over_threshold else 0 end) 
as UNKITV1_Programmes_Viewed_over_thresholdNon_LIVE
,sum(case when live=0 and analysis_right ='ITV4 Boxing' then total_programmes_viewed_over_threshold else 0 end) 
as BOXITV4_Programmes_Viewed_over_thresholdNon_LIVE
,sum(case when live=0 and analysis_right ='ITV4 Cricket' then total_programmes_viewed_over_threshold else 0 end) 
as CRIITV4_Programmes_Viewed_over_thresholdNon_LIVE
,sum(case when live=0 and analysis_right ='ITV4 Darts' then total_programmes_viewed_over_threshold else 0 end) 
as DARTITV4_Programmes_Viewed_over_thresholdNon_LIVE
,sum(case when live=0 and analysis_right ='ITV4 Extreme' then total_programmes_viewed_over_threshold else 0 end) 
as EXTITV4_Programmes_Viewed_over_thresholdNon_LIVE
,sum(case when live=0 and analysis_right ='ITV4 Football' then total_programmes_viewed_over_threshold else 0 end) 
as FOOTITV4_Programmes_Viewed_over_thresholdNon_LIVE
,sum(case when live=0 and analysis_right ='ITV4 Motor Sport' then total_programmes_viewed_over_threshold else 0 end) 
as MSPITV4_Programmes_Viewed_over_thresholdNon_LIVE
,sum(case when live=0 and analysis_right ='ITV4 Rugby' then total_programmes_viewed_over_threshold else 0 end) 
as RUGITV4_Programmes_Viewed_over_thresholdNon_LIVE
,sum(case when live=0 and analysis_right ='ITV4 Snooker/Pool' then total_programmes_viewed_over_threshold else 0 end) 
as SNPITV4_Programmes_Viewed_over_thresholdNon_LIVE
,sum(case when live=0 and analysis_right ='ITV4 Tennis' then total_programmes_viewed_over_threshold else 0 end) 
as TENITV4_Programmes_Viewed_over_thresholdNon_LIVE
,sum(case when live=0 and analysis_right ='ITV4 Unknown' then total_programmes_viewed_over_threshold else 0 end) 
as UNKITV4_Programmes_Viewed_over_thresholdNon_LIVE
,sum(case when live=0 and analysis_right ='Ligue 1 - BT Sport' then total_programmes_viewed_over_threshold else 0 end) 
as L1BTS_Programmes_Viewed_over_thresholdNon_LIVE
,sum(case when live=0 and analysis_right ='Ligue 1 - ESPN' then total_programmes_viewed_over_threshold else 0 end) 
as L1ESPN_Programmes_Viewed_over_thresholdNon_LIVE
,sum(case when live=0 and analysis_right ='Match of the day - BBC' then total_programmes_viewed_over_threshold else 0 end) 
as MOTDBBC_Programmes_Viewed_over_thresholdNon_LIVE
,sum(case when live=0 and analysis_right ='Matchroom Output Deal  2010-2012 (Other)- Sky Sports' then total_programmes_viewed_over_threshold else 0 end) 
as MROSS_Programmes_Viewed_over_thresholdNon_LIVE
,sum(case when live=0 and analysis_right ='Matchroom Output Deal  2010-2012 (Pool)- Sky Sports' then total_programmes_viewed_over_threshold else 0 end) 
as MRPSS_Programmes_Viewed_over_thresholdNon_LIVE
,sum(case when live=0 and analysis_right ='Matchroom Output Deal  2010-2012 (Snooker)- Sky Sports' then total_programmes_viewed_over_threshold else 0 end) 
as MRSSS_Programmes_Viewed_over_thresholdNon_LIVE
,sum(case when live=0 and analysis_right ='Moto GP BBC' then total_programmes_viewed_over_threshold else 0 end) 
as MGPBBC_Programmes_Viewed_over_thresholdNon_LIVE
,sum(case when live=0 and analysis_right ='NBA - Sky Sports' then total_programmes_viewed_over_threshold else 0 end) 
as NBASS_Programmes_Viewed_over_thresholdNon_LIVE
,sum(case when live=0 and analysis_right ='NFL - BBC' then total_programmes_viewed_over_threshold else 0 end) 
as NFLBBC_Programmes_Viewed_over_thresholdNon_LIVE
,sum(case when live=0 and analysis_right ='NFL - Channel 4' then total_programmes_viewed_over_threshold else 0 end) 
as NFLCH4_Programmes_Viewed_over_thresholdNon_LIVE
,sum(case when live=0 and analysis_right ='NFL Seasons 2012-2015 - Sky Sports' then total_programmes_viewed_over_threshold else 0 end) 
as NFLSS_Programmes_Viewed_over_thresholdNon_LIVE
,sum(case when live=0 and analysis_right ='Northern Ireland Football Association 2012-2014 - Sky Sports' then total_programmes_viewed_over_threshold else 0 end) 
as NIFSS_Programmes_Viewed_over_thresholdNon_LIVE
,sum(case when live=0 and analysis_right ='Oaks - Channel 4' then total_programmes_viewed_over_threshold else 0 end) 
as OAKCH4_Programmes_Viewed_over_thresholdNon_LIVE
,sum(case when live=0 and analysis_right ='Other American Football' then total_programmes_viewed_over_threshold else 0 end) 
as AMOTH_Programmes_Viewed_over_thresholdNon_LIVE
,sum(case when live=0 and analysis_right ='Other Athletics' then total_programmes_viewed_over_threshold else 0 end) 
as ATHOTH_Programmes_Viewed_over_thresholdNon_LIVE
,sum(case when live=0 and analysis_right ='Other Baseball' then total_programmes_viewed_over_threshold else 0 end) 
as BASEOTH_Programmes_Viewed_over_thresholdNon_LIVE
,sum(case when live=0 and analysis_right ='Other Basketball' then total_programmes_viewed_over_threshold else 0 end) 
as BASKOTH_Programmes_Viewed_over_thresholdNon_LIVE
,sum(case when live=0 and analysis_right ='Other Boxing' then total_programmes_viewed_over_threshold else 0 end) 
as BOXOTH_Programmes_Viewed_over_thresholdNon_LIVE
,sum(case when live=0 and analysis_right ='Other Cricket' then total_programmes_viewed_over_threshold else 0 end) 
as CRIOTH_Programmes_Viewed_over_thresholdNon_LIVE
,sum(case when live=0 and analysis_right ='Other Darts' then total_programmes_viewed_over_threshold else 0 end) 
as DARTOTH_Programmes_Viewed_over_thresholdNon_LIVE
,sum(case when live=0 and analysis_right ='Other Equestrian' then total_programmes_viewed_over_threshold else 0 end) 
as EQUOTH_Programmes_Viewed_over_thresholdNon_LIVE
,sum(case when live=0 and analysis_right ='Other Extreme' then total_programmes_viewed_over_threshold else 0 end) 
as EXTOTH_Programmes_Viewed_over_thresholdNon_LIVE
,sum(case when live=0 and analysis_right ='Other Fishing' then total_programmes_viewed_over_threshold else 0 end) 
as FSHOTH_Programmes_Viewed_over_thresholdNon_LIVE
,sum(case when live=0 and analysis_right ='Other Football' then total_programmes_viewed_over_threshold else 0 end) 
as FOOTOTH_Programmes_Viewed_over_thresholdNon_LIVE
,sum(case when live=0 and analysis_right ='Other Golf' then total_programmes_viewed_over_threshold else 0 end) 
as GOLFOTH_Programmes_Viewed_over_thresholdNon_LIVE
,sum(case when live=0 and analysis_right ='Other Ice Hockey' then total_programmes_viewed_over_threshold else 0 end) 
as IHOTH_Programmes_Viewed_over_thresholdNon_LIVE
,sum(case when live=0 and analysis_right ='Other Motor Sport' then total_programmes_viewed_over_threshold else 0 end) 
as MSPOTH_Programmes_Viewed_over_thresholdNon_LIVE
,sum(case when live=0 and analysis_right ='Other Racing' then total_programmes_viewed_over_threshold else 0 end) 
as RACOTH_Programmes_Viewed_over_thresholdNon_LIVE
,sum(case when live=0 and analysis_right ='Other Rugby' then total_programmes_viewed_over_threshold else 0 end) 
as RUGOTH_Programmes_Viewed_over_thresholdNon_LIVE
,sum(case when live=0 and analysis_right ='Other Rugby Internationals - ESPN' then total_programmes_viewed_over_threshold else 0 end) 
as ORUGESPN_Programmes_Viewed_over_thresholdNon_LIVE
,sum(case when live=0 and analysis_right ='Other Snooker/Pool' then total_programmes_viewed_over_threshold else 0 end) 
as OTHSNP_Programmes_Viewed_over_thresholdNon_LIVE
,sum(case when live=0 and analysis_right ='Other Tennis' then total_programmes_viewed_over_threshold else 0 end) 
as OTHTEN_Programmes_Viewed_over_thresholdNon_LIVE
,sum(case when live=0 and analysis_right ='Other Unknown' then total_programmes_viewed_over_threshold else 0 end) 
as OTHUNK_Programmes_Viewed_over_thresholdNon_LIVE
,sum(case when live=0 and analysis_right ='Other Watersports' then total_programmes_viewed_over_threshold else 0 end) 
as OTHWAT_Programmes_Viewed_over_thresholdNon_LIVE
,sum(case when live=0 and analysis_right ='Other Wintersports' then total_programmes_viewed_over_threshold else 0 end) 
as OTHWIN_Programmes_Viewed_over_thresholdNon_LIVE
,sum(case when live=0 and analysis_right ='Other Wrestling' then total_programmes_viewed_over_threshold else 0 end) 
as OTHWRE_Programmes_Viewed_over_thresholdNon_LIVE
,sum(case when live=0 and analysis_right ='PGA Tour Golf 2010-2017 - Sky Sports' then total_programmes_viewed_over_threshold else 0 end) 
as PGASS_Programmes_Viewed_over_thresholdNon_LIVE
,sum(case when live=0 and analysis_right ='Premier League - BT Sport' then total_programmes_viewed_over_threshold else 0 end) 
as PLBTS_Programmes_Viewed_over_thresholdNon_LIVE
,sum(case when live=0 and analysis_right ='Premier League - ESPN' then total_programmes_viewed_over_threshold else 0 end) 
as PLESPN_Programmes_Viewed_over_thresholdNon_LIVE
,sum(case when live=0 and analysis_right ='Premier League Darts 2010-2013 - Sky Sports' then total_programmes_viewed_over_threshold else 0 end) 
as PLDSS_Programmes_Viewed_over_thresholdNon_LIVE
,sum(case when live=0 and analysis_right ='Premier League Football - Sky Sports' then total_programmes_viewed_over_threshold else 0 end) 
as GPLSS_Programmes_Viewed_over_thresholdNon_LIVE
,sum(case when live=0 and analysis_right ='Premier League Football - Sky Sports (Match Choice)' then total_programmes_viewed_over_threshold else 0 end) 
as PLMCSS_Programmes_Viewed_over_thresholdNon_LIVE
,sum(case when live=0 and analysis_right ='Premier League Football - Sky Sports (MNF)' then total_programmes_viewed_over_threshold else 0 end) 
as PLMNFSS_Programmes_Viewed_over_thresholdNon_LIVE
,sum(case when live=0 and analysis_right ='Premier League Football - Sky Sports (non Live)' then total_programmes_viewed_over_threshold else 0 end) 
as PLNLSS_Programmes_Viewed_over_thresholdNon_LIVE
,sum(case when live=0 and analysis_right ='Premier League Football - Sky Sports (other Live)' then total_programmes_viewed_over_threshold else 0 end) 
as PLOLSS_Programmes_Viewed_over_thresholdNon_LIVE
,sum(case when live=0 and analysis_right ='Premier League Football - Sky Sports (Sat Lunchtime)' then total_programmes_viewed_over_threshold else 0 end) 
as PLSLSS_Programmes_Viewed_over_thresholdNon_LIVE
,sum(case when live=0 and analysis_right ='Premier League Football - Sky Sports (Sat Night Live)' then total_programmes_viewed_over_threshold else 0 end) 
as PLSNSS_Programmes_Viewed_over_thresholdNon_LIVE
,sum(case when live=0 and analysis_right ='Premier League Football - Sky Sports (Sun 4pm)' then total_programmes_viewed_over_threshold else 0 end) 
as PLS4SS_Programmes_Viewed_over_thresholdNon_LIVE
,sum(case when live=0 and analysis_right ='Premier League Football - Sky Sports (Sun Lunchtime)' then total_programmes_viewed_over_threshold else 0 end) 
as PLSULSS_Programmes_Viewed_over_thresholdNon_LIVE
,sum(case when live=0 and analysis_right ='Premiership Rugby - Sky Sports' then total_programmes_viewed_over_threshold else 0 end) 
as PRUSS_Programmes_Viewed_over_thresholdNon_LIVE
,sum(case when live=0 and analysis_right ='Republic Of Ireland FA - 2010/11 To 2013/14 - Sky Sports' then total_programmes_viewed_over_threshold else 0 end) 
as ROISS_Programmes_Viewed_over_thresholdNon_LIVE
,sum(case when live=0 and analysis_right ='Royal Ascot - Channel 4' then total_programmes_viewed_over_threshold else 0 end) 
as RASCH4_Programmes_Viewed_over_thresholdNon_LIVE
,sum(case when live=0 and analysis_right ='Rugby Internationals (England) - BBC' then total_programmes_viewed_over_threshold else 0 end) 
as RIEBBC_Programmes_Viewed_over_thresholdNon_LIVE
,sum(case when live=0 and analysis_right ='Rugby Internationals (Ireland) - BBC' then total_programmes_viewed_over_threshold else 0 end) 
as RIIBBC_Programmes_Viewed_over_thresholdNon_LIVE
,sum(case when live=0 and analysis_right ='Rugby Internationals (Scotland) - BBC' then total_programmes_viewed_over_threshold else 0 end) 
as RISBBC_Programmes_Viewed_over_thresholdNon_LIVE
,sum(case when live=0 and analysis_right ='Rugby Internationals (Wales) - BBC' then total_programmes_viewed_over_threshold else 0 end) 
as RIWBBC_Programmes_Viewed_over_thresholdNon_LIVE
,sum(case when live=0 and analysis_right ='Rugby League  Challenge Cup- BBC' then total_programmes_viewed_over_threshold else 0 end) 
as RLCCBBC_Programmes_Viewed_over_thresholdNon_LIVE
,sum(case when live=0 and analysis_right ='Rugby League - Sky Sports' then total_programmes_viewed_over_threshold else 0 end) 
as RLGSS_Programmes_Viewed_over_thresholdNon_LIVE
,sum(case when live=0 and analysis_right ='Rugby League  World Cup- BBC' then total_programmes_viewed_over_threshold else 0 end) 
as RLWCBBC_Programmes_Viewed_over_thresholdNon_LIVE
,sum(case when live=0 and analysis_right ='Sanzar Rugby Union 2011 - 2015 - Sky Sports' then total_programmes_viewed_over_threshold else 0 end) 
as SARUSS_Programmes_Viewed_over_thresholdNon_LIVE
,sum(case when live=0 and analysis_right ='Scottish FA Deal - 2010/11 To 2013/14 - Sky Sports' then total_programmes_viewed_over_threshold else 0 end) 
as SFASS_Programmes_Viewed_over_thresholdNon_LIVE
,sum(case when live=0 and analysis_right ='Serie A - BT Sport' then total_programmes_viewed_over_threshold else 0 end) 
as SABTS_Programmes_Viewed_over_thresholdNon_LIVE
,sum(case when live=0 and analysis_right ='Serie A - ESPN' then total_programmes_viewed_over_threshold else 0 end) 
as SAESPN_Programmes_Viewed_over_thresholdNon_LIVE
,sum(case when live=0 and analysis_right ='SFL - ESPN' then total_programmes_viewed_over_threshold else 0 end) 
as SFLESPN_Programmes_Viewed_over_thresholdNon_LIVE
,sum(case when live=0 and analysis_right ='Six Nations - BBC' then total_programmes_viewed_over_threshold else 0 end) 
as SNRBBC_Programmes_Viewed_over_thresholdNon_LIVE
,sum(case when live=0 and analysis_right ='Sky 1 and Sky 2 Boxing' then total_programmes_viewed_over_threshold else 0 end) 
as BOXS12_Programmes_Viewed_over_thresholdNon_LIVE
,sum(case when live=0 and analysis_right ='Sky 1 and Sky 2 Football' then total_programmes_viewed_over_threshold else 0 end) 
as FOOTS12_Programmes_Viewed_over_thresholdNon_LIVE
,sum(case when live=0 and analysis_right ='Sky 1 and Sky 2 Motor Sport' then total_programmes_viewed_over_threshold else 0 end) 
as MSPS12_Programmes_Viewed_over_thresholdNon_LIVE
,sum(case when live=0 and analysis_right ='Sky 1 and Sky 2 Unknown' then total_programmes_viewed_over_threshold else 0 end) 
as UNKS12_Programmes_Viewed_over_thresholdNon_LIVE
,sum(case when live=0 and analysis_right ='Sky 1 and Sky 2 Wrestling' then total_programmes_viewed_over_threshold else 0 end) 
as WRES12_Programmes_Viewed_over_thresholdNon_LIVE
,sum(case when live=0 and analysis_right ='Sky Sports American Football' then total_programmes_viewed_over_threshold else 0 end) 
as AMSS_Programmes_Viewed_over_thresholdNon_LIVE
,sum(case when live=0 and analysis_right ='Sky Sports Athletics' then total_programmes_viewed_over_threshold else 0 end) 
as ATHSS_Programmes_Viewed_over_thresholdNon_LIVE
,sum(case when live=0 and analysis_right ='Sky Sports Baseball' then total_programmes_viewed_over_threshold else 0 end) 
as BASESS_Programmes_Viewed_over_thresholdNon_LIVE
,sum(case when live=0 and analysis_right ='Sky Sports Basketball' then total_programmes_viewed_over_threshold else 0 end) 
as BASKSS_Programmes_Viewed_over_thresholdNon_LIVE
,sum(case when live=0 and analysis_right ='Sky Sports Boxing' then total_programmes_viewed_over_threshold else 0 end) 
as BOXSS_Programmes_Viewed_over_thresholdNon_LIVE
,sum(case when live=0 and analysis_right ='Sky Sports Cricket' then total_programmes_viewed_over_threshold else 0 end) 
as CRISS_Programmes_Viewed_over_thresholdNon_LIVE
,sum(case when live=0 and analysis_right ='Sky Sports Darts' then total_programmes_viewed_over_threshold else 0 end) 
as DARTSS_Programmes_Viewed_over_thresholdNon_LIVE
,sum(case when live=0 and analysis_right ='Sky Sports Equestrian' then total_programmes_viewed_over_threshold else 0 end) 
as EQUSS_Programmes_Viewed_over_thresholdNon_LIVE
,sum(case when live=0 and analysis_right ='Sky Sports Extreme' then total_programmes_viewed_over_threshold else 0 end) 
as EXTSS_Programmes_Viewed_over_thresholdNon_LIVE
,sum(case when live=0 and analysis_right ='Sky Sports Fishing' then total_programmes_viewed_over_threshold else 0 end) 
as FISHSS_Programmes_Viewed_over_thresholdNon_LIVE
,sum(case when live=0 and analysis_right ='Sky Sports Football' then total_programmes_viewed_over_threshold else 0 end) 
as FOOTSS_Programmes_Viewed_over_thresholdNon_LIVE
,sum(case when live=0 and analysis_right ='Sky Sports Golf' then total_programmes_viewed_over_threshold else 0 end) 
as GOLFSS_Programmes_Viewed_over_thresholdNon_LIVE
,sum(case when live=0 and analysis_right ='Sky Sports Ice Hockey' then total_programmes_viewed_over_threshold else 0 end) 
as IHSS_Programmes_Viewed_over_thresholdNon_LIVE
,sum(case when live=0 and analysis_right ='Sky Sports Motor Sport' then total_programmes_viewed_over_threshold else 0 end) 
as MSPSS_Programmes_Viewed_over_thresholdNon_LIVE
,sum(case when live=0 and analysis_right ='Sky Sports Racing' then total_programmes_viewed_over_threshold else 0 end) 
as RACSS_Programmes_Viewed_over_thresholdNon_LIVE
,sum(case when live=0 and analysis_right ='Sky Sports Rugby' then total_programmes_viewed_over_threshold else 0 end) 
as RUGSS_Programmes_Viewed_over_thresholdNon_LIVE
,sum(case when live=0 and analysis_right ='Sky Sports Snooker/Pool' then total_programmes_viewed_over_threshold else 0 end) 
as SNPSS_Programmes_Viewed_over_thresholdNon_LIVE
,sum(case when live=0 and analysis_right ='Sky Sports Tennis' then total_programmes_viewed_over_threshold else 0 end) 
as TENSS_Programmes_Viewed_over_thresholdNon_LIVE
,sum(case when live=0 and analysis_right ='Sky Sports Unknown' then total_programmes_viewed_over_threshold else 0 end) 
as UNKSS_Programmes_Viewed_over_thresholdNon_LIVE
,sum(case when live=0 and analysis_right ='Sky Sports Watersports' then total_programmes_viewed_over_threshold else 0 end) 
as WATSS_Programmes_Viewed_over_thresholdNon_LIVE
,sum(case when live=0 and analysis_right ='Sky Sports Wintersports' then total_programmes_viewed_over_threshold else 0 end) 
as WINSS_Programmes_Viewed_over_thresholdNon_LIVE
,sum(case when live=0 and analysis_right ='Sky Sports Wrestling' then total_programmes_viewed_over_threshold else 0 end) 
as WRESS_Programmes_Viewed_over_thresholdNon_LIVE
,sum(case when live=0 and analysis_right ='Solheim Cup 2013 Ladies Golf - Sky Sports' then total_programmes_viewed_over_threshold else 0 end) 
as SOLSS_Programmes_Viewed_over_thresholdNon_LIVE
,sum(case when live=0 and analysis_right ='South African Home Cricket  2012-2020 Sky Sports' then total_programmes_viewed_over_threshold else 0 end) 
as SACSS_Programmes_Viewed_over_thresholdNon_LIVE
,sum(case when live=0 and analysis_right ='Spanish Football 2012/13 To 14/15 - Sky Sports' then total_programmes_viewed_over_threshold else 0 end) 
as SPFSS_Programmes_Viewed_over_thresholdNon_LIVE
,sum(case when live=0 and analysis_right ='SPFL - BT Sport' then total_programmes_viewed_over_threshold else 0 end) 
as SPFLBTS_Programmes_Viewed_over_thresholdNon_LIVE
,sum(case when live=0 and analysis_right ='SPL - ESPN' then total_programmes_viewed_over_threshold else 0 end) 
as SPLESPN_Programmes_Viewed_over_thresholdNon_LIVE
,sum(case when live=0 and analysis_right ='SPL - Sky Sports' then total_programmes_viewed_over_threshold else 0 end) 
as SPLSS_Programmes_Viewed_over_thresholdNon_LIVE
,sum(case when live=0 and analysis_right ='Sportfive World Cup 2014 Qualifers - Sky Sports' then total_programmes_viewed_over_threshold else 0 end) 
as SP5SS_Programmes_Viewed_over_thresholdNon_LIVE
,sum(case when live=0 and analysis_right ='The boat race - BBC' then total_programmes_viewed_over_threshold else 0 end) 
as BTRBBC_Programmes_Viewed_over_thresholdNon_LIVE
,sum(case when live=0 and analysis_right ='The football league show - BBC' then total_programmes_viewed_over_threshold else 0 end) 
as FLSBBC_Programmes_Viewed_over_thresholdNon_LIVE
,sum(case when live=0 and analysis_right ='The Masters Golf - BBC' then total_programmes_viewed_over_threshold else 0 end) 
as MGBBC_Programmes_Viewed_over_thresholdNon_LIVE
,sum(case when live=0 and analysis_right ='TNA Wrestling Challenge' then total_programmes_viewed_over_threshold else 0 end) 
as TNACHA_Programmes_Viewed_over_thresholdNon_LIVE
,sum(case when live=0 and analysis_right ='Tour de France - Eurosport' then total_programmes_viewed_over_threshold else 0 end) 
as TDFEUR_Programmes_Viewed_over_thresholdNon_LIVE
,sum(case when live=0 and analysis_right ='Tour de France - ITV' then total_programmes_viewed_over_threshold else 0 end) 
as TDFITV_Programmes_Viewed_over_thresholdNon_LIVE
,sum(case when live=0 and analysis_right ='U S Masters Golf 2011-2014 Sky Sports' then total_programmes_viewed_over_threshold else 0 end) 
as USMGSS_Programmes_Viewed_over_thresholdNon_LIVE
,sum(case when live=0 and analysis_right ='U.S Open Tennis 2013-2015 Sky Sports' then total_programmes_viewed_over_threshold else 0 end) 
as USOTSS_Programmes_Viewed_over_thresholdNon_LIVE
,sum(case when live=0 and analysis_right ='U.S Open/Womens/Seniors/Amateur Golf Sky Sports' then total_programmes_viewed_over_threshold else 0 end) 
as USOGSS_Programmes_Viewed_over_thresholdNon_LIVE
,sum(case when live=0 and analysis_right ='UEFA Champions League -  Sky Sports' then total_programmes_viewed_over_threshold else 0 end) 
as CLASS_Programmes_Viewed_over_thresholdNon_LIVE
,sum(case when live=0 and analysis_right ='UEFA Champions League -  Sky Sports (non Live)' then total_programmes_viewed_over_threshold else 0 end) 
as CLNSS_Programmes_Viewed_over_thresholdNon_LIVE
,sum(case when live=0 and analysis_right ='UEFA Champions League -  Sky Sports (other Live)' then total_programmes_viewed_over_threshold else 0 end) 
as CLOSS_Programmes_Viewed_over_thresholdNon_LIVE
,sum(case when live=0 and analysis_right ='UEFA Champions League -  Sky Sports (Tue)' then total_programmes_viewed_over_threshold else 0 end) 
as CLTSS_Programmes_Viewed_over_thresholdNon_LIVE
,sum(case when live=0 and analysis_right ='UEFA Champions League -  Sky Sports (Wed)' then total_programmes_viewed_over_threshold else 0 end) 
as CLWSS_Programmes_Viewed_over_thresholdNon_LIVE
,sum(case when live=0 and analysis_right ='US Open Tennis - Eurosport' then total_programmes_viewed_over_threshold else 0 end) 
as USOTEUR_Programmes_Viewed_over_thresholdNon_LIVE
,sum(case when live=0 and analysis_right ='USA Football - BT Sport' then total_programmes_viewed_over_threshold else 0 end) 
as USFBTS_Programmes_Viewed_over_thresholdNon_LIVE
,sum(case when live=0 and analysis_right ='USPGA Championship (2007-2016) Sky Sports' then total_programmes_viewed_over_threshold else 0 end) 
as USPGASS_Programmes_Viewed_over_thresholdNon_LIVE
,sum(case when live=0 and analysis_right ='WCQ - ESPN' then total_programmes_viewed_over_threshold else 0 end) 
as WCQESPN_Programmes_Viewed_over_thresholdNon_LIVE
,sum(case when live=0 and analysis_right ='Welsh Home International Football 2012-2014 Sky Sports' then total_programmes_viewed_over_threshold else 0 end) 
as WIFSS_Programmes_Viewed_over_thresholdNon_LIVE
,sum(case when live=0 and analysis_right ='West Indies Home Cricket 2013-2020 Sky Sports' then total_programmes_viewed_over_threshold else 0 end) 
as WICSS_Programmes_Viewed_over_thresholdNon_LIVE
,sum(case when live=0 and analysis_right ='Wimbledon - BBC' then total_programmes_viewed_over_threshold else 0 end) 
as WIMBBC_Programmes_Viewed_over_thresholdNon_LIVE
,sum(case when live=0 and analysis_right ='Womens Cricket - I C C Cricket Deal  2012-2015 Sky Sports' then total_programmes_viewed_over_threshold else 0 end) 
as WICCSS_Programmes_Viewed_over_thresholdNon_LIVE
,sum(case when live=0 and analysis_right ='World Athletics Championship - More 4' then total_programmes_viewed_over_threshold else 0 end) 
as WACMR4_Programmes_Viewed_over_thresholdNon_LIVE
,sum(case when live=0 and analysis_right ='World Club Championship - BBC' then total_programmes_viewed_over_threshold else 0 end) 
as WCLBBBC_Programmes_Viewed_over_thresholdNon_LIVE
,sum(case when live=0 and analysis_right ='World Cup Qualifiers - BT Sport' then total_programmes_viewed_over_threshold else 0 end) 
as WCQBTS_Programmes_Viewed_over_thresholdNon_LIVE
,sum(case when live=0 and analysis_right ='World Darts Championship 2009-2012 Sky Sports' then total_programmes_viewed_over_threshold else 0 end) 
as WDCSS_Programmes_Viewed_over_thresholdNon_LIVE
,sum(case when live=0 and analysis_right ='World snooker championship - BBC' then total_programmes_viewed_over_threshold else 0 end) 
as WSCBBC_Programmes_Viewed_over_thresholdNon_LIVE
,sum(case when live=0 and analysis_right ='WWE Sky 1 and 2' then total_programmes_viewed_over_threshold else 0 end) 
as WWES12_Programmes_Viewed_over_thresholdNon_LIVE
,sum(case when live=0 and analysis_right ='WWE Sky Sports' then total_programmes_viewed_over_threshold else 0 end) 
as WWESS_Programmes_Viewed_over_thresholdNon_LIVE
,sum(case when live=1 and analysis_right ='Africa Cup of Nations - Eurosport' then number_of_events_viewed else 0 end) 
as AFCEUR_Total_Viewing_Events_LIVE
,sum(case when live=1 and analysis_right ='Africa Cup of Nations - ITV' then number_of_events_viewed else 0 end) 
as AFCITV_Total_Viewing_Events_LIVE
,sum(case when live=1 and analysis_right ='Americas Cup - BBC' then number_of_events_viewed else 0 end) 
as AMCBBC_Total_Viewing_Events_LIVE
,sum(case when live=1 and analysis_right ='Asian Tour Golf 2012-2013 Sky Sports' then number_of_events_viewed else 0 end) 
as ATGSS_Total_Viewing_Events_LIVE
,sum(case when live=1 and analysis_right ='ATP Tennis Masters Series  2011-2013 Sky Sports' then number_of_events_viewed else 0 end) 
as ATPSS_Total_Viewing_Events_LIVE
,sum(case when live=1 and analysis_right ='Australia Home Cricket  2012-2016 Sky Sports' then number_of_events_viewed else 0 end) 
as AHCSS_Total_Viewing_Events_LIVE
,sum(case when live=1 and analysis_right ='Australian Football - BT Sport' then number_of_events_viewed else 0 end) 
as AUFBTS_Total_Viewing_Events_LIVE
,sum(case when live=1 and analysis_right ='Australian Open Tennis - BBC' then number_of_events_viewed else 0 end) 
as AOTBBC_Total_Viewing_Events_LIVE
,sum(case when live=1 and analysis_right ='Australian Open Tennis - Eurosport' then number_of_events_viewed else 0 end) 
as AOTEUR_Total_Viewing_Events_LIVE
,sum(case when live=1 and analysis_right ='Aviva Premiership - ESPN' then number_of_events_viewed else 0 end) 
as AVPSS_Total_Viewing_Events_LIVE
,sum(case when live=1 and analysis_right ='BBC American Football' then number_of_events_viewed else 0 end) 
as AFBBC_Total_Viewing_Events_LIVE
,sum(case when live=1 and analysis_right ='BBC Athletics' then number_of_events_viewed else 0 end) 
as ATHBBC_Total_Viewing_Events_LIVE
,sum(case when live=1 and analysis_right ='BBC Boxing' then number_of_events_viewed else 0 end) 
as BOXBBC_Total_Viewing_Events_LIVE
,sum(case when live=1 and analysis_right ='BBC Darts' then number_of_events_viewed else 0 end) 
as DRTBBC_Total_Viewing_Events_LIVE
,sum(case when live=1 and analysis_right ='BBC Equestrian' then number_of_events_viewed else 0 end) 
as EQUBBC_Total_Viewing_Events_LIVE
,sum(case when live=1 and analysis_right ='BBC Football' then number_of_events_viewed else 0 end) 
as FOOTBBC_Total_Viewing_Events_LIVE
,sum(case when live=1 and analysis_right ='BBC Golf' then number_of_events_viewed else 0 end) 
as GOLFBBC_Total_Viewing_Events_LIVE
,sum(case when live=1 and analysis_right ='BBC Motor Sport' then number_of_events_viewed else 0 end) 
as MSPBBC_Total_Viewing_Events_LIVE
,sum(case when live=1 and analysis_right ='BBC Rugby' then number_of_events_viewed else 0 end) 
as RUGBBC_Total_Viewing_Events_LIVE
,sum(case when live=1 and analysis_right ='BBC Snooker/Pool' then number_of_events_viewed else 0 end) 
as SNPBBC_Total_Viewing_Events_LIVE
,sum(case when live=1 and analysis_right ='BBC Tennis' then number_of_events_viewed else 0 end) 
as TENBBC_Total_Viewing_Events_LIVE
,sum(case when live=1 and analysis_right ='BBC Unknown' then number_of_events_viewed else 0 end) 
as UNKBBC_Total_Viewing_Events_LIVE
,sum(case when live=1 and analysis_right ='BBC Watersports' then number_of_events_viewed else 0 end) 
as WATBBC_Total_Viewing_Events_LIVE
,sum(case when live=1 and analysis_right ='BBC Wintersports' then number_of_events_viewed else 0 end) 
as WINBBC_Total_Viewing_Events_LIVE
,sum(case when live=1 and analysis_right ='Boxing  - Channel 5' then number_of_events_viewed else 0 end) 
as BOXCH5_Total_Viewing_Events_LIVE
,sum(case when live=1 and analysis_right ='Boxing - Matchroom 2012-2014 Sky Sports' then number_of_events_viewed else 0 end) 
as BOXMSS_Total_Viewing_Events_LIVE
,sum(case when live=1 and analysis_right ='Brazil Football - BT Sport' then number_of_events_viewed else 0 end) 
as BFTBTS_Total_Viewing_Events_LIVE
,sum(case when live=1 and analysis_right ='British & Irish Lions 2013 - Sky Sports' then number_of_events_viewed else 0 end) 
as BILSS_Total_Viewing_Events_LIVE
,sum(case when live=1 and analysis_right ='British Open Golf - BBC' then number_of_events_viewed else 0 end) 
as BOGSS_Total_Viewing_Events_LIVE
,sum(case when live=1 and analysis_right ='BT Sport American Football' then number_of_events_viewed else 0 end) 
as AFBTS_Total_Viewing_Events_LIVE
,sum(case when live=1 and analysis_right ='BT Sport Athletics' then number_of_events_viewed else 0 end) 
as ATHBTS_Total_Viewing_Events_LIVE
,sum(case when live=1 and analysis_right ='BT Sport Baseball' then number_of_events_viewed else 0 end) 
as BASEBTS_Total_Viewing_Events_LIVE
,sum(case when live=1 and analysis_right ='BT Sport Basketball' then number_of_events_viewed else 0 end) 
as BASKBTS_Total_Viewing_Events_LIVE
,sum(case when live=1 and analysis_right ='BT Sport Boxing' then number_of_events_viewed else 0 end) 
as BOXBTS_Total_Viewing_Events_LIVE
,sum(case when live=1 and analysis_right ='BT Sport Cricket' then number_of_events_viewed else 0 end) 
as CRIBTS_Total_Viewing_Events_LIVE
,sum(case when live=1 and analysis_right ='BT Sport Equestrian' then number_of_events_viewed else 0 end) 
as EQUBTS_Total_Viewing_Events_LIVE
,sum(case when live=1 and analysis_right ='BT Sport Extreme' then number_of_events_viewed else 0 end) 
as EXTBTS_Total_Viewing_Events_LIVE
,sum(case when live=1 and analysis_right ='BT Sport Football' then number_of_events_viewed else 0 end) 
as FOOTBTS_Total_Viewing_Events_LIVE
,sum(case when live=1 and analysis_right ='BT Sport Motor Sport' then number_of_events_viewed else 0 end) 
as MSPBTS_Total_Viewing_Events_LIVE
,sum(case when live=1 and analysis_right ='BT Sport Rugby' then number_of_events_viewed else 0 end) 
as RUGBTS_Total_Viewing_Events_LIVE
,sum(case when live=1 and analysis_right ='BT Sport Tennis' then number_of_events_viewed else 0 end) 
as TENBTS_Total_Viewing_Events_LIVE
,sum(case when live=1 and analysis_right ='BT Sport Unknown' then number_of_events_viewed else 0 end) 
as UNKBTS_Total_Viewing_Events_LIVE
,sum(case when live=1 and analysis_right ='BT Sport Wintersports' then number_of_events_viewed else 0 end) 
as WINBTS_Total_Viewing_Events_LIVE
,sum(case when live=1 and analysis_right ='Bundesliga - BT Sport' then number_of_events_viewed else 0 end) 
as BUNBTS_Total_Viewing_Events_LIVE
,sum(case when live=1 and analysis_right ='Bundesliga- ESPN' then number_of_events_viewed else 0 end) 
as BUNESPN_Total_Viewing_Events_LIVE
,sum(case when live=1 and analysis_right ='Challenge Darts' then number_of_events_viewed else 0 end) 
as DRTCHA_Total_Viewing_Events_LIVE
,sum(case when live=1 and analysis_right ='Challenge Extreme' then number_of_events_viewed else 0 end) 
as EXTCHA_Total_Viewing_Events_LIVE
,sum(case when live=1 and analysis_right ='Challenge Unknown' then number_of_events_viewed else 0 end) 
as UNKCHA_Total_Viewing_Events_LIVE
,sum(case when live=1 and analysis_right ='Challenge Wrestling' then number_of_events_viewed else 0 end) 
as WRECHA_Total_Viewing_Events_LIVE
,sum(case when live=1 and analysis_right ='Champions League - ITV' then number_of_events_viewed else 0 end) 
as CHLITV_Total_Viewing_Events_LIVE
,sum(case when live=1 and analysis_right ='Champions Trophy - I C C Cricket Deal  2012-2015 Sky Sports' then number_of_events_viewed else 0 end) 
as ICCSS_Total_Viewing_Events_LIVE
,sum(case when live=1 and analysis_right ='Channel 4 American Football' then number_of_events_viewed else 0 end) 
as AMCH4_Total_Viewing_Events_LIVE
,sum(case when live=1 and analysis_right ='Channel 4 Athletics' then number_of_events_viewed else 0 end) 
as ATHCH4_Total_Viewing_Events_LIVE
,sum(case when live=1 and analysis_right ='Channel 4 Boxing' then number_of_events_viewed else 0 end) 
as BOXCH4_Total_Viewing_Events_LIVE
,sum(case when live=1 and analysis_right ='Channel 4 Equestrian' then number_of_events_viewed else 0 end) 
as EQUCH4_Total_Viewing_Events_LIVE
,sum(case when live=1 and analysis_right ='Channel 4 Extreme' then number_of_events_viewed else 0 end) 
as EXTCH4_Total_Viewing_Events_LIVE
,sum(case when live=1 and analysis_right ='Channel 4 Football' then number_of_events_viewed else 0 end) 
as FOOTCH4_Total_Viewing_Events_LIVE
,sum(case when live=1 and analysis_right ='Channel 4 Motor Sport' then number_of_events_viewed else 0 end) 
as MSPCH4_Total_Viewing_Events_LIVE
,sum(case when live=1 and analysis_right ='Channel 4 Racing' then number_of_events_viewed else 0 end) 
as RACCH4_Total_Viewing_Events_LIVE
,sum(case when live=1 and analysis_right ='Channel 4 Unknown' then number_of_events_viewed else 0 end) 
as UNKCH4_Total_Viewing_Events_LIVE
,sum(case when live=1 and analysis_right ='Channel 4 Watersports' then number_of_events_viewed else 0 end) 
as WATCH4_Total_Viewing_Events_LIVE
,sum(case when live=1 and analysis_right ='Channel 4 Wintersports' then number_of_events_viewed else 0 end) 
as WINCH4_Total_Viewing_Events_LIVE
,sum(case when live=1 and analysis_right ='Channel 5 Athletics' then number_of_events_viewed else 0 end) 
as ATHCH5_Total_Viewing_Events_LIVE
,sum(case when live=1 and analysis_right ='Channel 5 Boxing' then number_of_events_viewed else 0 end) 
as BOXOCH5_Total_Viewing_Events_LIVE
,sum(case when live=1 and analysis_right ='Channel 5 Cricket' then number_of_events_viewed else 0 end) 
as CRICH5_Total_Viewing_Events_LIVE
,sum(case when live=1 and analysis_right ='Channel 5 Motor Sport' then number_of_events_viewed else 0 end) 
as MSPCH5_Total_Viewing_Events_LIVE
,sum(case when live=1 and analysis_right ='Channel 5 Unknown' then number_of_events_viewed else 0 end) 
as UNKCH5_Total_Viewing_Events_LIVE
,sum(case when live=1 and analysis_right ='Channel 5 Wrestling' then number_of_events_viewed else 0 end) 
as WRECH5_Total_Viewing_Events_LIVE
,sum(case when live=1 and analysis_right ='Cheltenham Festival - Channel 4' then number_of_events_viewed else 0 end) 
as CHELCH4_Total_Viewing_Events_LIVE
,sum(case when live=1 and analysis_right ='Community Shield - ITV' then number_of_events_viewed else 0 end) 
as CMSITV_Total_Viewing_Events_LIVE
,sum(case when live=1 and analysis_right ='Confederations Cup - BBC' then number_of_events_viewed else 0 end) 
as CONCBBC_Total_Viewing_Events_LIVE
,sum(case when live=1 and analysis_right ='Conference - BT Sport' then number_of_events_viewed else 0 end) 
as CONFBTS_Total_Viewing_Events_LIVE
,sum(case when live=1 and analysis_right ='Cycling - La Vuelta ITV' then number_of_events_viewed else 0 end) 
as CLVITV_Total_Viewing_Events_LIVE
,sum(case when live=1 and analysis_right ='Cycling - U C I World Tour Sky Sports' then number_of_events_viewed else 0 end) 
as CUCISS_Total_Viewing_Events_LIVE
,sum(case when live=1 and analysis_right ='Cycling Tour of Britain - Eurosport' then number_of_events_viewed else 0 end) 
as CTBEUR_Total_Viewing_Events_LIVE
,sum(case when live=1 and analysis_right ='Cycling: tour of britain ITV4' then number_of_events_viewed else 0 end) 
as CTCITV_Total_Viewing_Events_LIVE
,sum(case when live=1 and analysis_right ='Derby - Channel 4' then number_of_events_viewed else 0 end) 
as DERCH4_Total_Viewing_Events_LIVE
,sum(case when live=1 and analysis_right ='ECB (highlights) - Channel 5' then number_of_events_viewed else 0 end) 
as ECBHCH5_Total_Viewing_Events_LIVE
,sum(case when live=1 and analysis_right ='ECB Cricket Sky Sports' then number_of_events_viewed else 0 end) 
as GECRSS_Total_Viewing_Events_LIVE
,sum(case when live=1 and analysis_right ='ECB non-Test Cricket Sky Sports' then number_of_events_viewed else 0 end) 
as ECBNSS_Total_Viewing_Events_LIVE
,sum(case when live=1 and analysis_right ='ECB Test Cricket Sky Sports' then number_of_events_viewed else 0 end) 
as ECBTSS_Total_Viewing_Events_LIVE
,sum(case when live=1 and analysis_right ='England Football Internationals - ITV' then number_of_events_viewed else 0 end) 
as GENGITV_Total_Viewing_Events_LIVE
,sum(case when live=1 and analysis_right ='England Friendlies (Football) - ITV' then number_of_events_viewed else 0 end) 
as EFRITV_Total_Viewing_Events_LIVE
,sum(case when live=1 and analysis_right ='England Rugby Internationals 2010-2015 - Sky Sports' then number_of_events_viewed else 0 end) 
as ENRSS_Total_Viewing_Events_LIVE
,sum(case when live=1 and analysis_right ='England World Cup Qualifying (Away) - ITV' then number_of_events_viewed else 0 end) 
as EWQAITV_Total_Viewing_Events_LIVE
,sum(case when live=1 and analysis_right ='England World Cup Qualifying (Home) - ITV' then number_of_events_viewed else 0 end) 
as EWQHITV_Total_Viewing_Events_LIVE
,sum(case when live=1 and analysis_right ='ESPN American Football' then number_of_events_viewed else 0 end) 
as AMESPN_Total_Viewing_Events_LIVE
,sum(case when live=1 and analysis_right ='ESPN Athletics' then number_of_events_viewed else 0 end) 
as ATHESPN_Total_Viewing_Events_LIVE
,sum(case when live=1 and analysis_right ='ESPN Baseball' then number_of_events_viewed else 0 end) 
as BASEESPN_Total_Viewing_Events_LIVE
,sum(case when live=1 and analysis_right ='ESPN Basketball' then number_of_events_viewed else 0 end) 
as BASKESPN_Total_Viewing_Events_LIVE
,sum(case when live=1 and analysis_right ='ESPN Boxing' then number_of_events_viewed else 0 end) 
as BOXESPN_Total_Viewing_Events_LIVE
,sum(case when live=1 and analysis_right ='ESPN Cricket' then number_of_events_viewed else 0 end) 
as CRIESPN_Total_Viewing_Events_LIVE
,sum(case when live=1 and analysis_right ='ESPN Darts' then number_of_events_viewed else 0 end) 
as DARTESPN_Total_Viewing_Events_LIVE
,sum(case when live=1 and analysis_right ='ESPN Equestrian' then number_of_events_viewed else 0 end) 
as EQUESPN_Total_Viewing_Events_LIVE
,sum(case when live=1 and analysis_right ='ESPN Extreme' then number_of_events_viewed else 0 end) 
as EXTESPN_Total_Viewing_Events_LIVE
,sum(case when live=1 and analysis_right ='ESPN Football' then number_of_events_viewed else 0 end) 
as FOOTESPN_Total_Viewing_Events_LIVE
,sum(case when live=1 and analysis_right ='ESPN Golf' then number_of_events_viewed else 0 end) 
as GOLFESPN_Total_Viewing_Events_LIVE
,sum(case when live=1 and analysis_right ='ESPN Ice Hockey' then number_of_events_viewed else 0 end) 
as IHESPN_Total_Viewing_Events_LIVE
,sum(case when live=1 and analysis_right ='ESPN Motor Sport' then number_of_events_viewed else 0 end) 
as MSPESPN_Total_Viewing_Events_LIVE
,sum(case when live=1 and analysis_right ='ESPN Racing' then number_of_events_viewed else 0 end) 
as RACESPN_Total_Viewing_Events_LIVE
,sum(case when live=1 and analysis_right ='ESPN Rugby' then number_of_events_viewed else 0 end) 
as RUGESPN_Total_Viewing_Events_LIVE
,sum(case when live=1 and analysis_right ='ESPN Snooker/Pool' then number_of_events_viewed else 0 end) 
as SNPESPN_Total_Viewing_Events_LIVE
,sum(case when live=1 and analysis_right ='ESPN Tennis' then number_of_events_viewed else 0 end) 
as TENESPN_Total_Viewing_Events_LIVE
,sum(case when live=1 and analysis_right ='ESPN Unknown' then number_of_events_viewed else 0 end) 
as UNKESPN_Total_Viewing_Events_LIVE
,sum(case when live=1 and analysis_right ='ESPN Watersports' then number_of_events_viewed else 0 end) 
as WATESPN_Total_Viewing_Events_LIVE
,sum(case when live=1 and analysis_right ='ESPN Wintersports' then number_of_events_viewed else 0 end) 
as WINESPN_Total_Viewing_Events_LIVE
,sum(case when live=1 and analysis_right ='ESPN Wrestling' then number_of_events_viewed else 0 end) 
as WREESPN_Total_Viewing_Events_LIVE
,sum(case when live=1 and analysis_right ='Europa League - BT Sport' then number_of_events_viewed else 0 end) 
as ELBTSP_Total_Viewing_Events_LIVE
,sum(case when live=1 and analysis_right ='Europa League - ESPN' then number_of_events_viewed else 0 end) 
as ELESPN_Total_Viewing_Events_LIVE
,sum(case when live=1 and analysis_right ='Europa League - ITV' then number_of_events_viewed else 0 end) 
as ELITV_Total_Viewing_Events_LIVE
,sum(case when live=1 and analysis_right ='European Tour Golf - Sky Sports' then number_of_events_viewed else 0 end) 
as ETGSS_Total_Viewing_Events_LIVE
,sum(case when live=1 and analysis_right ='Eurosport American Football' then number_of_events_viewed else 0 end) 
as AMEUR_Total_Viewing_Events_LIVE
,sum(case when live=1 and analysis_right ='Eurosport Athletics' then number_of_events_viewed else 0 end) 
as ATHEUR_Total_Viewing_Events_LIVE
,sum(case when live=1 and analysis_right ='Eurosport Baseball' then number_of_events_viewed else 0 end) 
as BASEEUR_Total_Viewing_Events_LIVE
,sum(case when live=1 and analysis_right ='Eurosport Basketball' then number_of_events_viewed else 0 end) 
as BASKEUR_Total_Viewing_Events_LIVE
,sum(case when live=1 and analysis_right ='Eurosport Boxing' then number_of_events_viewed else 0 end) 
as BOXEUR_Total_Viewing_Events_LIVE
,sum(case when live=1 and analysis_right ='Eurosport Cricket' then number_of_events_viewed else 0 end) 
as CRIEUR_Total_Viewing_Events_LIVE
,sum(case when live=1 and analysis_right ='Eurosport Darts' then number_of_events_viewed else 0 end) 
as DARTEUR_Total_Viewing_Events_LIVE
,sum(case when live=1 and analysis_right ='Eurosport Equestrian' then number_of_events_viewed else 0 end) 
as EQUEUR_Total_Viewing_Events_LIVE
,sum(case when live=1 and analysis_right ='Eurosport Extreme' then number_of_events_viewed else 0 end) 
as EXTEUR_Total_Viewing_Events_LIVE
,sum(case when live=1 and analysis_right ='Eurosport Football' then number_of_events_viewed else 0 end) 
as FOOTEUR_Total_Viewing_Events_LIVE
,sum(case when live=1 and analysis_right ='Eurosport Golf' then number_of_events_viewed else 0 end) 
as GOLFEUR_Total_Viewing_Events_LIVE
,sum(case when live=1 and analysis_right ='Eurosport Ice Hockey' then number_of_events_viewed else 0 end) 
as IHEUR_Total_Viewing_Events_LIVE
,sum(case when live=1 and analysis_right ='Eurosport Motor Sport' then number_of_events_viewed else 0 end) 
as MSPEUR_Total_Viewing_Events_LIVE
,sum(case when live=1 and analysis_right ='Eurosport Racing' then number_of_events_viewed else 0 end) 
as RACEUR_Total_Viewing_Events_LIVE
,sum(case when live=1 and analysis_right ='Eurosport Snooker/Pool' then number_of_events_viewed else 0 end) 
as SNPEUR_Total_Viewing_Events_LIVE
,sum(case when live=1 and analysis_right ='Eurosport Tennis' then number_of_events_viewed else 0 end) 
as TENEUR_Total_Viewing_Events_LIVE
,sum(case when live=1 and analysis_right ='Eurosport Unknown' then number_of_events_viewed else 0 end) 
as UNKEUR_Total_Viewing_Events_LIVE
,sum(case when live=1 and analysis_right ='Eurosport Watersports' then number_of_events_viewed else 0 end) 
as WATEUR_Total_Viewing_Events_LIVE
,sum(case when live=1 and analysis_right ='Eurosport Wintersports' then number_of_events_viewed else 0 end) 
as WINEUR_Total_Viewing_Events_LIVE
,sum(case when live=1 and analysis_right ='F1 - BBC' then number_of_events_viewed else 0 end) 
as GF1BBC_Total_Viewing_Events_LIVE
,sum(case when live=1 and analysis_right ='F1 - Sky Sports' then number_of_events_viewed else 0 end) 
as GF1SS_Total_Viewing_Events_LIVE
,sum(case when live=1 and analysis_right ='F1 (non-Live)- BBC' then number_of_events_viewed else 0 end) 
as F1NBBC_Total_Viewing_Events_LIVE
,sum(case when live=1 and analysis_right ='F1 (Practice Live)- BBC' then number_of_events_viewed else 0 end) 
as F1PBBC_Total_Viewing_Events_LIVE
,sum(case when live=1 and analysis_right ='F1 (Qualifying Live)- BBC' then number_of_events_viewed else 0 end) 
as F1QBBC_Total_Viewing_Events_LIVE
,sum(case when live=1 and analysis_right ='F1 (Race Live)- BBC' then number_of_events_viewed else 0 end) 
as F1RBBC_Total_Viewing_Events_LIVE
,sum(case when live=1 and analysis_right ='FA Cup - ESPN' then number_of_events_viewed else 0 end) 
as FACESPN_Total_Viewing_Events_LIVE
,sum(case when live=1 and analysis_right ='FA Cup - ITV' then number_of_events_viewed else 0 end) 
as FACITV_Total_Viewing_Events_LIVE
,sum(case when live=1 and analysis_right ='Football League - Seasons 2012/13 To 14/15 Sky Sports - Capital 1 Cup' then number_of_events_viewed else 0 end) 
as FLCCSS_Total_Viewing_Events_LIVE
,sum(case when live=1 and analysis_right ='Football League - Seasons 2012/13 To 14/15 Sky Sports - others' then number_of_events_viewed else 0 end) 
as FLOTSS_Total_Viewing_Events_LIVE
,sum(case when live=1 and analysis_right ='Formula One 2012-2018 - (non-Live) Sky Sports' then number_of_events_viewed else 0 end) 
as F1NSS_Total_Viewing_Events_LIVE
,sum(case when live=1 and analysis_right ='Formula One 2012-2018 - (Practice Live) Sky Sports' then number_of_events_viewed else 0 end) 
as F1PSS_Total_Viewing_Events_LIVE
,sum(case when live=1 and analysis_right ='Formula One 2012-2018 - (Qualifying Live) Sky Sports' then number_of_events_viewed else 0 end) 
as F1QSS_Total_Viewing_Events_LIVE
,sum(case when live=1 and analysis_right ='Formula One 2012-2018 - (Race Live) Sky Sports' then number_of_events_viewed else 0 end) 
as F1RSS_Total_Viewing_Events_LIVE
,sum(case when live=1 and analysis_right ='French Open Tennis - Eurosport' then number_of_events_viewed else 0 end) 
as FOTEUR_Total_Viewing_Events_LIVE
,sum(case when live=1 and analysis_right ='French Open Tennis - ITV' then number_of_events_viewed else 0 end) 
as FOTITV_Total_Viewing_Events_LIVE
,sum(case when live=1 and analysis_right ='Grand National - Channel 4' then number_of_events_viewed else 0 end) 
as GDNCH4_Total_Viewing_Events_LIVE
,sum(case when live=1 and analysis_right ='Heineken Cup 2010/11 To 2013/14- Sky Sports' then number_of_events_viewed else 0 end) 
as HECSS_Total_Viewing_Events_LIVE
,sum(case when live=1 and analysis_right ='I R B Sevens 2008/09 -2014/15 - Sky Sports' then number_of_events_viewed else 0 end) 
as IRBSS_Total_Viewing_Events_LIVE
,sum(case when live=1 and analysis_right ='IAAF World Athletics Championship - Eurosport' then number_of_events_viewed else 0 end) 
as WACEUR_Total_Viewing_Events_LIVE
,sum(case when live=1 and analysis_right ='India Home Cricket 2012-2018 Sky Sports' then number_of_events_viewed else 0 end) 
as IHCSS_Total_Viewing_Events_LIVE
,sum(case when live=1 and analysis_right ='India Premier League - ITV' then number_of_events_viewed else 0 end) 
as IPLITV_Total_Viewing_Events_LIVE
,sum(case when live=1 and analysis_right ='International Freindlies - ESPN' then number_of_events_viewed else 0 end) 
as IFESPN_Total_Viewing_Events_LIVE
,sum(case when live=1 and analysis_right ='International Friendlies - BT Sport' then number_of_events_viewed else 0 end) 
as IFBTS_Total_Viewing_Events_LIVE
,sum(case when live=1 and analysis_right ='ITV1 Boxing' then number_of_events_viewed else 0 end) 
as BOXITV1_Total_Viewing_Events_LIVE
,sum(case when live=1 and analysis_right ='ITV1 Football' then number_of_events_viewed else 0 end) 
as FOOTITV1_Total_Viewing_Events_LIVE
,sum(case when live=1 and analysis_right ='ITV1 Motor Sport' then number_of_events_viewed else 0 end) 
as MOTSITV1_Total_Viewing_Events_LIVE
,sum(case when live=1 and analysis_right ='ITV1 Rugby' then number_of_events_viewed else 0 end) 
as RUGITV1_Total_Viewing_Events_LIVE
,sum(case when live=1 and analysis_right ='ITV1 Snooker/Pool' then number_of_events_viewed else 0 end) 
as SNPITV1_Total_Viewing_Events_LIVE
,sum(case when live=1 and analysis_right ='ITV1 Unknown' then number_of_events_viewed else 0 end) 
as UNKITV1_Total_Viewing_Events_LIVE
,sum(case when live=1 and analysis_right ='ITV4 Boxing' then number_of_events_viewed else 0 end) 
as BOXITV4_Total_Viewing_Events_LIVE
,sum(case when live=1 and analysis_right ='ITV4 Cricket' then number_of_events_viewed else 0 end) 
as CRIITV4_Total_Viewing_Events_LIVE
,sum(case when live=1 and analysis_right ='ITV4 Darts' then number_of_events_viewed else 0 end) 
as DARTITV4_Total_Viewing_Events_LIVE
,sum(case when live=1 and analysis_right ='ITV4 Extreme' then number_of_events_viewed else 0 end) 
as EXTITV4_Total_Viewing_Events_LIVE
,sum(case when live=1 and analysis_right ='ITV4 Football' then number_of_events_viewed else 0 end) 
as FOOTITV4_Total_Viewing_Events_LIVE
,sum(case when live=1 and analysis_right ='ITV4 Motor Sport' then number_of_events_viewed else 0 end) 
as MSPITV4_Total_Viewing_Events_LIVE
,sum(case when live=1 and analysis_right ='ITV4 Rugby' then number_of_events_viewed else 0 end) 
as RUGITV4_Total_Viewing_Events_LIVE
,sum(case when live=1 and analysis_right ='ITV4 Snooker/Pool' then number_of_events_viewed else 0 end) 
as SNPITV4_Total_Viewing_Events_LIVE
,sum(case when live=1 and analysis_right ='ITV4 Tennis' then number_of_events_viewed else 0 end) 
as TENITV4_Total_Viewing_Events_LIVE
,sum(case when live=1 and analysis_right ='ITV4 Unknown' then number_of_events_viewed else 0 end) 
as UNKITV4_Total_Viewing_Events_LIVE
,sum(case when live=1 and analysis_right ='Ligue 1 - BT Sport' then number_of_events_viewed else 0 end) 
as L1BTS_Total_Viewing_Events_LIVE
,sum(case when live=1 and analysis_right ='Ligue 1 - ESPN' then number_of_events_viewed else 0 end) 
as L1ESPN_Total_Viewing_Events_LIVE
,sum(case when live=1 and analysis_right ='Match of the day - BBC' then number_of_events_viewed else 0 end) 
as MOTDBBC_Total_Viewing_Events_LIVE
,sum(case when live=1 and analysis_right ='Matchroom Output Deal  2010-2012 (Other)- Sky Sports' then number_of_events_viewed else 0 end) 
as MROSS_Total_Viewing_Events_LIVE
,sum(case when live=1 and analysis_right ='Matchroom Output Deal  2010-2012 (Pool)- Sky Sports' then number_of_events_viewed else 0 end) 
as MRPSS_Total_Viewing_Events_LIVE
,sum(case when live=1 and analysis_right ='Matchroom Output Deal  2010-2012 (Snooker)- Sky Sports' then number_of_events_viewed else 0 end) 
as MRSSS_Total_Viewing_Events_LIVE
,sum(case when live=1 and analysis_right ='Moto GP BBC' then number_of_events_viewed else 0 end) 
as MGPBBC_Total_Viewing_Events_LIVE
,sum(case when live=1 and analysis_right ='NBA - Sky Sports' then number_of_events_viewed else 0 end) 
as NBASS_Total_Viewing_Events_LIVE
,sum(case when live=1 and analysis_right ='NFL - BBC' then number_of_events_viewed else 0 end) 
as NFLBBC_Total_Viewing_Events_LIVE
,sum(case when live=1 and analysis_right ='NFL - Channel 4' then number_of_events_viewed else 0 end) 
as NFLCH4_Total_Viewing_Events_LIVE
,sum(case when live=1 and analysis_right ='NFL Seasons 2012-2015 - Sky Sports' then number_of_events_viewed else 0 end) 
as NFLSS_Total_Viewing_Events_LIVE
,sum(case when live=1 and analysis_right ='Northern Ireland Football Association 2012-2014 - Sky Sports' then number_of_events_viewed else 0 end) 
as NIFSS_Total_Viewing_Events_LIVE
,sum(case when live=1 and analysis_right ='Oaks - Channel 4' then number_of_events_viewed else 0 end) 
as OAKCH4_Total_Viewing_Events_LIVE
,sum(case when live=1 and analysis_right ='Other American Football' then number_of_events_viewed else 0 end) 
as AMOTH_Total_Viewing_Events_LIVE
,sum(case when live=1 and analysis_right ='Other Athletics' then number_of_events_viewed else 0 end) 
as ATHOTH_Total_Viewing_Events_LIVE
,sum(case when live=1 and analysis_right ='Other Baseball' then number_of_events_viewed else 0 end) 
as BASEOTH_Total_Viewing_Events_LIVE
,sum(case when live=1 and analysis_right ='Other Basketball' then number_of_events_viewed else 0 end) 
as BASKOTH_Total_Viewing_Events_LIVE
,sum(case when live=1 and analysis_right ='Other Boxing' then number_of_events_viewed else 0 end) 
as BOXOTH_Total_Viewing_Events_LIVE
,sum(case when live=1 and analysis_right ='Other Cricket' then number_of_events_viewed else 0 end) 
as CRIOTH_Total_Viewing_Events_LIVE
,sum(case when live=1 and analysis_right ='Other Darts' then number_of_events_viewed else 0 end) 
as DARTOTH_Total_Viewing_Events_LIVE
,sum(case when live=1 and analysis_right ='Other Equestrian' then number_of_events_viewed else 0 end) 
as EQUOTH_Total_Viewing_Events_LIVE
,sum(case when live=1 and analysis_right ='Other Extreme' then number_of_events_viewed else 0 end) 
as EXTOTH_Total_Viewing_Events_LIVE
,sum(case when live=1 and analysis_right ='Other Fishing' then number_of_events_viewed else 0 end) 
as FSHOTH_Total_Viewing_Events_LIVE
,sum(case when live=1 and analysis_right ='Other Football' then number_of_events_viewed else 0 end) 
as FOOTOTH_Total_Viewing_Events_LIVE
,sum(case when live=1 and analysis_right ='Other Golf' then number_of_events_viewed else 0 end) 
as GOLFOTH_Total_Viewing_Events_LIVE
,sum(case when live=1 and analysis_right ='Other Ice Hockey' then number_of_events_viewed else 0 end) 
as IHOTH_Total_Viewing_Events_LIVE
,sum(case when live=1 and analysis_right ='Other Motor Sport' then number_of_events_viewed else 0 end) 
as MSPOTH_Total_Viewing_Events_LIVE
,sum(case when live=1 and analysis_right ='Other Racing' then number_of_events_viewed else 0 end) 
as RACOTH_Total_Viewing_Events_LIVE
,sum(case when live=1 and analysis_right ='Other Rugby' then number_of_events_viewed else 0 end) 
as RUGOTH_Total_Viewing_Events_LIVE
,sum(case when live=1 and analysis_right ='Other Rugby Internationals - ESPN' then number_of_events_viewed else 0 end) 
as ORUGESPN_Total_Viewing_Events_LIVE
,sum(case when live=1 and analysis_right ='Other Snooker/Pool' then number_of_events_viewed else 0 end) 
as OTHSNP_Total_Viewing_Events_LIVE
,sum(case when live=1 and analysis_right ='Other Tennis' then number_of_events_viewed else 0 end) 
as OTHTEN_Total_Viewing_Events_LIVE
,sum(case when live=1 and analysis_right ='Other Unknown' then number_of_events_viewed else 0 end) 
as OTHUNK_Total_Viewing_Events_LIVE
,sum(case when live=1 and analysis_right ='Other Watersports' then number_of_events_viewed else 0 end) 
as OTHWAT_Total_Viewing_Events_LIVE
,sum(case when live=1 and analysis_right ='Other Wintersports' then number_of_events_viewed else 0 end) 
as OTHWIN_Total_Viewing_Events_LIVE
,sum(case when live=1 and analysis_right ='Other Wrestling' then number_of_events_viewed else 0 end) 
as OTHWRE_Total_Viewing_Events_LIVE
,sum(case when live=1 and analysis_right ='PGA Tour Golf 2010-2017 - Sky Sports' then number_of_events_viewed else 0 end) 
as PGASS_Total_Viewing_Events_LIVE
,sum(case when live=1 and analysis_right ='Premier League - BT Sport' then number_of_events_viewed else 0 end) 
as PLBTS_Total_Viewing_Events_LIVE
,sum(case when live=1 and analysis_right ='Premier League - ESPN' then number_of_events_viewed else 0 end) 
as PLESPN_Total_Viewing_Events_LIVE
,sum(case when live=1 and analysis_right ='Premier League Darts 2010-2013 - Sky Sports' then number_of_events_viewed else 0 end) 
as PLDSS_Total_Viewing_Events_LIVE
,sum(case when live=1 and analysis_right ='Premier League Football - Sky Sports' then number_of_events_viewed else 0 end) 
as GPLSS_Total_Viewing_Events_LIVE
,sum(case when live=1 and analysis_right ='Premier League Football - Sky Sports (Match Choice)' then number_of_events_viewed else 0 end) 
as PLMCSS_Total_Viewing_Events_LIVE
,sum(case when live=1 and analysis_right ='Premier League Football - Sky Sports (MNF)' then number_of_events_viewed else 0 end) 
as PLMNFSS_Total_Viewing_Events_LIVE
,sum(case when live=1 and analysis_right ='Premier League Football - Sky Sports (non Live)' then number_of_events_viewed else 0 end) 
as PLNLSS_Total_Viewing_Events_LIVE
,sum(case when live=1 and analysis_right ='Premier League Football - Sky Sports (other Live)' then number_of_events_viewed else 0 end) 
as PLOLSS_Total_Viewing_Events_LIVE
,sum(case when live=1 and analysis_right ='Premier League Football - Sky Sports (Sat Lunchtime)' then number_of_events_viewed else 0 end) 
as PLSLSS_Total_Viewing_Events_LIVE
,sum(case when live=1 and analysis_right ='Premier League Football - Sky Sports (Sat Night Live)' then number_of_events_viewed else 0 end) 
as PLSNSS_Total_Viewing_Events_LIVE
,sum(case when live=1 and analysis_right ='Premier League Football - Sky Sports (Sun 4pm)' then number_of_events_viewed else 0 end) 
as PLS4SS_Total_Viewing_Events_LIVE
,sum(case when live=1 and analysis_right ='Premier League Football - Sky Sports (Sun Lunchtime)' then number_of_events_viewed else 0 end) 
as PLSULSS_Total_Viewing_Events_LIVE
,sum(case when live=1 and analysis_right ='Premiership Rugby - Sky Sports' then number_of_events_viewed else 0 end) 
as PRUSS_Total_Viewing_Events_LIVE
,sum(case when live=1 and analysis_right ='Republic Of Ireland FA - 2010/11 To 2013/14 - Sky Sports' then number_of_events_viewed else 0 end) 
as ROISS_Total_Viewing_Events_LIVE
,sum(case when live=1 and analysis_right ='Royal Ascot - Channel 4' then number_of_events_viewed else 0 end) 
as RASCH4_Total_Viewing_Events_LIVE
,sum(case when live=1 and analysis_right ='Rugby Internationals (England) - BBC' then number_of_events_viewed else 0 end) 
as RIEBBC_Total_Viewing_Events_LIVE
,sum(case when live=1 and analysis_right ='Rugby Internationals (Ireland) - BBC' then number_of_events_viewed else 0 end) 
as RIIBBC_Total_Viewing_Events_LIVE
,sum(case when live=1 and analysis_right ='Rugby Internationals (Scotland) - BBC' then number_of_events_viewed else 0 end) 
as RISBBC_Total_Viewing_Events_LIVE
,sum(case when live=1 and analysis_right ='Rugby Internationals (Wales) - BBC' then number_of_events_viewed else 0 end) 
as RIWBBC_Total_Viewing_Events_LIVE
,sum(case when live=1 and analysis_right ='Rugby League  Challenge Cup- BBC' then number_of_events_viewed else 0 end) 
as RLCCBBC_Total_Viewing_Events_LIVE
,sum(case when live=1 and analysis_right ='Rugby League - Sky Sports' then number_of_events_viewed else 0 end) 
as RLGSS_Total_Viewing_Events_LIVE
,sum(case when live=1 and analysis_right ='Rugby League  World Cup- BBC' then number_of_events_viewed else 0 end) 
as RLWCBBC_Total_Viewing_Events_LIVE
,sum(case when live=1 and analysis_right ='Sanzar Rugby Union 2011 - 2015 - Sky Sports' then number_of_events_viewed else 0 end) 
as SARUSS_Total_Viewing_Events_LIVE
,sum(case when live=1 and analysis_right ='Scottish FA Deal - 2010/11 To 2013/14 - Sky Sports' then number_of_events_viewed else 0 end) 
as SFASS_Total_Viewing_Events_LIVE
,sum(case when live=1 and analysis_right ='Serie A - BT Sport' then number_of_events_viewed else 0 end) 
as SABTS_Total_Viewing_Events_LIVE
,sum(case when live=1 and analysis_right ='Serie A - ESPN' then number_of_events_viewed else 0 end) 
as SAESPN_Total_Viewing_Events_LIVE
,sum(case when live=1 and analysis_right ='SFL - ESPN' then number_of_events_viewed else 0 end) 
as SFLESPN_Total_Viewing_Events_LIVE
,sum(case when live=1 and analysis_right ='Six Nations - BBC' then number_of_events_viewed else 0 end) 
as SNRBBC_Total_Viewing_Events_LIVE
,sum(case when live=1 and analysis_right ='Sky 1 and Sky 2 Boxing' then number_of_events_viewed else 0 end) 
as BOXS12_Total_Viewing_Events_LIVE
,sum(case when live=1 and analysis_right ='Sky 1 and Sky 2 Football' then number_of_events_viewed else 0 end) 
as FOOTS12_Total_Viewing_Events_LIVE
,sum(case when live=1 and analysis_right ='Sky 1 and Sky 2 Motor Sport' then number_of_events_viewed else 0 end) 
as MSPS12_Total_Viewing_Events_LIVE
,sum(case when live=1 and analysis_right ='Sky 1 and Sky 2 Unknown' then number_of_events_viewed else 0 end) 
as UNKS12_Total_Viewing_Events_LIVE
,sum(case when live=1 and analysis_right ='Sky 1 and Sky 2 Wrestling' then number_of_events_viewed else 0 end) 
as WRES12_Total_Viewing_Events_LIVE
,sum(case when live=1 and analysis_right ='Sky Sports American Football' then number_of_events_viewed else 0 end) 
as AMSS_Total_Viewing_Events_LIVE
,sum(case when live=1 and analysis_right ='Sky Sports Athletics' then number_of_events_viewed else 0 end) 
as ATHSS_Total_Viewing_Events_LIVE
,sum(case when live=1 and analysis_right ='Sky Sports Baseball' then number_of_events_viewed else 0 end) 
as BASESS_Total_Viewing_Events_LIVE
,sum(case when live=1 and analysis_right ='Sky Sports Basketball' then number_of_events_viewed else 0 end) 
as BASKSS_Total_Viewing_Events_LIVE
,sum(case when live=1 and analysis_right ='Sky Sports Boxing' then number_of_events_viewed else 0 end) 
as BOXSS_Total_Viewing_Events_LIVE
,sum(case when live=1 and analysis_right ='Sky Sports Cricket' then number_of_events_viewed else 0 end) 
as CRISS_Total_Viewing_Events_LIVE
,sum(case when live=1 and analysis_right ='Sky Sports Darts' then number_of_events_viewed else 0 end) 
as DARTSS_Total_Viewing_Events_LIVE
,sum(case when live=1 and analysis_right ='Sky Sports Equestrian' then number_of_events_viewed else 0 end) 
as EQUSS_Total_Viewing_Events_LIVE
,sum(case when live=1 and analysis_right ='Sky Sports Extreme' then number_of_events_viewed else 0 end) 
as EXTSS_Total_Viewing_Events_LIVE
,sum(case when live=1 and analysis_right ='Sky Sports Fishing' then number_of_events_viewed else 0 end) 
as FISHSS_Total_Viewing_Events_LIVE
,sum(case when live=1 and analysis_right ='Sky Sports Football' then number_of_events_viewed else 0 end) 
as FOOTSS_Total_Viewing_Events_LIVE
,sum(case when live=1 and analysis_right ='Sky Sports Golf' then number_of_events_viewed else 0 end) 
as GOLFSS_Total_Viewing_Events_LIVE
,sum(case when live=1 and analysis_right ='Sky Sports Ice Hockey' then number_of_events_viewed else 0 end) 
as IHSS_Total_Viewing_Events_LIVE
,sum(case when live=1 and analysis_right ='Sky Sports Motor Sport' then number_of_events_viewed else 0 end) 
as MSPSS_Total_Viewing_Events_LIVE
,sum(case when live=1 and analysis_right ='Sky Sports Racing' then number_of_events_viewed else 0 end) 
as RACSS_Total_Viewing_Events_LIVE
,sum(case when live=1 and analysis_right ='Sky Sports Rugby' then number_of_events_viewed else 0 end) 
as RUGSS_Total_Viewing_Events_LIVE
,sum(case when live=1 and analysis_right ='Sky Sports Snooker/Pool' then number_of_events_viewed else 0 end) 
as SNPSS_Total_Viewing_Events_LIVE
,sum(case when live=1 and analysis_right ='Sky Sports Tennis' then number_of_events_viewed else 0 end) 
as TENSS_Total_Viewing_Events_LIVE
,sum(case when live=1 and analysis_right ='Sky Sports Unknown' then number_of_events_viewed else 0 end) 
as UNKSS_Total_Viewing_Events_LIVE
,sum(case when live=1 and analysis_right ='Sky Sports Watersports' then number_of_events_viewed else 0 end) 
as WATSS_Total_Viewing_Events_LIVE
,sum(case when live=1 and analysis_right ='Sky Sports Wintersports' then number_of_events_viewed else 0 end) 
as WINSS_Total_Viewing_Events_LIVE
,sum(case when live=1 and analysis_right ='Sky Sports Wrestling' then number_of_events_viewed else 0 end) 
as WRESS_Total_Viewing_Events_LIVE
,sum(case when live=1 and analysis_right ='Solheim Cup 2013 Ladies Golf - Sky Sports' then number_of_events_viewed else 0 end) 
as SOLSS_Total_Viewing_Events_LIVE
,sum(case when live=1 and analysis_right ='South African Home Cricket  2012-2020 Sky Sports' then number_of_events_viewed else 0 end) 
as SACSS_Total_Viewing_Events_LIVE
,sum(case when live=1 and analysis_right ='Spanish Football 2012/13 To 14/15 - Sky Sports' then number_of_events_viewed else 0 end) 
as SPFSS_Total_Viewing_Events_LIVE
,sum(case when live=1 and analysis_right ='SPFL - BT Sport' then number_of_events_viewed else 0 end) 
as SPFLBTS_Total_Viewing_Events_LIVE
,sum(case when live=1 and analysis_right ='SPL - ESPN' then number_of_events_viewed else 0 end) 
as SPLESPN_Total_Viewing_Events_LIVE
,sum(case when live=1 and analysis_right ='SPL - Sky Sports' then number_of_events_viewed else 0 end) 
as SPLSS_Total_Viewing_Events_LIVE
,sum(case when live=1 and analysis_right ='Sportfive World Cup 2014 Qualifers - Sky Sports' then number_of_events_viewed else 0 end) 
as SP5SS_Total_Viewing_Events_LIVE
,sum(case when live=1 and analysis_right ='The boat race - BBC' then number_of_events_viewed else 0 end) 
as BTRBBC_Total_Viewing_Events_LIVE
,sum(case when live=1 and analysis_right ='The football league show - BBC' then number_of_events_viewed else 0 end) 
as FLSBBC_Total_Viewing_Events_LIVE
,sum(case when live=1 and analysis_right ='The Masters Golf - BBC' then number_of_events_viewed else 0 end) 
as MGBBC_Total_Viewing_Events_LIVE
,sum(case when live=1 and analysis_right ='TNA Wrestling Challenge' then number_of_events_viewed else 0 end) 
as TNACHA_Total_Viewing_Events_LIVE
,sum(case when live=1 and analysis_right ='Tour de France - Eurosport' then number_of_events_viewed else 0 end) 
as TDFEUR_Total_Viewing_Events_LIVE
,sum(case when live=1 and analysis_right ='Tour de France - ITV' then number_of_events_viewed else 0 end) 
as TDFITV_Total_Viewing_Events_LIVE
,sum(case when live=1 and analysis_right ='U S Masters Golf 2011-2014 Sky Sports' then number_of_events_viewed else 0 end) 
as USMGSS_Total_Viewing_Events_LIVE
,sum(case when live=1 and analysis_right ='U.S Open Tennis 2013-2015 Sky Sports' then number_of_events_viewed else 0 end) 
as USOTSS_Total_Viewing_Events_LIVE
,sum(case when live=1 and analysis_right ='U.S Open/Womens/Seniors/Amateur Golf Sky Sports' then number_of_events_viewed else 0 end) 
as USOGSS_Total_Viewing_Events_LIVE
,sum(case when live=1 and analysis_right ='UEFA Champions League -  Sky Sports' then number_of_events_viewed else 0 end) 
as CLASS_Total_Viewing_Events_LIVE
,sum(case when live=1 and analysis_right ='UEFA Champions League -  Sky Sports (non Live)' then number_of_events_viewed else 0 end) 
as CLNSS_Total_Viewing_Events_LIVE
,sum(case when live=1 and analysis_right ='UEFA Champions League -  Sky Sports (other Live)' then number_of_events_viewed else 0 end) 
as CLOSS_Total_Viewing_Events_LIVE
,sum(case when live=1 and analysis_right ='UEFA Champions League -  Sky Sports (Tue)' then number_of_events_viewed else 0 end) 
as CLTSS_Total_Viewing_Events_LIVE
,sum(case when live=1 and analysis_right ='UEFA Champions League -  Sky Sports (Wed)' then number_of_events_viewed else 0 end) 
as CLWSS_Total_Viewing_Events_LIVE
,sum(case when live=1 and analysis_right ='US Open Tennis - Eurosport' then number_of_events_viewed else 0 end) 
as USOTEUR_Total_Viewing_Events_LIVE
,sum(case when live=1 and analysis_right ='USA Football - BT Sport' then number_of_events_viewed else 0 end) 
as USFBTS_Total_Viewing_Events_LIVE
,sum(case when live=1 and analysis_right ='USPGA Championship (2007-2016) Sky Sports' then number_of_events_viewed else 0 end) 
as USPGASS_Total_Viewing_Events_LIVE
,sum(case when live=1 and analysis_right ='WCQ - ESPN' then number_of_events_viewed else 0 end) 
as WCQESPN_Total_Viewing_Events_LIVE
,sum(case when live=1 and analysis_right ='Welsh Home International Football 2012-2014 Sky Sports' then number_of_events_viewed else 0 end) 
as WIFSS_Total_Viewing_Events_LIVE
,sum(case when live=1 and analysis_right ='West Indies Home Cricket 2013-2020 Sky Sports' then number_of_events_viewed else 0 end) 
as WICSS_Total_Viewing_Events_LIVE
,sum(case when live=1 and analysis_right ='Wimbledon - BBC' then number_of_events_viewed else 0 end) 
as WIMBBC_Total_Viewing_Events_LIVE
,sum(case when live=1 and analysis_right ='Womens Cricket - I C C Cricket Deal  2012-2015 Sky Sports' then number_of_events_viewed else 0 end) 
as WICCSS_Total_Viewing_Events_LIVE
,sum(case when live=1 and analysis_right ='World Athletics Championship - More 4' then number_of_events_viewed else 0 end) 
as WACMR4_Total_Viewing_Events_LIVE
,sum(case when live=1 and analysis_right ='World Club Championship - BBC' then number_of_events_viewed else 0 end) 
as WCLBBBC_Total_Viewing_Events_LIVE
,sum(case when live=1 and analysis_right ='World Cup Qualifiers - BT Sport' then number_of_events_viewed else 0 end) 
as WCQBTS_Total_Viewing_Events_LIVE
,sum(case when live=1 and analysis_right ='World Darts Championship 2009-2012 Sky Sports' then number_of_events_viewed else 0 end) 
as WDCSS_Total_Viewing_Events_LIVE
,sum(case when live=1 and analysis_right ='World snooker championship - BBC' then number_of_events_viewed else 0 end) 
as WSCBBC_Total_Viewing_Events_LIVE
,sum(case when live=1 and analysis_right ='WWE Sky 1 and 2' then number_of_events_viewed else 0 end) 
as WWES12_Total_Viewing_Events_LIVE
,sum(case when live=1 and analysis_right ='WWE Sky Sports' then number_of_events_viewed else 0 end) 
as WWESS_Total_Viewing_Events_LIVE
,sum(case when live=0 and analysis_right ='Africa Cup of Nations - Eurosport' then number_of_events_viewed else 0 end) 
as AFCEUR_Total_Viewing_EventsNon_LIVE
,sum(case when live=0 and analysis_right ='Africa Cup of Nations - ITV' then number_of_events_viewed else 0 end) 
as AFCITV_Total_Viewing_EventsNon_LIVE
,sum(case when live=0 and analysis_right ='Americas Cup - BBC' then number_of_events_viewed else 0 end) 
as AMCBBC_Total_Viewing_EventsNon_LIVE
,sum(case when live=0 and analysis_right ='Asian Tour Golf 2012-2013 Sky Sports' then number_of_events_viewed else 0 end) 
as ATGSS_Total_Viewing_EventsNon_LIVE
,sum(case when live=0 and analysis_right ='ATP Tennis Masters Series  2011-2013 Sky Sports' then number_of_events_viewed else 0 end) 
as ATPSS_Total_Viewing_EventsNon_LIVE
,sum(case when live=0 and analysis_right ='Australia Home Cricket  2012-2016 Sky Sports' then number_of_events_viewed else 0 end) 
as AHCSS_Total_Viewing_EventsNon_LIVE
,sum(case when live=0 and analysis_right ='Australian Football - BT Sport' then number_of_events_viewed else 0 end) 
as AUFBTS_Total_Viewing_EventsNon_LIVE
,sum(case when live=0 and analysis_right ='Australian Open Tennis - BBC' then number_of_events_viewed else 0 end) 
as AOTBBC_Total_Viewing_EventsNon_LIVE
,sum(case when live=0 and analysis_right ='Australian Open Tennis - Eurosport' then number_of_events_viewed else 0 end) 
as AOTEUR_Total_Viewing_EventsNon_LIVE
,sum(case when live=0 and analysis_right ='Aviva Premiership - ESPN' then number_of_events_viewed else 0 end) 
as AVPSS_Total_Viewing_EventsNon_LIVE
,sum(case when live=0 and analysis_right ='BBC American Football' then number_of_events_viewed else 0 end) 
as AFBBC_Total_Viewing_EventsNon_LIVE
,sum(case when live=0 and analysis_right ='BBC Athletics' then number_of_events_viewed else 0 end) 
as ATHBBC_Total_Viewing_EventsNon_LIVE
,sum(case when live=0 and analysis_right ='BBC Boxing' then number_of_events_viewed else 0 end) 
as BOXBBC_Total_Viewing_EventsNon_LIVE
,sum(case when live=0 and analysis_right ='BBC Darts' then number_of_events_viewed else 0 end) 
as DRTBBC_Total_Viewing_EventsNon_LIVE
,sum(case when live=0 and analysis_right ='BBC Equestrian' then number_of_events_viewed else 0 end) 
as EQUBBC_Total_Viewing_EventsNon_LIVE
,sum(case when live=0 and analysis_right ='BBC Football' then number_of_events_viewed else 0 end) 
as FOOTBBC_Total_Viewing_EventsNon_LIVE
,sum(case when live=0 and analysis_right ='BBC Golf' then number_of_events_viewed else 0 end) 
as GOLFBBC_Total_Viewing_EventsNon_LIVE
,sum(case when live=0 and analysis_right ='BBC Motor Sport' then number_of_events_viewed else 0 end) 
as MSPBBC_Total_Viewing_EventsNon_LIVE
,sum(case when live=0 and analysis_right ='BBC Rugby' then number_of_events_viewed else 0 end) 
as RUGBBC_Total_Viewing_EventsNon_LIVE
,sum(case when live=0 and analysis_right ='BBC Snooker/Pool' then number_of_events_viewed else 0 end) 
as SNPBBC_Total_Viewing_EventsNon_LIVE
,sum(case when live=0 and analysis_right ='BBC Tennis' then number_of_events_viewed else 0 end) 
as TENBBC_Total_Viewing_EventsNon_LIVE
,sum(case when live=0 and analysis_right ='BBC Unknown' then number_of_events_viewed else 0 end) 
as UNKBBC_Total_Viewing_EventsNon_LIVE
,sum(case when live=0 and analysis_right ='BBC Watersports' then number_of_events_viewed else 0 end) 
as WATBBC_Total_Viewing_EventsNon_LIVE
,sum(case when live=0 and analysis_right ='BBC Wintersports' then number_of_events_viewed else 0 end) 
as WINBBC_Total_Viewing_EventsNon_LIVE
,sum(case when live=0 and analysis_right ='Boxing  - Channel 5' then number_of_events_viewed else 0 end) 
as BOXCH5_Total_Viewing_EventsNon_LIVE
,sum(case when live=0 and analysis_right ='Boxing - Matchroom 2012-2014 Sky Sports' then number_of_events_viewed else 0 end) 
as BOXMSS_Total_Viewing_EventsNon_LIVE
,sum(case when live=0 and analysis_right ='Brazil Football - BT Sport' then number_of_events_viewed else 0 end) 
as BFTBTS_Total_Viewing_EventsNon_LIVE
,sum(case when live=0 and analysis_right ='British & Irish Lions 2013 - Sky Sports' then number_of_events_viewed else 0 end) 
as BILSS_Total_Viewing_EventsNon_LIVE
,sum(case when live=0 and analysis_right ='British Open Golf - BBC' then number_of_events_viewed else 0 end) 
as BOGSS_Total_Viewing_EventsNon_LIVE
,sum(case when live=0 and analysis_right ='BT Sport American Football' then number_of_events_viewed else 0 end) 
as AFBTS_Total_Viewing_EventsNon_LIVE
,sum(case when live=0 and analysis_right ='BT Sport Athletics' then number_of_events_viewed else 0 end) 
as ATHBTS_Total_Viewing_EventsNon_LIVE
,sum(case when live=0 and analysis_right ='BT Sport Baseball' then number_of_events_viewed else 0 end) 
as BASEBTS_Total_Viewing_EventsNon_LIVE
,sum(case when live=0 and analysis_right ='BT Sport Basketball' then number_of_events_viewed else 0 end) 
as BASKBTS_Total_Viewing_EventsNon_LIVE
,sum(case when live=0 and analysis_right ='BT Sport Boxing' then number_of_events_viewed else 0 end) 
as BOXBTS_Total_Viewing_EventsNon_LIVE
,sum(case when live=0 and analysis_right ='BT Sport Cricket' then number_of_events_viewed else 0 end) 
as CRIBTS_Total_Viewing_EventsNon_LIVE
,sum(case when live=0 and analysis_right ='BT Sport Equestrian' then number_of_events_viewed else 0 end) 
as EQUBTS_Total_Viewing_EventsNon_LIVE
,sum(case when live=0 and analysis_right ='BT Sport Extreme' then number_of_events_viewed else 0 end) 
as EXTBTS_Total_Viewing_EventsNon_LIVE
,sum(case when live=0 and analysis_right ='BT Sport Football' then number_of_events_viewed else 0 end) 
as FOOTBTS_Total_Viewing_EventsNon_LIVE
,sum(case when live=0 and analysis_right ='BT Sport Motor Sport' then number_of_events_viewed else 0 end) 
as MSPBTS_Total_Viewing_EventsNon_LIVE
,sum(case when live=0 and analysis_right ='BT Sport Rugby' then number_of_events_viewed else 0 end) 
as RUGBTS_Total_Viewing_EventsNon_LIVE
,sum(case when live=0 and analysis_right ='BT Sport Tennis' then number_of_events_viewed else 0 end) 
as TENBTS_Total_Viewing_EventsNon_LIVE
,sum(case when live=0 and analysis_right ='BT Sport Unknown' then number_of_events_viewed else 0 end) 
as UNKBTS_Total_Viewing_EventsNon_LIVE
,sum(case when live=0 and analysis_right ='BT Sport Wintersports' then number_of_events_viewed else 0 end) 
as WINBTS_Total_Viewing_EventsNon_LIVE
,sum(case when live=0 and analysis_right ='Bundesliga - BT Sport' then number_of_events_viewed else 0 end) 
as BUNBTS_Total_Viewing_EventsNon_LIVE
,sum(case when live=0 and analysis_right ='Bundesliga- ESPN' then number_of_events_viewed else 0 end) 
as BUNESPN_Total_Viewing_EventsNon_LIVE
,sum(case when live=0 and analysis_right ='Challenge Darts' then number_of_events_viewed else 0 end) 
as DRTCHA_Total_Viewing_EventsNon_LIVE
,sum(case when live=0 and analysis_right ='Challenge Extreme' then number_of_events_viewed else 0 end) 
as EXTCHA_Total_Viewing_EventsNon_LIVE
,sum(case when live=0 and analysis_right ='Challenge Unknown' then number_of_events_viewed else 0 end) 
as UNKCHA_Total_Viewing_EventsNon_LIVE
,sum(case when live=0 and analysis_right ='Challenge Wrestling' then number_of_events_viewed else 0 end) 
as WRECHA_Total_Viewing_EventsNon_LIVE
,sum(case when live=0 and analysis_right ='Champions League - ITV' then number_of_events_viewed else 0 end) 
as CHLITV_Total_Viewing_EventsNon_LIVE
,sum(case when live=0 and analysis_right ='Champions Trophy - I C C Cricket Deal  2012-2015 Sky Sports' then number_of_events_viewed else 0 end) 
as ICCSS_Total_Viewing_EventsNon_LIVE
,sum(case when live=0 and analysis_right ='Channel 4 American Football' then number_of_events_viewed else 0 end) 
as AMCH4_Total_Viewing_EventsNon_LIVE
,sum(case when live=0 and analysis_right ='Channel 4 Athletics' then number_of_events_viewed else 0 end) 
as ATHCH4_Total_Viewing_EventsNon_LIVE
,sum(case when live=0 and analysis_right ='Channel 4 Boxing' then number_of_events_viewed else 0 end) 
as BOXCH4_Total_Viewing_EventsNon_LIVE
,sum(case when live=0 and analysis_right ='Channel 4 Equestrian' then number_of_events_viewed else 0 end) 
as EQUCH4_Total_Viewing_EventsNon_LIVE
,sum(case when live=0 and analysis_right ='Channel 4 Extreme' then number_of_events_viewed else 0 end) 
as EXTCH4_Total_Viewing_EventsNon_LIVE
,sum(case when live=0 and analysis_right ='Channel 4 Football' then number_of_events_viewed else 0 end) 
as FOOTCH4_Total_Viewing_EventsNon_LIVE
,sum(case when live=0 and analysis_right ='Channel 4 Motor Sport' then number_of_events_viewed else 0 end) 
as MSPCH4_Total_Viewing_EventsNon_LIVE
,sum(case when live=0 and analysis_right ='Channel 4 Racing' then number_of_events_viewed else 0 end) 
as RACCH4_Total_Viewing_EventsNon_LIVE
,sum(case when live=0 and analysis_right ='Channel 4 Unknown' then number_of_events_viewed else 0 end) 
as UNKCH4_Total_Viewing_EventsNon_LIVE
,sum(case when live=0 and analysis_right ='Channel 4 Watersports' then number_of_events_viewed else 0 end) 
as WATCH4_Total_Viewing_EventsNon_LIVE
,sum(case when live=0 and analysis_right ='Channel 4 Wintersports' then number_of_events_viewed else 0 end) 
as WINCH4_Total_Viewing_EventsNon_LIVE
,sum(case when live=0 and analysis_right ='Channel 5 Athletics' then number_of_events_viewed else 0 end) 
as ATHCH5_Total_Viewing_EventsNon_LIVE
,sum(case when live=0 and analysis_right ='Channel 5 Boxing' then number_of_events_viewed else 0 end) 
as BOXOCH5_Total_Viewing_EventsNon_LIVE
,sum(case when live=0 and analysis_right ='Channel 5 Cricket' then number_of_events_viewed else 0 end) 
as CRICH5_Total_Viewing_EventsNon_LIVE
,sum(case when live=0 and analysis_right ='Channel 5 Motor Sport' then number_of_events_viewed else 0 end) 
as MSPCH5_Total_Viewing_EventsNon_LIVE
,sum(case when live=0 and analysis_right ='Channel 5 Unknown' then number_of_events_viewed else 0 end) 
as UNKCH5_Total_Viewing_EventsNon_LIVE
,sum(case when live=0 and analysis_right ='Channel 5 Wrestling' then number_of_events_viewed else 0 end) 
as WRECH5_Total_Viewing_EventsNon_LIVE
,sum(case when live=0 and analysis_right ='Cheltenham Festival - Channel 4' then number_of_events_viewed else 0 end) 
as CHELCH4_Total_Viewing_EventsNon_LIVE
,sum(case when live=0 and analysis_right ='Community Shield - ITV' then number_of_events_viewed else 0 end) 
as CMSITV_Total_Viewing_EventsNon_LIVE
,sum(case when live=0 and analysis_right ='Confederations Cup - BBC' then number_of_events_viewed else 0 end) 
as CONCBBC_Total_Viewing_EventsNon_LIVE
,sum(case when live=0 and analysis_right ='Conference - BT Sport' then number_of_events_viewed else 0 end) 
as CONFBTS_Total_Viewing_EventsNon_LIVE
,sum(case when live=0 and analysis_right ='Cycling - La Vuelta ITV' then number_of_events_viewed else 0 end) 
as CLVITV_Total_Viewing_EventsNon_LIVE
,sum(case when live=0 and analysis_right ='Cycling - U C I World Tour Sky Sports' then number_of_events_viewed else 0 end) 
as CUCISS_Total_Viewing_EventsNon_LIVE
,sum(case when live=0 and analysis_right ='Cycling Tour of Britain - Eurosport' then number_of_events_viewed else 0 end) 
as CTBEUR_Total_Viewing_EventsNon_LIVE
,sum(case when live=0 and analysis_right ='Cycling: tour of britain ITV4' then number_of_events_viewed else 0 end) 
as CTCITV_Total_Viewing_EventsNon_LIVE
,sum(case when live=0 and analysis_right ='Derby - Channel 4' then number_of_events_viewed else 0 end) 
as DERCH4_Total_Viewing_EventsNon_LIVE
,sum(case when live=0 and analysis_right ='ECB (highlights) - Channel 5' then number_of_events_viewed else 0 end) 
as ECBHCH5_Total_Viewing_EventsNon_LIVE
,sum(case when live=0 and analysis_right ='ECB Cricket Sky Sports' then number_of_events_viewed else 0 end) 
as GECRSS_Total_Viewing_EventsNon_LIVE
,sum(case when live=0 and analysis_right ='ECB non-Test Cricket Sky Sports' then number_of_events_viewed else 0 end) 
as ECBNSS_Total_Viewing_EventsNon_LIVE
,sum(case when live=0 and analysis_right ='ECB Test Cricket Sky Sports' then number_of_events_viewed else 0 end) 
as ECBTSS_Total_Viewing_EventsNon_LIVE
,sum(case when live=0 and analysis_right ='England Football Internationals - ITV' then number_of_events_viewed else 0 end) 
as GENGITV_Total_Viewing_EventsNon_LIVE
,sum(case when live=0 and analysis_right ='England Friendlies (Football) - ITV' then number_of_events_viewed else 0 end) 
as EFRITV_Total_Viewing_EventsNon_LIVE
,sum(case when live=0 and analysis_right ='England Rugby Internationals 2010-2015 - Sky Sports' then number_of_events_viewed else 0 end) 
as ENRSS_Total_Viewing_EventsNon_LIVE
,sum(case when live=0 and analysis_right ='England World Cup Qualifying (Away) - ITV' then number_of_events_viewed else 0 end) 
as EWQAITV_Total_Viewing_EventsNon_LIVE
,sum(case when live=0 and analysis_right ='England World Cup Qualifying (Home) - ITV' then number_of_events_viewed else 0 end) 
as EWQHITV_Total_Viewing_EventsNon_LIVE
,sum(case when live=0 and analysis_right ='ESPN American Football' then number_of_events_viewed else 0 end) 
as AMESPN_Total_Viewing_EventsNon_LIVE
,sum(case when live=0 and analysis_right ='ESPN Athletics' then number_of_events_viewed else 0 end) 
as ATHESPN_Total_Viewing_EventsNon_LIVE
,sum(case when live=0 and analysis_right ='ESPN Baseball' then number_of_events_viewed else 0 end) 
as BASEESPN_Total_Viewing_EventsNon_LIVE
,sum(case when live=0 and analysis_right ='ESPN Basketball' then number_of_events_viewed else 0 end) 
as BASKESPN_Total_Viewing_EventsNon_LIVE
,sum(case when live=0 and analysis_right ='ESPN Boxing' then number_of_events_viewed else 0 end) 
as BOXESPN_Total_Viewing_EventsNon_LIVE
,sum(case when live=0 and analysis_right ='ESPN Cricket' then number_of_events_viewed else 0 end) 
as CRIESPN_Total_Viewing_EventsNon_LIVE
,sum(case when live=0 and analysis_right ='ESPN Darts' then number_of_events_viewed else 0 end) 
as DARTESPN_Total_Viewing_EventsNon_LIVE
,sum(case when live=0 and analysis_right ='ESPN Equestrian' then number_of_events_viewed else 0 end) 
as EQUESPN_Total_Viewing_EventsNon_LIVE
,sum(case when live=0 and analysis_right ='ESPN Extreme' then number_of_events_viewed else 0 end) 
as EXTESPN_Total_Viewing_EventsNon_LIVE
,sum(case when live=0 and analysis_right ='ESPN Football' then number_of_events_viewed else 0 end) 
as FOOTESPN_Total_Viewing_EventsNon_LIVE
,sum(case when live=0 and analysis_right ='ESPN Golf' then number_of_events_viewed else 0 end) 
as GOLFESPN_Total_Viewing_EventsNon_LIVE
,sum(case when live=0 and analysis_right ='ESPN Ice Hockey' then number_of_events_viewed else 0 end) 
as IHESPN_Total_Viewing_EventsNon_LIVE
,sum(case when live=0 and analysis_right ='ESPN Motor Sport' then number_of_events_viewed else 0 end) 
as MSPESPN_Total_Viewing_EventsNon_LIVE
,sum(case when live=0 and analysis_right ='ESPN Racing' then number_of_events_viewed else 0 end) 
as RACESPN_Total_Viewing_EventsNon_LIVE
,sum(case when live=0 and analysis_right ='ESPN Rugby' then number_of_events_viewed else 0 end) 
as RUGESPN_Total_Viewing_EventsNon_LIVE
,sum(case when live=0 and analysis_right ='ESPN Snooker/Pool' then number_of_events_viewed else 0 end) 
as SNPESPN_Total_Viewing_EventsNon_LIVE
,sum(case when live=0 and analysis_right ='ESPN Tennis' then number_of_events_viewed else 0 end) 
as TENESPN_Total_Viewing_EventsNon_LIVE
,sum(case when live=0 and analysis_right ='ESPN Unknown' then number_of_events_viewed else 0 end) 
as UNKESPN_Total_Viewing_EventsNon_LIVE
,sum(case when live=0 and analysis_right ='ESPN Watersports' then number_of_events_viewed else 0 end) 
as WATESPN_Total_Viewing_EventsNon_LIVE
,sum(case when live=0 and analysis_right ='ESPN Wintersports' then number_of_events_viewed else 0 end) 
as WINESPN_Total_Viewing_EventsNon_LIVE
,sum(case when live=0 and analysis_right ='ESPN Wrestling' then number_of_events_viewed else 0 end) 
as WREESPN_Total_Viewing_EventsNon_LIVE
,sum(case when live=0 and analysis_right ='Europa League - BT Sport' then number_of_events_viewed else 0 end) 
as ELBTSP_Total_Viewing_EventsNon_LIVE
,sum(case when live=0 and analysis_right ='Europa League - ESPN' then number_of_events_viewed else 0 end) 
as ELESPN_Total_Viewing_EventsNon_LIVE
,sum(case when live=0 and analysis_right ='Europa League - ITV' then number_of_events_viewed else 0 end) 
as ELITV_Total_Viewing_EventsNon_LIVE
,sum(case when live=0 and analysis_right ='European Tour Golf - Sky Sports' then number_of_events_viewed else 0 end) 
as ETGSS_Total_Viewing_EventsNon_LIVE
,sum(case when live=0 and analysis_right ='Eurosport American Football' then number_of_events_viewed else 0 end) 
as AMEUR_Total_Viewing_EventsNon_LIVE
,sum(case when live=0 and analysis_right ='Eurosport Athletics' then number_of_events_viewed else 0 end) 
as ATHEUR_Total_Viewing_EventsNon_LIVE
,sum(case when live=0 and analysis_right ='Eurosport Baseball' then number_of_events_viewed else 0 end) 
as BASEEUR_Total_Viewing_EventsNon_LIVE
,sum(case when live=0 and analysis_right ='Eurosport Basketball' then number_of_events_viewed else 0 end) 
as BASKEUR_Total_Viewing_EventsNon_LIVE
,sum(case when live=0 and analysis_right ='Eurosport Boxing' then number_of_events_viewed else 0 end) 
as BOXEUR_Total_Viewing_EventsNon_LIVE
,sum(case when live=0 and analysis_right ='Eurosport Cricket' then number_of_events_viewed else 0 end) 
as CRIEUR_Total_Viewing_EventsNon_LIVE
,sum(case when live=0 and analysis_right ='Eurosport Darts' then number_of_events_viewed else 0 end) 
as DARTEUR_Total_Viewing_EventsNon_LIVE
,sum(case when live=0 and analysis_right ='Eurosport Equestrian' then number_of_events_viewed else 0 end) 
as EQUEUR_Total_Viewing_EventsNon_LIVE
,sum(case when live=0 and analysis_right ='Eurosport Extreme' then number_of_events_viewed else 0 end) 
as EXTEUR_Total_Viewing_EventsNon_LIVE
,sum(case when live=0 and analysis_right ='Eurosport Football' then number_of_events_viewed else 0 end) 
as FOOTEUR_Total_Viewing_EventsNon_LIVE
,sum(case when live=0 and analysis_right ='Eurosport Golf' then number_of_events_viewed else 0 end) 
as GOLFEUR_Total_Viewing_EventsNon_LIVE
,sum(case when live=0 and analysis_right ='Eurosport Ice Hockey' then number_of_events_viewed else 0 end) 
as IHEUR_Total_Viewing_EventsNon_LIVE
,sum(case when live=0 and analysis_right ='Eurosport Motor Sport' then number_of_events_viewed else 0 end) 
as MSPEUR_Total_Viewing_EventsNon_LIVE
,sum(case when live=0 and analysis_right ='Eurosport Racing' then number_of_events_viewed else 0 end) 
as RACEUR_Total_Viewing_EventsNon_LIVE
,sum(case when live=0 and analysis_right ='Eurosport Snooker/Pool' then number_of_events_viewed else 0 end) 
as SNPEUR_Total_Viewing_EventsNon_LIVE
,sum(case when live=0 and analysis_right ='Eurosport Tennis' then number_of_events_viewed else 0 end) 
as TENEUR_Total_Viewing_EventsNon_LIVE
,sum(case when live=0 and analysis_right ='Eurosport Unknown' then number_of_events_viewed else 0 end) 
as UNKEUR_Total_Viewing_EventsNon_LIVE
,sum(case when live=0 and analysis_right ='Eurosport Watersports' then number_of_events_viewed else 0 end) 
as WATEUR_Total_Viewing_EventsNon_LIVE
,sum(case when live=0 and analysis_right ='Eurosport Wintersports' then number_of_events_viewed else 0 end) 
as WINEUR_Total_Viewing_EventsNon_LIVE
,sum(case when live=0 and analysis_right ='F1 - BBC' then number_of_events_viewed else 0 end) 
as GF1BBC_Total_Viewing_EventsNon_LIVE
,sum(case when live=0 and analysis_right ='F1 - Sky Sports' then number_of_events_viewed else 0 end) 
as GF1SS_Total_Viewing_EventsNon_LIVE
,sum(case when live=0 and analysis_right ='F1 (non-Live)- BBC' then number_of_events_viewed else 0 end) 
as F1NBBC_Total_Viewing_EventsNon_LIVE
,sum(case when live=0 and analysis_right ='F1 (Practice Live)- BBC' then number_of_events_viewed else 0 end) 
as F1PBBC_Total_Viewing_EventsNon_LIVE
,sum(case when live=0 and analysis_right ='F1 (Qualifying Live)- BBC' then number_of_events_viewed else 0 end) 
as F1QBBC_Total_Viewing_EventsNon_LIVE
,sum(case when live=0 and analysis_right ='F1 (Race Live)- BBC' then number_of_events_viewed else 0 end) 
as F1RBBC_Total_Viewing_EventsNon_LIVE
,sum(case when live=0 and analysis_right ='FA Cup - ESPN' then number_of_events_viewed else 0 end) 
as FACESPN_Total_Viewing_EventsNon_LIVE
,sum(case when live=0 and analysis_right ='FA Cup - ITV' then number_of_events_viewed else 0 end) 
as FACITV_Total_Viewing_EventsNon_LIVE
,sum(case when live=0 and analysis_right ='Football League - Seasons 2012/13 To 14/15 Sky Sports - Capital 1 Cup' then number_of_events_viewed else 0 end) 
as FLCCSS_Total_Viewing_EventsNon_LIVE
,sum(case when live=0 and analysis_right ='Football League - Seasons 2012/13 To 14/15 Sky Sports - others' then number_of_events_viewed else 0 end) 
as FLOTSS_Total_Viewing_EventsNon_LIVE
,sum(case when live=0 and analysis_right ='Formula One 2012-2018 - (non-Live) Sky Sports' then number_of_events_viewed else 0 end) 
as F1NSS_Total_Viewing_EventsNon_LIVE
,sum(case when live=0 and analysis_right ='Formula One 2012-2018 - (Practice Live) Sky Sports' then number_of_events_viewed else 0 end) 
as F1PSS_Total_Viewing_EventsNon_LIVE
,sum(case when live=0 and analysis_right ='Formula One 2012-2018 - (Qualifying Live) Sky Sports' then number_of_events_viewed else 0 end) 
as F1QSS_Total_Viewing_EventsNon_LIVE
,sum(case when live=0 and analysis_right ='Formula One 2012-2018 - (Race Live) Sky Sports' then number_of_events_viewed else 0 end) 
as F1RSS_Total_Viewing_EventsNon_LIVE
,sum(case when live=0 and analysis_right ='French Open Tennis - Eurosport' then number_of_events_viewed else 0 end) 
as FOTEUR_Total_Viewing_EventsNon_LIVE
,sum(case when live=0 and analysis_right ='French Open Tennis - ITV' then number_of_events_viewed else 0 end) 
as FOTITV_Total_Viewing_EventsNon_LIVE
,sum(case when live=0 and analysis_right ='Grand National - Channel 4' then number_of_events_viewed else 0 end) 
as GDNCH4_Total_Viewing_EventsNon_LIVE
,sum(case when live=0 and analysis_right ='Heineken Cup 2010/11 To 2013/14- Sky Sports' then number_of_events_viewed else 0 end) 
as HECSS_Total_Viewing_EventsNon_LIVE
,sum(case when live=0 and analysis_right ='I R B Sevens 2008/09 -2014/15 - Sky Sports' then number_of_events_viewed else 0 end) 
as IRBSS_Total_Viewing_EventsNon_LIVE
,sum(case when live=0 and analysis_right ='IAAF World Athletics Championship - Eurosport' then number_of_events_viewed else 0 end) 
as WACEUR_Total_Viewing_EventsNon_LIVE
,sum(case when live=0 and analysis_right ='India Home Cricket 2012-2018 Sky Sports' then number_of_events_viewed else 0 end) 
as IHCSS_Total_Viewing_EventsNon_LIVE
,sum(case when live=0 and analysis_right ='India Premier League - ITV' then number_of_events_viewed else 0 end) 
as IPLITV_Total_Viewing_EventsNon_LIVE
,sum(case when live=0 and analysis_right ='International Freindlies - ESPN' then number_of_events_viewed else 0 end) 
as IFESPN_Total_Viewing_EventsNon_LIVE
,sum(case when live=0 and analysis_right ='International Friendlies - BT Sport' then number_of_events_viewed else 0 end) 
as IFBTS_Total_Viewing_EventsNon_LIVE
,sum(case when live=0 and analysis_right ='ITV1 Boxing' then number_of_events_viewed else 0 end) 
as BOXITV1_Total_Viewing_EventsNon_LIVE
,sum(case when live=0 and analysis_right ='ITV1 Football' then number_of_events_viewed else 0 end) 
as FOOTITV1_Total_Viewing_EventsNon_LIVE
,sum(case when live=0 and analysis_right ='ITV1 Motor Sport' then number_of_events_viewed else 0 end) 
as MOTSITV1_Total_Viewing_EventsNon_LIVE
,sum(case when live=0 and analysis_right ='ITV1 Rugby' then number_of_events_viewed else 0 end) 
as RUGITV1_Total_Viewing_EventsNon_LIVE
,sum(case when live=0 and analysis_right ='ITV1 Snooker/Pool' then number_of_events_viewed else 0 end) 
as SNPITV1_Total_Viewing_EventsNon_LIVE
,sum(case when live=0 and analysis_right ='ITV1 Unknown' then number_of_events_viewed else 0 end) 
as UNKITV1_Total_Viewing_EventsNon_LIVE
,sum(case when live=0 and analysis_right ='ITV4 Boxing' then number_of_events_viewed else 0 end) 
as BOXITV4_Total_Viewing_EventsNon_LIVE
,sum(case when live=0 and analysis_right ='ITV4 Cricket' then number_of_events_viewed else 0 end) 
as CRIITV4_Total_Viewing_EventsNon_LIVE
,sum(case when live=0 and analysis_right ='ITV4 Darts' then number_of_events_viewed else 0 end) 
as DARTITV4_Total_Viewing_EventsNon_LIVE
,sum(case when live=0 and analysis_right ='ITV4 Extreme' then number_of_events_viewed else 0 end) 
as EXTITV4_Total_Viewing_EventsNon_LIVE
,sum(case when live=0 and analysis_right ='ITV4 Football' then number_of_events_viewed else 0 end) 
as FOOTITV4_Total_Viewing_EventsNon_LIVE
,sum(case when live=0 and analysis_right ='ITV4 Motor Sport' then number_of_events_viewed else 0 end) 
as MSPITV4_Total_Viewing_EventsNon_LIVE
,sum(case when live=0 and analysis_right ='ITV4 Rugby' then number_of_events_viewed else 0 end) 
as RUGITV4_Total_Viewing_EventsNon_LIVE
,sum(case when live=0 and analysis_right ='ITV4 Snooker/Pool' then number_of_events_viewed else 0 end) 
as SNPITV4_Total_Viewing_EventsNon_LIVE
,sum(case when live=0 and analysis_right ='ITV4 Tennis' then number_of_events_viewed else 0 end) 
as TENITV4_Total_Viewing_EventsNon_LIVE
,sum(case when live=0 and analysis_right ='ITV4 Unknown' then number_of_events_viewed else 0 end) 
as UNKITV4_Total_Viewing_EventsNon_LIVE
,sum(case when live=0 and analysis_right ='Ligue 1 - BT Sport' then number_of_events_viewed else 0 end) 
as L1BTS_Total_Viewing_EventsNon_LIVE
,sum(case when live=0 and analysis_right ='Ligue 1 - ESPN' then number_of_events_viewed else 0 end) 
as L1ESPN_Total_Viewing_EventsNon_LIVE
,sum(case when live=0 and analysis_right ='Match of the day - BBC' then number_of_events_viewed else 0 end) 
as MOTDBBC_Total_Viewing_EventsNon_LIVE
,sum(case when live=0 and analysis_right ='Matchroom Output Deal  2010-2012 (Other)- Sky Sports' then number_of_events_viewed else 0 end) 
as MROSS_Total_Viewing_EventsNon_LIVE
,sum(case when live=0 and analysis_right ='Matchroom Output Deal  2010-2012 (Pool)- Sky Sports' then number_of_events_viewed else 0 end) 
as MRPSS_Total_Viewing_EventsNon_LIVE
,sum(case when live=0 and analysis_right ='Matchroom Output Deal  2010-2012 (Snooker)- Sky Sports' then number_of_events_viewed else 0 end) 
as MRSSS_Total_Viewing_EventsNon_LIVE
,sum(case when live=0 and analysis_right ='Moto GP BBC' then number_of_events_viewed else 0 end) 
as MGPBBC_Total_Viewing_EventsNon_LIVE
,sum(case when live=0 and analysis_right ='NBA - Sky Sports' then number_of_events_viewed else 0 end) 
as NBASS_Total_Viewing_EventsNon_LIVE
,sum(case when live=0 and analysis_right ='NFL - BBC' then number_of_events_viewed else 0 end) 
as NFLBBC_Total_Viewing_EventsNon_LIVE
,sum(case when live=0 and analysis_right ='NFL - Channel 4' then number_of_events_viewed else 0 end) 
as NFLCH4_Total_Viewing_EventsNon_LIVE
,sum(case when live=0 and analysis_right ='NFL Seasons 2012-2015 - Sky Sports' then number_of_events_viewed else 0 end) 
as NFLSS_Total_Viewing_EventsNon_LIVE
,sum(case when live=0 and analysis_right ='Northern Ireland Football Association 2012-2014 - Sky Sports' then number_of_events_viewed else 0 end) 
as NIFSS_Total_Viewing_EventsNon_LIVE
,sum(case when live=0 and analysis_right ='Oaks - Channel 4' then number_of_events_viewed else 0 end) 
as OAKCH4_Total_Viewing_EventsNon_LIVE
,sum(case when live=0 and analysis_right ='Other American Football' then number_of_events_viewed else 0 end) 
as AMOTH_Total_Viewing_EventsNon_LIVE
,sum(case when live=0 and analysis_right ='Other Athletics' then number_of_events_viewed else 0 end) 
as ATHOTH_Total_Viewing_EventsNon_LIVE
,sum(case when live=0 and analysis_right ='Other Baseball' then number_of_events_viewed else 0 end) 
as BASEOTH_Total_Viewing_EventsNon_LIVE
,sum(case when live=0 and analysis_right ='Other Basketball' then number_of_events_viewed else 0 end) 
as BASKOTH_Total_Viewing_EventsNon_LIVE
,sum(case when live=0 and analysis_right ='Other Boxing' then number_of_events_viewed else 0 end) 
as BOXOTH_Total_Viewing_EventsNon_LIVE
,sum(case when live=0 and analysis_right ='Other Cricket' then number_of_events_viewed else 0 end) 
as CRIOTH_Total_Viewing_EventsNon_LIVE
,sum(case when live=0 and analysis_right ='Other Darts' then number_of_events_viewed else 0 end) 
as DARTOTH_Total_Viewing_EventsNon_LIVE
,sum(case when live=0 and analysis_right ='Other Equestrian' then number_of_events_viewed else 0 end) 
as EQUOTH_Total_Viewing_EventsNon_LIVE
,sum(case when live=0 and analysis_right ='Other Extreme' then number_of_events_viewed else 0 end) 
as EXTOTH_Total_Viewing_EventsNon_LIVE
,sum(case when live=0 and analysis_right ='Other Fishing' then number_of_events_viewed else 0 end) 
as FSHOTH_Total_Viewing_EventsNon_LIVE
,sum(case when live=0 and analysis_right ='Other Football' then number_of_events_viewed else 0 end) 
as FOOTOTH_Total_Viewing_EventsNon_LIVE
,sum(case when live=0 and analysis_right ='Other Golf' then number_of_events_viewed else 0 end) 
as GOLFOTH_Total_Viewing_EventsNon_LIVE
,sum(case when live=0 and analysis_right ='Other Ice Hockey' then number_of_events_viewed else 0 end) 
as IHOTH_Total_Viewing_EventsNon_LIVE
,sum(case when live=0 and analysis_right ='Other Motor Sport' then number_of_events_viewed else 0 end) 
as MSPOTH_Total_Viewing_EventsNon_LIVE
,sum(case when live=0 and analysis_right ='Other Racing' then number_of_events_viewed else 0 end) 
as RACOTH_Total_Viewing_EventsNon_LIVE
,sum(case when live=0 and analysis_right ='Other Rugby' then number_of_events_viewed else 0 end) 
as RUGOTH_Total_Viewing_EventsNon_LIVE
,sum(case when live=0 and analysis_right ='Other Rugby Internationals - ESPN' then number_of_events_viewed else 0 end) 
as ORUGESPN_Total_Viewing_EventsNon_LIVE
,sum(case when live=0 and analysis_right ='Other Snooker/Pool' then number_of_events_viewed else 0 end) 
as OTHSNP_Total_Viewing_EventsNon_LIVE
,sum(case when live=0 and analysis_right ='Other Tennis' then number_of_events_viewed else 0 end) 
as OTHTEN_Total_Viewing_EventsNon_LIVE
,sum(case when live=0 and analysis_right ='Other Unknown' then number_of_events_viewed else 0 end) 
as OTHUNK_Total_Viewing_EventsNon_LIVE
,sum(case when live=0 and analysis_right ='Other Watersports' then number_of_events_viewed else 0 end) 
as OTHWAT_Total_Viewing_EventsNon_LIVE
,sum(case when live=0 and analysis_right ='Other Wintersports' then number_of_events_viewed else 0 end) 
as OTHWIN_Total_Viewing_EventsNon_LIVE
,sum(case when live=0 and analysis_right ='Other Wrestling' then number_of_events_viewed else 0 end) 
as OTHWRE_Total_Viewing_EventsNon_LIVE
,sum(case when live=0 and analysis_right ='PGA Tour Golf 2010-2017 - Sky Sports' then number_of_events_viewed else 0 end) 
as PGASS_Total_Viewing_EventsNon_LIVE
,sum(case when live=0 and analysis_right ='Premier League - BT Sport' then number_of_events_viewed else 0 end) 
as PLBTS_Total_Viewing_EventsNon_LIVE
,sum(case when live=0 and analysis_right ='Premier League - ESPN' then number_of_events_viewed else 0 end) 
as PLESPN_Total_Viewing_EventsNon_LIVE
,sum(case when live=0 and analysis_right ='Premier League Darts 2010-2013 - Sky Sports' then number_of_events_viewed else 0 end) 
as PLDSS_Total_Viewing_EventsNon_LIVE
,sum(case when live=0 and analysis_right ='Premier League Football - Sky Sports' then number_of_events_viewed else 0 end) 
as GPLSS_Total_Viewing_EventsNon_LIVE
,sum(case when live=0 and analysis_right ='Premier League Football - Sky Sports (Match Choice)' then number_of_events_viewed else 0 end) 
as PLMCSS_Total_Viewing_EventsNon_LIVE
,sum(case when live=0 and analysis_right ='Premier League Football - Sky Sports (MNF)' then number_of_events_viewed else 0 end) 
as PLMNFSS_Total_Viewing_EventsNon_LIVE
,sum(case when live=0 and analysis_right ='Premier League Football - Sky Sports (non Live)' then number_of_events_viewed else 0 end) 
as PLNLSS_Total_Viewing_EventsNon_LIVE
,sum(case when live=0 and analysis_right ='Premier League Football - Sky Sports (other Live)' then number_of_events_viewed else 0 end) 
as PLOLSS_Total_Viewing_EventsNon_LIVE
,sum(case when live=0 and analysis_right ='Premier League Football - Sky Sports (Sat Lunchtime)' then number_of_events_viewed else 0 end) 
as PLSLSS_Total_Viewing_EventsNon_LIVE
,sum(case when live=0 and analysis_right ='Premier League Football - Sky Sports (Sat Night Live)' then number_of_events_viewed else 0 end) 
as PLSNSS_Total_Viewing_EventsNon_LIVE
,sum(case when live=0 and analysis_right ='Premier League Football - Sky Sports (Sun 4pm)' then number_of_events_viewed else 0 end) 
as PLS4SS_Total_Viewing_EventsNon_LIVE
,sum(case when live=0 and analysis_right ='Premier League Football - Sky Sports (Sun Lunchtime)' then number_of_events_viewed else 0 end) 
as PLSULSS_Total_Viewing_EventsNon_LIVE
,sum(case when live=0 and analysis_right ='Premiership Rugby - Sky Sports' then number_of_events_viewed else 0 end) 
as PRUSS_Total_Viewing_EventsNon_LIVE
,sum(case when live=0 and analysis_right ='Republic Of Ireland FA - 2010/11 To 2013/14 - Sky Sports' then number_of_events_viewed else 0 end) 
as ROISS_Total_Viewing_EventsNon_LIVE
,sum(case when live=0 and analysis_right ='Royal Ascot - Channel 4' then number_of_events_viewed else 0 end) 
as RASCH4_Total_Viewing_EventsNon_LIVE
,sum(case when live=0 and analysis_right ='Rugby Internationals (England) - BBC' then number_of_events_viewed else 0 end) 
as RIEBBC_Total_Viewing_EventsNon_LIVE
,sum(case when live=0 and analysis_right ='Rugby Internationals (Ireland) - BBC' then number_of_events_viewed else 0 end) 
as RIIBBC_Total_Viewing_EventsNon_LIVE
,sum(case when live=0 and analysis_right ='Rugby Internationals (Scotland) - BBC' then number_of_events_viewed else 0 end) 
as RISBBC_Total_Viewing_EventsNon_LIVE
,sum(case when live=0 and analysis_right ='Rugby Internationals (Wales) - BBC' then number_of_events_viewed else 0 end) 
as RIWBBC_Total_Viewing_EventsNon_LIVE
,sum(case when live=0 and analysis_right ='Rugby League  Challenge Cup- BBC' then number_of_events_viewed else 0 end) 
as RLCCBBC_Total_Viewing_EventsNon_LIVE
,sum(case when live=0 and analysis_right ='Rugby League - Sky Sports' then number_of_events_viewed else 0 end) 
as RLGSS_Total_Viewing_EventsNon_LIVE
,sum(case when live=0 and analysis_right ='Rugby League  World Cup- BBC' then number_of_events_viewed else 0 end) 
as RLWCBBC_Total_Viewing_EventsNon_LIVE
,sum(case when live=0 and analysis_right ='Sanzar Rugby Union 2011 - 2015 - Sky Sports' then number_of_events_viewed else 0 end) 
as SARUSS_Total_Viewing_EventsNon_LIVE
,sum(case when live=0 and analysis_right ='Scottish FA Deal - 2010/11 To 2013/14 - Sky Sports' then number_of_events_viewed else 0 end) 
as SFASS_Total_Viewing_EventsNon_LIVE
,sum(case when live=0 and analysis_right ='Serie A - BT Sport' then number_of_events_viewed else 0 end) 
as SABTS_Total_Viewing_EventsNon_LIVE
,sum(case when live=0 and analysis_right ='Serie A - ESPN' then number_of_events_viewed else 0 end) 
as SAESPN_Total_Viewing_EventsNon_LIVE
,sum(case when live=0 and analysis_right ='SFL - ESPN' then number_of_events_viewed else 0 end) 
as SFLESPN_Total_Viewing_EventsNon_LIVE
,sum(case when live=0 and analysis_right ='Six Nations - BBC' then number_of_events_viewed else 0 end) 
as SNRBBC_Total_Viewing_EventsNon_LIVE
,sum(case when live=0 and analysis_right ='Sky 1 and Sky 2 Boxing' then number_of_events_viewed else 0 end) 
as BOXS12_Total_Viewing_EventsNon_LIVE
,sum(case when live=0 and analysis_right ='Sky 1 and Sky 2 Football' then number_of_events_viewed else 0 end) 
as FOOTS12_Total_Viewing_EventsNon_LIVE
,sum(case when live=0 and analysis_right ='Sky 1 and Sky 2 Motor Sport' then number_of_events_viewed else 0 end) 
as MSPS12_Total_Viewing_EventsNon_LIVE
,sum(case when live=0 and analysis_right ='Sky 1 and Sky 2 Unknown' then number_of_events_viewed else 0 end) 
as UNKS12_Total_Viewing_EventsNon_LIVE
,sum(case when live=0 and analysis_right ='Sky 1 and Sky 2 Wrestling' then number_of_events_viewed else 0 end) 
as WRES12_Total_Viewing_EventsNon_LIVE
,sum(case when live=0 and analysis_right ='Sky Sports American Football' then number_of_events_viewed else 0 end) 
as AMSS_Total_Viewing_EventsNon_LIVE
,sum(case when live=0 and analysis_right ='Sky Sports Athletics' then number_of_events_viewed else 0 end) 
as ATHSS_Total_Viewing_EventsNon_LIVE
,sum(case when live=0 and analysis_right ='Sky Sports Baseball' then number_of_events_viewed else 0 end) 
as BASESS_Total_Viewing_EventsNon_LIVE
,sum(case when live=0 and analysis_right ='Sky Sports Basketball' then number_of_events_viewed else 0 end) 
as BASKSS_Total_Viewing_EventsNon_LIVE
,sum(case when live=0 and analysis_right ='Sky Sports Boxing' then number_of_events_viewed else 0 end) 
as BOXSS_Total_Viewing_EventsNon_LIVE
,sum(case when live=0 and analysis_right ='Sky Sports Cricket' then number_of_events_viewed else 0 end) 
as CRISS_Total_Viewing_EventsNon_LIVE
,sum(case when live=0 and analysis_right ='Sky Sports Darts' then number_of_events_viewed else 0 end) 
as DARTSS_Total_Viewing_EventsNon_LIVE
,sum(case when live=0 and analysis_right ='Sky Sports Equestrian' then number_of_events_viewed else 0 end) 
as EQUSS_Total_Viewing_EventsNon_LIVE
,sum(case when live=0 and analysis_right ='Sky Sports Extreme' then number_of_events_viewed else 0 end) 
as EXTSS_Total_Viewing_EventsNon_LIVE
,sum(case when live=0 and analysis_right ='Sky Sports Fishing' then number_of_events_viewed else 0 end) 
as FISHSS_Total_Viewing_EventsNon_LIVE
,sum(case when live=0 and analysis_right ='Sky Sports Football' then number_of_events_viewed else 0 end) 
as FOOTSS_Total_Viewing_EventsNon_LIVE
,sum(case when live=0 and analysis_right ='Sky Sports Golf' then number_of_events_viewed else 0 end) 
as GOLFSS_Total_Viewing_EventsNon_LIVE
,sum(case when live=0 and analysis_right ='Sky Sports Ice Hockey' then number_of_events_viewed else 0 end) 
as IHSS_Total_Viewing_EventsNon_LIVE
,sum(case when live=0 and analysis_right ='Sky Sports Motor Sport' then number_of_events_viewed else 0 end) 
as MSPSS_Total_Viewing_EventsNon_LIVE
,sum(case when live=0 and analysis_right ='Sky Sports Racing' then number_of_events_viewed else 0 end) 
as RACSS_Total_Viewing_EventsNon_LIVE
,sum(case when live=0 and analysis_right ='Sky Sports Rugby' then number_of_events_viewed else 0 end) 
as RUGSS_Total_Viewing_EventsNon_LIVE
,sum(case when live=0 and analysis_right ='Sky Sports Snooker/Pool' then number_of_events_viewed else 0 end) 
as SNPSS_Total_Viewing_EventsNon_LIVE
,sum(case when live=0 and analysis_right ='Sky Sports Tennis' then number_of_events_viewed else 0 end) 
as TENSS_Total_Viewing_EventsNon_LIVE
,sum(case when live=0 and analysis_right ='Sky Sports Unknown' then number_of_events_viewed else 0 end) 
as UNKSS_Total_Viewing_EventsNon_LIVE
,sum(case when live=0 and analysis_right ='Sky Sports Watersports' then number_of_events_viewed else 0 end) 
as WATSS_Total_Viewing_EventsNon_LIVE
,sum(case when live=0 and analysis_right ='Sky Sports Wintersports' then number_of_events_viewed else 0 end) 
as WINSS_Total_Viewing_EventsNon_LIVE
,sum(case when live=0 and analysis_right ='Sky Sports Wrestling' then number_of_events_viewed else 0 end) 
as WRESS_Total_Viewing_EventsNon_LIVE
,sum(case when live=0 and analysis_right ='Solheim Cup 2013 Ladies Golf - Sky Sports' then number_of_events_viewed else 0 end) 
as SOLSS_Total_Viewing_EventsNon_LIVE
,sum(case when live=0 and analysis_right ='South African Home Cricket  2012-2020 Sky Sports' then number_of_events_viewed else 0 end) 
as SACSS_Total_Viewing_EventsNon_LIVE
,sum(case when live=0 and analysis_right ='Spanish Football 2012/13 To 14/15 - Sky Sports' then number_of_events_viewed else 0 end) 
as SPFSS_Total_Viewing_EventsNon_LIVE
,sum(case when live=0 and analysis_right ='SPFL - BT Sport' then number_of_events_viewed else 0 end) 
as SPFLBTS_Total_Viewing_EventsNon_LIVE
,sum(case when live=0 and analysis_right ='SPL - ESPN' then number_of_events_viewed else 0 end) 
as SPLESPN_Total_Viewing_EventsNon_LIVE
,sum(case when live=0 and analysis_right ='SPL - Sky Sports' then number_of_events_viewed else 0 end) 
as SPLSS_Total_Viewing_EventsNon_LIVE
,sum(case when live=0 and analysis_right ='Sportfive World Cup 2014 Qualifers - Sky Sports' then number_of_events_viewed else 0 end) 
as SP5SS_Total_Viewing_EventsNon_LIVE
,sum(case when live=0 and analysis_right ='The boat race - BBC' then number_of_events_viewed else 0 end) 
as BTRBBC_Total_Viewing_EventsNon_LIVE
,sum(case when live=0 and analysis_right ='The football league show - BBC' then number_of_events_viewed else 0 end) 
as FLSBBC_Total_Viewing_EventsNon_LIVE
,sum(case when live=0 and analysis_right ='The Masters Golf - BBC' then number_of_events_viewed else 0 end) 
as MGBBC_Total_Viewing_EventsNon_LIVE
,sum(case when live=0 and analysis_right ='TNA Wrestling Challenge' then number_of_events_viewed else 0 end) 
as TNACHA_Total_Viewing_EventsNon_LIVE
,sum(case when live=0 and analysis_right ='Tour de France - Eurosport' then number_of_events_viewed else 0 end) 
as TDFEUR_Total_Viewing_EventsNon_LIVE
,sum(case when live=0 and analysis_right ='Tour de France - ITV' then number_of_events_viewed else 0 end) 
as TDFITV_Total_Viewing_EventsNon_LIVE
,sum(case when live=0 and analysis_right ='U S Masters Golf 2011-2014 Sky Sports' then number_of_events_viewed else 0 end) 
as USMGSS_Total_Viewing_EventsNon_LIVE
,sum(case when live=0 and analysis_right ='U.S Open Tennis 2013-2015 Sky Sports' then number_of_events_viewed else 0 end) 
as USOTSS_Total_Viewing_EventsNon_LIVE
,sum(case when live=0 and analysis_right ='U.S Open/Womens/Seniors/Amateur Golf Sky Sports' then number_of_events_viewed else 0 end) 
as USOGSS_Total_Viewing_EventsNon_LIVE
,sum(case when live=0 and analysis_right ='UEFA Champions League -  Sky Sports' then number_of_events_viewed else 0 end) 
as CLASS_Total_Viewing_EventsNon_LIVE
,sum(case when live=0 and analysis_right ='UEFA Champions League -  Sky Sports (non Live)' then number_of_events_viewed else 0 end) 
as CLNSS_Total_Viewing_EventsNon_LIVE
,sum(case when live=0 and analysis_right ='UEFA Champions League -  Sky Sports (other Live)' then number_of_events_viewed else 0 end) 
as CLOSS_Total_Viewing_EventsNon_LIVE
,sum(case when live=0 and analysis_right ='UEFA Champions League -  Sky Sports (Tue)' then number_of_events_viewed else 0 end) 
as CLTSS_Total_Viewing_EventsNon_LIVE
,sum(case when live=0 and analysis_right ='UEFA Champions League -  Sky Sports (Wed)' then number_of_events_viewed else 0 end) 
as CLWSS_Total_Viewing_EventsNon_LIVE
,sum(case when live=0 and analysis_right ='US Open Tennis - Eurosport' then number_of_events_viewed else 0 end) 
as USOTEUR_Total_Viewing_EventsNon_LIVE
,sum(case when live=0 and analysis_right ='USA Football - BT Sport' then number_of_events_viewed else 0 end) 
as USFBTS_Total_Viewing_EventsNon_LIVE
,sum(case when live=0 and analysis_right ='USPGA Championship (2007-2016) Sky Sports' then number_of_events_viewed else 0 end) 
as USPGASS_Total_Viewing_EventsNon_LIVE
,sum(case when live=0 and analysis_right ='WCQ - ESPN' then number_of_events_viewed else 0 end) 
as WCQESPN_Total_Viewing_EventsNon_LIVE
,sum(case when live=0 and analysis_right ='Welsh Home International Football 2012-2014 Sky Sports' then number_of_events_viewed else 0 end) 
as WIFSS_Total_Viewing_EventsNon_LIVE
,sum(case when live=0 and analysis_right ='West Indies Home Cricket 2013-2020 Sky Sports' then number_of_events_viewed else 0 end) 
as WICSS_Total_Viewing_EventsNon_LIVE
,sum(case when live=0 and analysis_right ='Wimbledon - BBC' then number_of_events_viewed else 0 end) 
as WIMBBC_Total_Viewing_EventsNon_LIVE
,sum(case when live=0 and analysis_right ='Womens Cricket - I C C Cricket Deal  2012-2015 Sky Sports' then number_of_events_viewed else 0 end) 
as WICCSS_Total_Viewing_EventsNon_LIVE
,sum(case when live=0 and analysis_right ='World Athletics Championship - More 4' then number_of_events_viewed else 0 end) 
as WACMR4_Total_Viewing_EventsNon_LIVE
,sum(case when live=0 and analysis_right ='World Club Championship - BBC' then number_of_events_viewed else 0 end) 
as WCLBBBC_Total_Viewing_EventsNon_LIVE
,sum(case when live=0 and analysis_right ='World Cup Qualifiers - BT Sport' then number_of_events_viewed else 0 end) 
as WCQBTS_Total_Viewing_EventsNon_LIVE
,sum(case when live=0 and analysis_right ='World Darts Championship 2009-2012 Sky Sports' then number_of_events_viewed else 0 end) 
as WDCSS_Total_Viewing_EventsNon_LIVE
,sum(case when live=0 and analysis_right ='World snooker championship - BBC' then number_of_events_viewed else 0 end) 
as WSCBBC_Total_Viewing_EventsNon_LIVE
,sum(case when live=0 and analysis_right ='WWE Sky 1 and 2' then number_of_events_viewed else 0 end) 
as WWES12_Total_Viewing_EventsNon_LIVE
,sum(case when live=0 and analysis_right ='WWE Sky Sports' then number_of_events_viewed else 0 end) 
as WWESS_Total_Viewing_EventsNon_LIVE


into dbarnett.v250_unannualised_right_activity_by_live_non_live
from dbarnett.v250_sports_rights_viewed_by_right_and_live_status
group by account_number
;
commit;

------
commit;
--select WWESS_Total_Viewing_EventsNon_LIVE from dbarnett.v250_unannualised_right_activity_by_live_non_live;

--select top 500 * from dbarnett.v250_sports_rights_viewed_by_right_and_live_status
--select count(*) from dbarnett.v250_master_account_list

---Create Annualised totals for each right--
--select distinct analysis_right from dbarnett.v250_rights_broadcast_overall order by analysis_right;
--Calculate Number of Days each right broadcast--
--select top 100 * from dbarnett.v250_sports_rights_epg_data_for_analysis
--select top 100 * from dbarnett.v250_rights_broadcast_overall;
--select distinct analysis_right from dbarnett.v250_rights_broadcast_overall order by analysis_right;
--

commit;
CREATE LF INDEX idx3 ON dbarnett.v250_rights_broadcast_overall (broadcast_date);
CREATE HG INDEX idx2 ON dbarnett.v250_rights_broadcast_overall (analysis_right);
commit;

commit;
CREATE HG INDEX idx1 ON dbarnett.v250_days_viewing_by_account (account_number);
CREATE LF INDEX idx3 ON dbarnett.v250_days_viewing_by_account (viewing_date);
commit;


