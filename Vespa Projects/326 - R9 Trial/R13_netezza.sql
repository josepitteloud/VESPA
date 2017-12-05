  SELECT DT
        ,PANEL
		,R13_Group
        ,COUNT(1) AS LOGS
        ,SUM(LATE_LOG_FLAG) AS LATE_LOGS
  FROM (
            SELECT DATE(LOG_RECEIVED_DATETIME) AS DT
                  ,SCMS_SUBSCRIBER_ID
                  ,PANEL_ID_REPORTED AS PANEL
                  ,MAX(CASE WHEN DATE(LOG_RECEIVED_DATETIME) - DATE(LOG_CREATION_DATETIME) > 1 THEN 1 ELSE 0 END) AS LATE_LOG_FLAG
				  ,'T0' AS R13_Group
              FROM DIS_REFERENCE..FINAL_DTH_VIEWING_EVENT_HISTORY
             WHERE DT >= '2017-03-23'
               AND PANEL_ID_REPORTED IN (3, 4, 13)
               AND SCMS_SUBSCRIBER_ID IN (
 '24528092'
,'29948071'
,'31425467'
,'35733899'
,'33690135'
,'32619390'
,'34575257'
,'31139453'
,'29594117'
,'34870893'
,'29288202'
,'29190053'
,'31734531'
,'33423929'
,'1091971'
,'17878813'
,'17644541'
,'16460341'
,'33456251'
,'38321327'
,'38761882'
,'18997515'
,'24508203'
,'23503164'
,'30283602'
,'24921563'
,'24934691'
,'35410967'
,'38531821'
,'37826987'
,'149356'
,'8420621'
,'36684330'
,'26412931'
,'37599521'
,'22656894'
,'3019790'
,'31114819'
,'10616942'
,'17499846'
,'26143114'
,'10095198'
,'15950846'
,'10655591'
,'27273914'
,'24677002'
,'20983709'
,'24873628'
,'29065938'
,'30508484'
,'33527781'
,'36467076'
,'31987236'
,'2136704'
,'18466728'
,'6109854'
,'36458237'
,'36458260'
,'36912128'
,'37234307'
,'37234310'
,'36354800'
,'30330916'
,'35780246'
,'34815395'
,'34560928'
,'11635405'
,'8368715'
,'24584229'
,'25759143'
,'16731723'
,'726773'
,'11068290'
,'34889827'
,'37616180'
,'16282543'
,'9146358'
,'25252594'
,'7509359'
,'26288011'
,'25775286'
,'34272026'
,'26698722'
,'15141638'
,'32867544'
,'36735767'
,'30560845'
,'32903614'
,'18745975'
,'18745974'
,'18745976'
,'5098969'
,'36986762'
,'17472549'
,'7128056'
,'20397880'
,'20397885'
,'9938131'
,'22058152'
,'7786712'
,'16610820'
,'19760469'
,'20509076'
,'19760464'
,'19760437'
,'37690923'
,'32610231'
,'33832172'
,'1935229'
,'18710966'
,'33681070'
,'33681087'
,'21908372'
,'19763190'
,'21808890'
,'32486215'
,'23141763'
,'23141576'
,'23141580'
,'23141766'
,'32486219'
,'32486217'
,'12139696'
,'19289030'
,'33097492'
,'271163'
,'30185283'
,'17329098'
,'17329100'
,'17329090'
,'7306648'
,'37599755'
,'24181431'
,'18030438'
,'34971459'
,'31215326'
,'31215327'
,'18288481'
,'7736073'
,'15068987'
,'12365630'
,'444297'
,'36211417'
,'1231668'
,'24509074'
,'31267755'
,'36392762'
,'30458835'
,'20417831'
,'29331250'
,'29364583'
,'27136573'
,'38155632'
,'15190045'
,'35451064'
,'35451065'
,'28022477'
,'38561530'
,'22108113'
,'22108114'
  					            )								
			       GROUP BY DT
                  ,SCMS_SUBSCRIBER_ID
                  ,PANEL_ID_REPORTED
UNION ALL				  
            SELECT DATE(LOG_RECEIVED_DATETIME) AS DT
                  ,SCMS_SUBSCRIBER_ID
                  ,PANEL_ID_REPORTED AS PANEL
                  ,MAX(CASE WHEN DATE(LOG_RECEIVED_DATETIME)	- DATE(LOG_CREATION_DATETIME) > 1 THEN 1 ELSE 0 END) AS	LATE_LOG_FLAG
				  ,'T1' AS R13_Group
              FROM DIS_REFERENCE..FINAL_DTH_VIEWING_EVENT_HISTORY
             WHERE DT	>= '2017-03-23'
               AND PANEL_ID_REPORTED IN (3, 4, 13)
               AND SCMS_SUBSCRIBER_ID IN (
 '37913237'
,'37913236'
,'18058047'
,'9499276'
,'37170573'
,'30586240'
,'15229593'
,'10799016'
,'19631685'
,'24613211'
,'1800159'
,'24709580'
,'3385163'
,'24709579'
,'24709581'
,'27182026'
,'1681558'
,'24538338'
,'11532888'
,'17734556'
,'23992286'
,'24025450'
,'24025453'
,'11968128'
,'35106580'
,'19442154'
,'32179040'
,'177245'
,'22101603'
,'23831370'
,'25858337'
,'14020364'
,'377625'
,'14785540'
,'20067498'
,'10373508'
,'26923370'
,'26923366'
,'10713481'
,'36836475'
,'18877093'
,'31504447'
,'23612315'
,'29245145'
,'18877091'
,'30985496'
,'24395052'
,'15611278'
,'23569215'
,'23266292'
,'10370950'
,'10748767'
,'26081504'
,'33866539'
,'16909389'
,'28614536'
,'23533613'
,'23264846'
,'19133494'
,'12179083'
,'2301909'
,'17334542'
,'25230666'
,'5285619'
,'31627784'
,'24572525'
,'28213239'
,'22136803'
,'15427140'
,'36009591'
,'36794859'
,'36794860'
,'36794861'
,'27347616'
,'24777294'
,'30390827'
,'19398166'
,'19398165'
,'26935827'
,'18117115'
,'37583163'
,'32167906'
,'10395072'
,'34403204'
,'30726435'
,'24391515'
,'23920816'
,'30168200'
,'37192035'
,'13026283'
,'27734554'
,'24565263'
,'13026282'
,'36781446'
,'793641'
,'7098860'
,'31714574'
,'25212551'
,'3577380'
,'27773979'
,'35899851'
,'29826287'
,'33934114'
,'29310273'
,'16736567'
,'9274473'
,'16316169'
,'15020527'
,'301572'
,'13582077'
,'21898594'
,'35894067'
,'16810533'
,'2434741'
,'1837175'
,'26117867'
,'36815439'
,'9046659'
,'18217154'
,'35665070'
,'38249219'
,'3557275'
,'21495879'
,'13665342'
,'21252570'
,'3085998'
,'20091536'
,'23842860'
,'10607035'
,'21890631'
,'37234830'
,'2267988'
,'20776931'
,'880472'
,'33860434'
,'30820790'
,'36421981'
,'10622354'
,'25448061'
,'33661894'
,'29641956'
,'33661893'
,'373463'
,'20745717'
,'33784850'
,'3130645'
,'13358099'
,'178731'
,'714619'
,'8462997'
,'28052288'
,'23679664'
,'22755151'
,'9350518'
,'27366762'
,'36876103'
,'27907792'
,'12255302'
,'27876267'
,'36240070'
,'2711759'
,'24327549'
,'24327552'
,'27017495'
,'24572213'
,'36939341'
,'21838918'
,'31060351'
,'916938'
,'19804129'
,'22718836'
,'34420757'
,'25761443'
,'14079025'
,'20775310'
,'267935'
,'18447630'
,'17814221'
,'18097751'
,'31939152'
,'31939151'
,'23124410'
,'38942056'
,'31311629'
,'38038890'
,'35184642'
,'21236864'
,'14761377'
,'26582611'
,'236930'
,'18047099'
,'16359749'
,'4491800'
,'20808550'
,'4382513'
,'28484373'
,'251910'
,'30252011'
,'30964102'
,'2026181'
,'16316153'
,'23099404'
,'9417658'
,'26890466'
,'26901983'
,'22498224'
,'35763811'
,'37532452'
,'28876275'
,'151910'
,'17148783'
,'18740137'
,'16324154'
,'10881348'
,'30583906'
,'1741799'
,'10682448'
,'26588140'
,'171513'
,'15944425'
,'22188837'
,'22364349'
,'22671179'
,'10743478'
,'13074508'
,'19894795'
,'13074509'
,'22551552'
,'1389685'
,'17565396'
,'20951704'
,'30663352'
,'33384593'
,'33384677'
,'27792364'
,'14137557'
,'34873583'
,'14409006'
,'29468682'
,'30040342'
,'24563553'
,'38278855'
,'15360914'
,'33068616'
,'33714897'
,'33686516'
,'8463140'
,'22176497'
,'10965072'
,'29684011'
,'18660741'
,'1899703'
,'10650760'
,'37330601'
,'25536913'
,'35416308'
,'17293538'
,'11606209'
,'21630303'
,'34634787'
,'34634932'
,'36057054'
,'16307508'
,'22951362'
,'22944779'
,'28155729'
,'19443808'
,'16597283'
,'25069224'
,'29821523'
,'29821522'
,'25694407'
,'26608328'
,'232572'
,'22379948'
,'31415014'
,'36329426'
,'33406990'
,'33406991'
,'17034214'
,'20048076'
,'30333002'
,'30333003'
,'30333001'
,'10714988'
,'21758831'
,'36876725'
,'24936217'
,'9655956'
,'18167148'
,'29842646'
,'9964420'
,'15503495'
,'16336369'
,'37603927'
,'17054689'
,'22202982'
,'33904720'
,'33904721'
,'34020807'
,'34048837'
,'17774295'
,'34048838'
,'29850448'
,'17774298'
,'8698291'
,'140997'
,'17096406'
,'34927680'
,'34912025'
,'36436130'
,'34499231'
,'38811909'
,'14111094'
,'38845910'
,'22410871'
,'19861079'
,'33808825'
,'969536'
,'19336026'
,'34273852'
,'33976442'
,'11159339'
,'18856810'
,'11159341'
,'31292811'
,'5604445'
,'16650753'
,'24612547'
,'15349264'
,'3454029'
,'22812815'
,'37790609'
,'19171786'
,'29468719'
,'31011369'
,'32385717'
,'25651373'
,'31025225'
,'32626504'
,'32626506'
,'31936073'
,'33013649'
,'33515167'
,'38484690'
,'2724735'
,'12368332'
,'17056114'
,'20607454'
,'17481861'
,'36371834'
,'16904988'
,'7097810'
,'7808895'
,'3780665'
,'36357450'
,'32030896'
,'26934171'
,'10248921'
,'27889561'
,'6065715'
,'30932749'
,'30932750'
,'18840079'
,'1023061'
,'23468137'
,'9600739'
,'30042245'
,'19623381'
,'32242913'
,'30790047'
,'30805264'
,'17614276'
,'30610487'
,'17614275'
,'2888863'
,'20508984'
,'23194281'
,'23241870'
,'23241872'
,'31036866'
,'27513934'
,'19704745'
,'20515702'
,'13360897'
,'33387741'
,'33387742'
,'19778521'
,'27807358'
,'27807354'
,'15456664'
,'6935729'
,'31370670'
,'21346312'
,'27252109'
,'11937552'
,'31643201'
,'28294460'
,'16410740'
,'27689595'
,'27689522'
,'9841895'
,'30509993'
,'36158944'
,'37648118'
,'24089180'
,'14566214'
,'32527738'
,'34664699'
,'33354203'
,'18683350'
,'10711257'
,'17580764'
,'31421118'
,'20350279'
,'234421'
,'20275128'
,'23095537'
,'31549155'
,'5259995'
,'11412924'
,'1495298'
,'24743391'
,'29617075'
,'1913446'
,'16220684'
,'29487279'
,'37721552'
,'33690300'
,'28000118'
,'28003791'
,'29804884'
,'20778989'
,'36415681'
,'14030828'
,'16485070'
,'34756189'
,'17165198'
,'33391406'
,'5993123'
,'23305445'
,'7483011'
,'23305442'
,'15420334'
,'10764922'
,'16414801'
,'19220096'
,'34571231'
,'29072292'
,'30999130'
,'29397168'
,'1442925'
,'22475160'
,'18533935'
,'2758294'
,'33073806'
,'24223562'
,'37507522'
,'2562427'
,'24448767'
,'24448769'
,'24448766'
,'3401412'
,'12554128'
,'20592405'
,'12202274'
,'15496523'
,'34309003'
,'20873365'
,'18014438'
,'18014434'
,'18014433'
,'19040102'
,'231818'
,'16349681'
,'188494'
,'24775444'
,'10231446'
,'16599375'
,'36815773'
,'24214470'
,'22785972'
,'31893448'
,'30312639'
,'31054260'
,'20200965'
,'17921204'
,'36358257'
,'38206171'
,'31862862'
,'32978817'
,'32978819'
,'23389815'
,'31165311'
,'22549968'
,'10845421'
,'22549965'
,'11640146'
,'9296921'
,'849841'
,'8795063'
,'31102176'
,'14265334'
,'28874557'
,'24444387'
,'28324800'
,'23360145'
,'4269546'
,'8076031'
,'23238895'
,'18698549'
,'7786709'
,'17329093'
,'16334627'
,'11500713'
,'37305763'
,'33751813'
,'33751713'
,'29069417'
,'22467105'
,'33684867'
,'24238711'
,'37166928'
,'28365700'
,'10785247'
,'20495395'
,'1191770'
,'20441652'
,'38036548'
,'24005940'
,'24519556'
,'24005937'
,'8849326'
,'9857803'
,'36359448'
,'37180323'
,'33813405'
,'33813452'
,'25991656'
,'33661529'
,'36969438'
,'15371176'
,'15371177'
,'30772972'
,'17588491'
,'29926277'
,'11204922'
,'21808168'
,'21564896'
,'274752'
,'9351288'
,'7773800'
,'22387995'
,'11487019'
,'33230749'
,'33055676'
,'33192535'
,'28669051'
,'10650489'
,'31472958'
,'26372678'
,'31277661'
,'35340865'
,'14082561'
,'2179652'
,'16358851'
,'23018628'
,'35445400'
,'32808739'
,'23435611'
,'32016183'
,'12957270'
,'8066117'
,'18023296'
,'37368901'
,'37368947'
,'25896728'
,'35143574'
,'2623110'
,'24588445'
,'623687'
,'23357076'
,'21603824'
,'33814518'
,'33814519'
,'24453862'
,'34456388'
,'562276'
,'10813767'
,'17375801'
,'11890833'
,'34853955'
,'35075274'
,'35075275'
,'10625552'
,'7767893'
,'27689882'
,'25301317'
,'31037789'
,'31037784'
,'19481513'
,'33561356'
,'24245429'
,'19625473'
,'22939597'
,'24470144'
,'15663529'
,'10722208'
,'1485894'
,'217510'
,'33836751'
,'17893659'
,'9757705'
,'24510896'
,'35127347'
,'1040872'
,'8446947'
,'19631694'
,'21635987'
,'26923550'
,'37207278'
,'26760771'
,'22341010'
,'2765767'
,'10634177'
,'22874646'
,'38338782'
,'36866988'
,'13655643'
,'3020060'
,'24519113'
,'339525'
,'2986980'
,'32759543'
,'31145233'
,'33151601'
,'33917639'
,'17091207'
,'35046091'
,'6874315'
,'11672438'
,'33617227'
,'17505066'
,'9236162'
,'1649877'
,'27289829'
,'30285891'
,'21371800'
,'36711302'
,'35962883'
,'334524'
,'7815715'
,'5815010'
,'17633419'
,'23971302'
,'16753211'
,'16470659'
,'340818'
,'21650909'
,'28016811'
,'28326441'				  
)
			       GROUP BY DT
                  ,SCMS_SUBSCRIBER_ID
                  ,PANEL_ID_REPORTED

       ) AS SUB
GROUP BY DT
        ,PANEL
		,R13_Group
ORDER BY DT
        ,PANEL
		,R13_Group
;
