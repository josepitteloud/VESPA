-- Appendix B – Reference Tables
-- These are the tables in your own schema that contain valid values for audited fields.


CREATE TABLE channel_pack(
    code integer NULL,
    name varchar(255) NULL
)
INSERT INTO channel_pack (code, name)
VALUES
(1, 'Diginets')

INSERT INTO channel_pack (code, name)
VALUES
(2, 'Diginets non-commercial')

INSERT INTO channel_pack (code, name)
VALUES
(3,'Other')

INSERT INTO channel_pack (code, name)
VALUES
(4, 'Other non-commercial')

INSERT INTO channel_pack (code, name)
VALUES
(5, 'Terrestrial')

INSERT INTO channel_pack (code, name)
VALUES
(6, 'Terrestrial non-commercial')

CREATE TABLE channel_group(
    code integer NULL,
    name varchar(255) NULL
)

INSERT INTO channel_group (code, name)
VALUES
(1,'UKTV')

INSERT INTO channel_group (code, name)
VALUES
(2,'C4')

INSERT INTO channel_group (code, name)
VALUES
(3, 'Miscellaneous')

INSERT INTO channel_group (code, name)
VALUES
(4, 'Kids')

INSERT INTO channel_group (code, name)
VALUES
(5, 'Pub')

INSERT INTO channel_group (code, name)
VALUES
(6, 'Ireland Kids')

INSERT INTO channel_group (code, name)
VALUES
(7, 'Ireland News')

INSERT INTO channel_group (code, name)
VALUES
(8, '')

INSERT INTO channel_group (code, name)
VALUES
(9, 'Sky-Active Ads')

INSERT INTO channel_group (code, name)
VALUES
(10, 'FIVE')

INSERT INTO channel_group (code, name)
VALUES
(11, 'Documentaries')

INSERT INTO channel_group (code, name)
VALUES
(12, 'Other')

INSERT INTO channel_group (code, name)
VALUES
(13, 'Ireland Documentaries')
INSERT INTO channel_group (code, name)
VALUES
(14, 'Entertainment')

INSERT INTO channel_group (code, name)
VALUES
(15, 'Music')

INSERT INTO channel_group (code, name)
VALUES
(16, 'Box Office')

INSERT INTO channel_group (code, name)
VALUES
(17, 'Lifestyle & Culture')

INSERT INTO channel_group (code, name)
VALUES
(18, 'C4 Digital')

INSERT INTO channel_group (code, name)
VALUES
(19, 'ITV Digital')

INSERT INTO channel_group (code, name)
VALUES
(20, 'Sports')

INSERT INTO channel_group (code, name)
VALUES
(21, 'FIVE Digital')

INSERT INTO channel_group (code, name)
VALUES
(22, '3D')

INSERT INTO channel_group (code, name)
VALUES
(23, 'BBC')

INSERT INTO channel_group (code, name)
VALUES
(24, 'Ireland Sport')

INSERT INTO channel_group (code, name)
VALUES
(25, 'News')

INSERT INTO channel_group (code, name)
VALUES
(26, 'Anytime TV')

INSERT INTO channel_group (code, name)
VALUES
(27, 'Other wholly-owned')

INSERT INTO channel_group (code, name)
VALUES
(28, 'Ireland Entertainment')

INSERT INTO channel_group (code, name)
VALUES
(29, 'Movies')

INSERT INTO channel_group (code, name)
VALUES
(30, 'ITV')

INSERT INTO channel_group (code, name)
VALUES
(31, 'None')

INSERT INTO channel_group (code, name)
VALUES
(32, 'BT')

INSERT INTO channel_group (code, name)
VALUES
(33, 'Ethnic')

CREATE TABLE channel_genre(
    code integer NULL,
    name varchar(255) NULL
)

INSERT INTO channel_genre (code, name)
VALUES
(1,'Adult')

INSERT INTO channel_genre (code, name)
VALUES
(2, 'Documentaries')

INSERT INTO channel_genre (code, name)
VALUES
(3, 'Entertainment')

INSERT INTO channel_genre (code, name)
VALUES
(4, 'EPG')

INSERT INTO channel_genre (code, name)
VALUES
(5, 'Gaming')

INSERT INTO channel_genre (code, name)
VALUES
(6, 'Dating')

INSERT INTO channel_genre (code, name)
VALUES
(7, 'Interactive')

INSERT INTO channel_genre (code, name)
VALUES
(8, 'International')

INSERT INTO channel_genre (code, name)
VALUES
(9, 'Kids')

INSERT INTO channel_genre (code, name)
VALUES
(10, 'Lifestyle & Culture')
INSERT INTO channel_genre (code, name)
VALUES
(11, 'Movies')

INSERT INTO channel_genre (code, name)
VALUES
(12, 'Music')

INSERT INTO channel_genre (code, name)
VALUES
(13, 'N/a')

INSERT INTO channel_genre (code, name)
VALUES(14, 'News')

INSERT INTO channel_genre (code, name)
VALUES(15, 'Radio')

INSERT INTO channel_genre (code, name)
VALUES(16, 'Religion')

INSERT INTO channel_genre (code, name)
VALUES(17, 'Shopping')

INSERT INTO channel_genre (code, name)
VALUES(18, 'Sport')

INSERT INTO channel_genre (code, name)
VALUES(19, 'Unknown')

INSERT INTO channel_genre (code, name)
VALUES(20, '')

INSERT INTO channel_genre (code, name)
VALUES(21, 'Specialist')


CREATE TABLE channel_owner(
    code integer NULL,
    name varchar(255) NULL
)

INSERT INTO channel_owner (code, name)
VALUES
(1, 'RTE')

INSERT INTO channel_owner (code, name)
VALUES
(2, 'Other')

INSERT INTO channel_owner (code, name)
VALUES
(3, 'Sky')

INSERT INTO channel_owner (code, name)
VALUES
(4, 'Warner Bros')

INSERT INTO channel_owner (code, name)
VALUES
(5, 'NBC Universal')

INSERT INTO channel_owner (code, name)
VALUES
(6, 'NewsCorp')

INSERT INTO channel_owner (code, name)
VALUES
(7, 'Discovery')

INSERT INTO channel_owner (code, name)
VALUES
(8, 'Channel 4')

INSERT INTO channel_owner (code, name)
VALUES
(9, 'BBC')

INSERT INTO channel_owner (code, name)
VALUES
(10, 'Disney')

INSERT INTO channel_owner (code, name)
VALUES
(11, 'AETN')

INSERT INTO channel_owner (code, name)
VALUES
(12, 'Viacom')

INSERT INTO channel_owner (code, name)
VALUES
(13, 'Channel 5')

INSERT INTO channel_owner (code, name)
VALUES
(14, 'Third Parties')

INSERT INTO channel_owner (code, name)
VALUES
(15, 'Sony')

INSERT INTO channel_owner (code, name)
VALUES
(16, 'CBS')

INSERT INTO channel_owner (code, name)
VALUES
(17, 'BT')

INSERT INTO channel_owner (code, name)
VALUES
(18, 'Discovery Europe')

INSERT INTO channel_owner (code, name)
VALUES
(19, 'ITV')

INSERT INTO channel_owner (code, name)
VALUES
(20, 'Chellomedia')

INSERT INTO channel_owner (code, name)
VALUES
(21, 'New Delhi Television Ltd')

INSERT INTO channel_owner (code, name)
VALUES
(22, 'Asia TV Ltd')

INSERT INTO channel_owner (code, name)
VALUES
(23, 'UK Channel Management')

INSERT INTO channel_owner (code, name)
VALUES
(24, 'E! Entertain Television Inc')

INSERT INTO channel_owner (code, name)
VALUES
(25, 'TV Today Network')

INSERT INTO channel_owner (code, name)
VALUES
(26, 'Vintage Entertainment Ltd')

INSERT INTO channel_owner (code, name)
VALUES
(27, 'Jan Media Ltd')

INSERT INTO channel_owner (code, name)
VALUES
(28, 'Viacom 18 Media Pvt Ltd')

INSERT INTO channel_owner (code, name)
VALUES
(29, 'Family Channel')

INSERT INTO channel_owner (code, name)
VALUES
(30, 'Chart Show Channels')

INSERT INTO channel_owner (code, name)
VALUES
(31, 'GEO TV Limited')

INSERT INTO channel_owner (code, name)
VALUES
(32, 'Ultimate Hits Ltd')

INSERT INTO channel_owner (code, name)
VALUES
(33, 'Exodus Media Limited')

INSERT INTO channel_owner (code, name)
VALUES
(34, 'TV 18 Broadcast Ltd')

INSERT INTO channel_owner (code, name)
VALUES
(35, 'History Channel')

INSERT INTO channel_owner (code, name)
VALUES
(36, 'Travel Channel')

INSERT INTO channel_owner (code, name)
VALUES
(37, 'Channel 6')

INSERT INTO channel_owner (code, name)
VALUES
(38, 'ESTV ltd')

INSERT INTO channel_owner (code, name)
VALUES
(39, 'Premier Media Sarl')
INSERT INTO channel_owner (code, name)
VALUES
(40, 'CSC Media Group Ltd')

INSERT INTO channel_owner (code, name)
VALUES
(41, 'HUM Network UK Ltd')

INSERT INTO channel_owner (code, name)
VALUES
(42, 'Turner Broadcasting Systems Ltd')

INSERT INTO channel_owner (code, name)
VALUES
(43, 'Media Worldwide UK Ptv Ltd')

INSERT INTO channel_owner (code, name)
VALUES
(44, 'Nollywood Movies Ltd')

INSERT INTO channel_owner (code, name)
VALUES
(45, 'UKTV')

INSERT INTO channel_owner (code, name)
VALUES
(46, 'Oireachtas Commission')

INSERT INTO channel_owner (code, name)
VALUES
(47, 'Liverpool F.C.')

INSERT INTO channel_owner (code, name)
VALUES
(48, 'FilmOn TV Limited')

INSERT INTO channel_owner (code, name)
VALUES
(49, 'Canis Retail Ltd')

INSERT INTO channel_owner (code, name)
VALUES
(50, 'Made Television')

INSERT INTO channel_owner (code, name)
VALUES
(51, 'Cellcast Group')

INSERT INTO channel_owner (code, name)
VALUES
(52, 'Bigfoot Entertainment')

CREATE TABLE sales_house(
    code integer NULL,
    primary_sales_house varchar(255) NULL,
    channel_group varchar(255) NULL
)
INSERT INTO sales_house (code, primary_sales_house,channel_group )
VALUES
(1, 'Channel 4 sales', 'C4')

INSERT INTO sales_house (code, primary_sales_house,channel_group )
VALUES
(2, 'Channel 4 sales', 'UKTV')

INSERT INTO sales_house (code, primary_sales_house,channel_group )
VALUES
(3, 'Channel 4 sales', 'C4 Digital')

INSERT INTO sales_house (code, primary_sales_house,channel_group )
VALUES
(4, 'Channel 4 sales', 'Music')

INSERT INTO sales_house (code, primary_sales_house,channel_group )
VALUES
(5, 'Channel 5 sales', 'FIVE')

INSERT INTO sales_house (code, primary_sales_house,channel_group )
VALUES
(6, 'Channel 5 sales', 'FIVE Digital')

INSERT INTO sales_house (code, primary_sales_house,channel_group )
VALUES
(7, 'ITV sales', 'ITV')
INSERT INTO sales_house (code, primary_sales_house,channel_group )
VALUES
(8, 'ITV sales', 'ITV Digital')

INSERT INTO sales_house (code, primary_sales_house,channel_group )
VALUES
(9, 'BBC', 'BBC')

INSERT INTO sales_house (code, primary_sales_house,channel_group )
VALUES
(10, 'Sky sales', 'Kids')

INSERT INTO sales_house (code, primary_sales_house,channel_group )
VALUES
(11, 'Sky sales', 'Ireland Kids')

INSERT INTO sales_house (code, primary_sales_house,channel_group )
VALUES
(12, 'Sky sales', 'Ireland News')

INSERT INTO sales_house (code, primary_sales_house,channel_group )
VALUES
(13, 'Sky sales', 'Sky-Active Ads')

INSERT INTO sales_house (code, primary_sales_house,channel_group )
VALUES
(14, 'Sky sales', 'Documentaries')

INSERT INTO sales_house (code, primary_sales_house,channel_group )
VALUES
(15, 'Sky sales', 'Ireland Documentaries')

INSERT INTO sales_house (code, primary_sales_house,channel_group )
VALUES
(16, 'Sky sales', 'Entertainment')

INSERT INTO sales_house (code, primary_sales_house,channel_group )
VALUES
(17, 'Sky sales', 'Music')

INSERT INTO sales_house (code, primary_sales_house,channel_group )
VALUES
(18, 'Sky sales', 'Box Office')

INSERT INTO sales_house (code, primary_sales_house,channel_group )
VALUES
(19, 'Sky sales', 'Lifestyle & Culture')

INSERT INTO sales_house (code, primary_sales_house,channel_group )
VALUES
(20, 'Sky sales', 'Sports')

INSERT INTO sales_house (code, primary_sales_house,channel_group )
VALUES
(21, 'Sky sales', '3D')

INSERT INTO sales_house (code, primary_sales_house,channel_group )
VALUES
(22, 'Sky sales', 'Ireland Sport')

INSERT INTO sales_house (code, primary_sales_house,channel_group )
VALUES
(23, 'Sky sales', 'News')

INSERT INTO sales_house (code, primary_sales_house,channel_group )
VALUES
(24, 'Sky sales', 'Ireland Entertainment')

INSERT INTO sales_house (code, primary_sales_house,channel_group )
VALUES
(25, 'Sky sales', 'Movies')

INSERT INTO sales_house (code, primary_sales_house,channel_group )
VALUES
(26, 'Sky sales', 'Other wholly-owned')

INSERT INTO sales_house (code, primary_sales_house,channel_group )
VALUES
(27, 'Sky Kids', 'Kids')

INSERT INTO sales_house (code, primary_sales_house,channel_group )
VALUES
(28, 'Sky Kids', 'Ireland Kids')

INSERT INTO sales_house (code, primary_sales_house,channel_group )
VALUES
(29, 'Sky Kids', 'Ireland News')

INSERT INTO sales_house (code, primary_sales_house,channel_group )
VALUES
(30, 'Sky Kids', 'Sky-Active Ads')

INSERT INTO sales_house (code, primary_sales_house,channel_group )
VALUES
(31, 'Sky Kids', 'Documentaries')

INSERT INTO sales_house (code, primary_sales_house,channel_group )
VALUES
(32, 'Sky Kids', 'Ireland Documentaries')

INSERT INTO sales_house (code, primary_sales_house,channel_group )
VALUES
(33, 'Sky Kids', 'Entertainment')

INSERT INTO sales_house (code, primary_sales_house,channel_group )
VALUES
(34, 'Sky Kids', 'Music')

INSERT INTO sales_house (code, primary_sales_house,channel_group )
VALUES
(35, 'Sky Kids', 'Box Office')

INSERT INTO sales_house (code, primary_sales_house,channel_group )
VALUES
(36, 'Sky Kids', 'Lifestyle & Culture')

INSERT INTO sales_house (code, primary_sales_house,channel_group )
VALUES
(37, 'Sky Kids', 'Sports')

INSERT INTO sales_house (code, primary_sales_house,channel_group )
VALUES
(38, 'Sky Kids', '3D')

INSERT INTO sales_house (code, primary_sales_house,channel_group )
VALUES
(39, 'Sky Kids', 'Ireland Sport')

INSERT INTO sales_house (code, primary_sales_house,channel_group )
VALUES
(40, 'Sky Kids', 'News')

INSERT INTO sales_house (code, primary_sales_house,channel_group )
VALUES
(41, 'Sky Kids', 'Ireland Entertainment')

INSERT INTO sales_house (code, primary_sales_house,channel_group )
VALUES
(42, 'Sky Kids', 'Movies')

INSERT INTO sales_house (code, primary_sales_house,channel_group )
VALUES
(43, 'Sky Kids', 'Other wholly-owned')

INSERT INTO sales_house (code, primary_sales_house,channel_group )
VALUES
(44, 'ARY', 'Other')

INSERT INTO sales_house (code, primary_sales_house,channel_group )
VALUES
(45, 'Multicultural/Ethnic Media Sls', 'Other')

INSERT INTO sales_house (code, primary_sales_house,channel_group )
VALUES
(46, 'Disney', 'Other')

INSERT INTO sales_house (code, primary_sales_house,channel_group )
VALUES
(47, 'Eurosport sales', 'Other')

INSERT INTO sales_house (code, primary_sales_house,channel_group )
VALUES
(48, 'Media Icon', 'Other')

INSERT INTO sales_house (code, primary_sales_house,channel_group )
VALUES
(49, 'Axiom Media', 'Other')

INSERT INTO sales_house (code, primary_sales_house,channel_group )
VALUES
(50, 'Dolphin TV', 'Other')

INSERT INTO sales_house (code, primary_sales_house,channel_group )
VALUES
(51, 'Media 15', 'Other')

INSERT INTO sales_house (code, primary_sales_house,channel_group )
VALUES
(52, 'Turner sales', 'Other')

INSERT INTO sales_house (code, primary_sales_house,channel_group )
VALUES
(53, 'Sunrise TV', 'Other')

INSERT INTO sales_house (code, primary_sales_house,channel_group )
VALUES
(54, 'Channel 4 sales', 'BT')
