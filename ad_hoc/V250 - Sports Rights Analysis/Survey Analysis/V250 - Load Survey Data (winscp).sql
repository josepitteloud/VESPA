drop table dbarnett.v250_sports_rights_survey_responses_winscp;
---Import Survey File---

create table dbarnett.v250_sports_rights_survey_responses_winscp
(ID_completed varchar(9)
,ID_endDate varchar(12)
,ID_name varchar(12)
,ID_start varchar(17)
,ID_date varchar(12)
,ID_time varchar(4)
,Sky_Sports varchar(1)
,Q1 varchar(129)
,Q2 varchar(6)
,Q3 varchar(8)
,Q4 varchar(45)
,Q5 varchar(38)
,Q6_a varchar(1)
,Q7_a varchar(1)
,Q8_a varchar(1)
,Q9_a varchar(1)
,Q10_a varchar(1)
,Q11_a varchar(1)
,Q12_a varchar(1)
,Q13_a varchar(1)
,Q14 varchar(18)
,Q15 varchar(1)
,Q16_a varchar(1)
,Q16_b varchar(1)
,Q16_c varchar(1)
,Q16_d varchar(1)
,Q17_a varchar(87)
,Q17_b varchar(87)
,Q17_c varchar(87)
,Q17_d varchar(100)
,Q18 varchar(20)
,Q19 varchar(100)
,Q20_a varchar(2)
,Q20_b varchar(2)
,Q20_c varchar(2)
,Q20_d varchar(2)
,Q21 varchar(100)
,Q22 varchar(2)
,Q23_a varchar(2)
,Q23_b varchar(2)
,Q23_c varchar(2)
,Q24 varchar(2)
,Q25_a varchar(2)
,Q25_b varchar(2)
,Q25_c varchar(2)
,Q26_a varchar(34)
,Q26_b varchar(34)
,Q26_c varchar(34)
,Q26_d varchar(34)
,Q26_e varchar(34)
,Q26_f varchar(34)
,Q26_g varchar(34)
,Q26_h varchar(34)
,Q26_i varchar(34)
,Q26_j varchar(34)
,Q26_k varchar(34)
,Q26_l varchar(34)
,Q26_m varchar(34)
,Q26_n varchar(34)
,Q26_o varchar(34)
,Q26_p varchar(34)
,DV26 varchar(150)
,Q27_a varchar(120)
,Q27_b varchar(120)
,Q27_c varchar(120)
,Q28_a varchar(120)
,Q28_b varchar(120)
,Q28_c varchar(120)
,Q29_a varchar(120)
,Q29_b varchar(120)
,Q29_c varchar(120)
,Q29_d varchar(120)
,Q29_e varchar(120)
,Q30_a varchar(120)
,Q30_b varchar(120)
,Q30_c varchar(120)
,Q31_a varchar(120)
,Q32_a varchar(120)
,Q32_b varchar(120)
,Q32_c varchar(120)
,Q33_a varchar(120)
,Q33_b varchar(120)
,Q33_c varchar(120)
,Q33_d varchar(120)
,Q33_e varchar(120)
,Q34_a varchar(120)
,Q34_b varchar(120)
,Q34_c varchar(120)
,Q34_d varchar(120)
,Q34_e varchar(120)
,Q35_a varchar(120)
,Q35_b varchar(120)
,Q35_c varchar(120)
,Q35_d varchar(120)
,Q36_a varchar(120)
,Q36_b varchar(120)
,Q36_c varchar(120)
,Q36_d varchar(120)
,Q37_a varchar(120)
,Q37_b varchar(120)
,Q37_c varchar(120)
,Q38_a varchar(120)
,Q38_b varchar(120)
,Q39_a varchar(120)
,Q39_b varchar(120)
,Q39_c varchar(120)
,Q39_d varchar(120)
,Q39_e varchar(120)
,Q39_f varchar(120)
,Q40_a varchar(120)
,Q40_b varchar(120)
,Q40_c varchar(120)
,Q41_a varchar(120)
,Q41_b varchar(120)
,Q42_a varchar(120)
,Q42_b varchar(120)
,Q42_c varchar(120)
,Q42_d varchar(120)
,DV38 varchar(1600)
,Q43 varchar(400)
,Q44_a varchar(3)
,Q44_b varchar(3)
,Q44_c varchar(3)
,Q44_d varchar(3)
,Q44_e varchar(3)
,Q44_f varchar(3)
,Q44_g varchar(3)
,Q44_h varchar(3)
,Q44_i varchar(3)
,Q44_j varchar(3)
,Q44_k varchar(3)
,Q44_l varchar(3)
,Q44_m varchar(3)
,Q44_n varchar(3)
,Q44_o varchar(3)
,Q44_p varchar(3)
,Q44_q varchar(3)
,Q44_r varchar(3)
,Q44_s varchar(3)
,Q44_t varchar(3)
,Q44_u varchar(3)
,Q44_v varchar(3)
,Q44_w varchar(3)
,Q44_x varchar(3)
,Q44_y varchar(3)
,Q44_z varchar(3)
,Q44_aa varchar(3)
,Q44_ab varchar(3)
,Q44_ac varchar(3)
,Q44_ad varchar(3)
,Q44_ae varchar(3)
,Q44_af varchar(3)
,Q44_ag varchar(3)
,Q44_ah varchar(3)
,Q44_ai varchar(3)
,Q44_aj varchar(3)
,Q44_ak varchar(3)
,Q44_al varchar(3)
,Q44_am varchar(3)
,Q44_an varchar(3)
,Q44_ao varchar(3)
,Q44_ap varchar(3)
,Q44_aq varchar(3)
,Q44_ar varchar(3)
,Q44_as varchar(3)
,Q44_at varchar(3)
,Q44_au varchar(3)
,Q44_av varchar(3)
,Q44_aw varchar(3)
,Q44_ax varchar(3)
,Q44_ay varchar(3)
,Q44_az varchar(3)
,Q44_ba varchar(3)
,Q44_bb varchar(3)
,Q44_bc varchar(3)
,Q44_bd varchar(3)
,Q44_be varchar(3)
,Q44_bf varchar(3)
,Q45 varchar(100)
,Q46 varchar(100)
,Q47 varchar(200)
,Q48 varchar(3)
)
;
commit;


LOAD TABLE v250_sports_rights_survey_responses_winscp (ID_completed
,ID_endDate
,ID_name
,ID_start
,ID_date
,ID_time
,Sky_Sports
,Q1
,Q2
,Q3
,Q4
,Q5
,Q6_a
,Q7_a
,Q8_a
,Q9_a
,Q10_a
,Q11_a
,Q12_a
,Q13_a
,Q14
,Q15
,Q16_a
,Q16_b
,Q16_c
,Q16_d
,Q17_a
,Q17_b
,Q17_c
,Q17_d
,Q18
,Q19
,Q20_a
,Q20_b
,Q20_c
,Q20_d
,Q21
,Q22
,Q23_a
,Q23_b
,Q23_c
,Q24
,Q25_a
,Q25_b
,Q25_c
,Q26_a
,Q26_b
,Q26_c
,Q26_d
,Q26_e
,Q26_f
,Q26_g
,Q26_h
,Q26_i
,Q26_j
,Q26_k
,Q26_l
,Q26_m
,Q26_n
,Q26_o
,Q26_p
,DV26
,Q27_a
,Q27_b
,Q27_c
,Q28_a
,Q28_b
,Q28_c
,Q29_a
,Q29_b
,Q29_c
,Q29_d
,Q29_e
,Q30_a
,Q30_b
,Q30_c
,Q31_a
,Q32_a
,Q32_b
,Q32_c
,Q33_a
,Q33_b
,Q33_c
,Q33_d
,Q33_e
,Q34_a
,Q34_b
,Q34_c
,Q34_d
,Q34_e
,Q35_a
,Q35_b
,Q35_c
,Q35_d
,Q36_a
,Q36_b
,Q36_c
,Q36_d
,Q37_a
,Q37_b
,Q37_c
,Q38_a
,Q38_b
,Q39_a
,Q39_b
,Q39_c
,Q39_d
,Q39_e
,Q39_f
,Q40_a
,Q40_b
,Q40_c
,Q41_a
,Q41_b
,Q42_a
,Q42_b
,Q42_c
,Q42_d
,DV38
,Q43
,Q44_a
,Q44_b
,Q44_c
,Q44_d
,Q44_e
,Q44_f
,Q44_g
,Q44_h
,Q44_i
,Q44_j
,Q44_k
,Q44_l
,Q44_m
,Q44_n
,Q44_o
,Q44_p
,Q44_q
,Q44_r
,Q44_s
,Q44_t
,Q44_u
,Q44_v
,Q44_w
,Q44_x
,Q44_y
,Q44_z
,Q44_aa
,Q44_ab
,Q44_ac
,Q44_ad
,Q44_ae
,Q44_af
,Q44_ag
,Q44_ah
,Q44_ai
,Q44_aj
,Q44_ak
,Q44_al
,Q44_am
,Q44_an
,Q44_ao
,Q44_ap
,Q44_aq
,Q44_ar
,Q44_as
,Q44_at
,Q44_au
,Q44_av
,Q44_aw
,Q44_ax
,Q44_ay
,Q44_az
,Q44_ba
,Q44_bb
,Q44_bc
,Q44_bd
,Q44_be
,Q44_bf
,Q45
,Q46
,Q47
,Q48
 '\n' )
FROM '/ETL013/prod/sky/olive/data/share/clarityq/export/Jim/BARB/responses_for_Olive.csv' QUOTES OFF ESCAPES OFF NOTIFY 1000 DELIMITED BY ',' START ROW ID 1
;
commit;
--from 'G:\RTCI\Lookup Tables\v250 - Sports Rights Analysis\Responses\responses for Olive.csv' format ascii;  File stored here copied over to Win SCP then deleted
select * from dbarnett.v250_sports_rights_survey_responses_winscp

grant all on dbarnett.v250_sports_rights_survey_responses_winscp to public; commit;

--select count(*) from dbarnett.v250_sports_rights_survey_responses_winscp
select top 100 * from dbarnett.v250_sports_rights_survey_responses_winscp

select distinct Q20_c from dbarnett.v250_sports_rights_survey_responses_winscp order by Q20_c


