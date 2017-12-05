------------------------------    SUMMARIZING VIQ_Indexes_viq_dq TABLE
SELECT Metric_ID,	Metric_Desc	,Panel	,Metric_Label	,SUM(Metric_Value)	Metric_Value	,Date_Created
INTO temp1_index
 FROM kinnairt.VIQ_Indexes_viq_dq
 GROUP BY Metric_ID,	Metric_Desc	,Panel	,Metric_Label	,Date_Created
--------------------------------- CREATING DUMP TABLE WITH TOTALS by Panel 
SELECT Metric_ID, Metric_Desc, Panel, 'Total '||Metric_Desc Metric_Label, sum(Metric_Value) Metric_Value, Date_Created
into temp1_index_total
FROM temp1_index
GROUP BY 
 Metric_ID, Metric_Desc, Panel, 'Total '||Metric_Desc, Date_Created;

commit;
 --------------------------------- INSERTING TOTAL by LAbel
INSERT INTO  temp1_index_total
SELECT Metric_ID, Metric_Desc, 98, Metric_Label, sum(Metric_Value) Metric_Value, Date_Created
FROM temp1_index
GROUP BY 
 Metric_ID, Metric_Desc, Metric_Label, Date_Created;
 
 commit; 
 ----------------------------------------- INSERTING SKY TOTALS
INSERT INTO  temp1_index_total
SELECT Metric_ID, Metric_Desc, 99, 'SKY TOTAL '||Metric_Desc, sum(Metric_Value) Metric_Value, Date_Created
FROM temp1_index_total
WHERE panel = 98
GROUP BY 
 Metric_ID, Metric_Desc,  'Index '||Metric_Desc, Date_Created; 
 commit ;
---------------------------------------- INSERTING INDEXES INTO THE MAIN TABLE
----------------------------------------
---------------------------------------- INDEXES WILL BE INSERTED INTO THE SUMMARIZED TABLE

INSERT INTO temp1_index
(Metric_ID, Metric_Desc, Panel, Metric_Label, Metric_Value, Date_Created)

SELECT i.Metric_ID, 'INDEX ' ||i.Metric_Desc, i.Panel 
, i.Metric_Label 
, V_Index = ((i.Metric_Value *1000000)/ t.Metric_Value) / ((lt.Metric_Value *10000)/ st.Metric_Value) 
, i.Date_Created

FROM temp1_index as i
  INNER JOIN temp1_index_total as t ON i.Metric_ID = t.Metric_ID AND i.Panel = t.Panel AND i.Date_Created = t.Date_Created 
  INNER JOIN temp1_index_total as st ON i.Metric_ID = st.Metric_ID AND st.Panel = 99 AND i.Date_Created = st.Date_Created 
  INNER JOIN temp1_index_total as lt ON i.Metric_ID = lt.Metric_ID AND lt.Panel = 98 AND i.Date_Created = lt.Date_Created AND i.Metric_Label = lt.Metric_Label
 
commit;
------------------- DROPPING DUMP TABLE
DROP TABLE temp1_index_total;

commit
 
