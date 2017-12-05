-------------------------- TECI
SELECT
cluster_name
, count(*)
FROM adsmartables_14092015  			AS a
LEFT JOIN mckanej.TECI_current_score 	AS b ON a.account_number = b.account_number 
WHERE sky_base_universe =  'Adsmartable with consent'
GROUP BY cluster_name

/*
cluster_name			count()
Aspiring Adopters		257,330
Budget Basics			734,249
Deteriorating Originals	335,859
Free Riders				1,128,380
Freedom Seekers			396,257
Regular Joes			1,075,816
TV Junkies				649,713
Tech Geeks				387,116
The Technophobes		1,100,186
Utility Users			606,117
*/