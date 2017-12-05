/******************************************************************************
**
**  Project Vespa: PROJECT  V186 - Callcredit Insurance Data Eval 1
**  	Data sampling and content validations
**
**  This script will compile some data samples to validate the content of the
**	variables from the dataset. This will complete the Analytical task 2 from the brief
**
**	Related Documents:
**		- VESPA_INSURANCE DATA EVALUATION 1.sql
**		- VESPA_INSURANCE DATA EVALUATION 2.sql
**		- VESPA_INSURANCE DATA EVALUATION 3.sql
**
**	Code Sections:
**
**	Section A - Checing lenght
**		A01	-	Checking for LEN(firstname) = 1
**		A02 - 	Checking for LEN(surname) = 2
**		A03 - 	Checking for LEN(firstname) <=2 and LEN (Surname) <= 2
**		A04 - 	Checking for postcodes lenght >7 or <5
**		A05 - 	Checking for Titles
**	Section B - Checking Contents
**		B01	-	Checking ADDRESS Special Characters
**		B02	-	Check date_of_update Validity
**		B03	-	Checking Names Special Characters
**		B04	-	Surnames
**		B05	-	Postcodes special character validation
**
**	Written by Jose Pitteloud
******************************************************************************/


-------------A01	-	Checking for LEN(firstname) = 1
SELECT firstname
  , count(firstname) Total
FROM sk_prod.VESPA_INSURANCE_DATA
GROUP BY firstname
HAVING LEN (firstname) =1
-------------A02	-	Checking for LEN(surname) = 2
SELECT surname
  , count(surname) Total
FROM sk_prod.VESPA_INSURANCE_DATA
GROUP BY surname
HAVING LEN (surname) =2
-------------A03	-	Checking for LEN(firstname) <=2 and LEN (Surname) <= 2
SELECT firstname 
  , surname
  , count(firstname) Total
FROM sk_prod.VESPA_INSURANCE_DATA
GROUP BY firstname
  , surname
HAVING LEN (firstname) <=2 AND LEN (surname) <=2;
---------------A04	-	Checking for postcodes lenght >7 or <5
SELECT 
  postcode
  , COUNT(postcode) Total
INTO VESPA_INSURANCE_POSTCODES
FROM sk_prod.VESPA_INSURANCE_DATA
GROUP BY postcode;

SELECT *
FROM VESPA_INSURANCE_POSTCODES
GROUP BY postcode, total
HAVING LEN (REPLACE(postcode, ' ', '')) >7 OR LEN (REPLACE(postcode, ' ', '')) <5
----------------A05	-	 Checking for Titles
SELECT title
  , count(title) Total
FROM sk_prod.VESPA_INSURANCE_DATA
GROUP BY title

----------------B	-	CONTENTS CHECKS
----------------B01	-	 Checking ADDRESS Special Characters
SELECT address_line_1
  , count(address_line_1) TOTAL
FROM sk_prod.VESPA_INSURANCE_DATA
WHERE 
 address_line_1 like '%#%'
  OR address_line_1 like '%{%'
  OR address_line_1 like '%{%'
  OR address_line_1 like '%}%'
  OR address_line_1 like '%(%'
  OR address_line_1 like '%)%'
  OR address_line_1 like '%&%'
  OR address_line_1 like '%@%'
  OR address_line_1 like '%;%'
  OR address_line_1 like '%:%'
GROUP BY address_line_1

----------------B02	-	Check date_of_update Validity
SELECT date_of_update
, count(date_of_update)
FROM  sk_prod.VESPA_INSURANCE_DATA
Group by date_of_update

----------------B03	-	Checking Names Special Characters
SELECT firstname
  , count(firstname) TOTAL
FROM sk_prod.VESPA_INSURANCE_DATA
WHERE 
 firstname like '%#%'
  OR firstname like '%{%'
  OR firstname like '%{%'
  OR firstname like '%}%'
  OR firstname like '%(%'
  OR firstname like '%)%'
  OR firstname like '%&%'
  OR firstname like '%@%'
  OR firstname like '%;%'
  OR firstname like '%:%'
  OR firstname like '%,%'
  OR firstname like '%!%'
  OR firstname like '%"%'
GROUP BY firstname

SELECT TOP 50 firstname
  , count(firstname) TOTAL
FROM sk_prod.VESPA_INSURANCE_DATA
GROUP BY firstname
ORDER BY firstname

SELECT firstname
  , count(firstname) TOTAL
FROM sk_prod.VESPA_INSURANCE_DATA
WHERE 
 firstname like '%   %'
  OR firstname like '%aaa%'
  OR firstname like '%bbb%'
  OR firstname like '%ccc%'
  OR firstname like '%ddd%'
  OR firstname like '%eee%'
  OR firstname like '%fff%'
  OR firstname like '%ggg%'
  OR firstname like '%hhh%'
  OR firstname like '%iii:%'
  OR firstname like '%jjj%'
  OR firstname like '%kkk%'
  OR firstname like '%lll%'
  OR firstname like '%mmm%'
  OR firstname like '%nnn%'
  OR firstname like '%ooo%'
  OR firstname like '%ppp%'
  OR firstname like '%qqq%'
  OR firstname like '%rrr%'
  OR firstname like '%sss%'
  OR firstname like '%ttt%'
  OR firstname like '%uuu%'
  OR firstname like '%vvv%'
  OR firstname like '%www%'
  OR firstname like '%xxx%'
  OR firstname like '%zzz%'
  OR firstname like '%1%'
  OR firstname like '%2%'
  OR firstname like '%3%'
  OR firstname like '%4%'
  OR firstname like '%5%'
  OR firstname like '%6%'
  OR firstname like '%7%'
  OR firstname like '%8%'
  OR firstname like '%9%'
  OR firstname like '%0%'
  OR firstname like '%/%'
GROUP BY firstname

-----------B04 	-	Surnames
SELECT surname
  , count(surname) TOTAL
FROM sk_prod.VESPA_INSURANCE_DATA
WHERE 
 surname like '%#%'
  OR surname like '%{%'
  OR surname like '%{%'
  OR surname like '%}%'
  OR surname like '%(%'
  OR surname like '%)%'
  OR surname like '%&%'
  OR surname like '%@%'
  OR surname like '%;%'
  OR surname like '%:%'
  OR surname like '%,%'
  OR surname like '%!%'
  OR surname like '%"%'
GROUP BY surname;

SELECT TOP 50 surname
  , count(surname) TOTAL
FROM sk_prod.VESPA_INSURANCE_DATA
GROUP BY surname
ORDER BY surname;

SELECT surname
  , count(surname) TOTAL
FROM sk_prod.VESPA_INSURANCE_DATA
WHERE 
 surname like '%   %'
  OR surname like '%aaa%'
  OR surname like '%bbb%'
  OR surname like '%ccc%'
  OR surname like '%ddd%'
  OR surname like '%eee%'
  OR surname like '%fff%'
  OR surname like '%ggg%'
  OR surname like '%hhh%'
  OR surname like '%iii:%'
  OR surname like '%jjj%'
  OR surname like '%kkk%'
  OR surname like '%lll%'
  OR surname like '%mmm%'
  OR surname like '%nnn%'
  OR surname like '%ooo%'
  OR surname like '%ppp%'
  OR surname like '%qqq%'
  OR surname like '%rrr%'
  OR surname like '%sss%'
  OR surname like '%ttt%'
  OR surname like '%uuu%'
  OR surname like '%vvv%'
  OR surname like '%www%'
  OR surname like '%xxx%'
  OR surname like '%zzz%'
  OR surname like '%1%'
  OR surname like '%2%'
  OR surname like '%3%'
  OR surname like '%4%'
  OR surname like '%5%'
  OR surname like '%6%'
  OR surname like '%7%'
  OR surname like '%8%'
  OR surname like '%9%'
  OR surname like '%0%'
  OR surname like '%/%'
GROUP BY surname
----------B05 	-	Postcodes special character validation
SELECT postcode
  , count(postcode) TOTAL
FROM sk_prod.VESPA_INSURANCE_DATA
WHERE 
 postcode like '%  %'
  OR postcode like '1%'
  OR postcode like '2%'
  OR postcode like '3%'
  OR postcode like '4%'
  OR postcode like '5%'
  OR postcode like '6%'
  OR postcode like '7%'
  OR postcode like '8%'
  OR postcode like '9%'
  OR postcode like '0%'
  OR postcode like '%/%'
  OR surname like 'aa%'
  OR surname like 'bb%'
  OR surname like 'cc%'
  OR surname like 'dd%'
  OR surname like 'ee%'
  OR surname like 'ff%'
  OR surname like 'gg%'
  OR surname like 'hh%'
  OR surname like 'ii:%'
  OR surname like 'jj%'
  OR surname like 'kk%'
  OR surname like 'll%'
  OR surname like 'mm%'
  OR surname like 'nn%'
  OR surname like 'oo%'
  OR surname like 'pp%'
  OR surname like 'qq%'
  OR surname like 'rr%'
  OR surname like 'ss%'
  OR surname like 'tt%'
  OR surname like 'uu%'
  OR surname like 'vv%'
  OR surname like 'ww%'
  OR surname like 'xx%'
  OR surname like 'zz%'
GROUP BY postcode
