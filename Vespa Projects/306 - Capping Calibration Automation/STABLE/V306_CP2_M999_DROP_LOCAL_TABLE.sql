----------------------------------------------------------------------------------
--	Generic SQL stored procedure for "smart" cleaning-up of local tables 
----------------------------------------------------------------------------------


CREATE OR REPLACE PROCEDURE	DROP_LOCAL_TABLE
									@TARGET_TABLE_NAME	VARCHAR(255)	=	NULL
AS	BEGIN

	IF EXISTS	(
					SELECT	1
					FROM	SYSOBJECTS
					WHERE
							[NAME]			=	@TARGET_TABLE_NAME
						AND UID				=	USER_ID()
						AND	UPPER([TYPE])	=	'U'
				)
       	BEGIN
 
			DECLARE	@SQL_ VARCHAR(255) = NULL
            COMMIT

    		SET @SQL_	=	'DROP TABLE ' || USER_NAME() || '.' || @TARGET_TABLE_NAME
            COMMIT

            EXECUTE(@SQL_)
			COMMIT
			
			MESSAGE CAST(NOW() AS TIMESTAMP) || ' | DROPPED LOCAL TABLE : ' || @TARGET_TABLE_NAME TO CLIENT
		END

END;	-- PROCEDURE
COMMIT;

GRANT EXECUTE ON DROP_LOCAL_TABLE TO VESPA_GROUP_LOW_SECURITY;
COMMIT;