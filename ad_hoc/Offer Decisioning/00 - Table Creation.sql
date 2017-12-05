-- TABLE CREATION SCRIPT
CREATE TABLE MCKINSEY_ACTIVATIONS_RTM (
	account_number VARCHAR(20) NOT NULL DEFAULT 0
	, Acquisition_Type VARCHAR(9) NULL DEFAULT NULL
	, Event_dt DATETIME NULL DEFAULT NULL
	, RTM_LEVEL_1 VARCHAR(50) NULL DEFAULT NULL
	, RTM_LEVEL_2 VARCHAR(50) NULL DEFAULT NULL
	, RTM_LEVEL_3 VARCHAR(50) NULL DEFAULT NULL
	)

COMMIT
CREATE HG INDEX id_1 ON MCKINSEY_ACTIVATIONS_RTM (account_number)
CREATE DTTM INDEX id_2 ON MCKINSEY_ACTIVATIONS_RTM (Event_dt)
COMMIT

GRANT SELECT ON MCKINSEY_ACTIVATIONS_RTM TO vespa_group_low_security
GRANT UPDATE ON MCKINSEY_ACTIVATIONS_RTM TO rka07

CREATE TABLE MCKINSEY_ACTIVE_OFFER_BASE (
	account_number 			VARCHAR (20) NOT NULL DEFAULT 0
	, subscription_id 		VARCHAR (50)
	, currency_code 		VARCHAR (5)
	, subscription_sub_type VARCHAR (80)
	, effective_from_dt 	DATETIME
	, effective_to_dt 		DATETIME
	, status_code_changed 	VARCHAR (1)
	, status_code 			VARCHAR (10)
	, prev_status_code 		VARCHAR (10)
	, ent_cat_prod_changed 	VARCHAR (1)
	, current_product_description VARCHAR (240)
	, prem_movies TINYINT
	, prem_sports TINYINT
	)

COMMIT
CREATE HG INDEX id_1 ON MCKINSEY_ACTIVE_OFFER_BASE (account_number)
COMMIT

GRANT SELECT ON MCKINSEY_ACTIVE_OFFER_BASE TO vespa_group_low_security
GRANT UPDATE ON MCKINSEY_ACTIVE_OFFER_BASE TO rka07


CREATE TABLE MCKINSEY_BILL_DETAILS
 (
	account_number 			VARCHAR (20) NOT NULL DEFAULT 0
	, mnth_id 				INT NULL DEFAULT NULL
	, bb_product_charges	FLOAT NULL DEFAULT  0 
	, bb_total_offer_amount	FLOAT NULL DEFAULT  0 
	, dtv_credit			FLOAT NULL DEFAULT  0 
	, dtv_product_charges	FLOAT NULL DEFAULT  0 
	, dtv_total_offer_amount	FLOAT NULL DEFAULT  0 
	, hd_credit				FLOAT NULL DEFAULT  0 
	, hd_product_charges	FLOAT NULL DEFAULT  0 
	, hd_total_offer_amount	FLOAT NULL DEFAULT  0 
	, lr_credit				FLOAT NULL DEFAULT  0 
	, lr_product_charges	FLOAT NULL DEFAULT  0 
	, lr_total_offer_amount	FLOAT NULL DEFAULT  0 
	, ms_credit				FLOAT NULL DEFAULT  0 
	, ms_product_charges	FLOAT NULL DEFAULT  0 
	, ms_total_offer_amount	FLOAT NULL DEFAULT  0 
	, ppv_credit			FLOAT NULL DEFAULT  0 
	, ppv_product_charges	FLOAT NULL DEFAULT  0 
	, ss_credit				FLOAT NULL DEFAULT  0 
	, ss_product_charges	FLOAT NULL DEFAULT  0 
	, tel_credit			FLOAT NULL DEFAULT  0 
	, tel_product_charges	FLOAT NULL DEFAULT  0 
	, total_credit			FLOAT NULL DEFAULT  0 
	, total_product_charges	FLOAT NULL DEFAULT  0 
	)

COMMIT
CREATE HG INDEX id_1 ON MCKINSEY_BILL_DETAILS(account_number)
COMMIT

GRANT SELECT ON MCKINSEY_BILL_DETAILS TO vespa_group_low_security
GRANT UPDATE ON MCKINSEY_BILL_DETAILS TO rka07

CREATE TABLE MCKINSEY_BOX_HIST (
	  account_number 	VARCHAR (20) NOT NULL DEFAULT 0
	, created_dt 		DATETIME 
	, x_model_number 	VARCHAR (20)
	, x_manufacturer 	VARCHAR (50)
	, x_box_type 		VARCHAR (20)
	, active_box_flag	VARCHAR (1)
	, box_installed_dt 	DATE 
	, box_replaced_dt 	DATE 
	)


COMMIT
CREATE HG INDEX id_1 ON MCKINSEY_BOX_HIST (account_number)
COMMIT

GRANT SELECT ON MCKINSEY_BOX_HIST  TO vespa_group_low_security
GRANT UPDATE ON MCKINSEY_BOX_HIST  TO rka07












