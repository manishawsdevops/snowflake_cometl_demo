USE WAREHOUSE COMETL_SFDC_XSMALL_WH;

USE DATABASE COMETL_SFDC_PRD_DB;

USE SCHEMA COMETL_SFDC_REPL;

create or replace TABLE ICUE_FMR_OM_CUST_PROD_SALES_AREA_CLONE1 (
	OM_CUSTOMER_PRD_SALES_AREA_SK VARCHAR(18) COLLATE 'en-ci',
	OM_CUSTOMER_SK VARCHAR(200) COLLATE 'en-ci',
	OM_PRODUCT_SK VARCHAR(200) COLLATE 'en-ci',
	OM_SALES_AREA_SK VARCHAR(200) COLLATE 'en-ci',
	PFIZER_CUSTOMER_ID VARCHAR(200) COLLATE 'en-ci',
	CUSTOMER_TYPE_DESC VARCHAR(200) COLLATE 'en-ci',
	PRODUCT_CD VARCHAR(200) COLLATE 'en-ci',
	SALES_AREA_CD VARCHAR(200) COLLATE 'en-ci',
	SOURCE_CD VARCHAR(4) COLLATE 'en-ci',
	TYPE_CD VARCHAR(4) COLLATE 'en-ci',
	DATA_SOURCE_CD VARCHAR(4) COLLATE 'en-ci',
	CUSTOMER_POOLING_NBR NUMBER(15,7),
	TENANT_ID NUMBER(20,0),
	EFFECTIVE_DATE TIMESTAMP_NTZ(0),
	END_DATE TIMESTAMP_NTZ(0),
	CREATION_DATE TIMESTAMP_NTZ(0),
	LAST_UPDATE_DATE TIMESTAMP_NTZ(0),
	PUBLISH_LOAD_DATE TIMESTAMP_NTZ(0)
);


create or replace TABLE ICUE_FMR_SF_OBJECTTERRITORY2ASSOCIATION_CLONE1 (
	ID VARCHAR(16777216),
	OBJECTID VARCHAR(16777216),
	TERRITORY2ID VARCHAR(16777216),
	ASSOCIATIONCAUSE VARCHAR(16777216),
	SOBJECTTYPE VARCHAR(16777216),
	ISDELETED NUMBER(38,0),
	LASTMODIFIEDDATE TIMESTAMP_NTZ(9),
	LASTMODIFIEDBYID VARCHAR(16777216),
	SYSTEMMODSTAMP TIMESTAMP_NTZ(9)
);


