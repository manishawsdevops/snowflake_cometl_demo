USE WAREHOUSE COMETL_SFDC_XSMALL_WH;

USE DATABASE COMETL_SFDC_PRD_DB;

USE SCHEMA COMETL_SFDC_CONTROL;

CREATE OR REPLACE PROCEDURE "SP_AUDIT_LOG"("P_APPLICATION_NAME" VARCHAR(16777216), "P_SUBJECT_AREA_NAME" VARCHAR(16777216), "P_INTERFACE_NAME" VARCHAR(16777216), "P_TASK_NAME" VARCHAR(16777216), "P_TASK_TYPE" VARCHAR(16777216), "P_STAT" VARCHAR(16777216), "P_SRC_COUNT" FLOAT, "P_TGT_COUNT" FLOAT, "P_EXCEP_ERR" VARCHAR(16777216))
RETURNS VARCHAR(16777216)
LANGUAGE JAVASCRIPT
EXECUTE AS OWNER
AS ' 
 try 
	{
		snowflake.execute({ sqlText: "Begin Transaction;"});
		var v_return_value="";
		v_return_value += "\\n P_APPLICATION_NAME: " + P_APPLICATION_NAME;
		v_return_value += "\\n P_SUBJECT_AREA_NAME: " + P_SUBJECT_AREA_NAME;
		v_return_value += "\\n P_INTERFACE_NAME: " + P_INTERFACE_NAME;
		v_return_value += "\\n P_TASK_NAME: " + P_TASK_NAME;
		v_return_value += "\\n P_TASK_TYPE: " + P_TASK_TYPE;
		v_return_value += "\\n P_STAT: " + P_STAT;
		v_return_value += "\\n P_SRC_COUNT: " + P_SRC_COUNT;
		v_return_value += "\\n P_TGT_COUNT: " + P_TGT_COUNT;
		v_return_value += "\\n P_EXCEP_ERR: " + P_EXCEP_ERR;
			
		var V_FETCH_BATCH_SQ = `SELECT BATCH_ID FROM COMETL_SFDC_CONTROL.BATCH_RUN_DTLS WHERE UPPER(APPLICATION_NAME)=UPPER(''`+ P_APPLICATION_NAME +`'') AND UPPER(SUBJECT_AREA_NAME)=UPPER(''`+ P_SUBJECT_AREA_NAME +`'') AND LOAD_STATUS = ''R''`;
		var V_FETCH_BATCH_ST = snowflake.createStatement( {sqlText: V_FETCH_BATCH_SQ} ).execute();
		if(V_FETCH_BATCH_ST.next())
		{
			var V_BATCH_ID = V_FETCH_BATCH_ST.getColumnValue(1);
			v_return_value += "\\n V_BATCH_ID: " + V_BATCH_ID;
		}
		else
		{
			v_return_value +=''No batch is open for the interface. Open a new batch.''
			throw v_return_value;
		}
		
		var V_STAT_DESC="";
		if ( P_STAT == ''R'' ) { V_STAT_DESC = "Running";}
		else if ( P_STAT == ''F'' ) { V_STAT_DESC = "Failed";}
		else if ( P_STAT == ''C'' ) { V_STAT_DESC = "Complete";}
		else {V_STAT_DESC="";}
		v_return_value += "\\n V_STAT_DESC: " + V_STAT_DESC;
		
		
		if ( P_STAT == ''R'' )
		{
		
		var V_AUD_DEL_RESTRT_SQ = `DELETE FROM COMETL_SFDC_CONTROL.AUDIT_DTLS WHERE BATCH_ID=`+ V_BATCH_ID +` AND UPPER(SUBJECT_AREA_NAME)=UPPER(''`+ P_SUBJECT_AREA_NAME +`'') AND UPPER(APPLICATION_NAME)=UPPER(''`+ P_APPLICATION_NAME +`'') AND UPPER(INTERFACE_NAME)=UPPER(''` + P_INTERFACE_NAME +`'') AND UPPER(TASK_NAME)=UPPER(''` + P_TASK_NAME +`'') AND UPPER(EXECUTION_STATUS)=UPPER(''` + P_STAT +`'')`;
		var V_AUD_DEL_RESTRT_ST = snowflake.createStatement( {sqlText: V_AUD_DEL_RESTRT_SQ} ).execute();
		
		var V_INS_AUDIT_DML = `INSERT INTO COMETL_SFDC_CONTROL.AUDIT_DTLS VALUES (` + V_BATCH_ID +` , ''`+ P_APPLICATION_NAME +`'' , ''` + P_SUBJECT_AREA_NAME +`'' , ''`+ P_INTERFACE_NAME +`'' , ''`+ P_TASK_NAME +`'' , ''`+ P_TASK_TYPE +`'' , NULL , NULL , CURRENT_TIMESTAMP() , NULL , ''`+P_STAT +`'' , ''`+ V_STAT_DESC + `'' , NULL , CURRENT_TIMESTAMP() , ''ETL_USER'' , CURRENT_TIMESTAMP() , ''ETL_USER'' )`;
		v_return_value += "\\n V_INS_AUDIT_DML: " + V_INS_AUDIT_DML;
		var V_INS_AUDIT_DML_ST = snowflake.createStatement( {sqlText: V_INS_AUDIT_DML} ).execute();
		}
		
		if ( P_STAT != ''R'' )
		{
		var output_return_value=P_EXCEP_ERR.replace(/''/g,"''''");
		
		var V_UPD_ARCH_DML = `UPDATE COMETL_SFDC_CONTROL.AUDIT_DTLS SET SOURCE_RECORD_COUNT= DECODE(''` + P_SRC_COUNT +`'',''undefined'',NULL,''` + P_SRC_COUNT +`''), TARGET_RECORD_COUNT= DECODE(''` + P_TGT_COUNT +`'',''undefined'',NULL,''` + P_TGT_COUNT +`'') ,EXECUTION_END_TIME=CURRENT_TIMESTAMP(), EXECUTION_STATUS=''` + P_STAT +`'', EXECUTION_STATUS_DESC=''` +V_STAT_DESC +`'', ERROR_DESC=DECODE(''` + output_return_value +`'',''undefined'',NULL,''` + output_return_value +`'') , LAST_UPDATE_DATE=CURRENT_TIMESTAMP()	WHERE UPPER(SUBJECT_AREA_NAME)=UPPER(''` + P_SUBJECT_AREA_NAME +`'') AND UPPER(APPLICATION_NAME)=UPPER(''` + P_APPLICATION_NAME +`'') AND UPPER(TASK_NAME)=UPPER(''`+ P_TASK_NAME +`'') AND BATCH_ID=` + V_BATCH_ID +` AND EXECUTION_STATUS=''R''`;
		v_return_value += "\\n V_UPD_ARCH_DML: " + V_UPD_ARCH_DML;

		var V_UPD_ARCH_DML_ST = snowflake.createStatement( {sqlText: V_UPD_ARCH_DML} ).execute();
		
		}

		
		snowflake.execute({ sqlText: "COMMIT;"}); 
		return "SUCCEEDED, Details: " +v_return_value;
		var check_flg=0;
		
		
  }
	catch (err)  
	{
      snowflake.execute({ sqlText: "ROLLBACK;"});
	  var check_flg=1;
	  v_return_value +=  "\\n  Failed: Code: " + err.code + "\\n  State: " + err.state;
      v_return_value += "\\n  Message: " + err.message;
      v_return_value += "\\nStack Trace:\\n" + err.stackTraceTxt;
	  
	}
	if(check_flg==0)
		{
			return "Succeeded" + v_return_value;
			
		}
		else
		{
		throw "FAILED, Details:" + v_return_value;
		}
';



CREATE OR REPLACE PROCEDURE "SP_BATCH_DTLS"("P_APPLICATION_NAME" VARCHAR(16777216), "P_SUBJECT_AREA_NAME" VARCHAR(16777216), "P_STEP" VARCHAR(16777216))
RETURNS VARCHAR(16777216)
LANGUAGE JAVASCRIPT
STRICT
EXECUTE AS OWNER
AS '
 try 
	{
		snowflake.execute({ sqlText: "Begin Transaction;"});
		var return_value="";
			if (P_STEP == ''BATCH_GEN'')
			{
				return_value +="P_STEP: " + P_STEP ;
				var sq1 = `UPDATE COMETL_SFDC_CONTROL.BATCH_RUN_DTLS SET LOAD_STATUS=''F'' WHERE UPPER(SUBJECT_AREA_NAME)=UPPER(''`+ P_SUBJECT_AREA_NAME +`'') AND UPPER(APPLICATION_NAME)=UPPER(''`+ P_APPLICATION_NAME +`'') AND LOAD_STATUS=''R''`;
				return_value += "\\nUPD_BATCH_DTLS_SQL: "+ sq1;
				var st1 = snowflake.createStatement( {sqlText: sq1} ).execute();
				var sq2 = `SELECT BATCH_ID, LOAD_STATUS FROM COMETL_SFDC_CONTROL.BATCH_RUN_DTLS WHERE UPPER(SUBJECT_AREA_NAME)=UPPER(''`+ P_SUBJECT_AREA_NAME +`'') AND UPPER(APPLICATION_NAME)=UPPER(''`+ P_APPLICATION_NAME +`'')  AND BATCH_ID = (SELECT MAX (BATCH_ID) FROM COMETL_SFDC_CONTROL.BATCH_RUN_DTLS WHERE UPPER(SUBJECT_AREA_NAME)=UPPER(''`+ P_SUBJECT_AREA_NAME +`''))`;
				var st2 = snowflake.createStatement( {sqlText: sq2} ).execute();
					if(st2.next())
					{
						MAX_BATCH_ID_VALUE =st2.getColumnValue(1);
						PREV_LOAD_STATUS =st2.getColumnValue(2);
						return_value += "\\nMAX_BATCH_ID_VALUE: " + MAX_BATCH_ID_VALUE;
						return_value += "\\nPREV_LOAD_STATUS: " + PREV_LOAD_STATUS;
						var V_BATCH_ID = MAX_BATCH_ID_VALUE+1;
					}
					else
					{
						var V_BATCH_ID = 1;
					}
				return_value += "\\nBATCH_ID_VALUE: " + V_BATCH_ID;
				var sq3 = `INSERT INTO COMETL_SFDC_CONTROL.BATCH_RUN_DTLS (BATCH_ID,APPLICATION_NAME,SUBJECT_AREA_NAME,LOAD_STATUS,LOAD_START_TIME,EXECUTION_DATE) VALUES (`+ V_BATCH_ID +`,''`+ P_APPLICATION_NAME +`'',''`+ P_SUBJECT_AREA_NAME +`'',''R'',CURRENT_DATE(),CURRENT_DATE())`;
				return_value += "\\n V_INS_BATCH_DTLS: " + sq3;
				var st3 = snowflake.createStatement( {sqlText: sq3} ).execute();
			}
			if (P_STEP == ''BATCH_CLS'')
			{
				var sq4 = `SELECT BATCH_ID FROM COMETL_SFDC_CONTROL.BATCH_RUN_DTLS WHERE UPPER(APPLICATION_NAME)=UPPER(''`+ P_APPLICATION_NAME +`'') AND UPPER(SUBJECT_AREA_NAME)=UPPER(''`+ P_SUBJECT_AREA_NAME +`'') AND LOAD_STATUS=''R''`;
				//return sq4;
				var st4 = snowflake.createStatement( {sqlText: sq4} ).execute();
					if(st4.next())
					{
						V_CURR_BATCH=st4.getColumnValue(1);
						var sq5 = `UPDATE COMETL_SFDC_CONTROL.BATCH_RUN_DTLS SET LOAD_STATUS=''C'', LOAD_END_TIME=CURRENT_DATE() WHERE UPPER(SUBJECT_AREA_NAME)=UPPER(''`+ P_SUBJECT_AREA_NAME +`'') AND UPPER(APPLICATION_NAME)=UPPER(''`+ P_APPLICATION_NAME +`'') AND LOAD_STATUS=''R''AND BATCH_ID=`+ V_CURR_BATCH +``;
						return_value += "\\n V_UPD_BATCH_DTLS2: " + sq5;
						var st5 = snowflake.createStatement( {sqlText: sq5} ).execute();
					}
					else
					{
						return_value+=''No batch is open for this interface. Hence the batch can not be closed''
						throw return_value;
					}
			}
		snowflake.execute({ sqlText: "COMMIT;"}); 
		return "SUCCEEDED, Details: " +return_value;
		var check_flg=0;
	}
 catch (err)  
	{
      snowflake.execute({ sqlText: "ROLLBACK;"});
	  var check_flg=1;
	  return_value +=  "\\n  Failed: Code: " + err.code + "\\n  State: " + err.state;
      return_value += "\\n  Message: " + err.message;
      return_value += "\\nStack Trace:\\n" + err.stackTraceTxt;
	  
	}
	if(check_flg==0)
		{
			return "Succeeded" + return_value;
			//return return_value + "Succeeded";
		}
		else
		{
		throw "FAILED, Details:" + return_value;
		}
';



CREATE OR REPLACE PROCEDURE "SP_DATA_VALDTN_INS"("P_APPLICATION_NAME" VARCHAR(16777216), "P_SUBJECT_AREA_NAME" VARCHAR(16777216), "P_INTERFACE_NAME" VARCHAR(16777216), "P_OBJECT_LAYER" VARCHAR(16777216))
RETURNS VARCHAR(16777216)
LANGUAGE JAVASCRIPT
STRICT
EXECUTE AS OWNER
AS '
 try 
	{
		snowflake.execute({ sqlText: "Begin Transaction;"});
		var v_return_value="";
		
		var V_AUDIT_INS_SQ = `CALL COMETL_SFDC_CONTROL.SP_AUDIT_LOG(''`+P_APPLICATION_NAME+`'', ''`+P_SUBJECT_AREA_NAME+`'', ''`+P_INTERFACE_NAME+`'', ''SP_DATA_VALDTN_INS'', ''Procedure'', ''R'', 0,0,''NA'' )`;
		v_return_value += "\\n V_AUDIT_INS_SQ: " + V_AUDIT_INS_SQ;
		var V_AUDIT_INS_ST = snowflake.createStatement( {sqlText: V_AUDIT_INS_SQ} ).execute();
			
		var V_SQ1 = `SELECT BATCH_ID FROM COMETL_SFDC_CONTROL.BATCH_RUN_DTLS WHERE UPPER(SUBJECT_AREA_NAME)=UPPER(''`+ P_SUBJECT_AREA_NAME +`'') AND UPPER(APPLICATION_NAME)=UPPER(''`+ P_APPLICATION_NAME +`'') AND LOAD_STATUS = ''R''`;
		var V_ST1 = snowflake.createStatement( {sqlText: V_SQ1} ).execute();
		if(V_ST1.next())
		{
			var V_BATCH_ID = V_ST1.getColumnValue(1);
			v_return_value += "\\nV_BATCH_ID: " + V_BATCH_ID;
		}
		else
		{
			v_return_value +=''No batch is open for the interface. Open a new batch.''
			throw v_return_value;
		}
		var V_SQ2 = `SELECT DISTINCT SRC_TABLE_SCHEMA, SRC_TABLENM FROM COMETL_SFDC_CONTROL.DATA_VLDTN_RULE WHERE IS_ACTIVE = ''Y''
            AND UPPER(INTERFACE_NAME) = UPPER(''` + P_INTERFACE_NAME + `'')
            AND UPPER(SUBJECT_AREA_NAME) = UPPER(''` + P_SUBJECT_AREA_NAME + `'')
			AND UPPER(APPLICATION_NAME) = UPPER(''` + P_APPLICATION_NAME + `'')
            AND UPPER(OBJECT_LAYER)= UPPER(''` + P_OBJECT_LAYER + `'')`;
		v_return_value += "\\n V_SQ2: " + V_SQ2;
		var V_ST2 = snowflake.createStatement( {sqlText: V_SQ2} ).execute();
		if(V_ST2.next())
		{
			var V_SRC_TABLE_SCHEMA = V_ST2.getColumnValue(1);
			v_return_value += "\\n V_SRC_TABLE_SCHEMA: " + V_SRC_TABLE_SCHEMA;
			var V_SRC_TABLENM = V_ST2.getColumnValue(2);
			v_return_value += "\\n V_SRC_TABLENM: " + V_SRC_TABLENM;
		}
		var V_TABLE_NM = V_SRC_TABLE_SCHEMA+`.`+V_SRC_TABLENM;
		v_return_value += "\\n V_TABLE_NM: " + V_TABLE_NM;
		var V_SQ3 = `UPDATE `+ V_TABLE_NM +` SET ERROR_FLAG = NULL  WHERE BATCH_ID=`+ V_BATCH_ID+``;
		v_return_value += "\\n V_SQ3: " + V_SQ3;
		var V_ST3 = snowflake.createStatement( {sqlText: V_SQ3} ).execute();	
		
		var V_SQ4 = `DELETE FROM  COMETL_SFDC_CONTROL.ERROR_DTLS WHERE BATCH_ID=`+ V_BATCH_ID +` AND SUBJECT_AREA_NAME = ''` + P_SUBJECT_AREA_NAME +`'' AND APPLICATION_NAME = ''` + P_APPLICATION_NAME +`''
		AND OBJECT_LAYER =''` + P_OBJECT_LAYER +`'' AND INTERFACE_NAME =''` + P_INTERFACE_NAME +`''`;
		
		var V_ST4 = snowflake.createStatement( {sqlText: V_SQ4} ).execute();
		
		var V_SQ5 =`SELECT RULE_ID,VALIDATION_COLUMN_NM,RULE_SQL,RULE_DESC,RULE_TYPE,ERROR_MSG
        FROM COMETL_SFDC_CONTROL.DATA_VLDTN_RULE WHERE IS_ACTIVE = ''Y'' AND UPPER(INTERFACE_NAME) = UPPER(''` + P_INTERFACE_NAME + `'') AND UPPER(SUBJECT_AREA_NAME) = UPPER(''` + P_SUBJECT_AREA_NAME +`'') AND UPPER(APPLICATION_NAME) = UPPER(''` + P_APPLICATION_NAME +`'') AND UPPER(OBJECT_LAYER)= UPPER(''` + P_OBJECT_LAYER +`'')`;
		var V_ST5 = snowflake.createStatement( {sqlText: V_SQ5} ).execute();
		
		while (V_ST5.next()){
		
		var V_RULE_ID = V_ST5.getColumnValue(1);
		var V_VALIDATION_COLUMN_NM = V_ST5.getColumnValue(2);
		var V_WHERE_COND = V_ST5.getColumnValue(3);
		var V_RULE_DESC = V_ST5.getColumnValue(4);
		var V_RULE_TYPE = V_ST5.getColumnValue(5);
		var V_ERROR_MSG = V_ST5.getColumnValue(6);
		
		v_return_value += "\\n V_RULE_ID: " + V_RULE_ID;
		v_return_value += "\\n V_VALIDATION_COLUMN_NM: " + V_VALIDATION_COLUMN_NM;
		v_return_value += "\\n V_WHERE_COND: " + V_WHERE_COND;
		v_return_value += "\\n V_RULE_DESC: " + V_RULE_DESC;
		v_return_value += "\\n V_RULE_TYPE: " + V_RULE_TYPE;
		v_return_value += "\\n V_ERROR_MSG: " + V_ERROR_MSG;
		
		var V_INPUT_PARAM = P_APPLICATION_NAME + `P_SUBJECT_AREA_NAME + `,`+ P_INTERFACE_NAME + `,` + P_OBJECT_LAYER;
		var	V_WHERE_CLAUSE = `(` + V_WHERE_COND +`)`;
		
		v_return_value += "\\n V_INPUT_PARAM: " + V_INPUT_PARAM;
		v_return_value += "\\n V_WHERE_CLAUSE: " + V_WHERE_CLAUSE;
		
		var V_SRC_CNT_SQ=`SELECT COUNT(*) FROM `+ V_TABLE_NM +` WHERE ` + V_WHERE_CLAUSE +` AND BATCH_ID =` + V_BATCH_ID + ``;
		var V_SRC_CNT_ST  = snowflake.createStatement( {sqlText: V_SRC_CNT_SQ} ).execute();
		if(V_SRC_CNT_ST.next())
		{
			var V_SRC_CNT = V_SRC_CNT_ST.getColumnValue(1);
			v_return_value += "\\n V_SRC_CNT: " + V_SRC_CNT;
			
		}
		
		var V_SQ6 =`INSERT INTO COMETL_SFDC_CONTROL.ERROR_DTLS SELECT
		` + V_BATCH_ID +`,` + V_RULE_ID + `, ''` + P_APPLICATION_NAME + `'', ''` + P_SUBJECT_AREA_NAME + `'', ''` + P_INTERFACE_NAME + `'', ''` + P_OBJECT_LAYER + `'', UNIQUE_ROW_ID, ''` + V_SRC_TABLE_SCHEMA + `'',''` + V_SRC_TABLENM + `'',''` + V_ERROR_MSG +`'', CURRENT_DATE(),  ''ETL_USER''  FROM `+ V_TABLE_NM +` WHERE ` + V_WHERE_CLAUSE +` AND BATCH_ID =` + V_BATCH_ID + ``;
		
		v_return_value += "\\n V_SQ6: " + V_SQ6;
		
		var V_ST6 = snowflake.createStatement( {sqlText: V_SQ6} ).execute();
		
		var V_SQ7 =`SELECT COUNT(*) FROM COMETL_SFDC_CONTROL.ERROR_DTLS WHERE BATCH_ID=` + V_BATCH_ID +` AND UPPER(INTERFACE_NAME) = UPPER(''` + P_INTERFACE_NAME +`'') AND AND UPPER(APPLICATION_NAME) = UPPER(''` + P_APPLICATION_NAME + `'') AND UPPER(SUBJECT_AREA_NAME) = UPPER(''` + P_SUBJECT_AREA_NAME + `'') AND UPPER(SRC_TABLENM)= UPPER(''` + V_SRC_TABLENM + `'') AND RULE_ID =` + V_RULE_ID +``;
		var V_ST7  = snowflake.createStatement( {sqlText: V_SQ7} ).execute();
		if(V_ST7.next())
		{
			var V_INS_COUNT = V_ST7.getColumnValue(1);
			v_return_value += "\\n V_INS_COUNT: " + V_INS_COUNT;
			
		}
		
		if( V_SRC_CNT != V_INS_COUNT )
		{
		v_return_value +="The actual error records count and error table record counts are different";
		throw v_return_value;
		}
  }
		snowflake.execute({ sqlText: "COMMIT;"}); 
		var V_AUDIT_UPD_COMP_SQ = `CALL COMETL_SFDC_CONTROL.SP_AUDIT_LOG(''`+ P_APPLICATION_NAME +`'', ''`+ P_SUBJECT_AREA_NAME +`'', ''`+P_INTERFACE_NAME+`'', ''SP_DATA_VALDTN_INS'', ''Procedure'', ''C'', 0,0, ''NA'' )`;
		var V_AUDIT_UPD_COMP_ST = snowflake.createStatement( {sqlText: V_AUDIT_UPD_COMP_SQ} ).execute();
		return "SUCCEEDED, Details: " +v_return_value;
		var check_flg=0;
  }
	catch (err)  
	{
      snowflake.execute({ sqlText: "ROLLBACK;"});
	  var check_flg=1;
	  v_return_value +=  "\\n  Failed: Code: " + err.code + "\\n  State: " + err.state;
      v_return_value += "\\n  Message: " + err.message;
      v_return_value += "\\nStack Trace:\\n" + err.stackTraceTxt;
	  var V_AUDIT_UPD_FAIL_SQ = `CALL COMETL_SFDC_CONTROL.SP_AUDIT_LOG(''`+ P_APPLICATION_NAME +`'', ''`+ P_SUBJECT_AREA_NAME +`'', ''`+P_INTERFACE_NAME+`'', ''SP_DATA_VALDTN_INS'', ''Procedure'', ''F'', 0,0, ''`+v_return_value +`'' )`;
	  var V_AUDIT_UPD_FAIL_ST = snowflake.createStatement( {sqlText: V_AUDIT_UPD_FAIL_SQ} ).execute();
	  
	}
	if(check_flg==0)
		{
			
			return "Succeeded" + v_return_value;
					
		}
		else
		{
		
		throw "FAILED, Details:" + v_return_value;
		}
';



CREATE OR REPLACE PROCEDURE "SP_DATA_VALDTN_UPD"("P_APPLICATION_NAME" VARCHAR(16777216), "P_SUBJECT_AREA_NAME" VARCHAR(16777216), "P_INTERFACE_NAME" VARCHAR(16777216), "P_OBJECT_LAYER" VARCHAR(16777216))
RETURNS VARCHAR(16777216)
LANGUAGE JAVASCRIPT
STRICT
EXECUTE AS OWNER
AS '
 try 
	{
		snowflake.execute({ sqlText: "Begin Transaction;"});
		var return_value="";
		var SQ_BATCH_ID = `SELECT BATCH_ID FROM COMETL_SFDC_CONTROL.BATCH_RUN_DTLS WHERE UPPER(SUBJECT_AREA_NAME)=UPPER(''`+ P_SUBJECT_AREA_NAME +`'') AND UPPER(APPLICATION_NAME)=UPPER(''`+ P_APPLICATION_NAME +`'') AND LOAD_STATUS=''R''`;
		//return SQ_BATCH_ID;
		var ST_BATCH_ID = snowflake.createStatement( {sqlText: SQ_BATCH_ID} ).execute();
			if(ST_BATCH_ID.next())
			{
				V_BATCH_ID=ST_BATCH_ID.getColumnValue(1);
				return_value += "\\n V_BATCH_ID: " + V_BATCH_ID;
			}
			else
			{
				return_value+=''No batch_id is open for the interface.''
				throw return_value;
			}
		var SQ_TABLE_NM = `select distinct SRC_TABLENM FROM COMETL_SFDC_CONTROL.DATA_VLDTN_RULE WHERE IS_ACTIVE = ''Y'' AND UPPER(INTERFACE_NAME) = UPPER(''`+ P_INTERFACE_NAME +`'') AND UPPER(APPLICATION_NAME) = UPPER(''`+ P_APPLICATION_NAME +`'') AND UPPER(SUBJECT_AREA_NAME) = UPPER(''`+ P_SUBJECT_AREA_NAME +`'') AND UPPER(OBJECT_LAYER)= UPPER(''`+ P_OBJECT_LAYER +`'')`;
		//return SQ_TABLE_NM;
		var ST_TABLE_NM = snowflake.createStatement( {sqlText: SQ_TABLE_NM} ).execute();
			if(ST_TABLE_NM.next())
			{
				V_TABLE_NAME=ST_TABLE_NM.getColumnValue(1);
				return_value += "\\n V_TABLE_NAME: " + V_TABLE_NAME;
			}
			else
			{
				return_value+="\\n V_TABLE_NAME: No Data Found!";
				throw return_value;
			}
		var SQ_SCHEMA_NM = `SELECT distinct SRC_TABLE_SCHEMA FROM COMETL_SFDC_CONTROL.DATA_VLDTN_RULE WHERE  UPPER(INTERFACE_NAME)=UPPER(''`+ P_INTERFACE_NAME +`'') AND UPPER(SRC_TABLENM) = UPPER(''`+ V_TABLE_NAME +`'')`;
		//return SQ_SCHEMA_NM;
		var ST_SCHEMA_NM = snowflake.createStatement( {sqlText: SQ_SCHEMA_NM} ).execute();
			if(ST_SCHEMA_NM.next())
			{
				V_SRC_TABLE_SCHEMA=ST_SCHEMA_NM.getColumnValue(1);
				return_value += "\\n V_SRC_TABLE_SCHEMA: " + V_SRC_TABLE_SCHEMA;
			}
			else
			{
				return_value+="\\n V_SRC_TABLE_SCHEMA: No Data Found!";
				throw return_value;
			}
		V_INPUT_PARAM = P_INTERFACE_NAME+`,`+P_INTERFACE_NAME+`,`+P_OBJECT_LAYER;
		return_value += "\\n V_INPUT_PARAM: " + V_INPUT_PARAM;
		V_TABLE_NM = V_SRC_TABLE_SCHEMA+`.`+V_TABLE_NAME;
		return_value += "\\n V_TABLE_NM: " + V_TABLE_NM;
		var SQ_ERR_UPD_STATEMENT_Y = `UPDATE `+ V_TABLE_NM +` SET ERROR_FLAG =''Y'' WHERE UNIQUE_ROW_ID in (Select distinct SRC_UNIQUE_KEY from COMETL_SFDC_CONTROL.ERROR_DTLS where BATCH_ID =`+ V_BATCH_ID +` AND UPPER(SUBJECT_AREA_NAME)= UPPER(''`+ P_SUBJECT_AREA_NAME +`'') AND UPPER(APPLICATION_NAME)= UPPER(''`+ P_APPLICATION_NAME +`'') AND UPPER(INTERFACE_NAME)=UPPER(''`+ P_INTERFACE_NAME +`'') AND UPPER(OBJECT_LAYER)=UPPER(''`+ P_OBJECT_LAYER +`''))`;
		return_value += "\\n SQ_ERR_UPD_STATEMENT_Y: " + SQ_ERR_UPD_STATEMENT_Y;

		//return SQ_ERR_UPD_STATEMENT_Y;
		var V_ERR_UPD_STATEMENT_Y = snowflake.createStatement( {sqlText: SQ_ERR_UPD_STATEMENT_Y} ).execute();
		var SQ_ERR_UPD_STATEMENT_N = `UPDATE `+ V_TABLE_NM +` SET ERROR_FLAG =''N'' WHERE BATCH_ID =`+ V_BATCH_ID +` AND ERROR_FLAG IS NULL`;
		return_value += "\\n SQ_ERR_UPD_STATEMENT_N: " + SQ_ERR_UPD_STATEMENT_N;
		var V_ERR_UPD_STATEMENT_N = snowflake.createStatement( {sqlText: SQ_ERR_UPD_STATEMENT_N} ).execute();
		
		return_value += "\\n Fetch respective flag update counts";
		var SQ_ERROR_REC_CNT_Y = `select count(*) FROM `+ V_TABLE_NM +` WHERE ERROR_FLAG = ''Y'' AND BATCH_ID = `+ V_BATCH_ID +``;
		//return SQ_ERROR_REC_CNT_Y;
		var ST_ERROR_REC_CNT_Y = snowflake.createStatement( {sqlText: SQ_ERROR_REC_CNT_Y} ).execute();
			if(ST_ERROR_REC_CNT_Y.next())
			{
				V_ERROR_REC_CNT_Y=ST_ERROR_REC_CNT_Y.getColumnValue(1);
				return_value += "\\n V_ERROR_REC_CNT_Y: " + V_ERROR_REC_CNT_Y;
			}
		var SQ_ERROR_REC_CNT_N = `select count(*) FROM `+ V_TABLE_NM +` WHERE ERROR_FLAG = ''N'' AND BATCH_ID = `+ V_BATCH_ID +``;
		//return SQ_ERROR_REC_CNT_N;
		var ST_ERROR_REC_CNT_N = snowflake.createStatement( {sqlText: SQ_ERROR_REC_CNT_N} ).execute();
			if(ST_ERROR_REC_CNT_N.next())
			{
				V_ERROR_REC_CNT_N=ST_ERROR_REC_CNT_N.getColumnValue(1);
				return_value += "\\n V_ERROR_REC_CNT_N: " + V_ERROR_REC_CNT_N;
			}
		var SQ_INSRT_AUDIT_LOG_Y = `Insert into COMETL_SFDC_CONTROL.DATA_VLDTN_AUDIT_DTLS(BATCH_ID,RULE_ID,APPLICATION_NAME,SUBJECT_AREA_NAME,INTERFACE_NAME,OBJECT_LAYER,SP_NM,SP_INPUT_PARM,RECORD_COUNT,RULE_EXEC_STATUS,RULE_EXEC_DESC,OPRATION_TYPE,RULE_EXEC_START_TIME,RULE_EXEC_END_DATE,LAST_MODIFIED_DATE,LAST_MODIFIED_BY) values(`+ V_BATCH_ID +`,NULL,''`+ P_APPLICATION_NAME +`'',''`+ P_SUBJECT_AREA_NAME +`'',''`+ P_INTERFACE_NAME +`'',''`+ P_OBJECT_LAYER +`'',''SP_ICUE_STG_UPDATE'',''`+ V_INPUT_PARAM +`'',''`+ V_ERROR_REC_CNT_Y +`'',''C'',''Records with errors updated to Y'',''U'',CURRENT_DATE(),CURRENT_DATE(),CURRENT_DATE(),''ETL_USER'')`;
		//return SQ_INSRT_AUDIT_LOG_Y;
		return_value += "\\n SQ_INSRT_AUDIT_LOG_Y: " + SQ_INSRT_AUDIT_LOG_Y;
		var V_INSRT_AUDIT_LOG_Y = snowflake.createStatement( {sqlText: SQ_INSRT_AUDIT_LOG_Y} ).execute();
		
		
		var SQ_INSRT_AUDIT_LOG_N = `Insert into COMETL_SFDC_CONTROL.DATA_VLDTN_AUDIT_DTLS(BATCH_ID,RULE_ID,APPLICATION_NAMESUBJECT_AREA_NAME,INTERFACE_NAME,OBJECT_LAYER,SP_NM,SP_INPUT_PARM,RECORD_COUNT,RULE_EXEC_STATUS,RULE_EXEC_DESC,OPRATION_TYPE,RULE_EXEC_START_TIME,RULE_EXEC_END_DATE,LAST_MODIFIED_DATE,LAST_MODIFIED_BY) values(`+ V_BATCH_ID +`,NULL,''`+ P_APPLICATION_NAME +`'',''`+ P_SUBJECT_AREA_NAME +`'',''`+ P_INTERFACE_NAME +`'',''`+ P_OBJECT_LAYER +`'',''SP_ICUE_STG_UPDATE'',''`+  V_INPUT_PARAM +`'',''`+ V_ERROR_REC_CNT_N +`'',''C'',''Records with errors updated to N'',''U'',CURRENT_DATE(),CURRENT_DATE(),CURRENT_DATE(),''ETL_USER'')`;
		//return SQ_INSRT_AUDIT_LOG_N;
		return_value += "\\n SQ_INSRT_AUDIT_LOG_N: " + SQ_INSRT_AUDIT_LOG_N;
		var V_INSRT_AUDIT_LOG_N = snowflake.createStatement( {sqlText: SQ_INSRT_AUDIT_LOG_N} ).execute();
		
		
		snowflake.execute({ sqlText: "COMMIT;"}); 
		return "SUCCEEDED, Details: " +return_value;
		var check_flg=0;
	}
 catch (err)  
	{
      snowflake.execute({ sqlText: "ROLLBACK;"});
	  var check_flg=1;
	  return_value +=  "\\n  Failed: Code: " + err.code + "\\n  State: " + err.state;
      return_value += "\\n  Message: " + err.message;
      return_value += "\\nStack Trace:\\n" + err.stackTraceTxt;
	  
	}
	if(check_flg==0)
		{
			return "Succeeded" + return_value;
			//return return_value + "Succeeded";
		}
		else
		{
		throw "FAILED, Details:" + return_value;
		}
';



CREATE OR REPLACE PROCEDURE "SP_FMR_DATA_LOAD"("P_TABLE_NAME" VARCHAR(16777216))
RETURNS VARCHAR(16777216)
LANGUAGE JAVASCRIPT
STRICT
EXECUTE AS OWNER
AS '
 try 
	{
	  
	  var return_value="";
	  
	  var V_AUDIT_INS_SQ = `CALL COMETL_SFDC_CONTROL.SP_AUDIT_LOG(''ICUE'',''FMR_US'',''`+P_TABLE_NAME+`'',''SP_FMR_DATA_LOAD'',''PROCEDURE'', ''R'', null,null,''NA'' )`;
				
	  var V_AUDIT_INS_ST = snowflake.createStatement( {sqlText: V_AUDIT_INS_SQ} ).execute();
	  return_value += "\\n Audit record for the SP execution is inserted";
	  snowflake.execute({ sqlText: "Begin Transaction;"});
	  var1 = snowflake.createStatement({sqlText: 
     `select INTERFACE_NAME, MIN(STTM_SEQ) MIN_STTM_SEQ, MAX(STTM_SEQ) MAX_STTM_SEQ from COMETL_SFDC_CONTROL.FMR_STTM_CONTROL where UPPER(TARGET_TABLE_NAME)=UPPER(''`+ P_TABLE_NAME +`'') group by INTERFACE_NAME`
      }). execute();
      var1.next();
	 
	  P_INTERFACE_NAME=var1.getColumnValue(1);
	  P_MIN_STTM_SEQ=var1.getColumnValue(2);	 
	  P_MAX_STTM_SEQ=var1.getColumnValue(3);
	  
	  return_value +="\\n P_INTERFACE_NAME: " +P_INTERFACE_NAME;
	  return_value +="\\n P_MIN_STTM_SEQ: " +P_MIN_STTM_SEQ;
	  return_value +="\\n P_MAX_STTM_SEQ: " +P_MAX_STTM_SEQ;
	 
	  var batch_id_sql = `SELECT BATCH_ID FROM COMETL_SFDC_CONTROL.BATCH_RUN_DTLS WHERE UPPER(SUBJECT_AREA_NAME)=UPPER(''`+ P_INTERFACE_NAME +`'') AND LOAD_STATUS=''R''`;
	  var batch_st = snowflake.createStatement( {sqlText: batch_id_sql} ).execute();
	  batch_st.next();
	 
	  P_CURRENT_BATCH_ID=batch_st.getColumnValue(1);
	  return_value +="\\n P_CURRENT_BATCH_ID:" +P_CURRENT_BATCH_ID;
	  
	  while (P_MIN_STTM_SEQ <= P_MAX_STTM_SEQ) 
             {
				sttm_to_be_exec = snowflake.createStatement({sqlText: 
			   `select EXEC_STTM from COMETL_SFDC_CONTROL.FMR_STTM_CONTROL where UPPER(INTERFACE_NAME)=UPPER(''`+ P_INTERFACE_NAME +`'') and UPPER(TARGET_TABLE_NAME)=UPPER(''`+ P_TABLE_NAME +`'') and STTM_SEQ=`+P_MIN_STTM_SEQ+`;`}).execute();
        
				sttm_to_be_exec.next();
         	    p_exec_sttm=sttm_to_be_exec.getColumnValue(1);
				
				snowflake.execute 
				({sqlText: ``+ p_exec_sttm +``, binds:[P_CURRENT_BATCH_ID,P_CURRENT_BATCH_ID,P_CURRENT_BATCH_ID,P_CURRENT_BATCH_ID,P_CURRENT_BATCH_ID,P_CURRENT_BATCH_ID]
				});
						
				return_value += "\\n STATEMENT SEQUENCE EXECUTED SUCCESSFULLY:" +P_MIN_STTM_SEQ;
				return_value += "\\n STATEMENT EXECUTED SUCCESSFULLY:" +p_exec_sttm;
				P_MIN_STTM_SEQ++;
                
             }
	  snowflake.execute({ sqlText: "COMMIT;"}); 
	  var V_AUDIT_UPD_COMP_SQ = `CALL COMETL_SFDC_CONTROL.SP_AUDIT_LOG(''ICUE'',''FMR_US'',''`+P_TABLE_NAME+`'',''SP_FMR_DATA_LOAD'',''PROCEDURE'', ''C'', null,null, ''NA'' )`;
		var V_AUDIT_UPD_COMP_ST = snowflake.createStatement( {sqlText: V_AUDIT_UPD_COMP_SQ} ).execute();
	  return "SUCCEEDED, Details: " +return_value;
	  var check_flg=0;
	  return_value += "\\n Audit record for the SP execution is updated";

	}
 catch (err)  
	{
      snowflake.execute({ sqlText: "ROLLBACK;"});
	  var check_flg=1;
	  return_value +=  "\\n  Failed: Code: " + err.code + "\\n  State: " + err.state;
      return_value += "\\n  Message: " + err.message;
      return_value += "\\nStack Trace:\\n" + err.stackTraceTxt;
	  var output_return_value=return_value.replace(/''/g,"''''");  
		
	  var V_AUDIT_UPD_FAIL_SQ = `CALL COMETL_SFDC_CONTROL.SP_AUDIT_LOG(''ICUE'',''FMR'',''`+P_TABLE_NAME+`'',''SP_FMR_DATA_LOAD'',''PROCEDURE'', ''F'', null,null, ''`+output_return_value +`'' )`;
	  
		var V_AUDIT_UPD_FAIL_ST = snowflake.createStatement( {sqlText: V_AUDIT_UPD_FAIL_SQ} ).execute();
	}
	if(check_flg==0)
		{
			return "Succeeded" + return_value;
			
		}
		else
		{
		throw "FAILED, Details:" + return_value;
		}
';



CREATE OR REPLACE PROCEDURE "SP_FMR_US_ALIGNMENT_LOAD"("P_APPLICATION_NAME" VARCHAR(16777216), "P_SUBJECT_AREA_NAME" VARCHAR(16777216))
RETURNS VARCHAR(16777216)
LANGUAGE JAVASCRIPT
STRICT
EXECUTE AS OWNER
AS '
 try 
	{
	  
		snowflake.execute({ sqlText: "Begin Transaction;"});
		var return_value="";
		
		var SQ_PARAM_FETCH = `SELECT (LISTAGG (PARAM_NAME|| (CASE WHEN PARAM_VALUE LIKE ''%NULL'' THEN '' IS '' ELSE ''='' END) ||CHR(39)|| PARAM_VALUE ||CHR(39), '' AND '') WITHIN GROUP (ORDER BY PARAM_VALUE)) AS PARAM_VAL
		FROM COMETL_SFDC_CONTROL.PARAMETER_TABLE
		WHERE ACTIVE_FLAG = ''Y'' AND UPPER(APPLICATION_NAME) = UPPER(''`+ P_APPLICATION_NAME +`'') AND UPPER(SUBJECT_AREA_NAME) = UPPER(''`+ P_SUBJECT_AREA_NAME +`'') GROUP BY TASK_NAME`;
		
		var ST_PARAM_FETCH = snowflake.createStatement( {sqlText: SQ_PARAM_FETCH} ).execute();
		
		if(ST_PARAM_FETCH.next())
			{
				V_PARAM_VALUE=ST_PARAM_FETCH.getColumnValue(1);
				return_value += "\\nV_PARAM_VALUE: " + V_PARAM_VALUE;
			}
			else
			{
				return_value+=''Error fetching parameter value from COMETL_SFDC_CONTROL.PARAMETER_TABLE''
				throw return_value;
			}
            
            var sq_trunc = `TRUNCATE TABLE COMETL_SFDC_STAGING.ICUE_FMR_ALIGNMENT_FINAL`;
            var st_trunc = snowflake.createStatement( {sqlText: sq_trunc} ).execute();
            st_trunc.next()
            return_value += ''TABLE COMETL_SFDC_STAGING.ICUE_FMR_ALIGNMENT_FINAL TRUNCATED SUCCESSFULLY  ''

			var sq1 =`INSERT INTO COMETL_SFDC_STAGING.ICUE_FMR_ALIGNMENT_FINAL
						SELECT DISTINCT BATCH_ID, UNIQUE_ROW_ID, CPA_SOURCE_CODE, OM_EXPLICIT_ALIGNMENT_PFI__C,  TERRITORY_ID, TERRITORY_NAME, USER_ID,
						PFIZER_CUSTOMER_ID, ACCOUNT_ID, ACCOUNT_FIRSTNAME, ACCOUNT_LASTNAME,
						ACCOUNT_EMAIL1_PFI__C, USER_FIRSTNAME, USER_LASTNAME, USER_EMAIL__C,
						USER_PHONE__C, USER_USERNAME, USER_EXTERNAL_ID_PFI__C, USER_NETWORK_ID,
						VEEVA_MSP_ID, VEEVA_PRODUCT_VOD__C, VEEVA_MSP_EXTERNAL_ID, VEEVA_PRE_USER__C, 
						VEEVA_FPRODUCTNAME_COE__C, VEEVA_PRE_FFRA_DETAIL_FLAG__C, PRODUCT_NAME, PRODUCTID,
						''US'' AS COUNTRY_CODE, SYSDATE() AS CREATED_DATE, ''ETL'' AS CREATED_BY
						FROM COMETL_SFDC_STAGING.ICUE_FMR_OM_VEEVA_ALIGNMENT
						WHERE `+V_PARAM_VALUE+` `;
			
			var st1 = snowflake.createStatement( {sqlText: sq1} ).execute()
			if(st1.next())
			{
				return_value += ''Alignment table with specified alignments is created''	
			}
			else 
			{
				return_value+=''Error in creating Alignment table''
			}
			
			
			snowflake.execute({ sqlText: "COMMIT;"});
			
		return "SUCCEEDED, Details: " +return_value;
		var check_flg=0;
	}
 catch (err)  
	{
      snowflake.execute({ sqlText: "ROLLBACK;"});
	  var check_flg=1;
	  return_value +=  "\\n  Failed: Code: " + err.code + "\\n  State: " + err.state;
      return_value += "\\n  Message: " + err.message;
      return_value += "\\nStack Trace:\\n" + err.stackTraceTxt;
	  
	}
	if(check_flg==0)
		{
			return "Succeeded" + return_value;
			//return return_value + "Succeeded";
		}
		else
		{
		throw "FAILED, Details:" + return_value;
		}
';

