package de.coac.robots.saifty.lake;

import java.sql.SQLException;
import java.sql.Statement;
import java.sql.ResultSet;

public class Integration {
    public static void estmjIntegration(Statement stmt) throws SQLException {
        Operations.materializeTable(stmt, "saifty_3_integration", "intg_product_material", " intg_v_product_material");
        String insert = "INSERT INTO saifty_4_core.PRODUCT_MATERIAL (	SYST_ID ,"
                + "	PRODUCT_ID, "
                + "	TYPE ,"
                + "	SUBSTANCE ,"
                + "	RECN,"
                + "	ACTN,"
                + "	VALID_FROM , "
                + "	VALID_TO , "
                + "	AENNR , "
                + "	DELFLG, "
                + "	PARKFLG, "
                + "	CRDAT , "
                + "	CRNAM , "
                + "	UPDDAT , "
                + "	UPDNAM , "
                + "	SRSID , "
                + "	OWNID , "
                + "	SUB_RECORD_NO , "
                + "	WERKS, "
                + "	MATNR,"
                + "	SAIFTY_CREATED_ON , "
                + "	NUMBER) "
                + "(select * from saifty_3_integration.INTG_PRODUCT_MATERIAL)";
        int res = stmt.executeUpdate(insert);
    }

    public static void estdhIntegration(Statement stmt) throws SQLException {
        Operations.materializeTable(stmt, "saifty_3_integration", "intg_estdh", "intg_v_estdh");
        Operations.deletionLogic("saifty_3_integration.intg_estdh", "saifty_3_integration.intg_estdh", "saifty_4_core.estdh", "product_id", stmt);


        String estdh = " INSERT INTO saifty_4_core.ESTDH "
                + "(SYST_ID,  "
                + "PRODUCT_ID  ,  "
                + "RECORD_NO ,  "
                + "CHNGSTATUS ,  "
                + "VALID_FROM ,  "
                + "VALID_TO ,  "
                + "CHANG_NO,  "
                + "DEL_IND,  "
                + "PARK_IND,  "
                + "CREATED_ON ,  "
                + "CREATED_BY,  "
                + "CHANGD_ON ,  "
                + "CHANGD_BY,  "
                + "DATAORIGIN,  "
                + "OWNER_ID,  "
                + "RECNO_ROOT ,  "
                + "REPORT_VARIANT ,  "
                + "LANGU  ,  "
                + "EHSDOC_TYPE  ,  "
                + "REPORT_VERSION ,  "
                + "REPORT_TYPE,  "
                + "REPORT_STATUS,  "
                + "DOC_SYSTEM_TYPE,  "
                + "DOC_GENERIC_KEY,  "
                + "VERSION ,  "
                + "SUBVERSION ,  "
                + "VALIDITY_DATE ,  "
                + "REASON_REQUEST,  "
                + "RELEVANT_CHANGE_MADE,  "
                + "NOTE,  "
                + "MAPS_CREATED_ON ) "
                + " (SELECT * from saifty_3_integration.intg_estdh) ";
        int res = stmt.executeUpdate(estdh);
    }

    public static void identifierIntegration(Statement stmt) throws SQLException {
        Operations.materializeTable(stmt, "saifty_3_integration", "INTG_IDENTIFIER", "INTG_V_IDENTIFIER");
        Operations.deletionLogic("saifty_3_integration.intg_identifier", "saifty_4_core.identifier", "product_id", "product_id", stmt);
        String identifier = " INSERT INTO  saifty_4_core.IDENTIFIER "
                + " ( "
                + " syst_id, "
                + " usag_id, "
                + " product_id, "
                + " id_type, "
                + " id_categry, "
                + " langu_iso, "
                + " created_on, "
                + " created_by, "
                + " changd_on, "
                + " changd_by, "
                + " subcategry, "
                + " identifier, "
                + " long_text, "
                + " sequence, "
                + " is_source_of_a_heredity, "
                + " saifty_created_on, "
                + " text_line, "
                + " dataorigin, "
                + " subauthgrp, "
                + " subcharact, "
                + " note, "
                + " record_no "
                + " ) "
                + " (SELECT * from  saifty_3_integration.INTG_IDENTIFIER) ";
        int res = stmt.executeUpdate(identifier);
    }
    public static void  phraseIntegration(Statement stmt) throws SQLException {
        String phrase = " MERGE INTO saifty_4_core.PHRASES AS PHRASES"
                +"  USING ("
                +"      SELECT"
                +" LOOKUP_INPUT_SUBQUERY.SYST_ID1 SYST_ID,"
                +" LOOKUP_INPUT_SUBQUERY.HEADER_RECORD_NO1 HEADER_RECORD_NO,"
                +" LOOKUP_INPUT_SUBQUERY.HEADER_RECNO_ROOT1 HEADER_RECNO_ROOT,"
                +" LOOKUP_INPUT_SUBQUERY.PHRASE1 PHRASE,"
                +" LOOKUP_INPUT_SUBQUERY.PHRLIB1 PHRLIB,"
                +" LOOKUP_INPUT_SUBQUERY.PHRGROUP1 PHRGROUP,"
                +" LOOKUP_INPUT_SUBQUERY.TEXT_RECORD_NO1 TEXT_RECORD_NO,"
                +" LOOKUP_INPUT_SUBQUERY.CREATED_ON1 CREATED_ON,"
                +" LOOKUP_INPUT_SUBQUERY.CREATED_BY1 CREATED_BY,"
                +" LOOKUP_INPUT_SUBQUERY.CHANGD_ON1 CHANGD_ON,"
                +" LOOKUP_INPUT_SUBQUERY.CHANGD_BY1 CHANGD_BY,"
                +" LOOKUP_INPUT_SUBQUERY.DATAORIGIN1 DATAORIGIN,"
                +" LOOKUP_INPUT_SUBQUERY.DATA_PROV1 DATA_PROV,"
                +" LOOKUP_INPUT_SUBQUERY.LANGU_ISO1 LANGU_ISO,"
                +" LOOKUP_INPUT_SUBQUERY.PHRASE_CODE1 PHRASE_CODE,"
                +" LOOKUP_INPUT_SUBQUERY.PHRASE_TEXT1 PHRASE_TEXT,"
                +" LOOKUP_INPUT_SUBQUERY.NOTE1 NOTE,"
                +" LOOKUP_INPUT_SUBQUERY.PHRGRAPHIC1 PHRGRAPHIC,"
                +" LOOKUP_INPUT_SUBQUERY.PPSTATE1 PPSTATE,"
                +" LOOKUP_INPUT_SUBQUERY.FORMAT_FLAG1 FORMAT_FLAG,"
                +" CASE "
                +"     WHEN  LOOKUP_INPUT_SUBQUERY.SAIFTY_CREATED_ON1 is not null then  LOOKUP_INPUT_SUBQUERY.SAIFTY_CREATED_ON1"
                +"     ELSE  LOOKUP_INPUT_SUBQUERY.INSERTED"
                +"   END SAIFTY_CREATED_ON"
                +"        FROM ( "
                +"   SELECT "
                +"       NEW.HEADER_RECORD_NO HEADER_RECORD_NO1,"
                +"       NEW.HEADER_RECNO_ROOT HEADER_RECNO_ROOT1,"
                +"       NEW.PHRASE PHRASE1,"
                +"       NEW.PHRLIB PHRLIB1,"
                +"       NEW.PHRGROUP PHRGROUP1,"
                +"       NEW.TEXT_RECORD_NO TEXT_RECORD_NO1,"
                +"       NEW.CREATED_ON CREATED_ON1,"
                +"       NEW.CREATED_BY CREATED_BY1,"
                +"       NEW.CHANGD_ON CHANGD_ON1,"
                +"       NEW.CHANGD_BY CHANGD_BY1,"
                +"       NEW.DATAORIGIN DATAORIGIN1,"
                +"       NEW.DATA_PROV DATA_PROV1,"
                +"       NEW.LANGU_ISO LANGU_ISO1,"
                +"       NEW.PHRASE_CODE PHRASE_CODE1,"
                +"       NEW.PHRASE_TEXT PHRASE_TEXT1,"
                +"       NEW.NOTE NOTE1,"
                +"       NEW.PHRGRAPHIC PHRGRAPHIC1,"
                +"       NEW.PPSTATE PPSTATE1,"
                +"       NEW.FORMAT_FLAG FORMAT_FLAG1,"
                +"       NEW.INSERTED INSERTED,"
                +"       NEW.SYST_ID SYST_ID1,"
                +"       OLD.SAIFTY_CREATED_ON SAIFTY_CREATED_ON1,"
                +"       OLD.PHRASE_CODE PHRASE_CODE_1,"
                +"       OLD.PHRASE_TEXT PHRASE_TEXT_1"
                +"     FROM ( "
                +"         SELECT "
                +"             SPLITTER_INPUT_SUBQUERY.SYSTEM SYSTEM,"
                +"             SPLITTER_INPUT_SUBQUERY.HEADER_RECORD_NO2 HEADER_RECORD_NO,"
                +"             SPLITTER_INPUT_SUBQUERY.HEADER_RECNO_ROOT2 HEADER_RECNO_ROOT,"
                +"             SPLITTER_INPUT_SUBQUERY.PHRASE2 PHRASE,"
                +"             SPLITTER_INPUT_SUBQUERY.PHRLIB2 PHRLIB,"
                +"             SPLITTER_INPUT_SUBQUERY.PHRGROUP2 PHRGROUP,"
                +"             SPLITTER_INPUT_SUBQUERY.HEADER_LANGU HEADER_LANGU,"
                +"             SPLITTER_INPUT_SUBQUERY.TEXT_RECORD_NO2 TEXT_RECORD_NO,"
                +"             SPLITTER_INPUT_SUBQUERY.CREATED_ON2 CREATED_ON,"
                +"             SPLITTER_INPUT_SUBQUERY.CREATED_BY2 CREATED_BY,"
                +"             SPLITTER_INPUT_SUBQUERY.CHANGD_ON2 CHANGD_ON,"
                +"             SPLITTER_INPUT_SUBQUERY.CHANGD_BY2 CHANGD_BY,"
                +"             SPLITTER_INPUT_SUBQUERY.DATAORIGIN2 DATAORIGIN,"
                +"             SPLITTER_INPUT_SUBQUERY.DATA_PROV2 DATA_PROV,"
                +"             SPLITTER_INPUT_SUBQUERY.LAGU LAGU,"
                +"             SPLITTER_INPUT_SUBQUERY.LANGU_ISO2 LANGU_ISO,"
                +"             SPLITTER_INPUT_SUBQUERY.PHRASE_CODE2 PHRASE_CODE,"
                +"             SPLITTER_INPUT_SUBQUERY.PHRASE_TEXT2 PHRASE_TEXT,"
                +"             SPLITTER_INPUT_SUBQUERY.LONG_TEXT LONG_TEXT,"
                +"             SPLITTER_INPUT_SUBQUERY.NOTE2 NOTE,"
                +"             SPLITTER_INPUT_SUBQUERY.PHRGRAPHIC2 PHRGRAPHIC,"
                +"             SPLITTER_INPUT_SUBQUERY.PPSTATE2 PPSTATE,"
                +"             SPLITTER_INPUT_SUBQUERY.FORMAT_FLAG2 FORMAT_FLAG,"
                +"             SPLITTER_INPUT_SUBQUERY.INSERTED1 INSERTED,"
                +"             SPLITTER_INPUT_SUBQUERY.SYST_ID2 SYST_ID"
                +"           FROM ("
                +"               SELECT "
                +"                   PHRASES1.SYSTEM SYSTEM,"
                +"                   PHRASES1.HEADER_RECORD_NO HEADER_RECORD_NO2,"
                +"                   PHRASES1.HEADER_RECNO_ROOT HEADER_RECNO_ROOT2,"
                +"                   PHRASES1.PHRASE PHRASE2,"
                +"                   PHRASES1.PHRLIB PHRLIB2,"
                +"                   PHRASES1.PHRGROUP PHRGROUP2,"
                +"                   PHRASES1.HEADER_LANGU HEADER_LANGU,"
                +"                   PHRASES1.TEXT_RECORD_NO TEXT_RECORD_NO2,"
                +"                   PHRASES1.CREATED_ON CREATED_ON2,"
                +"                   PHRASES1.CREATED_BY CREATED_BY2,"
                +"                   PHRASES1.CHANGD_ON CHANGD_ON2,"
                +"                   PHRASES1.CHANGD_BY CHANGD_BY2,"
                +"                   PHRASES1.DATAORIGIN DATAORIGIN2,"
                +"                   PHRASES1.DATA_PROV DATA_PROV2,"
                +"                   PHRASES1.LAGU LAGU,"
                +"                   PHRASES1.LANGU_ISO LANGU_ISO2,"
                +"                   PHRASES1.PHRASE_CODE PHRASE_CODE2,"
                +"                   PHRASES1.PHRASE_TEXT PHRASE_TEXT2,"
                +"                   PHRASES1.LONG_TEXT LONG_TEXT,"
                +"                   PHRASES1.NOTE NOTE2,"
                +"                   PHRASES1.PHRGRAPHIC PHRGRAPHIC2,"
                +"                   PHRASES1.PPSTATE PPSTATE2,"
                +"                   PHRASES1.FORMAT_FLAG FORMAT_FLAG2,"
                +"                   PHRASES1.INSERTED INSERTED1, "
                +"                   SYSTEM_S.SYST_ID SYST_ID2"
                +"                 FROM saifty_2_transform.TRA_PHRASES AS PHRASES1"
                +"                 LEFT OUTER JOIN  saifty_4_core.SYSTEM AS SYSTEM_S"
                +"                   ON SYSTEM_S.SYSTEM = PHRASES1.SYSTEM"
                +"             ) AS SPLITTER_INPUT_SUBQUERY"
                +"      WHERE "
                +"          (NOT ((SPLITTER_INPUT_SUBQUERY.SYST_ID2 is null)))) AS NEW"
                +"  LEFT OUTER JOIN  ( SELECT"
                +"  PHRASES2.SYST_ID SYST_ID,"
                +"  PHRASES2.HEADER_RECORD_NO HEADER_RECORD_NO,"
                +"  PHRASES2.HEADER_RECNO_ROOT HEADER_RECNO_ROOT,"
                +"  PHRASES2.PHRASE PHRASE,"
                +"  PHRASES2.PHRLIB PHRLIB,"
                +"  PHRASES2.PHRGROUP PHRGROUP,"
                +"  PHRASES2.TEXT_RECORD_NO TEXT_RECORD_NO,"
                +"  PHRASES2.CREATED_ON CREATED_ON,"
                +"  PHRASES2.CREATED_BY CREATED_BY,"
                +"  PHRASES2.CHANGD_ON CHANGD_ON,"
                +"  PHRASES2.CHANGD_BY CHANGD_BY,"
                +"  PHRASES2.DATAORIGIN DATAORIGIN,"
                +"  PHRASES2.DATA_PROV DATA_PROV,"
                +"  PHRASES2.LANGU_ISO LANGU_ISO,"
                +"  PHRASES2.PHRASE_CODE PHRASE_CODE,"
                +"  PHRASES2.PHRASE_TEXT PHRASE_TEXT,"
                +"  PHRASES2.NOTE NOTE,"
                +"  PHRASES2.PHRGRAPHIC PHRGRAPHIC,"
                +"  PHRASES2.PPSTATE PPSTATE,"
                +"  PHRASES2.FORMAT_FLAG FORMAT_FLAG,"
                +"  PHRASES2.SAIFTY_CREATED_ON SAIFTY_CREATED_ON"
                +"  FROM"
                +"  saifty_4_core.PHRASES AS PHRASES2 ) AS OLD ON ( (( NEW.SYST_ID = OLD.SYST_ID )) AND (( NEW.PHRASE = OLD.PHRASE )) AND (( NEW.LANGU_ISO = OLD.LANGU_ISO )) AND (( NEW.PHRLIB = OLD.PHRLIB )) )) AS LOOKUP_INPUT_SUBQUERY"
                +"  WHERE "
                +"  ( nvl( LOOKUP_INPUT_SUBQUERY.PHRASE_CODE1 ,'ISNULLINMAPS_###') <> nvl( LOOKUP_INPUT_SUBQUERY.PHRASE_CODE_1 ,'ISNULLINMAPS_###') OR"
                +"  nvl( LOOKUP_INPUT_SUBQUERY.PHRASE_TEXT1 ,'ISNULLINMAPS_###') <> nvl( LOOKUP_INPUT_SUBQUERY.PHRASE_TEXT_1 ,'ISNULLINMAPS_###'))"
                +"    ) AS MERGE_SUBQUERY"
                +"  ON "
                +"  PHRASES.SYST_ID = MERGE_SUBQUERY.SYST_ID AND"
                +"  PHRASES.PHRASE = MERGE_SUBQUERY.PHRASE AND"
                +"  PHRASES.PHRLIB = MERGE_SUBQUERY.PHRLIB AND"
                +"  PHRASES.LANGU_ISO = MERGE_SUBQUERY.LANGU_ISO"
                +"  WHEN MATCHED THEN"
                +"    UPDATE"
                +"  SET"
                +"    HEADER_RECORD_NO = MERGE_SUBQUERY.HEADER_RECORD_NO,"
                +"    HEADER_RECNO_ROOT = MERGE_SUBQUERY.HEADER_RECNO_ROOT,"
                +"    PHRGROUP = MERGE_SUBQUERY.PHRGROUP,"
                +"    TEXT_RECORD_NO = MERGE_SUBQUERY.TEXT_RECORD_NO,"
                +"    CREATED_ON = MERGE_SUBQUERY.CREATED_ON,"
                +"    CREATED_BY = MERGE_SUBQUERY.CREATED_BY,"
                +"    CHANGD_ON = MERGE_SUBQUERY.CHANGD_ON,"
                +"    CHANGD_BY = MERGE_SUBQUERY.CHANGD_BY,"
                +"    DATAORIGIN = MERGE_SUBQUERY.DATAORIGIN,"
                +"    DATA_PROV = MERGE_SUBQUERY.DATA_PROV,"
                +"    PHRASE_CODE = MERGE_SUBQUERY.PHRASE_CODE,"
                +"    PHRASE_TEXT = MERGE_SUBQUERY.PHRASE_TEXT,"
                +"    NOTE = MERGE_SUBQUERY.NOTE,"
                +"    PHRGRAPHIC = MERGE_SUBQUERY.PHRGRAPHIC,"
                +"    PPSTATE = MERGE_SUBQUERY.PPSTATE,"
                +"    FORMAT_FLAG = MERGE_SUBQUERY.FORMAT_FLAG,"
                +"    SAIFTY_CREATED_ON = MERGE_SUBQUERY.SAIFTY_CREATED_ON"
                +" WHEN NOT MATCHED THEN"
                +"    INSERT"
                +"    VALUES ("
                +"  MERGE_SUBQUERY.SYST_ID,"
                +"  MERGE_SUBQUERY.HEADER_RECORD_NO,"
                +"  MERGE_SUBQUERY.HEADER_RECNO_ROOT,"
                +"  MERGE_SUBQUERY.PHRASE,"
                +"  MERGE_SUBQUERY.PHRLIB,"
                +"  MERGE_SUBQUERY.PHRGROUP,"
                +"  MERGE_SUBQUERY.TEXT_RECORD_NO,"
                +"  MERGE_SUBQUERY.CREATED_ON,"
                +"  MERGE_SUBQUERY.CREATED_BY,"
                +"  MERGE_SUBQUERY.CHANGD_ON,"
                +"  MERGE_SUBQUERY.CHANGD_BY,"
                +"  MERGE_SUBQUERY.DATAORIGIN,"
                +"  MERGE_SUBQUERY.DATA_PROV,"
                +"  MERGE_SUBQUERY.PHRASE_CODE,"
                +"  MERGE_SUBQUERY.PHRASE_TEXT,"
                +"  MERGE_SUBQUERY.NOTE,"
                +"  MERGE_SUBQUERY.PHRGRAPHIC,"
                +"  MERGE_SUBQUERY.PPSTATE,"
                +"  MERGE_SUBQUERY.FORMAT_FLAG,"
                +"  MERGE_SUBQUERY.SAIFTY_CREATED_ON,"
                +"  MERGE_SUBQUERY.LANGU_ISO"
                +"    )";

        int res = stmt.executeUpdate(phrase);

    }
    public static void productIntegration(Statement stmt) throws SQLException {
        int prodId = Operations.getMaxValue("prod_id","saifty_4_core.products", stmt);
        int productId = Operations.getMaxValue("product_id","saifty_4_core.products", stmt);
        String insert = "INSERT "
                +" INTO"
                +" saifty_4_core.PRODUCTS "
                +" (PROD_ID,"
                +" PRODUCT_ID,"
                +" SYST_ID,"
                +" TYPE,"
                +" SUB_RECORD_NO,"
                +" SUBSTANCE,"
                +" CAS,"
                +" NAME,"
                +" NUMBER,"
                +" EG_INDEX_NUMBER,"
                +" ANNEX,"
                +" IHS,"
                +" HILL,"
                +" IUPAC,"
                +" SORT,"
                +" LINK,"
                +" MOSAIKSUB,"
                +" RTECS,"
                +" SUB,"
                +" STRUSUM,"
                +" TRIV,"
                +" SBU,"
                +" GPH,"
                +" INT_NAM,"
                +" PPH,"
                +" ARTTYP,"
                +" CMG,"
                +" DATAORIGIN,"
                +" IS_SOURCE_OF_A_HEREDITY,"
                +" SAP_CREATED_ON,"
                +" SAP_CREATED_BY,"
                +" SAP_CHANGD_ON,"
                +" SAP_CHANGD_BY,"
                +" SAIFTY_CREATED_ON ,"
                +" `FROM`,"
                +" `TO`,"
                +" SUBAUTHGRP,"
                +" NAME_DE,"
                +" TSCA,"
                +" WASTECOMP,"
                +" CMG_MSDS,"
                +" EGNR,"
                +" STKZ)"
                +" (SELECT "+ prodId+" + row_number() over () prod_id, " + productId +" + row_number() over () product_id,"
                +" SYST_ID,"
                +" TYPE1,"
                +" SUB_RECORD_NO1,"
                +" SUBSTANCE1,"
                +" CAS1,"
                +" NAME1,"
                +" EG_INDEX_NUMBER1,"
                +" EG_NUMBER1,"
                +" ANNEX1,"
                +" IHS1,"
                +" HILL1,"
                +" IUPAC1,"
                +" SORT1,"
                +" LINK1,"
                +" MOSAIKSUB1,"
                +" RTECS1,"
                +" SUB1,"
                +" STRUSUM1,"
                +" TRIV1,"
                +" SBU1,"
                +" GPH1,"
                +" INT_NAM1,"
                +" PPH1,"
                +" ARTTYP1,"
                +" CMG1,"
                +" DATAORIGIN1,"
                +" IS_SOURCE_OF_A_HEREDITY1,"
                +" SAP_CREATED_ON1,"
                +" SAP_CREATED_BY1,"
                +" SAP_CHANGD_ON1,"
                +" SAP_CHANGD_BY1,"
                +" SAIFTY_CREATED_ON1,"
                +" SAIFTY_CREATED_ON,"
                +" cast(substring(from_unixtime(unix_timestamp('9999-12-31', 'yyyy-MM-dd')), 1, 10) as DATE) `TO`, "
                +" SUBAUTHGRP1,"
                +" NAME_DE1,"
                +" TSCA1,"
                +" WASTECOMP1,"
                +" CMG_MSDS1,"
                +" EGNR1,"
                +" STKZ1"
                +" FROM "
                + "saifty_3_integration.intg_v_product)";

        String insert_changes_records = "INSERT "
                + "INTO"
                +" saifty_4_core.PRODUCTS "
                +" (PROD_ID,"
                +" PRODUCT_ID,"
                +" SYST_ID,"
                +" TYPE,"
                +" SUB_RECORD_NO,"
                +" SUBSTANCE,"
                +" CAS,"
                +" NAME,"
                +" NUMBER,"
                +" EG_INDEX_NUMBER,"
                +" ANNEX,"
                +" IHS,"
                +" HILL,"
                +" IUPAC,"
                +" SORT,"
                +" LINK,"
                +" MOSAIKSUB,"
                +" RTECS,"
                +" SUB,"
                +" STRUSUM,"
                +" TRIV,"
                +" SBU,"
                +" GPH,"
                +" INT_NAM,"
                +" PPH,"
                +" ARTTYP,"
                +" CMG,"
                +" DATAORIGIN,"
                +" IS_SOURCE_OF_A_HEREDITY,"
                +" SAP_CREATED_ON,"
                +" SAP_CREATED_BY,"
                +" SAP_CHANGD_ON,"
                +" SAP_CHANGD_BY,"
                +" SAIFTY_CREATED_ON ,"
                +" `FROM`,"
                +" `TO`,"
                +" SUBAUTHGRP,"
                +" NAME_DE,"
                +" TSCA,"
                +" WASTECOMP,"
                +" CMG_MSDS,"
                +" EGNR,"
                +" STKZ) "
                +"(SELECT " +prodId+" + row_number() over () prod_id,"
                +" LOOKUP_INPUT_SUBQUERY.PRODUCT_ID1 PRODUCT_ID,"
                +" LOOKUP_INPUT_SUBQUERY.SYST_ID1 SYST_ID,"
                +" LOOKUP_INPUT_SUBQUERY.TYPE1 TYPE,"
                +" LOOKUP_INPUT_SUBQUERY.SUB_RECORD_NO1 SUB_RECORD_NO,"
                +" LOOKUP_INPUT_SUBQUERY.SUBSTANCE1 SUBSTANCE,"
                +" LOOKUP_INPUT_SUBQUERY.CAS1 CAS,"
                +" LOOKUP_INPUT_SUBQUERY.NAME1 NAME,"
                +" LOOKUP_INPUT_SUBQUERY.EG_INDEX_NUMBER1 EG_INDEX_NUMBER,"
                +" LOOKUP_INPUT_SUBQUERY.EG_NUMBER1 NUMBER,"
                +" LOOKUP_INPUT_SUBQUERY.ANNEX1 ANNEX,"
                +" LOOKUP_INPUT_SUBQUERY.IHS1 IHS,"
                +" LOOKUP_INPUT_SUBQUERY.HILL1 HILL,"
                +" LOOKUP_INPUT_SUBQUERY.IUPAC1 IUPAC,"
                +" LOOKUP_INPUT_SUBQUERY.SORT1 SORT,"
                +" LOOKUP_INPUT_SUBQUERY.LINK1 LINK,"
                +" LOOKUP_INPUT_SUBQUERY.MOSAIKSUB_1 MOSAIKSUB,"
                +" LOOKUP_INPUT_SUBQUERY.RTECS1 RTECS,"
                +" LOOKUP_INPUT_SUBQUERY.SUB1 SUB,"
                +" LOOKUP_INPUT_SUBQUERY.STRUSUM1 STRUSUM,"
                +" LOOKUP_INPUT_SUBQUERY.TRIV1 TRIV,"
                +" LOOKUP_INPUT_SUBQUERY.SBU1 SBU,"
                +" LOOKUP_INPUT_SUBQUERY.GPH1 GPH,"
                +" LOOKUP_INPUT_SUBQUERY.INT_NAM1 INT_NAM,"
                +" LOOKUP_INPUT_SUBQUERY.PPH1 PPH,"
                +" LOOKUP_INPUT_SUBQUERY.ARTTYP1 ARTTYP,"
                +" LOOKUP_INPUT_SUBQUERY.CMG1 CMG,"
                +" LOOKUP_INPUT_SUBQUERY.DATAORIGIN1 DATAORIGIN,"
                +" LOOKUP_INPUT_SUBQUERY.IS_SOURCE_OF_A_HEREDITY1 IS_SOURCE_OF_A_HEREDITY,"
                +" LOOKUP_INPUT_SUBQUERY.SAP_CREATED_ON1 SAP_CREATED_ON,"
                +" LOOKUP_INPUT_SUBQUERY.SAP_CREATED_BY1 SAP_CREATED_BY,"
                +" LOOKUP_INPUT_SUBQUERY.SAP_CHANGD_ON1 SAP_CHANGD_ON,"
                +" LOOKUP_INPUT_SUBQUERY.SAP_CHANGD_BY1 SAP_CHANGD_BY,"
                +" LOOKUP_INPUT_SUBQUERY.SAIFTY_CREATED_ON1 SAIFTY_CREATED_ON,"
                +" cast(substring(from_unixtime(unix_timestamp(CURRENT_DATE, 'yyyy-MM-dd')), 1, 10) as DATE) `FROM`,"
                +" cast(substring(from_unixtime(unix_timestamp('9999-12-31', 'yyyy-MM-dd')),1,10) as DATE) `TO`,"
                +" LOOKUP_INPUT_SUBQUERY.SUBAUTHGRP1 SUBAUTHGRP,"
                +" LOOKUP_INPUT_SUBQUERY.NAME_DE1 NAME_DE,"
                +" LOOKUP_INPUT_SUBQUERY.TSCA1 TSCA,"
                +" LOOKUP_INPUT_SUBQUERY.WASTECOMP1 WASTECOMP,"
                +" LOOKUP_INPUT_SUBQUERY.CMG_MSDS1 CMG_MSDS,"
                +" LOOKUP_INPUT_SUBQUERY.EGNR1 EGNR,"
                +" LOOKUP_INPUT_SUBQUERY.STKZ1 STKZ"
                +" FROM "
                +" (SELECT distinct"
                +" DATA.SYSTEM SYSTEM,"
                +" DATA.TYPE TYPE1,"
                +" DATA.SUB_RECORD_NO SUB_RECORD_NO1,"
                +" DATA.SUBSTANCE SUBSTANCE1,"
                +" DATA.CAS CAS1,"
                +" DATA.NAME NAME1,"
                +" DATA.EG_INDEX_NUMBER EG_INDEX_NUMBER1,"
                +" DATA.EG_NUMBER EG_NUMBER1,"
                +" DATA.ANNEX ANNEX1,"
                +" DATA.IHS IHS1,"
                +" DATA.HILL HILL1,"
                +" DATA.IUPAC IUPAC1,"
                +" DATA.SORT SORT1,"
                +" DATA.LINK LINK1,"
                +" DATA.MOSAIKSUB MOSAIKSUB1,"
                +" DATA.RTECS RTECS1,"
                +" DATA.SUB SUB1,"
                +" DATA.STRUSUM STRUSUM1,"
                +" DATA.TRIV TRIV1,"
                +" DATA.SBU SBU1,"
                +" DATA.GPH GPH1,"
                +" DATA.INT_NAM INT_NAM1,"
                +" DATA.PPH PPH1,"
                +" DATA.ARTTYP ARTTYP1,"
                +" DATA.CMG CMG1,"
                +" DATA.DATAORIGIN DATAORIGIN1,"
                +" DATA.IS_SOURCE_OF_A_HEREDITY IS_SOURCE_OF_A_HEREDITY1,"
                +" DATA.SAP_CREATED_ON SAP_CREATED_ON1,"
                +" DATA.SAP_CREATED_BY SAP_CREATED_BY1,"
                +" DATA.SAP_CHANGD_ON SAP_CHANGD_ON1,"
                +" DATA.SAP_CHANGD_BY SAP_CHANGD_BY1,"
                +" DATA.SAIFTY_CREATED_ON SAIFTY_CREATED_ON1,"
                +" DATA.SUBAUTHGRP SUBAUTHGRP1,"
                +" DATA.NAME_DE NAME_DE1,"
                +" DATA.TSCA TSCA1,"
                +" DATA.WASTECOMP WASTECOMP1,"
                +" DATA.CMG_MSDS CMG_MSDS1,"
                +" DATA.EGNR EGNR1,"
                +" DATA.STKZ STKZ1,"
                +" DATA.SYST_ID SYST_ID1,"
                +" DATA.SYSTEM_1 SYSTEM_1,"
                +" DATA.SAIFTY_CREATED_ON_1 SAIFTY_CREATED_ON_1,"
                +" DATA.MAPS_UPDATED_ON MAPS_UPDATED_ON,"
                +" COR_PROD.PRODUCT_ID PRODUCT_ID1,"
                +" COR_PROD.MOSAIKSUB MOSAIKSUB_1"
                + " FROM "
                +"  ( SELECT"
                +" INT_PRODUCTS.SYSTEM SYSTEM,"
                +" INT_PRODUCTS.TYPE TYPE,"
                +" INT_PRODUCTS.SUB_RECORD_NO SUB_RECORD_NO,"
                +" INT_PRODUCTS.SUBSTANCE SUBSTANCE,"
                +" INT_PRODUCTS.CAS CAS,"
                +" INT_PRODUCTS.NAME NAME,"
                +" INT_PRODUCTS.EG_INDEX_NUMBER EG_INDEX_NUMBER,"
                +" INT_PRODUCTS.EG_NUMBER EG_NUMBER,"
                +" INT_PRODUCTS.ANNEX ANNEX,"
                +" INT_PRODUCTS.IHS IHS,"
                +" INT_PRODUCTS.HILL HILL,"
                +" INT_PRODUCTS.IUPAC IUPAC,"
                +" INT_PRODUCTS.SORT SORT,"
                +" INT_PRODUCTS.LINK LINK,"
                +" INT_PRODUCTS.MOSAIK MOSAIKSUB,"
                +" INT_PRODUCTS.RTECS RTECS,"
                +" INT_PRODUCTS.SUB SUB,"
                +" INT_PRODUCTS.STRUSUM STRUSUM,"
                +" INT_PRODUCTS.TRIV TRIV,"
                +" INT_PRODUCTS.SBU SBU,"
                +" INT_PRODUCTS.GPH GPH,"
                +" INT_PRODUCTS.INT_NAM INT_NAM,"
                +" INT_PRODUCTS.PPH PPH,"
                +" INT_PRODUCTS.ARTTYP ARTTYP,"
                +" INT_PRODUCTS.CMG CMG,"
                +" INT_PRODUCTS.DATAORIGIN DATAORIGIN,"
                +" INT_PRODUCTS.IS_SOURCE_OF_A_HEREDITY IS_SOURCE_OF_A_HEREDITY,"
                +" INT_PRODUCTS.SAP_CREATED_ON SAP_CREATED_ON,"
                +" INT_PRODUCTS.SAP_CREATED_BY SAP_CREATED_BY,"
                +" INT_PRODUCTS.SAP_CHANGD_ON SAP_CHANGD_ON,"
                +" INT_PRODUCTS.SAP_CHANGD_BY SAP_CHANGD_BY,"
                +" INT_PRODUCTS.SAIFTY_CREATED_ON SAIFTY_CREATED_ON,"
                +" INT_PRODUCTS.SUBAUTHGRP SUBAUTHGRP,"
                +" INT_PRODUCTS.NAME_DE NAME_DE,"
                +" INT_PRODUCTS.TSCA TSCA,"
                +" INT_PRODUCTS.WASTECOMP WASTECOMP,"
                +" INT_PRODUCTS.CMG_MSDS CMG_MSDS,"
                +" INT_PRODUCTS.EGNR EGNR,"
                +" INT_PRODUCTS.STKZ STKZ,"
                +" SYSTEM1.SYST_ID SYST_ID,"
                +" SYSTEM1.SYSTEM SYSTEM_1,"
                +" SYSTEM1.SAIFTY_CREATED_ON SAIFTY_CREATED_ON_1,"
                +" SYSTEM1.SAIFTY_UPDATED_ON MAPS_UPDATED_ON"
                +" FROM "
                +" saifty_2_transform.TRA_PRODUCTS_CHANGES  INT_PRODUCTS   "
                +" LEFT JOIN  saifty_4_core.system  SYSTEM1 "
                +" ON ( ( INT_PRODUCTS.SYSTEM = SYSTEM1.SYSTEM ) ) ) DATA  "
                +" LEFT JOIN saifty_4_core.products  COR_PROD "
                 +" ON ( (( DATA.SUBSTANCE = COR_PROD.SUBSTANCE )) AND (( DATA.SYST_ID = COR_PROD.SYST_ID )) )) LOOKUP_INPUT_SUBQUERY) ";

    String recordsToUpdate = " SELECT  distinct COR_PROD.PRODUCT_ID PRODUCT_ID FROM   " + "              ( SELECT  "
            + "              INT_PRODUCTS.SUBSTANCE SUBSTANCE,  "
            + "              SYSTEM1.SYST_ID SYST_ID "
            + "            FROM   "
            + "            saifty_2_transform.TRA_PRODUCTS_CHANGES  INT_PRODUCTS     "
            + "            LEFT OUTER JOIN  saifty_4_core.system  SYSTEM1   "
            + "            ON ( ( INT_PRODUCTS.SYSTEM = SYSTEM1.SYSTEM ) ) ) DATA    "
            + "            JOIN saifty_4_core.products  COR_PROD   "
            + "              ON ( (( DATA.SUBSTANCE = COR_PROD.SUBSTANCE )) AND (( DATA.SYST_ID = COR_PROD.SYST_ID )) ) ";
    String update = "UPDATE saifty_4_core.products SET `TO` = cast(substring(from_unixtime(unix_timestamp(CURRENT_DATE, 'yyyy-MM-dd')), 1, 10) as DATE) WHERE PRODUCT_ID IN %s";
    Operations.updateTable(stmt,recordsToUpdate,update);
    int res_changes = stmt.executeUpdate(insert_changes_records);
    int res_insert = stmt.executeUpdate(insert);


    }
    public static void compositionsIntegration(Statement stmt) throws SQLException {
        Operations.materializeTable(stmt,"saifty_3_integration","INTG_COMPOSITIONS","INTG_V_COMPOSITIONS");
        Operations.deletionLogic("saifty_3_integration.INTG_COMPOSITIONS","saifty_4_core.compositions","substance_id","product_id",stmt);

        String insert = " INSERT "
        +" INTO "
        +"saifty_4_core.COMPOSITIONS   "
        +"(SYST_ID, "
        +"PRODUCT_ID, "
        +"USAG_ID, "
        +"TYPE, "
        +"SUBCHACAT, "
        +"COMPONENT_TYPE, "
        +"COMPONENT_ID, "
        +"DATAORIGIN, "
        +"COMPCAT, "
        +"OPLOWLIMIT, "
        +"LOW_LIMIT, "
        +"OPUPPLIMIT, "
        +"UPP_LIMIT, "
        +"AVG_VAL, "
        +"EXPONENT, "
        +"SEQUENCE, "
        +"DECLOWLIMIT, "
        +"DECUPPLIMIT, "
        +"DECAVGVAL, "
        +"EXCEPT_VAL, "
        +"SAP_CREATED_ON, "
        +"SAP_CREATED_BY, "
        +"SAP_CHANGD_ON, "
        +"SAP_CHANGD_BY, "
        +"SAIFTY_CREATED_ON, "
        +"`FROM`, "
        +"`TO` "
        +") "
        +"( select * from saifty_3_integration.INTG_COMPOSITIONS) ";
        int res1 = stmt.executeUpdate(insert);

    }
    public static void usageIntegration(Statement stmt) throws SQLException {
        int max_count_usage = 0;
        Operations.materializeTable(stmt, "saifty_3_integration", "intg_usage", " intg_v_usage");
        int max_count = Operations.getMaxValue("usage_id","saifty_4_core.usage", stmt);
        final String usage_core = " insert into saifty_4_core.usage "
                + " select  " + max_count + " + row_number() over () usage_id, "
                + " rating,validity_area,saifty_created_on,saifty_updated_on,excl_ind,act_ind,rel_ind from saifty_3_integration.intg_usage ";
        int res1 = stmt.executeUpdate(usage_core);

    }

    public static void propertiesIntegration(Statement stmt) throws SQLException {

        Operations.materializeTable(stmt, "saifty_3_integration", "INTG_PROPERTIES_WITH_PHRASES", "INTG_V_PROPERTIES_WITH_PHRASES");
        Operations.materializeTable(stmt, "saifty_3_integration", "INTG_PROPERTIES_NO_PHRASES", "INTG_V_PROPERTIES_NO_PHRASES");
        Operations.deletionLogic("saifty_3_integration.INTG_PROPERTIES_NO_PHRASES","saifty_4_core.properties","substance_id","product_id",stmt);
        Operations.deletionLogic("saifty_3_integration.INTG_PROPERTIES_WITH_PHRASES","saifty_4_core.properties","substance_id","product_id",stmt);
        String insert_with_phrases = " insert into saifty_4_core.properties(SYST_ID,"
                +" USAG_ID,"
                +" PRODUCT_TYPE,"
                +" PRODUCT_ID,"
                +" SUB_RECORD_NO,"
                +" PROP_VAL_RECORD_NO,"
                +" SUB_RECORD_NO_ORIGINAL,"
                +" SAP_CREATED_ON,"
                +" SAP_CREATED_BY,"
                +" SAP_CHANGD_ON,"
                +" SAP_CHANGD_BY,"
                +" MAPS_CREATED_ON,"
                +" SEQUENCE,"
                +" SORT,"
                +" REFERENCE_QUANTITY,"
                +" SUB_CHAR_CATEGORY,"
                +" DATAORIGIN,"
                +" IS_SOURCE_OF_A_HEREDITY,"
                +" OBJ_TYPE,"
                +" RATING,"
                +" VAL_AREA,"
                +" IDENTIFIER,"
                +" FLG_PHRASE,"
                +" VALUE_PHRASE_LIB,"
                +" VALUE_PHRASE_KEY,"
                +" VALUE_DESCRIPTION,"
                +" VALUE_TXT,"
                +" VALUE_CODE,"
                +" VALUE_FROM,"
                +" VALUE_TO,"
                +" ISOCODE1,"
                +" ISOCODE2,"
                +" MEASURE_UNIT_1,"
                +" MEASURE_UNIT_2,"
                +" VAL_RELATION,"
                +" TOLERANCE_FROM,"
                +" TOLERANCE_TO,"
                +" PERCENTAGE,"
                +" OPER_INC,"
                +" ALLOC_NO,"
                +" DATA_AREA,"
                +" PROPERTY_NAME)"
                +" select * from saifty_3_integration.INTG_PROPERTIES_WITH_PHRASES ";

        String insert_without_phrases = " INSERT  "
                +"	INTO  "
                +" saifty_4_core.PROPERTIES  "
                +" (SYST_ID,  "
                +" USAG_ID, "
                +" 	PRODUCT_ID, 	 "
                +" PRODUCT_TYPE,  "
                +" SUB_RECORD_NO,  "
                +" PROP_VAL_RECORD_NO,  "
                +" SUB_RECORD_NO_ORIGINAL,  "
                +" SAP_CREATED_ON,  "
                +" SAP_CREATED_BY,  "
                +" SAP_CHANGD_ON,  "
                +" SAP_CHANGD_BY,  "
                +" MAPS_CREATED_ON,  "
                +" SEQUENCE,  "
                +" SORT,  "
                +" REFERENCE_QUANTITY,  "
                +" SUB_CHAR_CATEGORY,  "
                +" DATAORIGIN,  "
                +" IS_SOURCE_OF_A_HEREDITY,  "
                +" OBJ_TYPE,  "
                +" RATING,  "
                +" VAL_AREA,  "
                +" IDENTIFIER,  "
                +" VALUE_DESCRIPTION,  "
                +" VALUE_FROM,  "
                +" VALUE_TO,  "
                +" ISOCODE1,  "
                +" ISOCODE2,  "
                +" MEASURE_UNIT_1,  "
                +" MEASURE_UNIT_2,  "
                +" VAL_RELATION,  "
                +" TOLERANCE_FROM,  "
                +" TOLERANCE_TO,  "
                +" PERCENTAGE,  "
                +" OPER_INC,  "
                +" ALLOC_NO,  "
                +" DATA_AREA,  "
                +" PROPERTY_NAME)  "
                +" select * from saifty_3_integration.INTG_PROPERTIES_NO_PHRASES ";

                int res1 = stmt.executeUpdate(insert_without_phrases);
                int res2 = stmt.executeUpdate(insert_with_phrases);


    }
}
