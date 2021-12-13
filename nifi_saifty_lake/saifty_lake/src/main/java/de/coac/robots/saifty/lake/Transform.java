package de.coac.robots.saifty.lake;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.logging.Logger;

public class Transform {
    public static void estmjTransform(Statement stmt) throws SQLException {
        Operations.materializeTable(stmt, "saifty_2_transform", "tra_product_material", " tra_v_product_material");
    }

    public static void usageTransform(Statement stmt) throws SQLException {
        Operations.materializeTable(stmt, "saifty_1_staging", "STA_USAGE", "STA_V_USAGE");
        Operations.materializeTable(stmt, "saifty_2_transform", "TRA_USAGE", "TRA_V_USAGE");
    }

    public static void productTransform(Statement stmt) throws SQLException {
        Operations.materializeTable(stmt, "saifty_1_staging", "STA_PRODUCTS", "STA_V_PRODUCTS");
        Operations.materializeTable(stmt, "saifty_2_transform", "TRA_PRODUCTS_CHANGES", "TRA_V_PRODUCTS_CHANGES");
    }

    public static void propertiesTransform(Statement stmt) throws SQLException {
        Operations.materializeTable(stmt, "saifty_1_staging", "STA_DATA_HUB", "STA_V_DATA_HUB");
        Operations.materializeTable(stmt, "saifty_2_transform", "TRA_DATA_HUB_READY", "TRA_V_DATA_HUB_READY");
        Operations.materializeTable(stmt, "saifty_2_transform", "TRA_DATA_HUB_READY_NO_PHRASES", "TRA_V_DATA_HUB_READY_NO_PHRASES");
    }

    public static void phrasesTransform(Statement stmt) throws SQLException {
        Operations.materializeTable(stmt, "saifty_2_transform", "TRA_PHRASES", "TRA_V_PHRASES");
    }

    public static void compositionsTransform(Statement stmt) throws SQLException {
        Operations.materializeTable(stmt, "saifty_2_transform", "TRA_COMPOSITION_1_LOOKEDUP", "TRA_V_COMPOSITION_1_LOOKEDUP");
        Operations.materializeTable(stmt, "saifty_2_transform", "TRA_COMPOSITION_READY", "TRA_V_COMPOSITION_READY");
    }

    public static void estdhTransform(Statement stmt) throws SQLException {
        Operations.materializeTable(stmt, "saifty_2_transform", "TRA_ESTDH", "TRA_V_ESTDH");
    }

    public static void identifierTransform(Statement stmt) throws SQLException {
        Operations.materializeTable(stmt, "saifty_2_transform", "TRA_identifier_full", "TRA_V_identifier_full");
    }
}