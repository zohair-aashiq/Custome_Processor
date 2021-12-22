package de.coac.robots.saifty.lake;

import java.io.IOException;
import java.net.URISyntaxException;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Connection;
import java.sql.Statement;
import java.io.FileNotFoundException;

public class SaiftyLakeIntialization {
    java.util.logging.Logger logger =  java.util.logging.Logger.getLogger(this.getClass().getName());
    private static final String driverName = "org.apache.hive.jdbc.HiveDriver";
    private Boolean identifier_full = true;
    private String jdbc = null;
    private String portNumber = "10000";
    private Boolean phrases = true;
    private Boolean usage = true;
    private Boolean compositions = true;
    private Boolean products = true;
    private Boolean properties = true;
    private Boolean estdh = true;
    private Boolean estmj = true;


    public SaiftyLakeIntialization(String jdbc, String portNumber, Boolean usage, Boolean phrases, Boolean identifier_full, Boolean products, Boolean properties, Boolean compositions, Boolean estdh, Boolean estmj) {
        this.jdbc = jdbc;
        this.phrases = phrases;
        this.usage = usage;
        this.identifier_full = identifier_full;
        this.compositions = compositions;
        this.products = products;
        this.properties = properties;
        this.estdh = estdh;
        this.estmj = estmj;
        this.portNumber = portNumber;
    }

    public void triggerSaifty() throws Exception {
        try {
            Class.forName(driverName);
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
            System.exit(1);
        }
        Connection con = DriverManager.getConnection("jdbc:hive2://"+this.jdbc+":"+this.portNumber+"/ehs_base_tables", "", "");
        Statement stmt = con.createStatement();
        Materialization materialization = new TablesMaterialization(stmt);
        if (this.usage == true) {
            logger.info("Usage Integration started");
            materialization.usage();
            logger.info("Usage Integration Completed Successfully");
        }
        if (this.phrases == true) {
            logger.info("phrases Integration started");
            materialization.phrases();
            logger.info("phrases Integration Completed Successfully");
        }
        if (this.identifier_full == true) {
            logger.info("identifier_full Integration started");
            materialization.identifier_full();
            logger.info("identifier_full Integration Completed Successfully");
        }
        if (this.products == true) {
            logger.info("products Integration started");
            materialization.products();
            logger.info("products Integration Completed Successfully");

        }
        if (this.properties == true) {
            logger.info("properties Integration started");
            materialization.properties();
            logger.info("properties Integration Completed Successfully");
        }
        if (this.compositions == true) {
            logger.info("compositions Integration started");
            materialization.compositions();
            logger.info("compositions Integration Completed Successfully");

        }
        if (this.estdh == true) {
            logger.info("Estdh Integration started");
            materialization.estdh();
            logger.info("Estdh Integration Completed Successfully");

        }
        if (this.estmj == true) {
            logger.info("Estmj Integration started");
            materialization.estmj();
            logger.info("Estmj Integration Completed Successfully");
        }
    }
}