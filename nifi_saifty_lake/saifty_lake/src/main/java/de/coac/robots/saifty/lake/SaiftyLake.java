package de.coac.robots.saifty.lake;

import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Connection;
import java.sql.Statement;
import java.io.FileNotFoundException;

public class SaiftyLake {
    private String jdbc = null;
    private String portNumber = "10000";
    private Boolean phrases = true;
    private Boolean usage = true;
    private Boolean compositions = true;
    private Boolean products = true;
    private Boolean properties = true;
    private Boolean estdh = true;
    private Boolean estmj = true;

    public SaiftyLake(String jdbc){
        this.jdbc = jdbc;
        this.phrases = phrases;
    }

    public void triggerSaifty() throws SQLException, FileNotFoundException {
        String resources_path = "F:\\coac-robots\\coac-robots\\saifty-lake\\src\\main\\resources\\";
        String driverName = this.jdbc;
        try {
            Class.forName(driverName);
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
            System.exit(1);
        }
        Connection con = DriverManager.getConnection("jdbc:hive2://"+driverName+":"+portNumber+"/ehs_base_tables", "", "");
        Statement stmt = con.createStatement();
        TablesMaterialization materialization = new TablesMaterialization(stmt, resources_path);
        if (this.phrases == true) {
            materialization.phrases();
        }
        if (this.usage == true) {
            materialization.usage();
        }
        if (this.compositions == true){
        materialization.compositions();
        }
        if (this.products == true){
        materialization.products();
        }
        if (this.properties == true){
            materialization.products();
        }
        if (this.estdh == true){
            materialization.estdh();
        }
        if (this.estmj == true){
            materialization.estdh();
        }
    }
}
