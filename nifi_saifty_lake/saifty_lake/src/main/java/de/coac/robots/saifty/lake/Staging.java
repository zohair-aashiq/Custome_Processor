package de.coac.robots.saifty.lake;

import java.io.FileNotFoundException;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.Statement;

public class Staging {

    public static void deployStaging(Statement stmt,String resources_path,String ddl_name) throws SQLException, FileNotFoundException, SQLFeatureNotSupportedException {
        Operations op = new Operations();
        op.executeDDL(resources_path,ddl_name+".txt",stmt);

    }

}
