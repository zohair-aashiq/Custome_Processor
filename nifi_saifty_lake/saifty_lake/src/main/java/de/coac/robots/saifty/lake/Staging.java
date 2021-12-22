package de.coac.robots.saifty.lake;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Paths;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.Statement;

public class Staging {

    public static void deployStaging(Statement stmt,String ddl_name) throws SQLException, IOException, URISyntaxException {
        Operations op = new Operations();
        URL path = Staging.class.getClassLoader().getResource(ddl_name+".txt");
        op.executeDDL(path,stmt);
    }

}
