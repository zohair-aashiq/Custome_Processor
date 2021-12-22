package de.coac.robots.saifty.lake;
import com.google.common.io.Resources;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.Paths;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;
import java.util.stream.Collectors;

public class Operations {

    public static int getMaxValue(String column_name, String tableName,Statement stmt) throws SQLException {
        int max_count_usage = 0;
        String query = "select max( "+column_name+" ) from "+ tableName;
        ResultSet res = stmt.executeQuery(query);
        while (res.next()) {
            max_count_usage = Integer.valueOf(res.getInt(1));
        }
        return max_count_usage;

    }
    public static void updateTable(Statement stmt, String recordsToUpdate, String query) throws SQLException {
        List<String> idsList = new ArrayList<String>();
        ResultSet res = stmt.executeQuery(recordsToUpdate);
        while (res.next()) {
            idsList.add(res.getString(1));
        }
        res.close();
        String ids = null;
        if (idsList.size() != 0) {
            String syst_id = null;
            ids = idsList.stream().map(n -> String.valueOf(n)).collect(Collectors.joining(",", "(", ")"));
        }
        String updateQuery = String.format(query, ids);
        int res2 = stmt.executeUpdate(updateQuery);

    }

    public static void executeDDL(URL resources_path, Statement stmt) throws SQLException, IOException, SQLFeatureNotSupportedException, URISyntaxException {
        String content = Resources.toString(resources_path,  StandardCharsets.UTF_8);
        String[] queries = content.split(";");
        for (String query : queries) {
            int res = stmt.executeUpdate(query);

        }
    }

    public static void materializeTable(Statement stmt, String layerName, String tableName, String viewName) throws SQLException {
        String drop = "drop table " + layerName + '.' + tableName;
        int res = stmt.executeUpdate(drop);
        String materialize = " create table " + layerName + "." + tableName + " as select * from " + layerName + "." + viewName;
        int res1 = stmt.executeUpdate(materialize);
    }

    public static String getSystIds(String tableName, Statement stmt) throws SQLException {
        List<String> systIdList = new ArrayList<String>();
        String query = "select distinct syst_id from " + tableName;
        ResultSet res = stmt.executeQuery(query);
        while (res.next()) {
            systIdList.add(res.getString(1));
        }
        String syst_id = null;
        if (systIdList.size() != 0) {
            if (systIdList.size() == 1) {
                syst_id = systIdList.stream().map(n -> String.valueOf(n)).collect(Collectors.joining(",", "(", ")"));
                System.out.println(syst_id);
            }

        }
        return syst_id;

    }
    public static void deletionLogic(String intgTableName, String coreTableName, String deleteIdsColumns,String CoreColumnsName, Statement stmt) throws SQLException {
        String systId = getSystIds(intgTableName,stmt) ;


    }

}

