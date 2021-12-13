package de.coac.robots.saifty.lake;

import java.io.FileNotFoundException;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.Statement;


public class TablesMaterialization {

    private final String rsc;
    private Statement stmt = null;


    public  TablesMaterialization(Statement stmt,String resources_path ){
        this.stmt = stmt;
        this.rsc = resources_path;

    }
    public void usage() throws FileNotFoundException, SQLException {
        Staging.deployStaging(this.stmt,this.rsc,"usage");
        Transform.usageTransform(this.stmt);
        Integration.usageIntegration(this.stmt);

    }
    public void phrases() throws FileNotFoundException, SQLException {
        Staging.deployStaging(this.stmt,this.rsc,"phrases");
        Transform.phrasesTransform(this.stmt);
        Integration.phraseIntegration(this.stmt);
    }
    public void properties() throws FileNotFoundException, SQLException {
        Staging.deployStaging(this.stmt,this.rsc,"properties");
        Transform.propertiesTransform(this.stmt);
        Integration.propertiesIntegration(this.stmt);
    }
    public void products() throws FileNotFoundException, SQLException {
        Staging.deployStaging(this.stmt, this.rsc, "identifier");
        Transform.compositionsTransform(this.stmt);
        Integration.productIntegration(this.stmt);
    }
    public void compositions() throws FileNotFoundException, SQLException {
        Staging.deployStaging(this.stmt,this.rsc,"compositions");
        Transform.compositionsTransform(this.stmt);
        Integration.compositionsIntegration(this.stmt);
    }

    public void identifier_full() throws FileNotFoundException, SQLException {
        Staging.deployStaging(this.stmt,this.rsc,"identifier_full");
        Transform.identifierTransform(this.stmt);
        Integration.identifierIntegration(this.stmt);
    }
    public void estdh() throws FileNotFoundException, SQLException {
        Staging.deployStaging(this.stmt,this.rsc,"estdh");
        Transform.estdhTransform(this.stmt);
        Integration.estdhIntegration(this.stmt);
    }
    public void estmj() throws FileNotFoundException, SQLException {
        Staging.deployStaging(this.stmt,this.rsc,"estmj");
       Transform.estmjTransform(this.stmt);
        Integration.estmjIntegration(this.stmt);
    }


    }

