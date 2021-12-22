package de.coac.robots.saifty.lake;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URISyntaxException;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.Statement;


public class TablesMaterialization implements Materialization {

    
    private Statement stmt = null;
    java.util.logging.Logger logger =  java.util.logging.Logger.getLogger(this.getClass().getName());

    public  TablesMaterialization(Statement stmt){
        this.stmt = stmt;


    }
    @Override
    public void usage() throws Exception {

        Staging.deployStaging(this.stmt,"usage");
        logger.info("Usage Staging Successful");
        Transform.usageTransform(this.stmt);
        logger.info("Usage Transform Successful");
        Integration.usageIntegration(this.stmt);
        logger.info("Usage Integration Successful");


    }
    @Override
    public void phrases() throws Exception {

        Staging.deployStaging(this.stmt,"phrases");
        logger.info("Phrases Staging Successful");
        Transform.phrasesTransform(this.stmt);
        logger.info("Phrases Transform Successful");
        Integration.phraseIntegration(this.stmt);
        logger.info("Phrases Integration Successful");

    }
    @Override
    public void properties() throws Exception {

        Staging.deployStaging(this.stmt,"properties");
        logger.info("properties Staging Successful");
        Transform.propertiesTransform(this.stmt);
        logger.info("properties Transform Successful");
        Integration.propertiesIntegration(this.stmt);
        logger.info("properties Integration Successful");

    }
    @Override
    public void products() throws Exception {

        Staging.deployStaging(this.stmt,  "identifier");
        logger.info("Identifier Staging Successful");
        Transform.compositionsTransform(this.stmt);
        logger.info("Identifier Transform Successful");
        Integration.productIntegration(this.stmt);
        logger.info("Identifier Integration Successful");

    }
    @Override
    public void compositions() throws Exception {

        Staging.deployStaging(this.stmt,"compositions");
        logger.info("compositions Staging Successful");
        Transform.compositionsTransform(this.stmt);
        logger.info("compositions Transform Successful");
        Integration.compositionsIntegration(this.stmt);
        logger.info("compositions Integration Successful");


    }
    @Override
    public void identifier_full() throws Exception {

        Staging.deployStaging(this.stmt,"identifier_full");
        logger.info("identifier_full Staging Successful");
        Transform.identifierTransform(this.stmt);
        logger.info("identifier_full Transform Successful");
        Integration.identifierIntegration(this.stmt);
        logger.info("identifier_full Integration Successful");

    }
    @Override
    public void estdh() throws Exception {

        Staging.deployStaging(this.stmt,"estdh");
        logger.info("identifier_full Staging Successful");
        Transform.estdhTransform(this.stmt);
        logger.info("identifier_full Transform Successful");
        Integration.estdhIntegration(this.stmt);
        logger.info("identifier_full Integration Successful");

    }
    @Override
    public void estmj() throws Exception {

        Staging.deployStaging(this.stmt,"estmj");
        logger.info("estmj Staging Successful");
        Transform.estmjTransform(this.stmt);
        logger.info("estmj Transform Successful");
        Integration.estmjIntegration(this.stmt);
        logger.info("estmj Integration Successful");

    }


    }

