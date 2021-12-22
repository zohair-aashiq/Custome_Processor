package de.coac.robots.saifty.lake;

import java.io.IOException;

public interface Materialization {
    public void usage() throws IOException, Exception;
    public void phrases() throws IOException, Exception;
    public void properties() throws IOException, Exception;
    public void products() throws IOException, Exception;
    public void compositions() throws IOException, Exception;
    public void identifier_full() throws IOException, Exception;
    public void estdh() throws IOException, Exception;
    public void estmj() throws IOException, Exception;
}
