package uia.sir.ds;

import java.io.Closeable;

import uia.sir.simple.QueryRunner;

public interface Client extends Closeable {

    public QueryRunner runner(String collectionName);
}
