package uia.sir.ds.hana.db;

import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;

import uia.sir.ds.Client;
import uia.sir.ds.hana.HanaQueryRunner;
import uia.sir.simple.QueryRunner;

public class HanaClient implements Client {

    private final Connection conn;

    public HanaClient(Connection conn) {
        this.conn = conn;
    }

    public HanaYesDao dao(String collectionName) {
        return new HanaYesDao(this.conn, collectionName);
    }

    @Override
    public QueryRunner runner(String collectionName) {
        return new HanaQueryRunner(dao(collectionName));
    }

    @Override
    public void close() throws IOException {
        try {
            this.conn.close();
        }
        catch (SQLException e) {
            throw new IOException(e);
        }
    }

}
