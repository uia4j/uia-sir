package uia.sir.db.dao;

import java.io.Closeable;

import com.mongodb.client.MongoDatabase;

public class SIRClient implements Closeable {

    final MongoDatabase database;

    public SIRClient(MongoDatabase database) {
        this.database = database;
    }

    public DataSourceDao dataSource() {
        return new DataSourceDao(this.database);
    }

    public DatasetDao dataset() {
        return new DatasetDao(this.database);
    }

    public QueryPlanInfoDao queryPlan() {
        return new QueryPlanInfoDao(this.database);
    }

    @Override
    public void close() {
        // this.client.close();
    }
}
