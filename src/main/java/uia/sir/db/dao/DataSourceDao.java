package uia.sir.db.dao;

import static com.mongodb.client.model.Filters.eq;

import com.mongodb.client.MongoDatabase;
import com.mongodb.client.result.DeleteResult;

import uia.sir.db.DataSource;

public class DataSourceDao extends MgoDao<DataSource> {

    public DataSourceDao(MongoDatabase database) {
        super(database, "data_source", DataSource.class);
    }

    public DeleteResult delete(String name) {
        return this.collection.deleteMany(eq("name", name));
    }
}
