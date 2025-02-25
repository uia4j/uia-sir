package uia.sir.db.dao;

import static com.mongodb.client.model.Filters.eq;

import com.mongodb.client.MongoDatabase;
import com.mongodb.client.result.DeleteResult;

import uia.sir.db.Dataset;

public class DatasetDao extends MgoDao<Dataset> {

    public DatasetDao(MongoDatabase database) {
        super(database, "dataset", Dataset.class);
    }

    public Dataset selectOne(String name) {
        return this.collection.find(eq("name", name)).first();
    }

    public DeleteResult delete(String name) {
        return this.collection.deleteMany(eq("name", name));
    }

}
