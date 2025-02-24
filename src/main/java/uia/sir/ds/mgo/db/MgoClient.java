package uia.sir.ds.mgo.db;

import java.io.IOException;

import com.mongodb.client.MongoDatabase;

import uia.sir.ds.Client;
import uia.sir.ds.mgo.MgoQueryRunner;
import uia.sir.simple.QueryRunner;

public class MgoClient implements Client {

    final MongoDatabase database;

    public MgoClient(MongoDatabase database) {
        this.database = database;
    }

    public MgoYesDao dao(String collectionName) {
        return new MgoYesDao(this.database, collectionName);
    }

    @Override
    public QueryRunner runner(String collectionName) {
        return new MgoQueryRunner(dao(collectionName));
    }

    @Override
    public void close() throws IOException {

    }
}
