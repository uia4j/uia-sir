package uia.sir.db.dao;

import static com.mongodb.client.model.Sorts.ascending;

import java.util.List;

import org.bson.Document;
import org.bson.conversions.Bson;
import org.bson.types.ObjectId;

import com.mongodb.client.FindIterable;
import com.mongodb.client.ListIndexesIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Filters;
import com.mongodb.client.result.DeleteResult;
import com.mongodb.client.result.InsertManyResult;
import com.mongodb.client.result.InsertOneResult;
import com.mongodb.client.result.UpdateResult;

import uia.sir.db.Mgo;

public class MgoDao<T extends Mgo> {

    protected MongoDatabase database;

    protected MongoCollection<T> collection;

    public MgoDao(MongoDatabase database, String collectionName, Class<T> clz) {
        this.database = database;
        this.collection = database.getCollection(collectionName, clz);
    }

    public InsertOneResult insert(T one) {
        if (one == null) {
            return InsertOneResult.unacknowledged();
        }
        return this.collection.insertOne(one);
    }

    public InsertManyResult insert(List<T> many) {
        if (many.isEmpty()) {
            return InsertManyResult.unacknowledged();
        }
        return this.collection.insertMany(many);
    }

    public UpdateResult update(T one) {
        if (one == null) {
            return UpdateResult.unacknowledged();
        }
        return this.collection.replaceOne(Filters.eq("_id", one.getId()), one);
    }

    public void drop() {
        this.collection.drop();
    }

    public DeleteResult deleteById(String id) {
        return this.collection.deleteOne(Filters.eq("_id", new ObjectId(id)));
    }

    public DeleteResult deleteById(ObjectId oid) {
        return this.collection.deleteOne(Filters.eq("_id", oid));
    }

    public T selectById(String id) {
        return this.collection.find(Filters.eq("_id", new ObjectId(id))).first();
    }

    public FindIterable<T> selectAll() {
        return this.collection.find();
    }

    public FindIterable<T> selectAll(Bson orderBy) {
        return this.collection.find().sort(orderBy);
    }

    public FindIterable<T> selectAll(String... sort) {
        return this.collection.find().sort(ascending(sort));
    }

    public T selectOne(Bson where) {
        return this.collection.find(where).first();
    }

    public FindIterable<T> select(Bson where) {
        return this.collection.find(where);
    }

    public FindIterable<T> select(Bson where, Bson orderBy) {
        return this.collection.find(where).sort(orderBy);
    }

    public ListIndexesIterable<Document> listIndexes() {
        return this.collection.listIndexes();
    }

    public String createIndex(Bson keys) {
        return this.collection.createIndex(keys);
    }
}
