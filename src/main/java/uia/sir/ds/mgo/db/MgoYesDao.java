package uia.sir.ds.mgo.db;

import static com.mongodb.client.model.Aggregates.group;
import static com.mongodb.client.model.Aggregates.match;
import static com.mongodb.client.model.Projections.fields;
import static com.mongodb.client.model.Projections.include;
import static com.mongodb.client.model.Sorts.ascending;
import static com.mongodb.client.model.Sorts.descending;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.bson.Document;
import org.bson.conversions.Bson;

import com.mongodb.client.AggregateIterable;
import com.mongodb.client.DistinctIterable;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;

import uia.sir.simple.ValueReader;

public class MgoYesDao {

    protected MongoDatabase database;

    protected MongoCollection<Document> collection;

    public MgoYesDao(MongoDatabase database, String collectionName) {
        this.database = database;
        this.collection = database.getCollection(collectionName);
    }

    public Document selectOne(List<String> fields, MgoWhere where, String... orders) {
        return this.collection
                .find(where.build())
                .projection(fields(include(fields)))
                .sort(ascending(orders))
                .first();
    }

    public FindIterable<Document> select(List<String> fields, MgoWhere where, String... orders) {
        return select(fields, where, 0, true, orders);
    }

    public FindIterable<Document> select(List<String> fields, MgoWhere where, int limit, boolean ascending, String... orders) {
        if (ascending) {
            return this.collection
                    .find(where.build())
                    .projection(fields(include(fields)))
                    .limit(limit)
                    .sort(ascending(orders));
        }
        else {
            return this.collection
                    .find(where.build())
                    .projection(fields(include(fields)))
                    .limit(limit)
                    .sort(descending(orders));
        }
    }

    public DistinctIterable<String> distinctStr(String field, MgoWhere where) {
        return distinct(field, where, String.class);
    }

    public DistinctIterable<Integer> distinctInt(String field, MgoWhere where) {
        return distinct(field, where, Integer.class);
    }

    public <T> DistinctIterable<T> distinct(String field, MgoWhere where, Class<T> clz) {
        return this.collection.distinct(field, where.build(), clz);
    }

    /**
     * <p>
     * Returned documents must not violate the BSON
     * document size limit of 16 megabytes.
     * </p>
     *  <p>
     *  Pipeline stages have a memory limit of 100 megabytes
     *  by default. If required, you can exceed this limit
     *  by using the allowDiskUse method.
     * </p>
     *
     * @param where
     * @return
     */
    public AggregateIterable<Document> aggreagte(List<String> fields, MgoWhere where, MgoAccumulator acc) {
        Document grpFields = new Document(fields.get(0), "$" + fields.get(0));
        for (int i = 1; i < fields.size(); i++) {
            grpFields = grpFields.append(fields.get(i), "$" + fields.get(i));
        }

        List<Bson> pipeline = Arrays.asList(
                match(where.build()),
                group(grpFields, acc.build()));
        return this.collection.aggregate(pipeline);
    }

    public AggregateIterable<Document> daily(List<String> fields, String timeField, MgoWhere where, MgoAccumulator acc) {
        Document format = new Document();
        format.put("format", "%Y-%m-%d");
        format.put("date", "$" + timeField);
        format.put("timezone", "+0800");

        Document grpFields = new Document(timeField, new Document("$dateToString", format));
        for (int i = 0; i < fields.size(); i++) {
            grpFields = grpFields.append(fields.get(i), "$" + fields.get(i));
        }

        List<Bson> pipeline = Arrays.asList(
                match(where.build()),
                group(grpFields, acc.build()));
        return this.collection.aggregate(pipeline);
    }

    public AggregateIterable<Document> weekly(List<String> fields, String timeField, MgoWhere where, MgoAccumulator acc) {
        Document format = new Document();
        format.put("format", "%Y-%V");
        format.put("date", "$" + timeField);
        format.put("timezone", "+0800");

        Document grpFields = new Document(timeField, new Document("$dateToString", format));
        for (int i = 0; i < fields.size(); i++) {
            grpFields = grpFields.append(fields.get(i), "$" + fields.get(i));
        }

        List<Bson> pipeline = Arrays.asList(
                match(where.build()),
                group(grpFields, acc.build()));
        return this.collection.aggregate(pipeline);
    }

    public AggregateIterable<Document> monthly(List<String> fields, String timeField, MgoWhere where, MgoAccumulator acc) {
        Document format = new Document();
        format.put("format", "%Y-%m");
        format.put("date", "$" + timeField);
        format.put("timezone", "+0800");

        Document grpFields = new Document(timeField, new Document("$dateToString", format));
        for (int i = 0; i < fields.size(); i++) {
            grpFields = grpFields.append(fields.get(i), "$" + fields.get(i));
        }

        List<Bson> pipeline = Arrays.asList(
                match(where.build()),
                group(grpFields, acc.build()));
        return this.collection.aggregate(pipeline);
    }

    public AggregateIterable<Document> quarter(List<String> fields, String timeField, MgoWhere where, MgoAccumulator acc) {
        Document format = new Document();
        format.put("date", "$" + timeField);
        format.put("unit", "quarter");
        format.put("timezone", "+0800");

        Document grpFields = new Document(timeField, new Document("$dateTrunc", format));
        for (int i = 0; i < fields.size(); i++) {
            grpFields = grpFields.append(fields.get(i), "$" + fields.get(i));
        }

        List<Bson> pipeline = Arrays.asList(
                match(where.build()),
                group(grpFields, acc.build()));
        return this.collection.aggregate(pipeline);
    }

    public static Map<String, Object> keys(Document aggRow) {
        return new TreeMap<String, Object>(aggRow);
    }

    public static Map<String, Object> toRow(Map<String, ValueReader> readers, Document aggRow, String... extra) {
        return toRow(readers, aggRow, Arrays.asList(extra));
    }

    public static Map<String, Object> toRow(Map<String, ValueReader> readers, Document aggRow, List<String> extra) {
        Map<String, Object> result = new TreeMap<>();

        Document key = (Document) aggRow.remove("_id");
        key.forEach((k, v) -> {
            result.put(k, readers.getOrDefault(k, ValueReader.simple).apply(v));
        });
        for (String one : extra) {
            result.put(one, readers.getOrDefault(one, ValueReader.simple).apply(aggRow.get(one)));
        }
        return result;
    }
}
