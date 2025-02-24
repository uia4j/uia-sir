package uia.sir.ds.mgo.db;

import java.util.ArrayList;
import java.util.List;

import com.mongodb.client.model.Accumulators;
import com.mongodb.client.model.BsonField;

public class MgoAccumulator {

    private List<BsonField> fields;

    public MgoAccumulator() {
        this.fields = new ArrayList<>();
    }

    public MgoAccumulator count(String newField) {
        this.fields.add(Accumulators.sum(newField, 1));
        return this;
    }

    public MgoAccumulator sum(String newField, String acFieldName) {
        this.fields.add(Accumulators.sum(newField, "$" + acFieldName));
        return this;
    }

    public MgoAccumulator max(String newField, String acFieldName) {
        this.fields.add(Accumulators.max(newField, "$" + acFieldName));
        return this;
    }

    public MgoAccumulator min(String newField, String acFieldName) {
        this.fields.add(Accumulators.min(newField, "$" + acFieldName));
        return this;
    }

    public List<BsonField> build() {
        return this.fields;
    }
}
