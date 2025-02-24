package uia.sir.ds.mgo.db;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;

import org.bson.conversions.Bson;

import com.mongodb.client.model.Filters;

public abstract class MgoWhere {

    protected final List<Bson> criteria;

    protected MgoWhere() {
        this.criteria = new ArrayList<>();
    }

    public MgoWhere eq(String fieldName, Object value) {
        this.criteria.add(Filters.eq(fieldName, value));
        return this;
    }

    public MgoWhere ne(String fieldName, Object value) {
        this.criteria.add(Filters.ne(fieldName, value));
        return this;
    }

    public MgoWhere in(String fieldName, Object... values) {
        this.criteria.add(Filters.in(fieldName, values));
        return this;
    }

    public MgoWhere nin(String fieldName, Object... values) {
        this.criteria.add(Filters.nin(fieldName, values));
        return this;
    }

    public MgoWhere gt(String fieldName, Object value) {
        this.criteria.add(Filters.gt(fieldName, value));
        return this;
    }

    public MgoWhere gte(String fieldName, Object value) {
        this.criteria.add(Filters.gte(fieldName, value));
        return this;
    }

    public MgoWhere lt(String fieldName, Object value) {
        this.criteria.add(Filters.lt(fieldName, value));
        return this;
    }

    public MgoWhere lte(String fieldName, Object value) {
        this.criteria.add(Filters.lte(fieldName, value));
        return this;
    }

    public MgoWhere between(String fieldName, Object value1, Object value2) {
        return gte(fieldName, value1).lt(fieldName, value2);
    }

    public MgoWhere startsWith(String fieldName, String value) {
        this.criteria.add(Filters.regex(fieldName, Pattern.compile("^" + value)));

        return this;
    }

    public MgoWhere with(MgoWhere w) {
        this.criteria.add(w.build());
        return this;
    }

    public abstract Bson build();

    public static MgoWhere and() {
        return new MgoWhere() {

            @Override
            public Bson build() {
                return Filters.and(this.criteria);
            }
        };
    }

    public static MgoWhere or() {

        return new MgoWhere() {

            @Override
            public Bson build() {
                return Filters.or(this.criteria);
            }
        };
    }
}
