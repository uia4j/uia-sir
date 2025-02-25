package uia.sir.db;

import org.bson.codecs.pojo.annotations.BsonId;
import org.bson.types.ObjectId;

public abstract class Mgo {

    @BsonId
    private ObjectId id;

    public ObjectId getId() {
        return this.id;
    }

    public void setId(ObjectId id) {
        this.id = id;
    }

}
