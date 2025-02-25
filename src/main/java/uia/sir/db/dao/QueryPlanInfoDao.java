package uia.sir.db.dao;

import static com.mongodb.client.model.Filters.eq;

import com.mongodb.client.MongoDatabase;
import com.mongodb.client.result.DeleteResult;

import uia.sir.db.QueryPlanInfo;

public class QueryPlanInfoDao extends MgoDao<QueryPlanInfo> {

    public QueryPlanInfoDao(MongoDatabase database) {
        super(database, "query_plan_info", QueryPlanInfo.class);
    }

    public QueryPlanInfo selectOne(String planId) {
        return this.collection.find(eq("planId", planId)).first();
    }

    public DeleteResult delete(String planId) {
        return this.collection.deleteMany(eq("planId", planId));
    }
}
