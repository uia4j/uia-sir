package uia.sir.ds.mgo;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.stream.Collectors;

import org.bson.Document;

import com.mongodb.client.MongoIterable;

import uia.sir.ds.mgo.db.MgoAccumulator;
import uia.sir.ds.mgo.db.MgoWhere;
import uia.sir.ds.mgo.db.MgoYesDao;
import uia.sir.simple.Accumulator;
import uia.sir.simple.QueryPlan;
import uia.sir.simple.QueryRunner;

public class MgoQueryRunner extends QueryRunner {

    private final MgoYesDao dao;

    public MgoQueryRunner(MgoYesDao dao) {
        this.dao = dao;
    }

    @Override
    protected List<Map<String, Object>> select(QueryPlan plan) throws Exception {
        MgoWhere where = MgoKoV.and(plan.getCriteria());
        return convert1(
                plan,
                this.dao.select(
                        plan.getColumns(),
                        where));
    }

    @Override
    protected List<Map<String, Object>> aggregate(QueryPlan plan) throws Exception {
        return convert2(
                plan,
                this.dao.aggreagte(
                        plan.getColumns(),
                        MgoKoV.and(plan.getCriteria()),
                        create(plan.getAccumulators())));
    }

    @Override
    protected List<Map<String, Object>> daily(QueryPlan plan) throws Exception {
        if (plan.getTimeColumn() == null) {
            throw new Exception("plan.timeField not found");
        }
        return convert2(
                plan,
                this.dao.daily(
                        plan.getColumns(),
                        plan.getTimeColumn(),
                        MgoKoV.and(plan.getCriteria()),
                        create(plan.getAccumulators())));
    }

    @Override
    protected List<Map<String, Object>> weekly(QueryPlan plan) throws Exception {
        if (plan.getTimeColumn() == null) {
            throw new Exception("plan.timeField not found");
        }
        return convert2(
                plan,
                this.dao.weekly(
                        plan.getColumns(),
                        plan.getTimeColumn(),
                        MgoKoV.and(plan.getCriteria()),
                        create(plan.getAccumulators())));
    }

    @Override
    protected List<Map<String, Object>> monthly(QueryPlan plan) throws Exception {
        if (plan.getTimeColumn() == null) {
            throw new Exception("plan.timeField not found");
        }
        return convert2(
                plan,
                this.dao.monthly(
                        plan.getColumns(),
                        plan.getTimeColumn(),
                        MgoKoV.and(plan.getCriteria()),
                        create(plan.getAccumulators())));
    }

    @Override
    protected List<Map<String, Object>> quarter(QueryPlan plan) throws Exception {
        if (plan.getTimeColumn() == null) {
            throw new Exception("plan.timeField not found");
        }
        return convert2(
                plan,
                this.dao.quarter(
                        plan.getColumns(),
                        plan.getTimeColumn(),
                        MgoKoV.and(plan.getCriteria()),
                        create(plan.getAccumulators())));
    }

    protected MgoAccumulator create(List<Accumulator> accs) throws Exception {
        MgoAccumulator accImpl = new MgoAccumulator();
        for (Accumulator acc : accs) {
            if (Accumulator.count.equals(acc.getType())) {
                accImpl = accImpl.count(acc.getOutputField());
            }
            else if (Accumulator.sum.equals(acc.getType())) {
                accImpl = accImpl.sum(acc.getOutputField(), acc.getAccField());
            }
            else {
                throw new Exception(acc.getType() + " not supported");
            }
        }
        return accImpl;
    }

    private List<Map<String, Object>> convert1(QueryPlan plan, MongoIterable<Document> docs) {
        List<Map<String, Object>> result = new ArrayList<>();
        docs.forEach(d -> {
            Map<String, Object> row = new TreeMap<>(d);
            row.remove("_id");
            result.add(row);
        });
        return result;
    }

    private List<Map<String, Object>> convert2(QueryPlan plan, MongoIterable<Document> docs) {
        List<String> ofs = plan.getAccumulators()
                .stream()
                .map(a -> a.getOutputField())
                .collect(Collectors.toList());
        List<Map<String, Object>> result = new ArrayList<>();
        docs.forEach(d -> {
            result.add(MgoYesDao.toRow(d, ofs));
        });
        return result;
    }
}
