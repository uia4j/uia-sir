package uia.sir.ds.hana;

import java.util.List;
import java.util.Map;

import uia.dao.where.SimpleWhere;
import uia.sir.ds.hana.db.HanaAccumulator;
import uia.sir.ds.hana.db.HanaYesDao;
import uia.sir.simple.Accumulator;
import uia.sir.simple.QueryPlan;
import uia.sir.simple.QueryRunner;

public class HanaQueryRunner extends QueryRunner {

    private final HanaYesDao dao;

    public HanaQueryRunner(HanaYesDao dao) {
        this.dao = dao;
    }

    @Override
    protected List<Map<String, Object>> select(QueryPlan plan) throws Exception {
        SimpleWhere where = HanaKoV.and(plan.getCriteria(), this.dao.isSaveUTC());

        List<Map<String, Object>> result = this.dao.select(
                plan.getColumns(),
                where,
                plan.getValueReaders());
        return result;
    }

    @Override
    protected List<Map<String, Object>> aggregate(QueryPlan plan) throws Exception {
        List<Map<String, Object>> result = this.dao.aggregate(
                plan.getColumns(),
                HanaKoV.and(plan.getCriteria(), this.dao.isSaveUTC()),
                plan.getValueReaders(),
                create(plan.getAccumulators()));
        return result;
    }

    @Override
    protected List<Map<String, Object>> daily(QueryPlan plan) throws Exception {
        if (plan.getTimeColumn() == null) {
            throw new Exception("plan.timeField not found");
        }

        List<Map<String, Object>> result = this.dao.daily(
                plan.getColumns(),
                plan.getTimeColumn(),
                HanaKoV.and(plan.getCriteria(), true),
                plan.getValueReaders(),
                create(plan.getAccumulators()));
        return result;
    }

    @Override
    protected List<Map<String, Object>> weekly(QueryPlan plan) throws Exception {
        if (plan.getTimeColumn() == null) {
            throw new Exception("plan.timeField not found");
        }

        List<Map<String, Object>> result = this.dao.weekly(
                plan.getColumns(),
                plan.getTimeColumn(),
                HanaKoV.and(plan.getCriteria(), true),
                plan.getValueReaders(),
                create(plan.getAccumulators()));
        return result;
    }

    @Override
    protected List<Map<String, Object>> monthly(QueryPlan plan) throws Exception {
        if (plan.getTimeColumn() == null) {
            throw new Exception("plan.timeField not found");
        }

        List<Map<String, Object>> result = this.dao.monthly(
                plan.getColumns(),
                plan.getTimeColumn(),
                HanaKoV.and(plan.getCriteria(), true),
                plan.getValueReaders(),
                create(plan.getAccumulators()));
        return result;
    }

    @Override
    protected List<Map<String, Object>> quarter(QueryPlan plan) throws Exception {
        if (plan.getTimeColumn() == null) {
            throw new Exception("plan.timeField not found");
        }

        List<Map<String, Object>> result = this.dao.quarter(
                plan.getColumns(),
                plan.getTimeColumn(),
                HanaKoV.and(plan.getCriteria(), true),
                plan.getValueReaders(),
                create(plan.getAccumulators()));
        return result;
    }

    private HanaAccumulator create(List<Accumulator> accs) throws Exception {
        HanaAccumulator accImpl = new HanaAccumulator();
        for (Accumulator acc : accs) {
            if (Accumulator.count.equals(acc.getType())) {
                accImpl = accImpl.count(acc.getOutputField());
            }
            else if (Accumulator.countDistinct.equals(acc.getType())) {
                accImpl = accImpl.countDistinct(acc.getOutputField(), acc.getAccField());
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
}
