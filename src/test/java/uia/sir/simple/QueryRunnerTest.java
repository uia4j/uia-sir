package uia.sir.simple;

import static uia.sir.simple.Accumulator.countDistinctTo;
import static uia.sir.simple.KoV.between;
import static uia.sir.simple.KoV.startsWith;

import java.util.Date;

import org.junit.Test;

import uia.sir.AbstractTest;
import uia.sir.HandleEx;
import uia.sir.db.QueryPlanInfo;
import uia.sir.db.QueryPlanInfo.AccInfo;
import uia.sir.db.dao.SIR;
import uia.sir.db.dao.SIRClient;
import uia.sir.ds.Client;
import uia.sir.ds.ClientFactory;

public class QueryRunnerTest extends AbstractTest {

    public QueryRunnerTest() throws Exception {
    }

    @Test
    public void test() throws Exception {
        try (SIRClient sir = SIR.create()) {
            QueryPlanInfo pi = sir.queryPlan().selectOne("123");

            QueryPlan plan = new QueryPlan(pi.getDatasetName(), pi.getMethod());

            // columns
            pi.getColumns().forEach(c -> plan.addColumn(c.getName()));

            // criteria
            pi.getCriteria().forEach(kov -> {
                if (KoV.eq.equals(kov.getOp())) {
                    plan.addCriteria(KoV.eq(kov.getKey(), kov.getValues().get(0)));
                }
                else if (KoV.ne.equals(kov.getOp())) {
                    plan.addCriteria(KoV.ne(kov.getKey(), kov.getValues().get(0)));
                }
                else if (KoV.gt.equals(kov.getOp())) {
                    plan.addCriteria(KoV.gt(kov.getKey(), kov.getValues().get(0)));
                }
                else if (KoV.gte.equals(kov.getOp())) {
                    plan.addCriteria(KoV.gte(kov.getKey(), kov.getValues().get(0)));
                }
                else if (KoV.lt.equals(kov.getOp())) {
                    plan.addCriteria(KoV.lt(kov.getKey(), kov.getValues().get(0)));
                }
                else if (KoV.lte.equals(kov.getOp())) {
                    plan.addCriteria(KoV.lte(kov.getKey(), kov.getValues().get(0)));
                }
                else if (KoV.between.equals(kov.getOp())) {
                    plan.addCriteria(KoV.between(kov.getKey(), kov.getValues().get(0), kov.getValues().get(1)));
                }
            });

            // accumulators
            for (AccInfo ai : pi.getAccumulators()) {
                if (Accumulator.count.equals(ai.getType())) {
                    plan.addAccumulator(Accumulator.countTo(ai.getOutputField()));
                }
                else if (Accumulator.countDistinct.equals(ai.getType())) {
                    plan.addAccumulator(Accumulator.countDistinctTo(ai.getOutputField(), ai.getAccField()));
                }
                else if (Accumulator.sum.equals(ai.getType())) {
                    plan.addAccumulator(Accumulator.sumTo(ai.getOutputField(), ai.getAccField()));
                }
            }

            for (String ds : pi.getDataSources()) {
                // run
                try (Client client = ClientFactory.client(ds)) {
                    client.runner(plan.getCollectionName())
                            .run(plan)
                            .forEach(System.out::println);
                }
            }
        }
    }

    @Test
    public void test1() throws Exception {
        // plan> collecitonName, method
        QueryPlan plan = new QueryPlan("report_info", QueryPlan.select)
                .addColumn("report_id")
                .addColumn("fab_op_name", v -> HandleEx.trim((String) v))
                .addColumn("fab_lot_name", v -> HandleEx.trim((String) v))
                .addCriteria(startsWith("fab_lot_name", "SFCBO:1020,BF2AM008"))
                .addOrderBy("report_id");

        // run
        try (Client client = ClientFactory.client("TREK")) {    // data source: TREK
            client.runner(plan.getCollectionName())
                    .run(plan)
                    .forEach(System.out::println);

        }
    }

    @Test
    public void test2() throws Exception {
        // plan> collecitonName, method
        QueryPlan plan = new QueryPlan("wafer_ct_done_log", QueryPlan.aggregate)
                .addColumn("stage_name")
                .addCriteria(between(
                        "log_time",
                        new Date(System.currentTimeMillis() - 86400000L * 60),
                        new Date()))
                .addAccumulator(countDistinctTo("wafers", "wafer_id"))
                .addAccumulator(countDistinctTo("lot", "lot_id"));

        // run
        try (Client client = ClientFactory.client("KS")) {    // data source: KS
            client.runner(plan.getCollectionName())
                    .run(plan)
                    .forEach(System.out::println);

        }
    }
}
