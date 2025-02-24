package uia.sir.simple;

import static uia.sir.simple.Accumulator.countDistinctTo;
import static uia.sir.simple.KoV.between;
import static uia.sir.simple.KoV.startsWith;

import java.util.Date;

import org.junit.Test;

import uia.sir.AbstractTest;
import uia.sir.ds.Client;
import uia.sir.ds.ClientFactory;

public class QueryRunnerTest extends AbstractTest {

    public QueryRunnerTest() throws Exception {
    }

    @Test
    public void test1() throws Exception {
        // plan> collecitonName, method
        QueryPlan plan = new QueryPlan("report_info", QueryPlan.select)
                .addColumn("report_id")
                .addColumn("fab_op_name")
                .addColumn("fab_lot_name")
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
