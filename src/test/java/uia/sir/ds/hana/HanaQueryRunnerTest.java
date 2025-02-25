package uia.sir.ds.hana;

import java.io.IOException;
import java.util.Date;

import org.junit.Test;

import uia.sir.ds.Client;
import uia.sir.ds.ClientFactory;
import uia.sir.simple.Accumulator;
import uia.sir.simple.KoV;
import uia.sir.simple.QueryPlan;

public class HanaQueryRunnerTest extends AbstractTest {

    public HanaQueryRunnerTest() throws Exception {
    }

    @Test
    public void test() throws IOException, Exception {
        QueryPlan plan = new QueryPlan("wafer_ct_done_log", QueryPlan.select);
        // plan> columns
        plan.addColumn("id");
        plan.addColumn("log_time");
        // plan> criteria
        plan.getCriteria().add(KoV.between("log_time",
                new Date(System.currentTimeMillis() - 3600000L * 7),
                new Date(System.currentTimeMillis())));

        // run
        try (Client client = ClientFactory.client("KS")) {
            client.runner(plan.getCollectionName())
                    .run(plan)
                    .forEach(System.out::println);

        }
    }

    @Test
    public void testAggreagtion() throws Exception {
        // plan> collecitonName, method
        QueryPlan plan = new QueryPlan("wafer_ct_done_log", QueryPlan.aggregate);
        // plan> columns
        plan.addColumn("stage_name");
        // plan> criteria
        plan.getCriteria().add(KoV.between("log_time", new Date(System.currentTimeMillis() - 86400000L * 60), new Date()));
        // plan> accumulators
        plan.getAccumulators().add(Accumulator.countDistinctTo("wafers", "wafer_id"));
        plan.getAccumulators().add(Accumulator.countDistinctTo("lot", "lot_id"));

        // run
        try (Client client = ClientFactory.client("KS")) {
            client.runner(plan.getCollectionName())
                    .run(plan)
                    .forEach(System.out::println);

        }
    }

    @Test
    public void testDaily() throws Exception {
        // plan> collecitonName, method
        QueryPlan plan = new QueryPlan("wafer_ct_done_log", QueryPlan.daily);
        // plan> columns
        plan.addColumn("stage_name");
        plan.addColumn("operation_name");
        plan.addColumn("equip_name");
        // plan> criteria
        plan.getCriteria().add(KoV.between("log_time", new Date(System.currentTimeMillis() - 86400000L * 60), new Date()));
        plan.getCriteria().add(KoV.startsWith("equip_name", "BPLT"));
        // plan> accumulators
        plan.getAccumulators().add(Accumulator.countDistinctTo("wafers", "wafer_id"));
        plan.getAccumulators().add(Accumulator.countDistinctTo("lot", "lot_id"));
        // plan> timeField
        plan.setTimeColumn("log_time");
        // plan> orderBy
        plan.getOrderBy().add("stage_name");
        plan.getOrderBy().add("log_time");

        // run
        try (Client client = ClientFactory.client("KS")) {
            client.runner(plan.getCollectionName())
                    .run(plan)
                    .forEach(System.out::println);

        }
    }
}
