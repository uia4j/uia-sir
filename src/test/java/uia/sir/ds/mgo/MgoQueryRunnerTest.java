package uia.sir.ds.mgo;

import java.util.Date;

import org.junit.Test;

import uia.sir.ds.Client;
import uia.sir.ds.ClientFactory;
import uia.sir.simple.Accumulator;
import uia.sir.simple.KoV;
import uia.sir.simple.QueryPlan;

public class MgoQueryRunnerTest extends AbstractTest {

    public MgoQueryRunnerTest() throws Exception {
    }

    @Test
    public void testSelect() throws Exception {
        // plan> collecitonName, method
        QueryPlan plan = new QueryPlan("report_info", QueryPlan.select);
        // plan> columns
        plan.getColumns().add("report_id");
        plan.getColumns().add("fab_op_name");
        // plan> criteria
        plan.getCriteria().add(KoV.eq("fab_lot_name", "SFCBO:1020,BF2AM008.1"));
        // plan> orderBy
        plan.getOrderBy().add("report_id");

        // run
        try (Client client = ClientFactory.client("TREK")) {
            client.runner(plan.getCollectionName())
                    .run(plan)
                    .forEach(System.out::println);

        }
    }

    @Test
    public void testDaily() throws Exception {
        // plan> collecitonName, method
        QueryPlan plan = new QueryPlan("report_info", QueryPlan.daily);
        // plan> columns
        plan.getColumns().add("fab_op_name");
        // plan> criteria
        plan.getCriteria().add(KoV.between(
                "upload_time",
                new Date(System.currentTimeMillis() - 86400000L * 10),
                new Date(System.currentTimeMillis())));
        // plan> orderBy
        plan.getOrderBy().add("upload_time");
        plan.getOrderBy().add("fab_op_name");

        // plan> accumulators
        plan.getAccumulators().add(Accumulator.countTo("output"));
        plan.getAccumulators().add(Accumulator.sumTo("qty", "num_good"));

        // plan> timeColumn
        plan.setTimeColumn("upload_time");

        // run
        try (Client client = ClientFactory.client("TREK")) {
            client.runner(plan.getCollectionName())
                    .run(plan)
                    .forEach(System.out::println);
        }
    }
}
