package uia.sir;

import org.junit.Test;

import uia.sir.db.QueryPlanInfo;
import uia.sir.db.QueryPlanInfo.AccInfo;
import uia.sir.db.QueryPlanInfo.KoVInfo;
import uia.sir.db.QueryPlanInfo.SortInfo;
import uia.sir.db.dao.QueryPlanInfoDao;
import uia.sir.db.dao.SIR;
import uia.sir.db.dao.SIRClient;

public class SimpleReportBuilderTest extends AbstractTest {

    public SimpleReportBuilderTest() throws Exception {
    }

    @Test
    public void testCase1Create() throws Exception {
        QueryPlanInfo plan = new QueryPlanInfo();
        plan.setPlanId("case01");
        plan.setPlanner("30026400");
        plan.setPlanName("WAFER CT DONE");
        plan.setDatasetName("WAFER_CT_DONE_LOG");
        plan.setTableName("WAFER_CT_DONE_LOG");
        plan.getDataSources().add("KS.ROAD");
        plan.getDataSources().add("JS.ROAD");
        plan.setMethod("aggregate");

        // column
        plan.getColumns().add(new QueryPlanInfo.ColInfo(1, "stage_name", "大站", "S"));
        plan.getColumns().add(new QueryPlanInfo.ColInfo(2, "operation_name", "小站", "S"));
        plan.getColumns().add(new QueryPlanInfo.ColInfo(3, "device_name", "机种", "S"));
        plan.getColumns().add(new QueryPlanInfo.ColInfo(5, "equip_name", "设备", "S"));

        plan.getOrderBy().add(new SortInfo(1, "stage_name", true));
        plan.getOrderBy().add(new SortInfo(2, "operation_name", true));

        // criteria
        plan.getCriteria().add(new KoVInfo(
                "log_time",
                "between",
                "2025-02-23 00:00:00",
                "2025-02-24 00:00:00"));

        // aggregation
        plan.getAccumulators().add(new AccInfo(1, "countDistinct", "lot_count", "lot_id"));
        plan.getAccumulators().add(new AccInfo(2, "countDistinct", "wafer_count", "wafer_id"));

        try (SIRClient client = SIR.create()) {
            QueryPlanInfoDao pDao = client.queryPlan();
            pDao.delete(plan.getPlanId());
            pDao.insert(plan);
        }
    }

    @Test
    public void testCase1Run() throws Exception {
        SimpleReportBuilder builder = new SimpleReportBuilder();
        builder.run("case01").forEach(row -> {
            System.out.printf("%7s, %-30s, %-30s, %-30s, %5s, %5s\n",
                    row.get("__ds__"),
                    row.get("stage_name"),
                    row.get("operation_name"),
                    row.get("equip_name"),
                    row.get("lot_count"),
                    row.get("wafer_count"));
        });
    }
}
