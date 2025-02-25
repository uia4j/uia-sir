package uia.sir;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import uia.sir.db.QueryPlanInfo;
import uia.sir.db.QueryPlanInfo.AccInfo;
import uia.sir.db.dao.SIR;
import uia.sir.db.dao.SIRClient;
import uia.sir.ds.Client;
import uia.sir.ds.ClientFactory;
import uia.sir.simple.Accumulator;
import uia.sir.simple.KoV;
import uia.sir.simple.QueryPlan;

public class SimpleReportBuilder {

    public List<Map<String, Object>> run(String planId) throws Exception {
        try (SIRClient sir = SIR.create()) {
            QueryPlanInfo pi = sir.queryPlan().selectOne(planId);

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

            List<Map<String, Object>> merged = new ArrayList<>();
            ExecutorService es = Executors.newFixedThreadPool(4);
            for (final String ds : pi.getDataSources()) {
                es.execute(new Runnable() {

                    @Override
                    public void run() {
                        try (Client client = ClientFactory.client(ds)) {
                            client.runner(plan.getCollectionName())
                                    .run(plan)
                                    .forEach(row -> {
                                        row.put("__ds__", ds);
                                        merged.add(row);
                                    });
                        }
                        catch (Exception ex) {
                        }
                    }

                });
            }

            es.shutdown();
            try {
                if (!es.awaitTermination(60, TimeUnit.SECONDS)) {
                    es.shutdownNow();
                }
            }
            catch (InterruptedException e) {
                es.shutdownNow();
            }

            return merged;
        }
    }

}
