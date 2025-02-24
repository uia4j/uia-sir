package uia.sir.simple;

import java.util.List;
import java.util.Map;
import java.util.TreeMap;

public abstract class QueryRunner {

    protected Map<String, Builder> builders;

    protected QueryRunner() {
        this.builders = new TreeMap<>();
        this.builders.put(QueryPlan.select, this::select);
        this.builders.put(QueryPlan.aggregate, this::aggregate);
        this.builders.put(QueryPlan.daily, this::daily);
        this.builders.put(QueryPlan.weekly, this::weekly);
        this.builders.put(QueryPlan.monthly, this::monthly);
        this.builders.put(QueryPlan.quarter, this::quarter);
    }

    public List<Map<String, Object>> run(QueryPlan plan) throws Exception {
        if (plan.getColumns().isEmpty()) {
            throw new Exception("plan.fields not found");
        }
        if (!QueryPlan.select.equals(plan.getMethod()) && plan.getAccumulators().isEmpty()) {
            throw new Exception("plan.accumulators not found");
        }

        List<Map<String, Object>> result = this.builders.get(plan.getMethod()).run(plan);
        sort(result, plan.getOrderBy());
        return result;
    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    protected void sort(List<Map<String, Object>> result, List<String> orders) {
        if (orders.isEmpty()) {
            return;
        }

        result.sort((a, b) -> {
            for (String f : orders) {
                Object v1 = a.get(f);
                Object v2 = b.get(f);

                int c = 0;
                if (v1 != null && v2 != null) {
                    if (v1 instanceof Comparable && v2 instanceof Comparable) {
                        c = ((Comparable) v1).compareTo(v2);
                    }
                    else {
                        c = v1.toString().compareTo(v2.toString());
                    }
                }
                else if (v1 == null) {
                    c = -1;
                }
                else if (v2 == null) {
                    c = 1;
                }

                if (c != 0) {
                    return c;
                }
            }

            return 0;
        });
    }

    protected abstract List<Map<String, Object>> select(QueryPlan plan) throws Exception;

    protected abstract List<Map<String, Object>> aggregate(QueryPlan plan) throws Exception;

    protected abstract List<Map<String, Object>> daily(QueryPlan plan) throws Exception;

    protected abstract List<Map<String, Object>> weekly(QueryPlan plan) throws Exception;

    protected abstract List<Map<String, Object>> monthly(QueryPlan plan) throws Exception;

    protected abstract List<Map<String, Object>> quarter(QueryPlan plan) throws Exception;

    protected static interface Builder {

        public List<Map<String, Object>> run(QueryPlan plan) throws Exception;
    }

}
