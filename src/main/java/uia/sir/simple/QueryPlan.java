package uia.sir.simple;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

public class QueryPlan {

    public static final String select = "select";

    public static final String aggregate = "aggregate";

    public static final String daily = "daily";

    public static final String weekly = "weekly";

    public static final String monthly = "monthly";

    public static final String quarter = "quarter";

    private final String collectionName;

    private final String method;

    private final List<String> columns;

    private final Map<String, ValueReader> readers;

    private final List<KoV> criteria;

    private final List<String> orderBy;

    private final List<Accumulator> accumulators;

    private String timeColumn;

    /**
     *
     * @param collectionName The collection name.
     * @param method The method.
     */
    public QueryPlan(String collectionName, String method) {
        this.collectionName = collectionName;
        this.method = method;
        this.columns = new ArrayList<>();
        this.readers = new TreeMap<>();
        this.criteria = new ArrayList<>();
        this.orderBy = new ArrayList<>();
        this.accumulators = new ArrayList<>();
    }

    public String getCollectionName() {
        return this.collectionName;
    }

    public String getMethod() {
        return this.method;
    }

    public List<String> getColumns() {
        return this.columns;
    }

    public QueryPlan addColumn(String column) {
        this.columns.add(column);
        return this;
    }

    public QueryPlan addColumn(String column, ValueReader reader) {
        this.columns.add(column);
        this.readers.put(column, reader);
        return this;
    }

    public Map<String, ValueReader> getValueReaders() {
        return this.readers;
    }

    public QueryPlan addValueReader(String column, ValueReader reader) {
        this.readers.put(column, reader);
        return this;
    }

    public List<KoV> getCriteria() {
        return this.criteria;
    }

    public QueryPlan addCriteria(KoV kov) {
        this.criteria.add(kov);
        return this;
    }

    public List<String> getOrderBy() {
        return this.orderBy;
    }

    public QueryPlan addOrderBy(String orderBy) {
        this.orderBy.add(orderBy);
        return this;
    }

    public List<Accumulator> getAccumulators() {
        return this.accumulators;
    }

    public QueryPlan addAccumulator(Accumulator acc) {
        this.accumulators.add(acc);
        return this;
    }

    public String getTimeColumn() {
        return this.timeColumn;
    }

    public void setTimeColumn(String timeColumn) {
        this.timeColumn = timeColumn;
    }
}
