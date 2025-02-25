package uia.sir.db;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class QueryPlanInfo extends Mgo {

    private String planId;

    private String planner;

    private String planName;

    private String datasetName;

    private String tableName;

    private List<String> dataSources;

    private String method;

    private List<ColInfo> columns;

    private List<KoVInfo> criteria;

    private List<SortInfo> orderBy;

    private List<AccInfo> accumulators;

    private String timeColumn;

    public QueryPlanInfo() {
        this.dataSources = new ArrayList<>();
        this.method = "aggregate";
        this.columns = new ArrayList<>();
        this.criteria = new ArrayList<>();
        this.orderBy = new ArrayList<>();
        this.accumulators = new ArrayList<>();
    }

    public String getPlanId() {
        return this.planId;
    }

    public void setPlanId(String planId) {
        this.planId = planId;
    }

    public String getPlanner() {
        return this.planner;
    }

    public void setPlanner(String planner) {
        this.planner = planner;
    }

    public String getPlanName() {
        return this.planName;
    }

    public void setPlanName(String planName) {
        this.planName = planName;
    }

    public String getDatasetName() {
        return this.datasetName;
    }

    public void setDatasetName(String datasetName) {
        this.datasetName = datasetName;
    }

    public String getTableName() {
        return this.tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public List<String> getDataSources() {
        return this.dataSources;
    }

    public void setDataSources(List<String> dataSources) {
        this.dataSources = dataSources;
    }

    public String getMethod() {
        return this.method;
    }

    public void setMethod(String method) {
        this.method = method;
    }

    public List<ColInfo> getColumns() {
        return this.columns;
    }

    public void setColumns(List<ColInfo> columns) {
        this.columns = columns;
    }

    public List<KoVInfo> getCriteria() {
        return this.criteria;
    }

    public void setCriteria(List<KoVInfo> criteria) {
        this.criteria = criteria;
    }

    public List<SortInfo> getOrderBy() {
        return this.orderBy;
    }

    public void setOrderBy(List<SortInfo> orderBy) {
        this.orderBy = orderBy;
    }

    public List<AccInfo> getAccumulators() {
        return this.accumulators;
    }

    public void setAccumulators(List<AccInfo> accumulators) {
        this.accumulators = accumulators;
    }

    public String getTimeColumn() {
        return this.timeColumn;
    }

    public void setTimeColumn(String timeColumn) {
        this.timeColumn = timeColumn;
    }

    public static class ColInfo {

        private int seqNo;

        private String name;

        private String displayName;

        private String dataType;

        private String align;

        private String valueReader;

        public ColInfo() {
        }

        public ColInfo(int seqNo, String name, String displayName, String dataType) {
            this.seqNo = seqNo;
            this.name = name;
            this.displayName = displayName;
            this.dataType = dataType;
            this.align = "L";
        }

        public int getSeqNo() {
            return this.seqNo;
        }

        public void setSeqNo(int seqNo) {
            this.seqNo = seqNo;
        }

        public String getName() {
            return this.name;
        }

        public void setName(String name) {
            this.name = name;
        }

        public String getDisplayName() {
            return this.displayName;
        }

        public void setDisplayName(String displayName) {
            this.displayName = displayName;
        }

        public String getDataType() {
            return this.dataType;
        }

        public void setDataType(String dataType) {
            this.dataType = dataType;
        }

        public String getAlign() {
            return this.align;
        }

        public void setAlign(String align) {
            this.align = align;
        }

        public String getValueReader() {
            return this.valueReader;
        }

        public void setValueReader(String valueReader) {
            this.valueReader = valueReader;
        }
    }

    public static class SortInfo {

        private int seqNo;

        private String name;

        private boolean asccending;

        public SortInfo() {
            this.asccending = true;
        }

        public SortInfo(int seqNo, String name, boolean asccending) {
            this.seqNo = seqNo;
            this.name = name;
            this.asccending = asccending;
        }

        public int getSeqNo() {
            return this.seqNo;
        }

        public void setSeqNo(int seqNo) {
            this.seqNo = seqNo;
        }

        public String getName() {
            return this.name;
        }

        public void setName(String name) {
            this.name = name;
        }

        public boolean isAsccending() {
            return this.asccending;
        }

        public void setAsccending(boolean asccending) {
            this.asccending = asccending;
        }

    }

    public static class KoVInfo {

        private String key;

        private String op;

        private List<String> values;

        public KoVInfo() {
            this.values = new ArrayList<>();
        }

        public KoVInfo(String key, String op, String... values) {
            this.key = key;
            this.op = op;
            this.values = Arrays.asList(values);
        }

        public String getKey() {
            return this.key;
        }

        public void setKey(String key) {
            this.key = key;
        }

        public String getOp() {
            return this.op;
        }

        public void setOp(String op) {
            this.op = op;
        }

        public List<String> getValues() {
            return this.values;
        }

        public void setValues(List<String> values) {
            this.values = values;
        }
    }

    public static class AccInfo {

        private int seqNo;

        private String type;

        private String outputField;

        private String accField;

        public AccInfo() {
        }

        public AccInfo(int seqNo, String type, String outputField, String accField) {
            this.seqNo = seqNo;
            this.type = type;
            this.outputField = outputField;
            this.accField = accField;
        }

        public int getSeqNo() {
            return this.seqNo;
        }

        public void setSeqNo(int seqNo) {
            this.seqNo = seqNo;
        }

        public String getType() {
            return this.type;
        }

        public void setType(String type) {
            this.type = type;
        }

        public String getOutputField() {
            return this.outputField;
        }

        public void setOutputField(String outputField) {
            this.outputField = outputField;
        }

        public String getAccField() {
            return this.accField;
        }

        public void setAccField(String accField) {
            this.accField = accField;
        }

    }
}
