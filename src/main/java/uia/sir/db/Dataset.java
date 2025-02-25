package uia.sir.db;

import java.util.ArrayList;
import java.util.List;

public class Dataset extends Mgo {

    private String name;

    private String description;

    private String tableName;

    private List<String> dataSources;

    private List<ColumnInfo> columns;

    public Dataset() {
        this.dataSources = new ArrayList<>();
        this.columns = new ArrayList<>();
    }

    public String getName() {
        return this.name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getDescription() {
        return this.description;
    }

    public void setDescription(String description) {
        this.description = description;
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

    public List<ColumnInfo> getColumns() {
        return this.columns;
    }

    public void setColumns(List<ColumnInfo> columns) {
        this.columns = columns;
    }

    @Override
    public String toString() {
        return this.name;
    }

    public static class ColumnInfo {

        private String name;

        private String displayName;

        private String description;

        private String dataType;

        private boolean enabled;

        private String alignment;

        private boolean pk;

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

        public String getDescription() {
            return this.description;
        }

        public void setDescription(String description) {
            this.description = description;
        }

        public String getDataType() {
            return this.dataType;
        }

        public void setDataType(String dataType) {
            this.dataType = dataType;
        }

        public boolean isEnabled() {
            return this.enabled;
        }

        public void setEnabled(boolean enabled) {
            this.enabled = enabled;
        }

        public String getAlignment() {
            return this.alignment;
        }

        public void setAlignment(String alignment) {
            this.alignment = alignment;
        }

        public boolean isPk() {
            return this.pk;
        }

        public void setPk(boolean pk) {
            this.pk = pk;
        }

        @Override
        public String toString() {
            return this.name;
        }

    }
}
