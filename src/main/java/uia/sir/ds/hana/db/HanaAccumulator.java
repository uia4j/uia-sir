package uia.sir.ds.hana.db;

import java.util.ArrayList;
import java.util.List;

public class HanaAccumulator {

    private List<String> ref;

    private List<String> newNames;

    private List<String> stats;

    public HanaAccumulator() {
        this.ref = new ArrayList<>();
        this.newNames = new ArrayList<>();
        this.stats = new ArrayList<>();
    }

    public HanaAccumulator count(String newField) {
        this.newNames.add(newField);

        this.stats.add("COUNT(*) AS " + newField);
        return this;
    }

    public HanaAccumulator countDistinct(String newField, String acFieldName) {
        this.ref.add(acFieldName);
        this.newNames.add(newField);

        this.stats.add("COUNT(DISTINCT " + acFieldName + ") AS " + newField);
        return this;
    }

    public HanaAccumulator sum(String newField, String acFieldName) {
        this.ref.add(acFieldName);
        this.newNames.add(newField);

        this.stats.add("SUM(" + acFieldName + ") AS " + newField);
        return this;
    }

    public HanaAccumulator max(String newField, String acFieldName) {
        this.ref.add(acFieldName);
        this.newNames.add(newField);

        this.stats.add("MAX(" + acFieldName + ") AS " + newField);
        return this;
    }

    public HanaAccumulator min(String newField, String acFieldName) {
        this.ref.add(acFieldName);
        this.newNames.add(newField);

        this.stats.add("MIN(" + acFieldName + ") AS " + newField);
        return this;
    }

    public List<String> getRef() {
        return this.ref;
    }

    public List<String> getNewNames() {
        return this.newNames;
    }

    public void setNewNames(List<String> newNames) {
        this.newNames = newNames;
    }

    public void setRefs(List<String> refs) {
        this.ref = refs;
    }

    public String build() {
        return String.join(",", this.stats);
    }
}
