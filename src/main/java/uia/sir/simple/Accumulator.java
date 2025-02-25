package uia.sir.simple;

public class Accumulator {

    public static final String count = "count";

    public static final String countDistinct = "countDistinct";

    public static final String sum = "sum";

    private final String type;

    private final String outputField;

    private final String accField;

    private Accumulator(String type, String outputField, String accField) {
        this.type = type;
        this.outputField = outputField;
        this.accField = accField;
    }

    public static Accumulator countTo(String outputField) {
        return new Accumulator(count, outputField, null);
    }

    public static Accumulator countDistinctTo(String outputField, String accField) {
        return new Accumulator(countDistinct, outputField, accField);
    }

    public static Accumulator sumTo(String outputField, String accField) {
        return new Accumulator(sum, outputField, accField);
    }

    public String getType() {
        return this.type;
    }

    public String getAccField() {
        return this.accField;
    }

    public String getOutputField() {
        return this.outputField;
    }
}
