package uia.sir.simple;

import java.util.ArrayList;
import java.util.List;

public class KoV {

    public static final String eq = "eq";

    public static final String ne = "ne";

    public static final String gt = "gt";

    public static final String gte = "qte";

    public static final String lt = "lt";

    public static final String lte = "lte";

    public static final String between = "between";

    public static final String startsWith = "startsWith";

    private final String key;

    private final String op;

    private final List<Object> values;

    private KoV(String key, String op, Object... vs) {
        this.key = key;
        this.op = op;
        this.values = new ArrayList<>();
        for (Object v : vs) {
            this.values.add(v);
        }
    }

    public static KoV eq(String key, Object... vs) {
        return new KoV(key, eq, vs);
    }

    public static KoV ne(String key, Object... vs) {
        return new KoV(key, ne, vs);
    }

    public static KoV gt(String key, Object v) {
        return new KoV(key, gt, v);
    }

    public static KoV gte(String key, Object v) {
        return new KoV(key, gte, v);
    }

    public static KoV lt(String key, Object v) {
        return new KoV(key, lt, v);
    }

    public static KoV lte(String key, Object v) {
        return new KoV(key, lte, v);
    }

    public static KoV between(String key, Object v1, Object v2) {
        return new KoV(key, between, v1, v2);
    }

    public static KoV startsWith(String key, String value) {
        return new KoV(key, startsWith, value);
    }

    public String getKey() {
        return this.key;
    }

    public String getOp() {
        return this.op;
    }

    public List<Object> getValues() {
        return this.values;
    }

    public KoV addValue(Object v) {
        this.values.add(v);
        return this;
    }
}
