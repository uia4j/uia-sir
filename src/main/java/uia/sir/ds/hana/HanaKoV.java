package uia.sir.ds.hana;

import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.TreeSet;

import uia.dao.where.SimpleWhere;
import uia.dao.where.Where;
import uia.sir.simple.KoV;

public class HanaKoV {

    private static final Map<String, Builder> builders;

    static {
        builders = new TreeMap<>();
        builders.put(KoV.eq, HanaKoV::eq);
        builders.put(KoV.ne, HanaKoV::ne);
        builders.put(KoV.gt, HanaKoV::gt);
        builders.put(KoV.gte, HanaKoV::gte);
        builders.put(KoV.lt, HanaKoV::lt);
        builders.put(KoV.lte, HanaKoV::lte);
        builders.put(KoV.between, HanaKoV::between);
        builders.put(KoV.startsWith, HanaKoV::startsWith);
    }

    private HanaKoV() {
    }

    public static SimpleWhere and(List<KoV> vs) {
        SimpleWhere w = Where.simpleAnd();
        for (KoV v : vs) {
            builders.get(v.getOp()).run(w, v);
        }
        return w;
    }

    public static SimpleWhere or(List<KoV> vs) {
        SimpleWhere w = Where.simpleOr();
        for (KoV v : vs) {
            builders.get(v.getOp()).run(w, v);
        }
        return w;
    }

    private static void eq(SimpleWhere w, KoV kov) {
        if (kov.getValues().size() > 1) {
            TreeSet<String> strs = new TreeSet<>();
            kov.getValues().forEach(v -> strs.add(v.toString()));
            w.in(kov.getKey(), strs);
        }
        else {
            w.eq(kov.getKey(), kov.getValues().get(0));
        }
    }

    private static void ne(SimpleWhere w, KoV kov) {
        if (kov.getValues().size() > 1) {
            TreeSet<String> strs = new TreeSet<>();
            kov.getValues().forEach(v -> strs.add(v.toString()));
            w.notIn(kov.getKey(), strs);
        }
        else {
            w.notEq(kov.getKey(), kov.getValues().get(0));
        }
    }

    private static void between(SimpleWhere w, KoV kov) {
        w.between(kov.getKey(), kov.getValues().get(0), kov.getValues().get(1));
    }

    private static void gt(SimpleWhere w, KoV kov) {
        w.moreThan(kov.getKey(), kov.getValues().get(0), false);
    }

    private static void gte(SimpleWhere w, KoV kov) {
        w.moreThan(kov.getKey(), kov.getValues().get(0), true);
    }

    private static void lt(SimpleWhere w, KoV kov) {
        w.lessThan(kov.getKey(), kov.getValues().get(0), false);
    }

    private static void lte(SimpleWhere w, KoV kov) {
        w.lessThan(kov.getKey(), kov.getValues().get(0), true);
    }

    private static void startsWith(SimpleWhere w, KoV kov) {
        w.likeBegin(kov.getKey(), kov.getValues().get(0));
    }

    private static interface Builder {

        public void run(SimpleWhere w, KoV kov);
    }
}
