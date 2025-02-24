package uia.sir.ds.mgo;

import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import uia.sir.ds.mgo.db.MgoWhere;
import uia.sir.simple.KoV;

public class MgoKoV {

    private static final Map<String, Builder> builders;

    static {
        builders = new TreeMap<>();
        builders.put(KoV.eq, MgoKoV::eq);
        builders.put(KoV.ne, MgoKoV::ne);
        builders.put(KoV.gt, MgoKoV::gt);
        builders.put(KoV.gte, MgoKoV::gte);
        builders.put(KoV.lt, MgoKoV::lt);
        builders.put(KoV.lte, MgoKoV::lte);
        builders.put(KoV.between, MgoKoV::between);
        builders.put(KoV.startsWith, MgoKoV::startsWith);
    }

    private MgoKoV() {
    }

    public static MgoWhere and(List<KoV> vs) {
        MgoWhere w = MgoWhere.and();
        for (KoV v : vs) {
            builders.get(v.getOp()).run(w, v);
        }
        return w;
    }

    public static MgoWhere or(List<KoV> vs) {
        MgoWhere w = MgoWhere.or();
        for (KoV v : vs) {
            builders.get(v.getOp()).run(w, v);
        }
        return w;
    }

    private static void eq(MgoWhere w, KoV kov) {
        if (kov.getValues().size() > 1) {
            w.in(kov.getKey(), kov.getValues());
        }
        else {
            w.eq(kov.getKey(), kov.getValues().get(0));
        }
    }

    private static void ne(MgoWhere w, KoV kov) {
        if (kov.getValues().size() > 1) {
            w.nin(kov.getKey(), kov.getValues());
        }
        else {
            w.ne(kov.getKey(), kov.getValues().get(0));
        }
    }

    private static void between(MgoWhere w, KoV kov) {
        w.between(kov.getKey(), kov.getValues().get(0), kov.getValues().get(1));
    }

    private static void gt(MgoWhere w, KoV kov) {
        w.gt(kov.getKey(), kov.getValues().get(0));
    }

    private static void gte(MgoWhere w, KoV kov) {
        w.gte(kov.getKey(), kov.getValues().get(0));
    }

    private static void lt(MgoWhere w, KoV kov) {
        w.lt(kov.getKey(), kov.getValues().get(0));
    }

    private static void lte(MgoWhere w, KoV kov) {
        w.lte(kov.getKey(), kov.getValues().get(0));
    }

    private static void startsWith(MgoWhere w, KoV kov) {
        w.startsWith(kov.getKey(), kov.getValues().get(0).toString());
    }

    private static interface Builder {

        public void run(MgoWhere w, KoV kov);
    }
}
