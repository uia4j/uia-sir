package uia.sir.simple;

import java.util.function.Function;

public interface ValueReader extends Function<Object, Object> {

    public final static ValueReader simple = v -> v;

}
