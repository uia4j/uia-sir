package uia.sir.ds;

import java.util.Map;
import java.util.TreeMap;

import uia.sir.ds.hana.db.HanaClientFactory;
import uia.sir.ds.mgo.db.MgoClientFactory;

public abstract class ClientFactory {

    public static final String HANA = "HANA";

    public static final String MGODB = "MGODB";

    private static final Map<String, ClientFactory> factories;

    private static final Map<String, ClientFactory> names;

    static {
        factories = new TreeMap<>();
        factories.put(HANA, new HanaClientFactory());
        factories.put(MGODB, new MgoClientFactory());

        names = new TreeMap<>();

    }

    public static <T extends ClientFactory> void register(String factoryName, String name, String host, int port, String user, String pwd) throws Exception {
        ClientFactory factory = factories.get(factoryName);
        if (factory == null) {
            throw new Exception(factoryName + " NOT FOUND");
        }
        if (names.containsKey(name)) {
            throw new Exception(name + " is exist");
        }

        factory.register(name, host, port, user, pwd);
        names.put(name, factory);
    }

    public static <T extends ClientFactory> void register(String factoryName, String name, String[] hosts, int port, String user, String pwd) throws Exception {
        ClientFactory factory = factories.get(factoryName);
        if (factory == null) {
            throw new NullPointerException(factoryName + " NOT FOUND");
        }
        if (names.containsKey(name)) {
            throw new Exception(name + " is exist");
        }

        factory.register(name, hosts, port, user, pwd);
        names.put(name, factory);
    }

    public static Client client(String name) throws Exception {
        return names.get(name).create(name);
    }

    public abstract void register(String name, String hosts, int port, String user, String pwd) throws Exception;

    public abstract void register(String name, String[] hosts, int port, String user, String pwd) throws Exception;

    public abstract Client create(String name) throws Exception;

}
