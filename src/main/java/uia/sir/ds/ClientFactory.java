package uia.sir.ds;

import java.util.Map;
import java.util.TreeMap;

import uia.sir.ds.hana.db.HanaClientFactory;
import uia.sir.ds.mgo.db.MgoClientFactory;

public abstract class ClientFactory {

    public static final String HANA = "HANA";

    public static final String MGODB = "MGODB";

    private static final Map<String, ClientFactory> dbTypeClients;

    private static final Map<String, String> servDbTypes;

    static {
        dbTypeClients = new TreeMap<>();
        dbTypeClients.put(HANA, new HanaClientFactory());
        dbTypeClients.put(MGODB, new MgoClientFactory());

        servDbTypes = new TreeMap<>();

    }

    public static <T extends ClientFactory> void register(
            String dbType,
            String servName,
            String host,
            int port,
            String user,
            String pwd,
            boolean saveUTC) throws Exception {
        ClientFactory dbFactory = dbTypeClients.get(dbType);
        if (dbFactory == null) {
            throw new Exception(dbType + " NOT FOUND");
        }
        if (servDbTypes.containsKey(servName)) {
            throw new Exception(servName + " is exist");
        }

        dbFactory.register(servName, host, port, user, pwd, saveUTC);
        servDbTypes.put(servName, dbType);
    }

    public static <T extends ClientFactory> void register(
            String dbType,
            String servName,
            String[] hosts,
            int[] ports,
            String user,
            String pwd,
            boolean saveUTC) throws Exception {
        ClientFactory dbFactory = dbTypeClients.get(dbType);
        if (dbFactory == null) {
            throw new NullPointerException(dbType + " NOT FOUND");
        }
        if (servDbTypes.containsKey(servName)) {
            throw new Exception(servName + " is exist");
        }

        dbFactory.register(servName, hosts, ports, user, pwd, saveUTC);
        servDbTypes.put(servName, dbType);
    }

    public static Client client(String servName) throws Exception {
        String dbType = servDbTypes.get(servName);
        return dbTypeClients.get(dbType).create(servName);
    }

    public abstract void register(String servName, String hosts, int port, String user, String pwd, boolean saveUTC) throws Exception;

    public abstract void register(String servName, String[] hosts, int[] ports, String user, String pwd, boolean saveUTC) throws Exception;

    public abstract Client create(String servName) throws Exception;
}
