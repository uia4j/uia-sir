package uia.sir.ds.hana.db;

import java.sql.DriverManager;
import java.util.Map;
import java.util.TreeMap;

import uia.sir.ds.Client;
import uia.sir.ds.ClientFactory;

public class HanaClientFactory extends ClientFactory {

    private final Map<String, ClientInfo> clients;

    public HanaClientFactory() {
        this.clients = new TreeMap<>();
    }

    @Override
    public void register(String servName, String host, int port, String user, String pwd, boolean saveUTC) throws Exception {
        this.clients.put(servName, new ClientInfo(
                String.format("jdbc:sap://%s:%s?connectTimeout=5000&communicationTimeout=30000", host, port),
                user,
                pwd,
                saveUTC));
    }

    @Override
    public void register(String name, String[] hosts, int[] ports, String user, String pwd, boolean saveUTC) throws Exception {
    }

    @Override
    public Client create(String servName) throws Exception {
        ClientInfo info = this.clients.get(servName);
        return new HanaClient(DriverManager.getConnection(info.conn, info.user, info.pwd), info.saveUTC);
    }

    class ClientInfo {

        final String conn;

        final String user;

        final String pwd;

        final boolean saveUTC;

        ClientInfo(String conn, String user, String pwd, boolean saveUTC) {
            this.conn = conn;
            this.user = user;
            this.pwd = pwd;
            this.saveUTC = saveUTC;
        }
    }
}
