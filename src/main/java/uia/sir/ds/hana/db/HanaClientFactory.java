package uia.sir.ds.hana.db;

import java.sql.DriverManager;
import java.util.Map;
import java.util.TreeMap;

import uia.sir.ds.Client;
import uia.sir.ds.ClientFactory;

public class HanaClientFactory extends ClientFactory {

    private Map<String, ClientInfo> clients;

    public HanaClientFactory() {
        this.clients = new TreeMap<>();
    }

    @Override
    public void register(String name, String ip, int port, String user, String pwd) throws Exception {
        this.clients.put(name, new ClientInfo(
                String.format("jdbc:sap://%s:%s?connectTimeout=5000&communicationTimeout=30000", ip, port),
                user,
                pwd));
    }

    @Override
    public void register(String name, String[] hosts, int port, String user, String pwd) throws Exception {
    }

    @Override
    public Client create(String name) throws Exception {
        ClientInfo info = this.clients.get(name);
        return new HanaClient(DriverManager.getConnection(info.conn, info.user, info.pwd));
    }

    class ClientInfo {

        final String conn;

        final String user;

        final String pwd;

        ClientInfo(String conn, String user, String pwd) {
            this.conn = conn;
            this.user = user;
            this.pwd = pwd;
        }
    }
}
