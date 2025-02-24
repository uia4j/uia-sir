package uia.sir.ds.mgo.db;

import static org.bson.codecs.configuration.CodecRegistries.fromProviders;
import static org.bson.codecs.configuration.CodecRegistries.fromRegistries;

import java.util.Map;
import java.util.TreeMap;

import javax.naming.Context;
import javax.naming.InitialContext;
import javax.naming.NamingException;

import org.bson.codecs.configuration.CodecRegistry;
import org.bson.codecs.pojo.PojoCodecProvider;

import com.mongodb.ConnectionString;
import com.mongodb.MongoClientSettings;
import com.mongodb.ReadPreference;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.connection.ClusterConnectionMode;
import com.mongodb.connection.ClusterType;

import uia.sir.ds.Client;
import uia.sir.ds.ClientFactory;

public class MgoClientFactory extends ClientFactory {

    private CodecRegistry codec_registry;

    private Map<String, MongoClient> clients;

    public MgoClientFactory() {
        CodecRegistry pojoCodecRegistry = fromProviders(PojoCodecProvider.builder().automatic(true).build());
        this.codec_registry = fromRegistries(MongoClientSettings.getDefaultCodecRegistry(), pojoCodecRegistry);
        this.clients = new TreeMap<>();
    }

    @Override
    public void register(String name, String ip, int port, String user, String pwd) throws NamingException {
        if (port > 0) {
            String conn = (user == null || user.trim().isEmpty())
                    ? String.format("mongodb://%s:%s", ip, port)
                    : String.format("mongodb://%s:%s@%s:%s", user, pwd, ip, port);
            MongoClientSettings clientSettings = MongoClientSettings
                    .builder()
                    .applyConnectionString(new ConnectionString(conn))
                    .applyToClusterSettings(builder -> builder.mode(ClusterConnectionMode.SINGLE).requiredClusterType(ClusterType.STANDALONE))
                    .codecRegistry(this.codec_registry)
                    .readPreference(ReadPreference.secondaryPreferred())
                    .build();
            this.clients.put(name, MongoClients.create(clientSettings));
        }
        else {
            Context ctx = new InitialContext();
            this.clients.put(name, (MongoClient) ctx.lookup(ip));
        }
    }

    public void register(String name, String[] hosts, int port, String user, String pwd) throws NamingException {
        String conn = "mongodb://" + user + ":" + pwd + "@";
        for (String host : hosts) {
            conn += (host + ":" + port + ",");
        }

        MongoClientSettings clientSettings = MongoClientSettings.builder()
                .applyConnectionString(new ConnectionString(conn.substring(0, conn.length() - 1)))
                .codecRegistry(this.codec_registry)
                .readPreference(ReadPreference.secondaryPreferred())
                .build();
        this.clients.put(name, MongoClients.create(clientSettings));
    }

    @Override
    public Client create(String name) {
        MongoClient client = this.clients.get(name);
        if (client == null) {
            return null;
        }

        return new MgoClient(client.getDatabase("trek").withCodecRegistry(this.codec_registry));
    }

}
