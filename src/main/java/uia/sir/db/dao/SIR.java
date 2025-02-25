package uia.sir.db.dao;

import static org.bson.codecs.configuration.CodecRegistries.fromProviders;
import static org.bson.codecs.configuration.CodecRegistries.fromRegistries;

import javax.naming.Context;
import javax.naming.InitialContext;

import org.bson.codecs.configuration.CodecRegistry;
import org.bson.codecs.pojo.PojoCodecProvider;

import com.mongodb.ConnectionString;
import com.mongodb.MongoClientSettings;
import com.mongodb.ReadPreference;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.connection.ClusterConnectionMode;
import com.mongodb.connection.ClusterType;

public class SIR {

    private static MongoClientSettings client_settings;

    private static CodecRegistry codec_registry;

    private static MongoClient client;

    public static void initial(String ip, int port, String user, String pwd) throws Exception {
        CodecRegistry pojoCodecRegistry = fromProviders(PojoCodecProvider.builder().automatic(true).build());

        codec_registry = fromRegistries(MongoClientSettings.getDefaultCodecRegistry(), pojoCodecRegistry);
        if (port > 0) {
            String conn = (user == null || user.trim().isEmpty())
                    ? String.format("mongodb://%s:%s", ip, port)
                    : String.format("mongodb://%s:%s@%s:%s", user, pwd, ip, port);
            client_settings = MongoClientSettings
                    .builder()
                    .applyConnectionString(new ConnectionString(conn))
                    .applyToClusterSettings(builder -> builder.mode(ClusterConnectionMode.SINGLE).requiredClusterType(ClusterType.STANDALONE))
                    .codecRegistry(codec_registry)
                    .readPreference(ReadPreference.secondaryPreferred())
                    .build();
            client = MongoClients.create(client_settings);
        }
        else {
            Context ctx = new InitialContext();
            client = (MongoClient) ctx.lookup(ip);
        }
    }

    public static void initial(String[] hosts, int[] ports, String user, String pwd) throws Exception {
        String conn = "mongodb://" + user + ":" + pwd + "@";
        for (int i = 0; i < hosts.length; i++) {
            conn += (hosts[i] + ":" + ports[i] + ",");
        }

        CodecRegistry pojoCodecRegistry = fromProviders(PojoCodecProvider.builder().automatic(true).build());
        codec_registry = fromRegistries(MongoClientSettings.getDefaultCodecRegistry(), pojoCodecRegistry);
        client_settings = MongoClientSettings.builder()
                .applyConnectionString(new ConnectionString(conn.substring(0, conn.length() - 1)))
                .codecRegistry(codec_registry)
                .readPreference(ReadPreference.secondaryPreferred())
                .build();
        client = MongoClients.create(client_settings);
    }

    public synchronized static SIRClient create() throws Exception {
        return new SIRClient(client.getDatabase("sir").withCodecRegistry(codec_registry));
    }

    public static void close() {
        try {
            client.close();
        }
        catch (Exception ex) {

        }
        finally {
            client = null;
        }
    }
}
