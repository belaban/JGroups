package org.jgroups.protocols;

import com.mongodb.ConnectionString;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Filters;
import org.bson.Document;
import org.jgroups.Address;
import org.jgroups.PhysicalAddress;
import org.jgroups.View;
import org.jgroups.annotations.Property;
import org.jgroups.protocols.relay.SiteUUID;
import org.jgroups.stack.IpAddress;
import org.jgroups.util.NameCache;
import org.jgroups.util.Util;

import java.sql.*;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Executor;

import static com.mongodb.client.model.Filters.and;
import static com.mongodb.client.model.Filters.eq;

public class MONGO_PING extends JDBC_PING2 {

    protected static final String CLUSTERNAME = "clustername";
    protected static final String NAME = "name";
    protected static final String IP = "ip";
    protected static final String ISCOORD = "isCoord";

    @Property(description = "todo")
    protected String collection_name = "jgroups-apps";

    @Override
    public MONGO_PING setConnectionUrl(String c) {
        this.connection_url = c;
        return this;
    }

    public String getCollection_name() {
        return collection_name;
    }

    public MONGO_PING setCollection_name(String collection_name) {
        this.collection_name = collection_name;
        return this;
    }

    protected MongoClient getMongoConnection() {
        var connString = new ConnectionString(connection_url);
        return MongoClients.create(connString);
    }

    protected MongoCollection<Document> getCollection(MongoClient client) {
        var connString = new ConnectionString(connection_url);
        var db = client.getDatabase(connString.getDatabase());
        return db.getCollection(collection_name);
    }

    protected ConnectionString connectionString;

    @Override
    protected void removeAllNotInCurrentView() {
        View local_view = view;
        if (local_view == null) {
            return;
        }
        String cluster_name = getClusterName();
        try(var mongoClient = getMongoConnection()){
            var collection = getCollection(mongoClient);
            try {
                List<PingData> list = readFromDB(getClusterName());
                for (PingData data : list) {
                    Address addr = data.getAddress();
                    if (!local_view.containsMember(addr)) {
                        addDiscoveryResponseToCaches(addr, data.getLogicalName(), data.getPhysicalAddr());
                        delete(collection, cluster_name, addr);
                    }
                }
            } catch (Exception e) {
                log.error(String.format("%s: failed reading from the DB", local_addr), e);
            }
        }
    }

    @Override
    protected void loadDriver() {
        //do nothing
    }

    @Override
    protected void clearTable(String clustername) {
        try (var mongoClient = getMongoConnection()) {
            var collection = getCollection(mongoClient);
            collection.deleteMany(eq(CLUSTERNAME, clustername));
        }
    }

    @Override
    protected void writeToDB(PingData data, String clustername) {
        lock.lock();
        try {
            delete(clustername, data.getAddress());
            insert(data, clustername);
        } finally {
            lock.unlock();
        }
    }

    protected void insert(PingData data, String clustername) {
        lock.lock();
        try (var mongoClient = getMongoConnection()) {
            var collection = getCollection(mongoClient);
            Address address = data.getAddress();
            String addr = Util.addressToString(address);
            String name = address instanceof SiteUUID ? ((SiteUUID) address).getName() : NameCache.get(address);
            PhysicalAddress ip_addr = data.getPhysicalAddr();
            String ip = ip_addr.toString();
            collection.insertOne(new Document("_id", addr)
                    .append(NAME, name)
                    .append(CLUSTERNAME, clustername)
                    .append(IP, ip)
                    .append(ISCOORD, data.isCoord())
            );
        } finally {
            lock.unlock();
        }
    }

    @Override
    protected void createSchema() {
        connectionString = new ConnectionString(connection_url);
        try (var mongoClient = getMongoConnection()) {
            var db = mongoClient.getDatabase(connectionString.getDatabase());
            db.createCollection(collection_name);
        }
    }

    @Override
    protected void createInsertStoredProcedure() {
        //do nothing
    }

    @Override
    protected List<PingData> readFromDB(String cluster) throws Exception {
        try (var mongoClient = getMongoConnection()) {
            var collection = getCollection(mongoClient);
            try (var iterator = collection.find(eq(CLUSTERNAME, cluster)).iterator()) {
                reads++;
                List<PingData> retval = new LinkedList<>();

                while (iterator.hasNext()) {
                    var doc = iterator.next();
                    String uuid = doc.get("_id", String.class);
                    Address addr = Util.addressFromString(uuid);
                    String name = doc.get(NAME, String.class);
                    String ip = doc.get(IP, String.class);
                    IpAddress ip_addr = new IpAddress(ip);
                    boolean coord = doc.get(ISCOORD, Boolean.class);
                    PingData data = new PingData(addr, true, name, ip_addr).coord(coord);
                    retval.add(data);
                }

                return retval;
            }
        }
    }

    protected void delete(MongoCollection<Document> collection, String clustername, Address addressToDelete) {
        lock.lock();
        try {
            String addr = Util.addressToString(addressToDelete);
            collection.deleteOne(and(eq("_id", addr), eq(CLUSTERNAME, clustername)));
        }
       finally {
            lock.unlock();
        }
    }

    @Override
    protected void delete(String clustername, Address addressToDelete) {
        try (var mongoClient = getMongoConnection()) {
            var collection = getCollection(mongoClient);
            delete(collection, clustername, addressToDelete);
        }
    }
}
