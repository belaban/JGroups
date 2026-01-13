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
import org.jgroups.protocols.relay.SiteUUID;
import org.jgroups.stack.IpAddress;
import org.jgroups.util.NameCache;
import org.jgroups.util.Util;
import java.util.LinkedList;
import java.util.List;

public class MONGO_PING extends JDBC_PING2 {

    protected String collectionName = "jgroups-apps";

    public String getConnectionUrl() {
        return connection_url;
    }

    public MONGO_PING setConnectionUrl(String c) {
        this.connection_url = c;
        return this;
    }

    protected MongoDatabase mongoDb;
    protected MongoCollection<Document> collection;
    protected MongoClient mongoClient;

    @Override
    public void init() throws Exception {

        var connString = new ConnectionString(connection_url);
        mongoClient = MongoClients.create(connString);
        mongoDb = mongoClient.getDatabase(connString.getDatabase());
        super.init();
    }

    @Override
    public void stop() {
        super.stop();
        mongoClient.close();
    }

    @Override
    protected void loadDriver() {
        //do nothing
    }

    @Override
    protected void clearTable(String clustername) {
        collection.deleteMany(Filters.empty());
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
        try {
            Address address = data.getAddress();
            String addr = Util.addressToString(address);
            String name = address instanceof SiteUUID ? ((SiteUUID) address).getName() : NameCache.get(address);
            PhysicalAddress ip_addr = data.getPhysicalAddr();
            String ip = ip_addr.toString();
            collection.insertOne(new Document("_id", addr)
                    .append("name", name)
                    .append("clustername", clustername)
                    .append("ip", ip)
                    .append("isCoord", data.isCoord())
            );
        } finally {
            lock.unlock();
        }
    }

    @Override
    protected void createSchema() {
        mongoDb.createCollection(collectionName);
        collection = mongoDb.getCollection(collectionName);
    }

    @Override
    protected void createInsertStoredProcedure() {
        //do nothing
    }

    @Override
    protected List<PingData> readFromDB(String cluster) throws Exception {
        try (var iterator = collection.find().iterator()) {
            reads++;
            List<PingData> retval = new LinkedList<>();

            while (iterator.hasNext()) {
                var doc = iterator.next();
                String uuid = doc.get("_id", String.class);
                Address addr = Util.addressFromString(uuid);
                String name = doc.get("name", String.class);
                String ip = doc.get("ip", String.class);
                IpAddress ip_addr = new IpAddress(ip);
                boolean coord = doc.get("isCoord", Boolean.class);
                PingData data = new PingData(addr, true, name, ip_addr).coord(coord);
                retval.add(data);
            }

            return retval;
        }
    }

    @Override
    protected void delete(String clustername, Address addressToDelete) {
        String addr = Util.addressToString(addressToDelete);
        collection.deleteOne(Filters.eq("_id", addr));
    }
}
