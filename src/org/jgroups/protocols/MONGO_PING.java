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
import org.jgroups.annotations.Property;
import org.jgroups.protocols.relay.SiteUUID;
import org.jgroups.stack.IpAddress;
import org.jgroups.util.NameCache;
import org.jgroups.util.Util;
import java.util.LinkedList;
import java.util.List;

public class MONGO_PING extends JDBC_PING2 {

    protected static final String CLUSTERNAME="clustername";
    protected static final String NAME="name";
    protected static final String IP="ip";
    protected static final String ISCOORD="isCoord";


    @Property(description="todo")
    protected String collectionName = "jgroups-apps";

    @Override
    public MONGO_PING setConnectionUrl(String c) {
        this.connection_url = c;
        return this;
    }

    public String getCollectionName() { return collectionName;}
    public MONGO_PING setCollectionName(String collectionName) {
        this.collectionName = collectionName;
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
        mongoDb.createCollection(collectionName);
        collection = mongoDb.getCollection(collectionName);
    }

    @Override
    protected void createInsertStoredProcedure() {
        //do nothing
    }

    @Override
    protected List<PingData> readFromDB(String cluster) throws Exception {
        try (var iterator = collection.find(Filters.eq(CLUSTERNAME, cluster)).iterator()) {
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

    @Override
    protected void delete(String clustername, Address addressToDelete) {
        String addr = Util.addressToString(addressToDelete);
        collection.deleteOne(Filters.eq("_id", addr));
    }
}
