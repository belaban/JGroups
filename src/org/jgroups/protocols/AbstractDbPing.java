package org.jgroups.protocols;

import org.jgroups.Address;
import org.jgroups.Event;
import org.jgroups.PhysicalAddress;
import org.jgroups.View;
import org.jgroups.annotations.ManagedOperation;
import org.jgroups.util.NameCache;
import org.jgroups.util.Responses;

import java.sql.SQLException;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;

/**
 * The DbComponent interface represents a component responsible for managing the persistence of
 * cluster-related information such as cluster members and discovery data. Implementations of
 * this interface handle reading, writing, and deletion of data related to cluster configurations.
 *
 * @author rsobies
 */
interface DbComponent extends AutoCloseable {

    void delete(String clusterName, Address addressToDelete);

    List<PingData> readPingData(String clusterName) throws Exception;

    void writePingData(String clustername, PingData data) throws Exception;

    void initDb() throws Exception;
}

class DBException extends Exception {
    public DBException(String message) {
        super(message);
    }
}

/**
 * AbstractDbPing is an abstract extension of the {@link FILE_PING} discovery protocol.
 * It introduces database-centric logic for handling discovery data, utilizing custom
 * schema management and database operations to coordinate and manage cluster member information.
 * <p>
 * This class provides the capability to read, write, and remove data related to cluster members
 * from a persistent database storage using database components. Additionally, it overrides
 * certain behaviors to enhance the handling of cluster membership information during
 * coordinator transitions and view changes.
 * <p>
 * Subclasses are expected to define specific database operations such as schema creation
 * and database component initialization.
 * Responsibilities for subclass implementation:
 * - Provide the database-specific interactions by implementing the abstract {@code getDbComponent()} method.
 * - Implement the database schema creation logic in {@code createSchema()}.
 * <p>
 * This class handles scenarios such as:
 * - Discovery and removal of outdated or unnecessary entries in the database upon view change.
 * - Writing member information during initialization and view transitions.
 *
 * @author Bela Ban, rsobies
 * @since 5.4, 5.3.7
 */
public abstract class AbstractDbPing extends FILE_PING {
    protected final Lock lock = new ReentrantLock();

    protected abstract DbComponent getDbComponent() throws SQLException;

    @Override
    protected void createRootDir() {
        // do *not* create root file system (don't remove !)
    }

    protected boolean schemaWasCreated = false;

    @Override
    public void init() throws Exception {
        super.init();
        if (schemaWasCreated) return;
        try (DbComponent dbComponent = getDbComponent()) {
            dbComponent.initDb();
        }
        schemaWasCreated = true;
    }

    @ManagedOperation(description = "Lists all rows in the database")
    public String dump(String cluster) throws Exception {
        List<PingData> list = readFromDB(cluster);
        return list.stream().map(pd -> String.format("%s", pd)).collect(Collectors.joining("\n"));
    }

    @Override
    protected void handleView(View new_view, View old_view, boolean coord_changed) {
        // If we are the coordinator, it is good to learn about new entries that have been added before we delete them.
        // If we are not the coordinator, it is good to learn the new entries added by the coordinator.
        // This avoids a "JGRP000032: %s: no physical address for %s, dropping message" that leads to split clusters at concurrent startup.
        learnExistingAddresses();

        // This is an updated logic where we do not call removeAll but instead remove those obsolete entries.
        // This avoids the short moment where the table is empty and a new node might not see any other node.
        if (is_coord) {
            if (remove_old_coords_on_view_change) {
                Address old_coord = old_view != null ? old_view.getCreator() : null;
                if (old_coord != null)
                    remove(cluster_name, old_coord);
            }
            Address[] left = View.diff(old_view, new_view)[1];
            if (coord_changed || update_store_on_view_change || left.length > 0) {
                writeAll(left);
                if (remove_all_data_on_view_change) {
                    removeAllNotInCurrentView();
                }
                if (remove_all_data_on_view_change || remove_old_coords_on_view_change) {
                    startInfoWriter();
                }
            }
        } else if (coord_changed && !remove_all_data_on_view_change) {
            // I'm no longer the coordinator, usually due to a merge.
            // The new coordinator will update my status to non-coordinator, and remove me fully
            // if 'remove_all_data_on_view_change' is enabled and I'm no longer part of the view.
            // Maybe this branch even be removed completely, but for JDBC_PING 'remove_all_data_on_view_change' is always set to true.
            writeLocalAddress();
        }
    }

    protected void removeAllNotInCurrentView() {
        View local_view = view;
        if (local_view == null) {
            return;
        }
        lock.lock();
        try (var dbComponent = getDbComponent()) {
            List<PingData> list = readFromDB(dbComponent, cluster_name);
            for (PingData data : list) {
                Address addr = data.getAddress();
                if (!local_view.containsMember(addr)) {
                    addDiscoveryResponseToCaches(addr, data.getLogicalName(), data.getPhysicalAddr());
                    dbComponent.delete(cluster_name, addr);
                }
            }
        } catch (Exception e) {
            log.error(String.format("%s: failed reading from the DB", local_addr), e);
        }
        finally {
            lock.unlock();
        }
    }

    protected List<PingData> readFromDB(DbComponent dbComponent, String cluster) throws Exception {
        reads++;
        return dbComponent.readPingData(cluster);
    }

    protected void learnExistingAddresses() {
        try {
            List<PingData> list = readFromDB(getClusterName());
            for (PingData data : list) {
                Address addr = data.getAddress();
                if (local_addr != null && !local_addr.equals(addr)) {
                    addDiscoveryResponseToCaches(addr, data.getLogicalName(), data.getPhysicalAddr());
                }
            }
        } catch (Exception e) {
            log.error(String.format("%s: failed reading from the DB", local_addr), e);
        }
    }

    @Override
    public boolean isInfoWriterRunning() {
        // Do not rely on the InfoWriter, instead always write the missing information on find if it is missing. Find is also triggered by MERGE.
        return false;
    }

    @Override
    public void findMembers(List<Address> members, boolean initial_discovery, Responses responses) {
        if (initial_discovery) {
            findMembersInitialDiscovery();
        }
        super.findMembers(members, initial_discovery, responses);
    }

    protected void findMembersInitialDiscovery() {
        try {
            // Sending the discovery here, as the parent class will not execute it once there is data in the table
            PhysicalAddress physical_addr = (PhysicalAddress) down(new Event(Event.GET_PHYSICAL_ADDRESS, local_addr));
            sendDiscoveryResponse(local_addr, physical_addr, NameCache.get(local_addr), null, is_coord);

            try (DbComponent dbComponent = getDbComponent()) {
                List<PingData> pingData = readFromDB(dbComponent, cluster_name);
                writeLocalAddress();
                while (pingData.stream().noneMatch(PingData::isCoord)) {
                    // Do a quick check if more nodes have arrived, to have a more complete list of nodes to start with.
                    List<PingData> newPingData = readFromDB(dbComponent, cluster_name);
                    if (newPingData.stream().map(PingData::getAddress).collect(Collectors.toSet()).equals(pingData.stream().map(PingData::getAddress).collect(Collectors.toSet()))
                            || pingData.stream().anyMatch(PingData::isCoord)) {
                        break;
                    }
                    pingData = newPingData;
                }
            }
        } catch (Exception e) {
            log.error(String.format("%s: failed reading from the DB", local_addr), e);
        }
    }

    protected void writeLocalAddress() {
        PhysicalAddress physical_addr = (PhysicalAddress) down(new Event(Event.GET_PHYSICAL_ADDRESS, local_addr));
        PingData coord_data = new PingData(local_addr, true, NameCache.get(local_addr), physical_addr).coord(is_coord);
        write(Collections.singletonList(coord_data), cluster_name);
    }

    @Override
    protected void write(List<PingData> list, String clustername) {
        lock.lock();
        try (DbComponent dbComponent = getDbComponent()) {
            for (PingData data : list) {
                try {
                    dbComponent.writePingData(clustername, data);
                } catch (Exception e) {
                    log.error("%s: failed writing to DB: %s", local_addr, e);
                }
            }
            writes++;
        } catch (Exception e) {
            log.error("%s: failed writing to DB: %s", local_addr, e);
        } finally {
            lock.unlock();
        }
    }


    // It's possible that multiple threads in the same cluster node invoke this concurrently;
    // Since delete and insert operations are not atomic
    // (and there is no SQL standard way to do this without introducing a transaction)
    // we need the synchronization or risk a duplicate insertion on same primary key.
    // This synchronization should not be a performance problem as this is just a Discovery protocol.
    // Many SQL dialects have some "insert or update" expression, but that would need
    // additional configuration and testing on each database. See JGRP-1440
    protected void writeToDB(PingData data, String clustername) throws Exception {
        lock.lock();
        try (DbComponent dbComponent = getDbComponent()) {
            dbComponent.writePingData(clustername, data);
        } finally {
            lock.unlock();
        }
    }

    protected void delete(String clustername, Address addressToDelete) throws Exception {
        try (DbComponent dbComponent = getDbComponent()) {
            dbComponent.delete(clustername, addressToDelete);
        }
    }

    @Override
    protected void remove(String clustername, Address addr) {
        lock.lock();
        try {
            delete(clustername, addr);
        } catch (Exception e) {
            log.error(String.format("%s: failed deleting %s from the table", local_addr, addr), e);
        } finally {
            lock.unlock();
        }
    }

    @Override
    protected void removeAll(String clustername) {
        // This is unsafe as even if we would fill the table a moment later, a new node might see an empty table and become a coordinator
        throw new RuntimeException("Not implemented as it is unsafe");
    }

    @Override
    protected void readAll(List<Address> members, String cluster, Responses rsps) {
        try {
            List<PingData> list = readFromDB(cluster);
            for (PingData data : list) {
                Address addr = data.getAddress();
                if (data == null || (members != null && !members.contains(addr)))
                    continue;
                rsps.addResponse(data, false);
                if (local_addr != null && !local_addr.equals(addr))
                    addDiscoveryResponseToCaches(addr, data.getLogicalName(), data.getPhysicalAddr());
            }
        } catch (Exception e) {
            log.error(String.format("%s: failed reading from the DB", local_addr), e);
        }
    }

    protected List<PingData> readFromDB(String cluster) throws Exception {
        try (DbComponent dbComponent = getDbComponent()) {
            return readFromDB(dbComponent, cluster);
        }
    }
}
