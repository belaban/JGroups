package org.jgroups.protocols;

import org.jgroups.Address;
import org.jgroups.Event;
import org.jgroups.PhysicalAddress;
import org.jgroups.View;
import org.jgroups.annotations.Property;
import org.jgroups.util.Responses;
import org.jgroups.util.UUID;

import javax.naming.InitialContext;
import javax.naming.NamingException;
import javax.sql.DataSource;
import java.sql.*;
import java.util.List;

/**
 * <p>Discovery protocol using a JDBC connection to a shared database.
 * Connection options can be defined as configuration properties, or the JNDI
 * name of a <code>DataSource</code> can be provided (avoid providing both).</p>
 * 
 * <p>Both the schema and the used SQL statements can be customized; make sure
 * the order of parameters of such customized SQL statements is maintained and
 * that compatible types are used for the columns. The recommended schema uses a
 * single table, with two String columns being used primary key (local address,
 * cluster name) and a third column to store the serialized form of the objects
 * needed by JGroups.</p>
 * 
 * <p>A default table will be created at first connection, errors during this
 * operation are not considered critical. Set the <code>initialize_sql</code>
 * to an empty value to prevent this initial table creation, or change it to
 * create a customized table.</p>
 * 
 * @author Sanne Grinovero
 * @since 2.12
 */
public class JDBC_PING extends Discovery {

    /* -----------------------------------------    Properties     -------------------------------------------------- */

    @Property(description = "The JDBC connection URL", writable = false)
    protected String connection_url = null;

    @Property(description = "The JDBC connection username", writable = false)
    protected String connection_username = null;

    @Property(description = "The JDBC connection password", writable = false,exposeAsManagedAttribute=false)
    protected String connection_password = null;

    @Property(description = "The JDBC connection driver name", writable = false)
    protected String connection_driver = null;

    @Property(description = "If not empty, this SQL statement will be performed at startup."
                + "Customize it to create the needed table on those databases which permit table creation attempt without loosing data, such as "
                + "PostgreSQL and MySQL (using IF NOT EXISTS). To allow for creation attempts, errors performing this statement will be logged"
                + "but not considered fatal. To avoid any DDL operation, set this to an empty string.")
    protected String initialize_sql = 
        "CREATE TABLE JGROUPSPING (" +
        "own_addr varchar(200) NOT NULL, " +
        "cluster_name varchar(200) NOT NULL, " +
        "ping_data varbinary(5000) DEFAULT NULL, " +
        "PRIMARY KEY (own_addr, cluster_name) )";

    @Property(description = "SQL used to insert a new row. Customizable, but keep the order of parameters and pick compatible types: " + 
        "1)Own Address, as String 2)Cluster name, as String 3)Serialized PingData as byte[]")
    protected String insert_single_sql = "INSERT INTO JGROUPSPING (own_addr, cluster_name, ping_data) values (?, ?, ?)";
    
    @Property(description = "SQL used to delete a row. Customizable, but keep the order of parameters and pick compatible types: " + 
        "1)Own Address, as String 2)Cluster name, as String")
    protected String delete_single_sql = "DELETE FROM JGROUPSPING WHERE own_addr=? AND cluster_name=?";
    
    @Property(description = "SQL used to fetch all node's PingData. Customizable, but keep the order of parameters and pick compatible types: " + 
                "only one parameter needed, String compatible, representing the Cluster name. Must return a byte[], the Serialized PingData as" + 
                " it was stored by the insert_single_sql statement")
    protected String select_all_pingdata_sql = "SELECT ping_data FROM JGROUPSPING WHERE cluster_name=?";

    @Property(description = "To use a DataSource registered in JNDI, specify the JNDI name here. " +
        "This is an alternative to all connection_* configuration options: if this property is not empty, then all connection related" +
        "properties must be empty.")
    protected String datasource_jndi_name;

    /* --------------------------------------------- Fields ------------------------------------------------------ */

    private DataSource dataSourceFromJNDI = null;

    public boolean isDynamic() {return true;}

    @Override
    public void init() throws Exception {
        super.init();
        verifyconfigurationParameters();
        if (stringIsEmpty(datasource_jndi_name))
            loadDriver();
        else
            dataSourceFromJNDI = getDataSourceFromJNDI(datasource_jndi_name.trim());
        attemptSchemaInitialization();
        Runtime.getRuntime().addShutdownHook(new Thread() {
            public void run() {
                remove(cluster_name, local_addr);
            }
        });
    }

    @Override
    public void stop() {
        try {
            deleteSelf();
        } catch (SQLException e) {
            log.error("Error while unregistering of our own Address from JDBC_PING database during shutdown", e);
        }
        super.stop();
    }

    public Object down(Event evt) {
        switch(evt.getType()) {
            case Event.VIEW_CHANGE:
                View old_view=view;
                boolean previous_coord=is_coord;
                Object retval=super.down(evt);
                View new_view=(View)evt.getArg();
                handleView(new_view, old_view, previous_coord != is_coord);
                return retval;
        }
        return super.down(evt);
    }

    public void findMembers(final List<Address> members, final boolean initial_discovery, Responses responses) {
        readAll(members, cluster_name, responses);
        writeOwnInformation();
    }


    // remove all files which are not from the current members
    protected void handleView(View new_view, View old_view, boolean coord_changed) {
        if(is_coord && old_view != null && new_view != null) {
            Address[][] diff=View.diff(old_view, new_view);
            Address[] left_mbrs=diff[1];
            for(Address left_mbr: left_mbrs)
                if(left_mbr != null && !new_view.containsMember(left_mbr))
                    remove(cluster_name, left_mbr);
        }
        if(coord_changed)
            writeOwnInformation();
    }

    /** Write my own UUID,logical name and physical address to a file */
    protected void writeOwnInformation() {
        PhysicalAddress physical_addr=(PhysicalAddress)down(new Event(Event.GET_PHYSICAL_ADDRESS,local_addr));
        PingData data=new PingData(local_addr, is_server, UUID.get(local_addr), physical_addr).coord(is_coord);
        writeToDB(data, cluster_name); // write my own data to file
    }

    //It's possible that multiple threads in the same cluster node invoke this concurrently;
    //Since delete and insert operations are not atomic
    //(and there is no SQL standard way to do this without introducing a transaction)
    //we need the synchronization or risk a duplicate insertion on same primary key.
    //This synchronization should not be a performance problem as this is just a Discovery protocol.
    //Many SQL dialects have some "insert or update" expression, but that would need
    //additional configuration and testing on each database. See JGRP-1440
    protected synchronized void writeToDB(PingData data, String clustername) {
        final String ownAddress = addressAsString(data.getAddress());
        final Connection connection = getConnection();
        if (connection != null) {
            try {
                delete(connection, clustername, ownAddress);
                insert(connection, data, clustername, ownAddress);
            } catch (SQLException e) {
                log.error("Error updating JDBC_PING table", e);
            } finally {
                closeConnection(connection);
            }
        }
        else {
            log.error("Failed to store PingData in database");
        }
    }


    protected void remove(String clustername, Address addr) {
        final String addressAsString = addressAsString(addr);
        try {
            delete(clustername, addressAsString);
        } catch (SQLException e) {
            log.error("Error", e);
        }
    }

    protected void readAll(List<Address> members, String clustername, Responses responses) {
        final Connection connection = getConnection();
        if (connection != null) {
            try {
                readAll(connection, members, clustername, responses);
            } catch (SQLException e) {
                log.error("Error reading JDBC_PING table", e);
            } finally {
                closeConnection(connection);
            }
        }
    }

    protected void readAll(Connection connection, List<Address> members, String clustername, Responses rsps) throws SQLException {
        PreparedStatement ps = connection.prepareStatement(select_all_pingdata_sql);
        try {
            ps.setString(1, clustername);
            ResultSet resultSet = ps.executeQuery();
            while (resultSet.next()) {
                byte[] bytes = resultSet.getBytes(1);
                try {
                    PingData data=deserialize(bytes);
                    if(data == null || (members != null && !members.contains(data.getAddress())))
                        continue;
                    rsps.addResponse(data, false);
                    if(local_addr != null && !local_addr.equals(data.getAddress()))
                        addDiscoveryResponseToCaches(data.getAddress(), data.getLogicalName(), data.getPhysicalAddr());
                }
                catch(Exception e) {
                    int row=resultSet.getRow();
                    log.error("%s: failed deserializing row %d: %s; removing it from the table", local_addr, row, e);
                    try {
                        resultSet.deleteRow();
                    }
                    catch(Throwable t) {
                        log.error("%s: failed removing row %d: %s; please delete it manually", local_addr, row, e);
                    }
                }
            }
        } finally {
            ps.close();
        }
    }


    protected void attemptSchemaInitialization() {
        if(stringIsEmpty(initialize_sql)) {
            log.debug("Table creation step skipped: initialize_sql property is missing");
            return;
        }
        Connection connection=getConnection();
        if(connection == null)
            return;

        try {
            connection.prepareStatement(initialize_sql).execute();
            log.debug("Table created for JDBC_PING Discovery Protocol");
        }
        catch(SQLException e) {
            log.debug("Could not execute initialize_sql statement; not necessarily an error, we always attempt to create the schema. " +
                        "To suppress this message, set initialize_sql to an empty value. Cause: %s", e.getMessage());
        }
        finally {
            try {
                connection.close();
            }
            catch(SQLException e) {
                log.error("Error closing connection", e);
            }
        }
    }

    protected void loadDriver() {
        if (stringIsEmpty(connection_driver))
            return;
        log.debug("Registering JDBC Driver named '%s'", connection_driver);
        try {
            Class.forName(connection_driver);
        } catch (ClassNotFoundException e) {
            throw new IllegalArgumentException("JDBC Driver required for JDBC_PING "
                        + "protocol could not be loaded: '" + connection_driver + "'");
        }
    }

    protected Connection getConnection() {
        if (dataSourceFromJNDI == null) {
            Connection connection;
            try {
                connection = DriverManager.getConnection(connection_url, connection_username, connection_password);
            } catch (SQLException e) {
                log.error("Could not open connection to database", e);
                return null;
            }
            if (connection == null) {
                log.error("Received null connection from the DriverManager!");
            }
            return connection;
        }
        else {
            try {
                return dataSourceFromJNDI.getConnection();
            } catch (SQLException e) {
                log.error("Could not open connection to database", e);
                return null;
            }
        }
    }



    protected synchronized void insert(Connection connection, PingData data, String clustername, String address) throws SQLException {
        final byte[] serializedPingData = serializeWithoutView(data);
        PreparedStatement ps = connection.prepareStatement(insert_single_sql);
        try {
            ps.setString(1, address);
            ps.setString(2, clustername);
            ps.setBytes(3, serializedPingData);
            ps.executeUpdate();
            log.debug("Registered %s for clustername %s into database", address, clustername);
        } finally {
            ps.close();
        }
    }

    protected synchronized void delete(Connection connection, String clustername, String addressToDelete) throws SQLException {
        PreparedStatement ps = connection.prepareStatement(delete_single_sql);
        try {
            ps.setString(1, addressToDelete);
            ps.setString(2, clustername);
            ps.executeUpdate();
            log.debug("Removed %s for clustername %s from database", addressToDelete, clustername);
        } finally {
            ps.close();
        }
    }
    
    protected void delete(String clustername, String addressToDelete) throws SQLException {
        final Connection connection = getConnection();
        if (connection != null) {
            try {
                delete(connection, clustername, addressToDelete);
            } catch (SQLException e) {
                log.error("Error updating JDBC_PING table", e);
            } finally {
                closeConnection(connection);
            }
        } else {
            log.error("Failed to delete PingData in database");
        }
    }
    
    protected void deleteSelf() throws SQLException {
        final String ownAddress = addressAsString(local_addr);
        delete(cluster_name, ownAddress);
    }

    
    protected void closeConnection(final Connection connection) {
        try {
            connection.close();
        } catch (SQLException e) {
            log.error("Error closing connection to JDBC_PING database", e);
        }
    }
    
    protected DataSource getDataSourceFromJNDI(String name) {
        final DataSource dataSource;
        InitialContext ctx = null;
        try {
            ctx = new InitialContext();
            Object wathever = ctx.lookup(name);
            if (wathever == null) {
                throw new IllegalArgumentException(
                            "JNDI name " + name + " is not bound");
            } else if (!(wathever instanceof DataSource)) {
                throw new IllegalArgumentException(
                            "JNDI name " + name + " was found but is not a DataSource");
            } else {
                dataSource = (DataSource) wathever;
                log.debug("Datasource found via JNDI lookup via name: '%s'", name);
                return dataSource;
            }
        } catch (NamingException e) {
            throw new IllegalArgumentException(
                        "Could not lookup datasource " + name, e);
        } finally {
            if (ctx != null) {
                try {
                    ctx.close();
                } catch (NamingException e) {
                    log.warn("Failed to close naming context.", e);
                }
            }
        }
    }
    
    protected void verifyconfigurationParameters() {
        if (stringIsEmpty(this.connection_url) ||
          stringIsEmpty(this.connection_driver) ||
          stringIsEmpty(this.connection_username) ) {
            if (stringIsEmpty(this.datasource_jndi_name)) {
                throw new IllegalArgumentException("Either the 4 configuration properties starting with 'connection_' or the datasource_jndi_name must be set");
            }
        }
        if (stringNotEmpty(this.connection_url) ||
          stringNotEmpty(this.connection_driver) ||
          stringNotEmpty(this.connection_username) ) {
            if (stringNotEmpty(this.datasource_jndi_name)) {
                throw new IllegalArgumentException("When using the 'datasource_jndi_name' configuration property, all properties starting with 'connection_' must not be set");
            }
        }
        if (stringIsEmpty(this.insert_single_sql)) {
            throw new IllegalArgumentException("The insert_single_sql configuration property is mandatory");
        }
        if (stringIsEmpty(this.delete_single_sql)) {
            throw new IllegalArgumentException("The delete_single_sql configuration property is mandatory");
        }
        if (stringIsEmpty(this.select_all_pingdata_sql)) {
            throw new IllegalArgumentException("The select_all_pingdata_sql configuration property is mandatory");
        }
    }
    
    private static final boolean stringIsEmpty(final String value) {
        return value == null || value.trim().isEmpty();
    }
    
    private static final boolean stringNotEmpty(final String value) {
        return !stringIsEmpty(value);
    }

}
