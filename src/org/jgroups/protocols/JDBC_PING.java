package org.jgroups.protocols;

import org.jgroups.Address;
import org.jgroups.annotations.Property;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import javax.naming.InitialContext;
import javax.naming.NamingException;
import javax.sql.DataSource;

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
public class JDBC_PING extends FILE_PING {

    /* -----------------------------------------    Properties     -------------------------------------------------- */

    @Property(description = "The JDBC connection URL", writable = false)
    protected String connection_url = null;

    @Property(description = "The JDBC connection username", writable = false)
    protected String connection_username = null;

    @Property(description = "The JDBC connection password", writable = false)
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

    @Override
    public void init() throws Exception {
        super.init();
        verifyconfigurationParameters();
        if (stringIsEmpty(datasource_jndi_name)) {
            loadDriver();
        }
        else {
            dataSourceFromJNDI = getDataSourceFromJNDI(datasource_jndi_name.trim());
        }
        attemptSchemaInitialization();
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

    protected void attemptSchemaInitialization() {
        if (stringIsEmpty(initialize_sql)) {
            log.info("Table creation step skipped: initialize_sql property is missing");
            return;
        }
        Connection connection = getConnection();
        if (connection != null) {
            try {
                try {
                    PreparedStatement preparedStatement =
                        connection.prepareStatement(initialize_sql);
                    preparedStatement.execute();
                    log.info("Table created for JDBC_PING Discovery Protocol");
                } catch (SQLException e) {
                    if (log.isDebugEnabled()) {
                        log.debug("Could not execute initialize_sql statement; not necessarily an error.", e);
                    }
                    else {
                        //avoid printing out the stacktrace
                        log.info("Could not execute initialize_sql statement; not necessarily an error. Set to debug logging level for details.");
                    }
                }
            } finally {
                try {
                    connection.close();
                } catch (SQLException e) {
                    log.error("Error closing connection", e);
                }
            }
        }
    }

    protected void loadDriver() {
        if (stringIsEmpty(connection_driver)) {
            return;
        }
        if (log.isDebugEnabled()) {
            log.debug("Registering JDBC Driver named '" + connection_driver + "'");
        }
        try {
            Class.forName(connection_driver);
        } catch (ClassNotFoundException e) {
            throw new IllegalArgumentException("JDBC Driver required for JDBC_PING Discovery"
                        + "protocol could not be loaded: '" + connection_driver + "'");
        }
    }

    protected Connection getConnection() {
        if (dataSourceFromJNDI == null) {
            Connection connection;
            try {
                connection = DriverManager.getConnection(
                            connection_url, connection_username, connection_password);
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

    @Override
    protected void createRootDir() {
        // No-Op to prevent file creations from super.init().
        // TODO refactor this class and FILE_PING to have a common parent?
        // would also be nice to remove unwanted configuration properties which where inherited.
    }

    @Override
    protected void remove(String clustername, Address addr) {
        final String addressAsString = addressAsString(addr);
        try {
            delete(clustername, addressAsString);
        } catch (SQLException e) {
            log.error("Error", e);
        }
    }

    @Override
    protected List<PingData> readAll(String clustername) {
        final Connection connection = getConnection();
        if (connection != null) {
            try {
                return readAll(connection, clustername);
            } catch (SQLException e) {
                log.error("Error reading JDBC_PING table", e);
                return Collections.emptyList();
            } finally {
                closeConnection(connection);
            }
        } else {
            return Collections.emptyList();
        }
    }

    protected List<PingData> readAll(Connection connection, String clustername) throws SQLException {
        PreparedStatement ps = connection.prepareStatement(select_all_pingdata_sql);
        try {
            ps.setString(1, clustername);
            ResultSet resultSet = ps.executeQuery();
            ArrayList<PingData> results = new ArrayList<PingData>();
            while (resultSet.next()) {
                byte[] bytes = resultSet.getBytes(1);
                PingData pingData = deserialize(bytes);
                results.add(pingData);
            }
            return results;
        } finally {
            ps.close();
        }
    }

    @Override
    protected void writeToFile(PingData data, String clustername) {
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

    protected void insert(Connection connection, PingData data, String clustername, String address) throws SQLException {
        final byte[] serializedPingData = serializeWithoutView(data);
        PreparedStatement ps = connection.prepareStatement(insert_single_sql);
        try {
            ps.setString(1, address);
            ps.setString(2, clustername);
            ps.setBytes(3, serializedPingData);
            ps.executeUpdate();
            if (log.isDebugEnabled())
                log.debug("Registered " + address + " for clustername " + clustername + " into database.");
        } finally {
            ps.close();
        }
    }

    protected void delete(Connection connection, String clustername, String addressToDelete) throws SQLException {
        PreparedStatement ps = connection.prepareStatement(delete_single_sql);
        try {
            ps.setString(1, addressToDelete);
            ps.setString(2, clustername);
            ps.executeUpdate();
            if (log.isDebugEnabled())
                log.debug("Removed " + addressToDelete + " for clustername " + clustername + " from database.");
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
        delete(group_addr, ownAddress);
    }

    /**
     * Creates a byte[] representation of the PingData, but DISCARDING
     * the view it contains.
     * @param data the PingData instance to serialize.
     * @return
     */
    protected byte[] serializeWithoutView(PingData data) {
        final PingData clone = new PingData(data.getAddress(), null, data.isServer(), data.getLogicalName(),  data.getPhysicalAddrs());
        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream( 512 );
        DataOutputStream outputStream = new DataOutputStream(byteArrayOutputStream);
        try {
            clone.writeTo(outputStream);
        } catch (IOException e) {
            //not expecting this to happen as it's an in-memory stream
            log.error("Error", e);
        }
        return byteArrayOutputStream.toByteArray();
    }
    
    protected PingData deserialize(final byte[] data) {
        final PingData pingData = new PingData();
        final ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(data);
        final DataInputStream outputStream = new DataInputStream(byteArrayInputStream);
        try {
            pingData.readFrom(outputStream);
        } catch (IllegalAccessException e) {
            log.error("Error", e);
        } catch (InstantiationException e) {
            log.error("Error", e);
        } catch (IOException e) {
            // not expecting this to happen as it's an in-memory stream
            log.error("Error", e);
        }
        return pingData;
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
                if (log.isDebugEnabled()) {
                    log.debug(
                            "Datasource found via JNDI lookup via name: '"+ name + "'.");
                }
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
                    stringIsEmpty(this.connection_url) ||
                    stringIsEmpty(this.connection_username) ) {
            if (stringIsEmpty(this.datasource_jndi_name)) {
                throw new IllegalArgumentException("Either the 4 configuration properties starting with 'connection_' or the datasource_jndi_name must be set");
            }
        }
        if (stringNotEmpty(this.connection_url) ||
                    stringNotEmpty(this.connection_driver) ||
                    stringNotEmpty(this.connection_url) ||
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
        return value == null || value.trim().length() == 0;
    }
    
    private static final boolean stringNotEmpty(final String value) {
        return value != null && value.trim().length() >= 0;
    }

}
