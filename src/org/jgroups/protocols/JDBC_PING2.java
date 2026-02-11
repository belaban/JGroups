package org.jgroups.protocols;

import org.jgroups.Address;
import org.jgroups.PhysicalAddress;
import org.jgroups.annotations.Property;
import org.jgroups.protocols.relay.SiteUUID;
import org.jgroups.stack.IpAddress;
import org.jgroups.util.NameCache;
import org.jgroups.util.Util;

import javax.naming.InitialContext;
import javax.naming.NamingException;
import javax.sql.DataSource;
import java.sql.*;
import java.util.LinkedList;
import java.util.List;
import java.util.function.Function;

import static java.sql.ResultSet.CONCUR_UPDATABLE;
import static java.sql.ResultSet.TYPE_FORWARD_ONLY;

/**
 * New version of {@link JDBC_PING}. Has a new, better legible schema. plus some refactoring
 *
 * @author Bela Ban
 * @since 5.4, 5.3.7
 */
public class JDBC_PING2 extends AbstractDbPing {


    /* -----------------------------------------    Properties     -------------------------------------------------- */

    @Property(description = "The JDBC connection URL", writable = false)
    protected String connection_url;

    @Property(description = "The JDBC connection username", writable = false)
    protected String connection_username;

    @Property(description = "The JDBC connection password", writable = false, exposeAsManagedAttribute = false)
    protected String connection_password;

    @Property(description = "The JDBC connection driver name", writable = false)
    protected String connection_driver;

    @Property(description = "If not null, this SQL statement will be performed at startup. Customize it to create the " +
            "needed table. To allow for creation attempts, errors performing this statement will be logged "
            + "but not considered fatal. To avoid any creation, set this to null.")
    protected String initialize_sql = "CREATE TABLE jgroups (address varchar(200) NOT NULL, " +
            "name varchar(200), " +
            "cluster varchar(200) NOT NULL, " +
            "ip varchar(200) NOT NULL, " +
            "coord boolean, " +
            "PRIMARY KEY (address) )";

    @Property(description = "Definition of a stored procedure which deletes an existing row and inserts a new one. Used " +
            "only if non-null (as an optimization of calling delete, then insert (1 SQL statement instead of 2). Needs to " +
            "accept address (varchar), name (varchar), cluster (varchar), ip (varchar) and coord (boolean")
    protected String insert_sp;

    @Property(description = "Calls the insert_sp stored procedure. Not used if null.")
    protected String call_insert_sp;

    @Property(description = "SQL used to insert a new row")
    protected String insert_single_sql = "INSERT INTO jgroups values (?, ?, ?, ?, ?)";

    @Property(description = "SQL used to delete a row")
    protected String delete_single_sql = "DELETE FROM jgroups WHERE address=?";

    @Property(description = "SQL to clear the table")
    protected String clear_sql = "DELETE from jgroups WHERE cluster=?";

    @Property(description = "SQL used to fetch the data of all nodes")
    protected String select_all_pingdata_sql = "SELECT address, name, ip, coord FROM jgroups WHERE cluster=?";

    @Property(description = "To use a DataSource registered in JNDI, specify the JNDI name here")
    protected String datasource_jndi_name;

    @Property(description = "The fully qualified name of a class which implements a Function<JDBC_PING,DataSource>. " +
            "If not null, this has precedence over datasource_jndi_name.")
    protected String datasource_injecter_class;

    /* --------------------------------------------- Fields ------------------------------------------------------ */

    protected DataSource dataSource;

    @Override
    protected DbComponent getDbComponent() throws Exception{

        return new DbComponent() {
            private final Connection connection = getConnection();

            @Override
            public void close() throws Exception {
                connection.close();
            }

            @Override
            public void delete(String clusterName, Address addressToDelete) {
                try (PreparedStatement ps = connection.prepareStatement(delete_single_sql)) {
                    String addr = Util.addressToString(addressToDelete);
                    ps.setString(1, addr);
                    if (log.isTraceEnabled())
                        log.trace("%s: SQL for deletion: %s", local_addr, ps);
                    ps.executeUpdate();
                    log.debug("%s: removed %s for cluster %s from database", local_addr, addressToDelete, clusterName);
                } catch (SQLException e) {
                    throw new RuntimeException(e);
                }
            }

            private void insert(String statement, String clusterName, PingData data) throws Exception {
                try (PreparedStatement ps = connection.prepareStatement(statement)) {
                    Address address = data.getAddress();
                    String addr = Util.addressToString(address);
                    String name = address instanceof SiteUUID ? ((SiteUUID) address).getName() : NameCache.get(address);
                    PhysicalAddress ip_addr = data.getPhysicalAddr();
                    String ip = ip_addr.toString();
                    ps.setString(1, addr);
                    ps.setString(2, name);
                    ps.setString(3, clusterName);
                    ps.setString(4, ip);
                    ps.setBoolean(5, data.isCoord());
                    if (log.isTraceEnabled())
                        log.trace("%s: SQL for insertion: %s", local_addr, ps);
                    ps.executeUpdate();
                    log.debug("%s: inserted %s for cluster %s", local_addr, address, clusterName);
                }
            }

            @Override
            public List<PingData> readPingData(String clusterName) throws Exception {
                try (PreparedStatement ps = prepare(connection, select_all_pingdata_sql, TYPE_FORWARD_ONLY, CONCUR_UPDATABLE)) {
                    ps.setString(1, clusterName);
                    if (log.isTraceEnabled())
                        log.trace("%s: SQL for reading: %s", local_addr, ps);
                    try (ResultSet resultSet = ps.executeQuery()) {
                        List<PingData> retval = new LinkedList<>();
                        while (resultSet.next()) {
                            String uuid = resultSet.getString(1);
                            String name = resultSet.getString(2);
                            String ip = resultSet.getString(3);
                            boolean coord = resultSet.getBoolean(4);
                            Address addr = Util.addressFromString(uuid);
                            IpAddress ip_addr = new IpAddress(ip);
                            PingData data = new PingData(addr, true, name, ip_addr).coord(coord);
                            retval.add(data);
                        }
                        return retval;
                    }
                }
            }

            @Override
            public void writePingData(String clustername, PingData data) throws Exception {
                if (call_insert_sp != null && insert_sp != null)
                    insert(call_insert_sp, clustername, data);
                else {
                    boolean isAutocommit = connection.getAutoCommit();
                    try {
                        if (isAutocommit) {
                            // Always use a transaction for the delete+insert to make it atomic
                            // to avoid the short moment where there is no entry in the table.
                            connection.setAutoCommit(false);
                        }
                        delete(clustername, data.getAddress());
                        insert(insert_single_sql, clustername, data);
                        if (isAutocommit) // if set to false by the caller, let the caller also commit it
                            connection.commit();
                    } catch (SQLException ex) {
                        if (isAutocommit)
                            connection.rollback();
                        throw ex;
                    }
                }
            }

            private void createSchema() {
                if (initialize_sql == null) {
                    log.debug("%s: table creation step skipped: initialize_sql attribute is missing", local_addr);
                    return;
                }
                try (PreparedStatement ps = connection.prepareStatement(initialize_sql)) {
                    log.trace("%s: SQL for initializing schema: %s", local_addr, ps);
                    ps.execute();
                    log.debug("%s: table created for JDBC_PING discovery protocol", local_addr);
                } catch (SQLException e) {
                    log.debug("%s: failed executing initialize_sql statement; not necessarily an error, we always attempt to " +
                                    "create the schema. To suppress this message, set initialize_sql to null. Cause: %s",
                            local_addr, e.getMessage());
                }
            }

            private void createInsertStoredProcedure() throws SQLException {
                if (insert_sp == null)
                    return;

                try (PreparedStatement ps = connection.prepareStatement(insert_sp)) {
                    log.trace("%s: attempting to create stored procedure %s", local_addr, insert_sp);
                    ps.execute();
                    log.debug("%s: successfully created stored procedure %s", local_addr, insert_sp);
                } catch (SQLException ex) {
                    log.debug("%s: failed creating stored procedure %s: %s", local_addr, insert_sp, ex.getMessage());
                }
            }

            @Override
            public void initDb() throws Exception {
                if (dataSource == null) {
                    if (datasource_injecter_class != null) {
                        dataSource = injectDataSource(datasource_injecter_class);
                        if (dataSource == null) {
                            String m = String.format("datasource_injecter_class %s created null datasource", datasource_injecter_class);
                            throw new IllegalArgumentException(m);
                        }
                    } else {
                        if (datasource_jndi_name != null)
                            dataSource = getDataSourceFromJNDI(datasource_jndi_name.trim());
                        else
                            loadDriver();
                    }
                }
                createSchema();
                createInsertStoredProcedure();
            }
        };
    }

    public JDBC_PING2 setDataSource(DataSource ds) {
        this.dataSource = ds;
        return this;
    }

    public DataSource getDataSource() {
        return dataSource;
    }

    public String getConnectionUrl() {
        return connection_url;
    }

    public JDBC_PING2 setConnectionUrl(String c) {
        this.connection_url = c;
        return this;
    }

    public String getConnectionUsername() {
        return connection_username;
    }

    public JDBC_PING2 setConnectionUsername(String c) {
        this.connection_username = c;
        return this;
    }

    public String getConnectionPassword() {
        return connection_password;
    }

    public JDBC_PING2 setConnectionPassword(String c) {
        this.connection_password = c;
        return this;
    }

    public String getConnectionDriver() {
        return connection_driver;
    }

    public JDBC_PING2 setConnectionDriver(String c) {
        this.connection_driver = c;
        return this;
    }

    public String getInitializeSql() {
        return initialize_sql;
    }

    public JDBC_PING2 setInitializeSql(String i) {
        this.initialize_sql = i;
        return this;
    }

    public String getInsertSingleSql() {
        return insert_single_sql;
    }

    public JDBC_PING2 setInsertSingleSql(String i) {
        this.insert_single_sql = i;
        return this;
    }

    public String getInsertSp() {
        return insert_sp;
    }

    public JDBC_PING2 setInsertSp(String sp) {
        insert_sp = sp;
        return this;
    }

    public String getCallInsertSp() {
        return call_insert_sp;
    }

    public JDBC_PING2 setCallInsertSp(String sp) {
        call_insert_sp = sp;
        return this;
    }

    public String getDeleteSingleSql() {
        return delete_single_sql;
    }

    public JDBC_PING2 setDeleteSingleSql(String d) {
        this.delete_single_sql = d;
        return this;
    }

    public String getClearSql() {
        return clear_sql;
    }

    public JDBC_PING2 setClearSql(String c) {
        this.clear_sql = c;
        return this;
    }

    public String getSelectAllPingdataSql() {
        return select_all_pingdata_sql;
    }

    public JDBC_PING2 setSelectAllPingdataSql(String s) {
        this.select_all_pingdata_sql = s;
        return this;
    }

    public String getDatasourceJndiName() {
        return datasource_jndi_name;
    }

    public JDBC_PING2 setDatasourceJndiName(String d) {
        this.datasource_jndi_name = d;
        return this;
    }

    public String getDatasourceInjecterClass() {
        return datasource_injecter_class;
    }

    public JDBC_PING2 setDatasourceInjecterClass(String d) {
        this.datasource_injecter_class = d;
        return this;
    }

    protected static PreparedStatement prepare(final Connection conn, final String sql, final int resultSetType,
                                               final int resultSetConcurrency) throws SQLException {
        try {
            return conn.prepareStatement(sql, resultSetType, resultSetConcurrency);
        } catch (final SQLException x) {
            try {
                return conn.prepareStatement(sql);
            } catch (final SQLException x2) {
                x.addSuppressed(x2);
                throw x;
            }
        }
    }

    protected void loadDriver() {
        assertNonNull("connection_driver", connection_driver);
        log.debug("%s: loading JDBC driver %s", local_addr, connection_driver);
        try {
            Util.loadClass(connection_driver, this.getClass().getClassLoader());
        } catch (ClassNotFoundException e) {
            throw new IllegalArgumentException(String.format("JDBC driver could not be loaded: '%s'", connection_driver));
        }
    }

    protected DataSource injectDataSource(String ds_class) throws Exception {
        Class<?> cl = Util.loadClass(ds_class, Thread.currentThread().getContextClassLoader());
        Object obj = cl.getConstructor().newInstance();
        Function<JDBC_PING2, DataSource> fun = (Function<JDBC_PING2, DataSource>) obj;
        return fun.apply(this);
    }

    protected Connection getConnection() throws SQLException {
        return dataSource != null ? dataSource.getConnection() :
                DriverManager.getConnection(connection_url, connection_username, connection_password);
    }

    protected DataSource getDataSourceFromJNDI(String name) {
        final DataSource data_source;
        InitialContext ctx = null;
        try {
            ctx = new InitialContext();
            Object whatever = ctx.lookup(name);
            if (whatever == null)
                throw new IllegalArgumentException("JNDI name " + name + " is not bound");
            if (!(whatever instanceof DataSource))
                throw new IllegalArgumentException("JNDI name " + name + " was found but is not a DataSource");
            data_source = (DataSource) whatever;
            log.debug("%s: datasource found via JNDI lookup via name: %s", local_addr, name);
            return data_source;
        } catch (NamingException e) {
            throw new IllegalArgumentException("Could not lookup datasource " + name, e);
        } finally {
            if (ctx != null) {
                try {
                    ctx.close();
                } catch (NamingException e) {
                    log.warn("%s: failed to close naming context: %s", local_addr, e);
                }
            }
        }
    }


    protected static void assertNonNull(String... strings) {
        for (int i = 0; i < strings.length; i += 2) {
            String attr = strings[i], val = strings[i + 1];
            if (val == null)
                throw new IllegalArgumentException(String.format("%s must not be null", attr));
        }
    }


    public static void main(String[] args) throws Exception {
        String driver = "org.hsqldb.jdbcDriver";
        String user = "SA";
        String pwd = "";
        String conn = "jdbc:hsqldb:hsql://localhost/";
        String cluster = "draw";
        String select = "SELECT address, name, cluster, ip, coord FROM JGROUPS WHERE cluster=?";

        for (int i = 0; i < args.length; i++) {
            switch (args[i]) {
                case "-driver" -> {
                    driver = args[++i];
                    continue;
                }
                case "-conn" -> {
                    conn = args[++i];
                    continue;
                }
                case "-user" -> {
                    user = args[++i];
                    continue;
                }
                case "-pwd" -> {
                    pwd = args[++i];
                    continue;
                }
                case "-cluster" -> {
                    cluster = args[++i];
                    continue;
                }
                case "-select" -> {
                    select = args[++i];
                    continue;
                }
            }
            System.out.println("JDBC_PING2 [-driver driver] [-conn conn-url] [-user user] [-pwd password] " +
                    "[-cluster cluster-name] [-select select-stmt]");
            return;
        }

        Class.forName(driver);

        try (Connection c = DriverManager.getConnection(conn, user, pwd);
             PreparedStatement ps = prepare(c, select, TYPE_FORWARD_ONLY, CONCUR_UPDATABLE)) {
            ps.setString(1, cluster);
            try (ResultSet resultSet = ps.executeQuery()) {
                int index = 1;
                while (resultSet.next()) {
                    String uuid = resultSet.getString(1);
                    String name = resultSet.getString(2);
                    String cluster_name = resultSet.getString(3);
                    String ip = resultSet.getString(4);
                    boolean coord = resultSet.getBoolean(5);
                    System.out.printf("%d: %s, name=%s, ip=%s, %b (cluster=%s)\n",
                            index++, uuid, name, ip, coord ? "coord" : "server", cluster_name);
                }
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

}
