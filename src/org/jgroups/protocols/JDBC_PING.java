package org.jgroups.protocols;

import org.jgroups.Address;
import org.jgroups.annotations.Property;
import org.jgroups.util.Responses;
import org.jgroups.util.Util;

import javax.naming.InitialContext;
import javax.naming.NamingException;
import javax.sql.DataSource;
import java.sql.*;
import java.util.List;
import java.util.function.Function;

/**
 * <p>Discovery protocol using a JDBC connection to a shared database.
 * Connection options can be defined as configuration properties, or the JNDI
 * name of a {@code DataSource} can be provided (avoid providing both).</p>
 * 
 * <p>Both the schema and the used SQL statements can be customized; make sure
 * the order of parameters of such customized SQL statements is maintained and
 * that compatible types are used for the columns. The recommended schema uses a
 * single table, with two String columns being used primary key (local address,
 * cluster name) and a third column to store the serialized form of the objects
 * needed by JGroups.</p>
 * 
 * <p>A default table will be created at first connection, errors during this
 * operation are not considered critical. Set the {@code initialize_sql}
 * to an empty value to prevent this initial table creation, or change it to
 * create a customized table.</p>
 * 
 * @author Sanne Grinovero
 * @author Bela Ban
 * @since 2.12
 */
public class JDBC_PING extends FILE_PING {

    /* -----------------------------------------    Properties     -------------------------------------------------- */

    @Property(description = "The JDBC connection URL", writable = false)
    protected String connection_url;

    @Property(description = "The JDBC connection username", writable = false)
    protected String connection_username;

    @Property(description = "The JDBC connection password", writable = false, exposeAsManagedAttribute=false)
    protected String connection_password;

    @Property(description = "The JDBC connection driver name", writable = false)
    protected String connection_driver;

    @Property(description = "If not empty, this SQL statement will be performed at startup."
                + "Customize it to create the needed table on those databases which permit table creation attempt without losing data, such as "
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

    @Property(description="SQL to clear the table")
    protected String clear_sql="DELETE from JGROUPSPING WHERE cluster_name=?";
    
    @Property(description = "SQL used to fetch all node's PingData. Customizable, but keep the order of parameters and pick compatible types: " + 
                "only one parameter needed, String compatible, representing the Cluster name. Must return a byte[], the Serialized PingData as" + 
                " it was stored by the insert_single_sql statement. Must select primary keys subsequently for cleanup to work properly")
    protected String select_all_pingdata_sql = "SELECT ping_data, own_addr, cluster_name FROM JGROUPSPING WHERE cluster_name=?";

    @Property(description="Finds a given entry by its address and cluster name, used to implement a contains()")
    protected String contains_sql="SELECT count(own_addr) as RECORDCOUNT from JGROUPSPING WHERE cluster_name=? AND own_addr=?";

    @Property(description = "To use a DataSource registered in JNDI, specify the JNDI name here. " +
        "This is an alternative to all connection_* configuration options: if this property is not empty, then all connection related" +
        "properties must be empty.")
    protected String datasource_jndi_name;

    @Property(description="The fully qualified name of a class which implements a Function<JDBC_PING,DataSource>. " +
      "If not null, this has precedence over datasource_jndi_name.")
    protected String datasource_injecter_class;

    /* --------------------------------------------- Fields ------------------------------------------------------ */

    protected DataSource dataSource;


    @Override protected void createRootDir() {
        ; // do *not* create root file system (don't remove !)
    }

    public JDBC_PING  setDataSource(DataSource ds)         {this.dataSource=ds; return this;}
    public DataSource getDataSource()                      {return dataSource;}
    public String     getConnectionUrl()                   {return connection_url;}
    public JDBC_PING  setConnectionUrl(String c)           {this.connection_url=c; return this;}
    public String     getConnectionUsername()              {return connection_username;}
    public JDBC_PING  setConnectionUsername(String c)      {this.connection_username=c; return this;}
    public String     getConnectionPassword()              {return connection_password;}
    public JDBC_PING  setConnectionPassword(String c)      {this.connection_password=c; return this;}
    public String     getConnectionDriver()                {return connection_driver;}
    public JDBC_PING  setConnectionDriver(String c)        {this.connection_driver=c; return this;}
    public String     getInitializeSql()                   {return initialize_sql;}
    public JDBC_PING  setInitializeSql(String i)           {this.initialize_sql=i; return this;}
    public String     getInsertSingleSql()                 {return insert_single_sql;}
    public JDBC_PING  setInsertSingleSql(String i)         {this.insert_single_sql=i; return this;}
    public String     getDeleteSingleSql()                 {return delete_single_sql;}
    public JDBC_PING  setDeleteSingleSql(String d)         {this.delete_single_sql=d; return this;}
    public String     getClearSql()                        {return clear_sql;}
    public JDBC_PING  setClearSql(String c)                {this.clear_sql=c; return this;}
    public String     getSelectAllPingdataSql()            {return select_all_pingdata_sql;}
    public JDBC_PING  setSelectAllPingdataSql(String s)    {this.select_all_pingdata_sql=s; return this;}
    public String     getContainsSql()                     {return contains_sql;}
    public JDBC_PING  setContainsSql(String c)             {this.contains_sql=c; return this;}
    public String     getDatasourceJndiName()              {return datasource_jndi_name;}
    public JDBC_PING  setDatasourceJndiName(String d)      {this.datasource_jndi_name=d; return this;}
    public String     getDatasourceInjecterClass()         {return datasource_injecter_class;}
    public JDBC_PING  setDatasourceInjecterClass(String d) {this.datasource_injecter_class=d; return this;}


    @Override
    public void init() throws Exception {
        super.init();
        verifyConfigurationParameters();
        // If dataSource is already set, skip loading driver or JNDI lookup
        if(dataSource == null) {
            if(datasource_injecter_class != null) {
                dataSource=injectDataSource(datasource_injecter_class);
                if(dataSource == null) {
                    String m=String.format("datasource_injecter_class %s created null datasource", datasource_injecter_class);
                    throw new IllegalArgumentException(m);
                }
            }
            else {
                if(datasource_jndi_name != null)
                    dataSource=getDataSourceFromJNDI(datasource_jndi_name.trim());
                else
                    loadDriver();
            }
        }
        attemptSchemaInitialization();
    }


    protected void write(List<PingData> list, String clustername) {
        for(PingData data: list)
            writeToDB(data, clustername, true);
    }


    // It's possible that multiple threads in the same cluster node invoke this concurrently;
    // Since delete and insert operations are not atomic
    // (and there is no SQL standard way to do this without introducing a transaction)
    // we need the synchronization or risk a duplicate insertion on same primary key.
    // This synchronization should not be a performance problem as this is just a Discovery protocol.
    // Many SQL dialects have some "insert or update" expression, but that would need
    // additional configuration and testing on each database. See JGRP-1440
    protected synchronized void writeToDB(PingData data, String clustername, boolean overwrite) {
        final String ownAddress = addressAsString(data.getAddress());
        final Connection connection = getConnection();
        if (connection != null) {
            try {
                if(overwrite)
                    delete(connection, clustername, ownAddress);
                else {
                    if(contains(clustername, data.getAddress()))
                        return;
                }
                insert(connection, data, clustername, ownAddress);
            } catch (SQLException e) {
                log.error(Util.getMessage("ErrorUpdatingJDBCPINGTable"), e);
            } finally {
                closeConnection(connection);
            }
        }
        else {
            log.error(Util.getMessage("FailedToStorePingDataInDatabase"));
        }
    }


    protected boolean contains(String cluster_name, Address addr) {
        final String addressAsString = addressAsString(addr);
        try(Connection conn=getConnection()) {
            try (PreparedStatement ps=conn.prepareStatement(contains_sql)) {
                ps.setString(1, cluster_name);
                ps.setString(2, addressAsString);
                try (ResultSet resultSet=ps.executeQuery()) {
                	if(!resultSet.next())
                		return false;
                	int count=resultSet.getInt("RECORDCOUNT");
                	return count > 0;
                }
            }
        }
        catch(SQLException e) {
            log.error(Util.getMessage("ErrorReadingTable"), e);
        }
        return false;
    }

    protected void remove(String clustername, Address addr) {
        final String addressAsString = addressAsString(addr);
        try {
            delete(clustername, addressAsString);
        } catch (SQLException e) {
            log.error("Error", e);
        }
    }

    protected void removeAll(String clustername) {
        clearTable(clustername);
    }

    protected void readAll(List<Address> members, String clustername, Responses responses) {
        final Connection connection = getConnection();
        if (connection != null) {
            try {
                readAll(connection, members, clustername, responses);
            } catch (SQLException e) {
                log.error(Util.getMessage("ErrorReadingJDBCPINGTable"), e);
            } finally {
                closeConnection(connection);
            }
        }
    }

	protected static final PreparedStatement prepareStatement(final Connection connection, final String sql, final int resultSetType,
		final int resultSetConcurrency) throws SQLException {
		try {
			return connection.prepareStatement(sql, resultSetType, resultSetConcurrency);
		} catch(final SQLException x) {
			try {
				return connection.prepareStatement(sql);
			} catch(final SQLException x2) {
				x.addSuppressed(x2);
				throw x;
			}
		}
	}

    protected void readAll(Connection connection, List<Address> members, String clustername, Responses rsps) throws SQLException {
        try (PreparedStatement ps=prepareStatement(connection, select_all_pingdata_sql, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_UPDATABLE)) {
            ps.setString(1, clustername);
            if(log.isTraceEnabled())
                log.trace("%s: SQL for reading: %s", local_addr, ps);
            try (ResultSet resultSet=ps.executeQuery()) {
	            while(resultSet.next()) {
	                byte[] bytes=resultSet.getBytes(1);
	                try {
	                    PingData data=deserialize(bytes);
                        reads++;
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
            }
        }
    }


    protected void attemptSchemaInitialization() {
        if(initialize_sql == null) {
            log.debug("Table creation step skipped: initialize_sql attribute is missing");
            return;
        }
        Connection connection=getConnection();
        if(connection == null)
            return;

        try(PreparedStatement ps=connection.prepareStatement(initialize_sql)) {
            if(log.isTraceEnabled())
                log.trace("SQL for initializing schema: %s", ps);
            ps.execute();
            log.debug("Table created for JDBC_PING Discovery Protocol");
        }
        catch(SQLException e) {
            log.debug("Could not execute initialize_sql statement; not necessarily an error, we always attempt to create the schema. " +
                        "To suppress this message, set initialize_sql to an empty value. Cause: %s", e.getMessage());
        }
        finally {
            closeConnection(connection);
        }
    }

    protected void loadDriver() {
        assertNonNull("connection_driver", connection_driver);
        log.debug("Registering JDBC driver %s", connection_driver);
        try {
            Util.loadClass(connection_driver, this.getClass().getClassLoader());
        }
        catch (ClassNotFoundException e) {
            throw new IllegalArgumentException(String.format("JDBC driver could not be loaded: '%s'", connection_driver));
        }
    }

    protected DataSource injectDataSource(String ds_class) throws Exception {
        Class<?> cl=Util.loadClass(ds_class, Thread.currentThread().getContextClassLoader());
        Object obj=cl.getConstructor().newInstance();
        Function<JDBC_PING,DataSource> fun=(Function<JDBC_PING,DataSource>)obj;
        return fun.apply(this);
    }

    // todo: check if connections should be cached (connection pool?)
    protected Connection getConnection() {
        if (dataSource == null) {
            Connection connection;
            try {
                connection = DriverManager.getConnection(connection_url, connection_username, connection_password);
            } catch (SQLException e) {
                log.error(Util.getMessage("CouldNotOpenConnectionToDatabase"), e);
                return null;
            }
            if (connection == null) {
                log.error(Util.getMessage("ReceivedNullConnectionFromTheDriverManager"));
            }
            return connection;
        }
        else {
            try {
                return dataSource.getConnection();
            } catch (SQLException e) {
                log.error(Util.getMessage("CouldNotOpenConnectionToDatabase"), e);
                return null;
            }
        }
    }



    protected synchronized void insert(Connection connection, PingData data, String clustername, String address) throws SQLException {
        final byte[] serializedPingData = serializeWithoutView(data);
        try (PreparedStatement ps=connection.prepareStatement(insert_single_sql)) {
            ps.setString(1, address);
            ps.setString(2, clustername);
            ps.setBytes(3, serializedPingData);
            if(log.isTraceEnabled())
                log.trace("%s: SQL for insertion: %s", local_addr, ps);
            ps.executeUpdate();
            log.debug("Inserted %s for cluster %s into database", address, clustername);
        }
    }

    protected synchronized void delete(Connection connection, String clustername, String addressToDelete) throws SQLException {
        try(PreparedStatement ps=connection.prepareStatement(delete_single_sql)) {
            ps.setString(1, addressToDelete);
            ps.setString(2, clustername);
            if(log.isTraceEnabled())
                log.trace("%s: SQL for deletion: %s", local_addr, ps);
            ps.executeUpdate();
            log.debug("Removed %s for cluster %s from database", addressToDelete, clustername);
        }
    }
    
    protected void delete(String clustername, String addressToDelete) throws SQLException {
        final Connection connection = getConnection();
        if (connection != null) {
            try {
                delete(connection, clustername, addressToDelete);
            } catch (SQLException e) {
                log.error(Util.getMessage("ErrorUpdatingJDBCPINGTable"), e);
            } finally {
                closeConnection(connection);
            }
        } else {
            log.error(Util.getMessage("FailedToDeletePingDataInDatabase"));
        }
    }


    protected void clearTable(String clustername) {
        try(Connection conn=getConnection()) {
            if(conn != null) {
                try(PreparedStatement ps=conn.prepareStatement(clear_sql)) {
                    // check presence of cluster_name parameter for backwards compatibility
                    if(clear_sql.indexOf('?') >= 0)
                        ps.setString(1, clustername);
                    else
                        log.debug("Please update your clear_sql to include cluster_name parameter.");
                    if(log.isTraceEnabled())
                        log.trace("%s: SQL for clearing the table: %s", local_addr, ps);
                    ps.execute();
                    log.debug("%s: cleared table for cluster %s", local_addr, clustername);
                }
                catch(SQLException e1) {
                    log.error(Util.getMessage("ErrorClearingTable"), e1);
                }
                finally {
                    closeConnection(conn);
                }
            }
        }
        catch(SQLException e2) {
            log.error(Util.getMessage("ErrorClearingTable"), e2);
        }
    }

    
    protected void closeConnection(final Connection connection) {
        try {
            connection.close();
        } catch (SQLException e) {
            log.error(Util.getMessage("ErrorClosingConnectionToJDBCPINGDatabase"), e);
        }
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
            log.debug("Datasource found via JNDI lookup via name: %s", name);
            return data_source;
        } catch (NamingException e) {
            throw new IllegalArgumentException("Could not lookup datasource " + name, e);
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
    
    protected void verifyConfigurationParameters() {
        // initialize_sql is skipped as the table could be created external to JDBC_PING
        assertNonNull("insert_single_sql",         insert_single_sql,
                      "clear_sql",                 clear_sql,
                      "delete_single_sql",         delete_single_sql,
                      "select_all_pingdata_sql",   select_all_pingdata_sql,
                      "contains_sql",              contains_sql);
    }
    
    protected static void assertNonNull(String... strings) {
        for(int i=0; i < strings.length; i+=2) {
            String attr=strings[i], val=strings[i+1];
            if(val == null)
                throw new IllegalArgumentException(String.format("%s must not be null", attr));
        }
    }

    public static void main(String[] args) throws ClassNotFoundException {
        String driver="org.hsqldb.jdbcDriver";
        String user="SA";
        String pwd="";
        String conn="jdbc:hsqldb:hsql://localhost/";
        String cluster="draw";
        String select="SELECT ping_data, own_addr, cluster_name FROM JGROUPSPING WHERE cluster_name=?";

        for(int i=0; i < args.length; i++) {
            if(args[i].equals("-driver")) {
                driver=args[++i];
                continue;
            }
            if(args[i].equals("-conn")) {
                conn=args[++i];
                continue;
            }
            if(args[i].equals("-user")) {
                user=args[++i];
                continue;
            }
            if(args[i].equals("-pwd")) {
                pwd=args[++i];
                continue;
            }
            if(args[i].equals("-cluster")) {
                cluster=args[++i];
                continue;
            }
            if(args[i].equals("-select")) {
                select=args[++i];
                continue;
            }
            System.out.println("JDBC_PING [-driver driver] [-conn conn-url] [-user user] [-pwd password] " +
                               "[-cluster cluster-name] [-select select-stmt]");
            return;
        }

        Class.forName(driver);

        try(Connection c=DriverManager.getConnection(conn, user, pwd);
            PreparedStatement ps=prepareStatement(c, select, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_UPDATABLE)) {
            ps.setString(1, cluster);
            try(ResultSet resultSet=ps.executeQuery()) {
                int index=1;
                while(resultSet.next()) {
                    byte[] bytes=resultSet.getBytes(1);
                    try {
                        PingData data=deserialize(bytes);
                        System.out.printf("%d %s\n", index++, data);
                    }
                    catch(Exception e) {
                    }
                }
            }
        }
        catch(SQLException e) {
            e.printStackTrace();
        }

    }

}
