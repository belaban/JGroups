package org.jgroups.persistence;

/**
 * @author Mandar Shinde
 * This class is the factory to get access to any DB based or file based
 * implementation. None of the implemenations should expose directly
 * to user for migration purposes
 */


import java.io.FileInputStream;
import java.util.Properties;


public class PersistenceFactory
{

    /**
     * Default private constructor// does nothing
     */
    private PersistenceFactory()
    {
    }


    /**
     * Singular public method to get access to any of the Persistence
     * Manager implementations. It is important to known at this point
     * that properties determine the implementation of the Persistence
     * Manager, there is no direct interface which gives access to 
     * either DB implemented ot FILE implemented storage API.
     * @return PersistenceFactory;
     */
    public static PersistenceFactory getInstance() {
	System.err.println(" getting factory instance ");
        if (_factory == null)
	    _factory = new PersistenceFactory();
	return _factory;
    }

    
    /**
     * Reads the default properties and creates a persistencemanager
     * The default properties are picked up from the $USER_HOME or 
     * from the classpath. Default properties are represented by
     * "persist.properties"
     * @return PersistenceManager
     * @exception Exception;
     */ 
    public synchronized PersistenceManager createManager() throws Exception {
	// will return null if not initialized
	// uses default properties
	if (_manager == null)
	    {
		if (checkDB())
		    _manager = createManagerDB(propPath);
		else
		    _manager = createManagerFile(propPath);
	    }
	return _manager;	    
    }// end of default


    
    /**
     * Duplicated signature to create PersistenceManager to allow user to
     * provide property path. 
     * @param String; complete pathname to get the properties
     * @return PersistenceManager;
     * @exception Exception;
     */
    public synchronized PersistenceManager createManager (String filePath) throws Exception 
    {
	if (_manager == null)
	    {
		if (checkDB(filePath))
		    _manager = createManagerDB(filePath);
		else
		    _manager = createManagerFile(filePath);
	    }
	return _manager;
    }



    /**
     * Internal creator of DB persistence manager, the outside user accesses only
     * the PersistenceManager interface API
     * @param String path;
     * @return PersistenceManager;
     * @exception Exception;
     */
    private PersistenceManager createManagerDB(String filePath) throws Exception {
	System.err.println(" Calling db persist from factory");
        if (_manager == null)	    
            _manager = new DBPersistenceManager(filePath);
        return _manager;
    }// end of DB persistence



    /**
     * creates instance of file based persistency manager
     * @param String path;
     * @return PersistenceManager
     */
    private PersistenceManager createManagerFile(String filePath)
    {
	System.err.println(" Calling file persist from factory");
	try
	    {
		if (_manager == null)	    
		    _manager = new FilePersistenceManager(filePath);
		return _manager;
	    }catch (Throwable t)
		{
		    t.printStackTrace();
		    return null;
		}
    }// end of file persistence
    

    /**
     * checks the default properties for DB/File flag
     * @return boolean;
     * @exception Exception;
     */
    private boolean checkDB() throws Exception
    {
	Properties props = null;
	FileInputStream _stream = new FileInputStream(propPath);
	props = new Properties();
	props.load(_stream);
	String persist = props.getProperty(persistProp);
	if (persist.equals("DB"))
	    return true;
	return false;
    }// end of default check


    /**
     * checks the provided properties for DB/File flag
     * @return boolean;
     */
    private boolean checkDB(String filePath) throws Exception
    {
	Properties props = null;
	FileInputStream _stream = new FileInputStream(filePath);
	props = new Properties();
	props.load(_stream);
	String persist = props.getProperty(persistProp);
	if (persist.equals("DB"))
	    return true;
	return false;
    }
    

    private static PersistenceManager _manager = null;
    private static PersistenceFactory _factory = null;
   

    /* Please set this according to configuration */
    final static String propPath = "persist.properties";
    final static String persistProp = "persist";
}
