package  org.jgroups.persistence;

/**
 * @author Mandar Shinde
 * The class implements the PersistenceManager interface and provides users
 * a file based implementation when required.
 * The state of this class is current NOOP. Implementation will be in place 
 * once a better structure for file based properties will be designed.
 */


import java.io.Serializable;
import java.util.Map;

public class FilePersistenceManager implements PersistenceManager
{

    /**
     * Default constructor
     * Currently NOOP
     */
    public FilePersistenceManager(String filePath)
    {
    }

    /**
     * Save new NV pair as serializable objects or if already exist; store 
     * new state 
     * @param Serializable;
     * @param Serializable;
     * @exception CannotPersistException; 
     */
    public void save(Serializable key, Serializable val) throws CannotPersistException
    {
	return;
    }

    /**
     * Remove existing NV from being persisted
     * @param Serializable; key value
     * @return Serializable; gives back the value
     * @exception CannotRemoveException;
     */
    public Serializable  remove(Serializable key) throws CannotRemoveException
    {
	return null;
    }


    /**
     * Use to store a complete map into persistent state
     * @param Map;
     * @exception CannotPersistException;
     */
    public void saveAll(Map map) throws CannotPersistException
    {
	return;
    }


    /**
     * Gives back the Map in last known state
     * @return Map;
     * @exception CannotRetrieveException;
     */
    public Map retrieveAll() throws CannotRetrieveException
    {
	return null;
    }


    /**
     * Clears the complete NV state from the DB
     * @exception CannotRemoveException;
     x*/
    public void clear() throws CannotRemoveException
    {
	return;
    }


    /**
     * Used to handle shutdown call the PersistenceManager implementation. 
     * Persistent engines can leave this implementation empty.
     */
    public void shutDown()
    {
	return;
    }
}// end of class
