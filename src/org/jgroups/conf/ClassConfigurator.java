// $Id: ClassConfigurator.java,v 1.21.4.2 2008/05/22 13:23:05 belaban Exp $

package org.jgroups.conf;


import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jgroups.ChannelException;
import org.jgroups.Global;
import org.jgroups.util.Util;

import java.io.ObjectStreamClass;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * This class will be replaced with the class that read info
 * from the magic number configurator that reads info from the xml file.
 * The name and the relative path of the magic number map file can be specified
 * as value of the property <code>org.jgroups.conf.magicNumberFile</code>.
 * It must be relative to one of the classpath elements, to allow the
 * classloader to locate the file. If a value is not specified,
 * <code>MagicNumberReader.MAGIC_NUMBER_FILE</code> is used, which defaults
 * to "jg-magic-map.xml".
 *
 * @author Filip Hanik
 * @author Bela Ban
 * @see MagicNumberReader
 */
public class ClassConfigurator {
    static volatile ClassConfigurator instance=null; // works under the new JSR 133 memory model in JDK 5
    private static final short MIN_CUSTOM_MAGIC_NUMBER=1024;

    //this is where we store magic numbers
    private final Map<Class,Short> classMap=new ConcurrentHashMap<Class,Short>(); // key=Class, value=magic number
    private final Map<Short,Class> magicMap=new ConcurrentHashMap<Short,Class>(); // key=magic number, value=Class

    /** Map<Short,ObjectStreamClass> */
    private final Map<Short,ObjectStreamClass> streamMapId=new HashMap<Short,ObjectStreamClass>();

    /** Map<ObjectStreamClass, Short> */
    private final Map<ObjectStreamClass, Short> streamMapClass=new HashMap<ObjectStreamClass, Short>();

    protected final Log log=LogFactory.getLog(getClass());


    public ClassConfigurator() {
    }

    public void init() throws ChannelException {
        //populate the map
        try {
            // make sure we have a class for DocumentBuilderFactory
            // getClass().getClassLoader().loadClass("javax.xml.parsers.DocumentBuilderFactory");
            Util.loadClass("javax.xml.parsers.DocumentBuilderFactory", this.getClass());

            MagicNumberReader reader=new MagicNumberReader();
            
            // PropertyPermission not granted if running in an untrusted environment with JNLP.
            try {
                String mnfile=Util.getProperty(new String[]{Global.MAGIC_NUMBER_FILE, "org.jgroups.conf.magicNumberFile"},
                                               null, null, false, null);
                if(mnfile != null) {
                    if(log.isDebugEnabled()) log.debug("Using " + mnfile + " as magic number file");
                    reader.setFilename(mnfile);
                }
            }
            catch (SecurityException ex){
            }

            ObjectStreamClass objStreamClass;
            ClassMap[] mapping=reader.readMagicNumberMapping();
            if(mapping != null) {
                Short m;
                for(int i=0; i < mapping.length; i++) {
                    m=new Short(mapping[i].getMagicNumber());
                    try {
                        Class clazz=mapping[i].getClassForMap();
                        objStreamClass=ObjectStreamClass.lookup(clazz);
                        if(objStreamClass == null)
                            throw new ChannelException("ObjectStreamClass for " + clazz + " not found");
                        if(magicMap.containsKey(m)) {
                            throw new ChannelException("magic key " + m + " (" + clazz.getName() + ')' +
                                                       " is already in map; please make sure that " +
                                                       "all magic keys are unique");
                        }
                        else {
                            magicMap.put(m, clazz);
                            classMap.put(clazz, m);

                            streamMapId.put(m, objStreamClass);
                            streamMapClass.put(objStreamClass, m);
                        }
                    }
                    catch(ClassNotFoundException cnf) {
                        throw new ChannelException("failed loading class", cnf);
                    }
                }
                if(log.isDebugEnabled()) log.debug("mapping is:\n" + printMagicMap());
            }
        }
        catch(ChannelException ex) {
            throw ex;
        }
        catch(Throwable x) {
            // if(log.isErrorEnabled()) log.error("failed reading the magic number mapping file, reason: " + Util.print(x));
            throw new ChannelException("failed reading the magic number mapping file", x);
        }
    }


    public static ClassConfigurator getInstance(boolean init) throws ChannelException {
        if(instance == null) {
            instance=new ClassConfigurator();
            if(init)
                instance.init();
        }
        return instance;
    }

    public static ClassConfigurator getInstance() throws ChannelException {
        return getInstance(false);
    }

    /**
     * Method to register a user-defined header with jg-magic-map at runtime
     * @param magic The magic number. Needs to be > 1024
     * @param clazz The class. Usually a subclass of Header
     * @throws IllegalArgumentException If the magic number is already taken, or the magic number is <= 1024
     */
     public void add(short magic, Class clazz) throws IllegalArgumentException {
        if(magic <= MIN_CUSTOM_MAGIC_NUMBER)
            throw new IllegalArgumentException("magic number (" + magic + ") needs to be greater than " +
                    MIN_CUSTOM_MAGIC_NUMBER);
        if(magicMap.containsKey(magic) || classMap.containsKey(clazz))
            throw new IllegalArgumentException("magic number " + magic + " for class " + clazz.getName() +
                    " is already present");
        magicMap.put(magic, clazz);
        classMap.put(clazz, magic);
    }

    /**
     * Returns a class for a magic number.
     * Returns null if no class is found
     *
     * @param magic the magic number that maps to the class
     * @return a Class object that represents a class that implements java.io.Externalizable
     */
    public Class get(short magic) {
        return magicMap.get(magic);
    }

    /**
     * Loads and returns the class from the class name
     *
     * @param clazzname a fully classified class name to be loaded
     * @return a Class object that represents a class that implements java.io.Externalizable
     */
    public Class get(String clazzname) {
        try {
            // return ClassConfigurator.class.getClassLoader().loadClass(clazzname);
            return Util.loadClass(clazzname, this.getClass());
        }
        catch(Exception x) {
            if(log.isErrorEnabled()) log.error("failed loading class " + clazzname, x);
        }
        return null;
    }

    /**
     * Returns the magic number for the class.
     *
     * @param clazz a class object that we want the magic number for
     * @return the magic number for a class, -1 if no mapping is available
     */
    public short getMagicNumber(Class clazz) {
        Short i=classMap.get(clazz);
        if(i == null)
            return -1;
        else
            return i;
    }

    public short getMagicNumberFromObjectStreamClass(ObjectStreamClass objStream) {
        Short i=streamMapClass.get(objStream);
        if(i == null)
            return -1;
        else
            return i.shortValue();
    }

    public ObjectStreamClass getObjectStreamClassFromMagicNumber(short magic_number) {
        ObjectStreamClass retval=null;
        retval=streamMapId.get(magic_number);
        return retval;
    }


    public String toString() {
        return printMagicMap();
    }

    public String printMagicMap() {
        StringBuilder sb=new StringBuilder();
        SortedSet<Short> keys=new TreeSet<Short>(magicMap.keySet());

        for(Short key: keys) {
            sb.append(key).append(":\t").append(magicMap.get(key)).append('\n');
        }
        return sb.toString();
    }

    public String printClassMap() {
        StringBuilder sb=new StringBuilder();
        Map.Entry entry;

        for(Iterator it=classMap.entrySet().iterator(); it.hasNext();) {
            entry=(Map.Entry)it.next();
            sb.append(entry.getKey()).append(": ").append(entry.getValue()).append('\n');
        }
        return sb.toString();
    }



    /* --------------------------------- Private methods ------------------------------------ */

    /* ------------------------------ End of Pivate methods --------------------------------- */
    public static void main(String[] args)
            throws Exception {

        ClassConfigurator test=getInstance(true);
        System.out.println('\n' + test.printMagicMap());
    }
}
