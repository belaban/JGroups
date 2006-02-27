// $Id: ClassConfigurator.java,v 1.19 2006/02/27 14:08:45 belaban Exp $

package org.jgroups.conf;


import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jgroups.ChannelException;
import org.jgroups.util.Util;

import java.io.ObjectStreamClass;
import java.util.*;

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

    //this is where we store magic numbers
    private final Map classMap=new HashMap(); // key=Class, value=magic number
    private final Map magicMap=new HashMap(); // key=magic number, value=Class

    /** Map<Integer,ObjectStreamClass> */
    private final Map streamMapId=new HashMap();

    /** Map<ObjectStreamClass, Integer> */
    private final Map streamMapClass=new HashMap();

    protected final Log log=LogFactory.getLog(getClass());


    private ClassConfigurator() {
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
                String mnfile = System.getProperty("org.jgroups.conf.magicNumberFile");
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
                Integer m;
                for(int i=0; i < mapping.length; i++) {
                    m=new Integer(mapping[i].getMagicNumber());
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


    /**
     * Returns a class for a magic number.
     * Returns null if no class is found
     *
     * @param magic the magic number that maps to the class
     * @return a Class object that represents a class that implements java.io.Externalizable
     */
    public Class get(int magic) {
        return (Class)magicMap.get(new Integer(magic));
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
    public int getMagicNumber(Class clazz) {
        Integer i=(Integer)classMap.get(clazz);
        if(i == null)
            return -1;
        else
            return i.intValue();
    }

    public int getMagicNumberFromObjectStreamClass(ObjectStreamClass objStream) {
        Integer i=(Integer)streamMapClass.get(objStream);
        if(i == null)
            return -1;
        else
            return i.intValue();
    }

    public ObjectStreamClass getObjectStreamClassFromMagicNumber(int magic_number) {
        ObjectStreamClass retval=null;
        retval=(ObjectStreamClass)streamMapId.get(new Integer(magic_number));
        return retval;
    }


    public String toString() {
        return printMagicMap();
    }

    public String printMagicMap() {
        StringBuffer sb=new StringBuffer();
        Integer key;
        SortedSet keys=new TreeSet(magicMap.keySet());

        for(Iterator it=keys.iterator(); it.hasNext();) {
            key=(Integer)it.next();
            sb.append(key).append(":\t").append(magicMap.get(key)).append('\n');
        }
        return sb.toString();
    }

    public String printClassMap() {
        StringBuffer sb=new StringBuffer();
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
