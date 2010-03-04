
package org.jgroups.conf;


import org.jgroups.logging.Log;
import org.jgroups.logging.LogFactory;
import org.jgroups.ChannelException;
import org.jgroups.Global;
import org.jgroups.util.Util;
import org.jgroups.util.Tuple;
import org.w3c.dom.Document;
import org.w3c.dom.NodeList;
import org.w3c.dom.Node;
import org.w3c.dom.NamedNodeMap;

import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.DocumentBuilder;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.io.InputStream;
import java.io.FileInputStream;
import java.io.IOException;

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
 */
public class ClassConfigurator {
    public static final String MAGIC_NUMBER_FILE="jg-magic-map.xml";
    private static final short MIN_CUSTOM_MAGIC_NUMBER=1024;

    //this is where we store magic numbers
    private static final Map<Class,Short> classMap=new ConcurrentHashMap<Class,Short>(); // key=Class, value=magic number
    private static final Map<Short,Class> magicMap=new ConcurrentHashMap<Short,Class>(); // key=magic number, value=Class
    protected static final Log log=LogFactory.getLog(ClassConfigurator.class);


    static {
        try {
            init();
        }
        catch(Exception e) {
            throw new ExceptionInInitializerError(e);
        }
    }

    public ClassConfigurator() {
    }

    protected static void init() throws ChannelException {
        try {
            // make sure we have a class for DocumentBuilderFactory
            Util.loadClass("javax.xml.parsers.DocumentBuilderFactory", ClassConfigurator.class);

            String mnfile=null;
            try { // PropertyPermission not granted if running in an untrusted environment with JNLP
                mnfile=Util.getProperty(new String[]{Global.MAGIC_NUMBER_FILE, "org.jgroups.conf.magicNumberFile"},
                                               null, null, false, MAGIC_NUMBER_FILE);
                if(log.isDebugEnabled()) log.debug("Using " + mnfile + " as magic number file");
            }
            catch (SecurityException ex){
            }

            List<Tuple<Short,String>> mapping=readMagicNumberMapping(mnfile);
            for(Tuple<Short,String> tuple: mapping) {
                short m=tuple.getVal1();
                try {
                    Class clazz=Util.loadClass(tuple.getVal2(), ClassConfigurator.class);
                    if(magicMap.containsKey(m))
                        throw new ChannelException("key " + m + " (" + clazz.getName() + ')' +
                                " is already in map; please make sure that all keys are unique");
                    
                    magicMap.put(m, clazz);
                    classMap.put(clazz, m);
                }
                catch(ClassNotFoundException cnf) {
                    throw new ChannelException("failed loading class", cnf);
                }
            }
        }
        catch(ChannelException ex) {
            throw ex;
        }
        catch(Throwable x) {
            throw new ChannelException("failed reading the magic number mapping file", x);
        }
    }



    /**
     * Method to register a user-defined header with jg-magic-map at runtime
     * @param magic The magic number. Needs to be > 1024
     * @param clazz The class. Usually a subclass of Header
     * @throws IllegalArgumentException If the magic number is already taken, or the magic number is <= 1024
     */
     public static void add(short magic, Class clazz) throws IllegalArgumentException {
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
    public static Class get(short magic) {
        return magicMap.get(magic);
    }

    /**
     * Loads and returns the class from the class name
     *
     * @param clazzname a fully classified class name to be loaded
     * @return a Class object that represents a class that implements java.io.Externalizable
     */
    public static Class get(String clazzname) {
        try {
            // return ClassConfigurator.class.getClassLoader().loadClass(clazzname);
            return Util.loadClass(clazzname, ClassConfigurator.class);
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
    public static short getMagicNumber(Class clazz) {
        Short i=classMap.get(clazz);
        if(i == null)
            return -1;
        else
            return i;
    }




    public String toString() {
        return printMagicMap();
    }

    public static String printMagicMap() {
        StringBuilder sb=new StringBuilder();
        SortedSet<Short> keys=new TreeSet<Short>(magicMap.keySet());

        for(Short key: keys) {
            sb.append(key).append(":\t").append(magicMap.get(key)).append('\n');
        }
        return sb.toString();
    }

    public static String printClassMap() {
        StringBuilder sb=new StringBuilder();
        Map.Entry entry;

        for(Iterator it=classMap.entrySet().iterator(); it.hasNext();) {
            entry=(Map.Entry)it.next();
            sb.append(entry.getKey()).append(": ").append(entry.getValue()).append('\n');
        }
        return sb.toString();
    }


    /**
     * try to read the magic number configuration file as a Resource form the classpath using getResourceAsStream
     * if this fails this method tries to read the configuration file from mMagicNumberFile using a FileInputStream (not in classpath but somewhere else in the disk)
     *
     * @return an array of ClassMap objects that where parsed from the file (if found) or an empty array if file not found or had en exception
     */
    protected static List<Tuple<Short,String>> readMagicNumberMapping(String name) throws Exception {
        InputStream stream;
        try {
            stream=Util.getResourceAsStream(name, ClassConfigurator.class);
            // try to load the map from file even if it is not a Resource in the class path
            if(stream == null) {
                if(log.isTraceEnabled())
                    log.trace("Could not read " + name + " from the CLASSPATH, will try to read it from file");
                stream=new FileInputStream(name);
            }
        }
        catch(Exception x) {
            throw new ChannelException(name + " not found. Please make sure it is on the classpath", x);
        }
        return parse(stream);
    }

    protected static List<Tuple<Short,String>> parse(InputStream stream) throws Exception {
        DocumentBuilderFactory factory=DocumentBuilderFactory.newInstance();
        factory.setValidating(false); //for now
        DocumentBuilder builder=factory.newDocumentBuilder();
        Document document=builder.parse(stream);
        NodeList class_list=document.getElementsByTagName("class");
        List<Tuple<Short,String>> list=new LinkedList<Tuple<Short,String>>();
        for(int i=0; i < class_list.getLength(); i++) {
            if(class_list.item(i).getNodeType() == Node.ELEMENT_NODE) {
                list.add(parseClassData(class_list.item(i)));
            }
        }
        return list;
    }

    protected static Tuple<Short,String> parseClassData(Node protocol) throws java.io.IOException {
        try {
            protocol.normalize();
            NamedNodeMap attrs=protocol.getAttributes();
            String clazzname;
            String magicnumber;

            magicnumber=attrs.getNamedItem("id").getNodeValue();
            clazzname=attrs.getNamedItem("name").getNodeValue();
            return new Tuple<Short,String>(Short.valueOf(magicnumber), clazzname);
        }
        catch(Exception x) {
            IOException tmp=new IOException();
            tmp.initCause(x);
            throw tmp;
        }
    }




}
