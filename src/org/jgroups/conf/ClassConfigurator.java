
package org.jgroups.conf;


import org.jgroups.Constructable;
import org.jgroups.Global;
import org.jgroups.Header;
import org.jgroups.util.Triple;
import org.jgroups.util.Util;
import org.w3c.dom.Document;
import org.w3c.dom.NamedNodeMap;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import java.io.FileInputStream;
import java.io.InputStream;
import java.util.*;
import java.util.function.Supplier;

/**
 * Maintains a mapping between magic IDs and classes (defined in jg-magic-map.xml), and between protocol IDs and
 * protocol classes (defined in jg-protocol-ids.xml). The first mapping is used to for fast serialization, whereas
 * the second is used to assign protocol IDs to protocols at startup time.
 *
 * @author Filip Hanik
 * @author Bela Ban
 */
public class ClassConfigurator {
    public static final String MAGIC_NUMBER_FILE = "jg-magic-map.xml";
    public static final String PROTOCOL_ID_FILE  = "jg-protocol-ids.xml";
    private static final int   MAX_MAGIC_VALUE=100;
    private static final short MIN_CUSTOM_MAGIC_NUMBER=1024;
    private static final short MIN_CUSTOM_PROTOCOL_ID=512;

    // this is where we store magic numbers; contains data from jg-magic-map.xml;  key=Class, value=magic number
    private static final Map<Class,Short> classMap=new IdentityHashMap<>(MAX_MAGIC_VALUE);


    // Magic map for all values defined in jg-magic-map.xml; elements are supplier functions which create instances
    private static final Supplier<? extends Object>[] magicMap=new Supplier[MAX_MAGIC_VALUE];

    // Magic map for user-defined IDs / classes or suppliers
    private static final Map<Short,Object> magicMapUser=new HashMap<>(); // key=magic number, value=Class or Supplier<Header>

    /** Contains data read from jg-protocol-ids.xml */
    private static final Map<Class,Short> protocol_ids=new HashMap<>(MAX_MAGIC_VALUE);
    private static final Map<Short,Class> protocol_names=new HashMap<>(MAX_MAGIC_VALUE);

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



    /**
     * Method to register a user-defined header with jg-magic-map at runtime
     * @param magic The magic number. Needs to be > 1024
     * @param clazz The class. Usually a subclass of Header
     * @throws IllegalArgumentException If the magic number is already taken, or the magic number is <= 1024
     */
    public static void add(short magic, Class clazz) {
        if(magic < MIN_CUSTOM_MAGIC_NUMBER)
            throw new IllegalArgumentException("magic ID (" + magic + ") must be >= " + MIN_CUSTOM_MAGIC_NUMBER);
        if(magicMapUser.containsKey(magic) || classMap.containsKey(clazz))
            alreadyInMagicMap(magic, clazz.getName());

        Object inst=null;
        try {
            inst=clazz.newInstance();
        }
        catch(Exception e) {
            throw new IllegalStateException("failed creating instance " + clazz, e);
        }

        Object val=clazz;
        if(Header.class.isAssignableFrom(clazz)) { // class is a header
            checkSameId((Header)inst, magic);
            val=((Header)inst).create();
        }

        if(Constructable.class.isAssignableFrom(clazz)) {
            val=((Constructable)inst).create();
            inst=((Supplier<?>)val).get();
            if(!inst.getClass().equals(clazz))
                throw new IllegalStateException(String.format("%s.create() returned the wrong class: %s\n",
                                                              clazz.getSimpleName(), inst.getClass().getSimpleName()));
        }

        magicMapUser.put(magic, val);
        classMap.put(clazz, magic);
    }


    public static void addProtocol(short id, Class protocol) {
        if(id < MIN_CUSTOM_PROTOCOL_ID)
            throw new IllegalArgumentException("protocol ID (" + id + ") needs to be greater than or equal to " + MIN_CUSTOM_PROTOCOL_ID);
        if(protocol_ids.containsKey(protocol))
            alreadyInProtocolsMap(id, protocol.getName());
        protocol_ids.put(protocol, id);
        protocol_names.putIfAbsent(id, protocol);
    }


    public static <T extends Object> T create(short id) throws Exception {
        if(id >= MIN_CUSTOM_MAGIC_NUMBER) {
            Object val=magicMapUser.get(id);
            if(val == null)
                throw new ClassNotFoundException("Class for magic number " + id + " cannot be found");
            return (T) ((val instanceof Supplier)? ((Supplier)val).get() : ((Class)val).newInstance());
        }
        Supplier<?> supplier=magicMap[id];
        if(supplier == null)
            throw new ClassNotFoundException("Class for magic number " + id + " cannot be found");
        return (T)supplier.get();
    }


    /**
     * Loads and returns the class from the class name
     *
     * @param clazzname a fully classified class name to be loaded
     * @return a Class object that represents a class that implements java.io.Externalizable
     */
    public static Class get(String clazzname, ClassLoader loader) throws ClassNotFoundException {
        return Util.loadClass(clazzname, loader != null? loader : ClassConfigurator.class.getClassLoader());
    }

    public static Class get(String clazzname) throws ClassNotFoundException {
        return Util.loadClass(clazzname, ClassConfigurator.class);
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


    public static short getProtocolId(Class protocol) {
        Short retval=protocol_ids.get(protocol);
        return retval != null? retval : 0;
    }


    public static Class getProtocol(short id) {
        return protocol_names.get(id);
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


    protected static void init() throws Exception {
        // make sure we have a class for DocumentBuilderFactory
        Util.loadClass("javax.xml.parsers.DocumentBuilderFactory", ClassConfigurator.class);

        String magic_number_file=null, protocol_id_file=null;
        try { // PropertyPermission not granted if running in an untrusted environment with JNLP
            magic_number_file=Util.getProperty(new String[]{Global.MAGIC_NUMBER_FILE, "org.jgroups.conf.magicNumberFile"},
                                               null, null,  MAGIC_NUMBER_FILE);
            protocol_id_file=Util.getProperty(new String[]{Global.PROTOCOL_ID_FILE, "org.jgroups.conf.protocolIDFile"},
                                              null, null, PROTOCOL_ID_FILE);
        }
        catch (SecurityException ex){
        }

        // Read jg-magic-map.xml
        List<Triple<Short,String,Boolean>> mapping=readMappings(magic_number_file);
        for(Triple<Short,String,Boolean> tuple: mapping) {
            short m=tuple.getVal1();
            if(m >= MAX_MAGIC_VALUE)
                throw new IllegalArgumentException("ID " + m + " is bigger than MAX_MAGIC_VALUE (" +
                                                     MAX_MAGIC_VALUE + "); increase MAX_MAGIC_VALUE");
            boolean external=tuple.getVal3();
            if(external) {
                if(magicMap[m] != null)
                    alreadyInMagicMap(m, tuple.getVal2());
                continue;
            }
            Class clazz=Util.loadClass(tuple.getVal2(), ClassConfigurator.class);
            if(magicMap[m] != null)
                alreadyInMagicMap(m, clazz.getName());

            if(Constructable.class.isAssignableFrom(clazz)) {
                Constructable obj=(Constructable)clazz.newInstance();
                magicMap[m]=obj.create();
            }
            else {
                Supplier<? extends Object> supplier=(Supplier<Object>)() -> {
                    try {
                        return clazz.newInstance();
                    }
                    catch(Throwable throwable) {
                        return null;
                    }
                };
                magicMap[m]=supplier;
            }

            Object inst=magicMap[m].get();
            if(inst == null)
                continue;

            // test to confirm that the Constructable impl returns an instance of the correct type
            if(!inst.getClass().equals(clazz))
                throw new IllegalStateException(String.format("%s.create() returned the wrong class: %s\n",
                                                              clazz.getSimpleName(), inst.getClass().getSimpleName()));
            // check that the IDs are the same
            if(inst instanceof Header)
                checkSameId((Header)inst, m);
            classMap.put(clazz, m);
        }

        mapping=readMappings(protocol_id_file); // Read jg-protocol-ids.xml
        for(Triple<Short,String,Boolean> tuple: mapping) {
            short m=tuple.getVal1();
            boolean external=tuple.getVal3();
            if(external) {
                if(protocol_names.containsKey(m))
                    alreadyInProtocolsMap(m, tuple.getVal2());
                continue;
            }

            Class clazz=Util.loadClass(tuple.getVal2(), ClassConfigurator.class);
            if(protocol_ids.containsKey(clazz))
                alreadyInProtocolsMap(m, clazz.getName());
            protocol_ids.put(clazz, m);
            protocol_names.put(m, clazz);
        }
    }

    protected static void checkSameId(Header hdr, short magic) {
        short tmp_id=hdr.getMagicId();
        if(tmp_id != magic)
            throw new IllegalStateException(String.format("mismatch between %s.getId() (%d) and the defined ID (%d)",
                                                          hdr.getClass().getSimpleName(), magic, tmp_id));
    }

    protected static void alreadyInMagicMap(short magic, String classname) {
        throw new IllegalArgumentException("key " + magic + " (" + classname + ')' +
                                             " is already in magic map; make sure that all keys are unique");
    }

    protected static void alreadyInProtocolsMap(short prot_id, String classname) {
        throw new IllegalArgumentException("ID " + prot_id + " (" + classname + ')' +
                                             " is already in protocol-ids map; make sure that all protocol IDs are unique");
    }


    /**
     * try to read the magic number configuration file as a Resource form the classpath using getResourceAsStream
     * if this fails this method tries to read the configuration file from mMagicNumberFile using a FileInputStream (not in classpath but somewhere else in the disk)
     *
     * @return an array of ClassMap objects that where parsed from the file (if found) or an empty array if file not found or had en exception
     */
    protected static List<Triple<Short,String,Boolean>> readMappings(String name) throws Exception {
        InputStream stream;
        stream=Util.getResourceAsStream(name, ClassConfigurator.class);
        // try to load the map from file even if it is not a Resource in the class path
        if(stream == null)
            stream=new FileInputStream(name);
        return parse(stream);
    }

    protected static List<Triple<Short,String,Boolean>> parse(InputStream stream) throws Exception {
        DocumentBuilderFactory factory=DocumentBuilderFactory.newInstance();
        factory.setValidating(false); // for now
        DocumentBuilder builder=factory.newDocumentBuilder();
        Document document=builder.parse(stream);
        NodeList class_list=document.getElementsByTagName("class");
        List<Triple<Short,String,Boolean>> list=new LinkedList<>();
        for(int i=0; i < class_list.getLength(); i++) {
            if(class_list.item(i).getNodeType() == Node.ELEMENT_NODE) {
                list.add(parseClassData(class_list.item(i)));
            }
        }
        return list;
    }

    protected static Triple<Short,String,Boolean> parseClassData(Node protocol) {
        protocol.normalize();
        NamedNodeMap attrs=protocol.getAttributes();
        boolean external=false;

        String magicnumber=attrs.getNamedItem("id").getNodeValue();
        String clazzname=attrs.getNamedItem("name").getNodeValue();

        Node tmp=attrs.getNamedItem("external");
        if(tmp != null)
            external=Boolean.parseBoolean(tmp.getNodeValue());
        return new Triple<>(Short.valueOf(magicnumber), clazzname,external);
    }


}
