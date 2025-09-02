
package org.jgroups.conf;


import org.jgroups.Constructable;
import org.jgroups.Global;
import org.jgroups.Header;
import org.jgroups.util.IntHashMap;
import org.jgroups.util.Triple;
import org.jgroups.util.Util;

import java.io.FileInputStream;
import java.io.IOException;
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
    public static final String    MAGIC_NUMBER_FILE = "jg-magic-map.xml";
    public static final String    PROTOCOL_ID_FILE  = "jg-protocol-ids.xml";
    protected static final String CLASS             = "<class";
    protected static final String ID                = "id";
    protected static final String NAME              = "name";
    protected static final String EXTERNAL          = "external";
    private static final int      MAX_MAGIC_VALUE=100;
    private static final int      MAX_PROT_ID_VALUE=256;
    private static final short    MIN_CUSTOM_MAGIC_NUMBER=1024;
    private static final short    MIN_CUSTOM_PROTOCOL_ID=512;

    // this is where we store magic numbers; contains data from jg-magic-map.xml;  key=Class, value=magic number
    private static final Map<Class<?>,Short> classMap=new IdentityHashMap<>(MAX_MAGIC_VALUE);


    // Magic map for all values defined in jg-magic-map.xml; elements are supplier functions which create instances
    private static final Supplier<? extends Object>[] magicMap=new Supplier[MAX_MAGIC_VALUE];

    // Magic map for user-defined IDs / classes or suppliers
    private static final Map<Short,Object> magicMapUser=new HashMap<>(); // key=magic number, value=Class or Supplier<Header>

    /** Contains data read from jg-protocol-ids.xml */
    private static final Map<Class<?>,Short>  protocol_ids=new IdentityHashMap<>(MAX_PROT_ID_VALUE);
    private static final IntHashMap<Class<?>> protocol_names=new IntHashMap<>(MAX_PROT_ID_VALUE);

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


    public static void addIfAbsent(short magic, Class<?> clazz) {
        if(!magicMapUser.containsKey(magic) && !classMap.containsKey(clazz))
            add(magic, clazz);
    }


    /**
     * Method to register a user-defined header with jg-magic-map at runtime
     * @param magic The magic number. Needs to be &gt; 1024
     * @param clazz The class. Usually a subclass of Header
     * @throws IllegalArgumentException If the magic number is already taken, or the magic number is {@literal <= 1024}
     */
    public static void add(short magic, Class<?> clazz) {
        if(magic < MIN_CUSTOM_MAGIC_NUMBER)
            throw new IllegalArgumentException("magic ID (" + magic + ") must be >= " + MIN_CUSTOM_MAGIC_NUMBER);
        if(magicMapUser.containsKey(magic) || classMap.containsKey(clazz))
            alreadyInMagicMap(magic, clazz.getName());

        Object inst=null;
        try {
            inst=clazz.getDeclaredConstructor().newInstance();
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
            val=((Constructable<?>)inst).create();
            inst=((Supplier<?>)val).get();
            if(!inst.getClass().equals(clazz))
                throw new IllegalStateException(String.format("%s.create() returned the wrong class: %s\n",
                                                              clazz.getSimpleName(), inst.getClass().getSimpleName()));
        }

        magicMapUser.put(magic, val);
        classMap.put(clazz, magic);
    }


    public static void addProtocol(short id, Class<?> protocol) {
        if(id < MIN_CUSTOM_PROTOCOL_ID)
            throw new IllegalArgumentException("protocol ID (" + id + ") needs to be greater than or equal to " + MIN_CUSTOM_PROTOCOL_ID);
        if(protocol_ids.containsKey(protocol))
            alreadyInProtocolsMap(id, protocol.getName());
        protocol_ids.put(protocol, id);
        protocol_names.put(id, protocol);
    }


    public static <T extends Object> T create(short id) throws ClassNotFoundException {
        if(id >= MIN_CUSTOM_MAGIC_NUMBER) {
            Object val=magicMapUser.get(id);
            if(val == null)
                throw new ClassNotFoundException("Class for magic number " + id + " cannot be found, map: "+ magicMapUser);
            if (val instanceof Supplier) {
                return ((Supplier<T>) val).get();
            }
            try {
                return ((Class<T>) val).getDeclaredConstructor().newInstance();
            }
            catch (ReflectiveOperationException e) {
                throw new IllegalStateException(e);
            }
        }
        if(id < 0 || id > MAX_MAGIC_VALUE)
            throw new IllegalArgumentException(String.format("invalid magic number %d; needs to be in range [0..%d]",
                                                             id, MAX_MAGIC_VALUE));
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
    public static Class<?> get(String clazzname, ClassLoader loader) throws ClassNotFoundException {
        return Util.loadClass(clazzname, loader != null? loader : ClassConfigurator.class.getClassLoader());
    }

    public static Class<?> get(String clazzname) throws ClassNotFoundException {
        return Util.loadClass(clazzname, ClassConfigurator.class);
    }

    /**
     * Returns the magic number for the class.
     *
     * @param clazz a class object that we want the magic number for
     * @return the magic number for a class, -1 if no mapping is available
     */
    public static short getMagicNumber(Class<?> clazz) {
        Short i=classMap.get(clazz);
        if(i == null)
            return -1;
        else
            return i;
    }


    public static short getProtocolId(Class<?> protocol) {
        Short retval=protocol_ids.get(protocol);
        return retval != null? retval : 0;
    }


    public static Class<?> getProtocol(short id) {
        return protocol_names.get(id);
    }



    public static String printClassMap() {
        StringBuilder sb=new StringBuilder();

        for(Iterator<Map.Entry<Class<?>,Short>> it=classMap.entrySet().iterator(); it.hasNext();) {
            Map.Entry<Class<?>,Short> entry=it.next();
            sb.append(entry.getKey()).append(": ").append(entry.getValue()).append('\n');
        }
        return sb.toString();
    }


    protected static void init() throws Exception {
        String magic_number_file=null, protocol_id_file=null;
        try { // PropertyPermission not granted if running in an untrusted environment with JNLP
            magic_number_file=Util.getProperty(new String[]{Global.MAGIC_NUMBER_FILE, "org.jgroups.conf.magicNumberFile"},
                                               null, null,  MAGIC_NUMBER_FILE);
            protocol_id_file=Util.getProperty(new String[]{Global.PROTOCOL_ID_FILE, "org.jgroups.conf.protocolIDFile"},
                                              null, null, PROTOCOL_ID_FILE);
        }
        catch (SecurityException ignored) {
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
            Class<?> clazz=Util.loadClass(tuple.getVal2(), ClassConfigurator.class);
            if(magicMap[m] != null)
                alreadyInMagicMap(m, clazz.getName());

            if(Constructable.class.isAssignableFrom(clazz)) {
                Constructable<?> obj=(Constructable<?>)clazz.getDeclaredConstructor().newInstance();
                magicMap[m]=obj.create();
            }
            else {
                Supplier<? extends Object> supplier=(Supplier<Object>)() -> {
                    try {
                        return clazz.getDeclaredConstructor().newInstance();
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

            Class<?> clazz=Util.loadClass(tuple.getVal2(), ClassConfigurator.class);
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
        InputStream stream=Util.getResourceAsStream(name, ClassConfigurator.class);
        // try to load the map from file even if it is not a Resource in the class path
        if(stream == null)
            stream=new FileInputStream(name);
        return parse(stream);
    }

    protected static List<Triple<Short,String,Boolean>> parse(InputStream in) throws Exception {
        List<String> lines=parseLines(in);
        List<Triple<Short,String,Boolean>> retval=new ArrayList<>();

        for(String line: lines) {
            short   id;
            String  name;
            boolean external=false;

            int index=line.indexOf(ID);
            if(index == -1)
                notFound(ID, line);
            index+=ID.length()+1;
            id=Short.parseShort(parseNextString(line, index));

            index=line.indexOf(NAME);
            if(index == -1)
                notFound(NAME, line);
            index+=NAME.length()+1;
            name=parseNextString(line, index);

            index=line.indexOf(EXTERNAL);
            if(index >= 0) {
                index+=EXTERNAL.length()+1;
                external=Boolean.parseBoolean(parseNextString(line, index));
            }
            Triple<Short,String,Boolean> t=new Triple<>(id, name, external);
            retval.add(t);
        }

        return retval;
    }

    protected static void notFound(String id, String line) {
        throw new IllegalStateException(String.format("%s not found in line %s", id, line));
    }

    protected static String parseNextString(String line, int index) {
        int start=line.indexOf("\"", index);
        if(index == -1)
            notFound("\"", line.substring(index));
        int end=line.indexOf("\"", start+1);
        if(index == -1)
            notFound("\"", line.substring(index+1));
        return line.substring(start+1, end);
    }

    protected static List<String> parseLines(InputStream in) throws IOException {
        List<String> lines=new LinkedList<>();
        for(;;) {
            String token=Util.readToken(in);
            if(token == null)
                break;
            token=token.trim();
            if(token.startsWith(CLASS)) {
                String line=token + " " + readTillMatchingParens(in);
                lines.add(line);
            }
        }
        return lines;
    }

    protected static String readTillMatchingParens(InputStream in) throws IOException {
        StringBuilder sb=new StringBuilder();
        for(;;) {
            int ch=in.read();
            if(ch == -1)
                break;
            sb.append((char)ch);
            if(ch == '>')
                break;
        }
        return sb.toString();
    }

}
