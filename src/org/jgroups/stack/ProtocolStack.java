package org.jgroups.stack;

import org.jgroups.Address;
import org.jgroups.Event;
import org.jgroups.Global;
import org.jgroups.JChannel;
import org.jgroups.annotations.Property;
import org.jgroups.conf.ClassConfigurator;
import org.jgroups.conf.PropertyConverter;
import org.jgroups.conf.ProtocolConfiguration;
import org.jgroups.protocols.TP;
import org.jgroups.util.AsciiString;
import org.jgroups.util.MessageBatch;
import org.jgroups.util.Tuple;
import org.jgroups.util.Util;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.*;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;


/**
 * A ProtocolStack manages a number of protocols layered above each other. It
 * creates all protocol classes, initializes them and, when ready, starts all of
 * them, beginning with the bottom most protocol. It also dispatches messages
 * received from the stack to registered objects (e.g. channel, GMP) and sends
 * messages sent by those objects down the stack.
 * <p>
 * The ProtocolStack makes use of the Configurator to setup and initialize
 * stacks, and to destroy them again when not needed anymore
 *
 * @author Bela Ban
 */
public class ProtocolStack extends Protocol {
    public static final int       ABOVE = 1; // used by insertProtocol()
    public static final int       BELOW = 2; // used by insertProtocol()

    protected static final String max_list_print_size="max-list-print-size";
    /**
     * Holds the shared transports, keyed by 'TP.singleton_name'. The values are the transport and the use count for
     * init() (decremented by destroy()) and start() (decremented by stop()
     */
    protected static final ConcurrentMap<String,Tuple<TP,RefCounter>> singleton_transports=new ConcurrentHashMap<String,Tuple<TP,RefCounter>>();

    protected Protocol                      top_prot;
    protected Protocol                      bottom_prot;
    protected JChannel                      channel;
    protected volatile boolean              stopped=true;


    public ProtocolStack topProtocol(Protocol top)       {this.top_prot=top; return this;}
    public ProtocolStack bottomProtocol(Protocol bottom) {this.bottom_prot=bottom; return this;}

    protected final DiagnosticsHandler.ProbeHandler props_handler=new DiagnosticsHandler.ProbeHandler() {

        public Map<String, String> handleProbe(String... keys) {
            for(String key: keys) {
                if(key.equals("props")) {
                    String tmp=printProtocolSpec(true);
                    HashMap<String, String> map=new HashMap<String, String>(1);
                    map.put("props", tmp);
                    return map;
                }
                if(key.startsWith(max_list_print_size)) {
                    int index=key.indexOf("=");
                    if(index >= 0) {
                        Util.MAX_LIST_PRINT_SIZE=Integer.valueOf(key.substring(index+1));
                    }
                    HashMap<String, String> map=new HashMap<String, String>(1);
                    map.put(max_list_print_size, String.valueOf(Util.MAX_LIST_PRINT_SIZE));
                    return map;
                }
                if(key.startsWith("print-protocols")) {
                    List<Protocol> prots=getProtocols();
                    Collections.reverse(prots);
                    StringBuilder sb=new StringBuilder();
                    for(Protocol prot: prots)
                        sb.append(prot.getName()).append("\n");
                    HashMap<String, String> map=new HashMap<String, String>(1);
                    map.put("protocols", sb.toString());
                    return map;
                }
                if(key.startsWith("remove-protocol")) {
                    key=key.substring("remove-protocol".length());
                    int index=key.indexOf("=");
                    if(index != -1) {
                        String prot_name=key.substring(index +1);
                        if(prot_name != null && !prot_name.isEmpty()) {
                            try {
                                Protocol removed=removeProtocol(prot_name);
                                if(removed != null)
                                    log.debug("removed protocol " + prot_name + " from stack");
                            }
                            catch(Exception e) {
                                log.error("failed removing protocol " + prot_name, e);
                            }
                        }
                    }
                }
                if(key.startsWith("insert-protocol")) {
                    key=key.substring("insert-protocol".length()+1);
                    int index=key.indexOf("=");
                    if(index == -1) break;

                    // 1. name of the protocol to be inserted
                    String prot_name=key.substring(0, index).trim();
                    if(findProtocol(prot_name) != null) {
                        log.error("Protocol %s cannot be inserted as it is already present", prot_name);
                        break;
                    }
                    Protocol prot=null;
                    try {
                        prot=createProtocol(prot_name);
                        //prot.init();
                        //prot.start();
                    }
                    catch(Exception e) {
                        log.error("failed creating an instance of " + prot_name, e);
                        break;
                    }

                    key=key.substring(index+1);

                    index=key.indexOf('=');
                    if(index == -1) {
                        log.error("= missing in insert-protocol command");
                        break;
                    }

                    // 2. "above" or "below"
                    String tmp=key.substring(0, index);
                    if(!tmp.equalsIgnoreCase("above") && !tmp.equalsIgnoreCase("below")) {
                        log.error("Missing \"above\" or \"below\" in insert-protocol command");
                        break;
                    }

                    key=key.substring(index+1);
                    String neighbor_prot=key.trim();
                    Protocol neighbor=findProtocol(neighbor_prot);
                    if(neighbor == null) {
                        log.error("Neighbor protocol " + neighbor_prot + " not found in stack");
                        break;
                    }
                    int position=tmp.equalsIgnoreCase("above")? ABOVE : BELOW;
                    try {
                        insertProtocol(prot, position, neighbor.getClass());
                    }
                    catch(Exception e) {
                        log.error("failed inserting protocol " + prot_name + " " + tmp + " " + neighbor_prot, e);
                    }

                    try {
                        prot.init();
                        prot.start();
                    }
                    catch(Exception e) {
                        log.error("failed creating an instance of " + prot_name, e);
                    }
                }
            }
            return null;
        }

        public String[] supportedKeys() {
            return new String[]{"props", max_list_print_size + "[=number]", "print-protocols", "\nremove-protocol=<name>",
              "\ninsert-protocol=<name>=above | below=<name>"};
        }
    };


    public ProtocolStack(JChannel channel) throws Exception {
        this.channel=channel;
        Class<?> tmp=ClassConfigurator.class; // load this class, trigger init()
        tmp.newInstance();
    }


    /** Used for programmatic creation of ProtocolStack */
    public ProtocolStack() {
    }

    public JChannel getChannel() {return channel;}

    public void setChannel(JChannel ch) {
        this.channel=ch;
    }


    /** Returns all protocols in a list, from top to bottom. <em>These are not copies of protocols,
     so modifications will affect the actual instances !</em> */
    public List<Protocol> getProtocols() {
        List<Protocol> v=new ArrayList<Protocol>(15);
        Protocol p=top_prot;
        while(p != null) {
            v.add(p);
            p=p.getDownProtocol();
        }
        return v;
    }


    public List<Protocol> copyProtocols(ProtocolStack targetStack) throws IllegalAccessException, InstantiationException {
        List<Protocol> list=getProtocols();
        List<Protocol> retval=new ArrayList<Protocol>(list.size());
        for(Protocol prot: list) {
            Protocol new_prot=prot.getClass().newInstance();
            new_prot.setProtocolStack(targetStack);
            retval.add(new_prot);

            for(Class<?> clazz=prot.getClass(); clazz != null; clazz=clazz.getSuperclass()) {

                // copy all fields marked with @Property
                Field[] fields=clazz.getDeclaredFields();
                for(Field field: fields) {
                    if(field.isAnnotationPresent(Property.class)) {
                        Object value=Util.getField(field, prot);
                        Util.setField(field, new_prot, value);
                    }
                }

                // copy all setters marked with @Property
                Method[] methods=clazz.getDeclaredMethods();
                for(Method method: methods) {
                    String methodName=method.getName();
                    if(method.isAnnotationPresent(Property.class) && Configurator.isSetPropertyMethod(method)) {
                        Property annotation=method.getAnnotation(Property.class);
                        List<String> possible_names=new LinkedList<String>();
                        if(annotation.name() != null)
                            possible_names.add(annotation.name());
                        possible_names.add(Util.methodNameToAttributeName(methodName));
                        Field field=Util.findField(prot, possible_names);
                        if(field != null) {
                            Object value=Util.getField(field, prot);
                            Util.setField(field, new_prot, value);
                        }
                    }
                }
            }
        }
        return retval;
    }




    /** Returns the bottom most protocol */
    public TP getTransport() {
        return (TP)getBottomProtocol();
    }

    public static ConcurrentMap<String, Tuple<TP,RefCounter>> getSingletonTransports() {
        return singleton_transports;
    }

    /**
     *
     * @return Map<String,Map<key,val>>
     */
    public Map<String,Object> dumpStats() {
        Protocol p;
        Map<String,Object> retval=new HashMap<String,Object>(), tmp;
        String prot_name;

        p=top_prot;
        while(p != null) {
            prot_name=p.getName();
            tmp=p.dumpStats();
            if(prot_name != null && tmp != null)
                retval.put(prot_name, tmp);
            p=p.getDownProtocol();
        }
        return retval;
    }

    public Map<String,Object> dumpStats(String protocol_name) {
        return dumpStats(protocol_name, null);
    }


    public Map<String,Object> dumpStats(String protocol_name, List<String> attrs) {
        Protocol prot=findProtocol(protocol_name);
        if(prot == null)
            return null;

        Map<String,Object> retval=new HashMap<String,Object>(), tmp;
        tmp=prot.dumpStats();
        if(tmp != null) {
            if(attrs != null && !attrs.isEmpty()) {
                // weed out attrs not in list
                for(Iterator<String> it=tmp.keySet().iterator(); it.hasNext();) {
                    String attrname=it.next();
                    boolean found=false;
                    for(String attr: attrs) {
                        if(attrname.startsWith(attr)) {
                            found=true;
                            break; // found
                        }
                    }
                    if(!found)
                        it.remove();
                }
            }
            retval.put(protocol_name, tmp);
        }

        return retval;
    }

    /**
     * Prints the names of the protocols, from the bottom to top. If include_properties is true,
     * the properties for each protocol will also be printed.
     */
    public String printProtocolSpec(boolean include_properties) {
        StringBuilder sb=new StringBuilder();
        List<Protocol> protocols=getProtocols();

        if(protocols == null || protocols.isEmpty()) return null;
        boolean first_colon_printed=false;

        Collections.reverse(protocols);
        for(Protocol prot: protocols) {
            String prot_name=prot.getClass().getName();
            int index=prot_name.indexOf(Global.PREFIX);
            if(index >= 0)
                prot_name=prot_name.substring(Global.PREFIX.length());
            if(first_colon_printed) {
                sb.append(":");
            }
            else {
                first_colon_printed=true;
            }

            sb.append(prot_name);
            if(include_properties) {
                Map<String,String> tmp=getProps(prot);
                if(!tmp.isEmpty()) {
                    boolean printed=false;
                    sb.append("(");
                    for(Map.Entry<String,String> entry: tmp.entrySet()) {
                        if(printed) {
                            sb.append(";");
                        }
                        else {
                            printed=true;
                        }
                        sb.append(entry.getKey()).append("=").append(entry.getValue());
                    }
                    sb.append(")\n");
                }
            }
        }
        return sb.toString();
    }

    public String printProtocolSpecAsXML() {
        StringBuilder sb=new StringBuilder();
        Protocol prot=bottom_prot;
        int len, max_len=30;

        sb.append("<config>\n");
        while(prot != null) {
            String prot_name=prot.getName();
            if(prot_name != null) {
                if("ProtocolStack".equals(prot_name))
                    break;
                sb.append("  <").append(prot_name).append(" ");
                Map<String,String> tmpProps=getProps(prot);
                if(tmpProps != null) {
                    len=prot_name.length();
                    String s;
                    for(Iterator<Entry<String,String>> it=tmpProps.entrySet().iterator();it.hasNext();) {
                        Entry<String,String> entry=it.next();
                        s=entry.getKey() + "=\"" + entry.getValue() + "\" ";
                        if(len + s.length() > max_len) {
                            sb.append("\n       ");
                            len=8;
                        }
                        sb.append(s);
                        len+=s.length();
                    }
                }
                sb.append("/>\n");
                prot=prot.getUpProtocol();
            }
        }
        sb.append("</config>");

        return sb.toString();
    }

    public String printProtocolSpecAsPlainString() {
        return printProtocolSpecAsPlainString(false);
    }

    private String printProtocolSpecAsPlainString(boolean print_props) {
        StringBuilder sb=new StringBuilder();
        List<Protocol> protocols=getProtocols();

        if(protocols == null) return null;

        Collections.reverse(protocols);
        for(Protocol prot: protocols) {
            sb.append(prot.getClass().getName()).append("\n");
            if(print_props) {
                Map<String,String> tmp=getProps(prot);
                for(Map.Entry<String,String> entry: tmp.entrySet()) {
                    sb.append("    ").append(entry.getKey()).append("=").append(entry.getValue()).append("\n");
                }
            }
        }
        return sb.toString();
    }

    private static Map<String,String> getProps(Protocol prot) {
        Map<String,String> retval=new HashMap<String,String>();

        for(Class<?> clazz=prot.getClass(); clazz != null; clazz=clazz.getSuperclass()) {

            // copy all fields marked with @Property
            Field[] fields=clazz.getDeclaredFields();
            Property annotation;
            for(Field field: fields) {
                if(field.isAnnotationPresent(Property.class)) {
                    Object value=Util.getField(field, prot);
                    if(value != null) {
                        annotation=field.getAnnotation(Property.class);
                        Class<?> conv_class=annotation.converter();
                        PropertyConverter conv=null;
                        try {
                            conv=(PropertyConverter)conv_class.newInstance();
                        }
                        catch(Exception e) {
                        }
                        String tmp=conv != null? conv.toString(value) : value.toString();
                        retval.put(field.getName(), tmp);
                    }
                }
            }

            // copy all setters marked with @Property
            Method[] methods=clazz.getDeclaredMethods();
            for(Method method: methods) {
                String methodName=method.getName();
                if(method.isAnnotationPresent(Property.class) && Configurator.isSetPropertyMethod(method)) {
                    annotation=method.getAnnotation(Property.class);
                    List<String> possible_names=new LinkedList<String>();
                    if(annotation.name() != null)
                        possible_names.add(annotation.name());
                    possible_names.add(Util.methodNameToAttributeName(methodName));
                    Field field=Util.findField(prot, possible_names);
                    if(field != null) {
                        Object value=Util.getField(field, prot);
                        if(value != null) {
                            Class<?> conv_class=annotation.converter();
                            PropertyConverter conv=null;
                            try {
                                conv=(PropertyConverter)conv_class.newInstance();
                            }
                            catch(Exception e) {
                            }
                            String tmp=conv != null? conv.toString(value) : value.toString();
                            retval.put(field.getName(), tmp);
                        }
                    }
                }
            }
        }
        return retval;
    }


    public void setup(List<ProtocolConfiguration> configs) throws Exception {
        if(top_prot == null) {
            top_prot=new Configurator(this).setupProtocolStack(configs);
            top_prot.setUpProtocol(this);
            this.setDownProtocol(top_prot);
            bottom_prot=getBottomProtocol();
            initProtocolStack();
        }
    }



    public void setup(ProtocolStack stack) throws Exception {
        if(top_prot == null) {
            top_prot=new Configurator(this).setupProtocolStack(stack);
            top_prot.setUpProtocol(this);
            this.setDownProtocol(top_prot);
            bottom_prot=getBottomProtocol();
            initProtocolStack();
        }
    }




    /**
     * Adds a protocol at the tail of the protocol list
     * @param prot
     * @return
     * @since 2.11
     */
    public ProtocolStack addProtocol(Protocol prot) {
        if(prot == null)
            return this;
        prot.setProtocolStack(this);
        prot.setUpProtocol(this);
        if(bottom_prot == null) {
            top_prot=bottom_prot=prot;
            return this;
        }

        prot.setDownProtocol(top_prot);
        prot.getDownProtocol().setUpProtocol(prot);
        top_prot=prot;
        return this;
    }

    /**
     * Adds a list of protocols
     * @param prots
     * @return
     * @since 2.11
     */
    public ProtocolStack addProtocols(Protocol ... prots) {
        if(prots != null) {
            for(Protocol prot: prots)
                addProtocol(prot);
        }
        return this;
    }

    /**
     * Adds a list of protocols
     * @param prots
     * @return
     * @since 2.1
     */
    public ProtocolStack addProtocols(List<Protocol> prots) {
        if(prots != null) {
            for(Protocol prot: prots)
                addProtocol(prot);
        }
        return this;
    }


    /**
     * Inserts an already created (and initialized) protocol into the protocol list. Sets the links
     * to the protocols above and below correctly and adjusts the linked list of protocols accordingly.
     * Note that this method may change the value of top_prot or bottom_prot.
     * @param prot The protocol to be inserted. Before insertion, a sanity check will ensure that none
     *             of the existing protocols have the same name as the new protocol.
     * @param position Where to place the protocol with respect to the neighbor_prot (ABOVE, BELOW)
     * @param neighbor_prot The name of the neighbor protocol. An exception will be thrown if this name
     *                      is not found
     * @exception Exception Will be thrown when the new protocol cannot be created, or inserted.
     */
    public void insertProtocol(Protocol prot, int position, String neighbor_prot) throws Exception {
        if(neighbor_prot == null) throw new IllegalArgumentException("neighbor_prot is null");
        if(position != ProtocolStack.ABOVE && position != ProtocolStack.BELOW)
            throw new IllegalArgumentException("position has to be ABOVE or BELOW");

        Protocol neighbor=findProtocol(neighbor_prot);
        if(neighbor == null)
            throw new IllegalArgumentException("protocol " + neighbor_prot + " not found in " + printProtocolSpec(false));

        if(position == ProtocolStack.BELOW && neighbor instanceof TP)
            throw new IllegalArgumentException("Cannot insert protocol " + prot.getName() + " below transport protocol");

        insertProtocolInStack(prot, neighbor,  position);
    }


    public void insertProtocolInStack(Protocol prot, Protocol neighbor, int position) {
     // connect to the protocol layer below and above
        if(position == ProtocolStack.BELOW) {
            prot.setUpProtocol(neighbor);
            Protocol below=neighbor.getDownProtocol();
            prot.setDownProtocol(below);
            if(below != null)
                below.setUpProtocol(prot);
            neighbor.setDownProtocol(prot);
        }
        else { // ABOVE is default
            Protocol above=neighbor.getUpProtocol();
            checkAndSwitchTop(neighbor, prot);
            prot.setUpProtocol(above);
            if(above != null)
                above.setDownProtocol(prot);
            prot.setDownProtocol(neighbor);
            neighbor.setUpProtocol(prot);
        }
    }

    private void checkAndSwitchTop(Protocol oldTop, Protocol newTop){
        if(oldTop == top_prot) {
            top_prot = newTop;
            top_prot.setUpProtocol(this);
        }
    }

    public void insertProtocol(Protocol prot, int position, Class<? extends Protocol> neighbor_prot) throws Exception {
        if(neighbor_prot == null) throw new IllegalArgumentException("neighbor_prot is null");
        if(position != ProtocolStack.ABOVE && position != ProtocolStack.BELOW)
            throw new IllegalArgumentException("position has to be ABOVE or BELOW");

        Protocol neighbor=findProtocol(neighbor_prot);
        if(neighbor == null)
            throw new IllegalArgumentException("protocol \"" + neighbor_prot + "\" not found in " + stack.printProtocolSpec(false));

        if(position == ProtocolStack.BELOW && neighbor instanceof TP)
            throw new IllegalArgumentException("protocol \"" + prot + "\" cannot be inserted below the transport protocol (" +
                                                 neighbor + ")");

        insertProtocolInStack(prot, neighbor,  position);
    }


    public void insertProtocol(Protocol prot, int position, Class<? extends Protocol> ... neighbor_prots) throws Exception {
        if(neighbor_prots == null) throw new IllegalArgumentException("neighbor_prots is null");
        if(position != ProtocolStack.ABOVE && position != ProtocolStack.BELOW)
            throw new IllegalArgumentException("position has to be ABOVE or BELOW");

        Protocol neighbor=findProtocol(neighbor_prots);
        if(neighbor == null)
            throw new IllegalArgumentException("protocol \"" + Arrays.toString(neighbor_prots) + "\" not found in " + stack.printProtocolSpec(false));
        insertProtocolInStack(prot, neighbor,  position);
    }


    public void insertProtocolAtTop(Protocol prot) {
        if(prot == null)
            throw new IllegalArgumentException("prot needs to be non-null");

        // check if prot already exists (we cannot have more than 1 protocol of a given class)
        Class<? extends Protocol> clazz=prot.getClass();
        Protocol existing_instance=findProtocol(clazz);
        if(existing_instance != null)
            return;

        top_prot.up_prot=prot;
        prot.down_prot=top_prot;
        prot.up_prot=this;
        top_prot=prot;
        log.debug("inserted " + prot + " at the top of the stack");
    }


    /**
     * Removes a protocol from the stack. Stops the protocol and readjusts the linked lists of
     * protocols.
     * @param prot_name The name of the protocol. Since all protocol names in a stack have to be unique
     *                  (otherwise the stack won't be created), the name refers to just 1 protocol.
     * @exception Exception Thrown if the protocol cannot be stopped correctly.
     */
    public Protocol removeProtocol(String prot_name) {
        if(prot_name == null) return null;
        return removeProtocol(findProtocol(prot_name));
    }

    public void removeProtocols(String ... protocols) {
        for(String protocol: protocols)
            removeProtocol(protocol);
    }


    public Protocol removeProtocol(Class ... protocols) {
        Protocol retval=null;
        if(protocols != null)
            for(Class cl: protocols) {
                Protocol tmp=removeProtocol(cl);
                if(tmp != null)
                    retval=tmp;
            }

        return retval;
    }


    public Protocol removeProtocol(Class prot) {
        if(prot == null)
            return null;
        return removeProtocol(findProtocol(prot));
    }

    public Protocol removeProtocol(Protocol prot) {
        if(prot == null) return null;
        Protocol above=prot.getUpProtocol(), below=prot.getDownProtocol();
        checkAndSwitchTop(prot, below);
        if(above != null)
            above.setDownProtocol(below);
        if(below != null)
            below.setUpProtocol(above);
        prot.setUpProtocol(null);
        prot.setDownProtocol(null);
        try {
            prot.stop();
        }
        catch(Throwable t) {
            log.error("failed stopping " + prot.getName() + ": " + t);
        }
        try {
            prot.destroy();
        }
        catch(Throwable t) {
            log.error("failed destroying " + prot.getName() + ": " + t);
        }
        return prot;
    }


    /** Returns a given protocol or null if not found */
    public Protocol findProtocol(String name) {
        Protocol tmp=top_prot;
        String   prot_name;
        while(tmp != null) {
            prot_name=tmp.getName();
            if(prot_name != null && prot_name.equals(name))
                return tmp;
            tmp=tmp.getDownProtocol();
        }
        return null;
    }

    public Protocol getBottomProtocol() {
        Protocol curr_prot=this;
        while(curr_prot != null && curr_prot.getDownProtocol() !=null) {
            curr_prot=curr_prot.getDownProtocol();
        }
        return curr_prot;
    }

    public Protocol getTopProtocol() {
        return top_prot;
    }

    public Protocol findProtocol(Class<?> clazz) {
        Protocol tmp=top_prot;
        while(tmp != null) {
            Class<?> protClass=tmp.getClass();
            if(clazz.isAssignableFrom(protClass)){
                return tmp;
            }
            tmp=tmp.getDownProtocol();
        }
        return null;
    }

    /**
     * Finds the first protocol of a list and returns it. Returns null if no protocol can be found
     * @param classes A list of protocol classes to find
     * @return Protocol The protocol found
     */
    public Protocol findProtocol(Class<?> ... classes) {
        for(Class<?> clazz: classes) {
            Protocol prot=findProtocol(clazz);
            if(prot != null)
                return prot;
        }
        return null;
    }

    /**
     * Replaces one protocol instance with another. Should be done before the stack is connected
     * @param existing_prot
     * @param new_prot
     */
    public void replaceProtocol(Protocol existing_prot, Protocol new_prot) throws Exception {
        Protocol up_neighbor=existing_prot.getUpProtocol(), down_neighbor=existing_prot.getDownProtocol();

        new_prot.setUpProtocol(existing_prot.getUpProtocol());
        new_prot.setDownProtocol(existing_prot.getDownProtocol());
        up_neighbor.setDownProtocol(new_prot);
        if(down_neighbor != null)
            down_neighbor.setUpProtocol(new_prot);

        existing_prot.setDownProtocol(null);
        existing_prot.setUpProtocol(null);
        existing_prot.stop();
        existing_prot.destroy();

        if(new_prot.getUpProtocol() == this)
            top_prot=new_prot;

        new_prot.init();
    }



    protected Protocol createProtocol(String classname) throws Exception {
        String defaultProtocolName=ProtocolConfiguration.protocol_prefix + '.' + classname;
        Class<?> clazz=null;

        try {
            clazz=Util.loadClass(defaultProtocolName, getClass());
        }
        catch(ClassNotFoundException e) {
        }

        if(clazz == null) {
            try {
                clazz=Util.loadClass(classname, getClass());
            }
            catch(ClassNotFoundException e) {
            }
            if(clazz == null) {
                throw new Exception("unable to load class for protocol " + classname + " (either as an absolute - "
                                      + classname + " - or relative - " + defaultProtocolName + " - package name)");
            }
        }

        Protocol retval=(Protocol)clazz.newInstance();
        if(retval == null)
            throw new Exception("creation of instance for protocol " + classname + "failed");
        retval.setProtocolStack(this);
        return retval;
    }


    public void init() throws Exception {
        List<Protocol> protocols=getProtocols();
        Collections.reverse(protocols);
        top_prot=Configurator.connectProtocols(protocols);
        top_prot.setUpProtocol(this);
        this.setDownProtocol(top_prot);
        bottom_prot=getBottomProtocol();
        Configurator.setDefaultValues(protocols);
        initProtocolStack();
    }

    public void initProtocolStack() throws Exception {
        List<Protocol> protocols = getProtocols();
        Collections.reverse(protocols);
        for(Protocol prot: protocols) {
            if(prot.getProtocolStack() == null)
                prot.setProtocolStack(this);
            if(prot instanceof TP) {
                TP transport=(TP)prot;
                if(transport.isSingleton()) {
                    String singleton_name=transport.getSingletonName();
                    synchronized(singleton_transports) {
                        Tuple<TP,RefCounter> val=singleton_transports.get(singleton_name);
                        if(val == null)
                            singleton_transports.put(singleton_name, new Tuple<TP,RefCounter>(transport,new RefCounter((short)1, (short)0)));
                        else {
                            RefCounter counter=val.getVal2();
                            short num_inits=counter.incrementInitCount();
                            if(num_inits >= 1)
                                continue;
                        }
                        prot.init(); // if shared TP, call init() with lock : https://issues.jboss.org/browse/JGRP-1887
                        continue;
                    }
                }
            }
            prot.init();
        }
    }



    public void destroy() {
        if(top_prot != null) {
            for(Protocol prot: getProtocols()) {
                if(prot instanceof TP) {
                    TP transport=(TP)prot;
                    if(transport.isSingleton()) {
                        String singleton_name=transport.getSingletonName();
                        synchronized(singleton_transports) {
                            Tuple<TP, ProtocolStack.RefCounter> val=singleton_transports.get(singleton_name);
                            if(val != null) {
                                ProtocolStack.RefCounter counter=val.getVal2();
                                short num_inits=counter.decrementInitCount();
                                if(num_inits >= 1) {
                                    continue;
                                }
                                else
                                    singleton_transports.remove(singleton_name);
                            }
                        }
                    }
                }
                prot.destroy();
            }

            /*
             *Do not null top_prot reference since we need recreation of channel properties (JChannel#getProperties)
             *during channel recreation, especially if those properties were modified after channel was created.
             *We modify channel properties after channel creation in some tests for example
             *
             */
            //top_prot=null;
        }
    }



    /**
     * Start all layers. The {@link Protocol#start()} method is called in each protocol,
     * <em>from top to bottom</em>.
     * Each layer can perform some initialization, e.g. create a multicast socket
     */
    public void startStack(String cluster, Address local_addr) throws Exception {
        if(stopped == false) return;
        final AsciiString cluster_name=new AsciiString(cluster);
        Protocol above_prot=null;
        for(final Protocol prot: getProtocols()) {
            if(prot instanceof TP) {
                String singleton_name=((TP)prot).getSingletonName();
                TP transport=(TP)prot;
                if(transport.isSingleton() && cluster_name != null) {
                    final Map<AsciiString, Protocol> up_prots=transport.getUpProtocols();

                    synchronized(singleton_transports) {
                        synchronized(up_prots) {
                            Set<AsciiString> keys=up_prots.keySet();
                            if(keys.contains(cluster_name))
                                throw new IllegalStateException("cluster '" + cluster_name + "' is already connected to singleton " +
                                        "transport: " + keys);

                            for(Iterator<Map.Entry<AsciiString,Protocol>> it=up_prots.entrySet().iterator(); it.hasNext();) {
                                Map.Entry<AsciiString,Protocol> entry=it.next();
                                Protocol tmp=entry.getValue();
                                if(tmp == above_prot) {
                                    it.remove();
                                }
                            }

                            if(above_prot != null) {
                                TP.ProtocolAdapter ad=new TP.ProtocolAdapter(new AsciiString(cluster_name), local_addr, prot.getId(),
                                                                             above_prot, prot,
                                                                             transport.getThreadNamingPattern());
                                ad.setProtocolStack(above_prot.getProtocolStack());
                                above_prot.setDownProtocol(ad);
                                up_prots.put(cluster_name, ad);
                            }
                        }
                        Tuple<TP, ProtocolStack.RefCounter> val=singleton_transports.get(singleton_name);
                        if(val != null) {
                            ProtocolStack.RefCounter counter=val.getVal2();
                            short num_starts=counter.incrementStartCount();
                            if(num_starts >= 1) {
                                continue;
                            }
                            else {
                                try {
                                    prot.start();
                                }
                                catch(Exception ex) {
                                    counter.decrementStartCount();
                                    up_prots.remove(cluster_name);
                                    throw ex;
                                }
                                above_prot=prot;
                                continue;
                            }
                        }
                    }
                }
            }
            prot.start();
            above_prot=prot;
        }

        TP transport=getTransport();
        transport.registerProbeHandler(props_handler);
        stopped=false;
    }




    /**
     * Iterates through all the protocols <em>from top to bottom</em> and does the following:
     * <ol>
     * <li>Waits until all messages in the down queue have been flushed (ie., size is 0)
     * <li>Calls stop() on the protocol
     * </ol>
     */
    public void stopStack(String cluster) {
        if(stopped) return;
        final AsciiString cluster_name=new AsciiString(cluster);
        for(final Protocol prot: getProtocols()) {
            if(prot instanceof TP) {
                TP transport=(TP)prot;
                if(transport.isSingleton()) {
                    String singleton_name=transport.getSingletonName();
                    final Map<AsciiString,Protocol> up_prots=transport.getUpProtocols();
                    synchronized(up_prots) {
                        Protocol adapter=up_prots.remove(cluster_name);
                        if(adapter != null) {
                            Protocol neighbor_above=adapter.getUpProtocol();
                            if(neighbor_above != null)
                                neighbor_above.setDownProtocol(transport);
                        }
                    }
                    synchronized(singleton_transports) {
                        Tuple<TP, ProtocolStack.RefCounter> val=singleton_transports.get(singleton_name);
                        if(val != null) {
                            ProtocolStack.RefCounter counter=val.getVal2();
                            short num_starts=counter.decrementStartCount();
                            if(num_starts > 0) {
                                continue; // don't call TP.stop() if we still have references to the transport
                            }
                            //else
                                // singletons.remove(singleton_name); // do the removal in destroyProtocolStack()
                        }
                    }
                }
            }
            prot.stop();
        }

        TP transport=getTransport();
        transport.unregisterProbeHandler(props_handler);
        stopped=true;
    }



    /*--------------------------- Protocol functionality ------------------------------*/
    public String getName()  {return "ProtocolStack";}

    public Object up(Event evt) {
        return channel.up(evt);
    }

    public void up(MessageBatch batch) {
        channel.up(batch);
    }

    public Object down(Event evt) {
        if(top_prot != null)
            return top_prot.down(evt);
        return null;
    }




    /**
     * Keeps track of the number os times init()/destroy() and start()/stop have been called. The variables
     * init_count and start_count are incremented or decremented accoordingly. Note that this class is not synchronized
     */
    public static class RefCounter {
        private short init_count=0;
        private short start_count=0;

        public RefCounter(short init_count, short start_count) {
            this.init_count=init_count;
            this.start_count=start_count;
        }

        public short getInitCount() {
            return init_count;
        }

        public short getStartCount() {
            return start_count;
        }

        /**
         * Increments init_count, returns the old value before incr
         * @return
         */
        public short incrementInitCount(){
            return init_count++;
        }

        public short decrementInitCount() {
            init_count=(short)Math.max(init_count -1, 0);
            return init_count;
        }

        public short decrementStartCount() {
            start_count=(short)Math.max(start_count -1, 0);
            return start_count;
        }

        public short incrementStartCount() {
            return start_count++;
        }

        public String toString() {
            return "init_count=" + init_count + ", start_count=" + start_count;
        }
    }


}
