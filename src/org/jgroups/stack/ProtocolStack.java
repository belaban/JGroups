package org.jgroups.stack;

import org.jgroups.*;
import org.jgroups.annotations.Property;
import org.jgroups.conf.ClassConfigurator;
import org.jgroups.conf.PropertyConverter;
import org.jgroups.conf.ProtocolConfiguration;
import org.jgroups.jmx.AdditionalJmxObjects;
import org.jgroups.jmx.ResourceDMBean;
import org.jgroups.logging.Log;
import org.jgroups.protocols.TP;
import org.jgroups.util.MessageBatch;
import org.jgroups.util.StackType;
import org.jgroups.util.Util;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.net.Inet6Address;
import java.net.InetAddress;
import java.util.*;
import java.util.Map.Entry;
import java.util.regex.Pattern;


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
    public enum Position {ABOVE, BELOW};
    protected static final String max_list_print_size="max-list-print-size";

    protected Protocol            top_prot;
    protected Protocol            bottom_prot;
    protected JChannel            channel;
    protected volatile boolean    stopped=true;


    public ProtocolStack topProtocol(Protocol top)       {this.top_prot=top; return this;}
    public ProtocolStack bottomProtocol(Protocol bottom) {this.bottom_prot=bottom; return this;}

    protected final DiagnosticsHandler.ProbeHandler props_handler=new DiagnosticsHandler.ProbeHandler() {

        public Map<String, String> handleProbe(String... keys) {
            for(String key: keys) {
                if(Objects.equals(key, "props")) {
                    String tmp=printProtocolSpec(true);
                    HashMap<String, String> map=new HashMap<>(1);
                    map.put("props", tmp);
                    return map;
                }
                if(key.startsWith(max_list_print_size)) {
                    int index=key.indexOf('=');
                    if(index >= 0) {
                        Util.MAX_LIST_PRINT_SIZE=Integer.parseInt(key.substring(index+1));
                    }
                    HashMap<String, String> map=new HashMap<>(1);
                    map.put(max_list_print_size, String.valueOf(Util.MAX_LIST_PRINT_SIZE));
                    return map;
                }
                if(key.equals("pp") || key.startsWith("print-protocols")) {
                    List<Protocol> prots=getProtocols();
                    Collections.reverse(prots);
                    StringBuilder sb=new StringBuilder();
                    for(Protocol prot: prots)
                        sb.append(prot.getName()).append("\n");
                    HashMap<String, String> map=new HashMap<>(1);
                    map.put("protocols", sb.toString());
                    return map;
                }
                if(key.startsWith("rp") || key.startsWith("remove-protocol")) {
                    int len=key.startsWith("rp")? "rp".length() : "remove-protocol".length();
                    key=key.substring(len);
                    int index=key.indexOf('=');
                    if(index != -1) {
                        String rest=key.substring(index +1);
                        if(rest != null && !rest.isEmpty()) {
                            List<String> prots=Util.parseCommaDelimitedStrings(rest);
                            if(!prots.isEmpty()) {
                                for(String p: prots) {
                                    List<Protocol> protocols=findProtocols(p);
                                    if(protocols != null && !protocols.isEmpty()) {
                                        for(Protocol prot_to_remove: protocols) {
                                            try {
                                                Protocol removed=removeProtocol(prot_to_remove);
                                                if(removed != null)
                                                    log.debug("removed protocol %s from stack", prot_to_remove.getName());
                                            }
                                            catch(Exception e) {
                                                log.error(Util.getMessage("FailedRemovingProtocol") + rest, e);
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
                if(key.startsWith("insert-protocol")) {
                    key=key.substring("insert-protocol".length()+1);
                    int index=key.indexOf('=');
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
                    }
                    catch(Exception e) {
                        log.error(Util.getMessage("FailedCreatingAnInstanceOf") + prot_name, e);
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
                        log.error(Util.getMessage("NeighborProtocol") + " " + neighbor_prot + " not found in stack");
                        break;
                    }
                    Position position=tmp.equalsIgnoreCase("above")? Position.ABOVE : Position.BELOW;
                    try {
                        insertProtocol(prot, position, neighbor.getClass());
                    }
                    catch(Exception e) {
                        log.error(Util.getMessage("FailedInsertingProtocol") + prot_name + " " + tmp + " " + neighbor_prot, e);
                    }

                    try {
                        callAfterCreationHook(prot, afterCreationHook());
                        prot.init();
                        prot.start();
                    }
                    catch(Exception e) {
                        log.error(Util.getMessage("FailedCreatingAnInstanceOf") + prot_name, e);
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
        tmp.getDeclaredConstructor().newInstance();
    }


    /** Used for programmatic creation of ProtocolStack */
    public ProtocolStack() {
    }

    public JChannel getChannel() {return channel;}

    public ProtocolStack setChannel(JChannel ch) {
        this.channel=ch; return this;
    }


    /** Returns all protocols in a list, from top to bottom. <em>These are not copies of protocols,
     so modifications will affect the actual instances !</em> */
    public List<Protocol> getProtocols() {
        List<Protocol> v=new ArrayList<>(15);
        Protocol p=top_prot;
        while(p != null) {
            v.add(p);
            p=p.getDownProtocol();
        }
        return v;
    }


    public List<Protocol> copyProtocols(ProtocolStack targetStack) throws Exception {
        List<Protocol> list=getProtocols();
        List<Protocol> retval=new ArrayList<>(list.size());
        for(Protocol prot: list) {
            Protocol new_prot=prot.getClass().getDeclaredConstructor().newInstance();
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
                    if(method.isAnnotationPresent(Property.class) && Configurator.isSetPropertyMethod(method, clazz)) {
                        Property annotation=method.getAnnotation(Property.class);
                        List<String> possible_names=new LinkedList<>();
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


    public Map<String,Map<String,Object>> dumpStats() {
        Map<String,Map<String,Object>> retval=new HashMap<>(); // no need to be sorted, we need order of protocols as in the config!
        for(Protocol p=top_prot; p != null; p=p.getDownProtocol()) {
            String prot_name=p.getName();
            if(prot_name == null)
                continue;
            Map<String,Object> tmp=new TreeMap<>();
            dumpStats(p, tmp, log);
            if(!tmp.isEmpty())
                retval.put(prot_name, tmp);
        }
        return retval;
    }


    public Map<String,Map<String,Object>> dumpStats(final String protocol_name, List<String> attrs) {
        List<Protocol> prots=null;

        try {
            Class<? extends Protocol> cl=Util.loadProtocolClass(protocol_name, this.getClass());
            Protocol prot=findProtocol(cl);
            if(prot != null)
                prots=Collections.singletonList(prot);
        }
        catch(Exception e) {
        }

        if(prots ==null)
            prots=findProtocols(protocol_name);
        if(prots == null || prots.isEmpty())
            return null;
        Map<String,Map<String,Object>> retval=new HashMap<>();
        for(Protocol prot: prots) {
            Map<String,Object> tmp=new TreeMap<>();
            dumpStats(prot, tmp, log);
            if(attrs != null && !attrs.isEmpty()) {
                // weed out attrs not in list
                for(Iterator<String> it=tmp.keySet().iterator(); it.hasNext(); ) {
                    String attrname=it.next();
                    boolean found=false;
                    for(String attr : attrs) {
                        if(attrname.startsWith(attr)) {
                            found=true;
                            break; // found
                        }
                    }
                    if(!found)
                        it.remove();
                }
            }
            String pname=prot.getName();
            if(retval.containsKey(pname))
                retval.put(pname + "-" + prot.getId(), tmp);
            else
                retval.put(pname, tmp);
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
            if(first_colon_printed)
                sb.append(":");
            else
                first_colon_printed=true;

            sb.append(prot_name);
            if(include_properties) {
                Map<String,String> tmp=getProps(prot);
                if(!tmp.isEmpty()) {
                    boolean printed=false;
                    sb.append("(");
                    for(Map.Entry<String,String> entry: tmp.entrySet()) {
                        if(printed)
                            sb.append(";");
                        else
                            printed=true;
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
        while(prot != null && !Objects.equals(prot.getClass(), ProtocolStack.class)) {
            String prot_name=prot.getClass().getName();
            if(prot_name != null) {
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

    protected static void dumpStats(Object obj, Map<String,Object> map, Log log) {
        ResourceDMBean.dumpStats(obj, map, log); // dumps attrs and operations into tmp
        if(obj instanceof AdditionalJmxObjects) {
            Object[] objs=((AdditionalJmxObjects)obj).getJmxObjects();
            if(objs != null && objs.length > 0) {
                for(Object o: objs) {
                    if(o != null)
                        ResourceDMBean.dumpStats(o, map, log);
                }
            }
        }
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
        Map<String,String> retval=new HashMap<>();

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
                            conv=(PropertyConverter)conv_class.getDeclaredConstructor().newInstance();
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
                if(method.isAnnotationPresent(Property.class) && Configurator.isSetPropertyMethod(method, clazz)) {
                    annotation=method.getAnnotation(Property.class);
                    List<String> possible_names=new LinkedList<>();
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
                                conv=(PropertyConverter)conv_class.getDeclaredConstructor().newInstance();
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
        if(prots != null)
            prots.forEach(this::addProtocol);
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
    public void insertProtocol(Protocol prot, Position position, String neighbor_prot) throws Exception {
        if(neighbor_prot == null) throw new IllegalArgumentException("neighbor_prot is null");
        Protocol neighbor=findProtocol(neighbor_prot);
        if(neighbor == null)
            throw new IllegalArgumentException("protocol " + neighbor_prot + " not found in " + printProtocolSpec(false));

        if(position == Position.BELOW && neighbor instanceof TP)
            throw new IllegalArgumentException("Cannot insert protocol " + prot.getName() + " below transport protocol");

        insertProtocolInStack(prot, neighbor,  position);
    }


    public void insertProtocolInStack(Protocol prot, Protocol neighbor, Position position) {
     // connect to the protocol layer below and above
        if(position == Position.BELOW) {
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

    public void insertProtocol(Protocol prot, Position position, Class<? extends Protocol> neighbor_prot) throws Exception {
        if(neighbor_prot == null) throw new IllegalArgumentException("neighbor_prot is null");
        Protocol neighbor=findProtocol(neighbor_prot);
        if(neighbor == null)
            throw new IllegalArgumentException("protocol \"" + neighbor_prot + "\" not found in " + stack.printProtocolSpec(false));

        if(position == Position.BELOW && neighbor instanceof TP)
            throw new IllegalArgumentException("\"" + prot + "\" cannot be inserted below the transport (" + neighbor + ")");
        insertProtocolInStack(prot, neighbor,  position);
    }


    @SafeVarargs
    public final void insertProtocol(Protocol prot, Position position, Class<? extends Protocol>... neighbor_prots) throws Exception {
        if(neighbor_prots == null) throw new IllegalArgumentException("neighbor_prots is null");
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
     * Removes a protocol from the stack. Stops the protocol and readjusts the linked lists of protocols.
     * @param prot_name The name of the protocol. Since all protocol names in a stack have to be unique
     *                  (otherwise the stack won't be created), the name refers to just 1 protocol.
     * @exception Exception Thrown if the protocol cannot be stopped correctly.
     */
    public <T extends Protocol> T removeProtocol(String prot_name) {
        if(prot_name == null) return null;
        return removeProtocol(findProtocol(prot_name));
    }

    public ProtocolStack removeProtocols(String ... protocols) {
        for(String protocol: protocols)
            removeProtocol(protocol);
        return this;
    }

    @SafeVarargs
    public final ProtocolStack removeProtocols(Class<? extends Protocol>... protocols) {
        for(Class<? extends Protocol> protocol: protocols)
            removeProtocol(protocol);
        return this;
    }


    @SafeVarargs
    public final <T extends Protocol> T removeProtocol(Class<? extends Protocol>... protocols) {
        for(Class<? extends Protocol> cl: protocols) {
            T tmp=removeProtocol(cl);
            if(tmp != null)
                return tmp;
        }
        return null;
    }


    public <T extends Protocol> T removeProtocol(Class<? extends Protocol> prot) {
        if(prot == null)
            return null;
        return removeProtocol(findProtocol(prot));
    }

    public <T extends Protocol> T removeProtocol(T prot) {
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
            log.error(Util.getMessage("FailedStopping") + prot.getName() + ": " + t);
        }
        try {
            prot.destroy();
        }
        catch(Throwable t) {
            log.error(Util.getMessage("FailedDestroying") + prot.getName() + ": " + t);
        }
        return prot;
    }


    /** Returns a given protocol or null if not found */
    public <T extends Protocol> T findProtocol(String name) {
        T tmp=(T)top_prot;
        String   prot_name;
        while(tmp != null) {
            prot_name=tmp.getName();
            if(Objects.equals(prot_name, name))
                return tmp;
            tmp=tmp.getDownProtocol();
        }
        return null;
    }

    public <T extends Protocol> List<T> findProtocols(String regexp) {
        List<T> retval=null;
        Pattern pattern=Pattern.compile(regexp);

        for(T prot=(T)top_prot; prot != null; prot=prot.getDownProtocol()) {
            String prot_name=prot.getName();
            if(pattern.matcher(prot_name).matches()) {
                if(retval == null)
                    retval=new ArrayList<>();
                retval.add(prot);
            }
        }
        return retval;
    }

    public <T extends Protocol> T getBottomProtocol() {
        T curr_prot=(T)this;
        while(curr_prot != null && curr_prot.getDownProtocol() !=null)
            curr_prot=curr_prot.getDownProtocol();
        return curr_prot;
    }

    public Protocol getTopProtocol() {
        return top_prot;
    }

    public <T extends Protocol> T findProtocol(Class<? extends Protocol> clazz) {
        Protocol tmp=top_prot;
        while(tmp != null) {
            Class<?> protClass=tmp.getClass();
            if(clazz.isAssignableFrom(protClass))
                return (T)tmp;
            tmp=tmp.getDownProtocol();
        }
        return null;
    }

    /**
     * Finds the first protocol of a list and returns it. Returns null if no protocol can be found
     * @param classes A list of protocol classes to find
     * @return Protocol The protocol found
     */
    @SafeVarargs
    public final <T extends Protocol> T findProtocol(Class<? extends Protocol>... classes) {
        for(Class<? extends Protocol> clazz: classes) {
            T prot=findProtocol(clazz);
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
        callAfterCreationHook(new_prot, afterCreationHook());
        new_prot.init();
    }



    protected Protocol createProtocol(String classname) throws Exception {
        Class<? extends Protocol> clazz=Util.loadProtocolClass(classname, getClass());
        Protocol retval=clazz.getDeclaredConstructor().newInstance();
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

        StackType ip_version=Util.getIpStackType();
        InetAddress resolved_addr=Configurator.getValueFromProtocol(bottom_prot, "bind_addr");
        if(resolved_addr != null)
            ip_version=resolved_addr instanceof Inet6Address? StackType.IPv6 : StackType.IPv4;
        else if(ip_version == StackType.Dual)
            ip_version=StackType.IPv4; // prefer IPv4 addresses
        Configurator.setDefaultAddressValues(protocols, ip_version);
        initProtocolStack();
    }

    /** Calls @link{{@link Protocol#init()}} in all protocols, from bottom to top */
    public void initProtocolStack() throws Exception {
        List<Protocol> protocols=getProtocols();
        Collections.reverse(protocols);
        try {
            for(Protocol prot : protocols) {
                if(prot.getProtocolStack() == null)
                    prot.setProtocolStack(this);
                callAfterCreationHook(prot, prot.afterCreationHook());
                prot.init();
            }
        }
        catch(Exception ex) {
            this.destroy();
            throw ex;
        }
    }



    public void destroy() {
        if(top_prot != null)
            getProtocols().forEach(Protocol::destroy);
    }



    /**
     * Start all protocols. The {@link Protocol#start()} method is called in each protocol,
     * <em>from bottom to top</em>. Each protocol can perform some initialization, e.g. create a multicast socket
     */
    public void startStack() throws Exception {
        if(!stopped) return;
        List<Protocol> protocols=getProtocols();
        Collections.reverse(protocols);
        for(Protocol prot: protocols)
            prot.start();
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
        getProtocols().forEach(Protocol::stop);
        TP transport=getTransport();
        transport.unregisterProbeHandler(props_handler);
        stopped=true;
    }



    /*--------------------------- Protocol functionality ------------------------------*/
    public String getName()  {return "ProtocolStack";}

    public Object up(Event evt) {
        return channel.up(evt);
    }
    public Object up(Message msg) {return channel.up(msg);}

    public void up(MessageBatch batch) {
        channel.up(batch);
    }

    public Object down(Event evt) {
        if(top_prot != null)
            return top_prot.down(evt);
        return null;
    }

    public Object down(Message msg) {
        if(top_prot != null)
            return top_prot.down(msg);
        return null;
    }



    protected static void callAfterCreationHook(Protocol prot, String classname) throws Exception {
        if(classname == null || prot == null)
            return;
        Class<ProtocolHook> clazz=(Class<ProtocolHook>)Util.loadClass(classname, prot.getClass());
        ProtocolHook hook=clazz.getDeclaredConstructor().newInstance();
        hook.afterCreation(prot);
    }

}
