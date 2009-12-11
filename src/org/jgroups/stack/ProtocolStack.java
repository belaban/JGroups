package org.jgroups.stack;

import org.jgroups.*;
import org.jgroups.annotations.Property;
import org.jgroups.conf.ClassConfigurator;
import org.jgroups.conf.PropertyConverter;
import org.jgroups.protocols.TP;
import org.jgroups.util.ThreadFactory;
import org.jgroups.util.TimeScheduler;
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
 * @version $Id: ProtocolStack.java,v 1.97 2009/12/11 13:19:49 belaban Exp $
 */
public class ProtocolStack extends Protocol implements Transport {
    public static final int ABOVE = 1; // used by insertProtocol()
    public static final int BELOW = 2; // used by insertProtocol()

    /**
     * Holds the shared transports, keyed by 'TP.singleton_name'. The values are
     * the transport and the use count for init() (decremented by destroy()) and
     * start() (decremented by stop()
     */
    private static final ConcurrentMap<String,Tuple<TP,RefCounter>> singleton_transports=new ConcurrentHashMap<String,Tuple<TP,RefCounter>>();



    private Protocol top_prot = null;
    private Protocol bottom_prot = null;
    private String   setup_string;
    private JChannel channel = null;
    private volatile boolean stopped=true;


    private final TP.ProbeHandler props_handler=new TP.ProbeHandler() {

        public Map<String, String> handleProbe(String... keys) {
            for(String key: keys) {
                if(key.equals("props")) {
                    String tmp=printProtocolSpec(true);
                    HashMap<String, String> map=new HashMap<String, String>(1);
                    map.put("props", tmp);
                    return map;
                }
            }
            return null;
        }

        public String[] supportedKeys() {
            return new String[]{"props"};
        }
    };


    public ProtocolStack(JChannel channel, String setup_string) throws ChannelException {
        this.setup_string=setup_string;
        this.channel=channel;

        Class<?> tmp=ClassConfigurator.class; // load this class, trigger init()
        try {
            tmp.newInstance();
        }
        catch(Exception e) {
            throw new ChannelException("failed initializing ClassConfigurator", e);
        }
    }



    /** Only used by Simulator; don't use */
    public ProtocolStack() throws ChannelException {
        this(null,null);
    }


    public String getSetupString() {
        return setup_string;
    }

    /**
     * @deprecated Use {@link org.jgroups.stack.Protocol#getThreadFactory()}  instead
     * @return
     */
    public ThreadFactory getThreadFactory() {

        getTransport().getThreadFactory();
        TP transport=getTransport();
        return transport != null? transport.getThreadFactory() : null;
    }

    @Deprecated
    public static ThreadFactory getTimerThreadFactory() {
        throw new UnsupportedOperationException("get the timer thread factory directly from the transport");
    }

    /**
     * @deprecated Use {@link org.jgroups.stack.Protocol#getThreadFactory()} instead
     * @param f
     */
    public void setThreadFactory(ThreadFactory f) {
    }

    /**
     * @deprecated Use {@link TP#setTimerThreadFactory(org.jgroups.util.ThreadFactory)} instead
     * @param f
     */
    public static void setTimerThreadFactory(ThreadFactory f) {
    }


    public Channel getChannel() {
        return channel;
    }


    /**
     * @deprecated Use {@link org.jgroups.protocols.TP#getTimer()} to fetch the timer and call getCorePoolSize() directly
     * @return
     */
    public int getTimerThreads() {
        TP transport=getTransport();
        TimeScheduler timer;
        if(transport != null) {
            timer=transport.getTimer();
            if(timer != null)
                return timer.getCorePoolSize();
        }
        return -1;
    }

    /** Returns all protocols in a list, from top to bottom. <em>These are not copies of protocols,
     so modifications will affect the actual instances !</em> */
    public Vector<Protocol> getProtocols() {
        Vector<Protocol> v=new Vector<Protocol>();
        Protocol p=top_prot;
        while(p != null) {
            v.addElement(p);
            p=p.getDownProtocol();
        }
        return v;
    }


    public Vector<Protocol> copyProtocols(ProtocolStack targetStack) throws IllegalAccessException, InstantiationException {
        Vector<Protocol> list=getProtocols();
        Vector<Protocol> retval=new Vector<Protocol>(list.size());
        for(Protocol prot: list) {
            Protocol new_prot=prot.getClass().newInstance();
            new_prot.setProtocolStack(targetStack);
            retval.add(new_prot);

            for(Class<?> clazz=prot.getClass(); clazz != null; clazz=clazz.getSuperclass()) {

                // copy all fields marked with @Property
                Field[] fields=clazz.getDeclaredFields();
                for(Field field: fields) {
                    if(field.isAnnotationPresent(Property.class)) {
                        Object value=Configurator.getField(field, prot);
                        Configurator.setField(field, new_prot, value);
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
                        possible_names.add(methodName.substring(3));
                        possible_names.add(Util.methodNameToAttributeName(methodName));
                        Field field=findField(prot, possible_names);
                        if(field != null) {
                            Object value=Configurator.getField(field, prot);
                            Configurator.setField(field, new_prot, value);
                        }
                    }
                }
            }
        }
        return retval;
    }

    private static Field findField(Object target, List<String> possible_names) {
        if(target == null)
            return null;
        for(Class<?> clazz=target.getClass(); clazz != null; clazz=clazz.getSuperclass()) {
            for(String name: possible_names) {
                try {
                    Field field=clazz.getDeclaredField(name);
                    if(field != null)
                        return field;
                }
                catch(NoSuchFieldException e) {
                }
            }
        }

        return null;
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
        Protocol p;
        Map<String,Object> retval=new HashMap<String,Object>(), tmp;
        String prot_name;

        p=top_prot;
        while(p != null) {
            prot_name=p.getName();
            if(prot_name.equals(protocol_name)) {
                tmp=p.dumpStats();
                if(tmp != null)
                    retval.put(prot_name, tmp);
            }
            p=p.getDownProtocol();
        }
        return retval;
    }

    /**
     * @deprecated Use {@link org.jgroups.protocols.TP#getTimer()} instead to fetch the timer from the
     * transport and then invoke the method on it
     * @return
     */
    public String dumpTimerQueue() {
        TP transport=getTransport();
        TimeScheduler timer;
        if(transport != null) {
            timer=transport.getTimer();
            if(timer != null)
                return timer.dumpTaskQueue();
        }
        return "";
    }

    /**
     * Prints the names of the protocols, from the bottom to top. If include_properties is true,
     * the properties for each protocol will also be printed.
     */
    public String printProtocolSpec(boolean include_properties) {
        StringBuilder sb=new StringBuilder();
        Vector<Protocol> protocols=getProtocols();

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
            String name=prot.getName();
            if(name != null) {
                if("ProtocolStack".equals(name))
                    break;
                sb.append("  <").append(name).append(" ");
                Map<String,String> tmpProps=getProps(prot);
                if(tmpProps != null) {
                    len=name.length();
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
        Vector<Protocol> protocols=getProtocols();

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
                    Object value=Configurator.getField(field, prot);
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
                    possible_names.add(methodName.substring(3));
                    possible_names.add(Util.methodNameToAttributeName(methodName));
                    Field field=findField(prot, possible_names);
                    if(field != null) {
                        Object value=Configurator.getField(field, prot);
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


    public void setup() throws Exception {
        if(top_prot == null) {
            top_prot=getProtocolStackFactory().setupProtocolStack();
            top_prot.setUpProtocol(this);
            this.setDownProtocol(top_prot);
            bottom_prot=getBottomProtocol();
            initProtocolStack();
        }
    }


    protected ProtocolStackFactory getProtocolStackFactory() {
        return new Configurator(this);
    }



    public void setup(ProtocolStack stack) throws Exception {
        if(top_prot == null) {
            top_prot=getProtocolStackFactory().setupProtocolStack(stack);
            top_prot.setUpProtocol(this);
            this.setDownProtocol(top_prot);
            bottom_prot=getBottomProtocol();
            initProtocolStack();
        }
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
        if(neighbor_prot == null) throw new IllegalArgumentException("Configurator.insertProtocol(): neighbor_prot is null");
        if(position != ProtocolStack.ABOVE && position != ProtocolStack.BELOW)
            throw new IllegalArgumentException("position has to be ABOVE or BELOW");

        Protocol neighbor=findProtocol(neighbor_prot);
        if(neighbor == null)
            throw new IllegalArgumentException("protocol \"" + neighbor_prot + "\" not found in " + stack.printProtocolSpec(false));

        if(position == ProtocolStack.BELOW && neighbor instanceof TP)
            throw new IllegalArgumentException("Cannot insert protocol " + prot.getName() + " below transport protocol");

        insertProtocolInStack(prot, neighbor,  position);
    }


    private void insertProtocolInStack(Protocol prot, Protocol neighbor, int position) {
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
        if(oldTop == top_prot)
            top_prot = newTop;
    }

    public void insertProtocol(Protocol prot, int position, Class<? extends Protocol> neighbor_prot) throws Exception {
        if(neighbor_prot == null) throw new IllegalArgumentException("Configurator.insertProtocol(): neighbor_prot is null");
        if(position != ProtocolStack.ABOVE && position != ProtocolStack.BELOW)
            throw new IllegalArgumentException("position has to be ABOVE or BELOW");

        Protocol neighbor=findProtocol(neighbor_prot);
        if(neighbor == null)
            throw new IllegalArgumentException("protocol \"" + neighbor_prot + "\" not found in " + stack.printProtocolSpec(false));

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
        if(log.isDebugEnabled())
            log.debug("inserted " + prot + " on top of the stack");
    }


    /**
     * Removes a protocol from the stack. Stops the protocol and readjusts the linked lists of
     * protocols.
     * @param prot_name The name of the protocol. Since all protocol names in a stack have to be unique
     *                  (otherwise the stack won't be created), the name refers to just 1 protocol.
     * @exception Exception Thrown if the protocol cannot be stopped correctly.
     */
    public Protocol removeProtocol(String prot_name) throws Exception {
        if(prot_name == null) return null;
        Protocol prot=findProtocol(prot_name);
        if(prot == null) return null;
        Protocol above=prot.getUpProtocol(), below=prot.getDownProtocol();
        checkAndSwitchTop(prot, below);
        if(above != null)
            above.setDownProtocol(below);
        if(below != null)
            below.setUpProtocol(above);
        prot.setUpProtocol(null);
        prot.setDownProtocol(null);
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

    public void initProtocolStack() throws Exception {
        Vector <Protocol> protocols = getProtocols();
        Collections.reverse(protocols);
        for(Protocol prot: protocols) {
            if(prot instanceof TP) {
                TP transport=(TP)prot;
                if(transport.isSingleton()) {
                    String singleton_name=transport.getSingletonName();
                    synchronized(singleton_transports) {
                        Tuple<TP, ProtocolStack.RefCounter> val=singleton_transports.get(singleton_name);
                        if(val == null) {
                            singleton_transports.put(singleton_name, new Tuple<TP, ProtocolStack.RefCounter>(transport,new ProtocolStack.RefCounter((short)1, (short)0)));
                        }
                        else {
                            ProtocolStack.RefCounter counter=val.getVal2();
                            short num_inits=counter.incrementInitCount();
                            if(num_inits >= 1) {
                                continue;
                            }
                        }
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
    public void startStack(String cluster_name, Address local_addr) throws Exception {
        if(stopped == false) return;

        Protocol above_prot=null;
        for(final Protocol prot: getProtocols()) {
            if(prot instanceof TP) {
                String singleton_name=((TP)prot).getSingletonName();
                TP transport=(TP)prot;
                if(transport.isSingleton() && cluster_name != null) {
                    final Map<String, Protocol> up_prots=transport.getUpProtocols();

                    synchronized(singleton_transports) {
                        synchronized(up_prots) {
                            Set<String> keys=up_prots.keySet();
                            if(keys.contains(cluster_name))
                                throw new IllegalStateException("cluster '" + cluster_name + "' is already connected to singleton " +
                                        "transport: " + keys);

                            for(Iterator<Map.Entry<String,Protocol>> it=up_prots.entrySet().iterator(); it.hasNext();) {
                                Map.Entry<String,Protocol> entry=it.next();
                                Protocol tmp=entry.getValue();
                                if(tmp == above_prot) {
                                    it.remove();
                                }
                            }

                            if(above_prot != null) {
                                TP.ProtocolAdapter ad=new TP.ProtocolAdapter(cluster_name, local_addr, prot.getName(),
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
                                prot.start();
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
    public void stopStack(String cluster_name) {
        if(stopped) return;
        for(final Protocol prot: getProtocols()) {
            if(prot instanceof TP) {
                TP transport=(TP)prot;
                if(transport.isSingleton()) {
                    String singleton_name=transport.getSingletonName();
                    final Map<String,Protocol> up_prots=transport.getUpProtocols();
                    synchronized(up_prots) {
                        up_prots.remove(cluster_name);
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

    /**
     * Not needed anymore, just left in here for backwards compatibility with JBoss AS
     * @deprecated
     */
    public void flushEvents() {

    }



    /*--------------------------- Transport interface ------------------------------*/

    public void send(Message msg) throws Exception {
        down(new Event(Event.MSG, msg));
    }

    public Object receive(long timeout) throws Exception {
        throw new Exception("ProtocolStack.receive(): not implemented !");
    }
    /*------------------------- End of  Transport interface ---------------------------*/





    /*--------------------------- Protocol functionality ------------------------------*/
    public String getName()  {return "ProtocolStack";}




    public Object up(Event evt) {
        return channel.up(evt);
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

    public interface ProtocolStackFactory{
        public Protocol setupProtocolStack() throws Exception;
        public Protocol setupProtocolStack(ProtocolStack copySource) throws Exception;
    }

}
