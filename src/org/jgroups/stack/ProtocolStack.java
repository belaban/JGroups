
package org.jgroups.stack;

import org.jgroups.*;
import org.jgroups.annotations.Property;
import org.jgroups.conf.ClassConfigurator;
import org.jgroups.conf.PropertyConverters;
import org.jgroups.conf.PropertyConverter;
import org.jgroups.protocols.TP;
import org.jgroups.util.*;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.locks.ReentrantLock;
import java.lang.reflect.Field;
import java.lang.reflect.Method;


/**
 * A ProtocolStack manages a number of protocols layered above each other. It creates all
 * protocol classes, initializes them and, when ready, starts all of them, beginning with the
 * bottom most protocol. It also dispatches messages received from the stack to registered
 * objects (e.g. channel, GMP) and sends messages sent by those objects down the stack.<p>
 * The ProtocolStack makes use of the Configurator to setup and initialize stacks, and to
 * destroy them again when not needed anymore
 * @author Bela Ban
 * @version $Id: ProtocolStack.java,v 1.73 2008/05/28 11:31:02 belaban Exp $
 */
public class ProtocolStack extends Protocol implements Transport {
    public static final int ABOVE = 1; // used by insertProtocol()
    public static final int BELOW = 2; // used by insertProtocol()

    private Protocol top_prot = null;
    private Protocol bottom_prot = null;
    private String   setup_string;
    private JChannel channel = null;
    private volatile boolean stopped=true;


    /** Locks acquired by protocol below, need to get released on down().
     * See http://jira.jboss.com/jira/browse/JGRP-535 for details */
    private final Map<Thread, ReentrantLock> locks=new ConcurrentHashMap<Thread,ReentrantLock>();


    /** Holds the shared transports, keyed by 'TP.singleton_name'.
     * The values are the transport and the use count for start() (decremented by stop() */
    private static final ConcurrentMap<String,Tuple<TP,Short>> singleton_transports=new ConcurrentHashMap<String,Tuple<TP,Short>>();



    public ProtocolStack(JChannel channel, String setup_string) throws ChannelException {
        this.setup_string=setup_string;
        this.channel=channel;
        // ClassConfigurator.getInstance(true); // will create the singleton

        Class tmp=ClassConfigurator.class; // load this class, trigger init()
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

    public Map<Thread,ReentrantLock> getLocks() {
        return locks;
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
        Protocol         p;
        Vector<Protocol> v=new Vector<Protocol>();

        p=top_prot;
        while(p != null) {
            v.addElement(p);
            p=p.getDownProtocol();
        }
        return v;
    }


    public static List<Protocol> copyProtocols(List<Protocol> list) throws IllegalAccessException, InstantiationException {
        if(list == null) return null;
        List<Protocol> retval=new ArrayList<Protocol>(list.size());
        for(Protocol prot: list) {
            Protocol new_prot=prot.getClass().newInstance();
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
                        possible_names.add(Configurator.renameFromJavaCodingConvention(methodName.substring(3)));
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
        for(Class clazz=target.getClass(); clazz != null; clazz=clazz.getSuperclass()) {
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
        Vector<Protocol> prots=getProtocols();
        return (TP)(!prots.isEmpty()? prots.lastElement() : null);
    }

    public static ConcurrentMap<String, Tuple<TP, Short>> getSingletonTransports() {
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
        Protocol     prot=top_prot;
        Properties   tmpProps;
        String       name;
        Map.Entry    entry;

        while(prot != null) {
            name=prot.getName();
            if(name != null) {
                if("ProtocolStack".equals(name))
                    break;
                sb.append(name);
                if(include_properties) {
                    tmpProps=prot.getProperties();
                    if(tmpProps != null) {
                        sb.append('\n');
                        for(Iterator it=tmpProps.entrySet().iterator(); it.hasNext();) {
                            entry=(Map.Entry)it.next();
                            sb.append(entry).append("\n");
                        }
                    }
                }
                sb.append('\n');

                prot=prot.getDownProtocol();
            }
        }

        return sb.toString();
    }

    public String printProtocolSpecAsXML() {
        StringBuilder sb=new StringBuilder();
        Protocol     prot=bottom_prot;
        Properties   tmpProps;
        String       name;
        Map.Entry    entry;
        int len, max_len=30;

        sb.append("<config>\n");
        while(prot != null) {
            name=prot.getName();
            if(name != null) {
                if("ProtocolStack".equals(name))
                    break;
                sb.append("  <").append(name).append(" ");
                tmpProps=prot.getProperties();
                if(tmpProps != null) {
                    len=name.length();
                    String s;
                    for(Iterator it=tmpProps.entrySet().iterator(); it.hasNext();) {
                        entry=(Map.Entry)it.next();
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
        StringBuilder sb=new StringBuilder();
        Protocol      prot=bottom_prot;
        Properties    tmpProps;
        boolean       initialized=false;
        Class         clazz;

        while(prot != null) {
            clazz=prot.getClass();
            if(clazz.equals(ProtocolStack.class))
                break;
            if(initialized)
                sb.append(":");
            else
                initialized=true;
            sb.append(clazz.getName());
            tmpProps=prot.getProperties();
            if(tmpProps != null && !tmpProps.isEmpty()) {
                sb.append("(");
                boolean first=true;
                for(Map.Entry<Object,Object> entry: tmpProps.entrySet()) {
                    if(first)
                        first=false;
                    else
                        sb.append(";");
                    sb.append(entry.getKey() + "=" + entry.getValue());
                }
                sb.append(")");
            }
            sb.append("\n");
            prot=prot.getUpProtocol();
        }
        return sb.toString();
    }


    public void setup() throws Exception {
        if(top_prot == null) {
            top_prot=Configurator.setupProtocolStack(setup_string, this);
            top_prot.setUpProtocol(this);
            bottom_prot=Configurator.getBottommostProtocol(top_prot);
            List<Protocol> protocols=getProtocols();
            Configurator.initProtocolStack(protocols);         // calls init() on each protocol, from bottom to top
        }
    }




    /**
     * Creates a new protocol given the protocol specification.
     * @param prot_spec The specification of the protocol. Same convention as for specifying a protocol stack.
     *                  An exception will be thrown if the class cannot be created. Example:
     *                  <pre>"VERIFY_SUSPECT(timeout=1500)"</pre> Note that no colons (:) have to be
     *                  specified
     * @return Protocol The newly created protocol
     * @exception Exception Will be thrown when the new protocol cannot be created
     */
    public Protocol createProtocol(String prot_spec) throws Exception {
        return Configurator.createProtocol(prot_spec, this);
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
        Configurator.insertProtocol(prot, position, neighbor_prot, this);
    }





    /**
     * Removes a protocol from the stack. Stops the protocol and readjusts the linked lists of
     * protocols.
     * @param prot_name The name of the protocol. Since all protocol names in a stack have to be unique
     *                  (otherwise the stack won't be created), the name refers to just 1 protocol.
     * @exception Exception Thrown if the protocol cannot be stopped correctly.
     */
    public Protocol removeProtocol(String prot_name) throws Exception {
        return Configurator.removeProtocol(top_prot, prot_name);
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



    public void destroy() {
        if(top_prot != null) {
            Configurator.destroyProtocolStack(getProtocols());           // destroys msg queues and threads
            top_prot=null;
        }        
    }



    /**
     * Start all layers. The {@link Protocol#start()} method is called in each protocol,
     * <em>from top to bottom</em>.
     * Each layer can perform some initialization, e.g. create a multicast socket
     */
    public void startStack(String cluster_name) throws Exception {
        if(stopped == false) return;
        Configurator.startProtocolStack(getProtocols(), cluster_name, singleton_transports);
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
        Configurator.stopProtocolStack(getProtocols(), cluster_name, singleton_transports);
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
        ReentrantLock lock=locks.remove(Thread.currentThread());
        if(lock != null && lock.isHeldByCurrentThread()) {
            lock.unlock();
            if(log.isTraceEnabled())
                log.trace("released lock held by " + Thread.currentThread());
        }
        if(top_prot != null)
            return top_prot.down(evt);
        return null;
    }
    
    

}

