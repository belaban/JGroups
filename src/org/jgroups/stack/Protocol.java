

package org.jgroups.stack;


import org.jgroups.Event;
import org.jgroups.Message;
import org.jgroups.annotations.ManagedAttribute;
import org.jgroups.annotations.ManagedOperation;
import org.jgroups.annotations.Property;
import org.jgroups.conf.ClassConfigurator;
import org.jgroups.jmx.ResourceDMBean;
import org.jgroups.logging.Log;
import org.jgroups.logging.LogFactory;
import org.jgroups.protocols.TP;
import org.jgroups.util.MessageBatch;
import org.jgroups.util.SocketFactory;
import org.jgroups.util.ThreadFactory;
import org.jgroups.util.Util;
import org.w3c.dom.Node;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.*;


/**
 * The Protocol class provides a set of common services for protocol layers. Each layer has to
 * be a subclass of Protocol and override a number of methods (typically just <code>up()</code>,
 * <code>down()</code> and <code>getName()</code>. Layers are stacked in a certain order to form
 * a protocol stack. <a href=org.jgroups.Event.html>Events</a> are passed from lower
 * layers to upper ones and vice versa. E.g. a Message received by the UDP layer at the bottom
 * will be passed to its higher layer as an Event. That layer will in turn pass the Event to
 * its layer and so on, until a layer handles the Message and sends a response or discards it,
 * the former resulting in another Event being passed down the stack.
 * <p/>
 * The important thing to bear in mind is that Events have to passed on between layers in FIFO
 * order which is guaranteed by the Protocol implementation and must be guranteed by subclasses
 * implementing their on Event queuing.<p>
 * <b>Note that each class implementing interface Protocol MUST provide an empty, public
 * constructor !</b>
 *
 * @author Bela Ban
 */
public abstract class Protocol {
    protected Protocol         up_prot, down_prot;
    protected ProtocolStack    stack;
    
    @Property(description="Determines whether to collect statistics (and expose them via JMX). Default is true",writable=true)
    protected boolean          stats=true;

    @Property(description="Enables ergonomics: dynamically find the best values for properties at runtime")
    protected boolean          ergonomics=true;

    /** The name of the protocol. Is by default set to the protocol's classname. This property should rarely need to
     * be set, e.g. only in cases where we want to create more than 1 protocol of the same class in the same stack */
    @Property(name="name",description="Give the protocol a different name if needed so we can have multiple " +
            "instances of it in the same stack (also change ID)",writable=false)
    protected String           name=getClass().getSimpleName();

    @Property(description="Give the protocol a different ID if needed so we can have multiple " +
            "instances of it in the same stack",writable=false)
    protected short            id=ClassConfigurator.getProtocolId(getClass());

    protected final Log        log=LogFactory.getLog(this.getClass());




    /**
     * Sets the level of a logger. This method is used to dynamically change the logging level of a
     * running system, e.g. via JMX. The appender of a level needs to exist.
     * @param level The new level. Valid values are "fatal", "error", "warn", "info", "debug", "trace"
     * (capitalization not relevant)
     */
    public void          setLevel(String level)            {log.setLevel(level);}
    @Property(name="level", description="logger level (see javadocs)")
    public String        getLevel()                        {return log.getLevel();}
    public Protocol      level(String level)               {this.log.setLevel(level); return this;}
    public boolean       isErgonomics()                    {return ergonomics;}
    public void          setErgonomics(boolean ergonomics) {this.ergonomics=ergonomics;}
    public ProtocolStack getProtocolStack()                {return stack;}
    public boolean       statsEnabled()                    {return stats;}
    public void          enableStats(boolean flag)         {stats=flag;}
    public String        getName()                         {return name;}
    public short         getId()                           {return id;}
    public void          setId(short id)                   {this.id=id;}
    public Protocol      getUpProtocol()                   {return up_prot;}
    public Protocol      getDownProtocol()                 {return down_prot;}
    public void          setUpProtocol(Protocol prot)      {this.up_prot=prot;}
    public void          setDownProtocol(Protocol prot)    {this.down_prot=prot;}
    public void          setProtocolStack(ProtocolStack s) {this.stack=s;}


    public Object getValue(String name) {
        if(name == null) return null;
        Field field=Util.getField(getClass(), name);
        if(field == null)
            throw new IllegalArgumentException("field \"" + name + "\n not found");
        return Util.getField(field, this);
    }

    public Protocol setValues(Map<String,Object> values) {
        if(values == null)
            return this;
        for(Map.Entry<String,Object> entry: values.entrySet()) {
            String attrname=entry.getKey();
            Object value=entry.getValue();
            Field field=Util.getField(getClass(), attrname);
            if(field != null) {
                Util.setField(field, this, value);
            }
        }
        return this;
    }


    public Protocol setValue(String name, Object value) {
        if(name == null || value == null)
            return this;
        Field field=Util.getField(getClass(), name);
        if(field == null)
            throw new IllegalArgumentException("field \"" + name + "\n not found");
        Property prop=field.getAnnotation(Property.class);
        if(prop != null) {
            String deprecated_msg=prop.deprecatedMessage();
            if(deprecated_msg != null && !deprecated_msg.isEmpty())
                log.warn("Field " + getName() + "." + name + " is deprecated: " + deprecated_msg);
        }
        Util.setField(field, this, value);
        return this;
    }



    /**
     * After configuring the protocol itself from the properties defined in the XML config, a protocol might have
     * additional objects which need to be configured. This callback allows a protocol developer to configure those
     * other objects. This call is guaranteed to be invoked <em>after</em> the protocol itself has
     * been configured. See AUTH for an example.
     * @return
     */
    protected List<Object> getConfigurableObjects() {return null;}

    /** Called by the XML parser when subelements are found in the configuration of a protocol. This allows
     * a protocol to define protocol-specific information and to parse it */
    public void parse(Node node) throws Exception {;}

    /** Returns the protocol IDs of all protocols above this one (excluding the current protocol) */
    public short[] getIdsAbove() {
        short[]     retval;
        List<Short> ids=new ArrayList<Short>();
        Protocol    current=up_prot;
        while(current != null) {
            ids.add(current.getId());
            current=current.up_prot;
        }
        retval=new short[ids.size()];
        for(int i=0; i < ids.size(); i++)
            retval[i]=ids.get(i);
        return retval;
    }


    protected TP getTransport() {
        Protocol retval=this;
        while(retval != null && retval.down_prot != null) {
            retval=retval.down_prot;
        }
        return (TP)retval;
    }

    /** Supposed to be overwritten by subclasses. Usually the transport returns a valid non-null thread factory, but
     * thread factories can also be created by individual protocols
     * @return
     */
    public ThreadFactory getThreadFactory() {
        return down_prot != null? down_prot.getThreadFactory(): null;
    }

    /**
     * Returns the SocketFactory associated with this protocol, if overridden in a subclass, or passes the call down
     * @return SocketFactory
     */
    public SocketFactory getSocketFactory() {
        return down_prot != null? down_prot.getSocketFactory() : null;
    }


    /**
     * Sets a SocketFactory. Socket factories are typically provided by the transport ({@link org.jgroups.protocols.TP})
     * or {@link org.jgroups.protocols.TP.ProtocolAdapter}
     * @param factory
     */
    public void setSocketFactory(SocketFactory factory) {
        if(down_prot != null)
            down_prot.setSocketFactory(factory);
    }


    @ManagedOperation(description="Resets all stats")
    public void resetStatistics() {resetStats();}


    public void resetStats() {
        ;
    }

    public String printStats() {
        return null;
    }

    public Map<String,Object> dumpStats() {
        HashMap<String,Object> map=new HashMap<String,Object>();
        for(Class<?> clazz=this.getClass();clazz != null;clazz=clazz.getSuperclass()) {
            Field[] fields=clazz.getDeclaredFields();
            for(Field field: fields) {
                if(field.isAnnotationPresent(ManagedAttribute.class) ||
                  (field.isAnnotationPresent(Property.class) && field.getAnnotation(Property.class).exposeAsManagedAttribute())) {

                    ManagedAttribute attr_annotation=field.getAnnotation(ManagedAttribute.class);
                    Property         prop=field.getAnnotation(Property.class);
                    String attr_name=attr_annotation != null? attr_annotation.name() : prop != null? prop.name() : null;
                    if(attr_name != null && !attr_name.trim().isEmpty())
                        attr_name=attr_name.trim();
                    else
                        attr_name=field.getName();

                    try {
                        field.setAccessible(true);
                        Object value=field.get(this);
                        map.put(attr_name, value != null? value.toString() : null);
                    }
                    catch(Exception e) {
                        log.warn("Could not retrieve value of attribute (field) " + attr_name, e);
                    }
                }
            }

            Method[] methods=this.getClass().getMethods();
            for(Method method: methods) {
                if(method.isAnnotationPresent(ManagedAttribute.class) ||
                        (method.isAnnotationPresent(Property.class) && method.getAnnotation(Property.class).exposeAsManagedAttribute())) {

                    ManagedAttribute attr_annotation=method.getAnnotation(ManagedAttribute.class);
                    Property         prop=method.getAnnotation(Property.class);
                    String method_name=attr_annotation != null? attr_annotation.name() : prop != null? prop.name() : null;
                    if(method_name != null && !method_name.trim().isEmpty())
                        method_name=method_name.trim();
                    else {
                        String field_name=Util.methodNameToAttributeName(method.getName());
                        method_name=Util.attributeNameToMethodName(field_name);
                    }

                    if(ResourceDMBean.isGetMethod(method)) {
                        try {
                            Object value=method.invoke(this);
                            String attributeName=Util.methodNameToAttributeName(method_name);
                            map.put(attributeName, value != null? value.toString() : null);
                        }
                        catch(Exception e) {
                            log.warn("Could not retrieve value of attribute (method) " + method_name,e);
                        }
                    }
                }
            }
        }
        return map;
    }
    


    
    /**
     * Called after instance has been created (null constructor) and before protocol is started.
     * Properties are already set. Other protocols are not yet connected and events cannot yet be sent.
     * @exception Exception Thrown if protocol cannot be initialized successfully. This will cause the
     *                      ProtocolStack to fail, so the channel constructor will throw an exception
     */
    public void init() throws Exception {
        short real_id=ClassConfigurator.getProtocolId(getClass());
        if(real_id > 0 && id != real_id)
            name=name+"-"+id;
    }

    /**
     * This method is called on a {@link org.jgroups.Channel#connect(String)}. Starts work.
     * Protocols are connected and queues are ready to receive events.
     * Will be called <em>from bottom to top</em>. This call will replace
     * the <b>START</b> and <b>START_OK</b> events.
     * @exception Exception Thrown if protocol cannot be started successfully. This will cause the ProtocolStack
     *                      to fail, so {@link org.jgroups.Channel#connect(String)} will throw an exception
     */
    public void start() throws Exception {
    }

    /**
     * This method is called on a {@link org.jgroups.Channel#disconnect()}. Stops work (e.g. by closing multicast socket).
     * Will be called <em>from top to bottom</em>. This means that at the time of the method invocation the
     * neighbor protocol below is still working. This method will replace the
     * <b>STOP</b>, <b>STOP_OK</b>, <b>CLEANUP</b> and <b>CLEANUP_OK</b> events. The ProtocolStack guarantees that
     * when this method is called all messages in the down queue will have been flushed
     */
    public void stop() {
    }


    /**
     * This method is called on a {@link org.jgroups.Channel#close()}.
     * Does some cleanup; after the call the VM will terminate
     */
    public void destroy() {
    }


    /** List of events that are required to be answered by some layer above */
    public List<Integer> requiredUpServices() {
        return null;
    }

    /** List of events that are required to be answered by some layer below */
    public List<Integer> requiredDownServices() {
        return null;
    }

    /** List of events that are provided to layers above (they will be handled when sent down from above) */
    public List<Integer> providedUpServices() {
        return null;
    }

    /** List of events that are provided to layers below (they will be handled when sent down below) */
    public List<Integer> providedDownServices() {
        return null;
    }

    /** Returns all services provided by protocols below the current protocol */
    public final List<Integer> getDownServices() {
        List<Integer> retval=new ArrayList<Integer>();
        Protocol prot=down_prot;
        while(prot != null) {
            List<Integer> tmp=prot.providedUpServices();
            if(tmp != null && !tmp.isEmpty())
                retval.addAll(tmp);
            prot=prot.down_prot;
        }
        return retval;
    }

    /** Returns all services provided by the protocols above the current protocol */
    public final List<Integer> getUpServices() {
        List<Integer> retval=new ArrayList<Integer>();
        Protocol prot=up_prot;
        while(prot != null) {
            List<Integer> tmp=prot.providedDownServices();
            if(tmp != null && !tmp.isEmpty())
                retval.addAll(tmp);
            prot=prot.up_prot;
        }
        return retval;
    }



    /**
     * An event was received from the layer below. Usually the current layer will want to examine
     * the event type and - depending on its type - perform some computation
     * (e.g. removing headers from a MSG event type, or updating the internal membership list
     * when receiving a VIEW_CHANGE event).
     * Finally the event is either a) discarded, or b) an event is sent down
     * the stack using <code>down_prot.down()</code> or c) the event (or another event) is sent up
     * the stack using <code>up_prot.up()</code>.
     */
    public Object up(Event evt) {
        return up_prot.up(evt);
    }

    /**
     * Called by the default implementation of {@link #up(org.jgroups.util.MessageBatch)} for each message to determine
     * if the message should be removed from the message batch (and handled by the current protocol) or not.
     * @param msg The message. Guaranteed to be non-null
     * @return True if the message should be handled by this protocol (will be removed from the batch), false if the
     * message should remain in the batch and be passed up.<p/>
     * The default implementation tries to find a header matching the current protocol's ID and returns true if there
     * is a match, or false otherwise
     */
    protected boolean accept(Message msg) {
        short tmp_id=getId();
        return tmp_id > 0 && msg.getHeader(tmp_id) != null;
    }


    /**
     * Sends up a multiple messages in a {@link MessageBatch}. The sender of the batch is always the same, and so is the
     * destination (null == multicast messages). Messages in a batch can be OOB messages, regular messages, or mixed
     * messages, although the transport itself will create initial MessageBatches that contain only either OOB or
     * regular messages.<p/>
     * The default processing below sends messages up the stack individually, based on a matching criteria
     * (calling {@link #accept(org.jgroups.Message)}), and - if true - calls {@link #up(org.jgroups.Event)}
     * for that message and removes the message. If the batch is not empty, it is passed up, or else it is dropped.<p/>
     * Subclasses should check if there are any messages destined for them (e.g. using
     * {@link MessageBatch#getMatchingMessages(short,boolean)}), then possibly remove and process them and finally pass
     * the batch up to the next protocol. Protocols can also modify messages in place, e.g. ENCRYPT could decrypt all
     * encrypted messages in the batch, not remove them, and pass the batch up when done.
     * @param batch The message batch
     */
    public void up(MessageBatch batch) {
        for(Iterator<Message> it=batch.iterator(); it.hasNext();) {
            Message msg=it.next();
            if(msg != null && accept(msg)) {
                it.remove();
                try {
                    up(new Event(Event.MSG, msg));
                }
                catch(Throwable t) {
                    log.error(Util.getMessage("PassUpFailure"), t);
                }
            }
        }
        if(!batch.isEmpty())
            up_prot.up(batch);
    }


    /**
     * An event is to be sent down the stack. The layer may want to examine its type and perform
     * some action on it, depending on the event's type. If the event is a message MSG, then
     * the layer may need to add a header to it (or do nothing at all) before sending it down
     * the stack using <code>down_prot.down()</code>. In case of a GET_ADDRESS event (which tries to
     * retrieve the stack's address from one of the bottom layers), the layer may need to send
     * a new response event back up the stack using <code>up_prot.up()</code>.
     */
    public Object down(Event evt) {
        return down_prot.down(evt);
    }




}
