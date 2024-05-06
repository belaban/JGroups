

package org.jgroups.stack;


import org.jgroups.*;
import org.jgroups.annotations.ManagedAttribute;
import org.jgroups.annotations.ManagedOperation;
import org.jgroups.annotations.Property;
import org.jgroups.conf.ClassConfigurator;
import org.jgroups.conf.PropertyConverters;
import org.jgroups.conf.XmlNode;
import org.jgroups.logging.Log;
import org.jgroups.logging.LogFactory;
import org.jgroups.protocols.TP;
import org.jgroups.util.*;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.stream.Collectors;


/**
 * The Protocol class provides a set of common services for protocol layers. Each layer has to
 * be a subclass of Protocol and override a number of methods (typically just {@code up()},
 * {@code down()} and {@code getName()}. Layers are stacked in a certain order to form
 * a protocol stack. <a href=org.jgroups.Event.html>Events</a> are passed from lower
 * layers to upper ones and vice versa. E.g. a Message received by the UDP layer at the bottom
 * will be passed to its higher layer as an Event. That layer will in turn pass the Event to
 * its layer and so on, until a layer handles the Message and sends a response or discards it,
 * the former resulting in another Event being passed down the stack.
 * <p/>
 * The important thing to bear in mind is that <a href=org.jgroups.Event.html>Events</a> have to
 * be passed on between layers in FIFO order which is guaranteed by the Protocol implementation
 * and must be guaranteed by subclasses implementing their on Event queuing.<p>
 * <b>Note that each class implementing interface Protocol MUST provide an empty, public
 * constructor !</b>
 *
 * @author Bela Ban
 */
public abstract class Protocol implements Lifecycle {
    protected Protocol             up_prot, down_prot;
    protected ProtocolStack        stack;

    @Property(description="Determines whether to collect statistics (and expose them via JMX). Default is true")
    protected boolean              stats=true;

    @Property(description="Enables ergonomics: dynamically find the best values for properties at runtime")
    protected boolean              ergonomics=true;

    @Property(description="Fully qualified name of a class implementing ProtocolHook, will be called after creation of " +
      "the protocol (before init())",writable=false)
    protected String               after_creation_hook;

    @Property(description="Give the protocol a different ID if needed so we can have multiple " +
            "instances of it in the same stack",writable=false)
    protected short                id=ClassConfigurator.getProtocolId(getClass());

    @ManagedAttribute(description="The local address of this member")
    protected Address              local_addr;

    protected final Log            log=LogFactory.getLog(this.getClass());

    protected List<Policy>         policies;


    /**
     * Sets the level of a logger. This method is used to dynamically change the logging level of a running system,
     * e.g. via JMX. The appender of a level needs to exist.
     * @param level The new level. Valid values are "fatal", "error", "warn", "info", "debug", "trace"
     * (capitalization not relevant)
     */
    @Property(name="level", description="Sets the level")
    public <T extends Protocol> T  setLevel(String level)            {log.setLevel(level); return (T)this;}
    @Property(name="level", description="logger level (see javadocs)")
    public String                  getLevel()                        {return log.getLevel();}
    public <T extends Protocol> T  level(String level)               {return setLevel(level);}
    public Address                 getAddress()                      {return local_addr;}
    public Address                 addr()                            {return local_addr;}
    public <T extends Protocol> T  addr(Address addr)                {this.local_addr=addr; return (T)this;}
    public <T extends Protocol> T  setAddress(Address addr)          {this.local_addr=addr; return (T)this;}
    public boolean                 isErgonomics()                    {return ergonomics;}
    public <T extends Protocol> T  setErgonomics(boolean ergonomics) {this.ergonomics=ergonomics; return (T)this;}
    public ProtocolStack           getProtocolStack()                {return stack;}
    public boolean                 statsEnabled()                    {return stats;}
    public void                    enableStats(boolean flag)         {stats=flag;}
    public String                  getName()                         {return getClass().getSimpleName();}
    public short                   getId()                           {return id;}
    public <T extends Protocol> T  setId(short id)                   {this.id=id; return (T)this;}
    public <T extends Protocol> T  getUpProtocol()                   {return (T)up_prot;}
    public <T extends Protocol> T  getDownProtocol()                 {return (T)down_prot;}
    public <T extends Protocol> T  setUpProtocol(Protocol prot)      {this.up_prot=prot; return (T)this;}
    public <T extends Protocol> T  setDownProtocol(Protocol prot)    {this.down_prot=prot; return (T)this;}
    public <T extends Protocol> T  setProtocolStack(ProtocolStack s) {this.stack=s; return (T)this;}
    public String                  afterCreationHook()               {return after_creation_hook;}
    public Log                     getLog()                          {return log;}
    public List<? extends Policy>  getPolicies()                     {return policies;}

    @ManagedAttribute(description="The list of policies")
    public String policies() {return policies == null? "n/a" :
      policies.stream().map(p -> p.getClass().getSimpleName()).collect(Collectors.joining(", "));}

    @Property(name="policies",converter= PropertyConverters.PolicyConverter.class)
    public <T extends Protocol> T  setPolicies(List<Policy> l) {
        this.policies=l;
        return (T)this;
    }

    public <T extends Protocol> T  addPolicy(Policy p) {
        if(policies == null)
            policies=new ArrayList<>();
        policies.add(Objects.requireNonNull(p));
        return (T)this;
    }

    public <T extends Protocol> T  removePolicy(Policy p) {
        if(policies != null && p != null)
            policies.remove(p);
        return (T)this;
    }

    public Object getValue(String name) {
        if(name == null) return null;
        Field field=Util.getField(getClass(), name);
        if(field == null)
            throw new IllegalArgumentException("field \"" + name + "\n not found");
        return Util.getField(field, this);
    }


    public <T extends Protocol> T setValue(String name, Object value) {
        if(name == null || value == null)
            return (T)this;
        Field field=Util.getField(getClass(), name);
        if(field == null)
            throw new IllegalArgumentException("field " + name + " not found");
        Property prop=field.getAnnotation(Property.class);
        if(prop != null) {
            String deprecated_msg=prop.deprecatedMessage();
            if(deprecated_msg != null && !deprecated_msg.isEmpty())
                log.warn("Field " + getName() + "." + name + " is deprecated: " + deprecated_msg);
        }
        Util.setField(field, this, value);
        return (T)this;
    }



    /**
     * After configuring the protocol itself from the properties defined in the XML config, a protocol might have
     * additional component objects which need to be configured. This callback allows a protocol developer to configure those
     * other objects. This call is guaranteed to be invoked <em>after</em> the protocol itself has been configured.<br/>
     * See AUTH for an example.
     */
    public List<Object> getComponents() {
        return Util.getComponents(this);
    }

    /** Called by the XML parser when subelements are found in the configuration of a protocol. This allows
     * a protocol to define protocol-specific information and to parse it */
    public void parse(XmlNode node) throws Exception {;}

    /** Returns the protocol IDs of all protocols above this one (excluding the current protocol) */
    public short[] getIdsAbove() {
        short[]     retval;
        List<Short> ids=new ArrayList<>();
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


    public TP getTransport() {
        Protocol retval=this;
        while(retval != null && retval.down_prot != null) {
            retval=retval.down_prot;
        }
        return retval instanceof TP? (TP)retval : null;
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



    /**
     * Called after a protocol has been created and before the protocol is started.
     * Attributes are already set. Other protocols are not yet connected and events cannot yet be sent.
     * @exception Exception Thrown if protocol cannot be initialized successfully. This will cause the
     *                      ProtocolStack to fail, so the the channel constructor will throw an exception
     */
    @Override public void init() throws Exception {
    }

    /**
     * This method is called on a {@link JChannel#connect(String)}; starts work. Protocols are connected ready to
     * receive events.  Will be called <em>from bottom to top</em>.
     * @exception Exception Thrown if protocol cannot be started successfully. This will cause the ProtocolStack
     *                      to fail, so {@link JChannel#connect(String)} will throw an exception
     */
    @Override public void start() throws Exception {
    }

    /**
     * Called on a {@link JChannel#disconnect()}; stops work (e.g. by closing multicast socket). Will be called
     * <em>from top to bottom</em>.
     */
    @Override public void stop() {
    }


    /**
     * This method is called on a {@link JChannel#close()}. Does some cleanup; after the call, the VM will terminate
     */
    @Override public void destroy() {
    }


    /** List of events that are required to be answered by some layer above */
    public List<Integer> requiredUpServices() {return null;}

    /** List of events that are required to be answered by some layer below */
    public List<Integer> requiredDownServices() {return null;}

    /** List of events that are provided to layers above (they will be handled when sent down from above) */
    public List<Integer> providedUpServices() {return null;}

    /** List of events that are provided to layers below (they will be handled when sent from down below) */
    public List<Integer> providedDownServices() {return null;}

    /** Returns all services provided by protocols below the current protocol */
    public final List<Integer> getDownServices() {
        List<Integer> retval=new ArrayList<>();
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
        List<Integer> retval=new ArrayList<>();
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
     * An event is to be sent down the stack. A protocol may want to examine its type and perform some action on it,
     * depending on the event's type. If the event is a message MSG, then the protocol may need to add a header to it
     * (or do nothing at all) before sending it down the stack using {@code down_prot.down()}.
     */
    public Object down(Event evt) {
        return down_prot.down(evt);
    }

    /**
     * A message is sent down the stack. Protocols may examine the message and do something (e.g. add a header)
     * with it, before passing it down.
     * @since 4.0
     */
    public Object down(Message msg) {
        return down_prot.down(msg);
    }


    /**
     * Passes a message down asynchronously. The sending is executed in the transport's thread pool. If the pool is full
     * and the message is marked as {@link org.jgroups.Message.TransientFlag#DONT_BLOCK}, then it will be dropped,
     * otherwise it will be sent on the caller's thread.
     * @param msg The message to be sent
     * @param async Whether to send the message asynchronously
     * @return A CompletableFuture of the result (or exception)
     */
    public CompletableFuture<Object> down(Message msg, boolean async) {
        AsyncExecutor<Object> executor=getTransport().getAsyncExecutor();
        Supplier<Object> s=() -> down(msg);
        try {
            return !async? CompletableFuture.completedFuture(down_prot.down(msg)):
              executor.execute(s, msg.isFlagSet(Message.TransientFlag.DONT_BLOCK));
        }
        catch(Throwable t) {
            return CompletableFuture.failedFuture(t);
        }
    }


    /**
     * An event was received from the protocol below. Usually the current protocol will want to examine the event type
     * and - depending on its type - perform some computation (e.g. removing headers from a MSG event type, or updating
     * the internal membership list when receiving a VIEW_CHANGE event).
     * Finally, the event is either a) discarded, or b) an event is sent down the stack using {@code down_prot.down()}
     * or c) the event (or another event) is sent up the stack using {@code up_prot.up()}.
     */
    public Object up(Event evt) {
        return up_prot.up(evt);
    }


    /**
     * A single message was received. Protocols may examine the message and do something (e.g. add a header) with it
     * before passing it up.
     * @since 4.0
     */
    public Object up(Message msg) {
        return up_prot.up(msg);
    }


    /**
     * Sends up a multiple messages in a {@link MessageBatch}. The sender of the batch is always the same, and so is the
     * destination (null == multicast messages). Messages in a batch can be OOB messages, regular messages, or mixed
     * messages, although the transport itself will create initial MessageBatches that contain only either OOB or
     * regular messages.<p/>
     * The default processing below sends messages up the stack individually, based on a matching criteria
     * (calling {@link #accept(Message)}), and - if true - calls {@link #up(org.jgroups.Event)}
     * for that message and removes the message. If the batch is not empty, it is passed up, or else it is dropped.<p/>
     * Subclasses should check if there are any messages destined for them (e.g. using
     * {@link MessageBatch#iterator(Predicate)}), then possibly remove and process them and finally pass
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
                    up(msg);
                }
                catch(Throwable t) {
                    log.error(Util.getMessage("PassUpFailure"), t);
                }
            }
        }
        if(!batch.isEmpty())
            up_prot.up(batch);
    }

    public String toString() {
        return String.format("%s%s", getClass().getSimpleName(), local_addr != null? String.format(" (%s)", local_addr) : "");
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

}
