

package org.jgroups.stack;


import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jgroups.Event;
import org.jgroups.util.Queue;

import java.util.Map;
import java.util.Properties;
import java.util.Vector;





/**
 * The Protocol class provides a set of common services for protocol layers. Each layer has to
 * be a subclass of Protocol and override a number of methods (typically just <code>up()</code>,
 * <code>Down</code> and <code>getName</code>. Layers are stacked in a certain order to form
 * a protocol stack. <a href=org.jgroups.Event.html>Events</a> are passed from lower
 * layers to upper ones and vice versa. E.g. a Message received by the UDP layer at the bottom
 * will be passed to its higher layer as an Event. That layer will in turn pass the Event to
 * its layer and so on, until a layer handles the Message and sends a response or discards it,
 * the former resulting in another Event being passed down the stack.<p>
 * Each layer has 2 FIFO queues, one for up Events and one for down Events. When an Event is
 * received by a layer (calling the internal upcall <code>ReceiveUpEvent</code>), it is placed
 * in the up-queue where it will be retrieved by the up-handler thread which will invoke method
 * <code>Up</code> of the layer. The same applies for Events traveling down the stack. Handling
 * of the up-handler and down-handler threads and the 2 FIFO queues is donw by the Protocol
 * class, subclasses will almost never have to override this behavior.<p>
 * The important thing to bear in mind is that Events have to passed on between layers in FIFO
 * order which is guaranteed by the Protocol implementation and must be guranteed by subclasses
 * implementing their on Event queuing.<p>
 * <b>Note that each class implementing interface Protocol MUST provide an empty, public
 * constructor !</b>
 *
 * @author Bela Ban
 * @version $Id: Protocol.java,v 1.48 2006/12/28 10:25:25 belaban Exp $
 */
public abstract class Protocol {
    protected final Properties props=new Properties();
    private   Protocol         up_prot=null, down_prot=null;
    protected ProtocolStack    stack=null;
    protected boolean          stats=true;  // determines whether to collect statistics (and expose them via JMX)
    protected final Log        log=LogFactory.getLog(this.getClass());
    protected boolean          trace=log.isTraceEnabled();
    protected boolean          warn=log.isWarnEnabled();


    /**
     * Configures the protocol initially. A configuration string consists of name=value
     * items, separated by a ';' (semicolon), e.g.:<pre>
     * "loopback=false;unicast_inport=4444"
     * </pre>
     */
    public boolean setProperties(Properties props) {
        if(props != null)
            this.props.putAll(props);
        return true;
    }


    /** Called by Configurator. Removes 2 properties which are used by the Protocol directly and then
     *	calls setProperties(), which might invoke the setProperties() method of the actual protocol instance.
     */
    public boolean setPropertiesInternal(Properties props) {
        this.props.putAll(props);

        String str=props.getProperty("down_thread");
        if(str != null) {
            if(warn)
                log.warn("down_thread was deprecated and is ignored");
            props.remove("down_thread");
        }

        str=props.getProperty("down_thread_prio");
        if(str != null) {
            if(warn)
                log.warn("down_thread_prio was deprecated and is ignored");
            props.remove("down_thread_prio");
        }

        str=props.getProperty("up_thread");
        if(str != null) {
            if(warn)
                log.warn("up_thread was deprecated and is ignored");
            props.remove("up_thread");
        }

        str=props.getProperty("up_thread_prio");
        if(str != null) {
            if(warn)
                log.warn("up_thread_prio was deprecated and is ignored");
            props.remove("up_thread_prio");
        }

        str=props.getProperty("stats");
        if(str != null) {
            stats=Boolean.valueOf(str).booleanValue();
            props.remove("stats");
        }

        return setProperties(props);
    }


    public Properties getProperties() {
        return props;
    }


    public boolean isTrace() {
        return trace;
    }

    public void setTrace(boolean trace) {
        this.trace=trace;
    }

    public boolean isWarn() {
        return warn;
    }

    public void setWarn(boolean warn) {
        this.warn=warn;
    }

    /** @deprecated up_thread was removed
     * @return false by default
     */
    public boolean upThreadEnabled() {
        return false;
    }

    /**
     * @deprecated down thread was removed
     * @return boolean False by default
     */
    public boolean downThreadEnabled() {
        return false;
    }

    public boolean statsEnabled() {
        return stats;
    }

    public void enableStats(boolean flag) {
        stats=flag;
    }

    public void resetStats() {
        ;
    }

    public String printStats() {
        return null;
    }

    public Map dumpStats() {
        return null;
    }


    /**
     * Called after instance has been created (null constructor) and before protocol is started.
     * Properties are already set. Other protocols are not yet connected and events cannot yet be sent.
     * @exception Exception Thrown if protocol cannot be initialized successfully. This will cause the
     *                      ProtocolStack to fail, so the channel constructor will throw an exception
     */
    public void init() throws Exception {
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


    public Queue getUpQueue() {
        throw new UnsupportedOperationException("queues were removed in 2.5");
    }    // used by Debugger (ProtocolView)

    public Queue getDownQueue() {
        throw new UnsupportedOperationException("queues were removed in 2.5");
    }  // used by Debugger (ProtocolView)


    /** List of events that are required to be answered by some layer above.
     @return Vector (of Integers) */
    public Vector requiredUpServices() {
        return null;
    }

    /** List of events that are required to be answered by some layer below.
     @return Vector (of Integers) */
    public Vector requiredDownServices() {
        return null;
    }

    /** List of events that are provided to layers above (they will be handled when sent down from
     above).
     @return Vector (of Integers) */
    public Vector providedUpServices() {
        return null;
    }

    /** List of events that are provided to layers below (they will be handled when sent down from
     below).
     @return Vector (of Integers) */
    public Vector providedDownServices() {
        return null;
    }


    public abstract String getName();   // all protocol names have to be unique !

    public Protocol getUpProtocol() {
        return up_prot;
    }

    public Protocol getDownProtocol() {
        return down_prot;
    }

    public void setUpProtocol(Protocol up_prot) {
        this.up_prot=up_prot;
    }

    public void setDownProtocol(Protocol down_prot) {
        this.down_prot=down_prot;
    }

    public void setProtocolStack(ProtocolStack stack) {
        this.stack=stack;
    }



    /**
     * Causes the event to be forwarded to the next layer up in the hierarchy. Typically called
     * by the implementation of <code>Up</code> (when done).
     */
    public void passUp(Event evt) {
        up_prot.up(evt);
    }

    /**
     * Causes the event to be forwarded to the next layer down in the hierarchy.Typically called
     * by the implementation of <code>Down</code> (when done).
     */
    public void passDown(Event evt) {
        down_prot.down(evt);
    }


    /**
     * An event was received from the layer below. Usually the current layer will want to examine
     * the event type and - depending on its type - perform some computation
     * (e.g. removing headers from a MSG event type, or updating the internal membership list
     * when receiving a VIEW_CHANGE event).
     * Finally the event is either a) discarded, or b) an event is sent down
     * the stack using <code>passDown()</code> or c) the event (or another event) is sent up
     * the stack using <code>passUp()</code>.
     */
    public void up(Event evt) {
        passUp(evt);
    }


    /** Temporary method, will be changed to up() */
    public Object upcall(Event evt) {
        return up_prot.upcall(evt);
    }

    /**
     * An event is to be sent down the stack. The layer may want to examine its type and perform
     * some action on it, depending on the event's type. If the event is a message MSG, then
     * the layer may need to add a header to it (or do nothing at all) before sending it down
     * the stack using <code>passDown()</code>. In case of a GET_ADDRESS event (which tries to
     * retrieve the stack's address from one of the bottom layers), the layer may need to send
     * a new response event back up the stack using <code>passUp()</code>.
     */
    public void down(Event evt) {
        passDown(evt);
    }


    /** Temporary method, will be changed to down() */
    public Object downcall(Event evt) {
        return down_prot.downcall(evt);
    }


}
