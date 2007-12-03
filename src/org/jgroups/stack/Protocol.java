

package org.jgroups.stack;


import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jgroups.Event;

import java.util.Map;
import java.util.Properties;
import java.util.Vector;


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
 * @version $Id: Protocol.java,v 1.55 2007/12/03 13:17:55 belaban Exp $
 */
public abstract class Protocol {
    protected final Properties props=new Properties();
    protected Protocol         up_prot=null, down_prot=null;
    protected ProtocolStack    stack=null;
    protected boolean          stats=true;  // determines whether to collect statistics (and expose them via JMX)
    protected final Log        log=LogFactory.getLog(this.getClass());


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


    public void setProperty(String key, String val) {
        this.props.put(key, val);
    }


    /** Called by Configurator. Removes 2 properties which are used by the Protocol directly and then
     *	calls setProperties(), which might invoke the setProperties() method of the actual protocol instance.
     */
    public boolean setPropertiesInternal(Properties props) {
        this.props.putAll(props);

        String str=props.getProperty("down_thread");
        if(str != null) {
            if(log.isWarnEnabled())
                log.warn("down_thread was deprecated and is ignored");
            props.remove("down_thread");
        }

        str=props.getProperty("down_thread_prio");
        if(str != null) {
            if(log.isWarnEnabled())
                log.warn("down_thread_prio was deprecated and is ignored");
            props.remove("down_thread_prio");
        }

        str=props.getProperty("up_thread");
        if(str != null) {
            if(log.isWarnEnabled())
                log.warn("up_thread was deprecated and is ignored");
            props.remove("up_thread");
        }

        str=props.getProperty("up_thread_prio");
        if(str != null) {
            if(log.isWarnEnabled())
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
    
    public ProtocolStack getProtocolStack(){
        return stack;
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

    public Map<String,Object> dumpStats() {
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


    /** List of events that are required to be answered by some layer above.
     @return Vector (of Integers) */
    public Vector<Integer> requiredUpServices() {
        return null;
    }

    /** List of events that are required to be answered by some layer below.
     @return Vector (of Integers) */
    public Vector<Integer> requiredDownServices() {
        return null;
    }

    /** List of events that are provided to layers above (they will be handled when sent down from
     above).
     @return Vector (of Integers) */
    public Vector<Integer> providedUpServices() {
        return null;
    }

    /** List of events that are provided to layers below (they will be handled when sent down from
     below).
     @return Vector<Integer (of Integers) */
    public Vector<Integer> providedDownServices() {
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
