// $Id: Protocol.java,v 1.1 2003/09/09 01:24:12 belaban Exp $

package org.jgroups.stack;


import org.jgroups.Event;
import org.jgroups.log.Trace;
import org.jgroups.util.Queue;
import org.jgroups.util.QueueClosedException;

import java.util.Properties;
import java.util.Vector;




class UpHandler extends Thread {
    private Queue mq=null;
    private Protocol handler=null;
    private ProtocolObserver observer=null;


    public UpHandler(Queue mq, Protocol handler, ProtocolObserver observer) {
        this.mq=mq;
        this.handler=handler;
        this.observer=observer;
        if(handler != null)
            setName("UpHandler (" + handler.getName() + ")");
        else
            setName("UpHandler");
    }


    public void setObserver(ProtocolObserver observer) {
        this.observer=observer;
    }


    /** Removes events from mq and calls handler.up(evt) */
    public void run() {
        Event evt;
        while(!mq.closed()) {
            try {
                evt=(Event)mq.remove();
                if(evt == null) {
                    Trace.warn("Protocol.UpHandler.run()", "removed null event");
                    continue;
                }

                if(observer != null) {                          // call debugger hook (if installed)
                    if(observer.up(evt, mq.size()) == false) {  // false means discard event
                        return;
                    }
                }
                handler.up(evt);
                evt=null;
            }
            catch(QueueClosedException queue_closed) {
                break;
            }
            catch(Throwable e) {
                Trace.warn("Protocol.UpHandler.run()", getName() + " exception: " + e);
                e.printStackTrace();
            }
        }
    }

}


class DownHandler extends Thread {
    private Queue mq=null;
    private Protocol handler=null;
    private ProtocolObserver observer=null;


    public DownHandler(Queue mq, Protocol handler, ProtocolObserver observer) {
        this.mq=mq;
        this.handler=handler;
        this.observer=observer;
        if(handler != null)
            setName("DownHandler (" + handler.getName() + ")");
        else
            setName("DownHandler");
    }


    public void setObserver(ProtocolObserver observer) {
        this.observer=observer;
    }


    /** Removes events from mq and calls handler.down(evt) */
    public void run() {
        Event evt;
        while(!mq.closed()) {
            try {
                evt=(Event)mq.remove();
                if(evt == null) {
                    Trace.warn("Protocol.DownHandler.run()", "removed null event");
                    continue;
                }

                if(observer != null) {                            // call debugger hook (if installed)
                    if(observer.down(evt, mq.size()) == false) {  // false means discard event
                        continue;
                    }
                }
                handler.down(evt);
                evt=null;
            }
            catch(QueueClosedException queue_closed) {
                break;
            }
            catch(Throwable e) {
                Trace.warn("Protocol.DownHandler.run()", getName() + " exception is " + e);
                e.printStackTrace();
            }
        }
    }

}


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
 */
public abstract class Protocol {
    protected Properties       props=null;
    protected Protocol         up_prot=null, down_prot=null;
    protected ProtocolStack    stack=null;
    protected Queue            up_queue=new Queue(), down_queue=new Queue();
    protected UpHandler        up_handler=null;
    protected int              up_thread_prio=-1;
    protected DownHandler      down_handler=null;
    protected int              down_thread_prio=-1;
    protected ProtocolObserver observer=null; // hook for debugger
    private final long         THREAD_JOIN_TIMEOUT=1000;
    protected boolean          down_thread=true;  // determines whether the down_handler thread should be started
    protected boolean          up_thread=true;    // determines whether the up_handler thread should be started


    /**
     * Configures the protocol initially. A configuration string consists of name=value
     * items, separated by a ';' (semicolon), e.g.:<pre>
     * "loopback=false;unicast_inport=4444"
     * </pre>
     */
    public boolean setProperties(Properties props) {
        this.props=props;
        return true;
    }


    /** Called by Configurator. Removes 2 properties which are used by the Protocol directly and then
     *	calls setProperties(), which might invoke the setProperties() method of the actual protocol instance.
     */
    public boolean setPropertiesInternal(Properties props) {
        String str;
        this.props=(Properties)props.clone();

        str=props.getProperty("down_thread");
        if(str != null) {
            down_thread=new Boolean(str).booleanValue();
            props.remove("down_thread");
        }

        str=props.getProperty("down_thread_prio");
        if(str != null) {
            down_thread_prio=Integer.parseInt(str);
            props.remove("down_thread_prio");
        }

        str=props.getProperty("up_thread");
        if(str != null) {
            up_thread=new Boolean(str).booleanValue();
            props.remove("up_thread");
        }

        str=props.getProperty("up_thread_prio");
        if(str != null) {
            up_thread_prio=Integer.parseInt(str);
            props.remove("up_thread_prio");
        }

        return setProperties(props);
    }


    public Properties getProperties() {
        return props;
    }


    public void setObserver(ProtocolObserver observer) {
        this.observer=observer;
        if(up_handler != null)
            up_handler.setObserver(observer);
        if(down_handler != null)
            down_handler.setObserver(observer);
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
        return up_queue;
    }    // used by Debugger (ProtocolView)

    public Queue getDownQueue() {
        return down_queue;
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


    /** Used internally. If overridden, call this method first. Only creates the up_handler thread
     if down_thread is true */
    public void startUpHandler() {
        if(up_thread) {
            if(up_handler == null) {
                up_handler=new UpHandler(up_queue, this, observer);
                if(up_thread_prio >= 0) {
                    try {
                        up_handler.setPriority(up_thread_prio);
                    }
                    catch(Throwable t) {
                        Trace.error("Protocol.startUpHandler()",
                                    "priority " + up_thread_prio +
                                    " could not be set for thread: " + Trace.getStackTrace(t));
                    }
                }
                up_handler.start();
            }
        }
    }


    /** Used internally. If overridden, call this method first. Only creates the down_handler thread
     if down_thread is true */
    public void startDownHandler() {
        if(down_thread) {
            if(down_handler == null) {
                down_handler=new DownHandler(down_queue, this, observer);
                if(down_thread_prio >= 0) {
                    try {
                        down_handler.setPriority(down_thread_prio);
                    }
                    catch(Throwable t) {
                        Trace.error("Protocol.startDownHandler()",
                                    "priority " + down_thread_prio +
                                    " could not be set for thread: " + Trace.getStackTrace(t));
                    }
                }
                down_handler.start();
            }
        }
    }


    /** Used internally. If overridden, call parent's method first */
    public void stopInternal() {
        up_queue.close(false);  // this should terminate up_handler thread

        if(up_handler != null && up_handler.isAlive()) {
            try {
                up_handler.join(THREAD_JOIN_TIMEOUT);
            }
            catch(Exception ex) {
            }
            if(up_handler.isAlive()) {
                up_handler.interrupt();  // still alive ? let's just kill it without mercy...
                try {
                    up_handler.join(THREAD_JOIN_TIMEOUT);
                }
                catch(Exception ex) {
                }
                if(up_handler.isAlive())
                    Trace.error("Protocol.stopInternal()", "up_handler thread for " + getName() +
                                                           " was interrupted (in order to be terminated), but is still alive");
            }
        }
        up_handler=null;

        down_queue.close(false); // this should terminate down_handler thread
        if(down_handler != null && down_handler.isAlive()) {
            try {
                down_handler.join(THREAD_JOIN_TIMEOUT);
            }
            catch(Exception ex) {
            }
            if(down_handler.isAlive()) {
                down_handler.interrupt(); // still alive ? let's just kill it without mercy...
                try {
                    down_handler.join(THREAD_JOIN_TIMEOUT);
                }
                catch(Exception ex) {
                }
                if(down_handler.isAlive())
                    Trace.error("Protocol.stopInternal()", "down_handler thread for " + getName() +
                                                           " was interrupted (in order to be terminated), but is is still alive");
            }
        }
        down_handler=null;
    }


    /**
     * Internal method, should not be called by clients. Used by ProtocolStack. I would have
     * used the 'friends' modifier, but this is available only in C++ ... If the up_handler thread
     * is not available (down_thread == false), then directly call the up() method: we will run on the
     * caller's thread (e.g. the protocol layer below us).
     */
    protected void receiveUpEvent(Event evt) {
        if(up_handler == null) {
            if(observer != null) {                               // call debugger hook (if installed)
                if(observer.up(evt, up_queue.size()) == false) {  // false means discard event
                    return;
                }
            }
            up(evt);
            return;
        }
        try {
            up_queue.add(evt);
        }
        catch(Exception e) {
            Trace.warn("Protocol.receiveUpEvent()", "exception: " + e);
        }
    }

    /**
     * Internal method, should not be called by clients. Used by ProtocolStack. I would have
     * used the 'friends' modifier, but this is available only in C++ ... If the down_handler thread
     * is not available (down_thread == false), then directly call the down() method: we will run on the
     * caller's thread (e.g. the protocol layer above us).
     */
    protected void receiveDownEvent(Event evt) {
        if(down_handler == null) {
            if(observer != null) {                                    // call debugger hook (if installed)
                if(observer.down(evt, down_queue.size()) == false) {  // false means discard event
                    return;
                }
            }
            down(evt);
            return;
        }
        try {
            down_queue.add(evt);
        }
        catch(Exception e) {
            Trace.warn("Protocol.receiveDownEvent()", "exception: " + e);
        }
    }

    /**
     * Causes the event to be forwarded to the next layer up in the hierarchy. Typically called
     * by the implementation of <code>Up</code> (when done).
     */
    public void passUp(Event evt) {
        if(observer != null) {                   // call debugger hook (if installed)
            if(observer.passUp(evt) == false) {  // false means don't pass up (=discard) event
                return;
            }
        }

        if(up_prot != null) {
            up_prot.receiveUpEvent(evt);
        }
        else
            Trace.error("Protocol.passUp()", "no upper layer available");
    }

    /**
     * Causes the event to be forwarded to the next layer down in the hierarchy.Typically called
     * by the implementation of <code>Down</code> (when done).
     */
    public void passDown(Event evt) {
        if(observer != null) {                     // call debugger hook (if installed)
            if(observer.passDown(evt) == false) {  // false means don't pass down (=discard) event
                return;
            }
        }

        if(down_prot != null)
            down_prot.receiveDownEvent(evt);
        else
            Trace.error("Protocol.passDown()", "no lower layer available");
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


}
