// $Id: PullPushAdapter.java,v 1.1.1.1 2003/09/09 01:24:08 belaban Exp $

package org.jgroups.blocks;


import org.jgroups.*;
import org.jgroups.log.Trace;
import org.jgroups.util.Util;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;


/**
 * Allows a client of <em>Channel</em> to be notified when messages have been received
 * instead of having to actively poll the channel for new messages. Typically used in the
 * client role (receive()). As this class does not implement interface
 * <code>Transport</code>, but <b>uses</b> it for receiving messages, an underlying object
 * has to be used to send messages (e.g. the channel on which an object of this class relies).<p>
 * Multiple MembershipListeners can register with the PullPushAdapter; when a view is received, they
 * will all be notified. There is one main message listener which sends and receives message. In addition,
 * MessageListeners can register with a certain tag (identifier), and then send messages tagged with this
 * identifier. When a message with such an identifier is received, the corresponding MessageListener will be
 * looked up and the message dispatched to it. If no tag is found (default), the main MessageListener will
 * receive the message.
 * @author Bela Ban
 * @version $Revision
 */
public class PullPushAdapter implements Runnable {
    protected Transport       transport=null;
    protected MessageListener listener=null;           // main message receiver
    protected List            membership_listeners=new ArrayList();
    protected Thread          receiver_thread=null;
    protected HashMap         listeners=new HashMap(); // keys=identifier (Serializable), values=MessageListeners
    static final String       PULL_HEADER="PULL_HEADER";


    public PullPushAdapter(Transport transport) {
        this.transport=transport;
        start();
    }

    public PullPushAdapter(Transport transport, MessageListener l) {
        this.transport=transport;
        setListener(l);
        start();
    }


    public PullPushAdapter(Transport transport, MembershipListener ml) {
        this.transport=transport;
        addMembershipListener(ml);
        start();
    }


    public PullPushAdapter(Transport transport, MessageListener l, MembershipListener ml) {
        this.transport=transport;
        setListener(l);
        addMembershipListener(ml);
        start();
    }


    public Transport getTransport() {
        return transport;
    }


    public void start() {
        if(receiver_thread == null) {
            receiver_thread=new Thread(this, "PullPushAdapterThread");
            receiver_thread.setDaemon(true);
            receiver_thread.start();
        }
    }

    public void stop() {
        Thread tmp=null;
        if(receiver_thread != null && receiver_thread.isAlive()) {
            tmp=receiver_thread;
            receiver_thread=null;
            tmp.interrupt();
            try {
                tmp.join(1000);
            }
            catch(Exception ex) {
            }
        }
        receiver_thread=null;
    }


    public void send(Serializable identifier, Message msg) throws Exception {
        if(msg == null) {
            Trace.error("PullPushAdapter.send()", "msg is null");
            return;
        }
        if(identifier == null)
            transport.send(msg);
        else {
            msg.putHeader(PULL_HEADER, new PullHeader(identifier));
            transport.send(msg);
        }
    }


    public void send(Message msg) throws Exception {
        send(null, msg);
    }


    public void setListener(MessageListener l) {
        listener=l;
    }

    public void registerListener(Serializable identifier, MessageListener l) {
        if(l == null || identifier == null) {
            Trace.error("PullPushAdapter.registerListener()", "message listener or identifier is null");
            return;
        }
        if(listeners.containsKey(identifier)) {
            Trace.error("PullPushAdapter.registerListener()", "listener with identifier=" + identifier +
                    " already exists, choose a different identifier");
        }
        listeners.put(identifier, l);
    }


    /** @deprecated Use {@link #addMembershipListener} */
    public void setMembershipListener(MembershipListener ml) {
        addMembershipListener(ml);
    }

    public void addMembershipListener(MembershipListener l) {
        if(l != null && !membership_listeners.contains(l))
            membership_listeners.add(l);
    }


    /**
     * Reentrant run(): message reception is serialized, then the listener is notified of the
     * message reception
     */
    public void run() {
        Object obj;

        while(receiver_thread != null) {
            try {
                obj=transport.receive(0);
                if(obj == null)
                    continue;

                if(obj instanceof Message) {
                    handleMessage((Message)obj);

                }
                else if(obj instanceof GetStateEvent) {
                    if(listener != null) {
                        if(transport instanceof Channel)
                            ((Channel)transport).returnState(listener.getState());
                        else {
                            Trace.error("PullPushAdapter.run()", "underlying transport is not a Channel, but a " +
                                    transport.getClass().getName() + ": cannot fetch state using returnState()");
                            continue;
                        }
                    }
                }
                else if(obj instanceof SetStateEvent) {
                    if(listener != null) {
                        try {
                            listener.setState(((SetStateEvent)obj).getArg());
                        }
                        catch(ClassCastException cast_ex) {
                            Trace.error("PullPushAdapter.run()", "received SetStateEvent, but argument " +
                                    ((SetStateEvent)obj).getArg() + " is not serializable ! Discarding message.");
                            continue;
                        }
                    }
                }
                else if(obj instanceof View) {
                    notifyViewChange((View)obj);
                }
                else if(obj instanceof SuspectEvent) {
                    notifySuspect((Address)((SuspectEvent)obj).getMember());
                }
                else if(obj instanceof BlockEvent) {
                    notifyBlock();
                }
            }
            catch(ChannelNotConnectedException conn) {
                Address local_addr=((Channel)transport).getLocalAddress();
                Trace.warn("PullPushAdapter.run()", "[" + (local_addr == null ? "<null>" : local_addr.toString()) +
                        "] channel not connected, exception is " + conn);
                Util.sleep(1000);
                break;
            }
            catch(ChannelClosedException closed_ex) {
                Address local_addr=((Channel)transport).getLocalAddress();
                Trace.warn("PullPushAdapter.run()", "[" + (local_addr == null ? "<null>" : local_addr.toString()) +
                        "] channel closed, exception is " + closed_ex);
                Util.sleep(1000);
                break;
            }
            catch(Throwable e) {
            }
        }
    }


    /**
     * Check whether the message has an identifier. If yes, lookup the MessageListener associated with the
     * given identifier in the hashtable and dispatch to it. Otherwise just use the main (default) message
     * listener
     */
    protected void handleMessage(Message msg) {
        PullHeader hdr=(PullHeader)msg.getHeader(PULL_HEADER);
        Serializable identifier;
        MessageListener l;

        if(hdr != null && (identifier=hdr.getIdentifier()) != null) {
            l=(MessageListener)listeners.get(identifier);
            if(l == null) {
                Trace.error("PullPushAdapter.handleMessage()", "received a messages tagged with identifier=" +
                        identifier + ", but there is no registration for that identifier. Will drop message");
            }
            else
                l.receive(msg);
        }
        else {
            if(listener != null)
                listener.receive(msg);
        }
    }


    protected void notifyViewChange(View v) {
        MembershipListener l;

        if(v == null) return;
        for(Iterator it=membership_listeners.iterator(); it.hasNext();) {
            l=(MembershipListener)it.next();
            try {
                l.viewAccepted(v);
            }
            catch(Throwable ex) {
                Trace.error("PullPushAdapter.notifyViewChange()", "exception notifying " + l + ": " + ex);
            }
        }
    }

    protected void notifySuspect(Address suspected_mbr) {
        MembershipListener l;

        if(suspected_mbr == null) return;
        for(Iterator it=membership_listeners.iterator(); it.hasNext();) {
            l=(MembershipListener)it.next();
            try {
                l.suspect(suspected_mbr);
            }
            catch(Throwable ex) {
                Trace.error("PullPushAdapter.notifySuspect()", "exception notifying " + l + ": " + ex);
            }
        }
    }

    protected void notifyBlock() {
        MembershipListener l;

        for(Iterator it=membership_listeners.iterator(); it.hasNext();) {
            l=(MembershipListener)it.next();
            try {
                l.block();
            }
            catch(Throwable ex) {
                Trace.error("PullPushAdapter.block()", "exception notifying " + l + ": " + ex);
            }
        }
    }


    public static final class PullHeader extends Header {
        Serializable identifier=null;

        public PullHeader() {
            ; // used by externalization
        }

        public PullHeader(Serializable identifier) {
            this.identifier=identifier;
        }

        public Serializable getIdentifier() {
            return identifier;
        }

        public long size() {
            return 128;
        }


        public String toString() {
            return "PullHeader";
        }

        public void writeExternal(ObjectOutput out) throws IOException {
            out.writeObject(identifier);
        }

        public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
            identifier=(Serializable)in.readObject();
        }

    }


}
