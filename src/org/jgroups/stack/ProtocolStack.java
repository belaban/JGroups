// $Id: ProtocolStack.java,v 1.1 2003/09/09 01:24:12 belaban Exp $

package org.jgroups.stack;

import java.util.Iterator;
import java.util.Map;
import java.util.Properties;
import java.util.Vector;
import org.jgroups.Transport;
import org.jgroups.Message;
import org.jgroups.util.TimeScheduler;
import org.jgroups.JChannel;
import org.jgroups.Event;
import org.jgroups.log.Trace;
import org.jgroups.conf.ClassConfigurator;
import org.jgroups.util.Queue;
import org.jgroups.util.Util;




/**
 * A ProtocolStack manages a number of protocols layered above each other. It creates all
 * protocol classes, initializes them and, when ready, starts all of them, beginning with the
 * bottom most protocol. It also dispatches messages received from the stack to registered
 * objects (e.g. channel, GMP) and sends messages sent by those objects down the stack.<p>
 * The ProtocolStack makes use of the Configurator to setup and initialize stacks, and to
 * destroy them again when not needed anymore
 * @author Bela Ban
 */
public class ProtocolStack extends Protocol implements Transport {
    private Protocol                top_prot=null;
    private Protocol                bottom_prot=null;
    private Configurator            conf=new Configurator();
    private Object                  mutex=new Object();
    private String                  setup_string;
    private JChannel                channel=null;
    private Object                  local_addr=null;
    private boolean                 stopped=true;
    public  TimeScheduler           timer=new TimeScheduler(5000);
    public static final int         ABOVE=1; // used by insertProtocol()
    public static final int         BELOW=2; // used by insertProtocol()



    public ProtocolStack(JChannel channel, String setup_string) {
        this.setup_string=setup_string;
        this.channel=channel;
        ClassConfigurator.getInstance(); // will create the singleton
    }




    /** Returns all protocols in a list, from top to bottom. <em>These are not copies of protocols,
     so modifications will affect the actual instances !</em> */
    public Vector getProtocols() {
        Protocol p;
        Vector   v=new Vector();

        p=top_prot;
        while(p != null) {
            v.addElement(p);
            p=p.getDownProtocol();
        }
        return v;
    }


    /**
     * Prints the names of the protocols, from the bottom to top. If include_properties is true,
     * the properties for each protocol will also be printed.
     */
    public String printProtocolSpec(boolean include_properties) {
        StringBuffer sb=new StringBuffer();
        Protocol     prot=top_prot;
        Properties   props;
        String       name;
        Map.Entry    entry;

        while(prot != null) {
            name=prot.getName();
            if(name != null) {
                if(name.equals("ProtocolStack"))
                    break;
                sb.append(name);
                if(include_properties) {
                    props=prot.getProperties();
                    if(props != null) {
                        sb.append("\n");
                        for(Iterator it=props.entrySet().iterator(); it.hasNext();) {
                            entry=(Map.Entry)it.next();
                            sb.append(entry + "\n");
                        }
                    }
                }
                sb.append("\n");

                prot=prot.getDownProtocol();
            }
        }

        return sb.toString();
    }


    public void setup() throws Exception {
        if(top_prot == null) {
            top_prot=conf.setupProtocolStack(setup_string, this);
            if(top_prot == null)
                throw new Exception("ProtocolStack.setup(): couldn't create protocol stack");
            top_prot.setUpProtocol(this);
            bottom_prot=conf.getBottommostProtocol(top_prot);
            conf.startProtocolStack(bottom_prot);        // sets up queues and threads
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
        return conf.createProtocol(prot_spec, this);
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
        conf.insertProtocol(prot, position, neighbor_prot, this);
    }





    /**
     * Removes a protocol from the stack. Stops the protocol and readjusts the linked lists of
     * protocols.
     * @param prot_name The name of the protocol. Since all protocol names in a stack have to be unique
     *                  (otherwise the stack won't be created), the name refers to just 1 protocol.
     * @exception Exception Thrown if the protocol cannot be stopped correctly.
     */
    public void removeProtocol(String prot_name) throws Exception {
        conf.removeProtocol(prot_name);
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


    public void destroy() {
        if(top_prot != null) {
            conf.stopProtocolStack(top_prot);           // destroys msg queues and threads
            top_prot=null;
        }
    }



    /**
     * Start all layers. The {@link Protocol#start()} method is called in each protocol,
     * <em>from top to bottom</em>.
     * Each layer can perform some initialization, e.g. create a multicast socket
     */
    public void start() throws Exception {
        Protocol p;
        Vector   prots=getProtocols();

        if(stopped == false) return;

        timer.start();

        synchronized(mutex) {
            for(int i=0; i < prots.size(); i++) {
                p=(Protocol)prots.elementAt(i);
                p.start();
            }
            stopped=false;
        }
    }



    public void startUpHandler() {
        // DON'T REMOVE !!!!  Avoids a superfluous thread
    }

    public void startDownHandler() {
        // DON'T REMOVE !!!!  Avoids a superfluous thread
    }


    /**
     * Iterates through all the protocols <em>from top to bottom</em> and does the following:
     * <ol>
     * <li>Waits until all messages in the down queue have been flushed (ie., size is 0)
     * <li>Calls stop() on the protocol
     * </ol>
     */
    public void stop() {
        if(timer != null) {
            try {
                timer.stop();
            }
            catch(Exception ex) {
            }
        }

        if(stopped) return;
        synchronized(mutex) {
            Protocol p;
            Vector   prots=getProtocols(); // from top to bottom
            Queue    down_queue;

            for(int i=0; i < prots.size(); i++) {
                p=(Protocol)prots.elementAt(i);

                // 1. Wait until down queue is empty
                down_queue=p.getDownQueue();
                if(down_queue != null) {
                    while(down_queue.size() > 0 && !down_queue.closed()) {
                        Util.sleep(100);  // FIXME: use a signal when empty (implement when switching to util.concurrent)
                    }
                }

                // 2. Call stop() on protocol
                p.stop();
            }
            stopped=true;
        }
    }

    public void stopInternal() {
        // do nothing, DON'T REMOVE !!!!
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




    public void up(Event evt) {
        switch(evt.getType()) {

        case Event.SET_LOCAL_ADDRESS:
            local_addr=evt.getArg();
            break;
        }

        if(channel != null)
            channel.up(evt);
    }




    public void down(Event evt) {
        if(top_prot != null)
            top_prot.receiveDownEvent(evt);
        else
            Trace.error("ProtocolStack.down()", "no down protocol available !");
    }



    protected void receiveUpEvent(Event evt) {
        up(evt);
    }



    /** Override with null functionality: we don't need any threads to be started ! */
    public void startWork() {}

    /** Override with null functionality: we don't need any threads to be started ! */
    public void stopWork()  {}


    /*----------------------- End of Protocol functionality ---------------------------*/




}
