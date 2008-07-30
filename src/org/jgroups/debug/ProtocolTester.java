// $Id: ProtocolTester.java,v 1.16 2008/07/30 15:10:20 vlada Exp $

package org.jgroups.debug;


import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jgroups.Event;
import org.jgroups.JChannel;
import org.jgroups.Message;
import org.jgroups.stack.Configurator;
import org.jgroups.stack.Protocol;
import org.jgroups.stack.ProtocolStack;
import org.jgroups.util.Util;

import java.util.LinkedList;
import java.util.List;
import java.util.Vector;
import java.util.ArrayList;


/**
 * Generic class to test one or more protocol layers directly. Sets up a protocol stack consisting of
 * the top layer (which is essentially given by the user and is the test harness), the specified protocol(s) and
 * a bottom layer (which is automatically added), which sends all received messages immediately back up the
 * stack (swapping sender and receiver address of the message).
 * 
 * @author Bela Ban
 * @author March 23 2001
 */
public class ProtocolTester {
    Protocol harness=null, top, bottom;
    String props=null;
    Configurator config=null;

    protected final Log log=LogFactory.getLog(this.getClass());


    public ProtocolTester(String prot_spec, Protocol harness) throws Exception {
        if(prot_spec == null || harness == null)
            throw new Exception("ProtocolTester(): prot_spec or harness is null");

        props=prot_spec;
        this.harness=harness;
        props="LOOPBACK:" + props; // add a loopback layer at the bottom of the stack

        config=new Configurator();
        JChannel mock_channel=new JChannel() {};
        ProtocolStack stack=new ProtocolStack(mock_channel,props);
        stack.setup();
        stack.insertProtocol(harness, ProtocolStack.ABOVE, stack.getTopProtocol().getClass());
        
        bottom=stack.getBottomProtocol();

        // has to be set after StartProtocolStack, otherwise the up and down handler threads in the harness
        // will be started as well (we don't want that) !
        // top.setUpProtocol(harness);
    }

    public Vector<Protocol> getProtocols() {
        Vector<Protocol> retval=new Vector<Protocol>();
        Protocol tmp=top;
        while(tmp != null) {
            retval.add(tmp);
            tmp=tmp.getDownProtocol();
        }
        return retval;
    }

    public String getProtocolSpec() {
        return props;
    }


    public Protocol getBottom() {
        return bottom;
    }

    public Protocol getTop() {
        return top;
    }

    public void start() throws Exception {
        Protocol p;
        if(harness != null) {
            p=harness;
            while(p != null) {
                p.start();
                p=p.getDownProtocol();
            }
        }
        else if(top != null) {
            p=top;
            while(p != null) {
                p.start();
                p=p.getDownProtocol();
            }
        }
    }

    public void stop() {
        Protocol p;
        if(harness != null) {
            List<Protocol> protocols=new LinkedList<Protocol>();
            p=harness;
            while(p != null) {
                protocols.add(p);
                p.stop();
                p=p.getDownProtocol();
            }
            p=harness;
            while(p != null) {                
                p.destroy();
                p=p.getDownProtocol();
            }            
        }
        else if(top != null) {
            p=top;
            List<Protocol> protocols=new LinkedList<Protocol>();
            while(p != null) {
                protocols.add(p);
                p.stop();
                p=p.getDownProtocol();
            }    
            p=top;
            while(p != null) {                
                p.destroy();
                p=p.getDownProtocol();
            }    
        }
    }


    private final Protocol getBottomProtocol(Protocol top) {
        Protocol tmp;

        if(top == null)
            return null;

        tmp=top;
        while(tmp.getDownProtocol() != null)
            tmp=tmp.getDownProtocol();
        return tmp;
    }


    public static void main(String[] args) {
        String props;
        ProtocolTester t;
        Harness h;

        if(args.length < 1 || args.length > 2) {
            System.out.println("ProtocolTester <protocol stack spec> [-trace]");
            return;
        }
        props=args[0];

        try {
            h=new Harness();
            t=new ProtocolTester(props, h);
            System.out.println("protocol specification is " + t.getProtocolSpec());
            h.down(new Event(Event.BECOME_SERVER));
            for(int i=0; i < 5; i++) {
                System.out.println("Sending msg #" + i);
                h.down(new Event(Event.MSG, new Message(null, null, "Hello world #" + i)));
            }
            Util.sleep(500);
            t.stop();
        }
        catch(Exception ex) {
            System.err.println(ex);
        }
    }




    private static class Harness extends Protocol {

        public String getName() {
            return "Harness";
        }


        public Object up(Event evt) {
            System.out.println("Harness.up(): " + evt);
            return null;
        }

    }

}
