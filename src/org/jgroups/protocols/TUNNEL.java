// $Id: TUNNEL.java,v 1.38 2007/05/07 16:36:41 vlada Exp $


package org.jgroups.protocols;


import java.io.DataInputStream;
import java.io.IOException;
import java.net.SocketException;
import java.util.Enumeration;
import java.util.Properties;

import org.jgroups.Address;
import org.jgroups.Event;
import org.jgroups.Message;
import org.jgroups.stack.RouterStub;
import org.jgroups.util.Util;


/**
 * Replacement for UDP. Instead of sending packets via UDP, a TCP connection is opened to a Router
 * (using the RouterStub client-side stub),
 * the IP address/port of which was given using channel properties <code>router_host</code> and
 * <code>router_port</code>. All outgoing traffic is sent via this TCP socket to the Router which
 * distributes it to all connected TUNNELs in this group. Incoming traffic received from Router will
 * simply be passed up the stack.
 * <p>A TUNNEL layer can be used to penetrate a firewall, most firewalls allow creating TCP connections
 * to the outside world, however, they do not permit outside hosts to initiate a TCP connection to a host
 * inside the firewall. Therefore, the connection created by the inside host is reused by Router to
 * send traffic from an outside host to a host inside the firewall.
 * @author Bela Ban
 */
public class TUNNEL extends TP {           
    private String router_host=null;
    private int router_port=0;     
    private RouterStub stub;    
    long reconnect_interval=5000; /** time to wait in ms between reconnect attempts */

    public TUNNEL() {
    	//loopback turned on is mandatory
    	loopback = true;
    }


    public String toString() {
        return "Protocol TUNNEL(local_addr=" + local_addr + ')';
    }
    /*------------------------------ Protocol interface ------------------------------ */

    public String getName() {
        return "TUNNEL";
    }

    public void start() throws Exception {
    	stub = new RouterStub(router_host,router_port,bind_addr);
    	stub.setConnectionListener(new StubConnectionListener());
        local_addr=stub.getLocalAddress();
        super.start();              
    }


    public void stop() {
    	super.stop();      
        teardownTunnel();       
        local_addr=null;        
    }



    /** Setup the Protocol instance acording to the configuration string */
    public boolean setProperties(Properties props) {
        String str;

        super.setProperties(props);
        str=props.getProperty("router_host");
        if(str != null) {
            router_host=str;
            props.remove("router_host");
        }

        str=props.getProperty("router_port");
        if(str != null) {
            router_port=Integer.parseInt(str);
            props.remove("router_port");
        }

        if(log.isDebugEnabled()) {
            log.debug("router_host=" + router_host + ";router_port=" + router_port);
        }

        if(router_host == null || router_port == 0) {
            if(log.isErrorEnabled()) {
                log.error("both router_host and router_port have to be set !");
                return false;
            }
        }

        str=props.getProperty("reconnect_interval");
        if(str != null) {
            reconnect_interval=Long.parseLong(str);
            props.remove("reconnect_interval");
        }
        
        if(!props.isEmpty()) {
            StringBuffer sb=new StringBuffer();
            for(Enumeration e=props.propertyNames(); e.hasMoreElements();) {
                sb.append(e.nextElement().toString());
                if(e.hasMoreElements()) {
                    sb.append(", ");
                }
            }
            if(log.isErrorEnabled()) log.error("The following properties are not recognized: " + sb);
            return false;
        }
        return true;
    }     

    /** Creates a TCP connection to the router */
    void createTunnel() throws Exception {
        if(router_host == null || router_port == 0)
            throw new Exception("router_host and/or router_port not set correctly; tunnel cannot be created");

       
        stub.connect(channel_name);            
    }


    /** Tears the TCP connection to the router down */
    void teardownTunnel() {
        stub.disconnect();
    }
    
    public Object handleDownEvent(Event evt) {
		Object retEvent = super.handleDownEvent(evt);
		switch(evt.getType()){
		case Event.CONNECT:
			try{
				createTunnel();				
			}catch(Exception e){
				if(log.isErrorEnabled())
					log.error("failed connecting to GossipRouter at " + router_host + ":"
							+ router_port);				
				break;
			}		
			break;

		case Event.DISCONNECT:			
			teardownTunnel();				
			break;
		}
		return retEvent;
	}
    
    private class StubConnectionListener implements RouterStub.ConnectionListener{

    	private volatile int currentState = RouterStub.STATUS_DISCONNECTED;
		public void connectionStatusChange(int newState) {
			if(currentState == RouterStub.STATUS_CONNECTED && newState == RouterStub.STATUS_CONNECTION_LOST){
				 Thread reconnector = new Thread(Util.getGlobalThreadGroup(), new TunnelReconnector(), "TUNNEL reconnector");
            	 reconnector.setDaemon(true);
            	 reconnector.start();	
		    }
			else if(currentState != RouterStub.STATUS_CONNECTED && newState == RouterStub.STATUS_CONNECTED){
				Thread receiver = new Thread(Util.getGlobalThreadGroup(), new TunnelReceiver(), "TUNNEL receiver");
				receiver.setDaemon(true);
				receiver.start();
			}
			currentState = newState;
		}    	
    }
    /* ------------------------------------------------------------------------------- */


    private class TunnelReceiver implements Runnable {
		public void run() {
			while(stub.isConnected()){
				Address dest = null;
				Address src = null; 
				int len;
				byte[] data = null;
				DataInputStream input = null;
				try{
					input = stub.getInputStream();
					dest = Util.readAddress(input);
					len = input.readInt();
					if(len > 0){
						data = new byte[len];
						input.readFully(data, 0, len);						
						receive(dest, src, data, 0, len);
					}
				}catch(SocketException se){
					//if(log.isWarnEnabled()) log.warn("failure in TUNNEL receiver thread", se);					
				}catch(IOException ioe){			
					 //if(log.isWarnEnabled()) log.warn("failure in TUNNEL receiver thread", ioe);					
				}catch(Exception e){
					if(log.isWarnEnabled())
						log.warn("failure in TUNNEL receiver thread", e);
				}
			}
		}
	}

    private class TunnelReconnector implements Runnable {       

        public void run() {
        	try{
				stub.reconnect(reconnect_interval);
			}catch(Exception e){
				if(log.isErrorEnabled())
					log.error(this + " failed reconnecting to GossipRouter at " + router_host + ":"
							+ router_port);
			}            
        }
    }	
	public void sendToAllMembers(byte[] data, int offset, int length) throws Exception {
		stub.sendToAllMembers(data, offset, length);		
	}


	public void sendToSingleMember(Address dest, byte[] data, int offset, int length) throws Exception {
		stub.sendToSingleMember(dest, data, offset, length);		
	}

	
	public String getInfo() {
		if(stub!=null)
			return stub.toString();
		else
			return "RouterStub not yet initialized";
	}


	public void postUnmarshalling(Message msg, Address dest, Address src, boolean multicast) {	
		msg.setDest(dest);
	}

	
	public void postUnmarshallingList(Message msg, Address dest, boolean multicast) {
		msg.setDest(dest);
	}
}
