// $Id$

package org.jgroups.protocols.pbcast;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Vector;

import org.jgroups.Address;
import org.jgroups.Event;
import org.jgroups.Header;
import org.jgroups.Message;
import org.jgroups.View;
import org.jgroups.stack.IpAddress;
import org.jgroups.stack.Protocol;
import org.jgroups.stack.StateTransferInfo;
import org.jgroups.util.Streamable;
import org.jgroups.util.Util;

import EDU.oswego.cs.dl.util.concurrent.BoundedBuffer;
import EDU.oswego.cs.dl.util.concurrent.PooledExecutor;


/**
 * 
 */
public class STREAMING_STATE_TRANSFER extends Protocol {
    Address        local_addr=null;
    final Vector   members=new Vector();
    long           state_id=1; 
    
    final List     state_requesters=new ArrayList();

    /** set to true while waiting for a STATE_RSP */
    boolean        waiting_for_state_response=false;

    Digest         digest=null;
    final HashMap  map=new HashMap(); // to store configuration information
    long           start, stop; // to measure state transfer time
    int            num_state_reqs=0;
    final static   String NAME="STREAMING_STATE_TRANSFER";
    
	private InetAddress bind_addr;
	private int port = 0;
	private StateProviderThreadSpawner spawner;
	private int max_pool;
	private int pool_buffer_length;	
	private long pool_thread_keep_alive;
	private int socket_buffer_size;
    
    public String getName() {
        return NAME;
    }

    public Vector requiredDownServices() {
        Vector retval=new Vector();
        retval.addElement(new Integer(Event.GET_DIGEST_STATE));
        retval.addElement(new Integer(Event.SET_DIGEST));
        return retval;
    }

    public boolean setProperties(Properties props) {
        super.setProperties(props);
        
        try {
			bind_addr = Util.parseBindAddress(props,"bind_addr");
		} catch (UnknownHostException e) {
			 log.error("(bind_addr): host " + e.getLocalizedMessage() + " not known");
             return false;
		}	
        port = Util.parseInt(props,"start_port",0);
        socket_buffer_size = Util.parseInt(props,"socket_buffer_size",8*1024); //8K
        max_pool = Util.parseInt(props,"max_pool",5);  
        pool_buffer_length = Util.parseInt(props,"pool_buffer_length",2);        
        pool_thread_keep_alive = Util.parseLong(props,"pool_thread_keep_alive",2000);        
        if(props.size() > 0) {
            log.error("the following properties are not recognized: " + props);

            return false;
        }
        return true;
    }

    public void init() throws Exception {
        map.put("state_transfer", Boolean.TRUE);
        map.put("protocol_class", getClass().getName());
    }


    public void start() throws Exception {
        passUp(new Event(Event.CONFIG, map));
    }

    public void stop() {
        super.stop();
        waiting_for_state_response=false;
        if(spawner!=null)
        {
        	spawner.stop();
        }
    }


    public void up(Event evt) {
		switch (evt.getType()) {
			case Event.BECOME_SERVER:
				break;

			case Event.SET_LOCAL_ADDRESS:
				local_addr = (Address) evt.getArg();
				break;

			case Event.TMP_VIEW:
			case Event.VIEW_CHANGE:
				handleViewChange((View) evt.getArg());
				break;

			case Event.GET_DIGEST_STATE_OK:
				synchronized (state_requesters) {
					digest = (Digest) evt.getArg();
					if (log.isDebugEnabled())
						log.debug("GET_DIGEST_STATE_OK: digest is " + digest);
				}
				respondToStateRequester();
				return;

			case Event.MSG:
				Message msg = (Message) evt.getArg();
				StateHeader hdr = (StateHeader) msg.removeHeader(getName());
				if (hdr != null) {
					switch (hdr.type) {
						case StateHeader.STATE_REQ:
							handleStateReq(hdr);
							break;
						case StateHeader.STATE_RSP:
							handleStateRsp(hdr);
							break;
						default:
							if (log.isErrorEnabled())
								log.error("type " + hdr.type
										+ " not known in StateHeader");
							break;
					}
					return;
				}
				break;
			case Event.CONFIG:
				if (bind_addr == null) {
					Map config = (Map) evt.getArg();
					bind_addr = (InetAddress) config.get("bind_addr");
					if (log.isDebugEnabled())
						log.debug("using bind_addr from CONFIG event "
								+ bind_addr);
				}
				break;
		}
		passUp(evt);
    }    

	public void down(Event evt) {
        Address target;
        StateTransferInfo info;        

        switch(evt.getType()) {

            case Event.TMP_VIEW:
            case Event.VIEW_CHANGE:
                handleViewChange((View)evt.getArg());
                break;
                
            case Event.GET_STATE:
                info=(StateTransferInfo)evt.getArg();
                if(info.target == null) {
                    target=determineCoordinator();
                }
                else {
                    target=info.target;
                    if(target.equals(local_addr)) {
                        if(log.isErrorEnabled()) log.error("GET_STATE: cannot fetch state from myself !");
                        target=null;
                    }
                }
                if(target == null) {
                    if(log.isDebugEnabled()) log.debug("GET_STATE: first member (no state)");
                    passUp(new Event(Event.GET_STATE_OK, new StateTransferInfo()));
                }
                else {
                    Message state_req=new Message(target, null, null);
                    state_req.putHeader(NAME, new StateHeader(StateHeader.STATE_REQ, local_addr));
                    if(log.isDebugEnabled()) log.debug("GET_STATE: asking " + target + " for state");

                    // suspend sending and handling of mesage garbage collection gossip messages,
                    // fixes bugs #943480 and #938584). Wake up when state has been received
                    if(log.isDebugEnabled())
                        log.debug("passing down a SUSPEND_STABLE event");
                    passDown(new Event(Event.SUSPEND_STABLE, new Long(info.timeout)));
                    waiting_for_state_response=true;
                    start=System.currentTimeMillis();
                    passDown(new Event(Event.MSG, state_req));
                }
                return;                 // don't pass down any further !
        }

        passDown(evt);              // pass on to the layer below us
    }









    /* --------------------------- Private Methods -------------------------------- */


	private void respondToStateRequester() {

		// setup the plumbing if needed
		if (spawner==null) {			
			ServerSocket serverSocket = Util.createServerSocket(bind_addr, port); 				
			spawner = new StateProviderThreadSpawner(setupThreadPool(), serverSocket);
			new Thread(spawner,"StateProviderThreadSpawner").start();			
		}
		
		synchronized (state_requesters) {
			if (state_requesters.isEmpty()) {
				if (warn)
					log.warn("Should be responding to state requester, but there are no requesters !");
				return;
			}
			if (digest == null)
				if (warn)
					log.warn("Should be responding to state requester, but there is no digest !");
				else
					digest = digest.copy();
			if (stats) {
				num_state_reqs++;
			}
			
			if (log.isDebugEnabled())
				log.debug("Iterating state requesters " + state_requesters);
			
			Address requester = null;
			for (Iterator it = state_requesters.iterator(); it.hasNext();) {
				requester = (Address) it.next();
				final Message state_rsp = new Message(requester);
				StateHeader hdr = new StateHeader(StateHeader.STATE_RSP,
						local_addr,spawner.getServerSocketAddress(), digest);
				state_rsp.putHeader(NAME, hdr);

				// This has to be done in a separate thread, so we don't block
				// on FC
				// (see http://jira.jboss.com/jira/browse/JGRP-225 for details).
				// This will be reverted once
				// we have the threadless stack
				// (http://jira.jboss.com/jira/browse/JGRP-181)
				// and out-of-band messages
				// (http://jira.jboss.com/jira/browse/JGRP-205)
				if (log.isDebugEnabled())
					log.debug("Responding to state requester "
							+ requester + " with address "
							+ spawner.getServerSocketAddress()
							+ " and digest " + digest);
				new Thread() {					
					public void run() {						
						passDown(new Event(Event.MSG, state_rsp));
					}
				}.start();			
				it.remove();			
			}
		}
	}	

	private PooledExecutor setupThreadPool() {
		PooledExecutor threadPool = new PooledExecutor(new BoundedBuffer(
				pool_buffer_length), max_pool);
		threadPool.setMinimumPoolSize(1);
		threadPool.setKeepAliveTime(pool_thread_keep_alive);
		return threadPool;
	}
	  
    private Address determineCoordinator() {
        Address ret=null;
        synchronized(members) {
            if(members != null && !members.isEmpty()) {
                for(int i=0; i < members.size(); i++)
                    if(!local_addr.equals(members.elementAt(i)))
                        return (Address)members.elementAt(i);
            }
        }
        return ret;
    }


    private void handleViewChange(View v) {
        Address old_coord;
        Vector new_members=v.getMembers();
        boolean send_up_null_state_rsp=false;

        synchronized(members) {
            old_coord=(Address)(members.size() > 0? members.firstElement() : null);
            members.clear();
            members.addAll(new_members);

            // this handles the case where a coord dies during a state transfer; prevents clients from hanging forever
            // Note this only takes a coordinator crash into account, a getState(target, timeout), where target is not
            // null is not handled ! (Usually we get the state from the coordinator)
            // http://jira.jboss.com/jira/browse/JGRP-148
            if(waiting_for_state_response && old_coord != null && !members.contains(old_coord)) {
                send_up_null_state_rsp=true;
            }
        }

        if (send_up_null_state_rsp) {
			log.warn("discovered that the state provider (" + old_coord
					+ ") crashed; will return null state to application");
		}
    }
   
    private void handleStateReq(StateHeader hdr) {
        Object sender=hdr.sender;
        if(sender == null) {
            if(log.isErrorEnabled()) log.error("sender is null !");
            return;
        }        
        synchronized(state_requesters) {
            boolean empty=state_requesters.isEmpty();
            state_requesters.add(sender);
            if (empty){               
                digest=null;
                if(log.isDebugEnabled()) log.debug("passing down GET_DIGEST_STATE");
                passDown(new Event(Event.GET_DIGEST_STATE));
            }
        }
    }
    
    void handleStateRsp(StateHeader hdr) {		
		Digest tmp_digest = hdr.my_digest;

		waiting_for_state_response = false;
		if (tmp_digest == null) {
			if (warn)
				log.warn("digest received from " + hdr.sender
						+ " is null, skipping setting digest !");
		} else {
			passDown(new Event(Event.SET_DIGEST, tmp_digest)); // set the
																// digest (e.g.
																// in NAKACK)
		}
		stop = System.currentTimeMillis();
		connectToStateProvider(hdr.bind_addr);
	}
    

    private void connectToStateProvider(IpAddress address) {	
    	Socket socket = null; 
    	StreamingInputStreamWrapper wrapper = null;
		try {
			socket = new Socket();			
			int bufferSize = socket.getReceiveBufferSize();
			socket.setReceiveBufferSize(socket_buffer_size);
			if (log.isDebugEnabled())
				log.debug("Connecting to state provider "
					+ address.getIpAddress()
					+ ":"
					+ address.getPort()
					+ ", original buffer size was "
					+ bufferSize
					+ " and was reset to "
					+ socket.getReceiveBufferSize());
			socket.connect(new InetSocketAddress(address.getIpAddress(),address.getPort()));			
			if (log.isDebugEnabled())
				log.debug("Connected to state provider, my end of the socket is "
						+ socket.getLocalAddress() + ":"
						+ socket.getLocalPort() + " passing inputstream up...");
			wrapper = new StreamingInputStreamWrapper(socket,socket.getInputStream());
			passUp(new Event(Event.STATE_TRANSFER_INPUTSTREAM,wrapper));
		} catch (IOException e) {
			if(log.isErrorEnabled())
			{
				log.error("State reader socket thread spawned abnormaly",e);
			}
		}
		finally
		{
			if(!socket.isConnected())
			{
				if (log.isErrorEnabled())
					log.error("Could not connect to state provider. Closing socket...");
				try {
					if (wrapper != null) {
						wrapper.close();
					} else {
						socket.close();
					}
						
				} catch (IOException e) {}
			}			
		}
	}

    /*
	 * ------------------------ End of Private Methods
	 * ------------------------------
	 */

    private class StateProviderThreadSpawner implements Runnable
	{		
		PooledExecutor pool;
		ServerSocket serverSocket;
		IpAddress address;
		volatile boolean running = true;
	
		public StateProviderThreadSpawner(PooledExecutor pool, ServerSocket stateServingSocket) {
			super();			
			this.pool = pool;
			this.serverSocket = stateServingSocket;
			this.address = new IpAddress(STREAMING_STATE_TRANSFER.this.bind_addr, 
										 serverSocket.getLocalPort());
		}
		
		
		
		public void run() {
			for (;running;) {				
				try {	
					if (log.isDebugEnabled())
						log.debug("StateProviderThreadSpawner listening at "
								+ getServerSocketAddress() + "...");
					final Socket socket = serverSocket.accept();										
					pool.execute(new Runnable() {
						public void run() {		
							if (log.isDebugEnabled())
								log.debug("Accepted request for state transfer from "
												+ socket.getInetAddress()
												+ ":"
												+ socket.getPort()
												+ " handing of to PooledExecutor thread");
							new StateProviderHandler().process(socket);
						}
					});
					
				} catch (IOException e) {
					if (log.isErrorEnabled())
					{
						//we get this exception when we close server socket
						//exclude that case
						if(serverSocket!=null && !serverSocket.isClosed())
						{
							log.error("Spawning socket from server socket finished abnormaly",e);
						}
					}
				} catch (InterruptedException e) {
					// should not happen
				} 					
			}
		}


		public IpAddress getServerSocketAddress() {
			return address;
		}
		
		public void stop() {
			running = false;
			try {
				if (serverSocket != null && !serverSocket.isClosed()) {
					serverSocket.close();
				}
			} catch (IOException e) {
			} finally {
				if (log.isDebugEnabled())
					log.debug("Shutting the thread pool down... ");
				pool.shutdownNow();
			}
		}		
	}
    private class StateProviderHandler
    {
    	public void process(Socket socket)
    	{    		
    		StreamingOutputStreamWrapper wrapper = null;
    		try {	
    			int bufferSize = socket.getSendBufferSize();					
				socket.setSendBufferSize(socket_buffer_size);
				if (log.isDebugEnabled())
					log.debug("Accepted request for state transfer from "
									+ socket.getInetAddress()
									+ ":"
									+ socket.getPort()
									+ ", original buffer size was "
									+ bufferSize
									+ " and was reset to "
									+ socket.getSendBufferSize()+ 
									", passing outputstream up... ");
				
				wrapper = new StreamingOutputStreamWrapper(socket, socket.getOutputStream());
				passUp(new Event(Event.STATE_TRANSFER_OUTPUTSTREAM,wrapper));
			} catch (IOException e) {				
				if(log.isErrorEnabled())
				{
					log.error("State writer socket thread spawned abnormaly",e);
				}
			}
			finally
			{
				if (socket!=null && !socket.isConnected()) {
					if (log.isErrorEnabled())
						log.error("Accepted request for state transfer but socket "
										+ socket
										+ " not connected properly. Closing it...");
					try {
						if (wrapper != null) {
							wrapper.close();
						}
						else{
							socket.close();
						}
					} catch (IOException e) {
					}
				}
			}
    	}
    }
    
    private class StreamingInputStreamWrapper extends InputStream
    {
    	
    	private Socket inputStreamOwner;
    	private InputStream delegate;
    	
		public StreamingInputStreamWrapper(Socket inputStreamOwner, InputStream delegate) {
			super();			
			this.inputStreamOwner = inputStreamOwner;
			this.delegate = delegate;
		}

		public int available() throws IOException {			
			return delegate.available();
		}

		public void close() throws IOException {
			if (log.isDebugEnabled()) {
				log.debug("State reader " + inputStreamOwner + " is closing the socket ");
			}
			inputStreamOwner.close();
		}

		public synchronized void mark(int readlimit) {			
			delegate.mark(readlimit);
		}

		public boolean markSupported() {			
			return delegate.markSupported();
		}

		public int read() throws IOException {			
			return delegate.read();
		}

		public int read(byte[] b, int off, int len) throws IOException {			
			return delegate.read(b, off, len);
		}

		public int read(byte[] b) throws IOException {			
			return delegate.read(b);
		}

		public synchronized void reset() throws IOException {			
			delegate.reset();
		}

		public long skip(long n) throws IOException {			
			return delegate.skip(n);
		}    	    	
    }
    
    private class StreamingOutputStreamWrapper extends OutputStream
    {
    	private Socket outputStreamOwner;
    	private OutputStream delegate;
    	
		public StreamingOutputStreamWrapper(Socket outputStreamOwner, OutputStream delegate) {
			super();			
			this.outputStreamOwner = outputStreamOwner;
			this.delegate = delegate;
		}

		public void close() throws IOException {
			if (log.isDebugEnabled()) {
				log.debug("State writer " + outputStreamOwner + " is closing the socket ");
			}
			outputStreamOwner.close();
		}

		public void flush() throws IOException {			
			delegate.flush();
		}

		public void write(byte[] b, int off, int len) throws IOException {			
			delegate.write(b, off, len);
		}

		public void write(byte[] b) throws IOException {			
			delegate.write(b);
		}

		public void write(int b) throws IOException {			
			delegate.write(b);
		}
    }
    
    public static class StateHeader extends Header implements Streamable {
        public static final byte STATE_REQ=1;
        public static final byte STATE_RSP=2;


        long    id=0;               // state transfer ID (to separate multiple state transfers at the same time)
        byte    type=0;
        Address sender;             // sender of state STATE_REQ or STATE_RSP
        Digest  my_digest=null;     // digest of sender (if type is STATE_RSP)
		IpAddress bind_addr;
                

        public StateHeader() {  // for externalization
        }

        public StateHeader(byte type, Address sender) {
            this.type=type;
            this.sender=sender;
        }
        
        public StateHeader(byte type, Address sender, long id, Digest digest) {
            this.type=type;
            this.sender=sender;
            this.id=id;
            this.my_digest=digest;
        }    
        
        public StateHeader(byte type, Address sender, IpAddress bind_addr, Digest digest) {
            this.type=type;
            this.sender=sender;
            this.my_digest=digest;
            this.bind_addr=bind_addr;
        }     

        public int getType() {
            return type;
        }

        public Digest getDigest() {
            return my_digest;
        }     


        public boolean equals(Object o) {
            StateHeader other;

            if(sender != null && o != null) {
                if(!(o instanceof StateHeader))
                    return false;
                other=(StateHeader)o;
                return sender.equals(other.sender) && id == other.id;
            }
            return false;
        }


        public int hashCode() {
            if(sender != null)
                return sender.hashCode() + (int)id;
            else
                return (int)id;
        }


        public String toString() {
            StringBuffer sb=new StringBuffer();
            sb.append("type=").append(type2Str(type));
            if(sender != null) sb.append(", sender=").append(sender).append(" id=").append(id);
            if(my_digest != null) sb.append(", digest=").append(my_digest);            
            return sb.toString();
        }


        static String type2Str(int t) {
            switch(t) {
                case STATE_REQ:
                    return "STATE_REQ";
                case STATE_RSP:
                    return "STATE_RSP";
                default:
                    return "<unknown>";
            }
        }


        public void writeExternal(ObjectOutput out) throws IOException {
            out.writeObject(sender);
            out.writeLong(id);
            out.writeByte(type);
            out.writeObject(my_digest); 
            out.writeObject(bind_addr);
        }


        public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
            sender=(Address)in.readObject();
            id=in.readLong();
            type=in.readByte();
            my_digest=(Digest)in.readObject();  
            bind_addr=(IpAddress)in.readObject();
        }



        public void writeTo(DataOutputStream out) throws IOException {
            out.writeByte(type);
            out.writeLong(id);
            Util.writeAddress(sender, out);
            Util.writeStreamable(my_digest, out);  
            Util.writeStreamable(bind_addr, out);
        }

        public void readFrom(DataInputStream in) throws IOException, IllegalAccessException, InstantiationException {
            type=in.readByte();
            id=in.readLong();
            sender=Util.readAddress(in);
            my_digest=(Digest)Util.readStreamable(Digest.class, in);   
            bind_addr=(IpAddress)Util.readStreamable(IpAddress.class,in);
        }        

    }


}
