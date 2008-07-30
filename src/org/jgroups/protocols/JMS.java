// $Id: JMS.java,v 1.24.4.1 2008/07/30 12:04:52 belaban Exp $ 

package org.jgroups.protocols;

import org.jgroups.Address;
import org.jgroups.Event;
import org.jgroups.Message;
import org.jgroups.View;
import org.jgroups.stack.Protocol;
import org.jgroups.util.Util;

import javax.naming.Context;
import javax.naming.InitialContext;
import java.io.*;
import java.util.Hashtable;
import java.util.Properties;
import java.util.Vector;

/**
 * Implementation of the transport protocol using the Java Message Service (JMS).
 * This implementation depends on the JMS server that will distribute messages
 * published to the specific topic to all topic subscribers.
 * <p>
 * Protocol parameters are:
 * <ul>
 * <li><code>topicName</code> - (required), full JNDI name of the topic to be 
 * used for message publishing;
 * 
 * <li><code>cf</code> - (optional), full JNDI name of the topic connection 
 * factory that will create topic connection, default value is 
 * <code>"ConnectionFactory"</code>;
 * 
 * <li><code>jndiCtx</code> - (optional), value of the 
 * <code>javax.naming.Context.INITIAL_CONTEXT_FACTORY</code> property; you can
 * specify it as the JVM system property 
 * <code>-Djava.naming.factory.initial=factory.class.Name</code>;
 * 
 * <li><code>providerURL</code> - (optional), value of the 
 * <code>javax.naming.Context.PROVIDER_URL</code> property; you can specify it 
 * as the JVM system property <code>-Djava.naming.provider.url=some_url</code>
 * 
 * <li><code>ttl</code> - (required), time to live in milliseconds. Default 
 * value is 0, that means that messages will never expire and will be 
 * accumulated by a JMS server.
 * 
 * </ul>
 * 
 * <p>
 * Note, when you are using the JMS protocol, try to avoid using protocols 
 * that open server socket connections, like FD_SOCK. I belive that FD is more
 * appropriate failure detector for JMS case.
 * 
 * @author Roman Rokytskyy (rrokytskyy@acm.org)
 */
public class JMS extends Protocol implements javax.jms.MessageListener {

    public static final
        String DEFAULT_CONNECTION_FACTORY = "ConnectionFactory";

    public static final
        String INIT_CONNECTION_FACTORY = "cf";

    public static final
        String INIT_TOPIC_NAME = "topicName";

    public static final
        String INIT_JNDI_CONTEXT = "jndiCtx";

    public static final
        String INIT_PROVIDER_URL = "providerURL";
        
    public static final
    	String TIME_TO_LIVE = "ttl";

    public static final
        String GROUP_NAME_PROPERTY = "jgroups_group_name";

    public static final
        String SRC_PROPERTY = "src";

    public static final
        String DEST_PROPERTY = "dest";

    private final Vector members = new Vector();

    private javax.jms.TopicConnectionFactory connectionFactory;
    private javax.jms.Topic topic;

    private javax.jms.TopicConnection connection;

    private javax.jms.TopicSession session;
    private javax.jms.TopicPublisher publisher;
    private javax.jms.TopicSubscriber subscriber;

    private String cfName;
    private String topicName;
    private String initCtxFactory;
    private String providerUrl;
    private long timeToLive;

    private Context ctx;

    private String group_addr;
    private Address local_addr;
    private Address mcast_addr;

    private final ByteArrayOutputStream out_stream = new ByteArrayOutputStream(65535);
    
    private static final java.util.Random RND = new java.util.Random();

    /**
     * Empty constructor. 
     */
    public JMS() {
    }

    /**
     * Get the name of the protocol.
     * 
     * @return always returns the <code>"JMS"</code> string.
     */
    public String getName() {
        return "JMS";
    }

    /**
     * Get the string representation of the protocol.
     * 
     * @return string representation of the protocol (not very useful though).
     */
    public String toString() {
        return "Protocol JMS(local address: " + local_addr + ')';
    }

    /**
     * Set protocol properties. Properties are:
     * <ul>
     * <li><code>topicName</code> - (required), full JNDI name of the topic to be 
     * used for message publishing;
     * 
     * <li><code>cf</code> - (optional), full JNDI name of the topic connection 
     * factory that will create topic connection, default value is 
     * <code>"ConnectionFactory"</code>;
     * 
     * <li><code>jndiCtx</code> - (optional), value of the 
     * <code>javax.naming.Context.INITIAL_CONTEXT_FACTORY</code> property; you can
     * specify it as the JVM system property 
     * <code>-Djava.naming.factory.initial=factory.class.Name</code>;
     * 
     * <li><code>providerURL</code> - (optional), value of the 
     * <code>javax.naming.Context.PROVIDER_URL</code> property; you can specify it 
     * as the JVM system property <code>-Djava.naming.provider.url=some_url</code>
     * </ul>
     * 
     */
    public boolean setProperties(Properties props) {
        super.setProperties(props);
        cfName = props.getProperty(INIT_CONNECTION_FACTORY,
                DEFAULT_CONNECTION_FACTORY);

        props.remove(INIT_CONNECTION_FACTORY);

        topicName = props.getProperty(INIT_TOPIC_NAME);

        if (topicName == null)
                throw new IllegalArgumentException(
                "JMS topic has not been specified.");

        props.remove(INIT_TOPIC_NAME);

        initCtxFactory = props.getProperty(INIT_JNDI_CONTEXT);
        props.remove(INIT_JNDI_CONTEXT);

        providerUrl = props.getProperty(INIT_PROVIDER_URL);
        props.remove(INIT_PROVIDER_URL);
        
        String ttl = props.getProperty(TIME_TO_LIVE);
        
        if (ttl == null) {
            if(log.isErrorEnabled()) log.error("ttl property not found.");
            return false;
        }
        
        props.remove(TIME_TO_LIVE);
        
        // try to parse ttl property
        try {
            
            timeToLive = Long.parseLong(ttl);
            
        } catch(NumberFormatException nfex) {
            if(log.isErrorEnabled()) log.error("ttl property does not contain numeric value.");
            
            return false;
        }

        return props.isEmpty();
    }




    /**
     * Implementation of the <code>javax.jms.MessageListener</code> interface.
     * This method receives the JMS message, checks the destination group name.
     * If the group name is the same as the group name of this channel, it 
     * checks the destination address. If destination address is either 
     * multicast or is the same as local address then message is unwrapped and
     * passed up the protocol stack. Otherwise it is ignored.
     * 
     * @param jmsMessage instance of <code>javax.jms.Message</code>.
     */
    public void onMessage(javax.jms.Message jmsMessage) {
        try {
            String groupName = jmsMessage.getStringProperty(GROUP_NAME_PROPERTY);

            // there might be other messages in this topic
            if (groupName == null)
                return;
            

                if(log.isDebugEnabled()) log.debug("Got message for group [" +
                groupName + ']' + ", my group is [" + group_addr + ']');

            // not our message, ignore it
            if (!group_addr.equals(groupName))
                return;

            JMSAddress src =
                jmsMessage.getStringProperty(SRC_PROPERTY) != null ?
                    new JMSAddress(jmsMessage.getStringProperty(SRC_PROPERTY)) :
                    null;

            JMSAddress dest =
                jmsMessage.getStringProperty(DEST_PROPERTY) != null ?
                    new JMSAddress(jmsMessage.getStringProperty(DEST_PROPERTY)) :
                    null;

            // if message is unicast message and I'm not the destination - ignore
            if (src != null && dest != null && 
                !dest.equals(local_addr) && !dest.isMulticastAddress())
                return;
            

            if (jmsMessage instanceof javax.jms.ObjectMessage) {
                byte[] buf = (byte[])((javax.jms.ObjectMessage)jmsMessage).getObject();

                ByteArrayInputStream inp_stream=new ByteArrayInputStream(buf);
                DataInputStream inp=new DataInputStream(inp_stream);
                    
                Message msg=new Message();
                msg.readFrom(inp);
                    
                Event evt=new Event(Event.MSG, msg);

                 // +++ remove
                    if(log.isDebugEnabled()) log.debug("Message is " + msg +
                        ", headers are " + msg.printHeaders());

                up_prot.up(evt);
            }
        } catch(javax.jms.JMSException ex) {
            ex.printStackTrace();
            if(log.isErrorEnabled()) log.error("JMSException : " + ex.toString());
        } catch(IOException ioex) {
            ioex.printStackTrace();
            if(log.isErrorEnabled()) log.error("IOException : " + ioex.toString());
        }
        catch(InstantiationException e) {
            e.printStackTrace();
        }
        catch(IllegalAccessException e) {
            e.printStackTrace();
        }
    }

    /**
     * Handle down event, if it is not a Event.MSG type.
     * 
     * @param evt event to handle.
     */
    protected Object handleDownEvent(Event evt) {
        switch(evt.getType()) {

            // we do not need this at present time, maybe in the future
            case Event.TMP_VIEW:
            case Event.VIEW_CHANGE:
                synchronized(members) {
                        members.removeAllElements();
                        Vector tmpvec=((View)evt.getArg()).getMembers();
                        for(int i=0; i < tmpvec.size(); i++)
                        members.addElement(tmpvec.elementAt(i));
                }
                break;

            case Event.CONNECT:
                group_addr=(String)evt.getArg();
                break;
        }
        return null;
    }

    /**
     * Called by the protocol above this. We check the event type, and if it is
     * message, we publish it in the topic, otherwise we let the 
     * {@link #handleDownEvent(Event)} take care of it.
     * 
     * @param evt event to process.
     */
    public Object down(Event evt) {

        if(log.isTraceEnabled())
            log.trace("event is " + evt + ", group_addr=" + group_addr + ", hdrs are " + Util.printEvent(evt));

        // handle all non-message events
        if(evt.getType() != Event.MSG) {
            return handleDownEvent(evt);
        }

        // extract message
        Message msg=(Message)evt.getArg();

        // publish the message to the topic
        sendMessage(msg);
        return null;
    }



    /**
     * Publish message in the JMS topic. We set the message source and 
     * destination addresses if they were <code>null</code>.
     * 
     * @param msg message to publish.
     */
    protected void sendMessage(Message msg) {
        try {
            if (msg.getSrc() == null)
                    msg.setSrc(local_addr);

            if (msg.getDest() == null)
                    msg.setDest(mcast_addr);


                    if(log.isInfoEnabled()) log.info("msg is " + msg);

            // convert the message into byte array.
            out_stream.reset();

            DataOutputStream out=new DataOutputStream(out_stream);
            msg.writeTo(out);
            out.flush();
            
            byte[] buf = out_stream.toByteArray();

            javax.jms.ObjectMessage jmsMessage = session.createObjectMessage();
            
            // set the payload
            jmsMessage.setObject(buf);
            
            // set the group name
            jmsMessage.setStringProperty(GROUP_NAME_PROPERTY, group_addr);

            // if the source is JMSAddress, copy it to the header
            if (msg.getSrc() instanceof JMSAddress)
                    jmsMessage.setStringProperty(
                            SRC_PROPERTY, msg.getSrc().toString());

            // if the destination is JMSAddress, copy it to the header
            if (msg.getDest() instanceof JMSAddress)
                    jmsMessage.setStringProperty(
                            DEST_PROPERTY, msg.getDest().toString());

            // publish message
            publisher.publish(jmsMessage);
                
        } catch(javax.jms.JMSException ex) {
                if(log.isErrorEnabled()) log.error("JMSException : " + ex.toString());
        } catch(IOException ioex) {
                if(log.isErrorEnabled()) log.error("IOException : " + ioex.toString());
        }
    }

    /**
     * Start the JMS protocol. This method instantiates the JNDI initial context
     * and looks up the topic connection factory and topic itself. If this step
     * is successful, it creates a connection to JMS server, opens a session
     * and obtains publisher and subscriber instances.
     * 
     * @throws javax.jms.JMSException if something goes wrong with JMS.
     * @throws javax.naming.NamingException if something goes wrong with JNDI.
     * @throws IllegalArgumentException if the connection factory or topic
     * cannot be found under specified names.
     */
    public void start() throws Exception 
    {
        if (initCtxFactory != null && providerUrl != null) {
            Hashtable env = new Hashtable();
            env.put(Context.INITIAL_CONTEXT_FACTORY, initCtxFactory);
            env.put(Context.PROVIDER_URL, providerUrl);

            ctx = new InitialContext(env);
        } else
            ctx = new InitialContext();

        connectionFactory = (javax.jms.TopicConnectionFactory)ctx.lookup(cfName);
        
        if (connectionFactory == null)
            throw new IllegalArgumentException(
                    "Topic connection factory cannot be found in JNDI.");
        
        topic = (javax.jms.Topic)ctx.lookup(topicName);
        
        if (topic == null)
            throw new IllegalArgumentException("Topic cannot be found in JNDI.");

        connection = connectionFactory.createTopicConnection();

        boolean addressAssigned = false;
        
        // check if JMS connection contains client ID, 
        // if not, try to assign randomly generated one
        /*while(!addressAssigned) {
            if (connection.getClientID() != null)
                addressAssigned = true;
            else
                try {
                    connection.setClientID(generateLocalAddress());
                    addressAssigned = true;
                } catch(javax.jms.InvalidClientIDException ex) {
                    // duplicate... ok, let's try again                    
                }
        }*/


        // Patch below submitted by Greg Woolsey
        // Check if JMS connection contains client ID, if not, try to assign randomly generated one
        // setClientID() must be the first method called on a new connection, per the JMS spec.
        // If the client ID is already set, this will throw IllegalStateException and keep the original value.
        while(!addressAssigned) {
            try {
                connection.setClientID(generateLocalAddress());
                addressAssigned = true;
            } catch (javax.jms.IllegalStateException e) {
                // expected if connection already has a client ID.
                addressAssigned = true;
            } catch(javax.jms.InvalidClientIDException ex) {
                // duplicate... OK, let's try again
            }
        }

        local_addr = new JMSAddress(connection.getClientID(), false);
        mcast_addr = new JMSAddress(topicName, true);

        session = connection.createTopicSession(false,
                                                javax.jms.Session.AUTO_ACKNOWLEDGE);

        publisher = session.createPublisher(topic);
        publisher.setTimeToLive(timeToLive);
        
        subscriber = session.createSubscriber(topic);
        subscriber.setMessageListener(this);
        
        connection.start();

        up_prot.up(new Event(Event.SET_LOCAL_ADDRESS, local_addr));
    }

    /**
     * Stops the work of the JMS protocol. This method closes JMS session and
     * connection and deregisters itself from the message notification.
     */
    public void stop() {

            if(log.isInfoEnabled()) log.info("finishing JMS transport layer.");

        try {
            connection.stop();
            subscriber.setMessageListener(null);
            session.close();
            connection.close();
        }
        catch(Throwable ex) {
            if(log.isErrorEnabled()) log.error("exception is " + ex);
        }
    }
    
    /**
     * Generate random local address. This method takes host name and appends
     * it with randomly generated integer.
     * 
     * @return randomly generated local address.
     */
    protected String generateLocalAddress() throws java.net.UnknownHostException {
        String  hostName = java.net.InetAddress.getLocalHost().getHostName();
       
        int rndPort = RND.nextInt(65535);
        
        return hostName + ':' + rndPort;
    }

    /**
     * Simple {@link Address} representing the JMS node ID or JMS topic group.
     */
    public static class JMSAddress implements Address {
        
    	private static final long serialVersionUID = -2311584492745452246L;
		
    	private String address;
        private boolean isMCast;


        /**
         * Empty constructor to allow externalization work.
         */
        public JMSAddress() {
        }


        /**
         * Create instance of this class for given address string.
         * 
         * Current implementation uses a hash mark <code>'#'</code> to determine
         * if the address is a unicast or multicast. Therefore, this character is
         * considered as reserved and is not allowed in the <code>address</code>
         * parameter passed to this constructor.
         * 
         * @param address string representing the address of the node connected
         * to the JMS topic, usually, a value of 
         * <code>connection.getClientID()</code>, where the connection is 
         * instance of <code>javax.jms.TopicConnection</code>.
         * 
         * @param isMCast <code>true</code> if the address is multicast address,
         * otherwise - <code>false</code>.
         */
        JMSAddress(String address, boolean isMCast) {
            this.address = address;
            this.isMCast = isMCast;
        }



        /**
         * Reconstruct the address from the string representation. If the 
         * <code>str</code> starts with <code>'#'</code>, address is considered
         * as unicast, and node address is the substring after <code>'#'</code>.
         * Otherwise, address is multicast and address is <code>str</code> 
         * itself.
         * 
         * @param str string used to reconstruct the instance.
         */
        JMSAddress(String str) {
            if (str.startsWith("#")) {
                address = str.substring(1);
                isMCast = false;
            } else {
                address = str;
                isMCast = true;
            }
        }

        /**
         * Get the node address.
         * 
         * @return node address in the form passed to the constructor
         * {@link #JMS.JMSAddress(String, boolean)}.
         */
        public String getAddress() { return address; }

        /**
         * Set the node address.
         * 
         * @param address new node address.
         */
        public void setAddress(String address) { this.address = address; }

        /**
         * Is the address a multicast address?
         * 
         * @return <code>true</code> if the address is multicast address.
         */
        public boolean isMulticastAddress() {
            return isMCast;
        }

        public int size() {
            return 22;
        }

        /**
         * Clone the object.
         */
        protected Object clone() throws CloneNotSupportedException {
            return new JMSAddress(address, isMCast);
        }

        /**
         * Compare this object to <code>o</code>. It is possible to compare only
         * addresses of the same class. Also they both should be either 
         * multicast or unicast addresses.
         * 
         * @return value compliant with the {@link Comparable#compareTo(Object)}
         * specififaction.
         */
        public int compareTo(Object o) throws ClassCastException {
            if (!(o instanceof JMSAddress))
                throw new ClassCastException("Cannot compare different classes.");

            JMSAddress that = (JMSAddress)o;

            if (that.isMCast != this.isMCast)
                throw new ClassCastException(
                    "Addresses are different: one is multicast, and one is not");

            return this.address.compareTo(that.address);
        }

        /**
         * Test is this object is equal to <code>obj</code>. 
         * 
         * @return <code>true</code> iff the <code>obj</code> is 
         * <code>JMSAddress</code>, node addresses are equal and they both are
         * either multicast or unicast addresses.
         */
        public boolean equals(Object obj) {
            if (obj == this) return true;

            if (!(obj instanceof JMSAddress))
                    return false;

            JMSAddress that = (JMSAddress)obj;

            if (this.isMCast) 
                return this.isMCast == that.isMCast;
            else
                return !(this.address == null || that.address == null) && this.address.equals(that.address) &&
                        this.isMCast == that.isMCast;
        }

        /**
         * Get the hash code of this address.
         * 
         * @return hash code of this object.
         */
        public int hashCode() {
            return toString().hashCode();
        }

        /**
         * Read object from external input.
         */
        public void readExternal(ObjectInput in) 
            throws IOException, ClassNotFoundException 
        {
            address = (String)in.readObject();
            isMCast = in.readBoolean();
        }

        /**
         * Get the string representation of the address. The following property
         * holds: <code>a2.equals(a1)</code> is always <code>true</code>, where
         * <code>a2</code> is 
         * <code>JMSAddress a2 = new JMSAddress(a1.toString());</code>
         * 
         * @return string representation of the address.
         */
        public String toString() {
            return !isMCast ? '#' + address : address;
        }

        /**
         * Write the object to external output.
         */
        public void writeExternal(ObjectOutput out) throws IOException {
            out.writeObject(address);
            out.writeBoolean(isMCast);
        }


        public void writeTo(DataOutputStream outstream) throws IOException {
            outstream.writeUTF(address);
            outstream.writeBoolean(isMCast);
        }

        public void readFrom(DataInputStream instream) throws IOException, IllegalAccessException, InstantiationException {
            address=instream.readUTF();
            isMCast=instream.readBoolean();
        }
    }

}
