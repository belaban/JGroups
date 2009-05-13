// $Id: ReplicatedTree2.java,v 1.3 2009/05/13 13:06:54 belaban Exp $

package org.jgroups.blocks;


import org.jgroups.logging.Log;
import org.jgroups.logging.LogFactory;
import org.jgroups.*;
import org.jgroups.annotations.Unsupported;
import org.jgroups.jmx.JmxConfigurator;
import org.jgroups.util.Queue;
import org.jgroups.util.QueueClosedException;
import org.jgroups.util.Util;

import javax.management.MBeanServer;
import java.io.Serializable;
import java.util.*;




/**
 * A tree-like structure that is replicated across several members. Updates will be multicast to all group
 * members reliably and in the same order.
 * @author Bela Ban Jan 17 2002
 * @author <a href="mailto:aolias@yahoo.com">Alfonso Olias-Sanz</a>
 */
@Unsupported
public class ReplicatedTree2 implements Runnable, MessageListener, MembershipListener {
    public static final String SEPARATOR="/";
    final static int INDENT=4;
    Node root=new NodeImpl(SEPARATOR, SEPARATOR, null, null);
    final Vector listeners=new Vector();
    final Queue request_queue=new Queue();
    Thread request_handler=null;
    JChannel channel=null;
    PullPushAdapter adapter=null;
    String groupname="ReplicatedTree-Group";
    final Vector members=new Vector();
    long state_fetch_timeout=10000;
    boolean jmx=false;

    protected final Log log=LogFactory.getLog(this.getClass());


    /** Whether or not to use remote calls. If false, all methods will be invoked directly on this
     instance rather than sending a message to all replicas and only then invoking the method.
     Useful for testing */
    boolean remote_calls=true;
    String props="UDP(mcast_addr=224.0.0.36;mcast_port=55566;ip_ttl=32;" +
            "mcast_send_buf_size=150000;mcast_recv_buf_size=80000):" +
            "PING(timeout=2000;num_initial_members=3):" +
            "MERGE2(min_interval=5000;max_interval=10000):" +
            "FD_SOCK:" +
            "VERIFY_SUSPECT(timeout=1500):" +
            "pbcast.STABLE(desired_avg_gossip=20000):" +
            "pbcast.NAKACK(gc_lag=50;retransmit_timeout=600,1200,2400,4800):" +
            "UNICAST(timeout=5000):" +
            "FRAG(frag_size=16000;down_thread=false;up_thread=false):" +
            "pbcast.GMS(join_timeout=5000;" +
            "shun=false;print_local_addr=true):" +
            "pbcast.STATE_TRANSFER";
    // "PERF(details=true)";

	/** Determines when the updates have to be sent across the network, avoids sending unnecessary
     * messages when there are no member in the group */
	private boolean send_message = false;



    public interface ReplicatedTreeListener {
        void nodeAdded(String fqn);

        void nodeRemoved(String fqn);

        void nodeModified(String fqn);

        void viewChange(View new_view);  // might be MergeView after merging
    }


    /**
     * Creates a channel with the given properties. Connects to the channel, then creates a PullPushAdapter
     * and starts it
     */
    public ReplicatedTree2(String groupname, String props, long state_fetch_timeout) throws Exception {
        if(groupname != null)
            this.groupname=groupname;
        if(props != null)
            this.props=props;
        this.state_fetch_timeout=state_fetch_timeout;
        channel=new JChannel(this.props);
        channel.connect(this.groupname);
        start();
    }

    public ReplicatedTree2(String groupname, String props, long state_fetch_timeout, boolean jmx) throws Exception {
        if(groupname != null)
            this.groupname=groupname;
        if(props != null)
            this.props=props;
        this.jmx=jmx;
        this.state_fetch_timeout=state_fetch_timeout;
        channel=new JChannel(this.props);
        channel.connect(this.groupname);
        if(jmx) {
            MBeanServer server=Util.getMBeanServer();
            if(server == null)
                throw new Exception("No MBeanServers found; need to run with an MBeanServer present, or inside JDK 5");
            JmxConfigurator.registerChannel(channel, server, "jgroups", channel.getClusterName() , true);
        }
        start();
    }

    public ReplicatedTree2() {
    }


    /**
     * Expects an already connected channel. Creates a PullPushAdapter and starts it
     */
    public ReplicatedTree2(JChannel channel) throws Exception {
        this.channel=channel;
        start();
    }


    public void setRemoteCalls(boolean flag) {
        remote_calls=flag;
    }

    public void setRootNode(Node n) {
        root=n;
    }

    public Address getLocalAddress() {
        return channel != null? channel.getAddress() : null;
    }

    public Vector getMembers() {
        return members;
    }


    /**
     * Fetch the group state from the current coordinator. If successful, this will trigger setState().
     */
    public void fetchState(long timeout) throws ChannelClosedException, ChannelNotConnectedException {
        boolean rc=channel.getState(null, timeout);
        if(log.isInfoEnabled()) {
            if(rc)
                log.info("state was retrieved successfully");
            else
                log.info("state could not be retrieved (first member)");
        }
    }


    public void addReplicatedTreeListener(ReplicatedTreeListener listener) {
        if(!listeners.contains(listener))
            listeners.addElement(listener);
    }


    public void removeReplicatedTreeListener(ReplicatedTreeListener listener) {
        listeners.removeElement(listener);
    }


    public final void start() throws Exception {
        if(request_handler == null) {
            request_handler=new Thread(this, "ReplicatedTree2.RequestHandler thread");
            request_handler.setDaemon(true);
            request_handler.start();
        }
        adapter=new PullPushAdapter(channel, this, this);
        adapter.setListener(this);
        boolean rc=channel.getState(null, state_fetch_timeout);

        if(log.isInfoEnabled()) {
            if(rc)
                log.info("state was retrieved successfully");
            else
                log.info("state could not be retrieved (first member)");
        }
    }


    public void stop() {
        if(request_handler != null && request_handler.isAlive()) {
            request_queue.close(true);
            request_handler=null;
        }

        request_handler=null;
        if(channel != null) {
            channel.close();
        }
        if(adapter != null) {
            adapter.stop();
            adapter=null;
        }
        channel=null;
    }


    /**
     * Adds a new node to the tree and sets its data. If the node doesn not yet exist, it will be created.
     * Also, parent nodes will be created if not existent. If the node already has data, then the new data
     * will override the old one. If the node already existed, a nodeModified() notification will be generated.
     * Otherwise a nodeCreated() motification will be emitted.
     * @param fqn The fully qualified name of the new node
     * @param data The new data. May be null if no data should be set in the node.
     */
    public void put(String fqn, HashMap data) {
        if(!remote_calls) {
            _put(fqn, data);
            return;
        }

		//Changes done by <aos>
		//if true, propagate action to the group
        if(send_message == true) {
            if(channel == null) {
                if(log.isErrorEnabled()) log.error("channel is null, cannot broadcast PUT request");
                return;
            }
            try {
                channel.send(
                        new Message(
                                null,
                                null,
                                new Request(Request.PUT, fqn, data)));
            }
            catch(Exception ex) {
                if(log.isErrorEnabled()) log.error("failure bcasting PUT request: " + ex);
            }
        }
        else {
            _put(fqn, data);
        }
    }


    /**
     * Adds a key and value to a given node. If the node doesn't exist, it will be created. If the node
     * already existed, a nodeModified() notification will be generated. Otherwise a
     * nodeCreated() motification will be emitted.
     * @param fqn The fully qualified name of the node
     * @param key The key
     * @param value The value
     */
    public void put(String fqn, String key, Object value) {
        if(!remote_calls) {
            _put(fqn, key, value);
            return;
        }

        //Changes done by <aos>
        //if true, propagate action to the group
        if(send_message == true) {

            if(channel == null) {
                if(log.isErrorEnabled()) log.error("channel is null, cannot broadcast PUT request");
                return;
            }
            try {
                channel.send(
                        new Message(
                                null,
                                null,
                                new Request(Request.PUT, fqn, key, value)));
            }
            catch(Exception ex) {
                if(log.isErrorEnabled()) log.error("failure bcasting PUT request: " + ex);
            }
        }
        else {
            _put(fqn, key, value);
        }
    }


    /**
     * Removes the node from the tree.
     * @param fqn The fully qualified name of the node.
     */
    public void remove(String fqn) {
        if(!remote_calls) {
            _remove(fqn);
            return;
        }
		//Changes done by <aos>
		//if true, propagate action to the group
        if(send_message == true) {
            if(channel == null) {
                if(log.isErrorEnabled()) log.error("channel is null, cannot broadcast REMOVE request");
                return;
            }
            try {
                channel.send(
                        new Message(null, null, new Request(Request.REMOVE, fqn)));
            }
            catch(Exception ex) {
                if(log.isErrorEnabled()) log.error("failure bcasting REMOVE request: " + ex);
            }
        }
        else {
            _remove(fqn);
        }
    }


    /**
     * Removes <code>key</code> from the node's hashmap
     * @param fqn The fullly qualified name of the node
     * @param key The key to be removed
     */
    public void remove(String fqn, String key) {
        if(!remote_calls) {
            _remove(fqn, key);
            return;
        }
		//Changes done by <aos>
		//if true, propagate action to the group
        if(send_message == true) {
            if(channel == null) {
                if(log.isErrorEnabled()) log.error("channel is null, cannot broadcast REMOVE request");
                return;
            }
            try {
                channel.send(
                        new Message(
                                null,
                                null,
                                new Request(Request.REMOVE, fqn, key)));
            }
            catch(Exception ex) {
                if(log.isErrorEnabled()) log.error("failure bcasting REMOVE request: " + ex);
            }
        }
        else {
            _remove(fqn, key);
        }
    }


    /**
     * Checks whether a given node exists in the tree
     * @param fqn The fully qualified name of the node
     * @return boolean Whether or not the node exists
     */
    public boolean exists(String fqn) {
        if(fqn == null) return false;
        return findNode(fqn) != null;
    }


    /**
     * Gets the keys of the <code>data</code> map. Returns all keys as Strings. Returns null if node
     * does not exist.
     * @param fqn The fully qualified name of the node
     * @return Set A set of keys (as Strings)
     */
    public Set getKeys(String fqn) {
        Node n=findNode(fqn);
        Map data;

        if(n == null) return null;
        data=n.getData();
        if(data == null) return null;
        return data.keySet();
    }


    /**
     * Finds a node given its name and returns the value associated with a given key in its <code>data</code>
     * map. Returns null if the node was not found in the tree or the key was not found in the hashmap.
     * @param fqn The fully qualified name of the node.
     * @param key The key.
     */
    public Object get(String fqn, String key) {
        Node n=findNode(fqn);

        if(n == null) return null;
        return n.getData(key);
    }


    /**
     * Returns the data hashmap for a given node. This method can only be used by callers that are inside
     * the same package. The reason is that callers must not modify the return value, as these modifications
     * would not be replicated, thus rendering the replicas inconsistent.
     * @param fqn The fully qualified name of the node
     * @return HashMap The data hashmap for the given node
     */
    Map get(String fqn) {
        Node n=findNode(fqn);

        if(n == null) return null;
        return n.getData();
    }


    /**
     * Prints a representation of the node defined by <code>fqn</code>. Output includes name, fqn and
     * data.
     */
    public String print(String fqn) {
        Node n=findNode(fqn);
        if(n == null) return null;
        return n.toString();
    }


    /**
     * Returns all children of a given node
     * @param fqn The fully qualified name of the node
     * @return Set A list of child names (as Strings)
     */
    public Set getChildrenNames(String fqn) {
        Node n=findNode(fqn);
        Map m;

        if(n == null) return null;
        m=n.getChildren();
        if(m != null)
            return m.keySet();
        else
            return null;
    }


//    public String toString() {
//        StringBuilder sb=new StringBuilder();
//
//        return sb.toString();
//    }

   /**
    * Returns the name of the group that the DistributedTree is connected to
    * @return String
    */
    public String  getGroupName()           {return groupname;}
	 	
   /**
    * Returns the Channel the DistributedTree is connected to 
    * @return Channel
    */
    public Channel getChannel()             {return channel;}

   /**
    * Returns the number of current members joined to the group
    * @return int
    */
    public int getGroupMembersNumber()			{return members.size();}




    /* --------------------- Callbacks -------------------------- */


    public void _put(String fqn, HashMap data) {
        Node n;
        StringHolder child_name=new StringHolder();
        boolean child_exists=false;

        if(fqn == null) return;
        n=findParentNode(fqn, child_name, true); // create all nodes if they don't exist
        if(child_name.getValue() != null) {
            child_exists=n.childExists(child_name.getValue());
            n.createChild(child_name.getValue(), fqn, n, data);
        }
        else {
            child_exists=true;
            n.setData(data);
        }
        if(child_exists)
            notifyNodeModified(fqn);
        else
            notifyNodeAdded(fqn);
    }


    public void _put(String fqn, String key, Object value) {
        Node n;
        boolean child_exists=false;

        if(fqn == null || key == null || value == null) return;
        List<String> elements=Util.split(fqn, '/');

        String child_name=elements.get(elements.size() -1);

        n=findParentNode(elements, true);
        if(child_name != null) {
            child_exists=n.childExists(child_name);
            n.createChild(child_name, fqn, n, key, value);
        }
        else {
            child_exists=true;
            n.setData(key, value);
        }
        if(child_exists)
            notifyNodeModified(fqn);
        else
            notifyNodeAdded(fqn);
    }


    public void _remove(String fqn) {
        Node n;
        StringHolder child_name=new StringHolder();

        if(fqn == null) return;
        if(fqn.equals(SEPARATOR)) {
            root.removeAll();
            notifyNodeRemoved(fqn);
            return;
        }
        n=findParentNode(fqn, child_name, false);
        if(n == null) return;
        n.removeChild(child_name.getValue(), fqn);
        notifyNodeRemoved(fqn);
    }


    public void _remove(String fqn, String key) {
        Node n;

        if(fqn == null || key == null) return;
        n=findNode(fqn);
        if(n != null)
            n.removeData(key);
    }


    public void _removeData(String fqn) {
        Node n;

        if(fqn == null) return;
        n=findNode(fqn);
        if(n != null)
            n.removeData();
    }


    /* ----------------- End of  Callbacks ---------------------- */






    /*-------------------- MessageListener ----------------------*/

    /** Callback. Process the contents of the message; typically an _add() or _set() request */
    public void receive(Message msg) {
        Request req=null;

        if(msg == null || msg.getLength() == 0)
            return;
        try {
            req=(Request)msg.getObject();
            request_queue.add(req);
        }
        catch(QueueClosedException queue_closed_ex) {
            if(log.isErrorEnabled()) log.error("request queue is null");
        }
        catch(Exception ex) {
            if(log.isErrorEnabled()) log.error("failed unmarshalling request: " + ex);
        }
    }

    /** Return a copy of the current cache (tree) */
    public byte[] getState() {
        try {
            return Util.objectToByteBuffer(root.clone());
        }
        catch(Throwable ex) {
            if(log.isErrorEnabled()) log.error("exception returning cache: " + ex);
            return null;
        }
    }

    /** Set the cache (tree) to this value */
    public void setState(byte[] new_state) {
        Node new_root=null;
        Object obj;

        if(new_state == null) {
            if(log.isInfoEnabled()) log.info("new cache is null");
            return;
        }
        try {
            obj=Util.objectFromByteBuffer(new_state);
            new_root=(Node)((Node)obj).clone();
            root=new_root;
            notifyAllNodesCreated(root);
        }
        catch(Throwable ex) {
            if(log.isErrorEnabled()) log.error("could not set cache: " + ex);
        }
    }

    /*-------------------- End of MessageListener ----------------------*/





    /*----------------------- MembershipListener ------------------------*/

    public void viewAccepted(View new_view) {
        Vector new_mbrs=new_view.getMembers();

        // todo: if MergeView, fetch and reconcile state from coordinator
        // actually maybe this is best left up to the application ? we just notify them and let
        // the appl handle it ?

        if(new_mbrs != null) {
            notifyViewChange(new_view);
            members.removeAllElements();
            for(int i=0; i < new_mbrs.size(); i++)
                members.addElement(new_mbrs.elementAt(i));
        }
		//if size is bigger than one, there are more peers in the group
		//otherwise there is only one server.
        send_message=members.size() > 1;
    }


    /** Called when a member is suspected */
    public void suspect(Address suspected_mbr) {
        ;
    }


    /** Block sending and receiving of messages until viewAccepted() is called */
    public void block() {
    }

    /*------------------- End of MembershipListener ----------------------*/



    /** Request handler thread */
    public void run() {
        Request req;
        String fqn=null;

        while(request_handler != null) {
            try {
                req=(Request)request_queue.remove(0);
                fqn=req.fqn;
                switch(req.type) {
                    case Request.PUT:
                        if(req.key != null && req.value != null)
                            _put(fqn, req.key, req.value);
                        else
                            _put(fqn, req.data);
                        break;
                    case Request.REMOVE:
                        if(req.key != null)
                            _remove(fqn, req.key);
                        else
                            _remove(fqn);
                        break;
                    default:
                        if(log.isErrorEnabled()) log.error("type " + req.type + " unknown");
                        break;
                }
            }
            catch(QueueClosedException queue_closed_ex) {
                request_handler=null;
                break;
            }
            catch(Throwable other_ex) {
                if(log.isWarnEnabled()) log.warn("exception processing request: " + other_ex);
            }
        }
    }


    /**
     * Find the node just <em>above</em> the one indicated by <code>fqn</code>. This is needed in many cases,
     * e.g. to add a new node or remove an existing node.
     * @param fqn The fully qualified name of the node.
     * @param child_name Will be filled with the name of the child when this method returns. The child name
     *                   is the last relative name of the <code>fqn</code>, e.g. in "/a/b/c" it would be "c".
     * @param create_if_not_exists Create parent nodes along the way if they don't exist. Otherwise, this method
     *                             will return when a node cannot be found.
     */
    Node findParentNode(String fqn, StringHolder child_name, boolean create_if_not_exists) {
        Node curr=root, node;
        StringTokenizer tok;
        String name;
        StringBuilder sb=null;

        if(fqn == null || fqn.equals(SEPARATOR) || "".equals(fqn))
            return curr;

        sb=new StringBuilder();
        tok=new StringTokenizer(fqn, SEPARATOR);
        while(tok.countTokens() > 1) {
            name=tok.nextToken();
            sb.append(SEPARATOR).append(name);
            node=curr.getChild(name);
            if(node == null && create_if_not_exists)
                node=curr.createChild(name, sb.toString(), null, null);
            if(node == null)
                return null;
            else
                curr=node;
        }

        if(tok.countTokens() > 0 && child_name != null)
            child_name.setValue(tok.nextToken());
        return curr;
    }



    Node findParentNode(List<String> fqn, boolean create_if_not_exists) {
        if(fqn == null || fqn.isEmpty())
            return root;

        Node curr=root, node;
        StringBuilder sb=new StringBuilder();
        for(int i=0; i < fqn.size() -1; i++) {
            String name=fqn.get(i);
            sb.append(SEPARATOR).append(name);
            node=curr.getChild(name);
            if(node == null && create_if_not_exists)
                node=curr.createChild(name, sb.toString(), null, null);
            if(node == null)
                return null;
            else
                curr=node;
        }

        return curr;
    }


    /**
     * Returns the node at fqn. This method should not be used by clients (therefore it is package-private):
     * it is only used internally (for navigation). C++ 'friend' would come in handy here...
     * @param fqn The fully qualified name of the node
     * @return Node The node at fqn
     */
    Node findNode(String fqn) {
        StringHolder sh=new StringHolder();
        Node n=findParentNode(fqn, sh, false);
        String child_name=sh.getValue();

        if(fqn == null || fqn.equals(SEPARATOR) || "".equals(fqn))
            return root;

        if(n == null || child_name == null)
            return null;
        else
            return n.getChild(child_name);
    }


    void notifyNodeAdded(String fqn) {
        for(int i=0; i < listeners.size(); i++)
            ((ReplicatedTreeListener)listeners.elementAt(i)).nodeAdded(fqn);
    }

    void notifyNodeRemoved(String fqn) {
        for(int i=0; i < listeners.size(); i++)
            ((ReplicatedTreeListener)listeners.elementAt(i)).nodeRemoved(fqn);
    }

    void notifyNodeModified(String fqn) {
        for(int i=0; i < listeners.size(); i++)
            ((ReplicatedTreeListener)listeners.elementAt(i)).nodeModified(fqn);
    }

    void notifyViewChange(View v) {
        for(int i=0; i < listeners.size(); i++)
            ((ReplicatedTreeListener)listeners.elementAt(i)).viewChange(v);
    }

    /** Generates NodeAdded notifications for all nodes of the tree. This is called whenever the tree is
     initially retrieved (state transfer) */
    void notifyAllNodesCreated(Node curr) {
        Node n;
        Map children;

        if(curr == null) return;
        notifyNodeAdded(curr.getFqn());
        if((children=curr.getChildren()) != null) {
            for(Iterator it=children.values().iterator(); it.hasNext();) {
                n=(Node)it.next();
                notifyAllNodesCreated(n);
            }
        }
    }

    public static Map createMap() {
        return new HashMap();
    }

    public static Map createMap(Map map) {
        return new HashMap(map);
    }


    public static interface Node extends Serializable {
        void setData(Map data);

        void setData(String key, Object value);

        Map getData();

        String getFqn();

        Object getData(String key);

        boolean childExists(String child_name);

        Node createChild(String child_name, String fqn, Node parent, HashMap data);

        Node createChild(String child_name, String fqn, Node parent, String key, Object value);

        Node getChild(String child_name);

        Map getChildren();

        void setChildren(Map children);

        void removeData(String key);

        void removeData();

        void removeChild(String child_name, String fqn);

        void removeAll();

        void print(StringBuilder sb, int indent);

        void printIndent(StringBuilder sb, int indent);

        public Object clone() throws CloneNotSupportedException;
    }

    public static class NodeImpl implements Node {
        String name=null;     // relative name (e.g. "Security")
        String fqn=null;      // fully qualified name (e.g. "/federations/fed1/servers/Security")
        Map children=null; // keys: child name, value: Node
        Map data=null;     // data for current node
        private static final long serialVersionUID = -3077676554440038890L;
        // Address       creator=null;  // member that created this node (needed ?)


        private NodeImpl(String child_name, String fqn, Node parent, Map data) {
            name=child_name;
            this.fqn=fqn;
            if(data != null) this.data=createMap(data);
        }

        private NodeImpl(String child_name, String fqn, Node parent, String key, Object value) {
            name=child_name;
            this.fqn=fqn;
            if(data == null) data=createMap();
            data.put(key, value);
        }

        public void setData(Map data) {
            if(data == null) return;
            if(this.data == null)
                this.data=createMap();
            this.data.putAll(data);
        }

        public void setData(String key, Object value) {
            if(this.data == null)
                this.data=createMap();
            this.data.put(key, value);
        }

        public Map getData() {
            return data;
        }

        public String getFqn() {
            return fqn;
        }

        public Object getData(String key) {
            return data != null? data.get(key) : null;
        }


        public boolean childExists(String child_name) {
            return child_name != null && children != null && children.containsKey(child_name);
        }


        public Node createChild(String child_name, String fqn, Node parent, HashMap data) {
            Node child=null;

            if(child_name == null) return null;
            if(children == null) children=createMap();
            child=(Node)children.get(child_name);
            if(child != null)
                child.setData(data);
            else {
                child=new NodeImpl(child_name, fqn, parent, data);
                children.put(child_name, child);
            }
            return child;
        }

        public Node createChild(String child_name, String fqn, Node parent, String key, Object value) {
            Node child=null;

            if(child_name == null) return null;
            if(children == null) children=createMap();
            child=(Node)children.get(child_name);
            if(child != null)
                child.setData(key, value);
            else {
                child=new LeafNodeOneKey(key, value);
                children.put(child_name, child);
            }
            return child;
        }


        public Node getChild(String child_name) {
            return child_name == null? null : children == null? null : (Node)children.get(child_name);
        }

        public Map getChildren() {
            return children;
        }

        public void setChildren(Map children) {
            this.children=children;
        }

        public void removeData(String key) {
            if(data != null)
                data.remove(key);
        }

        public void removeData() {
            if(data != null)
                data.clear();
        }

        public void removeChild(String child_name, String fqn) {
            if(child_name != null && children != null && children.containsKey(child_name)) {
                children.remove(child_name);
            }
        }

        public void removeAll() {
            if(children != null)
                children.clear();
        }

        public void print(StringBuilder sb, int indent) {
            printIndent(sb, indent);
            sb.append(SEPARATOR).append(name);
            if(children != null && !children.isEmpty()) {
                Collection values=children.values();
                for(Iterator it=values.iterator(); it.hasNext();) {
                    sb.append('\n');
                    ((Node)it.next()).print(sb, indent + INDENT);
                }
            }
        }

        public void printIndent(StringBuilder sb, int indent) {
            if(sb != null) {
                for(int i=0; i < indent; i++)
                    sb.append(' ');
            }
        }


        public String toString() {
            StringBuilder sb=new StringBuilder();
            if(name != null) sb.append("\nname=" + name);
            if(fqn != null) sb.append("\nfqn=" + fqn);
            if(data != null) sb.append("\ndata=" + data);
            return sb.toString();
        }


        public Object clone() throws CloneNotSupportedException {
            Node n=new NodeImpl(name, fqn, null, data);
            if(children != null) n.setChildren(createMap(children));
            return n;
        }

    }

    public static class LeafNodeOneKey implements Node {
        private static final long serialVersionUID=-8848955385138463778L;

        private String key;
        private Object val;


        private LeafNodeOneKey(Map data) {
            if(data == null)
                return;
            if(data.size() > 1)
                throw new IllegalArgumentException("map can have only 1 key/value pair");
            Iterator it=data.entrySet().iterator();
            Map.Entry entry=(Map.Entry)it.next();
            key=(String)entry.getKey();
            val=entry.getValue();
        }

        private LeafNodeOneKey(String key, Object value) {
            this.key=key;
            this.val=value;
        }


        public void setData(Map data) {
            if(data == null)
                return;
            if(data.size() > 1)
                throw new IllegalArgumentException("map can have only 1 key/value pair");
            Iterator it=data.entrySet().iterator();
            Map.Entry entry=(Map.Entry)it.next();
            key=(String)entry.getKey();
            val=entry.getValue();
        }

        public void setData(String key, Object value) {
            this.key=key;
            this.val=value;
        }

        public Map getData() {
            Map retval=createMap();
            retval.put(key, val);
            return retval;
        }

        public String getFqn() {
            return null;
        }

        public Object getData(String key) {
            return val;
        }


        public boolean childExists(String child_name) {
            return false;
        }


        public Node createChild(String child_name, String fqn, Node parent, HashMap data) {
            throw new UnsupportedOperationException();
        }

        public Node createChild(String child_name, String fqn, Node parent, String key, Object value) {
            throw new UnsupportedOperationException();
        }


        public Node getChild(String child_name) {
            return null;
        }

        public Map getChildren() {
            return null;
        }

        public void setChildren(Map children) {
            throw new UnsupportedOperationException();
        }

        public void removeData(String key) {
            if(key != null && this.key != null && this.key.equals(key)) {
                key=null;
                val=null;
            }
        }

        public void removeData() {
            key=null;
            val=null;
        }

        public void removeChild(String child_name, String fqn) {
        }

        public void removeAll() {
        }

        public void print(StringBuilder sb, int indent) {
        }

        public void printIndent(StringBuilder sb, int indent) {
            if(sb != null) {
                for(int i=0; i < indent; i++)
                    sb.append(' ');
            }
        }


        public String toString() {
            StringBuilder sb=new StringBuilder();
            sb.append(key).append("=").append(val);
            return sb.toString();
        }


        public Object clone() throws CloneNotSupportedException {
            return new LeafNodeOneKey(key, val);
        }

    }


    private static class StringHolder {
        String s=null;

        private StringHolder() {
        }

        void setValue(String s) {
            this.s=s;
        }

        String getValue() {
            return s;
        }
    }


    /**
     * Class used to multicast add(), remove() and set() methods to all members.
     */
    private static class Request implements Serializable {
        static final int PUT=1;
        static final int REMOVE=2;

        int type=0;
        String fqn=null;
        String key=null;
        Object value=null;
        HashMap data=null;
        private static final long serialVersionUID = 7772753222127676782L;

        private Request(int type, String fqn) {
            this.type=type;
            this.fqn=fqn;
        }

        private Request(int type, String fqn, HashMap data) {
            this(type, fqn);
            this.data=data;
        }

        private Request(int type, String fqn, String key) {
            this(type, fqn);
            this.key=key;
        }

        private Request(int type, String fqn, String key, Object value) {
            this(type, fqn);
            this.key=key;
            this.value=value;
        }

        public String toString() {
            StringBuilder sb=new StringBuilder();
            sb.append(type2String(type)).append(" (");
            if(fqn != null) sb.append(" fqn=" + fqn);
            switch(type) {
                case PUT:
                    if(data != null) sb.append(", data=" + data);
                    if(key != null) sb.append(", key=" + key);
                    if(value != null) sb.append(", value=" + value);
                    break;
                case REMOVE:
                    if(key != null) sb.append(", key=" + key);
                    break;
                default:
                    break;
            }
            sb.append(')');
            return sb.toString();
        }

        static String type2String(int t) {
            switch(t) {
                case PUT:
                    return "PUT";
                case REMOVE:
                    return "REMOVE";
                default:
                    return "UNKNOWN";
            }
        }

    }





    static class MyListener implements ReplicatedTreeListener {

        public void nodeAdded(String fqn) {
            System.out.println("** node added: " + fqn);
        }

        public void nodeRemoved(String fqn) {
            System.out.println("** node removed: " + fqn);
        }

        public void nodeModified(String fqn) {
            System.out.println("** node modified: " + fqn);
        }

        public void viewChange(View new_view) {
            System.out.println("** view change: " + new_view);
        }

    }


}


