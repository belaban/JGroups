
package org.jgroups.blocks;


import org.jgroups.*;
import org.jgroups.annotations.Unsupported;
import org.jgroups.jmx.JmxConfigurator;
import org.jgroups.logging.Log;
import org.jgroups.logging.LogFactory;
import org.jgroups.util.Util;

import javax.management.MBeanServer;
import java.io.*;
import java.util.*;




/**
 * A tree-like structure that is replicated across several members. Updates will be multicast to all group
 * members reliably and in the same order.
 * @author Bela Ban Jan 17 2002
 * @author <a href="mailto:aolias@yahoo.com">Alfonso Olias-Sanz</a>
 */
@Unsupported
public class ReplicatedTree extends ReceiverAdapter {
    public static final String SEPARATOR="/";
    final static int INDENT=4;
    Node root=new Node(SEPARATOR, SEPARATOR, null, null);
    final List<ReplicatedTreeListener> listeners=new ArrayList<>();
    JChannel channel=null;
    String groupname="ReplicatedTree-Group";
    final List<Address> members=new ArrayList<>();
    long state_fetch_timeout=10000;
    boolean jmx=false;

    protected final Log log=LogFactory.getLog(this.getClass());


    /** Whether or not to use remote calls. If false, all methods will be invoked directly on this
     instance rather than sending a message to all replicas and only then invoking the method.
     Useful for testing */
    boolean remote_calls=true;
    String props="udp.xml";

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
    public ReplicatedTree(String groupname, String props, long state_fetch_timeout) throws Exception {
        if(groupname != null)
            this.groupname=groupname;
        if(props != null)
            this.props=props;
        this.state_fetch_timeout=state_fetch_timeout;
        channel=new JChannel(this.props);
        channel.setReceiver(this);
        channel.connect(this.groupname);
        start();
    }

    public ReplicatedTree(String groupname, String props, long state_fetch_timeout, boolean jmx) throws Exception {
        if(groupname != null)
            this.groupname=groupname;
        if(props != null)
            this.props=props;
        this.jmx=jmx;
        this.state_fetch_timeout=state_fetch_timeout;
        channel=new JChannel(this.props);
        channel.setReceiver(this);
        channel.connect(this.groupname);

        if(jmx) {
            MBeanServer server=Util.getMBeanServer();
            if(server == null)
                throw new Exception("No MBeanServers found; need to run with an MBeanServer present, or inside JDK 5");
            JmxConfigurator.registerChannel(channel, server, "jgroups", channel.getClusterName() , true);
        }
        start();
    }

    public ReplicatedTree() {
    }


    /**
     * Expects an already connected channel. Creates a PullPushAdapter and starts it
     */
    public ReplicatedTree(JChannel channel) throws Exception {
        this.channel=channel;
        channel.setReceiver(this);
        viewAccepted(channel.getView());
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

    public List<Address> getMembers() {
        return members;
    }


    /**
     * Fetch the group state from the current coordinator. If successful, this will trigger setState().
     */
    public void fetchState(long timeout) throws Exception {
        channel.getState(null, timeout);
    }


    public void addReplicatedTreeListener(ReplicatedTreeListener listener) {
        if(!listeners.contains(listener))
            listeners.add(listener);
    }


    public void removeReplicatedTreeListener(ReplicatedTreeListener listener) {
        listeners.remove(listener);
    }


    public final void start() throws Exception {
        channel.getState(null, state_fetch_timeout);
    }


    public void stop() {
        Util.close(channel);
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
        if(send_message) {
            if(channel == null) {
                if(log.isErrorEnabled()) log.error("channel is null, cannot broadcast PUT request");
                return;
            }
            try {
                channel.send(new Message(null, new Request(Request.PUT, fqn, data)));
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
        if(send_message) {

            if(channel == null) {
                if(log.isErrorEnabled()) log.error("channel is null, cannot broadcast PUT request");
                return;
            }
            try {
                channel.send(new Message(null, new Request(Request.PUT, fqn, key, value)));
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
        if(send_message) {
            if(channel == null) {
                if(log.isErrorEnabled()) log.error("channel is null, cannot broadcast REMOVE request");
                return;
            }
            try {
                channel.send(new Message(null, new Request(Request.REMOVE, fqn)));
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
     * Removes {@code key} from the node's hashmap
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
        if(send_message) {
            if(channel == null) {
                if(log.isErrorEnabled()) log.error("channel is null, cannot broadcast REMOVE request");
                return;
            }
            try {
                channel.send(new Message(null, new Request(Request.REMOVE, fqn, key)));
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
        return fqn != null && findNode(fqn) != null;
    }


    /**
     * Gets the keys of the {@code data} map. Returns all keys as Strings. Returns null if node
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
     * Finds a node given its name and returns the value associated with a given key in its {@code data}
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
    Map<String,Object> get(String fqn) {
        Node n=findNode(fqn);

        if(n == null) return null;
        return n.getData();
    }


    /**
     * Prints a representation of the node defined by {@code fqn}. Output includes name, fqn and
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


    public String toString() {
        StringBuilder sb=new StringBuilder();
        int indent=0;
        Map children;

        children=root.getChildren();
        if(children != null && !children.isEmpty()) {
            Collection nodes=children.values();
            for(Iterator it=nodes.iterator(); it.hasNext();) {
                ((Node)it.next()).print(sb, indent);
                sb.append('\n');
            }
        }
        else
            sb.append(SEPARATOR);
        return sb.toString();
    }

   /**
    * Returns the name of the group that the DistributedTree is connected to
    * @return String
    */
    public String  getGroupName()           {return groupname;}
	 	
   /**
    * Returns the Channel the DistributedTree is connected to 
    * @return Channel
    */
    public JChannel getChannel()             {return channel;}

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
        StringHolder child_name=new StringHolder();
        boolean child_exists=false;

        if(fqn == null || key == null || value == null) return;
        n=findParentNode(fqn, child_name, true);
        if(child_name.getValue() != null) {
            child_exists=n.childExists(child_name.getValue());
            n.createChild(child_name.getValue(), fqn, n, key, value);
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
            req=msg.getObject();

            String fqn=req.fqn;
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
        catch(Exception ex) {
            if(log.isErrorEnabled()) log.error("failed unmarshalling request: " + ex);
        }
    }


    public void getState(OutputStream ostream) throws Exception {
        Util.objectToStream(root.clone(), new DataOutputStream(ostream));
    }


    public void setState(InputStream istream) throws Exception {
        Object obj=Util.objectFromStream(new DataInputStream(istream));
        root=(Node)((Node)obj).clone();
        notifyAllNodesCreated(root);
    }

    /*-------------------- End of MessageListener ----------------------*/





    /*----------------------- MembershipListener ------------------------*/

    public void viewAccepted(View new_view) {
        List<Address> new_mbrs=new_view.getMembers();
        if(new_mbrs != null) {
            notifyViewChange(new_view);
            members.clear();
            members.addAll(new_mbrs);
        }
		//if size is bigger than one, there are more peers in the group
		//otherwise there is only one server.
        send_message=members.size() > 1;
    }

    /*------------------- End of MembershipListener ----------------------*/




    /**
     * Find the node just <em>above</em> the one indicated by {@code fqn}. This is needed in many cases,
     * e.g. to add a new node or remove an existing node.
     * @param fqn The fully qualified name of the node.
     * @param child_name Will be filled with the name of the child when this method returns. The child name
     *                   is the last relative name of the {@code fqn}, e.g. in "/a/b/c" it would be "c".
     * @param create_if_not_exists Create parent nodes along the way if they don't exist. Otherwise, this method
     *                             will return when a node cannot be found.
     */
    Node findParentNode(String fqn, StringHolder child_name, boolean create_if_not_exists) {
        Node curr=root, node;
        StringTokenizer tok;
        String name;
        StringBuilder sb=null;

        if(fqn == null || fqn.equals(SEPARATOR) || fqn != null && fqn.isEmpty())
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

        if(fqn == null || fqn.equals(SEPARATOR) || fqn != null && fqn.isEmpty())
            return root;

        if(n == null || child_name == null)
            return null;
        else
            return n.getChild(child_name);
    }


    void notifyNodeAdded(String fqn) {
        for(int i=0; i < listeners.size(); i++)
            listeners.get(i).nodeAdded(fqn);
    }

    void notifyNodeRemoved(String fqn) {
        for(int i=0; i < listeners.size(); i++)
            listeners.get(i).nodeRemoved(fqn);
    }

    void notifyNodeModified(String fqn) {
        for(int i=0; i < listeners.size(); i++)
            listeners.get(i).nodeModified(fqn);
    }

    void notifyViewChange(View v) {
        for(int i=0; i < listeners.size(); i++)
            listeners.get(i).viewChange(v);
    }

    /** Generates NodeAdded notifications for all nodes of the tree. This is called whenever the tree is
     initially retrieved (state transfer) */
    void notifyAllNodesCreated(Node curr) {
        Node n;
        Map children;

        if(curr == null) return;
        notifyNodeAdded(curr.fqn);
        if((children=curr.getChildren()) != null) {
            for(Iterator it=children.values().iterator(); it.hasNext();) {
                n=(Node)it.next();
                notifyAllNodesCreated(n);
            }
        }
    }


    public static final class Node implements Serializable {
        String name=null;     // relative name (e.g. "Security")
        String fqn=null;      // fully qualified name (e.g. "/federations/fed1/servers/Security")
        Node parent=null;   // parent node
        TreeMap<String,Node> children=null; // keys: child name, value: Node
        Map<String,Object> data=null;     // data for current node
        private static final long serialVersionUID = -3077676554440038890L;


        private Node(String child_name, String fqn, Node parent, Map<String,Object> data) {
            name=child_name;
            this.fqn=fqn;
            this.parent=parent;
            if(data != null) this.data=(HashMap<String,Object>)((HashMap)data).clone();
        }

        private Node(String child_name, String fqn, Node parent, String key, Object value) {
            name=child_name;
            this.fqn=fqn;
            this.parent=parent;
            if(data == null) data=new HashMap<>();
            data.put(key, value);
        }

        void setData(Map data) {
            if(data == null) return;
            if(this.data == null)
                this.data=new HashMap<>();
            this.data.putAll(data);
        }

        void setData(String key, Object value) {
            if(this.data == null)
                this.data=new HashMap<>();
            this.data.put(key, value);
        }

        Map<String,Object> getData() {
            return data;
        }

        Object getData(String key) {
            return data != null? data.get(key) : null;
        }


        boolean childExists(String child_name) {
            return child_name != null && children != null && children.containsKey(child_name);
        }


        Node createChild(String child_name, String fqn, Node parent, HashMap<String,Object> data) {
            Node child=null;

            if(child_name == null) return null;
            if(children == null) children=new TreeMap<>();
            child=children.get(child_name);
            if(child != null)
                child.setData(data);
            else {
                child=new Node(child_name, fqn, parent, data);
                children.put(child_name, child);
            }
            return child;
        }

        Node createChild(String child_name, String fqn, Node parent, String key, Object value) {
            Node child=null;

            if(child_name == null) return null;
            if(children == null) children=new TreeMap<>();
            child=children.get(child_name);
            if(child != null)
                child.setData(key, value);
            else {
                child=new Node(child_name, fqn, parent, key, value);
                children.put(child_name, child);
            }
            return child;
        }


        Node getChild(String child_name) {
            return child_name == null? null : children == null? null : children.get(child_name);
        }

        Map<String,Node> getChildren() {
            return children;
        }

        void removeData(String key) {
            if(data != null)
                data.remove(key);
        }

        void removeData() {
            if(data != null)
                data.clear();
        }

        void removeChild(String child_name, String fqn) {
            if(child_name != null && children != null && children.containsKey(child_name)) {
                children.remove(child_name);
            }
        }

        void removeAll() {
            if(children != null)
                children.clear();
        }

        void print(StringBuilder sb, int indent) {
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

        static void printIndent(StringBuilder sb, int indent) {
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
            Node n=new Node(name, fqn, parent != null? (Node)parent.clone() : null, data);
            if(children != null) n.children=(TreeMap)children.clone();
            return n;
        }

    }


    private static final class StringHolder {
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
    private static final class Request implements Serializable {
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


    public static void main(String[] args) {
        ReplicatedTree tree=null;
        HashMap m=new HashMap();
        String props=null;

        try {

            tree=new ReplicatedTree(null, props, 10000);
            // tree.setRemoteCalls(false);
            tree.addReplicatedTreeListener(new MyListener());
            tree.put("/a/b/c", null);
            tree.put("/a/b/c1", null);
            tree.put("/a/b/c2", null);
            tree.put("/a/b1/chat", null);
            tree.put("/a/b1/chat2", null);
            tree.put("/a/b1/chat5", null);
            System.out.println(tree);
            m.put("name", "Bela Ban");
            m.put("age",36);
            m.put("cube", "240-17");
            tree.put("/a/b/c", m);
            System.out.println("info for for \"/a/b/c\" is " + tree.print("/a/b/c"));
            tree.put("/a/b/c", "age",37);
            System.out.println("info for for \"/a/b/c\" is " + tree.print("/a/b/c"));
            tree.remove("/a/b");
            System.out.println(tree);
            tree.stop();
        }
        catch(Exception ex) {
            System.err.println(ex);
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


