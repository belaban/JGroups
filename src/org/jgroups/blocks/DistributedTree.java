// $Id: DistributedTree.java,v 1.21 2009/05/13 13:06:54 belaban Exp $

package org.jgroups.blocks;


import org.jgroups.logging.Log;
import org.jgroups.logging.LogFactory;
import org.jgroups.*;
import org.jgroups.annotations.Unsupported;
import org.jgroups.util.Util;

import java.io.Serializable;
import java.util.StringTokenizer;
import java.util.Vector;




/**
 * A tree-like structure that is replicated across several members. Updates will be multicast to all group
 * members reliably and in the same order.
 * @author Bela Ban
 * @author <a href="mailto:aolias@yahoo.com">Alfonso Olias-Sanz</a>
 */
@Unsupported
public class DistributedTree implements MessageListener, MembershipListener {
    private Node root=null;
    final Vector listeners=new Vector();
    final Vector view_listeners=new Vector();
    final Vector members=new Vector();
    protected Channel channel=null;
    protected RpcDispatcher disp=null;
    // rc is global and protected so that extensions can detect when 
    // state has been transferred
    protected boolean rc = false;
    String groupname="DistributedTreeGroup";
    String channel_properties="UDP(mcast_addr=228.1.2.3;mcast_port=45566;ip_ttl=0):" +
            "PING(timeout=5000;num_initial_members=6):" +
            "FD_SOCK:" +
            "VERIFY_SUSPECT(timeout=1500):" +
            "pbcast.STABLE(desired_avg_gossip=10000):" +
            "pbcast.NAKACK(gc_lag=5;retransmit_timeout=3000;trace=true):" +
            "UNICAST(timeout=5000):" +
            "FRAG(down_thread=false;up_thread=false):" +
            "pbcast.GMS(join_timeout=5000;" +
            "shun=false;print_local_addr=true):" +
            // trace=true is not supported anymore
            "pbcast.STATE_TRANSFER()";
    static final long state_timeout=5000;   // wait 5 secs max to obtain state

	/** Determines when the updates have to be sent across the network, avoids sending unnecessary
     * messages when there are no member in the group */
    
    // Make this protected so that extensions 
    // can control whether or not to send
	protected boolean send_message = false;

    protected static final Log log=LogFactory.getLog(DistributedTree.class);



    public interface DistributedTreeListener {
        void nodeAdded(String fqn, Serializable element);

        void nodeRemoved(String fqn);

        void nodeModified(String fqn, Serializable old_element, Serializable new_element);
    }


    public interface ViewListener {
        void viewChange(Vector new_mbrs, Vector old_mbrs);
    }


    public DistributedTree() {
    }


    public DistributedTree(String groupname, String channel_properties) {
        this.groupname=groupname;
        if(channel_properties != null)
            this.channel_properties=channel_properties;
    }

    /*
     * Uses a user-provided PullPushAdapter to create the dispatcher rather than a Channel. If id is non-null, it will be
     * used to register under that id. This is typically used when another building block is already using
     * PullPushAdapter, and we want to add this building block in addition. The id is the used to discriminate
     * between messages for the various blocks on top of PullPushAdapter. If null, we will assume we are the
     * first block created on PullPushAdapter.
     * @param adapter The PullPushAdapter which to use as underlying transport
     * @param id A serializable object (e.g. an Integer) used to discriminate (multiplex/demultiplex) between
     *           requests/responses for different building blocks on top of PullPushAdapter.
     * @param state_timeout Max number of milliseconds to wait until state is
     * retrieved
     */
    public DistributedTree(PullPushAdapter adapter, Serializable id, long state_timeout) 
        throws ChannelException {
        channel = (Channel)adapter.getTransport();
        disp=new RpcDispatcher(adapter, id, this, this, this);
        boolean flag = channel.getState(null, state_timeout);
        if(flag) {
            if(log.isInfoEnabled()) log.info("state was retrieved successfully");
        }
        else
            if(log.isInfoEnabled()) log.info("state could not be retrieved (must be first member in group)");
    }

    public Object getLocalAddress() {
        return channel != null? channel.getAddress() : null;
    }

    public void setDeadlockDetection(boolean flag) {
        if(disp != null)
            disp.setDeadlockDetection(flag);
    }

    public void start() throws Exception {
        start(8000);
    }


    public void start(long timeout) throws Exception {
        if(channel != null) // already started
            return;
        channel=new JChannel(channel_properties);
        disp=new RpcDispatcher(channel, this, this, this);
        channel.connect(groupname);
        rc=channel.getState(null, timeout);
        if(rc) {
            if(log.isInfoEnabled()) log.info("state was retrieved successfully");
        }
        else
            if(log.isInfoEnabled()) log.info("state could not be retrieved (must be first member in group)");
    }


    public void stop() {
        if(channel != null) {
            channel.close();
            disp.stop();
        }
        channel=null;
        disp=null;
    }


    public void addDistributedTreeListener(DistributedTreeListener listener) {
        if(!listeners.contains(listener))
            listeners.addElement(listener);
    }


    public void removeDistributedTreeListener(DistributedTreeListener listener) {
        listeners.removeElement(listener);
    }


    public void addViewListener(ViewListener listener) {
        if(!view_listeners.contains(listener))
            view_listeners.addElement(listener);
    }


    public void removeViewListener(ViewListener listener) {
        view_listeners.removeElement(listener);
    }


    public void add(String fqn) {
        //Changes done by <aos>
        //if true, propagate action to the group
        if(send_message == true) {
            try {
				MethodCall call = new MethodCall("_add", new Object[] {fqn}, new String[] {String.class.getName()});
                disp.callRemoteMethods(null, call, GroupRequest.GET_ALL, 0);
            }
            catch(Exception ex) {
                if(log.isErrorEnabled()) log.error("exception=" + ex);
            }
        }
        else {
            _add(fqn);
        }
    }

    public void add(String fqn, Serializable element) {
        add(fqn, element, 0);
    }

    /** resets an existing node, useful after a merge when you want to tell other 
     *  members of your state, but do not wish to remove and then add as two separate calls */
    public void reset(String fqn, Serializable element) 
    {
        reset(fqn, element, 0);
    }

    public void remove(String fqn) {
        remove(fqn, 0);
    }

    public void add(String fqn, Serializable element, int timeout) {
        //Changes done by <aos>
        //if true, propagate action to the group
        if(send_message == true) {
            try {
				MethodCall call = new MethodCall("_add", new Object[] {fqn, element}, 
                    new String[] {String.class.getName(), Serializable.class.getName()});
                disp.callRemoteMethods(null, call, GroupRequest.GET_ALL, timeout);
            }
            catch(Exception ex) {
                if(log.isErrorEnabled()) log.error("exception=" + ex);
            }
        }
        else {
            _add(fqn, element);
        }
    }

    /** resets an existing node, useful after a merge when you want to tell other 
     *  members of your state, but do not wish to remove and then add as two separate calls */
    public void reset(String fqn, Serializable element, int timeout) 
    {
        //Changes done by <aos>
        //if true, propagate action to the group
        if(send_message == true) {
            try {
				MethodCall call = new MethodCall("_reset", new Object[] {fqn, element}, 
                    new String[] {String.class.getName(), Serializable.class.getName()});
                disp.callRemoteMethods(null, call, GroupRequest.GET_ALL, timeout);
            }
            catch(Exception ex) {
                if(log.isErrorEnabled()) log.error("exception=" + ex);
            }
        }
        else {
            _add(fqn, element);
        }
    }

    public void remove(String fqn, int timeout) {
        //Changes done by <aos>
        //if true, propagate action to the group
        if(send_message == true) {
            try {
            	MethodCall call = new MethodCall("_remove", new Object[] {fqn}, new String[] {String.class.getName()});
                disp.callRemoteMethods(null, call, GroupRequest.GET_ALL, timeout);
            }
            catch(Exception ex) {
                if(log.isErrorEnabled()) log.error("exception=" + ex);
            }
        }
        else {
            _remove(fqn);
        }
    }


    public boolean exists(String fqn) {
        return fqn != null && (findNode(fqn) != null);
    }


    public Serializable get(String fqn) {
        Node n=null;

        if(fqn == null) return null;
        n=findNode(fqn);
        if(n != null) {
            return n.element;
        }
        return null;
    }


    public void set(String fqn, Serializable element) {
		set(fqn, element, 0);
    }

    public void set(String fqn, Serializable element, int timeout) {
		//Changes done by <aos>
		//if true, propagate action to the group
        if(send_message == true) {
            try {
				MethodCall call = new MethodCall("_set", new Object[] {fqn, element}, 
                    new String[] {String.class.getName(), Serializable.class.getName()});
                disp.callRemoteMethods(null, call, GroupRequest.GET_ALL, timeout);
            }
            catch(Exception ex) {
                if(log.isErrorEnabled()) log.error("exception=" + ex);
            }
        }
        else {
            _set(fqn, element);
        }
    }


    /** Returns all children of a Node as strings */
    public Vector getChildrenNames(String fqn) {
        Vector ret=new Vector();
        Node n;

        if(fqn == null) return ret;
        n=findNode(fqn);
        if(n == null || n.children == null) return ret;
        for(int i=0; i < n.children.size(); i++)
            ret.addElement(((Node)n.children.elementAt(i)).name);
        return ret;
    }


    public String print() {
        StringBuilder sb=new StringBuilder();
        int indent=0;

        if(root == null)
            return "/";

        sb.append(root.print(indent));
        return sb.toString();
    }


    /** Returns all children of a Node as Nodes */
    Vector getChildren(String fqn) {
        Node n;

        if(fqn == null) return null;
        n=findNode(fqn);
        if(n == null) return null;
        return n.children;
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
    public Channel getChannel()             {return channel;}

   /**
     * Returns the number of current members joined to the group
     * @return int
     */
    public int getGroupMembersNumber()			{return members.size();}




    /*--------------------- Callbacks --------------------------*/

    public void _add(String fqn) {
        _add(fqn, null);
    }


    public void _add(String fqn, Serializable element) {
        Node curr, n;
        StringTokenizer tok;
        String child_name;
        String tmp_fqn="";

        if(root == null) {
            root=new Node("/", null);
            notifyNodeAdded("/", null);
        }
        if(fqn == null)
            return;
        curr=root;
        tok=new StringTokenizer(fqn, "/");

        while(tok.hasMoreTokens()) {
            child_name=tok.nextToken();
            tmp_fqn=tmp_fqn + '/' + child_name;
            n=curr.findChild(child_name);
            if(n == null) {
                n=new Node(child_name, null);
                curr.addChild(n);
                if(!tok.hasMoreTokens()) {
                    n.element=element;
                    notifyNodeAdded(tmp_fqn, element);
                    return;
                }
                else
                    notifyNodeAdded(tmp_fqn, null);
            }
            curr=n;
        }
        // If the element is not null, we install it and notify the
        // listener app that the node is modified.
        if(element != null){
        	curr.element=element;
        	notifyNodeModified(fqn, null, element);
        }
    }


    public void _remove(String fqn) {
        Node curr, n;
        StringTokenizer tok;
        String child_name=null;

        if(fqn == null || root == null)
            return;
        curr=root;
        tok=new StringTokenizer(fqn, "/");

        while(tok.countTokens() > 1) {
            child_name=tok.nextToken();
            n=curr.findChild(child_name);
            if(n == null) // node does not exist
                return;
            curr=n;
        }
        try {
            child_name=tok.nextToken();
            if(child_name != null) {
                n=curr.removeChild(child_name);
                if(n != null)
                    notifyNodeRemoved(fqn);
            }
        }
        catch(Exception ex) {
        }
    }


    public void _set(String fqn, Serializable element) {
        Node n;
        Serializable old_el=null;

        if(fqn == null || element == null) return;
        n=findNode(fqn);
        if(n == null) {
            if(log.isErrorEnabled()) log.error("node " + fqn + " not found");
            return;
        }
        old_el=n.element;
        n.element=element;
        notifyNodeModified(fqn, old_el, element);
    }

    /** similar to set, but does not error if node does not exist, but rather does an add instead */
    public void _reset(String fqn, Serializable element) {
        Node n;
        Serializable old_el=null;

        if(fqn == null || element == null) return;
        n=findNode(fqn);
        if(n == null) {
            _add(fqn, element);
        }
        else {
            old_el=n.element;
            n.element=element;
        }
        notifyNodeModified(fqn, old_el, element);
    }

    /*----------------- End of  Callbacks ----------------------*/






    /*-------------------- State Exchange ----------------------*/

    public void receive(Message msg) {
    }

    /** Return a copy of the tree */
    public byte[] getState() {
        Object copy=root != null? root.copy() : null;
        try {
            return Util.objectToByteBuffer(copy);
        }
        catch(Throwable ex) {
            if(log.isErrorEnabled()) log.error("exception marshalling state: " + ex);
            return null;
        }
    }

    public void setState(byte[] data) {
        Object new_state;

        try {
            new_state=Util.objectFromByteBuffer(data);
        }
        catch(Throwable ex) {
            if(log.isErrorEnabled()) log.error("exception unmarshalling state: " + ex);
            return;
        }
        if(new_state == null) return;
        if(!(new_state instanceof Node)) {
            if(log.isErrorEnabled()) log.error("object is not of type 'Node'");
            return;
        }
        root=((Node)new_state).copy();
        
        // State transfer needs to notify listeners in the new
        // cluster member about everything that exists.  This
        // is working ok now.
        this.notifyAllNodesCreated(root, "");
    }



    /*------------------- Membership Changes ----------------------*/

    public void viewAccepted(View new_view) {
        Vector new_mbrs=new_view.getMembers();

        if(new_mbrs != null) {
            sendViewChangeNotifications(new_mbrs, members); // notifies observers (joined, left)
            members.removeAllElements();
            for(int i=0; i < new_mbrs.size(); i++)
                members.addElement(new_mbrs.elementAt(i));
        }
		//if size is bigger than one, there are more peers in the group
		//otherwise there is only one server.
        send_message=true;
        send_message=members.size() > 1;
    }


    /** Called when a member is suspected */
    public void suspect(Address suspected_mbr) {
    }


    /** Block sending and receiving of messages until ViewAccepted is called */
    public void block() {
    }


    void sendViewChangeNotifications(Vector new_mbrs, Vector old_mbrs) {
        Vector joined, left;
        Object mbr;

        if(view_listeners.isEmpty() || old_mbrs == null || new_mbrs == null)
            return;


        // 1. Compute set of members that joined: all that are in new_mbrs, but not in old_mbrs
        joined=new Vector();
        for(int i=0; i < new_mbrs.size(); i++) {
            mbr=new_mbrs.elementAt(i);
            if(!old_mbrs.contains(mbr))
                joined.addElement(mbr);
        }


        // 2. Compute set of members that left: all that were in old_mbrs, but not in new_mbrs
        left=new Vector();
        for(int i=0; i < old_mbrs.size(); i++) {
            mbr=old_mbrs.elementAt(i);
            if(!new_mbrs.contains(mbr))
                left.addElement(mbr);
        }
        notifyViewChange(joined, left);
    }


    private Node findNode(String fqn) {
        Node curr=root;
        StringTokenizer tok;
        String child_name;

        if(fqn == null || root == null) return null;
        if("/".equals(fqn) || "".equals(fqn))
            return root;

        tok=new StringTokenizer(fqn, "/");
        while(tok.hasMoreTokens()) {
            child_name=tok.nextToken();
            curr=curr.findChild(child_name);
            if(curr == null) return null;
        }
        return curr;
    }


    void notifyNodeAdded(String fqn, Serializable element) {
        for(int i=0; i < listeners.size(); i++)
            ((DistributedTreeListener)listeners.elementAt(i)).nodeAdded(fqn, element);
    }

    void notifyNodeRemoved(String fqn) {
        for(int i=0; i < listeners.size(); i++)
            ((DistributedTreeListener)listeners.elementAt(i)).nodeRemoved(fqn);
    }

    void notifyNodeModified(String fqn, Serializable old_element, Serializable new_element) {
        for(int i=0; i < listeners.size(); i++)
            ((DistributedTreeListener)listeners.elementAt(i)).nodeModified(fqn, old_element, new_element);
    }

    /** Generates NodeAdded notifications for all nodes of the tree. This is called whenever the tree is
     initially retrieved (state transfer) */
    void notifyAllNodesCreated(Node curr, String tmp_fqn) {
        Node n;
        // We need a local string here to handle the empty string (root)
        // otherwise, we start off with two slashes in the path.
        String path = "";
        if(curr == null) return;
        if(curr.name == null) {
            if(log.isErrorEnabled()) log.error("curr.name is null");
            return;
        }
        // If we're the root node, then we supply a "starter" slash.
        // This lets us properly initiate the recursion with an empty
        // string, and then prepend a slash for each additional depth
        path = (curr.equals(root)) ? "/" : tmp_fqn;
        
        // Recursion must occur _before_ we look for children, or we
        // never notifyNodeAdded() for leaf nodes.
        notifyNodeAdded(path, curr.element);
        if(curr.children != null) {
            for(int i=0; i < curr.children.size(); i++) {
                n=(Node)curr.children.elementAt(i);
                System.out.println("*** nodeCreated(): tmp_fqn is " + tmp_fqn);         
                notifyAllNodesCreated(n, tmp_fqn + '/' + n.name);
            }
        }
    }


    void notifyViewChange(Vector new_mbrs, Vector old_mbrs) {
        for(int i=0; i < view_listeners.size(); i++)
            ((ViewListener)view_listeners.elementAt(i)).viewChange(new_mbrs, old_mbrs);
    }


    private static class Node implements Serializable {
        String name=null;
        Vector children=null;
        Serializable element=null;
        private static final long serialVersionUID=-635336369135391033L;


        Node() {
        }

        Node(String name, Serializable element) {
            this.name=name;
            this.element=element;
        }


        void addChild(String relative_name, Serializable element) {
            if(relative_name == null)
                return;
            if(children == null)
                children=new Vector();
            else {
                if(!children.contains(relative_name))
                    children.addElement(new Node(relative_name, element));
            }
        }


        void addChild(Node n) {
            if(n == null) return;
            if(children == null)
                children=new Vector();
            if(!children.contains(n))
                children.addElement(n);
        }


        Node removeChild(String rel_name) {
            Node n=findChild(rel_name);

            if(n != null)
                children.removeElement(n);
            return n;
        }


        Node findChild(String relative_name) {
            Node child;

            if(children == null || relative_name == null)
                return null;
            for(int i=0; i < children.size(); i++) {
                child=(Node)children.elementAt(i);
                if(child.name == null) {
                    if(log.isErrorEnabled()) log.error("child.name is null for " + relative_name);
                    continue;
                }

                if(child.name.equals(relative_name))
                    return child;
            }

            return null;
        }


        public boolean equals(Object other) {
            return other != null && ((Node)other).name != null && name != null && name.equals(((Node)other).name);
        }


        Node copy() {
            Node ret=new Node(name, element);

            if(children != null)
                ret.children=(Vector)children.clone();
            return ret;
        }


        String print(int indent) {
            StringBuilder sb=new StringBuilder();
            boolean is_root=name != null && "/".equals(name);

            for(int i=0; i < indent; i++)
                sb.append(' ');
            if(!is_root) {
                if(name == null)
                    sb.append("/<unnamed>");
                else {
                    sb.append('/' + name);
                    // if(element != null) sb.append(" --> " + element);
                }
            }
            sb.append('\n');
            if(children != null) {
                if(is_root)
                    indent=0;
                else
                    indent+=4;
                for(int i=0; i < children.size(); i++)
                    sb.append(((Node)children.elementAt(i)).print(indent));
            }
            return sb.toString();
        }


        public String toString() {
            if(element != null)
                return "[name: " + name + ", element: " + element + ']';
            else
                return "[name: " + name + ']';
        }

    }


}


