// $Id: DistributedTreeDemo.java,v 1.4 2004/06/25 01:10:28 belaban Exp $

package org.jgroups.demos;


import org.jgroups.blocks.DistributedTree;

import javax.swing.*;
import javax.swing.event.TableModelEvent;
import javax.swing.event.TableModelListener;
import javax.swing.event.TreeSelectionEvent;
import javax.swing.event.TreeSelectionListener;
import javax.swing.table.DefaultTableModel;
import javax.swing.tree.DefaultMutableTreeNode;
import javax.swing.tree.DefaultTreeModel;
import javax.swing.tree.TreeNode;
import javax.swing.tree.TreePath;
import java.awt.*;
import java.awt.event.*;
import java.io.Serializable;
import java.util.Enumeration;
import java.util.Properties;
import java.util.StringTokenizer;
import java.util.Vector;





class MyNode extends DefaultMutableTreeNode {
    String name="<unnamed>";


    MyNode(String name) {
	this.name=name;
    }
    
    MyNode(String name, Serializable user_obj) {
	super(user_obj);
	this.name=name;
    }


    void add(String fqn) {
	add(fqn, null);
    }

    public void add(String fqn, Serializable user_obj) {
	MyNode            curr, n;
	StringTokenizer   tok;
	String            child_name;

	if(fqn == null) return;
	curr=this;
	tok=new StringTokenizer(fqn, "/");

	while(tok.hasMoreTokens()) {
	    child_name=tok.nextToken();
	    n=curr.findChild(child_name);
	    if(n == null) {
		n=new MyNode(child_name, user_obj);
		curr.add(n);
	    }
	    curr=n;
	}
	curr.userObject=user_obj;
    }




    void modify(String fqn, Serializable new_element) {
	if(fqn == null || new_element == null) return;
	MyNode n=findNode(fqn);
	if(n != null)
	    n.userObject=new_element;
    }


    void remove(String fqn) {
	System.out.println("MyNode.remove(" + fqn + ")");
	removeFromParent();
    }
    
    
    public MyNode findNode(String fqn) {
	MyNode            curr, n;
	StringTokenizer   tok;
	String            child_name;

	if(fqn == null) return null;
	curr=this;
	tok=new StringTokenizer(fqn, "/");

	while(tok.hasMoreTokens()) {
	    child_name=tok.nextToken();
	    n=curr.findChild(child_name);
	    if(n == null)
		return null;
	    curr=n;
	}
	return curr;
    }



    MyNode findChild(String relative_name) {
	MyNode  child;

	if(relative_name == null || getChildCount() == 0)
	    return null;
	for(int i=0; i < getChildCount(); i++) {
	    child=(MyNode)getChildAt(i);
	    if(child.name == null) {
		System.err.println("MyNode.findChild(" + relative_name + "): child.name is null");
		continue;
	    }
		
	    if(child.name.equals(relative_name))
		return child;
	}
	return null;
    }


    
    String print(int indent) {
	StringBuffer sb=new StringBuffer();

	for(int i=0; i < indent; i++)
	    sb.append(" ");
	if(!isRoot()) {
	    if(name == null) 
		sb.append("/<unnamed>");
	    else {
		sb.append("/" + name);
		if(userObject != null)
		    sb.append(" --> " + userObject);
	    }
	}
	sb.append("\n");
	if(getChildCount() > 0) {
	    if(isRoot()) indent=0;
	    else indent+=4;
	    for(int i=0; i < getChildCount(); i++)
		sb.append(((MyNode)getChildAt(i)).print(indent));
	}
	return sb.toString();
    }


    public String toString() {
	return name;
    }


}



/**
 * Demo showing the DistributedTree class. It displays a panel with the tree structure in the upper half,
 * and the properties of a chosen node on the bottom half. All updates are broadcast to all members.
 */
public class DistributedTreeDemo extends Frame implements WindowListener, 
							  DistributedTree.DistributedTreeListener,
							  TreeSelectionListener, TableModelListener {
    DefaultTreeModel         tree_model=null;
    JTree                    jtree=null;
    DefaultTableModel        table_model=new DefaultTableModel();
    JTable                   table=new JTable(table_model);
    JScrollPane              scroll_pane=null;
    MyNode                   root=new MyNode("/");
    DistributedTree          dt=null;
    String                   props=null;
    String                   selected_node=null;
    boolean                  create=false;

  
    public DistributedTreeDemo(boolean create) throws Exception {	
	// we need state transfer here
	// props="UDP:PING:FD:STABLE:NAKACK:UNICAST:FRAG:FLUSH:GMS:VIEW_ENFORCER:STATE_TRANSFER:QUEUE";

	// test for pbcast
        props="UDP(mcast_addr=228.1.2.3;mcast_port=45566;ip_ttl=0):" +
                "PING(timeout=5000;num_initial_members=6):" +
                "FD_SOCK:" +
                "VERIFY_SUSPECT(timeout=1500):" +
                "pbcast.NAKACK(gc_lag=5;retransmit_timeout=3000):" +
                "UNICAST(timeout=5000):" +
                "pbcast.STABLE(desired_avg_gossip=10000):" +
                "FRAG(down_thread=false;up_thread=false):" +
                "pbcast.GMS(join_timeout=5000;join_retry_timeout=2000;" +
                "shun=false;print_local_addr=true):" +
                "pbcast.STATE_TRANSFER";


	this.create=create;
	dt=new DistributedTree("DistributedTreeDemo", props);
	dt.addDistributedTreeListener(this);
	setLayout(new BorderLayout());
        addNotify();
	setSize(getInsets().left + getInsets().right + 485, 
		getInsets().top + getInsets().bottom + 367);
        setTitle("DistributedTree");
        
        tree_model=new DefaultTreeModel(root);
        jtree=new JTree(tree_model);
        jtree.setDoubleBuffered(true);
	
        scroll_pane=new JScrollPane();
        scroll_pane.getViewport().add(jtree);
        scroll_pane.setDoubleBuffered(true);        
        add(scroll_pane, BorderLayout.CENTER);
        addWindowListener(this);
	
	table_model.setColumnIdentifiers(new String[]{"Name", "Value"});
	table_model.addTableModelListener(this);
	add(table, BorderLayout.SOUTH);
	
	
	dt.start();
	System.out.println("Constructing initial GUI tree");
	populateTree(dt, ""); // creates initial GUI from model
	System.out.println("Constructing initial GUI tree -- done");


	Properties props1=new Properties();
	props1.setProperty("name",   "EventService");
	props1.setProperty("path",   "/usr/local/Orbix2000/bin/es");
	props1.setProperty("up",     "true");
	props1.setProperty("active", "false");

	Properties props2=new Properties();
	props2.setProperty("name", "NamingService");
	props2.setProperty("path", "/usr/local/Orbix2000/bin/ns");
	props2.setProperty("up",   "true");
	props2.setProperty("active", "true");

	Properties props3=new Properties();
	props3.setProperty("name", "ORBIX daemon");
	props3.setProperty("path", "/usr/local/Orbix2000/bin/orbixd");
	props3.setProperty("up",   "true");
	props3.setProperty("active", "true");
	props3.setProperty("restart", "true");
	props3.setProperty("restart_time", "3000");
	props3.setProperty("restart_max", "10");

	Properties props4=new Properties();
	props4.setProperty("name", "Orbix2000 Version 1.1");
	props4.setProperty("valid until", "11/12/2001");
	props4.setProperty("up", "false");
	props4.setProperty("active", "false");

	Properties props5=new Properties();
	props5.setProperty("name", "Orbix2000 Version 1.3b");
	props5.setProperty("valid until", "12/31/2000");
	props5.setProperty("up", "true");
	props5.setProperty("active", "false");


	if(create) {
	    dt.add("/procs/NETSMART/es",                    props1);
	    dt.add("/procs/NETSMART/ns",                    props2);
	    dt.add("/procs/NETSMART/orbixd",                props3);
	    dt.add("/procs/NETSMART/orbixd/Version_1.1",    props4);
	    dt.add("/procs/NETSMART/orbixd/Version_1.2",    props5);
	    Properties props6=(Properties)props5.clone();
	    props6.setProperty("name", "osagent daemon");
	    props6.setProperty("path", "/usr/local/Visigenics/bin/osagent");
	    
	    Properties props7=new Properties();
	    props7.setProperty("name", "Visigenics latest product");
	    props7.setProperty("license", "/vob/iem/Devp/etc/license.txt");
	    
	    dt.set("/procs/NETSMART/orbixd/Version_1.2",    props6);
	    dt.add("/procs/NETSMART/orbixd/Version_2.0",    props7);
	}


	jtree.addTreeSelectionListener(this);
	
	
	MouseListener ml = new MouseAdapter() {
	    public void mouseClicked(MouseEvent e) {
		int selRow = jtree.getRowForLocation(e.getX(), e.getY());
		TreePath selPath = jtree.getPathForLocation(e.getX(), e.getY());
		if(selRow != -1)
		    selected_node=makeFQN(selPath.getPath());
	    }
	};
	jtree.addMouseListener(ml);
	
    }


    String makeFQN(Object[] path) {
	StringBuffer sb=new StringBuffer("");
	String       tmp_name;

	if(path == null) return null;
	for(int i=0; i < path.length; i++) {
	    tmp_name=((MyNode)path[i]).name;
	    if(tmp_name.equals("/"))
		continue;
	    else
		sb.append("/" + tmp_name);
	}
	tmp_name=sb.toString();
	if(tmp_name.length() == 0)
	    return "/";
	else
	    return tmp_name;
    }


    void clearTable() {
	int num_rows=table.getRowCount();

	if(num_rows > 0) {
	    for(int i=0; i < num_rows; i++)
		table_model.removeRow(0);
	    table_model.fireTableRowsDeleted(0, num_rows-1);
	    repaint();
	}
    }

    void populateTable(Properties props) {
	String key, val;
	int    num_rows=0;

	if(props == null) return;
	num_rows=props.size();
	clearTable();

	if(num_rows > 0) {
	    for(Enumeration e=props.keys(); e.hasMoreElements();) {
		key=(String)e.nextElement();
		val=(String)props.get(key);
		if(val == null) val="<n/a>";
		table_model.addRow(new Object[]{key, val});
	    }
	    
	    table_model.fireTableRowsInserted(0, num_rows-1);
	    validate();
	}
	
    }


    void populateTree(DistributedTree tree, String tmp_fqn) {
	if(tree == null) return;
	Vector        children=tree.getChildrenNames(tmp_fqn);
	String        child_name, tmp_name;
	Serializable  element;
	
	for(int i=0; i < children.size(); i++) {
	    child_name=(String)children.elementAt(i);
	    tmp_name=tmp_fqn + "/" + child_name;
	    root.add(tmp_name, tree.get(tmp_name));
	    populateTree(tree, tmp_name);
	}
    }



    public synchronized void setVisible(boolean show) {
        setLocation(50, 50);
        super.setVisible(show);
    }
   
    public void windowClosed(WindowEvent event) {}
    public void windowDeiconified(WindowEvent event) {}
    public void windowIconified(WindowEvent event) {}
    public void windowActivated(WindowEvent event) {}
    public void windowDeactivated(WindowEvent event) {}
    public void windowOpened(WindowEvent event) {}
  
    public void windowClosing(WindowEvent event) {
	dt.stop();
        System.exit(0);
    }


    
    public void tableChanged(TableModelEvent evt) {
	int         row, col;
	String      key, val;

	if(evt.getType() == TableModelEvent.UPDATE) {
	    row=evt.getFirstRow();
	    col=evt.getColumn();

	    Properties props=(Properties)dt.get(selected_node);	    
	    if(col == 0) {  // set()
		key=(String)table_model.getValueAt(row, col);
		val=(String)table_model.getValueAt(row, col+1);
		if(props != null && key != null && val != null) {
		    props.setProperty(key, val);
		    dt.set(selected_node, props);
		}
	    }
	    else {          // add()
		key=(String)table_model.getValueAt(row, col-1);
		val=(String)table.getValueAt(row, col);
		if(props != null && key != null && val != null) {
		    props.setProperty(key, val);
		    dt.add(selected_node, props);
		}
	    }
	    System.out.println("key=" + key + ", val=" + val);

	}
    }

    

    public void valueChanged(TreeSelectionEvent evt) {
	TreePath    path=evt.getPath();
	String      fqn="/";
	String      component_name;
	Properties  props=null;
	
	for(int i=0; i < path.getPathCount(); i++) {
	    component_name=((MyNode)path.getPathComponent(i)).name;
	    if(component_name.equals("/"))
		continue;
	    if(fqn.equals("/"))
		fqn+=component_name;
	    else
		fqn=fqn + "/" + component_name;
	}
	props=(Properties)dt.get(fqn);
	if(props != null)
	    populateTable(props);
	else
	    clearTable();
    }



    /* ------------------ DistributedTree.DistributedTreeListener interface ------------ */

    public void nodeAdded(String fqn, Serializable element) {
	MyNode n;
	System.out.println("** nodeCreated(" + fqn + ")");

	root.add(fqn, element);
	n=root.findNode(fqn);
	if(n != null)
	    tree_model.reload(n.getParent());
    }

    public void nodeRemoved(String fqn) {
	MyNode   n;
	TreeNode par;
	System.out.println("** nodeRemoved(" + fqn + ")");
	n=root.findNode(fqn);
	if(n != null) {
	    n.removeAllChildren();	    
	    par=n.getParent();
	    n.removeFromParent();
	    tree_model.reload(par);
	}
    }

    public void nodeModified(String fqn, Serializable old_element, Serializable new_element) {
	System.out.println("** nodeModified(" + fqn + ")");
	root.modify(fqn, new_element);
	populateTable((Properties)new_element);
    }
    
    /* ---------------- End of DistributedTree.DistributedTreeListener interface -------- */

   
    public static void main(String args[]) {
	DistributedTreeDemo demo;
	boolean             create=false;

	for(int i=0; i < args.length; i++) {
	    if(args[i].equals("-help")) {
		System.out.println("DistributedTreeDemo [-create] [-help]");
		return;
	    }
	    if(args[i].equals("-create")) {
		create=true;
		continue;
	    }
	}

	try {
	    demo=new DistributedTreeDemo(create);
	    demo.setVisible(true);
	}
	catch(Exception ex) {
	    System.err.println(ex);
	}
    }  
}
