// $Id: ReplicatedTreeDemo.java,v 1.7 2005/06/14 08:36:49 belaban Exp $

package org.jgroups.demos;


import org.jgroups.View;
import org.jgroups.blocks.ReplicatedTree;

import javax.swing.*;
import javax.swing.event.TableModelEvent;
import javax.swing.event.TableModelListener;
import javax.swing.event.TreeSelectionEvent;
import javax.swing.event.TreeSelectionListener;
import javax.swing.table.DefaultTableModel;
import javax.swing.table.TableColumn;
import javax.swing.tree.*;
import java.awt.*;
import java.awt.event.*;
import java.io.File;
import java.util.*;


/**
 * Graphical view of a ReplicatedTree
 *
 * @author Bela Ban
 */
public class ReplicatedTreeDemo {


    /**
     * Graphical view of a ReplicatedTree (using the MVC paradigm). An instance of this class needs to be given a
     * reference to the underlying model (ReplicatedTree) and needs to registers as a ReplicatedTreeListener. Changes
     * to the tree structure are propagated from the model to the view (via ReplicatedTreeListener), changes from the
     * GUI (e.g. by a user) are executed on the tree model (which will broadcast the changes to all replicas).<p>
     * The view itself caches only the nodes, but doesn't cache any of the data (HashMap) associated with it. When
     * data needs to be displayed, the underlying tree will be accessed directly.
     *
     * @author Bela Ban
     */
    static class ReplicatedTreeView extends JFrame implements WindowListener, ReplicatedTree.ReplicatedTreeListener,
            TreeSelectionListener, TableModelListener {
        DefaultTreeModel tree_model=null;
        JTree jtree=null;
        final DefaultTableModel table_model=new DefaultTableModel();
        final JTable table=new JTable(table_model);
        final MyNode root=new MyNode(SEP);
        final String props=null;
        String selected_node=null;
        ReplicatedTree tree=null;  // the underlying model
        JPanel tablePanel=null;
        JMenu operationsMenu=null;
        JPopupMenu operationsPopup=null;
        JMenuBar menubar=null;
        static final String SEP=ReplicatedTree.SEPARATOR;
        private static final int KEY_COL_WIDTH=20;
        private static final int VAL_COL_WIDTH=300;


        public ReplicatedTreeView(ReplicatedTree tree, Object title) throws Exception {
            this.tree=tree;
            tree.addReplicatedTreeListener(this);

            addNotify();
            setTitle("ReplicatedTreeDemo: mbr=" + title);

            tree_model=new DefaultTreeModel(root);
            jtree=new JTree(tree_model);
            jtree.setDoubleBuffered(true);
            jtree.getSelectionModel().setSelectionMode(TreeSelectionModel.SINGLE_TREE_SELECTION);

            JScrollPane scroll_pane=new JScrollPane(jtree);

            populateTree();

            getContentPane().add(scroll_pane, BorderLayout.CENTER);
            addWindowListener(this);

            table_model.setColumnIdentifiers(new String[]{"Name", "Value"});
            table_model.addTableModelListener(this);

            setTableColumnWidths();

            tablePanel=new JPanel();
            tablePanel.setLayout(new BorderLayout());
            tablePanel.add(table.getTableHeader(), BorderLayout.NORTH);
            tablePanel.add(table, BorderLayout.CENTER);

            getContentPane().add(tablePanel, BorderLayout.SOUTH);

            jtree.addTreeSelectionListener(this);//REVISIT

            MouseListener ml=new MouseAdapter() {
                public void mouseClicked(MouseEvent e) {
                    int selRow=jtree.getRowForLocation(e.getX(), e.getY());
                    TreePath selPath=jtree.getPathForLocation(e.getX(), e.getY());
                    if(selRow != -1) {
                        selected_node=makeFQN(selPath.getPath());
                        jtree.setSelectionPath(selPath);

                        if(e.getModifiers() == java.awt.event.InputEvent.BUTTON3_MASK) {
                            operationsPopup.show(e.getComponent(),
                                    e.getX(), e.getY());
                        }
                    }
                }
            };

            jtree.addMouseListener(ml);

            createMenus();
            setLocation(50, 50);
            setSize(getInsets().left + getInsets().right + 485,
                    getInsets().top + getInsets().bottom + 367);

            init();
            setVisible(true);
        }

        public void windowClosed(WindowEvent event) {
        }

        public void windowDeiconified(WindowEvent event) {
        }

        public void windowIconified(WindowEvent event) {
        }

        public void windowActivated(WindowEvent event) {
        }

        public void windowDeactivated(WindowEvent event) {
        }

        public void windowOpened(WindowEvent event) {
        }

        public void windowClosing(WindowEvent event) {
            System.exit(0);
        }


        public void tableChanged(TableModelEvent evt) {
            int row, col;
            String key, val;

            if(evt.getType() == TableModelEvent.UPDATE) {
                row=evt.getFirstRow();
                col=evt.getColumn();
                if(col == 0) {  // set()
                    key=(String)table_model.getValueAt(row, col);
                    val=(String)table_model.getValueAt(row, col + 1);
                    if(key != null && val != null) {
                        tree.put(selected_node, key, val);
                    }
                }
                else {          // add()
                    key=(String)table_model.getValueAt(row, col - 1);
                    val=(String)table.getValueAt(row, col);
                    if(key != null && val != null) {
                        tree.put(selected_node, key, val);
                    }
                }
            }
        }


        public void valueChanged(TreeSelectionEvent evt) {
            TreePath path=evt.getPath();
            String fqn=SEP;
            String component_name;
            HashMap data=null;

            for(int i=0; i < path.getPathCount(); i++) {
                component_name=((MyNode)path.getPathComponent(i)).name;
                if(component_name.equals(SEP))
                    continue;
                if(fqn.equals(SEP))
                    fqn+=component_name;
                else
                    fqn=fqn + SEP + component_name;
            }
            data=getData(tree, fqn);
            if(data != null) {
                getContentPane().add(tablePanel, BorderLayout.SOUTH);
                populateTable(data);
                validate();
            }
            else {
                clearTable();
                getContentPane().remove(tablePanel);
                validate();
            }
        }



        /* ------------------ ReplicatedTree.ReplicatedTreeListener interface ------------ */

        public void nodeAdded(String fqn) {
            MyNode n, p;

            n=root.add(fqn);
            if(n != null) {
                p=(MyNode)n.getParent();
                tree_model.reload(p);
                jtree.scrollPathToVisible(new TreePath(n.getPath()));
            }
        }

        public void nodeRemoved(String fqn) {
            MyNode n;
            TreeNode par;

            n=root.findNode(fqn);
            if(n != null) {
                n.removeAllChildren();
                par=n.getParent();
                n.removeFromParent();
                tree_model.reload(par);
            }
        }

        public void nodeModified(String fqn) {
//            HashMap data;
  //          data=getData(tree, fqn);
            //populateTable(data); REVISIT
            /*
              poulateTable is the current table being shown is the info of the node. that is modified.
            */
        }

        public void viewChange(View new_view) {
            Vector mbrship;
            if(new_view != null && (mbrship=new_view.getMembers()) != null) {
                tree._put(SEP, "members", mbrship);
                tree._put(SEP, "coordinator", mbrship.firstElement());
            }
        }




        /* ---------------- End of ReplicatedTree.ReplicatedTreeListener interface -------- */

        /*----------------- Runnable implementation to make View change calles in AWT Thread ---*/

        public void run() {

        }



        /* ----------------------------- Private Methods ---------------------------------- */

        /**
         * Fetches all data from underlying tree model and display it graphically
         */
        void init() {
            Vector mbrship=null;

            addGuiNode(SEP);

            mbrship=tree != null && tree.getMembers() != null ? (Vector)tree.getMembers().clone() : null;
            if(mbrship != null) {
                tree._put(SEP, "members", mbrship);
                tree._put(SEP, "coordinator", mbrship.firstElement());
            }
        }


        /**
         * Fetches all data from underlying tree model and display it graphically
         */
        private void populateTree() {
            addGuiNode(SEP);
        }


        /**
         * Recursively adds GUI nodes starting from fqn
         */
        void addGuiNode(String fqn) {
            Set children;
            String child_name;

            if(fqn == null) return;

            // 1 . Add myself
            root.add(fqn);

            // 2. Then add my children
            children=tree.getChildrenNames(fqn);
            if(children != null) {
                for(Iterator it=children.iterator(); it.hasNext();) {
                    child_name=(String)it.next();
                    addGuiNode(fqn + SEP + child_name);
                }
            }
        }


        String makeFQN(Object[] path) {
            StringBuffer sb=new StringBuffer("");
            String tmp_name;

            if(path == null) return null;
            for(int i=0; i < path.length; i++) {
                tmp_name=((MyNode)path[i]).name;
                if(tmp_name.equals(SEP))
                    continue;
                else
                    sb.append(SEP + tmp_name);
            }
            tmp_name=sb.toString();
            if(tmp_name.length() == 0)
                return SEP;
            else
                return tmp_name;
        }

        void clearTable() {
            int num_rows=table.getRowCount();

            if(num_rows > 0) {
                for(int i=0; i < num_rows; i++)
                    table_model.removeRow(0);
                table_model.fireTableRowsDeleted(0, num_rows - 1);
                repaint();
            }
        }


        void populateTable(HashMap data) {
            String key, strval="<null>";
            Object val;
            int num_rows=0;
            Map.Entry entry;

            if(data == null) return;
            num_rows=data.size();
            clearTable();

            if(num_rows > 0) {
                for(Iterator it=data.entrySet().iterator(); it.hasNext();) {
                    entry=(Map.Entry)it.next();
                    key=(String)entry.getKey();
                    val=entry.getValue();
                    if(val != null) strval=val.toString();
                    table_model.addRow(new Object[]{key, strval});
                }
                table_model.fireTableRowsInserted(0, num_rows - 1);
                validate();
            }
        }

        private void setTableColumnWidths() {
            table.sizeColumnsToFit(JTable.AUTO_RESIZE_NEXT_COLUMN);
            TableColumn column=null;
            column=table.getColumnModel().getColumn(0);
            column.setMinWidth(KEY_COL_WIDTH);
            column.setPreferredWidth(KEY_COL_WIDTH);
            column=table.getColumnModel().getColumn(1);
            column.setPreferredWidth(VAL_COL_WIDTH);
        }

        private void createMenus() {
            menubar=new JMenuBar();
            operationsMenu=new JMenu("Operations");
            AddNodeAction addNode=new AddNodeAction();
            addNode.putValue(AbstractAction.NAME, "Add to this node");
            RemoveNodeAction removeNode=new RemoveNodeAction();
            removeNode.putValue(AbstractAction.NAME, "Remove this node");
            AddModifyDataForNodeAction addModAction=new AddModifyDataForNodeAction();
            addModAction.putValue(AbstractAction.NAME, "Add/Modify data");
            ExitAction exitAction=new ExitAction();
            exitAction.putValue(AbstractAction.NAME, "Exit");
            operationsMenu.add(addNode);
            operationsMenu.add(removeNode);
            operationsMenu.add(addModAction);
            operationsMenu.add(exitAction);
            menubar.add(operationsMenu);
            setJMenuBar(menubar);

            operationsPopup=new JPopupMenu();
            operationsPopup.add(addNode);
            operationsPopup.add(removeNode);
            operationsPopup.add(addModAction);
        }

        HashMap getData(ReplicatedTree tree, String fqn) {
            HashMap data;
            Set keys;
            String key;
            Object value;

            if(tree == null || fqn == null) return null;
            keys=tree.getKeys(fqn);
            if(keys == null) return null;
            data=new HashMap();
            for(Iterator it=keys.iterator(); it.hasNext();) {
                key=(String)it.next();
                value=tree.get(fqn, key);
                if(value != null)
                    data.put(key, value);
            }
            return data;
        }




        /* -------------------------- End of Private Methods ------------------------------ */

        /*----------------------- Actions ---------------------------*/
        class ExitAction extends AbstractAction {
            public void actionPerformed(ActionEvent e) {
                System.exit(0);
            }
        }

        class AddNodeAction extends AbstractAction {
            public void actionPerformed(ActionEvent e) {
                JTextField fqnTextField=new JTextField();
                if(selected_node != null)
                    fqnTextField.setText(selected_node);
                Object[] information={"Enter fully qualified name",
                                      fqnTextField};
                final String btnString1="OK";
                final String btnString2="Cancel";
                Object[] options={btnString1, btnString2};
                int userChoice=JOptionPane.showOptionDialog(null,
                        information,
                        "Add Node",
                        JOptionPane.YES_NO_OPTION,
                        JOptionPane.PLAIN_MESSAGE,
                        null,
                        options,
                        options[0]);
                if(userChoice == 0) {
                    String userInput=fqnTextField.getText();
                    tree.put(userInput, null);
                }
            }
        }

        class RemoveNodeAction extends AbstractAction {
            public void actionPerformed(ActionEvent e) {
                tree.remove(selected_node);
            }
        }

        class AddModifyDataForNodeAction extends AbstractAction {
            public void actionPerformed(ActionEvent e) {
                HashMap data=getData(tree, selected_node);
                if(data != null) {
                }
                else {
                    clearTable();
                    data=new HashMap();
                    data.put("Add Key", "Add Value");

                }
                populateTable(data);
                getContentPane().add(tablePanel, BorderLayout.SOUTH);
                validate();

            }
        }


//        public static void main(String args[]) {
//            ReplicatedTree tree;
//
//            for(int i=0; i < args.length; i++) {
//                if(args[i].equals("-help")) {
//                    System.out.println("ReplicatedTreeView [-help]");
//                    return;
//                }
//            }
//
//            try {
//                tree=new ReplicatedTree(null);
//                tree.setRemoteCalls(false);
//                HashMap map=new HashMap();
//                map.put("name", "Framework");
//                map.put("pid", new Integer(322649));
//                tree.put("/federations/fed1/servers/Framework", map);
//                tree.put("/federations/fed1/servers/Security", null);
//
//                // demo.setVisible(true);
//                new ReplicatedTreeView(tree, "<null address>");
//
//                tree.put("/federations/fed1/servers/Security/components/RuntimeMonitor", null);
//                tree.put("/federations/fed1/servers/fenics", null);
//
//
//            }
//            catch(Exception ex) {
//                ex.printStackTrace(System.err);
//            }
//        }


        class MyNode extends DefaultMutableTreeNode {
            String name="<unnamed>";


            MyNode(String name) {
                this.name=name;
            }


            /**
             * Adds a new node to the view. Intermediary nodes will be created if they don't yet exist.
             * Returns the first node that was created or null if node already existed
             */
            public MyNode add(String fqn) {
                MyNode curr, n, ret=null;
                StringTokenizer tok;
                String child_name;

                if(fqn == null) return null;
                curr=this;
                tok=new StringTokenizer(fqn, ReplicatedTreeView.SEP);

                while(tok.hasMoreTokens()) {
                    child_name=tok.nextToken();
                    n=curr.findChild(child_name);
                    if(n == null) {
                        n=new MyNode(child_name);
                        if(ret == null) ret=n;
                        curr.add(n);
                    }
                    curr=n;
                }
                return ret;
            }


            /**
             * Removes a node from the view. Child nodes will be removed as well
             */
            public void remove(String fqn) {
                removeFromParent();
            }


            MyNode findNode(String fqn) {
                MyNode curr, n;
                StringTokenizer tok;
                String child_name;

                if(fqn == null) return null;
                curr=this;
                tok=new StringTokenizer(fqn, ReplicatedTreeView.SEP);

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
                MyNode child;

                if(relative_name == null || getChildCount() == 0)
                    return null;
                for(int i=0; i < getChildCount(); i++) {
                    child=(MyNode)getChildAt(i);
                    if(child.name == null) {
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
                    sb.append(' ');
                if(!isRoot()) {
                    if(name == null)
                        sb.append("/<unnamed>");
                    else {
                        sb.append(ReplicatedTreeView.SEP + name);
                    }
                }
                sb.append('\n');
                if(getChildCount() > 0) {
                    if(isRoot())
                        indent=0;
                    else
                        indent+=4;
                    for(int i=0; i < getChildCount(); i++)
                        sb.append(((MyNode)getChildAt(i)).print(indent));
                }
                return sb.toString();
            }


            public String toString() {
                return name;
            }

        }


    }


    public static void main(String args[]) {
        ReplicatedTree tree;
        String start_directory=null;
        boolean jmx=false;

        String props="UDP(mcast_addr=224.0.0.36;mcast_port=55566;ip_ttl=32;" +
                "mcast_send_buf_size=150000;mcast_recv_buf_size=80000):" +
                "PING(timeout=2000;num_initial_members=3):" +
                "MERGE2(min_interval=5000;max_interval=10000):" +
                "FD_SOCK:" +
                "VERIFY_SUSPECT(timeout=1500):" +
                "pbcast.NAKACK(gc_lag=50;retransmit_timeout=600,1200,2400,4800):" +
                "UNICAST(timeout=600,1200,2400,4800):" +
                "pbcast.STABLE(desired_avg_gossip=20000):" +
                "FRAG(frag_size=16000;down_thread=false;up_thread=false):" +
                "pbcast.GMS(join_timeout=5000;join_retry_timeout=2000;" +
                "shun=false;print_local_addr=true):" +
                "pbcast.STATE_TRANSFER";
        // "PERF(details=true)";


        for(int i=0; i < args.length; i++) {
            if("-props".equals(args[i])) {
                props=args[++i];
                continue;
            }
            if("-start_directory".equals(args[i])) {
                start_directory=args[++i];
                continue;
            }
            if("-jmx".equals(args[i])) {
                jmx=true;
                continue;
            }
            help();
            return;
        }

        try {
            tree=new ReplicatedTree("ReplicatedTreeDemo-Group", props, 10000, jmx);
            new ReplicatedTreeView(tree, tree.getLocalAddress());
            // demo.setVisible(true);

            if(start_directory != null && start_directory.length() > 0) {
                populateTree(tree, start_directory);
            }
            else {
                /*
                HashMap map=new HashMap();
                map.put("name", "Framework");
                map.put("pid", new Integer(322649));
                tree.put("/federations/fed1/servers/Framework", map);
                tree.put("/federations/fed1/servers/Security", null);
                tree.put("/federations/fed1/servers/Security/components/RuntimeMonitor", null);
                tree.put("/federations/fed1/servers/fenics", null);
                */
            }
        }
        catch(Exception ex) {
            ex.printStackTrace(System.err);
        }
    }


    static void help() {
        System.out.println("ReplicatedTreeView [-help] " +
                "[-props <channel properties>] [-start_directory <dirname>] [-jmx]");
    }

    static void populateTree(ReplicatedTree tree, String dir) {
        File file=new File(dir);

        if(!file.exists()) return;
        tree.put(dir, null);

        if(file.isDirectory()) {
            String[] children=file.list();
            if(children != null && children.length > 0) {
                for(int i=0; i < children.length; i++)
                    populateTree(tree, dir + '/' + children[i]);
            }
        }
    }


}








