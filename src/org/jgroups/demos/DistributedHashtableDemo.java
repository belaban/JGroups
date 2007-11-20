// $Id: DistributedHashtableDemo.java,v 1.8.2.1 2007/11/20 08:53:42 belaban Exp $


package org.jgroups.demos;


import org.jgroups.ChannelException;
import org.jgroups.ChannelFactory;
import org.jgroups.JChannelFactory;
import org.jgroups.blocks.DistributedHashtable;
import org.jgroups.persistence.PersistenceFactory;

import javax.swing.*;
import java.awt.*;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.awt.event.WindowEvent;
import java.awt.event.WindowListener;
import java.util.Enumeration;
import java.util.Map;
import java.util.Vector;




/**
 * Uses the DistributedHashtable building block. The latter subclasses java.util.Hashtable and overrides
 * the methods that modify the hashtable (e.g. put()). Those methods are multicast to the group, whereas
 * read-only methods such as get() use the local copy. A DistributedHashtable is created given the name
 * of a group; all hashtables with the same name find each other and form a group.
 * @author Bela Ban
 */
public class DistributedHashtableDemo extends Frame implements WindowListener, ActionListener,
                                          DistributedHashtable.Notification {
    static final String          groupname="HashDemo";
    DistributedHashtable  h=null;
    final JButton               get=new JButton("Get");
    final JButton set=new JButton("Set");
    final JButton               quit=new JButton("Quit");
    final JButton get_all=new JButton("All");
    final JButton               delete=new JButton("Delete");
    final JLabel                stock=new JLabel("Key");
    final JLabel value=new JLabel("Value");
    final JLabel                err_msg=new JLabel("Error");
    final JTextField            stock_field=new JTextField();
    final JTextField value_field=new JTextField();
    final java.awt.List         listbox=new java.awt.List();
    final Font            default_font=new Font("Helvetica", Font.PLAIN,12);




    public DistributedHashtableDemo() {
        super();
        addWindowListener(this);
    }

    private void showMsg(String msg) {
        err_msg.setText(msg);
        err_msg.setVisible(true);
    }

    private void clearMsg() {err_msg.setVisible(false);}


    private void removeItem() {
        int index=listbox.getSelectedIndex();
        if(index == -1) {
            showMsg("No item selected in listbox to be deleted !");
            return;
        }
        String s=listbox.getSelectedItem();
        String key=s.substring(0, s.indexOf(':', 0));
        if(key != null)
            h.remove(key);
    }

    private void showAll() {
        if(listbox.getItemCount() > 0)
            listbox.removeAll();
        if(h.size() == 0)
            return;
        clearMsg();
        String key;
        Float val;

        for(Enumeration en=h.keys(); en.hasMoreElements();) {
            key=(String)en.nextElement();
            val=(Float)h.get(key);
            if(val == null)
                continue;
            listbox.add(key + ": " + val.toString());
        }
    }





    public void start(ChannelFactory factory, String props, boolean persist) 
            throws ChannelException {
        h=new DistributedHashtable(groupname, factory, props, persist, 10000);
        h.addNotifier(this);

        setLayout(null);
        setSize(400, 300);
        setFont(default_font);

        stock.setBounds(new Rectangle(10, 30, 60, 30));
        value.setBounds(new Rectangle(10, 60, 60, 30));
        stock_field.setBounds(new Rectangle(100, 30, 100, 30));
        value_field.setBounds(new Rectangle(100, 60, 100, 30));
        listbox.setBounds(new Rectangle(210, 30, 150, 160));
        err_msg.setBounds(new Rectangle(10, 200, 350, 30));
        err_msg.setFont(new Font("Helvetica",Font.ITALIC,12));
        err_msg.setForeground(Color.red);
        err_msg.setVisible(false);
        get.setBounds(new Rectangle(10, 250, 60, 30));
        set.setBounds(new Rectangle(80, 250, 60, 30));
        quit.setBounds(new Rectangle(150, 250, 60, 30));
        get_all.setBounds(new Rectangle(220, 250, 60, 30));
        delete.setBounds(new Rectangle(290, 250, 80, 30));

        get.addActionListener(this);
        set.addActionListener(this);
        quit.addActionListener(this);
        get_all.addActionListener(this);
        delete.addActionListener(this);

        add(stock); add(value);
        add(stock_field); add(value_field);
        add(err_msg);
        add(get); add(set); add(quit); add(get_all); add(delete);
        add(listbox);
        setTitle("DistributedHashtable Demo");
        showAll();
        // pack();
        setVisible(true);


//          new Thread() {
//              public void run() {
//                  System.out.println("-- sleeping");
//                  Util.sleep(10000);
//                  for(int i=0; i < 10; i++) {
//                      System.out.println("-- put()");
//                      h.put("Bela#" + i, new Float(i));
//                  }

//                  while(true) {
//                      Util.sleep(500);
//                      System.out.println(h.get("Bela#1"));
//                  }

//              }
//          }.start();



    }




    public void windowActivated(WindowEvent e)   {}
    public void windowClosed(WindowEvent e)      {}
    public void windowClosing(WindowEvent e)     {System.exit(0);}
    public void windowDeactivated(WindowEvent e) {}
    public void windowDeiconified(WindowEvent e) {}
    public void windowIconified(WindowEvent e)   {}
    public void windowOpened(WindowEvent e)      {}


    public void actionPerformed(ActionEvent e) {
        String command=e.getActionCommand();
        try {
            if(command == "Get") {
                String stock_name=stock_field.getText();
                if(stock_name == null || stock_name.length() == 0) {
                    showMsg("Key is empty !");
                    return;
                }
                showMsg("Looking up value for " + stock_name + ':');
                Float val=(Float)h.get(stock_name);
                if(val != null) {
                    value_field.setText(val.toString());
                    clearMsg();
                }
                else {
                    value_field.setText("");
                    showMsg("Value for " + stock_name + " not found");
                }
            }
            else if(command == "Set") {
                String stock_name=stock_field.getText();
                String stock_val=value_field.getText();
                if(stock_name == null || stock_val == null || stock_name.length() == 0 ||
                   stock_val.length() == 0) {
                    showMsg("Both key and value have to be present to create a new entry");
                    return;
                }
                Float val=new Float(stock_val);
                h.put(stock_name, val);
                showMsg("Key " + stock_name + " set to " + val);
            }
            else if(command == "All") {
                showAll();
            }
            else if(command == "Quit") {
                setVisible(false);
                System.exit(0);
            }
            else if(command == "Delete")
                removeItem();
            else
                System.out.println("Unknown action");
        }
        catch(Exception ex) {
            value_field.setText("");
            showMsg(ex.toString());
        }
    }



    public void entrySet(Object key, Object value) {showAll();}

    public void entryRemoved(Object key)           {showAll();}

    public void viewChange(Vector joined, Vector left) {
        System.out.println("New members: " + joined + ", left members: " + left);
    }

    public void contentsSet(Map m) {
        System.out.println("new contents: " + m);
    }

    public void contentsCleared() {
        System.out.println("contents cleared");
    }


    public static void main(String args[]) {
        DistributedHashtableDemo  client=new DistributedHashtableDemo();
        ChannelFactory            factory=new JChannelFactory();
        String                    arg;
        boolean                   persist=false;

        // test for pbcast
        /*
        String props="UDP:" +
            "PING(num_initial_members=2;timeout=3000):" +
            "FD:" +
            // "DISCARD(down=0.1):" +  // this is for discarding of 10% of the up messages !
            "pbcast.PBCAST(gossip_interval=5000;gc_lag=50):" +
            "UNICAST:" +
            "FRAG:" +
            "pbcast.GMS:" +
            "pbcast.STATE_TRANSFER";
        */

        String props="udp.xml";

        try {
            for(int i=0; i < args.length; i++) {
                arg=args[i];
                if("-persist".equals(arg) && i+1<args.length) {
                    persist=true;
                    PersistenceFactory.getInstance().createManager(args[++i]);
                    continue;
                }
                if("-props".equals(arg)) {
                    props=args[++i];
                    continue;
                }
                help();
                return;
            }
        }
        catch(Exception e) {
            help();
            return;
        }
        try {
            client.start(factory, props, persist);
        }
        catch(Throwable t) {
            t.printStackTrace();
        }
    }


    static void help() {
        System.out.println("DistributedHashtableDemo [-help] [-persist] [-props <properties>]");
    }

}
