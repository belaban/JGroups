

package org.jgroups.demos;


import org.jgroups.Address;
import org.jgroups.JChannel;
import org.jgroups.View;
import org.jgroups.blocks.ReplicatedHashMap;

import javax.swing.*;
import java.awt.*;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.awt.event.WindowEvent;
import java.awt.event.WindowListener;
import java.util.Map;


/**
 * Uses the ReplicatedHashMap building block, which subclasses java.util.HashMap and overrides
 * the methods that modify the hashmap (e.g. put()). Those methods are multicast to the group, whereas
 * read-only methods such as get() use the local copy. A ReplicatedtHashMap is created given the name
 * of a group; all hashmaps with the same name find each other and form a group.
 * @author Bela Ban
 */
public class ReplicatedHashMapDemo extends Frame implements WindowListener, ActionListener, ReplicatedHashMap.Notification<Object,Object> {
    ReplicatedHashMap<String,Float>  map=null;
    final JButton                    get=new JButton("Get");
    final JButton                    set=new JButton("Set");
    final JButton                    quit=new JButton("Quit");
    final JButton                    get_all=new JButton("All");
    final JButton                    delete=new JButton("Delete");
    final JLabel                     stock=new JLabel("Key");
    final JLabel                     value=new JLabel("Value");
    final JLabel                     err_msg=new JLabel("Error");
    final JTextField                 stock_field=new JTextField();
    final JTextField                 value_field=new JTextField();
    final List                       listbox=new List();
    final Font                       default_font=new Font("Helvetica", Font.PLAIN,12);




    public ReplicatedHashMapDemo() {
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
            map.remove(key);
    }

    private void showAll() {
        if(listbox.getItemCount() > 0)
            listbox.removeAll();
        if(map.isEmpty())
            return;
        clearMsg();
        String key;
        Float val;

        for(Map.Entry<String,Float> entry: map.entrySet()) {
            key=entry.getKey();
            val=entry.getValue();
            if(val == null)
                continue;
            listbox.add(key + ": " + val.toString());
        }
    }





    public void start(JChannel channel) throws Exception {
        map=new ReplicatedHashMap<>(channel);
        map.addNotifier(this);
        map.start(10000);

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
        _setTitle();
        showAll();
        setVisible(true);
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
            switch(command) {
                case "Get": {
                    String stock_name=stock_field.getText();
                    if(stock_name == null || stock_name.isEmpty()) {
                        showMsg("Key is empty !");
                        return;
                    }
                    showMsg("Looking up value for " + stock_name + ':');
                    Float val=map.get(stock_name);
                    if(val != null) {
                        value_field.setText(val.toString());
                        clearMsg();
                    }
                    else {
                        value_field.setText("");
                        showMsg("Value for " + stock_name + " not found");
                    }
                    break;
                }
                case "Set":
                    String stock_name=stock_field.getText();
                    String stock_val=value_field.getText();
                    if(stock_name == null || stock_val == null || stock_name.isEmpty() ||
                      stock_val.isEmpty()) {
                        showMsg("Both key and value have to be present to create a new entry");
                        return;
                    }
                    Float val=Float.parseFloat(stock_val);
                    map.put(stock_name, val);
                    showMsg("Key " + stock_name + " set to " + val);
                    break;
                case "All":
                    showAll();
                    break;
                case "Quit":
                    setVisible(false);
                    System.exit(0);
                case "Delete":
                    removeItem();
                    break;
                default:
                    System.out.println("Unknown action");
                    break;
            }
        }
        catch(Exception ex) {
            value_field.setText("");
            showMsg(ex.toString());
        }
    }

    public void entrySet(Object key, Object value) {
        showAll();
    }

    public void entryRemoved(Object key) {
        showAll();
    }

    public void contentsSet(Map<Object,Object> m) {
        System.out.println("new contents: " + m);
    }

    public void contentsCleared() {
        System.out.println("contents cleared");
    }


    public void viewChange(View view, java.util.List<Address> new_mbrs, java.util.List<Address> old_mbrs) {
        System.out.println("** view: " + view);
        _setTitle();
    }

    private void _setTitle() {
        int num=map.getChannel().getView().size();
        setTitle("ReplicatedHashMapDemo: " + num + " server(s)");
    }

    public static void main(String[] args) {
        ReplicatedHashMapDemo     client=new ReplicatedHashMapDemo();
        JChannel                  channel;
        String                    props="udp.xml";

        try {
            for(int i=0; i < args.length; i++) {
                String arg=args[i];
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
            channel=new JChannel(props);
            channel.connect("ReplicatedHashMapDemo-Cluster");
            client.start(channel);
        }
        catch(Throwable t) {
            t.printStackTrace();
        }
    }


    static void help() {
        System.out.println("ReplicatedHashMapDemo [-help] [-props <properties>]");
    }

}
