// $Id: DistributedQueueDemo.java,v 1.4 2004/03/30 06:47:16 belaban Exp $
package org.jgroups.demos;

import org.jgroups.ChannelException;
import org.jgroups.ChannelFactory;
import org.jgroups.JChannelFactory;
import org.jgroups.blocks.DistributedQueue;

import javax.swing.*;
import java.awt.*;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.awt.event.WindowEvent;
import java.awt.event.WindowListener;
import java.util.Collection;
import java.util.Vector;


/**
 * Uses the DistributedQueue building block. The latter subclasses org.jgroups.util.Queue and overrides
 * the methods that modify the queue (e.g. add()). Those methods are multicast to the group, whereas
 * read-only methods such as peek() use the local copy. A DistributedQueue is created given the name
 * of a group; all queues with the same name find each other and form a group.
 * @author Romuald du Song
 */
public class DistributedQueueDemo extends Frame implements WindowListener, ActionListener,
                                                           DistributedQueue.Notification
{
    DistributedQueue h = null;
    JButton add = new JButton("Add");
    JButton quit = new JButton("Quit");
    JButton get_all = new JButton("All");
    JButton remove = new JButton("Remove");
    JLabel value = new JLabel("Value");
    JLabel err_msg = new JLabel("Error");
    JTextField value_field = new JTextField();
    java.awt.List listbox = new java.awt.List();
    final Font default_font = new Font("Helvetica", Font.PLAIN, 12);

    public DistributedQueueDemo()
    {
        super();
        addWindowListener(this);
    }

    private void showMsg(String msg)
    {
        err_msg.setText(msg);
        err_msg.setVisible(true);
    }

    private void clearMsg()
    {
        err_msg.setVisible(false);
    }

    private void removeItem()
    {
        h.remove();
    }

    private void showAll()
    {
        if (listbox.getItemCount() > 0)
        {
            listbox.removeAll();
        }

        if (h.size() == 0)
        {
            return;
        }

        clearMsg();

        String key;

        Vector v = h.getContents();

        for (int i = 0; i < v.size(); i++)
        {
            listbox.add((String)v.elementAt(i));
        }
    }

    public void start(String groupname, ChannelFactory factory, String props)
               throws ChannelException
    {
        h = new DistributedQueue(groupname, factory, props, 10000);
        h.addNotifier(this);

        setLayout(null);
        setSize(400, 300);
        setFont(default_font);

        value.setBounds(new Rectangle(10, 60, 60, 30));
        value_field.setBounds(new Rectangle(100, 60, 100, 30));
        listbox.setBounds(new Rectangle(210, 30, 150, 160));
        err_msg.setBounds(new Rectangle(10, 200, 350, 30));
        err_msg.setFont(new Font("Helvetica", Font.ITALIC, 12));
        err_msg.setForeground(Color.red);
        err_msg.setVisible(false);
        add.setBounds(new Rectangle(60, 250, 60, 30));
        quit.setBounds(new Rectangle(130, 250, 70, 30));
        get_all.setBounds(new Rectangle(210, 250, 60, 30));
        remove.setBounds(new Rectangle(280, 250, 90, 30));

        add.addActionListener(this);
        quit.addActionListener(this);
        get_all.addActionListener(this);
        remove.addActionListener(this);

        add(value);
        add(value_field);
        add(err_msg);
        add(add);
        add(quit);
        add(get_all);
        add(remove);
        add(listbox);
        setTitle("DistributedQueue Demo");
        showAll();
        pack();
        setVisible(true);

        /*
                 new Thread() {
                     public void run() {
                         System.out.println("-- sleeping");
                         Util.sleep(10000);
                         for(int i=0; i < 10; i++) {
                             System.out.println("-- add()");
                             h.add("Bela#" + i);
                         }

                         while(true) {
                             Util.sleep(500);
                             try
                            {
                                System.out.println(h.remove());
                            }
                            catch (QueueClosedException e)
                            {
                                e.printStackTrace();
                            }
                         }

                     }
                 }.start();
        */
    }

    public void windowActivated(WindowEvent e)
    {
    }

    public void windowClosed(WindowEvent e)
    {
    }

    public void windowClosing(WindowEvent e)
    {
        System.exit(0);
    }

    public void windowDeactivated(WindowEvent e)
    {
    }

    public void windowDeiconified(WindowEvent e)
    {
    }

    public void windowIconified(WindowEvent e)
    {
    }

    public void windowOpened(WindowEvent e)
    {
    }

    public void actionPerformed(ActionEvent e)
    {
        String command = e.getActionCommand();

        try
        {
            if (command == "Add")
            {
                String value_name = value_field.getText();

                if ((value_name == null) || (value_name.length() == 0))
                {
                    showMsg("Value is empty !");

                    return;
                }

                showMsg("Adding value " + value_name + ":");
                h.add(value_name);
            }
            else if (command == "All")
            {
                showAll();
            }
            else if (command == "Quit")
            {
                setVisible(false);
                System.exit(0);
            }
            else if (command == "Remove")
            {
                removeItem();
            }
            else
            {
                System.out.println("Unknown action");
            }
        }
        catch (Exception ex)
        {
            value_field.setText("");
            showMsg(ex.toString());
        }
    }

    public void entryAdd(Object value)
    {
        showAll();
    }

    public void entryRemoved(Object key)
    {
        showAll();
    }

    public void viewChange(Vector joined, Vector left)
    {
        System.out.println("New members: " + joined + ", left members: " + left);
    }

    public void contentsSet(Collection new_entries)
    {
        System.out.println("Contents Set:" + new_entries);
    }

    public void contentsCleared()
    {
        System.out.println("Contents cleared()");
    }

    public static void main(String[] args)
    {
        String groupname = "QueueDemo";
        DistributedQueueDemo client = new DistributedQueueDemo();
        ChannelFactory factory = new JChannelFactory();
        String arg;
        String next_arg;
        boolean trace = false;
        boolean persist = false;

        String props =
            "UDP(mcast_addr=228.8.8.8;mcast_port=45566;ip_ttl=32;" +
            "mcast_send_buf_size=150000;mcast_recv_buf_size=80000):" + "PING(timeout=2000;num_initial_members=3):" +
            "MERGE2(min_interval=5000;max_interval=10000):" + "FD_SOCK:" + "VERIFY_SUSPECT(timeout=1500):" +
            "UNICAST(timeout=5000):" + "FRAG(frag_size=8192;down_thread=false;up_thread=false):" +
            "TOTAL_TOKEN(block_sending=50;unblock_sending=10):" +
            "pbcast.GMS(join_timeout=5000;join_retry_timeout=2000;" + "shun=false;print_local_addr=true):" +
            "STATE_TRANSFER:" + "QUEUE";

        try
        {
            for (int i = 0; i < args.length; i++)
            {
                arg = args[i];

                if (arg.equals("-trace"))
                {
                    trace = true;
                    continue;
                }

                if (args[i].equals("-groupname"))
                {
                    groupname = args[++i];
                    continue;
                }

                help();
                return;
            }
        }
        catch (Exception e)
        {
            help();

            return;
        }

        try
        {
            client.start(groupname, factory, props);
        }
        catch (Throwable t)
        {
            t.printStackTrace();
        }
    }

    static void help()
    {
        System.out.println("DistributedQueueDemo [-help]");
    }
}
