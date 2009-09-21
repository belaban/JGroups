// $Id: QuoteClient.java,v 1.12 2009/09/21 09:57:32 belaban Exp $

package org.jgroups.demos;


import org.jgroups.*;
import org.jgroups.blocks.GroupRequest;
import org.jgroups.blocks.RpcDispatcher;
import org.jgroups.util.Rsp;
import org.jgroups.util.RspList;

import java.awt.*;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.awt.event.WindowEvent;
import java.awt.event.WindowListener;
import java.util.Enumeration;
import java.util.Hashtable;




/**
 * Used in conjunction with QuoteServer: a client is member of a group of quote servers which replicate
 * stock quotes among themselves. The client broadcasts its request (set, get quotes) and (in the case of get
 * waits for the first reply received (usually the one from the quote server closest to it). The client
 * can get and set quotes as long as a minimum of 1 server (in the group) is running.
 * @author Bela Ban
 */
public class QuoteClient extends Frame implements WindowListener, ActionListener,
        MembershipListener {
    static final String channel_name="Quotes";
    RpcDispatcher disp;
    Channel channel;
    final Button get=new Button("Get");
    final Button set=new Button("Set");
    final Button quit=new Button("Quit");
    final Button get_all=new Button("All");
    final Label stock=new Label("Stock");
    final Label value=new Label("Value");
    final Label err_msg=new Label("Error");
    final TextField stock_field=new TextField();
    final TextField value_field=new TextField();
    final java.awt.List listbox=new java.awt.List();
    final Font default_font=new Font("Helvetica", Font.PLAIN, 12);

    final String props=null; // default stack from JChannel


    public QuoteClient() {
        super();
        try {
            channel=new JChannel(props);
            channel.setOpt(Channel.LOCAL, Boolean.FALSE);
            disp=new RpcDispatcher(channel, null, this, this);
            channel.connect(channel_name);
        }
        catch(Exception e) {
            System.err.println("QuoteClient(): " + e);
        }
        addWindowListener(this);
    }

    private void showMsg(String msg) {
        err_msg.setText(msg);
        err_msg.setVisible(true);
    }

    private void clearMsg() {
        err_msg.setVisible(false);
    }


    public void start() {
        setLayout(null);
        setSize(400, 300);
        setFont(default_font);

        stock.setBounds(new Rectangle(10, 30, 60, 30));
        value.setBounds(new Rectangle(10, 60, 60, 30));
        stock_field.setBounds(new Rectangle(100, 30, 100, 30));
        value_field.setBounds(new Rectangle(100, 60, 100, 30));
        listbox.setBounds(210, 30, 150, 160);
        err_msg.setBounds(new Rectangle(10, 200, 350, 30));
        err_msg.setFont(new Font("Helvetica", Font.ITALIC, 12));
        err_msg.setForeground(Color.red);
        err_msg.setVisible(false);
        get.setBounds(new Rectangle(10, 250, 80, 30));
        set.setBounds(new Rectangle(100, 250, 80, 30));
        quit.setBounds(new Rectangle(190, 250, 80, 30));
        get_all.setBounds(new Rectangle(280, 250, 80, 30));

        get.addActionListener(this);
        set.addActionListener(this);
        quit.addActionListener(this);
        get_all.addActionListener(this);

        add(stock);
        add(value);
        add(stock_field);
        add(value_field);
        add(err_msg);
        add(get);
        add(set);
        add(quit);
        add(get_all);
        add(listbox);
        // stock_field.requestFocus();
        setVisible(true);
    }


    public void windowActivated(WindowEvent e) {
    }

    public void windowClosed(WindowEvent e) {
    }

    public void windowClosing(WindowEvent e) {
        System.exit(0);
    }

    public void windowDeactivated(WindowEvent e) {
    }

    public void windowDeiconified(WindowEvent e) {
    }

    public void windowIconified(WindowEvent e) {
    }

    public void windowOpened(WindowEvent e) {
    }


    public void actionPerformed(ActionEvent e) {
        String command=e.getActionCommand();
        RspList rsp_list;
        Rsp     first_rsp;

        try {
            if(command.equals("Get")) {
                String stock_name=stock_field.getText();
                if(stock_name == null || stock_name.length() == 0) {
                    showMsg("Stock name is empty !");
                    return;
                }
                showMsg("Looking up value for " + stock_name + ':');
                rsp_list=disp.callRemoteMethods(null, "getQuote", new Object[]{stock_name},
                                                new String[]{String.class.getName()},
                                                GroupRequest.GET_ALL, 10000);

                Float val=null;
                for(int i=0; i < rsp_list.size(); i++) {
                    Rsp rsp=(Rsp)rsp_list.elementAt(i);
                    Object obj=rsp.getValue();
                    if(obj == null || obj instanceof Throwable)
                        continue;
                    val=(Float)obj;
                    break;
                }

                if(val != null) {
                    value_field.setText(val.toString());
                    clearMsg();
                }
                else {
                    value_field.setText("");
                    showMsg("Value for " + stock_name + " not found");
                }
            }
            else
                if(command.equals("Set")) {
                    String stock_name=stock_field.getText();
                    String stock_val=value_field.getText();
                    if(stock_name == null || stock_val == null || stock_name.length() == 0 ||
                            stock_val.length() == 0) {
                        showMsg("Stock name and value have to be present to enter a new value");
                        return;
                    }
                    Float val=new Float(stock_val);
                    disp.callRemoteMethods(null, "setQuote", new Object[]{stock_name, val},
                                           new Class[]{String.class, Float.class},
                                           GroupRequest.GET_FIRST, 0);

                    showMsg("Stock " + stock_name + " set to " + val);
                }
                else
                    if(command.equals("All")) {
                        listbox.removeAll();
                        showMsg("Getting all stocks:");
                        rsp_list=disp.callRemoteMethods(null, "getAllStocks",
                                                        null, (Class[])null,
                                                        GroupRequest.GET_ALL, 5000);

                        System.out.println("rsp_list is " + rsp_list);

                        Hashtable all_stocks=null;
                        for(int i=0; i < rsp_list.size(); i++) {
                            Rsp rsp=(Rsp)rsp_list.elementAt(i);
                            Object obj=rsp.getValue();
                            if(obj == null || obj instanceof Throwable)
                                continue;
                            all_stocks=(Hashtable)obj;
                            break;
                        }

                        if(all_stocks == null) {
                            showMsg("No stocks found");
                            return;
                        }
                        clearMsg();
                        listbox.removeAll();
                        String key;
                        Float val;
                        for(Enumeration en=all_stocks.keys(); en.hasMoreElements();) {
                            key=(String)en.nextElement();
                            val=(Float)all_stocks.get(key);
                            if(val == null)
                                continue;
                            listbox.add(key + ": " + val.toString());
                        }
                    }
                    else
                        if(command.equals("Quit")) {
                            setVisible(false);
                            channel.close();
                            System.exit(0);
                        }
                        else
                            System.out.println("Unknown action");
        }
        catch(Exception ex) {
            value_field.setText("");
            ex.printStackTrace();
            showMsg(ex.toString());
        }
    }



    public void setQuote(String stock_name, Float value) {
        ;
    }

    public void printAllStocks() {
    }


    public void viewAccepted(View new_view) {
        setTitle("Members in " + channel_name + ": " + (new_view.size() - 1));
    }

    public void suspect(Address suspected_mbr) {
    }

    public void block() {
    }


    public static void main(String args[]) {
        QuoteClient client=new QuoteClient();
        client.start();
    }

}
