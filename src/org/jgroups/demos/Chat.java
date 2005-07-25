package org.jgroups.demos;


import org.jgroups.*;
import org.jgroups.util.Util;
import org.jgroups.blocks.PullPushAdapter;

import javax.swing.*;
import java.awt.*;
import java.awt.event.MouseEvent;
import java.awt.event.MouseListener;
import java.awt.event.WindowEvent;
import java.awt.event.WindowListener;
import java.util.LinkedList;
import java.util.Iterator;
import java.io.*;


/**
 * Simple chat demo
 * @author Bela Ban
 * @version $Id: Chat.java,v 1.11 2005/07/25 16:24:16 belaban Exp $
 */
public class Chat implements MouseListener, WindowListener, MessageListener, MembershipListener {
    Channel channel;
    PullPushAdapter ad;
    Thread mainThread;
    final String group_name="ChatGroup";
    String props=null;
    Frame mainFrame;
    TextArea ta;
    TextField tf;
    Label csLabel;
    JButton leaveButton;
    JButton sendButton;
    JButton clearButton;
    String username=null;
    LinkedList history=new LinkedList();


    public Chat(String props) {
        this.props=props;
        try {
            username=System.getProperty("user.name");
        }
        catch(Throwable t) {}
    }


    public static void main(String[] args) {
        String props=null;

        for(int i=0; i < args.length; i++) {
            if("-props".equals(args[i])) {
                props=args[++i];
                continue;
            }
            help();
            return;
        }

        Chat chat=new Chat(props);
        chat.start();
    }


    static void help() {
        System.out.println("Chat [-help] [-props <properties>]");
    }


    public void start() {
        mainFrame=new Frame();
        mainFrame.setLayout(null);
        mainFrame.setSize(600, 507);
        mainFrame.addWindowListener(this);

        ta=new TextArea();
        ta.setBounds(12, 36, 550, 348);
        ta.setEditable(false);
        mainFrame.add(ta);

        tf=new TextField();
        tf.setBounds(100, 392, 400, 30);
        mainFrame.add(tf);

        csLabel=new Label("Send:");
        csLabel.setBounds(12, 392, 85, 30);
        mainFrame.add(csLabel);

        leaveButton=new JButton("Leave");
        leaveButton.setBounds(12, 428, 150, 30);
        leaveButton.addMouseListener(this);
        mainFrame.add(leaveButton);

        sendButton=new JButton("Send");
        sendButton.setBounds(182, 428, 150, 30);
        sendButton.addMouseListener(this);
        mainFrame.add(sendButton);

        clearButton=new JButton("Clear");
        clearButton.setBounds(340, 428, 150, 30);
        clearButton.addMouseListener(this);
        mainFrame.add(clearButton);

        try {
            channel=new JChannel(props);
            channel.setOpt(Channel.AUTO_RECONNECT, Boolean.TRUE);
            channel.setOpt(Channel.AUTO_GETSTATE, Boolean.TRUE);
            channel.setOpt(Channel.GET_STATE_EVENTS, Boolean.TRUE);
            System.out.println("Connecting to " + group_name);
            channel.connect(group_name);
            ad=new PullPushAdapter(channel, this, this);
            channel.getState(null, 5000);
        }
        catch(Exception e) {
            ta.append(e.toString());
        }
        mainFrame.pack();
        mainFrame.setLocation(15, 25);
        mainFrame.setBounds(new Rectangle(580, 480));
        mainFrame.setVisible(true);
        mainFrame.show();
        if(history.size() > 0) {
            for(Iterator it=history.iterator(); it.hasNext();) {
                String s=(String)it.next();
                ta.append(s + "\n");
            }
        }
    }



    /* -------------------- Interface MessageListener ------------------- */

    public void receive(Message msg) {
        Object o;

        try {
            o=msg.getObject();
            ta.append(o + " [" + msg.getSrc() + "]\n");
            history.add(o);
        }
        catch(Exception e) {
            ta.append("Chat.receive(): " + e);
        }
    }

    public byte[] getState() {
        try {
            return Util.objectToByteBuffer(history);
        }
        catch(Exception e) {
            return null;
        }
    }

    public void setState(byte[] state) {
        try {
            history=(LinkedList)Util.objectFromByteBuffer(state);
        }
        catch(Exception e) {
            e.printStackTrace();
        }
    }

    /* ----------------- End of Interface MessageListener --------------- */





    /* ------------------- Interface MembershipListener ----------------- */

    public void viewAccepted(View new_view) {
        ta.append("Received view " + new_view + '\n');
    }


    public void suspect(Address suspected_mbr) {
    }


    public void block() {
    }

    /* --------------- End of Interface MembershipListener -------------- */



    private synchronized void handleLeave() {
        try {
            System.out.print("Stopping PullPushAdapter");
            ad.stop();
            System.out.println(" -- done");

            System.out.print("Disconnecting the channel");
            channel.disconnect();
            System.out.println(" -- done");

            System.out.print("Closing the channel");
            channel.close();
            System.out.println(" -- done");
            System.exit(0);
        }
        catch(Exception e) {
            e.printStackTrace();
            ta.append("Failed leaving the group: " + e.toString() + '\n');
        }
    }


    private void handleSend() {
        try {
            Message msg=new Message(null, null, username + ": " + tf.getText());
            channel.send(msg);
        }
        catch(Exception e) {
            ta.append("Failed sending message: " + e.toString() + '\n');
        }
    }


    public void mouseClicked(MouseEvent e) {
        Object obj=e.getSource();

        if(obj == leaveButton)
            handleLeave();
        else if(obj == sendButton)
                handleSend();
        else if(obj == clearButton)
            ta.setText("");
    }

    public void mouseEntered(MouseEvent e) {}
    public void mouseExited(MouseEvent e) {}
    public void mousePressed(MouseEvent e) {}
    public void mouseReleased(MouseEvent e) {}

    public void windowActivated(WindowEvent e) {}
    public void windowClosed(WindowEvent e) {}
    public void windowClosing(WindowEvent e) { System.exit(0); }
    public void windowDeactivated(WindowEvent e) {}
    public void windowDeiconified(WindowEvent e) {}
    public void windowIconified(WindowEvent e) {}
    public void windowOpened(WindowEvent e) {}

}
