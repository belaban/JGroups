// $Id: Chat.java,v 1.2 2003/09/24 23:20:47 belaban Exp $

package org.jgroups.demos;


import org.jgroups.*;
import org.jgroups.blocks.PullPushAdapter;
import org.jgroups.log.Trace;
import org.jgroups.util.Util;

import javax.swing.*;
import java.awt.*;
import java.awt.event.MouseEvent;
import java.awt.event.MouseListener;
import java.awt.event.WindowEvent;
import java.awt.event.WindowListener;


/**
 * Instances of the group can broadcast short messages to the group, and receive them. A special button
 * (leaveJoin()) leaves the group and the re-joins it.
 * Originally written by a student, modified by Bela Ban
 */
public class Chat implements MouseListener, WindowListener, MessageListener, MembershipListener {
    static Chat selfRef;
    Channel channel;
    PullPushAdapter ad;
    Thread mainThread;
    String group_name="ChatGroup";
    String props=null;
    Frame mainFrame;
    TextArea ta;
    TextField tf;
    Label csLabel;
    JButton ltjButton;
    JButton castButton;
    JButton sendButton;
    JButton rnvButton;


    public Chat(String props) {
        this.props=props;
    }


    public static void main(String[] args) {
        String props=null;

        for(int i=0; i < args.length; i++) {
            if(args[i].equals("-props")) {
                props=args[++i];
                continue;
            }
            help();
            return;
        }

        Trace.init();

        selfRef=new Chat(props);
        selfRef.go();
    }


    static void help() {
        System.out.println("Chat [-help] [-props <properties>]");
    }


    public void go() {
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

        csLabel=new Label("Cast/Send:");
        csLabel.setBounds(12, 392, 85, 30);
        mainFrame.add(csLabel);

        ltjButton=new JButton("LeaveThenJoin");
        ltjButton.setBounds(12, 428, 150, 30);
        ltjButton.addMouseListener(this);
        mainFrame.add(ltjButton);

        castButton=new JButton("Cast");
        castButton.setBounds(182, 428, 150, 30);
        castButton.addMouseListener(this);
        mainFrame.add(castButton);

        sendButton=new JButton("Send");
        sendButton.setBounds(355, 428, 86, 30);
        sendButton.addMouseListener(this);
        sendButton.setEnabled(false);
        mainFrame.add(sendButton);

        try {
            channel=new JChannel(props);
            channel.setOpt(Channel.AUTO_RECONNECT, Boolean.TRUE);
            System.out.println("Connecting to " + group_name);
            channel.connect(group_name);
            ad=new PullPushAdapter(channel, this, this);
        }
        catch(Exception e) {
            ta.append(e.toString());
        }
        mainFrame.pack();
        mainFrame.show();
    }



    /* -------------------- Interface MessageListener ------------------- */

    public void receive(Message msg) {
        Object o;

        try {
            o=Util.objectFromByteBuffer(msg.getBuffer());
            ta.append(o.toString() + " [" + msg.getSrc() + "]\n");
        }
        catch(Exception e) {
            ta.append("Chat.receive(): " + e);
        }
    }

    public byte[] getState() {
        return null;
    }

    public void setState(byte[] state) {
    }

    /* ----------------- End of Interface MessageListener --------------- */





    /* ------------------- Interface MembershipListener ----------------- */

    public void viewAccepted(View new_view) {
        ta.append("Received view " + new_view + "\n");
    }


    public void suspect(Address suspected_mbr) {

    }


    public void block() {

    }

    /* --------------- End of Interface MembershipListener -------------- */



    private synchronized void handleLTJ() {
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

            System.out.print("Reopening the channel");
            channel.open();
            System.out.println(" -- done");

            System.out.print("Connecting to " + group_name);
            channel.connect(group_name);
            System.out.println(" -- done");

            System.out.print("Starting PullPushAdapter");
            ad.start();
            System.out.println(" -- done");

            ta.append("successfully rejoined the group" + "\n");
        }
        catch(Exception e) {
            e.printStackTrace();
            ta.append("Failed rejoined the group: " + e.toString() + "\n");
        }
    }


    private void handleCast() {
        Message msg=new Message(null, null, tf.getText());

        try {
            channel.send(msg);
            // ta.append("Multicasted: " + tf.getText() + "\n");
        }
        catch(Exception e) {
            ta.append("Failed casting: " + e.toString() + "\n");
        }
    }

    private void handleSend() {
    }

    public void mouseClicked(MouseEvent e) {
        Object obj=e.getSource();

        if(obj == ltjButton) {
            handleLTJ();
        }
        else
            if(obj == castButton) {
                handleCast();
            }
            else
                if(obj == sendButton) {
                    handleSend();
                }
    }

    public void mouseEntered(MouseEvent e) {
    }

    public void mouseExited(MouseEvent e) {
    }

    public void mousePressed(MouseEvent e) {
    }

    public void mouseReleased(MouseEvent e) {
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

}
