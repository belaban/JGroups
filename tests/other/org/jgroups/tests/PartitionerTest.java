package org.jgroups.tests;


import org.jgroups.Address;
import org.jgroups.JChannel;
import org.jgroups.Message;
import org.jgroups.View;
import org.jgroups.debug.Debugger;

import javax.swing.*;
import javax.swing.border.BevelBorder;
import javax.swing.border.Border;
import javax.swing.border.EtchedBorder;
import javax.swing.border.TitledBorder;
import java.awt.*;
import java.awt.event.ActionEvent;
import java.awt.event.KeyEvent;
import java.awt.event.WindowEvent;
import java.util.Hashtable;
import java.util.Map;


/**
 * Title:        Demos of PARTITIONER protocol. Simulates network partitions.
 * Description:  The usage of PartitionerTestFrame is quite simple:
 * As the program finds new members in the view it adds them to the Hashtable and
 * to Processes list.
 * You can chose a Process from the processes list and digit a number over the set button.
 * Once you push the Set button the number is stored in the local Hastable.
 * Once you push the Execute button the local Hashtable is sent with a SET_PARTITIONS Event.
 * <p/>
 * The TextArea under the output area is to send messages over the channel and you view
 * the results on the output area.
 * <p/>
 * In the future it may be a good thing let the PARTITIONER layer spawn a control Frame
 * so that the use of this layer is transparent to the application, but it is also possible
 * to use a partitionerTestFrame (a little modified) in a Group of other Processes just
 * to control their behaviour against partitions.
 * Copyright:    Copyright (c) 2000
 * Company:      Computer Network Laboratory
 * 
 * @author Gianluca Collot
 * @version 1.1
 */


public class PartitionerTest extends JFrame implements Runnable {
    static String channel_properties="UDP:PARTITIONER:SHUFFLE:MERGE" +
            ":PING:FD(shun=false):" +
            "STABLE:" +
            "NAKACK:FRAG:FLUSH:GMS:" +
            "VIEW_ENFORCER:" +
            "QUEUE";
    JChannel channel;
    Debugger debugger;

    boolean connected=false;
    Hashtable ht=new Hashtable();

    JPanel jPanel1=new JPanel();
    BorderLayout borderLayout1=new BorderLayout();
    JPanel statusbar=new JPanel();
    JLabel status=new JLabel();
    BorderLayout borderLayout2=new BorderLayout();
    JPanel jPanel3=new JPanel();
    JScrollPane jScrollPane2=new JScrollPane();
    JList partitionsList=new JList();
    BorderLayout borderLayout3=new BorderLayout();
    JLabel partitionLabel=new JLabel();
    JTextField partitionField=new JTextField();
    JButton execute=new JButton();
    JButton set=new JButton();
    JPanel jPanel2=new JPanel();
    GridLayout gridLayout1=new GridLayout();
    TitledBorder titledBorder1;
    Border border1;
    Border border2;
    TitledBorder titledBorder2;
    Border border3;
    TitledBorder titledBorder3;
    BorderLayout borderLayout5=new BorderLayout();
    JButton connect=new JButton();
    JPanel applicationPanel=new JPanel();
    JTextArea output=new JTextArea();
    JScrollPane jScrollPane1=new JScrollPane();
    JTextField commandTextField=new JTextField();
    BorderLayout borderLayout4=new BorderLayout();


    public PartitionerTest(JChannel channel) {
        this.channel=channel;
        init();
    }

    public PartitionerTest() {
        try {
            channel=new JChannel(channel_properties);
        }
        catch(Exception ex) {
            ex.printStackTrace();
            System.exit(4);
        }
        init();
    }

    /**
     * Initializes the channel and application threads
     */

    void init() {
        try {
            channel.connect("prova");
            connected=true;
//      if (channel.getView().getMembers().elementAt(0).equals(channel.getLocalAddress())) {
//	Debugger debugger = new Debugger(channel);
//	debugger.start();
//      }
        }
        catch(Exception ex) {
            System.exit(4);
        }
        try {
            jbInit();
        }
        catch(Exception e) {
            e.printStackTrace();
        }
        Thread receiver=new Thread(this, "Receiver");
        receiver.start();
    }

    /**
     * jbInit()
     * This function provides initialization for the gui objects
     */

    private void jbInit() throws Exception {
        titledBorder1=new TitledBorder("");
        border1=BorderFactory.createCompoundBorder(BorderFactory.createEtchedBorder(Color.white, new Color(148, 145, 140)), BorderFactory.createEmptyBorder(4, 4, 4, 4));
        border2=BorderFactory.createBevelBorder(BevelBorder.LOWERED, Color.white, Color.white, new Color(148, 145, 140), new Color(103, 101, 98));
        titledBorder2=new TitledBorder(new EtchedBorder(EtchedBorder.RAISED, Color.white, new Color(148, 145, 140)), "Processes");
        border3=BorderFactory.createLineBorder(Color.black, 2);
        titledBorder3=new TitledBorder(new EtchedBorder(EtchedBorder.RAISED, Color.white, new Color(148, 145, 140)), "Channel Output");
        this.setSize(400, 400);
        this.setTitle("Partitioner Test :" + channel.getLocalAddress());
        this.addWindowListener(new java.awt.event.WindowAdapter() {
            public void windowClosing(WindowEvent e) {
                this_windowClosing(e);
            }
        });
        this.getContentPane().setLayout(borderLayout1);
        jPanel1.setLayout(borderLayout3);
        statusbar.setLayout(borderLayout2);
        status.setMinimumSize(new Dimension(100, 20));
        status.setText("ciao");
        statusbar.setBorder(BorderFactory.createBevelBorder(BevelBorder.LOWERED, Color.white, Color.white, new Color(148, 145, 140), new Color(103, 101, 98)));
        jPanel3.setLayout(borderLayout5);
        partitionLabel.setHorizontalAlignment(SwingConstants.CENTER);
        partitionLabel.setText("Partition");
        execute.setText("Execute");
        execute.addActionListener(new java.awt.event.ActionListener() {
            public void actionPerformed(ActionEvent e) {
                execute_actionPerformed(e);
            }
        });
        set.addActionListener(new java.awt.event.ActionListener() {
            public void actionPerformed(ActionEvent e) {
                set_actionPerformed(e);
            }
        });
        set.setText("Set");
        set.setActionCommand("Set");
        jPanel2.setLayout(gridLayout1);
        partitionField.setMinimumSize(new Dimension(20, 20));
        gridLayout1.setRows(8);
        gridLayout1.setColumns(1);
        gridLayout1.setVgap(4);
        jPanel3.setBorder(border1);
        jPanel3.setMinimumSize(new Dimension(50, 50));
        jPanel3.setPreferredSize(new Dimension(200, 200));
        jScrollPane2.setBorder(titledBorder2);
        jScrollPane2.setMinimumSize(new Dimension(100, 80));
        jPanel2.setMinimumSize(new Dimension(77, 100));
        partitionsList.setBorder(BorderFactory.createLoweredBevelBorder());
        partitionsList.setMinimumSize(new Dimension(100, 80));
        connect.setActionCommand("Connect");
        connect.setText("Disconnect");
        connect.addActionListener(new java.awt.event.ActionListener() {
            public void actionPerformed(ActionEvent e) {
                connect_actionPerformed(e);
            }
        });
        jScrollPane1.setAutoscrolls(true);
        jScrollPane1.setBorder(titledBorder3);
        jScrollPane1.setMinimumSize(new Dimension(100, 100));
        jScrollPane1.setPreferredSize(new Dimension(300, 100));
        applicationPanel.setLayout(borderLayout4);
        commandTextField.addKeyListener(new java.awt.event.KeyAdapter() {
            public void keyTyped(KeyEvent e) {
                commandTextField_keyTyped(e);
            }
        });
        commandTextField.setMaximumSize(new Dimension(300, 21));
        this.getContentPane().add(statusbar, BorderLayout.SOUTH);
        statusbar.add(status, BorderLayout.CENTER);
        this.getContentPane().add(jPanel1, BorderLayout.NORTH);
        jPanel1.add(jPanel3, BorderLayout.CENTER);
        jPanel3.add(jScrollPane2, BorderLayout.NORTH);
        jScrollPane2.getViewport().add(partitionsList, null);
        jPanel1.add(jPanel2, BorderLayout.WEST);
        jPanel2.add(partitionLabel, null);
        jPanel2.add(partitionField, null);
        jPanel2.add(set, null);
        jPanel2.add(execute, null);
        jPanel2.add(connect, null);
        this.getContentPane().add(applicationPanel, BorderLayout.CENTER);
        applicationPanel.add(jScrollPane1, BorderLayout.CENTER);
        applicationPanel.add(commandTextField, BorderLayout.SOUTH);
        jScrollPane1.getViewport().add(output, null);
    }

    public void run() {
        boolean running=true;
        Object received=null;
        View view;
        Message msg;
        String payload;
        while(running) {
            try {
                received=channel.receive(0);
            }
            catch(Exception ex) {
                System.out.println("PartitionerTest.run() :" + ex);
                System.exit(-1);
            }
            if(received instanceof View) {
                view=(View)received;
                status.setText(view.toString());
                for(int i=0; i < view.size(); i++) {
                    Address member=(Address)view.getMembers().elementAt(i);
                    if(ht.get(member) == null) {
                        ht.put(member, new Integer(1));
                    }
                }
                partitionsList.setListData(ht.entrySet().toArray());
                output.append(view + "\n");
                output.setCaretPosition(output.getText().length());
            }

            if(received instanceof Message) {
                msg=(Message)received;
                Object tmp=null;
                tmp=msg.getObject();
                if(tmp instanceof String) {
                    payload=(String)tmp;
                    if(payload.equals("stop")) {
                        running=false;
                    }
                    output.append(":" + payload + "\n");
                }
                else
                    output.append("Received not a String\n");
            }
            output.setCaretPosition(output.getText().length());
        }
        channel.close();
        output.append("Channel Closed\n");
    } //run()

    /**
     * Sets the partition for the selected component on the hashtable
     */

    void set_actionPerformed(ActionEvent e) {
        if(partitionsList.getSelectedValue() != null) {
            Map.Entry entry=(Map.Entry)partitionsList.getSelectedValue();
            entry.setValue(new Integer(partitionField.getText()));
            partitionsList.setListData(ht.entrySet().toArray());
        }
    }

    /**
     * Sends a message over the channel
     */

    void sendButton_actionPerformed(ActionEvent e) {
        try {
            channel.send(new Message(null, null, commandTextField.getText()));
            commandTextField.setText("");
        }
        catch(Exception ex) {
            ex.printStackTrace();
        }

    }

    void this_windowClosing(WindowEvent e) {
        channel.close();
        System.out.println("Exiting");
        System.exit(0);
    }

    void execute_actionPerformed(ActionEvent e) {
        channel.down(new org.jgroups.Event(org.jgroups.Event.SET_PARTITIONS, ht));
    }

    static void main(String[] args) {

        PartitionerTest frame=new PartitionerTest();
        frame.validate();
        System.out.println("Frame validated");
        // Center the frame
        Dimension screenSize=Toolkit.getDefaultToolkit().getScreenSize();
        Dimension frameSize=frame.getSize();
        if(frameSize.height > screenSize.height)
            frameSize.height=screenSize.height;
        if(frameSize.width > screenSize.width)
            frameSize.width=screenSize.width;
        frame.setLocation((screenSize.width - frameSize.width) / 2, (screenSize.height - frameSize.height) / 2);
        frame.setVisible(true);
        System.out.println("Frame Visible");
    }

    void connect_actionPerformed(ActionEvent e) {
        try {
            if(!connected) {
                channel.connect("prova");
                connected=true;
                connect.setText("Disconnect");
            }
            else {
                channel.disconnect();
                connected=false;
                connect.setText("Connect");
                status.setText("Disconnected");
            }
        }
        catch(Exception ex) {
            ex.printStackTrace();
        }
    }

    void commandTextField_keyTyped(KeyEvent e) {
        if(e.getKeyChar() == '\n') sendButton_actionPerformed(null);
    }
}
