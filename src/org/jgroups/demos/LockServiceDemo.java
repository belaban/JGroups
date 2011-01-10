package org.jgroups.demos;

import org.jgroups.util.Util;

import javax.swing.*;
import java.awt.*;
import java.awt.event.*;
import java.util.*;
import java.util.List;


/**
 * Chat client using STOMP to talk to other clients
 */
public class LockServiceDemo {
    JFrame        mainFrame;
    TextArea      txtArea;
    JTextField    txtField;
    final JLabel  csLabel=new JLabel("Send: "), status=new JLabel("");
    JButton       leaveButton;
    JButton       sendButton;
    JButton       clearButton;
    final JLabel  cluster=new JLabel("Cluster: "), users_label=new JLabel("Users: ");

    private int                    num_servers=1;
    private int                    num_clients=0;
    protected final Set<String>    users=new HashSet<String>();
    private final List<String>     servers=new ArrayList<String>();
    private final Set<String>      clients=new HashSet<String>();






    
    public static void main(String[] args) throws Exception {
        LockServiceDemo instance=new LockServiceDemo();
        instance.start();
    }

    void showMessage(String msg) {
        txtArea.append(msg + "\n");
    }

    void userJoined(String name) {
        users.add(name);
        showStatus(name + " joined the chat");
        users_label.setText("Users: " + users);
    }

    void userLeft(String name) {
        users.remove(name);
        showStatus(name + " left the chat");
        users_label.setText("Users: " + users);
    }

    void newView(String view) {
        cluster.setText("Cluster: " + view);
    }

    void usersReceived(Collection<String> users) {
        this.users.addAll(users);
        users_label.setText("Users: " + this.users);
    }

    static void help() {
        System.out.println("Chat [-help] [-host <host>] [-port <port>] [-user <user>]");
    }

    public void start() throws Exception {
        mainFrame=new JFrame("Chat demo");
        mainFrame.setPreferredSize(new Dimension(600,600));
        mainFrame.setBackground(Color.white);
        mainFrame.addWindowListener(new WindowAdapter() {
            public void windowClosing(WindowEvent e) {
                System.exit(0);
            }
        });

        Box main_box=Box.createVerticalBox();
        main_box.setBackground(Color.white);
        Box input=Box.createHorizontalBox();   // input field
        Box buttons=Box.createHorizontalBox(); // for all the buttons
        mainFrame.add(main_box);

        main_box.add(Box.createVerticalStrut(10));
        main_box.add(cluster);
        cluster.setAlignmentX(Component.LEFT_ALIGNMENT);
        main_box.add(Box.createVerticalStrut(10));

        main_box.add(Box.createVerticalStrut(10));
        main_box.add(users_label);
        main_box.add(Box.createVerticalStrut(10));

        txtArea=new TextArea();
        txtArea.setPreferredSize(new Dimension(550, 500));
        txtArea.setEditable(false);
        txtArea.setBackground(Color.white);
        main_box.add(txtArea);

        main_box.add(Box.createVerticalStrut(10));
        main_box.add(input);
        main_box.add(Box.createVerticalStrut(10));
        main_box.add(buttons);

        csLabel.setPreferredSize(new Dimension(85, 30));
        input.add(csLabel);

        txtField=new JTextField();
        txtField.setPreferredSize(new Dimension(200, 30));
        txtField.setBackground(Color.white);
        input.add(txtField);


        leaveButton=new JButton("Leave");
        leaveButton.setPreferredSize(new Dimension(150, 30));
        buttons.add(leaveButton);
        leaveButton.addMouseListener(new MouseAdapter() {
            public void mouseClicked(MouseEvent e) {
                System.exit(0);
            }
        });

        sendButton=new JButton("Send");
        sendButton.setPreferredSize(new Dimension(150, 30));
        buttons.add(sendButton);
        sendButton.addMouseListener(new MouseAdapter() {
            public void mouseClicked(MouseEvent e) {
                // send(txtField.getText());
                txtField.selectAll();
            }
        });

        clearButton=new JButton("Clear");
        clearButton.setPreferredSize(new Dimension(150, 30));
        clearButton.addMouseListener(new MouseAdapter() {
            public void mouseClicked(MouseEvent e) {
                txtArea.setText("");
            }
        });
        buttons.add(clearButton);

        status.setForeground(Color.red);
        main_box.add(status);

        mainFrame.pack();
        mainFrame.setLocation(15, 25);
        Dimension main_frame_size=mainFrame.getSize();
        txtArea.setPreferredSize(new Dimension((int)(main_frame_size.width * 0.9), (int)(main_frame_size.height * 0.8)));
        mainFrame.setVisible(true);
        txtField.setFocusable(true);
        txtField.requestFocusInWindow();
        txtField.setToolTipText("type and then press enter to send");
        txtField.addActionListener(new ActionListener() {
            public void actionPerformed(ActionEvent e) {
                String cmd=e.getActionCommand();
                if(cmd != null && cmd.length() > 0) {
                    // send(txtField.getText());
                    txtField.selectAll();
                }
            }
        });

    }



    protected void showStatus(final String msg) {
        new Thread() {
            public void run() {
                synchronized(status) {
                    status.setText(msg);
                    Util.sleep(2000);
                    status.setText("");
                }
            }
        }.start();
    }


    public void onInfo(Map<String, String> information) {
         String view=information.get("view");
         Collection<String> list;
         if(view != null) {
             list=Util.parseCommaDelimitedStrings(view);
             if(list != null) {
                 num_servers=list.size();
                 if(mainFrame != null)
                     setTitle();
                 servers.clear();
                 servers.addAll(list);
                 newView(view);
             }
             else {
                 String targets=information.get("endpoints");
                 if(targets != null) {
                     list=Util.parseCommaDelimitedStrings(targets);
                     if(list != null) {
                         num_servers=list.size();
                         if(mainFrame != null)
                             setTitle();
                         servers.clear();
                         servers.addAll(list);
                     }
                 }
             }
         }

        
     }





    void setTitle() {
        if(mainFrame != null)
            mainFrame.setTitle(num_servers + " server(s), " + num_clients + " client(s)");
    }

    int getNumberOfClients() {
        synchronized(clients) {
            return clients.size();
        }
    }

    String getAllClients() {
        StringBuilder sb=new StringBuilder();
        boolean first=true;
        for(String client: clients) {
            if(first)
                first=false;
            else
                sb.append(",");
            sb.append(client);
        }

        return sb.toString();
    }
    

}
