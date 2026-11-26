package org.jgroups.demos;

import org.jgroups.client.StompConnection;
import org.jgroups.util.Util;

import javax.swing.*;
import java.awt.*;
import java.awt.event.*;
import java.util.*;
import java.util.List;


/**
 * Chat client using STOMP to talk to other clients
 */
public class StompChat implements StompConnection.Listener {
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
    protected String               username=null;
    protected final Set<String>    users=new HashSet<>();
    private final List<String>     servers=new ArrayList<>();
    private final Set<String>      clients=new HashSet<>();
    protected StompConnection      stomp_client;


    // ======================== reserved topic ==========================
    public static final String    MESSAGES      = "/messages";      // headers + body
    public static final String    CLIENT_JOINED = "/client-joined"; // client: 1234-2532-2665
    public static final String    CLIENT_LEFT   = "/client-left";   // client: 1432-7263-1002
    public static final String    CLIENTS       = "/clients";       // clients: 355352,3343,2232
    public static final String    USER_JOINED   = "/user-joined";   // user: Bela
    public static final String    USER_LEFT     = "/user-left";     // user: Bela
    public static final String    GET_USERS     = "/get-users";     //
    public static final String    USERS         = "/users";         // users: Bela, Michelle


    // reserved keywords in INFO messages
    public static final String    ENDPOINTS     = "endpoints";
    public static final String    VIEW          = "view";
    public static final String    CLIENTS_KW    = "clients";
    public static final String    DESTINATION   = "destination";
    public static final String    USER          = "user";
    public static final String    USERS_KW      = "users";
    public static final String    CLIENT        = "client";

    


    public StompChat(String host, int port, String user) {
        stomp_client=new StompConnection(host + ":" + port);
        stomp_client.addListener(this);

        username=user;
        try {
            if(username == null)
                username=System.getProperty("user.name");
        }
        catch(Throwable t) {
        }
    }

    
    public static void main(String[] args) throws Exception {
        String host="localhost";
        int    port=8787;
        String user=null;

        for(int i=0; i < args.length; i++) {
            if(args[i].equals("-host") || args[i].equals("-h")) {
                host=args[++i];
                continue;
            }
            if(args[i].equals("-port") || args[i].equals("-p")) {
                port=Integer.parseInt(args[++i]);
                continue;
            }
            if(args[i].equals("-user") || args[i].equals("-name")) {
                user=args[++i];
                continue;
            }
            help();
            return;
        }

        StompChat instance=new StompChat(host, port, user);
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
                stomp_client.send(USER_LEFT, USER, username);
                stomp_client.send(CLIENT_LEFT, CLIENT, username);
                stomp_client.disconnect();
                System.exit(0);
            }
        });

        connect();

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
                stomp_client.send(USER_LEFT, USER, username);
                stomp_client.send(CLIENT_LEFT, CLIENT, username);
                stomp_client.disconnect();
                System.exit(0);
            }
        });

        sendButton=new JButton("Send");
        sendButton.setPreferredSize(new Dimension(150, 30));
        buttons.add(sendButton);
        sendButton.addMouseListener(new MouseAdapter() {
            public void mouseClicked(MouseEvent e) {
                send(txtField.getText());
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
        txtField.addActionListener(e -> {
            String cmd=e.getActionCommand();
            if(cmd != null && !cmd.isEmpty()) {
                send(txtField.getText());
                txtField.selectAll();
            }
        });

        sendGetUsers();
    }

    protected void connect() throws Exception {
        stomp_client.connect();
        stomp_client.send(USER_JOINED, USER, username);
        stomp_client.subscribe(MESSAGES);
        stomp_client.subscribe(CLIENT_JOINED);
        stomp_client.subscribe(CLIENT_LEFT);
        stomp_client.subscribe(CLIENTS);
        stomp_client.subscribe(USER_JOINED);
        stomp_client.subscribe(USER_LEFT);
        stomp_client.subscribe(GET_USERS);
        stomp_client.subscribe(USERS);
        stomp_client.send(CLIENT_JOINED, CLIENT, username);
        stomp_client.send(USER_JOINED, USER, username);
    }

    protected void send(String msg) {
        try {
            String tmp=username + ": " + msg;
            byte[] buf=tmp.getBytes();
            stomp_client.send(MESSAGES, buf, 0, buf.length);
        }
        catch(Exception e) {
            System.err.println("Failed sending message: " + e);
        }
    }

    public void sendGetUsers() {
        stomp_client.send(GET_USERS); 
    }

    protected void showStatus(final String msg) {
        new Thread(() -> {
            synchronized(status) {
                status.setText(msg);
                Util.sleep(2000);
                status.setText("");
            }
        }).start();
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
                 String targets=information.get(ENDPOINTS);
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

     public void onMessage(Map<String, String> headers, byte[] buf, int offset, int length) {
         String destination=headers.get(DESTINATION);
         if(destination == null)
             return;

         if(destination.equals(MESSAGES)) {
             showMessage(new String(buf, offset, length));
             return;
         }


         if(destination.equals(CLIENT_JOINED)) {
             String new_client=headers.get(CLIENT);
             if(new_client != null) {
                 synchronized(clients) {
                     if(clients.add(new_client)) {
                         num_clients=clients.size();
                         setTitle();
                     }
                 }
                 stomp_client.send(CLIENTS, null, 0, 0, CLIENTS_KW, getAllClients());
             }
             return;
         }

         if(destination.equals(CLIENT_LEFT)) {
             String left_client=headers.get(CLIENT);
             if(left_client != null) {
                 synchronized(clients) {
                     if(clients.remove(left_client)) {
                         num_clients=clients.size();
                         setTitle();
                     }
                 }
             }
             return;
         }

         if(destination.equals(CLIENTS)) {
             String all_clients=headers.get(CLIENTS_KW);
             if(all_clients != null) {
                 List<String> list=Util.parseCommaDelimitedStrings(all_clients);
                 if(list != null) {
                     synchronized(clients) {
                         if(clients.addAll(list)) {
                             num_clients=clients.size();
                             setTitle();
                         }
                     }
                 }
             }
             return;
         }

         if(destination.equals(USER_JOINED)) {
             String name=headers.get(USER);
             if(name != null)
                 userJoined(name);
             return;
         }

         if(destination.equals(USER_LEFT)) {
             String name=headers.get(USER);
             if(name != null)
                 userLeft(name);
             return;
         }

         if(destination.equals(GET_USERS)) {
             stomp_client.send(USERS, USERS_KW, usersToStr());
             return;
         }

         if(destination.equals(USERS)) {
             String tmp=headers.get(USERS_KW);
             if(tmp != null) {
                 List<String> list=Util.parseCommaDelimitedStrings(tmp);
                 if(list != null)
                     usersReceived(list);
             }
         }
     }

    private String usersToStr() {
        StringBuilder sb=new StringBuilder();
        boolean first=true;
        for(String user: users) {
            if(first)
                first=false;
            else
                sb.append(",");
            sb.append(user);
        }
        return sb.toString();
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
