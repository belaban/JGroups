// $Id: Gossip.java,v 1.2 2003/09/24 23:20:47 belaban Exp $

package org.jgroups.demos;


import org.jgroups.*;
import org.jgroups.log.Trace;
import org.jgroups.util.Util;

import javax.swing.*;
import java.awt.*;
import java.awt.event.*;
import java.io.*;
import java.util.Random;
import java.util.Vector;




/**
 * Demos that tries to graphically illustrating the gossip (or pbcast) protocol: every sender periodically sends
 * a DRAW command to a random subset of the group members. Each member checks whether it already received the
 * message and applies it if not yet received. Otherwise it discards it. If not yet received, the message will
 * be forwarded to 10% of the group members. This demo is probably only interesting when we have a larger
 * number of members: a gossip will gradually reach all members, coloring their whiteboards.
 */
public class Gossip implements Runnable, WindowListener, ActionListener, ChannelListener {
    private Graphics graphics=null;
    private Frame mainFrame=null;
    private JPanel panel=null, sub_panel=null;
    private ByteArrayOutputStream out=new ByteArrayOutputStream();
    private Random random=new Random(System.currentTimeMillis());
    private Button gossip_button, clear_button, leave_button;
    private final Font default_font=new Font("Helvetica", Font.PLAIN, 12);
    private String groupname="GossipGroupDemo";
    private Channel channel=null;
    private Thread receiver=null;
    private int member_size=1;
    private Vector members=new Vector();
    private int red=0, green=0, blue=0;
    private Color default_color=null;
    boolean first=true;
    double subset=0.1;
    Address local_addr=null;
    TrafficGenerator gen=null;
    long traffic_interval=0;


    public Gossip(String props, long traffic) throws Exception {
        Trace.init();
        channel=new JChannel(props);
        channel.setChannelListener(this);
        channel.setOpt(Channel.AUTO_RECONNECT, Boolean.TRUE);
        traffic_interval=traffic;
        if(traffic_interval > 0) {
            gen=new TrafficGenerator();
            gen.start();
        }
    }


    public static void main(String[] args) {
        Gossip gossip=null;
        String props=null;
        long traffic=0;


        for(int i=0; i < args.length; i++) {
            if(args[i].equals("-help")) {
                System.out.println("Gossip [-traffic_interval <interval in msecs>] [-help]");
                return;
            }
            if(args[i].equals("-traffic_interval")) {
                traffic=new Long(args[++i]).longValue();
                continue;
            }
        }


        // props="UDP:PING:FD:STABLE:NAKACK:UNICAST:FRAG:FLUSH:GMS:VIEW_ENFORCER:PERF(trace=;details=true)";



        /**
         props="TCP(start_port=8000):" +
         "TCPPING(num_initial_members=1;timeout=3000;port_range=2;"+
         "initial_hosts=daddy[8000],terrapin[8000],sindhu[8000]):" +
         "FD:" +
         "pbcast.PBCAST(gossip_interval=5000;gc_lag=50):" +
         "UNICAST:" +
         "FRAG:" +
         "pbcast.GMS";
         // "PERF(trace=true;details=true)";
         **/





        props="UDP(mcast_addr=224.10.10.100;mcast_port=5678;ip_ttl=32):" +
                "PING:" +
                // "FD(shun=true;timeout=5000):" +
                "pbcast.FD(timeout=3000):" +
                "VERIFY_SUSPECT(timeout=2000;num_msgs=2):" +
                "pbcast.PBCAST(desired_avg_gossip=8000;mcast_gossip=true;gc_lag=30;max_queue=20):" +
                "UNICAST:" +
                "FRAG:" +
                "pbcast.GMS"; // :" + // ;join_timeout=20):" +
        // "PERF(trace=true;details=true)";



        try {
            gossip=new Gossip(props, traffic);
            gossip.go();
        }
        catch(Exception e) {
            System.err.println(e);
            System.exit(0);
        }
    }


    private void selectColor() {
        red=(Math.abs(random.nextInt()) % 255);
        green=(Math.abs(random.nextInt()) % 255);
        blue=(Math.abs(random.nextInt()) % 255);
        default_color=new Color(red, green, blue);
    }


    public void go() {
        try {
            channel.connect(groupname);
            local_addr=channel.getLocalAddress();
            startThread();
            mainFrame=new Frame();
            panel=new MyPanel();
            sub_panel=new JPanel();
            mainFrame.setSize(250, 250);
            mainFrame.add("Center", panel);
            clear_button=new Button("Clear");
            clear_button.setFont(default_font);
            clear_button.addActionListener(this);
            gossip_button=new Button("Gossip");
            gossip_button.setFont(default_font);
            gossip_button.addActionListener(this);
            leave_button=new Button("Leave & Exit");
            leave_button.setFont(default_font);
            leave_button.addActionListener(this);
            sub_panel.add("South", gossip_button);
            sub_panel.add("South", clear_button);
            sub_panel.add("South", leave_button);
            mainFrame.add("South", sub_panel);
            mainFrame.addWindowListener(this);
            mainFrame.setVisible(true);
            setTitle();
            graphics=panel.getGraphics();
            graphics.setColor(default_color);
            mainFrame.setBackground(Color.white);
            mainFrame.pack();
            gossip_button.setForeground(Color.blue);
            clear_button.setForeground(Color.blue);
            leave_button.setForeground(Color.blue);
        }
        catch(Exception e) {
            System.err.println(e);
            return;
        }
    }


    void startThread() {
        receiver=new Thread(this, "GossipThread");
        receiver.setPriority(Thread.MAX_PRIORITY);
        receiver.start();
    }


    void setTitle() {
        String title="";
        if(local_addr != null)
            title+=local_addr;
        title+=" (" + member_size + ") mbrs";
        mainFrame.setTitle(title);
    }


    public void run() {
        Object tmp;
        Message msg=null;
        Command comm;
        boolean fl=true;
        Vector mbrs;
        ObjectOutputStream os;

        while(fl) {
            try {
                tmp=channel.receive(0);
                // System.out.println("Gossip.run(): received " + tmp);

                if(tmp == null) continue;

                if(tmp instanceof View) {
                    View v=(View)tmp;
                    member_size=v.size();
                    mbrs=v.getMembers();
                    members.removeAllElements();
                    for(int i=0; i < mbrs.size(); i++)
                        members.addElement(mbrs.elementAt(i));
                    if(mainFrame != null)
                        setTitle();
                    continue;
                }

                if(tmp instanceof ExitEvent) {
                    // System.out.println("-- Gossip.main(): received EXIT, waiting for ChannelReconnected callback");
                    break;
                }

                if(!(tmp instanceof Message))
                    continue;

                msg=(Message)tmp;
                comm=null;

                Object obj=Util.objectFromByteBuffer(msg.getBuffer());

                // System.out.println("obj is " + obj);

                if(obj instanceof Command)
                    comm=(Command)obj;
                else
                    if(obj instanceof Message) {
                        System.out.println("*** Message is " + Util.printMessage((Message)obj));
                        Util.dumpStack(true);
                    }
                    else {
                        if(obj != null)
                            System.out.println("obj is " + obj.getClass() + ", hdrs are" + msg.printObjectHeaders());
                        else
                            System.out.println("hdrs are" + msg.printObjectHeaders());
                        Util.dumpStack(true);
                    }

                switch(comm.mode) {
                    case Command.GOSSIP:
                        if(graphics != null) {
                            colorPanel(comm.r, comm.g, comm.b);
                            comm.not_seen.removeElement(local_addr);
                            if(comm.not_seen.size() > 0) { // forward gossip
                                Vector v=Util.pickSubset(comm.not_seen, subset);
                                out.reset();
                                os=new ObjectOutputStream(out);
                                os.writeObject(comm);
                                os.flush();
                                for(int i=0; i < v.size(); i++) {
                                    channel.send(new Message((Address)v.elementAt(i), null, out.toByteArray()));
                                }
                            }
                        }
                        break;
                    case Command.CLEAR:
                        clearPanel();
                        continue;
                    default:
                        System.err.println("***** Gossip.run(): received invalid draw command " + comm.mode);
                        break;
                }

            }
            catch(ChannelNotConnectedException not) {
                System.err.println("Gossip: " + not);
                break;
            }
            catch(ChannelClosedException closed) {
                System.err.println("Gossip: channel was closed");
                break;
            }
            catch(Exception e) {
                System.err.println(e);
                continue; // break;
            }
        }
    }


    /* --------------- Callbacks --------------- */


    public void mouseMoved(MouseEvent e) {
    }


    public void clearPanel() {
        Rectangle bounds=null;
        if(panel == null || graphics == null)
            return;

        bounds=panel.getBounds();
        graphics.clearRect(0, 0, bounds.width, bounds.height);
    }


    public void colorPanel(int r, int g, int b) {
        if(graphics != null) {
            red=r;
            green=g;
            blue=b;
            graphics.setColor(new Color(red, green, blue));
            Rectangle bounds=panel.getBounds();
            graphics.fillRect(0, 0, bounds.width, bounds.height);
            graphics.setColor(default_color);
        }
    }


    void sendGossip() {
        int tmp[]=new int[1];
        tmp[0]=0;
        Command comm;
        ObjectOutputStream os;
        Vector dests=(Vector)members.clone();

        try {
            selectColor();  // set a new randomly chosen color
            dests.removeElement(local_addr);
            dests=Util.pickSubset(dests, subset);
            if(dests == null || dests.size() == 0) {  // only apply new color locally
                // System.out.println("-- local");
                colorPanel(red, green, blue);
                return;
            }

            colorPanel(red, green, blue);
            comm=new Command(Command.GOSSIP, red, green, blue);
            comm.not_seen=(Vector)members.clone();
            comm.not_seen.removeElement(local_addr);
            out.reset();
            os=new ObjectOutputStream(out);
            os.writeObject(comm);
            os.flush();
            for(int i=0; i < dests.size(); i++) {
                channel.send(new Message((Address)dests.elementAt(i), null, out.toByteArray()));
            }
        }
        catch(Exception ex) {
            System.err.println(ex);
        }
    }


    public void sendClearPanelMsg() {
        int tmp[]=new int[1];
        tmp[0]=0;
        Command comm=new Command(Command.CLEAR);
        ObjectOutputStream os;

        try {
            out.reset();
            os=new ObjectOutputStream(out);
            os.writeObject(comm);
            os.flush();
            channel.send(new Message(null, null, out.toByteArray()));
        }
        catch(Exception ex) {
            System.err.println(ex);
        }
    }


    public void windowActivated(WindowEvent e) {
    }

    public void windowClosed(WindowEvent e) {
    }

    public void windowClosing(WindowEvent e) {
        System.exit(0);  // exit the dirty way ...
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
        if(command.equals("Gossip")) {
            sendGossip();
        }
        else
            if(command.equals("Clear"))
                sendClearPanelMsg();
            else
                if(command.equals("Leave & Exit")) {
                    try {
                        channel.disconnect();
                        channel.close();
                    }
                    catch(Exception ex) {
                        System.err.println(ex);
                    }
                    mainFrame.setVisible(false);
                    System.exit(0);
                }
                else
                    System.out.println("Unknown action");
    }


    public void channelConnected(Channel channel) {
        if(first)
            first=false;
        else
            startThread();
    }

    public void channelDisconnected(Channel channel) {
        // System.out.println("----> channelDisconnected()");
    }

    public void channelClosed(Channel channel) {
        // System.out.println("----> channelClosed()");
    }

    public void channelShunned() {
        System.out.println("----> channelShunned()");
    }

    public void channelReconnected(Address new_addr) {
        System.out.println("----> channelReconnected(" + new_addr + ")");
        local_addr=new_addr;
    }


    private static class Command implements Serializable {
        static final int GOSSIP=1;
        static final int CLEAR=2;
        int mode;
        int r=0;
        int g=0;
        int b=0;
        Vector not_seen=new Vector();

        Command(int mode) {
            this.mode=mode;
        }

        Command(int mode, int r, int g, int b) {
            this.mode=mode;
            this.r=r;
            this.g=g;
            this.b=b;
        }


        public String toString() {
            StringBuffer ret=new StringBuffer();
            switch(mode) {
                case GOSSIP:
                    ret.append("GOSSIP(" + r + "|" + g + "|" + b);
                    break;
                case CLEAR:
                    ret.append("CLEAR");
                    break;
                default:
                    return "<undefined>";
            }
            ret.append(", not_seen=" + not_seen);
            return ret.toString();
        }
    }


    private class TrafficGenerator implements Runnable {
        Thread generator=null;

        public void start() {
            if(generator == null) {
                generator=new Thread(this, "TrafficGeneratorThread");
                generator.start();
            }
        }

        public void stop() {
            if(generator != null)
                generator=null;
            generator=null;
        }

        public void run() {
            while(generator != null) {
                Util.sleep(traffic_interval);
                if(generator != null)
                    sendGossip();
            }
        }
    }


    private class MyPanel extends JPanel {
        Dimension preferred_size=new Dimension(200, 200);

        public Dimension getPreferredSize() {
            return preferred_size;
        }

    }


}






