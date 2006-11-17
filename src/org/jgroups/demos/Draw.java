// $Id: Draw.java,v 1.34 2006/11/17 13:39:18 belaban Exp $


package org.jgroups.demos;


import org.jgroups.*;
import org.jgroups.debug.Debugger;
import org.jgroups.jmx.JmxConfigurator;
import org.jgroups.util.Util;

import javax.management.MBeanServer;
import javax.swing.*;
import java.awt.*;
import java.awt.event.*;
import java.util.Random;




/**
 * Shared whiteboard, each new instance joins the same group. Each instance chooses a random color,
 * mouse moves are broadcast to all group members, which then apply them to their canvas<p>
 * @author Bela Ban, Oct 17 2001
 */
public class Draw extends ExtendedReceiverAdapter implements ActionListener, ChannelListener {
    String                         groupname="DrawGroupDemo";
    private Channel                channel=null;
    private int                    member_size=1;
    Debugger                       debugger=null;
    final boolean                  first=true;
    private JFrame                 mainFrame=null;
    private JPanel                 sub_panel=null;
    private DrawPanel              panel=null;
    private JButton                clear_button, leave_button;
    private final Random           random=new Random(System.currentTimeMillis());
    private final Font             default_font=new Font("Helvetica",Font.PLAIN,12);
    private final Color            draw_color=selectColor();
    private final Color background_color=Color.white;
    boolean                        no_channel=false;
    boolean                        jmx;


    public Draw(String props, boolean debug, boolean no_channel, boolean jmx) throws Exception {
        this.no_channel=no_channel;
        this.jmx=jmx;
        if(no_channel)
            return;

        channel=new JChannel(props);
        // channel.setOpt(Channel.BLOCK, Boolean.TRUE);
        if(debug) {
            debugger=new Debugger((JChannel)channel);
            debugger.start();
        }
        channel.setOpt(Channel.AUTO_RECONNECT, Boolean.TRUE);
        channel.setReceiver(this);
        channel.addChannelListener(this);
    }

    public Draw(Channel channel) throws Exception {
        this.channel=channel;
        channel.setOpt(Channel.AUTO_RECONNECT, Boolean.TRUE);
        channel.setReceiver(this);
        channel.addChannelListener(this);
    }


    public String getGroupName() {
        return groupname;
    }

    public void setGroupName(String groupname) {
        if(groupname != null)
            this.groupname=groupname;
    }


   public static void main(String[] args) {
       Draw             draw=null;
       String           props=null;
       boolean          debug=false;
       boolean          no_channel=false;
       boolean          jmx=false;
       String           group_name=null;

        for(int i=0; i < args.length; i++) {
            if("-help".equals(args[i])) {
                help();
                return;
            }
            if("-debug".equals(args[i])) {
                debug=true;
                continue;
            }
            if("-props".equals(args[i])) {
                props=args[++i];
                continue;
            }
            if("-no_channel".equals(args[i])) {
                no_channel=true;
                continue;
            }
            if("-jmx".equals(args[i])) {
                jmx=true;
                continue;
            }
            if("-groupname".equals(args[i])) {
                group_name=args[++i];
                continue;
            }

            help();
            return;
        }

        if(props == null) {
            props="UDP(down_thread=false;mcast_send_buf_size=640000;mcast_port=45566;discard_incompatible_packets=true;" +
                    "ucast_recv_buf_size=20000000;mcast_addr=228.10.10.10;up_thread=false;loopback=false;" +
                    "mcast_recv_buf_size=25000000;max_bundle_size=64000;max_bundle_timeout=30;" +
                    "use_incoming_packet_handler=true;use_outgoing_packet_handler=false;" +
                    "ucast_send_buf_size=640000;tos=16;enable_bundling=true;ip_ttl=2):" +
                  "PING(timeout=2000;down_thread=false;num_initial_members=3;up_thread=false):" +
                  "MERGE2(max_interval=10000;down_thread=false;min_interval=5000;up_thread=false):" +
                  "FD(timeout=2000;max_tries=3;down_thread=false;up_thread=false):" +
                  "VERIFY_SUSPECT(timeout=1500;down_thread=false;up_thread=false):" +
                  "pbcast.NAKACK(max_xmit_size=60000;down_thread=false;use_mcast_xmit=false;gc_lag=0;" +
                    "discard_delivered_msgs=true;up_thread=false;retransmit_timeout=100,200,300,600,1200,2400,4800):" +
                  "UNICAST(timeout=300,600,1200,2400,3600;down_thread=false;up_thread=false):" +
                    "pbcast.STABLE(stability_delay=1000;desired_avg_gossip=50000;max_bytes=400000;down_thread=false;" +
                    "up_thread=false):" +
                  "VIEW_SYNC(down_thread=false;avg_send_interval=60000;up_thread=false):" +
                    "pbcast.GMS(print_local_addr=true;join_timeout=3000;down_thread=false;" +
                    "join_retry_timeout=2000;up_thread=false;shun=true):" +
                  "FC(max_credits=2000000;down_thread=false;up_thread=false;min_threshold=0.10):" +
                  "FRAG2(frag_size=60000;down_thread=false;up_thread=false):" +
                    "pbcast.STATE_TRANSFER(down_thread=false;up_thread=false)";
        }


        try {
            draw=new Draw(props, debug, no_channel, jmx);
            if(group_name != null)
                draw.setGroupName(group_name);
            draw.go();
        }
        catch(Throwable e) {
            e.printStackTrace();
            System.exit(0);
        }
    }


    static void help() {
        System.out.println("\nDraw [-help] [-debug] [-no_channel] [-props <protocol stack definition>]" +
                           " [-groupname <name>]");
        System.out.println("-debug: brings up a visual debugger");
        System.out.println("-no_channel: doesn't use JGroups at all, any drawing will be relected on the " +
                           "whiteboard directly");
        System.out.println("-props: argument can be an old-style protocol stack specification, or it can be " +
                           "a URL. In the latter case, the protocol specification will be read from the URL\n");
    }


    private Color selectColor() {
        int red=(Math.abs(random.nextInt()) % 255);
        int green=(Math.abs(random.nextInt()) % 255);
        int blue=(Math.abs(random.nextInt()) % 255);
        return new Color(red, green, blue);
    }



    public void go() throws Exception {
        if(!no_channel) {
            channel.connect(groupname);
            if(jmx) {
                MBeanServer server=Util.getMBeanServer();
                if(server == null)
                    throw new Exception("No MBeanServers found;" +
                            "\nDraw needs to be run with an MBeanServer present, or inside JDK 5");
                JmxConfigurator.registerChannel((JChannel)channel, server, "jgroups", channel.getClusterName(), true);
            }
        }
        mainFrame=new JFrame();
        mainFrame.setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
        panel=new DrawPanel();
        panel.setBackground(background_color);
        sub_panel=new JPanel();
        mainFrame.getContentPane().add("Center", panel);
        clear_button=new JButton("Clear");
        clear_button.setFont(default_font);
        clear_button.addActionListener(this);
        leave_button=new JButton("Leave");
        leave_button.setFont(default_font);
        leave_button.addActionListener(this);
        sub_panel.add("South", clear_button);
        sub_panel.add("South", leave_button);
        mainFrame.getContentPane().add("South", sub_panel);
        mainFrame.setBackground(background_color);
        clear_button.setForeground(Color.blue);
        leave_button.setForeground(Color.blue);
        setTitle();
        mainFrame.pack();
        mainFrame.setLocation(15, 25);
        mainFrame.setBounds(new Rectangle(250, 250));
        mainFrame.setVisible(true);
    }




    void setTitle(String title) {
        String tmp="";
        if(no_channel) {
            mainFrame.setTitle(" Draw Demo ");
            return;
        }
        if(title != null) {
            mainFrame.setTitle(title);
        }
        else {
            if(channel.getLocalAddress() != null)
                tmp+=channel.getLocalAddress();
            tmp+=" (" + member_size + ")";
            mainFrame.setTitle(tmp);
        }
    }

    void setTitle() {
        setTitle(null);
    }



    public void receive(Message msg) {
        byte[] buf=msg.getRawBuffer();
        if(buf == null) {
            System.err.println("received null buffer from " + msg.getSrc() + ", headers: " + msg.getHeaders());
            return;
        }

        try {
            DrawCommand comm=(DrawCommand)Util.streamableFromByteBuffer(DrawCommand.class, buf, msg.getOffset(), msg.getLength());
            switch(comm.mode) {
                case DrawCommand.DRAW:
                    if(panel != null)
                        panel.drawPoint(comm);
                    break;
                case DrawCommand.CLEAR:
                    clearPanel();
                    break;
                default:
                    System.err.println("***** received invalid draw command " + comm.mode);
                    break;
            }
        }
        catch(Exception e) {
            e.printStackTrace();
        }
    }

    public void viewAccepted(View v) {
        if(v instanceof MergeView)
            System.out.println("** MergeView=" + v);
        else
            System.out.println("** View=" + v);
        member_size=v.size();
        if(mainFrame != null)
            setTitle();
    }

    public void block() {
        System.out.println("--  received BlockEvent");
    }

    public void unblock() {
        System.out.println("-- received UnblockEvent");
    }



    /* --------------- Callbacks --------------- */



    public void clearPanel() {
        if(panel != null)
            panel.clear();
    }

    public void sendClearPanelMsg() {
        int                  tmp[]=new int[1]; tmp[0]=0;
        DrawCommand          comm=new DrawCommand(DrawCommand.CLEAR);

        try {
            byte[] buf=Util.streamableToByteBuffer(comm);
            channel.send(new Message(null, null, buf));
        }
        catch(Exception ex) {
            System.err.println(ex);
        }
    }


    public void actionPerformed(ActionEvent e) {
        String     command=e.getActionCommand();
        if("Clear".equals(command)) {
            if(no_channel) {
                clearPanel();
                return;
            }
            sendClearPanelMsg();
        }
        else if("Leave".equals(command)) {
            stop();
        }
        else
            System.out.println("Unknown action");
    }


    public void stop() {
        if(!no_channel) {
            try {
                channel.close();
            }
            catch(Exception ex) {
                System.err.println(ex);
            }
        }
        mainFrame.setVisible(false);
        mainFrame.dispose();
    }


    /* ------------------------------ ChannelListener interface -------------------------- */

    public void channelConnected(Channel channel) {

    }

    public void channelDisconnected(Channel channel) {

    }

    public void channelClosed(Channel channel) {

    }

    public void channelShunned() {
        System.out.println("-- received EXIT, waiting for ChannelReconnected callback");
        setTitle(" Draw Demo - shunned ");
    }

    public void channelReconnected(Address addr) {
        setTitle();
    }


    /* --------------------------- End of ChannelListener interface ---------------------- */



    private class DrawPanel extends JPanel implements MouseMotionListener {
        final Dimension        preferred_size=new Dimension(235, 170);
        Image            img=null; // for drawing pixels
        Dimension        d, imgsize=null;
        Graphics         gr=null;


        public DrawPanel() {
            createOffscreenImage();
            addMouseMotionListener(this);
            addComponentListener(new ComponentAdapter() {
                public void componentResized(ComponentEvent e) {
                    if(getWidth() <= 0 || getHeight() <= 0) return;
                    createOffscreenImage();
                }
            });
        }



        final void createOffscreenImage() {
            d=getSize();
            if(img == null || imgsize == null || imgsize.width != d.width || imgsize.height != d.height) {
                img=createImage(d.width, d.height);
                if(img != null)
                    gr=img.getGraphics();
                imgsize=d;
            }
        }


        /* ---------------------- MouseMotionListener interface------------------------- */

        public void mouseMoved(MouseEvent e) {}

        public void mouseDragged(MouseEvent e) {
            int                 x=e.getX(), y=e.getY();
            DrawCommand         comm=new DrawCommand(DrawCommand.DRAW, x, y,
                                                     draw_color.getRed(), draw_color.getGreen(), draw_color.getBlue());

            if(no_channel) {
                drawPoint(comm);
                return;
            }

            try {
                byte[] buf=Util.streamableToByteBuffer(comm);
                channel.send(new Message(null, null, buf));
                Thread.yield(); // gives the repainter some breath
            }
            catch(Exception ex) {
                System.err.println(ex);
            }
        }

        /* ------------------- End of MouseMotionListener interface --------------------- */


        /**
         * Adds pixel to queue and calls repaint() whenever we have MAX_ITEMS pixels in the queue
         * or when MAX_TIME msecs have elapsed (whichever comes first). The advantage compared to just calling
         * repaint() after adding a pixel to the queue is that repaint() can most often draw multiple points
         * at the same time.
         */
        public void drawPoint(DrawCommand c) {
            if(c == null || gr == null) return;
            gr.setColor(new Color(c.r, c.g, c.b));
            gr.fillOval(c.x, c.y, 10, 10);
            repaint();
        }



        public void clear() {
            if(gr == null) return;
            gr.clearRect(0, 0, getSize().width, getSize().height);
            repaint();
        }


        public Dimension getPreferredSize() {
            return preferred_size;
        }


        public void paintComponent(Graphics g) {
            super.paintComponent(g);
            if(img != null) {
                g.drawImage(img, 0, 0, null);
            }
        }

    }





}

