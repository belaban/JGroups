// $Id: Draw.java,v 1.51 2007/09/07 04:56:34 belaban Exp $


package org.jgroups.demos;


import org.jgroups.*;
import org.jgroups.jmx.JmxConfigurator;
import org.jgroups.util.Util;

import javax.management.MBeanServer;
import javax.swing.*;
import java.awt.*;
import java.awt.event.*;
import java.util.*;
import java.io.*;


/**
 * Shared whiteboard, each new instance joins the same group. Each instance chooses a random color,
 * mouse moves are broadcast to all group members, which then apply them to their canvas<p>
 * @author Bela Ban, Oct 17 2001
 */
public class Draw extends ExtendedReceiverAdapter implements ActionListener, ChannelListener {
    String                         groupname="DrawGroupDemo";
    private Channel                channel=null;
    private int                    member_size=1;
    static final boolean           first=true;
    private JFrame                 mainFrame=null;
    private JPanel                 sub_panel=null;
    private DrawPanel              panel=null;
    private JButton                clear_button, leave_button;
    private final Random           random=new Random(System.currentTimeMillis());
    private final Font             default_font=new Font("Helvetica",Font.PLAIN,12);
    private final Color            draw_color=selectColor();
    private static final Color     background_color=Color.white;
    boolean                        no_channel=false;
    boolean                        jmx;
    private boolean                use_state=false;
    private long                   state_timeout=5000;


    public Draw(String props, boolean no_channel, boolean jmx, boolean use_state, long state_timeout,
                boolean use_blocking) throws Exception {
        this.no_channel=no_channel;
        this.jmx=jmx;
        this.use_state=use_state;
        this.state_timeout=state_timeout;
        if(no_channel)
            return;

        channel=new JChannel(props);
        if(use_blocking)
            channel.setOpt(Channel.BLOCK, Boolean.TRUE);
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


    public Draw(Channel channel, boolean use_state, long state_timeout) throws Exception {
        this.channel=channel;
        channel.setOpt(Channel.AUTO_RECONNECT, Boolean.TRUE);
        channel.setReceiver(this);
        channel.addChannelListener(this);
        this.use_state=use_state;
        this.state_timeout=state_timeout;
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
       boolean          no_channel=false;
       boolean          jmx=false;
       boolean          use_state=false;
       boolean          use_blocking=false;
       String           group_name=null;
       long             state_timeout=5000;

        for(int i=0; i < args.length; i++) {
            if("-help".equals(args[i])) {
                help();
                return;
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
            if("-state".equals(args[i])) {
                use_state=true;
                continue;
            }
            if("-use_blocking".equals(args[i])) {
                use_blocking=true;
                continue;
            }
            if("-timeout".equals(args[i])) {
                state_timeout=Long.parseLong(args[++i]);
                continue;
            }
            if("-bind_addr".equals(args[i])) {
                System.setProperty("jgroups.bind_addr", args[++i]);
                continue;
            }

            help();
            return;
        }

        try {
            draw=new Draw(props, no_channel, jmx, use_state, state_timeout, use_blocking);
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
        System.out.println("\nDraw [-help] [-no_channel] [-props <protocol stack definition>]" +
                           " [-groupname <name>] [-state] [-use_blocking] [-timeout <state timeout>] [-bind_addr <addr>]");
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
        if(!no_channel && !use_state) {
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
        panel=new DrawPanel(use_state);
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
        mainFrame.pack();
        mainFrame.setLocation(15, 25);
        mainFrame.setBounds(new Rectangle(250, 250));

        if(!no_channel && use_state) {
            channel.connect(groupname,null,null, state_timeout);
        }
        mainFrame.setVisible(true);
        setTitle();
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
            System.err.println("[" + channel.getLocalAddress() + "] received null buffer from " + msg.getSrc() +
                    ", headers: " + msg.printHeaders());
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


    public byte[] getState() {
        return panel.getState();
    }

    public void setState(byte[] state) {
        panel.setState(state);
    }


    public void getState(OutputStream ostream) {
        try {
            try {
                panel.writeState(ostream);
            }
            catch(IOException e) {
                e.printStackTrace();
            }
        }
        finally {
            Util.close(ostream);
        }
    }

    public void setState(InputStream istream) {
        try {
            try {
                panel.readState(istream);
            }
            catch(IOException e) {
                e.printStackTrace();
            }
        }
        finally {
            Util.close(istream);
        }
    }

    /* --------------- Callbacks --------------- */



    public void clearPanel() {
        if(panel != null)
            panel.clear();
    }

    public void sendClearPanelMsg() {
        DrawCommand comm=new DrawCommand(DrawCommand.CLEAR);

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
        final Dimension         preferred_size=new Dimension(235, 170);
        Image                   img=null; // for drawing pixels
        Dimension               d, imgsize=null;
        Graphics                gr=null;
        final Map<Point,Color>  state;


        public DrawPanel(boolean use_state) {
            if(use_state)
                state=new LinkedHashMap<Point,Color>();
            else
                state=null;
            createOffscreenImage(false);
            addMouseMotionListener(this);
            addComponentListener(new ComponentAdapter() {
                public void componentResized(ComponentEvent e) {
                    if(getWidth() <= 0 || getHeight() <= 0) return;
                    createOffscreenImage(false);
                }
            });
        }


        public byte[] getState() {
            byte[] retval=null;
            if(state == null) return null;
            synchronized(state) {
                try {
                    retval=Util.objectToByteBuffer(state);
                }
                catch(Exception e) {
                    e.printStackTrace();
                }
            }
            return retval;
        }


        public void setState(byte[] buf) {
            synchronized(state) {
                try {
                    Map<Point,Color> tmp=(Map<Point,Color>)Util.objectFromByteBuffer(buf);
                    state.clear();
                    state.putAll(tmp);
                    System.out.println("received state: " + buf.length + " bytes, " + state.size() + " entries");
                    createOffscreenImage(true);
                }
                catch(Exception e) {
                    e.printStackTrace();
                }
            }
        }

        public void writeState(OutputStream outstream) throws IOException {
            synchronized(state) {
                if(state != null) {
                    DataOutputStream dos=new DataOutputStream(outstream);
                    dos.writeInt(state.size());
                    Point point;
                    Color col;
                    for(Map.Entry<Point,Color> entry: state.entrySet()) {
                        point=entry.getKey();
                        col=entry.getValue();
                        dos.writeInt(point.x);
                        dos.writeInt(point.y);
                        dos.writeInt(col.getRGB());
                    }
                    dos.flush();
                }
            }
        }


        public void readState(InputStream instream) throws IOException {
            DataInputStream in=new DataInputStream(instream);
            Map<Point,Color> new_state=new HashMap<Point,Color>();
            int num=in.readInt();
            Point point;
            Color col;
            for(int i=0; i < num; i++) {
                point=new Point(in.readInt(), in.readInt());
                col=new Color(in.readInt());
                new_state.put(point, col);
            }

            synchronized(state) {
                state.clear();
                state.putAll(new_state);
                System.out.println("read state: " + state.size() + " entries");
                createOffscreenImage(true);
            }
        }


        final void createOffscreenImage(boolean discard_image) {
            d=getSize();
            if(discard_image) {
                img=null;
                imgsize=null;
            }
            if(img == null || imgsize == null || imgsize.width != d.width || imgsize.height != d.height) {
                img=createImage(d.width, d.height);
                if(img != null) {
                    gr=img.getGraphics();
                    if(gr != null && state != null) {
                        drawState();
                    }
                }
                imgsize=d;
            }
            repaint();
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
                // Thread.yield();
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
            Color col=new Color(c.r, c.g, c.b);
            gr.setColor(col);
            gr.fillOval(c.x, c.y, 10, 10);
            repaint();
            if(state != null) {
                synchronized(state) {
                    state.put(new Point(c.x, c.y), col);
                }
            }
        }



        public void clear() {
            if(gr == null) return;
            gr.clearRect(0, 0, getSize().width, getSize().height);
            repaint();
            if(state != null) {
                synchronized(state) {
                    state.clear();
                }
            }
        }





        /** Draw the entire panel from the state */
        public void drawState() {
            // clear();
            Map.Entry entry;
            Point pt;
            Color col;
            synchronized(state) {
                for(Iterator it=state.entrySet().iterator(); it.hasNext();) {
                    entry=(Map.Entry)it.next();
                    pt=(Point)entry.getKey();
                    col=(Color)entry.getValue();
                    gr.setColor(col);
                    gr.fillOval(pt.x, pt.y, 10, 10);

                }
            }
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

