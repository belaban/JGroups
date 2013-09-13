

package org.jgroups.demos;


import org.jgroups.*;
import org.jgroups.jmx.JmxConfigurator;
import org.jgroups.util.Util;

import javax.management.MBeanServer;
import javax.swing.*;
import java.awt.*;
import java.awt.event.*;
import java.io.*;
import java.util.*;
import java.util.List;


/**
 * Shared whiteboard, each new instance joins the same group. Each instance chooses a random color,
 * mouse moves are broadcast to all group members, which then apply them to their canvas<p>
 * @author Bela Ban, Oct 17 2001
 */
public class Draw extends ReceiverAdapter implements ActionListener, ChannelListener {
    protected String               cluster_name="draw-cluster";
    private JChannel               channel=null;
    private int                    member_size=1;
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
    private boolean                use_unicasts=false;
    protected boolean              send_own_state_on_merge=true;
    private final                  List<Address> members=new ArrayList<Address>();


    public Draw(String props, boolean no_channel, boolean jmx, boolean use_state, long state_timeout,
                boolean use_unicasts, String name, boolean send_own_state_on_merge) throws Exception {
        this.no_channel=no_channel;
        this.jmx=jmx;
        this.use_state=use_state;
        this.state_timeout=state_timeout;
        this.use_unicasts=use_unicasts;
        if(no_channel)
            return;

        channel=new JChannel(props);
        if(name != null)
            channel.setName(name);
        channel.setReceiver(this);
        channel.addChannelListener(this);
        this.send_own_state_on_merge=send_own_state_on_merge;
    }

    public Draw(JChannel channel) throws Exception {
        this.channel=channel;
        channel.setReceiver(this);
        channel.addChannelListener(this);
    }


    public Draw(JChannel channel, boolean use_state, long state_timeout) throws Exception {
        this.channel=channel;
        channel.setReceiver(this);
        channel.addChannelListener(this);
        this.use_state=use_state;
        this.state_timeout=state_timeout;
    }


    public String getClusterName() {
        return cluster_name;
    }

    public void setClusterName(String clustername) {
        if(clustername != null)
            this.cluster_name=clustername;
    }


    public static void main(String[] args) {
        Draw             draw=null;
        String           props=null;
        boolean          no_channel=false;
        boolean          jmx=true;
        boolean          use_state=false;
        String           group_name=null;
        long             state_timeout=5000;
        boolean          use_unicasts=false;
        String           name=null;
        boolean          send_own_state_on_merge=true;

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
                jmx=Boolean.parseBoolean(args[++i]);
                continue;
            }
            if("-clustername".equals(args[i])) {
                group_name=args[++i];
                continue;
            }
            if("-state".equals(args[i])) {
                use_state=true;
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
            if("-use_unicasts".equals(args[i])) {
                use_unicasts=true;
                continue;
            }
            if("-name".equals(args[i])) {
                name=args[++i];
                continue;
            }
            if("-send_own_state_on_merge".equals(args[i])) {
                send_own_state_on_merge=Boolean.getBoolean(args[++i]);
                continue;
            }

            help();
            return;
        }

        try {
            draw=new Draw(props, no_channel, jmx, use_state, state_timeout, use_unicasts, name, send_own_state_on_merge);
            if(group_name != null)
                draw.setClusterName(group_name);
            draw.go();
        }
        catch(Throwable e) {
            e.printStackTrace(System.err);
            System.exit(0);
        }
    }


    static void help() {
        System.out.println("\nDraw [-help] [-no_channel] [-props <protocol stack definition>]" +
                " [-clustername <name>] [-state] [-timeout <state timeout>] [-use_unicasts] " +
                "[-bind_addr <addr>] [-jmx <true | false>] [-name <logical name>] [-send_own_state_on_merge true|false]");
        System.out.println("-no_channel: doesn't use JGroups at all, any drawing will be relected on the " +
                "whiteboard directly");
        System.out.println("-props: argument can be an old-style protocol stack specification, or it can be " +
                "a URL. In the latter case, the protocol specification will be read from the URL\n");
    }


    private Color selectColor() {
        int red=Math.abs(random.nextInt()) % 255;
        int green=Math.abs(random.nextInt()) % 255;
        int blue=Math.abs(random.nextInt()) % 255;
        return new Color(red, green, blue);
    }


    private void sendToAll(byte[] buf) throws Exception {
        for(Address mbr: members)
            channel.send(new Message(mbr, buf));
    }


    public void go() throws Exception {
        if(!no_channel && !use_state)
            channel.connect(cluster_name);
        mainFrame=new JFrame();
        mainFrame.setDefaultCloseOperation(WindowConstants.EXIT_ON_CLOSE);
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
            channel.connect(cluster_name, null, state_timeout);
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
            if(channel.getAddress() != null)
                tmp+=channel.getAddress();
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
            System.err.println("[" + channel.getAddress() + "] received null buffer from " + msg.getSrc() +
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
        member_size=v.size();
        if(mainFrame != null)
            setTitle();
        members.clear();
        members.addAll(v.getMembers());

        if(v instanceof MergeView) {
            System.out.println("** " + v);

            // This is an example of a simple merge function, which fetches the state from the coordinator
            // on a merge and overwrites all of its own state
            if(use_state && !members.isEmpty()) {
                Address coord=members.get(0);
                Address local_addr=channel.getAddress();
                if(local_addr != null && !local_addr.equals(coord)) {
                    try {

                        // make a copy of our state first
                        Map<Point,Color> copy=null;
                        if(send_own_state_on_merge) {
                            synchronized(panel.state) {
                                copy=new LinkedHashMap<Point,Color>(panel.state);
                            }
                        }
                        System.out.println("fetching state from " + coord);
                        channel.getState(coord, 5000);
                        if(copy != null)
                            sendOwnState(copy); // multicast my own state so everybody else has it too
                    }
                    catch(Exception e) {
                        e.printStackTrace();
                    }
                }
            }
        }
        else
            System.out.println("** View=" + v);
    }


    public void getState(OutputStream ostream) throws Exception {
        panel.writeState(ostream);
    }

    public void setState(InputStream istream) throws Exception {
        panel.readState(istream);
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
            if(use_unicasts)
                sendToAll(buf);
            else
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

    protected void sendOwnState(final Map<Point,Color> copy) {
        if(copy == null)
            return;
        for(Point point: copy.keySet()) {
            // we don't need the color: it is our draw_color anyway
            DrawCommand comm=new DrawCommand(DrawCommand.DRAW, point.x, point.y, draw_color.getRGB());
            try {
                byte[] buf=Util.streamableToByteBuffer(comm);
                if(use_unicasts)
                    sendToAll(buf);
                else
                    channel.send(new Message(null, buf));
            }
            catch(Exception ex) {
                System.err.println(ex);
            }
        }
    }


    /* ------------------------------ ChannelListener interface -------------------------- */

    public void channelConnected(Channel channel) {
        if(jmx) {
            Util.registerChannel((JChannel)channel, "jgroups");
        }
    }

    public void channelDisconnected(Channel channel) {
        if(jmx) {
            MBeanServer server=Util.getMBeanServer();
            if(server != null) {
                try {
                    JmxConfigurator.unregisterChannel((JChannel)channel,server,cluster_name);
                }
                catch(Exception e) {
                    e.printStackTrace();
                }
            }
        }
    }

    public void channelClosed(Channel channel) {

    }


    /* --------------------------- End of ChannelListener interface ---------------------- */



    protected class DrawPanel extends JPanel implements MouseMotionListener {
        protected final Dimension         preferred_size=new Dimension(235, 170);
        protected Image                   img; // for drawing pixels
        protected Dimension               d, imgsize;
        protected Graphics                gr;
        protected final Map<Point,Color>  state;


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


        public void writeState(OutputStream outstream) throws IOException {
            if(state == null)
                return;
            synchronized(state) {
                DataOutputStream dos=new DataOutputStream(new BufferedOutputStream(outstream));
                // DataOutputStream dos=new DataOutputStream(outstream);
                dos.writeInt(state.size());
                for(Map.Entry<Point,Color> entry: state.entrySet()) {
                    Point point=entry.getKey();
                    Color col=entry.getValue();
                    dos.writeInt(point.x);
                    dos.writeInt(point.y);
                    dos.writeInt(col.getRGB());
                }
                dos.flush();
                System.out.println("wrote " + state.size() + " elements");
            }
        }


        public void readState(InputStream instream) throws IOException {
            DataInputStream in=new DataInputStream(new BufferedInputStream(instream));
            Map<Point,Color> new_state=new LinkedHashMap<Point,Color>();
            int num=in.readInt();
            for(int i=0; i < num; i++) {
                Point point=new Point(in.readInt(), in.readInt());
                Color col=new Color(in.readInt());
                new_state.put(point, col);
            }

            synchronized(state) {
                state.clear();
                state.putAll(new_state);
                System.out.println("read " + state.size() + " elements");
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
            DrawCommand         comm=new DrawCommand(DrawCommand.DRAW, x, y, draw_color.getRGB());

            if(no_channel) {
                drawPoint(comm);
                return;
            }

            try {
                byte[] buf=Util.streamableToByteBuffer(comm);
                if(use_unicasts)
                    sendToAll(buf);
                else
                    channel.send(new Message(null, null, buf));
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
            Color col=new Color(c.rgb);
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

