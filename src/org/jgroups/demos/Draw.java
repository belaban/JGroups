// $Id: Draw.java,v 1.2 2003/09/24 23:20:47 belaban Exp $


package org.jgroups.demos;


import org.jgroups.*;
import org.jgroups.debug.Debugger;
import org.jgroups.log.Trace;
import org.jgroups.util.Util;

import javax.swing.*;
import java.awt.*;
import java.awt.event.*;
import java.io.ByteArrayOutputStream;
import java.io.ObjectOutputStream;
import java.util.Random;




/**
 * Shared whiteboard, each new instance joins the same group. Each instance chooses a random color,
 * mouse moves are broadcast to all group members, which then apply them to their canvas<p>
 * @author Bela Ban, Oct 17 2001
 */
public class Draw implements ActionListener, ChannelListener {
    private ByteArrayOutputStream  out=new ByteArrayOutputStream();
    private String                 groupname="DrawGroupDemo";
    private JChannel               channel=null;
    private int                    member_size=1;
    Debugger                       debugger=null;
    boolean                        first=true, cummulative=true;
    private JFrame                 mainFrame=null;
    private JPanel                 sub_panel=null;
    private DrawPanel              panel=null;
    private JButton                clear_button, leave_button;
    private Random                 random=new Random(System.currentTimeMillis());
    private final Font             default_font=new Font("Helvetica",Font.PLAIN,12);
    private Color                  draw_color=selectColor(), background_color=Color.white;
    boolean                        no_channel=false;





    public Draw(String props, boolean debug, boolean no_channel) throws Exception {
        Trace.init();
        this.no_channel=no_channel;
        if(no_channel)
            return;

        channel=new JChannel(props);
        if(debug) {
            debugger=new Debugger(channel, cummulative);
            debugger.start();
        }
        channel.setOpt(Channel.AUTO_RECONNECT, Boolean.TRUE);
        channel.setChannelListener(this);
    }






   public static void main(String[] args) {
        Draw             draw=null;
        String           props=null;
        boolean          debug=false;
        boolean          no_channel=false;

        for(int i=0; i < args.length; i++) {
            if(args[i].equals("-help")) {
                help();
                return;
            }
            if(args[i].equals("-debug")) {
                debug=true;
                continue;
            }
            if(args[i].equals("-props")) {
                props=args[++i];
                continue;
            }
            if(args[i].equals("-no_channel")) {
                no_channel=true;
                continue;
            }
            help();
            return;
        }

        if(props == null) {
            props="UDP(mcast_addr=228.8.8.8;mcast_port=45566;ip_ttl=32;" +
                    "mcast_send_buf_size=64000;mcast_recv_buf_size=64000):" +
                    //"PIGGYBACK(max_wait_time=100;max_size=32000):" +
                    "PING(timeout=2000;num_initial_members=3):" +
                    "MERGE2(min_interval=5000;max_interval=10000):" +
                    "FD_SOCK:" +
                    "VERIFY_SUSPECT(timeout=1500):" +
                    "pbcast.NAKACK(max_xmit_size=8096;gc_lag=50;retransmit_timeout=600,1200,2400,4800):" +
                    "UNICAST(timeout=600,1200,2400,4800):" +
                    "pbcast.STABLE(desired_avg_gossip=20000):" +
                    "FRAG(frag_size=8096;down_thread=false;up_thread=false):" +
                    // "CAUSAL:" +
                    "pbcast.GMS(join_timeout=5000;join_retry_timeout=2000;" +
                    "shun=false;print_local_addr=true)";
        }


        try {
            draw=new Draw(props, debug, no_channel);
            draw.go();
        }
        catch(Throwable e) {
            e.printStackTrace();
            System.exit(0);
        }
    }


    static void help() {
        System.out.println("\nDraw [-help] [-debug] [-no_channel] [-props <protocol stack definition>]");
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
        leave_button=new JButton("Leave & Exit");
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
        mainFrame.setVisible(true);
        if(!no_channel)
            mainLoop();
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
            tmp+=" (" + member_size + ") mbrs";
            mainFrame.setTitle(tmp);
        }
    }

    void setTitle() {
        setTitle(null);
    }




    public void mainLoop() {
        Object       tmp;
        Message      msg=null;
        DrawCommand  comm;
        boolean      fl=true;

        while(fl) {
            try {
                tmp=channel.receive(0);
                if(tmp == null) continue;

                if(tmp instanceof View) {
                    View v=(View)tmp;
                    System.out.println("** View=" + v);
                    member_size=v.size();
                    if(mainFrame != null)
                        setTitle();
                    continue;
                }

                if(tmp instanceof ExitEvent) {
                    System.out.println("-- Draw.main(): received EXIT, waiting for ChannelReconnected callback");
                    setTitle(" Draw Demo - shunned ");
                    break;
                }

                if(!(tmp instanceof Message))
                    continue;

                msg=(Message)tmp;
                comm=null;

                Object obj=Util.objectFromByteBuffer(msg.getBuffer());
                if(obj instanceof DrawCommand)
                    comm=(DrawCommand)obj;
                else if(obj instanceof Message) {
                    System.out.println("*** Draw.run(): message is " + Util.printMessage((Message)obj));
                    Util.dumpStack(false);
                    continue;
                }
                else {
                    if(obj != null)
                        System.out.println("*** Draw.run(): obj is " + obj.getClass() +
                                           ", hdrs are" + msg.printObjectHeaders());
                    else
                        System.out.println("*** Draw.run(): hdrs are" + msg.printObjectHeaders());
                    Util.dumpStack(false);
                    continue;
                }

                switch(comm.mode) {
                case DrawCommand.DRAW:
                    if(panel != null)
                        panel.drawPoint(comm);
                    break;
                case DrawCommand.CLEAR:
                    clearPanel();
                    continue;
                default:
                    System.err.println("***** Draw.run(): received invalid draw command " + comm.mode);
                    break;
                }

            }
            catch(ChannelNotConnectedException not) {
                System.err.println("Draw: " + not);
                break;
            }
            catch(ChannelClosedException closed) {
                break;
            }
            catch(Exception e) {
                System.err.println(e);
                continue;
            }
        }
    }





    /* --------------- Callbacks --------------- */



    public void clearPanel() {
        if(panel != null)
            panel.clear();
    }

    public void sendClearPanelMsg() {
        int                  tmp[]=new int[1]; tmp[0]=0;
        DrawCommand          comm=new DrawCommand(DrawCommand.CLEAR);
        ObjectOutputStream   os;

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


    public void actionPerformed(ActionEvent e) {
        String     command=e.getActionCommand();
        if(command.equals("Clear")) {
            if(no_channel) {
                clearPanel();
                return;
            }
            sendClearPanelMsg();
        }
        else if(command.equals("Leave & Exit")) {
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
            System.exit(0);
        }
        else
            System.out.println("Unknown action");
    }


    /* ------------------------------ ChannelListener interface -------------------------- */

    public void channelConnected(Channel channel) {

    }

    public void channelDisconnected(Channel channel) {

    }

    public void channelClosed(Channel channel) {

    }

    public void channelShunned() {

    }

    public void channelReconnected(Address addr) {
        setTitle();
        new Thread() {
            public void run() {
                mainLoop();
            }
        }.start();
    }


    /* --------------------------- End of ChannelListener interface ---------------------- */



    private class DrawPanel extends JPanel implements MouseMotionListener {
        Dimension        preferred_size=new Dimension(235, 170);
        Image            img=null; // for drawing pixels
        Dimension        d, imgsize;
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



        void createOffscreenImage() {
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
            ObjectOutputStream  os;
            int                 x=e.getX(), y=e.getY();
            DrawCommand         comm=new DrawCommand(DrawCommand.DRAW, x, y,
                                                     draw_color.getRed(), draw_color.getGreen(), draw_color.getBlue());

            if(no_channel) {
                drawPoint(comm);
                return;
            }

            try {
                out.reset();
                os=new ObjectOutputStream(out);
                os.writeObject(comm);
                os.flush();
                channel.send(new Message(null, null, out.toByteArray()));
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

