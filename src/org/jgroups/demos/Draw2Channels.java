// $Id: Draw2Channels.java,v 1.14 2009/06/17 16:20:13 belaban Exp $


package org.jgroups.demos;


import org.jgroups.*;
import org.jgroups.Event;
import org.jgroups.util.Util;

import javax.swing.*;
import java.awt.*;
import java.awt.event.*;
import java.util.Random;


/**
 * Same as Draw but using 2 channels: one for view changes (control channel) and the other one for drawing
 * (data channel). Ported to use Swing Nov 1 2001, not tested.
 * @author Bela Ban, Nov 1 2001
 */
public class Draw2Channels implements ActionListener {
    private final String control_groupname="Draw2ChannelsGroup-Control";
    private final String data_groupname="Draw2ChannelsGroup-Data";
    private Channel control_channel=null;
    private Channel data_channel=null;
    String control_props=null, data_props=null;
    private Receiver control_receiver=null;
    private Receiver data_receiver=null;
    private int member_size=1;
    final boolean first=true;
    private JFrame mainFrame=null;
    private JPanel sub_panel=null;
    private DrawPanel panel=null;
    private JButton clear_button, leave_button;
    private final Random random=new Random(System.currentTimeMillis());
    private final Font default_font=new Font("Helvetica", Font.PLAIN, 12);
    private final Color draw_color=selectColor();
    private final Color background_color=Color.white;
    boolean no_channel=false;


    public Draw2Channels(String control_props, String data_props, boolean no_channel) throws Exception {

        this.control_props=control_props;
        this.data_props=data_props;
        this.no_channel=no_channel;
    }


    public static void main(String[] args) {
        Draw2Channels draw=null;
        String control_props=null, data_props=null;
        boolean no_channel=false;

        for(int i=0; i < args.length; i++) {
            if("-help".equals(args[i])) {
                help();
                return;
            }
            if("-no_channel".equals(args[i])) {
                no_channel=true;
                continue;
            }
            help();
            return;
        }


        control_props="udp.xml";


        data_props="UDP(mcast_addr=224.10.10.200;mcast_port=5679)";


        try {

            draw=new Draw2Channels(control_props, data_props, no_channel);
            draw.go();
        }
        catch(Exception e) {
            System.err.println(e);
            System.exit(0);
        }
    }


    static void help() {
        System.out.println("Draw2Channels [-help] [-no_channel]");
    }


    private Color selectColor() {
        int red=(Math.abs(random.nextInt()) % 255);
        int green=(Math.abs(random.nextInt()) % 255);
        int blue=(Math.abs(random.nextInt()) % 255);
        return new Color(red, green, blue);
    }


    public void go() {
        try {
            if(!no_channel) {
                control_receiver=new ControlReceiver();
                data_receiver=new DataReceiver();
                System.out.println("Creating control channel");
                control_channel=new JChannel(control_props);
                control_channel.setReceiver(control_receiver);
                System.out.println("Creating data channel");
                data_channel=new JChannel(data_props);
                data_channel.setReceiver(data_receiver);
                // data_channel.SetOpt(Channel.VIEW, Boolean.FALSE);
                System.out.println("Connecting data channel");
                data_channel.connect(data_groupname);
                System.out.println("Connecting control channel");
                control_channel.connect(control_groupname);
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
            mainFrame.setVisible(true);
            mainFrame.setBackground(background_color);
            clear_button.setForeground(Color.blue);
            leave_button.setForeground(Color.blue);
            setTitle();
            mainFrame.pack();
            mainFrame.setLocation(15, 25);
            mainFrame.setVisible(true);
        }
        catch(Exception e) {
            System.err.println(e);
        }
    }


    void setTitle() {
        String title="";
        if(no_channel) {
            mainFrame.setTitle(" Draw Demo ");
            return;
        }
        if(control_channel.getAddress() != null)
            title+=control_channel.getAddress();
        title+=" (" + member_size + ") mbrs";
        mainFrame.setTitle(title);
    }



    public void clearPanel() {
        if(panel != null)
            panel.clear();
    }

    public void sendClearPanelMsg() {
        int tmp[]=new int[1];
        tmp[0]=0;
        DrawCommand comm=new DrawCommand(DrawCommand.CLEAR);

        try {
            byte[] buf=Util.streamableToByteBuffer(comm);
            data_channel.send(new Message(null, null, buf));
        }
        catch(Exception ex) {
            System.err.println(ex);
        }
    }


    public void actionPerformed(ActionEvent e) {
        String command=e.getActionCommand();
        if("Clear".equals(command)) {
            if(no_channel) {
                clearPanel();
                return;
            }
            sendClearPanelMsg();
        }
        else if("Leave & Exit".equals(command)) {
            if(!no_channel) {
                try {
                    control_channel.close();
                }
                catch(Exception ex) {
                    System.err.println(ex);
                }
                try {
                    data_channel.close();
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


    private class DrawPanel extends JPanel implements MouseMotionListener {
        final Dimension preferred_size=new Dimension(235, 170);
        Image img=null; // for drawing pixels
        Dimension d, imgsize;
        Graphics gr=null;


        public DrawPanel() {
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
                gr=img.getGraphics();
                imgsize=d;
            }
        }

        /* ---------------------- MouseMotionListener interface------------------------- */

        public void mouseMoved(MouseEvent e) {
        }

        public void mouseDragged(MouseEvent e) {
            int x=e.getX(), y=e.getY();
            DrawCommand comm=new DrawCommand(DrawCommand.DRAW, x, y,
                                             draw_color.getRed(), draw_color.getGreen(), draw_color.getBlue());

            if(no_channel) {
                drawPoint(comm);
                return;
            }

            try {
                byte[] buf=Util.streamableToByteBuffer(comm);
                data_channel.send(new Message(null, null, buf));
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
            if(c == null) return;
            gr.setColor(new Color(c.r, c.g, c.b));
            gr.fillOval(c.x, c.y, 10, 10);
            repaint();
        }


        public void clear() {
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


    class ControlReceiver extends ExtendedReceiverAdapter {
        public void viewAccepted(View v) {
            member_size=v.size();
            if(mainFrame != null)
                mainFrame.setTitle(member_size + " mbrs");
            data_channel.down(new Event(Event.VIEW_CHANGE, v));
        }
    }


    class DataReceiver extends ExtendedReceiverAdapter implements ChannelListener {

        public void receive(Message msg) {
            byte[] buf=msg.getRawBuffer();
            DrawCommand comm=null;
            try {
                comm=(DrawCommand)Util.streamableFromByteBuffer(DrawCommand.class, buf, msg.getOffset(), msg.getLength());
                switch(comm.mode) {
                    case DrawCommand.DRAW:
                        if(panel != null)
                            panel.drawPoint(comm);
                        break;
                    case DrawCommand.CLEAR:
                        clearPanel();
                        break;
                    default:
                        System.err.println("***** Draw2Channels.run(): received invalid draw command " + comm.mode);
                        break;
                }
            }
            catch(Exception ex) {
                ex.printStackTrace();
            }
        }

        public void viewAccepted(View v) {
            System.out.println("** View=" + v);
            member_size=v.size();
            if(mainFrame != null)
                setTitle();
        }


        public void channelConnected(Channel channel) {
        }

        public void channelDisconnected(Channel channel) {
        }

        public void channelClosed(Channel channel) {
        }

        public void channelShunned() {
        }

        public void channelReconnected(Address addr) {
        }
    }


}



