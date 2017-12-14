

package org.jgroups.demos;


import org.jgroups.client.StompConnection;
import org.jgroups.util.Util;

import javax.swing.*;
import java.awt.*;
import java.awt.event.*;
import java.io.*;
import java.util.*;
import java.util.List;


/**
 * Simple STOMP demo client. Use -h and -p to connect to *any* JGroups server (has to have STOMP in the config)
 * <p>
 * @author Bela Ban, Oct 17 2001
 */
public class StompDraw implements StompConnection.Listener, ActionListener {
    private int                    num_servers=1;
    private int                    num_clients=0;
    private JFrame                 mainFrame=null;
    private JPanel                 sub_panel=null;
    private DrawPanel              panel=null;
    private JButton                clear_button, leave_button;
    private final Random           random=new Random(System.currentTimeMillis());
    private final Font             default_font=new Font("Helvetica",Font.PLAIN,12);
    private final Color            draw_color=selectColor();
    private static final Color     background_color=Color.white;
    private final                  List<String> servers=new ArrayList<>();
    private final Set<String>      clients=new HashSet<>();

    protected StompConnection      stomp_client;
    protected static final String  draw_dest="/topics/draw-demo";
    protected static final String  clients_dest="/topics/clients";


    public StompDraw(String host, String port) throws Exception {
        stomp_client=new StompConnection(host + ":" + port);
        stomp_client.addListener(this);
    }



   public static void main(String[] args) {
       StompDraw        draw=null;

       String host="localhost", port="8787";
       for(int i=0; i < args.length; i++) {
           if("-help".equals(args[i])) {
               help();
               return;
           }
           if("-h".equals(args[i])) {
               host=args[++i];
               continue;
           }
           if("-p".equals(args[i])) {
               port=args[++i];
               continue;
           }
           help();
           return;
       }

       try {
           draw=new StompDraw(host, port);
           draw.go();
       }
       catch(Throwable e) {
           System.err.println("fatal error: " + e.getLocalizedMessage() + ", cause: ");
           Throwable t=e.getCause();
           if(t != null)
               t.printStackTrace(System.err);
           System.exit(0);
       }
   }


    static void help() {
        System.out.println("\nDraw [-help] [-no_channel] [-h host] [-port port]");
    }


    private Color selectColor() {
        int red=Math.abs(random.nextInt() % 255);
        int green=Math.abs(random.nextInt() % 255);
        int blue=Math.abs(random.nextInt() % 255);
        return new Color(red, green, blue);
    }


    private void sendToAll(byte[] buf) throws Exception {
        if(buf != null)
            stomp_client.send(draw_dest, buf, 0, buf.length);
    }


    public void go() throws Exception {
        stomp_client.connect();
        stomp_client.subscribe(draw_dest);
        stomp_client.subscribe(clients_dest);

        mainFrame=new JFrame();
        mainFrame.setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
        panel=new DrawPanel(false);
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
        mainFrame.setVisible(true);
        setTitle();

        String session_id=stomp_client.getSessionId();
        if(session_id != null)
            stomp_client.send(clients_dest, null, 0, 0, "client-joined", session_id);
        Runtime.getRuntime().addShutdownHook(new Thread() {
            public void run() {
                StompDraw.this.stop();
            }
        });
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

    public void onMessage(Map<String, String> headers, byte[] buf, int offset, int length) {
        if(buf == null)
            return;
        String destination=headers.get("destination");
        if(Objects.equals(destination, clients_dest)) {
            String new_client=headers.get("client-joined");
            if(new_client != null) {
                synchronized(clients) {
                    if(clients.add(new_client)) {
                        num_clients=clients.size();
                        setTitle();
                    }
                }

                stomp_client.send(clients_dest, null, 0, 0, "clients", getAllClients());
            }

            String left_client=headers.get("client-left");
            if(left_client != null) {
                synchronized(clients) {
                    if(clients.remove(left_client)) {
                        num_clients=clients.size();
                        setTitle();
                    }
                }
            }

            String all_clients=headers.get("clients");
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

        try {
            DrawCommand comm=Util.streamableFromByteBuffer(DrawCommand::new, buf, offset, length);
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




    /* --------------- Callbacks --------------- */



    public void clearPanel() {
        if(panel != null)
            panel.clear();
    }

    public void sendClearPanelMsg() {
        DrawCommand comm=new DrawCommand(DrawCommand.CLEAR);

        try {
            byte[] buf=Util.streamableToByteBuffer(comm);
            sendToAll(buf);
        }
        catch(Exception ex) {
            System.err.println(ex);
        }
    }


    public void actionPerformed(ActionEvent e) {
        String     command=e.getActionCommand();
        if("Clear".equals(command)) {
            sendClearPanelMsg();
        }
        else if("Leave".equals(command)) {
            stop();
            mainFrame.setVisible(false);
            mainFrame.dispose();
        }
        else
            System.out.println("Unknown action");
    }


    public void stop() {
        if(!stomp_client.isConnected())
            return;
        String session_id=stomp_client.getSessionId();
        if(session_id != null) {
            stomp_client.send(clients_dest, null, 0, 0, "client-left", session_id);
        }
        stomp_client.disconnect();
    }




    private class DrawPanel extends JPanel implements MouseMotionListener {
        final Dimension         preferred_size=new Dimension(235, 170);
        Image                   img=null; // for drawing pixels
        Dimension               d, imgsize=null;
        Graphics                gr=null;
        final Map<Point,Color>  state;


        public DrawPanel(boolean use_state) {
            if(use_state)
                state=new LinkedHashMap<>();
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
                DataOutputStream dos = new DataOutputStream(outstream);
                dos.writeInt(state.size());
                Point point;
                Color col;
                for (Map.Entry<Point, Color> entry : state.entrySet()) {
                    point = entry.getKey();
                    col = entry.getValue();
                    dos.writeInt(point.x);
                    dos.writeInt(point.y);
                    dos.writeInt(col.getRGB());
                }
                dos.flush();
            }
        }


        public void readState(InputStream instream) throws IOException {
            DataInputStream in=new DataInputStream(instream);
            Map<Point,Color> new_state=new HashMap<>();
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
            DrawCommand         comm=new DrawCommand(DrawCommand.DRAW, x, y, draw_color.getRGB());

            try {
                byte[] buf=Util.streamableToByteBuffer(comm);
                sendToAll(buf);
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

