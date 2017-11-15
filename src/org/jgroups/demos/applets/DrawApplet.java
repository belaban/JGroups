package org.jgroups.demos.applets;

import org.jgroups.*;
import org.jgroups.logging.Log;
import org.jgroups.logging.LogFactory;

import java.applet.Applet;
import java.awt.*;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.awt.event.MouseEvent;
import java.awt.event.MouseMotionListener;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;


public class DrawApplet extends Applet implements MouseMotionListener, ActionListener {
    private Graphics graphics=null;
    private Panel panel=null, sub_panel=null;
    private final ByteArrayOutputStream out=new ByteArrayOutputStream();
    private DataOutputStream outstream;
    private DataInputStream instream;
    private final Random random=new Random(System.currentTimeMillis());
    private Button clear_button, leave_button;
    private Label mbr_label;
    private final Font default_font=new Font("Helvetica", Font.PLAIN, 12);
    private static final String groupname="DrawGroup";
    private JChannel channel=null;
    private int member_size=1;
    private int red=0, green=0, blue=0;
    private Color default_color=null;

    private String props="tunnel.xml";

    private final List<Address> members=new ArrayList<>();
    private boolean fl=true;
    Log log=LogFactory.getLog(getClass());


    public void init() {
        System.out.println("INIT");
        setLayout(new BorderLayout());

        String tmp_props=getParameter("properties");
        if(tmp_props != null) {
            System.out.println("Setting parameters " + tmp_props);
            props=tmp_props;
        }


        try {
            channel=new JChannel(props);
            channel.setReceiver(new ReceiverAdapter() {
                public void viewAccepted(View v) {
                    List<Address> mbrs=v.getMembers();
                    System.out.println("View accepted: " + v);
                    member_size=v.size();

                    if(mbr_label != null)
                        mbr_label.setText(member_size + " mbr(s)");

                    members.clear();
                    members.addAll(mbrs);
                }

                public void receive(Message msg) {
                    if(msg == null || msg.getLength() == 0) {
                        log.error("DrawApplet.run(): msg or msg.buffer is null !");
                        return;
                    }

                    instream=new DataInputStream(new ByteArrayInputStream(msg.getArray(), msg.getOffset(), msg.getLength()));
                    int r=0;
                    try {
                        r=instream.readInt();
                        if(r == -13) {
                            clearPanel();
                            return;
                        }
                        int g=instream.readInt();
                        int b=instream.readInt();
                        int my_x=instream.readInt();
                        int my_y=instream.readInt();
                        if(graphics != null) {
                            graphics.setColor(new Color(r, g, b));
                            graphics.fillOval(my_x, my_y, 10, 10);
                            graphics.setColor(default_color);
                        }
                    }
                    catch(Exception ex) {
                        ex.printStackTrace();
                    }
                }
            });
            showStatus("Connecting to group " + groupname);
            channel.connect(groupname);
        }
        catch(Exception e) {
            e.printStackTrace();
        }
        go();
    }


    public void start() {
        System.out.println("------- START");
    }


    public void destroy() {
        System.out.println("------- DESTROY");
        showStatus("Disconnecting from " + groupname);
        channel.close();
        showStatus("Disconnected");
    }


    public void paint(Graphics g) {
        Rectangle bounds=panel.getBounds();

        if(bounds == null || graphics == null)
            return;

        Color old=graphics.getColor();
        graphics.setColor(Color.black);
        graphics.drawRect(0, 0, bounds.width - 1, bounds.height - 1);
        graphics.setColor(old);
    }


    private void selectColor() {
        red=Math.abs(random.nextInt() % 255);
        green=Math.abs(random.nextInt() % 255);
        blue=Math.abs(random.nextInt() % 255);
        default_color=new Color(red, green, blue);
    }


    public void go() {
        try {
            panel=new Panel();
            sub_panel=new Panel();
            resize(200, 200);
            add("Center", panel);
            clear_button=new Button("Clear");
            clear_button.setFont(default_font);
            clear_button.addActionListener(this);
            leave_button=new Button("Exit");
            leave_button.setFont(default_font);
            leave_button.addActionListener(this);
            mbr_label=new Label("0 mbr(s)");
            mbr_label.setFont(default_font);
            sub_panel.add("South", clear_button);
            sub_panel.add("South", leave_button);
            sub_panel.add("South", mbr_label);
            add("South", sub_panel);
            panel.addMouseMotionListener(this);
            setVisible(true);
            mbr_label.setText(member_size + " mbrs");
            graphics=panel.getGraphics();
            selectColor();
            graphics.setColor(default_color);
            panel.setBackground(Color.white);
            clear_button.setForeground(Color.blue);
            leave_button.setForeground(Color.blue);
        }
        catch(Exception e) {
            log.error(e.toString());
        }
    }


    /* --------------- Callbacks --------------- */


    public void mouseMoved(MouseEvent e) {
    }

    public void mouseDragged(MouseEvent e) {
        int x=e.getX(), y=e.getY();
        graphics.fillOval(x, y, 10, 10);

        try {
            out.reset();
            outstream=new DataOutputStream(out);
            outstream.writeInt(red);
            outstream.writeInt(green);
            outstream.writeInt(blue);
            outstream.writeInt(x);
            outstream.writeInt(y);
            channel.send(new BytesMessage(null, out.toByteArray()));
            out.reset();
        }
        catch(Exception ex) {
            log.error(ex.toString());
        }
    }


    public void clearPanel() {
        Rectangle bounds=null;
        if(panel == null || graphics == null)
            return;

        bounds=panel.getBounds();
        graphics.clearRect(1, 1, bounds.width - 2, bounds.height - 2);


    }


    public void sendClearPanelMsg() {
        clearPanel();
        try {
            out.reset();
            outstream=new DataOutputStream(out);
            outstream.writeInt(-13);
            channel.send(new BytesMessage(null, out.toByteArray()));
            outstream.flush();
        }
        catch(Exception ex) {
            log.error(ex.toString());
        }
    }


    public void actionPerformed(ActionEvent e) {
        String command=e.getActionCommand();
        if(command == "Clear") {
            System.out.println("Members are " + members);
            sendClearPanelMsg();
        }
        else if(command == "Exit") {
            try {
                destroy();
                setVisible(false);
            }
            catch(Exception ex) {
                log.error(ex.toString());
            }

        }
        else
            System.out.println("Unknown action");
    }


}

