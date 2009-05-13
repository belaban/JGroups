// $Id: DrawApplet.java,v 1.6 2009/05/13 13:07:13 belaban Exp $

package org.jgroups.demos.applets;

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
import java.util.Random;
import java.util.Vector;
import org.jgroups.*;
import org.jgroups.logging.Log;
import org.jgroups.logging.LogFactory;




public class DrawApplet extends Applet implements Runnable, MouseMotionListener, ActionListener {
    private Graphics               graphics=null;
    private Panel                  panel=null, sub_panel=null;
    private final ByteArrayOutputStream  out=new ByteArrayOutputStream();
    private DataOutputStream       outstream;
    private DataInputStream        instream;
    private final Random                 random=new Random(System.currentTimeMillis());
    private Button                 clear_button, leave_button;
    private Label                  mbr_label;
    private final Font             default_font=new Font("Helvetica",Font.PLAIN,12);
    private final String                 groupname="DrawGroup";
    private Channel                channel=null;
    private Thread                 receiver=null;
    private int                    member_size=1;
    private int                    red=0, green=0, blue=0;
    private Color                  default_color=null;

    private final ChannelFactory   factory=new JChannelFactory();
    private String                 props="TUNNEL(router_host=janet;router_port=12002):" + 
	                                 "PING(gossip_host=janet;gossip_port=12002):" +
	                                 "FD:STABLE:NAKACK:UNICAST:FRAG:FLUSH:GMS:VIEW_ENFORCER:QUEUE";

    private final Vector           members=new Vector();
    private boolean                fl=true;
    Log                            log=LogFactory.getLog(getClass());





    public void init() {
	System.out.println("INIT");
	setLayout(new BorderLayout());

	String tmp_props=getParameter("properties");
	if(tmp_props != null) {
	    System.out.println("Setting parameters " + tmp_props);
	    props=tmp_props;
	}


	try {
	    channel=factory.createChannel(props);
	    showStatus("Connecting to group " + groupname);
	    channel.connect(groupname);
	}
	catch(Exception e) {
	    log.error(e.toString());
	}
	receiver=new Thread(this, "DrawThread");
	receiver.start();
	go();
    }


    
    public void start() {
	System.out.println("------- START");	
    }


    


    public void destroy() {
	System.out.println("------- DESTROY");

	if(receiver != null && receiver.isAlive()) {
	    fl=false;
	    receiver.interrupt();
	    try {receiver.join(1000);} catch(Exception ex) {}
	}
	receiver=null;
	showStatus("Disconnecting from " + groupname);
	channel.disconnect();
	showStatus("Disconnected");
    }


    public void paint(Graphics g) {	
	Rectangle bounds=panel.getBounds();
	Color     old=graphics.getColor();

	if(bounds == null || graphics == null)
	    return;

	graphics.setColor(Color.black);
	graphics.drawRect(0, 0, bounds.width-1, bounds.height-1);
	graphics.setColor(old);
    }


    private void selectColor() {
	red=(Math.abs(random.nextInt()) % 255);
	green=(Math.abs(random.nextInt()) % 255);
	blue=(Math.abs(random.nextInt()) % 255);
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
	    return;
	}
    }


    public void run() {
	Object tmp;
	Message msg=null;
 	int my_x=10, my_y=10, r=0, g=0, b=0;
		

	while(fl) {
	    my_x=10; 
	    my_y=10;
	    try {
		tmp=channel.receive(0);
		if(tmp instanceof View) {
		    viewAccepted((View)tmp);
		    continue;
		}
		if(!(tmp instanceof Message))
		    continue;
		msg=(Message)tmp;

		if(msg == null || msg.getLength() == 0) {
		    log.error("DrawApplet.run(): msg or msg.buffer is null !");
		    continue;
		}

		instream=new DataInputStream(new ByteArrayInputStream(msg.getRawBuffer(), msg.getOffset(), msg.getLength()));
		r=instream.readInt();   // red
		if(r == -13) {
		    clearPanel();
		    continue;
		}
		g=instream.readInt();   // green
		b=instream.readInt();   // blue
		my_x=instream.readInt();
		my_y=instream.readInt();
	    }
	    catch(ChannelNotConnectedException conn) {
		break;
	    }
	    catch(Exception e) {
		log.error(e.toString());
	    }
	    if(graphics != null) {
		graphics.setColor(new Color(r, g, b));
		graphics.fillOval(my_x, my_y, 10, 10);
		graphics.setColor(default_color);
	    }
	}
    }


    /* --------------- Callbacks --------------- */


    public void mouseMoved(MouseEvent e) {}

    public void mouseDragged(MouseEvent e) {
	int          tmp[]=new int[1], x, y; 

	tmp[0]=0;
	x=e.getX();
	y=e.getY();

	graphics.fillOval(x, y, 10, 10);

	try {
	    out.reset();
	    outstream=new DataOutputStream(out);
	    outstream.writeInt(red);
	    outstream.writeInt(green);
	    outstream.writeInt(blue);
	    outstream.writeInt(x);
	    outstream.writeInt(y);
	    channel.send(new Message(null, null, out.toByteArray()));
	    out.reset();	
	}
	catch(Exception ex) {
	    log.error(ex.toString());
	}
    }


    public void clearPanel() {
	Rectangle  bounds=null;
	if(panel == null || graphics == null)
	    return;

	bounds=panel.getBounds();
	graphics.clearRect(1, 1, bounds.width-2, bounds.height-2);
	

    }


    public void sendClearPanelMsg() {
	int      tmp[]=new int[1]; tmp[0]=0;

	clearPanel();

	try {
	    out.reset();
	    outstream=new DataOutputStream(out);
	    outstream.writeInt(-13);
	    channel.send(new Message(null, null, out.toByteArray()));
	    outstream.flush();
	}
	catch(Exception ex) {
	    log.error(ex.toString());
	}
    }




    public void actionPerformed(ActionEvent e) {
	String     command=e.getActionCommand();	
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


    public void viewAccepted(View v) {
	Vector mbrs=v.getMembers();
	if(v != null) {
	    System.out.println("View accepted: " +v);
	    member_size=v.size();

	    if(mbr_label != null)
		mbr_label.setText(member_size + " mbr(s)");

	    members.removeAllElements();
	    for(int i=0; i < mbrs.size(); i++)
		members.addElement(mbrs.elementAt(i));
	}
    }





}

