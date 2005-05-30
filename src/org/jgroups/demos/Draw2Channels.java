// $Id: Draw2Channels.java,v 1.8 2005/05/30 14:31:02 belaban Exp $


package org.jgroups.demos;


import org.jgroups.*;
import org.jgroups.Event;
import org.jgroups.util.Util;

import javax.swing.*;
import java.awt.*;
import java.awt.event.*;
import java.io.ByteArrayOutputStream;
import java.io.ObjectOutputStream;
import java.util.Random;




/**
 * Same as Draw but using 2 channels: one for view changes (control channel) and the other one for drawing
 * (data channel). Ported to use Swing Nov 1 2001, not tested.
 * @author Bela Ban, Nov 1 2001
 */
public class Draw2Channels implements ActionListener {
    private final ByteArrayOutputStream  out=new ByteArrayOutputStream();
    private final String                 control_groupname="Draw2ChannelsGroup-Control";
    private final String                 data_groupname="Draw2ChannelsGroup-Data";
    private Channel                control_channel=null;
    private Channel                data_channel=null;
    String                         control_props=null, data_props=null;
    private Thread                 control_receiver=null;
    private Thread                 data_receiver=null;
    private int                    member_size=1;
    final boolean                        first=true;
    final boolean cummulative=true;
    private JFrame                 mainFrame=null;
    private JPanel                 sub_panel=null;
    private DrawPanel              panel=null;
    private JButton                clear_button, leave_button;
    private final Random                 random=new Random(System.currentTimeMillis());
    private final Font             default_font=new Font("Helvetica",Font.PLAIN,12);
    private final Color                  draw_color=selectColor();
    private final Color background_color=Color.white;
    boolean                        no_channel=false;





    public Draw2Channels(String control_props, String data_props, boolean no_channel) throws Exception {

	this.control_props=control_props;
	this.data_props=data_props;
	this.no_channel=no_channel;
	if(no_channel)
	    return;
    }






   public static void main(String[] args) {
	Draw2Channels    draw=null;
	String           control_props=null, data_props=null;
	boolean          no_channel=false;

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



       control_props="UDP(mcast_addr=224.0.0.35;mcast_port=45566;ip_ttl=32;" +
               "mcast_send_buf_size=150000;mcast_recv_buf_size=80000):" +
               "PING(timeout=2000;num_initial_members=3):" +
               "MERGE2(min_interval=5000;max_interval=10000):" +
               "FD_SOCK:" +
               "VERIFY_SUSPECT(timeout=1500):" +
               "pbcast.NAKACK(gc_lag=50;retransmit_timeout=300,600,1200,2400,4800):" +
               "UNICAST(timeout=5000):" +
               "pbcast.STABLE(desired_avg_gossip=20000):" +
               "FRAG(frag_size=4096;down_thread=false;up_thread=false):" +
               "pbcast.GMS(join_timeout=5000;join_retry_timeout=2000;" +
               "shun=false;print_local_addr=true)";


	data_props="UDP(mcast_addr=224.10.10.200;mcast_port=5679)";

	/*
	data_props="UDP(mcast_addr=224.0.1.1;mcast_port=10000):" +
	    "PING:" +
	    "FD:" +
	    "pbcast.PBCAST(gossip_interval=5000;gc_lag=50):" +
	    "UNICAST:" +
	    "pbcast.GMS";
	*/





	/*
	props="TCP(start_port=7800):" +
	    "TCPPING(initial_hosts=dev1-150[7800];port_range=5;timeout=3000;" +
	    "num_initial_members=3;up_thread=true;down_thread=true):" +
	    "VERIFY_SUSPECT(timeout=1500;down_thread=false;up_thread=false):" +
	    "pbcast.STABLE(desired_avg_gossip=20000;down_thread=false;up_thread=false):" +
	    "pbcast.NAKACK(down_thread=true;up_thread=true;gc_lag=100;retransmit_timeout=3000):" +
	    "pbcast.GMS(join_timeout=5000;join_retry_timeout=2000;shun=false;" +
	    "print_local_addr=false;down_thread=true;up_thread=true)";
	*/



	
	/*
	// Gianluca's stack properties
	props="UDP:" +
	    "PING:" +
	    "FD(timeout=6000;shun=false):" +
	    "STABLE:" +
	    "MERGE:" +
	    "NAKACK:" +
	    "FLUSH:" +
	    "GMS:" + 
	    "VIEW_ENFORCER:" +
	    "FLOWCONTROL:" +
	    "QUEUE";
	*/


	/*
	// Bill Burke's props
	props="UDP:" +
	    "PING:" +
	    "FD(timeout=5000):" +
	    "STABLE:" +
	    "VERIFY_SUSPECT(timeout=1500):" +
	    "MERGE:" +
	    "NAKACK:" +
	    "UNICAST(timeout=5000)" +
	    ":FRAG:" +
	    "FLUSH:" +
	    "GMS:" +
	    "STATE_TRANSFER:" +
	    "QUEUE";
	*/



	try {

	    draw=new Draw2Channels(control_props, data_props, no_channel);
	    draw.go();
	}
	catch(Exception e) {
	    log.error(e);
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
		System.out.println("Creating control channel");
		control_channel=new JChannel(control_props);
		System.out.println("Connecting control channel");
		control_channel.connect(control_groupname);	
		System.out.println("Creating data channel");
		data_channel=new JChannel(data_props);
		// data_channel.SetOpt(Channel.VIEW, Boolean.FALSE);
		System.out.println("Connecting data channel");
		data_channel.connect(data_groupname);
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
	    if(!no_channel) {
		System.out.println("Starting control receiver thread");
		control_receiver=new ControlReceiver();
		control_receiver.start();
		
		
		System.out.println("Starting data receiver thread");
		data_receiver=new DataReceiver();
		data_receiver.start();
	    }
	}
	catch(Exception e) {
	    log.error(e);
	    return;
	}
    }




    void setTitle() {
	String title="";
	if(no_channel) {
	    mainFrame.setTitle(" Draw Demo ");
	    return;
	}
	if(control_channel.getLocalAddress() != null)
	    title+=control_channel.getLocalAddress();
	title+=" (" + member_size + ") mbrs";
	mainFrame.setTitle(title);
    }


    /*
    public void mainLoop() {
	Object       tmp;
	Message      msg=null;
	DrawCommand  comm;
	boolean      fl=true;

	while(fl) {
	    try {
		tmp=channel.Receive(0);
		if(tmp == null) continue;

		if(tmp instanceof View) {
		    View v=(View)tmp;
		    System.out.println("** View=" + v);
		    member_size=v.Size();
		    if(mainFrame != null)
			SetTitle();
		    continue;
		}

		if(tmp instanceof ExitEvent) {
		    System.out.println("-- Draw2Channels.main(): received EXIT, waiting for ChannelReconnected callback");
		    break;
		}

		if(!(tmp instanceof Message))
		    continue;

		msg=(Message)tmp;
		comm=null;

		Object obj=Util.ObjectFromByteBuffer(msg.GetBuffer());
		if(obj instanceof DrawCommand)
		    comm=(DrawCommand)obj;
		else if(obj instanceof Message) {
		    System.out.println("*** Draw2Channels.run(): message is " + Util.PrintMessage((Message)obj));
		    Util.DumpStack(false);
		    continue;
		}
		else {
		    if(obj != null)
			System.out.println("*** Draw2Channels.run(): obj is " + obj.getClass() + 
					   ", hdrs are" + msg.PrintObjectHeaders());
		    else
			System.out.println("*** Draw2Channels.run(): hdrs are" + msg.PrintObjectHeaders());
		    Util.DumpStack(false);
		    continue;
		}

		switch(comm.mode) {
		case DrawCommand.DRAW:
		    if(panel != null)
			panel.drawPoint(comm);
		    break;
		case DrawCommand.CLEAR:
		    ClearPanel();
		    continue;
		default:
		    log.error("***** Draw2Channels.run(): received invalid draw command " + comm.mode);
		    break;
		}

	    }
	    catch(ChannelNotConnected not) {
		log.error("Draw2Channels: " + not);
		break;
	    }
	    catch(ChannelClosed closed) {
		break;
	    }
	    catch(Exception e) {
		log.error(e);
		continue;
	    }
	}
    }
    */




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
	    data_channel.send(new Message(null, null, out.toByteArray()));
	}
	catch(Exception ex) {
	    log.error(ex);
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
	else if("Leave & Exit".equals(command)) {
	    if(!no_channel) {
		try {
		    control_channel.close();
		}
		catch(Exception ex) {
		    log.error(ex);
		}
		try {
		    data_channel.close();
		}
		catch(Exception ex) {
		    log.error(ex);
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
	final Dimension        preferred_size=new Dimension(235, 170);
	Image            img=null; // for drawing pixels
	Dimension        d, imgsize;
	Graphics         gr=null;

	
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
		data_channel.send(new Message(null, null, out.toByteArray()));
		Thread.yield(); // gives the repainter some breath
	    }
	    catch(Exception ex) {
		log.error(ex);
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



    class ControlReceiver extends Thread {
	public void run() {
	    Object    tmp;

	    while(true) {
		try {
		    tmp=control_channel.receive(0);
		
		    if(tmp == null) continue;
		
		    if(tmp instanceof View) {
			View v=(View)tmp;

			data_channel.down(new Event(Event.VIEW_CHANGE, v));

			member_size=v.size();
			if(mainFrame != null)
			    mainFrame.setTitle(member_size + " mbrs");
			continue;
		    }
		}
		catch(ChannelNotConnectedException not) {
		    break;
		}
		catch(ChannelClosedException closed) {
		    return;
		}
		catch(Exception e) {
		    log.error("Draw2Channels.ControlReceiver.run(): " + e);
		    break;
		}
	    }	    
	}
	
    }


    class DataReceiver extends Thread {

	public void run() {
	    Object      tmp;
	    Message     msg=null;
	    DrawCommand comm;

	    while(true) {
		try {
		    tmp=data_channel.receive(0);
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
			System.out.println("-- Draw2Channels.main(): received EXIT, waiting for ChannelReconnected callback");
			break;
		    }

		    if(!(tmp instanceof Message))
			continue;

		    msg=(Message)tmp;
		    comm=null;

		    Object obj=msg.getObject();
		    if(obj instanceof DrawCommand)
			comm=(DrawCommand)obj;
		    else if(obj instanceof Message) {
			System.out.println("*** Draw2Channels.run(): message is " + Util.printMessage((Message)obj));
			Util.dumpStack(false);
			continue;
		    }
		    else {
			if(obj != null)
			    System.out.println("*** Draw2Channels.run(): obj is " + obj.getClass() + 
					       ", hdrs are" + msg.printObjectHeaders());
			else
			    System.out.println("*** Draw2Channels.run(): hdrs are" + msg.printObjectHeaders());
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
			log.error("***** Draw2Channels.run(): received invalid draw command " + comm.mode);
			break;
		    }

		}
		catch(ChannelNotConnectedException not) {
		    log.error("Draw2Channels: " + not);
		    break;
		}
		catch(ChannelClosedException closed) {
		    break;
		}
		catch(Exception e) {
		    log.error(e);
		    continue;
	    }
	    }	    
	}

    }



    
}



