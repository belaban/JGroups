// $Id: TotalOrder.java,v 1.7 2004/09/23 16:29:35 belaban Exp $


package org.jgroups.demos;

import org.jgroups.*;
import org.jgroups.util.Util;

import java.awt.*;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.awt.event.WindowAdapter;
import java.awt.event.WindowEvent;
import java.net.InetAddress;




/**
 * Originally written to be a demo for TOTAL order (code to be written by a student). In the meantime,
 * it evolved into a state transfer demo. All members maintain a shared matrix and continually
 * broadcast changes to be applied to a randomly chosen field (e.g. multiplication of field with new
 * value, division, addition, subtraction). Each member can be started independently (starts to
 * broadcast update messages to all members). When "Stop" is pressed, a stop message is broadcast to
 * all members, causing them to stop sending messages. The "Clear" button clears the shared state;
 * "GetState" refreshes it from the shared group state (using the state transfer protocol).<p>If the
 * demo is to be used to show TOTAL order, then the TOTAL protocol would have to be added to the
 * stack.
 * @author Bela Ban
 */
public class TotalOrder extends Frame {
    final Font     def_font=new Font("Helvetica", Font.BOLD, 12);
    final Font     def_font2=new Font("Helvetica", Font.PLAIN, 12);
    MyCanvas       canvas;
    final MenuBar        menubar=createMenuBar();
    final Button         start=new Button("Start");
    final Button stop=new Button("Stop");
    final Button         clear=new Button("Clear");
    final Button get_state=new Button("Get State");
    final Button         quit=new Button("Quit");
    final Panel          button_panel=new Panel();
    SenderThread   sender=null;
    ReceiverThread receiver=null;
    Channel        channel;
    Dialog         error_dlg;
    long           timeout=0;
    int            field_size=0;
    int            num_fields=0;
    static final int      x_offset=30;
    static final int      y_offset=40;



    void error(String s) {
	System.err.println(s);
    }



    class EventHandler extends WindowAdapter {
	final Frame gui;

	public EventHandler(Frame g) {gui=g;}

	public void windowClosing(WindowEvent e) {
	    gui.dispose();
	    System.exit(0);
	}
    }




    class SenderThread extends Thread {
	TotOrderRequest req;
	boolean         running=true;

	public void stopSender() {
	    running=false;
	    interrupt();
	}

	public void run() {
	    this.setName("SenderThread");

	    while(running) {
		try {
		    req=createRandomRequest();
		    channel.send(new Message(null, null, Util.objectToByteBuffer(req)));
		    Util.sleep(timeout);
		}
		catch(Exception e) {
		    error(e.toString());
		    return;
		}
	    }
	}
    }


    class ReceiverThread extends Thread {
	Object           o;
	Message          msg;
	TotOrderRequest  req;
	SetStateEvent    set_state_evt;
	boolean          running=true;


	public void stopReceiver() {
	    running=false;
	    interrupt();
	}

	public void run() {
	    this.setName("ReceiverThread");
	    while(running) {
		try {
		    o=channel.receive(0);
		    if(o instanceof Message) {
			try {
			    msg=(Message)o;

			    Object tmp=msg.getObject();

			    if(!(tmp instanceof TotOrderRequest)) {
				System.err.println("tmp is " + tmp.getClass() + ": " + tmp);
				if(tmp instanceof Message) {
				    System.out.print("Hdrs are ");
				    ((Message)tmp).printObjectHeaders();
				}

			    }


			    req=(TotOrderRequest)tmp;
			    processRequest(req);
			}
			catch(Exception e) {
			    System.err.println(e);
			}
		    }
		    else if(o instanceof GetStateEvent) {
			int[][] copy_of_state=canvas.getCopyOfState();
			channel.returnState(Util.objectToByteBuffer(copy_of_state));
		    }
		    else if(o instanceof SetStateEvent) {  // state was received, set it !
			set_state_evt=(SetStateEvent)o;
			canvas.setState(Util.objectFromByteBuffer(set_state_evt.getArg()));
		    } else if (o instanceof View) System.out.println(o.toString());
		}
		catch(ChannelClosedException closed) {
		    error("Channel has been closed; receiver thread quits");
		    return;
		}
		catch(Exception e) {
		    error(e.toString());
		    return;
		}
	    }
	}
    }



    void processRequest(TotOrderRequest req) throws Exception {
	int x=req.x, y=req.y, val=req.val, result=0;

	if(req.type == TotOrderRequest.STOP) {
	    stopSender();
	    return;
	}

	switch(req.type) {
	case TotOrderRequest.ADDITION:
	    result=canvas.addValueTo(x, y, val);
	    break;
	case TotOrderRequest.SUBTRACTION:
	    result=canvas.subtractValueFrom(x, y, val);
	    break;
	case TotOrderRequest.MULTIPLICATION:
	    result=canvas.multiplyValueWith(x, y, val);
	    break;
	case TotOrderRequest.DIVISION:
	    result=canvas.divideValueBy(x, y, val);
	    break;
	}
	canvas.update();
    }



	public TotalOrder(String title, long timeout, int num_fields, int field_size, String props) {
		Dimension s;

		this.timeout = timeout;
		this.num_fields = num_fields;
		this.field_size = field_size;
		setFont(def_font);

		try {
			channel = new JChannel(props);
			channel.setOpt(Channel.GET_STATE_EVENTS, Boolean.TRUE);
			channel.connect("TotalOrderGroup");
			boolean rc = channel.getState(null, 8000);
		}
		catch (Exception e) {
			error(e.toString());
			System.exit(-1);
		}

		start.addActionListener(new ActionListener() {
			public void actionPerformed(ActionEvent e) {
				startSender();
			}
		});

		stop.addActionListener(new ActionListener() {
			public void actionPerformed(ActionEvent e) {
				try {
					channel.send(
						new Message(
							null,
							null,
							Util.objectToByteBuffer(new TotOrderRequest(TotOrderRequest.STOP, 0, 0, 0))));
				}
				catch (Exception ex) {
				}

			}
		});

		clear.addActionListener(new ActionListener() {
			public void actionPerformed(ActionEvent e) {
				canvas.clear();
			}
		});

		get_state.addActionListener(new ActionListener() {
			public void actionPerformed(ActionEvent e) {
                try {
                    boolean rc = channel.getState(null, 3000);
                    if (rc == false)
                        error("State could not be retrieved !");
                }
                catch(Throwable t) {
                    error("exception fetching state: " + t);
                }
			}
		});

		quit.addActionListener(new ActionListener() {
			public void actionPerformed(ActionEvent e) {
				channel.disconnect();
				channel.close();
				System.exit(0);
			}
		});

		setTitle(title);
		addWindowListener(new EventHandler(this));
		setBackground(Color.white);
		setMenuBar(menubar);

		setLayout(new BorderLayout());
		canvas = new MyCanvas(num_fields, field_size, x_offset, y_offset);

		add("Center", canvas);
		button_panel.setLayout(new FlowLayout());
		button_panel.setFont(def_font2);
		button_panel.add(start);
		button_panel.add(stop);
		button_panel.add(clear);
		button_panel.add(get_state);
		button_panel.add(quit);
		add("South", button_panel);

		s = canvas.getSize();
		s.height += 100;
		setSize(s);
		startReceiver();
	}


    void startSender() {
	if(sender == null) {
	    sender=new SenderThread();
	    sender.start();
	}
    }

    void stopSender() {
	if(sender != null) {
	    sender.stopSender();
	    sender=null;
	}
    }

    void startReceiver() {
	if(receiver == null) {
	    receiver=new ReceiverThread();
	    receiver.setPriority(Thread.MAX_PRIORITY);
	    receiver.start();
	}
    }

    void stopReceiver() {
	if(receiver != null) {
	    receiver.stopReceiver();
	    receiver=null;
	}
    }



    MenuBar createMenuBar() {
	MenuBar   ret=new MenuBar();
	Menu      file=new Menu("File");
	MenuItem  quit=new MenuItem("Quit");

	ret.setFont(def_font2);
	ret.add(file);

	file.addSeparator();
	file.add(quit);


	quit.addActionListener(
			       new ActionListener() {
	    public void actionPerformed(ActionEvent e) {
		System.exit(1);
	    }});
	return ret;
    }


    public void rrror(String msg) {
	Button  ok=new Button("Ok");
	Label   l=new Label(msg);

	error_dlg=new Dialog(this, msg, true);
	error_dlg.setLocation(90, 150);
	error_dlg.setSize(420, 100);
	error_dlg.setLayout(new BorderLayout());
	error_dlg.setFont(def_font2);

	ok.addActionListener(new ActionListener() {
	    public void actionPerformed(ActionEvent e) {
		error_dlg.dispose();
	    }
	});

	error_dlg.add("Center", l);
	error_dlg.add("South", ok);
	error_dlg.show();
    }



    TotOrderRequest createRandomRequest() {
	TotOrderRequest ret=null;
	int     op_type=(int)(((Math.random() * 10) % 4)+1);  // 1 - 4
	int     x=(int)((Math.random() * num_fields * 2) % num_fields);
	int     y=(int)((Math.random() * num_fields * 2) % num_fields);
	int     val=(int)((Math.random() * num_fields * 200) % 10);

	ret=new TotOrderRequest(op_type, x, y, val);
	return ret;
    }




    public static void main(String[] args) {
	TotalOrder  g;
	String      arg;
	long        timeout=200;
	int         num_fields=3;
	int         field_size=80;
	String      props=null;

	props="UDP(mcast_addr=224.0.0.35;mcast_port=45566;ip_ttl=0;" +
	    "mcast_send_buf_size=150000;mcast_recv_buf_size=80000):" +
	    "PING(timeout=2000;num_initial_members=3):" +
	    "MERGE2(min_interval=5000;max_interval=10000):" +
	    "FD_SOCK:" +
	    "VERIFY_SUSPECT(timeout=1500):" +
	    "pbcast.STABLE(desired_avg_gossip=20000):" +
	    "pbcast.NAKACK(gc_lag=50;retransmit_timeout=300,600,1200,2400,4800):" +
	    "UNICAST(timeout=5000):" +
	    "FRAG(frag_size=4096;down_thread=false;up_thread=false):" +
	    "pbcast.GMS(join_timeout=5000;join_retry_timeout=2000;" +
	    "shun=false;print_local_addr=true):" +
	    "pbcast.STATE_TRANSFER";
	
	

	for(int i=0; i < args.length; i++) {
	    arg=args[i];
	    if("-timeout".equals(arg)) {
		timeout=Long.parseLong(args[++i]);
		continue;
	    }
	    if("-num_fields".equals(arg)) {
		num_fields=Integer.parseInt(args[++i]);
		continue;
	    }
	    if("-field_size".equals(arg)) {
		field_size=Integer.parseInt(args[++i]);
		continue;
	    }
	    if("-help".equals(arg)) {
		System.out.println("\nTotalOrder [-timeout <value>] [-num_fields <value>] "+
				   "[-field_size <value>] [-props <properties (can be URL)>]\n");
		return;
	    }
	    if("-props".equals(arg)) {
		props=args[++i];
		continue;
	    }
	}


	try {
	    g=new TotalOrder("Total Order Demo on " + InetAddress.getLocalHost().getHostName(),
			     timeout, num_fields, field_size, props);
	    g.show();
	}
	catch(Exception e) {
	    System.err.println(e);
	}
    }



}




class TotOrderRequest implements java.io.Serializable {
    public static final int STOP            = 0;
    public static final int ADDITION        = 1;
    public static final int SUBTRACTION     = 2;
    public static final int MULTIPLICATION  = 3;
    public static final int DIVISION        = 4;


    public int  type=ADDITION;
    public int  x=0;
    public int  y=0;
    public int  val=0;


    TotOrderRequest(int type, int x, int y, int val) {
	this.type=type; this.x=x; this.y=y; this.val=val;
    }

    public static String type2Str(int t) {
	switch(t) {
	case STOP:           return "STOP";
	case ADDITION:       return "ADDITION";
	case SUBTRACTION:    return "SUBTRACTION";
	case MULTIPLICATION: return "MULTIPLICATION";
	case DIVISION:       return "DIVISION";
	default:             return "<unknown>";
	}
    }

    public String toString() {
	return "[" + x + ',' + y + ": " + type2Str(type) + '(' + val + ")]";
    }
}



class MyCanvas extends Canvas {
    int           field_size=100;
    int           num_fields=4;
    int           x_offset=30;
    int           y_offset=30;

    final Font    def_font=new Font("Helvetica", Font.BOLD, 14);
    int[][]       array=null;      // state

    Dimension     off_dimension=null;
    Image         off_image=null;
    Graphics      off_graphics=null;
    final Font    def_font2=new Font("Helvetica", Font.PLAIN, 12);
    static final Color   checksum_col=Color.blue;
    int           checksum=0;


    public MyCanvas(int num_fields, int field_size, int x_offset, int y_offset) {
	this.num_fields=num_fields; this.field_size=field_size;
	this.x_offset=x_offset; this.y_offset=y_offset;

	array=new int[num_fields][num_fields];
	setBackground(Color.white);
	setSize(2*x_offset + num_fields*field_size+30, y_offset + num_fields*field_size+50);

	for(int i=0; i < num_fields; i++)
	    for(int j=0; j < num_fields; j++)
		array[i][j]=0;
    }


    public void setFieldSize(int fs) {field_size=fs;}
    public void setNumFields(int nf) {num_fields=nf;}
    public void setXOffset(int o)    {x_offset=o;}
    public void setYOffset(int o)    {y_offset=o;}


    public int addValueTo(int x, int y, int value) {
	synchronized(array) {
	    array[x][y]+=value;
	    repaint();
	    return array[x][y];
	}
    }

    public int subtractValueFrom(int x, int y, int value) {
	synchronized(array) {
	    array[x][y]-=value;
	    repaint();
	    return array[x][y];
	}
    }

    public int multiplyValueWith(int x, int y, int value) {
	synchronized(array) {
	    array[x][y]*=value;
	    repaint();
	    return array[x][y];
	}
    }

    public int divideValueBy(int x, int y, int value) {
	if(value == 0)
	    return array[x][y];
	synchronized(array) {
	    array[x][y]/=value;
	    repaint();
	    return array[x][y];
	}
    }


    public void setValueAt(int x, int y, int value) {
	synchronized(array) {
	    array[x][y]=value;
	}
	repaint();
    }



    public int getValueAt(int x, int y) {
	synchronized(array) {
	    return array[x][y];
	}
    }


    public void clear() {
	synchronized(array) {
	    for(int i=0; i < num_fields; i++)
		for(int j=0; j < num_fields; j++)
		    array[i][j]=0;
	    checksum=checksum();
	    repaint();
	}
    }


    public int[][] getState() {
	synchronized(array) {
	    return array;
	}
    }


    public int[][] getCopyOfState() {
	int[][] retval=new int[num_fields][num_fields];

	synchronized(array) {
	    for(int i=0; i < num_fields; i++)
            System.arraycopy(array[i], 0, retval[i], 0, num_fields);
	    return retval;
	}
    }


    public void update() {
	checksum=checksum();
	repaint();
    }


    public void setState(Object new_state) {

	if(new_state == null)
	    return;

	try {
	    int[][] new_array=(int[][])new_state;
	    synchronized(array) {
		clear();

		for(int i=0; i < num_fields; i++)
            System.arraycopy(new_array[i], 0, array[i], 0, num_fields);
		checksum=checksum();
		repaint();
	    }
	}
	catch(Exception e) {
	    System.err.println(e);
	    return;
	}
    }


    public int checksum() {
	int retval=0;

	synchronized(array) {
	    for(int i=0; i < num_fields; i++)
		for(int j=0; j < num_fields; j++)
		    retval+=array[i][j];
	}
	return retval;
    }



    public void update(Graphics g) {
	Dimension d=getSize();

	if(off_graphics == null ||
	   d.width != off_dimension.width ||
	   d.height != off_dimension.height) {
	    off_dimension=d;
	    off_image=createImage(d.width, d.height);
	    off_graphics=off_image.getGraphics();
	}

	//Erase the previous image.
	off_graphics.setColor(getBackground());
	off_graphics.fillRect(0, 0, d.width, d.height);
	off_graphics.setColor(Color.black);
	off_graphics.setFont(def_font);
	drawEmptyBoard(off_graphics);
	drawNumbers(off_graphics);
	g.drawImage(off_image, 0, 0, this);
    }




    public void paint(Graphics g) {
	update(g);
    }




    /** Draws the empty board, no pieces on it yet, just grid lines */
    void drawEmptyBoard(Graphics g) {
	int   x=x_offset, y=y_offset;
	Color old_col=g.getColor();

	g.setFont(def_font2);
	old_col=g.getColor();
	g.setColor(checksum_col);
	g.drawString(("Checksum: " + checksum), x_offset + field_size, y_offset - 20);
	g.setFont(def_font);
	g.setColor(old_col);

	for(int i=0; i < num_fields; i++) {
	    for(int j=0; j < num_fields; j++) {  // draws 1 row
		g.drawRect(x, y, field_size, field_size);
		x+=field_size;
	    }
	    g.drawString(("" + (num_fields-i-1)), x+20, y+field_size/2);
	    y+=field_size;
	    x=x_offset;
	}

	for(int i=0; i < num_fields; i++) {
	    g.drawString((""+i), x_offset + i*field_size + field_size/2, y+30);
	}
    }



    void drawNumbers(Graphics g) {
	Point        p;
	String       num;
	FontMetrics  fm=g.getFontMetrics();
	int          len=0;

	synchronized(array) {
	    for(int i=0; i < num_fields; i++)
		for(int j=0; j < num_fields; j++) {
		    num="" + array[i][j];
		    len=fm.stringWidth(num);
		    p=index2Coord(i, j);
		    g.drawString(num, p.x - (len/2), p.y);
		}
	}
    }




    Point coord2Index(int x, int y) {
	Point ret=new Point();

	ret.x=x_offset + (x * field_size);
	ret.y=y_offset + ((num_fields - 1 - y) * field_size);
	return ret;
    }


    Point index2Coord(int i, int j) {
	int x=x_offset + i*field_size + field_size/2;

	// int y=y_offset + j*field_size + field_size/2;

	int y=y_offset + num_fields*field_size - j*field_size - field_size/2;

	return new Point(x, y);
    }


}








