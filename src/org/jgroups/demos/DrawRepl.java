// $Id: DrawRepl.java,v 1.3 2004/07/05 05:45:31 belaban Exp $

package org.jgroups.demos;


import org.jgroups.Channel;
import org.jgroups.JChannel;
import org.jgroups.blocks.GroupRequest;
import org.jgroups.blocks.MethodCall;
import org.jgroups.blocks.RpcDispatcher;

import java.awt.*;
import java.awt.event.*;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.util.Hashtable;
import java.util.Random;




/**
 * NOT SUPPORTED !
 * Replicates the whiteboard demo by intercepting central AWT event queue and mcasting events to
 * all members. Not very useful in all cases, e.g. when the "Leave" button is pressed, and this event
 * is broadcast to all members, all members will leave ! This demo would clearly benefit from more work !
 */
public class DrawRepl implements MouseMotionListener, WindowListener, ActionListener,
				 Runnable {
    private Graphics               graphics=null;
    private Frame                  mainFrame=null;
    private Panel                  panel=null, sub_panel=null;
    private byte                   buf[]=new byte[128];
    private ByteArrayOutputStream  out=new ByteArrayOutputStream();
    private DataOutputStream       outstream;
    private ByteArrayInputStream   inp;
    private DataInputStream        instream;
    private int                    x, y;
    private Hashtable              colors=new Hashtable();
    private Random                 random=new Random(System.currentTimeMillis());
    private int                    col_val=1;
    private Color                  current_color=Color.red;
    private Button                 clear_button, leave_button;
    private String                 groupname="DrawReplGroup";
    private final Font             default_font=new Font("Helvetica",Font.PLAIN,12);

    private EventQueue             event_queue=null;
    private Thread                 mythread=null;
    private RpcDispatcher          dispatcher;
    private Channel                channel;


    public DrawRepl() {
	colors.put(new Integer(1),  Color.white);
	colors.put(new Integer(2),  Color.black);
	colors.put(new Integer(3),  Color.red);
	colors.put(new Integer(4),  Color.orange);
	colors.put(new Integer(5),  Color.green);
	colors.put(new Integer(6),  Color.magenta);
	colors.put(new Integer(7),  Color.cyan);
	colors.put(new Integer(8),  Color.blue);
	mythread=new Thread(this);
	try {
	    channel=new JChannel();
	    dispatcher=new RpcDispatcher(channel, null, null, this);
	    channel.connect(groupname);
	}
	catch(Exception e) {
	    System.err.println(e);
	    System.exit(0);
	}
    }


    public static void main(String[] args) {
	DrawRepl draw=new DrawRepl();
	draw.go();
    }


    private Color SelectColor() {
	col_val=(Math.abs(random.nextInt()) % 8)+1;
	Color ret=(Color)colors.get(new Integer(col_val));
	if(ret == null)
	    ret=Color.red;
	return ret;
    }



    AWTEvent copyEvent(Component src, AWTEvent evt) {

	if(evt instanceof MouseEvent) {
	    MouseEvent mev=(MouseEvent)evt;
	    return new MouseEvent(src, evt.getID(), mev.getWhen(), mev.getModifiers(),
				  mev.getX(), mev.getY(), mev.getClickCount(),
				  mev.isPopupTrigger());
	}

	if(evt instanceof KeyEvent) {
	    KeyEvent kev=(KeyEvent)evt;
	    return new KeyEvent(src, evt.getID(), kev.getWhen(), kev.getModifiers(), 
				kev.getKeyCode(), kev.getKeyChar());
	}

	if(evt instanceof ActionEvent)
	    return new ActionEvent(src, evt.getID(), ((ActionEvent)evt).getActionCommand(),
				   ((ActionEvent)evt).getModifiers());


	if(evt instanceof PaintEvent)
	    return new PaintEvent(src, evt.getID(), ((PaintEvent)evt).getUpdateRect());


	if(evt instanceof FocusEvent)
	    return new FocusEvent(src, evt.getID(), ((FocusEvent)evt).isTemporary());

	if(evt instanceof ComponentEvent)
	    return new ComponentEvent(src, evt.getID());
				
	return null;
    }



    void dispatch(Object src, AWTEvent evt) {
 	if (src instanceof Component)
 	    ((Component)src).dispatchEvent(evt);
	else if (src instanceof MenuComponent)
 	    ((MenuComponent)src).dispatchEvent(evt);
 	else
 	    System.err.println("++++++++++");	
    }
    

    public Component findComponent(Container parent, String comp_name) {
	Component  retval=null;

	if(comp_name != null && comp_name.equals(parent.getName()))
	    return parent;

	int ncomponents = parent.getComponentCount();
        Component components[] = parent.getComponents();
	for (int i = ncomponents-1 ; i >= 0; i--) {
	    Component comp = components[i], tmp;
	    if (comp != null) {
		if(comp instanceof Container) {
		    retval=findComponent((Container)comp, comp_name);
		    if(retval != null)
			return retval;
		}
		else if(comp_name.equals(comp.getName()))
		    return comp;
	    }
	}
	return retval;
    }


    // public void setSize(Integer x, Integer y) {
    //   mainFrame.setSize(new Dimension(x.intValue(), y.intValue()));
    // }


    /* Called by Dispatcher */
    public void processEvent(String comp_name, AWTEvent evt) {
	AWTEvent   copy_evt=null;
	Component  src=findComponent(mainFrame, comp_name);
	if(src == null) {
	    System.err.println("processEvent(): src is null");
	    return;
	}

	System.out.println("Received " + evt.getClass().getName());

	copy_evt=copyEvent(src, evt);
	if(copy_evt == null) {
	    System.err.println("copy_evt is NULL");
	    return;
	}
	dispatch(src, copy_evt);


// 	if(evt instanceof ComponentEvent && evt.getID() == ComponentEvent.COMPONENT_RESIZED) {
// 	    Dimension dim=mainFrame.getSize();
// 	    try {
// 		dispatcher.sendGetN(groupname, "setSize", new Integer(dim.height),
// 				    new Integer(dim.width), 0, 0);
// 	    }
// 	    catch(Exception e) {
// 		System.err.println(e);
// 	    }
// 	}
    }


    void processLocally(AWTEvent evt) {
	dispatch(evt.getSource(), evt);
    }



    public void run() {
	String comp_name;
	
	while(true) {
	    try {
		AWTEvent evt=event_queue.getNextEvent();
		Object obj=evt.getSource();
		if(obj == null) {
		    System.err.println("src is NULL");
		    continue;
		}		

		if(obj instanceof Component)
		    comp_name=((Component)obj).getName();
		else if(obj instanceof MenuComponent)
		    comp_name=((MenuComponent)obj).getName();
		else {
		    System.err.println("src is of type " + obj.getClass().getName());
		    continue;
		}

		if(evt instanceof FocusEvent || evt instanceof PaintEvent) {
		    System.out.println(evt.getClass().getName() + " not copied");
		    processLocally(evt);
		    continue;
		}
		System.out.println("MCasting "+evt.getClass().getName()+" event...");
		MethodCall call = new MethodCall("processEvent", new Object[] {comp_name, evt}, 
            new String[] {String.class.getName(), AWTEvent.class.getName()});
		dispatcher.callRemoteMethods(null, call, GroupRequest.GET_NONE, 0);
	    }
	    catch(Exception e) {
		System.err.println(e);
	    }
	}
    }



	
    public void go() {
	mainFrame=new Frame();
	panel=new Panel();
	sub_panel=new Panel();
	
	event_queue=mainFrame.getToolkit().getSystemEventQueue();
	mythread.start();
	
	mainFrame.setSize(200,200);
	mainFrame.add("Center", panel);
	clear_button=new Button("Clear");
	clear_button.setFont(default_font);
	clear_button.addActionListener(this);
	leave_button=new Button("Exit");
	leave_button.setFont(default_font);
	leave_button.addActionListener(this);
	sub_panel.add("South", clear_button);
	sub_panel.add("South", leave_button);
	mainFrame.add("South", sub_panel);

	mainFrame.addWindowListener(this);
	// mainFrame.addComponentListener(this);

	panel.addMouseMotionListener(this);

	mainFrame.setVisible(true);

	graphics=panel.getGraphics();
	current_color=SelectColor();
	if(current_color == null)
	    current_color=Color.red;
	graphics.setColor(current_color);
    }



    /* --------------- Callbacks --------------- */


    public void mouseMoved(MouseEvent e) {
    }

    public void mouseDragged(MouseEvent e) {
	x=e.getX();
	y=e.getY();
	graphics.fillOval(x, y, 10, 10);
    }


    public void clearPanel() {

	System.out.println("CLEAR");

	Rectangle  bounds=panel.getBounds();
	graphics.clearRect(0, 0, bounds.width, bounds.height);	
    }



    public void windowActivated(WindowEvent e) {}
    public void windowClosed(WindowEvent e) {}
    public void windowClosing(WindowEvent e) {
	System.exit(0);
    }
    public void windowDeactivated(WindowEvent e) {}
    public void windowDeiconified(WindowEvent e) {}
    public void windowIconified(WindowEvent e) {}
    public void windowOpened(WindowEvent e) {}



//     public void componentResized(ComponentEvent e) {
// 	System.out.println("RESIZED, size is " + mainFrame.getBounds());
//     }

//     public void componentMoved(ComponentEvent e) {
// 	System.out.println("MOVED, location is: " + mainFrame.getLocation());
//     }

//     public void componentShown(ComponentEvent e) {}

//     public void componentHidden(ComponentEvent e) {}




    public void actionPerformed(ActionEvent e) {
	String command=e.getActionCommand();
	if("Clear".equals(command))
	    clearPanel();
	else if("Exit".equals(command)) {
	    mainFrame.setVisible(false);
	    System.exit(0);
	}
	else
	    System.out.println("Unknown action");
    }


}

