// $Id: GraphPanel.java,v 1.7 2009/05/13 13:07:07 belaban Exp $


package org.jgroups.demos.wb;


import org.jgroups.logging.Log;
import org.jgroups.logging.LogFactory;
import org.jgroups.Address;
import org.jgroups.blocks.GroupRequest;
import org.jgroups.blocks.MethodCall;
import org.jgroups.util.Util;

import java.awt.*;
import java.awt.event.MouseEvent;
import java.awt.event.MouseListener;
import java.awt.event.MouseMotionListener;
import java.util.Vector;




public class GraphPanel extends Panel implements MouseListener, MouseMotionListener {    
    final Whiteboard     wb;
    final Vector         nodes=new Vector();
    final Vector copy=new Vector();
    String         myname=null;
    public Object  my_addr=null;
    Node           pick;
    boolean        pickfixed;
    Image          offscreen;
    Dimension      offscreensize;
    Graphics       offgraphics;
    static final Color    fixedColor = Color.red;
    static final Color    selectColor = Color.pink;
    final Color    nodeColor = new Color(250, 220, 100);
    final Font     default_font=new Font("Helvetica",Font.PLAIN,12);
    Log            log=LogFactory.getLog(getClass());



    private Frame findParent() {
	Component retval=getParent();

	while(retval != null) {
	    if(retval instanceof Frame)
		return (Frame)retval;
	    retval=retval.getParent();
	}
	return null;
    }



    Node findNodeAtPoint(Point p) {
	int   x=p.x, y=p.y;
	Node  n;
	
	synchronized(nodes) {
	    if(nodes.size() < 1)
		return null;
	    for(int i=nodes.size()-1; i >= 0; i--) {
		n=(Node)nodes.elementAt(i);
		if(x >= n.xloc && x <= (n.xloc + n.width) && y >= n.yloc && y <= (n.yloc + n.height))
		    return n;
	    }
	}
	return null;
    }



    public GraphPanel(Whiteboard wb) {
	this.wb = wb;
	addMouseListener(this);
	addMouseMotionListener(this);
    }


    public void addNode(String lbl, Address addr, int xloc, int yloc) {
	Node n = new Node();
	n.x = xloc;
	n.y = yloc;
	n.lbl = lbl;
	n.addr=addr;
	nodes.addElement(n);
	repaint();
    }


    public void removeNode(Object addr) {
	Node   n;
	Object a;

	if(addr == null) {
	    log.error("removeNode(): address of node to be removed is null !");
	    return;
	}

	synchronized(nodes) {
	    for(int i=0; i < nodes.size(); i++) {
		n=(Node)nodes.elementAt(i);
		a=n.addr;
		if(a == null)
		    continue;
		if(addr.equals(a)) {
		    nodes.removeElement(n);
		    System.out.println("Removed node " + n);
		    break;
		}
	    }
	    repaint();
	}
    }


    // Removes nodes that are not in the view
    public void adjustNodes(Vector v) {
	Node n;
	boolean removed=false;

	synchronized(nodes) {
	    for(int i=0; i < nodes.size(); i++) {
		n=(Node)nodes.elementAt(i);
		if(!v.contains(n.addr)) {
		    System.out.println("adjustNodes(): node " + n + " was removed");
		    nodes.removeElement(n);
		    removed=true;
		}
	    }
	    if(removed)
		repaint();
	}
    }


    public void paintNode(Graphics g, Node n, FontMetrics fm) {
	String addr=n.addr != null ? n.addr.toString() : null;
	int x = (int)n.x;
	int y = (int)n.y;
	g.setColor((n == pick) ? selectColor : (n.fixed ? fixedColor : nodeColor));
	int w = fm.stringWidth(n.lbl) + 10;

	if(addr != null)
	    w=Math.max(w, fm.stringWidth(addr) + 10);

	if(addr == null)
	    addr="<no address>";

	int h = (fm.getHeight() + 4) * 2;
	n.width=w;
	n.height=h;
	n.xloc=x - w/2;
	n.yloc=y - h/2;
	g.fillRect(x - w/2, y - h / 2, w, h);
	g.setColor(Color.black);
	g.drawRect(x - w/2, y - h / 2, w-1, h-1);
	g.drawString(n.lbl, x - (w-10)/2, (y - (h-4)/2) + fm.getAscent());
	g.drawString(addr, x - (w-10)/2, (y - (h-4)/2) + 2 * fm.getAscent() +4  );
    }



    public synchronized void update(Graphics g) {
	Dimension d = getSize();
	if ((offscreen == null) || (d.width != offscreensize.width) || 
	    (d.height != offscreensize.height)) {
	    offscreen = createImage(d.width, d.height);
	    offscreensize = d;
	    offgraphics = offscreen.getGraphics();
	    offgraphics.setFont(default_font);
	}

	offgraphics.setColor(getBackground());
	offgraphics.fillRect(0, 0, d.width, d.height);

	FontMetrics fm = offgraphics.getFontMetrics();
	for (int i = 0; i < nodes.size(); i++) {
	    paintNode(offgraphics, (Node)nodes.elementAt(i), fm);
	}

	g.drawImage(offscreen, 0, 0, null);
    }





    public void mouseDragged(MouseEvent e) {
	Point   p=e.getPoint();
	int     mod=e.getModifiers();

	if(pick == null)
	    return;

	pick.x=p.x;
	pick.y=p.y;
	repaint();
    }



    
    public void mousePressed(MouseEvent e) {
	Point   p=e.getPoint();
	double  bestdist = Double.MAX_VALUE, dist;
	int     mod=e.getModifiers();
	Node    n;

	
	if((mod & MouseEvent.BUTTON3_MASK) != 0) {
	    System.out.println("\nright button at " + p);
	    n=findNodeAtPoint(p);
	    if(n != null) {
		System.out.println("Found node at " + p + ": " + n); 
		SendDialog dlg=new SendDialog(findParent(), n, myname, wb.disp);
		repaint();
	    }
	    e.consume();
	    return;
	}


	for (int i = 0 ; i < nodes.size() ; i++) {
	    n=(Node)nodes.elementAt(i);
	    dist = (n.x - p.x) * (n.x - p.x) + (n.y - p.y) * (n.y - p.y);
	    if (dist < bestdist) {
		pick = n;
		bestdist = dist;
	    }
	}
	pickfixed = pick.fixed;
	pick.fixed = true;
	pick.x = p.x;
	pick.y = p.y;
	repaint();
    }




    public void mouseReleased(MouseEvent e) {
	Point   p=e.getPoint();
	int     mod=e.getModifiers();

	if(pick == null)
	    return;

	pick.x = p.x;
	pick.y = p.y;
	pick.fixed = pickfixed;


	try {
		MethodCall call = new MethodCall("moveNode", new Object[] {pick}, new String[] {Node.class.getName()});
	    wb.disp.callRemoteMethods(null, call, GroupRequest.GET_ALL, 0);
	}
	catch(Exception ex) {
	    log.error(ex.toString());
	}
	
	pick = null;
    }


    public void mouseEntered(MouseEvent e) {}
    public void mouseExited(MouseEvent e)  {}
    public void mouseMoved(MouseEvent e)   {}    
    public void mouseClicked(MouseEvent e) {}




    public void start(String name) {
	myname=name;
	int xloc = (int)(10 + 250*Math.random());
	int yloc = (int)(10 + 250*Math.random());

	try {
	    MethodCall call=new MethodCall("addNode", 
	        new Object[] {name, my_addr, new Integer(xloc), new Integer(yloc)}, 
	        new String[] {String.class.getName(), Address.class.getName(), int.class.getName(), int.class.getName()});
	    wb.disp.callRemoteMethods(null, call, GroupRequest.GET_ALL, 0);
	}
	catch(Exception e) {
	    log.error(e.toString());
	}
	repaint();
    }


    public void stop() {
	nodes.removeAllElements();
    }




    public void saveState() {
	copy.removeAllElements();
	synchronized(nodes) {
	    for(int i=0; i < nodes.size(); i++)
		copy.addElement(nodes.elementAt(i));
	}
    }


    public byte[] getState() {  // return the copy previously saved by saveState()
        try {
            return Util.objectToByteBuffer(copy);
        }
        catch(Throwable ex) {
            ex.printStackTrace();
            return null;
        }
    }


    public void setState(byte[] data) {
	Vector n;
        Object new_state;

        try {
            new_state=Util.objectFromByteBuffer(data);
        }
        catch(Exception ex) {
            ex.printStackTrace();
            return;
        }

	synchronized(nodes) {
	    nodes.removeAllElements();
	    if(new_state != null) {
		n=(Vector)new_state;
		for(int i=0; i < n.size(); i++)
		    nodes.addElement(n.elementAt(i));
		repaint();
	    }
	}
    }


    public void moveNode(Node n) {
	Node    tmp;
	boolean changed=false;

	synchronized(nodes) {
	    for(int i=0; i < nodes.size(); i++) {
		tmp=(Node)nodes.elementAt(i);
		if(n.addr.equals(tmp.addr)) {
		    tmp.x=n.x;
		    tmp.y=n.y;
		    changed=true;
		    break;
		}
	    }
	    if(changed)
		repaint();
	}
    }

}
