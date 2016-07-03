package org.jgroups.demos.wb;


import org.jgroups.Address;
import org.jgroups.blocks.MethodCall;
import org.jgroups.blocks.RequestOptions;
import org.jgroups.blocks.ResponseMode;
import org.jgroups.logging.Log;
import org.jgroups.logging.LogFactory;
import org.jgroups.util.Util;

import java.awt.*;
import java.awt.event.InputEvent;
import java.awt.event.MouseEvent;
import java.awt.event.MouseListener;
import java.awt.event.MouseMotionListener;
import java.io.*;
import java.util.*;
import java.util.List;


public class GraphPanel extends Panel implements MouseListener, MouseMotionListener {
    final Whiteboard wb;
    final java.util.List<Node> nodes=new ArrayList<>();
    String myname=null;
    public Object my_addr=null;
    Node pick;
    boolean pickfixed;
    Image offscreen;
    Dimension offscreensize;
    Graphics offgraphics;
    static final Color fixedColor=Color.red;
    static final Color selectColor=Color.pink;
    final Color nodeColor=new Color(250, 220, 100);
    final Font default_font=new Font("Helvetica", Font.PLAIN, 12);
    Log log=LogFactory.getLog(getClass());


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
        int x=p.x, y=p.y;
        Node n;

        synchronized(nodes) {
            if(nodes.isEmpty())
                return null;
            for(int i=nodes.size() - 1; i >= 0; i--) {
                n=nodes.get(i);
                if(x >= n.xloc && x <= (n.xloc + n.width) && y >= n.yloc && y <= (n.yloc + n.height))
                    return n;
            }
        }
        return null;
    }


    public GraphPanel(Whiteboard wb) {
        this.wb=wb;
        addMouseListener(this);
        addMouseMotionListener(this);
    }


    public void addNode(String lbl, Address addr, int xloc, int yloc) {
        Node n=new Node();
        n.x=xloc;
        n.y=yloc;
        n.lbl=lbl;
        n.addr=addr;
        nodes.add(n);
        repaint();
    }


    public void removeNode(Object addr) {
        Node n;
        Object a;

        if(addr == null) {
            log.error("removeNode(): address of node to be removed is null !");
            return;
        }

        synchronized(nodes) {
            for(int i=0; i < nodes.size(); i++) {
                n=nodes.get(i);
                a=n.addr;
                if(a == null)
                    continue;
                if(addr.equals(a)) {
                    nodes.remove(n);
                    System.out.println("Removed node " + n);
                    break;
                }
            }
            repaint();
        }
    }


    // Removes nodes that are not in the view
    public void adjustNodes(java.util.List<Address> v) {
        Node n;
        boolean removed=false;

        synchronized(nodes) {
            for(int i=0; i < nodes.size(); i++) {
                n=nodes.get(i);
                if(!v.contains(n.addr)) {
                    System.out.println("adjustNodes(): node " + n + " was removed");
                    nodes.remove(n);
                    removed=true;
                }
            }
            if(removed)
                repaint();
        }
    }


    public void paintNode(Graphics g, Node n, FontMetrics fm) {
        String addr=n.addr != null? n.addr.toString() : null;
        int x=(int)n.x;
        int y=(int)n.y;
        g.setColor((n == pick)? selectColor : (n.fixed? fixedColor : nodeColor));
        int w=fm.stringWidth(n.lbl) + 10;

        if(addr != null)
            w=Math.max(w, fm.stringWidth(addr) + 10);

        if(addr == null)
            addr="<no address>";

        int h=(fm.getHeight() + 4) * 2;
        n.width=w;
        n.height=h;
        n.xloc=x - w / 2;
        n.yloc=y - h / 2;
        g.fillRect(x - w / 2, y - h / 2, w, h);
        g.setColor(Color.black);
        g.drawRect(x - w / 2, y - h / 2, w - 1, h - 1);
        g.drawString(n.lbl, x - (w - 10) / 2, (y - (h - 4) / 2) + fm.getAscent());
        g.drawString(addr, x - (w - 10) / 2, (y - (h - 4) / 2) + 2 * fm.getAscent() + 4);
    }


    public synchronized void update(Graphics g) {
        Dimension d=getSize();
        if((offscreen == null) || (d.width != offscreensize.width) ||
          (d.height != offscreensize.height)) {
            offscreen=createImage(d.width, d.height);
            offscreensize=d;
            offgraphics=offscreen.getGraphics();
            offgraphics.setFont(default_font);
        }

        offgraphics.setColor(getBackground());
        offgraphics.fillRect(0, 0, d.width, d.height);

        FontMetrics fm=offgraphics.getFontMetrics();
        for(int i=0; i < nodes.size(); i++) {
            paintNode(offgraphics, nodes.get(i), fm);
        }

        g.drawImage(offscreen, 0, 0, null);
    }


    public void mouseDragged(MouseEvent e) {
        Point p=e.getPoint();
        if(pick == null)
            return;

        pick.x=p.x;
        pick.y=p.y;
        repaint();
    }


    public void mousePressed(MouseEvent e) {
        Point p=e.getPoint();
        double bestdist=Double.MAX_VALUE, dist;
        int mod=e.getModifiers();
        Node n;


        if((mod & InputEvent.BUTTON3_MASK) != 0) {
            System.out.println("\nright button at " + p);
            n=findNodeAtPoint(p);
            if(n != null) {
                System.out.println("Found node at " + p + ": " + n);
                new SendDialog(findParent(), n, myname, wb.disp);
                repaint();
            }
            e.consume();
            return;
        }


        for(int i=0; i < nodes.size(); i++) {
            n=nodes.get(i);
            dist=(n.x - p.x) * (n.x - p.x) + (n.y - p.y) * (n.y - p.y);
            if(dist < bestdist) {
                pick=n;
                bestdist=dist;
            }
        }
        pickfixed=pick.fixed;
        pick.fixed=true;
        pick.x=p.x;
        pick.y=p.y;
        repaint();
    }


    public void mouseReleased(MouseEvent e) {
        Point p=e.getPoint();
        if(pick == null)
            return;

        pick.x=p.x;
        pick.y=p.y;
        pick.fixed=pickfixed;


        try {
            MethodCall call=new MethodCall("moveNode", new Object[]{pick}, new Class[]{Node.class});
            wb.disp.callRemoteMethods(null, call, new RequestOptions(ResponseMode.GET_ALL, 0));
        }
        catch(Exception ex) {
            log.error(ex.toString());
        }

        pick=null;
    }


    public void mouseEntered(MouseEvent e) {
    }

    public void mouseExited(MouseEvent e) {
    }

    public void mouseMoved(MouseEvent e) {
    }

    public void mouseClicked(MouseEvent e) {
    }


    public void start(String name) {
        myname=name;
        int xloc=(int)(10 + 250 * Math.random());
        int yloc=(int)(10 + 250 * Math.random());

        try {
            MethodCall call=new MethodCall("addNode",
                                           new Object[]{name,my_addr,Integer.valueOf(xloc),Integer.valueOf(yloc)},
                                           new Class[]{String.class,Address.class,int.class,int.class});
            wb.disp.callRemoteMethods(null, call, new RequestOptions(ResponseMode.GET_ALL, 0));
        }
        catch(Exception e) {
            log.error(e.toString());
        }
        repaint();
    }


    public void stop() {
        nodes.clear();
    }


    public void getState(OutputStream output) throws Exception {
        DataOutputStream out=new DataOutputStream(new BufferedOutputStream(output, 1000));
        try {
            synchronized(nodes) {
                Util.objectToStream(nodes, out);
            }
        }
        finally {
            Util.close(out);
        }
    }

    public void setState(InputStream input) throws Exception {
        java.util.List<Node> copy=(List<Node>)Util.objectFromStream(new DataInputStream(input));
        synchronized(nodes) {
            nodes.clear();
            nodes.addAll(copy);
        }
        repaint();
    }


    public void moveNode(Node n) {
        Node tmp;
        boolean changed=false;

        synchronized(nodes) {
            for(int i=0; i < nodes.size(); i++) {
                tmp=nodes.get(i);
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
