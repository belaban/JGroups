// $Id: Topology.java,v 1.9 2009/04/09 09:11:27 belaban Exp $


package org.jgroups.demos;


import org.jgroups.*;
import org.jgroups.blocks.PullPushAdapter;

import java.awt.*;
import java.awt.event.WindowEvent;
import java.awt.event.WindowListener;
import java.util.Vector;




/**
 * Demonstrates the membership service. Each member is represented by a rectangle that contains the
 * addresses of all the members. The coordinator (= oldest member in the group) is painted in blue.
 * New members can be started; all existing members will update their graphical appearance to reflect
 * the new membership. When the coordinator itself is killed, another one will take over (the next in rank).<p>
 * A nice demo is to start a number of Topology instances at the same time. All of them will be blue (all are
 * coordinators since they don't find each other). Then the MERGE2 protocol sets in and only one will retain
 * its coordinator role.
 * @todo Needs to be ported to Swing.
 * @author Bela Ban
 */
public class Topology extends Frame implements WindowListener, MembershipListener {
    private final Vector members=new Vector();
    private final Font myFont;
    private final FontMetrics fm;
    private final Color node_color=new Color(250, 220, 100);
    private boolean coordinator=false;
    private static final int NormalStyle=0;
    private static final int CheckStyle=1;
    private Channel channel;
    private Object my_addr=null;
    private static final String channel_name="FD-Heartbeat";


    public Topology() {
        addWindowListener(this);
        //g=getGraphics();
        fm=getFontMetrics(new Font("Helvetica", Font.PLAIN, 12));
        myFont=new Font("Helvetica", Font.PLAIN, 12);

    }


    public void addNode(Object member) {
        Object tmp;
        for(int i=0; i < members.size(); i++) {
            tmp=members.elementAt(i);
            if(member.equals(tmp))
                return;
        }
        members.addElement(member);
        repaint();
    }


    public void removeNode(Object member) {
        Object tmp;
        for(int i=0; i < members.size(); i++) {
            tmp=members.elementAt(i);
            if(member.equals(tmp)) {
                members.removeElement(members.elementAt(i));
                break;
            }
        }
        repaint();
    }


    public void drawNode(Graphics g, int x, int y, String label, int style) {
        Color old=g.getColor();
        int width, height;
        width=fm.stringWidth(label) + 10;
        height=fm.getHeight() + 5;

        g.setColor(node_color);

        g.fillRect(x, y, width, height);
        g.setColor(old);
        g.drawString(label, x + 5, y + 15);
        g.drawRoundRect(x - 1, y - 1, width + 1, height + 1, 10, 10);
        if(style == CheckStyle) {
            g.drawRoundRect(x - 2, y - 2, width + 2, height + 2, 10, 10);
            g.drawRoundRect(x - 3, y - 3, width + 3, height + 3, 10, 10);
        }
    }


    public void drawTopology(Graphics g) {
        int x=20, y=50;
        String label;
        Dimension box=getSize();
        Color old=g.getColor();

        if(coordinator) {
            g.setColor(Color.cyan);
            g.fillRect(11, 31, box.width - 21, box.height - 61);
            g.setColor(old);
        }

        g.drawRect(10, 30, box.width - 20, box.height - 60);
        g.setFont(myFont);

        for(int i=0; i < members.size(); i++) {
            label=members.elementAt(i).toString();
            drawNode(g, x, y, label, NormalStyle);
            y+=50;
        }


    }


    public void paint(Graphics g) {
        drawTopology(g);
    }


    /* ------------ Callbacks ------------- */


    public void viewAccepted(View view) {
        setState(view.getMembers());
    }

    public void suspect(Address suspected_mbr) {
    }

    public void block() {
    }


    public void setState(Vector mbrs) {
        members.removeAllElements();
        for(int i=0; i < mbrs.size(); i++)
            addNode(mbrs.elementAt(i));
        if(mbrs.size() <= 1 || (mbrs.size() > 1 && mbrs.elementAt(0).equals(my_addr)))
            coordinator=true;
        else
            coordinator=false;
        repaint();
    }


    public void coordinatorChosen() {
        coordinator=true;
        repaint();
    }


    public void windowActivated(WindowEvent e) {
    }

    public void windowClosed(WindowEvent e) {
    }

    public void windowClosing(WindowEvent e) {
        channel.close();
        System.exit(0);
    }

    public void windowDeactivated(WindowEvent e) {
    }

    public void windowDeiconified(WindowEvent e) {
    }

    public void windowIconified(WindowEvent e) {
    }

    public void windowOpened(WindowEvent e) {
    }


    public void start() throws Exception {

        //String props="TCP:" +
        //   "TCPPING(timeout=2000;num_initial_members=1;port_range=3;" +
        //  "initial_hosts=localhost[8880]):" +
        //  "FD:STABLE:NAKACK:FLUSH:GMS(join_timeout=12000):VIEW_ENFORCER:QUEUE";


        // test for pbcast
        //String props="UDP:PING:FD:" +
        //           "pbcast.PBCAST:UNICAST:FRAG:pbcast.GMS:" +
        //           "STATE_TRANSFER:QUEUE";


        // test for pbcast
        //String props="TCP:TCPPING(port_range=2;initial_hosts=daddy[8880],terrapin[8880]):FD:" +
        //            "pbcast.PBCAST:UNICAST:FRAG:pbcast.GMS:" +
        //           "STATE_TRANSFER:QUEUE";


        // test2 for pbcast
        //String props="UDP:PING:FD:" +
        //   "pbcast.PBCAST:UNICAST:FRAG:pbcast.GMS:" +
        //  "pbcast.STATE_TRANSFER";

        // String props=null; // default properties

        String props="udp.xml";

        channel=new JChannel(props);
        channel.connect(channel_name);
        new PullPushAdapter(channel, this);
        my_addr=channel.getAddress();
        if(my_addr != null)
            setTitle(my_addr.toString());
        pack();
        show();
    }


    public static void main(String[] args) {
        try {
            Topology top=new Topology();
            top.setLayout(null);
            top.setSize(240, 507);
            top.start();
        }
        catch(Exception e) {
            System.err.println(e);
            e.printStackTrace();
            System.exit(0);
        }
    }


}


