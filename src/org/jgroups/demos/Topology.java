

package org.jgroups.demos;


import org.jgroups.Address;
import org.jgroups.JChannel;
import org.jgroups.Receiver;
import org.jgroups.View;

import java.awt.*;
import java.awt.event.WindowEvent;
import java.awt.event.WindowListener;
import java.util.ArrayList;




/**
 * Demonstrates the membership service. Each member is represented by a rectangle that contains the
 * addresses of all the members. The coordinator (= oldest member in the group) is painted in blue.
 * New members can be started; all existing members will update their graphical appearance to reflect
 * the new membership. When the coordinator itself is killed, another one will take over (the next in rank).<p>
 * A nice demo is to start a number of Topology instances at the same time. All of them will be blue (all are
 * coordinators since they don't find each other). Then the MERGE3 protocol sets in and only one will retain
 * its coordinator role.
 * @todo Needs to be ported to Swing.
 * @author Bela Ban
 */
public class Topology extends Frame implements WindowListener {
    private final java.util.List<Address> members=new ArrayList<>();
    private final Font myFont;
    private final FontMetrics fm;
    private final Color node_color=new Color(250, 220, 100);
    private boolean coordinator=false;
    private static final int NormalStyle=0;
    private static final int CheckStyle=1;
    private JChannel channel;
    private Object my_addr=null;
    private String props="udp.xml";
    private String name;
    private static final String channel_name="FD-Heartbeat";


    public Topology(String props, String name) {
        this.props=props;
        this.name=name;
        addWindowListener(this);
        fm=getFontMetrics(new Font("Helvetica", Font.PLAIN, 12));
        myFont=new Font("Helvetica", Font.PLAIN, 12);
    }


    public void addNode(Address member) {
        Address tmp;
        for(int i=0; i < members.size(); i++) {
            tmp=members.get(i);
            if(member.equals(tmp))
                return;
        }
        members.add(member);
        repaint();
    }


    public void removeNode(Object member) {
        Object tmp;
        for(int i=0; i < members.size(); i++) {
            tmp=members.get(i);
            if(member.equals(tmp)) {
                members.remove(members.get(i));
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
            label=members.get(i).toString();
            drawNode(g, x, y, label, NormalStyle);
            y+=50;
        }


    }


    public void paint(Graphics g) {
        drawTopology(g);
    }


    /* ------------ Callbacks ------------- */




    public void coordinatorChosen() {
        coordinator=true;
        repaint();
    }


    public void windowActivated(WindowEvent e) {
    }

    public void windowClosed(WindowEvent e) {
    }

    public void windowClosing(WindowEvent e) {
        setVisible(false);
        dispose();
        channel.close();
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
        channel=new JChannel(props).name(name);

        channel.setReceiver(new Receiver() {
            public void viewAccepted(View view) {
                setInternalState(view.getMembers());
            }

            public void setInternalState(java.util.List<Address> mbrs) {
                members.clear();
                for(Address mbr : mbrs)
                    addNode(mbr);
                coordinator=mbrs.size() <= 1 || (mbrs.size() > 1 && mbrs.iterator().next().equals(my_addr));
                repaint();
            }
        });
        channel.connect(channel_name);
        my_addr=channel.getAddress();
        if(my_addr != null)
            setTitle(my_addr.toString());
        pack();
        setVisible(true);
    }


    public static void main(String[] args) {
        String name=null, props="udp.xml";

        for(int i=0; i < args.length; i++) {
            if(args[i].equals("-name")) {
                name=args[++i];
                continue;
            }
            if(args[i].equals("-props")) {
                props=args[++i];
                continue;
            }
            System.out.println("Topology [-props config file] [-name name]");
            return;
        }
        try {
            Topology top=new Topology(props, name);
            top.setLayout(null);
            top.setSize(240, 507);
            top.start();
        }
        catch(Exception e) {
            e.printStackTrace();
            System.exit(0);
        }
    }


}


