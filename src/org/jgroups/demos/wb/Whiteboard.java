// $Id: Whiteboard.java,v 1.8 2009/04/09 09:11:38 belaban Exp $

package org.jgroups.demos.wb;

import org.jgroups.*;
import org.jgroups.blocks.*;
import org.jgroups.blocks.GroupRequest;
import org.jgroups.blocks.RpcDispatcher;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.applet.Applet;
import java.awt.*;
import java.awt.event.*;



/**
 * Shared whiteboard: members are represented by rectangles that contain their names and the OS/arch of
 * the machine they are working on. The boxes can be moved by anyone and by clicking on them, messages can
 * be sent to specific or all members. Whiteboard is both an application and an applet.
 * @author Bela Ban
 */
public class Whiteboard extends Applet implements ActionListener, MessageListener, MembershipListener,
						  ComponentListener, FocusListener {
    public RpcDispatcher           disp;
    Channel                        channel;
    GraphPanel                     panel;
    private Button                 leave_button;
    private Label                  mbr_label;
    private final Font             default_font=new Font("Helvetica",Font.PLAIN,12);
    private String                 props=null;
    public static final String     groupname="WbGrp";
    private boolean                application=false;
    Log                            log=LogFactory.getLog(getClass());


    public void receive(Message m) {
        ;
    }

    public byte[] getState() {
        panel.saveState();
        return panel.getState();
    }

    public void setState(byte[] new_state) {
        panel.setState(new_state);
    }


    private String getInfo() {
        StringBuilder ret = new StringBuilder();
        ret.append(" (" + System.getProperty("os.name") + ' ' + System.getProperty("os.version") +
                   ' ' + System.getProperty("os.arch") + ')');
        return ret.toString();
    }

    private Frame findParent() {
        Component retval = getParent();

        while (retval != null) {
            if (retval instanceof Frame)
                return (Frame) retval;
            retval = retval.getParent();
        }
        return null;
    }


    public Whiteboard() {	             // called when started as applet
    }

    public Whiteboard(String properties) {   // called when started as application
        application = true;
        props = properties;

    }


    public void init() {
        setLayout(new BorderLayout());
        panel = new GraphPanel(this);
        panel.setBackground(Color.white);
        add("Center", panel);
        Panel p = new Panel();
        leave_button = new Button("Exit");
        leave_button.setFont(default_font);
        leave_button.addActionListener(this);
        mbr_label = new Label("1 mbr(s)");
        mbr_label.setFont(default_font);
        p.add("South", leave_button);
        p.add("South", mbr_label);
        add("South", p);

        if (!application)
            props = getParameter("properties");
        if (props == null) {
            props = "udp.xml";
        }

        System.out.println("properties are " + props);

        try {
            channel = new JChannel(props);
            disp = new RpcDispatcher(channel, this, this, this);
            channel.connect(groupname);
            channel.getState(null, 0);
        } catch (Exception e) {
            log.error("Whiteboard.init(): " + e);
        }
        panel.my_addr = channel.getAddress();


        UserInfoDialog dlg = new UserInfoDialog(findParent());
        String n = dlg.getUserName();
        String info = getInfo();
        panel.start(n + info);


        addComponentListener(this);
        addFocusListener(this);
    }


    public void destroy() {
        if (disp != null) {
            try {
				MethodCall call = new MethodCall("removeNode", new Object[] {panel.my_addr}, new String[] {Object.class.getName()}); 
                disp.callRemoteMethods(null, call, GroupRequest.GET_ALL, 0);
            } catch (Exception e) {
                log.error(e);
            }
            channel.close();
            disp = null;
            if (panel != null) {
                panel.stop();
                panel = null;
            }
        }

    }


    public void repaint() {
        if (panel != null)
            panel.repaint();
    }


    public void actionPerformed(ActionEvent e) {
        String command = e.getActionCommand();

        if ("Exit".equals(command)) {
            try {
                setVisible(false);
                destroy();
                if (application) {
                    ((Frame) getParent()).dispose();
                    System.exit(0);
                }
            } catch (Exception ex) {
                log.error(ex);
            }

        } else
            System.out.println("Unknown action");
    }


    public void viewAccepted(View v) {
        if (v != null) {
            if (mbr_label != null)
                mbr_label.setText(v.size() + " mbr(s)");
        }
        panel.adjustNodes(v.getMembers());
    }

    public void suspect(Address obj) {
    }

    public void block() {
    }


    public void moveNode(Node n) {
        panel.moveNode(n);
    }


    public void addNode(String lbl, Address addr, int xloc, int yloc) {
        panel.addNode(lbl, addr, xloc, yloc);
    }


    public void removeNode(Object addr) {
        panel.removeNode(addr);
    }


    public void displayMessage(String sender, String msg) {
        new MessageDialog(findParent(), sender, msg);
        panel.repaint();
    }


    public void componentResized(ComponentEvent e) {
        if (panel != null) panel.repaint();
    }

    public void componentMoved(ComponentEvent e) {
    }

    public void componentShown(ComponentEvent e) {
        if (panel != null) panel.repaint();
    }

    public void componentHidden(ComponentEvent e) {
    }


    public void focusGained(FocusEvent e) {
        if (panel != null) panel.repaint();
    }


    public void focusLost(FocusEvent e) {
    }


    public static void main(String[] args) {
        String props = null;

        for (int i = 0; i < args.length; i++) {
            if ("-props".equals(args[i])) {
                props = args[++i];
                continue;
            }
            help();
            return;
        }

        Whiteboard wb = new Whiteboard(props);
        new ApplFrame("Whiteboard Application", wb);
    }

    static void help() {
        System.out.println("Whiteboard [-help] [-props <props>]");
    }


}


class ApplFrame extends Frame implements WindowListener, ComponentListener {
    Whiteboard wb = null;

    public ApplFrame(String title, Whiteboard wb) {
        super(title);
        this.wb = wb;
        add(wb);
        setSize(299, 299);
        setVisible(true);
        wb.init();
        setSize(300, 300);
        addWindowListener(this);
        addComponentListener(this);
    }


    public void windowOpened(WindowEvent e) {
    }

    public void windowClosing(WindowEvent e) {
        dispose();
        System.exit(0);
    }

    public void windowClosed(WindowEvent e) {
    }

    public void windowIconified(WindowEvent e) {
    }

    public void windowDeiconified(WindowEvent e) {
        wb.repaint();
    }

    public void windowActivated(WindowEvent e) {
        wb.repaint();
    }

    public void windowDeactivated(WindowEvent e) {
    }

    public void componentResized(ComponentEvent e) {
        wb.repaint();
    }

    public void componentMoved(ComponentEvent e) {
    }


    public void componentShown(ComponentEvent e) {
    }

    public void componentHidden(ComponentEvent e) {
    }


}


