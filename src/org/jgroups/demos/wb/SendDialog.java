// $Id: SendDialog.java,v 1.4 2005/05/30 16:14:37 belaban Exp $

package org.jgroups.demos.wb;

import java.awt.*;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import org.jgroups.blocks.*;



public class SendDialog extends Dialog implements ActionListener {
    private final TextArea       msg=new TextArea("");
    private final Font     default_font=new Font("Helvetica",Font.PLAIN,12);
    private RpcDispatcher  disp=null;
    private String         groupname=null;
    private Node           dest=null;
    private String         sender=null;

    

    public SendDialog(Frame parent, Node dest, String src, RpcDispatcher disp, String groupname) {
	super(parent, "Send message to " + dest.lbl + " at " + dest.addr, true);

	Panel      p1=new Panel(), p2=new Panel();
	Button     send=new Button("Send"), send_all=new Button("Send to all");
	Button     cancel=new Button("Cancel");

	this.disp=disp;
	this.groupname=groupname;
	this.dest=dest;
	sender=src;

	send.setFont(default_font);
	send_all.setFont(default_font);
	cancel.setFont(default_font);
	msg.setFont(default_font);

	p1.setLayout(new BorderLayout());
	p1.add(msg);

	p2.setLayout(new FlowLayout());
	send.addActionListener(this);
	send_all.addActionListener(this);
	cancel.addActionListener(this);
	p2.add(send); p2.add(send_all); p2.add(cancel);
	
	add("Center", p1);
	add("South", p2);

	setSize(300, 150);

	Point my_loc=parent.getLocation();
	my_loc.x+=50;
	my_loc.y+=150;
	setLocation(my_loc);
	show();
    }



    public String getMessage() {
	String retval=msg.getText();
	return retval.length() > 0 ? retval : null;
    }
    

    public void actionPerformed(ActionEvent e) {
	String  command=e.getActionCommand();
	String  retval=msg.getText();

	if(retval == null || retval.length() < 1) {
	    dispose();
	    return;
	}

	try {
		MethodCall call = new MethodCall("displayMessage", new Object[] {sender, retval}, 
			new String[] {String.class.getName(), String.class.getName()});
	    if(command == "Send")
			disp.callRemoteMethod(dest.addr, call, GroupRequest.GET_FIRST, 0);
	    else if(command == "Send to all")		
			disp.callRemoteMethods(null, call, GroupRequest.GET_ALL, 0);
	}
	catch(Exception ex) {
	    System.err.println(ex);
	}
	
	dispose();
	return;
    }


}
