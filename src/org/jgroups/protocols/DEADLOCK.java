// $Id: DEADLOCK.java,v 1.8 2007/01/11 12:57:14 belaban Exp $

package org.jgroups.protocols;


import org.jgroups.Address;
import org.jgroups.Event;
import org.jgroups.View;
import org.jgroups.blocks.GroupRequest;
import org.jgroups.blocks.MethodCall;
import org.jgroups.stack.RpcProtocol;
import org.jgroups.util.RspList;

import java.awt.*;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.util.Vector;




class MyFrame extends Frame {
    final List        list=new List();
    final Label       result=new Label("Result:          ");
    final Button      send=new Button("Send Request");
    final Button quit=new Button("Quit");
    final Panel       button_panel=new Panel();
    final Panel       main_panel=new Panel();
    DEADLOCK    deadlock=null;
    Vector      members=null;
    
    
    MyFrame(String title, DEADLOCK deadlock) {
	this.deadlock=deadlock;
	setSize(300, 200);
	setTitle(title);
	setBackground(Color.white);
	setFont(new Font("Helvetica", Font.PLAIN, 12));
	setLayout(new BorderLayout());
	main_panel.setLayout(new GridLayout(0, 2));
	main_panel.add(result);
	main_panel.add(list);
	button_panel.add(send);
	button_panel.add(quit);
	add("Center", main_panel);
	add("South", button_panel);
	addEventHandlers();
    }


    void addEventHandlers() {

	quit.addActionListener(
			       new ActionListener() {
	    public void actionPerformed(ActionEvent e) {dispose();}
	});

	send.addActionListener(
			       new ActionListener() {
	    public void actionPerformed(ActionEvent e) {
		Address dest;
		int     res;
		int     index=-1;
		
		index=list.getSelectedIndex();
		if(index == -1)
		    return;
		dest=members != null ? (Address)members.elementAt(index) : null;
		if(dest != null) {
		    res=deadlock.sendRequest(dest);
		    setResult(res);
		}
	    }
	});

	
    }


    void setResult(int res) {
	result.setText("Result: " + res);
    }

    void setMembers(Vector members) {
	list.removeAll();
	for(int i=0; i < members.size(); i++)
	    list.add(members.elementAt(i).toString());
	this.members=members;
    }

}





