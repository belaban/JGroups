// $Id: ProtocolView.java,v 1.3 2006/11/17 13:39:18 belaban Exp $

package org.jgroups.debug;

import org.jgroups.Event;
import org.jgroups.stack.Protocol;
import org.jgroups.stack.ProtocolObserver;

import javax.swing.*;
import javax.swing.table.DefaultTableModel;


/**
 * Graphical view of a protocol instance
 * @author Bela Ban, created July 22 2000
 */
public class ProtocolView implements ProtocolObserver {
    final DefaultTableModel model;
    int my_index=-1;
    Protocol prot=null;
    String prot_name=null;
    final JButton down_label=new JButton("0");
    final JButton up_label=new JButton("0");
    long tot_up=0, tot_down=0;


    public ProtocolView(Protocol p, DefaultTableModel model, int my_index) {
        prot=p;
        prot_name=p.getName();
        this.model=model;
        this.my_index=my_index;
    }



    /* ----------------------- ProtocolObserver interface ----------------------- */
    public void setProtocol(Protocol prot) {
        this.prot=prot;
    }


    public boolean up(Event evt, int num_evts) {
        tot_up++;
        model.setValueAt(String.valueOf(tot_up), my_index, 2);
        return true;
    }


    public boolean passUp(Event evt) {
        return true;
    }


    public boolean down(Event evt) {
        tot_down++;
        model.setValueAt(String.valueOf(tot_down), my_index, 3);
        return true;
    }

    public boolean passDown(Event evt) {
        return true;
    }

    /* ------------------- End of ProtocolObserver interface ---------------------- */


    public String toString() {
        return prot != null? prot.getName() : "<n|a>";
    }


}
