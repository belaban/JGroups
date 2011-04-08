
package org.jgroups.demos.wb;

import java.awt.*;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;



public class UserInfoDialog extends Dialog implements ActionListener {

    final Button              ok=new Button("OK");
    final Label               l=new Label("Name: ");
    final TextField           name=new TextField("");
    private final Font  default_font=new Font("Helvetica",Font.PLAIN,12);
    

    public UserInfoDialog(Frame parent) {
        super(parent, "Input", true);
        setLayout(null);

        l.setFont(default_font);
        l.setSize(50, 30);
        l.setLocation(30, 50);

        name.setFont(default_font);
        name.setSize(150, 30);
        name.setLocation(90, 50);
        //name.selectAll();

        ok.setFont(default_font);
        ok.setSize(50, 30);
        ok.setLocation(30, 90);
	

        add(l);	add(name); add(ok);
        ok.addActionListener(this);
        setSize(300, 150);

        Point my_loc=parent.getLocation();
        my_loc.x+=50;
        my_loc.y+=150;
        setLocation(my_loc);
        show();
    }


    public String getUserName() {
        return name.getText();
    }
    

    public void actionPerformed(ActionEvent e) {
        String     command=e.getActionCommand();
        String     tmp=name.getText();

        if(command == "OK") {
            if(tmp == null || tmp.length() < 1)
                ;
            else
                dispose();
        }
        else
            System.err.println("UserInfoDialog.actionPerfomed(): unknown action " +
                                 e.getActionCommand());
    }


}
