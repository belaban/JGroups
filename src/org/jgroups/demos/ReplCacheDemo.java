
package org.jgroups.demos;


import javax.swing.*;
import javax.swing.table.AbstractTableModel;
import javax.swing.event.ListSelectionEvent;
import javax.swing.event.ListSelectionListener;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.awt.*;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * GUI demo of ReplCache
 * @author Bela Ban
 * @version $Id: ReplCacheDemo.java,v 1.2 2009/01/08 14:49:29 belaban Exp $
 */
public class ReplCacheDemo extends JPanel
                                implements ActionListener { 
    private JTable table;
    private JTextField key_field=new JTextField(10);
    private JTextField value_field=new JTextField(10);
    private JTextField repl_count_field=new JTextField("1", 3);
    private JTextField timeout_field=new JTextField("0", 5);

    private MyTableModel model=new MyTableModel();


    public ReplCacheDemo() {
        super();
        setLayout(new BoxLayout(this, BoxLayout.Y_AXIS));

        table = new JTable(model);
        table.setPreferredScrollableViewportSize(new Dimension(500, 200));
        table.setFillsViewportHeight(true);
        table.getSelectionModel().addListSelectionListener(new RowListener());
        table.getColumnModel().getSelectionModel().
            addListSelectionListener(new ColumnListener());
        add(new JScrollPane(table));
        
        JPanel key=new JPanel(new FlowLayout(FlowLayout.LEFT));
        key.add(new JLabel("Key"));
        key.add(key_field);
        add(key);

        JPanel value=new JPanel(new FlowLayout(FlowLayout.LEFT));
        value.add(new JLabel("Value"));
        value.add(value_field);
        add(value);

        JPanel repl_count=new JPanel(new FlowLayout(FlowLayout.LEFT));
        repl_count.add(new JLabel("Replication count"));
        repl_count.add(repl_count_field);
        add(repl_count);

        JPanel timeout=new JPanel(new FlowLayout(FlowLayout.LEFT));
        timeout.add(new JLabel("Timeout"));
        timeout.add(timeout_field);
        add(timeout);

        JPanel buttons=new JPanel();
        buttons.add(createButton("Put"));
        buttons.add(createButton("Remove"));
        buttons.add(createButton("Exit"));
        add(buttons);

    }

    private JButton createButton(String text) {
        JButton retval=new JButton(text);
        retval.addActionListener(this);
        return retval;
    }



    public void actionPerformed(ActionEvent event) {
        String command = event.getActionCommand();
        System.out.println("command = " + command);

        if(command.equals("Put")) {
            System.out.println("key=" + key_field.getText() + ", value=" + value_field.getText() + ", K=" +
                    repl_count_field.getText() + ", timeout=" + timeout_field.getText());
        }
        else if(command.equals("Remove")) {
            model.change();
        }
        else if(command.equals("Exit")) {
            System.exit(1);
        }
    }



    private class RowListener implements ListSelectionListener {
        public void valueChanged(ListSelectionEvent event) {
            if (event.getValueIsAdjusting()) {
                return;
            }
        }
    }

    private class ColumnListener implements ListSelectionListener {
        public void valueChanged(ListSelectionEvent event) {
            if (event.getValueIsAdjusting()) {
                return;
            }
        }
    }

    private class Entry {
        private String value=null;
        private int repl_count=1;
        private long timeout=0;

        private Entry(String value, int repl_count, long timeout) {
            this.value=value;
            this.repl_count=repl_count;
            this.timeout=timeout;
        }
    }


    class MyTableModel extends AbstractTableModel {
        private String[] columnNames = {"Key",
                                        "Value",
                                        "K",
                                        "Timeout"};


//        private final Map<String,Entry> map=new ConcurrentHashMap<String,Entry>();
//
//        MyTableModel() {
//            map.put("name", new Entry("Bela", -1, 0));
//            map.put("id", new Entry("322649", 1, 5000));
//            map.put("hobbies", new Entry("Tennis, Running, Swimming", -1, 0));
//        }

        private Object[][] data = {
            {"name", "Bela",
             "-1", "5000"},
            {"id", "322649",
             "3", 5000},
            {"hobbies", "Tennis, Running, Biking",
             "-1", 0},
            {"country", "Switzerland",
             "1", 0},
            {"zip", "8280",
             "1", 0},
        };

        public int getColumnCount() {
            return columnNames.length;
        }

        public int getRowCount() {
            return data.length;
        }

        public String getColumnName(int col) {
            return columnNames[col];
        }

        public Object getValueAt(int row, int col) {
            return data[row][col];
        }

        /*
         * JTable uses this method to determine the default renderer/
         * editor for each cell.  If we didn't implement this method,
         * then the last column would contain text ("true"/"false"),
         * rather than a check box.
         */
        public Class getColumnClass(int c) {
            return getValueAt(0, c).getClass();
        }

        /*
         * Don't need to implement this method unless your table's
         * editable.
         */
        public boolean isCellEditable(int row, int col) {
            //Note that the data/cell address is constant,
            //no matter where the cell appears onscreen.
            if (col < 2) {
                return false;
            } else {
                return true;
            }
        }

        /*
         * Don't need to implement this method unless your table's
         * data can change.
         */
        public void setValueAt(Object value, int row, int col) {
            data[row][col] = value;
            fireTableCellUpdated(row, col);
        }


        public void change() {
            data = new String[][]{
                    {"name", "Michelle",
                            "-1", "5000"},
                    {"id2", "322649",
                            "3", "5000"},
                    {"hobbies", "Tennis, Running, Biking",
                            "-1", "0"},

                    {"zip", "8280",
                            "1", "0"},
            };

            fireTableDataChanged();
        }

    }

    /**
     * Create the GUI and show it.  For thread safety,
     * this method should be invoked from the
     * event-dispatching thread.
     */
    public static void main(String[] args) {
        //Disable boldface controls.
        UIManager.put("swing.boldMetal", Boolean.FALSE); 

        //Create and set up the window.
        JFrame frame = new JFrame("ReplCacheDemo");
        frame.setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);

        //Create and set up the content pane.
        ReplCacheDemo newContentPane = new ReplCacheDemo();
        newContentPane.setOpaque(true); //content panes must be opaque
        frame.setContentPane(newContentPane);

        //Display the window.
        frame.pack();
        frame.setVisible(true);
    }

//    public static void main(String[] args) {
//        //Schedule a job for the event-dispatching thread:
//        //creating and showing this application's GUI.
//        javax.swing.SwingUtilities.invokeLater(new Runnable() {
//            public void run() {
//                createAndShowGUI();
//            }
//        });
//    }
}
