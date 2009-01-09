
package org.jgroups.demos;


import org.jgroups.blocks.Cache;
import org.jgroups.blocks.ReplCache;
import org.jgroups.jmx.JmxConfigurator;

import javax.management.MBeanServer;
import javax.swing.*;
import javax.swing.table.AbstractTableModel;
import java.awt.*;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.io.IOException;
import java.io.InputStream;
import java.lang.management.ManagementFactory;
import java.net.InetAddress;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * GUI demo of ReplCache
 * @author Bela Ban
 * @version $Id: ReplCacheDemo.java,v 1.6 2009/01/09 08:53:05 belaban Exp $
 */
public class ReplCacheDemo extends JPanel implements ActionListener {
    private ReplCache<String,String> cache;
    private static final String BASENAME="replcache";

    private JFrame frame;
    private JTable table;
    private JTextField key_field=new JTextField(10);
    private JTextField value_field=new JTextField(10);
    private JTextField repl_count_field=new JTextField("1", 3);
    private JTextField timeout_field=new JTextField("0", 5);

    private MyTableModel model=null;


    public ReplCacheDemo() {
        super();


    }

    private JButton createButton(String text) {
        JButton retval=new JButton(text);
        retval.addActionListener(this);
        return retval;
    }



    public void actionPerformed(ActionEvent event) {
        String command = event.getActionCommand();

        if(command.equals("Put")) {
            String key=key_field.getText();
            String value=value_field.getText();
            String repl_count=repl_count_field.getText();
            String timeout=timeout_field.getText();

            if(key == null || value == null)
                return;

            if(repl_count == null)
                repl_count="1";
            if(timeout == null)
                timeout="0";
            model.put(key, value, Integer.valueOf(repl_count), Long.valueOf(timeout));

            cache.put(key, value, Short.valueOf(repl_count), Long.valueOf(timeout));

            model.fireTableDataChanged();
        }
        else if(command.equals("Remove")) {
            int[] rows=table.getSelectedRows();
            if(rows != null) {
                for(int row: rows) {
                    String key=(String)model.getValueAt(row, 0);
                    if(key != null)
                        cache.remove(key);
                }
                model.remove(rows);
                model.fireTableDataChanged();
            }
        }
        else if(command.equals("Exit")) {
            if(cache != null)
                cache.stop();
            frame.dispose();
            System.exit(1); // or can we break out of mainLoop() somehow else ?
        }
    }





    private static class Entry {
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


        private final Map<String,Entry> map=new ConcurrentHashMap<String,Entry>();
        private static final long serialVersionUID=1314724464389654329L;

        MyTableModel() {
            map.put("name", new Entry("Bela", -1, 0));
            cache.put("name", "Bela", (short)-1, 0);

            map.put("id", new Entry("322649", 1, 5000));
            cache.put("id", "322649", (short)1, 5000);

            map.put("hobbies", new Entry("Tennis, Running, Swimming", -1, 0));
            cache.put("hobbies", "Tennis, Running, Swimming", (short)-1, 0);
        }


        public int getColumnCount() {
            return columnNames.length;
        }

        public int getRowCount() {
            return map.size();
        }

        public String getColumnName(int col) {
            return columnNames[col];
        }

        public void put(String key, String value, int repl_count, long timeout) {
            map.put(key, new Entry(value, repl_count,  timeout));
        }

        public void remove(int[] rows) {
            int count=0;
            if(rows == null || rows.length == 0)
                return;
            for(Iterator<String> it=map.keySet().iterator(); it.hasNext();) {
                it.next();
                for(int i=0; i < rows.length; i++) {
                    if(rows[i] == count) {
                        it.remove();
                        break;
                    }
                }
                count++;
            }
        }

        public Object getValueAt(int row, int col) {
            int count=0;
            Entry retval=null;
            String key=null;

            for(Map.Entry<String,Entry> entry: map.entrySet()) {
                if(count++ >= row) {
                    retval=entry.getValue();
                    key=entry.getKey();
                    break;
                }
            }
            if(retval == null)
                throw new IllegalArgumentException("row=" + row + ", col=" + col);
            switch(col) {
                case 0: return key;
                case 1: return retval.value;
                case 2: return retval.repl_count;
                case 3: return retval.timeout;
                default: return "n/a";
            }
        }

    }


    private void start(String props,
                       long rpc_timeout, long caching_time, boolean migrate_data, boolean use_l1_cache,
                       int l1_max_entries, long l1_reaping_interval,
                       int l2_max_entries, long l2_reaping_interval) throws Exception {
        MBeanServer server=ManagementFactory.getPlatformMBeanServer();

        cache=new ReplCache<String,String>(props, "replcache-cluster");
        cache.setCallTimeout(rpc_timeout);
        cache.setCachingTime(caching_time);
        cache.setMigrateData(migrate_data);
        JmxConfigurator.register(cache, server, BASENAME + ":name=cache");
        JmxConfigurator.register(cache.getL2Cache(), server, BASENAME + ":name=l2-cache");

        if(use_l1_cache) {
            Cache<String,String> l1_cache=new Cache<String,String>();
            cache.setL1Cache(l1_cache);
            if(l1_reaping_interval > 0)
                l1_cache.enableReaping(l1_reaping_interval);
            if(l1_max_entries > 0)
                l1_cache.setMaxNumberOfEntries(l1_max_entries);
            JmxConfigurator.register(cache.getL1Cache(), server, BASENAME + ":name=l1-cache");
        }

        if(l2_max_entries > 0 || l2_reaping_interval > 0) {
            Cache<String, ReplCache.Value<String>> l2_cache=cache.getL2Cache();
            if(l2_max_entries > 0)
                l2_cache.setMaxNumberOfEntries(l2_max_entries);
            if(l2_reaping_interval > 0)
                l2_cache.enableReaping(l2_reaping_interval);
        }


        Runtime.getRuntime().addShutdownHook(new Thread() {
            public void run() {
                cache.stop();
            }
        });

        cache.start();

        model=new MyTableModel();

        frame=new JFrame("ReplCacheDemo");
        frame.setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);

        setLayout(new BoxLayout(this, BoxLayout.Y_AXIS));

        table = new JTable(model);
        table.setPreferredScrollableViewportSize(new Dimension(500, 200));
        table.setFillsViewportHeight(true);
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
        setOpaque(true);

        frame.setContentPane(this);
        frame.pack();
        frame.setVisible(true);
    }


    public static void main(String[] args) throws Exception {
        String props="udp.xml";
        long rpc_timeout=1500L, caching_time=30000L;
        boolean migrate_data=true, use_l1_cache=true;
        int l1_max_entries=5000, l2_max_entries=-1;
        long l1_reaping_interval=-1, l2_reaping_interval=30000L;

        for(int i=0; i < args.length; i++) {
            if(args[i].equals("-props")) {
                props=args[++i];
                continue;
            }
            if(args[i].equals("-rpc_timeout")) {
                rpc_timeout=Long.parseLong(args[++i]);
                continue;
            }
            if(args[i].equals("-caching_time")) {
                caching_time=Long.parseLong(args[++i]);
                continue;
            }
            if(args[i].equals("-migrate_data")) {
                migrate_data=Boolean.parseBoolean(args[++i]);
                continue;
            }
            if(args[i].equals("-use_l1_cache")) {
                use_l1_cache=Boolean.parseBoolean(args[++i]);
                continue;
            }
            if(args[i].equals("-l1_max_entries")) {
                l1_max_entries=Integer.parseInt(args[++i]);
                continue;
            }
            if(args[i].equals("-l1_reaping_interval")) {
                l1_reaping_interval=Long.parseLong(args[++i]);
                continue;
            }
            if(args[i].equals("-l2_max_entries")) {
                l2_max_entries=Integer.parseInt(args[++i]);
                continue;
            }
            if(args[i].equals("-l2_reaping_interval")) {
                l2_reaping_interval=Long.parseLong(args[++i]);
                continue;
            }

            help();
            return;
        }



        ReplCacheDemo demo = new ReplCacheDemo();
        demo.start(props, rpc_timeout, caching_time,
                             migrate_data, use_l1_cache, l1_max_entries, l1_reaping_interval,
                             l2_max_entries, l2_reaping_interval);
        demo.mainLoop();
    }

    private void mainLoop() throws IOException {
          while(true) {
              int c;
              System.in.skip(System.in.available());
              System.out.println("\n[1] Put [2] Get [3] Remove [4] Dump [5] view [x] Exit");
              c=System.in.read();
              switch(c) {
                  case -1:
                      break;
                  case '1':
                      put();
                      break;
                  case '2':
                      String key=readString("key");
                      String val=cache.get(key);
                      System.out.println("val = " + val);
                      break;
                  case '3':
                      key=readString("key");
                      cache.remove(key);
                      break;
                  case '4':
                      System.out.println(cache.dump());
                      break;
                  case '5':
                      System.out.println("view = " + cache.getView());
                      break;
                  case 'x':
                      cache.stop();
                      return;
                  default:
                      break;
              }
          }
      }

      private void put() throws IOException {
          String key=readString("key");
          String val=readString("value");
          String tmp=readString("replication count");
          short count=Short.parseShort(tmp);
          tmp=readString("timeout");
          long timeout=Long.parseLong(tmp);
          cache.put(key, val, count, timeout);
      }



      private static void skip(InputStream in) throws IOException {
          System.in.skip(in.available());
      }

      private static String readString(String s) throws IOException {
          int c;
          boolean looping=true;
          StringBuilder sb=new StringBuilder();
          System.out.print(s + ": ");
          System.out.flush();
          skip(System.in);

          while(looping) {
              c=System.in.read();
              switch(c) {
                  case -1:
                  case '\n':
                  case 13:
                      looping=false;
                      break;
                  default:
                      sb.append((char)c);
                      break;
              }
          }

          return sb.toString();
      }


    private static void help() {
        System.out.println("ReplCacheServer [-help] [-props <props>] " +
                "[-rpc_timeout <ms>] [-caching_time <ms>] " +
                "[-migrate_data <true|false>] [-use_l1_cache <true|false>] " +
                "[-l1_max_entries <num>] [-l1_reaping_interval <ms>] " +
                "[-l2_max_entries <num>] [-l2_reaping_interval <ms>] ");
    }


}
