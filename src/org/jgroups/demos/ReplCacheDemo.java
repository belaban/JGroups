
package org.jgroups.demos;


import org.jgroups.Receiver;
import org.jgroups.View;
import org.jgroups.blocks.Cache;
import org.jgroups.blocks.ReplCache;
import org.jgroups.jmx.JmxConfigurator;

import javax.management.MBeanServer;
import javax.swing.*;
import javax.swing.table.AbstractTableModel;
import javax.swing.table.TableModel;
import java.awt.*;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.awt.event.FocusAdapter;
import java.awt.event.FocusEvent;
import java.lang.management.ManagementFactory;
import java.util.Map;
import java.util.concurrent.ConcurrentMap;

/**
 * GUI demo of ReplCache
 * @author Bela Ban
 */
public class ReplCacheDemo extends JPanel implements ActionListener {
    private ReplCache<String,String> cache;
    private static final String BASENAME="replcache";

    private JFrame             frame;
    private final JTabbedPane  root_pane=new JTabbedPane();
    private JTable             table;
    private final JTextField   key_field=createTextField(null, 10);
    private final JTextField   value_field=createTextField(null, 10);
    private final JTextField   repl_count_field=createTextField("1", 3);
    private final JTextField   timeout_field=createTextField("0", 5);
    private final JTextField   perf_key_prefix=createTextField("key", 5);
    private final JTextField   perf_num_keys=createTextField("1000", 5);
    private final JTextField   perf_size=createTextField("1000", 5);
    private final JTextField   perf_repl_count_field=createTextField("1", 3);
    private final JTextField   perf_timeout_field=createTextField("0", 5);
    private final JTextArea    status=new JTextArea("Status area", 10, 5);
    private final JLabel       num_elements=new JLabel("0 elements");
    private MyTableModel       model;


    public void actionPerformed(ActionEvent event) {
        String command = event.getActionCommand();

        switch(command) {
            case "Put":
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

                cache.put(key, value, Short.valueOf(repl_count), Long.valueOf(timeout));
                break;
            case "Remove":
                int[] rows=table.getSelectedRows();
                if(rows != null) {
                    for(int row : rows) {
                        key=(String)model.getValueAt(row, 0);
                        if(key != null)
                            cache.remove(key);
                    }
                }
                break;
            case "Clear":
                clear();
                break;
            case "Rebalance":
                cache.mcastEntries();
                break;
            case "Reset":
                status.setText("");
                break;
            case "Start":
                startPerfTest();
                break;
            case "Stop":
                break;
            case "Exit":
                if(cache != null)
                    cache.stop();
                frame.dispose();
                System.exit(1); // or can we break out of mainLoop() somehow else ?

        }
    }

    private void clear() {
        cache.clear();
    }

    private void startPerfTest() {
        int   num_puts=1000;
        short repl_count=1;
        long  timeout=0;
        String key_prefix="key";

        String tmp=perf_key_prefix.getText();
        if(tmp != null)
            key_prefix=tmp;
        tmp=perf_num_keys.getText();
        if(tmp != null)
            num_puts=Integer.valueOf(tmp);
        tmp=perf_size.getText();
        Integer size;
        if(tmp != null)
            size=Integer.valueOf(tmp);
        tmp=perf_repl_count_field.getText();
        if(tmp != null)
            repl_count=Short.valueOf(tmp);
        tmp=perf_timeout_field.getText();
        if(tmp != null)
            timeout=Long.valueOf(tmp);

        long start=System.currentTimeMillis();
        for(int i=0; i < num_puts; i++) {
            String key=key_prefix + "-" + i;
            String value="val-" + i;
            cache.put(key, value, repl_count,  timeout);
        }
        long diff=System.currentTimeMillis() - start;
        status.setText("It took " + diff + " ms to insert " + num_puts + " elements");
    }


    private void start(String props, String cluster_name,
                       long rpc_timeout, long caching_time, boolean migrate_data, boolean use_l1_cache,
                       int l1_max_entries, long l1_reaping_interval,
                       int l2_max_entries, long l2_reaping_interval) throws Exception {
        MBeanServer server=ManagementFactory.getPlatformMBeanServer();

        cache=new ReplCache<>(props, cluster_name);
        cache.setCallTimeout(rpc_timeout);
        cache.setCachingTime(caching_time);
        cache.setMigrateData(migrate_data);
        JmxConfigurator.register(cache, server, BASENAME + ":name=cache");
        JmxConfigurator.register(cache.getL2Cache(), server, BASENAME + ":name=l2-cache");

        if(use_l1_cache) {
            Cache<String,String> l1_cache=new Cache<>();
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

        model=new MyTableModel<String,String>();
        model.setMap(cache.getL2Cache().getInternalMap());
        cache.addChangeListener(model);

        frame=new JFrame("ReplCacheDemo");
        frame.setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);

        setLayout(new BoxLayout(this, BoxLayout.Y_AXIS));

        table=new MyTable(model);
        table.setPreferredScrollableViewportSize(new Dimension(500, 200));
        // table.setFillsViewportHeight(true); // JDK 6 specific
        table.setShowGrid(false);
        table.setFont(table.getFont().deriveFont(Font.BOLD));
        add(new JScrollPane(table));

        JPanel key=new JPanel(new FlowLayout(FlowLayout.LEFT));
        key.add(new JLabel("Key  "));
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
        JButton put_button=createButton("Put");
        buttons.add(createButton("Put"));
        buttons.add(createButton("Remove"));
        buttons.add(createButton("Clear"));
        buttons.add(createButton("Rebalance"));
        buttons.add(createButton("Exit"));
        buttons.add(num_elements);
        add(buttons);
        setOpaque(true);

        root_pane.addTab("Data", this);
        JPanel perf_panel=new JPanel();
        perf_panel.setLayout(new BoxLayout(perf_panel, BoxLayout.Y_AXIS));
        perf_panel.setOpaque(true);
        root_pane.addTab("Perf test", perf_panel);

        perf_panel.add(status);
        status.setForeground(Color.BLUE);

        JPanel prefix=new JPanel(new FlowLayout(FlowLayout.LEFT));
        prefix.add(new JLabel("Key prefix"));
        prefix.add(perf_key_prefix);
        perf_panel.add(prefix);

        JPanel keys=new JPanel(new FlowLayout(FlowLayout.LEFT));
        keys.add(new JLabel("Number of keys to insert"));
        keys.add(perf_num_keys);
        perf_panel.add(keys);

        JPanel size=new JPanel(new FlowLayout(FlowLayout.LEFT));
        size.add(new JLabel("Size of each key (bytes)"));
        size.add(perf_size); size.add(new JLabel("    (ignored for now)"));
        perf_panel.add(size);

        JPanel perf_repl_count=new JPanel(new FlowLayout(FlowLayout.LEFT));
        perf_repl_count.add(new JLabel("Replication count"));
        perf_repl_count.add(perf_repl_count_field);
        perf_panel.add(perf_repl_count);

        JPanel perf_timeout=new JPanel(new FlowLayout(FlowLayout.LEFT));
        perf_timeout.add(new JLabel("Timeout"));
        perf_timeout.add(perf_timeout_field);
        perf_panel.add(perf_timeout);

        JPanel perf_buttons=new JPanel(new FlowLayout(FlowLayout.LEFT));
        perf_buttons.add(createButton("Start"));
        perf_buttons.add(createButton("Stop"));
        perf_buttons.add(createButton("Reset"));
        perf_buttons.add(createButton("Exit"));
        perf_panel.add(perf_buttons);

        frame.setContentPane(root_pane);
        frame.pack();
        frame.getRootPane().setDefaultButton(put_button);
        frame.setVisible(true);
        setTitle("ReplCacheDemo");

        cache.addReceiver(new Receiver() {
            public void viewAccepted(View new_view) {
                setTitle("ReplCacheDemo");
            }
        });
    }



    private JButton createButton(String text) {
        JButton retval=new JButton(text);
        retval.addActionListener(this);
        return retval;
    }

    private static JTextField createTextField(String name, int length) {
        JTextField retval=new JTextField(name, length);
        retval.addFocusListener(new MyFocusListener(retval));
        return retval;
    }



    public static void main(String[] args) throws Exception {
        String props="udp.xml";
        String cluster_name="replcache-cluster";
        long rpc_timeout=1500L, caching_time=30000L;
        boolean migrate_data=true, use_l1_cache=true;
        int l1_max_entries=5000, l2_max_entries=-1;
        long l1_reaping_interval=-1, l2_reaping_interval=30000L;

        for(int i=0; i < args.length; i++) {
            if(args[i].equals("-props")) {
                props=args[++i];
                continue;
            }
            if(args[i].equals("-cluster_name")) {
                cluster_name=args[++i];
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
        demo.start(props, cluster_name, rpc_timeout, caching_time,
                   migrate_data, use_l1_cache, l1_max_entries, l1_reaping_interval,
                   l2_max_entries, l2_reaping_interval);
    }


    void setTitle(String title) {
        String local_addr=cache != null? cache.getLocalAddressAsString() : null;
        int num_nodes=cache != null? cache.getClusterSize() : 0;
        frame.setTitle(title + ": " + local_addr + " (" + num_nodes + ")");
    }


    private static void help() {
        System.out.println("ReplCacheDemo [-help] [-props <props>] [-cluster_name <name>]" +
                "[-rpc_timeout <ms>] [-caching_time <ms>] " +
                "[-migrate_data <true|false>] [-use_l1_cache <true|false>] " +
                "[-l1_max_entries <num>] [-l1_reaping_interval <ms>] " +
                "[-l2_max_entries <num>] [-l2_reaping_interval <ms>] ");
    }



    private static class MyFocusListener extends FocusAdapter {
        private final JTextField field;

        public MyFocusListener(JTextField field) {
            this.field=field;
        }

        public void focusGained(FocusEvent e) {
            String value=field.getText();
            if(value != null && !value.isEmpty()) {
                field.selectAll();
            }
        }

    }

    private static class MyTable extends JTable {

        private MyTable(TableModel dm) {
            super(dm);
        }

        public boolean getScrollableTracksViewportHeight() {
            Container viewport=getParent();
            return viewport instanceof JViewport && getPreferredSize().height < viewport.getHeight();
        }

    }

    private class MyTableModel<K,V> extends AbstractTableModel implements ReplCache.ChangeListener {
        private ConcurrentMap<K, Cache.Value<ReplCache.Value<V>>> map;
        private final String[] columnNames = {"Key", "Value", "Replication Count", "Timeout"};
        private static final long serialVersionUID=1314724464389654329L;

        public void setMap(ConcurrentMap<K, Cache.Value<ReplCache.Value<V>>> map) {
            this.map=map;
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


        public Object getValueAt(int row, int col) {
            int count=0;

            for(Map.Entry<K,Cache.Value<ReplCache.Value<V>>> entry: map.entrySet()) {
                if(count++ >= row) {
                    K key=entry.getKey();
                    Cache.Value<ReplCache.Value<V>> val=entry.getValue();
                    ReplCache.Value<V> tmp=val.getValue();
                    switch(col) {
                        case 0:  return key;
                        case 1:
                            V value=tmp.getVal();
                            return value instanceof byte[]? ((byte[])value).length + " bytes" : value;
                        case 2:  return tmp.getReplicationCount();
                        case 3:  return val.getTimeout();
                        default: return "n/a";
                    }
                }
            }
            throw new IllegalArgumentException("row=" + row + ", col=" + col);

        }

        public void changed() {
            fireTableDataChanged();
            num_elements.setText(cache.getL2Cache().getSize() + " elements");
        }
    }
    

}
