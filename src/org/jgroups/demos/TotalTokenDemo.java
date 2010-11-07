//$Id: TotalTokenDemo.java,v 1.13 2009/06/17 16:20:13 belaban Exp $

package org.jgroups.demos;

import org.jgroups.*;
import org.jgroups.stack.Protocol;
import org.jgroups.stack.ProtocolStack;
import org.jgroups.util.Util;

import javax.swing.*;
import java.awt.*;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.io.Serializable;
import java.util.Iterator;
import java.util.Random;
import java.util.Vector;


/**
 *<p>
 * Demonstration of TOTAL_TOKEN protocol stack implementing total
 * order. TotalTokenDemo could however be used by any other
 * stack configuration which does not neccesarily have to satisfy
 * total order. If using stack configuration other than TOTAL_TOKEN
 * an appropriate xml configuration file should be used. See -help for
 * details.
 * <p>
 * TotalTokenDemo will verify :
 * <p>
 * total ordering of messages - by computing a color to be displayed
 * in a gui window.
 * <p>
 * virtual synchrony - by displaying number of messages transmitted
 * in last view.
 * <p>
 *
 *@author Vladimir Blagojevic vladimir@cs.yorku.ca
 *@author Ivan Bilenjkij  ivan@ibossa.com
 *@version $Revision: 1.13 $
 *
 *@see org.jgroups.protocols.TOTAL_TOKEN
 *
 **/


public class TotalTokenDemo extends JFrame implements Runnable
{
    private JChannel channel;
    //main tabbed pane

    final JTabbedPane tabbedPane;

    private ReceiverThread receiverThread;

    private ColorPanel colorPanel;
    private final ControlPanel control;
    private int mSize = 1024;
    private volatile boolean transmitting = false;
    private final String channelProperties;
    private Dimension preffered;

    public TotalTokenDemo(String props)
    {
        super();
        tabbedPane = new JTabbedPane();
        control = new ControlPanel();
        channelProperties = props;

        try
        {
            channel = new JChannel(channelProperties);
        }
        catch (ChannelException e)
        {
            System.err.println("Could not create channel, exiting ....");
            e.printStackTrace(System.err);
        }

        addPanel("Control", control);
        getContentPane().add(tabbedPane);
        connect();

    }
    public void addPanel(String name, JPanel panel)
    {


        if(tabbedPane.getTabCount() == 0)
        {
            preffered = panel.getPreferredSize();
        }


        panel.setPreferredSize(preffered);
        tabbedPane.add(name,panel);
    }

    public JChannel getChannel()
    {
        return channel;
    }


    public void connect()
    {
        try
        {
            channel.connect("TOTAL_TOKEN_DEMO_GROUP");
        }
        catch (ChannelException e)
        {
            e.printStackTrace();
        }
        receiverThread = new ReceiverThread();
        receiverThread.start();
        Address a = channel.getAddress();
        if(a != null)   setTitle(a.toString());
        else setTitle("Not connected");
        control.connected();
    }
    public void run()
    {
        Random r = new Random();

        while (true)
        {
            Util.sleep(10);
            try
            {
                if (transmitting)
                {
                    channel.send(new Message(null, null, new TotalPayload(r.nextInt(255))));
                }
                else
                {
                    Util.sleep(200);
                }

            }
            catch (Exception e)
            {
                e.printStackTrace();
            }
        }
    }

    public void disconnect()
    {
        transmitting = false;
        receiverThread.shutDown();
        channel.disconnect();
        control.disconnected();
        setTitle("Not connected");
    }

    private class ReceiverThread extends Thread
    {
        volatile boolean running = true;
        Thread nullifier = null;
        private long startTimeThroughput = System.currentTimeMillis();
        private final long oneSecond = 1000;
        private long throughput = 1;

        public ReceiverThread()
        {
            nullifier = new Thread(new Runnable()
            {
                public void run()
                {
                    //nullifies throughput display
                    while (running)
                    {
                        Util.sleep(2000);
                        if ((System.currentTimeMillis() - startTimeThroughput) > 2000)
                        {
                            control.throughput.setText("0 KB/sec");
                        }
                    }
                }
            });
            nullifier.start();
        }

        public void shutDown()
        {
            running = false;
        }

        private void measureThrougput(long size)
        {
            if ((System.currentTimeMillis() - startTimeThroughput) > oneSecond)
            {
                control.throughput.setText("" + (throughput / 1024) + " KB/sec");
                startTimeThroughput = System.currentTimeMillis();
                throughput = 0;
            }
            else
            {
                throughput += size;
            }
        }


        public void run()
        {
            Object tmp;
            Message msg = null;
            int counter = 0;
            Vector v = new Vector();
            while (running)
            {
                Util.sleep(10);
                try
                {

                    tmp = channel.receive(0);
                    if (tmp == null) continue;

                    if (tmp instanceof View)
                    {
                        View vw = (View) tmp;
                        control.viewNumber.setText(String.valueOf(vw.getVid().getId()));
                        control.numMessagesInLastView.setText(String.valueOf(counter));
                        counter = 0;
                        v.clear();
                        continue;
                    }

                    if (!(tmp instanceof Message))
                        continue;

                    msg = (Message) tmp;

                    measureThrougput(msg.size());
                    TotalPayload p =null;

                    p=(TotalPayload)msg.getObject();
                    v.addElement(new Integer(p.getRandomSequence()));
                    int size = v.size();
                    if (size % 50 == 0)
                    {
                        int value = 0;
                        int i = 0;
                        Iterator iter = v.iterator();
                        while (iter.hasNext())
                        {
                            i++;
                            int seq = ((Integer) iter.next()).intValue();
                            if (i % 2 == 0)
                            {
                                value *= seq;
                            }
                            else if (i % 3 == 0)
                            {
                                value -= seq;
                            }
                            else
                                value += seq;
                        }
                        v.clear();
                        value = Math.abs(value);
                        int r = value % 85;
                        int g = value % 170;
                        int b = value % 255;
                        colorPanel.setSeq(r, g, b);
                    }
                    counter++;
                }
                catch (ChannelNotConnectedException e)
                {
                    e.printStackTrace();
                }
                catch (ChannelClosedException e)
                {
                    e.printStackTrace();
                }
                catch (TimeoutException e)
                {
                    e.printStackTrace();
                }
            }

        }
    }

    public static class TotalPayload implements Serializable
    {
        private final int seqRandom;

        public TotalPayload(int random)
        {
            seqRandom = random;
        }

        public int getRandomSequence()
        {
            return seqRandom;
        }

    }

    class TransmitAction extends AbstractAction
    {

        private static final String TRANSMIT_OFF = "transmit off";
        private static final String TRANSMIT_ON = "transmit on";

        TransmitAction()
        {
            putValue(NAME, TRANSMIT_OFF);
        }

        public void actionPerformed(ActionEvent e)
        {
            if (getValue(NAME) == TRANSMIT_OFF)
            {
                putValue(NAME, TRANSMIT_ON);
                transmitting = true;
            }
            else
            {
                putValue(NAME, TRANSMIT_OFF);
                transmitting = false;
            }
        }


    }

    class ControlPanel extends JPanel
    {

        private static final String DISCONNECT = "Disconnect";
        private static final String CONNECT = "Connect";
        final JTextField numMessagesInLastView;
        final JTextField throughput;
        final JTextField viewNumber;
        final JTextField messageSize;
        final JTextField state;
        final JButton transmit;
        final JButton connectButton;

        JTabbedPane pane;

        public ControlPanel()
        {
            super();

            //Layout the labels in a panel
            JPanel labelPane = new JPanel();
            labelPane.setLayout(new GridLayout(0, 1));
            labelPane.add(new JLabel("Message size"));
            labelPane.add(new JLabel("Current view"));
            labelPane.add(new JLabel("Throughput"));
            labelPane.add(new JLabel("Last view count"));


            colorPanel = new ColorPanel();
            connectButton = new JButton(DISCONNECT);

            connectButton.addActionListener(new ActionListener()
            {
                public void actionPerformed(ActionEvent e)
                {
                    JButton b = (JButton) e.getSource();
                    String current_state = b.getText();
                    if (CONNECT.equals(current_state))
                    {
                        connect();
                    }
                    else if (DISCONNECT.equals(current_state))
                    {
                        disconnect();
                    }
                }
            });

            transmit = new JButton(new TransmitAction());
            labelPane.add(connectButton);
            labelPane.add(transmit);


            int size = 10;
            messageSize = new JTextField(size);
            messageSize.setText("" + mSize);
            messageSize.addActionListener(new ActionListener()
            {
                /**
                 * Invoked when an action occurs.
                 */
                public void actionPerformed(ActionEvent e)
                {
                    mSize = Integer.parseInt(messageSize.getText());
                }
            });
            viewNumber = new JTextField(size);
            viewNumber.setEditable(false);
            throughput = new JTextField(size);
            throughput.setEditable(false);
            numMessagesInLastView = new JTextField(size);
            numMessagesInLastView.setEditable(false);

            state = new JTextField(size);
            state.setEditable(false);


            //Layout the text fields in a panel
            JPanel fieldPane = new JPanel();
            fieldPane.setLayout(new GridLayout(0, 1));
            fieldPane.add(messageSize);
            fieldPane.add(viewNumber);
            fieldPane.add(throughput);
            fieldPane.add(numMessagesInLastView);
            fieldPane.add(state);
            fieldPane.add(colorPanel);



            //Put the panels in another panel, labels on left,
            //text fields on right
            JPanel contentPane = new JPanel();
            contentPane.setBorder(BorderFactory.createTitledBorder("Control"));
            contentPane.setLayout(new BorderLayout());
            contentPane.add(labelPane, BorderLayout.CENTER);
            contentPane.add(fieldPane, BorderLayout.EAST);
            this.setLayout(new BorderLayout());
            this.add(contentPane);
        }

        public void connected()
        {
            connectButton.setText(DISCONNECT);
            state.setText("connected ok");
        }

        public void disconnected()
        {
            connectButton.setText(CONNECT);
            state.setText("disconnected ok");
        }
    }

    class ColorPanel extends JPanel
    {

        long seq1,seq2,seq3;

        public ColorPanel()
        {
            super();
            setOpaque(false);
            this.setLayout(new BorderLayout());
            //setBorder(BorderFactory.createLineBorder(Color.black));
        }

        public Dimension getPreferredSize()
        {
            Dimension layoutSize = super.getPreferredSize();
            int max = Math.max(layoutSize.width, layoutSize.height);
            return new Dimension(max + 20, max + 20);
        }

        public void setSeq(long seq1, long seq2, long seq3)
        {
            this.seq1 = seq1;
            this.seq2 = seq2;
            this.seq3 = seq3;
            this.repaint();
        }

        protected void paintComponent(Graphics g)
        {
            Dimension size = this.getSize();
            int x = 0;
            int y = 0;
            g.setColor(new Color((int) seq1, (int) seq2, (int) seq3));
            g.fillRect(x, y, size.width, size.height);
        }
    }
    class StackPanel extends JPanel
    {
        final ProtocolStack stack;
        public StackPanel(JChannel channel)
        {
            super();
            setBorder(BorderFactory.createTitledBorder("ProtocolStack"));
            this.setLayout(new GridLayout(0, 2));
            this.stack = channel.getProtocolStack();
            Iterator iter = stack.getProtocols().iterator();
            String debugLevels [] = new String[]{"DEBUG","INFO","ERROR"};
            while (iter.hasNext())
            {
                Protocol p = (Protocol) iter.next();
                JLabel field = new JLabel(p.getName());
                JComboBox pane = new JComboBox(debugLevels);
                this.add(field);
                this.add(pane);

            }
        }
    }

    static void help()
    {
        System.out.println("\nTotalTokenDemo [-help] [-props <protocol stack definition>]");
        System.out.println("-props: argument can be an old-style protocol stack specification, or it can be " +
                           "a URL. In the latter case, the protocol specification will be read from the URL\n");
    }


    public static void main(String args[])
    {
        String props = null;

        for (int i = 0; i < args.length; i++)
        {
            if ("-help".equals(args[i]))
            {
                help();
                return;
            }
            if ("-props".equals(args[i]))
            {
                props = args[++i];
                continue;
            }
            help();
            return;
        }

        if (props == null)
        {
            props = "udp.xml";
        }



        TotalTokenDemo ttd = new TotalTokenDemo(props);
        //StackPanel not_done_yet = new StackPanel(ttd.getChannel());
        //ttd.addPanel("Debug", not_done_yet);
        ttd.setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
        ttd.pack();
        ttd.show();
        new Thread(ttd).start();
    }
}

