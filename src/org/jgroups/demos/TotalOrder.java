

package org.jgroups.demos;

import org.jgroups.*;
import org.jgroups.util.Util;

import java.awt.*;
import java.awt.event.WindowAdapter;
import java.awt.event.WindowEvent;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetAddress;
import java.nio.ByteBuffer;


/**
 * Originally written to be a demo for TOTAL order (code to be written by a student). In the meantime,
 * it evolved into a state transfer demo. All members maintain a shared matrix and continually
 * broadcast changes to be applied to a randomly chosen field (e.g. multiplication of field with new
 * value, division, addition, subtraction). Each member can be started independently (starts to
 * broadcast update messages to all members). When "Stop" is pressed, a stop message is broadcast to
 * all members, causing them to stop sending messages. The "Clear" button clears the shared state;
 * "GetState" refreshes it from the shared group state (using the state transfer protocol).<p>If the
 * demo is to be used to show total order, then the SEQUENCER protocol would have to be added to the
 * stack.
 *
 * @author Bela Ban
 */
public class TotalOrder extends Frame {
    final Font def_font=new Font("Helvetica", Font.BOLD, 12);
    final Font def_font2=new Font("Helvetica", Font.PLAIN, 12);
    MyCanvas canvas;
    final MenuBar menubar=createMenuBar();
    final Button start=new Button("Start");
    final Button stop=new Button("Stop");
    final Button clear=new Button("Clear");
    final Button get_state=new Button("Get State");
    final Button quit=new Button("Quit");
    final Panel button_panel=new Panel();
    SenderThread sender=null;
    JChannel channel;
    long timeout=0;
    int field_size=0;
    int num_fields=0;
    static final int x_offset=30;
    static final int y_offset=40;
    private int num=0;

    private int num_additions=0, num_subtractions=0, num_divisions=0, num_multiplications=0;


    static void error(String s) {
        System.err.println(s);
    }


    static class EventHandler extends WindowAdapter {
        final Frame gui;

        public EventHandler(Frame g) {
            gui=g;
        }

        public void windowClosing(WindowEvent e) {
            gui.dispose();
            System.exit(0);
        }
    }


    class SenderThread extends Thread {
        TotOrderRequest req;
        boolean running=true;

        public void stopSender() {
            running=false;
            interrupt();
            System.out.println("-- num_additions: " + num_additions +
                    "\n-- num_subtractions: " + num_subtractions +
                    "\n-- num_divisions: " + num_divisions +
                    "\n-- num_multiplications: " + num_multiplications);
            num_additions=num_subtractions=num_multiplications=num_divisions=0;
        }

        public void run() {
            this.setName("SenderThread");

            byte[] buf;
            int cnt=0;
            while(running) {
                try {
                    req=createRandomRequest();
                    buf=req.toBuffer();
                    channel.send(new BytesMessage(null, buf));
                    System.out.print("-- num requests sent: " + cnt + "\r");
                    if(timeout > 0)
                        Util.sleep(timeout);
                    cnt++;
                    if(num > 0 && cnt > num) {
                        running=false;
                        cnt=0;
                    }
                }
                catch(Exception e) {
                    error(e.toString());
                    return;
                }
            }
        }
    }




    void processRequest(TotOrderRequest req) throws Exception {
        int x=req.x, y=req.y, val=req.val;

        if(req.type == TotOrderRequest.STOP) {
            stopSender();
            return;
        }

        switch(req.type) {
            case TotOrderRequest.ADDITION:
                canvas.addValueTo(x, y, val);
                num_additions++;
                break;
            case TotOrderRequest.SUBTRACTION:
                canvas.subtractValueFrom(x, y, val);
                num_subtractions++;
                break;
            case TotOrderRequest.MULTIPLICATION:
                canvas.multiplyValueWith(x, y, val);
                num_multiplications++;
                break;
            case TotOrderRequest.DIVISION:
                canvas.divideValueBy(x, y, val);
                num_divisions++;
                break;
        }
        canvas.update();
    }


    public TotalOrder(String title, long timeout, int num_fields, int field_size, String props, int num) {
        Dimension s;

        this.timeout=timeout;
        this.num_fields=num_fields;
        this.field_size=field_size;
        this.num=num;
        setFont(def_font);



        start.addActionListener(e -> startSender());

        stop.addActionListener(e -> {
            try {
                TotOrderRequest req=new TotOrderRequest(TotOrderRequest.STOP, 0, 0, 0);
                byte[] buf=req.toBuffer();
                channel.send(new BytesMessage(null, buf));
            }
            catch(Exception ex) {
            }
        });

        clear.addActionListener(e -> canvas.clear());

        get_state.addActionListener(e -> {
            try {
                channel.getState(null, 3000);
            }
            catch(Throwable t) {
                error("exception fetching state: " + t);
            }
        });

        quit.addActionListener(e -> {
            channel.close();
            System.exit(0);
        });

        setTitle(title);
        addWindowListener(new EventHandler(this));
        setBackground(Color.white);
        setMenuBar(menubar);

        setLayout(new BorderLayout());
        canvas=new MyCanvas(num_fields, field_size, x_offset, y_offset);

        add("Center", canvas);
        button_panel.setLayout(new FlowLayout());
        button_panel.setFont(def_font2);
        button_panel.add(start);
        button_panel.add(stop);
        button_panel.add(clear);
        button_panel.add(get_state);
        button_panel.add(quit);
        add("South", button_panel);

        s=canvas.getSize();
        s.height+=100;
        setSize(s);

        try {
            channel=new JChannel(props);
            channel.setReceiver(new ReceiverAdapter() {
                public void receive(Message msg) {
                    try {
                        TotOrderRequest req=new TotOrderRequest();
                        ByteBuffer buf=ByteBuffer.wrap(msg.getArray(), msg.getOffset(), msg.getLength());
                        req.init(buf);
                        processRequest(req);
                    }
                    catch(Exception e) {
                        System.err.println(e);
                    }
                }

                public void getState(OutputStream output) throws Exception {
                    int[][] copy_of_state=canvas.getCopyOfState();
                    Util.objectToStream(copy_of_state, new DataOutputStream(output));
                }

                public void setState(InputStream input) throws Exception {
                    canvas.setState(Util.objectFromStream(new DataInputStream(input)));
                }

                public void viewAccepted(View view) {
                    System.out.println("view = " + view);
                }
            });
            channel.connect("TotalOrderGroup");
            channel.getState(null, 8000);
        }
        catch(Exception e) {
            e.printStackTrace();
            System.exit(-1);
        }
    }


    void startSender() {
        if(sender == null || !sender.isAlive()) {
            sender=new SenderThread();
            sender.start();
        }
    }

    void stopSender() {
        if(sender != null) {
            sender.stopSender();
            sender=null;
        }
    }




    private MenuBar createMenuBar() {
        MenuBar ret=new MenuBar();
        Menu file=new Menu("File");
        MenuItem quitm=new MenuItem("Quit");

        ret.setFont(def_font2);
        ret.add(file);

        file.addSeparator();
        file.add(quitm);

        quitm.addActionListener(e -> System.exit(1));
        return ret;
    }



    private TotOrderRequest createRandomRequest() {
        TotOrderRequest ret=null;
        byte op_type=(byte)(((Math.random() * 10) % 4) + 1);  // 1 - 4
        int x=(int)((Math.random() * num_fields * 2) % num_fields);
        int y=(int)((Math.random() * num_fields * 2) % num_fields);
        int val=(int)((Math.random() * num_fields * 200) % 10);

        ret=new TotOrderRequest(op_type, x, y, val);
        return ret;
    }


    public static void main(String[] args) {
        TotalOrder g;
        String arg;
        long timeout=200;
        int num_fields=3;
        int field_size=80;
        String props=null;
        int num=0;

        props="udp.xml";


        for(int i=0; i < args.length; i++) {
            arg=args[i];
            if("-timeout".equals(arg)) {
                timeout=Long.parseLong(args[++i]);
                continue;
            }
            if("-num_fields".equals(arg)) {
                num_fields=Integer.parseInt(args[++i]);
                continue;
            }
            if("-field_size".equals(arg)) {
                field_size=Integer.parseInt(args[++i]);
                continue;
            }
            if("-help".equals(arg)) {
                help();
                return;
            }
            if("-props".equals(arg)) {
                props=args[++i];
                continue;
            }
            if("-num".equals(arg)) {
                num=Integer.parseInt(args[++i]);
            }
            help();
            return;
        }


        try {
            g=new TotalOrder("Total Order Demo on " + InetAddress.getLocalHost().getHostName(),
                             timeout, num_fields, field_size, props, num);
            g.setVisible(true);
        }
        catch(Exception e) {
            System.err.println(e);
        }
    }

    protected static void help() {
        System.out.println("\nTotalOrder [-timeout <value>] [-num_fields <value>] " +
                             "[-field_size <value>] [-props <properties (can be URL)>] [-num <num requests>]\n");
    }

}


class TotOrderRequest {
    public static final byte STOP=0;
    public static final byte ADDITION=1;
    public static final byte SUBTRACTION=2;
    public static final byte MULTIPLICATION=3;
    public static final byte DIVISION=4;
    final static int SIZE=Global.BYTE_SIZE + Global.INT_SIZE * 3;


    public byte type=ADDITION;
    public int x=0;
    public int y=0;
    public int val=0;


    public TotOrderRequest() {
    }

    TotOrderRequest(byte type, int x, int y, int val) {
        this.type=type;
        this.x=x;
        this.y=y;
        this.val=val;
    }

    public String printType() {
        switch(type) {
            case STOP:
                return "STOP";
            case ADDITION:
                return "ADDITION";
            case SUBTRACTION:
                return "SUBTRACTION";
            case MULTIPLICATION:
                return "MULTIPLICATION";
            case DIVISION:
                return "DIVISION";
            default:
                return "<unknown>";
        }
    }

//    public void writeExternal(ObjectOutput out) throws IOException {
//        out.writeByte(type);
//        out.writeInt(x);
//        out.writeInt(y);
//        out.writeInt(val);
//    }
//
//    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
//        type=in.readByte();
//        x=in.readInt();
//        y=in.readInt();
//        val=in.readInt();
//    }

    public byte[] toBuffer() {
        ByteBuffer buf=ByteBuffer.allocate(SIZE);
        buf.put(type);
        buf.putInt(x);
        buf.putInt(y);
        buf.putInt(val);
        return buf.array();
    }

    public void init(ByteBuffer buf) {
        type=buf.get();
        x=buf.getInt();
        y=buf.getInt();
        val=buf.getInt();
    }

    public String toString() {
        return "[" + x + ',' + y + ": " + printType() + '(' + val + ")]";
    }
}


class MyCanvas extends Canvas {
    int field_size=100;
    int num_fields=4;
    int x_offset=30;
    int y_offset=30;

    final Font def_font=new Font("Helvetica", Font.BOLD, 14);
    final int[][] array;      // state

    Dimension off_dimension=null;
    Image off_image=null;
    Graphics off_graphics=null;
    final Font def_font2=new Font("Helvetica", Font.PLAIN, 12);
    static final Color checksum_col=Color.blue;
    int checksum=0;


    public MyCanvas(int num_fields, int field_size, int x_offset, int y_offset) {
        this.num_fields=num_fields;
        this.field_size=field_size;
        this.x_offset=x_offset;
        this.y_offset=y_offset;

        array=new int[num_fields][num_fields];
        setBackground(Color.white);
        setSize(2 * x_offset + num_fields * field_size + 30, y_offset + num_fields * field_size + 50);

        for(int i=0; i < num_fields; i++)
            for(int j=0; j < num_fields; j++)
                array[i][j]=0;
    }


    public void setFieldSize(int fs) {
        field_size=fs;
    }

    public void setNumFields(int nf) {
        num_fields=nf;
    }

    public void setXOffset(int o) {
        x_offset=o;
    }

    public void setYOffset(int o) {
        y_offset=o;
    }


    public void addValueTo(int x, int y, int value) {
        synchronized(array) {
            array[x][y]+=value;
            repaint();
        }
    }

    public void subtractValueFrom(int x, int y, int value) {
        synchronized(array) {
            array[x][y]-=value;
            repaint();
        }
    }

    public void multiplyValueWith(int x, int y, int value) {
        synchronized(array) {
            array[x][y]*=value;
            repaint();
        }
    }

    public void divideValueBy(int x, int y, int value) {
        if(value == 0)
            return;
        synchronized(array) {
            array[x][y]/=value;
            repaint();
        }
    }


    public void setValueAt(int x, int y, int value) {
        synchronized(array) {
            array[x][y]=value;
        }
        repaint();
    }


    public int getValueAt(int x, int y) {
        synchronized(array) {
            return array[x][y];
        }
    }


    public void clear() {
        synchronized(array) {
            for(int i=0; i < num_fields; i++)
                for(int j=0; j < num_fields; j++)
                    array[i][j]=0;
            checksum=checksum();
            repaint();
        }
    }


    public int[][] getState() {
        synchronized(array) {
            return array;
        }
    }


    public int[][] getCopyOfState() {
        int[][] retval=new int[num_fields][num_fields];

        synchronized(array) {
            for(int i=0; i < num_fields; i++)
                System.arraycopy(array[i], 0, retval[i], 0, num_fields);
            return retval;
        }
    }


    public void update() {
        checksum=checksum();
        repaint();
    }


    public void setState(Object new_state) {

        if(new_state == null)
            return;

        try {
            int[][] new_array=(int[][])new_state;
            synchronized(array) {
                clear();

                for(int i=0; i < num_fields; i++)
                    System.arraycopy(new_array[i], 0, array[i], 0, num_fields);
                checksum=checksum();
                repaint();
            }
        }
        catch(Exception e) {
            System.err.println(e);
        }
    }


    public int checksum() {
        int retval=0;

        synchronized(array) {
            for(int i=0; i < num_fields; i++)
                for(int j=0; j < num_fields; j++)
                    retval+=array[i][j];
        }
        return retval;
    }


    public void update(Graphics g) {
        Dimension d=getSize();

        if(off_graphics == null ||
                d.width != off_dimension.width ||
                d.height != off_dimension.height) {
            off_dimension=d;
            off_image=createImage(d.width, d.height);
            off_graphics=off_image.getGraphics();
        }

        //Erase the previous image.
        off_graphics.setColor(getBackground());
        off_graphics.fillRect(0, 0, d.width, d.height);
        off_graphics.setColor(Color.black);
        off_graphics.setFont(def_font);
        drawEmptyBoard(off_graphics);
        drawNumbers(off_graphics);
        g.drawImage(off_image, 0, 0, this);
    }


    public void paint(Graphics g) {
        update(g);
    }


    /**
     * Draws the empty board, no pieces on it yet, just grid lines
     */
    void drawEmptyBoard(Graphics g) {
        int x=x_offset, y=y_offset;
        Color old_col=g.getColor();

        g.setFont(def_font2);
        old_col=g.getColor();
        g.setColor(checksum_col);
        g.drawString(("Checksum: " + checksum), x_offset + field_size, y_offset - 20);
        g.setFont(def_font);
        g.setColor(old_col);

        for(int i=0; i < num_fields; i++) {
            for(int j=0; j < num_fields; j++) {  // draws 1 row
                g.drawRect(x, y, field_size, field_size);
                x+=field_size;
            }
            g.drawString((String.valueOf((num_fields - i - 1))), x + 20, y + field_size / 2);
            y+=field_size;
            x=x_offset;
        }

        for(int i=0; i < num_fields; i++) {
            g.drawString((String.valueOf(i)), x_offset + i * field_size + field_size / 2, y + 30);
        }
    }


    void drawNumbers(Graphics g) {
        Point p;
        String num;
        FontMetrics fm=g.getFontMetrics();
        int len=0;

        synchronized(array) {
            for(int i=0; i < num_fields; i++)
                for(int j=0; j < num_fields; j++) {
                    num=String.valueOf(array[i][j]);
                    len=fm.stringWidth(num);
                    p=index2Coord(i, j);
                    g.drawString(num, p.x - (len / 2), p.y);
                }
        }
    }


    Point coord2Index(int x, int y) {
        Point ret=new Point();

        ret.x=x_offset + (x * field_size);
        ret.y=y_offset + ((num_fields - 1 - y) * field_size);
        return ret;
    }


    Point index2Coord(int i, int j) {
        int x=x_offset + i * field_size + field_size / 2;
        int y=y_offset + num_fields * field_size - j * field_size - field_size / 2;
        return new Point(x, y);
    }


}








