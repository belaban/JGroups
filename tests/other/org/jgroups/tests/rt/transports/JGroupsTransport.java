package org.jgroups.tests.rt.transports;

import org.jgroups.*;
import org.jgroups.logging.Log;
import org.jgroups.logging.LogFactory;
import org.jgroups.protocols.TP;
import org.jgroups.tests.rt.RtReceiver;
import org.jgroups.tests.rt.RtTransport;
import org.jgroups.util.MessageBatch;
import org.jgroups.util.Util;

import java.util.List;

/**
 * @author Bela Ban
 * @since  4.0
 */
public class JGroupsTransport extends ReceiverAdapter implements RtTransport {
    protected JChannel     ch;
    protected RtReceiver   receiver;
    protected View         view;
    protected boolean      oob=true, dont_bundle;
    protected final Log    log=LogFactory.getLog(JGroupsTransport.class);


    public JGroupsTransport() {
    }

    public String[] options() {
        return new String[]{"-props <props>", "-name <name>", "-oob true|false", "-dont_bundle true|false"};
    }

    public void options(String... options) throws Exception {
        if(options == null)
            return;
        for(int i=0; i < options.length; i++) {
            if(options[i].startsWith("-oob")) {
                oob=Boolean.valueOf(options[++i]);
                continue;
            }
            if(options[i].startsWith("-dont_bundle")) {
                dont_bundle=Boolean.valueOf(options[++i]);
            }
        }
    }

    public void receiver(RtReceiver receiver) {
        this.receiver=receiver;
    }

    public Address localAddress() {
        return ch != null? ch.getAddress() : null;
    }

    public List<Address> clusterMembers() {
        return view.getMembers();
    }

    public void start(String ... options) throws Exception {
        String props="udp.xml", name=null;
        options(options);
        if(options !=null) {
            for(int i=0; i < options.length; i++) {
                if(options[i].startsWith("-props")) {
                    props=options[++i];
                    continue;
                }
                if(options[i].startsWith("-name")) {
                    name=options[++i];
                }
            }
        }
        ch=new JChannel(props).name(name).receiver(this);
        TP transport=ch.getProtocolStack().getTransport();
        // uncomment below to disable the regular and OOB thread pools
        // transport.setOOBThreadPool(new DirectExecutor());
        // transport.setDefaultThreadPool(new DirectExecutor());

        //ThreadPoolExecutor thread_pool=new ThreadPoolExecutor(4, 4, 30000, TimeUnit.MILLISECONDS, new ArrayBlockingQueue<>(5000));
        //transport.setDefaultThreadPool(thread_pool);
        //transport.setOOBThreadPool(thread_pool);
        //transport.setInternalThreadPool(thread_pool);

        ch.connect("rt");
        View v=ch.getView();
        if(v.size() > 2)
            throw new IllegalStateException(String.format("More than 2 members found (%s); terminating\n", v));
    }

    public void stop() {
        Util.close(ch);
    }

    public void send(Object dest, byte[] buf, int offset, int length) throws Exception {
        Message msg=new Message((Address)dest, buf, offset, length);
        if(oob)
            msg.setFlag(Message.Flag.OOB);
        if(dont_bundle)
            msg.setFlag(Message.Flag.DONT_BUNDLE);
        ch.send(msg);
    }


    public void receive(MessageBatch batch) {
        if(receiver == null)
            return;
        for(Message msg: batch) {
            try {
                receiver.receive(msg.src(), msg.getRawBuffer(), msg.getOffset(), msg.getLength());
            }
            catch(Throwable t) {
                log.error("failed delivering message from batch", t);
            }
        }
    }

    public void receive(Message msg) {
        if(receiver == null)
            return;
        try {
            receiver.receive(msg.src(), msg.getRawBuffer(), msg.getOffset(), msg.getLength());
        }
        catch(Throwable t) {
            log.error("failed delivering message", t);
        }
    }

    public void viewAccepted(View view) {
        System.out.println("view = " + view);
        this.view=view;
    }
}
