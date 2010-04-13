package org.jgroups.blocks.mux;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.jgroups.Event;
import org.jgroups.Message;
import org.jgroups.UpHandler;
import org.jgroups.conf.ClassConfigurator;

/**
 * Allows up handler multiplexing.
 * 
 * @author Bela Ban
 * @author Paul Ferraro
 * @version $Id: MuxUpHandler.java,v 1.1 2010/04/13 17:57:07 ferraro Exp $
 */
public class MuxUpHandler implements UpHandler, Muxer<UpHandler> {

    static {
        ClassConfigurator.add(MuxHeader.ID, MuxHeader.class);
    }
    
    private final Map<Short, UpHandler> handlers = new ConcurrentHashMap<Short, UpHandler>();
    private final UpHandler defaultHandler;
    
    /**
     * Creates a multiplexing up handler, with no default handler.
     */
    public MuxUpHandler() {
        this.defaultHandler = null;
    }

    /**
     * Creates a multiplexing up handler using the specified default handler.
     * @param defaultHandler a default up handler to handle messages with no {@link MuxHeader}
     */
    public MuxUpHandler(UpHandler defaultHandler) {
        this.defaultHandler = defaultHandler;
    }

    /**
     * {@inheritDoc}
     * @see org.jgroups.blocks.mux.Muxer#add(short, java.lang.Object)
     */
    @Override
    public void add(short id, UpHandler handler) {
        handlers.put(id, handler);
    }

    /**
     * {@inheritDoc}
     * @see org.jgroups.blocks.mux.Muxer#remove(short)
     */
    @Override
    public void remove(short id) {
        handlers.remove(id);
    }

    /**
     * {@inheritDoc}
     * @see org.jgroups.UpHandler#up(org.jgroups.Event)
     */
    @Override
    public Object up(Event evt) {
        switch (evt.getType()) {
            case Event.MSG: {
                Message msg = (Message) evt.getArg();
                MuxHeader hdr = (MuxHeader) msg.getHeader(MuxHeader.ID);
                if (hdr != null) {
                    short id = hdr.getId();
                    UpHandler handler = handlers.get(id);
                    return (handler != null) ? handler.up(evt) : new NoMuxHandler(id);
                }
                break;
            }
            case Event.VIEW_CHANGE: {
                for (UpHandler handler: handlers.values()) {
                    handler.up(evt);
                }
                break;
            }
        }
        
        return (defaultHandler != null) ? defaultHandler.up(evt) : null;
    }
}
