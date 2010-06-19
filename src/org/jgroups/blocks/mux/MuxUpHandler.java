package org.jgroups.blocks.mux;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.jgroups.Event;
import org.jgroups.Message;
import org.jgroups.UpHandler;
import org.jgroups.logging.Log;
import org.jgroups.logging.LogFactory;
import org.jgroups.util.ImmutableReference;

/**
 * Allows up handler multiplexing.
 * 
 * @author Bela Ban
 * @author Paul Ferraro
 * @version $Id: MuxUpHandler.java,v 1.4 2010/06/19 02:24:48 bstansberry Exp $
 */
public class MuxUpHandler implements UpHandler, Muxer<UpHandler> {

    protected final Log log=LogFactory.getLog(getClass());
    private final Map<Short, UpHandler> handlers = new ConcurrentHashMap<Short, UpHandler>();
    private volatile UpHandler defaultHandler;
    private volatile Event lastFlushEvent;
    private final Object flushMutex = new Object();
    
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
        synchronized (flushMutex)
        {          
            if (lastFlushEvent != null)
            {
                handler.up(lastFlushEvent);
            }
            handlers.put(id, handler);
        }
    }

    /**
     * {@inheritDoc}
     * @see org.jgroups.blocks.mux.Muxer#get(short)
     */
    @Override
    public UpHandler get(short id) {
        return handlers.get(id);
    }
    
    /**
     * {@inheritDoc}
     * @see org.jgroups.blocks.mux.Muxer#remove(short)
     */
    @Override
    public void remove(short id) {
        handlers.remove(id);
    }

    @Override
    public UpHandler getDefaultHandler() {
        return defaultHandler;
    }

    @Override
    public void setDefaultHandler(UpHandler handler) {
        this.defaultHandler = handler;      
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
                MuxHeader hdr = (MuxHeader) msg.getHeader(MuxRequestCorrelator.MUX_ID);
                if (hdr != null) {
                    short id = hdr.getId();
                    UpHandler handler = handlers.get(id);
                    return (handler != null) ? handler.up(evt) : new NoMuxHandler(id);
                }
                break;
            }
            case Event.GET_APPLSTATE:
            case Event.GET_STATE_OK: 
            case Event.STATE_TRANSFER_OUTPUTSTREAM: 
            case Event.STATE_TRANSFER_INPUTSTREAM: {
                ImmutableReference<Object> wrapper = handleStateTransferEvent(evt);
                if (wrapper != null)
                {
                   return wrapper.get();
                }
                break;
            } 
            case Event.BLOCK: 
            case Event.UNBLOCK: {
               synchronized (flushMutex)
               {
                  this.lastFlushEvent = evt;
                  passToAllHandlers(evt);
                  break;
               }
            }
            case Event.VIEW_CHANGE:
            case Event.SET_LOCAL_ADDRESS: 
            case Event.SUSPECT:  {
               passToAllHandlers(evt);
               break;
           }
           default: {
                passToAllHandlers(evt);
                break;
           }
        }
        
        return (defaultHandler != null) ? defaultHandler.up(evt) : null;
    }

    /**
      * Extension point for subclasses called by up() when an event
      * related to state transfer is received, allowing the subclass
      * to override the default behavior of passing the event to the
      * default up handler.
      *
      * @return an AtomicReference containing the return value for the event 
      *         if the event was handled and no further processing
      *         should be done in up(), or <code>null</code> if up() needs to 
      *         handle the event. If the event was handled but the return value 
      *         is <code>null</code>, an AtomicReference initialized to 
      *         <code>null</code> should be returned. This default 
      *         implementation always returns <code>null</code>
      */
    protected ImmutableReference<Object> handleStateTransferEvent(Event evt) {
         return null;
    } 
    
    

    private void passToAllHandlers(Event evt)
    {
       for (UpHandler handler: handlers.values()) {
            handler.up(evt);
        }
    }
}
