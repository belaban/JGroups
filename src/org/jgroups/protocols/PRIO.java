package org.jgroups.protocols;

import org.jgroups.Address;
import org.jgroups.Event;
import org.jgroups.Message;
import org.jgroups.annotations.Experimental;
import org.jgroups.annotations.Property;
import org.jgroups.stack.Protocol;
import org.jgroups.util.MessageBatch;

import java.util.Comparator;
import java.util.concurrent.PriorityBlockingQueue;

/**
 * This protocol will provide message sending and receiving prioritization.  The protocol assumes that any prioritized
 * message will contain a PrioHeader header entry that will contain the byte value priority.  Priority values are from
 * 0 to 255 where 0 is the highest priority.
 * 
 * When a message is received (up/down), it is added to the up/downMessageQueue.   The up/downMessageThread will block
 * on the queue until new message is added.  Messages with the highest priority (0=highest) will bubble to the top
 * of the queue and those be processed before other messages received at the same time.
 *
 * Example of setting a message priority:
 * <code>
 *  // Create a message to send to everyone
 *	Message message = new Message( null, null, messagePayload );
 *	// Add the priority protocol header
 *	PrioHeader header = new PrioHeader( 1 );
 *	short protocolId = ClassConfigurator.getProtocolId(PRIO.class);
 *	message.putHeader( protocolId, header);
 * </code>
 * @author Michael Earl
 */
@Experimental
public class PRIO extends Protocol {
	private PriorityBlockingQueue<PriorityMessage> downMessageQueue;
	private PriorityBlockingQueue<PriorityMessage> upMessageQueue;
    private DownMessageThread                      downMessageThread;
    private UpMessageThread                        upMessageThread;

	@Property(description="The number of miliseconds to sleep before after an error occurs before sending the next message")
	private int message_failure_sleep_time = 120000; // two seconds (bela: 2 minutes, is that what you wanted ?)

	@Property(description="true to prioritize outgoing messages")
	private boolean prioritize_down = true;

	@Property(description="true to prioritize incoming messages")
	private boolean prioritize_up = true;

    private Address local_addr;

    /**
     * This method is called on a {@link org.jgroups.Channel#connect(String)}. Starts work.
     * Protocols are connected and queues are ready to receive events.
     * Will be called <em>from bottom to top</em>. This call will replace
     * the <b>START</b> and <b>START_OK</b> events.
     * @exception Exception Thrown if protocol cannot be started successfully. This will cause the ProtocolStack
     *                      to fail, so {@link org.jgroups.Channel#connect(String)} will throw an exception
     */
    public void start() throws Exception {
		if (prioritize_down) {
			downMessageQueue = new PriorityBlockingQueue<>( 100, new PriorityCompare() );
			downMessageThread = new DownMessageThread( this, downMessageQueue );
			downMessageThread.start();
		}

		if (prioritize_up) {
			upMessageQueue = new PriorityBlockingQueue<>( 100, new PriorityCompare() );
			upMessageThread = new UpMessageThread( this, upMessageQueue );
			upMessageThread.start();
		}
    }

    /**
     * This method is called on a {@link org.jgroups.Channel#disconnect()}. Stops work (e.g. by closing multicast socket).
     * Will be called <em>from top to bottom</em>. This means that at the time of the method invocation the
     * neighbor protocol below is still working. This method will replace the
     * <b>STOP</b>, <b>STOP_OK</b>, <b>CLEANUP</b> and <b>CLEANUP_OK</b> events. The ProtocolStack guarantees that
     * when this method is called all messages in the down queue will have been flushed
     */
    public void stop() {
		if (downMessageThread != null) {
            downMessageThread.setRunning(false);
			downMessageThread.interrupt();
		}
		if (upMessageThread != null) {
            upMessageThread.setRunning(false);
			upMessageThread.interrupt();
		}
    }

	/**
     * An event was received from the layer below. Usually the current layer will want to examine
     * the event type and - depending on its type - perform some computation
     * (e.g. removing headers from a MSG event type, or updating the internal membership list
     * when receiving a VIEW_CHANGE event).
     * Finally the event is either a) discarded, or b) an event is sent down
     * the stack using <code>down_prot.down()</code> or c) the event (or another event) is sent up
     * the stack using <code>up_prot.up()</code>.
     */
    public Object up(Event evt) {
		switch(evt.getType()) {
            case Event.MSG:
				Message message = (Message)evt.getArg();
				if ( message.isFlagSet( Message.Flag.OOB ) ) {
					return up_prot.up(evt);
				}
				else {
					PrioHeader hdr=(PrioHeader)message.getHeader(id);
					if(hdr != null) {
						log.trace("%s: adding priority message %d to UP queue", local_addr, hdr.getPriority());
						upMessageQueue.add( new PriorityMessage( evt, hdr.getPriority() ) );
						// send with hdr.prio
						return null;
					}
        			return up_prot.up(evt);
                }
            default:
        		return up_prot.up(evt);
        }
    }


    public void up(MessageBatch batch) {
        for(Message msg: batch) {
            if(msg.isFlagSet(Message.Flag.OOB))
                continue;
            PrioHeader hdr=(PrioHeader)msg.getHeader(id);
            if(hdr != null) {
                log.trace("%s: adding priority message %d to UP queue", local_addr, hdr.getPriority());
                upMessageQueue.add( new PriorityMessage( new Event(Event.MSG, msg), hdr.getPriority() ) );
                batch.remove(msg); // sent up by UpMessageThread; we don't need to send it up, too
            }
        }

        if(!batch.isEmpty())
            up_prot.up(batch);
    }

    /**
     * An event is to be sent down the stack. The layer may want to examine its type and perform
     * some action on it, depending on the event's type. If the event is a message MSG, then
     * the layer may need to add a header to it (or do nothing at all) before sending it down
     * the stack using <code>down_prot.down()</code>. In case of a GET_ADDRESS event (which tries to
     * retrieve the stack's address from one of the bottom layers), the layer may need to send
     * a new response event back up the stack using <code>up_prot.up()</code>.
     */
    public Object down(Event evt) {
		switch(evt.getType()) {
            case Event.MSG:
				Message message = (Message)evt.getArg();
                if ( message.isFlagSet( Message.Flag.OOB ) )
                    return down_prot.down(evt);
                PrioHeader hdr=(PrioHeader)message.getHeader(id);
                if(hdr != null) {
                    log.trace("%s: adding priority message %d to DOWN queue", local_addr, hdr.getPriority());
                    downMessageQueue.add( new PriorityMessage( evt, hdr.getPriority()  ) );
                    // send with hdr.prio
                    return null;
                }
                return down_prot.down(evt);
            case Event.SET_LOCAL_ADDRESS:
                local_addr=(Address)evt.getArg();
                return down_prot.down(evt);
            default:
                return down_prot.down(evt);
        }
    }

    /**
     * This class is a simple wrapper to contain the Event, timestamp and priority of the message.
     * Instances of this class are added to the message queue
     */
    protected static class PriorityMessage {
		Event event;
        long timestamp;
        byte priority;

        protected PriorityMessage( Event event, byte priority ) {
            this.event = event;
            this.timestamp = System.currentTimeMillis();
			this.priority = priority;
        }
    }

	/**
	 * Thread to send messages to the down protocol. 
	 * <P>
	 * The messageQueue contains the prioritized messages 
	 */
	private class DownMessageThread extends MessageThread {
		private DownMessageThread( PRIO prio, PriorityBlockingQueue<PriorityMessage> messageQueue  ) {
			super( prio, messageQueue );
		}

		protected void handleMessage( PriorityMessage message ) {
			log.trace("%s: sending priority %d message", local_addr, message.priority);
			down_prot.down( message.event );
		}
	}

	/**
	 * Thread to send messages to the up protocol.
	 * <P>
	 * The messageQueue contains the prioritized messages
	 */
	private class UpMessageThread extends MessageThread {
		private UpMessageThread( PRIO prio, PriorityBlockingQueue<PriorityMessage> messageQueue  ) {
			super( prio, messageQueue );
		}

		protected void handleMessage( PriorityMessage message ) {
			log.trace("%s: delivering priority %d message", local_addr, message.priority);
			up_prot.up( message.event );
		}
	}

    /**
     * This Thread class will process PriorityMessage's off of the queue and call the handleMessage method
	 * to send the message
     */
    private abstract class MessageThread extends Thread
    {
		private final PRIO prio;
		private final PriorityBlockingQueue<PriorityMessage> messageQueue;
        private volatile boolean running=true;

        private MessageThread( PRIO prio, PriorityBlockingQueue<PriorityMessage> messageQueue  ) {
			this.prio = prio;
			this.messageQueue = messageQueue;
            setName( "PRIO " + (messageQueue == downMessageQueue ? "down" : "up") );
        }

		protected abstract void handleMessage( PriorityMessage message );

        @Override
        public void run() {
            while (running) {
                PriorityMessage priorityMessage = null;
                try {
                	priorityMessage = messageQueue.take();
					handleMessage( priorityMessage );
                }
                catch( InterruptedException e ) {
                    break;
                }
                catch (Exception e) {
                    log.error( "Error handling message.  Sleeping " + (prio.message_failure_sleep_time/1000) + " seconds", e );
					try
					{
						sleep( prio.message_failure_sleep_time );
					}
					catch (InterruptedException ex)
					{
						break;
					}
					/*
					 * Add it back to the queue to be processed again
					 */
					messageQueue.add( priorityMessage );
                }
            }
        }

        public void setRunning(boolean flag) {
            running=flag;
        }
    }

    /**
     * Comparator for PriorityMessage's
     */
    private static class PriorityCompare implements Comparator<PriorityMessage> {
        /**
         * Compare two messages based on priority and time stamp in that order
         * @param msg1 - first message
         * @param msg2 - second message
         * @return int result of comparison
         */
        @Override
        public int compare( PriorityMessage msg1, PriorityMessage msg2 ) {
            if ( msg1.priority > msg2.priority ) {
                return 1;
            }
            else if ( msg1.priority < msg2.priority ) {
                return -1;
            }
            else {
                if ( msg1.timestamp > msg2.timestamp ) {
                    return 1;
                }
                else if ( msg1.timestamp < msg2.timestamp ) {
                    return -1;
                }
                else {
                    return 0;
                }
            }
        }
    }
}