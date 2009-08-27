package org.jgroups;



/**
 * Used for inter-stack and intra-stack communication.
 * @author Bela Ban
 * @version $Id: Event.java,v 1.70 2009/08/27 07:30:05 belaban Exp $
 */
public class Event {
    public static final int MSG                                =  1;  // arg = Message
    public static final int CONNECT                            =  2;  // arg = cluster name (string)
    public static final int DISCONNECT                         =  4;  // arg = member address (Address)
    public static final int VIEW_CHANGE                        =  6;  // arg = View (or MergeView in case of merge)
    public static final int SET_LOCAL_ADDRESS                  =  8;  // arg = Address
    public static final int SUSPECT                            =  9;  // arg = Address of suspected member
    public static final int BLOCK                              = 10;  // arg = null (used by FLUSH)
    public static final int FIND_INITIAL_MBRS                  = 12;  // arg = JoinPromise (or null (merge2))
    public static final int FIND_ALL_INITIAL_MBRS              = 13;  // arg = JoinPromise (or null (merge2))
    public static final int MERGE                              = 14;  // arg = List<View>
    public static final int TMP_VIEW                           = 15;  // arg = View
    public static final int BECOME_SERVER                      = 16;  // sent when client has joined group
    public static final int GET_APPLSTATE                      = 17;  // get state from appl (arg=StateTransferInfo)
    public static final int GET_STATE                          = 19;  // arg = StateTransferInfo
    public static final int GET_STATE_OK                       = 20;  // arg = StateTransferInfo
    public static final int STATE_RECEIVED                     = 21;  // arg = StateTransferInfo (with state and state_id)
    public static final int STABLE                             = 30;  // arg = long[] (stable seqnos for mbrs)
    public static final int GET_DIGEST                         = 39;  //
    public static final int SET_DIGEST                         = 41;  // arg = Digest
    public static final int OVERWRITE_DIGEST                   = 42;  // arg = Digest
    public static final int UNSUSPECT                          = 51;  // arg = Address (of unsuspected member)
    public static final int MERGE_DIGEST                       = 53;  // arg = Digest
    public static final int CONFIG                             = 56;  // arg = Map<String,Object> (config properties)
    public static final int SUSPEND_STABLE                     = 65;  // arg = Long (max_suspend_time)
    public static final int RESUME_STABLE                      = 66;  // arg = null
    public static final int SUSPEND					           = 68;  // arg = HashMap (used by FLUSH)
    public static final int RESUME					           = 70;  // arg = null (used by FLUSH)
    public static final int STATE_TRANSFER_INPUTSTREAM         = 71;  // arg=java.io.InputStream subclass
    public static final int STATE_TRANSFER_OUTPUTSTREAM        = 72;  // arg=java.io.OutputStream subclass
    public static final int STATE_TRANSFER_INPUTSTREAM_CLOSED  = 73;  //arg=null
    public static final int STATE_TRANSFER_OUTPUTSTREAM_CLOSED = 74;  //arg=null
    public static final int UNBLOCK                            = 75;  //arg=null (indicate end of flush round)
    public static final int CLOSE_BARRIER                      = 76;  // arg = null
    public static final int OPEN_BARRIER                       = 77;  // arg = null
    public static final int REBROADCAST				           = 78;  // arg = Digest
    public static final int SHUTDOWN                           = 79;  // arg = null (shutdown without closing sockets or cleaning up)
    public static final int CONNECT_WITH_STATE_TRANSFER        = 80;  // arg = cluster name (string)
    public static final int PREPARE_VIEW                       = 86;  // arg = View
    public static final int GET_PHYSICAL_ADDRESS               = 87;  // arg = Address --> PhysicalAddress
    public static final int GET_LOGICAL_PHYSICAL_MAPPINGS      = 88;  // arg = null --> Map<Address,PhysicalAddress>
    public static final int SET_PHYSICAL_ADDRESS               = 89;  // arg = Tuple<Address,PhysicalAddress>
    public static final int REMOVE_ADDRESS                     = 90;  // arg = Address
    public static final int GET_LOCAL_ADDRESS                  = 91;  // arg = null --> UUID (local_addr)
    public static final int CONNECT_USE_FLUSH			       = 92;
    public static final int CONNECT_WITH_STATE_TRANSFER_USE_FLUSH = 93;
    public static final int USER_DEFINED                       = 1000; // arg = <user def., e.g. evt type + data>


    private final int    type;       // type of event
    private final Object arg;        // must be serializable if used for inter-stack communication


    public Event(int type) {
        this.type=type;
        this.arg=null;
    }

    public Event(int type, Object arg) {
        this.type=type;
        this.arg=arg;
    }

    public final int getType() {
        return type;
    }

    /**
     * Sets the new type
     * @param type
     * @deprecated in order to make an Event immutable
     */
    public void setType(int type) {
        throw new IllegalAccessError("setType() has been deprecated, to make Events immutable");
    }

    public Object getArg() {
        return arg;
    }

    public void setArg(Object arg) {
        throw new IllegalAccessError("setArg() has been deprecated, to make Events immutable");
    }



    public static String type2String(int t) {
        switch(t) {
            case MSG:	                 return "MSG";
            case CONNECT:	             return "CONNECT";
            case DISCONNECT:	         return "DISCONNECT";
            case VIEW_CHANGE:	         return "VIEW_CHANGE";
            case SET_LOCAL_ADDRESS:	     return "SET_LOCAL_ADDRESS";
            case SUSPECT:                return "SUSPECT";
            case BLOCK:	                 return "BLOCK";
            case FIND_INITIAL_MBRS:	     return "FIND_INITIAL_MBRS";
            case FIND_ALL_INITIAL_MBRS:  return "FIND_ALL_INITIAL_MBRS";
            case TMP_VIEW:	             return "TMP_VIEW";
            case BECOME_SERVER:	         return "BECOME_SERVER";
            case GET_APPLSTATE:          return "GET_APPLSTATE";
            case GET_STATE:              return "GET_STATE";
            case GET_STATE_OK:           return "GET_STATE_OK";
            case STATE_RECEIVED:         return "STATE_RECEIVED";
            case STABLE:                 return "STABLE";
            case GET_DIGEST:             return "GET_DIGEST";
            case SET_DIGEST:             return "SET_DIGEST";
            case OVERWRITE_DIGEST:       return "OVERWRITE_DIGEST";
            case MERGE:                  return "MERGE";
            case UNSUSPECT:              return "UNSUSPECT";
            case MERGE_DIGEST:           return "MERGE_DIGEST";
            case CONFIG:                 return "CONFIG";
            case SUSPEND_STABLE:         return "SUSPEND_STABLE";
            case RESUME_STABLE:          return "RESUME_STABLE";
            case SUSPEND:        		 return "SUSPEND";
            case RESUME:     			 return "RESUME";
            case STATE_TRANSFER_INPUTSTREAM: return "STATE_TRANSFER_INPUTSTREAM";
            case STATE_TRANSFER_OUTPUTSTREAM:return "STATE_TRANSFER_OUTPUTSTREAM";
            case STATE_TRANSFER_INPUTSTREAM_CLOSED: return "STATE_TRANSFER_INPUTSTREAM_CLOSED";
            case STATE_TRANSFER_OUTPUTSTREAM_CLOSED: return "STATE_TRANSFER_OUTPUTSTREAM_CLOSED";
            case UNBLOCK:                return "UNBLOCK";
            case CLOSE_BARRIER:          return "CLOSE_BARRIER";
            case OPEN_BARRIER:           return "OPEN_BARRIER";
            case REBROADCAST:            return "REBROADCAST";
            case SHUTDOWN:               return "SHUTDOWN";
            case CONNECT_WITH_STATE_TRANSFER:    return "CONNECT_WITH_STATE_TRANSFER";
            case PREPARE_VIEW:           return "PREPARE_VIEW";
            case USER_DEFINED:           return "USER_DEFINED";
            default:                     return "UNDEFINED(" + t + ")";
        }
    }

    public static final Event GET_DIGEST_EVT        = new Event(Event.GET_DIGEST);

    public String toString() {
        StringBuilder ret=new StringBuilder(64);
        ret.append("Event[type=" + type2String(type) + ", arg=" + arg + ']');
        if(type == MSG)
            ret.append(" (headers=").append(((Message)arg).printHeaders()).append(")");
        return ret.toString();
    }

}
