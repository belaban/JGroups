package org.jgroups;


/**
 * Event is a JGroups internal class used for inter-stack and intra-stack communication. 
 * 
 * @since 2.0
 * @author Bela Ban
 */
public class Event {
    public static final int CONNECT                            =  2;  // arg = cluster name (string)
    public static final int DISCONNECT                         =  4;  // arg = null (local address)
    public static final int VIEW_CHANGE                        =  6;  // arg = View (or MergeView in case of merge)
    public static final int SUSPECT                            =  9;  // arg = Collection<Address> (suspected members)
    public static final int FIND_MBRS                          = 11;  // arg = List<Address> (can be null) -> Responses
    public static final int FIND_INITIAL_MBRS                  = 12;  // timeout (ms) = null -> Responses
    public static final int FIND_MBRS_ASYNC                    = 13;  // arg = Consumer<PingData>
    public static final int MERGE                              = 14;  // arg = Map<Address,View>
    public static final int TMP_VIEW                           = 15;  // arg = View
    public static final int BECOME_SERVER                      = 16;  // sent when client has joined group
    public static final int GET_APPLSTATE                      = 17;  // get state from appl (arg=StateTransferInfo)
    public static final int GET_STATE                          = 19;  // arg = StateTransferInfo
    public static final int GET_STATE_OK                       = 20;  // arg = StateTransferInfo
    public static final int STABLE                             = 30;  // arg = long[] (stable seqnos for mbrs)
    public static final int GET_DIGEST                         = 39;  // arg= address (or null)
    public static final int SET_DIGEST                         = 41;  // arg = Digest
    public static final int OVERWRITE_DIGEST                   = 42;  // arg = Digest
    public static final int UNSUSPECT                          = 51;  // arg = Address (of unsuspected member)
    public static final int MERGE_DIGEST                       = 53;  // arg = Digest
    public static final int CONFIG                             = 56;  // arg = Map<String,Object> (config properties)
    public static final int SUSPEND_STABLE                     = 65;  // arg = Long (max_suspend_time)
    public static final int RESUME_STABLE                      = 66;  // arg = null
    public static final int STATE_TRANSFER_INPUTSTREAM         = 71;  // arg = InputStream
    public static final int STATE_TRANSFER_OUTPUTSTREAM        = 72;  // arg = OutputStream
    public static final int STATE_TRANSFER_INPUTSTREAM_CLOSED  = 73;  // arg = StateTransferResult
    public static final int CLOSE_BARRIER                      = 76;  // arg = null
    public static final int OPEN_BARRIER                       = 77;  // arg = null
    public static final int CONNECT_WITH_STATE_TRANSFER        = 80;  // arg = cluster name (string)
    public static final int GET_PHYSICAL_ADDRESS               = 87;  // arg = Address --> PhysicalAddress
    public static final int GET_LOGICAL_PHYSICAL_MAPPINGS      = 88;  // arg = boolean --> Map<Address,PhysicalAddress>
    public static final int ADD_PHYSICAL_ADDRESS               = 89;  // arg = Tuple<Address,PhysicalAddress> --> boolean
    public static final int REMOVE_ADDRESS                     = 90;  // arg = Address
    public static final int GET_LOCAL_ADDRESS                  = 91;  // arg = null --> UUID (local_addr)
    public static final int IS_MERGE_IN_PROGRESS               = 100; // returns true or false
    public static final int GET_PHYSICAL_ADDRESSES             = 102; // arg = null (returns all physical addresses)
    public static final int SITE_UNREACHABLE                   = 104; // arg = SiteMaster (RELAY2/RELAY3)
    public static final int MBR_UNREACHABLE                    = 105; // arg = Address (member)
    public static final int PUNCH_HOLE                         = 106; // arg = Address (member)
    public static final int CLOSE_HOLE                         = 107; // arg = Address (member)
    public static final int GET_VIEW_FROM_COORD                = 108;
    public static final int GET_PING_DATA                      = 109; // arg = cluster_name
    public static final int GET_SECRET_KEY                     = 111; // arg = null -> Tuple<SecretKey,byte[]> // PK+version
    public static final int SET_SECRET_KEY                     = 112; // arg = Tuple<SecretKey,byte[]> // PK+version
    public static final int INSTALL_MERGE_VIEW                 = 114; // arg = MergeView
    public static final int IS_LOCAL_SITEMASTER                = 115; // arg = SiteMaster(site), returns true / false
    public static final int IS_LOCAL                           = 116; // arg = SiteAddress(site), returns true / false
    public static final int MBR_DISCONNECTED                   = 117; // arg = Address (member)

    public static final int USER_DEFINED                       = 1000; // arg = <user def., e.g. evt type + data>



    public static final Event GET_DIGEST_EVT=new Event(Event.GET_DIGEST);


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

    public int                  type()    {return type;}
    public int                  getType() {return type;}
    public <T extends Object> T arg()     {return (T)arg;}
    public <T extends Object> T getArg()  {return (T)arg;}



    public static String type2String(int t) {
        switch(t) {
            case CONNECT:	             return "CONNECT";
            case DISCONNECT:	         return "DISCONNECT";
            case VIEW_CHANGE:	         return "VIEW_CHANGE";
            case SUSPECT:                return "SUSPECT";
            case FIND_MBRS:              return "FIND_MBRS";
            case FIND_INITIAL_MBRS:	     return "FIND_INITIAL_MBRS";
            case FIND_MBRS_ASYNC:        return "FIND_MBRS_ASYNC";
            case TMP_VIEW:	             return "TMP_VIEW";
            case BECOME_SERVER:	         return "BECOME_SERVER";
            case GET_APPLSTATE:          return "GET_APPLSTATE";
            case GET_STATE:              return "GET_STATE";
            case GET_STATE_OK:           return "GET_STATE_OK";
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
            case STATE_TRANSFER_INPUTSTREAM: return "STATE_TRANSFER_INPUTSTREAM";
            case STATE_TRANSFER_OUTPUTSTREAM:return "STATE_TRANSFER_OUTPUTSTREAM";
            case STATE_TRANSFER_INPUTSTREAM_CLOSED: return "STATE_TRANSFER_INPUTSTREAM_CLOSED";
            case CLOSE_BARRIER:          return "CLOSE_BARRIER";
            case OPEN_BARRIER:           return "OPEN_BARRIER";
            case CONNECT_WITH_STATE_TRANSFER:    return "CONNECT_WITH_STATE_TRANSFER";
            case GET_PHYSICAL_ADDRESS:   return "GET_PHYSICAL_ADDRESS";
            case GET_LOGICAL_PHYSICAL_MAPPINGS: return "GET_LOGICAL_PHYSICAL_MAPPINGS";
            case ADD_PHYSICAL_ADDRESS:   return "ADD_PHYSICAL_ADDRESS";
            case REMOVE_ADDRESS:         return "REMOVE_ADDRESS";
            case GET_LOCAL_ADDRESS:      return "GET_LOCAL_ADDRESS";
            case IS_MERGE_IN_PROGRESS:   return "IS_MERGE_IN_PROGRESS";
            case GET_PHYSICAL_ADDRESSES: return "GET_PHYSICAL_ADDRESSES";
            case SITE_UNREACHABLE:       return "SITE_UNREACHABLE";
            case MBR_UNREACHABLE:        return "MBR_UNREACHABLE";
            case PUNCH_HOLE:             return "PUNCH_HOLE";
            case CLOSE_HOLE:             return "CLOSE_HOLE";
            case GET_VIEW_FROM_COORD:    return "GET_VIEW_FROM_COORD";
            case GET_PING_DATA:          return "GET_PING_DATA";
            case GET_SECRET_KEY:         return "GET_SECRET_KEY";
            case SET_SECRET_KEY:         return "SET_SECRET_KEY";
            case INSTALL_MERGE_VIEW:     return "INSTALL_MERGE_VIEW";
            case IS_LOCAL_SITEMASTER:    return "IS_LOCAL_SITEMASTER";
            case IS_LOCAL:               return "IS_LOCAL";

            case USER_DEFINED:           return "USER_DEFINED";
            default:                     return "UNDEFINED(" + t + ")";

        }
    }

    public String toString() {return String.format("%s, arg=%s", type2String(type), arg);}

}
