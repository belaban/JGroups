package org.jgroups.protocols;


import org.jgroups.Address;
import org.jgroups.Event;
import org.jgroups.Message;
import org.jgroups.auth.AuthToken;
import org.jgroups.protocols.pbcast.GMS;
import org.jgroups.protocols.pbcast.JoinRsp;
import org.jgroups.stack.Protocol;
import java.util.Properties;


/**
 * The AUTH protocol adds a layer of authentication to JGroups
 * @author Chris Mills
 */
public class AUTH extends Protocol{

    static final String NAME = "AUTH";

    /**
     * used on the coordinator to authentication joining member requests against
     */
    private AuthToken serverSideToken = null;

    public AUTH(){
    }

    public boolean setProperties(Properties props) {

        String authClassString = props.getProperty("auth_class");

        if(authClassString != null){
            props.remove("auth_class");

            try{
                Object obj = Class.forName(authClassString).newInstance();
                serverSideToken = (AuthToken) obj;
                serverSideToken.setValue(props);
            }catch(Exception e){
                if(log.isFatalEnabled()){
                    log.fatal("Failed to create server side token (" + authClassString + ")");
                    log.fatal(e);
                }
                return false;
            }
        }

        if(props.size() > 0) {
            //this should never happen as everything is read in to the AuthToken instance
            if(log.isErrorEnabled()){
                log.error("AUTH.setProperties(): the following properties are not recognized: " + props);
            }
            return false;
        }
        return true;
    }

    public final String getName() {
        return AUTH.NAME;
    }
    /**
     * Used to create a failed JOIN_RSP message to pass back down the stack
     * @param joiner The Address of the requesting member
     * @param message The failure message to send back to the joiner
     * @return An Event containing a GmsHeader with a JoinRsp object
     */
    private Event createFailureEvent(Address joiner, String message){
        Message msg = new Message(joiner, null, null);

        if(log.isDebugEnabled()){
            log.debug("Creating JoinRsp with failure message - " + message);
        }
        JoinRsp joinRes = new JoinRsp(message);
        //need to specify the error message on the JoinRsp object once it's been changed

        GMS.GmsHeader gmsHeader = new GMS.GmsHeader(GMS.GmsHeader.JOIN_RSP, joinRes);
        msg.putHeader(GMS.name, gmsHeader);

        if(log.isDebugEnabled()){
            log.debug("GMSHeader created for failure JOIN_RSP");
        }

        return new Event(Event.MSG, msg);
    }

    /**
     * An event was received from the layer below. Usually the current layer will want to examine
     * the event type and - depending on its type - perform some computation
     * (e.g. removing headers from a MSG event type, or updating the internal membership list
     * when receiving a VIEW_CHANGE event).
     * Finally the event is either a) discarded, or b) an event is sent down
     * the stack using <code>passDown()</code> or c) the event (or another event) is sent up
     * the stack using <code>passUp()</code>.
     */
    public void up(Event evt) {
        GMS.GmsHeader hdr = isJoinMessage(evt);
        if((hdr != null) && (hdr.getType() == GMS.GmsHeader.JOIN_REQ)){
            if(log.isDebugEnabled()){
                log.debug("AUTH got up event");
            }
            //we found a join message - now try and get the AUTH Header
            Message msg = (Message)evt.getArg();

            if((msg.getHeader(AUTH.NAME) != null) && (msg.getHeader(AUTH.NAME) instanceof AuthHeader)){
                AuthHeader authHeader = (AuthHeader)msg.getHeader(AUTH.NAME);

                if(authHeader != null){
                    //Now we have the AUTH Header we need to validate it
                    if(this.serverSideToken.authenticate(authHeader.getToken(), msg)){
                        //valid token
                        if(log.isDebugEnabled()){
                            log.debug("AUTH passing up event");
                        }
                        passUp(evt);
                    }else{
                        //invalid token
                        if(log.isWarnEnabled()){
                            log.warn("AUTH failed to validate AuthHeader token");
                        }
                        passDown(createFailureEvent(msg.getSrc(), "Authentication failed"));
                    }
                }else{
                    //Invalid AUTH Header - need to send failure message
                    if(log.isWarnEnabled()){
                        log.warn("AUTH failed to get valid AuthHeader from Message");
                    }
                    passDown(createFailureEvent(msg.getSrc(), "Failed to find valid AuthHeader in Message"));
                }
            }else{
                if(log.isDebugEnabled()){
                    log.debug("No AUTH Header Found");
                }
                //should be a failure
                passDown(createFailureEvent(msg.getSrc(), "Failed to find an AuthHeader in Message"));
            }
        }else{
            //if debug
            if(log.isDebugEnabled()){
                log.debug("Message not a JOIN_REQ - ignoring it");
            }
            passUp(evt);
        }
    }

    /**
     * An event is to be sent down the stack. The layer may want to examine its type and perform
     * some action on it, depending on the event's type. If the event is a message MSG, then
     * the layer may need to add a header to it (or do nothing at all) before sending it down
     * the stack using <code>passDown()</code>. In case of a GET_ADDRESS event (which tries to
     * retrieve the stack's address from one of the bottom layers), the layer may need to send
     * a new response event back up the stack using <code>passUp()</code>.
     */
    public void down(Event evt) {
        GMS.GmsHeader hdr = isJoinMessage(evt);
        if((hdr != null) && (hdr.getType() == GMS.GmsHeader.JOIN_REQ)){
            if(log.isDebugEnabled()){
                log.debug("AUTH got down event");
            }
            //we found a join request message - now add an AUTH Header
            Message msg = (Message)evt.getArg();
            AuthHeader authHeader = new AuthHeader();
            authHeader.setToken(this.serverSideToken);
            msg.putHeader(AUTH.NAME, authHeader);

            if(log.isDebugEnabled()){
                log.debug("AUTH passing down event");
            }
        }

        if((hdr != null) && (hdr.getType() == GMS.GmsHeader.JOIN_RSP)){
            if(log.isDebugEnabled()){
                log.debug(hdr.toString());
            }
        }

        passDown(evt);
    }

    /**
     * Used to check if the message type is a Gms message
     * @param evt The event object passed in to AUTH
     * @return A GmsHeader object or null if the event contains a message of a different type
     */
    private static GMS.GmsHeader isJoinMessage(Event evt){
        Message msg;
        switch(evt.getType()){
          case Event.MSG:
                msg = (Message)evt.getArg();
                Object obj = msg.getHeader("GMS");
                if(obj == null || !(obj instanceof GMS.GmsHeader)){
                    return null;
                }
                return (GMS.GmsHeader)obj;
        }
        return null;
    }
}
