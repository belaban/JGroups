// $Id: MessageProtocolEXAMPLE.java,v 1.1.1.1 2003/09/09 01:24:10 belaban Exp $

package org.jgroups.protocols;




import org.jgroups.*;
import org.jgroups.stack.*;






/**

 */
public class MessageProtocolEXAMPLE extends MessageProtocol {

    public String  getName() {return "MessageProtocolEXAMPLE";}


    /**
       <b>Callback</b>. Called when a request for this protocol layer is received.
     */
    public Object handle(Message req) {
	System.out.println("MessageProtocolEXAMPLE.handle(): this method should be overridden !");
	return null;
    }



    
    /**
       <b>Callback</b>. Called by superclass when event may be handled.<p>
       <b>Do not use <code>PassUp</code> in this method as the event is passed up
       by default by the superclass after this method returns !</b>
       @return boolean Defaults to true. If false, event will not be passed up the stack.
     */
    public boolean handleUpEvent(Event evt) {return true;}


    /**
       <b>Callback</b>. Called by superclass when event may be handled.<p>
       <b>Do not use <code>PassDown</code> in this method as the event is passed down
       by default by the superclass after this method returns !</b>
       @return boolean Defaults to true. If false, event will not be passed down the stack.
    */
    public boolean handleDownEvent(Event evt) {return true;}



}
