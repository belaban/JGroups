// $Id: RpcProtocolEXAMPLE.java,v 1.2 2004/03/30 06:47:21 belaban Exp $

package org.jgroups.protocols;



import org.jgroups.Event;
import org.jgroups.stack.RpcProtocol;



/**

 */
public class RpcProtocolEXAMPLE extends RpcProtocol {

    public String  getName() {return "RpcProtocolEXAMPLE";}




    /* ------------------------- Request handler methods ----------------------------- */


    // Your methods, e.g.

    public void foo() {}
    public Object bar(int a, int b) {return null;}


    /* --------------------- End of Request handler methods -------------------------- */




    
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
