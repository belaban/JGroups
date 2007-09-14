package org.jgroups;


/**
 * <code>StateTransferException</code> is thrown to indicate failure of 
 * state transfer between cluster members.
 * <p>
 * 
 * @author  Vladimir Blagojevic  
 * @since   2.6
 * 
 */
public class StateTransferException extends ChannelException {
   
    private static final long serialVersionUID = -4070956583392020498L;

    public StateTransferException(){    
    }

    public StateTransferException(String reason){
        super(reason);        
    }

    public StateTransferException(String reason,Throwable cause){
        super(reason, cause);        
    }

}
