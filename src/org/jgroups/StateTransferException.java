package org.jgroups;

/**
 * {@code StateTransferException} is thrown to indicate a failure of a state transfer between
 * cluster members.
 * <p>
 * 
 * @author Vladimir Blagojevic
 * @since 2.6
 * 
 */
public class StateTransferException extends Exception {
    private static final long serialVersionUID=-4070956583392020498L;

    public StateTransferException(){    
    }

    public StateTransferException(String reason){
        super(reason);        
    }

    public StateTransferException(String reason,Throwable cause) {
        super(reason, cause);        
    }

}
