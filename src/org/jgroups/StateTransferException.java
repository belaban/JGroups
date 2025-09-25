package org.jgroups;

/**
 * {@code StateTransferException} is thrown to indicate a failure of a state transfer between
 * cluster members.
 *
 * @author Vladimir Blagojevic
 * @since 2.6
 * 
 */
public class StateTransferException extends JGroupsException {
    private static final long serialVersionUID=-4070956583392020498L;

    public StateTransferException(String reason){
        super(reason);        
    }

    public StateTransferException(Throwable cause) {
        super(cause);        
    }
}
