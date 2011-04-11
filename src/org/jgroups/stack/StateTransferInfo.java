
package org.jgroups.stack;


import java.io.InputStream;
import java.io.OutputStream;

import org.jgroups.Address;


/**
 * Contains parameters for state transfer. Exchanged between channel and STATE_TRANSFER
 * layer. The state is retrieved from 'target'. If target is null, then the state will be retrieved from the oldest
 * member (usually the coordinator).
 * @author Bela Ban
 */
public class StateTransferInfo {
    public Address      target=null;
    public long         timeout=0;
    public byte[]       state=null;
    public InputStream  inputStream = null;
    public OutputStream outputStream = null;



    public StateTransferInfo() {
    }

    public StateTransferInfo(Address target) {
        this.target=target;
    }

     public StateTransferInfo(Address target, long timeout) {
        this.target=target;
        this.timeout=timeout;
    }

    public StateTransferInfo(Address target, long timeout, byte[] state) {
        this.target=target;
        this.timeout=timeout;
        this.state=state;
    }


    public StateTransferInfo(Address target, InputStream is) {
        this.target=target;
        this.inputStream=is;
    }

    public StateTransferInfo(Address target, OutputStream os) {
        this.target=target;
        this.outputStream=os;
    }




    public StateTransferInfo copy() {
       if(inputStream!=null){
          return new StateTransferInfo(target,inputStream);
       }
       else if(outputStream!=null){
          return new StateTransferInfo(target,outputStream);
       }
       else{
          return new StateTransferInfo(target, timeout, state);
       }
    }


    public String toString() {
        StringBuilder ret=new StringBuilder();
        ret.append("target=" + target);
        if(state != null)
            ret.append(", state=" + state.length + " bytes");
        ret.append(", timeout=" + timeout);
        return ret.toString();
    }
}
