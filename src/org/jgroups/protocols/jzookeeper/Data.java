package org.jgroups.protocols.jzookeeper;

import java.io.Serializable;

public class Data implements Serializable {
	
    private static final long serialVersionUID=5946678490588947910L;

    private byte type;
    private String data;
    
    public String createBytes(int size){
    	
         for (int i = 0; i < size; i++) {
        	 data+="!";			
		}
         
         return data;
    }
}


