package org.jgroups.protocols.jzookeeper;

import java.util.Random;

public class ZUtil {
	
 	static final double  PROP = 0.5;
 	static double randomValue;
 	static Random random = new Random();
 	static boolean sendAck = false;
 	
 	
 	
public static boolean SendAckOrNoSend(){
		
    	
		randomValue=random.nextDouble();
		//System.out.println(randomValue);

		//TO check whether to send Ack or not
		return randomValue <= PROP? true : false;
			

}
}
