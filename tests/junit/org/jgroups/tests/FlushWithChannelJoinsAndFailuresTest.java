package org.jgroups.tests;

import org.jgroups.Channel;
import org.jgroups.Global;
import org.jgroups.JChannel;
import org.jgroups.util.Util;
import org.testng.annotations.Test;

/**
 * 
 * Flush and join problems during constant node failures and constant joins
 * https://jira.jboss.org/jira/browse/JGRP-985
 * 
 * 
 * @author vladimir
 * @since 2.8
 */
@Test(groups = Global.FLUSH, sequential = true)
public class FlushWithChannelJoinsAndFailuresTest extends ChannelTestBase {

   private static final String cName = "FlushWithChannelFailuresTest";
   
   @Test
   public void testAndLoop() throws Exception {

      int numChannels = 10;
      Channel channels [] = new Channel[numChannels];
      for(int j = 0; j<numChannels;j++){         
         if(j==0){
            channels[j]= createChannel(true,numChannels);
         }
         else{
            channels[j]= createChannel((JChannel)channels[0]);
         }         
         channels[j].connect(cName);         
      }
      
      for (int i = 1; i <= 4; i++) {
         
         //kill coordinator
         Util.shutdown(channels[0]);

         //kill another three members
         Util.shutdown(channels[4]);
         Util.shutdown(channels[5]);
         Util.shutdown(channels[8]); 
         
         //now create members again 
         channels[0]= createChannel((JChannel)channels[1]);
         channels[0].connect(cName);      
         channels[4]= createChannel((JChannel)channels[1]);
         channels[4].connect(cName);      
         channels[5]= createChannel((JChannel)channels[1]);
         channels[5].connect(cName);  
         channels[8]= createChannel((JChannel)channels[1]);
         channels[8].connect(cName);  
                  
         System.out.println("***** Round " + i + " done *****");
      }
      
      for(Channel c:channels){
         assert c.getView().getMembers().size() == 10;
      }      
   }
}