package org.jgroups.tests;

import org.jgroups.Channel;
import org.jgroups.Global;
import org.jgroups.JChannel;
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
         channels[j].connect("FlushWithChannelFailuresTest");         
      }
      
      for (int i = 1; i <= 4; i++) {
         
         //kill coordinator
         channels[0].shutdown();
         
         //kill another three members
         channels[4].shutdown();
         channels[5].shutdown();                           
         channels[8].shutdown(); 
         
         //now create members again 
         channels[0]= createChannel((JChannel)channels[1]);
         channels[0].connect("FlushWithChannelFailuresTest");      
         channels[4]= createChannel((JChannel)channels[1]);
         channels[4].connect("FlushWithChannelFailuresTest");      
         channels[5]= createChannel((JChannel)channels[1]);
         channels[5].connect("FlushWithChannelFailuresTest");  
         channels[8]= createChannel((JChannel)channels[1]);
         channels[8].connect("FlushWithChannelFailuresTest");  
                  
         System.out.println("***** Round " + i + " done *****");
      }
      
      for(Channel c:channels){
         assert c.getView().getMembers().size() == 10;
      }      
   }
}