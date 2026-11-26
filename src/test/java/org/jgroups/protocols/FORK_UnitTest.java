package org.jgroups.protocols;

import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertNotNull;
import static org.testng.AssertJUnit.assertTrue;

import java.util.Collections;
import java.util.concurrent.BlockingDeque;
import java.util.concurrent.LinkedBlockingDeque;

import org.jgroups.Event;
import org.jgroups.Global;
import org.jgroups.stack.Protocol;
import org.jgroups.stack.ProtocolStack;
import org.testng.annotations.Test;

/**
 * Unit test for the {@link FORK} protocol.
 *
 * @author Pedro Ruivo
 * @since 4.1
 */
@Test(groups = {Global.STACK_INDEPENDENT}, singleThreaded = true)
public class FORK_UnitTest {


   private static void assertEvent(EventQueueProtocol protocol, int eventType) {
      Event e = protocol.queue.poll();
      assertNotNull(e);
      assertEquals(eventType, e.getType());
   }

   private static void assertEmpty(EventQueueProtocol protocol) {
      assertTrue(protocol.queue.isEmpty());
   }

   /**
    * Tests if the {@link Event#SITE_UNREACHABLE} is received in all the protocol stacks.
    */
   public void testSiteUnreachableEvent() throws Exception {
      EventQueueProtocol main = new EventQueueProtocol();
      EventQueueProtocol stack1 = new EventQueueProtocol();
      EventQueueProtocol stack2 = new EventQueueProtocol();

      final FORK fork = new FORK();
      fork.setProtocolStack(new ProtocolStack());
      fork.setUpProtocol(main);
      fork.createForkStack("stack-1", Collections.singletonList(stack1), true);
      fork.createForkStack("stack-2", Collections.singletonList(stack2), true);

      assertEmpty(main);
      assertEmpty(stack1);
      assertEmpty(stack2);

      //mock a SITE_UNREACHABLE event from RELAY2
      fork.up(new Event(Event.SITE_UNREACHABLE));

      // check if the event is received
      assertEvent(main, Event.SITE_UNREACHABLE);
      assertEvent(stack1, Event.SITE_UNREACHABLE);
      assertEvent(stack2, Event.SITE_UNREACHABLE);

      // check no more events
      assertEmpty(main);
      assertEmpty(stack1);
      assertEmpty(stack2);
   }

   private static class EventQueueProtocol extends Protocol {

      private final BlockingDeque<Event> queue = new LinkedBlockingDeque<>();

      @Override
      public Object up(Event evt) {
         queue.add(evt);
         return null;
      }
   }

}
