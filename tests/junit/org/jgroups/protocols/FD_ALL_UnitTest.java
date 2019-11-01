package org.jgroups.protocols;

import org.jgroups.Address;
import org.jgroups.Event;
import org.jgroups.View;
import org.jgroups.stack.Protocol;
import org.testng.annotations.Test;

import java.io.DataInput;
import java.io.DataOutput;
import java.util.Arrays;

import static org.testng.Assert.assertEquals;

@Test
public class FD_ALL_UnitTest {

   /* JDG-3264 drop suspicions of non-members: this way, no messages will be sent to non-existant members (e.g. by VERIFY_SUSPECT). */
   public void testDropSuspicionsNonMembers() {

      // when the cluster is up and running
      Address address1 = mockAddress();
      Address address2 = mockAddress();

      Test_FD_ALL fd_all = new Test_FD_ALL();
      fd_all.setDownProtocol(mockProtocol());
      fd_all.down(new Event(Event.VIEW_CHANGE, new View(null, Arrays.asList(address1, address2))));

      // check for failures
      fd_all.startFailureDetection();
      assertEquals(2, fd_all.getTimestampsKeySize());

      // a message was sent from another node
      fd_all.down(new Event(Event.UNSUSPECT, mockAddress()));

      // check for failures
      fd_all.startFailureDetection();
      assertEquals(2, fd_all.getTimestampsKeySize());
   }

   private class Test_FD_ALL extends FD_ALL {
      TimeoutChecker timeoutChecker = new TimeoutChecker();
      @Override
      protected void startHeartbeatSender() {
      }
      @Override
      protected void startTimeoutChecker() {
      }
      @Override
      public void startFailureDetection() {
         timeoutChecker.run();
      }
   }

   private Protocol mockProtocol() {
      return new Protocol() {
         @Override
         public Object down(Event evt) {
            return null;
         }
      };
   }

   private Address mockAddress() {
      return new Address() {
         @Override
         public int compareTo(Address o) {
            return 0;
         }
         @Override
         public int serializedSize() {
            return 0;
         }
         @Override
         public void writeTo(DataOutput out) {
         }
         @Override
         public void readFrom(DataInput in) {
         }
      };
   }
}
