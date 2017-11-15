package org.jgroups.protocols;

import org.jgroups.*;
import org.jgroups.conf.ClassConfigurator;
import org.jgroups.protocols.tom.*;
import org.jgroups.stack.Protocol;
import org.jgroups.util.Util;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.Collections;
import java.util.LinkedList;
import java.util.List;

import static org.testng.AssertJUnit.assertEquals;

/**
 * Unit test for TOA protocol
 *
 * @author Pedro Ruivo
 * @since 4.0
 */
@Test(groups = {Global.STACK_INDEPENDENT}, singleThreaded = true)
public class TOA_UnitTest {

   private static final short TOA_ID = ClassConfigurator.getProtocolId(TOA.class);

   private final MockUpProtocol upProtocol;
   private final MockDownProtocol downProtocol;
   private final Address localAddress;
   private TOA toa;

   public TOA_UnitTest() {
      upProtocol = new MockUpProtocol();
      downProtocol = new MockDownProtocol();
      localAddress = Util.createRandomAddress("A");
   }

   @BeforeMethod
   public void setup() throws Exception {
      toa = new TOA();
      toa.setUpProtocol(upProtocol);
      toa.setDownProtocol(downProtocol);
      toa.start();
      toa.stop(); //stops the deliver thread. it isn't needed
      toa.down(new Event(Event.SET_LOCAL_ADDRESS, localAddress));
      upProtocol.messages.clear();
      downProtocol.messages.clear();
   }

   @AfterMethod
   public void shutdown() {
      toa = null;
      upProtocol.messages.clear();
      downProtocol.messages.clear();
   }

   public void testDataMessageFromNonMember() {
      Address b = Util.createRandomAddress("B");
      Address c = Util.createRandomAddress("C");

      setInitialView(localAddress, localAddress, b);

      ToaHeader header = ToaHeader.newDataMessageHeader(new MessageID(c, 1), 1);
      Message message = newMessage(header, c, localAddress);

      //Message from member C. it isn't in the view and the message should be discarded.
      toa.up(message);

      assertEquals("Deliver Manager doesn't have an empty list", Collections.emptyList(),
            deliveryManager().getAllMessages());
      assertDownMessagesEmpty();
      assertUpMessagesEmpty();
   }

   public void testSameSequenceNumberMessage() {
      Address b = Util.createRandomAddress("B");

      setInitialView(localAddress, localAddress, b);
      sequenceNumberManager().update(9); //it increments it to 10

      ToaHeader header = ToaHeader.newDataMessageHeader(new MessageID(b, 1), 1);
      header.setSequencerNumber(10);
      Message message = newMessage(header, b, localAddress);

      //Message from member C. it isn't in the view and the message should be discarded.
      toa.up(message);

      assertFirstMessage(10, false);
      assertEquals(11, sequenceNumberManager().get());
      assertProposedMessageSent(10);


      header = ToaHeader.newFinalMessageHeader(new MessageID(b, 1), 15);
      message = newMessage(header, b, localAddress);

      toa.up(message);

      assertFirstMessage(15, true);
      assertEquals(16, sequenceNumberManager().get());

      assertDownMessagesEmpty();
      assertUpMessagesEmpty();
   }

   public void testLowerSequenceNumberMessage() {
      Address b = Util.createRandomAddress("B");

      setInitialView(localAddress, localAddress, b);
      sequenceNumberManager().update(10); //next message will have 11

      ToaHeader header = ToaHeader.newDataMessageHeader(new MessageID(b, 1), 1);
      header.setSequencerNumber(2);
      Message message = newMessage(header, b, localAddress);

      //Message from member C. it isn't in the view and the message should be discarded.
      toa.up(message);

      assertFirstMessage(11, false);
      assertEquals(12, sequenceNumberManager().get());
      assertProposedMessageSent(11);


      header = ToaHeader.newFinalMessageHeader(new MessageID(b, 1), 11);
      message = newMessage(header, b, localAddress);

      toa.up(message);

      assertFirstMessage(11, true);
      assertEquals(12, sequenceNumberManager().get());

      assertDownMessagesEmpty();
      assertUpMessagesEmpty();
   }

   public void testHigherSequenceNumberMessage() {
      Address b = Util.createRandomAddress("B");

      setInitialView(localAddress, localAddress, b);
      sequenceNumberManager().update(10); //next message will have 11

      ToaHeader header = ToaHeader.newDataMessageHeader(new MessageID(b, 1), 1);
      header.setSequencerNumber(20);
      Message message = newMessage(header, b, localAddress);

      //Message from member C. it isn't in the view and the message should be discarded.
      toa.up(message);

      assertFirstMessage(20, false);
      assertEquals(21, sequenceNumberManager().get());
      assertProposedMessageSent(20);


      header = ToaHeader.newFinalMessageHeader(new MessageID(b, 1), 20);
      message = newMessage(header, b, localAddress);

      toa.up(message);

      assertFirstMessage(20, true);
      assertEquals(21, sequenceNumberManager().get());

      assertDownMessagesEmpty();
      assertUpMessagesEmpty();
   }

   public void testMessageBeforeFirstView() {
      Address b = Util.createRandomAddress("B");

      ToaHeader header = ToaHeader.newDataMessageHeader(new MessageID(b, 1), 1);
      Message message = newMessage(header, b, localAddress);

      // message from node B to A.
      // no view is installed yet.
      // message should be accepted
      toa.up(message);
      assertProposedMessageSent(0);
      assertFirstMessage(0, false);

      header = ToaHeader.newFinalMessageHeader(new MessageID(b, 1), 1);
      message = newMessage(header, b, localAddress);

      // final message
      // no view installed yet
      toa.up(message);
      assertFirstMessage(1, true);
      assertDownMessagesEmpty();
      assertUpMessagesEmpty();
   }

   public void testMessageBeforeFirstViewDead() {
      Address b = Util.createRandomAddress("B");

      ToaHeader header = ToaHeader.newDataMessageHeader(new MessageID(b, 1), 1);
      Message message = newMessage(header, b, localAddress);

      // message from node B to A.
      // no view is installed yet.
      // message should be accepted
      toa.up(message);
      assertProposedMessageSent(0);
      assertFirstMessage(0, false);

      // view {2|A}
      setView(localAddress, 2, localAddress);

      // message dropped. member no longer belongs to the view.
      assertNoMessageToDeliver();
      assertDownMessagesEmpty();
      assertUpMessagesEmpty();
   }

   public void testMessageBeforeFirstViewAlive() {
      Address b = Util.createRandomAddress("B");

      ToaHeader header = ToaHeader.newDataMessageHeader(new MessageID(b, 1), 1);
      Message message = newMessage(header, b, localAddress);

      // message from node B to A.
      // no view is installed yet.
      // message should be accepted
      toa.up(message);
      assertProposedMessageSent(0);
      assertFirstMessage(0, false);

      // view {2|A,B}
      setView(localAddress, 2, localAddress, b);

      // message is kept
      assertFirstMessage(0, false);

      header = ToaHeader.newFinalMessageHeader(new MessageID(b, 1), 1);
      message = newMessage(header, b, localAddress);

      // final message
      toa.up(message);
      assertFirstMessage(1, true);
      assertDownMessagesEmpty();
      assertUpMessagesEmpty();
   }

   public void testMessageFromPreviousViewAlive() {
      Address b = Util.createRandomAddress("B");

      // view {4|A,B}
      setView(localAddress, 4, localAddress, b);

      ToaHeader header = ToaHeader.newDataMessageHeader(new MessageID(b, 1), 3);
      Message message = newMessage(header, b, localAddress);

      // message from node B to A from view 3.
      // message is accepted
      toa.up(message);
      assertProposedMessageSent(0);
      assertFirstMessage(0, false);

      header = ToaHeader.newFinalMessageHeader(new MessageID(b, 1), 1);
      message = newMessage(header, b, localAddress);

      // final message
      toa.up(message);
      assertFirstMessage(1, true);
      assertDownMessagesEmpty();
      assertUpMessagesEmpty();
   }

   public void testMessageFromPreviousViewDead() {
      Address b = Util.createRandomAddress("B");

      // view {4|A}
      setView(localAddress, 4, localAddress);

      ToaHeader header = ToaHeader.newDataMessageHeader(new MessageID(b, 1), 3);
      Message message = newMessage(header, b, localAddress);

      // message from node B to A from view 3.
      // B isn't in the view and the message should be discarded
      toa.up(message);
      assertNoMessageToDeliver();
      assertDownMessagesEmpty();
      assertUpMessagesEmpty();
   }

   public void testMessageFromNewView() {
      Address b = Util.createRandomAddress("B");

      // view {1|A}
      setInitialView(localAddress, localAddress);

      ToaHeader header = ToaHeader.newDataMessageHeader(new MessageID(b, 1), 2);
      Message message = newMessage(header, b, localAddress);

      // message from node B to A from view 2.
      // B isn't in the view but the message is accepted
      toa.up(message);
      assertProposedMessageSent(0);
      assertFirstMessage(0, false);

      // view {2|A,B}
      // message is kept
      setView(localAddress, 2, localAddress, b);
      assertFirstMessage(0, false);

      header = ToaHeader.newFinalMessageHeader(new MessageID(b, 1), 1);
      message = newMessage(header, b, localAddress);

      //final message
      toa.up(message);
      assertFirstMessage(1, true);
      assertDownMessagesEmpty();
      assertUpMessagesEmpty();
   }


   private DeliveryManagerImpl deliveryManager() {
      return (DeliveryManagerImpl) toa.getDeliverManager();
   }

   private SequenceNumberManager sequenceNumberManager() {
      return deliveryManager().getSequenceNumberManager();
   }

   private void assertNoMessageToDeliver() {
      assertEquals(Collections.emptyList(), deliveryManager().getAllMessages());
   }

   private void assertProposedMessageSent(long expectedSequenceNumber) {
      assertEquals(1, downProtocol.messages.size());
      Message message = downProtocol.messages.remove(0);
      ToaHeader header = message.getHeader(TOA_ID);
      assertEquals(ToaHeader.PROPOSE_MESSAGE, header.getType());
      assertEquals(expectedSequenceNumber, header.getSequencerNumber());
   }

   private void assertFirstMessage(long sequenceNumber, boolean isReady) {
      List<DeliveryManagerImpl.MessageInfo> messages = deliveryManager().getAllMessages();
      assertEquals(1, messages.size());
      DeliveryManagerImpl.MessageInfo pending = messages.get(0);
      assertEquals(sequenceNumber, pending.getSequenceNumber());
      assertEquals(isReady, pending.isReadyToDeliver());
   }

   private void assertUpMessagesEmpty() {
      assertEquals("UP message list isn't empty.", Collections.emptyList(), upProtocol.messages);
   }

   private void assertDownMessagesEmpty() {
      assertEquals("DOWN message list isn't empty.", Collections.emptyList(), upProtocol.messages);
   }

   private void setInitialView(Address localAddress, Address... members) {
      setView(localAddress, 1, members);
   }

   private void setView(Address localAddress, long viewId, Address... members) {
      toa.down(new Event(Event.SET_LOCAL_ADDRESS, localAddress));
      toa.down(new Event(Event.VIEW_CHANGE, View.create(localAddress, viewId, members)));
   }

   private static Message newMessage(ToaHeader header, Address from, Address to) {
      return new EmptyMessage(to).setSrc(from).putHeader(TOA_ID, header);
   }

   private static class MockUpProtocol extends Protocol {

      private final List<Message> messages;

      private MockUpProtocol() {
         messages = Collections.synchronizedList(new LinkedList<>());
      }

      @Override
      public Object up(Message msg) {
         messages.add(msg);
         return null;
      }

      @Override
      public Object up(Event evt) {
         return null; //no-op
      }
   }

   private static class MockDownProtocol extends Protocol {

      private final List<Message> messages;

      private MockDownProtocol() {
         messages = Collections.synchronizedList(new LinkedList<>());
      }

      @Override
      public Object down(Event evt) {
         return null; //no-op
      }

      @Override
      public Object down(Message msg) {
         messages.add(msg);
         return null;
      }
   }

}
