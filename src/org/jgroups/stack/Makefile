## $Id: Makefile,v 1.1 2003/09/09 01:24:12 belaban Exp $

include ../../../../Makefile.include

vpath %.class $(CLASSDIR)/stack

CLASSFILES=AckMcastReceiverWindow.class		\
	   AckMcastSenderWindow.class		\
	   AckReceiverWindow.class		\
	   AckSenderWindow.class		\
	   Configurator.class			\
	   GossipClient.class			\
	   GossipData.class			\
	   GossipServer.class			\
	   Interval.class			\
	   IpAddress.class			\
	   MessageProtocol.class		\
	   NakReceiverWindow.class		\
	   Protocol.class			\
	   ProtocolObserver.class		\
	   ProtocolStack.class			\
	   Retransmitter.class			\
	   Router.class				\
	   RouterStub.class			\
	   RpcProtocol.class			\
	   StateTransferInfo.class


all: $(CLASSFILES)



clean: 
	@rm -f $(CLASSDIR)/stack/*.class *~ core



#GossipServer.class: GossipServer.java Gossip.java
#	javac $<
#	rmic JavaGroups.JavaStack.GossipServer







