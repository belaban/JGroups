## $Id: Makefile,v 1.1.1.1 2003/09/09 01:24:09 belaban Exp $

include ../../../../Makefile.include

vpath %.class $(CLASSDIR)/demos


CLASSES = CausalDemo.class			\
          Chat.class				\
	  DistributedHashtableDemo.class	\
	  DistributedTreeDemo.class		\
	  Draw.class				\
	  Draw2Channels.class			\
	  DrawCommand.class			\
	  DrawRepl.class			\
	  Gossip.class				\
	  NotificationBusDemo.class		\
	  QuoteClient.class			\
	  QuoteServer.class			\
	  ReplicatedTreeDemo.class		\
	  Topology.class			\
	  TotalOrder.class			\
	  TotalTokenDemo.class			\
	  ViewDemo.class




this: $(CLASSES) subdirs

subdirs:
	@(cd wb;        make)
	@(cd applets;   make)

subdirs-clean:
	@(cd wb;        make clean)
	@(cd applets;   make clean)


clean: subdirs-clean
	@rm -f $(CLASSDIR)/demos/*.class *~ core






