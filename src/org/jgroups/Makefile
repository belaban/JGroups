### $Id: Makefile,v 1.1 2003/09/09 01:24:07 belaban Exp $

include ../../../Makefile.include


JJ=$(JAVAC) $(JFLAGS)


vpath %.class $(CLASSDIR)


CLASSFILES=Address.class			\
	   ChannelException.class		\
	   Message.class			\
	   Event.class				\
	   UpHandler.class			\
	   Header.class				\
	   View.class				\
           Transport.class			\
	   Channel.class			\
	   ChannelListener.class		\
	   ChannelFactory.class			\
	   MergeView.class			\
	   ViewId.class				\
	   Membership.java			\
	   MembershipListener.class		\
	   MessageListener.class		\
	   ChannelNotConnectedException.class	\
	   ChannelClosedException.class		\
	   SuspectedException.class		\
	   JChannel.class			\
	   JChannelFactory.class		\
	   EnsChannel.class			\
	   EnsChannelFactory.class		\
	   TimeoutException.class		\
	   Version.class			\
           BlockEvent.class			\
	   SuspectEvent.class			\
	   GetStateEvent.class			\
	   SetStateEvent.class			\
	   ExitEvent.class




all: $(CLASSFILES) subdirs


subdirs:
	 @(cd blocks             ; make)
	 @(cd conf               ; make)
	 @(cd debug              ; make)
	 @(cd demos		 ; make)
	 @(cd ensemble           ; make)
	 @(cd log  		 ; make)
	 @(cd protocols  	 ; make)
	 @(cd stack              ; make)
	 @(cd service            ; make)
	 @(cd util               ; make)



subdirs-clean:
	 @(cd blocks             ; make clean)
	 @(cd conf               ; make clean)
	 @(cd debug              ; make clean)
	 @(cd demos		 ; make clean)
	 @(cd ensemble           ; make clean)
	 @(cd log  		 ; make clean)
	 @(cd protocols  	 ; make clean)
	 @(cd stack              ; make clean)
	 @(cd service            ; make clean)
	 @(cd util               ; make clean)






clean: subdirs-clean
	@rm -fr *~ core*

