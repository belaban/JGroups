## $Id: Makefile,v 1.1 2003/09/09 01:24:13 belaban Exp $

include ../../../../../Makefile.include

vpath %.class $(CLASSDIR)/tests

CLASSFILES= AckMcastSenderWindowTest.class	\
	AckSenderWindowTest.class		\
	CloseTest.class				\
	CommonTest.class			\
	ConnectTest.class			\
	DisconnectTest.class			\
	DigestTest.class			\
	FragTest.class				\
	InterruptTest.class			\
	IpAddressTest.class			\
	MembershipTest.class			\
	MessageLoadTest.class			\
	NakReceiverWindowTest.class		\
	NakackTest.class			\
	QueueTest.class				\
	ReusableThreadTest.class		\
	SchedulerTest.class			\
	SendAndReceiveTest.class		\
	TimeSchedulerTest.class			\
	TimerTest.class				\
	ViewIdTest.class			\
	ViewTest.class				\
	XmlConfigurationTest.class


all: $(CLASSFILES)



clean: 
	@rm -f $(CLASSDIR)/tests/*.class *~ core
