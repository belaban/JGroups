## $Id: Makefile,v 1.1 2003/09/09 01:24:13 belaban Exp $

include ../../../../../Makefile.include

vpath %.class $(CLASSDIR)/tests

CLASSFILES=Bsh.class				\
	ChannelTest.class			\
	ConnectionTableTest.class		\
	ContinousThroughputTest.class		\
	DeadlockTest.class			\
	DistributedHashDeadLock.class		\
	DistributedHashtableTest.class		\
	FragTest2.class				\
	FragTestUnicast.class			\
	GetStateTest.class			\
	GroupRequestPull.class			\
	HashtableTest.class			\
	LargeState.class			\
	McastDiscovery1_4.class			\
	McastReceiverTest.class			\
	McastReceiverTest1_4.class		\
	McastSenderTest.class			\
	McastSenderTest1_4.class		\
	McastTest.class				\
	MessageDispatcherTest.class		\
	MessageDispatcherTestAsync.class	\
	MessageSerializationTest.class		\
	NakReceiverWindowStressTest.class	\
	PartitionerTest.class			\
	Ping.class				\
	PullPushTest.class			\
	PullPushTestMux.class			\
	QueueSpeedTest.class			\
	RpcDispatcherBlocking.class		\
	RpcDispatcherTest.class			\
	RpcDispatcherSpeedTest.class		\
	RpcDispatcherStressTest.class		\
	SpeedTest.class				\
	ThreadPoolTest.class			\
	UcastTest.class				\
	UnicastTest.class			\
	UnicastTest2.class			\
	UpHandlerTest.class



all: $(CLASSFILES)



clean: 
	@rm -f $(CLASSDIR)/tests/*.class *~ core
