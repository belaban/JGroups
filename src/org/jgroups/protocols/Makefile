## $Id: Makefile,v 1.1 2003/09/09 01:24:10 belaban Exp $

include ../../../../Makefile.include


vpath %.class $(CLASSDIR)/protocols


CLASSFILES=AUTOCONF.class			\
	BSH.class				\
	CAUSAL.class				\
	ClientGmsImpl.class			\
	CoordGmsImpl.class			\
	DEADLOCK.class				\
	DELAY.class				\
	DISCARD.class				\
	DUMMY.class				\
	Digest.class				\
	EXAMPLE.class				\
	FD.class				\
	FD_PID.class				\
	FD_PROB.class				\
	FD_SIMPLE.class				\
	FD_SOCK.class				\
	FLOWCONTROL.class			\
	FLOW_CONTROL.class			\
	FLUSH.class				\
	FRAG.class				\
	FlushRsp.class				\
	GMS.class				\
	GmsImpl.class				\
	HDRS.class				\
	JMS.class				\
	LOOPBACK.class				\
	LOSS.class				\
	MERGE.class				\
	MERGE2.class				\
	MessageProtocolEXAMPLE.class		\
	NAKACK.class				\
	NakAckHeader.class			\
	PARTITIONER.class			\
	PERF.class				\
	PIGGYBACK.class				\
	PING.class				\
	PRINTMETHODS.class			\
	PRINTOBJS.class				\
	ParticipantGmsImpl.class		\
	PerfHeader.class			\
	PingHeader.class			\
	PingRsp.class				\
	QUEUE.class				\
	RpcProtocolEXAMPLE.class		\
	SHUFFLE.class				\
	SIZE.class				\
	SMACK.class				\
	STABLE.class				\
	STATE_TRANSFER.class			\
	TCP.class				\
	TCPGOSSIP.class				\
	TCPPING.class				\
	TOTAL.class				\
	TOTAL_OLD.class				\
	TOTAL_TOKEN.class			\
	TRACE.class				\
	TUNNEL.class				\
	TcpHeader.class				\
	TransportedVectorTime.class		\
	TunnelHeader.class			\
	UDP.class				\
	UNICAST.class				\
	VERIFY_SUSPECT.class			\
	VIEW_ENFORCER.class			\
	UdpHeader.class				\
	VectorTime.class			\
	WANPING.class				\
	WANPIPE.class				\
	WanPipeAddress.class



all: $(CLASSFILES) subdirs

subdirs:;
	@(cd pbcast ; make)
	@(cd ring   ; make)

clean: subdirs-clean
	@rm -f $(CLASSDIR)/protocols/*.class *~ core

subdirs-clean:
	@(cd pbcast ; make clean)
	@(cd ring   ; make clean)





