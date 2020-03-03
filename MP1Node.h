/**********************************
 * FILE NAME: MP1Node.cpp
 *
 * DESCRIPTION: Membership protocol run by this Node.
 * 				Header file of MP1Node class.
 **********************************/

#ifndef _MP1NODE_H_
#define _MP1NODE_H_

#include "stdincludes.h"
#include "Log.h"
#include "Params.h"
#include "Member.h"
#include "EmulNet.h"
#include "Queue.h"

/**
 * Macros
 */
#define TREMOVE 20
#define TFAIL 5

/*
 * Note: You can change/add any functions in MP1Node.{h,cpp}
 */

/**
 * Message Types
 */
enum MsgTypes{
    JOINREQ = 0,
    JOINREP = 1,
    GOSSIP = 2,
    DUMMYLASTMSGTYPE = 3
};

/**
 * STRUCT NAME: MessageHdr
 *
 * DESCRIPTION: Header and content of a message
 */
typedef struct MessageHdr {
	enum MsgTypes msgType;
}MessageHdr;

/**
 * STRUCT NAME: JoinReqContent
 *
 * DESCRIPTION: The content of a JoinReq message (the node's address and heartbeat) 
 */
typedef struct JoinReqContent {
    char addr[6];
    long heartbeat;
}JoinReqContent;

/**
 * STRUCT NAME: GossipContent
 *
 * DESCRIPTION: The content of a gossip message (membership list)
 */
typedef struct GossipContent {
    long memberCount;
    MemberListEntry memberList[1];
}GossipContent;

typedef union MessageContent {
   JoinReqContent joinReqContent;
   GossipContent gossipContent;
}MessageContent;

typedef struct MP1NodeMessage {
    MessageHdr msgHdr;
    MessageContent msgContent;
}mP1NodeMessage;

/**
 * CLASS NAME: MP1Node
 *
 * DESCRIPTION: Class implementing Membership protocol functionalities for failure detection
 */
class MP1Node {
private:
	EmulNet *emulNet;
	Log *log;
	Params *par;
	Member *memberNode;
	char NULLADDR[6];
    // int timestamp;
public:
	MP1Node(Member *, Params *, EmulNet *, Log *, Address *);
	Member * getMemberNode() {
		return memberNode;
	}
	int recvLoop();
	static int enqueueWrapper(void *env, char *buff, int size);
	void nodeStart(char *servaddrstr, short serverport);
	int initThisNode(Address *joinaddr);
	int introduceSelfToGroup(Address *joinAddress);
	int finishUpThisNode();
	void nodeLoop();
	void checkMessages();
	bool recvCallBack(void *env, char *data, int size);
	void nodeLoopOps();
	int isNullAddress(Address *addr);
	Address getJoinAddress();
	void initMemberListTable(Member *memberNode);
	void printAddress(Address *addr);
	virtual ~MP1Node();
private:

    vector<int> *membersToRemoveList;

    void receivedJoinReq(Member *memberNode, JoinReqContent *mesg_data);
	void receivedJoinRep(Member *memberNode);
    void receivedJoinRep(Member *memberNode, GossipContent *gossip_mesg);
    void receivedGossipMessage(Member *memberNode, GossipContent *gossip_mesg);
    void sendJoinRep(Address *addr);
	void sendMemberList(Address *addr);
    void sendGossipMessage(Address *addr);
    MemberListEntry *getMemberById(int id);
    void identifyAndRemoveFailedNodes();


};

#endif /* _MP1NODE_H_ */