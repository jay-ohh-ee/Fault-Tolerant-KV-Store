/**********************************
 * FILE NAME: MP2Node.h
 *
 * DESCRIPTION: MP2Node class header file
 **********************************/

#ifndef MP2NODE_H_
#define MP2NODE_H_

/**
 * Header files
 */
#include "stdincludes.h"
#include "EmulNet.h"
#include "Node.h"
#include "HashTable.h"
#include "Log.h"
#include "Params.h"
#include "Message.h"
#include "Queue.h"

/**
 * This is a struct that is used to hold all required data for a specific transaction.  
 * This is helpful for determining a quorum.  Given that messages are asynchronous they will not necessarily
 * arrive all at once and rather than continuously go back and recover the messages for each transaction this
 * structure will be much quicker to reference.
 */
typedef struct TransactionData {
	MessageType msgType;
	string key;
	string value;
	int failureReplyCount;
	int successReplyCount;
	bool isFailed;
	bool isCommitted;
	vector<Address> failedAddrs;
	vector<Address> successfulAddrs;
	int transId;
} TransactionData;


/**
 * CLASS NAME: MP2Node
 *
 * DESCRIPTION: This class encapsulates all the key-value store functionality
 * 				including:
 * 				1) Ring
 * 				2) Stabilization Protocol
 * 				3) Server side CRUD APIs
 * 				4) Client side CRUD APIs
 */
class MP2Node {
private:
	// Vector holding the next two neighbors in the ring who have my replicas
	vector<Node> hasMyReplicas;
	// Vector holding the previous two neighbors in the ring whose replicas I have
	vector<Node> haveReplicasOf;
	// Ring
	vector<Node> ring;
	// Hash Table
	HashTable * ht;
	// Member representing this member
	Member *memberNode;
	// Params object
	Params *par;
	// Object of EmulNet
	EmulNet * emulNet;
	// Object of Log
	Log * log;
	// A mapping of all the different data for each transaction
	map<int, TransactionData> transactionHistory;

public:
	MP2Node(Member *memberNode, Params *par, EmulNet *emulNet, Log *log, Address *addressOfMember);
	Member * getMemberNode() {
		return this->memberNode;
	}

	// ring functionalities
	void updateRing();
	vector<Node> getMembershipList();
	size_t hashFunction(string key);
	void findNeighbors();

	// client side CRUD APIs
	void clientCreate(string key, string value);
	void clientRead(string key);
	void clientUpdate(string key, string value);
	void clientDelete(string key);

	// receive messages from Emulnet
	bool recvLoop();
	static int enqueueWrapper(void *env, char *buff, int size);

	// handle messages from receiving queue
	void checkMessages();

	// coordinator dispatches messages to corresponding nodes
	void dispatchMessages(Message message);

	// find the addresses of nodes that are responsible for a key
	vector<Node> findNodes(string key);

	// server
	bool createKeyValue(string key, string value, ReplicaType replica);
	string readKey(string key);
	bool updateKeyValue(string key, string value, ReplicaType replica);
	bool deletekey(string key);

	// stabilization protocol - handle multiple failures
	void stabilizationProtocol();

	~MP2Node();
};

class MessageMetadata {
	public:
		MessageType msgType;
		ReplicaType replicaType;
		int transID;
		bool success;
		long timeStamp;
		MessageMetadata(){ }
};

class MessageData {
	public:
		string key;
		string value;
		Address fromAddr;	
		MessageData();
		MessageData(string k, string v): key(k), value(v) { }
		MessageData(string k, string v, Address addr): key(k), value(v), fromAddr(addr) { }
};

// Wrapper class for the message and added function
// https://www.geeksforgeeks.org/when-do-we-use-initializer-list-in-c/
class WrapperMessage {
	public:
		// MessageType msgType;
		// ReplicaType replicaType;
		// string key;
		// string value;
		// Address fromAddr;
		// int transID;
		// bool success;
		MessageData msgData;
		MessageMetadata msgMeta;
		WrapperMessage();
};

#endif /* MP2NODE_H_ */
