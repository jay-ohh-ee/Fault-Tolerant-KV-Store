/**********************************
 * FILE NAME: MP2Node.cpp
 *
 * DESCRIPTION: MP2Node class definition
 **********************************/
#include "MP2Node.h"

/**
 * constructor
 */
MP2Node::MP2Node(Member *memberNode, Params *par, EmulNet * emulNet, Log * log, Address * address) {
	this->memberNode = memberNode;
	this->par = par;
	this->emulNet = emulNet;
	this->log = log;
	ht = new HashTable();
	this->memberNode->addr = *address;
}

/**
 * Destructor
 */
MP2Node::~MP2Node() {
	delete ht;
	delete memberNode;
}

/**
 * FUNCTION NAME: updateRing
 *
 * DESCRIPTION: This function does the following:
 * 				1) Gets the current membership list from the Membership Protocol (MP1Node)
 * 				   The membership list is returned as a vector of Nodes. See Node class in Node.h
 * 				2) Constructs the ring based on the membership list
 * 				3) Calls the Stabilization Protocol
 */
void MP2Node::updateRing() {
	/*
	 * Implement this. Parts of it are already implemented
	 */
	vector<Node> curMemList;
	bool change = false;

	/*
	 *  Step 1. Get the current membership list from Membership Protocol / MP1
	 */
	curMemList = getMembershipList();

	/*
	 * Step 2: Construct the ring
	 */
	// Sort the list based on the hashCode
	sort(curMemList.begin(), curMemList.end());


	/*
	 * Step 3: Run the stabilization protocol IF REQUIRED
	 */
	// Run stabilization protocol if the hash table size is greater than zero and if there has been a changed in the ring
	stabilizationProtocol();
}

/**
 * FUNCTION NAME: getMemberhipList
 *
 * DESCRIPTION: This function goes through the membership list from the Membership protocol/MP1 and
 * 				i) generates the hash code for each member
 * 				ii) populates the ring member in MP2Node class
 * 				It returns a vector of Nodes. Each element in the vector contain the following fields:
 * 				a) Address of the node
 * 				b) Hash code obtained by consistent hashing of the Address
 */
vector<Node> MP2Node::getMembershipList() {
	unsigned int i;
	vector<Node> curMemList;
	for ( i = 0 ; i < this->memberNode->memberList.size(); i++ ) {
		Address addressOfThisMember;
		int id = this->memberNode->memberList.at(i).getid();
		short port = this->memberNode->memberList.at(i).getport();
		memcpy(&addressOfThisMember.addr[0], &id, sizeof(int));
		memcpy(&addressOfThisMember.addr[4], &port, sizeof(short));
		curMemList.emplace_back(Node(addressOfThisMember));
	}
	return curMemList;
}

/**
 * FUNCTION NAME: hashFunction
 *
 * DESCRIPTION: This functions hashes the key and returns the position on the ring
 * 				HASH FUNCTION USED FOR CONSISTENT HASHING
 *
 * RETURNS:
 * size_t position on the ring
 */
size_t MP2Node::hashFunction(string key) {
	std::hash<string> hashFunc;
	size_t ret = hashFunc(key);
	return ret%RING_SIZE;
}

/**
 * FUNCTION NAME: clientCreate
 *
 * DESCRIPTION: client side CREATE API
 * 				The function does the following:
 * 				1) Constructs the message
 * 				2) Finds the replicas of this key
 * 				3) Sends a message to the replica
 */
void MP2Node::clientCreate(string key, string value) {
	// Increment the global transaction Id 
	g_transID++;

	// Get the repicas for the key
	vector<Node> replicas = findNodes(key);

	int replicaType = 0;
	// Loop through the replicas and create a message for each one.
	// Use the for-loop variable to access the proper enum value for ReplicationType
	for(auto &&node: replicas) {
		// create message
		Message createMsg = Message(g_transID, this->memberNode->addr, CREATE, key, value, ReplicaType(replicaType));
		// send message to emulnet
		this->emulNet->ENsend(&memberNode->addr, &node.nodeAddress, (char*) &createMsg, sizeof(createMsg));
		// increase to next ReplicaType
		replicaType++;
	}

}

/**
 * FUNCTION NAME: clientRead
 *
 * DESCRIPTION: client side READ API
 * 				The function does the following:
 * 				1) Constructs the message
 * 				2) Finds the replicas of this key
 * 				3) Sends a message to the replica
 */
void MP2Node::clientRead(string key){
	// Increment the global transaction Id 
	g_transID++;

	// Get the repicas for the key
	vector<Node> replicas = findNodes(key);

	// Loop through the replicas and create a message for each one.
	// Use the for-loop variable to access the proper enum value for ReplicationType
	for(auto &&node: replicas) {
		// create message
		Message readMsg = Message(g_transID, this->memberNode->addr, READ, key);
		// send message to emulnet
		this->emulNet->ENsend(&memberNode->addr, &node.nodeAddress, (char*) &readMsg, sizeof(readMsg));		
	}
}

/**
 * FUNCTION NAME: clientUpdate
 *
 * DESCRIPTION: client side UPDATE API
 * 				The function does the following:
 * 				1) Constructs the message
 * 				2) Finds the replicas of this key
 * 				3) Sends a message to the replica
 */
void MP2Node::clientUpdate(string key, string value){
	// Increment the global transaction Id 
	g_transID++;

	// Get the repicas for the key
	vector<Node> replicas = findNodes(key);

	int replicaType = 0;
	// Loop through the replicas and create a message for each one.
	// Use the for-loop variable to access the proper enum value for ReplicationType
	for(auto &&node: replicas) {
		// create message
		Message updateMsg = Message(g_transID, this->memberNode->addr, UPDATE, key, value, ReplicaType(replicaType));
		// send message to emulnet
		this->emulNet->ENsend(&memberNode->addr, &node.nodeAddress, (char*) &updateMsg, sizeof(updateMsg));
		// increase to next ReplicaType
		replicaType++;
	}
}

/**
 * FUNCTION NAME: clientDelete
 *
 * DESCRIPTION: client side DELETE API
 * 				The function does the following:
 * 				1) Constructs the message
 * 				2) Finds the replicas of this key
 * 				3) Sends a message to the replica
 */
void MP2Node::clientDelete(string key){
	// Increment the global transaction Id 
	g_transID++;

	// Get the repicas for the key
	vector<Node> replicas = findNodes(key);

	// Loop through the replicas and create a message for each one.
	// Use the for-loop variable to access the proper enum value for ReplicationType
	for(auto &&node: replicas) {
		// create message
		Message deleteMsg = Message(g_transID, this->memberNode->addr, DELETE, key);
		// send message to emulnet
		this->emulNet->ENsend(&memberNode->addr, &node.nodeAddress, (char*) &deleteMsg, sizeof(deleteMsg));
	}
}

/**
 * FUNCTION NAME: createKeyValue
 *
 * DESCRIPTION: Server side CREATE API
 * 			   	The function does the following:
 * 			   	1) Inserts key value into the local hash table
 * 			   	2) Return true or false based on success or failure
 */
bool MP2Node::createKeyValue(string key, string value, ReplicaType replica) {
	// Insert key, value, replicaType into the hash table
	return ht->create(key, value);
}

/**
 * FUNCTION NAME: readKey
 *
 * DESCRIPTION: Server side READ API
 * 			    This function does the following:
 * 			    1) Read key from local hash table
 * 			    2) Return value
 */
string MP2Node::readKey(string key) {
	// Read key from local hash table and return value
	return ht->read(key);
}

/**
 * FUNCTION NAME: updateKeyValue
 *
 * DESCRIPTION: Server side UPDATE API
 * 				This function does the following:
 * 				1) Update the key to the new value in the local hash table
 * 				2) Return true or false based on success or failure
 */
bool MP2Node::updateKeyValue(string key, string value, ReplicaType replica) {
	// Update key in local hash table and return true or false
	return ht->update(key, value);
}

/**
 * FUNCTION NAME: deleteKey
 *
 * DESCRIPTION: Server side DELETE API
 * 				This function does the following:
 * 				1) Delete the key from the local hash table
 * 				2) Return true or false based on success or failure
 */
bool MP2Node::deletekey(string key) {
	// Delete the key from the local hash table and return true of false
	return ht->deleteKey(key);
}

/**
 * FUNCTION NAME: checkMessages
 *
 * DESCRIPTION: This function is the message handler of this node.
 * 				This function does the following:
 * 				1) Pops messages from the queue
 * 				2) Handles the messages according to message types
 */
void MP2Node::checkMessages() {
	/*
	 * Implement this. Parts of it are already implemented
	 */
	char * data;
	int size;

	/*
	 * Declare your local variables here
	 */

	// dequeue all messages and handle them
	while ( !memberNode->mp2q.empty() ) {
		/*
		 * Pop a message from the queue
		 */
		data = (char *)memberNode->mp2q.front().elt;
		size = memberNode->mp2q.front().size;
		memberNode->mp2q.pop();

		string message(data, data + size);

		/*
		 * Handle the message types here
		 */
	
		// Create a message wrapper class to hold all fields (key and value).
		// Need to have a default constructor
		// The below attempt only allows for fromAddr, replica, success, transID, type fields.
		// Message curMsg;
		WrapperMessage curMsg;
		// memcpy(&curMsg, data, size);
		memcpy(&curMsg, data, size);
		// http://www.cplusplus.com/forum/general/68994/
		switch(curMsg.msgMeta.msgType) {
			case CREATE: {
				// Insert the message (createKeyValue).
				bool isSuccess = createKeyValue(curMsg.msgData.key, curMsg.msgData.value, curMsg.msgMeta.replicaType);
				// Log the success or failure of the message.  This is not the coordinator as it is getting message from coordinator.
				if(isSuccess) {
					this->log->logCreateSuccess(&this->memberNode->addr, false, curMsg.msgMeta.transID, curMsg.msgData.key, curMsg.msgData.value);
				} else {
					this->log->logCreateFail(&this->memberNode->addr, false, curMsg.msgMeta.transID, curMsg.msgData.key, curMsg.msgData.value);
				}
				// Create a new message of type REPLY -- Consider modifying message
				// so reply message can be explicit by type instead of implicit by parameters.
				// Message newMsg = Message(curMsg.transID, this->memberNode->addr, isSuccess);  
				// Create a new message based on the current message that was received.
				// Update the messageType to reply
				WrapperMessage replyMsg = curMsg;
				replyMsg.msgMeta.msgType = REPLY;
				//newMsg.msgData.fromAddr = this->memberNode->addr;
				// newMsg.msgMeta.transID = 
				// send it back to the fromAddr.
				this->emulNet->ENsend(&this->memberNode->addr, &replyMsg.msgData.fromAddr, (char *)&replyMsg, (int)sizeof(replyMsg));
				break;
			} 
			case READ: {
				// Get the value of the key in the hash table
				WrapperMessage replyMsg = curMsg;
				replyMsg.msgMeta.msgType = READREPLY;
				string retValue = readKey(curMsg.msgData.key);
				replyMsg.msgData.value = retValue;
				if(retValue == "") {
					// Handle no key
					replyMsg.msgMeta.success = false;
					this->log->logReadFail(&this->memberNode->addr, false, curMsg.msgMeta.transID, curMsg.msgData.key);
				} else {
					// Handle the value
					replyMsg.msgMeta.success = true;
					this->log->logReadSuccess(&this->memberNode->addr, false, curMsg.msgMeta.transID, curMsg.msgData.key, curMsg.msgData.value);
				}
				// Send message back to the user with returned value and READREPLY msgType
				this->emulNet->ENsend(&this->memberNode->addr, &replyMsg.msgData.fromAddr, (char *)&replyMsg, (int)sizeof(replyMsg));
			}
			case UPDATE: {
				bool isSuccess = updateKeyValue(curMsg.msgData.key, curMsg.msgData.value, curMsg.msgMeta.replicaType);
				WrapperMessage replyMsg = curMsg;
				replyMsg.msgMeta.msgType = REPLY;
				replyMsg.msgMeta.success = isSuccess;
				if(isSuccess) {
					this->log->logUpdateSuccess(&this->memberNode->addr, false, curMsg.msgMeta.transID, curMsg.msgData.key, curMsg.msgData.value);
				} else {
					this->log->logUpdateFail(&this->memberNode->addr, false, curMsg.msgMeta.transID, curMsg.msgData.key, curMsg.msgData.value);
				}

				this->emulNet->ENsend(&this->memberNode->addr, &replyMsg.msgData.fromAddr, (char *)&replyMsg, (int)sizeof(replyMsg));
			}
			case DELETE: {
				bool isSuccess = createKeyValue(curMsg.msgData.key, curMsg.msgData.value, curMsg.msgMeta.replicaType);
				WrapperMessage replyMsg = curMsg;
				replyMsg.msgMeta.msgType = REPLY;
				replyMsg.msgMeta.success = isSuccess;
				if(isSuccess) {
					this->log->logDeleteSuccess(&this->memberNode->addr, false, curMsg.msgMeta.transID, curMsg.msgData.key);
				} else {
					this->log->logDeleteFail(&this->memberNode->addr, false, curMsg.msgMeta.transID, curMsg.msgData.key);
				}
				this->emulNet->ENsend(&this->memberNode->addr, &replyMsg.msgData.fromAddr, (char *)&replyMsg, (int)sizeof(replyMsg));
			}
			case REPLY: {
				// Check for quorum for the transID of the reply for updates.
				WrapperMessage replyMsg = curMsg;
				this->emulNet->ENsend(&this->memberNode->addr, &replyMsg.msgData.fromAddr, (char *)&replyMsg, (int)sizeof(replyMsg));
			}
			case READREPLY: {
				// Check for quorum for the transID of the read-reply for reads.
				// Create helper method for checking for quorum.  Shouldn't have code directly in READREPLY or REPLY cases.
				WrapperMessage replyMsg = curMsg;
				this->emulNet->ENsend(&this->memberNode->addr, &replyMsg.msgData.fromAddr, (char *)&replyMsg, (int)sizeof(replyMsg));
			}
		}


	}

	/*
	 * This function should also ensure all READ and UPDATE operation
	 * get QUORUM replies
	 */
}

/**
 * FUNCTION NAME: findNodes
 *
 * DESCRIPTION: Find the replicas of the given keyfunction
 * 				This function is responsible for finding the replicas of a key
 */
vector<Node> MP2Node::findNodes(string key) {
	size_t pos = hashFunction(key);
	vector<Node> addr_vec;
	if (ring.size() >= 3) {
		// if pos <= min || pos > max, the leader is the min
		if (pos <= ring.at(0).getHashCode() || pos > ring.at(ring.size()-1).getHashCode()) {
			addr_vec.emplace_back(ring.at(0));
			addr_vec.emplace_back(ring.at(1));
			addr_vec.emplace_back(ring.at(2));
		}
		else {
			// go through the ring until pos <= node
			for (int i=1; i<ring.size(); i++){
				Node addr = ring.at(i);
				if (pos <= addr.getHashCode()) {
					addr_vec.emplace_back(addr);
					addr_vec.emplace_back(ring.at((i+1)%ring.size()));
					addr_vec.emplace_back(ring.at((i+2)%ring.size()));
					break;
				}
			}
		}
	}
	return addr_vec;
}

/**
 * FUNCTION NAME: recvLoop
 *
 * DESCRIPTION: Receive messages from EmulNet and push into the queue (mp2q)
 */
bool MP2Node::recvLoop() {
    if ( memberNode->bFailed ) {
    	return false;
    }
    else {
    	return emulNet->ENrecv(&(memberNode->addr), this->enqueueWrapper, NULL, 1, &(memberNode->mp2q));
    }
}

/**
 * FUNCTION NAME: enqueueWrapper
 *
 * DESCRIPTION: Enqueue the message from Emulnet into the queue of MP2Node
 */
int MP2Node::enqueueWrapper(void *env, char *buff, int size) {
	Queue q;
	return q.enqueue((queue<q_elt> *)env, (void *)buff, size);
}
/**
 * FUNCTION NAME: stabilizationProtocol
 *
 * DESCRIPTION: This runs the stabilization protocol in case of Node joins and leaves
 * 				It ensures that there always 3 copies of all keys in the DHT at all times
 * 				The function does the following:
 *				1) Ensures that there are three "CORRECT" replicas of all the keys in spite of failures and joins
 *				Note:- "CORRECT" replicas implies that every key is replicated in its two neighboring nodes in the ring
 */
void MP2Node::stabilizationProtocol() {
	/*
	 * Implement this
	 */

	// One approach is to go through every key in the hashtable.  Find the nodes associated for that key.
	// Check to see if the Secondary/Tertiary nodes have the proper values.  If not send them a request.  
	// Consider nodes that have been down previously as well as nodes that might be down currently.  
	// Need to consider keys that are in the hashtable that are also being held when the current node
	// in scope is either Secondary/Tertiary replicaType.  Come back to this and consider helper methods.
}
