/**********************************
 * FILE NAME: MP1Node.cpp
 *
 * DESCRIPTION: Membership protocol run by this Node.
 * 				Definition of MP1Node class functions.
 **********************************/

#include "MP1Node.h"

#include <stdlib.h>     /* srand, rand */
#include <time.h>       /* time */
/*
 * Note: You can change/add any functions in MP1Node.{h,cpp}
 */

/**
 * Overloaded Constructor of the MP1Node class
 * You can add new members to the class if you think it
 * is necessary for your logic to work
 */
MP1Node::MP1Node(Member *member, Params *params, EmulNet *emul, Log *log, Address *address) {
    /* initialize random seed: */
    srand (time(NULL));

	for( int i = 0; i < 6; i++ ) {
		NULLADDR[i] = 0;
	}
	this->memberNode = member;
	this->emulNet = emul;
	this->log = log;
	this->par = params;
	this->memberNode->addr = *address;
	// this->timestamp = 0;
	this->membersToRemoveList = new vector<int>();
}

/**
 * Destructor of the MP1Node class
 */
MP1Node::~MP1Node() {
    if(membersToRemoveList) {
        delete membersToRemoveList;
    }
}

/**
 * FUNCTION NAME: recvLoop
 *
 * DESCRIPTION: This function receives message from the network and pushes into the queue
 * 				This function is called by a node to receive messages currently waiting for it
 */
int MP1Node::recvLoop() {
    if ( memberNode->bFailed ) {
    	return false;
    }
    else {
    	return emulNet->ENrecv(&(memberNode->addr), enqueueWrapper, NULL, 1, &(memberNode->mp1q));
    }
}

/**
 * FUNCTION NAME: enqueueWrapper
 *
 * DESCRIPTION: Enqueue the message from Emulnet into the queue
 */
int MP1Node::enqueueWrapper(void *env, char *buff, int size) {
	Queue q;
	return q.enqueue((queue<q_elt> *)env, (void *)buff, size);
}

/**
 * FUNCTION NAME: nodeStart
 *
 * DESCRIPTION: This function bootstraps the node
 * 				All initializations routines for a member.
 * 				Called by the application layer.
 */
void MP1Node::nodeStart(char *servaddrstr, short servport) {
    Address joinaddr;
    joinaddr = getJoinAddress();

    // Self booting routines
    if( initThisNode(&joinaddr) == -1 ) {
#ifdef DEBUGLOG
        log->LOG(&memberNode->addr, "init_thisnode failed. Exit.");
#endif
        exit(1);
    }

    if( !introduceSelfToGroup(&joinaddr) ) {
        finishUpThisNode();
#ifdef DEBUGLOG
        log->LOG(&memberNode->addr, "Unable to join self to group. Exiting.");
#endif
        exit(1);
    }

    return;
}

/**
 * FUNCTION NAME: initThisNode
 *
 * DESCRIPTION: Find out who I am and start up
 */
int MP1Node::initThisNode(Address *joinaddr) {
	/*
	 * This function is partially implemented and may require changes
	 */
	int id = *(int*)(&memberNode->addr.addr);
	int port = *(short*)(&memberNode->addr.addr[4]);

	memberNode->bFailed = false;
	memberNode->inited = true;
	memberNode->inGroup = false;
    // node is up!
	memberNode->nnb = 0;
	memberNode->heartbeat = 0;
	memberNode->pingCounter = TFAIL;
	memberNode->timeOutCounter = -1;
    initMemberListTable(memberNode);

    return 0;
}

/**
 * FUNCTION NAME: introduceSelfToGroup
 *
 * DESCRIPTION: Join the distributed system
 */
int MP1Node::introduceSelfToGroup(Address *joinaddr) {
#ifdef DEBUGLOG
    static char s[1024];
#endif

    if ( 0 == memcmp((char *)&(memberNode->addr.addr), (char *)&(joinaddr->addr), sizeof(memberNode->addr.addr))) {
        // I am the group booter (first process to join the group). Boot up the group
#ifdef DEBUGLOG
        log->LOG(&memberNode->addr, "Starting up group...");
#endif
        memberNode->inGroup = true;
    }
    else {
        Message *message = (Message *)malloc(sizeof(Message));
        message->msgHdr.msgType = JOINREQ;
        memcpy(message->msgContent.joinReqContent.addr, &memberNode->addr.addr, sizeof(memberNode->addr.addr));
        message->msgContent.joinReqContent.heartbeat = 0;
        size_t msgsize = sizeof(Message);
#ifdef DEBUGLOG
        sprintf(s, "Trying to join...");
        log->LOG(&memberNode->addr, s);
#endif
        // send JOINREQ message to introducer member
        emulNet->ENsend(&memberNode->addr, joinaddr, (char *)message, msgsize);
        free(message);
    }
    // Nodes should always add reference to themselves so when they pass their list other nodes will also
    // get their information as well as having a reference to self as the fist element in their memberList.
    int id;
    short port;
    memcpy(&id, &memberNode->addr.addr[0], sizeof(int));
    memcpy(&port, &memberNode->addr.addr[4], sizeof(short));

    //Add my own entry here
    MemberListEntry selfEntry = MemberListEntry(id, port, 0, par->getcurrtime());
    memberNode->memberList.push_back(selfEntry);

    return 1;

}

/**
 * FUNCTION NAME: finishUpThisNode
 *
 * DESCRIPTION: Wind up this node and clean up state
 */
int MP1Node::finishUpThisNode(){
    memberNode->inited = false;
    memberNode->inGroup = false;    
    memberNode->nnb = 0;
    memberNode->heartbeat = 0;
    memberNode->pingCounter = TFAIL;
    memberNode->timeOutCounter = -1;
    initMemberListTable(memberNode);
    emulNet->ENcleanup();
}

/**
 * FUNCTION NAME: nodeLoop
 *
 * DESCRIPTION: Executed periodically at each member
 * 				Check your messages in queue and perform membership protocol duties
 */
void MP1Node::nodeLoop() {
    if (memberNode->bFailed) {
    	return;
    }

    // Check my messages
    checkMessages();

    // Wait until you're in the group...
    if( !memberNode->inGroup ) {
    	return;
    }

    // ...then jump in and share your responsibilites!
    nodeLoopOps();

    return;
}

/**
 * FUNCTION NAME: checkMessages
 *
 * DESCRIPTION: Check messages in the queue and call the respective message handler
 */
void MP1Node::checkMessages() {
    void *ptr;
    int size;

    // Pop waiting messages from memberNode's mp1q
    while ( !memberNode->mp1q.empty() ) {
    	ptr = memberNode->mp1q.front().elt;
    	size = memberNode->mp1q.front().size;
    	memberNode->mp1q.pop();
    	recvCallBack((void *)memberNode, (char *)ptr, size);
    }
    return;
}

/**
 * FUNCTION NAME: recvCallBack
 *
 * DESCRIPTION: Message handler for different message types
 */
bool MP1Node::recvCallBack(void *env, char *data, int size ) {

    Member *memberNode = (Member*)env;
    Message *msg = (Message*)data;

    if(msg->msgHdr.msgType == JOINREQ) 
    {
        receivedJoinReq(memberNode, &(msg->msgContent.joinReqContent));
    } 
    else if(msg->msgHdr.msgType == JOINREP) 
    {        
        receivedJoinRep(memberNode);
        // receivedJoinRep(memberNode, &(msg->msgContent.gossipContent));
    } 
    else if(msg->msgHdr.msgType == GOSSIP) 
    {
        receivedGossipMessage(memberNode, &(msg->msgContent.gossipContent));
    }
}


/**
 * FUNCTION NAME: receivedJoinReq
 *
 * DESCRIPTION: Takes a message and a reference to a node and updates the membershipList of the node
 *              with by inserting the message data as a new node.
 * 
 */
void MP1Node::receivedJoinReq(Member *memberNode, JoinReqContent *mesg_data)
{
    #ifdef DEBUGLOG
        log->LOG(&memberNode->addr, "Hello World");
    #endif
    Address addr;
    memcpy(&addr.addr, &mesg_data->addr, sizeof(addr.addr));

    int id;
    short port;
    long heartbeat = mesg_data->heartbeat;
    // long timestamp = getTimeStamp();

    memcpy(&id, &addr.addr[0], sizeof(int));
    memcpy(&port, &addr.addr[4], sizeof(short));

    // Create a new MemberListEntry with the data from the message.  Get the time from the Param helper method.
    MemberListEntry entry = MemberListEntry(id, port, heartbeat, par->getcurrtime());

    // Add the new node to the memberList
    memberNode->memberList.push_back(entry);

    log->logNodeAdd(&memberNode->addr, &addr);

    // Call sendJoinRep message to send a response back to the address who sent the JoinReq via the EmulNet.
    sendJoinRep(&addr);
    // sendMemberList(&addr);
}


/**
 * FUNCTION NAME: receivedJoinRep
 *
 * DESCRIPTION: After receiving a JOINREP message node can set the inGroup field to true since it has been
 *              confirmed that they have been added to the system.  (This works but consider taking a memberList)
 * 
 */
void MP1Node::receivedJoinRep(Member *memberNode)
{
    memberNode->inGroup = true;
}

void MP1Node::receivedJoinRep(Member *memberNode, GossipContent *gossip_mesg)
{
    memberNode->inGroup = true;

    // MemberListEntry *senderEntry = &gossip_mesg->memberList[0];
    // vector<int>::iterator itr = find (membersToRemoveList->begin(),
    //             membersToRemoveList->end(), senderEntry->id);
    // if(itr != membersToRemoveList->end())
    // {            
    //     membersToRemoveList->erase(itr);

    // }
    if(gossip_mesg->memberCount > 0) {
        for(int i = 0; i < gossip_mesg->memberCount; i++)
        {
            MemberListEntry *new_entry = &gossip_mesg->memberList[i];
            new_entry->heartbeat = getMemberNode()->heartbeat;
            new_entry->timestamp = par->getcurrtime();
            Address addr;
            memcpy(&addr.addr[0], &new_entry->id, sizeof(int));
            memcpy(&addr.addr[4], &new_entry->port, sizeof(short));
            log->logNodeAdd(&memberNode->addr, &addr);
            getMemberNode()->memberList.push_back(*new_entry);
        }
    }
}


/**
 * FUNCTION NAME: receivedGossipMessage
 *
 * DESCRIPTION: Takes a message and a reference to a node and updates the membershipList of the node
 *              with by inserting the message data as a new node.
 * 
 */
void MP1Node::receivedGossipMessage(Member *memberNode, GossipContent *gossip_mesg)
{
    for(int i = 0; i < gossip_mesg->memberCount; ++i)
    {
        MemberListEntry *new_entry = &gossip_mesg->memberList[i];
        MemberListEntry *old_entry = getMemberById(new_entry->id);
        vector<int>::iterator itr = find (membersToRemoveList->begin(),
                    membersToRemoveList->end(), new_entry->id);
        if(itr != membersToRemoveList->end())
        {
            continue;
        }
        if(old_entry)
        {
            // If the heartbeat of the the existing (old) entry is less than the new entry, update the existing entry with
            // the new entry's heartbeat and update the timestamp.
            if (old_entry->heartbeat < new_entry->heartbeat)
            {
                old_entry->heartbeat = new_entry->heartbeat;
                old_entry->timestamp = par->getcurrtime();
            }
        }
        else
        {
            new_entry->heartbeat = getMemberNode()->heartbeat;
            new_entry->timestamp = par->getcurrtime();
            Address addr;
            memcpy(&addr.addr[0], &new_entry->id, sizeof(int));
            memcpy(&addr.addr[4], &new_entry->port, sizeof(short));
            log->logNodeAdd(&memberNode->addr, &addr);
            getMemberNode()->memberList.push_back(*new_entry);
        }
    }
}
/**
 * FUNCTION NAME: nodeLoopOps
 *
 * DESCRIPTION: Check if any node hasn't responded within a timeout period and then delete the nodes.
 * 				Propagate your membership list.
 */
void MP1Node::nodeLoopOps() {

    // Member *node = getMemberNode();
    // Increase the heartbeat of the node.  Update the reference to self (first element in the memberList) 
    // with the new heartbeat and current timestamp.
    memberNode->heartbeat++;
    memberNode->memberList[0].setheartbeat(memberNode->heartbeat);
    memberNode->memberList[0].settimestamp(par->getcurrtime());
    identifyAndRemoveFailedNodes();

	// Select 5 random address from membership list
	// Send gossip message to them
    if(!getMemberNode()->memberList.empty())
    {
        int numNodesToSendGossip = 5;

        // If the total memberList size is less than 5 set the number of members nodes to receive gossip as the size of the memberList
        numNodesToSendGossip = (getMemberNode()->memberList.size() < numNodesToSendGossip) ? getMemberNode()->memberList.size() :numNodesToSendGossip;

        // Iterate the amount of times as nodes to receive gossip and 
        for(int i=0;i<numNodesToSendGossip;i++)
        {
            // Use modulo operator so values will always be within the range of the memberList size.
            int randomeEntryId = rand() % getMemberNode()->memberList.size();

            // Get the random member to send gossip to.
            MemberListEntry &entry = getMemberNode()->memberList[randomeEntryId];

            // Check top make sure that the element isn't in the membersToRemoveList
            // Use std:find to check if the id of the selected member to receive gossip 
            // is in the membersToRemoveList.  If it (iterator) is not poiting to the end
            // then the ID was found and we need to increase the numNodesToSendGossip so that
            // loop continues and will allow for 5 correct members to be selected for gossip.
            vector<int>::iterator it = find (membersToRemoveList->begin(), membersToRemoveList->end(), entry.id);
            if (it != membersToRemoveList->end())
            {
                numNodesToSendGossip++;
                continue;
            }

            Address addr;
            memcpy(&addr.addr[0], &entry.id, sizeof(int));
            memcpy(&addr.addr[4], &entry.port, sizeof(short));
            sendGossipMessage(&addr);
        }
    }
    return;
}

/**
 * FUNCTION NAME: isNullAddress
 *
 * DESCRIPTION: Function checks if the address is NULL
 */
int MP1Node::isNullAddress(Address *addr) {
	return (memcmp(addr->addr, NULLADDR, 6) == 0 ? 1 : 0);
}

/**
 * FUNCTION NAME: getJoinAddress
 *
 * DESCRIPTION: Returns the Address of the coordinator
 */
Address MP1Node::getJoinAddress() {
    Address joinaddr;

    memset(&joinaddr, 0, sizeof(Address));
    *(int *)(&joinaddr.addr) = 1;
    *(short *)(&joinaddr.addr[4]) = 0;

    return joinaddr;
}

/**
 * FUNCTION NAME: initMemberListTable
 *
 * DESCRIPTION: Initialize the membership list
 */
void MP1Node::initMemberListTable(Member *memberNode) {
	memberNode->memberList.clear();
}

/**
 * FUNCTION NAME: printAddress
 *
 * DESCRIPTION: Print the Address
 * 
 */
void MP1Node::printAddress(Address *addr)
{
    printf("%d.%d.%d.%d:%d \n",  addr->addr[0],addr->addr[1],addr->addr[2],
                                                       addr->addr[3], *(short*)&addr->addr[4]) ;    
}


/**
 * FUNCTION NAME: sendJoinRep
 *
 * DESCRIPTION: Takes an address that is used to send a JOINREP message to. (Consider adding a memberList in the message)
 * 
 */
void MP1Node::sendJoinRep(Address *addr)
{
    Message *joinRepMessage = (Message *)malloc(sizeof(Message));
    joinRepMessage->msgHdr.msgType = JOINREP;
    size_t msgsize = sizeof(Message);
    
    // send JOINREP message to the address confirming that they have been added to the memberList.
    emulNet->ENsend(&memberNode->addr, addr, (char *)joinRepMessage, msgsize);
    free(joinRepMessage);
}

// void MP1Node::sendJoinRep(Address *addr) 
// {
//     int totalMembers = getMemberNode()->memberList.size();
//     if(totalMembers <= 0)
//     {
//         return;
//     }
//     size_t msgSize = sizeof(Message) + (totalMembers - 1) * sizeof(MemberListEntry);
//     Message *gossipMsg = (Message *)malloc(msgSize);
//     gossipMsg->msgHdr.msgType = JOINREP;

//     GossipContent *gossip = &gossipMsg->msgContent.gossipContent;
//     gossip->memberCount = totalMembers;

//     int i = 0;
//     // Iterate over the memberList, if a member exists in the membersToRemoveList then subtract 1 from
//     // the memberCount field so that when receiving a gossip message the totals are correct. 
//     for( auto &entry : getMemberNode()->memberList)
//     {
//         vector<int>::iterator itr = find (membersToRemoveList->begin(), membersToRemoveList->end(), entry.id);
//         if (itr != membersToRemoveList->end())
//         {
//             --gossip->memberCount;
//             continue;
//         }
//         memcpy(&(gossip->memberList[i]), &entry, sizeof(MemberListEntry));
//         i++;
//     }
//     // send JOINREP message with the accurate memberList
//     emulNet->ENsend(&memberNode->addr, addr, (char *)gossipMsg, msgSize);
//     free(gossipMsg);
// }

/**
 * FUNCTION NAME: sendGossipMessage
 *
 * DESCRIPTION: Takes a message and a reference to a node and updates the membershipList of the node
 *              with by inserting the message data as a new node.
 * 
 */
void MP1Node::sendGossipMessage(Address *addr)
{

    int totalMembers = getMemberNode()->memberList.size();
    if(totalMembers <= 0)
    {
        return;
    }
    size_t msgSize = sizeof(Message) + (totalMembers - 1) * sizeof(MemberListEntry);
    Message *gossipMsg = (Message *)malloc(msgSize);
    gossipMsg->msgHdr.msgType = GOSSIP;

    GossipContent *gossip = &gossipMsg->msgContent.gossipContent;
    gossip->memberCount = totalMembers;

    int i = 0;
    // Iterate over the memberList, if a member exists in the membersToRemoveList then subtract 1 from
    // the memberCount field so that when receiving a gossip message the totals are correct. 
    for( auto &entry : getMemberNode()->memberList)
    {
        vector<int>::iterator itr = find (membersToRemoveList->begin(), membersToRemoveList->end(), entry.id);
        if (itr != membersToRemoveList->end())
        {
            --gossip->memberCount;
            continue;
        }
        memcpy(&(gossip->memberList[i]), &entry, sizeof(MemberListEntry));
        i++;
    }
    // send Gossip message
    emulNet->ENsend(&memberNode->addr, addr, (char *)gossipMsg, msgSize);
    free(gossipMsg);
}

void MP1Node::sendMemberList(Address *addr) 
{
    int totalMembers = getMemberNode()->memberList.size();
    if(totalMembers <= 0)
    {
        return;
    }
    size_t msgSize = sizeof(Message) + (totalMembers - 1) * sizeof(MemberListEntry);
    Message *gossipMsg = (Message *)malloc(msgSize);
    gossipMsg->msgHdr.msgType = JOINREP;

    GossipContent *gossip = &gossipMsg->msgContent.gossipContent;
    gossip->memberCount = totalMembers;

    int i = 0;
    // Iterate over the memberList, if a member exists in the membersToRemoveList then subtract 1 from
    // the memberCount field so that when receiving a gossip message the totals are correct. 
    for( auto &entry : getMemberNode()->memberList)
    {
        vector<int>::iterator itr = find (membersToRemoveList->begin(), membersToRemoveList->end(), entry.id);
        if (itr != membersToRemoveList->end())
        {
            --gossip->memberCount;
            continue;
        }
        memcpy(&(gossip->memberList[i]), &entry, sizeof(MemberListEntry));
        i++;
    }
    // send JOINREP message with the accurate memberList
    emulNet->ENsend(&memberNode->addr, addr, (char *)gossipMsg, msgSize);
    free(gossipMsg);
}

/**
 * FUNCTION NAME: identifyFailedNodes
 *
 * DESCRIPTION: Iterate through the memberList and membersToRemoveList and identify if there is a match in both lists.
 * 
 */
void MP1Node::identifyAndRemoveFailedNodes()
{
    for(vector<MemberListEntry>::iterator it = getMemberNode()->memberList.begin();it != getMemberNode()->memberList.end(); )
    {
        MemberListEntry &entry = *it;

        // Calculate difference between latest timestamp of the node and the current time.
        long diff = par->getcurrtime() - entry.timestamp;

        // Iterate through the membersToRemoveList
        vector<int>::iterator removeItr = find (membersToRemoveList->begin(), membersToRemoveList->end(),
                                            entry.id);

        // If the difference is greater than the removal threshold remove the member from the memberList and 
        // if the member also is in the membersToRemoveList (which it shoud be) remove it from there too.
        if (diff > (TREMOVE))
        {
            // Address addr;
            // memcpy(&addr.addr[0], &entry.id, sizeof(int));
            // memcpy(&addr.addr[4], &entry.port, sizeof(short));
            // log->logNodeRemove(&getMemberNode()->addr, &addr);
            getMemberNode()->memberList.erase(it);
            // #ifdef DEBUGLOG
            //     log->logNodeRemove(&getMemberNode()->addr, &it->);
            // #endif
            
            if(removeItr != membersToRemoveList->end())
            {
                membersToRemoveList->erase(removeItr);
            }
            continue;
        }
        // If the difference is greater than the fail threshold add the member's id to the membersToRemoveList
        // so that it can be monitored for
        else if(diff > TFAIL)
        {
            if (removeItr == membersToRemoveList->end())
            {
                Address addr;
                memcpy(&addr.addr[0], &entry.id, sizeof(int));
                memcpy(&addr.addr[4], &entry.port, sizeof(short));
                log->logNodeRemove(&getMemberNode()->addr, &addr);
                membersToRemoveList->push_back(entry.id);
            }
        }
        it++;
    }
}

MemberListEntry *MP1Node::getMemberById(int id)
{
    for( auto &&entry : getMemberNode()->memberList)
    {
        if( id == entry.id)
        {
            return &entry;
        }
    }
    return NULL;
}