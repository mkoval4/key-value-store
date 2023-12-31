/**********************************
 * FILE NAME: MP2Node.h
 *
 * DESCRIPTION: MP2Node class header file (Revised 2020)
 *
 * MP2 Starter template version
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
#include "Member.h"
#include "Message.h"
#include "Queue.h"
#include <algorithm>

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

const int QUORUM_TIMEOUT= 6;
const int MIN_ID= -10000;

//METADATA for the transaction
struct message_meta
    {
		// {READ,CREATE,UPDATE,DELETE} messagetype for the transaction
		MessageType msg_type; 
		//Transaction id
        string id; 
		// Value
        string value; 
		// metadata on num of replies on a transaction
        int replies; 
		//global timestamp for all nodes
        int timestamp; 
		//when a read op occurts store the timestamp
        int read_ts; 
    };

public:
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
	// string delimiter
	string delimiter;
	
	//MP2 CODE 
	// Transaction map
	map<int, Message>transMap;
    map<int,message_meta> tr_message;
    
	

	MP2Node(Member *memberNode, Params *par, EmulNet *emulNet, Log *log, Address *addressOfMember);
	Member * getMemberNode() {
		return this->memberNode;
	}

	void updateRing();
	vector<Node> getMembershipList();
	size_t hashFunction(string key);


	bool compareNode(const Node& first, const Node& second) {
		return first.nodeHashCode < second.nodeHashCode;
	}

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

	// find the addresses of nodes that are responsible for a key
	vector<Node> findNodes(string key);

	// server
	bool createKeyValue(string key, string value, ReplicaType replica);
	string readKey(string key);
	bool updateKeyValue(string key, string value, ReplicaType replica);
	bool deletekey(string key);

	// stabilization protocol - handle multiple failures
	void stabilizationProtocol();
	
	//sets the information associated with the transaction
    void initializeTranData(MessageType msg_type, int transId, string key, string value);

    //Checks if the passed node is still alive - present in the current ring
    bool isStale(Node node, vector<Node> nodes);

	~MP2Node();
};

#endif /* MP2NODE_H_ */
