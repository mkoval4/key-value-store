/**********************************
 * FILE NAME: MP2Node.cpp
 *
 * DESCRIPTION: MP2Node class definition (Revised 2020)
 *
 * MP2 Starter template version
 **********************************/
#include "MP2Node.h"

/**
 * constructor
 */
MP2Node::MP2Node(Member *memberNode, Params *par, EmulNet *emulNet, Log *log, Address *address)
{
     this->memberNode = memberNode;
    this->par = par;
    this->emulNet = emulNet;
    this->log = log;
    ht = new HashTable();
    this->memberNode->addr = *address;
    this->delimiter = "::";
}

/**
 * Destructor
 */
MP2Node::~MP2Node()
{
    delete ht;
}

/**
 * FUNCTION NAME: updateRing
 *
 * DESCRIPTION: This function does the following:
 *1) Gets the current membership list from the Membership Protocol (MP1Node)
 *The membership list is returned as a vector of Nodes. See Node class in Node.h
 *2) Constructs the ring based on the membership list
 *3) Calls the Stabilization Protocol
 */
void MP2Node::updateRing()
{
    /*
    * Implement this. Parts of it are already implemented
    */
    vector<Node> curMemList;
    bool change = false;

    /*
     *Step 1. Get the current membership list from Membership Protocol / MP1
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
    if (ring.empty())
    {
        vector<Node>::iterator nodes_iterator;
        for (nodes_iterator = curMemList.begin(); nodes_iterator != curMemList.end(); ++nodes_iterator)
        {
            ring.push_back(*nodes_iterator);
        }

        for (int i = 0; i < ring.size(); i++)
        {
            int mem_size = memcmp(ring.at(i).getAddress()->addr, &this->memberNode->addr, sizeof(Address));
            if (mem_size == 0)
            {
                switch (i)
                {
                case 0:
                    haveReplicasOf.push_back(ring.at(ring.size() - 1));
                    haveReplicasOf.push_back(ring.at(ring.size() - 2));
                    break;

                case 1:
                    haveReplicasOf.push_back(ring.at(0));
                    haveReplicasOf.push_back(ring.at(ring.size() - i));
                    break;

                default:
                    haveReplicasOf.push_back(ring.at(i - 1));
                    haveReplicasOf.push_back(ring.at(i - 2));
                    break;
                }

                hasMyReplicas.push_back(ring.at((i + 1) % ring.size()));
                hasMyReplicas.push_back(ring.at((i + 2) % ring.size()));
            }
        }
    }
    

        bool modification = false;

    if (curMemList.size() == ring.size())
    {
        for (size_t i = 0; i < curMemList.size(); ++i)
        {
            if (*ring[i].getAddress() != *curMemList[i].getAddress())
            {
                modification = true;
                break;
            }
        }
    }
    else
    {
        modification = true;
    }

    if (curMemList.size() >= 1 && modification)
    {
        ring = curMemList;
        stabilizationProtocol();
    }
}

/**
 * FUNCTION NAME: getMembershipList
 *
 * DESCRIPTION: This function goes through the membership list from the Membership protocol/MP1 and
 *                 i) generates the hash code for each member
 *                 ii) populates the ring member in MP2Node class
 *                 It returns a vector of Nodes. Each element in the vector contain the following fields:
 *                 a) Address of the node
 *                 b) Hash code obtained by consistent hashing of the Address
 */
vector<Node> MP2Node::getMembershipList()
{
    unsigned int i;
    vector<Node> curMemList;
    for (i = 0; i < this->memberNode->memberList.size(); i++)
    {
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
 *                 HASH FUNCTION USED FOR CONSISTENT HASHING
 *
 * RETURNS:
 * size_t position on the ring
 */
size_t MP2Node::hashFunction(string key)
{
    std::hash<string> hashFunc;
    size_t ret = hashFunc(key);
    return ret % RING_SIZE;
}

/**
 * FUNCTION NAME: clientCreate
 *
 * DESCRIPTION: client side CREATE API
 *                 The function does the following:
 *                 1) Constructs the message
 *                 2) Finds the replicas of this key
 *                 3) Sends a message to the replica
 */
void MP2Node::clientCreate(string key, string value)
{
    /*
     * Implement this
     */
    vector<Node> replicas = findNodes(key);

    Message *msg_pointer;
    string msg_data;

    for (size_t i = PRIMARY; i <= TERTIARY; i++)
    {
        ReplicaType val = static_cast<ReplicaType>(i);
        msg_pointer = new Message(g_transID, this->memberNode->addr, CREATE, key, value, val);
        msg_data = msg_pointer->toString();
        emulNet->ENsend(&this->memberNode->addr, replicas[i].getAddress(), msg_data);
    }

    initializeTranData(CREATE, g_transID, key, value);

    g_transID++;
}

/**
 * FUNCTION NAME: clientRead
 *
 * DESCRIPTION: client side READ API
 *                 The function does the following:
 *                 1) Constructs the message
 *                 2) Finds the replicas of this key
 *                 3) Sends a message to the replica
 */
void MP2Node::clientRead(string key)
{
    /*
     * Implement this
     */

    Message *msg_pointer;
    string msg_data;
    vector<Node> replicas = findNodes(key);

    for (size_t i = PRIMARY; i <= TERTIARY; i++)
    {
        msg_pointer = new Message(g_transID, this->memberNode->addr, READ, key);
        msg_data = msg_pointer->toString();
        emulNet->ENsend(&this->memberNode->addr, replicas[i].getAddress(), msg_data);
    }
    // init transaction data
    initializeTranData(READ, g_transID, key, "");
    g_transID++;
}

/**
 * FUNCTION NAME: clientUpdate
 *
 * DESCRIPTION: client side UPDATE API
 *                 The function does the following:
 *                 1) Constructs the message
 *                 2) Finds the replicas of this key
 *                 3) Sends a message to the replica
 */
void MP2Node::clientUpdate(string key, string value)
{
    /*
     * Implement this
     */
    // Find the replicas where the key is stored
    vector<Node> replicas = findNodes(key);
    Message *msg_pointer;
    string msg_data;

    for (size_t i = PRIMARY; i <= TERTIARY; i++)
    {
        ReplicaType val = static_cast<ReplicaType>(i);
        msg_pointer = new Message(g_transID, this->memberNode->addr, UPDATE, key, value, val);
        msg_data = msg_pointer->toString();
        int ret = emulNet->ENsend(&this->memberNode->addr, replicas[i].getAddress(), msg_data);
    }
    // Initialize Transaction
    initializeTranData(UPDATE, g_transID, key, value);
    g_transID++;
}

/**
 * FUNCTION NAME: clientDelete
 *
 * DESCRIPTION: client side DELETE API
 *                 The function does the following:
 *                 1) Constructs the message
 *                 2) Finds the replicas of this key
 *                 3) Sends a message to the replica
 */
void MP2Node::clientDelete(string key)
{
    /*
     * Implement this
     */
    vector<Node> replicas = findNodes(key);
    Message *message_pointer;
    string msg_data;

    for (size_t i = PRIMARY; i <= TERTIARY; i++)
    {
        ReplicaType val = static_cast<ReplicaType>(i);
        message_pointer = new Message(g_transID, this->memberNode->addr, DELETE, key);
        msg_data = message_pointer->toString();
        emulNet->ENsend(&this->memberNode->addr, replicas[i].getAddress(), msg_data);
    }

    // Initialize transaction values
    initializeTranData(DELETE, g_transID, key, "");

    g_transID++;
}

/**
 * FUNCTION NAME: createKeyValue
 *
 * DESCRIPTION: Server side CREATE API
 *                    The function does the following:
 *                    1) Inserts key value into the local hash table
 *                    2) Return true or false based on success or failure
 */
bool MP2Node::createKeyValue(string key, string value, ReplicaType replica)
{
    /*
     * Implement this
     */
    Entry *entrry = new Entry(value, par->getcurrtime(), replica);
    return ht->create(key, entrry->convertToString());
}

/**
 * FUNCTION NAME: readKey
 *
 * DESCRIPTION: Server side READ API
 *                 This function does the following:
 *                 1) Read key from local hash table
 *                 2) Return value
 */
string MP2Node::readKey(string key)
{
    /*
     * Implement this
     */
    return ht->read(key);
}

/**
 * FUNCTION NAME: updateKeyValue
 *
 * DESCRIPTION: Server side UPDATE API
 *This function does the following:
 *1) Update the key to the new value in the local hash table
 *2) Return true or false based on success or failure
 */
bool MP2Node::updateKeyValue(string key, string value, ReplicaType replica)
{
    /*
     * Implement this
     */
    Entry entry(value, par->getcurrtime(), replica);
    return ht->update(key, entry.convertToString());
    ;
}

/**
 * FUNCTION NAME: deleteKey
 *
 * DESCRIPTION: Server side DELETE API
 *This function does the following:
 *1) Delete the key from the local hash table
 *2) Return true or false based on success or failure
 */
bool MP2Node::deletekey(string key)
{
    /*
     * Implement this
     */
    return ht->deleteKey(key);
}

/**
 * FUNCTION NAME: checkMessages
 *
 * DESCRIPTION: This function is the message handler of this node.
 *This function does the following:
 *1) Pops messages from the queue
 *2) Handles the messages according to message types
 */
void MP2Node::checkMessages()
{
    /*
     * Implement this. Parts of it are already implemented
     */
    char *data;
    int size;
    vector<int> soft_delete_msg;
     map<int, message_meta>::iterator messages_lp;

    /*
     * Declare your local variables here
     */

    // dequeue all messages and handle them
    while (!memberNode->mp2q.empty())
    {
        /*
         * Pop a message from the queue
         */
        data = (char *)memberNode->mp2q.front().elt;
        size = memberNode->mp2q.front().size;
        memberNode->mp2q.pop();

        string message(data, data + size);

        Message *msg = new Message(message);

        switch (msg->type)
        {
        case CREATE:
        {
            // Create the Key-Value pair
            bool succ = createKeyValue(msg->key, msg->value, msg->replica);
            if (succ)
            {
                // log CREATE success
                log->logCreateSuccess(&getMemberNode()->addr, false, msg->transID, msg->key, msg->value);
            }
            else
            {
                // log CREATE failure
                log->logCreateFail(&getMemberNode()->addr, false, msg->transID, msg->key, msg->value);
            }

            // Construct and send REPLY message to the coordinator
            Message *m = new Message(msg->transID, this->memberNode->addr, REPLY, succ);
            string msg1 = m->toString();
            Address to_address(msg->fromAddr);
            emulNet->ENsend(&memberNode->addr, &to_address, msg1);
            free(m);
        }
        break;

        case REPLY:
        {
            bool success = msg->success;

            Address from_Addr(msg->fromAddr);

            if (success)
            {
                // Increment the number of replies received for the transaction - at Coordinator
                tr_message[msg->transID].replies++;
            }
        }
        break;
        case DELETE:
        {

            // Delete the key
            bool success = deletekey(msg->key);
            if (success)
            {
                // log DELETE success
                log->logDeleteSuccess(&getMemberNode()->addr, false, msg->transID, msg->key);
            }
            else
            {
                // log DELETE failure
                log->logDeleteFail(&getMemberNode()->addr, false, msg->transID, msg->key);
            }

            // Construct and send REPLY message to the coordinator
            Message *m = new Message(msg->transID, this->memberNode->addr, REPLY, success);
            string msg1 = m->toString();
            Address to_address(msg->fromAddr);
            emulNet->ENsend(&memberNode->addr, &to_address, msg1);
            free(m);
        }
        break;
        case READ:
        {

            // check if there is a message
            if (!readKey(msg->key).empty())
                
                {
                     log->logReadSuccess(&getMemberNode()->addr, false, msg->transID, msg->key, readKey(msg->key));
                    // FAIL
                }
            else
            {
                log->logReadFail(&getMemberNode()->addr, false, msg->transID, msg->key);
            }
            Message *coordinator_message = new Message(msg->transID, this->memberNode->addr, readKey(msg->key));
            string msg_string = coordinator_message->toString();
            Address to_address(msg->fromAddr);
            emulNet->ENsend(&memberNode->addr, &to_address, msg_string);
        }
        break;
        case READREPLY:
        {

            Address from_Addr(msg->fromAddr);
            if (!msg->value.empty())
            {
                tr_message[msg->transID].replies++;
                Entry *e = new Entry(msg->value);

                // Check if the current read value has larger timestamp than previous value read for the key
                if (tr_message[msg->transID].read_ts < e->timestamp)
                {
                    tr_message[msg->transID].read_ts = e->timestamp;
                    tr_message[msg->transID].value = e->value;
                }
            }
        }
        break;
        case UPDATE:
        {
            int ok = updateKeyValue(msg->key, msg->value, msg->replica);
            
                //OK
                if (ok)
            {
                log->logUpdateSuccess(&getMemberNode()->addr, false, msg->transID, msg->key, msg->value);
            }
            // FAIL
            else
            {
                log->logUpdateFail(&getMemberNode()->addr, false, msg->transID, msg->key, msg->value);
            }

            Message *m = new Message(msg->transID, this->memberNode->addr, REPLY, ok);
            string msg1 = m->toString();
            Address to_address(msg->fromAddr);
            emulNet->ENsend(&memberNode->addr, &to_address, msg1);
        }
        break;
        default:
            break;
        }

        free(msg);
    }

    int sys_timestamp = par->getcurrtime();
    for (messages_lp = tr_message.begin(); messages_lp != tr_message.end(); messages_lp++)
    {
        if (messages_lp->second.replies < 2)
        {
            // If Quorum is not received
            if (QUORUM_TIMEOUT <= (sys_timestamp - messages_lp->second.timestamp))
            {
                switch (this->tr_message[messages_lp->first].msg_type)
                {
                case UPDATE:
                    log->logUpdateFail(&getMemberNode()->addr, true, messages_lp->first, messages_lp->second.id, messages_lp->second.value);
                    soft_delete_msg.push_back(messages_lp->first);
                    break;
                case READ:
                    log->logReadFail(&getMemberNode()->addr, true, messages_lp->first, messages_lp->second.id);
                    soft_delete_msg.push_back(messages_lp->first);
                    break;
                case DELETE:
                    log->logDeleteFail(&getMemberNode()->addr, true, messages_lp->first, messages_lp->second.id);
                    soft_delete_msg.push_back(messages_lp->first);
                    break;

                default:
                    // Handle the default case if needed
                    break;
                }
            }
        }
        else
        {
            switch (this->tr_message[messages_lp->first].msg_type)
            {
            case UPDATE:
                log->logUpdateSuccess(&getMemberNode()->addr, true, messages_lp->first, messages_lp->second.id, messages_lp->second.value);
                break;
            case READ:
                log->logReadSuccess(&getMemberNode()->addr, true, messages_lp->first, messages_lp->second.id, messages_lp->second.value);
                break;

            case CREATE:
                log->logCreateSuccess(&getMemberNode()->addr, true, messages_lp->first, messages_lp->second.id, messages_lp->second.value);
                break;

            case DELETE:
                log->logDeleteSuccess(&getMemberNode()->addr, true, messages_lp->first, messages_lp->second.id);
                break;

            default:
                // Handle the default case if needed
                break;
            }
            soft_delete_msg.push_back(messages_lp->first);
        }
    }

    // clean up soft deleted tr_message
    for (int i = 0; i < soft_delete_msg.size(); i++)
    {
        tr_message.erase(soft_delete_msg[i]);
    }
}

/**
 * FUNCTION NAME: findNodes
 *
 * DESCRIPTION: Find the replicas of the given keyfunction
 *This function is responsible for finding the replicas of a key
 */
vector<Node> MP2Node::findNodes(string key)
{
    size_t pos = hashFunction(key);
    vector<Node> addr_vec;
    if (ring.size() >= 3)
    {
        // if pos <= min || pos > max, the leader is the min
        if (pos <= ring.at(0).getHashCode() || pos > ring.at(ring.size() - 1).getHashCode())
        {
            addr_vec.emplace_back(ring.at(0));
            addr_vec.emplace_back(ring.at(1));
            addr_vec.emplace_back(ring.at(2));
        }
        else
        {
            // go through the ring until pos <= node
            for (int i = 1; i < (int)ring.size(); i++)
            {
                Node addr = ring.at(i);
                if (pos <= addr.getHashCode())
                {
                    addr_vec.emplace_back(addr);
                    addr_vec.emplace_back(ring.at((i + 1) % ring.size()));
                    addr_vec.emplace_back(ring.at((i + 2) % ring.size()));
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
bool MP2Node::recvLoop()
{
    if (memberNode->bFailed)
    {
        return false;
    }
    else
    {
        return emulNet->ENrecv(&(memberNode->addr), this->enqueueWrapper, NULL, 1, &(memberNode->mp2q));
    }
}

/**
 * FUNCTION NAME: enqueueWrapper
 *
 * DESCRIPTION: Enqueue the message from Emulnet into the queue of MP2Node
 */
int MP2Node::enqueueWrapper(void *env, char *buff, int size)
{
    Queue q;
    return q.enqueue((queue<q_elt> *)env, (void *)buff, size);
}

/**
 * FUNCTION NAME: stabilizationProtocol
 *
 * DESCRIPTION: This runs the stabilization protocol in case of Node joins and leaves
 *It ensures that there always 3 copies of all keys in the DHT at all times
 *The function does the following:
 *1) Ensures that there are three "CORRECT" replicas of all the keys in spite of failures and joins
 *Note:- "CORRECT" replicas implies that every key is replicated in its two neighboring nodes in the ring
 */
void MP2Node::stabilizationProtocol()
{
    /*
     * Implement this
     */
    int addr;
    vector<Node> enteredCopy, existsCopy;
    vector<string> primaryKeys;
    ReplicaType type;
    map<string, string>::iterator it;

    // loop through the ring and find neighbours with copies
    for (int ring_counter = 0; ring_counter < ring.size(); ring_counter++)
    {
        addr = memcmp(ring.at(ring_counter).getAddress()->addr, &this->memberNode->addr, sizeof(Address));
        if (0 == addr)
        {
            switch (ring_counter)
            {

            case 1:
                enteredCopy.push_back(ring.at(0));
                enteredCopy.push_back(ring.at(ring.size() - ring_counter));
                break;
            case 0:
                enteredCopy.push_back(ring.at(ring.size() - 1));
                enteredCopy.push_back(ring.at(ring.size() - 2));
                break;
            default:
                enteredCopy.push_back(ring.at(ring_counter - 1));
                enteredCopy.push_back(ring.at(ring_counter - 2));
                break;
            }

            existsCopy.push_back(ring.at((ring_counter + 1) % ring.size()));
            existsCopy.push_back(ring.at((ring_counter + 2) % ring.size()));
        }
    }

    // Compare old and new HasReplicas
    for (int ring_counter = 0; ring_counter < hasMyReplicas.size(); ring_counter++)
    {
        addr = memcmp(hasMyReplicas.at(ring_counter).getAddress()->addr, existsCopy.at(ring_counter).getAddress()->addr, sizeof(Address));

        switch (ring_counter)
        {
        case 0:
            type = SECONDARY;
            break;

        default:
            type = TERTIARY;
            break;
        }
        if (0 != addr)
        {

            // If hasMyReplica has changed
            it = ht->hashTable.begin();
            for (auto it = ht->hashTable.begin(); it != ht->hashTable.end(); ++it)
            {
                string next = it->second;
                Entry entry_node(next);
                if (entry_node.replica == PRIMARY)
                {
                    primaryKeys.push_back(it->first);
                }
            }
            if (isStale(existsCopy.at(ring_counter), hasMyReplicas))
            {
                // type change the neighbour is an existing one
                Message *msg_data = new Message(MIN_ID, this->memberNode->addr, UPDATE, "", "", type);

                for (int secondary_ring_counter = 0; secondary_ring_counter < primaryKeys.size(); ++secondary_ring_counter)
                {
                    string key = primaryKeys.at(secondary_ring_counter);
                    msg_data->key = key;
                    msg_data->value = ht->hashTable[key];

                    // send the updated messsage
                    string msg_string = msg_data->toString();
                    emulNet->ENsend(&this->memberNode->addr, existsCopy.at(ring_counter).getAddress(), msg_string);
                }
            }
            else
            {
                // brand new neighbeour
                Message *msg_data = new Message(MIN_ID, this->memberNode->addr, CREATE, "", "", type);

                for (const string &key : primaryKeys)
                {
                    msg_data->key = key;
                    msg_data->value = ht->hashTable[key];
                    string msg_string = msg_data->toString();
                    emulNet->ENsend(&this->memberNode->addr, existsCopy.at(ring_counter).getAddress(), msg_string);
                }

                // Free the Message object outside the loop
                delete msg_data;
            }
        }
    } // RECONFIGURE THE nodes
    hasMyReplicas = existsCopy;
    haveReplicasOf = enteredCopy;
}

/**
 * FUNCTION NAME: initializeTranData
 *
 * DESCRIPTION: initialize transaction metadata
 * AUTHOR: Maksym
 *
 */
void MP2Node::initializeTranData(MessageType msg_type, int transId, string key, string value)
{
    // ensure it exists
    if (tr_message.end() == tr_message.find(transId))
    {
        tr_message[transId].replies = 0;
        tr_message[transId].id = key;
        tr_message[transId].value = value;
        tr_message[transId].msg_type = msg_type;
        tr_message[transId].timestamp = par->getcurrtime();
        tr_message[transId].read_ts = 0;
    }
}
/**
 * FUNCTION NAME: isStale
 *
 * DESCRIPTION: checks if a node is no longer alive
 * AUTHOR: Maksym
 *
 */
bool MP2Node::isStale(Node node, vector<Node> nodes)
{
    vector<Node>::iterator it = nodes.begin();
    for (auto it = nodes.begin(); it != nodes.end(); ++it)
    {
        if (memcmp(it->getAddress()->addr, node.getAddress()->addr, sizeof(Address)) == 0)
        {
            return true;
        }
    }
    return false;
}
