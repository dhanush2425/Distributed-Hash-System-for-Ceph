#include <iostream>
#include <string>
#include <cmath>
#include<iostream>
#include<math.h>
#include<vector>
#include<unordered_map>

using namespace std;


/*
	Node Class structure of each node in CEPH DHT.
	
*/
class Node{
    private:
        int NodeId; // Unique identifier for the node.
		int weight; // Weight assigned to each node.
		int cumulative_weight; // Sum of all the weights of nodes (including itself) succeeding the current node
		bool status; // Status of the node (Active or Failed)
		unordered_map<int,int> data; // Data held by the node in the form of <HashValue, ReplicaNo>.
		Node* next; // Pointer pointing to the next node in the distributed system.
		
    public:
		/* Constructor for Node Class. */
        Node(int NodeID, int weight)
		{
			this->NodeId = NodeID;
			this->weight = weight;
			this->cumulative_weight = weight;
			this->status = true;
			this->next = NULL;
		}
        ~Node();
		
		/* Function to insert the data(hash value and replica no in our case)*/
        void insertData(int hashValue, int replicaNo)
		{
			this->data[hashValue]=replicaNo;
		}

		void SetData(unordered_map<int, int> data)
		{
			this->data = data;
		}
		
		int getWeight()
		{
			return this->weight;
		}
		
		void setWeight(int weight)
		{
			this->weight = weight;
			setCumulativeWeight();
		}
		
		/* Function to return the Sum of all the weights of nodes (including itself) succeeding the current node*/
		int getCumulativeWeight()
		{
			return this->cumulative_weight;
		}
		
		void setCumulativeWeight()
		{
			this->cumulative_weight = this->weight;
			if(this->next)
				this->cumulative_weight += (this->next)->getCumulativeWeight();
		}
		
		/*Get the weight ratio to check if the data belongs to the node*/
		float getCumulativeWeightRatio()
		{
			return float(this->weight)/this->cumulative_weight;
		}
		
		
		
		/*Get the pointer to next Node*/
		Node* getNext()
		{
			return this->next;
		}
		
		/*Set the next pointer of current node to point to next node. */
		void setNext(Node** node)
		{
			if(node == NULL)
				return;
			this->next = *node;
			setCumulativeWeight();
		}
		
		/*Return Node ID*/
		int getNodeID()
		{
			return this->NodeId;
		}
		
		/*Return status of node(Active or failed).*/
		bool getStatus()
		{
			return this->status;
		}

		/*Set the status of current Node.*/
		void setStatus(bool status)
		{
			this->status = status;
		}
		
		unordered_map<int, int> getData()
		{
			return this->data;
		}
		
};





