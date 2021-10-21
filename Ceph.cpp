#include "Ceph.h"

#define crush_hash_seed 1315423911

#define crush_hashmix(a, b, c) do {     \
    a = a-b;  a = a-c;  a = a^(c>>13);  \
    b = b-c;  b = b-a;  b = b^(a<<8); \
    c = c-a;  c = c-b;  c = c^(b>>13);  \
    a = a-b;  a = a-c;  a = a^(c>>12);  \
    b = b-c;  b = b-a;  b = b^(a<<16);  \
    c = c-a;  c = c-b;  c = c^(b>>5); \
    a = a-b;  a = a-c;  a = a^(c>>3); \
    b = b-c;  b = b-a;  b = b^(a<<10);  \
    c = c-a;  c = c-b;  c = c^(b>>15);  \
  } while (0);

static unsigned int crush_hash32_rjenkins1_3(int a, int b, int c) {
  unsigned int hash = crush_hash_seed ^ a ^ b ^ c;
  unsigned int x = 231232;
  unsigned int y = 1232;
  crush_hashmix(a, b, hash);
  crush_hashmix(c, x, hash);
  crush_hashmix(y, a, hash);
  crush_hashmix(b, x, hash);
  crush_hashmix(y, c, hash);
  return hash;
}

int insert_data(Node** head, int hashValue, int replicaNo, int NodeCount, bool enableprints)
{
	Node *cur_node = *head;
	
	while(cur_node != NULL)
	{
		//cout<<cur_node->getNodeID()<<endl;
		/*if(cur_node->getStatus() == false)
		{
			cur_node = cur_node->getNext();
			continue;
		}*/

		int cumulative_weight = cur_node->getCumulativeWeight();
		unordered_map<int, int> cur_node_data = cur_node->getData();
		unsigned int jenkins_hash = crush_hash32_rjenkins1_3(cumulative_weight, replicaNo, hashValue);
		float hash = float((jenkins_hash)%cumulative_weight)/cumulative_weight;

		if(enableprints)
		{
			cout<<"Current Node is "<<cur_node->getNodeID()<<endl;
			cout<<"Data to insert is hash value: "<<hashValue<<" replica no:"<< replicaNo <<endl;
		}

		if(hash >= cur_node->getCumulativeWeightRatio())
		{
			cur_node = cur_node->getNext();
		}
		else
		{
			if(cur_node->getStatus() != false && cur_node_data.find(hashValue) == cur_node_data.end())
			{
				//cout<<"Inside Insert"<<endl;
				//cout<<hash<<" "<<cur_node->getCumulativeWeightRatio()<<" "<<cur_node->getNodeID()<<endl;
				cur_node->insertData(hashValue, replicaNo);
				return cur_node->getNodeID();
			}
			else{
				replicaNo+=1;
				cur_node = *head;
			}
		}
	}
	
	return -1;
}

void setCumulativeWeight(Node* cur, int nodeID, int weight)
{
	if(cur == NULL)
		return;
	
	if(cur->getNodeID() == nodeID)
	{
		cur->setWeight(weight);
		return;
	}
	
	setCumulativeWeight(cur->getNext(), nodeID, weight);
	cur->setCumulativeWeight();
}

void balance_load(Node** head, int nodeID, int weight, int NodeCount, int m)
{
	Node* cur = *head;
	
	setCumulativeWeight(cur, nodeID, weight);
	
	cur = *head;
	
	vector<vector<int>> MovableData;
	float prev_cumulative_weight_ratio = 0;
	
	while(cur!= NULL /* && cur->getNodeID() != nodeID*/)
	{
		if(cur->getStatus() != false)
		{
			unordered_map<int, int> nodeData = cur->getData();
			int cumulative_weight = cur->getCumulativeWeight();
			unordered_map<int, int>::iterator it = nodeData.begin();

			while(it != nodeData.end())
			{
				int hashValue = it->first;
				int replicaNo = it->second;
				
				unsigned int jenkins_hash = crush_hash32_rjenkins1_3(cumulative_weight, replicaNo, hashValue);
				float hash = float((jenkins_hash)%cumulative_weight)/cumulative_weight;
				if(hash < prev_cumulative_weight_ratio || hash >= cur->getCumulativeWeightRatio())
				{
					MovableData.push_back({hashValue, replicaNo, cur->getNodeID()});
					it = nodeData.erase(it);
				}
				else
				{
					it++;
				}
			}
			cur->SetData(nodeData);
			prev_cumulative_weight_ratio = cur->getCumulativeWeightRatio();
		}
		cur = cur->getNext();
	}
	
	for(auto it : MovableData)
	{
		//cout<<it[0]<<" "<<it[1]<<endl;
		int dest = insert_data(head, it[0], it[1], NodeCount, 0);
		it.push_back(dest);
	}

	cout<<"STATISTICS OF THE OPERATION:"<<endl;
	cout<<"Total Number of objects moved: "<<MovableData.size()<<endl;
	cout<<"Count of Data hosted by each Node:"<<endl<<endl;
	cur = *head;
	while(cur != NULL)
	{
		unordered_map<int, int> nodeData = cur->getData();
		cout<<"Node ID: "<<cur->getNodeID()<<" "<<"Number of data objects hosted: "<<nodeData.size()<<endl;
		cur = cur->getNext();
	}
	cout<<endl;
	
	if(m<=5)
	{
		cout<<endl;
		cout<<"Following hashValues are moved the during load balancing:"<<endl<<endl;

		for(auto data:MovableData)
		{
			cout<<"hashValue: "<<data[0]<<endl;
			cout<<"Source NodeID: "<<data[2]<<endl;
			cout<<"Destination NodeID: "<<data[3]<<endl<<endl;
		}
	}
}

void AddNode(Node** head, int nodeID, int weight, int m)
{
	Node* newNode = new Node(nodeID, weight);
	if(head == NULL)
		*head = newNode;
	else
	{
		newNode->setNext(head);
		*head = newNode;
	}
	
	Node* cur = *head;
	vector<vector<int>> newNode_data;
	int cumulative_weight = cur->getCumulativeWeight();
	float cumulative_weight_ratio = cur->getCumulativeWeightRatio();
	while(cur != NULL)
	{
		if(cur->getStatus() != false)
		{
			unordered_map<int,int> cur_node_data;
			cur_node_data = cur->getData();
			unordered_map<int, int>::iterator it = cur_node_data.begin();
			while(it != cur_node_data.end())
			{
				int hashValue = it->first;
				int replicaNo = it->second;
				
				unsigned int jenkins_hash = crush_hash32_rjenkins1_3(cumulative_weight, replicaNo, hashValue);
				float hash = float((jenkins_hash)%cumulative_weight)/cumulative_weight;

				if(hash < cumulative_weight_ratio)
				{
					newNode_data.push_back({hashValue, replicaNo, cur->getNodeID()});
					it = cur_node_data.erase(it);
				}
				else
				{
					it++;
				}
			}
			cur->SetData(cur_node_data);
			cur = cur->getNext();
		}
	}

	for(auto data:newNode_data)
	{
		data.push_back(insert_data(head, data[0], data[1], nodeID+1, 0));
	};

	cout<<"STATISTICS OF THE OPERATION:"<<endl<<endl;
	cout<<"Total Number of objects moved: "<<newNode_data.size()<<endl;
	cout<<"Count of Data hosted by each Node:"<<endl;
	cur = *head;
	while(cur != NULL)
	{
		unordered_map<int, int> nodeData = cur->getData();
		cout<<"Node ID: "<<cur->getNodeID()<<" "<<"Number of data objects hosted: "<<nodeData.size()<<endl;
		cur = cur->getNext();
	}
	cout<<endl;
	if(m <= 5)
	{
		cout<<"Following hashValues are moved the newly added node:"<<endl<<endl;

		for(auto data:newNode_data)
		{
			cout<<"hashValue: "<<data[0]<<endl;
			cout<<"Source NodeID: "<<data[2]<<endl;
			cout<<"Destination NodeID: "<<data[3]<<endl;
			cout<<endl;
		}
	}
	
}

void RemoveNode(Node** head, int nodeID, int NodeCount, int m, int r)
{
	if(head == NULL)
	{
		cout<<"Invalid Head."<<endl;
		return;
	}
	else
	{
		Node* cur = *head;
		int weight = 0;
		unordered_map<int, int> FailedData;

		while(cur != NULL)
		{
			//cout<<cur->getNodeID()<<endl;
			if(nodeID == cur->getNodeID())
			{
				//cout<<"hi"<<cur->getNodeID()<<endl;
				cur->setStatus(false);
				cur->SetData(FailedData);
				break;
			}
			else
			{
				cur = cur->getNext();
			}
		}


		//cout<<"hi"<<endl;
		//cout<<"head"<<(*head)->getNodeID()<<endl;
		if(cur != NULL)
		{
			cur = *head;
			while(cur!=NULL)
			{
				cur->SetData(FailedData);
				cur = cur->getNext();
			}
			for(int i=0;i<pow(2,m);i++)
			{
				for(int j=0;j<r;j++)
				{
					insert_data(head, i, j, NodeCount, 0);
				}
			}
		}
	}
}

void locate_data(Node** head, int data)
{
	Node* cur = *head;
	
	while(cur != NULL)
	{
		unordered_map<int, int> NodeData;
		NodeData = cur->getData();
		
		if(NodeData.find(data) != NodeData.end())
		{
			int NodeID = cur->getNodeID();
			int replicaNo = NodeData[data];
			cout<<"NodeID:"<<NodeID<<" "<<"Replica No:"<<replicaNo<<endl;
		}
		cur = cur->getNext();
	}
}

void print_data_all(Node** head)
{
	Node* cur = *head;

	while(cur!= NULL)
	{
		unordered_map<int, int> NodeData;
		cout<<"NodeID is "<<cur->getNodeID()<<endl;
		NodeData = cur->getData();
		for(auto it = NodeData.begin(); it != NodeData.end(); it++)
		{
			cout<<it->first<<"-"<<it->second<<" ";
		}
		cout<<endl;
		cur = cur->getNext();
	}
}

int main()
{
	int n, m, r;
	cout<<"Enter the values of n, m and r"<<endl;
	cin>>n>>m>>r;
	
	Node* head = NULL;
	int NodeCount = 0;
	
	cout<<"Enter the weights of "<<n<<" nodes"<<endl;
	for(int i=0;i<n;i++)
	{
		int weight;
		cin >> weight;
		Node* newNode = new Node(NodeCount, weight);
		if(head == NULL)
			head = newNode;
		else
		{
			newNode->setNext(&head);
			head = newNode;
		}
		NodeCount++;	
	}
	//cout<<"NodeCount "<<NodeCount<<endl;
	for(int i=0;i<pow(2,m);i++)
	{
		for(int j=0;j<r;j++)
		{
			insert_data(&head, i, j, NodeCount, 0);
		}
	}

	print_data_all(&head);
	
	int choice;
	
	do
	{
		cout<<"Please Select your operation."<<endl;
		cout<<"1. Add Node"<<endl;
		cout<<"2. Remove Node"<<endl;
		cout<<"3. Locate Data"<<endl;
		cout<<"4. Balance Load"<<endl;
		cout<<"5. Print data in all nodes"<<endl;
		cout<<"6. Exit"<<endl;
		cin>>choice;
		switch(choice)
		{
			case 1:
			{
				cout<<"Enter weight of newly added node"<<endl;
				int weight;
				cin>>weight;
				AddNode(&head, NodeCount, weight, m);
				NodeCount++;
				break;
			}
			case 2:
			{
				cout<<"Enter the ID of node to be removed"<<endl;
				int nodeID;
				cin>> nodeID;
				RemoveNode(&head, nodeID, NodeCount, m, r);
				break;
			}
			case 3:
			{
				cout<<"Enter the hash value of data to be located"<<endl;
				int data;
				cin >> data;
				locate_data(&head, data);
				break;
			}
			case 4:
			{
				cout<<"Enter the nodeID and new weight"<<endl;
				int nodeID, weight;
				cin >> nodeID >> weight;
				balance_load(&head, nodeID, weight, NodeCount, m);
				break;
			}
			case 5:
				print_data_all(&head);
				break;
			case 6:
				break;
			default:
			{
				cout<<"Invalid Option"<<endl;
				break;
			}
		}
	}while(choice!=6);
	
	return 0;	
}
