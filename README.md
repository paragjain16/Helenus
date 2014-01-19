Helenus
=======

Distributed In memory key-value store with features like 
● Failure detector (Gossip Protocol)
● Replication for fault tolerance
● Support for Consistency levels
● Consistent hashing was used for partitioning
● log4j used for logging 
● Distributed log querier to search logs on different machines.

A basic search application to lookup movie names over IMDB dataset on top of the key-value store.

Software requirements:
1. Java JRE 1.6

******************Starting Node Server***************************

1. Place the HelenusDSKeyValue.jar and HelenusDSKeyValueClient.jar file in each machine in the network.
2. Edit the network_configuration.xml file to include the "ip address and port" of contact machine in the network in <contactMachine> tag. Do the same even for the contact machine.
3. Place the network_configuration.xml file in the same directory as DSKeyValue.jar
4. Start the server by invoking java -jar DSKeyValue.jar <port_number> <machine_id>
5. Two Log files which would be generated and would be present in the same directory with the name machine.<machine_id>.log and /tmp directory with name FEMP4.log and AdminMP4.log
6. For cool application (IMDB search) movie list file has to be placed in the same folder as jar file

******************Invoking client to operate on key value store****************************

Invoke java -jar HelenusKeyValueClient.jar <parameters> for performing operations on key value store from any node.

Following parameters are available - (consistency level should be specified as one,quorum or all)
1. -s 				 			to look up all key values at that node.
2. -i -k <key> -v <value> -c <consistency_level>	to insert key value pair at that node.
3. -l -k <key> 	-c <consistency_level>  		to look up the key across all the nodes.
4. -u -k <key> -v <value> -c <consistency_level>	to update value of a key stored at any node.
5. -d -k <key> -c <consistency_level>			to delete a key stored at any node.
6. -q 							to quit and leave the group thus sending all local keys to successor.
7. -sr							to show recent read and writes at that node
	
