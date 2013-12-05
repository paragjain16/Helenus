package org.ds.server;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

import org.ds.hash.Hash;
import org.ds.logger.DSLogger;
import org.ds.member.Member;
import org.ds.socket.DSocket;

/**
 * @author { pjain11, mallapu2 } @ illinois.edu
 * Will listen to requests from all clients
 * serve the request sequentially and blocks till response for a request is received
 * thus ensuring FIFO ordering w.r.t. to FE. Here we are assuming that we have a SINGLE FE 
 * so request from FE to replica managers will incur constant message delay 
 * thus total ordering between FE and clients will be maintained
 * OR we can use vector time stamp for requests from a single FE but it will be redundant
 * In case of multiple FEs total ordering can be done using physical time stamp that FEs supplies
 * with their request
 * */

public class FrontEnd implements Runnable{
	
	private HashMap<String, Member> aliveMembers;
	private HashMap<String, Member> deadMembers;
	private ServerSocket serverSocket;
	private DSocket socket;
	private TreeMap<Integer, Member> sortedAliveMembers;
	private TreeMap<Integer, Member> sortedDeadMembers;
	private Object lock;
	private Member itself;
	private Map<String, Object> outputMap = new HashMap<String, Object>();
	
	
	//Shares the alive and dead member list of the contact server
	public FrontEnd(HashMap<String, Member> aliveMembers, HashMap<String, Member> deadMembers, Object lock, Member itself){
		this.aliveMembers = aliveMembers;
		this.deadMembers = deadMembers;
		this.lock = lock;
		this.itself = itself;
		outputMap = new HashMap<String, Object>();
		try {
			serverSocket = new ServerSocket(4000);
		} catch (IOException e) {
			DSLogger.logFE("FrontEnd", "FrontEnd", e.getMessage());
			e.printStackTrace();
		}
	}
	
	//For all send keys, send one more parameter to replace or not.
	@Override
	public void run() {
		//accept requests and blocks till it is served
		while(true){
				try {
					DSLogger.logFE("FrontEnd","run","Listening to commands");
					
						System.out.println("Waiting for command");
						//DSLogger.logFE("FrontEnd","run","Listening to commands");
						Socket normalSocket = serverSocket.accept();
						socket = new DSocket(normalSocket);						
						System.out.println("Accepted request from "+socket.getSocket().getRemoteSocketAddress());
						DSLogger.logFE("FrontEnd","run","Accepted request from "+socket.getSocket().getRemoteSocketAddress());
						List<Object> argList = (ArrayList<Object>)socket.readObject();
						String cmd=(String) argList.get(0);
						synchronized (lock) {
							sortedAliveMembers = this.constructSortedMap(aliveMembers);
							DSLogger.logFE(this.getClass().getName(), "run","Sorted Map :"+sortedAliveMembers);
						}
						DSLogger.logFE(this.getClass().getName(), "run","Received command from client: "+cmd);
						/*
						 * can receive different commands like read, write, update etc*/
						if(cmd.equals("joinMe")){
							// Partition key space and send to new node.
							Member newMember = (Member)argList.get(1); 
							int newMemberHashId = Integer.parseInt(newMember.getIdentifier());
							
							synchronized (lock) {
								aliveMembers.put(newMember.getIdentifier(), newMember);
								sortedAliveMembers = this.constructSortedMap(aliveMembers);
								DSLogger.logFE(this.getClass().getName(), "run","Sorted Map :"+sortedAliveMembers);
							}
							DSLogger.logFE(this.getClass().getName(), "run","Trying to join client: "+newMemberHashId);							
							
							DSLogger.logFE(this.getClass().getName(), "run","Contacting : "+itself.getAddress().getHostAddress()+":"+itself.getPort());
							
							//DSocket joinRequest = new DSocket(newMember.getAddress().getHostAddress(), newMember.getPort());
							//Contact its own server to let the new machine join the network
							DSocket joinRequest = new DSocket(itself.getAddress().getHostAddress(), itself.getPort());
/*							List<Object> newCmd = new ArrayList<Object>();
							newCmd.add("joinMe");
							newCmd.add(newMember);*/
							DSLogger.logFE(this.getClass().getName(), "run","Connection established : "+joinRequest.getSocket());
							DSLogger.logFE(this.getClass().getName(), "run","Writing to socket : "+argList);
							joinRequest.writeObjectList(argList);
							
							joinRequest.readObject();
							DSLogger.logFE(this.getClass().getName(), "run","Ack received from : "+joinRequest.getSocket().getRemoteSocketAddress());
							
							//Get primary from previous node to new node. It will become backup1 of new node. 
							argList.clear();
							argList.add(0, "sendKeys"); //command
							
							argList.add(1, 0); //keyspace to send
							argList.add(2, newMember); //To
							argList.add(3, 1);// keyspace of destination
							argList.add(4, 1);// replace 
							Integer prevNodeId = sortedAliveMembers.lowerKey(newMemberHashId)==null?sortedAliveMembers.lastKey():sortedAliveMembers.lowerKey(newMemberHashId);
							DSLogger.logFE(this.getClass().getName(), "run","Asking node "+prevNodeId+" to send its primay key space to "+newMemberHashId);
							DSocket sendMerge = new DSocket(aliveMembers.get(prevNodeId+"").getAddress().getHostAddress(), aliveMembers.get(prevNodeId+"").getPort());
							sendMerge.writeObjectList(argList);
							//consume ack
							sendMerge.readObject();
							
							sendMerge.close();
							
							//Step 2 For next to next node to new node
							if(aliveMembers.size()>2){
								DSLogger.logFE(this.getClass().getName(), "run","Entering Step 1 of join Me for "+newMemberHashId);
								Integer nextNodeId = sortedAliveMembers.higherKey(newMemberHashId)==null?sortedAliveMembers.firstKey():sortedAliveMembers.higherKey(newMemberHashId);
								Member nextNode = aliveMembers.get(nextNodeId+"");
								
								DSLogger.logFE(this.getClass().getName(), "run","Next node is "+nextNodeId+" : "+nextNode);
								
								Integer nextToNextNodeId = sortedAliveMembers.higherKey(nextNodeId)==null?sortedAliveMembers.firstKey():sortedAliveMembers.higherKey(nextNodeId);
								
								DSLogger.logFE(this.getClass().getName(), "run","Next to next node is "+nextToNextNodeId);
								
								if(nextToNextNodeId != newMemberHashId){
									DSLogger.logFE(this.getClass().getName(), "run","Asking node "+nextToNextNodeId+" to stabilize as per "+newMemberHashId);
									argList.clear();
									argList.add(0, "newNodeStabilization"); //command
									argList.add(1, newMember); //Min node for partition
									argList.add(2, nextNode); //Max node for partition
									sendMerge = new DSocket(aliveMembers.get(nextToNextNodeId+"").getAddress().getHostAddress(), aliveMembers.get(nextToNextNodeId+"").getPort());
									sendMerge.writeObjectList(argList);
									//consume ack
									sendMerge.readObject();
									
									sendMerge.close();
								}
							}
							//Step 3 Get backup1 from previous node to new node. It will become backup2 of new node
							if(aliveMembers.size()>2){
								DSLogger.logFE(this.getClass().getName(), "run","Entering Step 2 of join Me for "+newMemberHashId);
								argList.clear();
								argList.add(0, "sendKeys"); //command
								
								argList.add(1, 1); //keyspace to send
								argList.add(2, newMember); //To
								argList.add(3, 2);// keyspace of destination
								argList.add(4, 1);// keyspace of destination
								DSLogger.logFE(this.getClass().getName(), "run","Asking node "+prevNodeId+" to send its backup1 key space to "+newMemberHashId);
								sendMerge = new DSocket(aliveMembers.get(prevNodeId+"").getAddress().getHostAddress(), aliveMembers.get(prevNodeId+"").getPort());
								sendMerge.writeObjectList(argList);
								//consume ack
								sendMerge.readObject();
								
								sendMerge.close();
							}
							
							//Partition backup 2 of nextTonexTonext node of new node according to new node 
							if(aliveMembers.size()>3){
								DSLogger.logFE(this.getClass().getName(), "run","Entering Step 3 of join Me for "+newMemberHashId);								
								Integer nextNodeId = sortedAliveMembers.higherKey(newMemberHashId)==null?sortedAliveMembers.firstKey():sortedAliveMembers.higherKey(newMemberHashId);
								Member nextNode = aliveMembers.get(nextNodeId+"");
								
								DSLogger.logFE(this.getClass().getName(), "run","Next node is "+nextNodeId+" : "+nextNode);
								
								Integer nextToNextNodeId = sortedAliveMembers.higherKey(nextNodeId)==null?sortedAliveMembers.firstKey():sortedAliveMembers.higherKey(nextNodeId);
								
								DSLogger.logFE(this.getClass().getName(), "run","Next to next node is "+nextToNextNodeId);
								
								Integer nextToNextToNextNodeId = sortedAliveMembers.higherKey(nextToNextNodeId)==null?sortedAliveMembers.firstKey():sortedAliveMembers.higherKey(nextToNextNodeId);
								
								DSLogger.logFE(this.getClass().getName(), "run","Next to next to next node is "+nextToNextToNextNodeId);
								DSLogger.logFE(this.getClass().getName(), "run","Asking node "+nextToNextToNextNodeId+" to stabilize its backup 2 as per "+newMemberHashId);
								argList.clear();
								argList.add(0, "splitBackup2"); //command
								argList.add(1, newMember); //Min node for partition
								argList.add(2, nextNode); //Max node for partition
								sendMerge = new DSocket(aliveMembers.get(nextToNextToNextNodeId+"").getAddress().getHostAddress(), aliveMembers.get(nextToNextToNextNodeId+"").getPort());
								sendMerge.writeObjectList(argList);
								//consume ack
								sendMerge.readObject();
								sendMerge.close();
							}
						}
						else if(cmd.equals("put")){
							DSLogger.logFE(this.getClass().getName(), "run","In put request");
							Object lockResult = new Object();
							HashMap<String, Object> resultMap = new HashMap<String, Object>();
					
							
							
							String key= (String)argList.get(2);
							Integer hashedKey=Hash.doHash(key.toString());//Use hashedKey only for determining the node which needs to hold the actual key-value.
							Object value=(Object)argList.get(3);
							//1 - ONE
							//2 - QUORUM
							//3 - ALL
							Integer consistencyLevel = (Integer)argList.get(1);
							
							DSLogger.logFE(this.getClass().getName(), "run","Received parameters : Key, Value, CL "+key+":"+value+":"+consistencyLevel);							
							
							DSLogger.logFE(this.getClass().getName(), "run","Received put request from "+socket.getSocket().getRemoteSocketAddress());
							Integer primayReplica = -1;
							if(sortedAliveMembers.containsKey(hashedKey)){
								primayReplica = hashedKey;
							}else{
								primayReplica = sortedAliveMembers.higherKey(hashedKey)==null?sortedAliveMembers.firstKey():sortedAliveMembers.higherKey(hashedKey);
							}
							
							DSLogger.logFE(this.getClass().getName(), "run","Primary replica for this key is "+primayReplica);
							Integer firstReplica = null;
							if(sortedAliveMembers.size()>1){
								firstReplica = sortedAliveMembers.higherKey(primayReplica)==null?sortedAliveMembers.firstKey():sortedAliveMembers.higherKey(primayReplica);
							}
							Integer secondReplica = null;
							if(sortedAliveMembers.size()>2){
								secondReplica = sortedAliveMembers.higherKey(firstReplica)==null?sortedAliveMembers.firstKey():sortedAliveMembers.higherKey(firstReplica);
							}
							
							DSLogger.logFE(this.getClass().getName(), "run","First replica for this key is "+firstReplica);
							DSLogger.logFE(this.getClass().getName(), "run","Second replica for this key is "+secondReplica);
							
							
							// Contact all replicas to insert the received key but wait for reply from no of machines defined
							// by consistency level
							
							Executor executor = Executors.newFixedThreadPool(3);
							String address = "";
							int port = -1;
							if(aliveMembers.get(primayReplica+"")!=null){
								address = aliveMembers.get(primayReplica+"").getAddress().getHostAddress();
								port = aliveMembers.get(primayReplica+"").getPort();
								DSLogger.logFE(this.getClass().getName(), "run","Contacting Primary Replica "+address+" : "+port+ "for operation "+cmd);
								executor.execute(new AsynchFEExecutor(resultMap, lockResult, address, port, argList, 0));
							}
							
							//Contact first replica
							if(aliveMembers.get(firstReplica+"")!=null){
								address = aliveMembers.get(firstReplica+"").getAddress().getHostAddress();
								port = aliveMembers.get(firstReplica+"").getPort();
								DSLogger.logFE(this.getClass().getName(), "run","Contacting First Replica "+address+" : "+port+ "for operation "+cmd);
								executor.execute(new AsynchFEExecutor(resultMap, lockResult, address, port, argList, 1));
							}
							
							//contact second replica
							if(aliveMembers.get(secondReplica+"")!=null){
								address = aliveMembers.get(secondReplica+"").getAddress().getHostAddress();
								port = aliveMembers.get(secondReplica+"").getPort();
								DSLogger.logFE(this.getClass().getName(), "run","Contacting Second Replica "+address+" : "+port+ "for operation "+cmd);
								executor.execute(new AsynchFEExecutor(resultMap, lockResult, address, port, argList, 2));
							}
							
							DSLogger.logFE(this.getClass().getName(), "run","Waiting for "+consistencyLevel+" threads to finish operation");
							
							while(true){
								//DSLogger.logFE(this.getClass().getName(), "run"," consistency level is "+consistencyLevel+"Result Map size is "+resultMap.size());
								synchronized (lockResult) {
									if(resultMap.size()>=consistencyLevel){
										DSLogger.logFE(this.getClass().getName(), "run","Consistency level "+consistencyLevel+" satisfied");
										outputMap.clear();
										outputMap.putAll(resultMap);
										break;
									}
								}
								
							}
							
							DSLogger.logFE(this.getClass().getName(), "run","Got results from threads " +outputMap);
							socket.writeObject(outputMap.values().toArray()[0]);
							socket.close();
							DSLogger.logFE(this.getClass().getName(), "run","Exiting");
						}
						else if(cmd.equals("update")){
							DSLogger.logFE(this.getClass().getName(), "run","In update request");
							Object lockResult = new Object();
							HashMap<String, Object> resultMap = new HashMap<String, Object>();
					
							
							
							String key= (String)argList.get(2);
							Integer hashedKey=Hash.doHash(key.toString());//Use hashedKey only for determining the node which needs to hold the actual key-value.
							Object value=(Object)argList.get(3);
							//1 - ONE
							//2 - QUORUM
							//3 - ALL
							Integer consistencyLevel = (Integer)argList.get(1);
							
							DSLogger.logFE(this.getClass().getName(), "run","Received parameters : Key, Value, CL "+key+":"+value+":"+consistencyLevel);						
							
							DSLogger.logFE(this.getClass().getName(), "run","Received update request from "+socket.getSocket().getRemoteSocketAddress());
							Integer primayReplica = -1;
							if(sortedAliveMembers.containsKey(hashedKey)){
								primayReplica = hashedKey;
							}else{
								primayReplica = sortedAliveMembers.higherKey(hashedKey)==null?sortedAliveMembers.firstKey():sortedAliveMembers.higherKey(hashedKey);
							}
							
							DSLogger.logFE(this.getClass().getName(), "run","Primary replica for this key is "+primayReplica);
							Integer firstReplica = null;
							if(sortedAliveMembers.size()>1){
								firstReplica = sortedAliveMembers.higherKey(primayReplica)==null?sortedAliveMembers.firstKey():sortedAliveMembers.higherKey(primayReplica);
							}
							Integer secondReplica = null;
							if(sortedAliveMembers.size()>2){
								secondReplica = sortedAliveMembers.higherKey(firstReplica)==null?sortedAliveMembers.firstKey():sortedAliveMembers.higherKey(firstReplica);
							}
							
							DSLogger.logFE(this.getClass().getName(), "run","First replica for this key is "+firstReplica);
							DSLogger.logFE(this.getClass().getName(), "run","Second replica for this key is "+secondReplica);
							
							
							// Contact all replicas to insert the received key but wait for reply from no of machines defined
							// by consistency level
							
							Executor executor = Executors.newFixedThreadPool(3);
							String address = "";
							int port = -1;
							if(aliveMembers.get(primayReplica+"")!=null){
								address = aliveMembers.get(primayReplica+"").getAddress().getHostAddress();
								port = aliveMembers.get(primayReplica+"").getPort();
								DSLogger.logFE(this.getClass().getName(), "run","Contacting Primary Replica "+address+" : "+port+ "for operation "+cmd);
								executor.execute(new AsynchFEExecutor(resultMap, lockResult, address, port, argList, 0));
							}
							
							//Contact first replica
							if(aliveMembers.get(firstReplica+"")!=null){
								address = aliveMembers.get(firstReplica+"").getAddress().getHostAddress();
								port = aliveMembers.get(firstReplica+"").getPort();
								DSLogger.logFE(this.getClass().getName(), "run","Contacting First Replica "+address+" : "+port+ "for operation "+cmd);
								executor.execute(new AsynchFEExecutor(resultMap, lockResult, address, port, argList, 1));
							}
							
							//contact second replica
							if(aliveMembers.get(secondReplica+"")!=null){
								address = aliveMembers.get(secondReplica+"").getAddress().getHostAddress();
								port = aliveMembers.get(secondReplica+"").getPort();
								DSLogger.logFE(this.getClass().getName(), "run","Contacting Second Replica "+address+" : "+port+ "for operation "+cmd);
								executor.execute(new AsynchFEExecutor(resultMap, lockResult, address, port, argList, 2));
							}
							
							DSLogger.logFE(this.getClass().getName(), "run","Waiting for "+consistencyLevel+" threads to finish operation");
							while(true){
								//DSLogger.logFE(this.getClass().getName(), "run"," consistency level is "+consistencyLevel+"Result Map size is "+resultMap.size());
								synchronized (lockResult) {
									if(resultMap.size()>=consistencyLevel){
										DSLogger.logFE(this.getClass().getName(), "run","Consistency level "+consistencyLevel+" satisfied");
										outputMap.clear();
										outputMap.putAll(resultMap);
										break;
									}
								}
								
							}
							DSLogger.logFE(this.getClass().getName(), "run","Got results from threads " +outputMap);
							socket.writeObject(outputMap.values().toArray()[0]);
							socket.close();
							DSLogger.logFE(this.getClass().getName(), "run","Exiting");
						}
						else if(cmd.equals("delete")){
							DSLogger.logFE(this.getClass().getName(), "run","In delete request");
							Object lockResult = new Object();
							HashMap<String, Object> resultMap = new HashMap<String, Object>();
					
							
							
							String key= (String)argList.get(2);
							Integer hashedKey=Hash.doHash(key.toString());//Use hashedKey only for determining the node which needs to hold the actual key-value.
							
							//1 - ONE
							//2 - QUORUM
							//3 - ALL
							Integer consistencyLevel = (Integer)argList.get(1);
							
							DSLogger.logFE(this.getClass().getName(), "run","Received parameters : Key, CL "+key+":"+consistencyLevel);

							DSLogger.logFE(this.getClass().getName(), "run","Received delete request from "+socket.getSocket().getRemoteSocketAddress());
							Integer primayReplica = -1;
							if(sortedAliveMembers.containsKey(hashedKey)){
								primayReplica = hashedKey;
							}else{
								primayReplica = sortedAliveMembers.higherKey(hashedKey)==null?sortedAliveMembers.firstKey():sortedAliveMembers.higherKey(hashedKey);
							}
							
							DSLogger.logFE(this.getClass().getName(), "run","Primary replica for this key is "+primayReplica);
							Integer firstReplica = null;
							if(sortedAliveMembers.size()>1){
								firstReplica = sortedAliveMembers.higherKey(primayReplica)==null?sortedAliveMembers.firstKey():sortedAliveMembers.higherKey(primayReplica);
							}
							Integer secondReplica = null;
							if(sortedAliveMembers.size()>2){
								secondReplica = sortedAliveMembers.higherKey(firstReplica)==null?sortedAliveMembers.firstKey():sortedAliveMembers.higherKey(firstReplica);
							}
							
							DSLogger.logFE(this.getClass().getName(), "run","First replica for this key is "+firstReplica);
							DSLogger.logFE(this.getClass().getName(), "run","Second replica for this key is "+secondReplica);
							
							
							// Contact all replicas to insert the received key but wait for reply from no of machines defined
							// by consistency level
							
							Executor executor = Executors.newFixedThreadPool(3);
							String address = "";
							int port = -1;
							if(aliveMembers.get(primayReplica+"")!=null){
								address = aliveMembers.get(primayReplica+"").getAddress().getHostAddress();
								port = aliveMembers.get(primayReplica+"").getPort();
								DSLogger.logFE(this.getClass().getName(), "run","Contacting Primary Replica "+address+" : "+port+ "for operation "+cmd);
								executor.execute(new AsynchFEExecutor(resultMap, lockResult, address, port, argList, 0));
							}
							
							//Contact first replica
							if(aliveMembers.get(firstReplica+"")!=null){
								address = aliveMembers.get(firstReplica+"").getAddress().getHostAddress();
								port = aliveMembers.get(firstReplica+"").getPort();
								DSLogger.logFE(this.getClass().getName(), "run","Contacting First Replica "+address+" : "+port+ "for operation "+cmd);
								executor.execute(new AsynchFEExecutor(resultMap, lockResult, address, port, argList, 1));
							}
							
							//contact second replica
							if(aliveMembers.get(secondReplica+"")!=null){
								address = aliveMembers.get(secondReplica+"").getAddress().getHostAddress();
								port = aliveMembers.get(secondReplica+"").getPort();
								DSLogger.logFE(this.getClass().getName(), "run","Contacting Second Replica "+address+" : "+port+ "for operation "+cmd);
								executor.execute(new AsynchFEExecutor(resultMap, lockResult, address, port, argList, 2));
							}
							
							DSLogger.logFE(this.getClass().getName(), "run","Waiting for "+consistencyLevel+" threads to finish operation");
							while(true){
								//DSLogger.logFE(this.getClass().getName(), "run"," consistency level is "+consistencyLevel+"Result Map size is "+resultMap.size());
								synchronized (lockResult) {
									if(resultMap.size()>=consistencyLevel){
										DSLogger.logFE(this.getClass().getName(), "run","Consistency level "+consistencyLevel+" satisfied");
										outputMap.clear();
										outputMap.putAll(resultMap);
										break;
									}
								}
								
							}
							
							DSLogger.logFE(this.getClass().getName(), "run","Got results from threads " +outputMap);
							socket.writeObject(outputMap.values().toArray()[0]);
							
							socket.close();
							DSLogger.logFE(this.getClass().getName(), "run","Exiting");
						}
						else if(cmd.equals("crash")){
							DSLogger.logFE(this.getClass().getName(), "run","Starting crash recovery");
							
							// wait for dead member list to update
							try {
								Thread.sleep(1500);
							} catch (InterruptedException e) {
								DSLogger.logFE(this.getClass().getName(), "run","Problem while waiting"+e.getMessage());
								e.printStackTrace();
							}
							System.out.println("Starting crash recovery");
							synchronized (lock) {
								sortedDeadMembers = this.constructSortedMap(deadMembers);
								sortedAliveMembers = this.constructSortedMap(aliveMembers);
								DSLogger.logFE(this.getClass().getName(), "run","Sorted Dead member set : "+sortedDeadMembers);
								DSLogger.logFE(this.getClass().getName(), "run","Sorted Alive member set : "+sortedAliveMembers);
							}
							Set<Integer> deadSet = sortedDeadMembers.keySet();
							boolean sequentialFailure =false;
							ArrayList<Integer> nextToFailure= new ArrayList<Integer>();
							ArrayList<Integer> machines = new ArrayList<Integer>();
							Integer nextMachineId = -1;
							for(Integer i: deadSet){
								nextToFailure.add(sortedAliveMembers.higherKey(i)==null?sortedAliveMembers.firstKey():sortedAliveMembers.higherKey(i));
								nextMachineId = i;
							}
							if(nextToFailure.get(0)==nextToFailure.get(1)){
								sequentialFailure = true;
							}
							System.out.println(nextToFailure);
							System.out.println(nextMachineId);
							
							//Two machines failed sequentially
							int i=0;
							if(sequentialFailure){
								DSLogger.logFE(this.getClass().getName(), "run","Two neighboring machines failed ");
								while(i<3){
									nextMachineId = sortedAliveMembers.higherKey(nextMachineId)==null?sortedAliveMembers.firstKey():sortedAliveMembers.higherKey(nextMachineId);
									machines.add(i++, nextMachineId);
									DSLogger.logFE(this.getClass().getName(), "run","Machine "+i+" in Sequential order is : "+nextMachineId);
								}
							}else{
								DSLogger.logFE(this.getClass().getName(), "run","Two non neighboring machines failed ");
								Integer firstMachine =-1;
								if(nextToFailure.get(1)==(sortedAliveMembers.higherKey(nextToFailure.get(0))==null?sortedAliveMembers.firstKey():sortedAliveMembers.higherKey(nextToFailure.get(0)))){
									firstMachine = nextToFailure.get(0);
								}else{
									firstMachine = nextToFailure.get(1);
								}
								nextMachineId = firstMachine;
								machines.add(i++, nextMachineId);
								DSLogger.logFE(this.getClass().getName(), "run","Machine "+i+" in Sequential order are : "+nextMachineId);
								while(i<3){
									nextMachineId = sortedAliveMembers.higherKey(nextMachineId)==null?sortedAliveMembers.firstKey():sortedAliveMembers.higherKey(nextMachineId);
									machines.add(i++, nextMachineId);
									DSLogger.logFE(this.getClass().getName(), "run","Machine "+i+" in Sequential order are : "+nextMachineId);
								}
							}
							System.out.println("Running machines are "+machines);
							//Perform actions to handle crash, there are two cases- 
							//CASE I Neighboring nodes fails
							
							if(sequentialFailure){
								//Step 1
								//TODO locally for machine 1 Primary = P+B1+B2 before sending keys
								//Transfer Backup2 from machine 1 to machine 2
								//for machine 2 - backup1 = backup1+backup2+received keys from machine1
								System.out.println("In step1 of crash handle");
								DSLogger.logFE(this.getClass().getName(), "run","In step1 of crash handle");
								
								argList.clear();
								argList.add(0, "sendKeysCrash"); //command, clear the keyspace.
								
								argList.add(1, 2); //keyspace to send
								argList.add(2, aliveMembers.get(machines.get(1)+"")); //To
								argList.add(3, 1);// keyspace of destination
								
								DSLogger.logFE(this.getClass().getName(), "run","Asking node "+machines.get(0)+" to send its backup2 key space to "+machines.get(1));
								Member mem = aliveMembers.get(machines.get(0)+"");
								DSocket sendMerge = new DSocket(mem.getAddress().getHostAddress(), mem.getPort());
								sendMerge.writeObjectList(argList);
								System.out.println("Waiting for ack on "+sendMerge.getSocket()+" "+sendMerge.getSocket().getPort());
								//consume ack
								Object ack = sendMerge.readObject();
								System.out.println(ack);
								System.out.println("Ack recieved");
								sendMerge.close();	
								
								//Step 2
								//Send primary from machine 3 to machine 1 and it will become backup 1
								System.out.println("In step2 of crash handle");
								DSLogger.logFE(this.getClass().getName(), "run","In step2 of crash handle");
								
								argList.clear();
								argList.add(0, "sendKeys"); //command
								
								argList.add(1, 0); //keyspace to send
								argList.add(2, aliveMembers.get(machines.get(0)+"")); //To
								argList.add(3, 1);// keyspace of destination
								argList.add(4, 1);//whether to replace the destination map
								
								DSLogger.logFE(this.getClass().getName(), "run","Asking node "+machines.get(2)+" to send its primary key space to "+machines.get(0));
								mem = aliveMembers.get(machines.get(2)+"");
								sendMerge = new DSocket(mem.getAddress().getHostAddress(), mem.getPort());
								sendMerge.writeObjectList(argList);
								//consume ack
								sendMerge.readObject();
								sendMerge.close();	
								
								//Step 3
								//Send backup1 from machine 3 to machine 1 and it will become backup 2
								System.out.println("In step3 of crash handle");
								DSLogger.logFE(this.getClass().getName(), "run","In step3 of crash handle");
								
								argList.clear();
								argList.add(0, "sendKeys"); //command //replaceKeys
								
								argList.add(1, 1); //keyspace to send
								argList.add(2, aliveMembers.get(machines.get(0)+"")); //To
								argList.add(3, 2);// keyspace of destination
								argList.add(4, 1);// replace=true
								
								DSLogger.logFE(this.getClass().getName(), "run","Asking node "+machines.get(2)+" to send its backup1 key space to "+machines.get(0));
								mem = aliveMembers.get(machines.get(2)+"");
								sendMerge = new DSocket(mem.getAddress().getHostAddress(), mem.getPort());
								sendMerge.writeObjectList(argList);
								//consume ack
								sendMerge.readObject();
								sendMerge.close();	
								
								
								//Step 4
								//Send primary from machine 3 to machine 2 and it will become backup 2
								System.out.println("In step4 of crash handle");
								DSLogger.logFE(this.getClass().getName(), "run","In step4 of crash handle");
								
								argList.clear();
								argList.add(0, "sendKeys"); //command
								
								argList.add(1, 0); //keyspace to send
								argList.add(2, aliveMembers.get(machines.get(1)+"")); //To
								argList.add(3, 2);// keyspace of destination
								argList.add(4, 1);// replace=true
								
								DSLogger.logFE(this.getClass().getName(), "run","Asking node "+machines.get(2)+" to send its primary key space to "+machines.get(1));
								mem = aliveMembers.get(machines.get(2)+"");
								sendMerge = new DSocket(mem.getAddress().getHostAddress(), mem.getPort());
								sendMerge.writeObjectList(argList);
								//consume ack
								sendMerge.readObject();
								sendMerge.close();
								
								//Step 5
								//Send primary from machine 1 to machine 3 and it will become backup 2
								System.out.println("In step5 of crash handle");
								DSLogger.logFE(this.getClass().getName(), "run","In step5 of crash handle");
								
								argList.clear();
								argList.add(0, "sendKeys"); //command
								
								argList.add(1, 0); //keyspace to send
								argList.add(2, aliveMembers.get(machines.get(2)+"")); //To
								argList.add(3, 2);// keyspace of destination
								argList.add(4, 1);// replace=true
								
								DSLogger.logFE(this.getClass().getName(), "run","Asking node "+machines.get(0)+" to send its primary key space to "+machines.get(2));
								mem = aliveMembers.get(machines.get(0)+"");
								sendMerge = new DSocket(mem.getAddress().getHostAddress(), mem.getPort());
								sendMerge.writeObjectList(argList);
								//consume ack
								sendMerge.readObject();
								sendMerge.close();	
								
								System.out.println("Failure handling done");
								DSLogger.logFE(this.getClass().getName(), "run","Failure handling done");
							}else{
								//CASE II Non neighboring nodes failed
								
								//Step 1
								//TODO locally for machine 1 Primary = P+B1 before sending keys
								System.out.println("In Step1 of crash handle ");
								DSLogger.logFE(this.getClass().getName(), "run","In Step1 of crash handle");
								argList.clear();
								argList.add(0, "combine"); //command
								argList.add(1, 0); //destination keyspace 
								argList.add(2, 1); //key space to combine
								
								DSLogger.logFE(this.getClass().getName(), "run","Asking node "+machines.get(0)+" to combine its primary key space to backup1");
								Member mem = aliveMembers.get(machines.get(0)+"");
								DSocket sendMerge = new DSocket(mem.getAddress().getHostAddress(), mem.getPort());
								sendMerge.writeObjectList(argList);
								//consume ack
								sendMerge.readObject();
								sendMerge.close();
								
								//Step 2
								//TODO locally for machine 2 Primary = P+B1 before sending keys
								System.out.println("In Step2 of crash handle ");
								DSLogger.logFE(this.getClass().getName(), "run","In Step2 of crash handle");
								argList.clear();
								argList.add(0, "combine"); //command
								argList.add(1, 0); //destination keyspace 
								argList.add(2, 1); //key space to combine
								
								DSLogger.logFE(this.getClass().getName(), "run","Asking node "+machines.get(1)+" to combine its primary key space to backup1");
								mem = aliveMembers.get(machines.get(1)+"");
								sendMerge = new DSocket(mem.getAddress().getHostAddress(), mem.getPort());
								sendMerge.writeObjectList(argList);
								//consume ack
								sendMerge.readObject();
								sendMerge.close();
								
								
								
								//Step 3
								//TODO locally for machine 3 B1 = B1+B2 before sending keys
								System.out.println("In Step3 of crash handle ");
								DSLogger.logFE(this.getClass().getName(), "run","In Step3 of crash handle");
								argList.clear();
								argList.add(0, "combine"); //command
								argList.add(1, 1); //destination keyspace 
								argList.add(2, 2); //key space to combine
								
								DSLogger.logFE(this.getClass().getName(), "run","Asking node "+machines.get(2)+" to combine its primary key space to backup1");
								mem = aliveMembers.get(machines.get(2)+"");
								sendMerge = new DSocket(mem.getAddress().getHostAddress(), mem.getPort());
								sendMerge.writeObjectList(argList);
								//consume ack
								sendMerge.readObject();
								sendMerge.close();
								
								
								
								
								//Step 4
								//locally for machine 1 Replace Backup1 with backup2 
								System.out.println("In Step4 of crash handle ");
								DSLogger.logFE(this.getClass().getName(), "run","In Step4 of crash handle");
								
								argList.clear();
								
								argList.add(0, "sendKeysCrashN"); //command
								DSLogger.logFE(this.getClass().getName(), "run","Asking node "+machines.get(0)+" to Replace Backup1 with backup2  ");
								mem = aliveMembers.get(machines.get(0)+"");
								sendMerge = new DSocket(mem.getAddress().getHostAddress(), mem.getPort());
								sendMerge.writeObjectList(argList);
								//consume ack
								sendMerge.readObject();
								sendMerge.close();
								
								
								
								//Step 5
								//Send Primary keys from machine 1 to machine 3 and make it backup2
								System.out.println("In Step5 of crash handle ");
								DSLogger.logFE(this.getClass().getName(), "run","In Step5 of crash handle");
								
								argList.clear();
								argList.add(0, "sendKeys"); //command
								
								argList.add(1, 0); //keyspace to send
								argList.add(2, aliveMembers.get(machines.get(2)+"")); //To
								argList.add(3, 2);// keyspace of destination
								argList.add(4, 1);// replace=true
								
								DSLogger.logFE(this.getClass().getName(), "run","Asking node "+machines.get(0)+" to send its primary key space to "+machines.get(2));
								mem = aliveMembers.get(machines.get(0)+"");
								sendMerge = new DSocket(mem.getAddress().getHostAddress(), mem.getPort());
								sendMerge.writeObjectList(argList);
								//consume ack
								sendMerge.readObject();
								sendMerge.close();
								
								
								//Step 6						
								//Send Primary keys from machine 1 to machine 2 and make it backup1
								System.out.println("In Step6 of crash handle");
								DSLogger.logFE(this.getClass().getName(), "run","In Step6 of crash handle");
								
								argList.clear();
								argList.add(0, "sendKeys"); //command
								
								argList.add(1, 0); //keyspace to send
								argList.add(2, aliveMembers.get(machines.get(1)+"")); //To
								argList.add(3, 1);// keyspace of destination
								argList.add(4, 1);// replace=true
								
								DSLogger.logFE(this.getClass().getName(), "run","Asking node "+machines.get(0)+" to send its primary key space to "+machines.get(1));
								mem = aliveMembers.get(machines.get(0)+"");
								sendMerge = new DSocket(mem.getAddress().getHostAddress(), mem.getPort());
								sendMerge.writeObjectList(argList);
								//consume ack
								sendMerge.readObject();
								sendMerge.close();
								
								
								//Step 7
								//Send Primary keys from machine 3 to machine 2 and make it backup2
								System.out.println("In Step6 of crash handle");
								DSLogger.logFE(this.getClass().getName(), "run","In Step7 of crash handle");
								
								argList.clear();
								argList.add(0, "sendKeys"); //command
								
								argList.add(1, 0); //keyspace to send
								argList.add(2, aliveMembers.get(machines.get(1)+"")); //To
								argList.add(3, 2);// keyspace of destination
								argList.add(4, 1);// replace=true
								
								DSLogger.logFE(this.getClass().getName(), "run","Asking node "+machines.get(2)+" to send its primary key space to "+machines.get(1));
								mem = aliveMembers.get(machines.get(2)+"");
								sendMerge = new DSocket(mem.getAddress().getHostAddress(), mem.getPort());
								sendMerge.writeObjectList(argList);
								//consume ack
								sendMerge.readObject();
								sendMerge.close();
								
								//Step 8
								//Send Primary keys from machine 2 to machine 1 and make it backup2
								System.out.println("In Step8 of crash handle");
								DSLogger.logFE(this.getClass().getName(), "run","In Step8 of crash handle");
								
								argList.clear();
								argList.add(0, "sendKeys"); //command
								
								argList.add(1, 0); //keyspace to send
								argList.add(2, aliveMembers.get(machines.get(0)+"")); //To
								argList.add(3, 2);// keyspace of destination
								argList.add(4, 1);// replace=true
								
								DSLogger.logFE(this.getClass().getName(), "run","Asking node "+machines.get(1)+" to send its primary key space to "+machines.get(0));
								mem = aliveMembers.get(machines.get(1)+"");
								sendMerge = new DSocket(mem.getAddress().getHostAddress(), mem.getPort());
								sendMerge.writeObjectList(argList);
								//consume ack
								sendMerge.readObject();
								sendMerge.close();
								

								
								//Step 8
								//TODO locally for machine 3 send primary to machine 2
								//for machine 2 - backup2 = primary received from machine 3
								/*System.out.println("In Step8 of crash handle ");
								DSLogger.logFE(this.getClass().getName(), "run","In Step8 of crash handle");
								
								argList.clear();
								argList.add(0, "sendKeys"); //command
								
								argList.add(1, 0); //keyspace to send
								argList.add(2, aliveMembers.get(machines.get(1)+"")); //To
								argList.add(3, 2);// keyspace of destination
								argList.add(4, 1);// replace=true
								
								DSLogger.logFE(this.getClass().getName(), "run","Asking node "+machines.get(2)+" to send its primary key space to "+machines.get(1));
								mem = aliveMembers.get(machines.get(2)+"");
								sendMerge = new DSocket(mem.getAddress().getHostAddress(), mem.getPort());
								sendMerge.writeObjectList(argList);
								//consume ack
								sendMerge.readObject();
								sendMerge.close();*/
								
								System.out.println("Failure Handling done");
							}
							
						}
						
						else if(cmd.equals("get")){
							DSLogger.logFE(this.getClass().getName(), "run","In get request");
							Object lockResult = new Object();
							HashMap<String, Object> resultMap = new HashMap<String, Object>();
					
							
							Integer consistencyLevel = (Integer)argList.get(1);
							
							String key= (String)argList.get(2);
							Integer hashedKey=Hash.doHash(key.toString());//Use hashedKey only for determining the node which needs to hold the actual key-value.
							
							//1 - ONE
							//2 - QUORUM
							//3 - ALL
							
							
							DSLogger.logFE(this.getClass().getName(), "run","Received parameters : Key, CL "+key+":"+consistencyLevel);							
							
							DSLogger.logFE(this.getClass().getName(), "run","Received get request from "+socket.getSocket().getRemoteSocketAddress());
							
							Integer primayReplica = -1;
							if(sortedAliveMembers.containsKey(hashedKey)){
								primayReplica = hashedKey;
							}else{
								primayReplica = sortedAliveMembers.higherKey(hashedKey)==null?sortedAliveMembers.firstKey():sortedAliveMembers.higherKey(hashedKey);
							}
							
							DSLogger.logFE(this.getClass().getName(), "run","Primary replica for this key is "+primayReplica);
							Integer firstReplica = null;
							if(sortedAliveMembers.size()>1){
								firstReplica = sortedAliveMembers.higherKey(primayReplica)==null?sortedAliveMembers.firstKey():sortedAliveMembers.higherKey(primayReplica);
							}
							Integer secondReplica = null;
							if(sortedAliveMembers.size()>2){
								secondReplica = sortedAliveMembers.higherKey(firstReplica)==null?sortedAliveMembers.firstKey():sortedAliveMembers.higherKey(firstReplica);
							}
							
							DSLogger.logFE(this.getClass().getName(), "run","First replica for this key is "+firstReplica);
							DSLogger.logFE(this.getClass().getName(), "run","Second replica for this key is "+secondReplica);
							
							
							// Contact all replicas to insert the received key but wait for reply from no of machines defined
							// by consistency level
							
							Executor executor = Executors.newFixedThreadPool(3);
							String address = "";
							int port = -1;
							if(aliveMembers.get(primayReplica+"")!=null){
								address = aliveMembers.get(primayReplica+"").getAddress().getHostAddress();
								port = aliveMembers.get(primayReplica+"").getPort();
								DSLogger.logFE(this.getClass().getName(), "run","Contacting Primary Replica "+address+" : "+port+ "for operation "+cmd);
								executor.execute(new AsynchFEExecutor(resultMap, lockResult, address, port, argList, 0));
							}
							
							//Contact first replica
							if(aliveMembers.get(firstReplica+"")!=null){
								address = aliveMembers.get(firstReplica+"").getAddress().getHostAddress();
								port = aliveMembers.get(firstReplica+"").getPort();
								DSLogger.logFE(this.getClass().getName(), "run","Contacting First Replica "+address+" : "+port+ "for operation "+cmd);
								executor.execute(new AsynchFEExecutor(resultMap, lockResult, address, port, argList, 1));
							}
							
							//contact second replica
							if(aliveMembers.get(secondReplica+"")!=null){
								address = aliveMembers.get(secondReplica+"").getAddress().getHostAddress();
								port = aliveMembers.get(secondReplica+"").getPort();
								DSLogger.logFE(this.getClass().getName(), "run","Contacting Second Replica "+address+" : "+port+ "for operation "+cmd);
								executor.execute(new AsynchFEExecutor(resultMap, lockResult, address, port, argList, 2));
							}
							
							DSLogger.logFE(this.getClass().getName(), "run","Waiting for "+consistencyLevel+" threads to finish operation");
							while(true){
								//DSLogger.logFE(this.getClass().getName(), "run"," consistency level is "+consistencyLevel+"Result Map size is "+resultMap.size());
								synchronized (lockResult) {
									if(resultMap.size()>=consistencyLevel){
										DSLogger.logFE(this.getClass().getName(), "run","Consistency level "+consistencyLevel+" satisfied");
										outputMap.clear();
										outputMap.putAll(resultMap);
										break;
									}
								}
								
							}
							DSLogger.logFE(this.getClass().getName(), "run","Got results from threads " +outputMap);
							socket.writeObject(outputMap.values().toArray()[0]);
							socket.close();
							DSLogger.logFE(this.getClass().getName(), "run","Exiting");
						}
					
				} catch (IOException e) {
					DSLogger.logFE("FrontEnd","run","In exception "+e.getMessage());
					e.printStackTrace();
				}
			}
	}
	public TreeMap<Integer, Member> constructSortedMap(HashMap<String, Member> map){	
		sortedAliveMembers = new TreeMap<Integer, Member>();
		for(String key: map.keySet()){
			sortedAliveMembers.put(Integer.parseInt(key), map.get(key));
		}
		return sortedAliveMembers;
	}

}
