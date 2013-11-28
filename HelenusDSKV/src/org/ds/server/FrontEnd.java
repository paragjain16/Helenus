package org.ds.server;

import java.io.IOException;
import java.net.ServerSocket;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
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
	private Object lock;
	private Member itself;
	
	//Shares the alive and dead member list of the contact server
	public FrontEnd(HashMap<String, Member> aliveMembers, HashMap<String, Member> deadMembers, Object lock, Member itself){
		this.aliveMembers = aliveMembers;
		this.deadMembers = deadMembers;
		this.lock = lock;
		this.itself = itself;
		try {
			serverSocket = new ServerSocket(4000);
		} catch (IOException e) {
			DSLogger.logFE("FrontEnd", "FrontEnd", e.getMessage());
			e.printStackTrace();
		}
	}
	
	@Override
	public void run() {
		//accept requests and blocks till it is served
				try {
					DSLogger.logFE("FrontEnd","run","Listening to commands");
					while(true){
						DSLogger.logFE("FrontEnd","run","Listening to commands");
						socket = new DSocket(serverSocket.accept());
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
							//TODO wait for ack
							joinRequest.readObject();
							DSLogger.logFE(this.getClass().getName(), "run","Ack received from : "+joinRequest.getSocket().getRemoteSocketAddress());
							/*
							//Get primary and backup1 from previous node to new node. It will become backup1 and backup2 of new node respectively. 
							argList.clear();
							argList.add(0, "sendKeys"); //command
							argList.add(1, newMember); //To
							argList.add(2, 0); //keyspace to send
							Integer prevNodeId = sortedAliveMembers.lowerKey(newMemberHashId)==null?sortedAliveMembers.lastKey():sortedAliveMembers.lowerKey(newMemberHashId);
							DSLogger.logFE(this.getClass().getName(), "run","Asking node "+prevNodeId+" to send its primay key space to "+newMemberHashId);
							DSocket sendMerge = new DSocket(aliveMembers.get(prevNodeId+"").getAddress().getHostAddress(), aliveMembers.get(prevNodeId+"").getPort());
							sendMerge.writeObjectList(argList);
							//TODO wait for ack
							
							//Step 2
							argList.clear();
							argList.add(0, "sendKeys"); //command
							argList.add(1, newMember); //To
							argList.add(2, 1); //keyspace to send
							DSLogger.logFE(this.getClass().getName(), "run","Asking node "+prevNodeId+" to send its backup1 key space to "+newMemberHashId);
							sendMerge.writeObjectList(argList);
							
							//TODO wait for ack
							
							//Step 3 For next to next node to new node
							Integer nextNodeId = sortedAliveMembers.higherKey(newMemberHashId)==null?sortedAliveMembers.firstKey():sortedAliveMembers.higherKey(newMemberHashId);
							
							DSLogger.logFE(this.getClass().getName(), "run","Next node is "+nextNodeId);
							
							Integer nextToNextNodeId = sortedAliveMembers.higherKey(nextNodeId)==null?sortedAliveMembers.firstKey():sortedAliveMembers.higherKey(nextNodeId);
							
							DSLogger.logFE(this.getClass().getName(), "run","Next to next node is "+nextToNextNodeId);
							
							if(nextToNextNodeId != newMemberHashId){
								DSLogger.logFE(this.getClass().getName(), "run","Asking node "+nextToNextNodeId+" to stabilize as per "+newMemberHashId);
								argList.clear();
								argList.add(0, "newNodeStabilization"); //command
								argList.add(1, newMember); //To
								sendMerge = new DSocket(aliveMembers.get(nextToNextNodeId+"").getAddress().getHostAddress(), aliveMembers.get(nextToNextNodeId+"").getPort());
								sendMerge.writeObjectList(argList);
								//TODO wait for ack
							}
							*/
						}
						else if(cmd.equals("put")){
							DSLogger.logFE(this.getClass().getName(), "run","In put request");
							Object lockResult = new Object();
							HashMap<String, Object> resultMap = new HashMap<String, Object>();
							HashMap<Integer, Integer> replicas = new HashMap<Integer, Integer>();
							
							
							String key= (String)argList.get(2);
							Integer hashedKey=Hash.doHash(key.toString());//Use hashedKey only for determining the node which needs to hold the actual key-value.
							Object value=(Object)argList.get(3);
							//0 - ONE
							//1 - QUORUM
							//2 - ALL
							Integer consistencyLevel = (Integer)argList.get(1);
							
							DSLogger.logFE(this.getClass().getName(), "run","Received parameters : Key, Value, CL "+key+":"+value+":"+consistencyLevel);
							//int count = 1;
							/*switch(consistencyLevel){
							case 0:
								count = 1;
								break;
							case 1:
								count = 2;
								break;
							case 2:
								count = 3;
								break;
							default:
								count = 1;
							}*/
							
							
							DSLogger.logFE(this.getClass().getName(), "run","Received put request from "+socket.getSocket().getRemoteSocketAddress());
							Integer primayReplica = -1;
							if(sortedAliveMembers.containsKey(hashedKey)){
								primayReplica = hashedKey;
							}else{
								primayReplica = sortedAliveMembers.higherKey(hashedKey)==null?sortedAliveMembers.firstKey():sortedAliveMembers.higherKey(hashedKey);
							}
							replicas.put(0, primayReplica);
							DSLogger.logFE(this.getClass().getName(), "run","Primary replica for this key is "+replicas.get(0));
							
							Integer firstReplica = sortedAliveMembers.higherKey(primayReplica)==null?sortedAliveMembers.firstKey():sortedAliveMembers.higherKey(primayReplica);
							Integer secondReplica = sortedAliveMembers.higherKey(firstReplica)==null?sortedAliveMembers.firstKey():sortedAliveMembers.higherKey(firstReplica);
							
							if(!replicas.values().contains(firstReplica)){
								replicas.put(1, firstReplica);
							}
							if(!replicas.values().contains(secondReplica)){
								replicas.put(2, secondReplica);
							}
							DSLogger.logFE(this.getClass().getName(), "run","First replica for this key is "+replicas.get(1));
							DSLogger.logFE(this.getClass().getName(), "run","Second replica for this key is "+replicas.get(2));
							
							
							// Contact all replicas to insert the received key but wait for reply from no of machines defined
							// by consistency level
							
							Executor executor = Executors.newFixedThreadPool(3);
							String address = "";
							int port = -1;
							if(aliveMembers.get(primayReplica+"")!=null){
								address = aliveMembers.get(primayReplica+"").getAddress().getHostAddress();
								port = aliveMembers.get(primayReplica+"").getPort();
								DSLogger.logFE(this.getClass().getName(), "run","Contacting "+address+" : "+port+ "for operation "+cmd);
								executor.execute(new AsynchFEExecutor(resultMap, lockResult, address, port, argList, 0));
							}
							
							//Contact first replica
							if(aliveMembers.get(firstReplica+"")!=null){
								address = aliveMembers.get(firstReplica+"").getAddress().getHostAddress();
								port = aliveMembers.get(firstReplica+"").getPort();
								DSLogger.logFE(this.getClass().getName(), "run","Contacting "+address+" : "+port+ "for operation "+cmd);
								executor.execute(new AsynchFEExecutor(resultMap, lockResult, address, port, argList, 1));
							}
							
							//contact second replica
							if(aliveMembers.get(secondReplica+"")!=null){
								address = aliveMembers.get(secondReplica+"").getAddress().getHostAddress();
								port = aliveMembers.get(secondReplica+"").getPort();
								DSLogger.logFE(this.getClass().getName(), "run","Contacting "+address+" : "+port+ "for operation "+cmd);
								executor.execute(new AsynchFEExecutor(resultMap, lockResult, address, port, argList, 2));
							}
							
							DSLogger.logFE(this.getClass().getName(), "run","Waiting for "+consistencyLevel+" threads to finish operation");
							while(true){
								//DSLogger.logFE(this.getClass().getName(), "run"," consistency level is "+consistencyLevel+"Result Map size is "+resultMap.size());
								if(resultMap.size()==consistencyLevel){
									DSLogger.logFE(this.getClass().getName(), "run","Consistency level "+consistencyLevel+" satisfied");
									break;
								}
							}
							DSLogger.logFE(this.getClass().getName(), "run","Got results from threads " +resultMap);
							socket.writeObject(resultMap.values().toArray()[0]);
							
							DSLogger.logFE(this.getClass().getName(), "run","Exiting");
						}
					}
				} catch (IOException e) {
					DSLogger.logFE("FrontEnd","run",e.getMessage());
					e.printStackTrace();
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
