package org.ds.server;

import java.io.IOException;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.BlockingQueue;



import org.ds.hash.Hash;
import org.ds.logger.DSLogger;
import org.ds.member.Member;
import org.ds.server.KVStoreOperation.MapType;
import org.ds.socket.DSocket;

/**
 * @author { pjain11, mallapu2 } @ illinois.edu
 * This class takes care of what appropriate action
 * needs to be taken for command received from Node Client
 * Separate thread is created for handling each command
 */

public class HandleCommands implements Runnable{
	
	DSocket socket;
	TreeMap<Integer, Member> sortedAliveMembers;
	HashMap<String, Member> aliveMembers;
	BlockingQueue<KVStoreOperation> operationQueue;
	BlockingQueue<Object> resultQueue;
	Object lock;
	Member itself;
	
	public HandleCommands(Socket s, HashMap<String, Member> aliveMembers, Object lock,BlockingQueue<KVStoreOperation> operationQueue,
			BlockingQueue<Object> resultQueue, Member itself){
		try {
			socket = new DSocket(s);
			this.aliveMembers = aliveMembers;
			this.lock = lock;
			this.operationQueue=operationQueue;
			this.resultQueue=resultQueue;
			this.itself = itself;
		} catch (UnknownHostException e) {
			DSLogger.logAdmin(this.getClass().getName(), "HandleCommand",e.getMessage());
			e.printStackTrace();
		} catch (IOException e) {			
			e.printStackTrace();
		}
	}
	
	public void run(){
		try{
			DSLogger.logAdmin(this.getClass().getName(), "run","Entering");
			//sort the map
			synchronized (lock) {
				sortedAliveMembers = this.constructSortedMap(aliveMembers);
				DSLogger.logAdmin(this.getClass().getName(), "run","Sorted Map :"+sortedAliveMembers);
			}
			List<Object> argList = (ArrayList<Object>)socket.readObject();
			//System.out.println("Received argList of size:"+argList.size()+"from socket: "+socket.getSocket());
			String cmd=(String) argList.get(0);
			DSLogger.logFE("HandleCommands", "run", "For command: "+cmd+" Received argList of size:"+argList.size()+"from socket: "+socket.getSocket());
			DSLogger.logFE("HandleCommands", "run", "Received list: "+argList);
			DSLogger.logAdmin(this.getClass().getName(), "run","Executing command:"+cmd);
			/*
			 * Handle different commands
			 * 
			 * 
			 * */
			//sent by new node wanting to join network
			if(cmd.equals("joinMe")){
				Member newMember = (Member) argList.get(1);
				DSLogger.logAdmin("Node", "listenToCommands", "Received join request from "+newMember.getIdentifier());
				String memberId = newMember.getIdentifier()+"";
				synchronized (lock) {
					aliveMembers.put(memberId, newMember);
					sortedAliveMembers = this.constructSortedMap(aliveMembers);
				}
				Integer newMemberHashId = Integer.parseInt(newMember.getIdentifier());
				Integer nextNodeId = sortedAliveMembers.higherKey(newMemberHashId)==null?sortedAliveMembers.firstKey():sortedAliveMembers.higherKey(newMemberHashId);
				DSLogger.logAdmin("Node", "listenToCommands", "Asking next node "+nextNodeId+" to send its keys ");
				//Member nextNode = aliveMembers.get(nextNodeId+"");
				
				DSocket sendMerge = new DSocket(aliveMembers.get(nextNodeId+"").getAddress().getHostAddress(), aliveMembers.get(nextNodeId+"").getPort());
				List<Object>  objList= new ArrayList<Object>();
				objList.add("partition");
				objList.add(newMember);
				sendMerge.writeObjectList(objList);
				//consuming the ack
				DSLogger.logFE("Node", "listenToCommands", "trying to receive ack from "+sendMerge.getSocket().getRemoteSocketAddress());
				Object obj = sendMerge.readObject();
				DSLogger.logFE("Node", "listenToCommands", "Received ack "+obj+" from "+sendMerge.getSocket().getRemoteSocketAddress());
				//Sending ack back to FE
				DSLogger.logFE("Node", "listenToCommands", "Sending ack "+obj+" to "+socket.getSocket().getRemoteSocketAddress());
				socket.writeObject(obj);
				
			}
			// sent by node leaving the network
			else if(cmd.equals("leave")){
				Integer itselfId = Integer.parseInt(itself.getIdentifier());
				DSLogger.logAdmin("Node", "listenToCommands", "Leaving group");
				Integer nextNodeId = sortedAliveMembers.higherKey(itselfId)==null?sortedAliveMembers.firstKey():sortedAliveMembers.higherKey(itselfId);
				
				DSLogger.logAdmin("Node", "listenToCommands", "Contacting key value store locally to get keys");
				KVStoreOperation operation=new KVStoreOperation("-1", KVStoreOperation.OperationType.LEAVE,KVStoreOperation.MapType.PRIMARY);
				operationQueue.put(operation);
				HashMap<Integer, Object> keyValueStore = (HashMap<Integer, Object>)resultQueue.take();
				
				DSLogger.logAdmin("Node", "listenToCommands", "Sending keys to next node "+nextNodeId);
				DSocket sendMerge = new DSocket(aliveMembers.get(nextNodeId+"").getAddress().getHostAddress(), aliveMembers.get(nextNodeId+"").getPort());
				List<Object>  objList= new ArrayList<Object>();
				objList.add("merge");
				objList.add(keyValueStore);
				objList.add(Integer.parseInt("0"));//Merge with primary map
				sendMerge.writeObjectList(objList);
				String ack = (String)sendMerge.readObject();
				if(ack.equals("ack")){
					System.exit(0);
				}
				
			}
			//for getting a key
			else if(cmd.equals("get")){
				String key= (String)argList.get(1);
				Integer mapNumber=(Integer)argList.get(2);		
				MapType mapType=MapType.values()[mapNumber];
				DSLogger.logAdmin("HandleCommand", "run","Retrieving value for key:"+key+" from local key value store from mapType:"+mapType);
				//System.out.println("Retrieving value for key:"+key+" from local key value store");
				KVStoreOperation operation=new KVStoreOperation(key, KVStoreOperation.OperationType.GET,mapType);
				operationQueue.put(operation);
				Object value=resultQueue.take();
				
				//Write back the value to the requesting client socket
				if(value!=null)
				DSLogger.logAdmin("HandleCommand", "run","Writing back value"+value+" to the client socket");
				socket.writeObject(value);
			}
			//for putting a key
			else if(cmd.equals("put")){
				String key= (String)argList.get(1);				
				Object value=(Object)argList.get(2);				
				Integer mapNumber=(Integer)argList.get(3);			
				MapType mapType=MapType.values()[mapNumber];
				DSLogger.logAdmin("HandleCommand", "run","Entered put on node "+itself.getIdentifier());
				
				DSLogger.logAdmin("HandleCommand", "run","In local key-value store, putting up key:"+key+" and value:"+value);
				KVStoreOperation operation=new KVStoreOperation(key,value, KVStoreOperation.OperationType.PUT,mapType);
				operationQueue.put(operation);	
				DSLogger.logFE(this.getClass().getName(), "run","Sending ack to  "+socket.getSocket().getRemoteSocketAddress());
				//Sending ack back to asynch executor
				socket.writeObject("ack");
			}
			//for updating a key
			else if(cmd.equals("update")){
				String key= (String)argList.get(1);				
				Object value=(Object)argList.get(2);
				Integer mapNumber=(Integer)argList.get(3);			
				MapType mapType=MapType.values()[mapNumber];
				DSLogger.logAdmin("HandleCommand", "run","Entered update operation on node "+itself.getIdentifier());
				DSLogger.logAdmin("HandleCommand", "run","Updating in local key-value store for key:"+key+" and new value:"+value);
				KVStoreOperation operation=new KVStoreOperation(key,value, KVStoreOperation.OperationType.UPDATE,mapType);
				operationQueue.put(operation);	
				DSLogger.logFE(this.getClass().getName(), "run","Sending ack to  "+socket.getSocket().getRemoteSocketAddress());
				//Sending ack back to asynch executor
				socket.writeObject("ack");
			}
			else if(cmd.equals("delete")){
				String key= (String)argList.get(1);	
				Integer hashedKey=Hash.doHash(key.toString());//Use hashedKey only for determining the node which needs to delete the actual key-value.
				Integer mapNumber=(Integer)argList.get(3);			
				MapType mapType=MapType.values()[mapNumber];
				DSLogger.logAdmin("HandleCommand", "run","Entered delete operation on node "+itself.getIdentifier());
				DSLogger.logAdmin("HandleCommand", "run","Deleting object in local key store for key:"+key);
				KVStoreOperation operation=new KVStoreOperation(key, KVStoreOperation.OperationType.DELETE,mapType);
				operationQueue.put(operation);	
				DSLogger.logFE(this.getClass().getName(), "run","Sending ack to  "+socket.getSocket().getRemoteSocketAddress());
				//Sending ack back to asynch executor
				socket.writeObject("ack");
			}
			// tell this node that new node has come up before this node
			// so partition its key space and send the required keys to that node
			else if(cmd.equals("partition")){
				Member newMember = (Member)argList.get(1);
				KVStoreOperation operation=new KVStoreOperation(newMember.getIdentifier(), KVStoreOperation.OperationType.PARTITION,KVStoreOperation.MapType.PRIMARY);
				operationQueue.put(operation);
				Object partitionedMap = resultQueue.take();
				DSocket sendMerge = new DSocket(newMember.getAddress().getHostAddress(), newMember.getPort());
				List<Object>  objList= new ArrayList<Object>();
				objList.add("merge");
				objList.add(partitionedMap);
				objList.add(Integer.parseInt("0"));//Merge with primary map
				sendMerge.writeObjectList(objList);			
					
				//Consuming the acknowledgment send by merging node
				sendMerge.readObject();
				// Send ack on socket object established with FE.
				String ack="ack";
				socket.writeObject(ack);
			}
			// tells this node to merge the received key list to its key space
			else if(cmd.equals("merge")){
				HashMap<String, Object> recievedKeys = (HashMap<String, Object>)argList.get(1);
				Integer mapNumber=(Integer)argList.get(2);				
				MapType mapType=MapType.values()[mapNumber];  //map to be merged
				DSLogger.logAdmin("HandleCommand", "run","In merge request");
				KVStoreOperation operation=new KVStoreOperation(recievedKeys, KVStoreOperation.OperationType.MERGE,mapType);
				
				operationQueue.put(operation);
				DSLogger.logAdmin("HandleCommand", "run","In merge request waiting for ack");
				String ack = (String)resultQueue.take();
				DSLogger.logAdmin("HandleCommand", "run","In merge request got "+ack);
				socket.writeObject(ack);
			}
			else if(cmd.equals("sendKeys")){
				Integer mapNumber=(Integer)argList.get(1);				
				MapType mapType=MapType.values()[mapNumber];  //map to be sent
				Member newMember = (Member)argList.get(2);				
				Integer destMapNumber=(Integer)argList.get(3);	 //destination map number to be sent to merge command			
				KVStoreOperation operation=new KVStoreOperation("-1",KVStoreOperation.OperationType.SEND_KEYS,mapType);
				operationQueue.put(operation);
				
				Object mapToBeSent = resultQueue.take();
				DSocket sendMerge = new DSocket(newMember.getAddress().getHostAddress(), newMember.getPort());
				List<Object>  objList= new ArrayList<Object>();
				objList.add("merge");
				objList.add(mapToBeSent);
				objList.add(destMapNumber);
				sendMerge.writeObjectList(objList);			
					
				//Consuming the acknowledgment send by merging node
				sendMerge.readObject();
				// Send ack on socket object established with FE.
				String ack="ack";
				socket.writeObject(ack);
			}
			else if(cmd.equals("newNodeStabilization")){ //required for next to next node of new node
				Member newMember = (Member)argList.get(1);
				Integer newMemberId = Integer.parseInt(newMember.getIdentifier());
				Member nextToNewMember = (Member)argList.get(2);
				Integer nextToNewMemberId = Integer.parseInt(nextToNewMember.getIdentifier());
				KVStoreOperation operation=new KVStoreOperation(newMemberId.toString(), nextToNewMemberId.toString(),KVStoreOperation.OperationType.SPLIT_BACKUP_LOCAL,KVStoreOperation.MapType.BACKUP1);
				operationQueue.put(operation);
				
				String ack=(String)resultQueue.take();
				socket.writeObject(ack);
				
			}
			else if(cmd.equals("splitBackup2")){ //required for 3 nodes ahead of  new node
				Member newMember = (Member)argList.get(1);
				Integer newMemberId = Integer.parseInt(newMember.getIdentifier());
				Member nextToNewMember = (Member)argList.get(2);
				Integer nextToNewMemberId = Integer.parseInt(nextToNewMember.getIdentifier());
				KVStoreOperation operation=new KVStoreOperation(newMemberId.toString(), nextToNewMemberId.toString(),KVStoreOperation.OperationType.SPLIT_BACKUP_TWO,KVStoreOperation.MapType.BACKUP2);
				operationQueue.put(operation);
				
				String ack=(String)resultQueue.take();
				socket.writeObject(ack);
				
			}
			
			//for showing the key space on console
			else if(cmd.equals("display")){
				DSLogger.logAdmin("HandleCommand", "run","Retrieving local hashmap for display");
				KVStoreOperation operation=new KVStoreOperation("-1", KVStoreOperation.OperationType.DISPLAY,KVStoreOperation.MapType.PRIMARY);
				operationQueue.put(operation);
				Object value=resultQueue.take();
				DSLogger.logAdmin("HandleCommand", "run", "Display Map received in Handle Command");
				List<Map> map=(List<Map>)value;
				//map.put(-1, itself.getIdentifier()); //This key is only used for display purpose at client
				DSLogger.logAdmin("HandleCommand", "run", "Sending map to node client of size "+map.size());
				socket.writeObject(map);
			}
			
		}catch(Exception e){
			e.printStackTrace();
		}finally{
			try {
				socket.close();
				DSLogger.logAdmin("HandleCommand", "run", "Exiting...");
			} catch (IOException e) {
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
