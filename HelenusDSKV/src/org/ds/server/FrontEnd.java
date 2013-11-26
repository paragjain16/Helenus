package org.ds.server;

import java.io.IOException;
import java.net.ServerSocket;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.TreeMap;

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
	
	//Shares the alive and dead member list of the contact server
	public FrontEnd(HashMap<String, Member> aliveMembers, HashMap<String, Member> deadMembers, Object lock){
		this.aliveMembers = aliveMembers;
		this.deadMembers = deadMembers;
		this.lock = lock;
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
						socket = new DSocket(serverSocket.accept());
						List<Object> argList = (ArrayList<Object>)socket.readObject();
						String cmd=(String) argList.get(0);
						synchronized (lock) {
							sortedAliveMembers = this.constructSortedMap(aliveMembers);
							DSLogger.logAdmin(this.getClass().getName(), "run","Sorted Map :"+sortedAliveMembers);
						}
						DSLogger.logFE(this.getClass().getName(), "run","Received command from client: "+cmd);
						/*
						 * can receive different commands like read, write, update etc*/
						if(cmd.equals("joinMe")){
							Member newMember = (Member)argList.get(1); 
							int newMemberHashId = Integer.parseInt(newMember.getIdentifier());
							DSocket joinRequest = new DSocket(newMember.getAddress().getHostAddress(), newMember.getPort());
/*							List<Object> newCmd = new ArrayList<Object>();
							newCmd.add("joinMe");
							newCmd.add(newMember);*/
							joinRequest.writeObjectList(argList);
							//TODO wait for ack
							argList.clear();
							argList.add(0, "sendKeys"); //command
							argList.add(1, newMember); //To
							argList.add(2, "Primary"); //keyspace to send
							Integer nextNodeId = sortedAliveMembers.higherKey(newMemberHashId)==null?sortedAliveMembers.firstKey():sortedAliveMembers.higherKey(newMemberHashId);
							DSLogger.logFE(this.getClass().getName(), "run","Asking node "+nextNodeId+" to send its primay key space to "+newMemberHashId);
							DSocket sendMerge = new DSocket(aliveMembers.get(nextNodeId+"").getAddress().getHostAddress(), aliveMembers.get(nextNodeId+"").getPort());
							sendMerge.writeObjectList(argList);
							//TODO wait for ack
							
							//Step 2
							argList.clear();
							argList.add(0, "sendKeys"); //command
							argList.add(1, newMember); //To
							argList.add(2, "Backup1"); //keyspace to send
							DSLogger.logFE(this.getClass().getName(), "run","Asking node "+nextNodeId+" to send its backup1 key space to "+newMemberHashId);
							sendMerge.writeObjectList(argList);
							//TODO wait for ack
							
							//Step 3
							Integer nextToNextNodeId = sortedAliveMembers.higherKey(nextNodeId)==null?sortedAliveMembers.firstKey():sortedAliveMembers.higherKey(nextNodeId);
							if(nextToNextNodeId != newMemberHashId){
								DSLogger.logFE(this.getClass().getName(), "run","Asking node "+nextToNextNodeId+" to stabilize as per "+newMemberHashId);
								argList.clear();
								argList.add(0, "newNodeStabilization"); //command
								argList.add(1, newMember); //To
								sendMerge = new DSocket(aliveMembers.get(nextToNextNodeId+"").getAddress().getHostAddress(), aliveMembers.get(nextToNextNodeId+"").getPort());
								sendMerge.writeObjectList(argList);
								//TODO wait for ack
							}
							
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
