package org.ds.server;

import java.io.IOException;
import java.net.ServerSocket;
import java.util.HashMap;

import org.ds.logger.DSLogger;
import org.ds.member.Member;
import org.ds.socket.DSocket;

/**
 * @author { pjain11, mallapu2 } @ illinois.edu
 * Will listen from requests from all clients
 * serve the request sequentially and it blocks till response for a request is received
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
	private ServerSocket socket;
	
	//Shares the alive and dead member list of the contact server
	public FrontEnd(HashMap<String, Member> aliveMembers, HashMap<String, Member> deadMembers){
		this.aliveMembers = aliveMembers;
		this.deadMembers = deadMembers;
		try {
			socket = new ServerSocket(4000);
		} catch (IOException e) {
			DSLogger.logFE("FrontEnd", "FrontEnd", e.getMessage());
			e.printStackTrace();
		}
	}
	
	@Override
	public void run() {
		
		
	}

}
