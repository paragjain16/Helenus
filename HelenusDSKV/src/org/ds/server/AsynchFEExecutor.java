package org.ds.server;

/**
* @author { pjain11, mallapu2 } @ illinois.edu
* Used to carry out requests from Front End. 
* This threads will be created according to contact primary and replicas of a key 
* and instruct those nodes to carry out the required operation
* The front end will wait for as many as threads to finish as specified by consistency level
* */

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Random;

import org.ds.logger.DSLogger;
import org.ds.socket.DSocket;

public class AsynchFEExecutor implements Runnable{

	private HashMap<String, Object> resultMap;
	private Object lock;
	private DSocket socket;
	private String address;
	private int port;
	private List<Object> argList;
	
	public AsynchFEExecutor(HashMap<String, Object> resultMap, Object lock, String address, int port, List<Object> argList, int keySpace){
		this.resultMap = resultMap;
		this.lock = lock;
		this.address = address;
		this.port = port;
		this.argList = new ArrayList<Object>();
		this.argList.addAll(argList);
		this.argList.add(keySpace);
	}
	@Override
	public void run() {
		
		try {
			socket = new DSocket(address, port);
			DSLogger.logFE(this.getClass().getName(), "run","Contacting "+socket.getSocket().getRemoteSocketAddress());
			argList.remove(1);
			DSLogger.logFE(this.getClass().getName(), "run","Sending "+argList);
			socket.writeObjectList(argList);
			DSLogger.logFE(this.getClass().getName(), "run","Waiting for ack from  "+socket.getSocket().getRemoteSocketAddress());
			//wait for ack or result
			Object result = socket.readObject();
			DSLogger.logFE(this.getClass().getName(), "run","ack received from  "+socket.getSocket().getRemoteSocketAddress());
			synchronized (lock) {
				DSLogger.logFE(this.getClass().getName(), "run","Putting value in resultMap from  "+socket.getSocket().getRemoteSocketAddress()+" now the size is "+resultMap.size());
				resultMap.put(((InetSocketAddress)(socket.getSocket().getRemoteSocketAddress())).getAddress().getHostAddress(), result);
				DSLogger.logFE(this.getClass().getName(), "run","value put in resultMap of size  "+resultMap.size());
			}
		} catch (UnknownHostException e1) {
			resultMap.put(new Random().nextInt(1000)+"", "Consistency cannot be satisfied at this moment Please retry");
			e1.printStackTrace();
		} catch (IOException e1) {
			resultMap.put(new Random().nextInt(1000)+"", "Consistency cannot be satisfied at this moment Please retry");
			e1.printStackTrace();
		}
		
	}

}
