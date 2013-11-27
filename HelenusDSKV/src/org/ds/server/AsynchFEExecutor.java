package org.ds.server;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

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
		
		DSocket socket;
		try {
			socket = new DSocket(address, port);
			argList.remove(1);
			socket.writeObjectList(argList);
			//wait for ack or result
			Object result = socket.readObject();
			synchronized (lock) {
				resultMap.put(((InetSocketAddress)(socket.getSocket().getRemoteSocketAddress())).getAddress().getHostAddress(), result);
			}
		} catch (UnknownHostException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		} catch (IOException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		
	}

}
