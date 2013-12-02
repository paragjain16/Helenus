package org.ds.member;

import java.io.Serializable;
import java.net.InetAddress;
import java.util.Date;

import org.ds.hash.Hash;
import org.ds.logger.DSLogger;

/**
 * @author pjain11, mallapu2
 * Represents a member having - 
 * identifier as - id#ipaddress where id passed from command line while starting the node
 * heartBeat - which is updated before gossiping
 * some other housekeeping variables and related methods
 */
public class Member implements Serializable{
	/**
	 * 
	 */
	private static final long serialVersionUID = 7087982295747726293L;
	private InetAddress address;
	private int heartBeat;
	private String identifier;
	private long timeStamp;
	private int port;

	public Member(InetAddress address, String id, int port){
		this.address = address;
		this.heartBeat = 0;
		this.identifier = Hash.doHash(id+"#"+address.getHostAddress())+"";
		DSLogger.logAdmin("Member", "Member", "New Member created with id "+identifier);
		this.timeStamp = new Date().getTime();
		this.port = port;
	}
	
	public Member(InetAddress address, int heartbeat,int port){ //For node leave purpose
		this.address = address;
		this.heartBeat = 0;
		this.timeStamp = new Date().getTime();
		this.port = port;
	}
	
	
	
	public int getPort() {
		return port;
	}

	public void setPort(int port) {
		this.port = port;
	}

	public long getTimeStamp() {
		return timeStamp;
	}

	public void setTimeStamp(long timeStamp) {
		this.timeStamp = timeStamp;
	}
	public void incrementHB(){
		heartBeat = heartBeat+1;
		//this.setHeartBeat(getHeartBeat()+1);
	}
	
	public InetAddress getAddress() {
		return address;
	}
	public void setAddress(InetAddress address) {
		this.address = address;
	}
	public int getHeartBeat() {
		return heartBeat;
	}
	public void setHeartBeat(int heartBeat) {
		this.heartBeat = heartBeat;
	}
	public String getIdentifier() {
		return identifier;
	}
	public void setIdentifier(String identifier) {
		this.identifier = identifier;
	}
	public boolean checkTimeOut(){
		if(new Date().getTime() - this.timeStamp > 8000){
			return true;
		}
		return false;
	}
	
}
