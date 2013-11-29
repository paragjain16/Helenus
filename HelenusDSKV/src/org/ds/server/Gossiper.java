package org.ds.server;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Random;
import java.util.Set;

import org.ds.logger.DSLogger;
import org.ds.member.Member;

/**
 * @author pjain11, mallapu2
 * A thread of this class is invoked every 500 ms 
 * this thread randomly chooses a member from 
 * the member list and gossips its member list 
 *
 */
public class Gossiper implements Runnable{
	private HashMap<String, Member> aliveMembers;
	private HashMap<String, Member> deadMembers;
	private Object lockUpdateMember;
	private Member itself;
	private ArrayList<Member> memberList;
	private DatagramSocket socket;
	private ArrayList<String> keysToRemove;
	
	public Gossiper(HashMap<String, Member> aliveMembers, HashMap<String, Member> deadMembers, Object lockUpdateMember, Member itself){
		this.aliveMembers = aliveMembers;
		this.deadMembers = deadMembers;
		this.lockUpdateMember = lockUpdateMember;
		this.itself = itself;
		//this.socket = socket;
	}
	
	public void run(){	
		DSLogger.log("Gossiper", "run", "Entered");
		
		//Acquire a lock on membership list
		synchronized(lockUpdateMember){
			
			DSLogger.log("Gossiper", "run", "Lock Acquired by gossiper");
			//Increment and update its own heartbeat and timestamp
			itself.incrementHB();
			itself.setTimeStamp(new Date().getTime());
			aliveMembers.put(itself.getIdentifier(), itself);
			DSLogger.log("Gossiper", "run", itself.getIdentifier()+" added to member list");
			/*//System.out.println(aliveMembers);
			for(Member mem :aliveMembers.values()){
				System.out.println(mem.getHeartBeat());
			}*/
			Set<String> keys = aliveMembers.keySet();
			Member aMember;
			keysToRemove = new ArrayList<String>();
			for(String key: keys){
				aMember =aliveMembers.get(key);
				//Check members for timeout
				if(aMember.checkTimeOut()){
					System.out.println(aMember.getIdentifier()+" timed out after "+aMember.getHeartBeat());
					keysToRemove.add(aMember.getIdentifier());
					deadMembers.put(aMember.getIdentifier(), aMember);
					DSLogger.report(aMember.getIdentifier()," added to dead list");
				}
			}
			for(String keytoRemove: keysToRemove){
				aliveMembers.remove(keytoRemove);
				DSLogger.log("Gossiper", "run", keytoRemove+" removed from alive list");
			}
			DSLogger.log("Gossiper", "run", "Alive and dead member list updated");
			memberList = new ArrayList<Member>(aliveMembers.values());
		
			DSLogger.log("Gossiper", "run", "Lock released by gossiper");
			
			ByteArrayOutputStream baos = new ByteArrayOutputStream();
			ObjectOutputStream oos = null;
			try {
				socket = new DatagramSocket();
				oos = new ObjectOutputStream(baos);
				oos.writeObject(memberList);
				DSLogger.log("Gossiper", "run", "Sending Membership list by gossiper"+memberList);
				byte[] buf = baos.toByteArray();
				/* Using expression 2 seconds  = Log N base k where N is no of machines and k is no of machines to gossip
				 * As out timeout time is 3 seconds and 5 seconds is the deadline in which the 
				 * gossip of new dead or alive member should spread thats why 
				 * taking using sqrt of no of machines multiplied by gossip time period in seconds */

				int noOfMachinestoGossip = (int)Math.floor(Math.sqrt(memberList.size()*0.5));
				
				if(noOfMachinestoGossip < 1){ // Gossip to atleast one machine
					noOfMachinestoGossip=1;
				}
				DSLogger.log("Gossiper", "run", "No of members to gossip "+noOfMachinestoGossip);
				for(int i=0; i < noOfMachinestoGossip; i++ ){
					Member memberToGossip = chooseRandom();
					printGossip(memberToGossip);
					if(memberToGossip!=null){
						DatagramPacket packet = new DatagramPacket(buf, buf.length, memberToGossip.getAddress(), memberToGossip.getPort());
						socket.send(packet);
					}
				}
				DSLogger.log("Gossiper", "run", "Exiting gossiper");
			}catch (IOException e) {
				DSLogger.log("Gossiper", "run", e.getMessage());
				e.printStackTrace();
			}finally{
				socket.close();
			}
		}
	}
	//Choose a random member to gossip.. try 15 times if the chosen member is itself otherwise return null
	public Member chooseRandom(){
		//DSLogger.log("Gossiper", "chooseRandom", "Choosing a Random member");
		Random random = new Random();
		int tryAnother = 15;
		while(tryAnother-- >0){
			int i = random.nextInt(memberList.size());
			//DSLogger.log("Gossiper", "chooseRandom", "Random "+memberList.get(i).getIdentifier());
			if(!(memberList.get(i) == itself)){
				DSLogger.log("Gossiper", "chooseRandom", "Member "+memberList.get(i).getIdentifier()+" chosen to gossip");
				return memberList.get(i);
			}
		}
		DSLogger.log("Gossiper", "run", "No members to choose");
		return null;
		
	}
	
	
	/*Print Gossip method*/
	public void printGossip(Member mem){
		if(mem!=null){
			DSLogger.report("Gossiping to "+mem.getIdentifier(), "");
		}
		DSLogger.report("Alive Members ---------------------------- Local Time " + new Date(), "");
		//System.out.println("Alive Members ---------------------------- Local Time " + new Date());
		Set<String> keys = aliveMembers.keySet();
		Member aMember;
		for(String key: keys){
			aMember =aliveMembers.get(key);
			//System.out.println(aMember.getIdentifier()+" "+aMember.getHeartBeat()+" "+aMember.getTimeStamp());
			DSLogger.report(aMember.getIdentifier(), "");
		}
		DSLogger.report("Dead Members ----------------------------- Local Time "+ new Date(), "");
		//System.out.println("Dead Members ----------------------------- Local Time "+ new Date());
		keys = deadMembers.keySet();
		for(String key: keys){
			aMember =deadMembers.get(key);
			//System.out.println(aMember.getIdentifier()+" "+aMember.getHeartBeat()+" "+aMember.getTimeStamp());
			DSLogger.report(aMember.getIdentifier(), "");
		}
	}
}
