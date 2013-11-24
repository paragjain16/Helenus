package org.ds.server;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.ds.logger.DSLogger;
import org.ds.member.Member;
/**
 * 
 * @author pjain11,mallapu2
 * Class responsible for receiver thread which updates the local membership list maintained in the node 
 * with the membership list received over the network.
 */
public class Receiver implements Runnable {

	private Map<String, Member> aliveMap;
	private Map<String, Member> deadMap;
	private DatagramSocket nodeSocket;
	private Object nodeLockObject;

	public Receiver(Map<String, Member> aliveMap, Map<String, Member> deadMap,
			DatagramSocket nodeSocket, Object nodeLockObject) {
		super();
		this.nodeSocket = nodeSocket;
		this.aliveMap = aliveMap;
		this.deadMap = deadMap;
		this.nodeLockObject = nodeLockObject;
	}

	@Override
	public void run() {
		DSLogger.log("Receiver", "run", "Entered Run");
		byte[] msgBuffer = new byte[2048];
		DatagramPacket msgPacket = new DatagramPacket(msgBuffer, msgBuffer.length);
		while (true) {

			try {
				DSLogger.log("Receiver", "run", "Waiting to receive UDP data");

				nodeSocket.receive(msgPacket);

				ByteArrayInputStream bis = new ByteArrayInputStream(msgPacket.getData());
				ObjectInputStream ois = new ObjectInputStream(bis);

				Object memberList = ois.readObject();

				if (memberList instanceof List<?>) {
					List<Member> memList = (List<Member>) memberList;
					DSLogger.log("Receiver", "run", "Received member list of size: " + memList.size());
					for (Member mem : memList) {
						DSLogger.log("Receiver", "run", "Received member:  "+ mem.getIdentifier() + " with heartbeat:"+ mem.getHeartBeat());
					}
					synchronized (nodeLockObject) {
						DSLogger.log("Receiver", "run","Lock Acquired by receiver");
						for (Member member : memList) { // Iterate over the
														// member
														// list
														// received over the
														// network

							String memAddress = member.getIdentifier();

							/*
							 * If its a contact machine address then update its
							 * id as this node has just started and only knows
							 * the ip address of contact machine
							 */
						/*	if (aliveMap.containsKey("#"
									+ member.getAddress().getHostAddress())) {
								aliveMap.remove("#"
										+ member.getAddress().getHostAddress());
								aliveMap.put(memAddress, member);
								DSLogger.report(memAddress,
										"New member added to the list");
								continue;
							}*/

							if (aliveMap.containsKey(memAddress)) { // Found
																			// a
																			// match
								DSLogger.log("Receiver", "run",
										"Found match in alive map for: "
												+ memAddress);
								Member localMemberObj = aliveMap.get(memAddress);
								// This member is leaving the network. Remove it
								// from alive Map and add it to dead map
								if (member.getHeartBeat() == -1) {
									aliveMap.remove(memAddress);
									deadMap.put(memAddress, member);
									DSLogger.report(memAddress," This machine is voluntarily leaving the network");
									continue;
								}
								if (localMemberObj.getHeartBeat() >= member.getHeartBeat()) {
									// Ignore, as the local member's heartbeat
									// is
									// greater than incoming member's heartbeat.

								} else { // Update the local member's heartbeat
											// with
											// the
											// received heartbeat.
									Member localObj = aliveMap.get(memAddress);
									localObj.setHeartBeat(member.getHeartBeat());
									localObj.setTimeStamp(new Date().getTime());
								}
							}

							// else if the member was not found in the alive
							// map,
							// either
							// it is a new member or an old update of an already
							// dead
							// member
							else {
								if (deadMap.containsKey(memAddress)) {
									// DSLogger.log("Receiver", "run",
									// "Found match in dead map for: "+memAddress);

									// Check if the local member present in the
									// dead
									// Map
									// has a heartbeat greater than the
									// heartbeat of
									// the
									// received member.
									Member localMemberObj = deadMap.get(memAddress);
									if (localMemberObj.getHeartBeat() >= member.getHeartBeat()) {
										// Ignore, as the local member's
										// heartbeat
										// is
										// greater than incoming member's
										// heartbeat.

									} else { // False positive!!
												// remove from dead member list
												// and add
												// it
												// to alive member list.
										DSLogger.report(memAddress, " False positive detected for this member");
										Member localObj = deadMap.get(memAddress);
										localObj.setHeartBeat(member.getHeartBeat());
										localObj.setTimeStamp(new Date().getTime());
										deadMap.remove(memAddress);
										aliveMap.put(memAddress, localObj);

									}
								}

								else { // A new member is being added to the
										// list. (Might include reincarnations
										// of previously dead members
									if (!memAddress.startsWith("#")) {
										DSLogger.log("Receiver", "run","New member added with "+ memAddress);
										DSLogger.report(memAddress,"New member added to the list");
										aliveMap.put(memAddress, member);
									}
								}
							}
						}
					}
					DSLogger.log("Receiver", "run","********Alive members after update*******");
					printMemberMap(aliveMap);
					DSLogger.log("Receiver", "run","**********Dead members after update*******");
					printMemberMap(deadMap);
					DSLogger.log("Receiver", "run", "Lock released by receiver");
				} else {
					nodeSocket.close();
					DSLogger.log("Receiver", "run", "Shutting down reciver");
					break;
				}

			} catch (IOException e) {
				DSLogger.log("Receiver", "run", e.getMessage());
				e.printStackTrace();
			} catch (ClassNotFoundException e) {
				DSLogger.log("Receiver", "run", e.getMessage());
				e.printStackTrace();
			}
		}

	}

	public DatagramSocket getNodeSocket() {
		return nodeSocket;
	}

	public void setNodeSocket(DatagramSocket nodeSocket) {
		this.nodeSocket = nodeSocket;
	}

	/* Method to shutdown this thread when the node is leaving the network*/
	public void shutDown() {
		String close = new String("close");

		try {
			ByteArrayOutputStream baos = new ByteArrayOutputStream();
			ObjectOutputStream oos = new ObjectOutputStream(baos);
			oos.writeObject(close);
			DatagramPacket p = new DatagramPacket(baos.toByteArray(),baos.toByteArray().length, InetAddress.getLocalHost(), 3456);
			nodeSocket.send(p);
		} catch (UnknownHostException e) {
			DSLogger.log("Receiver", "run", e.getMessage());
			e.printStackTrace();
		} catch (IOException e) {
			DSLogger.log("Receiver", "run", e.getMessage());
			e.printStackTrace();
		}
	}

	/* Print Gossip method */
	public void printMemberMap(Map<String, Member> memberMap) {

		Set<String> keys = memberMap.keySet();
		Member aMember;
		for (String key : keys) {
			aMember = memberMap.get(key);
			DSLogger.log("Receiver", "printMemberMap ", aMember.getIdentifier());
		}
	}
}
