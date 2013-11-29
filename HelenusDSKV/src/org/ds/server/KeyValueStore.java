package org.ds.server;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.prefs.BackingStoreException;

import org.ds.hash.Hash;
import org.ds.logger.DSLogger;
import org.ds.member.Member;
import org.ds.server.KVStoreOperation.MapType;

/**
 * @author pjain11, mallapu2 A thread of this class is constantly running as
 *         soon as the Node is running. This class is responsible for
 *         maintaining the local key-value hash map and it responds to
 *         operations requested by HandleCommand class by taking the arguments
 *         from a BlockingQueue known as 'operationQueue' and puts back the
 *         result in another BlockingQueue known as 'resultQueue'
 * 
 */
public class KeyValueStore implements Runnable {
	BlockingQueue<KVStoreOperation> operationQueue = null;
	BlockingQueue<Object> resultQueue = null;
	Member itself;
	BlockingQueue<KVStoreOperation> oper = null;
	private Map<String, Object> chosenKeyValueStoreMap = null;
	private Map<String, Object> primaryKeyValueStoreMap = new HashMap<String, Object>();
	private Map<String, Object> firstBackupKeyValueStore = new HashMap<String, Object>();
	private Map<String, Object> secondBackupKeyValueStore = new HashMap<String, Object>();

	public KeyValueStore(BlockingQueue<KVStoreOperation> operationQueue,
			BlockingQueue<Object> resultQueue, Member itself) {
		super();
		this.operationQueue = operationQueue;
		this.resultQueue = resultQueue;
		this.itself = itself;
	}

	@Override
	public void run() {
		DSLogger.logAdmin("KeyValueStore", "run", "Entered Run");
		KVStoreOperation oper = null;
		while (true) {
			try {
				oper = operationQueue.take();
				performOperation(oper); // TO-DO: Enhance to
										// put operation id
										// to enable
										// multiple threads
										// to get
										// concurrently.
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}

	}

	private void performOperation(KVStoreOperation oper) {
		// Select a keystore to operate on based on the hash of the key.
		MapType chosenType = oper.getMapType();
		switch (chosenType) {
		case PRIMARY:
			chosenKeyValueStoreMap = primaryKeyValueStoreMap;
			break;
		case BACKUP1:
			chosenKeyValueStoreMap = firstBackupKeyValueStore;
			break;
		case BACKUP2:
			chosenKeyValueStoreMap = secondBackupKeyValueStore;
			break;
		}
		DSLogger.logAdmin("KeyValueStore", "performOperation",
				"Entered performOperation to operate on the map:"+chosenType);
		// DSLogger.logAdmin("KeyValueStore", "performOperation",
		// chosenKeyValueStoreMap.toString());
		Object retValue = null;
		switch (oper.getOperType()) {
		case GET:
			retValue = chosenKeyValueStoreMap.get(oper.getKey());
			DSLogger.logAdmin("KeyValueStore", "performOperation", "got value:"
					+ retValue);
			try {
				if (retValue == null) { // Key Not found
					retValue = "!#KEYNOTFOUND#!";
				}
				resultQueue.put(retValue);
			} catch (InterruptedException e1) {
				e1.printStackTrace();
			}
			break;
		case PUT:
			DSLogger.logAdmin(
					"KeyValueStore",
					"performOperation",
					"putting key:" + oper.getKey() + "and value:"
							+ oper.getValue());
			chosenKeyValueStoreMap.put(oper.getKey(), oper.getValue());
			break;

		case UPDATE:
			DSLogger.logAdmin("KeyValueStore", "performOperation",
					"updating for  key:" + oper.getKey() + "and new value:"
							+ oper.getValue());
			chosenKeyValueStoreMap.put(oper.getKey(), oper.getValue());
			break;

		case DELETE:
			DSLogger.logAdmin("KeyValueStore", "performOperation",
					"Deleting object for  key:" + oper.getKey());
			chosenKeyValueStoreMap.remove(oper.getKey());
			break;

		case PARTITION:
			DSLogger.logAdmin("KeyValueStore", "performOperation",
					"Partitioning key value store until key:" + oper.getKey());
			// Sort the keyvalue store and return the set until the key of the
			// new node.
			chosenKeyValueStoreMap = primaryKeyValueStoreMap; // Always
																// partition the
																// primary map.
			Integer minNodeKey = Integer.parseInt(oper.getKey());
			Integer maxNodeKey = Integer.parseInt(itself.getIdentifier());
			DSLogger.logAdmin("KeyValueStore", "performOperation",
					"Partitioning key value store in range :" + minNodeKey
							+ " - " + maxNodeKey);
			Map<String, Object> newMap = new HashMap<String, Object>();
			Set<String> origKeys = new HashSet<String>(
					chosenKeyValueStoreMap.keySet());
			DSLogger.logAdmin("KeyValueStore", "performOperation",
					"Original keyset of size:" + origKeys.size()+"  "+chosenKeyValueStoreMap);
			// Collections.sort(new ArrayList<Integer>(origKeys));
			Integer hashedKey = null;
			for (String key : origKeys) {
				hashedKey = Hash.doHash(key.toString());// Use hashedKey for
														// partitioning the
														// keyset.
				if (minNodeKey > maxNodeKey) {
					if ((hashedKey > minNodeKey && hashedKey <= 255)
							|| (hashedKey >= 0 && hashedKey <= maxNodeKey)) {
						if (minNodeKey == 0 && hashedKey == 0) { // Special
																	// handling
																	// for node
																	// 0 and key
																	// 0.
							Object value = chosenKeyValueStoreMap.get(key);
							chosenKeyValueStoreMap.remove(key);
							newMap.put(key, value);
						} else {
							continue;
						}
					} else {
						Object value = chosenKeyValueStoreMap.get(key);
						chosenKeyValueStoreMap.remove(key);
						newMap.put(key, value);
					}
				} else {
					if (hashedKey > minNodeKey && hashedKey <= maxNodeKey) {
						continue;
					} else {
						Object value = chosenKeyValueStoreMap.get(key);
						chosenKeyValueStoreMap.remove(key);
						newMap.put(key, value);
					}
				}
			}
			// Copy backup1 to backup2 (local)
			secondBackupKeyValueStore = firstBackupKeyValueStore;
			// Copy the partitioned key space to backup1
			firstBackupKeyValueStore = newMap;
			try {
				DSLogger.logAdmin("KeyValueStore", "performOperation",
						"Putting new hashmap of size:" + newMap.size()+" with map Values: "+newMap);
				resultQueue.put(newMap);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
			break;
		case SEND_KEYS:
			DSLogger.logFE("KeyValueStore", "performOperation",
					"In Send_Keys, sending map:" + oper.getMapType());
			Map<String, Object> mapSent = new HashMap<String, Object>();		
			mapSent.putAll(chosenKeyValueStoreMap);
			try {
				resultQueue.put(mapSent);
			} catch (InterruptedException e1) {
				e1.printStackTrace();
			}
			break;
		case SPLIT_BACKUP_LOCAL:
			DSLogger.logFE("KeyValueStore", "performOperation",
					"Splitting backup1 value store until key:" + oper.getKey());
			minNodeKey = Integer.parseInt(oper.getKey());
			maxNodeKey = Integer.parseInt(((String) (oper.getValue())));
			DSLogger.logAdmin("KeyValueStore", "performOperation",
					"Partitioning key value store in range :" + minNodeKey
							+ " - " + maxNodeKey);
			Map<String, Object> splitMap = new HashMap<String, Object>();
			chosenKeyValueStoreMap = firstBackupKeyValueStore;// Split the
																// backup1
																// keyValue
																// store.
			 origKeys = new HashSet<String>(
					chosenKeyValueStoreMap.keySet());
			DSLogger.logAdmin("KeyValueStore", "performOperation",
					"Original keyset of size:" + origKeys.size());
			// Collections.sort(new ArrayList<Integer>(origKeys));
			hashedKey = null;
			for (String key : origKeys) {
				hashedKey = Hash.doHash(key.toString());// Use hashedKey for
														// partitioning the
														// keyset.
				if (minNodeKey > maxNodeKey) {
					if ((hashedKey > minNodeKey && hashedKey <= 255)
							|| (hashedKey >= 0 && hashedKey <= maxNodeKey)) {
						if (minNodeKey == 0 && hashedKey == 0) { // Special
																	// handling
																	// for node
																	// 0 and key
																	// 0.
							Object value = chosenKeyValueStoreMap.get(key);
							chosenKeyValueStoreMap.remove(key);
							splitMap.put(key, value);
						} else {
							continue;
						}
					} else {
						Object value = chosenKeyValueStoreMap.get(key);
						chosenKeyValueStoreMap.remove(key);
						splitMap.put(key, value);
					}
				} else {
					if (hashedKey > minNodeKey && hashedKey <= maxNodeKey) {
						continue;
					} else {
						Object value = chosenKeyValueStoreMap.get(key);
						chosenKeyValueStoreMap.remove(key);
						splitMap.put(key, value);
					}
				}
			}
			secondBackupKeyValueStore=chosenKeyValueStoreMap;
			firstBackupKeyValueStore=splitMap;
			try {
				resultQueue.put("ack");
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
			break;
			
		case SPLIT_BACKUP_TWO: //For 3 nodes ahead of the  node which joined,split backup2 map according to new node joined.
			DSLogger.logFE("KeyValueStore", "performOperation",
					"Splitting backup2 value store until key:" + oper.getKey());
			minNodeKey = Integer.parseInt(oper.getKey()); //New node
			maxNodeKey = Integer.parseInt(((String) (oper.getValue()))); // Next to new node
			DSLogger.logAdmin("KeyValueStore", "performOperation",
					"Partitioning key value store in range :" + minNodeKey
							+ " - " + maxNodeKey);
			 splitMap = new HashMap<String, Object>();
			chosenKeyValueStoreMap = secondBackupKeyValueStore;// Split the
																// backup1
																// keyValue
																// store.
			 origKeys = new HashSet<String>(
					chosenKeyValueStoreMap.keySet());
			DSLogger.logAdmin("KeyValueStore", "performOperation",
					"Original keyset of size:" + origKeys.size());
			// Collections.sort(new ArrayList<Integer>(origKeys));
			hashedKey = null;
			for (String key : origKeys) {
				hashedKey = Hash.doHash(key.toString());// Use hashedKey for
														// partitioning the
														// keyset.
				if (minNodeKey > maxNodeKey) {
					if ((hashedKey > minNodeKey && hashedKey <= 255)
							|| (hashedKey >= 0 && hashedKey <= maxNodeKey)) {
						if (minNodeKey == 0 && hashedKey == 0) { // Special
																	// handling
																	// for node
																	// 0 and key
																	// 0.
							Object value = chosenKeyValueStoreMap.get(key);
							chosenKeyValueStoreMap.remove(key);
							splitMap.put(key, value);
						} else {
							continue;
						}
					} else {
						Object value = chosenKeyValueStoreMap.get(key);
						chosenKeyValueStoreMap.remove(key);
						splitMap.put(key, value);
					}
				} else {
					if (hashedKey > minNodeKey && hashedKey <= maxNodeKey) {
						continue;
					} else {
						Object value = chosenKeyValueStoreMap.get(key);
						chosenKeyValueStoreMap.remove(key);
						splitMap.put(key, value);
					}
				}
			}
			secondBackupKeyValueStore=chosenKeyValueStoreMap;			
			try {
				resultQueue.put("ack");
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
			break;
		case DISPLAY:
			DSLogger.logAdmin(
					"KeyValueStore",
					"performOperation",
					"Display local hashmap of size:"
							+ chosenKeyValueStoreMap.size());
			try {
				List<Map> mapList = new ArrayList<Map>();
				mapList.add(primaryKeyValueStoreMap);
				mapList.add(firstBackupKeyValueStore);
				mapList.add(secondBackupKeyValueStore);
				resultQueue.put(mapList);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
			break;

		case MERGE:
			DSLogger.logAdmin("KeyValueStore", "performOperation",
					"Merging map received from previous node");
			Map<String, Object> mapToBeMerged = oper.getMapToBeMerged();		
			chosenKeyValueStoreMap.putAll(mapToBeMerged);
			try {
				resultQueue.put("ack");
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			break;

		case LEAVE:
			try {
				DSLogger.logAdmin("KeyValueStore", "performOperation",
						"Leave command received");
				Map<String, Object> mapToBeSent = new HashMap<String, Object>();
				mapToBeSent.putAll(chosenKeyValueStoreMap);
				resultQueue.put(mapToBeSent);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}

			break;
		// return retValue;
		}
	}
}