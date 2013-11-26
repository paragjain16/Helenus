package org.ds.server;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.BlockingQueue;

import org.ds.hash.Hash;
import org.ds.logger.DSLogger;
import org.ds.member.Member;

/**
 * @author pjain11, mallapu2
 * A thread of this class is constantly running as soon as the Node is running.
 * This class is responsible for maintaining the local key-value hash map
 * and it responds to operations requested by HandleCommand class by taking the arguments from a BlockingQueue known as 'operationQueue'
 * and puts back the result in another BlockingQueue known as 'resultQueue'
 *
 */
public class KeyValueStore implements Runnable {
	BlockingQueue<KVStoreOperation> operationQueue = null;
	BlockingQueue<Object> resultQueue = null;
	Member itself;
	BlockingQueue<KVStoreOperation> oper = null;
	private Map<String, Object> chosenKeyValueStoreMap =null;
	private Map<String, Object> primaryKeyValueStoreMap = new HashMap<String, Object>();
	private Map<String, Object> firstBackupKeyValueStore = new HashMap<String, Object>();
	private Map<String, Object> secondBackupKeyValueStore = new HashMap<String, Object>();
	
	public KeyValueStore(BlockingQueue<KVStoreOperation> operationQueue, BlockingQueue<Object> resultQueue, Member itself) {
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
		//Select a keystore to operate on based on the hash of the key.
		DSLogger.logAdmin("KeyValueStore", "performOperation", "Entered performOperation");
		DSLogger.logAdmin("KeyValueStore", "performOperation", chosenKeyValueStoreMap.toString());
		Object retValue = null;
		switch (oper.getOperType()) {
		case GET:
			retValue = chosenKeyValueStoreMap.get(oper.getKey());
			DSLogger.logAdmin("KeyValueStore", "performOperation", "got value:"
					+ retValue);
			try {
				if(retValue==null){ //Key Not found
					retValue="!#KEYNOTFOUND#!";	
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
			Integer minNodeKey = Hash.doHash(oper.getKey());
			Integer maxNodeKey = Integer.parseInt(itself.getIdentifier());
			DSLogger.logAdmin("KeyValueStore", "performOperation",
					"Partitioning key value store in range :" + minNodeKey+" - "+maxNodeKey);
			Map<String, Object> newMap = new HashMap<String, Object>();
			Set<String> origKeys = new HashSet<String>(chosenKeyValueStoreMap.keySet());
			DSLogger.logAdmin("KeyValueStore", "performOperation","Original keyset of size:" + origKeys.size());
			//Collections.sort(new ArrayList<Integer>(origKeys));
			Integer hashedKey=null;
			for (String key : origKeys) {
				hashedKey=Hash.doHash(key.toString());//Use hashedKey for partitioning the keyset. 
				if(minNodeKey > maxNodeKey){
					if( (hashedKey > minNodeKey && hashedKey<= 255) 
							|| (hashedKey>=0 && hashedKey <=maxNodeKey)){
						if(minNodeKey==0 && hashedKey==0){ // Special handling for node 0 and key 0.
							Object value = chosenKeyValueStoreMap.get(key);
							chosenKeyValueStoreMap.remove(key);
							newMap.put(key, value);
						}else{
							continue;
						}
					}else{
						Object value = chosenKeyValueStoreMap.get(key);
						chosenKeyValueStoreMap.remove(key);
						newMap.put(key, value);
					}
				}else{
					if(hashedKey > minNodeKey && hashedKey <= maxNodeKey){
						continue;
					}else{
						Object value = chosenKeyValueStoreMap.get(key);
						chosenKeyValueStoreMap.remove(key);
						newMap.put(key, value);
					}
				}				
			}
			try {
				DSLogger.logAdmin("KeyValueStore", "performOperation","Putting hashmap of size:" + newMap.size());
				resultQueue.put(newMap);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
			break;
		case DISPLAY:
			DSLogger.logAdmin("KeyValueStore", "performOperation",
					"Display local hashmap of size:" + chosenKeyValueStoreMap.size());
			try {
				Map<Integer,Object> displayMap=new HashMap<Integer,Object>();
			//	displayMap.putAll(chosenKeyValueStoreMap);
				resultQueue.put(displayMap);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
			break;

		case MERGE:
			DSLogger.logAdmin("KeyValueStore", "performOperation",
					"Merging map received from previous node");
			Map<String,Object> mapToBeMerged=oper.getMapToBeMerged();
			chosenKeyValueStoreMap.putAll(mapToBeMerged);
			try {
				resultQueue.put("ack");
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			break;
			
		case LEAVE:
			try{
			DSLogger.logAdmin("KeyValueStore", "performOperation",
					"Leave command received");
			Map<String,Object> mapToBeSent=new HashMap<String,Object>();
			mapToBeSent.putAll(chosenKeyValueStoreMap);
			resultQueue.put(mapToBeSent);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}

		break;
//		return retValue;
	}
 }
}