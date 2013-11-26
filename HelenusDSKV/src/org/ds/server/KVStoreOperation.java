package org.ds.server;

import java.util.Map;

/**
 * @author pjain11, mallapu2 This class is used to represent an operation which
 *         can be performed on key value store.
 * 
 */
public class KVStoreOperation {

	private String key;
	private Object value;
	private OperationType operType;
	private Map<String, Object> mapToBeMerged;
	private MapType mapType;// indicates the map on which the operation should
							// be executed.

	public MapType getMapType() {
		return mapType;
	}

	

	public KVStoreOperation(String key, OperationType operType, MapType mapType) {
		super();
		this.key = key;
		this.operType = operType;
		this.mapType=mapType;
	}

	public KVStoreOperation(String key, Object value, OperationType operType, MapType mapType) {
		super();
		this.key = key;
		this.value = value;
		this.operType = operType;
		this.mapType=mapType;
	}

	// Used only in case of merge operation for the receiving node.
	public KVStoreOperation(Map<String, Object> mapToBeMerged,
			OperationType operType, MapType mapType) {
		super();
		this.mapToBeMerged = mapToBeMerged;
		this.operType = operType;
		this.mapType=mapType;
	}

	public enum OperationType {
		PUT, GET, UPDATE, DELETE, MERGE, PARTITION, LEAVE, DISPLAY
	}

	public enum MapType {
		PRIMARY, BACKUP1, BACKUP2
	}

	public String getKey() {
		return key;
	}

	public Object getValue() {
		return value;
	}

	public OperationType getOperType() {
		return operType;
	}

	public Map<String, Object> getMapToBeMerged() {
		return mapToBeMerged;
	}

}
