package org.ds.server;

import java.util.Map;

/**
 * @author pjain11, mallapu2
 * This class is used to represent an operation which can be performed on key value store.
 *
 */
public class KVStoreOperation {

    private Integer key;
    private Object value;
    private OperationType operType;
    private Map<Integer,Object> mapToBeMerged;
	

	public KVStoreOperation(Integer key, OperationType operType) {
		super();
		this.key = key;
		this.operType = operType;
	}

	public KVStoreOperation(Integer key, Object value, OperationType operType) {
		super();
		this.key = key;
		this.value = value;
		this.operType = operType;
	}
	
	//Used only in case of merge operation for the receiving node.
	public KVStoreOperation(Map<Integer,Object> mapToBeMerged, OperationType operType) {
		super();
		this.mapToBeMerged=mapToBeMerged;	
		this.operType = operType;
	}

	public enum OperationType{
		PUT,
		GET,
		UPDATE,
		DELETE,
		MERGE,
		PARTITION,
		LEAVE,
		DISPLAY
	}
	
	
	public Integer getKey() {
		return key;
	}

	public Object getValue() {
		return value;
	}

	public OperationType getOperType() {
		return operType;
	}

	public Map<Integer,Object> getMapToBeMerged() {
		return mapToBeMerged;
	}

	
}
