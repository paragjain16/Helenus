package org.ds.server;

import java.util.LinkedList;

public class MostRecentOperations<E> extends LinkedList<E>{
	 static final int DISPLAY_LIMIT=10; 
	
	 @Override
	    public boolean add(E o) {
	        boolean additionPerformed = super.add(o);
	        while (additionPerformed && size() > DISPLAY_LIMIT) {
	           super.remove();  //As long as size is greater than 10, remove from head of list.
	        }
	        return additionPerformed;
	    }
}
