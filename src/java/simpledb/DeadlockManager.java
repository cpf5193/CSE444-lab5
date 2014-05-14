package simpledb;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.Queue;
import java.util.Set;


public class DeadlockManager {
	/** The waits-for graph representing the TransactionId's that transactions are 
	 * waiting for*/
	private HashMap<TransactionId, ArrayList<TransactionId>> waitsForGraph;
	
	/**
	 * Creates a new instance of a DeadlockManager with an empty waits-for graph
	 */
	public DeadlockManager() {
		this.waitsForGraph = new HashMap<TransactionId, ArrayList<TransactionId>>();
	}
	
	/**
	 * Adds a new dependency from tid1 to tid2
	 * @param tid1 the transaction that is waiting
	 * @param tid2 the transaction that tid2 is waiting for
	 * @throws TransactionAbortedException 
	 */
	public synchronized boolean addToGraph(TransactionId tid1, TransactionId tid2) 
			throws TransactionAbortedException {
		System.out.println("Trying to add dependency " + tid1 + "->" + tid2 + " to " + waitsForGraph);
		// don't allow an edge to be added twice or to itself
		if (tid1.equals(tid2) || (waitsForGraph.containsKey(tid1) && 
				waitsForGraph.get(tid1).equals(tid2))) {
			return true;
		} else if (wouldHaveCycle(tid1, tid2)) {
			// Adding this edge would produce a deadlock, signify abort by returning false
			System.out.println("Aborting: " + tid1);
			return false;
		} else {
			System.out.println("Adding dependency to graph: tid1=" + tid1 + ", tid2=" + tid2 + "\n");
			// Legal to add this dependency
			if(waitsForGraph.containsKey(tid1)) {
				ArrayList<TransactionId> dependsOnList = waitsForGraph.get(tid1);
				if(!dependsOnList.contains(tid2))
					dependsOnList.add(tid2);
				waitsForGraph.put(tid1, dependsOnList);
			} else {
				waitsForGraph.put(tid1, new ArrayList<TransactionId>(Arrays.asList(tid2)));
				System.out.println("waitsForGraph: " + waitsForGraph  + "\n");
			}
		}
		return true;
	}
	
	/**
	 * Removes a dependency between tid1 and tid2
	 * @param tid1 the transaction that was previously waiting
	 * @param tid2 the transaction that tid1 was previously waiting for
	 */
	public void removeFromGraph(TransactionId tid1, TransactionId tid2) {
		if (waitsForGraph.containsKey(tid1)) {
			ArrayList<TransactionId> dependsOnList = waitsForGraph.get(tid1);
			dependsOnList.remove(tid2);
			if(dependsOnList.size() == 0) {
				waitsForGraph.remove(tid1);
			} else {
				waitsForGraph.put(tid1, dependsOnList);
			}
		}
	}
	
	
	/**
	 * Removes all dependencies that depend on desttid
	 * @param desttid the dependency that is the destination of all dependencies to remove
	 */
	/* When we commit or abort a transaction, we will want to remove all dependencies from 
	 * transaction tid1 to desttid */
	public synchronized void removeAllDependenciesTo(TransactionId desttid) {
		// Make copy of keySet to avoid ConcurrentModificationException
		Set<TransactionId> keySetCopy = new HashSet<TransactionId>();
		keySetCopy.addAll(waitsForGraph.keySet());
		for(TransactionId tid : keySetCopy) {
			ArrayList<TransactionId> dependsOnList = new ArrayList<TransactionId>();
			dependsOnList.addAll(waitsForGraph.get(tid));
			//waitsForGraph.get(tid);
			
			if(waitsForGraph.get(tid).contains(desttid)) {
				dependsOnList.remove(desttid);
			}
			if(dependsOnList.size() == 0) {
				waitsForGraph.remove(tid);
			}
			else {
				waitsForGraph.put(tid, dependsOnList);
			}
		}
		if(waitsForGraph.containsKey(desttid)) {
			waitsForGraph.remove(desttid);
		}
		System.out.println("End of removeAllDependenciesTo");
	}
	
	
	/**
	 * Returns true if the dependency graph would have a cycle if we were to add
	 * a dependency from tid1 to tid2
	 * @param tid1 the hypothetical source TransactionId
	 * @param tid2 the hypothetical destination TransactionId
	 * @return whether the graph would have a deadlock if edge (tid1, tid2) were added
	 */
	/* Abstracts away the confusing parameters in opposite order, 
	 * depthFirstSearch does the work*/
	private synchronized boolean wouldHaveCycle(TransactionId tid1, TransactionId tid2) {
		return depthFirstSearch(tid2, tid1);
	}
	
	/* Returns true if a path is found from tid1 to tid2 
	 * Note that normal usage will be checking the opposite direction of the edge we
	 * want to add to the dependency graph */
	private synchronized boolean depthFirstSearch(TransactionId tid1, TransactionId tid2) {
		Queue<TransactionId> q = new LinkedList<TransactionId>();
		ArrayList<TransactionId> visitedtids = new ArrayList<TransactionId>();
		q.add(tid1);
		visitedtids.add(tid1);
//		System.out.println("\nInput to depthFirstSearch: \ntid1: " + tid1 + "\ntid2: " + tid2);
//		System.out.println("contents of waitsForGraph: " + waitsForGraph + "\n");
		while(!q.isEmpty()) {
			TransactionId tid = q.remove();
			if(tid.equals(tid2)) {
				// We've found a path, return true
				return true;
			}
			ArrayList<TransactionId> dependsOnList = waitsForGraph.get(tid1);
			// If there exist dependencies
			if(dependsOnList != null) {
				for(TransactionId t : dependsOnList) {
					// If we've already seen this TransactionId, skip; otherwise add to queue
					if(!visitedtids.contains(t)) {
						q.add(t);
						visitedtids.add(t);
					}
				}
			}
		}
		return false;
	}
	
	/**
	 * Returns true if tid is waiting for any other transactions
	 * @param tid the TransactionId to test dependencies for
	 */
	public boolean hasDependencies(TransactionId tid) {
		return waitsForGraph.containsKey(tid);
	}
	
	public String toString() {
		return waitsForGraph.toString();
	}
}
