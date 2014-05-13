package simpledb;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.concurrent.ConcurrentHashMap;

public class LockManager {
	
	// Holds mappings from PageId's to TransactionId ArrayLists
	// Conceptually, tells which transactions hold shared locks on the page
	ConcurrentHashMap<PageId, ArrayList<TransactionId>> sharedLocks;
	
	// Holds mappings from PageId's to TransactionId's
	ConcurrentHashMap<PageId, TransactionId> exclusiveLocks;
	
	// Manager for the dependency graph
	DeadlockManager dependencies;
	
	// Determines whether we deal with deadlocks using a dependency graph or timeouts
	boolean DEPENDENCIES = true;
	
	// Constructor
	public LockManager(int maxPages) {
		this.sharedLocks = new ConcurrentHashMap<PageId, ArrayList<TransactionId>>();
		this.exclusiveLocks = new ConcurrentHashMap<PageId, TransactionId>();
		this.dependencies = new DeadlockManager();
	}
	
	// Releases all locks by tid on pid
	public synchronized void releaseLock(TransactionId tid, PageId pid) {
		if(sharedLocks.containsKey(pid) && 
				sharedLocks.get(pid).size() > 0){  // at least 1 read lock in use
			removeFromSharedLocks(tid, pid);
		}
		if(exclusiveLocks.containsKey(pid)){  // write lock is in use
			removeFromExclusiveLocks(tid, pid);
		}
	}
	
	// Updates state to reflect tid having a shared lock on pid
	private synchronized void addToSharedLocks(TransactionId tid, PageId pid) {
		ArrayList<TransactionId> temp;
		if(!sharedLocks.containsKey(pid)){
			sharedLocks.put(pid, new ArrayList<TransactionId>(Arrays.asList(tid)));
		} else {
			temp = sharedLocks.get(pid);
			if(!temp.contains(tid)){  // only add tid if not already in the list
				temp.add(tid);
			}
			sharedLocks.put(pid, temp);
		}
	}
	
	// Updates state to reflect tid having an exclusive lock on pid
	private synchronized void addToExclusiveLocks(TransactionId tid, PageId pid) {
		exclusiveLocks.put(pid, tid);
	}
	
	// Attempts to remove pid, tid from sharedLocks. If it doesn't exist, returns false.
	// If the pair existed and was removed, returns true
	private synchronized boolean removeFromSharedLocks(TransactionId tid, PageId pid){
		if(sharedLocks.containsKey(pid)){
			ArrayList<TransactionId> tids = sharedLocks.get(pid);
			if(tids.contains(tid)){
				int index = tids.indexOf(tid);
				tids.remove(index);
				return true;
			}
			sharedLocks.put(pid, tids);
			return false;
		}
		return false;
	}
	
	// Attempts to remove pid, tid from exclusiveLocks. If it doesn't exist,
	// returns false. If it exists and was removed, returns true
	private synchronized boolean removeFromExclusiveLocks(TransactionId tid,
													   PageId pid){
		if(exclusiveLocks.containsKey(pid) && 
				exclusiveLocks.get(pid) == tid){
				exclusiveLocks.remove(pid);
				return true;
		}
		return false;
	}
	
	
	// Releases all locks that this transaction holds
	public synchronized void releaseAllLocksForTxn(TransactionId tid) {
		// Remove all shared locks that this tid is holding
		for(PageId key : sharedLocks.keySet()) {
			ArrayList<TransactionId> tidList = sharedLocks.get(key);
			if(tidList.contains(tid)) {
				tidList.remove(tid);
				if(tidList.size() == 0) {
					// Removed the last shared lock, remove listing from lock map
					sharedLocks.remove(key);
				} else {
					sharedLocks.put(key, tidList);
				}
			}
		}
		
		// Remove exclusive lock that tid is holding if it is holding one
		for(PageId key : exclusiveLocks.keySet()) {
			if(exclusiveLocks.get(key) == tid){
				exclusiveLocks.remove(key);
			}
		}
		dependencies.removeAllDependenciesTo(tid);
	}
	
	// Returns an ArrayList of all PageId's of pages locked by transaction tid
	public ArrayList<PageId> getPagesLockedByTxn(TransactionId tid) {
		ArrayList<PageId> pageIdList = new ArrayList<PageId>();
		for(PageId key : sharedLocks.keySet()) {
			if(sharedLocks.get(key).contains(tid) && !pageIdList.contains(key)) {
				pageIdList.add(key);
			}
		}
		for(PageId key : exclusiveLocks.keySet()) {
			if(exclusiveLocks.get(key) == tid && !pageIdList.contains(key)) {
				pageIdList.add(key);
			}
		}
		return pageIdList;
	}
	
	// Returns true if an exclusive lock exists for pid
	private boolean hasExclusiveLock(PageId pid) {
		return exclusiveLocks.containsKey(pid);
	}

	// Returns true if a shared lock exists for pid
	private boolean hasSharedLock(PageId pid) {
		if(sharedLocks.containsKey(pid)){
			return sharedLocks.get(pid).size() > 0;
		}
		return false;
	}

	// Returns true if another transaction besides tid holds an exclusive lock on pid
	private boolean otherHasExclusiveLock(TransactionId tid, PageId pid){
		return hasExclusiveLock(pid) && !holdsExclusiveLock(tid, pid);
	}

	// Returns true if another transaction besides tid holds a shared lock on pid
	private boolean otherHasSharedLock(TransactionId tid, PageId pid){
		if(hasSharedLock(pid)){
			ArrayList<TransactionId> sharedLocksList = sharedLocks.get(pid);
			return !holdsSharedLock(tid, pid) || sharedLocksList.size() > 1;
		} return false;
	}

	// Returns true if tid holds an exclusive lock on pid
	private boolean holdsExclusiveLock(TransactionId tid, PageId pid) {
		if(exclusiveLocks.containsKey(pid)){
			return exclusiveLocks.get(pid) == tid;
		}
		return false;
	}

	// Returns true if tid holds a shared lock on pid
	private boolean holdsSharedLock(TransactionId tid, PageId pid) {
		if(sharedLocks.containsKey(pid)){
			ArrayList<TransactionId> tids = sharedLocks.get(pid);
			return tids.contains(tid);
		}
		return false;
	}

	// Returns true if tid holds any lock on pid
	public boolean holdsLock(TransactionId tid, PageId pid){
		return holdsSharedLock(tid, pid) || holdsExclusiveLock(tid, pid);
	}

	// Attempts to acquire the lock perm by tid on pid
	// Delegates to getSharedLock and getExclusiveLock respectively
	public void requestLock(TransactionId tid, 
			PageId pid, Permissions perm) throws TransactionAbortedException {
		if(perm == Permissions.READ_ONLY){
			getSharedLock(tid, pid);
			System.out.println("Done requesting read lock for " + tid);
		} else {
			getExclusiveLock(tid, pid);
			System.out.println("Done requesting write lock for " + tid);
		}
	}

	// Returns true if we can grant an exclusive lock to this tid on this page
	// We can grant an exclusive lock when there are no other locks at all.
	// The upgrade case will be taken care of by the caller after the shared lock is released
	private boolean canGrantExclusiveLock(TransactionId tid, PageId pid) {
		boolean otherHasLock = otherHasExclusiveLock(tid, pid) ||
							   otherHasSharedLock(tid, pid);
		boolean selfHasLock = holdsLock(tid, pid);
		return !otherHasLock && !selfHasLock;
	}
	
	// Returns true if we can grant a shared lock to this tid on this page
	private boolean canGrantSharedLock(TransactionId tid, PageId pid) {
		return !otherHasExclusiveLock(tid, pid) && !holdsExclusiveLock(tid, pid);
	}
	
	// Returns a list of TransactionIds that block this request; returns an empty list if none
	private ArrayList<TransactionId> txnsBlockingRequest(TransactionId tid, PageId pid, Permissions perm) {
		ArrayList<TransactionId> blockingTxns = new ArrayList<TransactionId>();
		if(perm == Permissions.READ_ONLY && otherHasExclusiveLock(tid, pid)) {
			// Shared lock request is blocked by another txn having an exclusive lock
			blockingTxns.add(exclusiveLocks.get(pid));
		} else if(perm == Permissions.READ_WRITE) {
			if (otherHasExclusiveLock(tid, pid) && !blockingTxns.contains(exclusiveLocks.get(pid))) {
				blockingTxns.add(exclusiveLocks.get(pid));
			} 
			if (otherHasSharedLock(tid, pid)) {
				for(TransactionId t : sharedLocks.get(pid)) {
					// If the shared lock is not owned by this transaction
					// And this transaction doesn't already exist in blockingTxns
					if(!t.equals(tid) && !blockingTxns.contains(t)) {
						blockingTxns.add(t);
					}
				}
			}
		}
		return blockingTxns;
	}
	
	// Attempts to acquire a shared lock for tid on pid
	private void getSharedLock(TransactionId tid, PageId pid) 
			throws TransactionAbortedException {
		if(DEPENDENCIES){
			getSharedLockDependencies(tid, pid);
		} else {
			getSharedLockTimeout(tid, pid);
		}
	}
	
	// Gets a shared lock, dealing with deadlocks using the dependency graph
	private void getSharedLockDependencies(TransactionId tid, PageId pid) throws TransactionAbortedException {
		ArrayList<TransactionId> blockers = txnsBlockingRequest(tid, pid, Permissions.READ_ONLY);
//		System.out.println("blockers in getSharedLockDependencies: " + blockers);
		if(blockers.isEmpty()) {
			// Nothing blocking us from getting the lock, grant the lock
			addToSharedLocks(tid, pid);
		} else {
			// there is an exclusive lock blocking the request
//			TransPagePerm requestingTpp = new TransPagePerm(tid, pid, Permissions.READ_ONLY);
			
			// tries to add dependency to graph, aborts if there would be a deadlock
			if(!dependencies.addToGraph(tid, blockers.get(0))) {
//				System.out.println("sdependencies: " + dependencies.toString());
//				System.out.println("sExclusive Locks: " + exclusiveLocks);
//				System.out.println("sShared Locks: " + sharedLocks);
//				dependencies.removeAllDependenciesTo(tid);
//				System.out.println("sdependencies: " + dependencies.toString());
//				System.out.println("sExclusive Locks: " + exclusiveLocks);
//				System.out.println("sShared Locks: " + sharedLocks);
				throw new TransactionAbortedException();
			}
			
			// Use a sleep statement until our entry in dependencies is empty
			while(dependencies.hasDependencies(tid)){
				try {
					Thread.sleep(10);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}
			
			// The dependency list is now empty, we can execute now
			addToSharedLocks(tid, pid);
			
			// We have completed our request, can remove this transaction from the dependencies
//			dependencies.removeAllDependenciesTo(tid);
			
			// REMOVE AND REPLACE THIS CALL ^
		}
	}
	
	// Gets a shared lock, dealing with deadlocks using a simple timeout policy
	private synchronized void getSharedLockTimeout(TransactionId tid, PageId pid) throws TransactionAbortedException {
		if(otherHasExclusiveLock(tid, pid)){
			// wait until can grant shared lock, or deadlock and abort
			long startTime = System.currentTimeMillis();
			while(!canGrantSharedLock(tid, pid)){
				long currentTime = System.currentTimeMillis();
				if((currentTime - startTime) > 1000) {
					// Assume there is a deadlock after 1 second
					throw new TransactionAbortedException();
				}
				try {
					Thread.sleep(10);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}
			// If we made it out of the loop, we acquired the lock successfully
			addToSharedLocks(tid, pid);
		} else if (!holdsSharedLock(tid, pid)){
			// If this transaction doesn't already hold a read lock, grant the read lock
			addToSharedLocks(tid, pid);
		}		
	}

	// Attempts to acquire exclusive lock for tid on pid
	private void getExclusiveLock(TransactionId tid, PageId pid) throws TransactionAbortedException {
		if(DEPENDENCIES){
//			System.out.println("In getExclusiveLock");
			getExclusiveLockDependencies(tid, pid);
		} else {
			getExclusiveLockTimeout(tid, pid);
		}
	}
	
	// Gets an exclusive lock, dealing with deadlocks using a dependency graph
	private void getExclusiveLockDependencies(TransactionId tid, PageId pid) throws TransactionAbortedException {
		ArrayList<TransactionId> blockers = txnsBlockingRequest(tid, pid, Permissions.READ_WRITE);
		System.out.println("blockers in getExclusiveLockDependencies: " + blockers);
		if(blockers.isEmpty()) {
			// Nothing blocking us from getting the lock, grant the lock
			addToExclusiveLocks(tid, pid);
		} else {
			// there is an exclusive lock blocking the request
			// tries to add dependency to graph, aborts if there would be a deadlock
			if(!dependencies.addToGraph(tid, blockers.get(0))){

//				dependencies.removeAllDependenciesTo(tid);
				throw new TransactionAbortedException();
			}
			
			// Use a sleep statement until our entry in dependencies is empty
			while(dependencies.hasDependencies(tid)){
				try {
					Thread.sleep(10);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}
			System.out.println("cleared dependency list so transaction " + tid + " can execute");
			// The dependency list is now empty, we can execute now
			addToExclusiveLocks(tid, pid);
//			System.out.println("dependencies: " + dependencies.toString());
//			System.out.println("Exclusive Locks: " + exclusiveLocks);
//			System.out.println("Shared Locks: " + sharedLocks);
			// We have completed our request, can remove this transaction from the dependencies
//			dependencies.removeAllDependenciesTo(tid);
		}
	}
	
	// Gets an exclusive lock, dealing with deadlocks using a simple timeout policy
	private synchronized void getExclusiveLockTimeout(TransactionId tid, PageId pid) throws TransactionAbortedException {
		if(otherHasExclusiveLock(tid, pid) || otherHasSharedLock(tid, pid)){
			// wait until can grant exclusive lock, or deadlock and abort
			long startTime = System.currentTimeMillis();
			while(!canGrantExclusiveLock(tid, pid)){
				long currentTime = System.currentTimeMillis();
				if((currentTime - startTime) > 1000) {
					// Assume there is a deadlock after 1 second
					throw new TransactionAbortedException();
				}
				try {
					Thread.sleep(10);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}
			// If we made it out of the loop, we acquired the lock successfully
			addToExclusiveLocks(tid, pid);
		} else if (holdsSharedLock(tid, pid)) {
			// If this txn has a read lock, upgrade and grant
			removeFromSharedLocks(tid, pid);
			addToExclusiveLocks(tid, pid);
		} else if (!holdsExclusiveLock(tid, pid)) {
			// No locks exist on page, grant exclusive lock
			addToExclusiveLocks(tid, pid);
		}
	}
}