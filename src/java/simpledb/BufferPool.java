package simpledb;

import java.io.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * BufferPool manages the reading and writing of pages into memory from
 * disk. Access methods call into it to retrieve pages, and it fetches
 * pages from the appropriate location.
 * <p>
 * The BufferPool is also responsible for locking;  when a transaction fetches
 * a page, BufferPool checks that the transaction has the appropriate
 * locks to read/write the page.
 * 
 * @Threadsafe, all fields are final
 */
@SuppressWarnings("unused")
public class BufferPool {
    /** Bytes per page, including header. */
    public static final int PAGE_SIZE = 4096;

    private static int pageSize = PAGE_SIZE;
    
    /** Default number of pages passed to the constructor. This is used by
    other classes. BufferPool should use the numPages argument to the
    constructor instead. */
    public static final int DEFAULT_PAGES = 50;
    
    private static Map<Integer, Page> pageMap;
    private static ArrayList<PageId> usedPages;
    private static int maxPages;
    private static HeapFile file;
    private static int lastTid;
    private LockManager lockManager;

    /**
     * Creates a BufferPool that caches up to numPages pages.
     *
     * @param numPages maximum number of pages in this buffer pool.
     */
    public BufferPool(int numPages) {
        maxPages = numPages;
        pageMap = new HashMap<Integer, Page>();
        usedPages = new ArrayList<PageId>();
        lockManager = new LockManager(numPages+1);
    }
    
    public static int getPageSize() {
      return pageSize;
    }
    
    // THIS FUNCTION SHOULD ONLY BE USED FOR TESTING!!
    public static void setPageSize(int pageSize) {
    	BufferPool.pageSize = pageSize;
    }

    /**
     * Retrieve the specified page with the associated permissions.
     * Will acquire a lock and may block if that lock is held by another
     * transaction.
     * <p>
     * The retrieved page should be looked up in the buffer pool.  If it
     * is present, it should be returned.  If it is not present, it should
     * be added to the buffer pool and returned.  If there is insufficient
     * space in the buffer pool, an page should be evicted and the new page
     * should be added in its place.
     *
     * @param tid the ID of the transaction requesting the page
     * @param pid the ID of the requested page
     * @param perm the requested permissions on the page
     */
    public Page getPage(TransactionId tid, PageId pid, Permissions perm)
        throws TransactionAbortedException, DbException {
    	lockManager.requestLock(tid, pid, perm);
        if(pageMap.containsKey(pid.hashCode())){  //access a page in the BufferPool
        	Page page = pageMap.get(pid.hashCode());
        	movePageToFront(pid);
        	return page;
        } else {
        	if(pageMap.keySet().size() >= maxPages){
        		evictPage();  //New page but no room, evict one
        	}
	        //Add the new page
	        int tableId = pid.getTableId();
	        Page newPage = ((HeapFile) Database.getCatalog().getDatabaseFile(pid.getTableId())).readPage(pid);
	        pageMap.put(pid.hashCode(), newPage);
	        usedPages.add(0, pid);
	        return newPage;
        }
    }
    
    //Moves the page corresponding to the given PageId to the front of the LRU list
    private synchronized void movePageToFront(PageId pid){
    	if(usedPages.contains(pid)){
    		int oldIndex = usedPages.indexOf(pid);
	    	PageId toFront = usedPages.remove(oldIndex);
	    	System.out.println(usedPages.size());
	    	usedPages.add(0, toFront);
    	}
    	
    }

    /**
     * Releases the lock on a page.
     * Calling this is very risky, and may result in wrong behavior. Think hard
     * about who needs to call this and why, and why they can run the risk of
     * calling it.
     *
     * @param tid the ID of the transaction requesting the unlock
     * @param pid the ID of the page to unlock
     */
    public void releasePage(TransactionId tid, PageId pid) {
        // some code goes here
        // not necessary for lab1|lab2
    	lockManager.releaseLock(tid, pid);
    }

    /**
     * Release all locks associated with a given transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     */
    public synchronized void transactionComplete(TransactionId tid) throws IOException {
        // some code goes here
        // not necessary for lab1|lab2
    	transactionComplete(tid, true);
    }

    /** Return true if the specified transaction has a lock on the specified page */
    public boolean holdsLock(TransactionId tid, PageId p) {
        return lockManager.holdsLock(tid, p);
    }

    /**
     * Commit or abort a given transaction; release all locks associated to
     * the transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     * @param commit a flag indicating whether we should commit or abort
     */
    public synchronized void transactionComplete(TransactionId tid, boolean commit)
        throws IOException {
        // some code goes here
        // not necessary for lab1|lab2
    	if(commit){
    		// Flush all pages associated with tid to disk
    		flushPages(tid);
//    		// save on disk state
//    		for(PageId pid : lockManager.getPagesLockedByTxn(tid)){
//    			pageMap.get(pid.hashCode()).setBeforeImage();
//    		}
    	} else {
    		// Since we're implementing Force/No-Steal, we can abort by discarding the
    		// dirty pages and re-reading them from disk
    		int tableId;
	    	Page newPage;
//	    	System.out.println("Starting actual abort in transactionComplete for " + tid);
	    	// Discard all dirty pages
	    	ArrayList<PageId> pages = lockManager.getPagesLockedByTxn(tid);
	    	int foundIndex;
//	    	System.out.println("removing pages in transactionComplete for " + tid);
	    	for(PageId pid : pages) {
	    		foundIndex = usedPages.indexOf(pid);
	    		if(foundIndex > -1) {
		    		usedPages.remove(foundIndex);
		    		pageMap.remove(pid.hashCode());
	    		}
	    	}
	    	
	    	// What happened to the 8 pages that didn't fit in the bufferpool?
	    	// Apparently they were clean and they were evicted
	    	// Need to ask what happens when there are 10 pages of tuples inserted but only 2 pages in the bufferpool
	    	// Reread the pages from disk
//	    	System.out.println("re-reading pages in transactionComplete for " + tid);
	    	for(PageId pid : pages) {
	    		try {
					Database.getBufferPool().getPage(tid, pid, Permissions.READ_WRITE);
				} catch (TransactionAbortedException e) {
					e.printStackTrace();
				} catch (DbException e) {
					e.printStackTrace();
				}
	    	}
	    	// Update dependency graph here??
	    	
	    	
//	    	usedPages.remove();
//        	pageMap.remove(nextOldestPageId.hashCode());
	    	
//	    	ArrayList<PageId> usedPagesCopy = new ArrayList<PageId>(usedPages);
//	    	for(PageId pid : lockManager.getPagesLockedByTxn(tid)){
//	    		newPage = pageMap.get(pid.hashCode()).getBeforeImage();
//	    		pageMap.put(pid.hashCode(), newPage);
//	    	}
//	    	
//    		for(PageId pid : usedPagesCopy) {
//    			// Evict dirty page
//    			int pidhash = pid.hashCode();
//	        	usedPages.remove(usedPages.indexOf(pid));
//	        	pageMap.remove(pidhash);
//	        	
//	        	// Restore the page to its on-disk state
//	    		tableId = pid.getTableId();
//		        newPage = ((HeapFile) Database.getCatalog().getDatabaseFile(pid.getTableId()))
//		        										   .readPage(pid);
//		        pageMap.put(pidhash, newPage);
//		        usedPages.add(0, pid);
//    		}
    	}
    	// Release all locks that this transaction holds
//		System.out.println("\nbpdependencies: " + lockManager.dependencies.toString());
//		System.out.println("bpExclusive Locks: " + lockManager.exclusiveLocks);
//		System.out.println("bpShared Locks: " + lockManager.sharedLocks);
		lockManager.releaseAllLocksForTxn(tid);
//		System.out.println("\nAfter removing locks and dependencies for tid = " + tid + ": ");
//		System.out.println("bpdependencies: " + lockManager.dependencies.toString());
//		System.out.println("bpExclusive Locks: " + lockManager.exclusiveLocks);
//		System.out.println("bpShared Locks: " + lockManager.sharedLocks + "\n");
    }

    /**
     * Add a tuple to the specified table on behalf of transaction tid.  Will
     * acquire a write lock on the page the tuple is added to and any other 
     * pages that are updated (Lock acquisition is not needed for lab2). 
     * May block if the lock(s) cannot be acquired.
     * 
     * Marks any pages that were dirtied by the operation as dirty by calling
     * their markDirty bit, and updates cached versions of any pages that have 
     * been dirtied so that future requests see up-to-date pages. 
     *
     * @param tid the transaction adding the tuple
     * @param tableId the table to add the tuple to
     * @param t the tuple to add
     */
    public void insertTuple(TransactionId tid, int tableId, Tuple t)
        throws DbException, IOException, TransactionAbortedException {
    	DbFile file = Database.getCatalog().getDatabaseFile(tableId);
    	ArrayList<Page> pages = file.insertTuple(tid, t);
    	for(Page p : pages){
    		p.markDirty(true, tid);
    	}
    }

    /**
     * Remove the specified tuple from the buffer pool.
     * Will acquire a write lock on the page the tuple is removed from and any
     * other pages that are updated. May block if the lock(s) cannot be acquired.
     *
     * Marks any pages that were dirtied by the operation as dirty by calling
     * their markDirty bit, and updates cached versions of any pages that have 
     * been dirtied so that future requests see up-to-date pages. 
     *
     * @param tid the transaction deleting the tuple.
     * @param t the tuple to delete
     */
    public void deleteTuple(TransactionId tid, Tuple t)
        throws DbException, IOException, TransactionAbortedException {
    	int tableId = t.getRecordId().getPageId().getTableId();
    	DbFile file = Database.getCatalog().getDatabaseFile(tableId);
    	ArrayList<Page> pages = file.deleteTuple(tid, t);
    	for(Page p : pages){
    		p.markDirty(true, tid);
    	}
    }

    /**
     * Flush all dirty pages to disk.
     * NB: Be careful using this routine -- it writes dirty data to disk so will
     *     break simpledb if running in NO STEAL mode.
     */
    public synchronized void flushAllPages() throws IOException {
    	for(int key : pageMap.keySet()){
        	Page p = pageMap.get(key);
        	flushPage(p.getId());
        }
    }

    /** Remove the specific page id from the buffer pool.
        Needed by the recovery manager to ensure that the
        buffer pool doesn't keep a rolled back page in its
        cache.
    */
    public synchronized void discardPage(PageId pid) {
        // some code goes here
        // only necessary for lab5
    }

    /**
     * Flushes a certain page to disk
     * @param pid an ID indicating the page to flush
     */
    private void flushPage(PageId pid) throws IOException {
    	DbFile file = Database.getCatalog().getDatabaseFile(pid.getTableId());
    	file.writePage(pageMap.get(pid.hashCode()));
    }

    /** Write all pages of the specified transaction to disk.
     */
    public synchronized void flushPages(TransactionId tid) throws IOException {
        // some code goes here
        // not necessary for lab1|lab2
    	
    	//For all pages that this transaction has locks on, flush to disk using flushPage
    	ArrayList<PageId> pages = lockManager.getPagesLockedByTxn(tid);
    	for(PageId pid : pages) {
    		if(pageMap.get(pid.hashCode()) != null &&
    				pageMap.get(pid.hashCode()).isDirty() != null) {
    			flushPage(pid);
    		}
    	}
    }
    
    /**
     * Discards a page from the buffer pool.
     * Flushes the page to disk to ensure dirty pages are updated on disk.
     * @throws DbException 
     * @throws IOException 
     */
    private synchronized void evictPage() throws DbException {
    	int i = usedPages.size()-1;
    	
    	// Loop through pages in BufferPool, evict least recently used clean page
    	while(i >= 0) {
    		PageId nextOldestPageId = usedPages.get(i);
    		int nextOldestPageHash = nextOldestPageId.hashCode();
    		// Check that we're evicting a clean page
    		if(pageMap.get(nextOldestPageHash).isDirty() == null){
	        	try {
					flushPage(nextOldestPageId);
				} catch (IOException e) {
					e.printStackTrace();
				}
	        	usedPages.remove(i);
	        	pageMap.remove(nextOldestPageId.hashCode());
	        	// Evicted a page, end the loop
	        	return;
    		}
    		i--;
    	}
    	
    	// If we reach here, there are only dirty pages, throw exception
    	throw new DbException("All pages are dirty");
    }

}
