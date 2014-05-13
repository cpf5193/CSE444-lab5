package simpledb;

import java.io.*;
import java.util.*;

/**
 * HeapFile is an implementation of a DbFile that stores a collection of tuples
 * in no particular order. Tuples are stored on pages, each of which is a fixed
 * size, and the file is simply a collection of those pages. HeapFile works
 * closely with HeapPage. The format of HeapPages is described in the HeapPage
 * constructor.
 * 
 * @see simpledb.HeapPage#HeapPage
 * @author Sam Madden
 */
public class HeapFile implements DbFile {
	
	private File heapFile;
	private TupleDesc td;
	private int numPages;
    /**
     * Constructs a heap file backed by the specified file.
     * 
     * @param f
     *            the file that stores the on-disk backing store for this heap
     *            file.
     */
    public HeapFile(File f, TupleDesc td) {
        this.heapFile = f;
        this.td = td;
        this.numPages = (int)Math.ceil(f.length() * 1.0 / BufferPool.PAGE_SIZE);
    }

    /**
     * Returns the File backing this HeapFile on disk.
     * 
     * @return the File backing this HeapFile on disk.
     */
    public File getFile() {
        return this.heapFile;
    }

    /**
     * Returns an ID uniquely identifying this HeapFile. Implementation note:
     * you will need to generate this tableid somewhere ensure that each
     * HeapFile has a "unique id," and that you always return the same value for
     * a particular HeapFile. We suggest hashing the absolute file name of the
     * file underlying the heapfile, i.e. f.getAbsoluteFile().hashCode().
     * 
     * @return an ID uniquely identifying this HeapFile.
     */
    public int getId() {
        return heapFile.getAbsoluteFile().hashCode();
    }

    /**
     * Returns the TupleDesc of the table stored in this DbFile.
     * 
     * @return TupleDesc of this DbFile.
     */
    public TupleDesc getTupleDesc() {
        return this.td;
    }

    // see DbFile.java for javadocs
    public Page readPage(PageId pid) {
    	byte[] buf = new byte[BufferPool.PAGE_SIZE];
        FileInputStream fis = null;
        HeapPage p = null;
        try {
            fis = new FileInputStream(heapFile);
            fis.skip(pid.pageNumber() * BufferPool.PAGE_SIZE);
            fis.read(buf);
            fis.close();
            p = new HeapPage((HeapPageId)pid, buf);
        } catch (IOException e) {
            throw new IllegalArgumentException("the page does not exist in this file");
        }

        return p;
    }

    // see DbFile.java for javadocs
    public void writePage(Page page) throws IOException {
        RandomAccessFile raf = new RandomAccessFile(heapFile, "rws");
//        System.out.println("pageNumber: " + page.getId().pageNumber());
//        System.out.println("stuff * page_size: " + page.getId().pageNumber() * BufferPool.PAGE_SIZE);
        raf.seek(page.getId().pageNumber() * BufferPool.PAGE_SIZE);
        raf.write(page.getPageData());
        raf.close();
        
        // Since we're writing the page to disk, it is no longer dirty
        // We don't set the dirtying page since we're un-dirtying the page (use null)
        page.markDirty(false, null);
    }

    /**
     * Returns the number of pages in this HeapFile.
     */
    // some code goes here
    public int numPages() {
    	return numPages;
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> insertTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
    	// iterate over all existing pages, looking for an empty slot
    	for (int i = 0; i < numPages(); i++) {
             PageId pid = new HeapPageId(getId(), i);
             HeapPage page = (HeapPage) Database.getBufferPool()
             				 	.getPage(tid, pid, Permissions.READ_WRITE);
             if (page.getNumEmptySlots() > 0) {
                 page.insertTuple(t);
                 ArrayList<Page> pages = new ArrayList<Page>();
                 pages.add(page);
                 return pages;
             }
         }
    	 // No open slots; add new page
    	 HeapPageId nextPageId = new HeapPageId(getId(), numPages());
         HeapPage nextPage = (HeapPage) Database.getBufferPool()
         					 	.getPage(tid, nextPageId, Permissions.READ_WRITE);
         numPages++;
         nextPage.insertTuple(t);
         ArrayList<Page> pages = new ArrayList<Page>();
         pages.add(nextPage);
         return pages;
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> deleteTuple(TransactionId tid, Tuple t) throws DbException,
            TransactionAbortedException {
    	 HeapPage page = (HeapPage) Database.getBufferPool()
		 	.getPage(tid, t.getRecordId().getPageId(), Permissions.READ_WRITE);
         page.deleteTuple(t);
         ArrayList<Page> pageList = new ArrayList<Page>();
         pageList.add(page);
         return pageList;
    }

    // see DbFile.java for javadocs
    public DbFileIterator iterator(TransactionId tid) {
    	return new HeapFileIterator(tid);
    }
    
    private class HeapFileIterator implements DbFileIterator {
    	
    	private TransactionId transId;
    	private int pageNum;
    	private Page currentPage;
    	private Iterator<Tuple> tupleItr;
//    	private int unIterated;
    	
    	public HeapFileIterator(TransactionId tid){
    		this.transId = tid;
    		pageNum = 0;
//    		unIterated = 0;
    	}
    	
    	public boolean hasNext() throws TransactionAbortedException, DbException{
    		if(tupleItr == null){
    			return false;
    		}
    		boolean nextInPage = tupleItr.hasNext();
    		if(nextInPage)
    			return true;
    		if(pageNum+1 < numPages()){ //more pages exist
	    		pageNum++;
	    		HeapPageId nextPid = new HeapPageId(getId(), pageNum);
	    		Page nextPage = Database.getBufferPool().getPage(transId, nextPid, Permissions.READ_ONLY);
	    		tupleItr = ((HeapPage) nextPage).iterator();
//	    		unIterated = ((HeapPage)nextPage).getUsedSlots();
	    		return hasNext(); //try again on the next page
    		} else {  //no more pages or tuples
    			return false;
    		}
    	}
    	
    	public Tuple next() throws TransactionAbortedException, DbException, NoSuchElementException{
    		if(tupleItr == null){
    			throw new NoSuchElementException("open() must be called before using iterator");
    		} else {
//    			unIterated--;
    			return tupleItr.next();
    		}
    	}
    	
    	@SuppressWarnings("unused")
		public void remove() throws UnsupportedOperationException{
    		throw new UnsupportedOperationException("remove() is not supported at this time");
    	}

		@Override
		public void open() throws DbException, TransactionAbortedException {
			HeapPageId pid = new HeapPageId(getId(), pageNum);
			currentPage = Database.getBufferPool().getPage(transId, pid, Permissions.READ_ONLY);
			tupleItr = ((HeapPage)currentPage).iterator();
		}

		@Override
		public void rewind() throws DbException,
				TransactionAbortedException {
			close();
			open();
		}

		@Override
		public void close() {
			pageNum = 0;
			currentPage = null;
			tupleItr = null;
		}
    };
}

