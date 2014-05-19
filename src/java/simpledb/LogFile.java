
package simpledb;

import java.io.*;
import java.util.*;
import java.lang.reflect.*;

/**
LogFile implements the recovery subsystem of SimpleDb.  This class is
able to write different log records as needed, but it is the
responsibility of the caller to ensure that write ahead logging and
two-phase locking discipline are followed.  <p>

<u> Locking note: </u>
<p>

Many of the methods here are synchronized (to prevent concurrent log
writes from happening); many of the methods in BufferPool are also
synchronized (for similar reasons.)  Problem is that BufferPool writes
log records (on page flushed) and the log file flushes BufferPool
pages (on checkpoints and recovery.)  This can lead to deadlock.  For
that reason, any LogFile operation that needs to access the BufferPool
must not be declared synchronized and must begin with a block like:

<p>
<pre>
    synchronized (Database.getBufferPool()) {
       synchronized (this) {

       ..

       }
    }
</pre>
*/

/**
<p> The format of the log file is as follows:

<ul>

<li> The first long integer of the file represents the offset of the
last written checkpoint, or -1 if there are no checkpoints

<li> All additional data in the log consists of log records.  Log
records are variable length.

<li> Each log record begins with an integer type and a long integer
transaction id.

<li> Each log record ends with a long integer file offset representing
the position in the log file where the record began.

<li> There are five record types: ABORT, COMMIT, UPDATE, BEGIN, and
CHECKPOINT

<li> ABORT, COMMIT, and BEGIN records contain no additional data

<li>UPDATE RECORDS consist of two entries, a before image and an
after image.  These images are serialized Page objects, and can be
accessed with the LogFile.readPageData() and LogFile.writePageData()
methods.  See LogFile.print() for an example.

<li> CHECKPOINT records consist of active transactions at the time
the checkpoint was taken and their first log record on disk.  The format
of the record is an integer count of the number of transactions, as well
as a long integer transaction id and a long integer first record offset
for each active transaction.

</ul>

*/

public class LogFile {

    final File logFile;
    private RandomAccessFile raf;
    Boolean recoveryUndecided; // no call to recover() and no append to log

    static final int ABORT_RECORD = 1;
    static final int COMMIT_RECORD = 2;
    static final int UPDATE_RECORD = 3;
    static final int BEGIN_RECORD = 4;
    static final int CHECKPOINT_RECORD = 5;
    static final long NO_CHECKPOINT_ID = -1;

    final static int INT_SIZE = 4;
    final static int LONG_SIZE = 8;

    long currentOffset = -1;//protected by this
//    int pageSize;
    int totalRecords = 0; // for PatchTest //protected by this

    HashMap<Long, Long> tidToFirstLogRecord = new HashMap<Long, Long>();
    // Dirty page table
    HashMap<PageId, Long> dirtyPages = new HashMap<PageId, Long>();
    // Active transaction table
    HashMap<TransactionId, Long> activeTxns = new HashMap<TransactionId, Long>();
    // Transactions to updates
    HashMap<TransactionId, ArrayList<Long>> undoChain = new HashMap<TransactionId, ArrayList<Long>>();

    /** Constructor.
        Initialize and back the log file with the specified file.
        We're not sure yet whether the caller is creating a brand new DB,
        in which case we should ignore the log file, or whether the caller
        will eventually want to recover (after populating the Catalog).
        So we make this decision lazily: if someone calls recover(), then
        do it, while if someone starts adding log file entries, then first
        throw out the initial log file contents.

        @param f The log file's name
    */
    public LogFile(File f) throws IOException {
	this.logFile = f;
        raf = new RandomAccessFile(f, "rw");
        recoveryUndecided = true;

        // install shutdown hook to force cleanup on close
        // Runtime.getRuntime().addShutdownHook(new Thread() {
                // public void run() { shutdown(); }
            // });

        //XXX WARNING -- there is nothing that verifies that the specified
        // log file actually corresponds to the current catalog.
        // This could cause problems since we log tableids, which may or
        // may not match tableids in the current catalog.
    }

    // we're about to append a log record. if we weren't sure whether the
    // DB wants to do recovery, we're sure now -- it didn't. So truncate
    // the log.
    void preAppend() throws IOException {
        totalRecords++;
        if(recoveryUndecided){
            recoveryUndecided = false;
            raf.seek(0);
            raf.setLength(0);
            raf.writeLong(NO_CHECKPOINT_ID);
            raf.seek(raf.length());
            currentOffset = raf.getFilePointer();
        }
    }

    public synchronized int getTotalRecords() {
        return totalRecords;
    }
    
    /** Write an abort record to the log for the specified tid, force
        the log to disk, and perform a rollback
        @param tid The aborting transaction.
    */
    public void logAbort(TransactionId tid) throws IOException {
        // must have buffer pool lock before proceeding, since this
        // calls rollback

        synchronized (Database.getBufferPool()) {

            synchronized(this) {
                preAppend();
                //Debug.log("ABORT");
                //should we verify that this is a live transaction?

                // must do this here, since rollback only works for
                // live transactions (needs tidToFirstLogRecord)
                rollback(tid);

                raf.writeInt(ABORT_RECORD);
                raf.writeLong(tid.getId());
                raf.writeLong(currentOffset);
                currentOffset = raf.getFilePointer();
                force();
                tidToFirstLogRecord.remove(tid.getId());
            }
        }
    }

    /** Write a commit record to disk for the specified tid,
        and force the log to disk.

        @param tid The committing transaction.
    */
    public synchronized void logCommit(TransactionId tid) throws IOException {
        preAppend();
        Debug.log("COMMIT " + tid.getId());
        //should we verify that this is a live transaction?

        raf.writeInt(COMMIT_RECORD);
        raf.writeLong(tid.getId());
        raf.writeLong(currentOffset);
        currentOffset = raf.getFilePointer();
        force();
        tidToFirstLogRecord.remove(tid.getId());
    }

    /** Write an UPDATE record to disk for the specified tid and page
        (with provided         before and after images.)
        @param tid The transaction performing the write
        @param before The before image of the page
        @param after The after image of the page

        @see simpledb.Page#getBeforeImage
    */
    public  synchronized void logWrite(TransactionId tid, Page before,
                                       Page after)
        throws IOException  {
        Debug.log("WRITE, offset = " + raf.getFilePointer());
        preAppend();
        /* update record conists of

           record type
           transaction id
           before page data (see writePageData)
           after page data
           start offset
        */
        raf.writeInt(UPDATE_RECORD);
        raf.writeLong(tid.getId());

        writePageData(raf,before);
        writePageData(raf,after);
        raf.writeLong(currentOffset);
        currentOffset = raf.getFilePointer();

        Debug.log("WRITE OFFSET = " + currentOffset);
    }

    void writePageData(RandomAccessFile raf, Page p) throws IOException{
        PageId pid = p.getId();
        int pageInfo[] = pid.serialize();

        //page data is:
        // page class name
        // id class name
        // id class bytes
        // id class data
        // page class bytes
        // page class data

        String pageClassName = p.getClass().getName();
        String idClassName = pid.getClass().getName();

        raf.writeUTF(pageClassName);
        raf.writeUTF(idClassName);

        raf.writeInt(pageInfo.length);
        for (int i = 0; i < pageInfo.length; i++) {
            raf.writeInt(pageInfo[i]);
        }
        byte[] pageData = p.getPageData();
        raf.writeInt(pageData.length);
        raf.write(pageData);
        //        Debug.log ("WROTE PAGE DATA, CLASS = " + pageClassName + ", table = " +  pid.getTableId() + ", page = " + pid.pageno());
    }

    Page readPageData(RandomAccessFile raf) throws IOException {
        PageId pid;
        Page newPage = null;

        String pageClassName = raf.readUTF();
        String idClassName = raf.readUTF();

        try {
            Class<?> idClass = Class.forName(idClassName);
            Class<?> pageClass = Class.forName(pageClassName);

            Constructor<?>[] idConsts = idClass.getDeclaredConstructors();
            int numIdArgs = raf.readInt();
            Object idArgs[] = new Object[numIdArgs];
            for (int i = 0; i<numIdArgs;i++) {
                idArgs[i] = new Integer(raf.readInt());
            }
            pid = (PageId)idConsts[0].newInstance(idArgs);

            Constructor<?>[] pageConsts = pageClass.getDeclaredConstructors();
            int pageSize = raf.readInt();

            byte[] pageData = new byte[pageSize];
            raf.read(pageData); //read before image

            Object[] pageArgs = new Object[2];
            pageArgs[0] = pid;
            pageArgs[1] = pageData;

            newPage = (Page)pageConsts[0].newInstance(pageArgs);

            //            Debug.log("READ PAGE OF TYPE " + pageClassName + ", table = " + newPage.getId().getTableId() + ", page = " + newPage.getId().pageno());
        } catch (ClassNotFoundException e){
            e.printStackTrace();
            throw new IOException();
        } catch (InstantiationException e) {
            e.printStackTrace();
            throw new IOException();
        } catch (IllegalAccessException e) {
            e.printStackTrace();
            throw new IOException();
        } catch (InvocationTargetException e) {
            e.printStackTrace();
            throw new IOException();
        }
        return newPage;

    }

    /** Write a BEGIN record for the specified transaction
        @param tid The transaction that is beginning

    */
    public synchronized  void logXactionBegin(TransactionId tid)
        throws IOException {
        Debug.log("BEGIN");
        if(tidToFirstLogRecord.get(tid.getId()) != null){
            System.err.printf("logXactionBegin: already began this tid\n");
            throw new IOException("double logXactionBegin()");
        }
        preAppend();
        raf.writeInt(BEGIN_RECORD);
        raf.writeLong(tid.getId());
        raf.writeLong(currentOffset);
        tidToFirstLogRecord.put(tid.getId(), currentOffset);
        currentOffset = raf.getFilePointer();

        Debug.log("BEGIN OFFSET = " + currentOffset);
    }

    /** Checkpoint the log and write a checkpoint record. */
    public void logCheckpoint() throws IOException {
        //make sure we have buffer pool lock before proceeding
        synchronized (Database.getBufferPool()) {
            synchronized (this) {
                //Debug.log("CHECKPOINT, offset = " + raf.getFilePointer());
                preAppend();
                long startCpOffset, endCpOffset;
                Set<Long> keys = tidToFirstLogRecord.keySet();
                Iterator<Long> els = keys.iterator();
                force();
                Database.getBufferPool().flushAllPages();
                startCpOffset = raf.getFilePointer();
                raf.writeInt(CHECKPOINT_RECORD);
                raf.writeLong(-1); //no tid , but leave space for convenience

                //write list of outstanding transactions
                raf.writeInt(keys.size());
                while (els.hasNext()) {
                    Long key = els.next();
                    Debug.log("WRITING CHECKPOINT TRANSACTION ID: " + key);
                    raf.writeLong(key);
                    //Debug.log("WRITING CHECKPOINT TRANSACTION OFFSET: " + tidToFirstLogRecord.get(key));
                    raf.writeLong(tidToFirstLogRecord.get(key));
                }

                //once the CP is written, make sure the CP location at the
                // beginning of the log file is updated
                endCpOffset = raf.getFilePointer();
                raf.seek(0);
                raf.writeLong(startCpOffset);
                raf.seek(endCpOffset);
                raf.writeLong(currentOffset);
                currentOffset = raf.getFilePointer();
                //Debug.log("CP OFFSET = " + currentOffset);
            }
        }

        logTruncate();
    }

    /** Truncate any unneeded portion of the log to reduce its space
        consumption */
    public synchronized void logTruncate() throws IOException {
        preAppend();
        raf.seek(0);
        long cpLoc = raf.readLong();

        long minLogRecord = cpLoc;

        if (cpLoc != -1L) {
            raf.seek(cpLoc);
            int cpType = raf.readInt();
            @SuppressWarnings("unused")
            long cpTid = raf.readLong();

            if (cpType != CHECKPOINT_RECORD) {
                throw new RuntimeException("Checkpoint pointer does not point to checkpoint record");
            }

            int numOutstanding = raf.readInt();

            for (int i = 0; i < numOutstanding; i++) {
                @SuppressWarnings("unused")
                long tid = raf.readLong();
                long firstLogRecord = raf.readLong();
                if (firstLogRecord < minLogRecord) {
                    minLogRecord = firstLogRecord;
                }
            }
        }

        // we can truncate everything before minLogRecord
        File newFile = new File("logtmp" + System.currentTimeMillis());
        RandomAccessFile logNew = new RandomAccessFile(newFile, "rw");
        logNew.seek(0);
        logNew.writeLong((cpLoc - minLogRecord) + LONG_SIZE);

        raf.seek(minLogRecord);

        //have to rewrite log records since offsets are different after truncation
        while (true) {
            try {
                int type = raf.readInt();
                long record_tid = raf.readLong();
                long newStart = logNew.getFilePointer();

                Debug.log("NEW START = " + newStart);

                logNew.writeInt(type);
                logNew.writeLong(record_tid);

                switch (type) {
                case UPDATE_RECORD:
                    Page before = readPageData(raf);
                    Page after = readPageData(raf);

                    writePageData(logNew, before);
                    writePageData(logNew, after);
                    break;
                case CHECKPOINT_RECORD:
                    int numXactions = raf.readInt();
                    logNew.writeInt(numXactions);
                    while (numXactions-- > 0) {
                        long xid = raf.readLong();
                        long xoffset = raf.readLong();
                        logNew.writeLong(xid);
                        logNew.writeLong((xoffset - minLogRecord) + LONG_SIZE);
                    }
                    break;
                case BEGIN_RECORD:
                    tidToFirstLogRecord.put(record_tid,newStart);
                    break;
                }

                //all xactions finish with a pointer
                logNew.writeLong(newStart);
                raf.readLong();

            } catch (EOFException e) {
                break;
            }
        }

        Debug.log("TRUNCATING LOG;  WAS " + raf.length() + " BYTES ; NEW START : " + minLogRecord + " NEW LENGTH: " + (raf.length() - minLogRecord));

        raf.close();
        logFile.delete();
        newFile.renameTo(logFile);
        raf = new RandomAccessFile(logFile, "rw");
        raf.seek(raf.length());
        newFile.delete();

        currentOffset = raf.getFilePointer();
        //print();
    }

    /** Rollback the specified transaction, setting the state of any
        of pages it updated to their pre-updated state.  To preserve
        transaction semantics, this should not be called on
        transactions that have already committed (though this may not
        be enforced by this method.)

        @param tid The transaction to rollback
    */
    public void rollback(TransactionId tid)
        throws NoSuchElementException, IOException {
        synchronized (Database.getBufferPool()) {
            synchronized(this) {
//            	print();
                preAppend();
                // some code goes here
                long savedOffset = raf.getFilePointer();
                raf.seek(raf.length() - LONG_SIZE);
                System.out.println(raf.getFilePointer());
                System.out.println(raf.getFilePointer());
                long recordStart; 
                int entryType;
                while(raf.getFilePointer() >= tidToFirstLogRecord.get(tid.getId())) {
                	// Find start of this LogRecord
                	recordStart = raf.readLong();
                	// Go to the start
                	raf.seek(recordStart);
                	// Beginning element is the type
                	entryType = raf.readInt();
            		if(entryType == UPDATE_RECORD && raf.readLong() == tid.getId()) {
                		Page beforeImage = readPageData(raf).getBeforeImage();
                		// Write beforeImage to the local file
                		System.out.println("in rollback: " + beforeImage.getId());
                		((HeapFile) Database.getCatalog().getDatabaseFile(beforeImage.getId()
                				.getTableId())).writePage(beforeImage);
                		Database.getBufferPool().insertIntoPageMap(beforeImage.getId(), beforeImage);
                		Database.getBufferPool().discardPage(beforeImage.getId());
            		}
                	// Done with this record, set to start and move back
                	// to go to the previous record
                	if(!((recordStart - LONG_SIZE) < 0))
                		raf.seek(recordStart - LONG_SIZE);
                	else raf.seek(0);
                }
                raf.seek(savedOffset);
            }
        }
//        print();
    }

    /** Shutdown the logging system, writing out whatever state
        is necessary so that start up can happen quickly (without
        extensive recovery.)
    */
    public synchronized void shutdown() {
        try {
            logCheckpoint();  //simple way to shutdown is to write a checkpoint record
            raf.close();
        } catch (IOException e) {
            System.out.println("ERROR SHUTTING DOWN -- IGNORING.");
            e.printStackTrace();
        }
    }

    /** Recover the database system by ensuring that the updates of
        committed transactions are installed and that the
        updates of uncommitted transactions are not installed.
    */
    public void recover() throws IOException {
        synchronized (Database.getBufferPool()) {
            synchronized (this) {
                recoveryUndecided = false;
                // Search from the back to find the last checkpoint.
                long lastOffset = findLastCkpt();
                raf.seek(lastOffset);
                analysis();
                redo();
                undo();
            }
         }
    }
    
    /* Returns the offset of the last checkpoint.
     * If no checkpoints exist, returns 0*/
    private long findLastCkpt() throws IOException {
    	long offset = 0;
    	long savedPoint = raf.getFilePointer();
    	raf.seek(raf.length() - LONG_SIZE);
    	long recordStart;
    	int entryType;
    	while(raf.getFilePointer() >= LONG_SIZE) {
    		// Find start of this LogRecord
        	recordStart = raf.readLong();
        	// Go to the start
        	raf.seek(recordStart);
        	// Beginning element is the type
        	entryType = raf.readInt();
        	// Skip over the tid
        	raf.seek(raf.getFilePointer() + LONG_SIZE);
    		if(entryType == CHECKPOINT_RECORD) {
    			offset = recordStart;
    			raf.seek(0);  // ends loop
    		}
    	}
    	raf.seek(savedPoint);
    	return offset;
    }
    
    // Build up the activeTxns and dirty page tables from log
    private void analysis() throws IOException {
    	long savedPoint = raf.getFilePointer();
    	long lastCkpt = findLastCkpt();
        raf.seek(lastCkpt);
        // From the last checkpoint, re-build the tables
        // Skip over checkpoint's type and tid
        raf.seek(raf.getFilePointer() + INT_SIZE + LONG_SIZE);
    	
    	raf.seek(savedPoint);
    }
    
    private void redo() throws IOException {
    	long savedPoint = raf.getFilePointer();
    	
    	
    	raf.seek(savedPoint);
    }
    
    private void undo() throws IOException {
    	long savedPoint = raf.getFilePointer();
    	
    	
    	raf.seek(savedPoint);
    }
    

    /** Print out a human readable representation of the log */
    public void print() throws IOException {
        // save the current raf pointer
    	long savedPtr = raf.getFilePointer();
    	long fileSize = raf.length();
    	raf.seek(0);
    	// Read the last written checkpoint offset
    	if(fileSize > LONG_SIZE)
    		System.out.println(raf.getFilePointer() + ": Last checkpoint = " + raf.readLong());
    	while(raf.getFilePointer() < fileSize) {
    		System.out.print("\n" + raf.getFilePointer() + " Type: ");
    		int type = raf.readInt();
    		switch(type) {
    		case CHECKPOINT_RECORD :
    			System.out.println("<CHECKPOINT>");
    			System.out.println(raf.getFilePointer() + ": tid = " + raf.readLong());
    			// The format of the record is an integer count of the number of transactions, 
    			// as well as a long integer transaction id and a long integer first record offset for each active transaction.
    			printCkptRecord();
    			break;
    		case UPDATE_RECORD :
    			System.out.println("<UPDATE>");
    			System.out.println(raf.getFilePointer() + ": tid = " + raf.readLong());
    			printUpdateRecord();
    			break;
    		case COMMIT_RECORD :
    			System.out.println("<COMMIT>");
    			System.out.println(raf.getFilePointer() + ": tid = " + raf.readLong());
    			break;
    		case BEGIN_RECORD :
    			System.out.println("<BEGIN>");
    			System.out.println(raf.getFilePointer() + ": tid = " + raf.readLong());
    			break;
    		case ABORT_RECORD :
    			System.out.println("<ABORT>");
    			System.out.println(raf.getFilePointer() + ": tid = " + raf.readLong());
    			break;
    		}
    		System.out.println(raf.getFilePointer() + ": LogRecord offset = " + raf.readLong());
    	}
    	System.out.println("Log size: " + raf.length());
    	raf.seek(savedPtr);
    }
    
    private void printCkptRecord() throws IOException {
    	// The format of the record is an integer count of the number of transactions, 
		// as well as a long integer transaction id and a long integer first record offset for each active transaction.
    	System.out.print(raf.getFilePointer() + ": Num transactions in CKPT = ");
		int numTransactions = raf.readInt();
		System.out.println(numTransactions);
		for(int i=0; i<numTransactions; i++) {
			System.out.println(raf.getFilePointer() + ": tid of t" + i + " = " + raf.readLong());
			System.out.println(raf.getFilePointer() + ": first log record offset of t" + i +
							   "= " + raf.readLong());			
		}
    }
    
    private void printUpdateRecord() throws IOException {
    	System.out.print(raf.getFilePointer() + ": beforeImage = ");
		Page beforeImage = readPageData(raf);
		byte[] bidata = beforeImage.getPageData();
		String dirty = "Dirty";
		if(beforeImage.isDirty() == null)
			dirty = "Clean";
		System.out.println("id: " + beforeImage.getId() + " (" + dirty + ")");    			
		System.out.print(raf.getFilePointer() + ": afterImage = ");
		Page afterImage = readPageData(raf);
		byte[] aidata = afterImage.getPageData();
		dirty = "Dirty";
		if(afterImage.isDirty() == null)
			dirty = "Clean";
		System.out.println("id: " + afterImage.getId() + " (" + dirty + ")");
    	boolean isDifferent = false;
    	for(int i=0; i<aidata.length; i++) {
    		if(aidata[i] != bidata[i])
    			System.out.println("Difference at position " + i + ": beforeImage = " + 
    					bidata[i] + ", afterImage = " + aidata[i]);
    			isDifferent = true;
    	}
    	System.out.println("In flushPage: " + isDifferent);
    }

    public  synchronized void force() throws IOException {
        raf.getChannel().force(true);
    }

}
