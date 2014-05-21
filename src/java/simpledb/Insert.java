package simpledb;

import java.io.IOException;
import java.util.NoSuchElementException;

/**
 * Inserts tuples read from the child operator into the tableid specified in the
 * constructor
 */
public class Insert extends Operator {

    private static final long serialVersionUID = 1L;
    private TransactionId tid;
    private DbIterator child;
    private int tableid;
    private boolean called;
    private TupleDesc td;
    
    /**
     * Constructor.
     * 
     * @param t
     *            The transaction running the insert.
     * @param child
     *            The child operator from which to read tuples to be inserted.
     * @param tableid
     *            The table in which to insert tuples.
     * @throws DbException
     *             if TupleDesc of child differs from table into which we are to
     *             insert.
     */
    public Insert(TransactionId t, DbIterator child, int tableid)
            throws DbException {
        this.tid = t;
        this.child = child;
        if(Database.getCatalog().getTupleDesc(tableid).equals(child.getTupleDesc())){
        	this.tableid = tableid;
        	td = new TupleDesc(new Type[]{Type.INT_TYPE}, 
 				   new String[]{"NumInserted"});
        } else {
        	throw new DbException("Child TupleDesc does not match table TupleDesc");
        }
    }

    public TupleDesc getTupleDesc() {
        return td;
    }

    public void open() throws DbException, TransactionAbortedException {
        child.open();
        super.open();
        called = false;
    }

    public void close() {
    	super.close();
    	child.close();
    }

    public void rewind() throws DbException, TransactionAbortedException {
    	close();
    	open();
    	child.rewind();
    }

    /**
     * Inserts tuples read from child into the tableid specified by the
     * constructor. It returns a one field tuple containing the number of
     * inserted records. Inserts should be passed through BufferPool. An
     * instance of BufferPool is available via Database.getBufferPool(). Note
     * that insert DOES NOT need check to see if a particular tuple is a
     * duplicate before inserting it.
     * 
     * @return A 1-field tuple containing the number of inserted records, or
     *         null if called more than once.
     * @see Database#getBufferPool
     * @see BufferPool#insertTuple
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        int numInserted = 0;
        if(called) {
        	return null;
        }
    	while(child.hasNext()){
        	try {
				Database.getBufferPool().insertTuple(tid, tableid, child.next());
			} catch (NoSuchElementException e) {
				e.printStackTrace();
				return null;
			} catch (IOException e) {
				e.printStackTrace();
				return null;
			}
        	numInserted++;
        }
    	Tuple tuple = new Tuple(td);
    	Field ctField = new IntField(numInserted);
    	tuple.setField(0, ctField);
    	called = true;
    	return tuple;
    }

    @Override
    public DbIterator[] getChildren() {
    	return new DbIterator[]{child};
    }

    @Override
    public void setChildren(DbIterator[] children) {
        if(children == null || children.length != 1) {
        	throw new IllegalArgumentException("Argument should be an array of length 1");
        }
        child = children[0];
    }
}
