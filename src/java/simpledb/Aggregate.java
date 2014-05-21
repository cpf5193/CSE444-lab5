package simpledb;

import java.util.*;

/**
 * The Aggregation operator that computes an aggregate (e.g., sum, avg, max,
 * min). Note that we only support aggregates over a single column, grouped by a
 * single column.
 */
public class Aggregate extends Operator {

    private static final long serialVersionUID = 1L;
    private DbIterator child;
    private int aggFieldPos;
    private int gFieldPos;
    private Aggregator.Op operator;
    private Aggregator aggregator;
    private DbIterator aggIterator;

    /**
     * Constructor.
     * 
     * Implementation hint: depending on the type of afield, you will want to
     * construct an {@link IntAggregator} or {@link StringAggregator} to help
     * you with your implementation of readNext().
     * 
     * 
     * @param child
     *            The DbIterator that is feeding us tuples.
     * @param afield
     *            The column over which we are computing an aggregate.
     * @param gfield
     *            The column over which we are grouping the result, or -1 if
     *            there is no grouping
     * @param aop
     *            The aggregation operator to use
     */
    public Aggregate(DbIterator child, int afield, int gfield, Aggregator.Op aop) {
    	this.child = child;
    	this.aggFieldPos = afield;
    	this.gFieldPos = gfield;
    	this.operator = aop;
    	Type gFieldType;
    	if(gfield == Aggregator.NO_GROUPING){
    		gFieldType = null;
    	} else {
    		gFieldType = child.getTupleDesc().getFieldType(gfield);
    	}
    	if(child.getTupleDesc().getFieldType(afield).equals(Type.INT_TYPE)){
    		this.aggregator = new IntegerAggregator(gfield, gFieldType, afield, aop);
    	} else {  // Type: Type.STRING_TYPE
    		this.aggregator = new StringAggregator(gfield, gFieldType, afield, aop);
    	}
		this.aggIterator = aggregator.iterator();
    }

    /**
     * @return If this aggregate is accompanied by a groupby, return the groupby
     *         field index in the <b>INPUT</b> tuples. If not, return
     *         {@link simpledb.Aggregator#Aggregator.NO_GROUPING}
     * */
    public int groupField() {
    	if(gFieldPos == -1) {
    		return simpledb.Aggregator.NO_GROUPING;
    	} else {
    		return gFieldPos;
    	}
    }

    /**
     * @return If this aggregate is accompanied by a group by, return the name
     *         of the groupby field in the <b>OUTPUT</b> tuples If not, return
     *         null;
     * */
    public String groupFieldName() {
    	if(gFieldPos == -1){
    		return null;
    	} else {
    		return child.getTupleDesc().getFieldName(gFieldPos);
    	}
    }

    /**
     * @return the aggregate field
     * */
    public int aggregateField() {
    	return aggFieldPos;
    }

    /**
     * @return return the name of the aggregate field in the <b>OUTPUT</b>
     *         tuples
     * */
    public String aggregateFieldName() {
    	return child.getTupleDesc().getFieldName(aggFieldPos);
    }

    /**
     * @return return the aggregate operator
     * */
    public Aggregator.Op aggregateOp() {
    	return operator;
    }

    public static String nameOfAggregatorOp(Aggregator.Op aop) {
    	return aop.toString();
    }

    public void open() throws NoSuchElementException, DbException,
	    TransactionAbortedException {
		child.open();
		while(child.hasNext()){
			aggregator.mergeTupleIntoGroup(child.next());
		}
		aggIterator = aggregator.iterator();
		aggIterator.open();
		super.open();
    }

    /**
     * Returns the next tuple. If there is a group by field, then the first
     * field is the field by which we are grouping, and the second field is the
     * result of computing the aggregate, If there is no group by field, then
     * the result tuple should contain one field representing the result of the
     * aggregate. Should return null if there are no more tuples.
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
    	if(aggIterator.hasNext()){
    		return aggIterator.next();
    	} else {
    		return null;
    	}
    }

    public void rewind() throws DbException, TransactionAbortedException {
		child.rewind();
		aggIterator.rewind();
    }

    /**
     * Returns the TupleDesc of this Aggregate. If there is no group by field,
     * this will have one field - the aggregate column. If there is a group by
     * field, the first field will be the group by field, and the second will be
     * the aggregate value column.
     * 
     * The name of an aggregate column should be informative. For example:
     * "aggName(aop) (child_td.getFieldName(afield))" where aop and afield are
     * given in the constructor, and child_td is the TupleDesc of the child
     * iterator.
     */
    public TupleDesc getTupleDesc() {
    	Type[] typeArr;
    	String[] nameArr;
    	TupleDesc childDesc = child.getTupleDesc();
    	String aggName = childDesc.getFieldName(aggFieldPos);
    	if(gFieldPos == Aggregator.NO_GROUPING && 
    				childDesc.getFieldType(gFieldPos) == Type.INT_TYPE){
    		typeArr = new Type[]{Type.INT_TYPE};
    		nameArr = new String[]{aggName};
    	}else if(gFieldPos == Aggregator.NO_GROUPING){  //childDesc.getFieldType(gFieldPos) == Type.STRING_TYPE
    		typeArr = new Type[]{Type.STRING_TYPE};
    		nameArr = new String[]{aggName};
    	} else {
    		typeArr = new Type[]{childDesc.getFieldType(gFieldPos), Type.INT_TYPE};
    		String groupName = childDesc.getFieldName(gFieldPos);
    		nameArr = new String[]{groupName, aggName};
    	}
    	return new TupleDesc(typeArr, nameArr);
    }

    public void close() {
		super.close();
		aggregator.iterator().close();
		child.close();
    }

    @Override
    public DbIterator[] getChildren() {
		return new DbIterator[]{this.child};
    }

    @Override
    public void setChildren(DbIterator[] children) {
		if(children.length != 1){
			throw new IllegalArgumentException("Ony one column supported");
		}
		this.child = children[0];
    }
    
}
