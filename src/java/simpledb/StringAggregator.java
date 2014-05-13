package simpledb;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Knows how to compute some aggregate over a set of StringFields.
 */
public class StringAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;
    private int gbField;
    private Type gbFieldType;
    private int aggField;
    private Op operator;
    private TupleDesc childTD;
    private Map<Field, Integer> tupleCounts;
    
    /**
     * Aggregate constructor
     * @param gbfield the 0-based index of the group-by field in the tuple, or NO_GROUPING if there is no grouping
     * @param gbfieldtype the type of the group by field (e.g., Type.INT_TYPE), or null if there is no grouping
     * @param afield the 0-based index of the aggregate field in the tuple
     * @param what aggregation operator to use -- only supports COUNT
     * @throws IllegalArgumentException if what != COUNT
     */

    public StringAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
    	if(!what.toString().equals("count")){
        	throw new IllegalArgumentException("StringAggregator only supports COUNT");
        }
    	this.gbField = gbfield;
        this.gbFieldType = gbfieldtype;
        this.aggField = afield;
        this.operator = what;
        this.tupleCounts = new HashMap<Field, Integer>();
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the constructor
     * @param tup the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
    	if(this.childTD == null){
    		//get the child TupleDesc on the first merge
    		this.childTD = tup.getTupleDesc();
    	}
        if(gbField == NO_GROUPING){
        	if(tupleCounts.keySet().size() == 0){ // first entry
        		tupleCounts.put(null, 1);
        	} else {
        		tupleCounts.put(null, tupleCounts.get(null)+1);
        	}
        } else {
        	Field index = tup.getField(gbField);
        	if(!tupleCounts.containsKey(index)){ // first entry
        		tupleCounts.put(index, 1);
        	} else {
        		Integer newVal = tupleCounts.get(index)+1;
        		tupleCounts.put(index, newVal);
        	}
        	
        }
    }
    
    //used internally to create a 
    private TupleDesc generateTupleDesc(){
    	if(this.childTD == null){ //no tuples merged yet
    		//return a general tupleDesc
    		return generalTupleDesc();
    	} else {
    		//generate a helpful, well-named TupleDesc
    		Type[] typeArr;
        	String[] nameArr;
        	String aggName = operator.toString() + "(" + childTD.getFieldName(aggField) + ")";
        	if(gbField == NO_GROUPING && gbFieldType == Type.INT_TYPE){
        		typeArr = new Type[]{Type.INT_TYPE};
        		nameArr = new String[]{aggName};
        	}else if(gbField == NO_GROUPING){ //gbFieldType == Type.STRING_TYPE
        		typeArr = new Type[]{Type.STRING_TYPE};
        		nameArr = new String[]{aggName};
        	} else {
        		typeArr = new Type[]{gbFieldType, Type.INT_TYPE};
        		String groupName = "group by(" + childTD.getFieldName(gbField) + ")";
        		nameArr = new String[]{groupName, aggName};
        	}
        	return new TupleDesc(typeArr, nameArr);
    	}
    }
    
    //A helper for generating a generally-named tupleDesc when no tuples have been merged yet
    private TupleDesc generalTupleDesc(){
    	Type[] typeArr;
    	String[] nameArr;
    	if(gbField == NO_GROUPING && gbFieldType == Type.INT_TYPE){
    		typeArr = new Type[]{Type.INT_TYPE};
    		nameArr = new String[]{"aggregateValue"};
    	}else if(gbField == NO_GROUPING){ //gbFieldType == Type.STRING_TYPE
    		typeArr = new Type[]{Type.STRING_TYPE};
    		nameArr = new String[]{"aggregateValue"};
    	} else {
    		typeArr = new Type[]{gbFieldType, Type.INT_TYPE};
    		nameArr = new String[]{"groupValue", "aggregateValue"};
    	}
    	return new TupleDesc(typeArr, nameArr);
    }

    /**
     * Create a DbIterator over group aggregate results.
     *
     * @return a DbIterator whose tuples are the pair (groupVal,
     *   aggregateVal) if using group, or a single (aggregateVal) if no
     *   grouping. The aggregateVal is determined by the type of
     *   aggregate specified in the constructor.
     */
    public DbIterator iterator() {
    	TupleDesc td = generateTupleDesc();
        List<Tuple> tuples = new ArrayList<Tuple>();
        if(gbField == NO_GROUPING){
        	Tuple tuple = new Tuple(td);
        	tuple.setField(0, new IntField(tupleCounts.get(null)));
        	tuples.add(tuple);
        } else {
        	for(Object key : tupleCounts.keySet()){
        		Tuple tuple = new Tuple(td);
        		tuple.setField(0, (Field)key);
        		tuple.setField(1, new IntField(tupleCounts.get(key)));
        		tuples.add(tuple);
        	}
        }
        return new TupleIterator(td, tuples);
    }

}
