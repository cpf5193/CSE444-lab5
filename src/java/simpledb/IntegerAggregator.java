package simpledb;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Knows how to compute some aggregate over a set of IntFields.
 */
public class IntegerAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;
    private int gbField;
    private Type gbFieldType;
    private int aggField;
    private Op operator;
    private TupleDesc childTD;
    private Map<Object, Integer> tupleValues; 
    private Map<Object, Integer> tupleCounts;
    
    /**
     * Aggregate constructor
     * 
     * @param gbfield
     *            the 0-based index of the group-by field in the tuple, or
     *            NO_GROUPING if there is no grouping
     * @param gbfieldtype
     *            the type of the group by field (e.g., Type.INT_TYPE), or null
     *            if there is no grouping
     * @param afield
     *            the 0-based index of the aggregate field in the tuple
     * @param what
     *            the aggregation operator
     */

    public IntegerAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        this.gbField = gbfield;
        this.gbFieldType = gbfieldtype;
        this.aggField = afield;
        this.operator = what;
        this.tupleCounts = new HashMap<Object, Integer>();
        this.tupleValues = new HashMap<Object, Integer>();
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the
     * constructor
     * 
     * @param tup
     *            the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
    	if(this.childTD == null){
    		//get the child TupleDesc on the first merge
    		this.childTD = tup.getTupleDesc();
    	}
    	int value = ((IntField)tup.getField(this.aggField)).getValue();
         if(this.gbField == NO_GROUPING){  // keep single aggregation value in null key
        	 noGroupPutTuple(value);  // merge tuple with no grouping
         } else {  //use group value as keys for tables
        	 Object index;
        	 if(gbFieldType == Type.STRING_TYPE){
        		 index = ((StringField)tup.getField(gbField)).getValue();
        	 } else {
        		 index = ((IntField)tup.getField(gbField)).getValue();
        	 }
        	 putTupleWithGrouping(index, value);  //merge tuple with grouping
         }
    }
    
    //Private method to merge the tuples int the case where there is no grouping
    private void noGroupPutTuple(int value){
    	if(tupleCounts.keySet().size() == 0){ // no data merged yet
	   		 tupleCounts.put(null, 1);
	   		 if(operator.toString().equals("count")){
	   			 tupleValues.put(null, 1);
	   		 } else {
	   			 tupleValues.put(null, value);
	   		 }
	   	 } else { //null exists as a key in the maps
	   		 tupleCounts.put(null, tupleCounts.get(null)+1);
	   		 if(operator.toString().equals("count")){
	   			 tupleValues.put(null, tupleValues.get(null)+1);
	   		 } else {
	   			 tupleValues.put(null, tupleValues.get(null)+value);
	   		 }
	   	 }
    }
    
    //Private method to merge the tuples in the case where there is grouping
    private void putTupleWithGrouping(Object index, int value){
    	if(!tupleCounts.containsKey(index)){  // first tuple for this group
	   		 tupleCounts.put(index, 1);
	   		 if(operator.toString().equals("count")){
	   			 tupleValues.put(index, 1);
	   		 } else {
	   			 tupleValues.put(index, value);
	   		 }
	   	 } else {
	   		 putTupleGroupingWithKey(index, value);
	   	 }
    }
    
    // Private method for doing the switch statement in the case where
    // There is grouping, and the specified group already has a value
    private void putTupleGroupingWithKey(Object index, int value){
		 tupleCounts.put(index, tupleCounts.get(index)+1);
		 switch(operator){
		 case MAX : 
			 if(tupleValues.get(index) < value){
				 tupleValues.put(index, value);
			 }
			 break;
		 case MIN: 
			 if(tupleValues.get(index) > value){
				 tupleValues.put(index, value);
			 }
			 break;
		 case COUNT:
			 tupleValues.put(index, tupleValues.get(index)+1);
			 break;
		 default:  // SUM, AVG
			 tupleValues.put(index, tupleValues.get(index)+value);
			 break;
		 }
    }
    
    private TupleDesc generateTupleDesc(){
    	if(this.childTD == null){ //no tuples merged yet
    		//return a general tupleDesc
    		return generalTupleDesc();
    	} else {
    		//generate a helpful, well-named TupleDesc
    		Type[] typeArr;
        	String[] nameArr;
        	String aggName = operator.toString() + "(" + childTD.getFieldName(aggField) + ")";
        	if(gbField == NO_GROUPING){
        		typeArr = new Type[]{Type.INT_TYPE};
        		nameArr = new String[]{aggName};
        	} else {
        		typeArr = new Type[]{gbFieldType, Type.INT_TYPE};
        		String groupName = "group by(" + childTD.getFieldName(gbField) + ")";
        		nameArr = new String[]{groupName, aggName};
        	}
        	return new TupleDesc(typeArr, nameArr);
    	}
    }
    
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
    
    //Generates an ArrayList of tuples for grouped queries, 1 tuple per group
    private ArrayList<Tuple> generateGroupedTuples(TupleDesc td){
    	ArrayList<Tuple> tuples = new ArrayList<Tuple>();
    	for(Object o : tupleValues.keySet()){
    		Tuple tuple = new Tuple(td);
    		if(o instanceof String){
    			String groupVal = (String)o;
    			tuple.setField(0, new StringField(groupVal, groupVal.length()));
    		} else { // o is an integer
    			int groupVal = (Integer)o;
    			tuple.setField(0, new IntField(groupVal));
    		}
    		if(operator.toString().equals("avg")){
    			int roundedAvg = (tupleValues.get(o) / tupleCounts.get(o));
        		tuple.setField(1, new IntField(roundedAvg));
    		} else {
    			tuple.setField(1, new IntField(tupleValues.get(o)));
    		}
    		tuples.add(tuple);
    	}
    	return tuples;
    }
    
    //Generates a tuple in an ArrayList for a non-grouping aggregation
    private ArrayList<Tuple> generateSingleTuple(TupleDesc td){
    	ArrayList<Tuple> tuples = new ArrayList<Tuple>();
    	if(tupleValues.keySet().size() > 0){
	    	Tuple tuple = new Tuple(td);
	    	if(operator.toString().equals("avg")){
	    		int roundedAvg = (tupleValues.get(null) / tupleCounts.get(null));
	    		tuple.setField(0, new IntField(roundedAvg));
	    	} else { //min, max, sum, count
	    		tuple.setField(0, new IntField(tupleValues.get(null)));
	    	}
	    	tuples.add(tuple);
    	}
    	return tuples;
    }
    
    /**
     * Create a DbIterator over group aggregate results.
     * 
     * @return a DbIterator whose tuples are the pair (groupVal, aggregateVal)
     *         if using group, or a single (aggregateVal) if no grouping. The
     *         aggregateVal is determined by the type of aggregate specified in
     *         the constructor.
     */
    public DbIterator iterator() {
    	TupleDesc tupleDesc = generateTupleDesc();
    	List<Tuple> tuples;
    	if(gbField == NO_GROUPING){
    		tuples = generateSingleTuple(tupleDesc);
    	} else {
    		tuples = generateGroupedTuples(tupleDesc);
    	}
    	return new TupleIterator(tupleDesc, tuples);
    }

}
