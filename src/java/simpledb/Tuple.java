package simpledb;

import java.io.Serializable;
import java.util.Iterator;

/**
 * Tuple maintains information about the contents of a tuple. Tuples have a
 * specified schema specified by a TupleDesc object and contain Field objects
 * with the data for each field.
 */
public class Tuple implements Serializable {

    private static final long serialVersionUID = 1L;
    private RecordId recordId;
    private TupleDesc schema;
    private Field[] fields;
    
    /**
     * Create a new tuple with the specified schema (type).
     * 
     * @param td
     *            the schema of this tuple. It must be a valid TupleDesc
     *            instance with at least one field.
     */
    public Tuple(TupleDesc td) {
        this.schema = td;
        this.fields = new Field[td.numFields()];
    }

    /**
     * @return The TupleDesc representing the schema of this tuple.
     */
    public TupleDesc getTupleDesc() {
        return this.schema;
    }

    /**
     * @return The RecordId representing the location of this tuple on disk. May
     *         be null.
     */
    public RecordId getRecordId() {
        return this.recordId;
    }

    /**
     * Set the RecordId information for this tuple.
     * 
     * @param rid
     *            the new RecordId for this tuple.
     */
    public void setRecordId(RecordId rid) {
        this.recordId = rid;
    }

    /**
     * Change the value of the ith field of this tuple.
     * 
     * @param i
     *            index of the field to change. It must be a valid index.
     * @param f
     *            new value for the field.
     * @throws IllegalArgumentException if i is not a valid index or f is
     * 			  not the right type for the corresponding existing type
     */
    public void setField(int i, Field f) {
        if(i<0 || i>=fields.length){
        	throw new IllegalArgumentException("Not a valid index");
        } else if (!f.getType().equals(getTupleDesc().getFieldType(i))){
        	throw new IllegalArgumentException("Not a valid field type");
        } else {
        	this.fields[i] = f;
        }
    }

    /**
     * @return the value of the ith field, or null if it has not been set.
     * 
     * @param i
     *            field index to return. Must be a valid index.
     */
    public Field getField(int i) {
    	if(i<0 || i>=fields.length){
        	throw new IllegalArgumentException("Not a valid index");
        } else {
        	return fields[i];
        }
    }

    /**
     * Returns the contents of this Tuple as a string. Note that to pass the
     * system tests, the format needs to be as follows:
     * 
     * column1\tcolumn2\tcolumn3\t...\tcolumnN\n
     * 
     * where \t is any whitespace, except newline, and \n is a newline
     */
    public String toString() {
    	String result = "";
    	for(int i=0; i<fields.length-1; i++){
        	result += fields[i].toString() + "\t";
        }
    	result += fields[fields.length-1] + "\n";
    	return result;
    }
    
    /**
     * @return
     *        An iterator which iterates over all the fields of this tuple
     * */
    public Iterator<Field> fields()
    {
        Iterator<Field> itr = new Iterator<Field>() {
        	
        	private int current = -1;
        	
        	public boolean hasNext(){
        		return current+1 < fields.length && !fields[current+1].equals(null);
        	}
        	
        	public Field next(){
        		return fields[current++];
        	}
        	
        	public void remove() throws UnsupportedOperationException{
        		throw new UnsupportedOperationException("remove() is not supported at this time");
        	}
        };
        return itr;
    }
    
    /**
     * reset the TupleDesc of this tuple
     * */
    public void resetTupleDesc(TupleDesc td){
        this.schema = td;
    }
    
    /**
     * @return whether or not this Tuple is equal to tuple other
     * @param other the Tuple to compare to
     **/
    public boolean equals(Object other) {
    	if(other == null || !(other instanceof Tuple)) {
    		return false;
    	}
    	Tuple o = (Tuple)other;
    	boolean equalFields = true;
    	for(int i=0; i<o.fields.length; i++){
    		
    		if(o.fields[i].equals(this.fields[i])){
    			equalFields = true;
    			i = o.fields.length;
    		}
    	}
    	return (equalFields && this.recordId.equals(o.recordId));
    }
}
