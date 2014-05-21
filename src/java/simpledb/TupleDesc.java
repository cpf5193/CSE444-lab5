package simpledb;

import java.io.Serializable;
import java.util.*;

/**
 * TupleDesc describes the schema of a tuple.
 */
public class TupleDesc implements Serializable {

	private TDItem[] items;
	
    /**
     * A help class to facilitate organizing the information of each field
     * */
    public static class TDItem implements Serializable {

        private static final long serialVersionUID = 1L;

        /**
         * The type of the field
         * */
        public final Type fieldType;
        
        /**
         * The name of the field
         * */
        public final String fieldName;

        public TDItem(Type t, String n) {
            this.fieldName = n;
            this.fieldType = t;
        }

        public String toString() {
            return fieldName + "(" + fieldType + ")";
        }
        
        public String toString(int index){
        	return fieldName + "[" + index + "](" + fieldType + ")";
        }
    }

    /**
     * @return
     *        An iterator which iterates over all the field TDItems
     *        that are included in this TupleDesc
     * */
    public Iterator<TDItem> iterator() {
        Iterator<TDItem> itr = new Iterator<TDItem>() {
        	private int current = -1;
        	
        	public boolean hasNext(){
        		return (current+1)<numFields() && !items[current+1].equals(null);
        	}
        	
        	public TDItem next(){
        		return items[current++];
        	}
        	
        	public void remove() throws UnsupportedOperationException{
//    			List<TDItem> list = new ArrayList<TDItem>(Arrays.asList(items));
//    			list.remove(current);
//    			items = list.toArray(items);
//    			if(current!=0){
//    				current--;
//    			}
        		throw new UnsupportedOperationException("remove() is not supported at this time");
        	}
        };
        return itr;
    }

    private static final long serialVersionUID = 1L;

    /**
     * Create a new TupleDesc with typeAr.length fields with fields of the
     * specified types, with associated named fields.
     * 
     * @param typeAr
     *            array specifying the number of and types of fields in this
     *            TupleDesc. It must contain at least one entry.
     * @param fieldAr
     *            array specifying the names of the fields. Note that names may
     *            be null.
     * @throws IllegalArgumentException if there are no entries or the arrays are
     * 			  different lengths
     */
    public TupleDesc(Type[] typeAr, String[] fieldAr) throws IllegalArgumentException{
        if(typeAr.length < 1){
        	throw new IllegalArgumentException("There must be at least one type");
        } else if(typeAr.length != fieldAr.length){
        	throw new IllegalArgumentException("There must be the same number of types and fields (fields may be null)");
        } else {
        	this.items = new TDItem[typeAr.length];
        	for(int i=0; i<typeAr.length; i++){
        		this.items[i] = new TDItem(typeAr[i], fieldAr[i]);
        	}
        }
    }

    /**
     * Constructor. Create a new tuple desc with typeAr.length fields with
     * fields of the specified types, with anonymous (unnamed) fields.
     * 
     * @param typeAr
     *            array specifying the number of and types of fields in this
     *            TupleDesc. It must contain at least one entry.
     * @throws IllegalArgumentException if there are no entries or the arrays are
     * 			  different lengths           
     */
    public TupleDesc(Type[] typeAr) throws IllegalArgumentException{
    	if(typeAr.length < 1){
    		throw new IllegalArgumentException("There must be at least one type");
    	} else {
    		this.items = new TDItem[typeAr.length];
    		for(int i=0; i<typeAr.length; i++){
    			this.items[i] = new TDItem(typeAr[i], null);
    		}
    	}
    }
    
    /**
     * @return the number of fields in this TupleDesc
     */
    public int numFields() {
        return this.items.length;
    }

    /**
     * Gets the (possibly null) field name of the ith field of this TupleDesc.
     * 
     * @param i
     *            index of the field name to return. It must be a valid index.
     * @return the name of the ith field
     * @throws NoSuchElementException
     *             if i is not a valid field reference.
     */
    public String getFieldName(int i) throws NoSuchElementException {
        if(i < 0 || i>=this.items.length){
        	throw new NoSuchElementException("Not a valid index");
        } else {
        	return this.items[i].fieldName;
        }
    }

    /**
     * Gets the type of the ith field of this TupleDesc.
     * 
     * @param i
     *            The index of the field to get the type of. It must be a valid
     *            index.
     * @return the type of the ith field
     * @throws NoSuchElementException
     *             if i is not a valid field reference.
     */
    public Type getFieldType(int i) throws NoSuchElementException {
    	if(i < 0 || i>=this.items.length){
        	throw new NoSuchElementException("Not a valid index");
        } else {
        	return this.items[i].fieldType;
        }
    }

    /**
     * Find the index of the field with a given name.
     * 
     * @param name
     *            name of the field.
     * @return the index of the field that is first to have the given name.
     * @throws NoSuchElementException
     *             if no field with a matching name is found.
     */
    public int fieldNameToIndex(String name) throws NoSuchElementException {
    	int index = -1;
    	
    	for(int i=0; i<this.numFields(); i++){
    		if(name!=null && name.equals(this.getFieldName(i))){
    			index = i;
    			i = this.numFields();
    		}
    	}
        if(index != -1){
        	return index;
        } else {
        	throw new NoSuchElementException("Not a valid field name");
        }
    }

    /**
     * @return The size (in bytes) of tuples corresponding to this TupleDesc.
     *         Note that tuples from a given TupleDesc are of a fixed size.
     */
    public int getSize() {
        int byteSum = 0;
        for(int i=0; i<this.numFields(); i++){
        	byteSum += this.getFieldType(i).getLen();
        }
        return byteSum;
    }

    /**
     * Merge two TupleDescs into one, with td1.numFields + td2.numFields fields,
     * with the first td1.numFields coming from td1 and the remaining from td2.
     * 
     * @param td1
     *            The TupleDesc with the first fields of the new TupleDesc
     * @param td2
     *            The TupleDesc with the last fields of the TupleDesc
     * @return the new TupleDesc
     */
    public static TupleDesc merge(TupleDesc td1, TupleDesc td2) {
    	int newSize = td1.numFields() + td2.numFields();
    	Type[] newTypes = new Type[newSize];
    	String[] newFields = new String[newSize];
    	for(int i=0; i<td1.numFields(); i++){
    		newTypes[i] = td1.getFieldType(i);
    		newFields[i] = td1.getFieldName(i);
    	}
    	for(int i=0; i<td2.numFields(); i++){
    		newTypes[i+td1.numFields()] = td2.getFieldType(i);
    		newFields[i+td1.numFields()] = td2.getFieldName(i);
    	}
    	return new TupleDesc(newTypes, newFields);
    }

    /**
     * Compares the specified object with this TupleDesc for equality. Two
     * TupleDescs are considered equal if they are the same size and if the n-th
     * type in this TupleDesc is equal to the n-th type in td.
     * 
     * @param o
     *            the Object to be compared for equality with this TupleDesc.
     * @return true if the object is equal to this TupleDesc.
     */
    public boolean equals(Object o) {
    	if(!(o instanceof TupleDesc)){
    		return false;
    	} 
    	TupleDesc casted = (TupleDesc)o;
        if(this.numFields() != casted.numFields()){
        	return false;
        }
        for(int i=0; i<casted.numFields(); i++){
        	if(!this.getFieldType(i).equals(casted.getFieldType(i))){
        		return false;
        	}
        }
        return true;
    }

    public int hashCode() {
        // If you want to use TupleDesc as keys for HashMap, implement this so
        // that equal objects have equals hashCode() results
        throw new UnsupportedOperationException("unimplemented");
        // return this.getSize() * this.numFields();
    }

    /**
     * Returns a String describing this descriptor. It should be of the form
     * "fieldType[0](fieldName[0]), ..., fieldType[M](fieldName[M])", although
     * the exact format does not matter.
     * 
     * @return String describing this descriptor.
     */
    public String toString() {
    	String result = "";
    	int lastIndex = this.numFields() - 1;
        for(int i=0; i<lastIndex; i++){
        	result += this.items[i].toString(i) + ", ";
        }
        result += this.items[lastIndex].toString(lastIndex);
        return result;
    }
}
