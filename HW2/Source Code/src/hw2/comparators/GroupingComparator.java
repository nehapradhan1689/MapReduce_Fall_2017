package hw2.comparators;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

import hw2.dataStructure.CompositeKey;

//Class that groups records with the same station ID so that all records
//with matching station IDs is sent to the same reducer irrespective of
//the year
//
public class GroupingComparator extends WritableComparator {

	//Constructor
	protected GroupingComparator() {
		
		super(CompositeKey.class, true);
		
	}

	/* (non-Javadoc)
	 * @see org.apache.hadoop.io.WritableComparator#compare(org.apache.hadoop.io.WritableComparable, org.apache.hadoop.io.WritableComparable)
	 *Overriding the compareTo method to group map emitted records
	 */
	@Override
	public int compare(WritableComparable obj1 , WritableComparable obj2) {
		
		CompositeKey compKey1 = (CompositeKey) obj1;
		CompositeKey compKey2 = (CompositeKey) obj2;
		
		return compKey1.compareTo(compKey2);
	}
	
	
	
}
