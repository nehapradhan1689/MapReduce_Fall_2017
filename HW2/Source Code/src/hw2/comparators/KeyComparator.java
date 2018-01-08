package hw2.comparators;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

import hw2.dataStructure.CompositeKey;

//This class provides the criteria in which the key
//should be sorted in the shuffling phase
public class KeyComparator extends WritableComparator {

	//Constructor
	protected KeyComparator() {
		super(CompositeKey.class, true);
	}

	/* (non-Javadoc)
	 * @see org.apache.hadoop.io.WritableComparator#compare(org.apache.hadoop.io.WritableComparable, org.apache.hadoop.io.WritableComparable)
	 */
	@Override
	public int compare(WritableComparable obj1, WritableComparable obj2) {
		
		CompositeKey key1 = (CompositeKey) obj1;
		CompositeKey key2 = (CompositeKey) obj2;
		
		String stnID1 = key1.getStationID().toString();
		String stnID2 = key2.getStationID().toString();
		Integer year1 = Integer.valueOf(key1.getYear().get());
		Integer year2 = Integer.valueOf(key2.getYear().get());
		
		if(stnID1.compareTo(stnID2) == 0) {
			return year1.compareTo(year2);
		}
		else {
			return stnID1.compareTo(stnID2);
		}
		
		
		
	}

}
