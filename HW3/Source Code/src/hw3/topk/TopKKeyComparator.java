package hw3.topk;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

import hw3.datastructures.Page;

///The TopKKeyComparator provides the criteria in which the keys are sorted in so
//that the key is sorted in descending order of their page ranks.
public class TopKKeyComparator extends WritableComparator {
	
	//Constructor
	protected TopKKeyComparator() {
		super(DoubleWritable.class, true);
	}
		

		@Override
		public int compare(WritableComparable obj1, WritableComparable obj2) {
			
			DoubleWritable val1 = (DoubleWritable) obj1;
			DoubleWritable val2 = (DoubleWritable) obj2;
			 
			return ((Double) val2.get()).compareTo((Double) val1.get());
		}
}
