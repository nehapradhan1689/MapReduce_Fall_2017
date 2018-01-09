package hw3.preprocessor;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

import hw3.datastructures.Page;

//The GroupingComparator ensures that the pages with the same page names are
//grouped together to be allocated to the same reducer
public class GroupingComparator extends WritableComparator {

	protected GroupingComparator() {
		super(Page.class, true);
	}

	@Override
	public int compare(WritableComparable obj1, WritableComparable obj2) {
		
		Page page1 = (Page) obj1;
		Page page2 = (Page) obj2;
		
		return page1.compareTo(page2);
	}

}
