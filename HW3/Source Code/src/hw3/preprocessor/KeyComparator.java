package hw3.preprocessor;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

import hw3.datastructures.Page;

//The KeyComparator provides the criteria in which the keys are sorted in so
//that the key is sorted in descending order of outlinks.
public class KeyComparator extends WritableComparator {

	//Constructor
	protected KeyComparator() {
		super(Page.class, true);
	}

	@Override
	public int compare(WritableComparable obj1, WritableComparable obj2) {
		
		Page page1 = (Page) obj1;
		Page page2 = (Page) obj2;
		String pageName1 = page1.getPageName().toString();
		String pageName2 = page2.getPageName().toString();
		Integer noOfOutlinks1 = page1.getNoOfOutlinks().get();
		Integer noOfOutlinks2 = page2.getNoOfOutlinks().get();
				
		if(pageName1.compareTo(pageName2) == 0) {
			return noOfOutlinks2.compareTo(noOfOutlinks1);
		}
		
		return pageName1.compareTo(pageName2);
	}

}
