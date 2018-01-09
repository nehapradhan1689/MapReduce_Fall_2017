package hw3.preprocessor;

import org.apache.hadoop.mapreduce.Partitioner;

import hw3.datastructures.Page;

//The HashPartitioner ensures the records grouped by the GroupingComparator
//end up in being allocated to the same reducer
public class HashPartitioner extends Partitioner<Page, Page>{

	@Override
	public int getPartition(Page pageKey, Page page, int numOfReduceTasks) {
		
		return (pageKey.getPageName().hashCode() & Integer.MAX_VALUE) % numOfReduceTasks;
		
	}

}
