package hw2.comparators;

import org.apache.hadoop.mapreduce.Partitioner;

import hw2.dataStructure.CompositeKey;
import hw2.dataStructure.StationStatsCombiner;

//This partitioner is defined to partition the map output to different reducers
public class HashPartitioner extends Partitioner<CompositeKey, StationStatsCombiner>{

	@Override
	public int getPartition(CompositeKey compKey, StationStatsCombiner stn, int numOfReduceTasks) {
		return (compKey.getStationID().hashCode() & Integer.MAX_VALUE) % numOfReduceTasks;
	}
	
}
