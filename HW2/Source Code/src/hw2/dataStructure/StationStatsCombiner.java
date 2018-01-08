package hw2.dataStructure;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Writable;

//Data structure defined to store tmin and tmax temperatures for when 
//using a combiner
public class StationStatsCombiner implements Writable{
	
	private DoubleWritable tMaxSum;
	private DoubleWritable tMaxCount;
	private DoubleWritable tMinSum;
	private DoubleWritable tMinCount;
	
	//Constructors
	public StationStatsCombiner() {
	
		this.tMaxSum = new DoubleWritable();
		this.tMaxCount = new DoubleWritable();
		this.tMinSum = new DoubleWritable();
		this.tMinCount = new DoubleWritable();
	}
	
	public StationStatsCombiner(double tMaxSum, double tMaxCount, double tMinSum,
			double tMinCount) {
	
		this.tMaxSum = new DoubleWritable(tMaxSum);
		this.tMaxCount = new DoubleWritable(tMaxCount);
		this.tMinSum = new DoubleWritable(tMinSum);
		this.tMinCount = new DoubleWritable(tMinCount);
	}
	
	//Getters and setters
	public DoubleWritable gettMaxSum() {
		return tMaxSum;
	}

	public DoubleWritable gettMaxCount() {
		return tMaxCount;
	}

	public DoubleWritable gettMinSum() {
		return tMinSum;
	}

	public DoubleWritable gettMinCount() {
		return tMinCount;
	}

	public void settMaxSum(DoubleWritable tMaxSum) {
		this.tMaxSum = tMaxSum;
	}

	public void settMaxCount(DoubleWritable tMaxCount) {
		this.tMaxCount = tMaxCount;
	}

	public void settMinSum(DoubleWritable tMinSum) {
		this.tMinSum = tMinSum;
	}

	public void settMinCount(DoubleWritable tMinCount) {
		this.tMinCount = tMinCount;
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		
		this.tMaxSum.readFields(in);
		this.tMaxCount.readFields(in);
		this.tMinSum.readFields(in);
		this.tMinCount.readFields(in);
		
	}
	@Override
	public void write(DataOutput out) throws IOException {
		
		this.tMaxSum.write(out);
		this.tMaxCount.write(out);
		this.tMinSum.write(out);
		this.tMinCount.write(out);
		
	}
}
