package hw2.dataStructure;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

//This class defines the composite key that is a combination of the StationID 
//and the year
public class CompositeKey implements Writable, WritableComparable<CompositeKey>{
	
	private Text stationID;
	private IntWritable year;
	
	//Default constructor	
	public CompositeKey() {
		this.stationID = new Text();
		this.year = new IntWritable();
	}

	//Constructor
	public CompositeKey(Text stationID, IntWritable year) {
		this.stationID = stationID;
		this.year = year;
	}
	
	//Getters and Setters
	public Text getStationID() {
		return stationID;
	}



	public IntWritable getYear() {
		return year;
	}



	public void setStationID(Text stationID) {
		this.stationID = stationID;
	}



	public void setYear(IntWritable year) {
		this.year = year;
	}
	
	
	/* (non-Javadoc)
	 * @see java.lang.Object#equals(java.lang.Object)
	 * Overriding the equals method to equate two objects
	 */
	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		CompositeKey other = (CompositeKey) obj;
		if (stationID == null) {
			if (other.stationID != null)
				return false;
		} else if (!stationID.equals(other.stationID))
			return false;
		if (year == null) {
			if (other.year != null)
				return false;
		} else if (!year.equals(other.year))
			return false;
		return true;
	}
	
	/* (non-Javadoc)
	 * @see java.lang.Object#hashCode()
	 * Overriding the hashCode method to generate a hash suitable to differentiate
	 * composite keys
	 */
	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((stationID == null) ? 0 : stationID.hashCode());
		result = prime * result + ((year == null) ? 0 : year.hashCode());
		return result;
	}

	//Overriding compareTo method to define the criteria for comparison of keys
	@Override
	public int compareTo(CompositeKey objKey) {
		
		return this.stationID.toString().compareTo(objKey.getStationID().toString());
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		
		this.stationID.readFields(in);
		this.year.readFields(in);
		
	}

	@Override
	public void write(DataOutput out) throws IOException {
		
		this.stationID.write(out);
		this.year.write(out);
		
	}
	
}
