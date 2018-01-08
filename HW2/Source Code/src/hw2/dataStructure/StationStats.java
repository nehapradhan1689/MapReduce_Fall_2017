package hw2.dataStructure;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

//Custom data structure to store intermediate values of temperature
//for each station id
public class StationStats implements Writable{
	private Text obsvType;
	private DoubleWritable temperature;

	//Constructors
	public StationStats() {
		this.obsvType = new Text();
		this.temperature = new DoubleWritable(); 
	}

	public StationStats(String obsvType, double temperature) {
		super();
		this.obsvType = new Text(obsvType);
		this.temperature = new DoubleWritable(temperature);
	}

	//Getters and Setters
	public Text getObsvType() {
		return obsvType;
	}



	public DoubleWritable getTemperature() {
		return temperature;
	}



	public void setObsvType(Text obsvType) {
		this.obsvType = obsvType;
	}



	public void setTemperature(DoubleWritable temperature) {
		this.temperature = temperature;
	}


	@Override
	public void readFields(DataInput in) throws IOException {
		
		this.obsvType.readFields(in);
		this.temperature.readFields(in);
	}

	@Override
	public void write(DataOutput out) throws IOException {
		
		this.obsvType.write(out);
		this.temperature.write(out);	
	}
}
