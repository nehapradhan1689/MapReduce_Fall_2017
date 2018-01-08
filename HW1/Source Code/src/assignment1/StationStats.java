package assignment1;

public class StationStats {
	private double sumOfTemp;
	private double count;
	private double average;
	
	public StationStats() {
		super();
		this.sumOfTemp = 0;
		this.count = 0;
		this.average = 0;
	}
	
	public StationStats(double initialTemp) {
		super();
		this.sumOfTemp = initialTemp;
		this.count = 1;
		this.average = 0;
	}
	
	public StationStats(double sumOfTemp, double count, double average) {
		super();
		this.sumOfTemp = sumOfTemp;
		this.count = count;
		this.average = average;
	}

	public double getSumOfTemp() {
		return sumOfTemp;
	}
	public void setSumOfTemp(double sumOfTemp) {
		this.sumOfTemp = sumOfTemp;
	}
	public double getCount() {
		return count;
	}
	public void setCount(double count) {
		this.count = count;
	}
	public double getAverage() {
		return average;
	}
	public void setAverage(double average) {
		this.average = average;
	}
	public void updateSumOfTemp(double observedValue) {
		this.sumOfTemp += observedValue;
	}
	public void updateCount() {
		this.count += 1;
	}
	public void updateCount(double count) {
		this.count += count;
	}
	public void calculateAverage() {
		this.average = this.sumOfTemp/this.count;
	}

	@Override
	public String toString() {
		return "StationStats [sumOfTemp=" + sumOfTemp + ", count=" + count + ", average=" + average + "]";
	}
	
	

}
