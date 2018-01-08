package assignment1;

import java.util.List;

public class AverageTMAXCoarseLock extends Thread{
	
	private List<String> linesToProcess;

	public AverageTMAXCoarseLock(List<String> linesToProcess) {
		this.linesToProcess = linesToProcess;
	}
	
	//Multi-threaded computation of average temperature by station id with coarse lock on data structure
	public void run() {
		for(String line : linesToProcess) {
			String[] recordParts = line.split(",");
			String key = recordParts[ConstantClass.REC_STATION_ID];
			String obsType = recordParts[ConstantClass.REC_OBSV_TYPE_ID];
			if(obsType.equals("TMAX")) {
				double obsValue = Double.parseDouble(recordParts[ConstantClass.REC_OBSV_VALUE_ID]);
				synchronized (ConstantClass.COARSE_LOCK_ACCUMULATOR) {
					StationStats tempStat = ConstantClass.COARSE_LOCK_ACCUMULATOR.get(key);
					if(tempStat == null) {
						tempStat = new StationStats();
					}
					tempStat.updateSumOfTemp(obsValue);
					tempStat.updateCount();
					tempStat.calculateAverage();
					if(InputLoader.MAKE_EXPENSIVE_RUN.equals("Y")) {
						ComputeFibonacci.fibonacci(17);
					}
					ConstantClass.COARSE_LOCK_ACCUMULATOR.put(key, tempStat);
				}
			}			
		}
	}

}
