package assignment1;

import java.util.List;

public class AverageTMAXFineLock extends Thread {
	private List<String> linesToProcess;

	public AverageTMAXFineLock(List<String> linesToProcess) {
		this.linesToProcess = linesToProcess;
	}

	//Multi-threaded computation of average temperature by station id with fine lock on data structure
	//Data structure is locked when a new key is added, but only entries are locked otherwise.
	public void run() {
		for(String line : linesToProcess) {
			String[] recordParts = line.split(",");
			String obsType = recordParts[ConstantClass.REC_OBSV_TYPE_ID];
			if(obsType.equals("TMAX")) {
				String key = recordParts[ConstantClass.REC_STATION_ID];
				double obsValue = Double.parseDouble(recordParts[ConstantClass.REC_OBSV_VALUE_ID]);

				StationStats tempStat = ConstantClass.FINE_LOCK_ACCUMULATOR.get(key);
				if(tempStat == null) {
					synchronized(ConstantClass.FINE_LOCK_ACCUMULATOR) {
						tempStat = new StationStats(obsValue);	
						if(InputLoader.MAKE_EXPENSIVE_RUN.equals("Y")) {
							ComputeFibonacci.fibonacci(17);
						}
						ConstantClass.FINE_LOCK_ACCUMULATOR.put(key, tempStat);
					}
				}
				else {

					synchronized (tempStat) {
						tempStat.updateSumOfTemp(obsValue);
						tempStat.updateCount();
						tempStat.calculateAverage();
						if(InputLoader.MAKE_EXPENSIVE_RUN.equals("Y")) {
							ComputeFibonacci.fibonacci(17);
						}
						ConstantClass.FINE_LOCK_ACCUMULATOR.put(key, tempStat);
					}
				}			
			}			
		}		
	}
}
