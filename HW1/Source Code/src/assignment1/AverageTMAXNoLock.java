package assignment1;

import java.util.List;

public class AverageTMAXNoLock extends Thread {
	private List<String> linesToProcess;

	public AverageTMAXNoLock(List<String> linesToProcess) {
		this.linesToProcess = linesToProcess;
	}
	
	//Multi-threaded computation of average temperature by station id with no locks on data structure
	public void run() {
		for(String line : linesToProcess) {
			String[] recordParts = line.split(",");
			String key = recordParts[ConstantClass.REC_STATION_ID];
			String obsType = recordParts[ConstantClass.REC_OBSV_TYPE_ID];
			if(obsType.equals("TMAX")) {
				double obsValue = Double.parseDouble(recordParts[ConstantClass.REC_OBSV_VALUE_ID]);
				StationStats tempStat = ConstantClass.ACCUMULATOR.get(key);
				if(tempStat == null) {
					tempStat = new StationStats();
				}
				tempStat.updateSumOfTemp(obsValue);
				tempStat.updateCount();
				tempStat.calculateAverage();
				if(InputLoader.MAKE_EXPENSIVE_RUN.equals("Y")) {
					ComputeFibonacci.fibonacci(17);
				}
				ConstantClass.ACCUMULATOR.put(key, tempStat);
			}			
		}		
	}
}
