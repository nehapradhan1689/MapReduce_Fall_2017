package assignment1;

import java.util.List;

public class AverageTMAXSeq {
	private List<String> linesToProcess;

	public AverageTMAXSeq(List<String> linesToProcess) {
		this.linesToProcess = linesToProcess;
	}

	//Sequential computation of average temperature by station id
	public void findAvgTMAXSeq() {
		
		for(String line : linesToProcess) {
			String[] recordParts = line.split(",");
			String key = recordParts[ConstantClass.REC_STATION_ID];
			String obsType = recordParts[ConstantClass.REC_OBSV_TYPE_ID];
			if(obsType.equals("TMAX")) {
				double obsValue = Double.parseDouble(recordParts[ConstantClass.REC_OBSV_VALUE_ID]);
				StationStats tempStat = ConstantClass.SEQACCUMULATOR.get(key);
				if(tempStat == null) {
					tempStat = new StationStats();						
				}
				tempStat.updateSumOfTemp(obsValue);
				tempStat.updateCount();
				tempStat.calculateAverage();
				if(InputLoader.MAKE_EXPENSIVE_RUN.equals("Y")) {
					ComputeFibonacci.fibonacci(17);
				}
				ConstantClass.SEQACCUMULATOR.put(key, tempStat);
			}			
		}
	}
}
