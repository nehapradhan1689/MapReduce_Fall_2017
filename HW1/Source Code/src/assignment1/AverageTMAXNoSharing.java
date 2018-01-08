package assignment1;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class AverageTMAXNoSharing extends Thread {
	private List<String> linesToProcess;
	public Map<String, StationStats> mapDS = new LinkedHashMap<String, StationStats>();

	public AverageTMAXNoSharing(List<String> linesToProcess) {
		this.linesToProcess = linesToProcess;
	}

	//Multi-threaded computation of average temperature by station id with no sharing of data structure
	//This results in a data structure created for every thread that was spawned. The results of each
	//thread must be combined to obtain the final results.
	public void run() {
		for(String line : linesToProcess) {
			String[] recordParts = line.split(",");
			String key = recordParts[ConstantClass.REC_STATION_ID];
			String obsType = recordParts[ConstantClass.REC_OBSV_TYPE_ID];
			if(obsType.equals("TMAX")) {
				double obsValue = Double.parseDouble(recordParts[ConstantClass.REC_OBSV_VALUE_ID]);
				StationStats tempStat = mapDS.get(key);
				if(tempStat == null) {
					tempStat = new StationStats();						
				}
				tempStat.updateSumOfTemp(obsValue);
				tempStat.updateCount();
				tempStat.calculateAverage();
				if(InputLoader.MAKE_EXPENSIVE_RUN.equals("Y")) {
					ComputeFibonacci.fibonacci(17);
				}
				mapDS.put(key, tempStat);
			}			
		}		
	}

	public Map<String, StationStats> getMapDS() {
		return mapDS;
	}

	public void setMapDS(Map<String, StationStats> mapDS) {
		this.mapDS = mapDS;
	}

}
