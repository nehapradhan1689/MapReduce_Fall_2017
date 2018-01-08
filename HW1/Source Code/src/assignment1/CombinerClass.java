package assignment1;

import java.util.Map;

public class CombinerClass {
	private AverageTMAXNoSharing[] elementsToCombine;
	
			

	
	public CombinerClass(AverageTMAXNoSharing[] elementsToCombine) {
		super();
		this.elementsToCombine = elementsToCombine;
	}
	
	public void combiner() {
		for(AverageTMAXNoSharing element : elementsToCombine) {
			if(ConstantClass.NO_SHARING_ACCUMULATOR == null) {
				ConstantClass.NO_SHARING_ACCUMULATOR = element.getMapDS();
			}
			else {
				for(Map.Entry<String,StationStats> entry : element.getMapDS().entrySet()) {
					String key = entry.getKey();
					StationStats valueFromDS = entry.getValue();
					StationStats tempStats = ConstantClass.NO_SHARING_ACCUMULATOR.get(key);
					if(tempStats == null) {
						ConstantClass.NO_SHARING_ACCUMULATOR.put(key, valueFromDS);
					} else {
						tempStats.updateSumOfTemp(valueFromDS.getSumOfTemp());
						tempStats.updateCount(valueFromDS.getCount());
						tempStats.calculateAverage();
					}
				}

			}
		}
	
	}

}
