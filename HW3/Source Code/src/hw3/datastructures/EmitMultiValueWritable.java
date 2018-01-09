package hw3.datastructures;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.GenericWritable;
import org.apache.hadoop.io.Writable;

public class EmitMultiValueWritable extends GenericWritable{
	
	private static Class[] CLASSES = new Class[] {
			Page.class,
			DoubleWritable.class
	};
	
	public EmitMultiValueWritable() {
	}
	
	public EmitMultiValueWritable(Writable value) {
		set(value);
	}

	@Override
	protected Class<? extends Writable>[] getTypes() {
		return CLASSES;
	}
}
