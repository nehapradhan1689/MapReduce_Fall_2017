package hw3.datastructures;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

//This user-defined class represents each page and defines the page name, its 
//rank and information about its outlinks
public class Page implements Writable, WritableComparable<Page> {
	
	private Text pageName;
	private DoubleWritable pageRank;
	private IntWritable noOfOutlinks;
	private List<String> outlinks;
	
	//Constructors
	public Page() {
		this.pageName = new Text();
		this.pageRank = new DoubleWritable();
		this.noOfOutlinks = new IntWritable();
		this.outlinks = new ArrayList<String>();
	}

	public Page(Text pageName, DoubleWritable pageRank, IntWritable noOfOutlinks, List<String> outlinks) {
		this.pageName = pageName;
		this.pageRank = pageRank;
		this.noOfOutlinks = noOfOutlinks;
		this.outlinks = outlinks;
	}

	public Page(Text pageName, IntWritable noOfOutlinks, List<String> outlinks) {
		this.pageName = pageName;
		this.pageRank = new DoubleWritable(0.0);
		this.noOfOutlinks = noOfOutlinks;
		this.outlinks = outlinks;
	}	

	public Page(Text pageName) {
		this.pageName = pageName;
		this.pageRank = new DoubleWritable(0.0);
		this.noOfOutlinks = new IntWritable(0);
		this.outlinks = new ArrayList<String>();
	}

	//Getters and Setters
	public Text getPageName() {
		return pageName;
	}

	public DoubleWritable getPageRank() {
		return pageRank;
	}

	public IntWritable getNoOfOutlinks() {
		return noOfOutlinks;
	}

	public List<String> getOutlinks() {
		return outlinks;
	}

	public void setPageName(Text pageName) {
		this.pageName = pageName;
	}

	public void setPageRank(DoubleWritable pageRank) {
		this.pageRank = pageRank;
	}

	public void setNoOfOutlinks(IntWritable noOfOutlinks) {
		this.noOfOutlinks = noOfOutlinks;
	}

	public void setOutlinks(List<String> outlinks) {
		this.outlinks = outlinks;
	}
	
	@Override
	public void readFields(DataInput in) throws IOException {
		
		this.pageName.readFields(in);
		this.pageRank.readFields(in);
		this.noOfOutlinks.readFields(in);
		this.outlinks = new ArrayList<String>();
		for(int i = 0; i < noOfOutlinks.get(); i++) {
			Text incomingLink = new Text();
			incomingLink.readFields(in);
			this.outlinks.add(incomingLink.toString());
		}
		
	}

	@Override
	public void write(DataOutput out) throws IOException {
		
		this.pageName.write(out);
		this.pageRank.write(out);
		this.noOfOutlinks.write(out);
		for(String link : this.outlinks) {
			Text outLink = new Text(link);
			outLink.write(out);
		}
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((noOfOutlinks == null) ? 0 : noOfOutlinks.hashCode());
		result = prime * result + ((outlinks == null) ? 0 : outlinks.hashCode());
		result = prime * result + ((pageName == null) ? 0 : pageName.hashCode());
		result = prime * result + ((pageRank == null) ? 0 : pageRank.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		Page other = (Page) obj;
		if (noOfOutlinks == null) {
			if (other.noOfOutlinks != null)
				return false;
		} else if (!noOfOutlinks.equals(other.noOfOutlinks))
			return false;
		if (outlinks == null) {
			if (other.outlinks != null)
				return false;
		} else if (!outlinks.equals(other.outlinks))
			return false;
		if (pageName == null) {
			if (other.pageName != null)
				return false;
		} else if (!pageName.equals(other.pageName))
			return false;
		if (pageRank == null) {
			if (other.pageRank != null)
				return false;
		} else if (!pageRank.equals(other.pageRank))
			return false;
		return true;
	}

	@Override
	public String toString() {
		StringBuilder sb = new StringBuilder(pageName + "~" + pageRank + "~" + noOfOutlinks
				+ "~");
		for(int i = 0; i < noOfOutlinks.get(); i++) {
			if(i == (noOfOutlinks.get()-1)) {
				sb.append(outlinks.get(i));
			}
			else {
				sb.append(outlinks.get(i));
				sb.append("->");
			}
		}
		sb.append("~");
		return sb.toString();
	}

	public String toStringPageRank() {
		StringBuilder sb = new StringBuilder(pageName + "=" + pageRank);
		return sb.toString();
	}
	
	@Override
	public int compareTo(Page p) {
		
		return this.pageName.toString().compareTo(p.getPageName().toString());
				
	}
	
	
	
}
