package ScottsTots;
import java.awt.Point;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.BitSet;

public class Entry implements Serializable{
	
	Object value;
	String bitmapindex;
//	ArrayList<Point> pointers;
	
	public Entry(Object value,String bitmapindex) {
		this.value = value;
		this.bitmapindex = bitmapindex;
		//this.pointers = new ArrayList<Point>();
	}
	
	public void addBit(String bit) {
		this.bitmapindex += bit;
	}
	
//	public void addPointer(Point p) {
//		this.pointers.add(p);
//	}
//	
	
	public String toString() {
		return value + " : " + bitmapindex;
		
	}
	

}
