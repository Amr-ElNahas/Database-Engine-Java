package ScottsTots;
import java.awt.Point;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.Hashtable;

public class Index implements Serializable {

	String tableName;
	String coloumnName;

	int numberOfPages;
	ArrayList<Point> pointers = new ArrayList<Point>();
	// String[] pagesFilePaths;

	static final int size = 3;

	public Index(String tableName, String coloumnName) throws Throwable {
		this.tableName = tableName;
		this.coloumnName = coloumnName;

		DBApp.saveIndex(this);
		try {

			IndexPage page = new IndexPage(this, 0);
			savePage(page);

		} catch (Throwable e) {
			e.printStackTrace();
		}

		this.createBitmapIndex();
	}

	public void savePage(IndexPage p) throws Throwable {
		FileOutputStream fileOut = new FileOutputStream(
				"./data/" + this.tableName + "Index" + this.coloumnName + "Page" + (p.i) + "");
		ObjectOutputStream out = new ObjectOutputStream(fileOut);
		out.writeObject(p);
		// pagesFilePaths[numberOfPages] =
		// this.tableName+"_"+this.coloumnName+"Page"+(numberOfPages);
		Index i = DBApp.getIndex(this.tableName, this.coloumnName);
		i.numberOfPages++;
		DBApp.updateIndex(i);
		fileOut.close();
		out.close();
	}

	public IndexPage getPage(int index) throws IOException, ClassNotFoundException {
		File dir = new File("./data");
		File[] filesList = dir.listFiles();
		ArrayList<String> pages = new ArrayList<String>();

		for (File file : filesList) {
			if (file.isFile() && file.getName().startsWith(this.tableName + "Index" + this.coloumnName + "Page")) {
				// System.out.println(file.getName());
				pages.add(file.getName());
			}
		}
		Collections.sort(pages);

		//System.out.println("Array list returned by getPage(" + index + ") :: " + pages);

		FileInputStream fileIn = new FileInputStream("./data/" + pages.get(index));
		ObjectInputStream in = new ObjectInputStream(fileIn);
		IndexPage p = null;
		try {
			p = (IndexPage) in.readObject();
		} catch (IOException e) {
			System.out.println("Couldn't find file");
		} catch (ClassNotFoundException e) {
			System.out.println("Couldn't find class Page");
		}
	//	System.out.println("line 85 " + p.rows);
		in.close();
		fileIn.close();
		return p;

	}

	public void createBitmapIndex() throws Throwable {

		System.out.println("inside function");

		System.out.println(DBApp.getTable(this.tableName).isKey(this.coloumnName));

		if (DBApp.getTable(this.tableName).isKey(this.coloumnName)) {

			Table table = DBApp.getTable(tableName);

			for (int i = 0; i < table.numberOfPages; i++) {
				Page page = table.getPage(i);
				int rowNumber = 0;
				for (Hashtable<String, Object> row : page.rows) {
					// this.pointers.add(new Point(i,rowNumber));
					Object value = row.get(coloumnName) ;
					Entry e = new Entry(value, "");
					for (int j = 0; j < table.numberOfPages; j++) {
						Page _page = table.getPage(j);
						for (Hashtable<String, Object> _row : _page.rows) {
							if (value.equals(_row.get(coloumnName) )) {
								e.addBit("1");
								// e.addPointer(new Point(_page.index,rowNumber));
							} else {
								e.addBit("0");
								// e.addPointer(new Point(_page.index,rowNumber));
							}
							// rowNumber++;
						}
					}
					// System.out.println(this.getPage(0).rows);
//					System.out.println("Beofre update ====> " + DBApp.getIndex(this.tableName, this.coloumnName).pointers);
//					System.out.println("AFter update ====> " + DBApp.getIndex(this.tableName, this.coloumnName).pointers);
					DBApp.getIndex(this.tableName, this.coloumnName).insertIntoIndex(e);

					// this.refreshPage(this.getPage(0));
					// System.out.println(this.getPage(0).rows);
				}
			}

			// updating the pointers
			Index index = DBApp.getIndex(this.tableName, this.coloumnName);
			for (int i = 0; i < table.numberOfPages; i++) {
				Page page = table.getPage(i);
				int rowNumber = 0;
				for (Hashtable<String, Object> row : page.rows) {
					index.pointers.add(new Point(i, rowNumber));
					rowNumber++;
				}
			}
			DBApp.updateIndex(index); // ???

		} else {

			Table table = DBApp.getTable(tableName);

			Index index = DBApp.getIndex(this.tableName, this.coloumnName);
			for (int i = 0; i < table.numberOfPages; i++) {
				Page page = table.getPage(i);
				int rowNumber = 0;
				for (Hashtable<String, Object> row : page.rows) {
					index.pointers.add(new Point(i, rowNumber));
					rowNumber++;
				}
			}
			DBApp.updateIndex(index); // ???

			ArrayList<Object> tempRows = new ArrayList<Object>();

			for (int i = 0; i < table.numberOfPages; i++) {
				Page page = table.getPage(i);
				for (Hashtable<String, Object> row : page.rows) {
					tempRows.add(row.get(coloumnName));
				}
			}
			//System.out.println(tempRows);
			//System.out.println("Line 167::::   " + this.sortPoints(DBApp.getIndex(tableName, coloumnName).pointers, tempRows));
			
			index = DBApp.getIndex(this.tableName, this.coloumnName);
			index.pointers = this.sortPoints(DBApp.getIndex(tableName, coloumnName).pointers, tempRows);
			DBApp.updateIndex(index); // ???

			table = DBApp.getTable(tableName);

			for (int i = 0; i < table.numberOfPages; i++) {
				Page page = table.getPage(i);
				int rowNumber = 0;
				for (Hashtable<String, Object> row : page.rows) {
					// this.pointers.add(new Point(i,rowNumber));
					Object value = row.get(coloumnName) ;
					Entry e = new Entry(value, "");
					for (Point p : DBApp.getIndex(this.tableName, this.coloumnName).pointers) {
						System.out.println((DBApp.getTable(tableName).getPage(p.x).rows.get(p.y).get(coloumnName) + ""));

						if ((DBApp.getTable(tableName).getPage(p.x).rows.get(p.y).get(coloumnName) ).equals(value)) {
							e.addBit("1");
						} else {
							e.addBit("0");
						}
					}
					// System.out.println(this.getPage(0).rows);
//					System.out.println("Beofre update ====> " + DBApp.getIndex(this.tableName, this.coloumnName).pointers);
//					System.out.println("AFter update ====> " + DBApp.getIndex(this.tableName, this.coloumnName).pointers);
					DBApp.getIndex(this.tableName, this.coloumnName).insertIntoIndex(e);

					// this.refreshPage(this.getPage(0));
					// System.out.println(this.getPage(0).rows);
				}
			}

		}

	}

	public void insertIntoIndex(Entry e) throws Throwable {

		IndexPage p = DBApp.getIndex(this.tableName, this.coloumnName).getPage(0);
		p.insertIntoPageSorted(e);
		// this.refreshPage(p);

	}

	public void refreshPage(IndexPage p) throws IOException, ClassNotFoundException {
		FileOutputStream fileOut = new FileOutputStream(
				"./data/" + this.tableName + "Index" + this.coloumnName + "Page" + (p.i));
		ObjectOutputStream out = new ObjectOutputStream(fileOut);
		out.writeObject(p);
		System.out.println("line 138 " + p.rows);
		// pagesFilePaths[p.i] = this.tableName+"_"+this.coloumnName+"Page"+(p.index);
		fileOut.flush();
		out.flush();
		fileOut.close();
		out.close();
	}

	public String toString() {

		try {
			System.out.println("Pointers :" + this.pointers);
			for (int i = 0; i < this.numberOfPages; i++) {
				IndexPage page = getPage(i);
				System.out.println("Page " + page.i);
				for (Entry e : page.rows) {
					System.out.println(e.value + " : " + e.bitmapindex);
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
		}

		return "";

	}

	@SuppressWarnings("unchecked")
	public ArrayList<Point> sortPoints(ArrayList<Point> pointers, ArrayList<Object> rows) {
		ArrayList<Point> pointersCopy = new ArrayList<Point>(pointers);
		ArrayList<Point> newPointers = new ArrayList<Point>();
		ArrayList<Object> temp = new ArrayList<Object>(rows);
		ArrayList<Object> rowsCopy = new ArrayList<Object>(rows);

		while (!rowsCopy.isEmpty()) {
			int indexSmallest = -1;
			Object min = rowsCopy.get(0);
			for (Object s : rowsCopy) {
				if (((Comparable) s).compareTo(min) <= 0) {
					System.out.println("enter if");
					min = s;
					indexSmallest = temp.indexOf(s);

				}
			}
			temp.set(indexSmallest, null);
			rowsCopy.remove(min);
			System.out.println(rowsCopy);
			newPointers.add(pointersCopy.get(indexSmallest));

		}
		return newPointers;
	}
	
	
	public void shufflePointers(String coloumnName) throws Throwable {
		
		Table table = DBApp.getTable(this.tableName);
		Index index = DBApp.getIndex(this.tableName, this.coloumnName);
		index.pointers = new ArrayList<Point>();
		for (int i = 0; i < table.numberOfPages; i++) {
			Page page = table.getPage(i);
			int rowNumber = 0;
			for (Hashtable<String, Object> row : page.rows) {
				index.pointers.add(new Point(i, rowNumber));
				rowNumber++;
			}
		}
		//DBApp.updateIndex(index); // ???

		ArrayList<Object> tempRows = new ArrayList<Object>();

		for (int i = 0; i < table.numberOfPages; i++) {
			Page page = table.getPage(i);
			for (Hashtable<String, Object> row : page.rows) {
				tempRows.add(row.get(coloumnName));
			}
		}
		System.out.println("LINE293=======>>>>>>>>>>    >>> >> > TABLE");
		System.out.println(table);
		System.out.println("LINE293=======>>>>>>>>>>    >>> >> >"+tempRows);
		System.out.println("LINE293=======>>>>>>>>>>    >>> >> >"+index.pointers);

		//System.out.println("Line 167::::   " + this.sortPoints(DBApp.getIndex(tableName, coloumnName).pointers, tempRows));
		
		//index = DBApp.getIndex(this.tableName, this.coloumnName);
		index.pointers = this.sortPoints(index.pointers, tempRows);
		
		System.out.println("LINE293=======>>>>>>>>>>    >>> >> >"+index.pointers);

		DBApp.updateIndex(index); // ???

	}
//0.95, 1.25, 1.5, 0.88, 1.5, 1.25, 0.95]
}
