package ScottsTots;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.Enumeration;
import java.util.Hashtable;
import java.util.LinkedList;
import java.util.Queue;
import java.util.Vector;

public class Table implements Serializable {
	// Vector<Hashtable<String,Object>> tuples;
//	Vector<Page> pages;
	String tableName;
	String primarykey;
	Hashtable<String, String> dataTypes;
	int numberOfPages;
	String[] pagesFilePaths;

	public Table(String tableName, String primarykey, Hashtable<String, String> dataTypes) throws IOException {

		dataTypes.put("TouchDate", "java.lang.Date");
		// this.pages = new Vector<Page>();
		this.tableName = tableName;
		this.primarykey = primarykey;
		this.dataTypes = dataTypes;

		pagesFilePaths = new String[200];
		DBApp.saveTable(this);

		try {
			System.out.println("Insinde the constructor before save page (should be 0)   =>"
					+ DBApp.getTable(tableName).numberOfPages);

		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		Page page = new Page(this, 0);
		savePage(page);

		try {
			System.out.println("Insinde the constructor after save page (should be 1)   =>"
					+ DBApp.getTable(tableName).numberOfPages);

		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		writeCSV(tableName, primarykey, dataTypes);

		// pages.add(page);
		// DBApp.tables.add(this);
	}

	public void insertIntoTable(Hashtable<String, Object> htblColNameValue) throws IOException, ClassNotFoundException {
		for (int i = 0; i < numberOfPages; i++) {
			Page p = this.getPage(i);
			if (!p.isFull) {
				p.insertIntoPage(htblColNameValue);
				this.refreshPage(p);
				return;
			}
		}

		Page newPage = new Page(this, 0);

		// pages.add(newPage);
		newPage.insertIntoPage(htblColNameValue);
		this.savePage(newPage);

	}

	@SuppressWarnings("unchecked")
	public void insertIntoTableSorted(Hashtable<String, Object> htblColNameValue)
			throws Throwable {

		Date date = new Date();
		htblColNameValue.put("TouchDate", date);
		
		
		if (this.numberOfPages == 1) {
			Page p = this.getPage(0);
			p.insertIntoPageSorted(htblColNameValue, this.primarykey,true);
			this.refreshPage(p);
		} else {
			for (int i = 1; i < this.numberOfPages; i++) {
				
				if (((Comparable) this.getPage(i).rows.get(0).get(primarykey)).compareTo(htblColNameValue.get(primarykey)) >= 0) {
					Page p = this.getPage(i - 1);
					p.insertIntoPageSorted(htblColNameValue, this.primarykey,true);
					this.refreshPage(p);
					return;
				}
				
			}
			Page p = this.getPage(this.numberOfPages - 1);
			p.insertIntoPageSorted(htblColNameValue, this.primarykey,true);
			this.refreshPage(p);

		}

	}

	/*
	 * public void updateTable(Hashtable<String, Object> htblColNameValue, String
	 * key ) throws ClassNotFoundException, IOException { for(int i = 0;i<
	 * numberOfPages ;i++){ Page p = this.getPage(i); Hashtable<String, Object>
	 * foundPage = p.searchForTuple(key); if(foundPage != null){
	 * 
	 * } } }
	 */

	public void updateTableSorted(String strKey, Hashtable<String, Object> htblColNameValue)
			throws Throwable {
		Date date = new Date();
		htblColNameValue.put("TouchDate", date);
		for (int i = 0; i < numberOfPages; i++) {
			Page p = DBApp.getTable(this.tableName).getPage(i);
			if (p.updatePageSorted(strKey, htblColNameValue))
				return;
		}
	}

	/*public void deleteFromTable(Hashtable<String, Object> htblColNameValue) throws IOException, ClassNotFoundException {
		for (int i = 0; i < numberOfPages; i++) {
			Page p = this.getPage(i);
			p.deleteFromPage(htblColNameValue);
			refreshPage(p);
		}
		sortTable();
	}*/

	public void deleteFromTableSorted(Hashtable<String, Object> htblColNameValue)
			throws IOException, ClassNotFoundException {
		int num = DBApp.getTable(this.tableName).numberOfPages;
		for (int i = 0; i < num; i++) {
			num = DBApp.getTable(this.tableName).numberOfPages;
			System.out.println("getPage(  " + num + "  )");
			Page p = this.getPage(i);
			boolean deletedPage = p.deleteFromPageSorted(htblColNameValue);
			num = DBApp.getTable(this.tableName).numberOfPages;

			// p = this.getPage(i);
			if (!deletedPage)
				refreshPage(p);
		}
	}

	/*public void sortTable() throws IOException, ClassNotFoundException {
		Queue<Hashtable<String, Object>> q = new LinkedList<Hashtable<String, Object>>();
		Vector<Page> newPages = new Vector<Page>();
		Vector<Hashtable<String, Object>> newRows = new Vector<Hashtable<String, Object>>();
		for (int i = 0; i < numberOfPages; i++) {
			Page page = this.getPage(i);
			for (Hashtable<String, Object> row : page.rows) {
				q.add(row);
			}
		}
		int size = q.size();
		int c = 0;
		while (!q.isEmpty()) {
			Page x = new Page(this, c);
			for (int j = 0; j < config.maximumRowsCountInPage; j++) {
				newRows.add(q.remove());
				if (q.isEmpty()) {
					break;
				}
			}
			x.rows = newRows;
			newPages.add(x);
		}
		for (int i = 0; i < numberOfPages; i++) {
			refreshPage(newPages.get(i));
		}
		for (int i = numberOfPages; i < newPages.size(); i++) {
			if (newPages.get(i) != null) {
				savePage(newPages.get(i));
			}
		}
	}*/

	public Page getPage(int index) throws IOException, ClassNotFoundException {
		File dir = new File("./data");
		File[] filesList = dir.listFiles();
		ArrayList<String> pages = new ArrayList<String>();
		for (File file : filesList) {
			if (file.isFile() && file.getName().startsWith(this.tableName + "Page")) {
				// System.out.println(file.getName());
				pages.add(file.getName());
			}
		}
		Collections.sort(pages);
		// System.out.println(pages);

		// FileInputStream fileIn = new FileInputStream("data/" + this.tableName +
		// "Page" + index+ "");
		FileInputStream fileIn = new FileInputStream("./data/" + pages.get(index));
		ObjectInputStream in = new ObjectInputStream(fileIn);
		Page p = null;
		try {
			p = (Page) in.readObject();
		} catch (IOException e) {
			System.out.println("Couldn't find file");
		} catch (ClassNotFoundException e) {
			System.out.println("Couldn't find class Page");
		}

		in.close();
		fileIn.close();
		return p;

	}

	public void savePage(Page p) throws IOException {

		try {

			System.out.println("(savePageb4)==> " + DBApp.getTable(this.tableName).numberOfPages);

			FileOutputStream fileOut = new FileOutputStream("./data/" + this.tableName + "Page" + (numberOfPages) + "");
			ObjectOutputStream out = new ObjectOutputStream(fileOut);
			out.writeObject(p);
			// pagesFilePaths[numberOfPages] = this.tableName+"Page"+(numberOfPages);
			Table t = DBApp.getTable(this.tableName);
			t.numberOfPages++;
			DBApp.updateFile(t);
			System.out.println("(savePageafter)==> " + DBApp.getTable(this.tableName).numberOfPages);

			// DBApp.updateFile(this);
			out.flush();
			out.close();
			fileOut.flush();
			fileOut.close();

		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		}
	}

	public void refreshPage(Page p) throws IOException {

		FileOutputStream fileOut = new FileOutputStream("./data/" + this.tableName + "Page" + (p.index) + "");
		ObjectOutputStream out = new ObjectOutputStream(fileOut);
		out.writeObject(p);
		// pagesFilePaths[p.index] = this.tableName+"Page"+(p.index);
		fileOut.flush();
		out.flush();
		fileOut.close();
		out.close();
	}

	public void removePage(Page p) throws ClassNotFoundException, IOException {
		int currentIndex = p.index;
		for (int i = currentIndex; i < numberOfPages; i++) {
			Page nextPage = getPage(i + 1);
			nextPage.index--;
			refreshPage(nextPage);
		}
	}

	/*
	 * public Page getPages(int index) { for (Page page : pages) { if(page.index ==
	 * index) { return page; } } return null; }
	 */

	public String toString() {
		String s = "";
		int i = 0;
		try {
			for (int j = 0; j < numberOfPages; j++) {
				Page page;
				page = getPage(j);

				s += "Page " + page.index + " \n";
				for (Hashtable<String, Object> record : page.rows) {
					s += record + " \n";
				}
			}
		} catch (ClassNotFoundException | IOException e) {
			e.printStackTrace();
		}
		return s;
	}

	public static void writeCSV(String tableName, String primarykey, Hashtable<String, String> dataTypes)
			throws IOException {

		String csvBody = "";
		FileWriter outputfile = new FileWriter("./data/metadata.csv", true);

		Enumeration<String> columnNames = dataTypes.keys();
		Enumeration<String> columnDataTypes = dataTypes.elements();

		for (int i = 0; i < dataTypes.size(); i++) {

			csvBody += tableName;
			csvBody += ",";
			String thisKey = columnNames.nextElement();
			csvBody += thisKey;
			csvBody += ",";
			csvBody += columnDataTypes.nextElement();
			csvBody += ",";
			csvBody += (thisKey.equals(primarykey)) ? "True" : "False";
			csvBody += ",";
			csvBody += "False";
			csvBody += "\n";

			outputfile.append(csvBody);
			csvBody = "";

		}

		outputfile.flush();
		outputfile.close();

	}

	public boolean isKey(String coloumnName) throws IOException {

		FileReader inputfile = new FileReader("data/metadata.csv");
		BufferedReader reader = new BufferedReader(inputfile);

		String[] coloumnValues;

		while (reader.ready()) {
			String line = reader.readLine();
			coloumnValues = line.split(",");
			if (coloumnValues[0].equals(this.tableName) && coloumnValues[1].equals(coloumnName)
					&& coloumnValues[3].equals("True")) {
				return true;
			}
		}

		return false;
	}

	public boolean isIndexed(String coloumnName) throws IOException {

		FileReader inputfile = new FileReader("data/metadata.csv");
		BufferedReader reader = new BufferedReader(inputfile);

		String[] coloumnValues;

		while (reader.ready()) {
			String line = reader.readLine();
	//		System.out.println(line);
			coloumnValues = line.split(",");
	//		System.out.println(coloumnValues[0].equals(this.tableName) +" "+ coloumnValues[1].equals(coloumnName) +" "+ coloumnValues[4].substring(0, 4));
			if (coloumnValues[0].equals(this.tableName) && coloumnValues[1].equals(coloumnName) && coloumnValues[4].substring(0, 4).equals("True")) {
//				System.out.println("TTRRRRRUUUUUEEEE");
				return true;
			}
		}

		return false;
	}
	
	public String checkType(String coloumnName) throws IOException {

		FileReader inputfile = new FileReader("data/metadata.csv");
		BufferedReader reader = new BufferedReader(inputfile);

		String[] coloumnValues;

		while (reader.ready()) {
			String line = reader.readLine();
	//		System.out.println(line);
			coloumnValues = line.split(",");
			if (coloumnValues[0].equals(this.tableName) && coloumnValues[1].equals(coloumnName)) {
				String splitted[] = coloumnValues[2].split(".");
				return splitted[2];
				
			}
		}

		return null;
	}
	
	

}
