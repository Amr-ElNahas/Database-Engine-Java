package ScottsTots;
import java.awt.Point;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Date;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.PriorityQueue;
import java.util.Properties;
import java.util.Queue;
import java.util.Set;
import java.util.Stack;
import java.util.Vector;

@SuppressWarnings("serial")
public class Page implements Serializable {
	static int fucking = 0;
	Table table;
	Vector<Hashtable<String, Object>> rows;
	int index;
	//static final int size = 3;
	boolean isFull;
	boolean isEmpty;

	public Page(Table table, int index) throws IOException {
		this.table = table;
		this.rows = new Vector<Hashtable<String, Object>>();
		this.index = index;
	}

	public void insertIntoPage(Hashtable<String, Object> htblColNameValue) {
		Properties prop = new Properties();
		try {
			prop.load(new FileInputStream("config/DBApp.properties"));
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		int size= Integer.parseInt(prop.getProperty("maximumPageSize"));
		if (rows.size() == size - 1)
			isFull = true;

		rows.add(htblColNameValue);
	}

	@SuppressWarnings({ "rawtypes", "unchecked", "unused" })
	public void insertIntoPageSorted(Hashtable<String, Object> htblColNameValue, String primaryKey, boolean isFirstTime) throws Throwable {

		Point insertedPoint = new Point();

		System.out.println("**** Number of pages at insert:  " + DBApp.getTable(this.table.tableName).numberOfPages);

		System.out.println(rows.size());
		// int pageNum = 0;
		TEST: if (rows.size() == 0) {
			System.out.println("fady");
			rows.add(htblColNameValue);
			insertedPoint.x = this.index;
			insertedPoint.y = 0;
			DBApp.getTable(this.table.tableName).refreshPage(this);
			break TEST;
		} else {
			if (!isFull) {
				Properties prop = new Properties();
				try {
					prop.load(new FileInputStream("config/DBApp.properties"));
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				int size= Integer.parseInt(prop.getProperty("maximumPageSize"));
				if (rows.size() == size - 1)
					isFull = true;

				int SIZE = rows.size();
				for (int i = 0; i < SIZE; i++) {
					if ((((Comparable) rows.get(i).get(primaryKey)).compareTo(htblColNameValue.get(primaryKey))) >= 0) { // row
																															// >
																															// htblColNameValue
						Stack<Hashtable> temp = new Stack<Hashtable>();
						for (int j = SIZE - 1; j >= i; j--) {
							Hashtable<String, Object> row = rows.remove(j);
							temp.add(row);
						}
						rows.add(htblColNameValue);
						insertedPoint.x = this.index;
						insertedPoint.y = i;
						while (!temp.isEmpty()) {
							rows.add(temp.pop());
						}
						DBApp.getTable(this.table.tableName).refreshPage(this); //NOW

						System.out.println("AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA" + this.table.isIndexed("gpa"));
//								if(this.table.isIndexed("gpa")) {
//									this.UpdateBitmapIndex(insertedPoint,htblColNameValue,this.table.tableName,"gpa");
//								}
						break TEST;
					}
				}
				rows.add(htblColNameValue);
				insertedPoint.x = this.index;
				insertedPoint.y = size - 1;
				DBApp.getTable(this.table.tableName).refreshPage(this); //NOW

//						return;

			} else {

				Hashtable<String, Object> overflow;
				System.out.println("overflow");
				int SIZE = rows.size();
				for (int i = 0; i < SIZE; i++) {
					if ((((Comparable) rows.get(i).get(primaryKey)).compareTo(htblColNameValue.get(primaryKey))) >= 0) { // row
																															// >
																															// htblColNameValue
						Stack<Hashtable> temp = new Stack<Hashtable>();
						overflow = rows.remove(SIZE - 1);
						for (int j = SIZE - 2; j >= i; j--) {
							Hashtable<String, Object> row = rows.remove(j);
							temp.add(row);
						}
						rows.add(htblColNameValue);
						insertedPoint.x = this.index;
						insertedPoint.y = i;
						while (!temp.isEmpty()) {
							rows.add(temp.pop());
						}

						System.out.println(
								"debug should be 1 ======> " + DBApp.getTable(this.table.tableName).numberOfPages);

						System.out.println(
								"Check: " + index + " == " + DBApp.getTable(this.table.tableName).numberOfPages + "-1");

						if (index == DBApp.getTable(this.table.tableName).numberOfPages - 1) {
							System.out.println("create a new page with index = " + (index + 1));

							Page newPage = new Page(DBApp.getTable(this.table.tableName), index + 1);
							DBApp.getTable(this.table.tableName).savePage(newPage);
							System.out.println("before insert call :: " + newPage.rows);
						
							newPage.insertIntoPageSorted(overflow, primaryKey,false);
							insertedPoint.x = this.index;
							insertedPoint.y = i;
							DBApp.getTable(this.table.tableName).refreshPage(newPage);
						} else {
							System.out.println("next page = " + (index + 1) + "");
							
							Page p = this.table.getPage(index + 1);
							p.insertIntoPageSorted(overflow, primaryKey,false);
							insertedPoint.x = this.index;
							insertedPoint.y = i;
							table.refreshPage(p);
						}
						break TEST;
					}
				}
				if (index == DBApp.getTable(this.table.tableName).numberOfPages - 1) {
					System.out.println("create a new page");
					Page newPage = new Page(this.table, index + 1);
					this.table.savePage(newPage);
					newPage.insertIntoPageSorted(htblColNameValue, primaryKey,true);
					table.refreshPage(newPage);
				} else {
					System.out.println("next page");
					Page p = this.table.getPage(index + 1);
					p.insertIntoPageSorted(htblColNameValue, primaryKey,true);
					table.refreshPage(p);
				}
				break TEST;

			}
		}
		if(isFirstTime) {
			for (String key : htblColNameValue.keySet()) {
				System.out.println("UPDATE LOOP " + htblColNameValue.keySet() + " "+ key + " is indexed: " + this.table.isIndexed(key));
				if (htblColNameValue.get(key) != null && this.table.isIndexed(key)) {
					this.UpdateBitmapIndex(insertedPoint, htblColNameValue, this.table.tableName, key);
					System.out.println("INSIDE UPDATE 180 =====>>>>>>>> " + insertedPoint + " " + htblColNameValue.get(key) );
				}
			}
		}

	}

	public Hashtable<String, Object> searchForTuple(String key) {

		for (Hashtable<String, Object> p : rows) {
			if (p.get(key) != null) {
				return p;
			}
		}
		return null;
	}

	/*public void deleteFromPage(Hashtable<String, Object> htblColNameValue) throws IOException, ClassNotFoundException {
		int size = rows.size();
		Set<String> keys = htblColNameValue.keySet();
		int count = 0;
		if (keys.size() > 0) {
			// if (!isFull) {
			for (int i = 0; i < size; i++) {
				boolean delete = true;
				for (String key : keys) {
					if (((Comparable) htblColNameValue.get(key)).compareTo(rows.get(i).get(key)) != 0) {
						delete = false;
					}
				}
				if (delete) {
					// count++;
					rows.remove(i);
				}
			}
			if (rows.size() == 0) {
				isEmpty = true;
				Page deletedPage = table.getPage(index);
				this.table.removePage(deletedPage);
			}
			this.table.sortTable();
			/*
			 * } else { for (int i = 0; i < size; i++) { boolean delete = true; for (String
			 * key : keys) { if (((Comparable)
			 * htblColNameValue.get(key)).compareTo(rows.get(i).get(key)) != 0) { delete =
			 * false; } } if (delete) { count++; rows.remove(i); } } for(int
			 * j=0;j<count;j++){
			 * 
			 * } }
			 }
	}*/

	public boolean deleteFromPageSorted(Hashtable<String, Object> htblColNameValue)
			throws IOException, ClassNotFoundException {
		int size = rows.size();
		Set<String> keys = htblColNameValue.keySet();
		// int count = 0;
		boolean delete = true;
		boolean deletedPage = false;

		if (keys.size() > 0) {
			// if (!isFull) {
			for (int i = 0; i < rows.size(); i++) {
				delete = true;
				for (String key : keys) {
					if (rows.get(i).get(key) == null)
						continue;
					System.out.println(htblColNameValue.get(key));
					if (((Comparable) htblColNameValue.get(key)).compareTo(rows.get(i).get(key)) != 0) {
						delete = false;
					}
				}
				if (delete) {
					// count++;
					if (this.isFull)
						this.isFull = false;
					rows.remove(i);
					Point point = new Point(this.index, i);
					System.out.println("DELETETETETTE   >>>>> " + point);
					for (String key : htblColNameValue.keySet()) {
						if (htblColNameValue.get(key) != null && this.table.isIndexed(key)) {
							deleteBitmapIndex(point, this.table.tableName, key);
						}
					}
					i--;
					/* for(int w */
				}			
			}
			if (delete && (rows.size() == 0)) {
				deletedPage = true;

				System.out.print("I should delete now  :: ");
				System.out.println(this.table.tableName + "Page" + this.index + "");
				Table t = DBApp.getTable(this.table.tableName);
				t.numberOfPages--;
				System.out.println(t.numberOfPages);
				DBApp.updateFile(t);
				isEmpty = true;
//				File file = new File("StudentPage0");
//				File file2 = new File("DELETED");
//				boolean success = file.renameTo(file2);
				// System.out.println(success);
				File fileIn = new File("./data/" + this.table.tableName + "Page" + this.index + "");
				System.out.println(fileIn.getAbsolutePath());
				fileIn.delete();
			}
		}

		return deletedPage;
	}

	public void shiftLeft() throws ClassNotFoundException, IOException {
		int x = table.numberOfPages;
		int c = index;
		Properties prop = new Properties();
		try {
			prop.load(new FileInputStream("config/DBApp.properties"));
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		int size= Integer.parseInt(prop.getProperty("maximumPageSize"));
		for (; c < x; c++) {
			try {
				if (c + 1 < table.numberOfPages && table.getPage(c).rows != null
						&& table.getPage(c).rows.size() == (size - 1) && table.getPage(c + 1).rows.size() >= 1) {
					table.getPage(c).rows.add(table.getPage(c + 1).rows.remove(0));
				}
			} catch (Exception e) {
				System.out.println("193:" + index);
				System.out.println("194:" + c);
				System.out.println("195:" + x);
			}
			if (table.getPage(c).isFull && table.getPage(c).rows.size() < size) {
				table.getPage(c).isFull = false;
			}
			if (table.getPage(c).rows.size() == 0) {
				table.removePage(table.getPage(c));
			}
		}
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	public boolean updatePageSorted(String strKey, Hashtable<String, Object> htblColNameValue) throws Throwable {
		for (Hashtable<String, Object> row : rows) {
			System.out.println(row);
			if (((Comparable) row.get(table.primarykey) + "").compareTo(strKey) == 0) {
				Set<String> keys = htblColNameValue.keySet();
				Hashtable<String, Object> newRow = new Hashtable<String, Object>(row);
				System.out.println(":: rows before delete ::");
				for (Hashtable<String, Object> i : this.rows)
					System.out.println(i);
				deleteFromPageSorted(row);
				Point p = new Point(this.index, rows.indexOf(row));
//				if(this.table.isIndexed("gpa")) {
//					deleteBitmapIndex(p,this.table.tableName,"gpa");
//				}
//				
				this.table.refreshPage(this);
				System.out.println(":: rows after delete ::");
				for (Hashtable<String, Object> i : this.rows)
					System.out.println(i);
				for (String key : keys) {
					newRow.put(key, htblColNameValue.get(key));
				}

				insertIntoPageSorted(newRow, table.primarykey, true);
//				if(this.table.isIndexed("gpa")) {
//					this.UpdateBitmapIndex(insertedPoint,newRow,this.table.tableName,"gpa");
//				}
				this.table.refreshPage(this);
				return true;
			}
		}
		return false;
	}

	public void UpdateBitmapIndex(Point p, Hashtable<String, Object> row, String tableName, String columnName)
			throws Throwable {
		Index index = DBApp.getIndex(tableName, columnName);
		
		//if(index.numberOfPages == 1 && index.getPage(0).rows.size() == 0)
			
		
		boolean foundDuplicate = false;
		for (int i = 0; i < index.numberOfPages; i++) {
			IndexPage indexPage = index.getPage(i);
			for (Entry e : indexPage.rows) {
				System.out.println("line 305 ===*******&#^$^#^#^$^@$^@$^$#&$&$&$%&===");
				// System.out.println(e.value.substring(0, 3) + " 55555555555 " +
				// row.get(columnName) + " " +
				// e.value.substring(0,3).equals(row.get(columnName).substring(0,3)));
				if (e.value.equals(row.get(columnName))) {
					foundDuplicate = true;
					System.out.println("line 306 ======");
					for (int j = 0; j < e.bitmapindex.length() - 1; j++) {
						System.out.println("!!!!!!!!!!:::::     " + e.bitmapindex);

						if (e.bitmapindex.charAt(j) == '1' && e.bitmapindex.charAt(j + 1) == '0') {
							System.out.println("line 305 ===55555555555555555555555555555555===");

							index.pointers.add(j, p);
							char[] charArr = e.bitmapindex.toCharArray();
							charArr[j + 1] = '1';
							String s = "";
							for (char c : charArr) {
								s += c;
							}
							e.bitmapindex = s;
							System.out.println(e.bitmapindex);
							index.refreshPage(indexPage);
							break;

						}
					}
					
//					  if(e.bitmapindex.charAt(e.bitmapindex.length()-1) == '1') { 
//						  System.out.println("TRUEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEE986216498162981624912649862149861289461298461982649826498126491264981264981624986129846129846129846129864");
//						  for(int l = 0; l<index.numberOfPages;l++) { 
//							  IndexPage _indexPage = index.getPage(l);
//					  for(Entry _e:_indexPage.rows) { 
//						  if(l==index.numberOfPages-1 && _indexPage.rows.indexOf(_e) == 3-1) 
//							  _e.addBit("1"); 
//						  else 
//							  _e.addBit("0"); }
//					  
//						  }
//						 }
					 
					boolean flag = true;
					for (int k = 0; k < index.numberOfPages; k++) {
						IndexPage _indexPage = index.getPage(k);

						for (Entry _e : _indexPage.rows) {
							if (_e.value.equals(e.value)) {
								System.out.println("line 373 static call: " + fucking++);
								flag = false;
								_e.addBit("0");
							} else if ((!_e.value.equals(e.value)) && flag) {
								System.out.println("line 378 static call: " + fucking++);
								_e.addBit("0");
							} else {
								_e.bitmapindex = "0" + _e.bitmapindex;
								System.out.println("line 383 static call: " + fucking++);
							}

						}
						index.refreshPage(_indexPage);
						// return;
					}

				}
			}
			// index.refreshPage(indexPage);
		}
		DBApp.updateIndex(index);

		if (!foundDuplicate) {
			System.out.println(" LINE 394***************** UNIQUEEEEEEEE VALUEEEEEEEEEEE ");
			Entry unique = new Entry(row.get(columnName), "");
		
			index = DBApp.getIndex(tableName, columnName);
			index.shufflePointers(columnName);
			//DBApp.updateIndex(index);
			index.insertIntoIndex(unique);

			boolean flag = true;
			for (int i = 0; i < index.numberOfPages; i++) {
				IndexPage indexPage = index.getPage(i);

				for (Entry e : indexPage.rows) {
					if (((Comparable) e.value).compareTo(unique.value) == 0) {
						flag = false;
						for (Point point : DBApp.getIndex(tableName, columnName).pointers) {
							System.out.println("uniquwwwwwwhfiuafhsiufhisdh: " + index.pointers);

							System.out.println(
									(DBApp.getTable(tableName).getPage(point.x).rows.get(point.y).get(columnName)
											+ ""));
							System.out.println("idek whhy: " + DBApp.getIndex(tableName, columnName).pointers);
							if (((Comparable) DBApp.getTable(tableName).getPage(point.x).rows.get(point.y).get(columnName)).compareTo(unique.value) == 0) {
								e.addBit("1");
							} else {
								e.addBit("0");
							}
						}
					} else if ((! (((Comparable) e.value).compareTo(unique.value) == 0) )&& flag) {
						e.addBit("0");
					} else {
						e.bitmapindex = "0" + e.bitmapindex;
					}
					index.refreshPage(indexPage);

				}
				// index.refreshPage(_indexPage);
				// return;
			}

		}
	}

	public void deleteBitmapIndex(Point p, String tableName, String columnName)
			throws ClassNotFoundException, IOException {
		Index index = DBApp.getIndex(tableName, columnName);
		int pos = -1;
		// Point returnPoint = new Point();
		for (int i = 0; i < index.pointers.size(); i++) {
			if (index.pointers.get(i).x == p.x && index.pointers.get(i).y == p.y) {
				pos = i;
				// returnPoint = index.pointers.get(i);
				index.pointers.remove(pos);
				break;
			}
		}
		for (int i = 0; i < index.numberOfPages; i++) {
			List<Entry> toRemove = new ArrayList<Entry>();
			IndexPage indexPage = index.getPage(i);
			for (Entry e : indexPage.rows) {
				String s = e.bitmapindex.substring(0, pos);
				String s1 = e.bitmapindex.substring(pos + 1);
				System.out.println("INSIDE DELLLEEETETTETETE");
				System.out.println(s + s1);
				e.bitmapindex = s + s1;
				System.out.println("line 459: IM HERE");

				boolean isAllZeros = true;
				for (int j = 0; j < e.bitmapindex.length(); j++) {
					if (e.bitmapindex.charAt(j) == '1') {
						isAllZeros = false;
					}
				}
				System.out.println("line 467: IM HERE");

				if (isAllZeros) {
					System.out.println("line 470: IM HERE");

					// indexPage.rows.remove(e);
					for (Entry temp : indexPage.rows) {
						if (temp.equals(e)) {
							toRemove.add(temp);
						}
					}
					System.out.println("line 480 im heeerrerere");
				}
			}
			indexPage.rows.removeAll(toRemove);
			index.refreshPage(indexPage);
		}
		DBApp.updateIndex(index);
		// return returnPoint;
	}
	
	public Vector <Hashtable<String, Object>> searchRows(Hashtable <String, Object> row, String operator) throws DBAppException{
		Object[] keys = row.keySet().toArray();
		String key = (String) (keys[0]);
		//System.out.println("Row key: "+ key);
		Vector <Hashtable<String, Object>>list = new Vector<Hashtable<String, Object>>();
		//System.out.println("Row Needed: " + row.toString());
		for(int i = 0;i < rows.size(); i++){
			//System.out.println("Rows size: " + rows.size()); 
			Hashtable<String, Object> rowFound = rows.get(i);
			//System.out.println("Row Found: " + rowFound.toString());
			Object obj = rowFound.get(key);
			if(obj!=null){
			switch(operator){
			case ("="): {
				if(obj.equals(row.get(key))){
					list.add(rowFound);
			} break;
			}
			case ("!="): {
				if(!obj.equals(row.get(key))){
					list.add(rowFound);
				} break;
			}
			case (">"): {
				Comparable compare = (Comparable) obj;
				Comparable compared = (Comparable) row.get(key);
				if((compare.compareTo(compared))>0){
					list.add(rowFound);
					//System.out.println("Added row: "+ rowFound.toString());
				}
				break;
			}
			case ("<"): {
				Comparable compare = (Comparable) obj;
				Comparable compared = (Comparable) row.get(key);
				if((compare.compareTo(compared))<0){
					list.add(rowFound);
					//System.out.println("Added row: "+ rowFound.toString());
				}
				break;
			}
			case ("<="): {
				Comparable compare = (Comparable) obj;
				Comparable compared = (Comparable) row.get(key);
				if((compare.compareTo(compared))<=0){
					list.add(rowFound);
				//	System.out.println("Added row: "+ rowFound.toString());
				}
				break;
			}
			case (">="): {
				Comparable compare = (Comparable) obj;
				Comparable compared = (Comparable) row.get(key);
				if((compare.compareTo(compared))>=0){
					list.add(rowFound);
					//System.out.println("Added row: "+ rowFound.toString());
				}
				break;
			}
			default : throw new DBAppException ("Undefined comparator");	
			//System.out.println("Object: " + obj.toString());
				}
			}
		}
		return list;
	}
	
	

}
