package ScottsTots;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.Enumeration;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.Vector;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.awt.Point;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.lang.Integer;

public class DBApp implements Serializable {
	// static Vector<Table> tables;
	static String[] tableFilePaths;
	static int numberOfTables;
	static int numberOfIndexes;
	static String[] indexFilePaths;

	public void init() throws IOException {
		// tables = new Vector<Table>();
		numberOfTables = 0;
		tableFilePaths = new String[200];
		numberOfIndexes = 0;
		indexFilePaths = new String[200];

		File file = new File("./data/metadata.csv");
		try {
			FileWriter outputfile = new FileWriter("./data/metadata.csv");

			String row = "Table Name, Column Name, Column Type, Key, Indexed \n";
			outputfile.append(row);
			outputfile.flush();
			outputfile.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
		Properties DBAppProperties = new Properties();
		DBAppProperties.setProperty("maximumPageSize", "3");
		DBAppProperties.setProperty("maximumIndexSize", "3");
		DBAppProperties.store(new FileOutputStream("config/DBApp.properties"), null);
		// tableFile = new FileOutputStream("/tables.ser");
	}

	public void createTable(String strTableName, String primarykey, Hashtable<String, String> htblColNameType)
			throws DBAppException, IOException {

		Hashtable<String, String> newHashtable = new Hashtable<String, String>(htblColNameType);

		Table table = new Table(strTableName, primarykey, newHashtable);
		// tables.add(table);
	}

	public void createBitmapIndex(String strTableName, String strColName) throws Throwable {

		try {

			FileReader inputfile = new FileReader("./data/metadata.csv");

			BufferedReader reader = new BufferedReader(inputfile);

			String[] coloumnValues;
			String body = "";

			while (reader.ready()) {
				String line = reader.readLine();
				coloumnValues = line.split(",");
				if (coloumnValues[0].equals(strTableName) && coloumnValues[1].equals(strColName)) {
					coloumnValues[4] = "True";
				}
				String newLine = "";
				for (int i = 0; i < coloumnValues.length - 1; i++) {
					newLine += coloumnValues[i] + ",";
				}
				newLine += coloumnValues[coloumnValues.length - 1];
				newLine += " \n";
				body += newLine;

			}

			System.out.println(body);

			FileWriter outputfile = new FileWriter("./data/metadata.csv");
			outputfile.write(body);

			outputfile.flush();
			outputfile.close();

		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		Index newIndex = new Index(strTableName, strColName);
		// saveIndex(newIndex);
		// newIndex.createBitmapIndex();

	}

	public void insertIntoTable(String strTableName, Hashtable<String, Object> htblColNameValue) throws Throwable {
		
		Hashtable<String, Object> newHashtable = new Hashtable<String, Object>(htblColNameValue);
		Set<String> keys = htblColNameValue.keySet();
		String[] coloumnValues;
		System.out.println("keys: "+ keys);
		for (String key : keys) {
			FileReader inputfile = new FileReader("./data/metadata.csv");
			BufferedReader reader = new BufferedReader(inputfile);
			while (reader.ready()) {
				String line = reader.readLine();
				coloumnValues = line.split(",");
				if (htblColNameValue.get(key) instanceof Double && coloumnValues[0].equals(strTableName) && coloumnValues[1].equals(key)
						&& !coloumnValues[2].equals("java.lang.double")) {
					throw new DBAppException(key+" should be of type double");
				}
				else if (htblColNameValue.get(key) instanceof Integer && coloumnValues[0].equals(strTableName) && coloumnValues[1].equals(key)
						&& !coloumnValues[2].equals("java.lang.Integer")) {
					throw new DBAppException(key+"should be of type Integer");
				} else if (htblColNameValue.get(key) instanceof Date && coloumnValues[0].equals(strTableName) && coloumnValues[1].equals(key)
						&& !coloumnValues[2].equals("java.lang.Date")) {
					throw new DBAppException(key+" should be of type Date");
				} else if (htblColNameValue.get(key) instanceof String && coloumnValues[0].equals(strTableName) && coloumnValues[1].equals(key)
						&& !coloumnValues[2].equals("java.lang.String")) {
					throw new DBAppException(key+" should be of type String");
				} else if (htblColNameValue.get(key) instanceof Boolean && coloumnValues[0].equals(strTableName) && coloumnValues[1].equals(key)
						&& !coloumnValues[2].equals("java.lang.Boolean")) {
					throw new DBAppException(key+" should be of type boolean");
				}
			}
		}
		Table table = DBApp.getTable(strTableName);
		// table.insertIntoTable(htblColNameValue2);
		table.insertIntoTableSorted(newHashtable);
		// updateFile(table);
	}

	/*
	 * public void updateTable(String strTableName, String strKey,
	 * Hashtable<String, Object> htblColNameValue2) throws DBAppException,
	 * IOException, ClassNotFoundException { Table table =
	 * getTable(strTableName); table.updateTable(htblColNameValue2,strKey);
	 * updateFile(table); }
	 */

	/*public void deleteFromTable(String strTableName, Hashtable<String, Object> htblColNameValue)
			throws DBAppException, IOException, ClassNotFoundException {
		Hashtable<String, Object> newHashtable = new Hashtable<String, Object>(htblColNameValue);
		Set<String> keys = htblColNameValue.keySet();
		String[] coloumnValues;
		System.out.println("keys: "+ keys);
		for (String key : keys) {
			FileReader inputfile = new FileReader("./data/metadata.csv");
			BufferedReader reader = new BufferedReader(inputfile);
			while (reader.ready()) {
				String line = reader.readLine();
				coloumnValues = line.split(",");
				if (htblColNameValue.get(key) instanceof Double && coloumnValues[0].equals(strTableName) && coloumnValues[1].equals(key)
						&& !coloumnValues[2].equals("java.lang.double")) {
					throw new DBAppException(key+" should be of type double");
				}
				else if (htblColNameValue.get(key) instanceof Integer && coloumnValues[0].equals(strTableName) && coloumnValues[1].equals(key)
						&& !coloumnValues[2].equals("java.lang.Integer")) {
					throw new DBAppException(key+"should be of type Integer");
				} else if (htblColNameValue.get(key) instanceof Date && coloumnValues[0].equals(strTableName) && coloumnValues[1].equals(key)
						&& !coloumnValues[2].equals("java.lang.Date")) {
					throw new DBAppException(key+" should be of type Date");
				} else if (htblColNameValue.get(key) instanceof String && coloumnValues[0].equals(strTableName) && coloumnValues[1].equals(key)
						&& !coloumnValues[2].equals("java.lang.String")) {
					throw new DBAppException(key+" should be of type String");
				} else if (htblColNameValue.get(key) instanceof Boolean && coloumnValues[0].equals(strTableName) && coloumnValues[1].equals(key)
						&& !coloumnValues[2].equals("java.lang.Boolean")) {
					throw new DBAppException(key+" should be of type boolean");
				}
			}
		}
		Table table = getTable(strTableName);
		table.deleteFromTable(newHashtable);
		// updateFile(table);
	}*/

	public void deleteFromTableSorted(String strTableName, Hashtable<String, Object> htblColNameValue)
			throws DBAppException, IOException, ClassNotFoundException {
		Hashtable<String, Object> newHashtable = new Hashtable<String, Object>(htblColNameValue);
		Set<String> keys = htblColNameValue.keySet();
		String[] coloumnValues;
		System.out.println("keys: "+ keys);
		for (String key : keys) {
			FileReader inputfile = new FileReader("./data/metadata.csv");
			BufferedReader reader = new BufferedReader(inputfile);
			while (reader.ready()) {
				String line = reader.readLine();
				coloumnValues = line.split(",");
				if (htblColNameValue.get(key) instanceof Double && coloumnValues[0].equals(strTableName) && coloumnValues[1].equals(key)
						&& !coloumnValues[2].equals("java.lang.double")) {
					throw new DBAppException(key+" should be of type double");
				}
				else if (htblColNameValue.get(key) instanceof Integer && coloumnValues[0].equals(strTableName) && coloumnValues[1].equals(key)
						&& !coloumnValues[2].equals("java.lang.Integer")) {
					throw new DBAppException(key+"should be of type Integer");
				} else if (htblColNameValue.get(key) instanceof Date && coloumnValues[0].equals(strTableName) && coloumnValues[1].equals(key)
						&& !coloumnValues[2].equals("java.lang.Date")) {
					throw new DBAppException(key+" should be of type Date");
				} else if (htblColNameValue.get(key) instanceof String && coloumnValues[0].equals(strTableName) && coloumnValues[1].equals(key)
						&& !coloumnValues[2].equals("java.lang.String")) {
					throw new DBAppException(key+" should be of type String");
				} else if (htblColNameValue.get(key) instanceof Boolean && coloumnValues[0].equals(strTableName) && coloumnValues[1].equals(key)
						&& !coloumnValues[2].equals("java.lang.Boolean")) {
					throw new DBAppException(key+" should be of type boolean");
				}
			}
		}
		Table table = getTable(strTableName);
		table.deleteFromTableSorted(newHashtable);
		// updateFile(table);
	}

	public void updateTable(String strTableName, String strKey, Hashtable<String, Object> htblColNameValue)
			throws Throwable {
		Hashtable<String, Object> newHashtable = new Hashtable<String, Object>(htblColNameValue);
		Set<String> keys = htblColNameValue.keySet();
		String[] coloumnValues;
		System.out.println("keys: "+ keys);
		for (String key : keys) {
			FileReader inputfile = new FileReader("./data/metadata.csv");
			BufferedReader reader = new BufferedReader(inputfile);
			while (reader.ready()) {
				String line = reader.readLine();
				coloumnValues = line.split(",");
				if (htblColNameValue.get(key) instanceof Double && coloumnValues[0].equals(strTableName) && coloumnValues[1].equals(key)
						&& !coloumnValues[2].equals("java.lang.double")) {
					throw new DBAppException(key+" should be of type double");
				}
				else if (htblColNameValue.get(key) instanceof Integer && coloumnValues[0].equals(strTableName) && coloumnValues[1].equals(key)
						&& !coloumnValues[2].equals("java.lang.Integer")) {
					throw new DBAppException(key+"should be of type Integer");
				} else if (htblColNameValue.get(key) instanceof Date && coloumnValues[0].equals(strTableName) && coloumnValues[1].equals(key)
						&& !coloumnValues[2].equals("java.lang.Date")) {
					throw new DBAppException(key+" should be of type Date");
				} else if (htblColNameValue.get(key) instanceof String && coloumnValues[0].equals(strTableName) && coloumnValues[1].equals(key)
						&& !coloumnValues[2].equals("java.lang.String")) {
					throw new DBAppException(key+" should be of type String");
				} else if (htblColNameValue.get(key) instanceof Boolean && coloumnValues[0].equals(strTableName) && coloumnValues[1].equals(key)
						&& !coloumnValues[2].equals("java.lang.Boolean")) {
					throw new DBAppException(key+" should be of type boolean");
				}
			}
		}
		Table table = getTable(strTableName);
		table.updateTableSorted(strKey, newHashtable);
		// updateFile(table);
	}

	public Iterator selectFromTable(SQLTerm[] arrSQLTerms1, String[] strarrOperators)
			throws DBAppException, ClassNotFoundException, IOException {

		String[] coloumnValues;
		for(SQLTerm term:arrSQLTerms1){
			String tableName= term._strTableName;
			String columnName=term._strColumnName;
			Object objectvalue=term._objValue;
				FileReader inputfile = new FileReader("./data/metadata.csv");
				BufferedReader reader = new BufferedReader(inputfile);
				while (reader.ready()) {
					String line = reader.readLine();
					coloumnValues = line.split(",");
					if (objectvalue instanceof Double && coloumnValues[0].equals(tableName) && coloumnValues[1].equals(columnName)
							&& !coloumnValues[2].equals("java.lang.double")) {
						throw new DBAppException(columnName+" should be of type double");
					}
					else if (objectvalue instanceof Integer && coloumnValues[0].equals(tableName) && coloumnValues[1].equals(columnName)
							&& !coloumnValues[2].equals("java.lang.Integer")) {
						throw new DBAppException(columnName+" should be of type Integer");
					} else if (objectvalue instanceof Date && coloumnValues[0].equals(tableName) && coloumnValues[1].equals(columnName)
							&& !coloumnValues[2].equals("java.lang.Date")) {
						throw new DBAppException(columnName+" should be of type Date");
					} else if (objectvalue instanceof String && coloumnValues[0].equals(tableName) && coloumnValues[1].equals(columnName)
							&& !coloumnValues[2].equals("java.lang.String")) {
						throw new DBAppException(columnName+" should be of type String");
					} else if (objectvalue instanceof Boolean && coloumnValues[0].equals(tableName) && coloumnValues[1].equals(columnName)
							&& !coloumnValues[2].equals("java.lang.Boolean")) {
						throw new DBAppException(columnName+" should be of type boolean");
					}
				}
			
		}
		SQLTerm[] arrSQLTerms = new SQLTerm[arrSQLTerms1.length];
		for (int i = 0; i < arrSQLTerms1.length; i++) {
			arrSQLTerms[i] = new SQLTerm(arrSQLTerms1[i]._strTableName, arrSQLTerms1[i]._strColumnName,
					arrSQLTerms1[i]._strOperator, arrSQLTerms1[i]._objValue);
		}
		
		ArrayList<Set<Hashtable<String, Object>>> results = new ArrayList<Set<Hashtable<String, Object>>>();
		for (int i = 0; i < arrSQLTerms.length; i++) {
			Table table = DBApp.getTable(arrSQLTerms[i]._strTableName);
			if(table.isIndexed(arrSQLTerms[i]._strColumnName)) {
				 results.add( DBApp.selectIndexed(arrSQLTerms[i]));
			}
			else {
				results.add( DBApp.selectNormal(arrSQLTerms[i]));
			}
		}
		
		
		ArrayList<String> Operators = new ArrayList<String>(Arrays.asList(strarrOperators));
		
		
		for (int i = 0; i < Operators.size(); i++) {
			if (Operators.get(i).equals("AND")) {
				System.out.println("and");
				Table table = DBApp.getTable(arrSQLTerms[i]._strTableName);
				Set<Hashtable<String, Object>> a = results.get(i);
				Set<Hashtable<String, Object>> b = results.get(i+1);
				a.retainAll(b);
				results.remove(i);
				results.remove(i);
				results.add(i, a);
				Operators.remove(i);
				i--;
			}
		}
		
		for (int i = 0; i < Operators.size(); i++) {
			if (Operators.get(i).equals("XOR")) {
				System.out.println("xor");
				Table table = DBApp.getTable(arrSQLTerms[i]._strTableName);
				Set<Hashtable<String, Object>> a = results.get(i);
				Set<Hashtable<String, Object>> b = results.get(i+1);
				Set<Hashtable<String, Object>> c = new HashSet<Hashtable<String, Object>>();
				c.addAll(a); 
				c.addAll(b);
				a.retainAll(b); //a now has the intersection of a and b
				c.removeAll(a); 
				results.remove(i);
				results.remove(i);
				results.add(i, c);
				Operators.remove(i);
				i--;
			}
		}
		
		for (int i = 0; i < Operators.size(); i++) {
			if (Operators.get(i).equals("OR")) {
				System.out.println("or");
				Table table = DBApp.getTable(arrSQLTerms[i]._strTableName);
				Set<Hashtable<String, Object>> a = results.get(i);
				Set<Hashtable<String, Object>> b = results.get(i+1);
				System.out.println("A is" +a);
				System.out.println("B is" +b);
				a.addAll(b);
				System.out.println("C is" +a);
				System.out.println("results is" +results);
				results.remove(i);
				results.remove(i);
				results.add(i, a);
				System.out.println("results is" +results);
				Operators.remove(i);
				i--;
			}
		}
		
		

		return results.get(0).iterator();
	}

	//////////////////////////////////////////////// our methods
	//////////////////////////////////////////////// ////////////////////////////////////////////////
	public static Set<Hashtable<String, Object>> selectIndexed(SQLTerm SQLTerms)
			throws ClassNotFoundException, IOException, DBAppException {

		//Vector<Vector<Hashtable<String, Object>>> bigList = new Vector<Vector<Hashtable<String, Object>>>();
		Vector<Hashtable<String, Object>> list = new Vector<Hashtable<String, Object>>();

		//for (int i = 0; i <= strarrOperators.length; i++) {


			String tableName = SQLTerms._strTableName;
			Table table = getTable(tableName);
			String colName = SQLTerms._strColumnName;
			Object objectValue = SQLTerms._objValue;
			String op = SQLTerms._strOperator;
			Index index = DBApp.getIndex(tableName, colName);

			Set<String> keys = table.dataTypes.keySet();
			if (keys.contains(colName)) {
				int pos = -1;
				int j = -1;
				for (j = 0; j < index.numberOfPages; j++) {
					IndexPage page = index.getPage(j);
					
					List<Object> attributes = page.rows.stream().map(sc -> sc.value).collect(Collectors.toList());
					pos = Arrays.binarySearch(attributes.toArray(), objectValue); // BINARY SEARCHHH, VERY EFFECIENT
					
					System.out.println("pos ::: " +  pos);
					System.out.println("-1*pos - 1 ::: " + ((-1 * pos) -1 ));
					System.out.println("number of rows ::: " + page.rows.size()
					);
					if (! (((-1 * pos) -1 ) == page.rows.size() ))
						break;

				}
				if(pos<0)
					pos = (-1 * pos) - 1;				
				String bitmap = index.getPage(j).rows.get(pos).bitmapindex;
				switch (op) {
				case "=":
					for (int k = 0; k < bitmap.length(); k++) {
						if (bitmap.charAt(k) == '1') {
							Point p = index.pointers.get(k);
							list.add(table.getPage(p.x).rows.get(p.y));
						}
					}
					
					//bigList.add(list);
					break;
				case "!=":
					for (int k = 0; k < bitmap.length(); k++) {
						if (bitmap.charAt(k) == '0') {
							Point p = index.pointers.get(k);
							list.add(table.getPage(p.x).rows.get(p.y));
						}
					}
					//bigList.add(list);
					break;
				case ">=":
					for (int k = 0; k < bitmap.length(); k++) {
						if (bitmap.charAt(k) == '1') {
							for (int l = k; l < bitmap.length(); l++) {
								Point p = index.pointers.get(l);
								list.add(table.getPage(p.x).rows.get(p.y));
							}
							break;
						}
					}
					//bigList.add(list);
					break;
				case ">":
					for (int k = 0; k < bitmap.length() - 1; k++) {
						if (bitmap.charAt(k) == '1' && bitmap.charAt(k + 1) == '0') {
							for (int l = k + 1; l < bitmap.length(); l++) {
								Point p = index.pointers.get(l);
								list.add(table.getPage(p.x).rows.get(p.y));
							}
							break;
						}
					}
					//bigList.add(list);
					break;
				case "<=":
					for (int k = bitmap.length() - 1; k >= 0; k--) {
						if (bitmap.charAt(k) == '1') {
							for (int l = k; l >= 0; l--) {
								Point p = index.pointers.get(l);
								list.add(table.getPage(p.x).rows.get(p.y));
							}
							break;
						}
					}
					//bigList.add(list);
					break;
				case "<":
					for (int k = bitmap.length() - 1; k > 0; k--) {
						if (bitmap.charAt(k) == '1' && bitmap.charAt(k - 1) == '0') {
							for (int l = k - 1; l >= 0; l--) {
								Point p = index.pointers.get(l);
								list.add(table.getPage(p.x).rows.get(p.y));
							}
							break;
						}
					}
					//bigList.add(list);
					break;
				default:
					throw new DBAppException("Please enter a valid operator.");
				}
			}
//		}
//		for (int i = 0; i < strarrOperators.length; i++) {
//			if (strarrOperators[i].equals("AND")) {
//				Set<Hashtable<String, Object>> a = new HashSet<Hashtable<String, Object>>(bigList.get(i));
//				Set<Hashtable<String, Object>> b = new HashSet<Hashtable<String, Object>>(bigList.get(i + 1));
//				a.retainAll(b);
//			}
//		}
		Set<Hashtable<String, Object>> result = new HashSet<Hashtable<String, Object>>(list);

			
		return result;
	}

	public static Set<Hashtable<String, Object>> selectNormal(SQLTerm SQLTerms)
			throws ClassNotFoundException, IOException, DBAppException {
		//Vector<Vector<Hashtable<String, Object>>> bigList = new Vector<Vector<Hashtable<String, Object>>>();

		//for (int i = 0; i <= strarrOperators.length; i++) {
			Vector<Hashtable<String, Object>> list = new Vector<Hashtable<String, Object>>();
			String tableName = SQLTerms._strTableName;
			// System.out.println("Table Name: " + tableName);
			Table table = getTable(tableName);
			String colName = SQLTerms._strColumnName;
			// System.out.println("Column Name: " + colName);
			Set<String> keys = table.dataTypes.keySet();
			// System.out.println("Keys : " + keys.toString());
			if (keys.contains(colName)) {
				// System.out.println("Keys cotain colName");
				for (int j = 0; j < table.numberOfPages; j++) {
					Page page = table.getPage(j);
					System.out.println("Page Index: " + page.index);
					Object objectValue = SQLTerms._objValue;
					String operator = SQLTerms._strOperator;
					// System.out.println("Object value : " +
					// objectValue.toString());
					Hashtable rowNeeded = new Hashtable();
					rowNeeded.put(colName, objectValue);
					Vector<Hashtable<String, Object>> rows = page.searchRows(rowNeeded,operator);
					//System.out.println("Rows: " + rows.toString());
					if (!rows.isEmpty()) {
						for (int k = 0; k <= rows.size(); k++) {
							list.addElement(rows.remove(0));
						}
					}
					//System.out.println("List: " + list.toString());
					if (rows.isEmpty()) {
					//	System.out.println("Rows are empty");
					}
					// System.out.println("List: " + list.toString());
					/*
					 * for(int o = 0; o< list.size();o++){ Hashtable<String,
					 * Object> returnedRow = list.get(o);
					 * System.out.println("Returned Row: " +
					 * returnedRow.toString()); Object value =
					 * returnedRow.get(colName); returnedList[o] = value;
					 * System.out.println("Returned List: " + returnedList[o]);
					 * }
					 */
				}
				//bigList.add(list);
			}
		//}
		//System.out.println("Big List: " + bigList.toString());
//		Vector<Vector<Hashtable<String, Object>>> result = new Vector<Vector<Hashtable<String, Object>>>();
//		result.addElement(bigList.get(0));
//		for (int l = 1; l <= strarrOperators.length; l++) {
//			String operator = strarrOperators[l - 1];
//			// System.out.println("Operator: " + operator);
//			switch (operator) {
//			case ("OR"): {
//				result.addElement(bigList.get(l));
//				break;
//			}
//			case ("AND"): {
//				System.out.println("Entered AND condition");
//				for (int m = 0; m < result.size(); m++) {
//					Object resultValue = result.get(m);
//					System.out.println("Result value: " + resultValue);
//					if (resultValue == null) {
//						for (int n = 0; n < result.size(); n++) {
//							result.remove(n);
//						}
//						break;
//					}
//				}
//				result.add(bigList.get(l));
//				break;
//			}
//			}
//		}
//		System.out.println("Result: " + result.toString());
			Set<Hashtable<String, Object>> result = new HashSet<Hashtable<String, Object>>(list);
	
		return result;
	}

	public static Table getTable(String strTableName) throws IOException, ClassNotFoundException {
		FileInputStream fileIn = new FileInputStream("./data/" + strTableName + "");
		ObjectInputStream in = new ObjectInputStream(fileIn);
		Table table = null;
		try {
			table = (Table) in.readUnshared();
		} catch (ClassNotFoundException e) {
			System.out.println("Not a table");
			return null;
		} catch (IOException e) {
			System.out.println("Couldn't find file");
			return null;
		}
		in.close();
		fileIn.close();

		return table;
	}

	public static Index getIndex(String strTableName, String columnName) throws IOException, ClassNotFoundException {
		FileInputStream fileIn = new FileInputStream("./data/" + strTableName + "Index" + columnName);
		ObjectInputStream in = new ObjectInputStream(fileIn);
		Index index = null;
		try {
			index = (Index) in.readUnshared();
		} catch (ClassNotFoundException e) {
			System.out.println("Not a table");
			return null;
		} catch (IOException e) {
			System.out.println("Couldn't find file");
			return null;
		}
		in.close();
		fileIn.close();

		return index;
	}

	public static void saveTable(Table table) throws IOException {
		FileOutputStream fileOut = new FileOutputStream("./data/" + table.tableName + "");
		ObjectOutputStream out = new ObjectOutputStream(fileOut);
		out.writeObject(table);
		// tableFilePaths[numberOfTables++] = table.tableName;
		out.close();
		fileOut.close();
	}

	public static void saveIndex(Index index) throws IOException {
		FileOutputStream fileOut = new FileOutputStream("./data/" + index.tableName + "Index" + index.coloumnName);
		ObjectOutputStream out = new ObjectOutputStream(fileOut);
		out.writeObject(index);
		// tableFilePaths[numberOfTables++] = table.tableName;
		out.close();
		fileOut.close();
	}

	public static void updateFile(Table table) throws IOException {
		FileOutputStream fileOut = new FileOutputStream("./data/" + table.tableName + "", false);
		ObjectOutputStream out = new ObjectOutputStream(fileOut);
		out.reset();
		out.writeUnshared(table);
		out.flush();
		out.close();
		fileOut.flush();
		fileOut.close();
	}

	public static void updateIndex(Index index) throws IOException {
		FileOutputStream fileOut = new FileOutputStream("./data/" + index.tableName + "Index" + index.coloumnName, false);
		ObjectOutputStream out = new ObjectOutputStream(fileOut);
		out.reset();
		out.writeUnshared(index);
		out.flush();
		out.close();
		fileOut.flush();
		fileOut.close();
	}

	/*
	 * public static Table getTables(String tableName){ for(Table t:tables){ if
	 * (t.tableName==tableName){ return t; } } return null; }
	 */

	//// main

	@SuppressWarnings({ "unchecked", "rawtypes", "deprecation" })
	public static void main(String[] args) throws Throwable {
		
		DBApp db = new DBApp();
		db.init();
/*
		numberOfTables = 0;
		tableFilePaths = new String[200];

		String strTableName = "Student";

		Hashtable htblColNameType = new Hashtable();
		htblColNameType.put("id", "java.lang.Integer");
		htblColNameType.put("name", "java.lang.String");
		htblColNameType.put("gpa", "java.lang.double");
		htblColNameType.put("testboolean", "java.lang.Boolean");
		htblColNameType.put("testdate", "java.lang.Date");

		Hashtable htblColNameValue1 = new Hashtable();
		htblColNameValue1.put("id", new Integer(8));
		htblColNameValue1.put("name", new String("Ahmed Noor"));
		htblColNameValue1.put("gpa", new Double(1));
		htblColNameValue1.put("testboolean", new Boolean(true));
		htblColNameValue1.put("testdate", new Date(1998,5,1));

		Hashtable htblColNameValue2 = new Hashtable();
		htblColNameValue2.put("id", new Integer(7));
		htblColNameValue2.put("name", new String("Dalia Noor"));
		htblColNameValue2.put("gpa", new Double(1));

		Hashtable htblColNameValue3 = new Hashtable();
		htblColNameValue3.put("id", new Integer(6));
		htblColNameValue3.put("name", new String("John Noor"));
		htblColNameValue3.put("gpa", new Double(1));

		Hashtable htblColNameValue4 = new Hashtable();
		htblColNameValue4.put("id", new Integer(5));
		htblColNameValue4.put("name", new String("Zaky Noor"));
		htblColNameValue4.put("gpa", new Double(1));

		Hashtable htblColNameValue5 = new Hashtable();
		htblColNameValue5.put("id", new Integer(2));
		htblColNameValue5.put("name", new String("abir Noor"));
		htblColNameValue5.put("gpa", new Double(2));

		Hashtable htblColNameValue6 = new Hashtable();
		htblColNameValue6.put("id", new Integer(3));
		htblColNameValue6.put("name", new String("paul Noor"));
		htblColNameValue6.put("gpa", new Double(4));

		Hashtable htblColNameValue7 = new Hashtable();
		htblColNameValue7.put("id", new Integer(2));
		htblColNameValue7.put("name", new String("dany Noor"));
		htblColNameValue7.put("gpa", new Double(2));

		Hashtable htblColNameValue8 = new Hashtable();
		htblColNameValue8.put("id", new Integer(9));
		htblColNameValue8.put("name", new String("hello Noor"));
		htblColNameValue8.put("gpa", new Double(7));

		Hashtable htblColNameValue11 = new Hashtable();
		htblColNameValue11.put("id", new Integer(100));
		htblColNameValue11.put("name", new String("pleas work pleaseeeee Noor"));
		htblColNameValue11.put("gpa", new Double(3));

		Hashtable htblColNameValue12 = new Hashtable();
		htblColNameValue12.put("id", new Integer(100));
		htblColNameValue12.put("name", new String("hello Noor"));
		htblColNameValue12.put("gpa", new Double(6));

		Hashtable htblColNameValue13 = new Hashtable();
		htblColNameValue13.put("id", new Integer(10));
		// htblColNameValue13.put("name", new String("hello Noor" ));
		htblColNameValue13.put("gpa", new Double(5));

		Hashtable htblColNameValue9 = new Hashtable();
		htblColNameValue9.put("id", new Integer(1));

		Hashtable htblColNameValue10 = new Hashtable();
		// htblColNameValue10.put("id", new Integer( 8 ) );
		htblColNameValue10.put("name", new String("Balabzio newww"));
		htblColNameValue10.put("gpa", new Double(6));

		/////////// call//////////////////////////////////////

		db.createTable(strTableName, "id", htblColNameType);

		db.insertIntoTable(strTableName, htblColNameValue1);
		db.insertIntoTable(strTableName, htblColNameValue2);
		db.insertIntoTable(strTableName, htblColNameValue3);
		db.insertIntoTable(strTableName, htblColNameValue4);
		db.insertIntoTable(strTableName, htblColNameValue5);
		db.insertIntoTable(strTableName, htblColNameValue6);
		db.insertIntoTable(strTableName, htblColNameValue7);
		db.insertIntoTable(strTableName, htblColNameValue8);
		// db.insertIntoTable( strTableName , htblColNameValue12 );

		db.createBitmapIndex(strTableName, "gpa");

		System.out.println(getTable("Student").toString());
		System.out.println("**********BEFORE****************");
		System.out.println(getIndex("Student", "gpa"));

		 db.insertIntoTable( strTableName , htblColNameValue11);
		 db.insertIntoTable( strTableName , htblColNameValue13);
		// db.deleteFromTableSorted(strTableName, htblColNameValue7);
		// db.deleteFromTableSorted(strTableName, htblColNameValue8);
//		db.updateTable(strTableName, "7", htblColNameValue10);

		System.out.println("**********AFTER****************");
		System.out.println(getIndex("Student", "gpa"));
		System.out.println(getTable("Student").toString());

		// System.out.println("Number of pages before delete :: " +
		// DBApp.getTable(strTableName).numberOfPages );
		// db.deleteFromTableSorted(strTableName, htblColNameValue9);
		// System.out.println("Number of pages after delete :: " +
		// DBApp.getTable(strTableName).numberOfPages );
		// db.updateTable(strTableName, "7", htblColNameValue10);

		// File dir = new File("./src");
		// File[] filesList = dir.listFiles();
		// for (File file : filesList) {
		// if (file.isFile()) {
		// System.out.println(file.getName());
		// }
		// }

		// System.out.println(getTable("Student").toString());
		// System.out.println(getIndex("Student","gpa"));

		//
		// ArrayList<Point> p = new ArrayList<Point>();
		// p.add(new Point(1,1));
		// p.add(new Point(2,2));
		// p.add(new Point(3,3));
		// p.add(new Point(4,4));
		//
		// ArrayList<String> s = new ArrayList<String>();
		// s.add("2");
		// s.add("1");
		// s.add("4");
		// s.add("3");
		//
		// System.out.println(getIndex("Student","gpa").sortPoints(p, s));


		SQLTerm[] arrSQLTerms = new SQLTerm[1];
		
		SQLTerm sql1 = new SQLTerm("Student", "gpa", ">", new Double(5.5));
		arrSQLTerms[0] = sql1;

//		 SQLTerm sql2 = new SQLTerm("Student", "gpa", "=", new Double(1.0));
//		 arrSQLTerms[1] = sql2;
//
//		 SQLTerm sql3 = new SQLTerm("Student", "name", "=", new String("Balabzio newww"));
//		 arrSQLTerms[2] = sql3;
		 
		String[] strarrOperators = new String[0];
		
//		strarrOperators[0] = "AND";
		//strarrOperators[1] = "OR";

		Iterator resultSet = db.selectFromTable(arrSQLTerms, strarrOperators);

		while (resultSet.hasNext()) {
			System.out.println("Set: " + resultSet.next().toString());
		}
		Properties prop = new Properties();
		prop.load(new FileInputStream("config/DBApp.properties"));
		System.out.println(prop.getProperty("maximumPageSize"));
		System.out.println(prop.getProperty("maximumIndexSize"));
*/
		
		
		String strTableName = "Student";
		Hashtable htblColNameType = new Hashtable( );
		htblColNameType.put("id", "java.lang.Integer"); 
		htblColNameType.put("name", "java.lang.String"); 
		htblColNameType.put("gpa", "java.lang.double"); 
		db.createTable( strTableName, "id", htblColNameType );
		db.createBitmapIndex( strTableName, "gpa" );     
		
		Hashtable htblColNameValue = new Hashtable( ); 
		htblColNameValue.put("id", new Integer( 2343432 )); 
		htblColNameValue.put("name", new String("Ahmed Noor" ) ); 
		htblColNameValue.put("gpa", new Double( 1) ); 
		db.insertIntoTable( strTableName , htblColNameValue );
		htblColNameValue.clear( );
		System.out.println("%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%");
		System.out.println(DBApp.getIndex(strTableName, "gpa").getPage(0).rows.get(0).bitmapindex);
		
		htblColNameValue.put("id", new Integer( 453455 )); 
		htblColNameValue.put("name", new String("Ahmed Noor" ) ); 
		htblColNameValue.put("gpa", new Double( 2 ) ); 
		db.insertIntoTable( strTableName , htblColNameValue );
		htblColNameValue.clear( );
		System.out.println("%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%");
		System.out.println(DBApp.getIndex(strTableName, "gpa").getPage(0).rows.get(0).bitmapindex);
		
		htblColNameValue.put("id", new Integer( 5674567 )); 
		htblColNameValue.put("name", new String("Dalia Noor" ) ); 
		htblColNameValue.put("gpa", new Double( 3 ) ); 
		db.insertIntoTable( strTableName , htblColNameValue );
		htblColNameValue.clear( );
		System.out.println("%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%");
		System.out.println(DBApp.getIndex(strTableName, "gpa").getPage(0).rows.get(0).bitmapindex);
		
		htblColNameValue.put("id", new Integer( 23498 )); 
		htblColNameValue.put("name", new String("John Noor"));
		htblColNameValue.put("gpa", new Double(4 ) ); 
		db.insertIntoTable( strTableName , htblColNameValue );
		htblColNameValue.clear( ); 
		System.out.println("%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%");
		System.out.println(DBApp.getIndex(strTableName, "gpa").getPage(0).rows.get(0).bitmapindex);
		
		htblColNameValue.put("id", new Integer( 78452 )); 
		htblColNameValue.put("name", new String("Zaky Noor"));
		htblColNameValue.put("gpa", new Double( 5 ) ); 
		db.insertIntoTable( strTableName , htblColNameValue );
		System.out.println("%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%");
		System.out.println(DBApp.getIndex(strTableName, "gpa").getPage(0).rows.get(0).bitmapindex);
		
		
		SQLTerm[] arrSQLTerms;
		arrSQLTerms = new SQLTerm[2]; 
		arrSQLTerms[0] = new SQLTerm("Student","name","=","John Noor");
		arrSQLTerms[1] = new SQLTerm("Student","gpa","=",new Double(1.5));
		
		
		String[] strarrOperators = new String[1];
		strarrOperators[0] = "OR";
		
		// select * from Student where name = “John Noor” or gpa = 1.5; 
		Iterator resultSet = db.selectFromTable(arrSQLTerms , strarrOperators);
		
		System.out.println();
		System.out.println(getIndex("Student", "gpa"));
		System.out.println(getTable("Student").toString());
		while (resultSet.hasNext()) {
			System.out.println(resultSet.next().toString());
		}
	}

}
