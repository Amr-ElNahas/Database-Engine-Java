package ScottsTots;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.Serializable;
import java.util.Hashtable;
import java.util.Properties;
import java.util.Stack;
import java.util.Vector;

public class IndexPage implements Serializable {
	
	Index index;
	int i;
	
	Vector<Entry> rows = new Vector<Entry>();
	boolean isFull;
	
	//static final int size = 3;


	public IndexPage(Index index, int i) {
		this.index = index;
		this.i = i;
	}


	@SuppressWarnings({ "rawtypes", "unchecked", "unused" })
	public void insertIntoPageSorted(Entry e) throws Throwable {
			
		//System.out.println(rows.size());

		
			if(rows.size() == 0) {
				System.out.println("fady");
				//System.out.println(rows);
				rows.add(e);
				DBApp.getIndex(index.tableName, index.coloumnName).refreshPage(this);
				//System.out.println(index.getPage(i).rows);


			}
			else {
				int SIZE = rows.size();

				for(int i = 0; i < SIZE;i++) {
					System.out.println(rows.get(i).value);
					System.out.println(e.value);
					if((((Comparable)rows.get(i).value).compareTo((Comparable)e.value))==0) { 
						return;
					}
				}	
				
				
				if(!isFull) {
					Properties prop = new Properties();
					try {
						prop.load(new FileInputStream("config/DBApp.properties"));
					} catch (IOException exceptionaya) {
						// TODO Auto-generated catch block
						exceptionaya.printStackTrace();
					}
					int size= Integer.parseInt(prop.getProperty("maximumIndexSize"));
		
					if(rows.size() == size-1)
						isFull = true;
					
						SIZE = rows.size();
						for(int i = 0; i < SIZE;i++) {
							if((((Comparable)rows.get(i).value).compareTo(e.value))>0) { //row > htblColNameValue
								Stack<Entry> temp = new Stack<Entry>();
								for(int j = SIZE-1;j >= i;j-- ) {
									Entry row = rows.remove(j);
									temp.add(row);
								}
								rows.add(e);
								while(!temp.isEmpty()) {
									rows.add(temp.pop());
								}
								DBApp.getIndex(this.index.tableName, this.index.coloumnName).refreshPage(this);
								return;
							}
						}
						rows.add(e);
						DBApp.getIndex(this.index.tableName, this.index.coloumnName).refreshPage(this);
						return;
						
					}
					else {
					
						Entry overflow;
						System.out.println("overflow");
						//int SIZE = rows.size();
						for(int i = 0; i < SIZE;i++) {
							if((((Comparable)rows.get(i).value).compareTo(e.value))>0) { //row > htblColNameValue
								Stack<Entry> temp = new Stack<Entry>();
								overflow = rows.remove(SIZE-1);
								for(int j = SIZE-2;j >= i;j-- ) {
									Entry row = rows.remove(j);
									temp.add(row);
								}
								rows.add(e);
								while(!temp.isEmpty()) {
									rows.add(temp.pop());
								}
								System.out.println("Check1: " + this.i + " == " + DBApp.getIndex(this.index.tableName,this.index.coloumnName).numberOfPages + "-1");
								if(this.i == DBApp.getIndex(this.index.tableName,this.index.coloumnName).numberOfPages-1) {
								//	System.out.println("Line 93: creates a new page");
									IndexPage newPage = new IndexPage(this.index,this.i+1);
									this.index.savePage(newPage);
									newPage.insertIntoPageSorted(overflow);
								//	System.out.println("Just to make sure ::: " + newPage.rows);
									//index.refreshPage(newPage);
								}
								else {
									//System.out.println("Line 100: inserts into next page");

									IndexPage p = this.index.getPage(this.i+1);
									p.insertIntoPageSorted(overflow);
									//index.refreshPage(p);
								}
								DBApp.getIndex(this.index.tableName, this.index.coloumnName).refreshPage(this);

								return;
							}
						}
						
						System.out.println("Check: " + this.i + " == " + DBApp.getIndex(this.index.tableName,this.index.coloumnName).numberOfPages + "-1");

						if(this.i == DBApp.getIndex(this.index.tableName,this.index.coloumnName).numberOfPages-1) {
							System.out.println("Line 112: creates a new page");

							IndexPage newPage = new IndexPage(this.index,this.i+1);
							this.index.savePage(newPage);
							newPage.insertIntoPageSorted(e);
							//System.out.println("Before ==>"+DBApp.getIndex(this.index.tableName, this.index.coloumnName).getPage(newPage.i).rows);
							//index.refreshPage(DBApp.getIndex(this.index.tableName, this.index.coloumnName).getPage(newPage.i));
							//System.out.println("After ==>"+DBApp.getIndex(this.index.tableName, this.index.coloumnName).getPage(newPage.i).rows);
							//index.refreshPage(newPage);

							
						}
						else {
							//System.out.println("Line 1021: inserts into next page");

							IndexPage p = this.index.getPage(this.i+1);
							p.insertIntoPageSorted(e);
							//index.refreshPage(p);
						}
						//this.index.refreshPage(this);

						return;
						
				}
			}
		
	}
	
	

}
