package database_project;

/** * @author Wael Abouelsaadat */

import java.util.Iterator;
import java.util.Set;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.Reader;
import java.security.KeyStore.Entry;
import java.util.ArrayList;
import java.util.Hashtable;
import java.util.Map;
import java.util.Properties;
import java.util.Vector;

public class DBApp {

	// 2 vectors or 1 hashtable to hold table name and its serialized filename
	public static Hashtable<String, String> tblname; // would need to be serialized and deserialized later

	public DBApp() {
		// tblname = new Hashtable<String, String>();
	}

	// this does whatever
	// initialization you would like
	// or leave it empty if there is no code you want to
	// execute at application startup
	public void init() {
		// deserialize hashtable tblname

	}

	// following method creates one table only
	// strClusteringKeyColumn is the name of the column that will be the primary
	// key and the clustering column as well. The data type of that column will
	// be passed in htblColNameType
	// htblColNameValue will have the column name as key and the data
	// type as value
	public void createTable(String strTableName, String strClusteringKeyColumn,
			Hashtable<String, String> htblColNameType) throws DBAppException {
		Table tbl = new Table(strTableName);

		// Adding to csv file!

		String FilePath = "Meta-Data.csv";

		try {
			// Create a FileWriter to write to the CSV file
			FileWriter fileWriter1 = new FileWriter(FilePath,true);

			// Create a BufferedWriter to improve writing performance
			BufferedWriter bufferedWriter = new BufferedWriter(fileWriter1);

			// Storing all entries of Hashtable in a Set
			// using entrySet() method
			Set<java.util.Map.Entry<String, String>> entrySet = htblColNameType.entrySet();

			// Creating an Iterator object to
			// iterate over the given Hashtable
			Iterator<java.util.Map.Entry<String, String>> itr = entrySet.iterator();

			// Iterating through the Hashtable object
			// using iterator

			// Checking for next element
			// using hasNext() method
			while (itr.hasNext()) {
				boolean isPK = false;
				// Getting a particular entry of HashTable
				java.util.Map.Entry<String, String> entry = itr.next();
				if (entry.getKey().equals(strClusteringKeyColumn)) {
					isPK = true;
				}
				// Table Name, Column Name, Column Type, ClusteringKey, IndexName,IndexType
				bufferedWriter.write(strTableName + "," + entry.getKey() + "," + entry.getValue() + "," + isPK + ","
						+ "null" + "," + "null");
				(tbl.columns).add(entry.getKey());
				bufferedWriter.newLine(); // Move to the next line
			}

			// Close the BufferedWriter
			bufferedWriter.close();

		} catch (IOException e) {
			System.out.println("Error here in create method!");
			e.printStackTrace();
			System.out.println("Error here in create method!");
		}

		// serialize table
		tbl.tblserialize(strTableName);
	}

	// following method creates a B+tree index
	public void createIndex(String strTableName, String strColName, String strIndexName) {
		try {
			bplustree t = new bplustree(3);
			String filePath = "Meta-Data.csv";
			BufferedReader reader = new BufferedReader(new FileReader(filePath));
			String line;
			StringBuilder fileContent = new StringBuilder();

			while ((line = reader.readLine()) != null) {
				String[] parts = line.split(",");
				if (parts[0].equals(strTableName) && parts[1].equals(strColName)) {
					parts[4] = strIndexName;
					parts[5] = "B+Tree";
				}
				fileContent.append(String.join(",", parts)).append("\n");
			}

			reader.close();

			// Write back to the file
			BufferedWriter writer = new BufferedWriter(new FileWriter(filePath));
			writer.write(fileContent.toString());
			writer.close();

			System.out.println("Index created successfully.");
			Table table = Table.Tbldeserialize(strTableName + ".ser");
			int indexOfC = table.columns.indexOf(strColName);
			for (int y = 0; y < table.pages.size(); y++) {
				Page p = Page.pdeserialize((table.pages).elementAt(y));
				for (int u = 0; u < p.tuples.size(); u++) {
					Tuple d = Tuple.Tpldeserialize(p.tuples.elementAt(u));
//	        		if((d.values.elementAt(indexOfC)) instanceof Integer){
//		        		int stringTo=convertTo(d.values.elementAt(indexOfC));
//	        		}
//	        		else{
//	        			int stringTo=(int)d.values.elementAt(indexOfC);
//	        		}
					double stringTo;
					try {
						stringTo = (double) Double.parseDouble(d.values.elementAt(indexOfC));
					}

					catch (Exception e) {
						stringTo = convertTo(d.values.elementAt(indexOfC));
					}
					t.insert(stringTo, p.tuples.elementAt(u));
				}
			}
			//serialization of B+Tree with strIndexName \\Help!!
		} catch (IOException e) {
			e.printStackTrace();
		}

	}

	// following method inserts one row only.
	// htblColNameValue must include a value for the primary key
	public void insertIntoTable(String strTableName, 
			Hashtable<String, Object> htblColNameValue) throws DBAppException, IOException {
		// deserialize file based on table name "strTableName.ser"
		Table table = Table.Tbldeserialize(strTableName+".ser");
		int i = 0;
		int indexPK=0;
		//read from csv file if it is indexed
		FileReader fileRead = new FileReader("Meta-Data.csv");
		BufferedReader buffRead = new BufferedReader(fileRead);
		String lineRead;
		String [] tempArr = null; 
		Vector<String> currtuple=new Vector<String>();
		while ((lineRead = buffRead.readLine())!=null) {
			tempArr = lineRead.split(",");
			if(tempArr[0].equals(strTableName)){
				String currvalue= ""+htblColNameValue.get(tempArr[1]);
				currtuple.add(currvalue); 
				if(tempArr[3].equals("TRUE"))
					indexPK=i;
			i++;
			}
		}
		Page p;
		if(table.pages.size()==0){
			p= new Page(strTableName,0);
			p.serialize(p.name);
			table.pages.add(0,p.name+".ser");
			System.out.println(table.pages.size());
			table.tblserialize(strTableName);
		}
		else
			p=Page.pdeserialize(table.pages.get(0));
		//this makes sure the values is in the same order of the metadata
		
		Tuple tuple=new Tuple(p);
		tuple.values=currtuple;
		tuple.serialize(tuple.name);
		p.tuples.add(tuple.name+".ser");
		p.serialize(p.name);
		table.tblserialize(strTableName);
//		while(i!= table.columns.size()) {
//			
//			//find index of clustering key
//			
//			//if indexed use binary tree
//			if(tempArr[4]!= "null") {
//				
//			}
//			//not indexed so use binary_search() since clustering key is sorted
//			else {
//				String value=(String) htblColNameValue.get(tempArr[2]);
//				int[] location= table.binary_search(value, indexPK);
//				
//			}
//		}
		
//		throw new DBAppException("not implemented yet");
	}

	// following method updates one row only
	// htblColNameValue holds the key and new value
	// htblColNameValue will not include clustering key as column name
	// strClusteringKeyValue is the value to look for to find the row to update.
	public void updateTable(String strTableName, String strClusteringKeyValue,
			Hashtable<String, Object> htblColNameValue) throws DBAppException {

		throw new DBAppException("not implemented yet");
	}

	// following method could be used to delete one or more rows.
	// htblColNameValue holds the key and value. This will be used in search
	// to identify which rows/tuples to delete.
	// htblColNameValue enteries are ANDED together
	public void deleteFromTable(String strTableName, Hashtable<String, Object> htblColNameValue) throws DBAppException {
		
		Table table= Table.Tbldeserialize(strTableName);
		Vector<String> htblColName=new Vector<String>();
		Vector<Object> htblValues=new Vector<Object>();
			for (Map.Entry<String, Object> entry : htblColNameValue.entrySet()) {
			    // Extract the key (column name) from the entry
			    String colName = entry.getKey();
			    htblColName.add(colName);
			    Object colvalue=entry.getValue();
			    htblValues.add(colvalue);
				}
		try{
			FileReader fileRead = new FileReader("Meta-Data.csv");
			BufferedReader buffRead = new BufferedReader(fileRead);
			String lineRead=null;
			String BColName=null;
			int BColNameIndex=-1;
			String strIndexName=null;
			String primarykey=null;
			
			while ((lineRead = buffRead.readLine())!=null) {
				 String[] tempArr = lineRead.split(",");
				 if(tempArr[0].equals(strTableName)){
					 BColNameIndex++;
				 }
				 if(tempArr[0].equals(strTableName) && htblColName.contains(tempArr[1])){
					 if(tempArr[5].equals("B+Tree")){
						 BColName= tempArr[1];
						 strIndexName=tempArr[4];
						 break;
					 }
					 if(tempArr[3].equals("TRUE")){
						 primarykey=tempArr[1];
						 break;
					 }
				 }
			}
			if(BColName!=null){
				//deserialize B+Tree from strIndexName
			}
			else 
				if(primarykey!=null) {
				String valueOfPK= ""+htblColNameValue.get(primarykey);
				table.binary_search(valueOfPK, BColNameIndex);//store in variable(vector or iterator)//Help!! 
			}
				else{
					//search linearly //Help!!
				}
		}
		catch(Exception e){
			e.printStackTrace();
		}
	}

	public Iterator selectFromTable(SQLTerm[] arrSQLTerms, String[] strarrOperators) throws DBAppException {

		return null;
	}
//	public static void main(String []args) {
//		
//		Table t=Table.Tbldeserialize("Student.ser");
//		Page p= Page.pdeserialize(t.pages.get(0));
//		//t.pages.clear();
//		//t.tblserialize("Student");
//		//p.tuples.clear();
//		System.out.println(p.tuples.size());
//		System.out.println(p.tuples.get(0));
//		Tuple tu;
//		for(int i=0;i<p.tuples.size();i++){
//			tu= Tuple.Tpldeserialize(p.tuples.get(i));
//			System.out.println(tu.values.get(1));
//		}
//		Vector<int[]> v= t.search("Ahmed Noor",1);
//		if(v.isEmpty())
//			System.out.println("search is not successfull");
//		for(int i=0; i<v.size();i++){
//			int[] a=v.get(i);
//			System.out.print(a[0]+","+a[1]+"      ");
//		}
//		System.out.println();
//	}

	public static void main(String[] args) {

		try {
			Table t = new Table("Student");
			String strTableName = "Student";
			String strTableName1 = "Parent";
			DBApp dbApp = new DBApp();
			int N=DBApp.readNumberFromConfig();
			System.out.println(N);

			Hashtable htblColNameType = new Hashtable();
			htblColNameType.put("id", "java.lang.Integer");
			htblColNameType.put("name", "java.lang.String");
			htblColNameType.put("gpa", "java.lang.double");
			Hashtable htblColNameType2 = new Hashtable();
			htblColNameType2.put("id", "java.lang.Integer");
			htblColNameType2.put("name", "java.lang.String");
			htblColNameType2.put("gpa", "java.lang.double");
			
			dbApp.createTable(strTableName, "id", htblColNameType);
			dbApp.createTable(strTableName1, "name", htblColNameType2);
			dbApp.createIndex(strTableName, "gpa", "gpaIndex");
			Hashtable htblColNameValue = new Hashtable();
			htblColNameValue.put("id", new Integer(2343432));
			htblColNameValue.put("name", new String("Ahmed Noor"));
			htblColNameValue.put("gpa", new Double(0.95));
			dbApp.insertIntoTable(strTableName, htblColNameValue);
			
			

			htblColNameValue.clear();
			htblColNameValue.put("id", new Integer(453455));
			htblColNameValue.put("name", new String("Ahmed Noor"));
			htblColNameValue.put("gpa", new Double(0.95));
			dbApp.insertIntoTable(strTableName, htblColNameValue);
			

			htblColNameValue.clear();
			htblColNameValue.put("id", new Integer(5674567));
			htblColNameValue.put("name", new String("Dalia Noor"));
			htblColNameValue.put("gpa", new Double(1.25));
			dbApp.insertIntoTable(strTableName, htblColNameValue);
			for(int i=0;i<9;i++){
				htblColNameValue.clear();
				htblColNameValue.put("id", new Integer(23498));
				htblColNameValue.put("name", new String("John Noor"));
				htblColNameValue.put("gpa", new Double(1.5));
				dbApp.insertIntoTable(strTableName, htblColNameValue);
			}
			htblColNameValue.clear();
			htblColNameValue.put("id", new Integer(78452));
			htblColNameValue.put("name", new String("Zaky Noor"));
			htblColNameValue.put("gpa", new Double(1.7));
			dbApp.insertIntoTable(strTableName, htblColNameValue);
			
			t=Table.Tbldeserialize("Student.ser");
			Vector<int[]> v= t.binary_search("1.7",0);
//			for(int i=0; i<v.size()-1;i++){
//				int[] a=v.get(i);
//				System.out.print(a[0]+","+a[1]+"      ");
//			}
//			SQLTerm[] arrSQLTerms;
//			arrSQLTerms = new SQLTerm[2];
//			arrSQLTerms[0]._strTableName = "Student";
//			arrSQLTerms[0]._strColumnName = "name";
//			arrSQLTerms[0]._strOperator = "=";
//			arrSQLTerms[0]._objValue = "John Noor";
//
//			arrSQLTerms[1]._strTableName = "Student";
//			arrSQLTerms[1]._strColumnName = "gpa";
//			arrSQLTerms[1]._strOperator = "=";
//			arrSQLTerms[1]._objValue = new Double(1.5);
//
//			String[] strarrOperators = new String[1];
//			strarrOperators[0] = "OR";
//			// select * from Student where name = "John Noor" or gpa = 1.5;
//			Iterator resultSet = dbApp.selectFromTable(arrSQLTerms, strarrOperators);
		} catch (Exception exp) {
			System.out.println("Error here in main method!!!");
			exp.printStackTrace();
			System.out.println("Error here in main method!!!");
		}
	}

	public static double convertTo(String z) {
		String x = z.toLowerCase();
		int y = 1;
		double result = 0;
		for (int i = 0; i < x.length(); i++) {
			result += x.charAt(i) * Math.pow(10, y);
		}
		return result;
	}
	 public static int readNumberFromConfig() {
	        Properties prop = new Properties();
	        try{
	        	FileInputStream fileInputStream = new FileInputStream("DBApp.config") ;
	            prop.load(fileInputStream);
	            String property = prop.getProperty("MaximumRowsCountinPage");
	            if (property != null) {
	                return Integer.parseInt(property);
	            } else {
	                System.err.println("Property 'MaximumRowsCountinPage' not found in the configuration file.");
	                return -1; // Or handle the error as appropriate
	            }
	        } catch (Exception e) {
	            e.printStackTrace();
	            return -1;
	        }
	    }
}
