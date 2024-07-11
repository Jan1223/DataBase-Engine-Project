package database_project;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Vector;

public class Table implements Serializable {
	String name;
	int numberofpages;
	int numberoftuples;
	Vector<String> pages;
	Vector<String> columns;

	public Table(String tblName) {
		name = tblName;
		numberofpages = 0;
		numberoftuples = 0;
		pages = new Vector<String>();
		columns = new Vector<String>();

	}

	public void insert(Tuple j) {
		if (numberoftuples % Page.N == 0) {
			// Page p = new Page();
			numberofpages++;

			// p.insert(j);
			// -serialization-FileOutputStream s=new
			// FileOutputStream(pages.lastElement());
			// -serialization-ObjectOutputStream x=new ObjectOutputStream() ;
			// pages.add(p);
		}
	}

	public int[] search(String key) throws IOException, ClassNotFoundException {// search
																				// for
																				// insertion
																				// using
																				// index
																				// still
																				// to
																				// be
																				// defined
		// assuming keys values inputed as strings
		try {
			int[] arr = new int[2];
			int intKey = Integer.parseInt(key);
			int intLastElement = Integer.parseInt(pages.lastElement());// still
																		// need
																		// to be
																		// changed
																		// from
																		// tuples
																		// to
																		// int
			for (int i = 0; i < pages.size(); i++) {
				Page temp = pdeserialize(pages.get(i));
				// need deserialization first
				if (intLastElement >= intKey) {// duplicates are handled in
												// insert method
					for (int j = 0; j < temp.tuples.size(); j++) {
						// int intPg = Integer.parseInt(pageTemp.tuples.get(j));
						int intPg = 0;// needs adjustment
						if (intPg >= intKey) {
							arr[0] = i;
							arr[1] = j;
							return arr;
						}
					}
				}
			}
		} catch (NumberFormatException e) {
			int[] arr = new int[2];
			for (int i = 0; i < pages.size(); i++) {
				Page temp = pdeserialize(pages.get(i));
				if (pages.lastElement().compareTo(key) < 1) {// duplicates are
																// handled in
																// insert method
					for (int j = 0; j < temp.tuples.size(); j++) {
						// String strPg = pageTemp.tuples.get(j);
						String strPg = "";// needs adjustment
						if (strPg.compareTo(key) > -1) {
							arr[0] = i;
							arr[1] = j;
							return arr;
						}
					}
				}
			}
		}
		return null;
	}

	public static Page pdeserialize(String filename) {
		Page Temp = null;
		try {
			FileInputStream file = new FileInputStream(filename);
			ObjectInputStream in = new ObjectInputStream(file);

			// Method for deserialization of object
			Temp = (Page) in.readObject();

			in.close();
			file.close();
		} catch (IOException ex) {
			System.out.println("IOException is caught");
			System.out.println("page deserialize error!");
		} catch (ClassNotFoundException ex) {
			System.out.println("ClassNotFoundException is caught");
		}
		return Temp;
	}

	public static Table Tbldeserialize(String filename) {
		Table tableTemp = null;
		try {
			FileInputStream file = new FileInputStream(filename);
			ObjectInputStream in = new ObjectInputStream(file);

			// Method for deserialization of object
			tableTemp = (Table) in.readObject();

			in.close();
			file.close();
		} catch (IOException ex) {
			System.out.println("IOException is caught");
			System.out.println("Table deserialization error!!");
		} catch (ClassNotFoundException ex) {
			System.out.println("ClassNotFoundException is caught");
		}
		return tableTemp;
	}

	public void tblserialize(String tblname) {
		try {
			// Saving of object in a file
			FileOutputStream file = new FileOutputStream(tblname + ".ser");
			ObjectOutputStream out = new ObjectOutputStream(file);

			// Method for serialization of object
			out.writeObject(this);

			out.close();
			file.close();
		} catch (IOException ex) {
			ex.printStackTrace();
			System.out.println("IOException is caught");
			System.out.println("Table serialization error!!");
		}
	}

	public Vector<int[]> search(String value, int index) {// value could be any
															// type
		Vector<int[]> res = new Vector<int[]>();
		for (int i = 0; i < pages.size(); i++) {
			Page p = Page.pdeserialize((pages).elementAt(i));
			for (int u = 0; u < p.tuples.size(); u++) {
				Tuple d = Tuple.Tpldeserialize(p.tuples.elementAt(u));
				if (d.values.get(index).equals(value)) {
					int[] results = new int[2];
					results[0] = i;
					results[1] = u;
					res.add(results);
				}
			}
			System.out.println(pages.size());
		}
		return res;
	}
	public Vector<int[]> binary_search(String value, int index){
		Vector<int []> results=new Vector<int []>();
		int start =0;
		int end=pages.size()-1;
		int mid=(start+end)/2;
		if(start>end){
			return results;
		}
		Page tempPage=null;
		while(start<=end){
			mid=(start+end)/2;
			tempPage=Page.pdeserialize(pages.get(mid));
			Tuple first=Tuple.Tpldeserialize(tempPage.tuples.firstElement());
			Tuple last=Tuple.Tpldeserialize(tempPage.tuples.lastElement());
			Comparable finTable;
			Comparable linTable;
			Comparable dvalue;
			try{
				finTable=Double.parseDouble(first.values.get(index));
				linTable=Double.parseDouble(last.values.get(index));
				dvalue=Double.parseDouble(value);
			}
			catch(Exception e){
				finTable=first.values.get(index);
				linTable=last.values.get(index);
				dvalue=value;
			}
			
			if(dvalue.compareTo(finTable)<0){
				end=mid-1;
			}
			else if(dvalue.compareTo(linTable)>0){
				start =mid+1;
			}
			else if(dvalue.compareTo(finTable)>=0 && dvalue.compareTo(linTable)<=0){
				break;
			}
		}
		
		int startp=0;
		int endp=tempPage.tuples.size()-1;
		int midp= (startp+endp)/2;
		while(startp<=endp){
			midp=(startp+endp)/2;
			Tuple tempTuple =Tuple.Tpldeserialize(tempPage.tuples.get(midp));
			Comparable dvalue;
			Comparable inTable;
			try{
				dvalue=Double.parseDouble(value);
				inTable=Double.parseDouble(tempTuple.values.get(index));
			}
			catch(Exception e){
				dvalue=value;
				inTable=tempTuple.values.get(index);
			}
			if(dvalue.compareTo(inTable)<0)
				endp=midp-1;
			if(dvalue.compareTo(inTable)>0)
				startp=midp+1;
			if(dvalue.compareTo(inTable)==0)
				return getduplicates(mid, midp,value,index);
		}
		System.out.println("hi" +midp);
		return null;
	}

	public Vector<int[]> getduplicates(int i, int j, String value, int index) {
		Page tmp = pdeserialize(pages.get(i));
		Tuple t = Tuple.Tpldeserialize(tmp.tuples.get(j));
		int tempi = i;
		int tempj = j;
		Vector<int[]> results = new Vector<int[]>();
		while (t.values.get(index).equals(value)) {
			if (j == 0 && i != 0) {
				i--;
				tmp = pdeserialize(pages.get(i));
				j = tmp.tuples.size() ;
			}
			j--;
			if(j == -1 && i == 0)
				break;
			t = Tuple.Tpldeserialize(tmp.tuples.get(j));
		}
		if (j == tmp.tuples.size() - 1) {
			j = 0;
			i++;
		} else {
			j++;
		}
		Page p = pdeserialize(pages.get(tempi));
		Tuple t2 = Tuple.Tpldeserialize(p.tuples.get(tempj));
		while (t2.values.get(index).equals(value)) {
			if (tempj == p.tuples.size() - 1 && tempi != pages.size() - 1) {
				tempi++;
				p = pdeserialize(pages.get(tempi));
				tempj = -1;
			}
			tempj++;
			if(tempj == p.tuples.size() && tempi == pages.size() - 1)
				break;
			t2 = Tuple.Tpldeserialize(tmp.tuples.get(tempj));
		}
	
		
		while(i!=tempi || j!=tempj){
			Page resp=Page.pdeserialize(pages.get(i));
			int[] temp ={i,j};
			results.add(temp);
			if (j == resp.tuples.size() - 1 && i != pages.size() - 1) {
				i++;
				j=0;
				resp = pdeserialize(pages.get(i));
			}
			j++;
		}
		return results;

	}

}
