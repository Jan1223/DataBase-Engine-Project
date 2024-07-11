package database_project;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.Vector;

public class Page implements Serializable {
	final static int N = DBApp.readNumberFromConfig();
	String name;
	int numTuples=0;
	Vector<String> tuples;

	public Page(String tblName, int pageNum) {
		name = tblName + pageNum;
		tuples = new Vector<String>();
	}

	public String toString() {

		String string_page = "";
		for (int i = 0; i < tuples.size(); i++) {
			if (i != 0)
				string_page += "," + tuples.get(i).toString();
			else
				string_page = tuples.get(i).toString();

		}
		return string_page;
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
		}

		catch (ClassNotFoundException ex) {
			System.out.println("ClassNotFoundException is caught");
		}
		return Temp;

	}

	public void serialize(String pageName) {
		try {
			// Saving of object in a file
			FileOutputStream file = new FileOutputStream(pageName + ".ser");
			ObjectOutputStream out = new ObjectOutputStream(file);

			// Method for serialization of object
			out.writeObject(this);

			out.close();
			file.close();
		} catch (IOException ex) {
			ex.printStackTrace();
			System.out.println("IOException is caught");
			System.out.println("page serialization error!!");
		}
	}

	public Object get(int i) {
		return tuples.get(i);
	}

//	public int binary_search(String value, int index) {
//		Page curr = this.pdeserialize(name);
//		// check last value to skip page
//		if (curr.tuples.get(tuples.size()).values.get(index).compareTo(value) < 0) {
//			return -1;
//		}
////			if (curr.tuples.get(tuples.size()).values.get(index).compareTo((String) value) > 1) {
////				return -2;
////			}
//
//		int i = (tuples.size()) / 2;
//		while (i != tuples.size() && i != 0) { // binary search loop
//			int result = curr.tuples.get(i).values.get(index).compareTo(value);
//			if (result < 0) {
//				i += i / 2;
//			}
//			if (result == 1) {
//				return i;
//			} else {
//				i -= i / 2;
//			}
//		}
//		if (i == 0)
//			return 0;
//		return tuples.size(); // no -1 default as it already skips page fw2
//	}
//
//	public int search_insert(String value, int index) { // to br called by Table class
//		Page curr = this.pdeserialize(name);
//		if (curr.tuples.get(0).values.get(index).compareTo(value) > 1) {
//			return 0;
//		}
//		for (int i = 0; i < tuples.size() - 1; i++) {
//			if (value instanceof java.lang.String) {
//				if (curr.tuples.get(i).values.get(index).compareTo(value) < 0
//						&& curr.tuples.get(i).values.get(index).compareTo(value) > 1) {
//					return i + 1;
//				}
//			}
//		}
//		return tuples.size();
//	}
//
//	public int binary_search_insert(String value, int index) {
//		Page curr = this.pdeserialize(name);
//		// check last value to skip page
//		if (curr.tuples.get(tuples.size()).values.get(index).compareTo(value) < 0) {
//			return -1;
//		}
////			if (curr.tuples.get(tuples.size()).values.get(index).compareTo((String) value) > 1) {
////				return -2;
////			}
//
//		int i = (tuples.size()) / 2;
//		while ((i != tuples.size() - 1) && i != 0) { // binary search loop
//			int before = curr.tuples.get(i).values.get(index).compareTo((String) value);
//			int after = curr.tuples.get(i + 1).values.get(index).compareTo((String) value);
//			if (after < 0) {
//				i += i / 2;
//			}
//			if (before < 0 && after > 1) {
//				return i + 1;
//			} else {
//				i -= i / 2;
//			}
//		}
//		if (i == 0)
//			return 0;
//		return tuples.size(); // no -1 default as it already skips page fw2
//	}

}
