package database_project;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.Vector;

public class Tuple implements Serializable {
	String name;
	Vector<String> values;

	public Tuple(Page parent) {
		values = new Vector<String>();
		name= parent.name+"_"+parent.numTuples;
		parent.numTuples++;
	}

	public String toString() {

		String string_tuple = "";
		for (int i = 0; i < values.size(); i++) {
			if (i != 0)
				string_tuple += "," + values.get(i).toString();
			else
				string_tuple = values.get(i).toString();

		}
		return string_tuple;
	}

	public static Tuple Tpldeserialize(String filename) {
		Tuple Temp = null;
		try {
			FileInputStream file = new FileInputStream(filename);
			ObjectInputStream in = new ObjectInputStream(file);

			// Method for deserialization of object
			Temp = (Tuple) in.readObject();

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

	public void serialize(String tupleName) {
		try {
			// Saving of object in a file
			FileOutputStream file = new FileOutputStream(tupleName + ".ser");
			ObjectOutputStream out = new ObjectOutputStream(file);

			// Method for serialization of object
			out.writeObject(this);
			out.close();
			file.close();
		} 
		catch (IOException ex) {
			ex.printStackTrace();
			System.out.println("IOException is caught");
			System.out.println("tuple serialization error!!");
		}
	}

}
