package teamdb;

import java.io.File;
import java.util.*;

import teamdb.classes.*;

public class Run {
	public static int pageCount = 5;
	public static int tupleCount = 3;
	public static int id = 1;

	public static int getRandomInt(int upper) {
		Random random = new Random();
        int randomNumber = random.nextInt(upper - 0 + 1) + 0;
		if(randomNumber == 0)
			return 0;
        return randomNumber - 1;
	}

	public static double getRandomDouble(double upperBound) {
        Random random = new Random();
        double randomDouble = random.nextDouble() * (upperBound - 0.0) + 0.0;
        randomDouble = Math.round(randomDouble * 100.0) / 100.0;
        return randomDouble;
    }

    //@SuppressWarnings("rawtypes")
	@SuppressWarnings("unused")
	public static void main(String[] args) throws Exception {		
		File file = new File(Serializer.filePath + "AageIndex.class");
		file.delete();
		file = new File(Serializer.filePath + "AgpaIndex.class");
		file.delete();
		file = new File(Serializer.filePath + "AheightIndex.class");
		file.delete();
		file = new File(Serializer.filePath + "AidIndex.class");
		file.delete();
		file = new File(Serializer.filePath + "AmajorIndex.class");
		file.delete();
		file = new File(Serializer.filePath + "AnameIndex.class");
		file.delete();
		file = new File(Serializer.filePath + "AuniversityIndex.class");
		file.delete();
		file = new File(Serializer.filePath + "Student.class");
		file.delete();file = new File(Serializer.filePath + "AgpaIndex.class");
		file.delete();
		
		String[] names = {"ahmed","shiko","ali","engy","adam","tatos"};//,"hatem","mohamed","hany","amr","sameh","abdelrahman","adham"};
		String[] unis = {"guc","auc","bue"};//,"fue","miu","must","buc","ngu"};
		String[] majors = {"engineering","business","medicine"}; //,"architecture","pharmacy","law","applied arts"};
		double[] heights = {1.6,1.7,1.8,1.9};
		double[] gpas = {1.0,2.0,3.0,4.0};
		int[] ages = {19,20,21,22};
		
    	DBApp db = new DBApp();

    	Hashtable<String, String> ht = new Hashtable<String, String>();
    	ht.put("id", "java.lang.Integer");
    	ht.put("name", "java.lang.String");
    	ht.put("gpa", "java.lang.Double");
		ht.put("university", "java.lang.String");
    	ht.put("age", "java.lang.Integer");
    	ht.put("height", "java.lang.Double");
		ht.put("major", "java.lang.String");

		ArrayList<String> columns = new ArrayList<String>();
		columns.add("id");
		columns.add("name");
		columns.add("gpa");
		columns.add("university");
		columns.add("age");
		columns.add("height");
		columns.add("major");


    	db.createTable("Student", "id", ht);
		System.out.println("");

		Table table = (Table) Serializer.deSerialize("Student");

		ArrayList<String> indexNames = new ArrayList<String>();

		/*for(int k = 0; k < getRandomInt(columns.size());k++) {
			String col = columns.get(getRandomInt(columns.size()));
			db.createIndex("Student", col, col + "Index");
			indexNames.add(col + "Index");
		}*/

		for(int k = 0; k < 7;k++) {
			String col = columns.get(k);
			db.createIndex("Student", col, "A" + col + "Index");
			indexNames.add("A" + col + "Index");
		}
		table = (Table) Serializer.deSerialize("Student");

		Vector<Integer> ids = new Vector<Integer>();
		for(int i = 0; i< pageCount;i++) {
			for(int j = 0; j < tupleCount;j++) {
				Hashtable<String, Object> hm = new Hashtable<String, Object>();
				id = getRandomInt(1000);
				while(ids.contains(id)) {
					id = getRandomInt(1000);
				}
				System.out.println("Cluster: " + id);
				ids.add(id);
				hm.put("id", id);
				hm.put("name", names[getRandomInt(names.length)]);
				hm.put("gpa", gpas[getRandomInt(gpas.length)]);
				hm.put("university", unis[getRandomInt(unis.length)]);
				hm.put("age", ages[getRandomInt(ages.length)]);
				hm.put("height", heights[getRandomInt(heights.length)]);
				hm.put("major", majors[getRandomInt(majors.length)]);
				
				//Tuple tuple = new Tuple(hm);
				db.insertIntoTable("Student", hm);
				//db.insertAhmed(tuple, table);
			}
			//Serializer.serialize(table,table.getTableName());
		}
		System.out.println(" BEFORE:\n");
		Table t = (Table) Serializer.deSerialize("Student");
		System.out.println("Created table: \n" + Serializer.deSerialize("Student") + "\n \n \n");
		System.out.println(t.pageMin);
		System.out.println(t.pageMax);
		System.out.println("\n");

		Index index1 = (Index) Serializer.deSerialize("AidIndex");
			System.out.println("Id Index: ");
			int count1 = 0;
			for(int i = 0; i < ids.size(); i++) {
				System.out.println("" + ids.get(i) + index1.search(ids.get(i)));
			}

		// System.out.println("MANUAL DELETEEEEEEEEEE \n \n");
		// t = (Table) Serializer.deSerialize("Student");
		// Page p = (Page) Serializer.deSerialize(t.getPageFileNames().get(0));
		// System.out.println(p.getTuples());
		// System.out.println("index of tuple: " + 0);
		// Object de = p.getTuples().get(0).getValue("id");
		// Hashtable h = new Hashtable<>();
		// h.put("id", de);
		// System.out.println("MANUAL DELETE OF: "+ de);
		// db.deleteFromTable("Student", h);
		Vector<Double> gpaVals = new Vector<Double>();

		for(int i = 0; i< 3;i++){
				Hashtable<String, Object> hm = new Hashtable<String, Object>();
				
				id = ids.get(getRandomInt(ids.size()));
				double gpa = getRandomDouble(gpas.length);
				gpaVals.add(gpa);
				System.out.println("Update: " + id + " to : \n");
				hm.put("name", names[getRandomInt(names.length)]);
				hm.put("gpa", gpa);
				hm.put("university", unis[getRandomInt(unis.length)]);
				hm.put("age", ages[getRandomInt(ages.length)]);
				hm.put("height", heights[getRandomInt(heights.length)]);
				hm.put("major", majors[getRandomInt(majors.length)]);

				System.out.println(hm);
				System.out.println();

				db.updateTable("Student", id+"", hm);
				//db.insertAhmed(tuple, table);
			}

			for(int i = 0; i< 3;i++){
				Hashtable<String, Object> hm = new Hashtable<String, Object>();
				
				id = ids.get(getRandomInt(ids.size()));
				System.out.println("Delete: " + id + " to : \n");
				hm.put("id", id);
				// hm.put("name", names[getRandomInt(names.length)]);
				// hm.put("gpa", gpas[getRandomInt(gpas.length)]);
				// hm.put("university", unis[getRandomInt(unis.length)]);

				System.out.println(hm);
				System.out.println();

				db.deleteFromTable("Student", hm);
				//db.insertAhmed(tuple, table);
			}

		SQLTerm[] arrSQLTerms;
		arrSQLTerms = new SQLTerm[2];
		arrSQLTerms[0]=new SQLTerm();
		arrSQLTerms[1]=new SQLTerm();
		arrSQLTerms[0]._strTableName = "Student";
		arrSQLTerms[0]._strColumnName= "id";
		arrSQLTerms[0]._strOperator = "=";
		arrSQLTerms[0]._objValue = ids.get(4);
		arrSQLTerms[1]._strTableName = "Student";
		arrSQLTerms[1]._strColumnName= "name";
		arrSQLTerms[1]._strOperator = ">";
		arrSQLTerms[1]._objValue = "adam";
		String[]strarrOperators = new String[1];
		strarrOperators[0] = "AND";
		// select * from Student where name = “John Noor” or gpa = 1.5;
		Iterator resultSet = db.selectFromTable(arrSQLTerms , strarrOperators);
		System.out.println("\n \n \n \n");
		System.out.println(arrSQLTerms[0]._objValue);
		System.out.println("select results");
		while(resultSet.hasNext()){
			System.out.println(resultSet.next());
		}
		t = (Table) Serializer.deSerialize("Student");
		System.out.println("Created table: \n" + Serializer.deSerialize("Student") + "\n \n \n");
		System.out.println(t.pageMin);
		System.out.println(t.pageMax);
		System.out.println("\n");
	  /* s

		Hashtable<String,Object> deleteTest = new Hashtable<String,Object>();
		int randomNumOfDeleteTest = getRandomInt(7);
		for(int l = 0; l < randomNumOfDeleteTest; l++) {
			int m = getRandomInt(7);
			Object random;
			switch(m) {
				case 0: random = names[getRandomInt(names.length)];deleteTest.put("name", random); break;
				case 1: random = unis[getRandomInt(unis.length)];deleteTest.put("university", random); break;
				case 2: random = majors[getRandomInt(majors.length)];deleteTest.put("major", random); break;
				case 4: random = ages[getRandomInt(ages.length)];deleteTest.put("age", random); break;
				case 5: random = gpas[getRandomInt(gpas.length)];deleteTest.put("gpa", random); break;
				case 6: random = heights[getRandomInt(heights.length)];deleteTest.put("height", random); break;
				default: break;
			}
		}
		System.out.println("Deleting using: " + deleteTest + "\n \n");
		db.deleteFromTable("Student", deleteTest);
		System.out.println("\n\nTable after deleting: \n" + Serializer.deSerialize("Student") + "\n \n \n");

		 
		Hashtable<String,Object> updateTest = new Hashtable<String,Object>();
		int randomNumOfUpdateTest = getRandomInt(7);
		for(int l = 0; l < randomNumOfUpdateTest; l++) {
			int m = getRandomInt(6);
			switch(m) {
				case 0: updateTest.put("name", names[getRandomInt(names.length)]);break;
				case 1: updateTest.put("gpa",getRandomDouble(4.0));break;
				case 2: updateTest.put("university", unis[getRandomInt(unis.length)]);break;
				case 3: updateTest.put("age", getRandomInt(23));break;
				case 4: updateTest.put("height", getRandomDouble(2.02));break;
				case 5: updateTest.put("major", majors[getRandomInt(majors.length)]);break;
				default: break;
			}
		}
		String updateId = ids.get(getRandomInt(ids.size())) +"";
		System.out.println("Updating: " + updateId + "\n Using: " + updateTest + "\n \n");
		db.updateTable("Student", updateId, updateTest);
		Table newTable = (Table) Serializer.deSerialize("Student");
		System.out.println("Table after updating: \n" + newTable + "\n \n \n");
*/

		try {
			Index index = (Index) Serializer.deSerialize("AidIndex");
			System.out.println("Id Index: ");
			int count = 0;
			for(int i = 0; i < ids.size(); i++) {
				System.out.println("" + ids.get(i) + index.search(ids.get(i)));
			}
			System.out.println("");
			index = (Index) Serializer.deSerialize("AnameIndex");
			System.out.println("Name Index: ");
			count = 0;
			for(int i = 0; i < names.length; i++) {
				System.out.println(names[i] + index.search(names[i]));
			}
			System.out.println("");
			index = (Index) Serializer.deSerialize("AageIndex");
			System.out.println("Age Index: ");
			for(int i = 0; i < ages.length; i++) {
				System.out.println(ages[i]+"" + index.search(ages[i]));

			}
			System.out.println("");
			index = (Index) Serializer.deSerialize("AgpaIndex");
			System.out.println("Gpa Index: ");
			for(int i = 0; i < gpas.length; i++) {
				System.out.println(gpas[i] + "" + index.search(gpas[i]));
			}
			for(int i = 0; i < gpaVals.size(); i++) {
				System.out.println(gpaVals.get(i) + "" + index.search(gpaVals.get(i)));
			}
			System.out.println("");
			index = (Index) Serializer.deSerialize("AheightIndex");
			System.out.println("Height Index: ");
			for(int i = 0; i < heights.length; i++) {
				System.out.println(heights[i] + "" +index.search(heights[i]));
			}
			System.out.println("");
			index = (Index) Serializer.deSerialize("AmajorIndex");
			System.out.println("Major Index: ");
			for(int i = 0; i < majors.length; i++) {
				System.out.println(majors[i] + index.search(majors[i]));

			}
			System.out.println("");
			index = (Index) Serializer.deSerialize("AuniversityIndex");
			System.out.println("University Index: ");
			for(int i = 0; i < unis.length; i++) {
				System.out.println(unis[i] + index.search(unis[i]));
			}
			System.out.println("");
		} catch (Exception e) {
			System.out.println("Error");
		}
    }
}
