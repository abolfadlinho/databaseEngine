package teamdb;


/** * @author Wael Abouelsaadat */ 

import java.util.Iterator;
import java.util.Map;
import java.util.Vector;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.InvalidClassException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.Map.Entry;

import javax.print.DocFlavor.STRING;
import javax.sql.rowset.serial.SerialArray;

import java.util.Set;

import teamdb.classes.*;
import teamdb.classes.bplustree.LeafNode;

@SuppressWarnings({ "unchecked", "rawtypes", "unused", "removal", "resource"})
public class DBApp {

	private Vector<String> tableNamesArray;
	public File csv;

	public DBApp( ){
		init();
	}

	// this does whatever initialization you would like 
	// or leave it empty if there is no code you want to 
	// execute at application startup 
	public void init( ){
		tableNamesArray = new Vector<String>();
	}

	// following method creates one table only
	// strClusteringKeyColumn is the name of the column that will be the primary
	// key and the clustering column as well. The data type of that column will
	// be passed in htblColNameType
	// htblColNameValue will have the column name as key and the data 
	// type as value
	public void createTable(String strTableName, 
							String strClusteringKeyColumn,  
							Hashtable<String,String> htblColNameType) throws DBAppException{
		if(Serializer.wasSerialized(strTableName)){
			throw new DBAppException("Table already exists");
		}	
		for(Map.Entry<String,String> entry: htblColNameType.entrySet()) {
			String type =  entry.getValue().toLowerCase();
			if(type.compareTo("java.lang.integer") != 0 && type.compareTo("java.lang.string") !=0 && type.compareTo("java.lang.double") !=0){
				throw new DBAppException("Unsupported data type");
			}
		}	
		Table table = new Table(strTableName,strClusteringKeyColumn,htblColNameType);
		tableNamesArray.add(strTableName);
		try {
			updateMeta();
		} catch (Exception e) {
		}
	}


	// following method creates a B+tree index 
	public void createIndex(String   strTableName,
							String   strColName,
							String   strIndexName) throws DBAppException{

		if(!Serializer.wasSerialized(strTableName)){
			throw new DBAppException("Table does not exist");
		}	

		if(!tableNamesArray.contains(strTableName)){
			throw new DBAppException("Table does not exist");
		}
		if(Serializer.wasSerialized(strIndexName)){
			throw new DBAppException("Index already exists"); 
		}
		try {
			Table table = (Table) Serializer.deSerialize(strTableName);
			if(!(table.getColNameType().containsKey(strColName))){
				throw new DBAppException("column does not exist in table");
			}
			Index index = new Index(strTableName, strColName, strIndexName,table.getClusteringKeyColumn());
			// Enter all tuples in index
			Vector<String> tablePageNames = table.getPageFileNames();
			String type = typeOfKey(strTableName, strColName);
			for(int i=0;i<tablePageNames.size();i++){
				Page page = AdamHelpers.getPage(tablePageNames, i);
				Vector<Tuple> tuples = page.getTuples();
				for(int j=0;j<tuples.size();j++){
					Comparable convertedValue;
					String value = tuples.elementAt(j).getValues().get(strColName).toString();
					switch(type){
						case "java.lang.Integer": 
							convertedValue = Integer.parseInt(value);
							break;
						case "java.lang.Double": 
							convertedValue = Double.parseDouble(value);
							break;
						default: 
							convertedValue = value;
					}
					index.insert(convertedValue, page.fileName);
				}       
			}
			table.addIndexHash(strColName, strIndexName);
			Serializer.serialize(table, strTableName);
			Serializer.serialize(index, strIndexName);
			updateMeta();
		} catch (Exception e) {
			throw new DBAppException("error in createIndex: " + e.getMessage()); 		}

	}


	// following method inserts one row only. 
	// htblColNameValue must include a value for the primary key
	// following method inserts one row only. 
	// htblColNameValue must include a value for the primary key
	public void insertIntoTable(String strTableName, 
	Hashtable<String,Object>  htblColNameValue) throws DBAppException{
	Table t;
	if(!Serializer.wasSerialized(strTableName)){
		throw new DBAppException("Table does not exist");
	}
	try {
		t = (Table) Serializer.deSerialize(strTableName);
	} catch (Exception e) {
		throw new DBAppException("An error occured while deserializing the table");
	}



	Hashtable<String,String> hash = t.getIndexHash();  //column name , Index name
	Vector<Vector<String>> meta = getCSV();
	
	if(htblColNameValue.size()!=t.getColNameType().size()){
		// Or add the values but as null
		throw new DBAppException("Number of values are not the same");
	}
	
	for (Map.Entry<String, Object> entry : htblColNameValue.entrySet()) {
		String columnName = entry.getKey(); // Getting Key (column name)
		Object columnValue = entry.getValue();
		Class<?> columnClass = columnValue.getClass();
		String columnClassName = columnClass.getSimpleName();
		Boolean found = false;
		

		for(int i=0;i<meta.size();i++){
			String tableNameMeta = meta.get(i).get(0);
			String columnNameMeta = meta.get(i).get(1);
			if(tableNameMeta.compareTo(strTableName)==0 && columnNameMeta.compareToIgnoreCase(columnName)==0){
				// At current Table and current column
				found = true;
				String columnTypeMeta = meta.get(i).get(2);
				// Checking that the type of input and type of column in metadata are the same, if not throw an exception
				if(!(columnValue instanceof Integer) && !(columnValue instanceof String) && !(columnValue instanceof Double)){
					throw new DBAppException("Unsupported data type");
				}
				if(columnTypeMeta.toLowerCase().compareTo("java.lang.integer") == 0 && !(columnValue instanceof Integer)){
					throw new DBAppException("Expected type: " + columnTypeMeta + " while inserted type: " + columnValue.getClass());
				}else
				if(columnTypeMeta.toLowerCase().compareTo("java.lang.string") == 0 && !(columnValue instanceof String)){
					throw new DBAppException("Expected type: " + columnTypeMeta + " while inserted type: " + columnValue.getClass());
				}else
				if(columnTypeMeta.toLowerCase().compareTo("java.lang.double") == 0 && !(columnValue instanceof Double)){
					throw new DBAppException("Expected type: " + columnTypeMeta + " while inserted type: " + columnValue.getClass());
				}
			}								
		}
		// If said column not in metadata then throw exception
		if(!found){
			throw new DBAppException("Column: " + columnName + " does not exist in the table");
		}
	}							
	
	Index index;

	// Stopped Here

	//continued --> tatos

	// Transfer contents from Hashtable to HashMap

	HashMap<String, Object> hashMap1 = new HashMap<>();
	for (Map.Entry<String, Object> entry : htblColNameValue.entrySet()) {
		hashMap1.put(entry.getKey(), entry.getValue());
	}

	Tuple tupletoinsert = new Tuple(htblColNameValue);	// our tuple with values to insert

String primarykeyy = t.getClusteringKeyColumn();
String thedesiredpagename = "";
		//table has an index on the primary key column --> we'll use it in the insertion
if(!hash.containsKey(primarykeyy)){

//no index on the primary key --> insert normally

	   
		Object [] binarySearchReturn = binarysearchkonato(t, tupletoinsert);
// 3 return cases in binarysearchkonato: 
// 	 	1) string: this will be the first tuple to insert
//  	2) contains -1 : my tuple's PK is the smallest PK 
//  	3) normal ({rakameltuple, lastpage1, lastpage, rakamelpage})
		Page firstPage;
		if(binarySearchReturn.length == 2){
			throw new DBAppException("error occured when serializing or deserializing page");
		}
		if (binarySearchReturn[0] instanceof String) {
			try{
				 firstPage = t.createPage();
				
			}
			catch (Exception ex){
				  throw new DBAppException("An error occured while creating the first page");
				  
			}
			 try{
				Page despage = (Page) Serializer.deSerialize(firstPage.fileName);
				thedesiredpagename = despage.fileName;

				String ourinsertedrow = despage.addTuple(tupletoinsert);  //return string won't be used
				//serialize the page
				Vector<Tuple> voftupl = despage.getTuples();
				Comparable firstone = (Comparable) voftupl.get(0).getValue(t.getClusteringKeyColumn());
				Comparable lastone = (Comparable) voftupl.get(voftupl.size()-1).getValue(t.getClusteringKeyColumn());
				t.pageMin.put(despage.fileName, firstone);
				t.pageMax.put(despage.fileName, lastone);
				Serializer.serialize(despage, despage.fileName);
				
			}

			catch(Exception e){
				throw new DBAppException("An error occured while serializing or deserializing the page"); 
			}


		} else if (binarySearchReturn[0] instanceof Integer) {
			if((int)binarySearchReturn[0] == -1){
			 Object [] arrayofob = insertinpageskonato(t, tupletoinsert,0, -1);
			thedesiredpagename = (String) arrayofob[0];
			}
			else{
				int rakameltuple = (int) binarySearchReturn[0];  //tuple just before the tuple we wish to insert
				int rakamelpage = (int) binarySearchReturn[3]; 
				Object [] arrayofob = insertinpageskonato(t, tupletoinsert, rakamelpage, rakameltuple);
			thedesiredpagename = (String) arrayofob[0];
			}

		} 

	}

	else{
		//there exist an index on the primary key column that will be used in the insertion

		//recall: Hashtable<String,Index> hash = t.getIndexHash();  //column name , Index
		
		Object [] arrayofob = new Object[2]; 
		String primarykey = t.getClusteringKeyColumn();

		//table has an index on the primary key column --> we'll use it in the insertion
			String indexName = hash.get(primarykey);
			Index clusteringindex;
			try {
				clusteringindex = (Index) Serializer.deSerialize(indexName);
			} catch (Exception e) {
				throw new DBAppException("An error occured while deserializing the index");
			}
		   
		   bplustree bp = clusteringindex.getBTree();
		   
		   Object VTpInBt = tupletoinsert.getValue(primarykey); 
		  
		   Comparable comp = (Comparable) VTpInBt;
		   thedesiredpagename = bplustree.helperforindexsearch(bp,comp);
		
			if(thedesiredpagename.equals("we are going to insert our tuple in the last page if not full handled in DB APP")){  //if rightsibling = null
				try{
					if(t.getPageFileNames().size() == 0){
						Page firstp = t.createPage();

					}
					String thelastpagename = t.getPageFileNames().get((t.getPageFileNames()).size() -1);
					int rakamAkherPage = t.getPageFileNames().size()-1;
				
					Page thelastpage = (Page) Serializer.deSerialize(thelastpagename);
					
					int tupleToInsertAfter = binarysearchkonato2(thelastpage, tupletoinsert, t); //we found the page to insert into before the try but we still need to find our tuple's location in that page
		
					 arrayofob =  insertinpageskonato(t,tupletoinsert,rakamAkherPage,tupleToInsertAfter);
					thedesiredpagename = (String) arrayofob[0];
					
				} //holds the page name that stores our tuple
				catch(Exception ex){
					throw new DBAppException("An error occured while serializing or deserializing the page");
				}
				
		   }
		   else{  //right sibling != null        so thedesiredpagename holds the page name (String) that we'll insert the tuple into
				int rakamofthepage = 0;
			//	int i = 0;
				for(int i = 0 ; i< t.getPageFileNames().size(); i ++){
					if(thedesiredpagename.equals(t.getPageFileNames().get(i))){
						break;
					}
					rakamofthepage = rakamofthepage +1;
				}
			
				Page thedesiredpage;
				
				try{
					
					thedesiredpage = (Page) Serializer.deSerialize(thedesiredpagename);
				
					int rakamofthetuple = binarysearchkonato2(thedesiredpage, tupletoinsert, t);
					
					
					arrayofob = insertinpageskonato(t,tupletoinsert,rakamofthepage,rakamofthetuple);
					thedesiredpagename =  (String) arrayofob[0];
					
				}
				catch(Exception ex){
					throw new DBAppException("An error occured while serializing or deserializing the page");
				}
		   }

		   try{

			String comingfromarrofob = (String) arrayofob[1];
			if(comingfromarrofob.equals("hey")){

			}
			else{
				Serializer.serialize(clusteringindex, indexName);
			}
			
		   }
		   catch(Exception ex){
			throw new DBAppException("An error occured while serializing the index: " + indexName);
		   }
	   
	}

	//5alasna inserting fel pages (either normally or using PK index)

	// now we'll update all indexes we have and serialize them

	for (Map.Entry<String, String> entry : hash.entrySet()) {
		String columnName = entry.getKey(); // Getting Key (column name)
		String indexName = entry.getValue();
		try {
			index = (Index) Serializer.deSerialize(indexName);
		} catch (Exception e) {
			throw new DBAppException("error deserializing: " + indexName);
		}
		bplustree tree = index.getBTree();
		Comparable o1;


		//checking that the column that you have it's index is the same as the column you'll insert into it's index
		for (Map.Entry<String, Object> entry1 : htblColNameValue.entrySet()) {
			String column = entry1.getKey(); // Getting Key (column name)
			o1 = (Comparable)entry1.getValue();
			if(column.equals(columnName)){
				 Vector<String> vectorofpages = tree.search(o1);
				 if(vectorofpages == null){
					
				
					tree.insert(o1, thedesiredpagename);
				 }
				 else{
					
						 vectorofpages.add(thedesiredpagename);
	
					//}
				 }
				
				break;
			}
		}
		try{
			Serializer.serialize(index, indexName);
		}
		catch(Exception e){
			throw new DBAppException("An error occured while serializing the index: " + indexName);
		}
		

	}
	try{
		Serializer.serialize(t, strTableName);
	}
	catch(Exception ex){
		throw new DBAppException("An error occured while serializing the table ");
	}
}
	

	// following method updates one row only
	// htblColNameValue holds the key and new value 
	// htblColNameValue will not include clustering key as column name
	// strClusteringKeyValue is the value to look for to find the row to update.
	public void updateTable(String strTableName, String strClusteringKeyValue,Hashtable<String,Object> htblColNameValue)  throws DBAppException {
		if(!Serializer.wasSerialized(strTableName)){
			throw new DBAppException("Table does not exist");
		}	

		if(!validUpdate(strTableName, htblColNameValue)) {
			System.out.println("Invalid hashtable for update");
			return;
		}
		Table table = (Table) Serializer.deSerialize(strTableName);
		Object convertedClusterKeyValue = null;
		switch(typeOfClusterKey(strTableName)){
			case "java.lang.Integer": convertedClusterKeyValue = Integer.parseInt(strClusteringKeyValue);break;
			case "java.lang.Double": convertedClusterKeyValue = Double.parseDouble(strClusteringKeyValue);break;
			default: convertedClusterKeyValue = strClusteringKeyValue;
		}
		if(searchByIndexAndUpdate(strTableName,convertedClusterKeyValue,table.getClusteringKeyColumn(),htblColNameValue)) {
			System.out.println("Search was done by index");
		} else if (searchBinaryAndUpdate(strTableName, convertedClusterKeyValue, table.getClusteringKeyColumn(), htblColNameValue)) {
			System.out.println("Search was done binary");
		}
	}

	// following method could be used to delete one or more rows.
	// htblColNameValue holds the key and value. This will be used in search 
	// to identify which rows/tuples to delete. 	
	// htblColNameValue enteries are ANDED together
	public void deleteFromTable(String strTableName, Hashtable<String,Object> htblColNameValue) throws DBAppException{
		if(!Serializer.wasSerialized(strTableName)){
			throw new DBAppException("Table does not exist");
		}	

		if(htblColNameValue.size() == 0) {
			clearTable(strTableName);
		}
		
		if(!validDelete(strTableName, htblColNameValue)) {
			System.out.println("Invalid hashtable for delete");
			return;
		}

		if(searchByIndexAndDelete(strTableName, htblColNameValue)) {
			System.out.println("Search was done by index");
		} else if(searchBinaryAndDelete(strTableName, htblColNameValue)) {
			System.out.println("Search was done Binary");
		} else if(searchLinearAndDelete(strTableName, htblColNameValue)) {
			System.out.println("Search was done linearly");
		}
	}

	public Iterator selectFromTable(SQLTerm[] arrSQLTerms, String[]  strarrOperators) throws DBAppException{
		if(strarrOperators.length!=arrSQLTerms.length-1){
			throw new DBAppException("Invalid number of operators");
		}
        Vector<Tuple> totalResult = new Vector<Tuple>();
		if(arrSQLTerms.length==0){
			return totalResult.iterator();
		}
		String tableName = arrSQLTerms[0]._strTableName;
		for(int i=0;i<arrSQLTerms.length;i++) {
			Vector<Tuple> currentResult = new Vector<Tuple>();
			if(tableName.compareTo(arrSQLTerms[i]._strTableName)!=0){
				throw new DBAppException("All terms must be from the same table");
			}
			String columnName = arrSQLTerms[i]._strColumnName;
			String operator = arrSQLTerms[i]._strOperator;
			Comparable value = (Comparable) arrSQLTerms[i]._objValue;
			String columnType = typeOfKey(tableName, columnName);
			if(columnType==null){
				throw new DBAppException("Column type not found");
			}
			switch(columnType.toLowerCase()){ // to lower case because in main method: htblColNameType.put("gpa", "java.lang.double") which is not actually case sensitive to actual class name
				case "java.lang.integer": 
					if(value.getClass()!=Integer.class){
						throw new DBAppException(columnType +" expected but "+value.getClass()+" found in where clause.");
					}
					break;
				case "java.lang.double": 
					if(value.getClass()!=Double.class){
						throw new DBAppException(columnType +" expected but "+value.getClass()+" found in where clause.");
					}
					break;
				default: 
					if(value.getClass()!=String.class){
						throw new DBAppException(columnType +" expected but "+value.getClass()+" found in where clause.");
				}
			}
			if(!Serializer.wasSerialized(tableName)){
				throw new DBAppException("Table does not exist.");
			}
			Table table;
			try {
				table = (Table) Serializer.deSerialize(tableName);
			} catch (Exception e) {
				throw new DBAppException("Error while fetching table.");		
			}
			if(!table.getColNameType().containsKey(columnName)){
				throw new DBAppException("Column: " + columnName + " is not in table.");		
			}
			String clusterColumn = table.getClusteringKeyColumn();
			Hashtable<String,String> indexHash = table.getIndexHash();
			String indexName = (String) indexHash.get(columnName);
			Index index;
			if(indexName!=null){
				try {
					index = (Index) Serializer.deSerialize(indexName);
				} catch (Exception e) {
					throw new DBAppException(e.getMessage());
				}
			}else{
				index = null;
			}
			

			if(operator=="!="){
				// Loop through all pages and just add the ones that don't have this element because binary and index searches won't help
				try {
					currentResult = AdamHelpers.notEqual(table, columnName, value);
				} catch (Exception e) {
					throw new DBAppException(e.getMessage());
				}

			}else if(operator==">" || operator==">=" || operator == "<" || operator == "<="){

				// Index = log base m + number of tuples, binary = log base 2 + number of tuples, none = number of tuples
				switch (operator) {
					case ">":
							currentResult = AdamHelpers.linearSearchTuple(table, columnName, value , false, false);								
						break;										
					case ">=":
							currentResult = AdamHelpers.linearSearchTuple(table, columnName, value , false, true);																	
						break;		
					case "<":
							currentResult = AdamHelpers.linearSearchTuple(table, columnName, value , true, false);										
						break;	
					case "<=":
							currentResult = AdamHelpers.linearSearchTuple(table, columnName, value , true, true);								
						break;	
					default:
						throw new DBAppException("Impossible error");
				}			
			}else{
				// operator == '=='
				if(columnName.equals(table.getClusteringKeyColumn())){
					// Column is clustered
					Pair<Page,Integer> pair = AdamHelpers.binarySearch(table, value,true);
					if(pair!=null){
						currentResult.add(pair.getFirst().getTuples().get(pair.getSecond()));
					}	 					
				}else{
					// Column not clustered
					if(index!=null){						
						// Column is indexed
						// index will always be equal or better performance than binary according to m and better than linear
						try {
							currentResult = index.selectEqualTuples(columnName,value);
						} catch (Exception e) {
							throw new DBAppException("error while selecting from index: " + e.getMessage());
						}
					}else{
						// Column not indexed
						try{
							// Column is not clustered
							currentResult = AdamHelpers.linearSearchTuple(table,columnName, value);		
						}catch(DBAppException exception){
							throw new DBAppException("Selecting from pages without index did not work");
						}
					}
				}
			}

			if(i==0){
				// First results
				// So total results are the current results
				totalResult = currentResult;
			}else{
				// Not the first results
				// We get the relation according to the booleanOperator at strarrOperators[i-1]
				String booleanOperator =  strarrOperators[i-1];
				if(booleanOperator=="OR"){
					// Union of sets
					totalResult = AdamHelpers.getUnion(totalResult,currentResult,clusterColumn); // Not sure but has to be currentResult in second arguement because the second arguement changes in function 
				}else if(booleanOperator=="AND"){
					// Union of sets
					totalResult = AdamHelpers.getIntersection(totalResult,currentResult,clusterColumn); // Not sure but has to be currentResult in second arguement because the second arguement changes in function
				}else if(booleanOperator=="XOR"){
					totalResult = AdamHelpers.getXOR(totalResult, currentResult, clusterColumn); // Not sure but has to be currentResult in second arguement because the second arguement changes in function
				}else{
					throw new DBAppException("Boolean operator is not supported");
				}
			}
			currentResult = new Vector<Tuple>();
		}						
		return totalResult.iterator();
	}

	public void checkHtTypes(String tableName, Hashtable<String,Object> ht) throws DBAppException{
		for(Map.Entry<String,Object> column : ht.entrySet()) {
			String columnType = typeOfKey(tableName, column.getKey());
			Object value = column.getValue();
			switch(columnType){
				case "java.lang.Integer": 
					if(value.getClass()!=Integer.class){
						throw new DBAppException(columnType +" expected but "+value.getClass()+" found in where clause.");
					}
					break;
				case "java.lang.Double": 
					if(value.getClass()!=Double.class){
						throw new DBAppException(columnType +" expected but "+value.getClass()+" found in where clause.");
					}
					break;
				default: 
					if(value.getClass()!=String.class){
						throw new DBAppException(columnType +" expected but "+value.getClass()+" found in where clause.");
				}
			}
		}
	}

	public void updateMeta() throws FileNotFoundException,DBAppException {
		if(csv == null) {
			csv = new File(Serializer.filePath + "metadata.csv");
		}
		PrintWriter out = new PrintWriter(csv);
		
		out.printf("%s,%s,%s,%s,%s,%s\n","Table Name", "Column Name", "Column Type", "ClusteringKey", "IndexName","IndexType");
		for(int i=0;i<tableNamesArray.size();i++) {
			String tableName = tableNamesArray.get(i);
			Table table;
			try {
				table = (Table) Serializer.deSerialize(tableName);
			} catch (Exception e) {
				throw new DBAppException("Error while deserializing table: " + tableName);		
			}			
			Hashtable<String,String> ht = table.getColNameType();
			for (Map.Entry<String, String> entry : ht.entrySet()) {
				String hkey = entry.getKey();
				String value = entry.getValue();
				String indexName;
				String indexType;
				Index index;
				try {
					if(table.getIndexHash().get(hkey) != null) {
						index = (Index) Serializer.deSerialize(table.getIndexHash().get(hkey));
						indexName = index.getName();
						indexType = "B+tree";

					} else {
						indexType = "null";
						indexName = "null";
					}
				} catch(Exception e) {
					throw new DBAppException("error in updateMeta()");
				}

				String clusterBool = "False";
				if (hkey.equals(table.getClusteringKeyColumn())) {
					clusterBool = "True";
				}
				out.printf("%s,%s,%s,%s,%s,%s\n",table.getTableName(), hkey, value, clusterBool, indexName, indexType);
			}
		}	
		out.close();
	}

	public Vector<Vector<String>> getCSV() {
        String csvFile = Serializer.filePath + "metadata.csv"; // Path to your CSV file
        String csvDelimiter = ","; // CSV delimiter (usually comma)

        Vector<Vector<String>> data = new Vector<>();

        try (BufferedReader br = new BufferedReader(new FileReader(csvFile))) {
            String line;
            while ((line = br.readLine()) != null) {
                String[] values = line.split(csvDelimiter);
                Vector<String> row = new Vector<>();
                for (String value : values) {
                    row.add(value);
                }
                data.add(row);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return data;
    }

	public void printCSV() {
		Vector<Vector<String>> data = getCSV();
		for (Vector<String> row : data) {
            for (String value : row) {
                System.out.print(value + "\t");
            }
            System.out.println();
		}
	}

	public String hasIndex(Vector<Vector<String>> csv, String strTableName, String strColName) {
		for(int i = 0; i < csv.size();i++) {
			if(csv.get(i).get(0).equals(strTableName) && csv.get(i).get(1).equals(strColName)) {
				return csv.get(i).get(4);
			}
		}
		return "null";
	}

	public static Object[] insertinpageskonato  (Table table, Tuple tuple, int rakamelpage, int rakameltuple) throws DBAppException {
		Object [] tobereturned = new Object [2];
		tobereturned[1] = "";
		String primarykeycolo = table.getClusteringKeyColumn();
		Comparable thepkoftuple = (Comparable) tuple.getValue(primarykeycolo); 
		int theactualrakamofpage = rakamelpage;
		Vector<String> pageFileNames = table.getPageFileNames();
        
               boolean flag = true;
				String page = pageFileNames.get(rakamelpage);
				Page page1;
				try {
					
					page1 = (Page) Serializer.deSerialize(page);
					
				
				} catch (Exception e) {
					tobereturned[0] = "An error occured while deserializing the page";
					return tobereturned;
				}
				Vector<Tuple> tuples = page1.getTuples();
				int newindex = rakameltuple +1;  //position I will insert into
				if((newindex == tuples.size()) && (tuples.size() ==  (page1.getMaxRows())) ){

                  theactualrakamofpage = theactualrakamofpage +1;
				} 
                while (flag){
                    
					tuples.add(newindex, tuple);
					
					
					if(tuples.size() == (page1.getMaxRows()+1)){
						if(rakamelpage == (pageFileNames.size()-1)){  //check if it's the last page
							try{
								Page newest = table.createPage();
								String esmelPage = newest.getFileName();
								tuple = tuples.elementAt(tuples.size()-1); 
							
							
								tuples.remove(tuples.size()-1);
								
							
                                    
                                  tobereturned[1] = "hey";
                                 zabatindicesaccordingly(table, tuple, thepkoftuple, page1.fileName, newest.fileName);



								 Comparable firstone = (Comparable) tuples.get(0).getValue(table.getClusteringKeyColumn());
								 Comparable lastone = (Comparable) tuples.get(tuples.size()-1).getValue(table.getClusteringKeyColumn());
								 table.pageMin.put(page1.fileName, firstone);
								 table.pageMax.put(page1.fileName, lastone);


					
                                Serializer.serialize(page1,page1.fileName);
                                
								Page newest2 = (Page) Serializer.deSerialize(esmelPage);

								tuples = newest2.getTuples();

								 //removing the extra tuple from our full page (not the new created one)
								tuples.add(0,tuple);
                               Comparable firstone2 = (Comparable) tuples.get(0).getValue(table.getClusteringKeyColumn());
                               Comparable lastone2 = (Comparable) tuples.get(tuples.size()-1).getValue(table.getClusteringKeyColumn());
							   table.pageMin.put(esmelPage, firstone2);
							   table.pageMax.put(esmelPage, lastone2);
								Serializer.serialize(newest2, esmelPage); //check law el page beyet3emelaha serialize upon creation or not (serialize twice 3ady wala la2)
								
								break;
							}
							catch(Exception ex){
								tobereturned[0] = " ali An error occured while serializing the page";
								return tobereturned;
							}
							
						}
						tuple = tuples.elementAt(tuples.size()-1);  
					
						tuples.remove(tuples.size()-1);
						

						rakamelpage = rakamelpage +1;  //next page number
						String nextpage = pageFileNames.get(rakamelpage); // next page name
		
						tobereturned[1] = "hey";
						zabatindicesaccordingly(table, tuple, thepkoftuple, page1.fileName, nextpage);


						try {
							Comparable firstone = (Comparable) tuples.get(0).getValue(table.getClusteringKeyColumn());
							Comparable lastone = (Comparable) tuples.get(tuples.size()-1).getValue(table.getClusteringKeyColumn());
							table.pageMin.put(page1.fileName, firstone);
				
							table.pageMax.put(page1.fileName, lastone);
						
						    Serializer.serialize(page1, page1.fileName);
							page1 = (Page) Serializer.deSerialize(nextpage); // page1 is now holding the next page in the deserialized form
						
						} catch (Exception e) {
					       // handle the exception here
						   tobereturned[0] = "An error occured while serializing or deserializing the page";
							return tobereturned;
						}
						 tuples = page1.getTuples();
						 newindex = 0;
					}
					else{
						flag = false;
						try{
							Comparable firstone = (Comparable) tuples.get(0).getValue(table.getClusteringKeyColumn());
							Comparable lastone = (Comparable) tuples.get(tuples.size()-1).getValue(table.getClusteringKeyColumn());
							table.pageMin.put(page1.fileName, firstone);
							table.pageMax.put(page1.fileName, lastone);
							Serializer.serialize(page1, page1.fileName);
						}
						catch(Exception ex){
							tobereturned[0] = "An error occured while serializing the page";
							return tobereturned;
						}
						break;
					}
				}
				String thedesiredpage = table.getPageFileNames().get(theactualrakamofpage);
				tobereturned[0] = thedesiredpage;
				return tobereturned;
			   
		  
	}


	public static Object [] binarysearchkonato(Table table, Tuple tuple){
		
		String clusterColumnName = table.getClusteringKeyColumn();
		Vector<String> pageFileNames = table.getPageFileNames();
		int i =0;
		int j=pageFileNames.size()-1;
		boolean flag = true;
	    if(j == -1){
            Object [] theoutput = {"this will be the first tuple to insert"};
			// indicating that we will need to create the first page for the table and insert our first tuple
		    return theoutput; 
		}

		Page page1;
		Page firstpage1;
		Page lastpage1;
        String firstpage = pageFileNames.get(0);
		String lastpage = pageFileNames.get(j);
		
	

		Comparable mini =  table.pageMin.get(firstpage);
		Comparable mini2 = (Comparable) tuple.getValue(table.getClusteringKeyColumn());

         if(mini2.compareTo(mini)<0){

			// indicating that the tuple we want to insert will be the tuple with the smallest primary key
			// first object is location of tuple that will be just before the tuple we wish to insert
			Object [] theoutput = {-1, null, firstpage};
		    return theoutput;
		 }


		Comparable minforlp =  table.pageMin.get(lastpage);
		Comparable mini3 = (Comparable) tuple.getValue(table.getClusteringKeyColumn());
         if(mini3.compareTo(minforlp)>0){

			try {
				lastpage1 = (Page) Serializer.deSerialize(lastpage);
			
			} catch (Exception e) {
				Object [] pageandloc = {-1,-1};
				return pageandloc ;
			}

			int rakameltuple = binarysearchkonato2(lastpage1, tuple, table);
			// the tuple we want to insert will be located in the last page if there is space
			// first object is location of tuple that will be just before the tuple we wish to insert
			Object [] theoutput = {rakameltuple, lastpage1, lastpage, (pageFileNames.size()-1)};
		    return theoutput;
		 }
         


		while (flag){
			
			int midOfPages = (i+j)/2;
			String pageName1 = pageFileNames.get(midOfPages);
			if(midOfPages ==pageFileNames.size()-1){
				try {
					page1 = (Page) Serializer.deSerialize(pageName1);
					int rakameltuple = binarysearchkonato2(page1, tuple, table);
				   Object [] heyy = {rakameltuple, page1, pageName1, midOfPages};
				 return heyy;
				
				} catch (Exception e) {
					 Object [] pageandloc = {-1,-1};
					return pageandloc ;
				}

			}
			int oneaftermid = midOfPages +1;
			//String pageName1 = pageFileNames.get(midOfPages);
			String pageName2 = pageFileNames.get(oneaftermid);
		
			Page page2;
			
			
			int a=0;
            Comparable min1 =  table.pageMin.get(pageName1);
			Comparable min2 =  table.pageMin.get(pageName2);
			Comparable min4 = (Comparable) tuple.getValue(table.getClusteringKeyColumn());
			
			if(min4.compareTo(min1)>0 && min4.compareTo(min2)<0  ){
                
				try {
					page1 = (Page) Serializer.deSerialize(pageName1);
				} catch (Exception e) {
					 Object [] pageandloc = {-1,-1};
					return pageandloc ;
				}

				 int rakameltuple = binarysearchkonato2(page1, tuple, table);
				 Object [] theoutput = {rakameltuple, page1, pageName1, midOfPages};
				 return theoutput;
                 // it is the page we want and i mean the one labelled page1 
			     // first object is location of tuple that will be just before the tuple we wish to insert

			}
			 if(min4.compareTo(min1)<0){
				// please discard the bunch of pages to the right side
				j = midOfPages -1;
			}
			else if(min4.compareTo(min1)>0){
				// please discard the bunch of pages to the left side
				i = midOfPages + 1;
			}



		}
		Object [] m = {-1,-1};
		return m; 
		
	}
	
	public static int binarysearchkonato2(Page p, Tuple tuple, Table table ){
		Vector<Tuple> tuples1 = p.getTuples();
       String clusterColumnName = table.getClusteringKeyColumn();
		int i =0;
		int j=tuples1.size()-1;

		boolean flag = true;
		if(j!=-1){
            Tuple min = tuples1.elementAt(0);
		if (tuple.compareTo(min, clusterColumnName)<0){
			return -1;
		}
		}
		
		while (flag){
			if(j == -1){
				return -1;
			}
			
			int mid = (i+j)/2;
			if(mid == tuples1.size() -1){
				return mid;
			}
			Tuple min1 = tuples1.elementAt(mid);
			Tuple min2 = tuples1.elementAt(mid +1);
			if(tuple.compareTo(min1, clusterColumnName)>0 && tuple.compareTo(min2, clusterColumnName)<0 ){
				// we found location of tuple that will be just before the tuple we want to insert
				return mid;
			}
			if(tuple.compareTo(min1, clusterColumnName)>0){
				i = mid + 1;
			}
			else if(tuple.compareTo(min1, clusterColumnName)<0){
               j = mid ;
			}

		}
		return 0;
	}

	public static void zabatindicesaccordingly (Table table, Tuple tuple, Comparable thepkoftuple, String old, String newest) throws DBAppException{
		Hashtable<String,String> hash = table.getIndexHash();
                                Index index;
								 Hashtable<String, Object> htblColNameValue = tuple.getValues();
                        

								for (Map.Entry<String, String> entry : hash.entrySet()) {
									String columnName = entry.getKey(); // Getting Key (column name)
									String indexName = entry.getValue();
									try {
										index = (Index) Serializer.deSerialize(indexName);
									} catch (Exception e) {
										throw new DBAppException("error deserializing: " + indexName);
									}
									bplustree tree = index.getBTree();
									Comparable o1;
                                

									//checking that the column that you have it's index is the same as the column you'll insert into it's index
									for (Map.Entry<String, Object> entry1 : htblColNameValue.entrySet()) {
										String column = entry1.getKey(); // Getting Key (column name)
										o1 = (Comparable)entry1.getValue();
									
										if(column.equals(columnName)){
											 Vector<String> vectorofpages = tree.search(o1);
											
											 if(vectorofpages == null){
												String primarykeycol = table.getClusteringKeyColumn();
												Comparable pk = (Comparable) tuple.getValue(primarykeycol); 
												
										
												if (pk.compareTo(thepkoftuple) !=0){
                                                    
													tree.insert(o1, newest);
												}
                                            
											 }
											 else{


												String primarykeycol = table.getClusteringKeyColumn();
											   Comparable pk = (Comparable) tuple.getValue(primarykeycol);
												if (pk.compareTo(thepkoftuple) !=0){
													
													vectorofpages.remove(old);
													vectorofpages.add(newest);
                                                
												}
												
											 }
											 
											
											break;
										}
									}
									try{
										
										Serializer.serialize(index, indexName);
							

									}
									catch(Exception e){
										throw new DBAppException("An error occured while serializing the index: " + indexName);
									}
									

								}
	}

	
	public boolean searchBinaryAndDelete(String strTableName, Hashtable<String,Object> ht) throws DBAppException {
		Table table = (Table) Serializer.deSerialize(strTableName);
		if(!ht.containsKey(table.getClusteringKeyColumn())) {
			return false;
		}
		return binarySearchD(strTableName, table.getClusteringKeyColumn(), ht.get(table.getClusteringKeyColumn()), ht);
	}

	public boolean binarySearchD(String strTableName, String key, Object value, Hashtable<String,Object> ht) throws DBAppException { //used only when searching with clustering key
		Table table = (Table) Serializer.deSerialize(strTableName);
		Vector<String> pages = table.getPageFileNames();
		return binarySearchHelper2(pages, key, value, 0, (pages.size() - 1), ht, table);
	}

	public boolean binarySearchHelper2(Vector<String> pages, String key, Object value,int left, int right, Hashtable<String,Object> ht, Table table) throws DBAppException {
		if(left > right) {
			return false;
		}
		int mid = left + (right - left) / 2;
		System.out.println("Checking: " + pages.get(mid));
		Comparable<Object> firstTupleValue = (Comparable<Object>) table.pageMin.get(pages.get(mid));
        Comparable<Object> lastTupleValue = (Comparable<Object>) table.pageMax.get(pages.get(mid));
        Comparable<Object> myValue = (Comparable<Object>) value;
		if((firstTupleValue.compareTo(myValue) <= 0) && (lastTupleValue.compareTo(myValue) >= 0)) {
			Page p = (Page) Serializer.deSerialize(pages.get(mid));
			Vector<Integer> indices = binarySearchInternalD(p.getTuples(), key, value, ht);
			if(indices.size() == 0) {
				System.out.println("Rest of filters don't apply");
			}
			for(int i = indices.size() - 1; i >= 0; i--) {
				Tuple tuple = p.getTuples().get(indices.get(i));
				System.out.println(p.getTuples().get(indices.get(i)) + " was removed (Binary)");
				int indexAt = indices.get(i);
				p.getTuples().remove(indexAt);
				table.pageMax.put(p.getFileName(), (Comparable)p.getTuples().get(p.getTuples().size() - 1).getValue(table.getClusteringKeyColumn()));
				table.pageMin.put(p.getFileName(), (Comparable)p.getTuples().get(0).getValue(table.getClusteringKeyColumn()));
				Serializer.serialize(table, table.getTableName());
				updateIndexAccordingly(table, tuple, p);
				if(p.isEmpty()) {
					table.removemtpage(p);
				} else {
					Serializer.serialize(p, p.getFileName());
				}
				return true;
			}
		} else if(firstTupleValue.compareTo(myValue) > 0) {
			binarySearchHelper2(pages, key, value, left, mid-1, ht, table);
		} else if(lastTupleValue.compareTo(myValue) < 0) {
			binarySearchHelper2(pages, key, value, mid+1, right, ht, table);
		}
		return true;
	}
	
	public Vector<Integer> binarySearchInternalD(Vector<Tuple> tuples, String key, Object value, Hashtable<String,Object> ht) {
		if(tuples != null) {
			Vector<Integer> result = new Vector<Integer>();
			binarySearchRecursiveInternalD(tuples, key, value, 0, tuples.size() - 1, result, ht);
			return result;
		} else {
			System.out.println("Tuples entered was null");
			return new Vector<Integer>();
		}
    }

	private void binarySearchRecursiveInternalD(Vector<Tuple> tuples, String key, Object value, int left, int right, Vector<Integer> result, Hashtable<String,Object> ht) {
        if (left > right) {
            return;
        }
        
        int mid = left + (right - left) / 2;
		System.out.println("Checking tuple at: " + mid);
        Tuple tuple = tuples.get(mid);
        //String tupleValue = (tuple.getValues().get(key) + "");
        
        //int comparator = tupleValue.compareTo(value + "");
		int comparator = ((Comparable)tuple.getValue(key)).compareTo((Comparable)value);

        if(comparator == 0) {
			boolean tupleIsGood = true;
			for(Map.Entry<String,Object> filter: ht.entrySet()) {
				/*if(!(tuple.getValue(filter.getKey()) + "").equals(filter.getValue()+"")) {
					tupleIsGood = false;
					break;
				}*/
				if(((Comparable)tuple.getValue(filter.getKey())).compareTo((Comparable)filter.getValue()) != 0) {
					tupleIsGood = false;
					break;
				}
			}
			if(tupleIsGood) {
				result.add(mid);
			}
        } else if (comparator < 0) {
            binarySearchRecursiveInternalD(tuples, key, value, mid + 1, right, result, ht);
        } else if (comparator > 0){
            binarySearchRecursiveInternalD(tuples, key, value, left, mid - 1, result, ht);
        }
    }

	public boolean searchLinearAndDelete(String strTableName, Hashtable<String,Object> ht) throws DBAppException {
		boolean done = false;
		Table table = (Table) Serializer.deSerialize(strTableName);
		Vector<String> pageFileNames = table.getPageFileNames();
		for(int i = pageFileNames.size() - 1; i >= 0 ; i--) {
			Page page = (Page) Serializer.deSerialize(pageFileNames.get(i));
			for(int j = page.getTuples().size() - 1; j >= 0; j--) {
				Tuple tuple = page.getTuples().get(j);
				boolean tupleIsGood = true;
				for (Entry<String, Object> filter : ht.entrySet()) {
					if(!(filter.getValue()+"").equals((tuple.getValue(filter.getKey()) + ""))) {
						tupleIsGood = false;
						break;
					}
				}
				if(tupleIsGood) {
					System.out.println(page.getTuples().get(j) + " was deleted (Linear)");
					page.getTuples().remove(j);
					if(!page.isEmpty()) {
						table.pageMax.put(page.getFileName(), (Comparable)page.getTuples().get(page.getTuples().size() - 1).getValue(table.getClusteringKeyColumn()));
						table.pageMin.put(page.getFileName(), (Comparable)page.getTuples().get(0).getValue(table.getClusteringKeyColumn()));
					}
					updateIndexAccordingly(table, tuple, page);
					done = true;
				}
				
			}
			if(page.isEmpty()) {
				table.removemtpage(page);
			} else {
				Serializer.serialize(page, page.getFileName());
			}
		}
		return done;
	}

	public void updateIndexAccordingly(Table table, Tuple tuple, Page page) throws DBAppException {
		for(Map.Entry<String,String> entry: table.getIndexHash().entrySet()) {
			Index index = (Index) Serializer.deSerialize(entry.getValue());			
			index.delete((Comparable)tuple.getValue(entry.getKey()), page.getFileName());
			Serializer.serialize(index, index.getName());
		}
	}


	public boolean searchByIndexAndDelete(String strTableName, Hashtable<String,Object> ht) throws DBAppException {
		Table table = (Table) Serializer.deSerialize(strTableName);
		boolean done = false;
		boolean anythingRemoved = false;
		boolean hasClusterIndex = (table.getIndexHash().get(table.getClusteringKeyColumn()) != null) && (ht.containsKey(table.getClusteringKeyColumn()));
		if(hasClusterIndex) {
			if(table.getIndexHash().get(table.getClusteringKeyColumn()) != null) {
				Index index;
				boolean isCluster = true;
				System.out.println("Index used: " + table.getIndexHash().get(table.getClusteringKeyColumn()));
				done = true;
				index = (Index) Serializer.deSerialize(table.getIndexHash().get(table.getClusteringKeyColumn()));
				Vector<String>	res = index.search((Comparable)ht.get(table.getClusteringKeyColumn()));
				if(res == null) {
					System.out.println("No index points at these values");
					return true;
				}
				System.out.println("Index was found at: " + res);
				for(int i = res.size() - 1; i >= 0; i--) {
					Page page = (Page) Serializer.deSerialize(res.get(i));
					if(isCluster) {
						System.out.println("Within index, search was done binary");
						Vector<Integer> indices = binarySearchInternalD(page.getTuples(), table.getClusteringKeyColumn(), ht.get(table.getClusteringKeyColumn()), ht);
						if(indices.size() == 0) {
							System.out.println("No index points at these values");
							return true;
						}
						for(int k = indices.size() - 1; k >= 0; k--) {
							Tuple tuple = page.getTuples().get(indices.get(k));
							System.out.println(page.getTuples().get(indices.get(k)) + " was deleted (Index)");
							int indexAt = indices.get(k);
							page.getTuples().remove(indexAt);
							table.pageMax.put(page.getFileName(), (Comparable)page.getTuples().get(page.getTuples().size() - 1).getValue(table.getClusteringKeyColumn()));
            				table.pageMin.put(page.getFileName(), (Comparable)page.getTuples().get(0).getValue(table.getClusteringKeyColumn()));
							updateIndexAccordingly(table, tuple, page);
							Serializer.serialize(page, page.getFileName());
							anythingRemoved = true;
						}
						if(page.isEmpty()) {
							table.removemtpage(page);
						} else {
							Serializer.serialize(page, page.getFileName());
						}
					} else {
						System.out.println("Within index, search was done linear");
						for(int j = page.getTuples().size() - 1; j >= 0; j--) {
							if((page.getTuples().get(j).getValue(table.getClusteringKeyColumn())+"").equals((ht.get(table.getClusteringKeyColumn())+""))) {
								boolean tupleIsGood = true;
								if(ht.size() > 1) {
									for(Map.Entry<String,Object> filter: ht.entrySet()) {
										if(!(page.getTuples().get(j).getValue(filter.getKey())+"").equals(filter.getValue()+"")) {
											tupleIsGood = false;
											break;
										}
									}
								}
								if(tupleIsGood) {
									Tuple tuple = page.getTuples().get(j);
									System.out.println(page.getTuples().get(j) + " was deleted (Index)");
									page.getTuples().remove(j);
									table.pageMax.put(page.getFileName(), (Comparable)page.getTuples().get(page.getTuples().size() - 1).getValue(table.getClusteringKeyColumn()));
            						table.pageMin.put(page.getFileName(), (Comparable)page.getTuples().get(0).getValue(table.getClusteringKeyColumn()));						
									updateIndexAccordingly(table, tuple, page);
									Serializer.serialize(page, page.getFileName());
									anythingRemoved = true;
								}
							}
						}
						if(page.isEmpty()) {
							table.removemtpage(page);
						} else {
							Serializer.serialize(page, page.getFileName());
						}
					}
				}
			}
		} else {
			for(Map.Entry<String,Object> entry : ht.entrySet()) {
				if(table.getIndexHash().get(entry.getKey()) != null) {
					Index index;
					boolean isCluster;
					Vector<String> res;
					System.out.println("Index used: " + table.getIndexHash().get(entry.getKey()));
					done = true;
					isCluster = table.getClusteringKeyColumn().equals(entry.getKey());
					index = (Index) Serializer.deSerialize(table.getIndexHash().get(entry.getKey()));
					res = index.search((Comparable)entry.getValue());
					if(res == null) {
						System.out.println("No index points at these values");
						return true;
					}
					System.out.println("Index was found at: " + res);
					for(int i = res.size() - 1; i >= 0; i--) {
						Page page = (Page) Serializer.deSerialize(res.get(i));
						if(isCluster) {
							System.out.println("Within index, search was done binary");
							Vector<Integer> indices = binarySearchInternalD(page.getTuples(), entry.getKey(), entry.getValue(), ht);
							if(indices.size() == 0) {
								System.out.println("No index points at these values");
								return true;
							}
							for(int k = indices.size() - 1; k >= 0; k--) {
								Tuple tuple = page.getTuples().get(indices.get(k));
								System.out.println(page.getTuples().get(indices.get(k)) + " was deleted (Index)");
								int indexAt = indices.get(k);
								page.getTuples().remove(indexAt);
								table.pageMax.put(page.getFileName(), (Comparable)page.getTuples().get(page.getTuples().size() - 1).getValue(table.getClusteringKeyColumn()));
								table.pageMin.put(page.getFileName(), (Comparable)page.getTuples().get(0).getValue(table.getClusteringKeyColumn()));
								updateIndexAccordingly(table, tuple, page);
								Serializer.serialize(page, page.getFileName());
								anythingRemoved = true;
							}
							if(page.isEmpty()) {
								table.removemtpage(page);
							} else {
								Serializer.serialize(page, page.getFileName());
							}
						} else {
							System.out.println("Within index, search was done linear");
							for(int j = page.getTuples().size() - 1; j >= 0; j--) {
								if((page.getTuples().get(j).getValue(entry.getKey())+"").equals((entry.getValue()+""))) {
									boolean tupleIsGood = true;
									if(ht.size() > 1) {
										for(Map.Entry<String,Object> filter: ht.entrySet()) {
											if(!(page.getTuples().get(j).getValue(filter.getKey())+"").equals(filter.getValue()+"")) {
												tupleIsGood = false;
												break;
											}
										}
									}
									if(tupleIsGood) {
										Tuple tuple = page.getTuples().get(j);
										System.out.println(page.getTuples().get(j) + " was deleted (Index)");
										page.getTuples().remove(j);
										table.pageMax.put(page.getFileName(), (Comparable)page.getTuples().get(page.getTuples().size() - 1).getValue(table.getClusteringKeyColumn()));
										table.pageMin.put(page.getFileName(), (Comparable)page.getTuples().get(0).getValue(table.getClusteringKeyColumn()));						
										updateIndexAccordingly(table, tuple, page);
										Serializer.serialize(page, page.getFileName());
										anythingRemoved = true;
									}
								}
							}
							if(page.isEmpty()) {
								table.removemtpage(page);
							} else {
								Serializer.serialize(page, page.getFileName());
							}
						}
					}
				}
				break;
			}
		}
		if(!anythingRemoved) {
			if(ht.size() == 0) {
				System.out.println("Table was cleared");
			} else {
				System.out.println("No index points at these values");
			}
		}
		return done;
	}

	
	public boolean searchByIndexAndUpdate(String strTableName, Object clusteringKeyValue, String clusteringKeyColumn, Hashtable<String,Object> htblColNameValue) throws DBAppException {
		String indexName = hasIndex(getCSV(), strTableName, clusteringKeyColumn);
		if(indexName.equals("null")) {
			return false;
		} else {
			System.out.println("Index used: " + indexName);
			System.out.println("Within index, search was done binary");
			Index index = (Index) Serializer.deSerialize(indexName);
			Vector<String> res = index.search((Comparable)clusteringKeyValue);
			if(res == null) {
				System.out.println("This clustering key does not exist in the table");
				return true;
			}
			System.out.println("Index found: " + res);
			for(int i = 0; i < res.size();i++) {
				Page page = (Page) Serializer.deSerialize(res.get(i));
				Hashtable<String,Object> filters = new Hashtable<String,Object>();
				filters.put(clusteringKeyColumn,clusteringKeyValue);
				Vector<Integer> indices = binarySearchInternalD(page.getTuples(), clusteringKeyColumn, clusteringKeyValue, filters);
				if(indices.size() == 0) {
					System.out.println("This clustering key does not exist in this table (index)");
					return true;
				}
				Tuple tuple = page.getTuples().get(indices.get(0));
				for(Map.Entry<String,Object> entry: htblColNameValue.entrySet()) {
					String indexEntryName = hasIndex(getCSV(), strTableName, entry.getKey());
					//if(indexEntryName.equals())
					if(!indexEntryName.equals("null")) {
						Index indexEntry = (Index) Serializer.deSerialize(indexEntryName);
						indexEntry.delete((Comparable)tuple.getValue(entry.getKey()), page.getFileName());
						if(indexEntry.search((Comparable)entry.getValue()) == null) {
							indexEntry.insert((Comparable)entry.getValue(), page.getFileName());
						} else {
							indexEntry.search((Comparable)entry.getValue()).add(page.getFileName());
						}
						Serializer.serialize(indexEntry, indexEntryName);
					}
				}
				Tuple newTuple = tuple.updateTuple(htblColNameValue);
				page.setTuple(indices.get(0), newTuple);			
				Serializer.serialize(page, page.getFileName());
				break;
			}
		}
		return true;
	}
	
	public boolean searchBinaryAndUpdate(String strTableName, Object clusteringKeyValue, String clusteringKeyColumn, Hashtable<String,Object> htblColNameValue) throws DBAppException {
		Hashtable<String,Object> filter = new Hashtable<String,Object>();
		filter.put(clusteringKeyColumn,clusteringKeyValue);
		return binarySearchU(strTableName, clusteringKeyColumn, clusteringKeyValue, filter, htblColNameValue);
	}

	public boolean binarySearchU(String strTableName, String key, Object value, Hashtable<String,Object> ht, Hashtable<String,Object> updates) throws DBAppException { //used only when searching with clustering key
		Table table = (Table) Serializer.deSerialize(strTableName);
		Vector<String> pages = table.getPageFileNames();
		return binarySearchHelperU(pages, key, value, 0, (pages.size() - 1), ht, updates, table);
	}
	
	public boolean binarySearchHelperU(Vector<String> pages, String key, Object value,int left, int right, Hashtable<String,Object> ht,Hashtable<String,Object> updates, Table table) throws DBAppException {
		if(left > right) {
			return false;
		}
		int mid = left + (right - left) / 2;
		System.out.println("Checking: " + pages.get(mid));
		Comparable<Object> firstTupleValue = (Comparable<Object>) table.pageMin.get(pages.get(mid));
        Comparable<Object> lastTupleValue = (Comparable<Object>) table.pageMax.get(pages.get(mid));
        Comparable<Object> myValue = (Comparable<Object>) value;
		if((firstTupleValue.compareTo(myValue) <= 0) && (lastTupleValue.compareTo(myValue) >= 0)) {
			Page p = (Page) Serializer.deSerialize(pages.get(mid));
			Vector<Integer> indices = binarySearchInternalU(p.getTuples(), key, value, ht);	
			for(int i = 0; i < indices.size(); i++) {
				Tuple tuple = p.getTuples().get(indices.get(i));
				for(Map.Entry<String,Object> entry: updates.entrySet()) {
					String indexEntryName = table.getIndexHash().get(entry.getKey());
					//String indexEntryName = hasIndex(getCSV(), table.getTableName(), entry.getKey());
					if(indexEntryName != null) {
						Index indexEntry = (Index) Serializer.deSerialize(indexEntryName);
						indexEntry.delete((Comparable)tuple.getValue(entry.getKey()), p.getFileName());
						if(indexEntry.search((Comparable)entry.getValue()) == null) {
							indexEntry.insert((Comparable)entry.getValue(), p.getFileName());
						} else {
							indexEntry.search((Comparable)entry.getValue()).add(p.getFileName());
						}
						Serializer.serialize(indexEntry, indexEntryName);
					}
				}
				Tuple newTuple = tuple.updateTuple(updates);
				p.setTuple(indices.get(i), newTuple);			
				Serializer.serialize(p, p.getFileName());
			}
			if(indices.size() > 0) {
				return true;
			}
		} else if(firstTupleValue.compareTo(myValue) > 0) {
			binarySearchHelperU(pages, key, value, left, mid-1, ht, updates, table);
		} else if(lastTupleValue.compareTo(myValue) < 0) {
			binarySearchHelperU(pages, key, value, mid+1, right, ht, updates, table);
		}
		return false;
	}
	
	public Vector<Integer> binarySearchInternalU(Vector<Tuple> tuples, String key, Object value, Hashtable<String,Object> ht) {
		if(tuples != null) {
			Vector<Integer> result = new Vector<Integer>();
			binarySearchRecursiveInternalU(tuples, key, value, 0, tuples.size() - 1, result, ht);
			return result;
		} else {
			return new Vector<Integer>();
		}
    }

	private void binarySearchRecursiveInternalU(Vector<Tuple> tuples, String key, Object value, int left, int right, Vector<Integer> result, Hashtable<String,Object> ht) {
        if (left > right) {
            return;
        }
        
        int mid = left + (right - left) / 2;
        Tuple tuple = tuples.get(mid);
        Comparable tupleValue = (Comparable) tuple.getValues().get(key);
        
        int comparator = tupleValue.compareTo(value);
        
        if (comparator == 0) {
			boolean tupleIsGood = true;
			for(Map.Entry<String,Object> filter: ht.entrySet()) {
				if(!(tuple.getValue(filter.getKey()) + "").equals(filter.getValue()+"")) {
					tupleIsGood = false;
					break;
				}
			}
			if(tupleIsGood) {
				result.add(mid);
			} else {
				System.out.println("a result was eliminated");
			}
        } else if (comparator < 0) {
            binarySearchRecursiveInternalU(tuples, key, value, mid + 1, right, result, ht);
        } else {
            binarySearchRecursiveInternalU(tuples, key, value, left, mid - 1, result, ht);
        }
    }
	
	public boolean validUpdate(String strTableName, Hashtable<String, Object> ht) {
		Vector<Vector<String>> csv = getCSV();
		boolean ret = true;
		for(int i = 0; i < csv.size(); i++) {
			if(csv.get(i).get(0).equals(strTableName)) {
				if(csv.get(i).get(2).equals("java.lang.String") && !(ht.get(csv.get(i).get(1)) instanceof String) && (ht.get(csv.get(i).get(1)) != null)) {
					ret = false;
				} else if(csv.get(i).get(2).equals("java.lang.Integer") && !(ht.get(csv.get(i).get(1)) instanceof Integer) && (ht.get(csv.get(i).get(1)) != null)) {
					ret = false;
				} else if(csv.get(i).get(2).equals("java.lang.Double") && !(ht.get(csv.get(i).get(1)) instanceof Double) && (ht.get(csv.get(i).get(1)) != null)) {
					ret = false;
				}
				//condition for primary key
				if((ht.get(csv.get(i).get(1)) != null) && csv.get(i).get(3).equals("True"))  {
					ret = false;
				}
			}
		}
		return ret;
	}

	public String typeOfClusterKey(String strTableName) {
		Vector<Vector<String>> csv = getCSV();
		for(int i = 0; i < csv.size(); i++) {
			if(csv.get(i).get(0).equals(strTableName)) {
				if(csv.get(i).get(3).equals("True")) {
					return csv.get(i).get(2);
				}
			}
		}
		return null;
	}

	public String typeOfKey(String strTableName,String key) {
		Vector<Vector<String>> csv = getCSV();
		for(int i = 0; i < csv.size(); i++) {
			if(csv.get(i).get(0).equals(strTableName) && csv.get(i).get(1).equals(key)) {
					return csv.get(i).get(2);
			}
		}
		return null;
	}

	public boolean validDelete(String strTableName, Hashtable<String, Object> ht) {
		Vector<Vector<String>> csv = getCSV();
		boolean ret = true;
		for(int i = 0; i < csv.size(); i++) {
			if(csv.get(i).get(0).equals(strTableName)) {
				if(csv.get(i).get(2).equals("java.lang.String") && !(ht.get(csv.get(i).get(1)) instanceof String) && (ht.get(csv.get(i).get(1)) != null)) {
					ret = false;
				} else if(csv.get(i).get(2).equals("java.lang.Integer") && !(ht.get(csv.get(i).get(1)) instanceof Integer) && (ht.get(csv.get(i).get(1)) != null)) {
					ret = false;
				} else if(csv.get(i).get(2).equals("java.lang.Double") && !(ht.get(csv.get(i).get(1)) instanceof Double) && (ht.get(csv.get(i).get(1)) != null)) {
					ret = false;
				}
			}
		}
		return ret;
	}

	public void clearTable(String strTableName) throws DBAppException {
		Table table = (Table) Serializer.deSerialize(strTableName);
		table.clear();
		Serializer.serialize(table, strTableName);
	}





	public static void main( String[] args ) throws DBAppException{
		try{
			String strTableName = "Student";
			DBApp	dbApp = new DBApp( );
			
			Hashtable htblColNameType = new Hashtable( );
			htblColNameType.put("id", "java.lang.Integer");
			htblColNameType.put("name", "java.lang.String");
			htblColNameType.put("gpa", "java.lang.double");
			dbApp.createTable( strTableName, "id", htblColNameType );
			dbApp.createIndex( strTableName, "gpa", "gpaIndex" );

			Hashtable htblColNameValue = new Hashtable( );
			htblColNameValue.put("id", new Integer( 2343432 ));
			htblColNameValue.put("name", new String("Ahmed Noor" ) );
			htblColNameValue.put("gpa", new Double( 0.95 ) );
			dbApp.insertIntoTable( strTableName , htblColNameValue );

			htblColNameValue.clear( );
			htblColNameValue.put("id", new Integer( 453455 ));
			htblColNameValue.put("name", new String("Ahmed Noor" ) );
			htblColNameValue.put("gpa", new Double( 0.95 ) );
			dbApp.insertIntoTable( strTableName , htblColNameValue );

			htblColNameValue.clear( );
			htblColNameValue.put("id", new Integer( 5674567 ));
			htblColNameValue.put("name", new String("Dalia Noor" ) );
			htblColNameValue.put("gpa", new Double( 1.25 ) );
			dbApp.insertIntoTable( strTableName , htblColNameValue );

			htblColNameValue.clear( );
			htblColNameValue.put("id", new Integer( 23498 ));
			htblColNameValue.put("name", new String("John Noor" ) );
			htblColNameValue.put("gpa", new Double( 1.5 ) );
			dbApp.insertIntoTable( strTableName , htblColNameValue );

			htblColNameValue.clear( );
			htblColNameValue.put("id", new Integer( 78452 ));
			htblColNameValue.put("name", new String("Zaky Noor" ) );
			htblColNameValue.put("gpa", new Double( 0.88 ) );
			dbApp.insertIntoTable( strTableName , htblColNameValue );


			SQLTerm[] arrSQLTerms;
			arrSQLTerms = new SQLTerm[2];
			arrSQLTerms[0] = new SQLTerm();
			arrSQLTerms[1] = new SQLTerm();
			arrSQLTerms[0]._strTableName =  "Student";
			arrSQLTerms[0]._strColumnName=  "name";
			arrSQLTerms[0]._strOperator  =  "=";
			arrSQLTerms[0]._objValue     =  "John Noor";

			arrSQLTerms[1]._strTableName =  "Student";
			arrSQLTerms[1]._strColumnName=  "gpa";
			arrSQLTerms[1]._strOperator  =  "=";
			arrSQLTerms[1]._objValue     =  new Double( 1.5 );

			String[]strarrOperators = new String[1];
			strarrOperators[0] = "OR";
			// select * from Student where name = "John Noor" or gpa = 1.5;
			Iterator resultSet = dbApp.selectFromTable(arrSQLTerms , strarrOperators);
		}
		catch(Exception exp){
			exp.printStackTrace( );
		}
	}
}
