package teamdb.classes;

import java.security.Key;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.Set;
import java.util.Vector;


@SuppressWarnings({ "unchecked", "rawtypes", "unused", "resource"})
public class AdamHelpers {

	/**
		 * Binary searches through the entire table.
		 *
		 * @param table Table to search through.
		 * @param key Value of cluster column to search against.
		 * @param exact boolean value to indicate if value not found to return null if exact=true and return nearest index if exact=false.
		 * @return Pair of the page of that value at its index
	*/
	public static Pair<Page,Integer> binarySearch(Table table, Comparable key, Boolean exact){
		
		String clusterColumnName = table.getClusteringKeyColumn();
		Vector<String> pageFileNames = table.getPageFileNames();
		int i=0;
		int j=pageFileNames.size()-1;
		Hashtable<String,Comparable> minValues = table.pageMin;
		Hashtable<String,Comparable> maxValues = table.pageMax;
		while(i<=j){
			//Calculate midpoint of search for all pages
			int midOfPages = (i+j)/2;
			String pageName = pageFileNames.get(midOfPages);
			Comparable minValue = minValues.get(pageName);
			Comparable maxValue = maxValues.get(pageName);
			if(key.compareTo(minValue)<0){
				// If less than min in page then discard right half of pages
				j = midOfPages-1;

			}else if(key.compareTo(maxValue)>0){
				// If greater than max in page then discard left half of pages
				i = midOfPages+1;
			}
			else{
				Page page;
				try {
					page = (Page) Serializer.deSerialize(pageName);
				} catch (Exception e) {
					return null;
				}
				Vector<Tuple> tuples = page.getTuples();
				int a=0;
				int b=tuples.size()-1;
			// Is in page
				while(a<=b){
					// Normal binary search
					// Midpoint of all tuples in this page
					int midTuple = (a+b)/2;
					Tuple currentTuple = tuples.elementAt(midTuple);
					Comparable currentValue = (Comparable) currentTuple.getValue(clusterColumnName);
					if(key.compareTo(currentValue)==0){
						return new Pair<Page,Integer>(page, midTuple);
					}
					if(key.compareTo(currentValue)<0){
						// If less than min in page then discard right half of pages
						b = midTuple-1;
		
					}else if(key.compareTo(currentValue)>0){
						// If less than min in page then discard right half of pages
						a = midTuple+1;
					}				
				}
				// Not found in page
				if(exact)return null; // If exact we return null
				return new Pair<Page,Integer>(page, a); // If not exact we return nearest index
			}
		}
		return null;
	}


    public static Tuple binarySearchTuple(Table table, Comparable key){
		Pair<Page,Integer> pair = binarySearch(table, key, true);
		if(pair == null) return null; //If the element is not in the Table
		Vector<Tuple> tuples = pair.getFirst().getTuples();
		return tuples.get(pair.getSecond());
	}

	public static int getPageNumber(String pageName){
		String ret = "";
		for(int i=pageName.length()-1 ;i>=0;i--){
			char c = pageName.charAt(i);
			if(c >= '0' &&  c <= '9') ret = c + ret;
			else break;
		}
		return Integer.parseInt(ret);
	}

	public static Page getNextPage(Vector<String> pageFileNames,int index,Boolean increment) throws DBAppException{
		Page nextPage;
		if(!increment){
			index--;;
		}else{
			index++;
		}
		if(index<0 || index>pageFileNames.size()-1) return null; // Because idices are 0 based and file names are base 1
		try {
			nextPage = (Page) getPage(pageFileNames,index);
		} catch (Exception e) {
			throw new DBAppException("Error while deserializing");
		}
		return nextPage;
		// inc page number get new page and overwrite tuples
	}

	public static Page getPage(Vector<String> pageFileNames,int index) throws DBAppException{
		String pageName = pageFileNames.get(index);	
		Page page;
		try {
			page = (Page) Serializer.deSerialize(pageName);
		} catch (Exception e) {
			throw new DBAppException("Error while deserializing");
		}
		return page;
	}

	public static int binarySearchPage(Page page, String columnName,Comparable<Key> k) {
		// Return type int because we never binary search unless we have the clusterColumn as the columnName so no duplicates
		Vector<Tuple> tuples = page.getTuples();
		int a=0;
		int b=tuples.size()-1;
		while(a<=b){
			// Normal binary search
			// Midpoint of all tuples in this page
			int midTuple = (a+b)/2;
			Tuple currentTuple = tuples.elementAt(midTuple);
			if(currentTuple.compareTo(currentTuple, columnName)==0){
				return (a+b)/2;
			}
			if(currentTuple.compareTo(currentTuple, columnName)<0){
				// If less than min in page then discard right half of pages
				b = midTuple-1;

			}else if(currentTuple.compareTo(currentTuple, columnName)>0){
				// If less than min in page then discard right half of pages
				a = midTuple+1;
			}				
		}	
		// Temp until we know what we want to do with
		return -1;
	}

	public static Vector<Integer> linearSearchPage(Vector<Tuple> tuples, String columnName,Comparable<Key> k) {
		Vector<Integer> indices = new Vector<Integer>();
		for(int i=0;i<tuples.size();i++){
			if(tuples.elementAt(i)==null) continue;
			else{
				if(tuples.elementAt(i).getValues().get(columnName)==k){
					indices.add(i);
				}
			}
		}
		return indices;
	}

	public static Vector<Tuple> linearSearchPageTuple(Vector<Tuple> tuples, String columnName,Comparable<Key> k) {
		Vector<Tuple> result = new Vector<Tuple>();
		for(int i=0;i<tuples.size();i++){
			if(tuples.elementAt(i)==null) continue;
			else{
				Comparable value = (Comparable) tuples.elementAt(i).getValues().get(columnName);
				if(value.compareTo(k)==0){
					result.add(tuples.elementAt(i));
				}
			}
		}
		return result;
	}

	public static Vector<Tuple> linearSearchTuple(Table table, String columnName,Comparable<Key> k) throws DBAppException{
		Vector<String> pageFileNames = table.getPageFileNames();
		Vector<Tuple> result = new Vector<Tuple>();
		for(int i=0;i<pageFileNames.size();i++){		
			Page page = getPage(pageFileNames, i);
			Vector<Tuple> tuples = page.getTuples();
			result.addAll(linearSearchPageTuple(tuples, columnName, k));
		}
		return result;
	}

	public static Vector<Tuple> linearSearchPageTuple(Vector<Tuple> tuples, String columnName,Comparable<Key> k, Boolean upperBound,Boolean inclusive) {
		Vector<Tuple> result = new Vector<Tuple>();
		for(int i=0;i<tuples.size();i++){
			Tuple currentTuple = tuples.elementAt(i);
			if(currentTuple==null) continue;
			else{
				Comparable value = (Comparable) tuples.elementAt(i).getValues().get(columnName);
				if (upperBound && inclusive && value.compareTo(k) <=0 ||
					upperBound && !inclusive && value.compareTo(k) <0 ||
					!upperBound && inclusive && value.compareTo(k) >=0 ||
					!upperBound && !inclusive && value.compareTo(k) >0  ) {
					result.add(currentTuple);
				}
			}
		}
		return result;
	}

	public static Vector<Tuple> linearSearchTuple(Table table, String columnName,Comparable<Key> k, Boolean upperBound,Boolean inclusive) throws DBAppException{
		Vector<String> pageFileNames = table.getPageFileNames();
		Vector<Tuple> result = new Vector<Tuple>();
		for(int i=0;i<pageFileNames.size();i++){		
			Page page = getPage(pageFileNames, i);
			Vector<Tuple> tuples = page.getTuples();
			result.addAll(linearSearchPageTuple(tuples, columnName, k, upperBound,inclusive));
		}
		return result;
	}

	
	public static Vector<Tuple> notEqual(Table table, String columnName,Comparable<Key> k) throws DBAppException{
		Vector<Tuple> result = new Vector<Tuple>();
		Vector<String> tablePageNames = table.getPageFileNames();
		for(int i=0;i<tablePageNames.size();i++){
			Page page = getPage(tablePageNames, i);
			Vector<Tuple> tuples = page.getTuples();
			for(int j=0;j<tuples.size();j++){
				Tuple tuple = tuples.elementAt(j);
				Comparable value = (Comparable) tuples.elementAt(j).getValues().get(columnName);
				if(value.compareTo(k) != 0){
					result.add(tuple);
				}
			}       
		}
		return result;
	}

	public static Vector<Tuple> getUnionSorted(Vector<Tuple> tuples1,Vector<Tuple> tuples2,String clusterColumn)
    {
        Vector<Tuple> result = new Vector<Tuple>();
		int i = 0;
		int j = 0;
		Tuple tuple1 = tuples1.get(i);
		Tuple tuple2 = tuples2.get(j);
		while(i<tuples1.size()&&j<tuples2.size()){
			if(tuple1.compareTo(tuple2, clusterColumn)<0){
				// Cluster Column of tuple1 is smaller than tuple2 so we continue traversing through tuples1
				result.add(tuple1);
				i++;
			}else if(tuple1.compareTo(tuple2, clusterColumn)<0){
				// Cluster Column of tuple2 is smaller than tuple1 so we continue traversing through tuples2
				result.add(tuple2);
				j++;
			}else{
				// Cluster Column of tuple2 is equal to tuple1 so we continue traversing through both
				result.add(tuple2); // Doesn't matter which one 
				j++;
				i++;
			}
		}
		while(i<tuples1.size()){
			result.add(tuples1.get(i));
			i++;
		}
		while(j<tuples1.size()){
			result.add(tuples1.get(j));
			j++;
		}
		return result;
    }

	public static Vector<Tuple> getUnion(Vector<Tuple> tuples1,Vector<Tuple> tuples2,String clusterColumn)
    {
		// Traverse through tuples1 and check if tuple is in tuples2 if yes then don't add it otherwise add it into the union set
        for(int i=0;i<tuples1.size();i++){
			Tuple tuple1 = tuples1.elementAt(i);
			boolean found = false;
			for(int j=0;j<tuples2.size();j++){
				Tuple tuple2 = tuples2.elementAt(j);
				if(tuple1.compareTo(tuple2, clusterColumn)==0){
					found = true;
					break;
				}		
			}
			if(!found){
				tuples2.add(tuple1);
			}
		}
		return tuples2;
    }

	public static Vector<Tuple> getIntersection(Vector<Tuple> tuples1,Vector<Tuple> tuples2,String clusterColumn)
    {
		Vector<Tuple> result = new Vector<Tuple>();
		// Traverse through tuples1 and check if tuple is in tuples2 if yes then don't add it otherwise add it into the union set
        for(int i=0;i<tuples1.size();i++){
			Tuple tuple1 = tuples1.elementAt(i);
			for(int j=0;j<tuples2.size();j++){
				Tuple tuple2 = tuples2.elementAt(j);
				if(tuple1.compareTo(tuple2, clusterColumn)==0){
					result.add(tuple1);
					break;
				}
			}
		}
		return result;
    }

	public static Vector<Tuple> getXOR(Vector<Tuple> tuples1,Vector<Tuple> tuples2,String clusterColumn)
    {
		Vector<Tuple> result = new Vector<Tuple>();
		// Traverse through tuples1 and check if tuple is in tuples2 if yes then don't add it otherwise add it into the union set
        for(int i=0;i<tuples1.size();i++){
			Tuple tuple1 = tuples1.elementAt(i);
			boolean found = false;
			for(int j=0;j<tuples2.size();j++){
				Tuple tuple2 = tuples2.elementAt(j);
				if(tuple1.compareTo(tuple2, clusterColumn)==0){
					found = true;
					tuples2.removeElementAt(j); // we remove any tuple that is in both from tuples2 to be added as a whole at the end
					break;
				}
			}
			if(!found){
				result.add(tuple1);				
			}
		}
		result.addAll(tuples2);
		return result;
    }
}