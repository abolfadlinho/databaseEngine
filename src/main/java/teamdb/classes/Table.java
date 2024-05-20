package teamdb.classes;

import java.io.File;
import java.util.Hashtable;
import java.util.Vector;
import java.io.Serializable;

@SuppressWarnings("rawtypes")
public class Table implements Serializable,Cloneable{
    private String tableName;
    private String clusteringKeyColumn;
    private String clusterKeyType;
    private Hashtable<String, String> colNameType;  // Column name to data type mapping
    private Vector<String> pageFileNames;  // Vector of Page objects
    private Hashtable<String,String> indexHash; // column name , index name
    public Hashtable<String,Comparable> pageMax;
    public Hashtable<String,Comparable> pageMin;
    public int maximumTuplesPerPage;

    public Table(String tableName, String clusteringKeyColumn, Hashtable<String, String> colNameType) throws DBAppException {
        this.tableName = tableName;
        this.clusteringKeyColumn = clusteringKeyColumn;
        this.clusterKeyType = colNameType.get(clusteringKeyColumn);
        this.colNameType = colNameType;
        this.pageFileNames = new Vector<String>();
        this.indexHash = new Hashtable<String,String>();
        this.pageMax = new Hashtable<String,Comparable>();
        this.pageMin = new Hashtable<String,Comparable>();
        ConfigReader conf = new ConfigReader();
        maximumTuplesPerPage = conf.getMaxRows();
        try {
            Serializer.serialize(this, tableName);
        } catch (Exception e) {
            throw new DBAppException("Error while serializing table");
        }
    }

    public void clear() {
        pageFileNames = new Vector<String>();
        pageMax = new Hashtable<String,Comparable>();
        pageMin = new Hashtable<String,Comparable>();
        indexHash = new Hashtable<String,String>();
    }

    public Page createPage() throws DBAppException {
        String newPageName;
		int newIndex = 1;
		for (int i = (pageFileNames.size() - 1); i >= 0; i--) { 
		 	  if (pageFileNames.get(i).contains(tableName)) {
		 		  newIndex = Character.getNumericValue(pageFileNames.get(i).charAt(pageFileNames.get(i).length() -1)) + 1; //6 characters for .class, 1 character for normal string indexing
		 		  break;
		 	  }
	    }
		newPageName = tableName + newIndex;
        Page ret = new Page(newPageName, maximumTuplesPerPage);
		pageFileNames.add(newPageName);
        String typeOfCluster = getClusterKeyType();
        if(typeOfCluster.equals("java.lang.Integer")) {
            pageMin.put(newPageName,Integer.MAX_VALUE);
            pageMax.put(newPageName,Integer.MIN_VALUE);
        } else if(typeOfCluster.equals("java.lang.Double")) {
            pageMin.put(newPageName,Double.MAX_VALUE);
            pageMax.put(newPageName,Double.MIN_VALUE);
        } else if(typeOfCluster.equals("java.lang.String")) {
            pageMin.put(newPageName,"zzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzz");
            pageMax.put(newPageName,"");
        }
        Serializer.serialize(this, this.getTableName());
		return ret;
	}

    public Hashtable<String, String> getColNameType(){
        return colNameType;
    }

    public String getClusteringKeyColumn (){
        return clusteringKeyColumn;
    }

    public String getTableName(){
        return tableName;
    }

    public Hashtable<String,String> getIndexHash(){
        return indexHash;
    }

    public Vector<String> getPageFileNames(){
        return pageFileNames;
    }

    public void addIndexHash(String columnName,String indexName){
        indexHash.put(columnName, indexName);
    }

    public String toString() {
		String str = "";
		for(int i = 0; i < pageFileNames.size(); i++) {
			try {
				str += ((Page)Serializer.deSerialize(pageFileNames.get(i))).toString() + "\n";
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		return str;
	}

    public String getClusterKeyType(){
        return clusterKeyType;
    }

    public void setClusterKey(String key) {
		this.clusteringKeyColumn = key;
	}

    public void removemtpage(Page p) throws DBAppException {
	    if (p.isEmpty()) {
	        pageFileNames.remove(p.fileName);
	        try {
	            Serializer.serialize(this, tableName);
                String filename = p.fileName + ".class";
                File file = new File(Serializer.filePath + filename);
                file.delete();
	        } catch (Exception e) {
	            throw new DBAppException("Error While Updating Table After Removing Empty Pages" + e.getMessage());
	        }
	    } else {
	        System.out.println("Page is not empty, cannot remove.");
	    }
	}

}
