package teamdb.classes;

import java.io.IOException;
import java.security.Key;
import java.util.Vector;

import teamdb.classes.bplustree.LeafNode;

import java.util.Hashtable;
import java.util.Hashtable;
import java.util.Map;
import java.io.Serializable;

@SuppressWarnings({ "unchecked", "rawtypes", "unused", "resource"})

public class Index implements Serializable{
    private String name;
    private String tableName;
    private String colName;
    private String clusterColumn; // To eliminate the necessity of deserializing table
    private bplustree bTree;

    public Index(String tableName,String colName,String indexName,String clusterColumn) throws DBAppException{
        this.name = indexName;
        this.tableName = tableName; 
        this.colName = colName;
        this.bTree = new bplustree(5);
        this.clusterColumn = clusterColumn;
        try {
            Serializer.serialize(this, indexName);
        } catch (Exception e) {
            throw new DBAppException("Error While Creating Index");
        }
    }

    public String getName(){
        return name;
    }

    public String getTableName(){
        return tableName;
    }

    public String getColName(){
        return colName;
    }

    public bplustree getBTree(){
        return bTree;
    }

    public Hashtable<Page,Vector<Integer>> selectEqual(String columnName,Comparable<Key> k) throws Exception{
        Hashtable<Page,Vector<Integer>> result = new Hashtable<Page,Vector<Integer>>();
        Vector<String> pageNames = bTree.search(k);
        if(pageNames.size()==0){
            // Btree does not contain value
            return result;
        }
        for(int i=0;i<pageNames.size();i++){
            Page page;
            try {
                page = (Page) Serializer.deSerialize(pageNames.get(i));
            } catch (Exception e) {
                throw new DBAppException("Deserializer Error");
            }
            Vector<Integer> indices;
            if(columnName.compareTo(clusterColumn)==0){
                // only one because it is clustered by this column
                indices = new Vector<Integer>();
                indices.add(AdamHelpers.binarySearchPage(page,columnName,k));
            }else{
                indices = AdamHelpers.linearSearchPage(page.getTuples(),columnName,k);
            }
            if(indices.size()==0){
                System.out.println(name + " Index has incorrect pointer to page");
            }else{
                result.put(page, indices); // Add index to array list for that page
            }
        }
        return result;
    }

    public Hashtable<String,Vector<Integer>> selectEqualHT(String columnName,Comparable<Key> k) throws Exception{
        Hashtable<String,Vector<Integer>> result = new Hashtable<String,Vector<Integer>>();
        Vector<String> pageNames = bTree.search(k);
        if(pageNames.size()==0){
            // Btree does not contain value
            return result;
        }
        for(int i=0;i<pageNames.size();i++){
            Page page;
            try {
                page = (Page) Serializer.deSerialize(pageNames.get(i));
            } catch (Exception e) {
                throw new DBAppException("Deserializer Error");
            }
            Vector<Integer> indices;
            if(columnName.compareTo(clusterColumn)==0){
                // only one because it is clustered by this column
                indices = new Vector<Integer>();
                indices.add(AdamHelpers.binarySearchPage(page,columnName,k));
            }else{
                indices = AdamHelpers.linearSearchPage(page.getTuples(),columnName,k);
            }
            if(indices.size()==0){
                System.out.println(name + " Index has incorrect pointer to page");
            }else{
                result.put(page.getFileName(), indices); // Add index to array list for that page
            }
        }
        return result;
    }

    public Vector<Tuple> selectEqualTuples(String columnName,Comparable<Key> k) throws Exception{
        Vector<Tuple> result = new Vector<Tuple>();
        Vector<String> pageNames = bTree.search(k);
        if(pageNames==null || pageNames.size()==0){
            // Btree does not contain value
            return result;
        }
        for(int i=0;i<pageNames.size();i++){
            Page page;
            try {
                page = (Page) Serializer.deSerialize(pageNames.get(i));
            } catch (Exception e) {
                throw new DBAppException("Deserializer Error");
            }
            Vector<Tuple> tuples = page.getTuples();
            if(columnName.compareTo(clusterColumn)==0){
                // only one because it is clustered by this column
                int index = AdamHelpers.binarySearchPage(page,columnName,k);
                if(index!=-1){
                    result.add(tuples.get(index));
                }
            }else{
                    result = AdamHelpers.linearSearchPageTuple(page.getTuples(),columnName,k);
            }
        }
        return result;
    }

    public Hashtable<Page,Vector<Integer>> selectRange(String columnName,Comparable<Key> lower,Comparable<Key> upper,boolean lowerBoundInclusive,boolean upperBoundInclusive) throws Exception{
        Hashtable<Page,Vector<Integer>> result = new Hashtable<Page,Vector<Integer>>();
        Hashtable<Comparable,Vector<String>> pageNamesMap = bTree.search(lower,upper,lowerBoundInclusive,upperBoundInclusive);; // Results in vector of vectors of page names
        if(pageNamesMap.size()==0){
            // Btree does not contain value
            return result;
        }
        for (Map.Entry<Comparable,Vector<String>> entry : pageNamesMap.entrySet()) {
            Comparable key = entry.getKey();
			Vector<String> pageNames = pageNamesMap.get(key);
            for(int j=0;j<pageNames.size();j++){
                Page page;
                try {
                    page = (Page) Serializer.deSerialize(pageNames.get(j));
                } catch (Exception e) {
                    throw new DBAppException("Deserializer Error");
                }
                Vector<Integer> indices;
                if(columnName.compareTo(clusterColumn)==0){
                    // only one because it is clustered by this column
                    indices = new Vector<Integer>();
                    indices.add(AdamHelpers.binarySearchPage(page,columnName,key));
                }else{
                    indices = AdamHelpers.linearSearchPage(page.getTuples(),columnName,key);
                }
                // Vector<Integer> indices = AdamHelpers.linearSearchPageAdam(page.getTuples(),columnName,lower);
                if(indices.size()==0){
                    System.out.println(name + " Index has incorrect pointer to page");
                }else{
                    result.put(page, indices); // Add index to array list for that page
                }
            }
		}
        return result;
    }

    public Vector<Tuple> selectRangeTuples(String columnName,Comparable<Key> lower,Comparable<Key> upper,boolean lowerBoundInclusive,boolean upperBoundInclusive) throws Exception{
        Vector<Tuple> result = new Vector<Tuple>();
        Hashtable<Comparable,Vector<String>> pageNamesMap = bTree.search(lower,upper,lowerBoundInclusive,upperBoundInclusive); // Results in vector of vectors of page names
        if(pageNamesMap.size()==0){
            // Btree does not contain value
            return result;
        }
        for (Map.Entry<Comparable,Vector<String>> entry : pageNamesMap.entrySet()) {
            Comparable key = entry.getKey();
			Vector<String> pageNames = pageNamesMap.get(key);
            for(int i=0;i<pageNames.size();i++){
                Page page;
                try {
                    page = (Page) Serializer.deSerialize(pageNames.get(i));
                } catch (Exception e) {
                    throw new DBAppException("Deserializer Error");
                }
                Vector<Tuple> tuples = page.getTuples();
                Vector<Integer> indices;
                if(columnName.compareTo(clusterColumn)==0){
                    // only one because it is clustered by this column
                    indices = new Vector<Integer>();
                    indices.add(AdamHelpers.binarySearchPage(page,columnName,key));
                }else{
                    indices = AdamHelpers.linearSearchPage(page.getTuples(),columnName,key);
                }
                if(indices.size()==0){
                    System.out.println(name + " Index has incorrect pointer to page");
                }else{
                    for(int j=0;j<indices.size();j++){
                        result.add(tuples.get(j));
                    }
                }
            }
		}
        return result;
    }

    public void deleteTuple(){
        
    }

    public LeafNode insert(Comparable key,String value){
        return bTree.insert(key, value);
    }

    public void delete(Comparable key){
        // For cluster key
        // Deletes whole key value pair (whole vector)
        bTree.delete(key);
    }

    public void delete(Comparable key,String pageName){
        // For non cluster columns (can be duplicates)
        // So deletes specified pageName in vector of page names
        bTree.delete(key, pageName);
    }

    public Vector<String> search(Comparable key){
        // For non cluster columns (can be duplicates)
        return bTree.search(key);
    }

    public void edit(Comparable key,String pageName){
        // Edits the vector of that key to replace first inserted page name by pageName
        bTree.edit(key, pageName);
    }

    public void edit(Comparable key,String previousPage,String newPage){
        // Edit the previousPage name value (vector) of key to the newPage name
        bTree.edit(key, previousPage, newPage);
    }
}