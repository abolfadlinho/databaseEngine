package teamdb.classes;

import java.io.IOException;
import java.io.Serializable;
import java.util.*;

@SuppressWarnings({ "unchecked", "rawtypes", "unused", "resource"})
public class Tuple implements Serializable{
    private Hashtable<String, Object> values;
    private transient Iterator<Map.Entry<String, Object>> iterator;

    public Tuple(Hashtable<String,Object> keys){
        this.values = (Hashtable<String,Object>) keys.clone();
    }

    // Method to get the value of a specific column
    public Object getValue(String columnName) {
        return values.get(columnName);
    }

    public Iterator<Map.Entry<String, Object>> getIterator() {
        return iterator;
    }
    
    public void initIterator() {
        this.iterator = values.entrySet().iterator();
    }

    public String toString(){
        String str = "";
        boolean first = true;
        for (Map.Entry<String, Object> entry : values.entrySet()) {
            if(first) {
                first = false;
            } else {
                str += ", ";
            }
            Object value = entry.getValue();
            str += value;
        }
        return str + "\n";
    }

    public int compareTo(Tuple t,String clusteringKey){
        Comparable value1 = (Comparable) values.get(clusteringKey);
        Comparable value2 = (Comparable) t.values.get(clusteringKey);
        return value1.compareTo(value2);
    } 

    public Hashtable<String, Object> getValues(){
        return values;
    }

    public Tuple updateTuple(Hashtable<String, Object> ht) {
        for (Map.Entry<String, Object> entry : values.entrySet()) {
            String oldKey = entry.getKey();    
            if (ht.get(oldKey) != null) {
                values.replace(oldKey, ht.get(oldKey));
            }
        }
    	return this;
    }

    public int compareTo(Object clusterValue, String clusterKey) {
        if(clusterValue instanceof String) {
            return ((String) clusterValue).compareTo((String)getValue(clusterKey));
        } else if(clusterValue instanceof Integer) {
            if((Integer) clusterValue > (Integer) getValue(clusterKey)) {
                return 1;
            } else if((Integer) clusterValue < (Integer) getValue(clusterKey)) {
                return -1;
            } else {
                return 0;
            }
        } else if(clusterValue instanceof Double) {
            if((Double) clusterValue > (Double) getValue(clusterKey)) {
                return 1;
            } else if((Double) clusterValue < (Double) getValue(clusterKey)) {
                return -1;
            } else {
                return 0;
            }
        } else {
            return 2;
        }
    }
}
