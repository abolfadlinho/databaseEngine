package teamdb.classes;
import java.io.IOException;
import java.io.Serializable;
import java.util.Vector;


//@SuppressWarnings("rawtypes")
public class Page implements Serializable, Cloneable{
    private int tupleNum;
    private int maxRows; //Put in DBApp not necc for page AA
    private Vector<Tuple> tuples;
    public String fileName;
 
    public Page(String filename) throws DBAppException{
        ConfigReader conf = new ConfigReader();
        maxRows = conf.getMaxRows();
        tupleNum = 0;
        this.fileName = filename;
        tuples = new Vector<Tuple>();
        Serializer.serialize(this, filename);
    }

    public Page(String filename, int maxNumRows) throws DBAppException{
        maxRows = maxNumRows;
        tupleNum = 0;
        this.fileName = filename;
        tuples = new Vector<Tuple>();
        Serializer.serialize(this, filename);
    }

    public String addTuple(Tuple tuple){
        if(tupleNum > maxRows){
            return "Fakes"; // Add exception AA
        }
        tuples.add(tuple);
        tupleNum++;
        return tuple.toString();
    }

    public void setTuple(int i, Tuple t) {
        try {
            tuples.set(i, t);
        } catch(Exception e) {
            tuples.add(t);
        }
    }

    

    public String toString(){
        if(tuples == null) {
            return "Empty page";
        }
        String str = fileName + ": \n";
        for(int i=0;i<tuples.size();i++){
            if(i > 0) {
                str+= " - ";
            }
            str += tuples.elementAt(i).toString(); // Would like to add a break but not said in decription
        }
        return str;
    }

    private void readObject(java.io.ObjectInputStream stream)
            throws IOException, ClassNotFoundException {
        stream.defaultReadObject();
        // After deserialization, initialize the iterator for each Tuple
        if(tuples != null) {     
            for (Tuple tuple : tuples) {
                tuple.initIterator();
            }
        }
    }

    public Vector<Tuple> getTuples(){
        return tuples;
    }

    public int getMaxRows(){
        return maxRows;
    }

    public void setTuples(Vector<Tuple> tuples) {
    	this.tuples = tuples;
    }

    public boolean isEmpty() {
        return tuples.isEmpty();
    }

    public boolean isFull() {
        return maxRows <= tuples.size();
    }
    
    public String getFileName(){
        return this.fileName;
    }
}
