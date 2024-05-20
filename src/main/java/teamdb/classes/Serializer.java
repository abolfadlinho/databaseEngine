package teamdb.classes;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
//import DBAppException;

@SuppressWarnings("unused")
public class Serializer {
    // Replace with your desired folder name
    private static String extension = ".class";
    public static String filePath = "src/main/java/teamdb/Serializations/";
    public static void serialize(Object obj, String strFileName) throws DBAppException{   //obj = page     strFileName = esm el page
        try {
         FileOutputStream fileOut = new FileOutputStream(filePath+strFileName+extension);
         ObjectOutputStream out = new ObjectOutputStream(fileOut);
         out.writeObject(obj);
         out.close();
         fileOut.close();
        } catch (Exception e) {
            throw new DBAppException("error serializing");
        }
    }

    public static Object deSerialize(String strFileName) throws DBAppException {
        Object ret = null;
        try {
            File f = new File(filePath + strFileName + extension);
            FileInputStream fis = new FileInputStream(f);
            @SuppressWarnings("resource")
            ObjectInputStream ois = new ObjectInputStream(fis);	
            ret = ois.readObject(); 
        } catch (Exception e) {
            throw new DBAppException("error deserializing");
        }
		return ret;
	}

    public static boolean wasSerialized(String strFileName) {
        return wasSerialized(strFileName, extension, filePath);
    }

    public static boolean wasSerialized(String strFileName, String extension, String filePath) {
        String fileName = strFileName + extension;
        File directory = new File(filePath);

        if (!directory.isDirectory()) {
            System.out.println("The specified path is not a directory.");
            return false;
        }

        File[] files = directory.listFiles();
        if (files == null) {
            System.out.println("Unable to access directory contents.");
            return false;
        }

        for (File file : files) {
            if (file.getName().equals(fileName)) {
                return true;
            }
        }
        return false;
    }

}