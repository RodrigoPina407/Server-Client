package isel.g11;

import java.util.HashMap;

public class Memory {
    private static HashMap<String, String> values = new HashMap<String, String>();

    public static String Read(String key){
        synchronized (values){
            if(values.containsKey(key)){
                return values.get(key);
            }
            return null;
        }
    }

    public static void Write(String key, String value){
        synchronized (values){
            values.put(key, value);
        }
    }

    public static boolean Remove(String key){
        synchronized (values){
            if(values.containsKey(key)){
                values.remove(key);
                return true;
            }
        }
        return false;
    }

    public static boolean Contains(String key){
        synchronized (values){
            if(values.containsKey(key)){
                return true;
            }
        }
        return false;
    }
}
