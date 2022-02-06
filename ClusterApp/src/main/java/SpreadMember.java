import spread.*;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.Map;

public class SpreadMember {

    public SpreadConnection connection;
    //Grupos aos quais o utilizador pertence
    public Map<String, SpreadGroup> myGroups = new HashMap<String,SpreadGroup>();

    public SpreadMember(String user, String address, int port){
        // The constructor establishes a spread connection to the daemon
        try {
            connection = new SpreadConnection();
            connection.connect(InetAddress.getByName(address), port, user, false, true);
        } catch(SpreadException e) {
            System.err.println("There was an error connecting to the daemon.");
            e.printStackTrace();
            System.exit(1);
        } catch(UnknownHostException e) {
            System.err.println("Can't find the daemon " + address);
            System.exit(1);
        }
    }



}
