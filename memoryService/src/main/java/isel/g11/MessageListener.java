package isel.g11;

import spread.*;

import java.net.InetAddress;
import java.util.HashSet;
import java.util.Timer;
import java.util.TimerTask;
import java.util.function.Function;

public class MessageListener implements BasicMessageListener {
    private SpreadConnection connection;
    private static String MemoryClusterGroup = "Clusters";
    private static HashSet<Message> requests = new HashSet<Message>();
    private static Timer timer = new Timer();
    public static boolean IsLeader = false;
    public static String Leader = null;

    public MessageListener(SpreadConnection connection) {
        this.connection=connection;
    }

    public static void ListenForReply(String message, Function<String, Object> callback){
        ListenForReply(message, callback, 5000);
    }

    public static void ListenForReply(String message, Function<String, Object> callback, int timeout){
        long currentTime = System.currentTimeMillis();
        Message message2 = new Message();
        message2.Message = message;
        long elapsed = 0;
        while (elapsed < timeout){
            synchronized (requests){
                if(requests.contains(message2)){
                    for(Message m : requests){
                        if(m.equals(message2)){
                            message2.Value = m.Value;
                            break;
                        }
                    }
                    break;
                }
            }
            Thread.yield();
            elapsed = System.currentTimeMillis() - currentTime;
        }
        synchronized (requests){
            requests.remove(message2);
        }
        callback.apply(message2.Value);
    }

    @Override
    public void messageReceived(SpreadMessage spreadMessage) {
        if(spreadMessage.isRegular()){
            String message = new String(spreadMessage.getData());
            if(message.startsWith("WRITE_REQ: ")){
                int idxSpace = message.indexOf(' ', "WRITE_REQ:".length());
                int idxSpace2 = message.indexOf(' ', idxSpace + 1);
                String key = message.substring(idxSpace + 1, idxSpace2);
                String value = message.substring(idxSpace2 + 1);

                try {
                    SpreadMessage reply = new SpreadMessage();
                    String replyMessage = "COMMIT: " + key + " " + value;
                    reply.setData(replyMessage.getBytes());
                    reply.addGroup(spreadMessage.getSender().toString());
                    reply.setReliable();
                    reply.setSelfDiscard(true);
                    connection.multicast(reply);
                }catch (SpreadException e){

                }
            }
            else if(message.startsWith("COMMIT: ")){
                int idxSpace = message.indexOf(' ', "COMMIT:".length());
                int idxSpace2 = message.indexOf(' ', idxSpace + 1);
                String key = message.substring(idxSpace + 1, idxSpace2);
                String value = message.substring(idxSpace2 + 1);

                Memory.Write(key, value);
                Message message2 = new Message();
                message2.Message = "WRITE_REQ: " + key + " " + value;
                message2.Value = value;
                synchronized (requests){
                    requests.add(message2);
                }
            }
            else if(message.startsWith("INVALIDATE_REQ: ")){
                int idxSpace = message.indexOf(' ', "INVALIDATE_REQ:".length());
                String key = message.substring(idxSpace + 1);

                Message message2 = new Message();
                message2.Message = "REMOVE_REQ: " + key;
                message2.Value = null;
                synchronized (requests){
                    requests.add(message2);
                }
            }
            else if(message.startsWith("REMOVE_REQ: ")){
                int idxSpace = message.indexOf(' ', "REMOVE_REQ:".length());
                String key = message.substring(idxSpace + 1);
                try {
                    SpreadMessage reply = new SpreadMessage();
                    String replyMessage = "INVALIDATE_REQ: " + key;
                    reply.setData(replyMessage.getBytes());
                    reply.addGroup(MemoryClusterGroup);
                    reply.setReliable();
                    reply.setSelfDiscard(true);
                    connection.multicast(reply);
                }catch (SpreadException e){

                }
            }
            else if(message.startsWith("READ_REQ: ")){
                int idxSpace = message.indexOf(' ', "READ_REQ:".length());

                String key = message.substring(idxSpace + 1);
                String value = Memory.Read(key);

                try {
                    SpreadMessage reply = new SpreadMessage();
                    String replyMessage = "READ_RPY: " + key + " " + value;
                    reply.setData(replyMessage.getBytes());
                    reply.addGroup(spreadMessage.getSender().toString());
                    reply.setReliable();
                    reply.setSelfDiscard(true);
                    connection.multicast(reply);
                }catch (SpreadException e){

                }
            }
            else if(message.startsWith("READ_RPY: "))
            {
                int idxSpace = message.indexOf(' ', "READ_RPY:".length());
                int idxSpace2 = message.indexOf(" ", idxSpace + 1);
                String key = message.substring(idxSpace + 1, idxSpace2), idx;

                Message message2 = new Message();
                message2.Message = "READ_REQ: " + key;

                if(message.equals("READ_RPY: " + key + " null")){
                    //não tem valor

                    synchronized (requests){
                        if(requests.contains(message2)){
                            return;
                        }else{
                            requests.add(message2);
                        }
                    }
                }
                else{
                    //tem valor
                    String value = message.substring(idxSpace2 + 1);
                    message2.Value = value;

                    Memory.Write(key, value);
                    synchronized (requests){
                        requests.remove(message2);
                        requests.add(message2);
                    }

                    timer.schedule(new TimerTask() {
                        @Override
                        public void run() {
                            synchronized (requests){
                                requests.remove(message2);
                            }
                        }
                    }, 10000);
                }
            }
            else if(message.startsWith("SET_LEADER: ")){
                String leader = message.substring("SET_LEADER: ".length());
                if(leader.equals(connection.getPrivateGroup().toString())){
                    IsLeader = true;
                    System.out.println("Eu sou o Leader " + leader);
                }else{
                    IsLeader = false;
                    System.out.println("O " + leader + " é o leader");
                }
                Leader = leader;
            }
            else if(message.startsWith("DISCOVERY_REQ: ")){
                String group = message.substring("DISCOVERY_REQ: ".length());
                try {
                    InetAddress ip_hostname = InetAddress.getLocalHost();
                    //String hostname = ip_hostname.getHostName();
                    String private_ip = ip_hostname.getHostAddress();

                    SpreadMessage reply = new SpreadMessage();
                    reply.setData(("DISCOVERY_ACK: " + "localhost" + "|" + "localhost" + "|" + Program.Port).getBytes());
                    reply.addGroup(group);
                    reply.setReliable();
                    connection.multicast(reply);
                }  catch (Exception ex) {
                    ex.printStackTrace();
                }
            }
        }
    }
}
