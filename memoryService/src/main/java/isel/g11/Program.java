package isel.g11;

import io.grpc.ServerBuilder;
import spread.SpreadConnection;
import spread.SpreadException;
import spread.SpreadGroup;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.UUID;

public class Program {
    public static int Port = 8000;
    private static String daemonIP = "localhost";
    private static int daemonPort = 4803;
    private static String hostName = "localhost";
    private static String MemoryClusterGroup = "MemoryCluster";
    private static String ConfigClusterGroup = "Clusters";
    public static SpreadConnection connection;

    public static SpreadConnection setupSpread(){
        try{
            hostName = "memory_" + UUID.randomUUID();
            //Creates spread connection
            ///////////////////////////
            connection = new SpreadConnection();
            connection.connect(InetAddress.getByName(daemonIP), daemonPort, hostName, false, true);
            connection.add(new MessageListener(connection));

            SpreadGroup grupoClusters = new SpreadGroup();
            grupoClusters.join(connection, ConfigClusterGroup);

            while(MessageListener.Leader == null){
                Thread.yield();
            }

            System.out.println("CONFIG_SVC>> Spread setup completed with success.\n");
            return connection;
        } catch (SpreadException | UnknownHostException ex) {
            return null;
        }
    }

    public static void main(String[] args){
        if(args.length == 1){
            Port = Integer.parseInt(args[0]);
        }

        SpreadConnection spreadConnection = setupSpread();
        io.grpc.Server svc = ServerBuilder
                .forPort(Port)
                .addService(new MemoryService(spreadConnection))
                .build();
        try{
            svc.start();

            System.in.read();
        }catch (IOException e){

        }
    }
}
