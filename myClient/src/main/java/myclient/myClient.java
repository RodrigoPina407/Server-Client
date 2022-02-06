package myclient;

import ClusterConfigService.ClusterConfigServiceGrpc;
import ClusterConfigService.Void;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import isel.g11.memory.Key;
import isel.g11.memory.KeyValuePair;
import isel.g11.memory.MemoryServiceGrpc;

import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.util.Enumeration;
import java.util.Random;
import java.util.Scanner;

public class myClient {
    //Definição do IP e port do servidor
    private static String svcIP = "localhost";
    private static int svcPort = 6000;

    //Criação do canal de comunicação cliente-servidor
    private static ManagedChannel clusterChannel;
    private static ManagedChannel memoryChannel;

    private static ClusterConfigServiceGrpc.ClusterConfigServiceStub clusterStub;
    private static MemoryServiceGrpc.MemoryServiceBlockingStub memoryStub;
    private static Scanner scanner = new Scanner(System.in);
    private static Random r = new Random();

    private static void Write(){
        System.out.println("Introduza a chave");
        String key = scanner.nextLine();

        System.out.println("Introduza o valor");
        String value = scanner.nextLine();

        KeyValuePair data = KeyValuePair.newBuilder()
                .setKey(Integer.toString(key.hashCode()))
                .setValue(value)
                .build();

        memoryStub.write(data);
    }

    private static void Read(){
        System.out.println("Introduza a chave");
        String key = scanner.nextLine();

        Key key2 = Key.newBuilder().setKey(Integer.toString(key.hashCode())).build();
        KeyValuePair pair = memoryStub.read(key2);
        System.out.println(pair.getKey() + " - " + pair.getValue());
    }

    public static void main(String[] args) throws SocketException {
        if (args.length == 2) {
            svcIP = args[0]; svcPort = Integer.parseInt(args[1]);
        }

        clusterChannel = ManagedChannelBuilder.forAddress(svcIP, svcPort).usePlaintext().build();

        clusterStub = ClusterConfigServiceGrpc.newStub(clusterChannel);
        clusterStub.getClusterGroup(Void.newBuilder().build(), new ClusterGroupObserver(arr -> {
            if(arr.length > 0){
                int idx = r.nextInt(arr.length);
                String currentMemoryHost = arr[idx];

                String[] parts = currentMemoryHost.split("\\|");
                String publicIp = parts[0];
                int port = Integer.parseInt(parts[2]);

                memoryChannel = ManagedChannelBuilder.forAddress(publicIp, 8000).usePlaintext().build();
                memoryStub = MemoryServiceGrpc.newBlockingStub(memoryChannel);
            }
            else{
                memoryChannel = null;
                memoryStub = null;
            }
            return null;
        }));

        while (true){
            if(memoryStub == null){
                System.out.println("Waiting for memory service");
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {

                }
            }
            else{
                System.out.println("Escolha uma opção");
                System.out.println("1. Escrever");
                System.out.println("2. Ler");
                int option = scanner.nextInt();
                scanner.nextLine();

                if(option == 1){
                    Write();
                }
                else if(option == 2){
                    Read();
                }
            }
        }
    }
}

