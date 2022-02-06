import ClusterConfigService.ClusterConfigServiceGrpc;
import ClusterConfigService.Void;
import ClusterConfigService.CurrentConfiguration;
import io.grpc.ServerBuilder;
import io.grpc.stub.StreamObserver;

import spread.*;

import java.net.InetAddress;
import java.util.*;

public class ClusterConfigureServer extends ClusterConfigServiceGrpc.ClusterConfigServiceImplBase{

    //gRPC attributes
    static String hostName = "config_svc";
    private static io.grpc.Server configSVC;
    private static int svcPort = 6000;

    //Spread attributes
    static String daemonIP = "localhost";
    private static int daemonPort = 4803;
    public static SpreadConnection connection;
    private static String selfUnicastAddress;

    //Spread group attributes
    private static SpreadGroup grupoClusters;
    private static String clustersGroupName = "Clusters";
    private static String MemoryClusterGroup = "Clusters";
    private static String Leader = null;

    // Stores all available cluster members (db_svc's)
    //////////////////////////////////////////////////
    static HashMap<String, Database_Svc> db_svcs = new HashMap<String, Database_Svc>();

    // Stores all client's StreamObservers
    //////////////////////////////////////
    static HashSet<StreamObserver<CurrentConfiguration>> clients = new HashSet<StreamObserver<CurrentConfiguration>>();

    //Métodos nativos do config_svc
    ///////////////////////////////
    public static void setup_gRPC(){
        try {
            configSVC = ServerBuilder.forPort(svcPort).addService(new ClusterConfigureServer()).build();
            configSVC.start();
            System.out.println("CONFIG_SVC>> gRPC setup completed with success.\n");
            System.out.println("CONFIG_SVC>> Server started, listening on port " + svcPort + "\n");
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }
    public static void setupSpread(){
        try{
            //Creates spread connection
            ///////////////////////////
            connection = new SpreadConnection();
            connection.connect(InetAddress.getByName(daemonIP), daemonPort, hostName, false, true);

            //Get it's unicast address so cluster member can talk to this config-svc privately
            //////////////////////////////////////////////////////////////////////////////////
            selfUnicastAddress = connection.getPrivateGroup().toString();

            //Adds a listener do that connection
            ////////////////////////////////////
            connection.add(new MessageListener(connection));

            System.out.println("CONFIG_SVC>> Connected to Spread Daemon " + daemonIP + ":" + daemonPort + " w/ selfUnicastAddress: " + selfUnicastAddress + "\n");

            //Joins "Clusters" group
            ////////////////////////
            grupoClusters = new SpreadGroup();
            grupoClusters.join(connection, clustersGroupName);

            System.out.println("CONFIG_SVC>> Joined " + clustersGroupName + " multicast group with success.\n");

            System.out.println("CONFIG_SVC>> Sending DISCOVERY_REQ to multicast group " + clustersGroupName + "\n");

            //Multicast its own (config_svc's) private group name to the "Clusters" group
            discoveryRequestHandler();

            System.out.println("CONFIG_SVC>> Spread setup completed with success.\n");

        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }
    public static void notifyClients(){

        //Creating cluster group message
        ////////////////////////////////
        System.out.println("CONFIG_SVC>> Creating message with newly updated list of available db_servers...");
        CurrentConfiguration clusterConfigMsg = clusterGroup_Msg_Builder(getAvailableSvcs());
        System.out.println("CONFIG_SVC>> Message with newly updated of available db_servers created with success.\n");

        //Para cada cliente que consta nos registos do config_svc, é feita a notificação
        ////////////////////////////////////////////////////////////////////////////////
        for (StreamObserver<CurrentConfiguration> client : clients){
            try{
                client.onNext(clusterConfigMsg);
            } catch (Exception ex) {

                //Verifica se a exceção se deve ao facto do cliente se ter desconectado
                ///////////////////////////////////////////////////////////////////////
                if(ex.getMessage().startsWith("CANCELLED")){
                    System.out.println("CONFIG_SVC>> A client no longer is using the cluster service. Removing client...");
                    clients.remove(client);
                    System.out.println("CONFIG_SVC>> Client removed with success.\n");
                }

            }

        }
        System.out.println("CONFIG_SVC>> New list of available cluster members returned with success.\n");
    }
    public static CurrentConfiguration clusterGroup_Msg_Builder(List<Database_Svc> availableSvcs){

        //TODO: Meter try...catch aqui?
        CurrentConfiguration.Builder clusterConfig = CurrentConfiguration.newBuilder();
        String host;

        for (Database_Svc svc : availableSvcs) {
            //Estrutura da mensagem: <{public_ip}>|<{private_ip}>|<{svc_port}>
            ///////////////////////////////////////////
            host = svc.getPublicIP()+"|"+svc.getPrivateIP()+"|"+svc.getSvcPort();
            clusterConfig.addHostAndPort(host);
        }
        return clusterConfig.build();
    }

    //Métodos tipo Camada Domínio
    /////////////////////////////
    //TODO: Criar classe com estes métodos - DiscoveryService
    public static void discoveryRequestHandler(){

        try{

            //Multicast its own (config_svc's) private group name to the "Clusters" group
            /////////////////////////////////////////////////////////////////////////////
            SpreadMessage svcDiscoveryReq = new SpreadMessage();
            svcDiscoveryReq.setData(("DISCOVERY_REQ: " + selfUnicastAddress).getBytes());
            svcDiscoveryReq.addGroup(clustersGroupName);
            svcDiscoveryReq.setReliable();

            //Não necessita de receber as suas próprias mensagens
            /////////////////////////////////////////////////////
            svcDiscoveryReq.setSelfDiscard(true);

            //Send the message to everyone listening the "Clusters" group
            /////////////////////////////////////////////////////////////
            connection.multicast(svcDiscoveryReq);

        } catch(Exception ex){
            ex.printStackTrace();
        }

    }
    public static void discoveryAckHandler(String discoveryAck, String sender){
        System.out.println("CONFIG_SVC>> Received DISCOVERY_ACK message from: " + sender + "\n");
        System.out.println("CONFIG_SVC>> DISCOVERY_ACK message fields: " + discoveryAck.replace("DISCOVERY_ACK: ", "") + "\n");

        //Obter o corpo da mensagem com a informação do svc_db
        //////////////////////////////////////////////////////
        String msg_body = discoveryAck.replace("DISCOVERY_ACK: ", "");

        //Corpo da mensagem: "DISCOVERY_ACK: <{public_ip}>|<{private_ip}>|<{svc_port}>"
        //Exemplo: "DISCOVERY_ACK: 8.8.8.8|10.100.114.214|8000
        ///////////////////////////////////////////////////////////////////////////////
        String[] body_fields = msg_body.split("\\|");

        if (db_svcs.containsKey(sender))
            return;

        //Caso não exista, adicionar à lista
        //////////////////////////////////////////////////////
        Database_Svc db_svc = new Database_Svc(sender,body_fields[0], body_fields[1], body_fields[2], true);
        addClusterMember(db_svc);

        if(!MessageListener.HasLeader){
            Leader = sender;
            MessageListener.HasLeader = true;
        }
        try{
            SpreadMessage leaderMessage = new SpreadMessage();
            String replyMessage = "SET_LEADER: " + Leader;
            leaderMessage.setData(replyMessage.getBytes());
            leaderMessage.addGroup(MemoryClusterGroup);
            leaderMessage.setReliable();
            leaderMessage.setSelfDiscard(true);
            connection.multicast(leaderMessage);
        }catch (SpreadException e){

        }


        //Atualizar a configuração e enviar para todos os clientes
        //////////////////////////////////////////////////////////
        notifyClients();

    }

    //Métodos tipo Camada de Acesso a Dados
    ///////////////////////////////////////

    //TODO: Criar classes com estes métodos - ClusterRepository
    public static void addClusterMember(Database_Svc db_svc){
        System.out.println("CONFIG_SVC>> Adding " + db_svc.getHostname() + " to the available svc's list.");
        synchronized (db_svcs){
            db_svcs.put(db_svc.getHostname(), db_svc);
        }
        System.out.println("CONFIG_SVC>> " + db_svc.getHostname() + " added with success to the available svc's list.\n");
    }
    public static List<Database_Svc> getAvailableSvcs(){

        List<Database_Svc> availableSvcs = new ArrayList<Database_Svc>();

        for (Map.Entry<String, Database_Svc> db_svcEntry : db_svcs.entrySet()){
            availableSvcs.add(db_svcEntry.getValue());
        }

        return availableSvcs;
    }
    public static void removeClusterMember(String db_svc){
        System.out.println("CONFIG_SVC>> Removing " + db_svc + " from the available svc's list.");
        synchronized (db_svcs){
            db_svcs.remove(db_svc);
        }
        System.out.println("CONFIG_SVC>> " + db_svc + " removed with success from the available svc's list.\n");
    }

    @Override
    public void getClusterGroup(Void request, StreamObserver<CurrentConfiguration> responseObserver) {

        System.out.println("CONFIG_SVC>> A client wants to access the cluster. Storing client...");
        //TODO: Meter try...catch aqui?
        clients.add(responseObserver);
        System.out.println("CONFIG_SVC>> Client was stored with success.\n");

        //Creating cluster group message
        ////////////////////////////////
        System.out.println("CONFIG_SVC>> Creating message with list of available db_servers...\n");
        CurrentConfiguration clusterConfigMsg = clusterGroup_Msg_Builder(getAvailableSvcs());
        System.out.println("CONFIG_SVC>> Message with list of available db_servers created with success.\n");

        //Returning message to client
        /////////////////////////////
        System.out.println("CONFIG_SVC>> Returning to client message with list of available cluster members...");
        responseObserver.onNext(clusterConfigMsg);
        System.out.println("CONFIG_SVC>> Message with list of available cluster members returned with success.\n");

    }

    public static void main(String[] args) {
        try {
            //O utilizador pode introduzir o nome do cluster e o IP do daemon enquanto parâmetros de execução
            /////////////////////////////////////////////////////////////////////////////////////////////////
            if (args.length > 0 ) {
                hostName=args[0];
                daemonIP=args[1];
                svcPort= Integer.parseInt(args[2]);
            }

            //Startup dos serviços Spread e gRPC
            ////////////////////////////////////
            setup_gRPC();
            setupSpread();


            //Shut server
            /////////////
            System.out.println("CONFIG_SVC>> Press any <Key> to shut the server.\n");
            Scanner scan=new Scanner(System.in); scan.nextLine();

            //Abandona o grupo "Clusters", comunicação Spread, e termina o serviço gRPC
            ///////////////////////////////////////////////////////////////////////////
            grupoClusters.leave();
            configSVC.shutdown();

        } catch (Exception ex) {
            ex.printStackTrace();
        }

    }

}
