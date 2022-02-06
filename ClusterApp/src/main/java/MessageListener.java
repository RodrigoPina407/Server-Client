import spread.*;

public class MessageListener implements BasicMessageListener {
    private SpreadConnection connection;
    public static boolean HasLeader = false;

    public MessageListener(SpreadConnection connection) {
        this.connection=connection;
    }

    @Override
    public void messageReceived(SpreadMessage spreadMessage) {
        try {

            //Recebida mensagem de Membership
            /////////////////////////////////
            if (spreadMessage.isMembership()){
                HasLeader = false;
                MembershipInfo info = spreadMessage.getMembershipInfo();

                if(info.isCausedByJoin()) {
                    System.out.println("Spread>> " + info.getJoined() + " has JOINED group " + info.getGroup().toString() + "\n");

                    //Se não for uma membership message do próprio config_svc a entrar no grupo...
                    //////////////////////////////////////////////////////////////////////////////
                    if (!info.getJoined().toString().equals("#" + ClusterConfigureServer.hostName + "#" + ClusterConfigureServer.daemonIP)){

                        System.out.println("CONFIG_SVC>> A svc has joined the Cluster.\n");

                        //Invoca a descoberta de membros do cluster em operação
                        ///////////////////////////////////////////////////////
                        ClusterConfigureServer.discoveryRequestHandler();

                    }

                } else if(info.isCausedByLeave() || info.isCausedByDisconnect()) {
                    String clusterMember = info.isCausedByLeave() ? info.getLeft().toString() : info.getDisconnected().toString();
                    System.out.println("Spread>> " + clusterMember + " has LEFT group " + info.getGroup().toString() + "\n");
                    System.out.println("CONFIG_SVC>> A svc has left the Cluster.\n");

                    //Retira svc da lista de svc's disponíveis
                    //////////////////////////////////////////
                    ClusterConfigureServer.removeClusterMember(clusterMember);

                    //Notifica clientes do cluster
                    //////////////////////////////
                    ClusterConfigureServer.notifyClients();

                    //Invoca a descoberta de membros do cluster em operação
                    ///////////////////////////////////////////////////////
                    ClusterConfigureServer.discoveryRequestHandler();

                }

            //Recebida mensagem Regular
            ///////////////////////////
            } else {

                //Extracts the message
                String message = new String(spreadMessage.getData());

                //Extracts its sender
                String sender = spreadMessage.getSender().toString();

                //Verificar se a mensagem é de resposta a algum DISCOVERY_REQ efetuado anteriormente
                ////////////////////////////////////////////////////////////////////////////////////
                if (message.startsWith("DISCOVERY_ACK: ")){

                    //Reencaminha para o handler deste tipo de mensagens
                    ////////////////////////////////////////////////////
                    ClusterConfigureServer.discoveryAckHandler(message, sender);
                    return;
                }

                System.out.println("CONFIG_SVC>> Sender of this message is: " + sender);
                System.out.println("CONFIG_SVC>> Message content: " + message);
            }
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }
}
