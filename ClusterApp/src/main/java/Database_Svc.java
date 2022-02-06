public class Database_Svc {

    private String hostname;
    private String privateIP;
    private String publicIP;
    private String svcPort;
    private boolean isLeader;

    Database_Svc(String hostname, String privateIP, String publicIP, String svcPort, boolean isLeader){
        this.hostname = hostname;
        this.privateIP = privateIP;
        this.publicIP = publicIP;
        this.svcPort = svcPort;
        this.isLeader = isLeader;
    }

    Database_Svc(String hostname, String privateIP, String publicIP, String svcPort){
        this(hostname, privateIP, publicIP, svcPort, false);
    }

    public String getHostname(){
        return hostname;
    }

    public String getPrivateIP(){
        return privateIP;
    }

    public String getPublicIP(){
        return publicIP;
    }

    public String getSvcPort(){
        return svcPort;
    }

    public boolean isLeader(){ return isLeader; }

}
