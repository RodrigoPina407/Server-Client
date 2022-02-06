package isel.g11;

public class Message{
    public String Message;
    public String Value;

    @Override
    public int hashCode() {
        return  Message.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        return equals((Message)obj);
    }

    public  boolean equals(Message message){
        if(this.Message.equals(message.Message)){
            return true;
        }
        return false;
    }
}