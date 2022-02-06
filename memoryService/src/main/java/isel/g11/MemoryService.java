package isel.g11;

import com.google.common.base.Function;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;
import isel.g11.memory.Key;
import isel.g11.memory.KeyValuePair;
import isel.g11.memory.MemoryServiceGrpc;
import isel.g11.memory.Void;
import spread.SpreadConnection;
import spread.SpreadException;
import spread.SpreadMessage;

public class MemoryService extends MemoryServiceGrpc.MemoryServiceImplBase {
    //private static String MemoryClusterGroup = "MemoryCLuster";
    private static String MemoryClusterGroup = "Clusters";
    private final SpreadConnection spreadConnection;

    public MemoryService(SpreadConnection connection){
        this.spreadConnection = connection;
    }

    @Override
    public void read(Key request, StreamObserver<KeyValuePair> responseObserver) {
        read(request.getKey(), v -> {
            if(v == null){
                responseObserver.onError(new StatusRuntimeException(Status.NOT_FOUND));
                return null;
            }
            KeyValuePair result = KeyValuePair.newBuilder()
                    .setKey(request.getKey())
                    .setValue(v)
                    .build();
            responseObserver.onNext(result);
            responseObserver.onCompleted();
            return null;
        });
    }

    public void read(String key, Function<String, Object> callback) {
        String value = Memory.Read(key);
        if(value != null){
            callback.apply(value);
            return;
        }

        try {
            SpreadMessage reply = new SpreadMessage();
            String replyMessage = "READ_REQ: " + key;
            reply.setData(replyMessage.getBytes());
            reply.addGroup(MemoryClusterGroup);
            reply.setReliable();
            reply.setSelfDiscard(true);
            spreadConnection.multicast(reply);

            MessageListener.ListenForReply(replyMessage, v -> callback.apply(v));
        }
        catch (SpreadException e){

        }
    }


    @Override
    public void write(KeyValuePair request, StreamObserver<Void> responseObserver) {
        //se for lider -> response o pedido
        //senão reencaminha o pedido para o leader
        read(request.getKey(), value -> {
            if(value == null){
                if(MessageListener.IsLeader){
                    Memory.Write(request.getKey(), request.getValue());
                    responseObserver.onNext(Void.newBuilder().build());
                    responseObserver.onCompleted();
                    return null;
                }
                else {
                    try {
                        SpreadMessage reply = new SpreadMessage();
                        String replyMessage = "WRITE_REQ: " + request.getKey() + " " + request.getValue();
                        reply.setData(replyMessage.getBytes());
                        reply.addGroup(MessageListener.Leader);
                        reply.setReliable();
                        reply.setSelfDiscard(true);
                        spreadConnection.multicast(reply);

                        MessageListener.ListenForReply(replyMessage, v -> {
                            responseObserver.onNext(Void.newBuilder().build());
                            responseObserver.onCompleted();
                            return null;
                        });
                    }
                    catch (SpreadException e){

                    }
                }
            }
            else if(value != null){
                if(MessageListener.IsLeader){
                    try {
                        SpreadMessage reply = new SpreadMessage();
                        String replyMessage = "INVALIDATE_REQ: " + request.getKey();
                        reply.setData(replyMessage.getBytes());
                        reply.addGroup(MemoryClusterGroup);
                        reply.setReliable();
                        reply.setSelfDiscard(false);
                        spreadConnection.multicast(reply);

                        MessageListener.ListenForReply(replyMessage, v -> {
                            Memory.Write(request.getKey(), request.getValue());
                            responseObserver.onNext(Void.newBuilder().build());
                            responseObserver.onCompleted();
                            return null;
                        });
                    }
                    catch (SpreadException e){

                    }
                }else{
                    try {
                        SpreadMessage reply = new SpreadMessage();
                        String replyMessage = "REMOVE_REQ: " + request.getKey();
                        reply.setData(replyMessage.getBytes());
                        reply.addGroup(MessageListener.Leader);
                        reply.setReliable();
                        reply.setSelfDiscard(true);
                        spreadConnection.multicast(reply);

                        MessageListener.ListenForReply(replyMessage, v -> {
                            Memory.Write(request.getKey(), request.getValue());
                            responseObserver.onNext(Void.newBuilder().build());
                            responseObserver.onCompleted();
                            return null;
                        });
                    }
                    catch (SpreadException e){

                    }
                }
            }
            return null;
        });
    }
}
