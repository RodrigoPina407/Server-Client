package myclient;

import ClusterConfigService.CurrentConfiguration;
import com.google.common.base.Function;
import io.grpc.stub.StreamObserver;
import java.util.ArrayList;

public class ClusterGroupObserver implements StreamObserver<CurrentConfiguration> {
    private final ArrayList<String> hosts = new ArrayList<String>();
    private final Function<String[], Void> callback;

    public ClusterGroupObserver(Function<String[], Void> callback){
        this.callback = callback;
    }

    @Override
    public void onNext(CurrentConfiguration currentConfiguration) {
        String[] copy;
        synchronized (hosts){
            hosts.clear();
            hosts.addAll(currentConfiguration.getHostAndPortList());
            copy = hosts.toArray(new String[0]);
        }
        callback.apply(copy);
    }

    @Override
    public void onError(Throwable throwable) {

    }

    @Override
    public void onCompleted() {

    }
}
