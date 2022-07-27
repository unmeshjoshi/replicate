package distrib.patterns.singularupdatequeue;

import distrib.patterns.common.Logging;

import java.util.Optional;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

//<codeFragment name = "queue">
public class SingularUpdateQueue<Req, Res> extends Thread implements Logging {
    private ArrayBlockingQueue<RequestWrapper<Req, Res>> workQueue
            = new ArrayBlockingQueue<RequestWrapper<Req, Res>>(100);
    private Function<Req, Res> handler;
    private volatile boolean isRunning = false;
    //</codeFragment>

    public SingularUpdateQueue(Function<Req, Res> handler) {
        this.handler = handler;
    }


    //<codeFragment name = "submit">
    public CompletableFuture<Res> submit(Req request) {
        try {
            var requestWrapper = new RequestWrapper<Req, Res>(request);
            workQueue.put(requestWrapper);
            return requestWrapper.getFuture();
        }
        catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }
    //</codeFragment>

    //<codeFragment name = "run">
    @Override
    public void run() {
         isRunning = true;
         while(isRunning) {
             Optional<RequestWrapper<Req, Res>> item = take();
             item.ifPresent(requestWrapper -> {
                 try {
                     Res response = handler.apply(requestWrapper.getRequest());
                     requestWrapper.complete(response);

                 } catch (Exception e) {
                     requestWrapper.completeExceptionally(e);
                 }
             });
        }
    }
    //</codeFragment>
    //<codeFragment name = "take">
    private Optional<RequestWrapper<Req, Res>> take() {
        try {
            return Optional.ofNullable(workQueue.poll(2, TimeUnit.MILLISECONDS));

        } catch (InterruptedException e) {
            return Optional.empty();
        }
    }


    public void shutdown() {
        this.isRunning = false;
    }

    //</codeFragment>

    public int taskCount() {
        return workQueue.size();
    }

    public boolean isRunning() {
        return isRunning;
    }
}
