package replicator.singularupdatequeue;

import replicator.common.Logging;

import java.util.Optional;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;

//<codeFragment name = "queue">
public class ActorLikeSingularUpdateQueue<Req, Res> implements Runnable, Logging {
    private ArrayBlockingQueue<RequestWrapper<Req, Res>> workQueue
            = new ArrayBlockingQueue<RequestWrapper<Req, Res>>(100);
    private Function<Req, Res> handler;
    private volatile boolean isRunning = false;
    //</codeFragment>

    static Executor executor = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors());

    public ActorLikeSingularUpdateQueue(Function<Req, Res> handler) {
        this.handler = handler;
    }
    volatile AtomicBoolean isScheduled = new AtomicBoolean(false);

    //<codeFragment name = "submit">
    public CompletableFuture<Res> submit(Req request) {
        try {
            var requestWrapper = new RequestWrapper<Req, Res>(request);
            workQueue.put(requestWrapper);
            registerForExecution();
            return requestWrapper.getFuture();
        }
        catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    private void registerForExecution() {
        if (!workQueue.isEmpty()) {
            if (setAsScheduled()) {
                executor.execute(this);
            }
        }
    }

    private boolean setAsScheduled() {
        return isScheduled.compareAndSet(false, true);
    }
    //</codeFragment>

    //<codeFragment name = "run">
    @Override
    public void run() {
        try {
            Optional<RequestWrapper<Req, Res>> item = take();
            item.ifPresent(requestWrapper -> {
                try {
                    Res response = handler.apply(requestWrapper.getRequest());
                    requestWrapper.complete(response);

                } catch (Exception e) {
                    requestWrapper.completeExceptionally(e);
                }
            });
        } finally {
          isScheduled.set(false);
          registerForExecution();
        }
    }
    //</codeFragment>
    //<codeFragment name = "take">
    private Optional<RequestWrapper<Req, Res>> take() {
        try {
            return Optional.ofNullable(workQueue.poll(300, TimeUnit.MILLISECONDS));

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

    public void start() {

    }
}
