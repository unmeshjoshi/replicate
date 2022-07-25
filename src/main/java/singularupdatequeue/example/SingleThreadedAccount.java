package singularupdatequeue.example;

import singularupdatequeue.SingularUpdateQueue;

import java.util.concurrent.CompletableFuture;

public class SingleThreadedAccount {
    private SingularUpdateQueue<Request, Response> queue;
    private int balance = 0;

    //<codeFragment name="constructor">
    public SingleThreadedAccount(int balance) {
        this.balance = balance;
        this.queue = new SingularUpdateQueue<Request, Response>(this::handleMessage);
        this.queue.start();
    }
    //</codeFragment>

    //<codeFragment name="handleMessage">
    private Response handleMessage(Request request) {
        if (request.requestType == RequestType.CREDIT) {
            return creditAmount(request);

        } else if (request.requestType == RequestType.DEBIT) {
            return debitAmount(request);
        }
        throw new IllegalArgumentException("Unknown request type " + request.requestType);
    }

    private Response debitAmount(Request request) {
        if (balance < request.amount) {
            return Response.failure("Not enough balance");
        }
        balance -= request.amount;
        return Response.succecss(balance);
    }

    private Response creditAmount(Request request) {
        balance += request.amount;
        return Response.succecss(balance);
    }
    //</codeFragment>

    //<codeFragment name="methods">
    public CompletableFuture<Response> credit(int amount) {
        return queue.submit(new Request(amount, RequestType.CREDIT));
    }

    public CompletableFuture<Response> debit(int amount) {
        return queue.submit(new Request(amount, RequestType.DEBIT));
    }
    //</codeFragment>
}
