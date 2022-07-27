package distrib.patterns.singularupdatequeue.example;

//<codeFragment name="requestMessage">
class Request {
    int amount;
    RequestType requestType;

    public Request(int amount, RequestType requestType) {
        this.amount = amount;
        this.requestType = requestType;
    }
}
//</codeFragment>