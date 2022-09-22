package distrib.patterns.generation;

public class PrepareRequest {
    int number;

    public PrepareRequest(int number) {
        this.number = number;
    }

    public int getNumber() {
        return number;
    }
    //for jackson
    private PrepareRequest() {

    }
}
