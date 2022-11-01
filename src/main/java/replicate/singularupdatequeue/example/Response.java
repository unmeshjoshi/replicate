package replicate.singularupdatequeue.example;

//<codeFragment name="responseMessage">
class Response {
    ResponseCode responseCode;

    public Response(ResponseCode responseCode) {
        this.responseCode = responseCode;
    }

    public static Response succecss(int amount) {
        return new SuccessResponse(ResponseCode.SUCCESS, amount);
    }

    public static Response failure(String message) {
        return new ErrorResponse(ResponseCode.FAILURE, message);
    }

    public boolean isSuccess() {
        return responseCode == ResponseCode.SUCCESS;
    }

    public boolean isError() {
        return responseCode == ResponseCode.FAILURE;
    }
}


class SuccessResponse extends Response {
    int balance;

    public SuccessResponse(ResponseCode responseCode, int balance) {
        super(responseCode);
        this.balance = balance;
    }

    public SuccessResponse withAmount(int amount) {
        this.balance = amount;
        return this;
    }

    public int getBalance() {
        return balance;
    }

}

class ErrorResponse extends Response {
    private String errorMessage;

    public ErrorResponse(ResponseCode responseCode, String errorMessage) {
        super(responseCode);
        this.errorMessage = errorMessage;
    }

    public String getErrorMessage() {
        return errorMessage;
    }
}

//</codeFragment>