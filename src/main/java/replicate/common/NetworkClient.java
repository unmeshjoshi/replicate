package replicate.common;

import replicate.net.InetAddressAndPort;
import replicate.net.SocketClient;

import java.io.IOException;
import java.util.Optional;

public class NetworkClient {
    public static class Response<T> {
        T result;
        Optional<String> errorMessage = Optional.empty();

        public boolean isSuccess() {
            return errorMessage.isEmpty();
        }

        public boolean isError() {
            return errorMessage.isPresent();
        }

        public T getResult() {
            return result;
        }

        public Optional<String> getErrorMessage() {
            return errorMessage;
        }

        public static Response error(String message) {
            return new Response(message);
        }

        public  static <R> Response<R> result(R result) {
            return new Response(result);
        }

        private Response(String message) {
            this.errorMessage = Optional.of(message);
        }

        private Response(T result) {
            this.result = result;
        }
    }
    public <Req extends Request, Res> Response<Res> sendAndReceive(Req request, InetAddressAndPort address, Class<Res> responseClass) throws IOException {
        try(SocketClient<Object> client = new SocketClient<>(address)){
            RequestOrResponse getResponse = client.blockingSend(new RequestOrResponse(request.getRequestId().getId(),
                    JsonSerDes.serialize(request)));
            if (getResponse.isError()) {
                return Response.error(JsonSerDes.deserialize(getResponse.getMessageBodyJson(), String.class));
            };
            Res result = JsonSerDes.deserialize(getResponse.getMessageBodyJson(), responseClass);
            return Response.result(result);
        }
    }
}
