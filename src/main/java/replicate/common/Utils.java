package replicate.common;

import java.math.BigInteger;
import java.security.MessageDigest;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

public class Utils {

    public static BigInteger hash(String data)
    {
        byte[] result = hash("MD5", data.getBytes());
        BigInteger hash = new BigInteger(result);
        return hash.abs();
    }


    public static byte[] hash(String type, byte[] data)
    {
        byte[] result = null;
        try
        {
            MessageDigest messageDigest = MessageDigest.getInstance(type);
            result = messageDigest.digest(data);
        }
        catch (Exception e)
        {
            throw new RuntimeException(e);
        }
        return result;
    }


    public static <T> CompletableFuture<List<T>> sequence(List<CompletableFuture<T>> futuresList) {
        return CompletableFuture
                .allOf(futuresList.toArray(new CompletableFuture[futuresList.size()]))
                .thenApply(v ->
                        futuresList.stream()
                                .map(CompletableFuture::join)
                                .collect(Collectors.toList()));
    }
}
