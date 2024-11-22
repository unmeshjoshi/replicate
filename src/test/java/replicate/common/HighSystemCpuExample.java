package replicate.common;
import java.io.BufferedOutputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Paths;

public class HighSystemCpuExample {
    public static void main(String[] args) {
        String fileName = "testfile.bin";
        byte[] data = new byte[1024]; // 1 KB of data

        try (OutputStream fos =
                     new BufferedOutputStream(new FileOutputStream(Paths.get(System.getProperty(
                             "java.io.tmpdir"), fileName).toFile(), true))) {
            for (int i = 0; i < 100000000; i++) { // Large number of writes
                fos.write(data);
//                fos.flush(); // Frequently flush to disk
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
