import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.services.s3.AmazonS3Client;

import java.io.IOException;

public class Application {
    public static void main(String[] args) throws IOException {
        System.out.println("Application started");

        S3Scanner s3Scanner = new S3Scanner(
            new AmazonS3Client(new ProfileCredentialsProvider()),
            "velocity-dev",
            "integration/test/2d10377e-2f33-4dfc-99a5-176d07082cc2/test.txt",
            10485756);

        String line;
        while ((line = s3Scanner.getLine()) != null) {
            System.out.println(line);
        }
    }
}
