import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.services.s3.AmazonS3Client;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.io.IOException;
import java.util.Scanner;

public class S3ScannerTests {
    private static final long ONE_BYTE_BUFFER_SIZE = 1;
    private static final long SMALL_BUFFER_SIZE = 64;
    private static final long MEDIUM_BUFFER_SIZE = 32768;
    private static final long LARGE_BUFFER_SIZE = 10485756;
    private static final String DOMAIN = "velocity-dev";
    private static final String FILE_URL = "integration/test/77e8bc9d-f90d-44a6-a6e7-864001f99cf2/testData.csv";
    private static final String TEST_FILE_PATH = "testData.csv";

    private Scanner scanner;

    @Before
    public void setUp() {
        scanner = new Scanner(getClass().getClassLoader().getResourceAsStream(TEST_FILE_PATH));
    }

    @After
    public void tearDown() {
        scanner.close();
    }

    @Test
    public void When_FileContentIsBeingLoadedWithMediumBuffer_Expect_ToContainTheSameData() throws IOException {
        S3Scanner s3Scanner = new S3Scanner(new AmazonS3Client(new ProfileCredentialsProvider()), DOMAIN, FILE_URL, MEDIUM_BUFFER_SIZE);
        String line;
        long startTime = System.nanoTime();
        while ((line = s3Scanner.getLine()) != null) {
            assertThat(line, is(scanner.nextLine()));
        }

        System.out.println("Time passed: " + (System.nanoTime() - startTime)/Math.pow(10, 9) + "seconds");
    }

    @Test
    public void When_FileContentIsBeingLoadedWithLargeBuffer_Expect_ToContainTheSameData() throws IOException {
        S3Scanner s3Scanner = new S3Scanner(new AmazonS3Client(new ProfileCredentialsProvider()), DOMAIN, FILE_URL, LARGE_BUFFER_SIZE);
        String line;
        long startTime = System.nanoTime();
        while ((line = s3Scanner.getLine()) != null) {
            assertThat(line, is(scanner.nextLine()));
        }

        System.out.println("Time passed: " + (System.nanoTime() - startTime)/Math.pow(10, 9) + "seconds");
    }

    @Test
    public void When_FileContentIsBeingLoadedWithSmallBuffer_Expect_ToContainTheSameData() throws IOException {
        S3Scanner s3Scanner = new S3Scanner(new AmazonS3Client(new ProfileCredentialsProvider()), DOMAIN, FILE_URL, SMALL_BUFFER_SIZE);
        String line;
        long startTime = System.nanoTime();
        while ((line = s3Scanner.getLine()) != null) {
            assertThat(line, is(scanner.nextLine()));
        }

        System.out.println("Time passed: " + (System.nanoTime() - startTime)/Math.pow(10, 9) + " seconds");
    }

    @Test
    public void When_FileContentIsBeingLoadedWithOne_ByteBuffer_Expect_ToContainTheSameData() throws IOException {
        S3Scanner s3Scanner = new S3Scanner(new AmazonS3Client(new ProfileCredentialsProvider()), DOMAIN, FILE_URL, ONE_BYTE_BUFFER_SIZE);
        String line;
        long startTime = System.nanoTime();
        while ((line = s3Scanner.getLine()) != null) {
            assertThat(line, is(scanner.nextLine()));
        }

        System.out.println("Time passed: " + (System.nanoTime() - startTime)/Math.pow(10, 9) + "seconds");
    }
}
