import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.io.IOException;
import java.util.Scanner;

@Category(S3IntegrationTest.class)
public class SecondaryS3ScannerTests {
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
        SecondaryS3Scanner s3Scanner = new SecondaryS3Scanner(DOMAIN, FILE_URL, MEDIUM_BUFFER_SIZE);
        String line;
        long startTime = System.nanoTime();
        while ((line = s3Scanner.getLine()) != null) {
            assertThat(line, is(scanner.nextLine()));
        }

        System.out.println("Time passed for medium buffer: " + (System.nanoTime() - startTime)/Math.pow(10, 9) + " seconds");
    }

    @Test
    public void When_FileContentIsBeingLoadedWithLargeBuffer_Expect_ToContainTheSameData() throws IOException {
        SecondaryS3Scanner s3Scanner = new SecondaryS3Scanner(DOMAIN, FILE_URL, LARGE_BUFFER_SIZE);
        String line;
        long startTime = System.nanoTime();
        while ((line = s3Scanner.getLine()) != null) {
            assertThat(line, is(scanner.nextLine()));
        }

        System.out.println("Time passed for large buffer: " + (System.nanoTime() - startTime)/Math.pow(10, 9) + " seconds");
    }

    @Test
    public void When_FileContentIsBeingLoadedWithSmallBuffer_Expect_ToContainTheSameData() throws IOException {
        SecondaryS3Scanner s3Scanner = new SecondaryS3Scanner(DOMAIN, FILE_URL, SMALL_BUFFER_SIZE);
        String line;
        long startTime = System.nanoTime();
        while ((line = s3Scanner.getLine()) != null) {
            assertThat(line, is(scanner.nextLine()));
        }

        System.out.println("Time passed for small buffer: " + (System.nanoTime() - startTime)/Math.pow(10, 9) + " seconds");
    }

    @Test
    public void When_FileContentIsBeingLoadedWithOne_ByteBuffer_Expect_ToContainTheSameData() throws IOException {
        SecondaryS3Scanner s3Scanner = new SecondaryS3Scanner(DOMAIN, FILE_URL, ONE_BYTE_BUFFER_SIZE);
        String line;
        long startTime = System.nanoTime();
        while ((line = s3Scanner.getLine()) != null) {
            assertThat(line, is(scanner.nextLine()));
        }

        System.out.println("Time passed for one byte buffer: " + (System.nanoTime() - startTime)/Math.pow(10, 9) + " seconds");
    }
}
