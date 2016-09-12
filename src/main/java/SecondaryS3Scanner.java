import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.util.IOUtils;
import com.google.common.primitives.Bytes;
import org.apache.commons.lang3.ArrayUtils;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.Queue;

public class SecondaryS3Scanner {
    private long size;
    private long pointer = 0;
    private long chunkSize;
    private String bucket;
    private String key;
    private AmazonS3Client amazonS3Client = new AmazonS3Client();

    public SecondaryS3Scanner(String bucket, String key, long chunkSize) {
        ObjectMetadata objectMetadata = amazonS3Client.getObjectMetadata(bucket, key);
        size = objectMetadata.getContentLength();
        this.chunkSize = chunkSize;
        this.bucket = bucket;
        this.key = key;
    }

    ByteArrayOutputStream buffer = new ByteArrayOutputStream();
    Queue<String> lines = new LinkedList<>();

    public String getLine() throws IOException {
        if (!lines.isEmpty()) {
            return lines.poll();
        }

        if (containsLineSeparator(buffer.toByteArray())) {
            getAllLinesFromBuffer();
            return lines.poll();
        }

        while (pointer != size) {
            buffer.write(downloadContent());
            if (containsLineSeparator(buffer.toByteArray())) {
                getAllLinesFromBuffer();
                return lines.poll();
            }

            if (pointer - 1 == size) {
                return new String(buffer.toByteArray());
            }
        }
        return null;
    }

    private void getAllLinesFromBuffer() throws IOException {
        byte[] bytes = buffer.toByteArray();
        ArrayUtils.reverse(bytes);

        byte[] rest = Arrays.copyOfRange(bytes, 0, Bytes.indexOf(bytes, reversedSeparatorBytes));
        byte[] content = Arrays.copyOfRange(bytes, Bytes.indexOf(bytes, reversedSeparatorBytes) + reversedSeparatorBytes.length, bytes.length);
        ArrayUtils.reverse(content);
        ArrayUtils.reverse(rest);
        lines = new LinkedList<>(Arrays.asList(new String(content).split(System.lineSeparator())));
        buffer = new ByteArrayOutputStream();
        buffer.write(rest);
    }

    private byte[] downloadContent() throws IOException {
        GetObjectRequest objectRequest = new GetObjectRequest(bucket, key);
        objectRequest.setRange(pointer, Long.min(pointer + chunkSize, size) - 1);
        S3Object s3Object = amazonS3Client.getObject(objectRequest);
        pointer = Long.min(pointer + chunkSize, size);
        return IOUtils.toByteArray(s3Object.getObjectContent());
    }

    static final byte[] lineSeparatorBytes = System.lineSeparator().getBytes();
    static final byte[] reversedSeparatorBytes = lineSeparatorBytes;
    {
        ArrayUtils.reverse(reversedSeparatorBytes);
    }

    private boolean containsLineSeparator(byte[] array) {
        return array != null && Bytes.indexOf(array, lineSeparatorBytes) >= 0;
    }
}
