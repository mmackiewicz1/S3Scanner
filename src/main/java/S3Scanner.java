import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.GetObjectMetadataRequest;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.util.IOUtils;
import org.apache.commons.lang3.ArrayUtils;

import java.io.IOException;
import java.io.InputStream;
import java.util.LinkedList;
import java.util.Queue;

public class S3Scanner implements DataByLineReader {
    private final AmazonS3 amazonS3;
    private final GetObjectRequest request;
    private final long bufferSize;

    private boolean searching = true;
    private long bufferOffset;
    private long maximumRange;
    private byte[] bytes = new byte[0];
    private Queue<String> queue = new LinkedList<>();
    private Queue<Byte> byteList = new LinkedList<>();

    public S3Scanner(AmazonS3 amazonS3, String domain, String fileUrl, long bufferSize) {
        this.amazonS3 = amazonS3;
        this.bufferSize = bufferSize;
        request = new GetObjectRequest(domain, fileUrl);
        maximumRange = amazonS3.getObjectMetadata(new GetObjectMetadataRequest(domain, fileUrl)).getContentLength();
    }

    public String getLine() throws IOException {
        if (queue.isEmpty() && maximumRange > bufferOffset) {
            searching = true;
            getData();
        }

        if (!queue.isEmpty()) {
            return queue.poll();
        }

        return null;
    }

    private void getData() throws IOException {
        while (searching) {
            if (maximumRange > bufferOffset + bufferSize) {
                request.setRange(bufferOffset, bufferOffset + bufferSize - 1);
            } else if (maximumRange <= bufferOffset + bufferSize) {
                request.setRange(bufferOffset, maximumRange);
            }

            bufferOffset = bufferOffset + bufferSize;

            try (InputStream inputStream = amazonS3.getObject(request).getObjectContent()) {
                bytes = ArrayUtils.addAll(bytes, IOUtils.toByteArray(inputStream));
            }

            if (new String(bytes).contains(System.lineSeparator())) {
                commenceByteRowAddition();
            }
        }
    }

    private void commenceByteRowAddition() {
        for (byte characterByte : bytes) {
            if (hasLineSeparator(characterByte)) {
                convertAndAddRow(byteList);
            } else {
                byteList.add(characterByte);
            }
        }

        bytes = new byte[0];
        searching = false;
    }

    private void convertAndAddRow(Queue<Byte> byteList) {
        byte[] byteValues = new byte[byteList.size()];

        for (int i = 0; i < byteValues.length; i++) {
            byteValues[i] = byteList.poll();
        }

        queue.add(new String(byteValues));
    }

    private boolean hasLineSeparator(byte characterByte) {
        for (byte separatorByte : System.lineSeparator().getBytes()) {
            if (characterByte == separatorByte) {
                return true;
            }
        }

        return false;
    }
}
