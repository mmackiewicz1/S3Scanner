import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.GetObjectMetadataRequest;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.util.IOUtils;

import java.io.IOException;
import java.io.InputStream;
import java.util.LinkedList;
import java.util.Queue;

public class S3Scanner implements DataByLineReader {
    private final AmazonS3 amazonS3;
    private final GetObjectRequest request;
    private final long bufferSize;

    private boolean searching = true;
    private boolean lastRow = false;
    private long bufferOffset;
    private long maximumRange;
    private String receivedTextData = "";
    private String remainingTextData = "";
    private Queue<String> queue = new LinkedList<>();

    public S3Scanner(AmazonS3 amazonS3, String domain, String fileUrl, long bufferSize) {
        this.amazonS3 = amazonS3;
        this.bufferSize = bufferSize;
        request = new GetObjectRequest(domain, fileUrl);
        maximumRange = amazonS3.getObjectMetadata(new GetObjectMetadataRequest(domain, fileUrl)).getContentLength();
    }

    public String getLine() throws IOException {
        if (queue.isEmpty() && maximumRange >= bufferOffset) {
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
            } else {
                lastRow = true;
                request.setRange(bufferOffset, maximumRange);
            }

            bufferOffset = bufferOffset + bufferSize;

            try (InputStream inputStream = amazonS3.getObject(request).getObjectContent()) {
                remainingTextData = receivedTextData = IOUtils.toString(inputStream);
            }

            if (receivedTextData.contains(System.lineSeparator())) {
                commenceRowAddition(receivedTextData.split(System.lineSeparator()));
            }
        }
    }

    private void commenceRowAddition(String[] rows) {
        if (rows.length == 1) {
            queue.add(remainingTextData + rows[0]);
        } else {
            addDataFromRows(rows);

            if (lastRow) {
                queue.add(remainingTextData);
            }
        }

        if (receivedTextData.endsWith(System.lineSeparator())) {
            remainingTextData = "";
        }

        receivedTextData = "";
        searching = false;
    }

    private void addDataFromRows(String[] rows) {
        for (int j = 0; j < rows.length - 1; j++) {
            if (j == 0) {
                queue.add(remainingTextData + rows[j]);
            } else {
                queue.add(rows[j]);
            }
        }

        remainingTextData = rows[rows.length - 1];
    }
}
