package com.company.stream;

import com.company.vo.Employee;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.cloud.storage.*;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.integration.annotation.ServiceActivator;
import org.springframework.integration.file.FileHeaders;
import org.springframework.messaging.Message;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.WritableByteChannel;
import java.nio.charset.StandardCharsets;

@Service
@Slf4j
public class GCSStreaming {
    @Autowired
    Storage gcs;

    @Value("${gcs.input.bucket}")
    private String gcsBucket;

    @Value("${gcs.projectId}")
    private String projectId;

    Storage storage = StorageOptions.newBuilder().setProjectId(projectId).build().getService();

    @ServiceActivator(inputChannel = "streamChannel")
    public void filePoller(Message<?> message) throws Exception {
        String filePath = message.getHeaders().get(FileHeaders.REMOTE_FILE, String.class);
        log.info("File recieved as message ==> " + filePath);

        try {
            //Move to porocessing
            String targetFilePath = move_file(filePath, filePath.replace("incoming", "processing"));
            String result = processFile(targetFilePath);

            //Write output file
            write_file(result, filePath.replace("incoming", "result"));

            //Move to success
            move_file(targetFilePath, filePath.replace("incoming", "success"));
        } catch (Exception e) {
            log.error("Exception --> ", e);
            //move to error
            move_file(filePath.replace("incoming", "processing"), filePath.replace("incoming", "error"));
        }
    }

    public String move_file(String srcFilePath, String targetFilePath) {
        Blob blob = storage.get(gcsBucket, srcFilePath);
        CopyWriter copyWriter = blob.copyTo(gcsBucket, targetFilePath);
        Blob copiedBlob = copyWriter.getResult();
        blob.delete();

        log.info("Moved object "
                + srcFilePath
                + " from bucket "
                + gcsBucket
                + " to "
                + targetFilePath
                + " in bucket "
                + copiedBlob.getBucket());
        return targetFilePath;
    }

    public void write_file(String content, String targetFilePath) {
        BlobId blobId = BlobId.of(gcsBucket, targetFilePath);
        BlobInfo blobInfo = BlobInfo.newBuilder(blobId).setContentType("text/plain").build();
        storage.create(blobInfo, content.getBytes(StandardCharsets.UTF_8));

        log.info("Files written "
                + targetFilePath
                + " in bucket "
                + gcsBucket);
    }

    public String processFile(String filePath) throws Exception {
        Blob blob = storage.get(gcsBucket, filePath);
        String fileContent = new String(blob.getContent());
        log.debug("filecontent == " + fileContent);

        ObjectMapper om = new ObjectMapper();
        Employee e = om.readValue(fileContent, Employee.class);

        String json = om.writeValueAsString(applyTransformation(e));
        return json;
    }

    public Employee applyTransformation(Employee e) {
        if(Integer.parseInt(e.getPeriod_of_service()) < 5) {
            if(Integer.parseInt(e.getRating()) > 4 && Integer.parseInt(e.getRating()) <= 5)
                e.setBonus(Double.parseDouble(e.getSalary()) * 1.10);
            else if(Integer.parseInt(e.getRating()) > 3 && Integer.parseInt(e.getRating()) <= 4)
                e.setBonus(Double.parseDouble(e.getSalary()) * 1);
            else if(Integer.parseInt(e.getRating()) > 2 && Integer.parseInt(e.getRating()) <= 3)
                e.setBonus(Double.parseDouble(e.getSalary()) * 0.90);
            else if(Integer.parseInt(e.getRating()) <= 2)
                e.setBonus(Double.parseDouble(e.getSalary()) * 0.80);
        } else {
            if(Integer.parseInt(e.getRating()) > 4 && Integer.parseInt(e.getRating()) <= 5)
                e.setBonus(Double.parseDouble(e.getSalary()) * 1.25);
            else if(Integer.parseInt(e.getRating()) > 3 && Integer.parseInt(e.getRating()) <= 4)
                e.setBonus(Double.parseDouble(e.getSalary()) * 1.15);
            else if(Integer.parseInt(e.getRating()) > 2 && Integer.parseInt(e.getRating()) <= 3)
                e.setBonus(Double.parseDouble(e.getSalary()) * 1.05);
            else if(Integer.parseInt(e.getRating()) <= 2)
                e.setBonus(Double.parseDouble(e.getSalary()) * 1);
        }
        return e;
    }
}
