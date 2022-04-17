package com.company.stream;

import com.company.utils.CloudStorageService;
import com.google.cloud.storage.Storage;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.integration.annotation.ServiceActivator;
import org.springframework.integration.file.FileHeaders;
import org.springframework.messaging.Message;
import org.springframework.stereotype.Service;

@Service
@Slf4j
public class GCSStreaming {
    @Autowired
    Storage gcs;

    @Autowired
    CloudStorageService cloudStorageService;

    @Value("${gcs.input.bucket}")
    private String gcsBucket;

    @ServiceActivator(inputChannel = "streamChannel")
    public void readFile(Message<?> message) throws Exception {
        String filePath = message.getHeaders().get(FileHeaders.REMOTE_FILE, String.class);

        log.info("*******FILE PATH " + filePath);

        String s = cloudStorageService.getContentAsText("gs://" + gcsBucket + "/" + filePath);

        log.info("*******FILE CONTENT " + s);
    }
}
