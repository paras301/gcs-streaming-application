package com.company.application;

import com.google.cloud.spring.storage.integration.GcsRemoteFileTemplate;
import com.google.cloud.spring.storage.integration.GcsSessionFactory;
import com.google.cloud.spring.storage.integration.inbound.GcsStreamingMessageSource;
import com.google.cloud.storage.Storage;
import com.google.gson.Gson;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.integration.annotation.InboundChannelAdapter;
import org.springframework.integration.annotation.Poller;
import org.springframework.integration.core.MessageSource;

import java.io.InputStream;
import java.util.Arrays;
import java.util.stream.Collectors;

@Configuration
public class GcsSourceConfiguration {
    @Value("${gcs.input.bucket}")
    private String gcsBucket;

    @Value("${gcs.input.path}")
    private String path;

    @Value("${gcs.input.fileExtension}")
    private String fileExtension;

    @Bean
    public Gson gson() {
        return new Gson();
    }

    @Bean
    @InboundChannelAdapter(channel = "streamChannel", poller = @Poller(fixedDelay = "5000"))
    public MessageSource<InputStream> streamingAdapter(Storage gcs) {
        GcsStreamingMessageSource adapter =
                new GcsStreamingMessageSource(new GcsRemoteFileTemplate(new GcsSessionFactory(gcs)));
        adapter.setRemoteDirectory(gcsBucket);
        adapter.setFilter(files ->
                Arrays.stream(files)
                        .filter(f -> f.getName().contains(path))
                        .filter(f -> f.getName().endsWith(fileExtension))
                        .collect(Collectors.toUnmodifiableList())
        );
        return adapter;
    }
}
