package com.company.utils;

import com.google.cloud.storage.contrib.nio.CloudStorageFileSystem;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.List;
import java.util.stream.Collectors;

@Slf4j
@AllArgsConstructor
@Service
public class CloudStorageService {

	public String getContentAsText(String uriPath, String bucket) throws Exception {
		log.debug("Reading file ==> " + uriPath);
		URI uri = URI.create(uriPath);
		CloudStorageFileSystem fs = CloudStorageFileSystem.forBucket(bucket);
		uriPath = uri.getPath().substring(1);
		Path path = fs.getPath(uriPath);
		return Files.readString(path);
	}

	public List<String> listFiles(String uriPath) throws Exception {
		log.debug("List files at location " + uriPath);
		URI uri = URI.create(uriPath);
		CloudStorageFileSystem fs = CloudStorageFileSystem.forBucket(uri.getHost());
		uriPath = uri.getPath().substring(1);
		Path path = fs.getPath(uriPath);
		return Files.walk(path).map(Path::toString).collect(Collectors.toList());
	}

	public void moveFile(String srcUriPath, String targetUriPath) throws IOException {
		log.debug("Moving file from --> " + srcUriPath + " -- to location --> " + targetUriPath );
		URI srcUri = URI.create(srcUriPath);
		CloudStorageFileSystem fs = CloudStorageFileSystem.forBucket(srcUri.getHost());
		srcUriPath = srcUri.getPath().substring(1);
		Path srcPath = fs.getPath(srcUriPath);

		URI targetUri = URI.create(targetUriPath);
		targetUriPath = targetUri.getPath().substring(1);
		Path targetPath = fs.getPath(targetUriPath);

		Files.move(srcPath, targetPath, StandardCopyOption.REPLACE_EXISTING);
		log.debug("Files moved!");
	}

}
