package com.miketools.log4j2.appenders;

import java.io.IOException;
import java.net.URISyntaxException;
import java.security.InvalidKeyException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.TimeZone;

import org.apache.logging.log4j.core.appender.AbstractManager;
import com.microsoft.azure.storage.*;
import com.microsoft.azure.storage.blob.*;

public class AzureBlobManager extends AbstractManager {
	
	private final String storageConnectionString;
	private final CloudStorageAccount cStorageAccount;
	private final CloudBlobContainer bContainer;
	private final CloudBlobClient blobClient;
	
	public AzureBlobManager(String name, String storageAccount, String accountKey, String container, String endpointSuffix) throws InvalidKeyException, URISyntaxException, StorageException {
		super(null, name);
		
		storageConnectionString = "DefaultEndpointsProtocol=https;AccountName=" + storageAccount + ";AccountKey=" + accountKey + ((endpointSuffix != null && endpointSuffix.trim().length() > 0) ? (";EndpointSuffix=" + endpointSuffix.trim()) : "");
		cStorageAccount = CloudStorageAccount.parse(storageConnectionString);
		blobClient = cStorageAccount.createCloudBlobClient();
		bContainer = blobClient.getContainerReference(container);
		bContainer.createIfNotExists();
	}
	
	public AzureBlobManager(String name, String storageAccount, String accountKey, String container) throws InvalidKeyException, URISyntaxException, StorageException {
		this(name, storageAccount, accountKey, container, "");
	}
	
	public synchronized void write(byte[] message) throws URISyntaxException, StorageException, IOException, InterruptedException {
		
		Calendar cal = Calendar.getInstance();
		cal.add(Calendar.DATE, 1);
		SimpleDateFormat format1 = new SimpleDateFormat("yyyyMMdd");
		format1.setTimeZone(TimeZone.getTimeZone("UTC"));
		String formatted = super.getName() + format1.format(cal.getTime()) + ".log";
		
		CloudAppendBlob blob = bContainer.getAppendBlobReference(formatted);
		if(!blob.exists()) {
			blob.createOrReplace();
			// Probably worth writing headers here :)
		}
		
		int i = 0;
		boolean done = false;
		StorageException lastException = null;
		while(i < 3 && !done) {
			try {
				blob.appendFromByteArray(message, 0, message.length);
				done = true;
			}
			catch(StorageException ex) {
				lastException = ex;
				if(ex.getHttpStatusCode() == 404) {
					if(!blob.exists()) {
						blob.createOrReplace();
					}
					Thread.sleep(2000 + (i*2000));
				}
			}
			i++;
		}
		
		if(i == 3)
			throw lastException;
	}
}
