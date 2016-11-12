package com.miketools.log4j2.appenders;

import java.io.IOException;
import java.net.URISyntaxException;
import java.security.InvalidKeyException;

import org.apache.logging.log4j.core.Filter;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.appender.AbstractAppender;
import org.apache.logging.log4j.core.config.plugins.Plugin;
import org.apache.logging.log4j.core.config.plugins.PluginBuilderAttribute;
import org.apache.logging.log4j.core.config.plugins.PluginBuilderFactory;
import org.apache.logging.log4j.core.config.plugins.PluginElement;
import org.apache.logging.log4j.core.config.plugins.validation.constraints.Required;
import org.apache.logging.log4j.core.layout.PatternLayout;

import com.microsoft.azure.storage.StorageException;

@Plugin(name="AzureBlob", category="Core", elementType="appender", printObject=true)
public final class AzureBlobAppender extends AbstractAppender {

	private final AzureBlobManager manager;
	
	protected AzureBlobAppender(String name, Filter filter, PatternLayout layout, String storageAccount, String accountKey, String container, String endpointSuffix) throws InvalidKeyException, URISyntaxException, StorageException {
		super(name, filter, layout);
		manager = new AzureBlobManager(name, storageAccount, accountKey, container, endpointSuffix);
	}
	
	@PluginBuilderFactory
	public static Builder newBuilder() {
		return new Builder().asBuilder();
	}

	@Override
	public void append(LogEvent event) {
		try {
			manager.write(getLayout().toByteArray(event));
		} catch (URISyntaxException | StorageException | IOException | InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	public static class Builder implements org.apache.logging.log4j.core.util.Builder<AzureBlobAppender>
	{
		@PluginBuilderAttribute
	    @Required(message="No name provided for AzureBlobAppender")
	    private String name;
		@PluginBuilderAttribute
	    @Required(message="No storageAccount provided for AzureBlobAppender")
	    private String storageAccount;
		@PluginBuilderAttribute
	    @Required(message="No accountKey provided for AzureBlobAppender")
	    private String accountKey;
		@PluginBuilderAttribute
	    @Required(message="No container provided for AzureBlobAppender")
	    private String container;
		@PluginBuilderAttribute
	    private String endpointSuffix;
		
		@PluginElement("Filter")
	    private Filter filter;
		
		@PluginElement("PatternLayout")
	    private PatternLayout layout;

		public Builder setName(String name) {
			this.name = name;
			return this;
		}
		public Builder setStorageAccount(String storageAccount) {
			this.storageAccount = storageAccount;
			return this;
		}
		public Builder setAccountKey(String accountKey) {
			this.accountKey = accountKey;
			return this;
		}
		public Builder setContainer(String container) {
			this.container = container;
			return this;
		}
		public Builder setEndpointSuffix(String endpointSuffix) {
			this.endpointSuffix = endpointSuffix;
			return this;
		}
		public Builder getFilter(Filter filter) {
			this.filter = filter;
			return this;
		}
		public Builder getLayout(PatternLayout layout) {
			this.layout = layout;
			return this;
		}
		
		@Override
		public AzureBlobAppender build() {
			AzureBlobAppender test = null;
			try {
				test = new AzureBlobAppender(this.name, this.filter, this.layout, this.storageAccount, this.accountKey, this.container, this.endpointSuffix);
			} catch (InvalidKeyException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (URISyntaxException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (StorageException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			return test;
		}
		
		public Builder asBuilder() {
			return this;
		}
	}
}
