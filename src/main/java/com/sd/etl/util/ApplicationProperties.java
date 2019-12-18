package com.sd.etl.util;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SparkSession;

public class ApplicationProperties {

	private static volatile ApplicationProperties instance;

	private ClassLoader classLoader;
	private String inputKeyspace;
	private String outputKeyspace;
	private SparkSession sparkSession;
	private String profile;
	private boolean dryRun;

	private ApplicationProperties() {
		this.classLoader = this.getClass().getClassLoader();
	}

	public static ApplicationProperties getInstance() {
		if (instance == null) {
			instance = new ApplicationProperties();
			instance.setProfile();
			instance.getSparkSession();
		}
		return instance;
	}

	public SparkSession getSparkSession() {
		if (sparkSession == null) {
			try (InputStream input = this.classLoader.getResourceAsStream(getProfile().concat("-config.properties"))) {
				Properties profileProperties = new Properties();
				profileProperties.load(input);

				this.setInputKeyspace(profileProperties.getProperty("cassandra.keyspace.input"));
				this.setOutputKeyspace(profileProperties.getProperty("cassandra.keyspace.output"));
				this.setDryRun("true".equals(profileProperties.getProperty("dryrun")));
				this.initSparkSessionByProfile(profileProperties);

				return sparkSession;
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		return sparkSession;
	}

	public SQLContext getSqlContext() {
		return new SQLContext(this.getSparkSession());
	}

	public String getInputKeyspace() {
		return inputKeyspace;
	}

	public String getOutputKeyspace() {
		return outputKeyspace;
	}

	public String getFormat() {
		return "org.apache.spark.sql.cassandra";
	}

	public String getProfile() {
		return this.profile;
	}

	private ApplicationProperties setProfile() {
		try (InputStream input = this.classLoader.getResourceAsStream("config.properties")) {
			Properties applicationPoperties = new Properties();
			applicationPoperties.load(input);

			this.profile = applicationPoperties.getProperty("application.profile");
		} catch (IOException e) {
			e.printStackTrace();
		}
		return instance;
	}

	public void setInputKeyspace(String inputKeyspace) {
		this.inputKeyspace = inputKeyspace;
	}

	public void setOutputKeyspace(String outputKeyspace) {
		this.outputKeyspace = outputKeyspace;
	}

	public boolean isDryRun() {
		return dryRun;
	}

	public void setDryRun(boolean dryRun) {
		this.dryRun = dryRun;
	}
	
	private void initSparkSessionByProfile(Properties profileProperties) {
		if ("dev".equals(this.profile)) {
			this.sparkSession = SparkSession.builder().appName("App_name")
					.config("spark.cassandra.connection.ssl.enabled", profileProperties.getProperty("cassandra.ssl"))
					.config("spark.cassandra.connection.host", profileProperties.getProperty("cassandra.host"))
					.config("spark.cassandra.connection.port", profileProperties.getProperty("cassandra.port"))
					.config("spark.cassandra.auth.username", profileProperties.getProperty("cassandra.username"))
					.config("spark.cassandra.auth.password", profileProperties.getProperty("cassandra.password"))
					.getOrCreate();

		} else {
			this.sparkSession = SparkSession.builder().appName("App_name").master("local[*]")
					.config("spark.executor.memory", "2g")
				    .config("spark.driver.memory", "2g")
					.config("spark.cassandra.connection.ssl.enabled", profileProperties.getProperty("cassandra.ssl"))
					.config("spark.cassandra.connection.host", profileProperties.getProperty("cassandra.host"))
					.config("spark.cassandra.connection.port", profileProperties.getProperty("cassandra.port"))
					.config("spark.cassandra.auth.username", profileProperties.getProperty("cassandra.username"))
					.config("spark.cassandra.auth.password", profileProperties.getProperty("cassandra.password"))
					.config("spark.cassandra.output.consistency.level", "ANY")
					.getOrCreate();
		}
	}

}
