package org.springframework.data;

import javax.sql.DataSource;

import org.apache.commons.dbcp.BasicDataSource;
import org.springframework.batch.core.launch.support.SimpleJobLauncher;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.transaction.PlatformTransactionManager;

@Configuration
public class ExampleConfiguration {

	@Value("${batch.jdbc.driver}")
	private String driverClassName;

	@Value("${batch.jdbc.url}")
	private String driverUrl;

	@Value("${batch.jdbc.user}")
	private String driverUsername;

	@Value("${batch.jdbc.password}")
	private String driverPassword;

	@Autowired
	@Qualifier("jobRepository")
	private JobRepository jobRepository;

	@Bean
	public DataSource dataSource() {
		BasicDataSource dataSource = new BasicDataSource();
		dataSource.setDriverClassName(driverClassName);
		dataSource.setUrl(driverUrl);
		dataSource.setUsername(driverUsername);
		dataSource.setPassword(driverPassword);
		return dataSource;
	}

	@Bean
	public SimpleJobLauncher jobLauncher() {
		SimpleJobLauncher jobLauncher = new SimpleJobLauncher();
		jobLauncher.setJobRepository(jobRepository);
		return jobLauncher;
	}

	@Bean
	public PlatformTransactionManager transactionManager() {
		return new DataSourceTransactionManager(dataSource());
	}

}
