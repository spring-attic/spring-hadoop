package org.springframework.hadoop.test;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.jar.JarFile;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.ClusterStatus;
import org.apache.hadoop.mapred.JobClient;
import org.junit.Assume;
import org.junit.rules.TestWatchman;
import org.junit.runners.model.FrameworkMethod;
import org.springframework.hadoop.JobTemplate;

/**
 * <p>
 * A JUnit &#64;Rule that can be used to detect and configure a remote Hadoop
 * cluster, or fall back to running locally for a simple functional test.
 * </p>
 * 
 * <p>
 * Example usage
 * 
 * <pre>
 * &#064;Rule
 * public HadoopSetUp setUp = HadoopSetUp.preferClusterRunning(&quot;localhost&quot;, 9001);
 * 
 * private JobTemplate jobTemplate;
 * 
 * &#064;Before
 * public void init() throws Exception {
 * 	jobTemplate = new JobTemplate();
 * 	if (setUp.isClusterOnline()) {
 * 		setUp.setJarFile(&quot;target/my-test.jar&quot;);
 * 		jobTemplate.setExtraConfiguration(setUp.getExtraConfiguration());
 * 		setUp.copy(&quot;src/test/resources/input&quot;, &quot;target/input&quot;);
 * 	}
 * 	jobTemplate.setVerbose(true);
 * 	setUp.delete(&quot;target/output&quot;);
 * }
 * 
 * &#064;Test
 * public void testJob() throws Exception {
 * 	assertTrue(jobTemplate.run(JobConfiguration.class));
 * }
 * 
 * </pre>
 * 
 * </p>
 * 
 * @author Dave Syer
 * 
 */
public class HadoopSetUp extends TestWatchman {

	private static Log logger = LogFactory.getLog(HadoopSetUp.class);

	// Static so that we only test once on failure: speeds up test suite
	private static boolean clusterOnline = true;

	// Static so that we only test once on failure
	private static boolean clusterOffline = true;

	private final boolean assumeOnline;

	private final String hostname;

	private final int port;

	private JobTemplate jobTemplate = new JobTemplate();

	private String jarFile;

	/**
	 * Use this factory to run the job only on a remote cluster, and skip all
	 * tests if the cluster is not detected.
	 * 
	 * @return a new rule that assumes an existing running cluster
	 */
	public static HadoopSetUp ensureClusterRunning(String hostname, int port) {
		return new HadoopSetUp(hostname, port, true);
	}

	/**
	 * Detect a cluster with default parameters (job tracker at
	 * <code>locahost:9001</code>). Use this factory to run the job only on a
	 * remote cluster, and skip all tests if the cluster is not detected.
	 * 
	 * @return a new rule that assumes an existing running cluster
	 */
	public static HadoopSetUp ensureClusterRunning() {
		return new HadoopSetUp(true);
	}

	/**
	 * Use this factory method to prefer a cluster deployment, but fall back to
	 * local if the cluster is not running.
	 * 
	 * @return a new rule that prefers an existing running cluster
	 */
	public static HadoopSetUp preferClusterRunning(String hostname, int port) {
		return new HadoopSetUp(hostname, port, false);
	}

	/**
	 * Detect a cluster with default parameters (job tracker at
	 * <code>locahost:9001</code>). Use this factory method to prefer a cluster
	 * deployment, but fall back to local if the cluster is not running.
	 * 
	 * @return a new rule that prefers an existing running cluster
	 */
	public static HadoopSetUp preferClusterRunning() {
		return new HadoopSetUp(false);
	}

	/**
	 * @return a new rule that prefers an existing running cluster
	 */
	public static HadoopSetUp localOnly() {
		return new HadoopSetUp(null, 0, false);
	}

	/**
	 * @param jarFile the jar file path
	 */
	public void setJarFile(String jarFile) {
		this.jarFile = jarFile;
		jobTemplate.setJarFile(jarFile);
	}

	private HadoopSetUp(String hostname, int port, boolean assumeOnline) {
		this.hostname = hostname;
		this.port = port;
		this.assumeOnline = assumeOnline;
	}

	public String getHostname() {
		return clusterOnline ? hostname : null;
	}

	public int getPort() {
		return port;
	}

	public String getJarFile() {
		return jarFile;
	}

	public HadoopSetUp(boolean assumeOnline) {
		this("localhost", 9001, assumeOnline);
	}

	@Override
	public void starting(FrameworkMethod method) {
		init();
	}

	public void init() {

		if ((clusterOnline && !clusterOffline) || (clusterOffline && !clusterOnline)) {
			// We already tested and it's not there
			return;
		}
		
		if (hostname == null) {
			// nothing to do
			return;
		}

		// Check at the beginning, so this can be used as a static field
		if (assumeOnline) {
			Assume.assumeTrue(clusterOnline);
		}
		
		JobClient client = null;

		try {

			Configuration configuration = new Configuration();
			configuration.setInt("ipc.client.connect.max.retries", 1);
			client = new JobClient(new InetSocketAddress(hostname, port), configuration);
			ClusterStatus clusterStatus = client.getClusterStatus();
			if (clusterStatus.getUsedMemory() > 0) {
				clusterOffline = false;
				jarFile = buildJarFile();
				jobTemplate.setJarFile(jarFile);
				jobTemplate.setJobTrackerHostname(hostname);
				jobTemplate.setJobTrackerPort(port);
			}

		}
		catch (Exception e) {
			if (logger.isDebugEnabled()) {
				logger.warn("Basic connectivity test failed", e);
			}
			else {
				logger.warn("Basic connectivity test failed: " + e.getMessage());
			}
		}
		finally {
			if (client != null) {
				try {
					client.close();
				}
				catch (IOException e) {
					logger.warn("Could not close client", e);
				}
			}
		}

		if (clusterOffline) {
			clusterOnline = false;
			if (assumeOnline) {
				Assume.assumeTrue(clusterOnline);
			}
		}

		return;

	}

	/**
	 * @return true if the cluster is online
	 */
	public boolean isClusterOnline() {
		return clusterOnline;
	}

	/**
	 * @return the configuration
	 */
	public Properties getExtraConfiguration() {
		return jobTemplate.getExtraConfiguration();
	}

	/**
	 * @param directory
	 */
	public void delete(String directory) throws Exception {
		FileSystem.get(getConfiguration()).delete(new Path(directory), true);
	}

	/**
	 * @param directory
	 */
	public void copy(String directory) throws Exception {
		copy(directory, directory);
	}

	/**
	 * @param directory
	 */
	public void copy(String directory, String destination) throws Exception {
		if (!directory.equals(destination)) {
			delete(destination);
		}
		FileSystem.get(getConfiguration()).copyFromLocalFile(false, true, new Path(directory), new Path(destination));
	}

	/**
	 * @param zipFile
	 * @param destination
	 */
	public void unzip(String zipFile, String destination) throws IOException {
		File file = new File(destination);
		if (file.exists()) {
			FileUtil.fullyDelete(file);
		}
		FileUtil.unZip(new File(zipFile), file);
	}

	public Configuration getConfiguration() {
		Configuration configuration = new Configuration();
		for (Entry<Object, Object> entry : getExtraConfiguration().entrySet()) {
			configuration.set((String) entry.getKey(), (String) entry.getValue());
		}
		return configuration;
	}

	private String buildJarFile() {
		if (jarFile != null) {
			assertTrue("No jar file found at path provided: " + jarFile, new File(jarFile).exists());
			return jarFile;
		}
		String dir = "target";
		File jobJar = new File(dir, "job.jar");
		if (jobJar.exists()) {
			assertTrue("Could not delete dummy jar: " + jobJar, jobJar.delete());
		}
		File target = new File(dir);
		target.mkdirs();
		assertTrue("Could not create directory: " + dir, target.exists() && target.isDirectory());
		try {
			createJar(jobJar, target);
			assertTrue("Could not touch new jar file", jobJar.exists());
			assertTrue(new JarFile(jobJar).entries().hasMoreElements());
			return jobJar.getAbsolutePath();
		}
		catch (IOException e) {
			logger.error("Could not build jar", e);
			fail(e.getMessage());
			return null;
		}
	}

	private void createJar(File zipFile, File fileSource) throws IOException {

		FileOutputStream fout = new FileOutputStream(zipFile);
		ZipOutputStream zout = new ZipOutputStream(fout);
		addDirectory(zout, new File(fileSource, "classes"), "");
		addDirectory(zout, new File(fileSource, "test-classes"), "");
		zout.close();

	}

	private static void addDirectory(ZipOutputStream zout, File fileSource, String prefix) throws IOException {

		File[] files = fileSource.listFiles();
		for (int i = 0; i < files.length; i++) {
			if (files[i].isDirectory()) {
				addDirectory(zout, files[i], prefix + (prefix.endsWith("/") ? "" : "/") + files[i].getName() + "/");
				continue;
			}
			byte[] buffer = new byte[1024];
			FileInputStream fin = new FileInputStream(files[i]);
			zout.putNextEntry(new ZipEntry(prefix + files[i].getName()));
			int length;
			while ((length = fin.read(buffer)) > 0) {
				zout.write(buffer, 0, length);
			}
			zout.closeEntry();
			fin.close();
		}

	}

}
