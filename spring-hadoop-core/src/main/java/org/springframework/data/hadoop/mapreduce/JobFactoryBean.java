/*
 * Copyright 2011 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.springframework.data.hadoop.mapreduce;

import java.io.IOException;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.security.PrivilegedExceptionAction;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.security.UserGroupInformation;
import org.springframework.beans.factory.BeanClassLoaderAware;
import org.springframework.beans.factory.BeanNameAware;
import org.springframework.beans.factory.FactoryBean;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.core.io.Resource;
import org.springframework.core.io.support.ResourcePatternResolver;
import org.springframework.data.hadoop.configuration.JobConfUtils;
import org.springframework.util.Assert;
import org.springframework.util.ClassUtils;
import org.springframework.util.CollectionUtils;
import org.springframework.util.StringUtils;

/**
 * Factory bean for creating a Hadoop Map-Reduce job.
 * Note that the setters for the class access class names (String) instead of direct
 * classes. This is done on purpose in case the job itself and its dependencies are
 * on a jar not available on the classpath ({@link #setJar(Resource)}) in which case
 * a special, on-the-fly classloader is used.
 *
 * @author Costin Leau
 */
// TODO: extract input/output format configs
public class JobFactoryBean extends JobGenericOptions implements InitializingBean, FactoryBean<Job>, BeanNameAware,
		BeanClassLoaderAware {

	private Job job;
	private Configuration configuration;
	private Properties properties;

	private String name;

	private String key;
	private String value;

	private String mapKey;
	private String mapValue;

	private String mapper;
	private String reducer;
	private String combiner;
	private String inputFormat;
	private String outputFormat;
	private String partitioner;
	private String sortComparator;
	private String groupingComparator;

	private String workingDir;
	private Integer numReduceTasks;

	private Class<?> jarClass;
	private Resource jar;

	private List<String> inputPaths;
	private String outputPath;
	private Boolean compressOutput;
	private String codecClass;
	private ClassLoader beanClassLoader;

	@Override
	public void setBeanClassLoader(ClassLoader classLoader) {
		this.beanClassLoader = classLoader;
	}

	public void setBeanName(String name) {
		this.name = name;
	}

	public Job getObject() throws Exception {
		return job;
	}

	public Class<?> getObjectType() {
		return (job != null ? job.getClass() : Job.class);
	}

	public boolean isSingleton() {
		return true;
	}

	@SuppressWarnings({ "rawtypes", "deprecation" })
	public void afterPropertiesSet() throws Exception {
		final Configuration cfg = JobConfUtils.createFrom(configuration, properties);

		buildGenericOptions(cfg);

		if (StringUtils.hasText(user)) {
			UserGroupInformation ugi = UserGroupInformation.createProxyUser(user, UserGroupInformation.getLoginUser());
			ugi.doAs(new PrivilegedExceptionAction<Void>() {

				@Override
				public Void run() throws Exception {
					job = new Job(cfg);
					return null;
				}
			});
		}
		else {
			job = new Job(cfg);
		}

		ClassLoader loader = (beanClassLoader != null ? beanClassLoader : org.springframework.util.ClassUtils.getDefaultClassLoader());

		if (jar != null) {
			JobConf conf = (JobConf) job.getConfiguration();
			conf.setJar(jar.getURI().toString());
			loader = ExecutionUtils.createParentLastClassLoader(jar, beanClassLoader, cfg);
			conf.setClassLoader(loader);
		}


		// set first to enable auto-detection of K/V to skip the key/value types to be specified
		if (mapper != null) {
			Class<? extends Mapper> mapperClass = resolveClass(mapper, loader, Mapper.class);
			job.setMapperClass(mapperClass);
			configureMapperTypesIfPossible(job, mapperClass);
		}

		if (reducer != null) {
			Class<? extends Reducer> reducerClass = resolveClass(reducer, loader, Reducer.class);
			job.setReducerClass(reducerClass);
			configureReducerTypesIfPossible(job, reducerClass);
		}

		if (StringUtils.hasText(name)) {
			job.setJobName(name);
		}
		if (combiner != null) {
			job.setCombinerClass(resolveClass(combiner, loader, Reducer.class));
		}
		if (groupingComparator != null) {
			job.setGroupingComparatorClass(resolveClass(groupingComparator, loader, RawComparator.class));
		}
		if (inputFormat != null) {
			job.setInputFormatClass(resolveClass(inputFormat, loader, InputFormat.class));
		}
		if (mapKey != null) {
			job.setMapOutputKeyClass(resolveClass(mapKey, loader, Object.class));
		}
		if (mapValue != null) {
			job.setMapOutputValueClass(resolveClass(mapValue, loader, Object.class));
		}
		if (numReduceTasks != null) {
			job.setNumReduceTasks(numReduceTasks);
		}
		if (key != null) {
			job.setOutputKeyClass(resolveClass(key, loader, Object.class));
		}
		if (value != null) {
			job.setOutputValueClass(resolveClass(value, loader, Object.class));
		}
		if (outputFormat != null) {
			job.setOutputFormatClass(resolveClass(outputFormat, loader, OutputFormat.class));
		}
		if (partitioner != null) {
			job.setPartitionerClass(resolveClass(partitioner, loader, Partitioner.class));
		}
		if (sortComparator != null) {
			job.setSortComparatorClass(resolveClass(sortComparator, loader, RawComparator.class));
		}
		if (StringUtils.hasText(workingDir)) {
			job.setWorkingDirectory(new Path(workingDir));
		}
		if (jarClass != null) {
			job.setJarByClass(jarClass);
		}

		if (!CollectionUtils.isEmpty(inputPaths)) {
			for (String path : inputPaths) {
				FileInputFormat.addInputPath(job, new Path(path));
			}
		}

		if (StringUtils.hasText(outputPath)) {
			FileOutputFormat.setOutputPath(job, new Path(outputPath));
		}

		if (compressOutput != null) {
			FileOutputFormat.setCompressOutput(job, compressOutput);
		}

		if (codecClass != null) {
			FileOutputFormat.setOutputCompressorClass(job, resolveClass(codecClass, loader, CompressionCodec.class));
		}

		processJob(job);
	}

	@SuppressWarnings("unchecked")
	private <T> Class<? extends T> resolveClass(String className, ClassLoader cl, Class<T> type) {
		return (Class<? extends T>) ClassUtils.resolveClassName(className, cl);
	}

	private void configureMapperTypesIfPossible(Job j, @SuppressWarnings("rawtypes") Class<? extends Mapper> mapper) {
		// Find mapper
		Class<?> targetClass = mapper;
		Type targetType = mapper;

		do {
			targetType = targetClass.getGenericSuperclass();
			targetClass = targetClass.getSuperclass();
		} while (targetClass != null && targetClass != Object.class && !Mapper.class.equals(targetClass));


		if (targetType instanceof ParameterizedType) {
			Type[] params = ((ParameterizedType) targetType).getActualTypeArguments();
			if (params.length == 4) {
				// set each param (if possible);
				if (params[2] instanceof Class) {
					Class<?> clz = (Class<?>) params[2];
					if (!clz.isInterface())
						j.setMapOutputKeyClass(clz);
				}

				// set each param (if possible);
				if (params[3] instanceof Class) {
					Class<?> clz = (Class<?>) params[3];
					if (!clz.isInterface()) {
						j.setMapOutputValueClass(clz);
					}
				}
			}
		}
	}

	private void configureReducerTypesIfPossible(Job j, @SuppressWarnings("rawtypes") Class<? extends Reducer> reducer) {
		// don't do anything yet
	}

	@SuppressWarnings("unused")
	private void validatePaths(String path, ResourcePatternResolver resourceLoader, boolean shouldExist)
			throws IOException {
		Resource res = resourceLoader.getResource(path);

		if (shouldExist) {
			Assert.isTrue(res.exists(), "The input path [" + path + "] does not exist");
		}
		else {
			Assert.isTrue(!res.exists(), "The output path [" + path + "] already exists");
		}
	}

	protected void processJob(Job job) throws Exception {
		// no-op
	}

	/**
	 * Sets the Hadoop configuration to use.
	 *
	 * @param configuration The configuration to set.
	 */
	public void setConfiguration(Configuration configuration) {
		this.configuration = configuration;
	}

	/**
	 * Sets the job name.
	 *
	 * @param name The name to set.
	 */
	public void setName(String name) {
		this.name = name;
	}

	/**
	 * Sets the job key class.
	 *
	 * @param key The keyClass to set.
	 */
	public void setKey(String key) {
		this.key = key;
	}

	/**
	 * Sets the job value class.
	 *
	 * @param value The valueClass to set.
	 */
	public void setValue(String value) {
		this.value = value;
	}

	/**
	 * Sets the job map key class.
	 *
	 * @param mapKey The mapKeyClass to set.
	 */
	public void setMapKey(String mapKey) {
		this.mapKey = mapKey;
	}

	/**
	 * Sets the job map value class.
	 *
	 * @param mapValue The mapValueClass to set.
	 */
	public void setMapValue(String mapValue) {
		this.mapValue = mapValue;
	}

	/**
	 * Sets the job mapper class.
	 *
	 * @param mapper The mapper to set.
	 */
	public void setMapper(String mapper) {
		this.mapper = mapper;
	}

	/**
	 * Sets the job reducer class.
	 *
	 * @param reducer The reducer to set.
	 */
	public void setReducer(String reducer) {
		this.reducer = reducer;
	}

	/**
	 * Sets the job combiner class.
	 *
	 * @param combiner The combiner to set.
	 */
	public void setCombiner(String combiner) {
		this.combiner = combiner;
	}

	/**
	 * Sets the job input format class.
	 *
	 * @param inputFormat The inputFormat to set.
	 */
	public void setInputFormat(String inputFormat) {
		this.inputFormat = inputFormat;
	}

	/**
	 * Sets the job output format class.
	 *
	 * @param outputFormat The outputFormat to set.
	 */
	public void setOutputFormat(String outputFormat) {
		this.outputFormat = outputFormat;
	}

	/**
	 * Sets the job partitioner class.
	 *
	 * @param partitioner The partitioner to set.
	 */
	public void setPartitioner(String partitioner) {
		this.partitioner = partitioner;
	}

	/**
	 * Sets the job sort comparator class.
	 *
	 * @param sortComparator The sortComparator to set.
	 */
	public void setSortComparator(String sortComparator) {
		this.sortComparator = sortComparator;
	}

	/**
	 * Sets the job grouping comparator class.
	 *
	 * @param groupingComparator The groupingComparator to set.
	 */
	public void setGroupingComparator(String groupingComparator) {
		this.groupingComparator = groupingComparator;
	}

	/**
	 * Sets the job working directory.
	 *
	 * @param workingDir The workingDir to set.
	 */
	public void setWorkingDir(String workingDir) {
		this.workingDir = workingDir;
	}

	/**
	 * Sets the number of reduce task for this job.
	 *
	 * @param numReduceTasks The numReduceTasks to set.
	 */
	public void setNumberReducers(Integer numReduceTasks) {
		this.numReduceTasks = numReduceTasks;
	}

	/**
	 * Determines the job jar (available on the classpath) based on the given class.
	 *
	 * @param jarClass The jarClass to set.
	 */
	public void setJarByClass(Class<?> jarClass) {
		this.jarClass = jarClass;
	}

	/**
	 * Sets the job jar (which might not be on the classpath).
	 *
	 * @param jar The jar to set.
	 */
	public void setJar(Resource jar) {
		this.jar = jar;
	}

	/**
	 * Sets the job input path.
	 *
	 * @param inputPath job input path.
	 */
	public void setInputPath(String... inputPath) {
		// handle , strings here instead of the namespace to allow SpEL to kick in (if needed)
		if (inputPath != null && inputPath.length == 1) {
			inputPath = StringUtils.commaDelimitedListToStringArray(inputPath[0]);
		}
		this.inputPaths = Arrays.asList(inputPath);
	}

	/**
	 * Sets the job output path.
	 *
	 * @param outputPath The outputPath to set.
	 */
	public void setOutputPath(String outputPath) {
		this.outputPath = outputPath;
	}

	/**
	 * Indicates whether the job output should be compressed or not.
	 *
	 * @param compressOutput The compressOutput to set.
	 */
	public void setCompressOutput(Boolean compressOutput) {
		this.compressOutput = compressOutput;
	}

	/**
	 * Sets the job codec class.
	 *
	 * @param codecClass The codecClass to set.
	 */
	public void setCodec(String codecClass) {
		this.codecClass = codecClass;
	}

	/**
	 * The configuration properties to set for this job.
	 *
	 * @param properties The properties to set.
	 */
	public void setProperties(Properties properties) {
		this.properties = properties;
	}
}