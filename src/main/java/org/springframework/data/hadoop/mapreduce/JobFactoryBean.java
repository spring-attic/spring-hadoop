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
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
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
import org.springframework.beans.factory.BeanNameAware;
import org.springframework.beans.factory.FactoryBean;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.core.io.Resource;
import org.springframework.core.io.support.ResourcePatternResolver;
import org.springframework.data.hadoop.fs.HdfsResourceLoader;
import org.springframework.util.Assert;
import org.springframework.util.CollectionUtils;
import org.springframework.util.StringUtils;

/**
 * Factory bean for creating Hadoop jobs.
 * 
 * @author Costin Leau
 */
// TODO: extract input/output format configs
public class JobFactoryBean implements InitializingBean, FactoryBean<Job>, BeanNameAware {

	private Job job;
	private Configuration configuration;

	private String name;

	private Class<?> keyClass;
	private Class<?> valueClass;

	private Class<?> mapKeyClass;
	private Class<?> mapValueClass;

	private Class<? extends Mapper> mapper;
	private Class<? extends Reducer> reducer;
	private Class<? extends Reducer> combiner;
	private Class<? extends InputFormat> inputFormat;
	private Class<? extends OutputFormat> outputFormat;
	private Class<? extends Partitioner> partitioner;
	private Class<? extends RawComparator> sortComparator;
	private Class<? extends RawComparator> groupingComparator;

	private String workingDir;
	private Integer numReduceTasks;
	private Boolean userClassPrecendence;

	private Class<?> jarClass;
	private Resource jar;

	private List<String> inputPaths;
	private String outputPath;
	private Boolean compressOutput;
	private Class<? extends CompressionCodec> codecClass;
	private Boolean validatePaths = Boolean.FALSE;

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

	public void afterPropertiesSet() throws Exception {
		job = (configuration != null ? new Job(configuration) : new Job());


		// set first to enable auto-detection of K/V to skip the key/value types to be specified
		if (mapper != null) {
			job.setMapperClass(mapper);
			configureMapperTypesIfPossible(job, mapper);
		}

		if (reducer != null) {
			job.setReducerClass(reducer);
			configureReducerTypesIfPossible(job, reducer);
		}


		if (StringUtils.hasText(name)) {
			job.setJobName(name);
		}
		if (combiner != null) {
			job.setCombinerClass(combiner);
		}
		if (groupingComparator != null) {
			job.setGroupingComparatorClass(groupingComparator);
		}
		if (inputFormat != null) {
			job.setInputFormatClass(inputFormat);
		}
		if (mapKeyClass != null) {
			job.setMapOutputKeyClass(mapKeyClass);
		}
		if (mapValueClass != null) {
			job.setMapOutputValueClass(mapValueClass);
		}
		if (numReduceTasks != null) {
			job.setNumReduceTasks(numReduceTasks);
		}
		if (keyClass != null) {
			job.setOutputKeyClass(keyClass);
		}
		if (valueClass != null) {
			job.setOutputValueClass(valueClass);
		}
		if (outputFormat != null) {
			job.setOutputFormatClass(outputFormat);
		}
		if (partitioner != null) {
			job.setPartitionerClass(partitioner);
		}
		if (sortComparator != null) {
			job.setSortComparatorClass(sortComparator);
		}
		if (StringUtils.hasText(workingDir)) {
			job.setWorkingDirectory(new Path(workingDir));
		}
		if (jarClass != null) {
			job.setJarByClass(jarClass);
		}
		if (jar != null) {
			JobConf conf = (JobConf) job.getConfiguration();
			conf.setJar(jar.getURI().toString());
		}

		ResourcePatternResolver rl = new HdfsResourceLoader(FileSystem.get(job.getConfiguration()));

		if (!CollectionUtils.isEmpty(inputPaths)) {
			for (String path : inputPaths) {
				FileInputFormat.addInputPath(job, validatePaths(path, rl, true));
			}
		}

		if (StringUtils.hasText(outputPath)) {
			FileOutputFormat.setOutputPath(job, validatePaths(outputPath, rl, false));
		}

		if (compressOutput != null) {
			FileOutputFormat.setCompressOutput(job, compressOutput);
		}

		if (codecClass != null) {
			FileOutputFormat.setOutputCompressorClass(job, codecClass);
		}

		processJob(job);
	}

	private void configureMapperTypesIfPossible(Job j, Class<? extends Mapper> mapper) {
		// Find mapper
		Class<?> targetClass = mapper;
		Type targetType = mapper;

		do {
			targetType = targetClass.getGenericSuperclass();
			targetClass = targetClass.getSuperclass();
		} while (targetClass != null && targetClass != Object.class && Mapper.class.equals(targetType));


		if (targetType instanceof ParameterizedType) {
			Type[] params = ((ParameterizedType) targetType).getActualTypeArguments();
			if (params.length == 4) {
				// set each param (if possible);
				if (params[2] instanceof Class) {
					Class clz = (Class) params[2];
					if (!clz.isInterface())
						j.setMapOutputKeyClass(clz);
				}

				// set each param (if possible);
				if (params[3] instanceof Class) {
					Class clz = (Class) params[3];
					if (!clz.isInterface()) {
						j.setMapOutputValueClass(clz);
					}
				}
			}
		}
	}

	private void configureReducerTypesIfPossible(Job j, Class<? extends Reducer> reducer) {
		// don't do anything yet
	}

	private Path validatePaths(String path, ResourcePatternResolver resourceLoader, boolean shouldExist)
			throws IOException {
		if (Boolean.TRUE.equals(validatePaths)) {
			Resource res = resourceLoader.getResource(path);

			if (shouldExist) {
				Assert.isTrue(res.exists(), "The input path [" + path + "] does not exist");
			}
			else {
				Assert.isTrue(!res.exists(), "The output path [" + path + "] already exists");
			}
		}

		return new Path(path);
	}

	protected void processJob(Job job) throws Exception {
		// no-op
	}

	/**
	 * @param configuration The configuration to set.
	 */
	public void setConfiguration(Configuration configuration) {
		this.configuration = configuration;
	}

	/**
	 * @param name The name to set.
	 */
	public void setName(String name) {
		this.name = name;
	}

	/**
	 * @param keyClass The keyClass to set.
	 */
	public void setKey(Class<?> keyClass) {
		this.keyClass = keyClass;
	}

	/**
	 * @param valueClass The valueClass to set.
	 */
	public void setValue(Class<?> valueClass) {
		this.valueClass = valueClass;
	}

	/**
	 * @param mapKeyClass The mapKeyClass to set.
	 */
	public void setMapKey(Class<?> mapKeyClass) {
		this.mapKeyClass = mapKeyClass;
	}

	/**
	 * @param mapValueClass The mapValueClass to set.
	 */
	public void setMapValue(Class<?> mapValueClass) {
		this.mapValueClass = mapValueClass;
	}

	/**
	 * @param mapper The mapper to set.
	 */
	public void setMapper(Class<? extends Mapper> mapper) {
		this.mapper = mapper;
	}

	/**
	 * @param reducer The reducer to set.
	 */
	public void setReducer(Class<? extends Reducer> reducer) {
		this.reducer = reducer;
	}

	/**
	 * @param combiner The combiner to set.
	 */
	public void setCombiner(Class<? extends Reducer> combiner) {
		this.combiner = combiner;
	}

	/**
	 * @param inputFormat The inputFormat to set.
	 */
	public void setInputFormat(Class<? extends InputFormat> inputFormat) {
		this.inputFormat = inputFormat;
	}

	/**
	 * @param outputFormat The outputFormat to set.
	 */
	public void setOutputFormat(Class<? extends OutputFormat> outputFormat) {
		this.outputFormat = outputFormat;
	}

	/**
	 * @param partitioner The partitioner to set.
	 */
	public void setPartitioner(Class<? extends Partitioner> partitioner) {
		this.partitioner = partitioner;
	}

	/**
	 * @param sortComparator The sortComparator to set.
	 */
	public void setSortComparator(Class<? extends RawComparator> sortComparator) {
		this.sortComparator = sortComparator;
	}

	/**
	 * @param groupingComparator The groupingComparator to set.
	 */
	public void setGroupingComparator(Class<? extends RawComparator> groupingComparator) {
		this.groupingComparator = groupingComparator;
	}

	/**
	 * @param workingDir The workingDir to set.
	 */
	public void setWorkingDir(String workingDir) {
		this.workingDir = workingDir;
	}

	/**
	 * @param numReduceTasks The numReduceTasks to set.
	 */
	public void setNumReduceTasks(Integer numReduceTasks) {
		this.numReduceTasks = numReduceTasks;
	}

	/**
	 * @param jarClass The jarClass to set.
	 */
	public void setJarClass(Class<?> jarClass) {
		this.jarClass = jarClass;
	}

	/**
	 * @param jar The jar to set.
	 */
	public void setJar(Resource jar) {
		this.jar = jar;
	}

	/**
	 * 
	 * @param inputPath
	 */
	public void setInputPath(String inputPath) {
		this.inputPaths = new ArrayList<String>(1);
		inputPaths.add(inputPath);
	}

	/**
	 * @param inputPaths The inputPaths to set.
	 */
	public void setInputPaths(List<String> inputPaths) {
		this.inputPaths = inputPaths;
	}

	/**
	 * @param outputPath The outputPath to set.
	 */
	public void setOutputPath(String outputPath) {
		this.outputPath = outputPath;
	}

	/**
	 * @param compressOutput The compressOutput to set.
	 */
	public void setCompressOutput(Boolean compressOutput) {
		this.compressOutput = compressOutput;
	}

	/**
	 * @param codecClass The codecClass to set.
	 */
	public void setCodec(Class<? extends CompressionCodec> codecClass) {
		this.codecClass = codecClass;
	}

	/**
	 * @param validatePaths The validatePaths to set.
	 */
	public void setValidatePaths(Boolean validatePaths) {
		this.validatePaths = validatePaths;
	}
}