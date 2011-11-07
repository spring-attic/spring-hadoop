/*
 * Copyright 2006-2011 the original author or authors.
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
package org.springframework.data.hadoop.test.word;

import java.util.Map;
import java.util.StringTokenizer;

import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.BeanFactory;
import org.springframework.beans.factory.BeanFactoryAware;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.hadoop.mapreduce.MapperFactoryBean;
import org.springframework.data.hadoop.mapreduce.ReducerFactoryBean;

/**
 * @author Dave Syer
 * 
 */
@Configuration
public class PojoConfiguration extends JobConfiguration implements BeanFactoryAware {

	private BeanFactory beanFactory;

	public void setBeanFactory(BeanFactory beanFactory) throws BeansException {
		this.beanFactory = beanFactory;
	}

	@Bean
	@Override
	public Mapper<?, ?, ?, ?> mapper() throws Exception {
		MapperFactoryBean factory = new MapperFactoryBean();
		factory.setTarget(pojo());
		factory.setOutputKeyType(outputKeyType());
		factory.setOutputValueType(outputValueType());
		factory.setBeanFactory(beanFactory);
		factory.afterPropertiesSet();
		return factory.getObject();
	}

	@Bean
	@Override
	public Reducer<?, ?, ?, ?> reducer() throws Exception {
		ReducerFactoryBean factory = new ReducerFactoryBean();
		factory.setTarget(pojo());
		factory.setOutputKeyType(outputKeyType());
		factory.setOutputValueType(outputValueType());
		factory.setInputValueType(Integer.class);
		factory.setBeanFactory(beanFactory);
		factory.afterPropertiesSet();
		return factory.getObject();
	}

	@Bean
	public PojoMapReducer pojo() {
		return new PojoMapReducer();
	}

	public class PojoMapReducer {

		@org.springframework.data.hadoop.annotation.Mapper
		public void map(String value, Map<String, Integer> writer) {
			StringTokenizer itr = new StringTokenizer(value);
			while (itr.hasMoreTokens()) {
				writer.put(itr.nextToken(), 1);
			}
		}

		@org.springframework.data.hadoop.annotation.Reducer
		public int reduce(Iterable<Integer> values) {
			int sum = 0;
			for (Integer val : values) {
				sum += val;
			}
			return sum;
		}

	}

}
