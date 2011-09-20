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
package org.springframework.data.hadoop.mapreduce;

import java.io.IOException;

import org.apache.hadoop.mapreduce.Reducer;
import org.springframework.data.hadoop.context.HadoopApplicationContextUtils;

/**
 * @author Dave Syer
 * 
 */
public class AutowiringCombiner<KI, VI, KO, VO> extends Reducer<KI, VI, KO, VO> {
	
	private Reducer<KI, VI, KO, VO> delegate;
	
	protected void setup(Context context) throws IOException, InterruptedException {
		@SuppressWarnings("unchecked")
		Reducer<KI, VI, KO, VO> delegate = HadoopApplicationContextUtils.getBean(context.getConfiguration(), Reducer.class, "combiner");
		this.delegate = delegate;
	}

	public void run(Context context) throws IOException ,InterruptedException {
		setup(context);
		delegate.run(context);
		cleanup(context);
	}
	
	protected void cleanup(Context context) throws IOException ,InterruptedException {
		HadoopApplicationContextUtils.releaseContext(context.getConfiguration());
	}

}