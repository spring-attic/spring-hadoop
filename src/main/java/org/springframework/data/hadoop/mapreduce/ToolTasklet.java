/*
 * Copyright 2011-2013 the original author or authors.
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

import org.apache.hadoop.util.Tool;
import org.springframework.batch.core.ExitStatus;
import org.springframework.batch.core.StepContribution;
import org.springframework.batch.core.scope.context.ChunkContext;
import org.springframework.batch.core.step.tasklet.SimpleSystemProcessExitCodeMapper;
import org.springframework.batch.core.step.tasklet.SystemProcessExitCodeMapper;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.util.Assert;

import java.io.IOException;

/**
 * Tasklet for executing Hadoop {@link Tool}s.
 * 
 * @author Costin Leau
 */
public class ToolTasklet extends ToolExecutor implements Tasklet, InitializingBean {

    private SystemProcessExitCodeMapper systemProcessExitCodeMapper = new SimpleSystemProcessExitCodeMapper();

	@Override
	public RepeatStatus execute(StepContribution contribution, ChunkContext chunkContext) throws Exception {
        int exitCode = runCode();
        if(systemProcessExitCodeMapper.getExitStatus(exitCode) == ExitStatus.FAILED)
            throw new IOException("Hadoop tool failed with exit code: "+exitCode);
		return RepeatStatus.FINISHED;
	}

    @Override
    public void afterPropertiesSet() throws Exception {
        Assert.notNull(systemProcessExitCodeMapper, "SystemProcessExitCodeMapper must be set");
    }

    /**
     * @param systemProcessExitCodeMapper maps system process return value to
     * <code>ExitStatus</code> returned by Tasklet.
     * {@link SimpleSystemProcessExitCodeMapper} is used by default.
     */
    public void setSystemProcessExitCodeMapper(SystemProcessExitCodeMapper systemProcessExitCodeMapper) {
        this.systemProcessExitCodeMapper = systemProcessExitCodeMapper;
    }
}