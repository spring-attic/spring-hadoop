/*
 * Copyright 2014 the original author or authors.
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
package org.springframework.yarn.boot.support;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.boot.CommandLineRunner;
import org.springframework.core.Ordered;
import org.springframework.yarn.launch.ExitCodeMapper;
import org.springframework.yarn.launch.JvmSystemExiter;
import org.springframework.yarn.launch.SimpleJvmExitCodeMapper;
import org.springframework.yarn.launch.SystemExiter;

/**
 * Base support class for {@link CommandLineRunner} implementations.
 *
 * @author Janne Valkealahti
 *
 */
public abstract class CommandLineRunnerSupport implements Ordered {

	private static final Log log = LogFactory.getLog(CommandLineRunnerSupport.class);

	private CountDownLatch latch = new CountDownLatch(1);

	private boolean waitLatch = true;

	private int order = Ordered.HIGHEST_PRECEDENCE;

	private static SystemExiter systemExiter = new JvmSystemExiter();

	private ExitCodeMapper exitCodeMapper = new SimpleJvmExitCodeMapper();

	@Override
	public int getOrder() {
		return order;
	}

	/**
	 * Sets the order used for{@link Ordered}. The order used in this
	 * class is {@code Ordered.HIGHEST_PRECEDENCE}.
	 *
	 * @param order the new order
	 */
	public void setOrder(int order) {
		this.order = order;
	}

	/**
	 * Exit method wrapping handling through {@link SystemExiter}.
	 *
	 * @param status the exit code
	 */
	public void exit(int status) {
		log.info("About to exit using code= " + status);
		systemExiter.exit(status);
	}

	/**
	 * Exit method wrapping handling through {@link SystemExiter}. As exist status
	 * for this method is given as {@code String}, status value is translated
	 * via {@link ExitCodeMapper}.
	 *
	 * @param status the exit code
	 */
	public void exit(String status) {
		exit(exitCodeMapper.intValue(status));
	}

	/**
	 * Sets the wait latch.
	 *
	 * @param waitLatch the new wait latch
	 */
	public void setWaitLatch(boolean waitLatch) {
		this.waitLatch = waitLatch;
	}

	/**
	 * Checks if is wait latch.
	 *
	 * @return true, if is wait latch
	 */
	public boolean isWaitLatch() {
		return waitLatch;
	}

	/**
	 * Sets the exit code mapper.
	 *
	 * @param exitCodeMapper the new exit code mapper
	 */
	public void setExitCodeMapper(ExitCodeMapper exitCodeMapper) {
		this.exitCodeMapper = exitCodeMapper;
	}

	/**
	 * Gets the exit code mapper.
	 *
	 * @return the exit code mapper
	 */
	public ExitCodeMapper getExitCodeMapper() {
		return exitCodeMapper;
	}

	/**
	 * Count down current latch by one.
	 */
	protected void countDownLatch() {
		if (isWaitLatch()) {
			latch.countDown();
		}
	}

	/**
	 * Simply delegates to {@link CountDownLatch#await()}.
	 */
	protected void waitLatch() {
		try {
			latch.await();
		} catch (InterruptedException e) {
			log.info("CommandLineRunner wait latch interrupted");
		}
	}

	/**
	 * Simply delegates to {@link CountDownLatch#await(long, TimeUnit)}.
	 *
	 * @param timeout the timeout
	 * @param unit the unit
	 */
	protected void waitLatch(long timeout, TimeUnit unit) {
		try {
			latch.await(timeout, unit);
		} catch (InterruptedException e) {
			log.info("CommandLineRunner wait latch interrupted");
		}
	}

}
