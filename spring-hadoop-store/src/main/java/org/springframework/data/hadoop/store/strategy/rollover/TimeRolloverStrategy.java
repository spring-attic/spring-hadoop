/*
 * Copyright 2013 the original author or authors.
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
package org.springframework.data.hadoop.store.strategy.rollover;

import java.text.NumberFormat;
import java.text.ParseException;
import java.util.Locale;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * A {@code RolloverStrategy} using a time limiting a rollover operation.
 *
 * @author liu jiong
 *
 */
public class TimeRolloverStrategy extends AbstractRolloverStrategy {

	private final static Log log = LogFactory.getLog(TimeRolloverStrategy.class);

	private static final long DEFAULT_TIMEOUT = 1000 * 60 * 60;

	private long rolloverTime;
	
	private long startTime;

	/**
	 * Instantiates a new size rollover strategy.
	 */
	public TimeRolloverStrategy() {
		this(DEFAULT_TIMEOUT,System.currentTimeMillis());
	}

	/**
	 * Instantiates a new size rollover strategy.
	 *
	 * @param rolloverSize the rollover size
	 */
	public TimeRolloverStrategy(long rolloverSize) {
		this(rolloverSize,System.currentTimeMillis());
	}

	public TimeRolloverStrategy(long rolloverSize,long startTime) {
		this.rolloverTime = rolloverSize;
		this.startTime=startTime;
	}
	
	@Override
	public boolean hasRolled() {
		if (log.isDebugEnabled()) {
			log.debug("Checking rolloverSize=" + rolloverTime + " against currentSize=" + System.currentTimeMillis());
		}
		return rolloverTime+startTime <= System.currentTimeMillis();
	}

	/**
	 * Sets the rollover size.
	 *
	 * @param rolloverSize the new rollover size
	 */
	public void setRolloverSize(long rolloverSize) {
		this.rolloverTime = rolloverSize;
	}

	@Override
	public void reset() {
		// nothing to do
	}

	@Override
	public TimeRolloverStrategy createInstance() {
		TimeRolloverStrategy instance = new TimeRolloverStrategy(rolloverTime);
		instance.setOrder(getOrder());
		return instance;
	}

	

}
