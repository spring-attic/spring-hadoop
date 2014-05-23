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
 * A {@code RolloverStrategy} using a size limiting a rollover operation.
 *
 * @author Janne Valkealahti
 *
 */
public class SizeRolloverStrategy extends AbstractRolloverStrategy {

	private final static Log log = LogFactory.getLog(SizeRolloverStrategy.class);

	private static final long KB = 1024;

	private static final long MB = KB * KB;

	private static final long GB = KB * MB;

	private static final long TB = KB * GB;

	private static final Pattern VALUE_PATTERN =
			Pattern.compile("([0-9]+([\\.,][0-9]+)?)\\s*(|K|M|G|T)B?", Pattern.CASE_INSENSITIVE);

	private static final long DEFAULT_MAX_SIZE = 1000 * 1024 * 1024;

	private long rolloverSize;

	/**
	 * Instantiates a new size rollover strategy.
	 */
	public SizeRolloverStrategy() {
		this.rolloverSize = DEFAULT_MAX_SIZE;
	}

	/**
	 * Instantiates a new size rollover strategy.
	 *
	 * @param rolloverSize the rollover size
	 */
	public SizeRolloverStrategy(long rolloverSize) {
		this.rolloverSize = rolloverSize;
	}

	/**
	 * Instantiates a new size rollover strategy.
	 *
	 * @param rolloverSize the rollover size
	 */
	public SizeRolloverStrategy(String rolloverSize) {
		this.rolloverSize = parseValue(rolloverSize);
	}

	@Override
	public boolean hasRolled() {
		if (log.isDebugEnabled()) {
			log.debug("Checking rolloverSize=" + rolloverSize + " against currentSize=" + getPosition());
		}
		return rolloverSize <= getPosition();
	}

	/**
	 * Sets the rollover size.
	 *
	 * @param rolloverSize the new rollover size
	 */
	public void setRolloverSize(long rolloverSize) {
		this.rolloverSize = rolloverSize;
	}

	@Override
	public void reset() {
		// nothing to do
	}

	@Override
	public SizeRolloverStrategy createInstance() {
		SizeRolloverStrategy instance = new SizeRolloverStrategy(rolloverSize);
		instance.setOrder(getOrder());
		return instance;
	}

	private static long parseValue(final String string) {
		final Matcher matcher = VALUE_PATTERN.matcher(string);
		if (matcher.matches()) {
			try {
				final long value = NumberFormat.getNumberInstance(Locale.getDefault())
						.parse(matcher.group(1))
						.longValue();
				final String units = matcher.group(3);

				if (units.equalsIgnoreCase("")) {
					return value;
				}
				else if (units.equalsIgnoreCase("K")) {
					return value * KB;
				}
				else if (units.equalsIgnoreCase("M")) {
					return value * MB;
				}
				else if (units.equalsIgnoreCase("G")) {
					return value * GB;
				}
				else if (units.equalsIgnoreCase("T")) {
					return value * TB;
				}
				else {
					log.error("Unable to recognize units: " + string);
					return DEFAULT_MAX_SIZE;
				}
			}
			catch (ParseException e) {
				log.error("Unable to parse: " + string, e);
				return DEFAULT_MAX_SIZE;
			}
		}
		log.error("Unable to parse bytes: " + string);
		return DEFAULT_MAX_SIZE;
	}

}
