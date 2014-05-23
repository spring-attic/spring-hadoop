/*
 * Copyright 2013 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.springframework.data.hadoop.store.strategy.naming;

import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.fs.Path;
import org.springframework.data.hadoop.store.codec.CodecInfo;
import org.springframework.data.hadoop.store.support.OrderedComposite;

/**
 * A {@code FileNamingStrategy} chaining other strategies.
 *
 * @author Janne Valkealahti
 *
 */
public class ChainedFileNamingStrategy implements FileNamingStrategy {

	/** List of ordered composite strategies */
	private final OrderedComposite<FileNamingStrategy> strategies;

	/**
	 * Instantiates a new chained rollover strategy.
	 */
	public ChainedFileNamingStrategy() {
		this(Collections.<FileNamingStrategy> emptyList());
	}

	/**
	 * Instantiates a new chained rollover strategy.
	 *
	 * @param strategies the strategies
	 */
	public ChainedFileNamingStrategy(List<? extends FileNamingStrategy> strategies) {
		this.strategies = new OrderedComposite<FileNamingStrategy>();
		setStrategies(strategies);
	}

	@Override
	public Path resolve(Path path) {
		for (Iterator<? extends FileNamingStrategy> iterator = strategies.iterator(); iterator.hasNext();) {
			path = iterator.next().resolve(path);
		}
		return path;
	}

	@Override
	public void next() {
		for (Iterator<? extends FileNamingStrategy> iterator = strategies.iterator(); iterator.hasNext();) {
			iterator.next().next();
		}
	}

	@Override
	public void reset() {
		for (Iterator<? extends FileNamingStrategy> iterator = strategies.iterator(); iterator.hasNext();) {
			iterator.next().reset();
		}
	}

	@Override
	public Path init(Path path) {
		for (Iterator<? extends FileNamingStrategy> iterator = strategies.iterator(); iterator.hasNext();) {
			path = iterator.next().init(path);
		}
		return path;
	}

	@Override
	public void setCodecInfo(CodecInfo codecInfo) {
		for (Iterator<? extends FileNamingStrategy> iterator = strategies.iterator(); iterator.hasNext();) {
			iterator.next().setCodecInfo(codecInfo);
		}
	}

	@Override
	public ChainedFileNamingStrategy createInstance() {
		ChainedFileNamingStrategy instance = new ChainedFileNamingStrategy();
		for (FileNamingStrategy strategy : strategies.getItems()) {
			instance.register(((FileNamingStrategyFactory<? extends FileNamingStrategy>)strategy).createInstance());
		}
		return instance;
	}

	/**
	 * Sets the list of strategies. This clears all existing strategies.
	 *
	 * @param strategies the new strategies
	 */
	public void setStrategies(List<? extends FileNamingStrategy> strategies) {
		this.strategies.setItems(strategies);
	}

	/**
	 * Register a new strategy.
	 *
	 * @param strategy the strategy
	 */
	public void register(FileNamingStrategy strategy) {
		strategies.add(strategy);
	}

	/**
	 * Gets the strategies.
	 *
	 * @return the strategies
	 */
	public List<? extends FileNamingStrategy> getStrategies() {
		return strategies.getItems();
	}

}
