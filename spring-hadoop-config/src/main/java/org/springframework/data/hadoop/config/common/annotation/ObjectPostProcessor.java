package org.springframework.data.hadoop.config.common.annotation;

import org.springframework.beans.factory.Aware;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.InitializingBean;

/**
 * Allows initialization of Objects. Typically this is used to call the
 * {@link Aware} methods, {@link InitializingBean#afterPropertiesSet()}, and
 * ensure that {@link DisposableBean#destroy()} has been invoked.
 *
 * @param <T> the bound of the types of Objects this {@link ObjectPostProcessor} supports.
 *
 * @author Rob Winch
 */
public interface ObjectPostProcessor<T> {

	/**
	 * Initialize the object possibly returning a modified instance that should
	 * be used instead.
	 *
	 * @param object the object to initialize
	 * @param <O> the object type
	 * @return the initialized version of the object
	 */
	<O extends T> O postProcess(O object);

	/**
	 * A do nothing implementation of the {@link ObjectPostProcessor}
	 */
	ObjectPostProcessor<Object> QUIESCENT_POSTPROCESSOR = new ObjectPostProcessor<Object>() {
		@Override
		public <T> T postProcess(T object) {
			return object;
		}
	};

}
