package org.springframework.data.hadoop.store;

/**
 * Interface defining a serializer used to convert arbitrary entities to a format accepted by DataWriters
 * 
 * @author mcintyred
 *
 * @param <E> the type of the entity to serialize
 * @param <S> the serialization format
 */
public interface Serializer<E, S> {

	S serialize(E entity);
	
}
