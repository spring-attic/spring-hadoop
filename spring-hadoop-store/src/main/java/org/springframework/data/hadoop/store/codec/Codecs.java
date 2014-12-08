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
package org.springframework.data.hadoop.store.codec;

import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.io.compress.BZip2Codec;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.io.compress.SnappyCodec;

/**
 * Default codecs supported by store framework. We keep codec info here for implementations
 * which are supported out of the box. Reference to codec is a fully qualified name of a class,
 * not a class itself. This allows user to define and use codecs which are added into a classpath
 * unknown during the compilation time.
 * <p>
 * This enum also allows case insensitive lookup for main abbreviation. Defined abbreviations strings
 * in a constructor are just keywords to do back mapping from a lookup table. Registering a mixing
 * abbreviations is not checked.
 * <p>
 * Codecs.get("gz").getAbbreviation(); "GZIP"
 * <p>
 *
 * @author Janne Valkealahti
 *
 */
public enum Codecs {

	/**
	 * Non-splittable {@link GzipCodec}.
	 */
	GZIP(new DefaultCodecInfo(GzipCodec.class.getName(), false, "gz"), "GZIP"),

	/**
	 * Non-splittable {@link SnappyCodec}. This codec will need native snappy libraries.
	 */
	SNAPPY(new DefaultCodecInfo(SnappyCodec.class.getName(), false, "snappy"), "SNAPPY"),

	/**
	 * Splittable {@link BZip2Codec}.
	 */
	BZIP2(new DefaultCodecInfo(BZip2Codec.class.getName(), true, "bz2"), "BZIP2"),

	/**
	 * Non-splittable {@code LzoCodec}.
	 */
	LZO(new DefaultCodecInfo("com.hadoop.compression.lzo.LzoCodec", false, "lzo"), "LZO"),

	/**
	 * Non-splittable {@code LzopCodec}.
	 */
	LZOP(new DefaultCodecInfo("com.hadoop.compression.lzo.LzopCodec", false, "lzop"), "LZOP");

	private final CodecInfo codec;

	private final String[] abbreviations;

	private static final Map<String, Codecs> lookup = new HashMap<String, Codecs>();

	static {
		for (Codecs c : Codecs.values()) {
			String[] array = c.getAbbreviations();
			for (String abbv : array) {
				lookup.put(abbv.toLowerCase(), c);
			}
		}
	}

	/**
	 * Instantiates a new codecs.
	 *
	 * @param codec the codec info
	 * @param abbreviations the codec abbreviations
	 */
	private Codecs(CodecInfo codec, String... abbreviations) {
		this.codec = codec;
		this.abbreviations = abbreviations;
	}

	/**
	 * Gets the codec info.
	 *
	 * @return the codec info
	 */
	public CodecInfo getCodecInfo() {
		return codec;
	}

	/**
	 * Gets the main abbreviation.
	 *
	 * @return the main abbreviation
	 */
	public String getAbbreviation() {
		return abbreviations[0];
	}

	/**
	 * Gets the abbreviations.
	 *
	 * @return the abbreviations
	 */
	public String[] getAbbreviations() {
		return abbreviations;
	}

	/**
	 * Gets the {@code Codecs} by its abbreviation. Lookup returns <code>NULL</code> if abbreviation hasn't been
	 * registered.
	 *
	 * @param abbreviation the abbreviation
	 * @return the codecs resulted as a lookup
	 */
	public static Codecs get(String abbreviation) {
		return lookup.get(abbreviation.toLowerCase());
	}

	/**
	 * Gets the {@code CodecInfo} by {@code Codecs} abbreviation. Lookup returns <code>NULL</code> if abbreviation
	 * hasn't been registered.
	 *
	 * @param abbreviation the abbreviation
	 * @return the codec info resulted as a lookup
	 * @see #get(String)
	 */
	public static CodecInfo getCodecInfo(String abbreviation) {
		Codecs codecs = get(abbreviation);
		return codecs != null ? codecs.getCodecInfo() : null;
	}

}
