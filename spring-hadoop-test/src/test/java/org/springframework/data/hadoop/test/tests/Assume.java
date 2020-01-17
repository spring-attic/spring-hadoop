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
package org.springframework.data.hadoop.test.tests;

import static org.junit.Assume.assumeFalse;

import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.zlib.ZlibFactory;
import org.apache.hadoop.util.NativeCodeLoader;
import org.junit.AssumptionViolatedException;
import org.springframework.util.ClassUtils;
import org.springframework.util.ObjectUtils;


/**
 *
 * In addition to other assumptions, tests can be categorized into {@link TestGroup}s.
 * Active groups are enabled using the 'testGroups' system property,
 * usually activated from the gradle command line:
 * <pre>
 * gradle test -PtestGroups="performance"
 * </pre>
 *
 * Groups can be specified as a comma separated list of values, or using the pseudo group
 * 'all'. See {@link TestGroup} for a list of valid groups.
 *
 * @author Rob Winch
 * @author Phillip Webb
 * @author Janne Valkealahti
 */
public abstract class Assume {

	private static final Set<TestGroup> GROUPS = TestGroup.parse(System.getProperty("testGroups"));

    /**
     * Assume that a particular {@link TestGroup} has been specified.
     *
     * @param group the group that must be specified.
     */
    public static void group(TestGroup group) {
        if (!GROUPS.contains(group)) {
            throw new AssumptionViolatedException("Requires unspecified group " + group
                    + " from " + GROUPS);
        }
    }

    /**
     * Assume that a particular {@link Version} is currently used.
     *
     * @param version the version to expect
     */
	public static void hadoopVersion(Version version) {
		Version current = Version.resolveVersion();
		if (ObjectUtils.nullSafeEquals(version, current)) {
			return;
		} else {
			throw new AssumptionViolatedException("specified version [" + version
					+ "] not matched with current version [" + current + "]");
		}
	}

	/**
	 * Assume that the specified log is not set to Trace or Debug.
	 *
	 * @param log the log to test
	 */
	public static void notLogging(Log log) {
		assumeFalse(log.isTraceEnabled());
		assumeFalse(log.isDebugEnabled());
	}

	/**
	 * Assume that the specified codec class is present.
	 *
	 * @param codecClazz the codec class
	 */
	public static void codecExists(String codecClazz) {
		if(ClassUtils.isPresent(codecClazz, Assume.class.getClassLoader())) {
			Class<?> codecClass = ClassUtils.resolveClassName(codecClazz, Assume.class.getClassLoader());
			if (ClassUtils.isAssignable(CompressionCodec.class, codecClass)) {
				return;
			} else {
				throw new AssumptionViolatedException("Resolved class [" + codecClass
						+"] is not instance of CompressionCodec");
			}
		} else {
			throw new AssumptionViolatedException("Class [" + codecClazz
					+"] cannot be loaded");
		}
	}

	/**
	 * Assume that the hadoop native code is loaded.
	 */
	public static void nativeCode(Configuration configuration) {
		if (NativeCodeLoader.isNativeCodeLoaded() && ZlibFactory.isNativeZlibLoaded(configuration)) {
			return;
		} else {
			throw new AssumptionViolatedException("Native hadoop code not loaded");
		}
	}

}
