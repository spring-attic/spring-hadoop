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
import org.junit.internal.AssumptionViolatedException;

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
     * Assume that a particular {@link Distro} is currently used.
     *
     * @param distros the distros to expect
     */
    public static void distro(Distro... distros) {
    	Set<Distro> current = Distro.resolveDistros();
    	for (Distro d : distros) {
			if (current.contains(d)) {
				return;
			}
		}
		throw new AssumptionViolatedException("None of a spesified distros [" + distros
				+ "] matched with current distros [" + current + "]");
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

}
