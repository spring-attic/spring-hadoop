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
package org.springframework.yarn.support;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.Reader;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.springframework.util.StringUtils;

/**
 * Various static string and parsing utilities.
 *
 * @author Janne Valkealahti
 *
 */
public class ParsingUtils {

	/**
	 * Extracts a string from another string which can be used as a command in
	 * shell. Resulting string can't have any unnecessary spaces, line
	 * delimiters or any other characters which might prevent command to be run.
	 *
	 * @param source Source string
	 * @return Command string or null if error occurred
	 */
	public static String extractRunnableCommand(String source) {
		if (!StringUtils.hasText(source)) {
			return null;
		}
		StringBuilder buf = new StringBuilder();
		try {
			List<String> lines = readLines(new StringReader(source));
			for (String line : lines) {
				buf.append(line.trim() + ' ');
			}
		} catch (IOException e) {
			return null;
		}
		return buf.toString();
	}

	private static List<String> readLines(Reader input) throws IOException {
		BufferedReader reader = new BufferedReader(input);
		List<String> list = new ArrayList<String>();
		String line = reader.readLine();
		while (line != null) {
			list.add(line);
			line = reader.readLine();
		}
		return list;
	}

	/**
	 * Extracts a classpath string from a source text.
	 *
	 * @param source the source string
	 * @param delimiter the delimiter
	 * @return classpath string
	 */
	public static String extractClasspath(String source, String delimiter) {
		if (!StringUtils.hasText(source)) {
			return null;
		}
		StringBuilder buf = new StringBuilder();

		try {
			List<String> lines = readLines(new StringReader(source));
			List<String> linesWithContent = new ArrayList<String>();
			Iterator<String> iterator = lines.iterator();

			// remove empty entries
			while(iterator.hasNext()) {
				String line = iterator.next().trim();
				if(StringUtils.hasText(line)) {
					linesWithContent.add(line);
				}
			}
			iterator = linesWithContent.iterator();

			// finally add entries with content
			while(iterator.hasNext()) {
				buf.append(iterator.next());
				if(iterator.hasNext()) {
					buf.append(delimiter);
				}
			}

		} catch (IOException e) {
			return null;
		}

		return buf.toString();
	}

}
