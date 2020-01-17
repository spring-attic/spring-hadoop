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
import java.text.NumberFormat;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.springframework.util.StringUtils;

/**
 * Various static string and parsing utilities.
 *
 * @author Janne Valkealahti
 *
 */
public class ParsingUtils {

	private static final long KB = 1024;

	private static final long MB = KB * KB;

	private static final long GB = KB * MB;

	private static final long TB = KB * GB;

	private static final Pattern VALUE_PATTERN =
			Pattern.compile("([0-9]+([\\.,][0-9]+)?)\\s*(|K|M|G|T)B?", Pattern.CASE_INSENSITIVE);

	/**
	 * Parses the given string as a number representing megs.
	 *
	 * @param string the value string
	 * @return the value as megs
	 * @throws ParseException the parse exception
	 */
	public static long parseBytesAsMegs(final String string) throws ParseException {
		try {
			// expect input as megs if it's a number
			return Long.parseLong(string);
		} catch (NumberFormatException e) {
		}
		long bytes = parseBytes(string);
		return (int) (bytes / MB);
	}

	/**
	 * Parses the count of bytes from a string.
	 *
	 * @param string the value string
	 * @return the value as bytes
	 * @throws ParseException the parse exception
	 */
	public static long parseBytes(final String string) throws ParseException {
		long value = 0;
		final Matcher matcher = VALUE_PATTERN.matcher(string);
		if (matcher.matches()) {
				final long numeric = NumberFormat.getNumberInstance(Locale.getDefault())
						.parse(matcher.group(1))
						.longValue();
				final String units = matcher.group(3);
				if (units.equalsIgnoreCase("")) {
					value = numeric;
				} else if (units.equalsIgnoreCase("K")) {
					value = numeric * KB;
				} else if (units.equalsIgnoreCase("M")) {
					value = numeric * MB;
				} else if (units.equalsIgnoreCase("G")) {
					value = numeric * GB;
				} else if (units.equalsIgnoreCase("T")) {
					value = numeric * TB;
				} else {
					throw new ParseException(string, 0);
				}
		} else {
			throw new ParseException(string, 0);
		}
		return value;
	}

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
