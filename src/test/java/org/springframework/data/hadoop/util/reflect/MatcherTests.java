package org.springframework.data.hadoop.util.reflect;

import static org.junit.Assert.assertEquals;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.util.Date;
import java.util.List;

import org.junit.Test;
import org.springframework.core.MethodParameter;
import org.springframework.data.hadoop.util.reflect.AnnotatedParameters;
import org.springframework.data.hadoop.util.reflect.MethodUtils;
import org.springframework.data.hadoop.util.reflect.ParameterRules;
import org.springframework.data.hadoop.util.reflect.SimpleRule;
import org.springframework.data.hadoop.util.reflect.SingleParameter;
import org.springframework.data.hadoop.util.reflect.TypedParameters;

public class MatcherTests {

	@Retention(RetentionPolicy.RUNTIME)
	@Target(ElementType.PARAMETER)
	public static @interface Foo {
	}

	@Test
	public void testAllParameters() throws Exception {
		@SuppressWarnings("unused")
		class Target {
			public void foo(String bar) {
			}
		}
		ParameterRules rules = new ParameterRules();
		List<MethodParameter> parameters = rules.execute(MethodUtils.getSingleNamedMethod(Target.class, "foo"));
		assertEquals(1, parameters.size());
	}

	@Test
	public void testAnnotatedOrSingle() throws Exception {
		@SuppressWarnings("unused")
		class Target {
			public void foo(@Foo String foo, String bar) {
			}

			public void bar(@Foo String foo) {
			}

			public void spam(String foo) {
			}
		}
		ParameterRules rules = new ParameterRules(new SimpleRule(new AnnotatedParameters(Foo.class), null),
				new SimpleRule(new SingleParameter(), null));
		List<MethodParameter> parameters;
		parameters = rules.execute(MethodUtils.getSingleNamedMethod(Target.class, "foo"));
		assertEquals(0, parameters.size());
		parameters = rules.execute(MethodUtils.getSingleNamedMethod(Target.class, "bar"));
		assertEquals(0, parameters.size());
		parameters = rules.execute(MethodUtils.getSingleNamedMethod(Target.class, "spam"));
		assertEquals(0, parameters.size());
	}

	@Test
	public void testTypedParameter() throws Exception {
		@SuppressWarnings("unused")
		class Target {
			public void foo(String foo, Date bar) {
			}
		}
		ParameterRules rules = new ParameterRules(new SimpleRule(new TypedParameters(Date.class), null));
		List<MethodParameter> parameters;
		parameters = rules.execute(MethodUtils.getSingleNamedMethod(Target.class, "foo"));
		assertEquals(1, parameters.size());
	}

}
