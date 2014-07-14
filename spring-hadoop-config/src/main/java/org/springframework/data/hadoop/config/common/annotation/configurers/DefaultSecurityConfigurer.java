/*
 * Copyright 2014 the original author or authors.
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
package org.springframework.data.hadoop.config.common.annotation.configurers;

import org.springframework.data.hadoop.config.common.annotation.AnnotationBuilder;
import org.springframework.data.hadoop.config.common.annotation.AnnotationConfigurerAdapter;
import org.springframework.data.hadoop.security.HadoopSecurity;
import org.springframework.data.hadoop.security.SecurityAuthMethod;
import org.springframework.util.StringUtils;

/**
 * {@link org.springframework.data.hadoop.config.common.annotation.AnnotationConfigurer AnnotationConfigurer}
 * which knows how to handle configuring a {@link HadoopSecurity}.
 *
 * @author Janne Valkealahti
 *
 * @param <O> The Object being built by B
 * @param <I> The type of interface or builder itself returned by the configurer
 * @param <B> The Builder that is building O and is configured by {@link AnnotationConfigurerAdapter}
 */
public class DefaultSecurityConfigurer<O, I, B extends AnnotationBuilder<O>> extends
		AnnotationConfigurerAdapter<O, I, B> implements SecurityConfigurer<I> {

	private HadoopSecurity hadoopSecurity = new HadoopSecurity();

	@Override
	public SecurityConfigurer<I> authMethod(String authMethod) {
		if (StringUtils.hasText(authMethod)) {
			SecurityAuthMethod method = SecurityAuthMethod.valueOf(authMethod.toUpperCase());
			if (method != null) {
				hadoopSecurity.setSecurityAuthMethod(method);
			}
		}
		return this;
	}

	@Override
	public SecurityConfigurer<I> authMethod(SecurityAuthMethod authMethod) {
		if (authMethod != null) {
			hadoopSecurity.setSecurityAuthMethod(authMethod);
		}
		return this;
	}

	@Override
	public SecurityConfigurer<I> userPrincipal(String principal) {
		if (StringUtils.hasText(principal)) {
			hadoopSecurity.setUserPrincipal(principal);
		}
		return this;
	}

	@Override
	public SecurityConfigurer<I> userKeytab(String keytab) {
		if (StringUtils.hasText(keytab)) {
			hadoopSecurity.setUserKeytab(keytab);
		}
		return this;
	}

	@Override
	public void configure(B builder) throws Exception {
		if (!configureSecurity(builder, hadoopSecurity)) {
			if (builder instanceof SecurityConfigurerAware) {
				((SecurityConfigurerAware) builder).configureSecurity(hadoopSecurity);
			}
		}
	}

	@Override
	public SecurityConfigurer<I> namenodePrincipal(String principal) {
		if (StringUtils.hasText(principal)) {
			hadoopSecurity.setNamenodePrincipal(principal);
		}
		return this;
	}

	@Override
	public SecurityConfigurer<I> rmManagerPrincipal(String principal) {
		if (StringUtils.hasText(principal)) {
			hadoopSecurity.setRmManagerPrincipal(principal);
		}
		return this;
	}

	/**
	 * Gets the {@link HadoopSecurity} configured for this builder.
	 *
	 * @return the security
	 */
	public HadoopSecurity getSecurity() {
		return hadoopSecurity;
	}

	/**
	 * Configure security. If this implementation is extended, custom configure
	 * handling can be handled here.
	 *
	 * @param builder the builder
	 * @param security the security
	 * @return true, if security configure is handled
	 */
	protected boolean configureSecurity(B builder, HadoopSecurity security) {
		return false;
	}

}
