package org.springframework.yarn.support.statemachine;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

import org.junit.Test;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.task.SyncTaskExecutor;
import org.springframework.core.task.TaskExecutor;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.yarn.support.statemachine.config.EnableStateMachineFactory;
import org.springframework.yarn.support.statemachine.config.EnumStateMachineConfigurerAdapter;
import org.springframework.yarn.support.statemachine.config.EnumStateMachineFactory;
import org.springframework.yarn.support.statemachine.config.builders.StateMachineStateConfigurer;
import org.springframework.yarn.support.statemachine.config.builders.StateMachineTransitionConfigurer;
import org.springframework.yarn.support.statemachine.state.State;

public class StateMachineFactoryTests extends AbstractStateMachineTests {

	@SuppressWarnings({ "unchecked" })
	@Test
	public void testMachineFromFactory() {
		AnnotationConfigApplicationContext ctx = new AnnotationConfigApplicationContext(Config.class);

		EnumStateMachineFactory<TestStates, TestEvents> stateMachineFactory =
				ctx.getBean(StateMachineSystemConstants.DEFAULT_ID_STATEMACHINEFACTORY, EnumStateMachineFactory.class);

		StateMachine<State<TestStates, TestEvents>, TestEvents> machine = stateMachineFactory.getStateMachine();
		assertThat(machine.getState().getId(), is(TestStates.S1));
		machine.sendEvent(MessageBuilder.withPayload(TestEvents.E1).build());
		assertThat(machine.getState().getId(), is(TestStates.S2));
		ctx.close();
	}

	@Configuration
	@EnableStateMachineFactory
	static class Config extends EnumStateMachineConfigurerAdapter<TestStates, TestEvents> {

		@Override
		public void configure(StateMachineStateConfigurer<TestStates, TestEvents> states) throws Exception {
			states
				.withStates()
					.initial(TestStates.S1)
					.state(TestStates.S1)
					.state(TestStates.S2);
		}

		@Override
		public void configure(StateMachineTransitionConfigurer<TestStates, TestEvents> transitions) throws Exception {
			transitions
				.withExternal()
					.source(TestStates.S1)
					.target(TestStates.S2)
					.event(TestEvents.E1);
		}

		@Bean
		public TaskExecutor taskExecutor() {
			return new SyncTaskExecutor();
		}

	}

}
