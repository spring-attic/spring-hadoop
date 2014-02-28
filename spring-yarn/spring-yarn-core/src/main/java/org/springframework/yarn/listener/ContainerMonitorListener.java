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
package org.springframework.yarn.listener;

/**
 * Interface used for monitor to state of the monitor.
 *
 * @author Janne Valkealahti
 *
 */
public interface ContainerMonitorListener {

	/**
	 * Invoked when monitoring state is changed.
	 *
	 * @param state the {@link ContainerMonitorState}
	 */
	void state(ContainerMonitorState state);

	/**
	 * Class holding state info.
	 */
	public class ContainerMonitorState {

		private int free;

		private int running;

		private int completed;

		private int failed;

		/**
		 * Instantiates a new container monitor state.
		 *
		 * @param free the free count
		 * @param running the running count
		 * @param completed the completed count
		 * @param failed the failed count
		 */
		public ContainerMonitorState(int free, int running, int completed, int failed) {
			super();
			this.free = free;
			this.running = running;
			this.completed = completed;
			this.failed = failed;
		}

		/**
		 * Gets the free count.
		 *
		 * @return the allocated free
		 */
		public int getFree() {
			return free;
		}

		/**
		 * Gets the running count.
		 *
		 * @return the running count
		 */
		public int getRunning() {
			return running;
		}

		/**
		 * Gets the completed count.
		 *
		 * @return the completed count
		 */
		public int getCompleted() {
			return completed;
		}

		/**
		 * Gets the failed count.
		 *
		 * @return the failed count
		 */
		public int getFailed() {
			return failed;
		}

		@Override
		public String toString() {
			return "ContainerMonitorState [free=" + free + ", running=" + running + ", completed=" + completed
					+ ", failed=" + failed + "]";
		}

	}

}
