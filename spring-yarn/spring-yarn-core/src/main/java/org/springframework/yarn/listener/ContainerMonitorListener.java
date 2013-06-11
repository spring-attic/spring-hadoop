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

		/** Number of free allocations */
		private int allocated;
		/** Number of completed */
		private int completed;
		/** Number of failed */
		private int failed;
		/** Progress as 0..1 */
		private double progress;

		/**
		 * Instantiates a new container monitor state.
		 *
		 * @param allocated the allocated
		 * @param completed the completed
		 * @param failed the failed
		 * @param progress the progress
		 */
		public ContainerMonitorState(int allocated, int completed, int failed, double progress) {
			super();
			this.allocated = allocated;
			this.completed = completed;
			this.failed = failed;
			this.progress = progress;
		}

		/**
		 * Gets the allocated.
		 *
		 * @return the allocated
		 */
		public int getAllocated() {
			return allocated;
		}

		/**
		 * Gets the completed.
		 *
		 * @return the completed
		 */
		public int getCompleted() {
			return completed;
		}

		/**
		 * Gets the failed.
		 *
		 * @return the failed
		 */
		public int getFailed() {
			return failed;
		}

		/**
		 * Gets the progress.
		 *
		 * @return the progress
		 */
		public double getProgress() {
			return progress;
		}

		@Override
		public String toString() {
			return "ContainerMonitorState [allocated=" + allocated + ", completed=" + completed + ", failed=" + failed
					+ ", progress=" + progress + "]";
		}
	}

}
