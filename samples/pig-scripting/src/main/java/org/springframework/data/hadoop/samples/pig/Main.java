package org.springframework.data.hadoop.samples.pig;

import java.util.Queue;
import java.util.concurrent.ArrayBlockingQueue;

import org.springframework.batch.core.launch.support.CommandLineJobRunner;
import org.springframework.batch.core.launch.support.SystemExiter;

public class Main {

	public static void main(String[] args) throws Exception {
		final Queue<Integer> exitCode = new ArrayBlockingQueue<Integer>(1);
		CommandLineJobRunner.presetSystemExiter(new SystemExiter() {			
			@Override
			public void exit(int status) {
				exitCode.add(status);
			}
		});

		CommandLineJobRunner.main(new String[] {
			"/META-INF/spring/context.xml",
			"pigJob"
			}
		);
		
		System.out.println("Exit code = " + exitCode.poll());
	}

}
