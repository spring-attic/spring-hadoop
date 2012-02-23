package org.springframework.data.hadoop.samples;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import static org.junit.Assert.*;


@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration("/META-INF/spring/context.xml")
public class WordCountWorkflowTests {

    @Autowired
    private ApplicationContext ctx;

	@Test
	public void testSanityTest() throws Exception {
		assertNotNull(ctx);
	}
}