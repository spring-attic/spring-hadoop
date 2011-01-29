package org.springframework.hadoop.configuration.xml;

import org.apache.hadoop.mapreduce.Mapper;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

/**
 * Live-fire test of the Spring namespace
 *
 * @author Josh Long
 */
@ContextConfiguration("/configuration/xml/hadoop-ns-1.xml")
@RunWith(SpringJUnit4ClassRunner.class)
public class NamespaceParsingTests {

	@Autowired @Qualifier("nsMapper") private Mapper mapper ;

	@Test
	public void testParserCorrectlyInstantiatedMapper() throws Exception {
		Assert.assertNotNull( "the mapper should not be null" , this.mapper) ;
	}
}
