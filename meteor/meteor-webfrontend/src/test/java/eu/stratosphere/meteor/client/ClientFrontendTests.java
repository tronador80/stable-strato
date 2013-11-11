package eu.stratosphere.meteor.client;

import static org.junit.Assert.*;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyBoolean;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.MockitoAnnotations.initMocks;
import static org.powermock.api.mockito.PowerMockito.mockStatic;
import static org.powermock.api.mockito.PowerMockito.verifyStatic;
import static org.powermock.api.mockito.PowerMockito.whenNew;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.List;

import org.apache.commons.cli.CommandLine;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.contrib.java.lang.system.StandardOutputStreamLog;
import org.junit.runner.RunWith;
import org.mockito.InOrder;
import org.mockito.Mock;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import eu.stratosphere.meteor.QueryParser;
import eu.stratosphere.nephele.configuration.Configuration;
import eu.stratosphere.nephele.configuration.GlobalConfiguration;
import eu.stratosphere.sopremo.client.DefaultClient;
import eu.stratosphere.sopremo.client.StateListener;
import eu.stratosphere.sopremo.execution.SopremoID;
import eu.stratosphere.sopremo.operator.SopremoPlan;
import eu.stratosphere.sopremo.query.QueryParserException;

/**
 * This test class tests the ClientFrontend.java that based on CLClient 
 * from meteor-client. In this tests we assume that the CLCLient.java 
 * works fine and was tested in separate ways.
 *
 * This tests needs to prepare the GlobalConfiguration class and the ClientFrontend
 * class for tests.
 *
 * @author André Greiner-Petter
 *
 */
@RunWith( PowerMockRunner.class )
@PrepareForTest( { GlobalConfiguration.class, ClientFrontend.class } )
public class ClientFrontendTests {
	
	/**
	 * Create a rule for System.out
	 */
	@Rule
	public final StandardOutputStreamLog outLog = new StandardOutputStreamLog();
	
	/**
	 * The class we want to test
	 */
	private ClientFrontend client;
	
	/**
	 * Some global commands for execution
	 */
	private final String outLink = "file:///holla/save.json";
	private final String outVisLink = "file:///holla/vis_.json";
	private final String testMeteorScript = "write $curr to '" + outLink + "';";
	private final String testVisScript = "write $curr to '" + outVisLink + "';";
	
	/**
	 * Define the mockitos
	 */
	@Mock
	CLClient clientMock;
	
	@Mock
	DefaultClient defaultClientMock;
	
	@Mock
	CommandLine cmdMock;
	
	@Mock
	SopremoPlan sopremoPlanMock;
	
	@Mock
	SopremoID sopremoIDMock;
	
	@Mock
	QueryParser queryParserMock;
	
	/**
	 * Set up for tests
	 * @throws Exception
	 */
	@Before
	public void setUp() throws Exception {
		// init all mockitos
		initMocks( this );
		
		// init static mockito
		mockStatic( GlobalConfiguration.class );
		
		// define mocked statements
		whenNew( CLClient.class ).withNoArguments().thenReturn( clientMock );
		whenNew( DefaultClient.class ).withArguments( any(Configuration.class) ).thenReturn(defaultClientMock);
		
		when( clientMock.parseOptions( any(String[].class) ) ).thenReturn( cmdMock );
		when( cmdMock.getOptionValue( "configDir" ) ).thenReturn("TEST");
		when( cmdMock.hasOption( "updateTime" ) ).thenReturn( false );
		when( cmdMock.getOptionValue( "server" ) ).thenReturn( "localhost" );
		when( cmdMock.getOptionValue( "port" ) ).thenReturn( "8081" );
		
		// the following statements are useful for executionTests
		whenNew( QueryParser.class ).withNoArguments().thenReturn( queryParserMock );
		
		when( queryParserMock.tryParse( any( ByteArrayInputStream.class ) ) ).thenReturn( sopremoPlanMock );
		
		when( defaultClientMock.submit(
				any( SopremoPlan.class ), any( StateListener.class ), anyBoolean() ) )
				.thenReturn(sopremoIDMock);
		
		when( defaultClientMock.getMetaData( sopremoIDMock, "key" )).thenReturn("META_INF");
		
		// create our test object
		client = new ClientFrontend( null );
	}
	
	/**
	 * Test the default case.
	 */
	@Test
	public void initTest(){		
		// verify interactions with DefaultClient and CommandLine
		verify( clientMock, times(1) ).parseOptions( null );
		verify( cmdMock, times(1) ).getOptionValue( "configDir" );
		verify( cmdMock, times(1) ).hasOption( "updateTime" );
		verify( cmdMock, times(1) ).getOptionValue( "port" );
		verify( cmdMock, times(1) ).getOptionValue( "server" );
		
		// verify interaction with GlobalConfiguration
		verifyStatic( times(1) );
		GlobalConfiguration.loadConfiguration( "TEST" );
	}
	
	/**
	 * Verify the test throws an IOException if execute( ... ) from test object
	 * throws an IOException.
	 * @throws Exception
	 */
	@Test ( expected = IOException.class )
	public void executionFailureTest() throws Exception{
		when( queryParserMock.tryParse(any( ByteArrayInputStream.class )) ).thenThrow( new IOException() );
		client.execute( testMeteorScript );
	}
	
	/**
	 * Test normal execute( script )
	 * @throws QueryParserException
	 * @throws IOException
	 */
	@Test
	public void executionTest() throws QueryParserException, IOException{
		client.execute( testMeteorScript );
		
		// verify the class try to parse and submit the script
		InOrder inOrderTest = inOrder( queryParserMock, defaultClientMock );
		
		// verify the class try to parse the script before to submit it
		inOrderTest.verify( queryParserMock, times(1) )
				.tryParse( any( ByteArrayInputStream.class ) );
		inOrderTest.verify( defaultClientMock, times(1) )
				.submit( any( SopremoPlan.class ), any( StateListener.class ), anyBoolean() );
		
		// we expected an information on console
		assertTrue( outLog.getLog().contains("submitted with sopremoID " + sopremoIDMock.toString() + System.lineSeparator() ) );
	}
	
	/**
	 * Get the list of output links that contains in the testMeteorScript
	 * @throws QueryParserException
	 * @throws IOException
	 */
	@Test
	public void executionNormalOutputTest() throws QueryParserException, IOException{
		client.execute( testMeteorScript );
		
		// get output list
		List<String> pathsList = client.getOutputPaths();
		
		// verify the list isn't empty
		assertFalse( pathsList.isEmpty() );
		
		// get first link
		String link1 = pathsList.get(0);
		
		// verify its the same link as outLink that contains in the testMeteorScript
		assertEquals( link1, outLink );
		
		// verify there is no visualization URL
		assertNull( client.getVisualizationDataURL() );
	}
	
	/**
	 * Same test as above but with testVisScript that contains a visualization link.
	 * @throws QueryParserException
	 * @throws IOException
	 */
	@Test
	public void executionVisualOutputTest() throws QueryParserException, IOException {
		client.execute( testVisScript );
		
		// get outputs and verify the list isn't empty
		List<String> pathsList = client.getOutputPaths();
		assertFalse( pathsList.isEmpty() );
		
		// get first link and verify its the same as outVisLink
		String link1 = pathsList.get(0);
		assertEquals( link1, outVisLink );
		
		// get visualization URL and verify its equal to outVisLink
		assertEquals( client.getVisualizationDataURL(), outVisLink );
	}
	
	/**
	 * Get the metaData from the client.
	 * @throws QueryParserException
	 * @throws IOException
	 */
	@Test
	public void getMetaDataAfterExecutionTest() throws QueryParserException, IOException {		
		client.execute( testMeteorScript );
		
		// expected the META_INF
		assertEquals( client.getMetaData("key"), "META_INF" );
		
		// expected an information on console
		assertTrue( outLog.getLog().contains("returning metadata for " + sopremoIDMock.toString() + System.lineSeparator() ));
	}
}