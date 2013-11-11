/**
 * ------------------------ BE VERY CAREFUL WITH ECLIPSE ------------------------
 * Eclipse got a bug with PrepareForTest static classes in java 7. It doesn't run
 * the test. To fix that problem in eclipse start the test classes with VM parameter:
 * 		-XX:-UseSplitVerifier
 * That should be fix the problem.
 * 
 * For more informations please contact: 
 * 		André Greiner-Petter <andre.greiner-petter@math.tu-berlin.de>
 * ------------------------------------------------------------------------------
 */
package eu.stratosphere.meteor.client.common;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.MockitoAnnotations.initMocks;
import static org.powermock.api.mockito.PowerMockito.doThrow;
import static org.powermock.api.mockito.PowerMockito.mockStatic;
import static org.powermock.api.mockito.PowerMockito.verifyStatic;
import static org.powermock.api.mockito.PowerMockito.whenNew;

import java.io.IOException;
import java.util.List;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InOrder;
import org.mockito.Mock;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import eu.stratosphere.meteor.client.ClientFrontend;
import eu.stratosphere.meteor.client.WebFrontend;
import eu.stratosphere.meteor.client.web.ErrorServlet;
import eu.stratosphere.meteor.client.web.OutputServlet;
import eu.stratosphere.meteor.client.web.VisualizationServlet;
import eu.stratosphere.nephele.configuration.Configuration;
import eu.stratosphere.pact.common.util.PactConfigConstants;
import eu.stratosphere.sopremo.query.QueryParserException;

/**
 * 
 * This class test the MeteorContextHandler.class.
 * We test static and non-static methods of this class. It starts a new thread to run
 * a job so we use time-limits for tests.
 *
 * @author André Greiner-Petter
 *
 */
@SuppressWarnings({ "static-access" })
@RunWith( PowerMockRunner.class )
@PrepareForTest( { MeteorContextHandler.class, ErrorServlet.class } )
public class ContextHandlerTests {
	/**
	 * time limit for answer
	 */
	private static final int TIME_LIMIT = 2000;
	
	/**
	 * Visualization servlet needs a port
	 */
	private static final int VIS_PORT = 8081;
	
	/**
	 * Create mocks for real context handler.
	 * It needs a configuration and a client
	 */
	@Mock Configuration configMock;
	@Mock ClientFrontend clientMock;
	
	/**
	 * To test context handler in efficient way we have to initialize some servlets as mocks
	 */
	@Mock VisualizationServlet visServ;
	@Mock OutputServlet outServ;
	
	/**
	 * This class we want to test
	 */
	MeteorContextHandler contextHandler;
	
	/**
	 * Initialize all
	 * @throws Exception
	 */
	@Before
	public void setUp() throws Exception {
		// init mocks of this class
		initMocks( this );
		
		// init static mocks
		mockStatic( ErrorServlet.class );
		
		// make sure that we use our mocks
		whenNew( VisualizationServlet.class ).withArguments( VIS_PORT ).thenReturn( this.visServ );
		whenNew( OutputServlet.class ).withNoArguments().thenReturn( this.outServ );
		//whenNew( OutputServlet.class ).withNoArguments().thenThrow( new Exception("badass") );
		
		// what mocks have to returned
		when( configMock.getString( WebFrontend.RESOURCEDIR, null) ).thenReturn( "resources/web-docs" );
		when( configMock.getInteger( PactConfigConstants.WEB_FRONTEND_PORT_KEY,
				PactConfigConstants.DEFAULT_WEB_FRONTEND_PORT ) ).thenReturn( VIS_PORT );
		
		when( clientMock.getVisualizationDataURL() ).thenReturn("testURL");
		when( clientMock.getOutputPaths() ).thenReturn( null );
		
		// initialize class we want to test
		contextHandler = new MeteorContextHandler( configMock, clientMock );
	}
	
	/**
	 * Test getClient method
	 */
	@Test
	public void getClientTest(){
		assertEquals( contextHandler.getClient(), clientMock );
	}
	
	/**
	 * Test this default case. Initialize a context handler by given path and verify interactions.
	 */
	@Test
	public void getResourceByGivenPathTest(){
		verify( configMock, times(1) ).getString(WebFrontend.RESOURCEDIR, null);
		verify( configMock, times(0) ).getString(PactConfigConstants.STRATOSPHERE_BASE_DIR_PATH_KEY, "");
		verify( configMock, times(0) ).getString(PactConfigConstants.WEB_ROOT_PATH_KEY,
				PactConfigConstants.DEFAULT_WEB_ROOT_DIR);
	}
	
	/**
	 * Test whether we use Stratosphere_Base_Dir_Path if nothing else is said.
	 */
	@Test
	public void getResourceDoesNotExistTest() {
		// reinitialize mocks ( -> configMock never called before )
		initMocks( this );
		
		// initialize new case
		when( configMock.getString( WebFrontend.RESOURCEDIR, null) ).thenReturn( null );
		when( configMock.getString( PactConfigConstants.STRATOSPHERE_BASE_DIR_PATH_KEY, "") )
			.thenThrow(new IllegalArgumentException("test error"));
		
		// initialize test
		try {
			contextHandler = new MeteorContextHandler( configMock, clientMock );
			fail( "No error occured!" );
		} catch ( IllegalArgumentException iae ){
			// catch only the right exception
			assertEquals( iae.getMessage(), "test error");
			
			// verify interactions
			verify( configMock, times(1) ).getString(WebFrontend.RESOURCEDIR, null);
			verify( configMock, times(1) ).getString(PactConfigConstants.STRATOSPHERE_BASE_DIR_PATH_KEY, "");
		}
	}
	
	/**
	 * Test update method. This method works asynchronous to the rest of this class. 
	 * Its initialize a new thread to start the job. For this reason we have to wait for an answer.
	 * If we have to wait longer than {@value TIME_LIMIT} milliseconds this test will fail.
	 * @throws QueryParserException from clientMock
	 * @throws IOException from clientMock
	 */
	@Test
	@SuppressWarnings("unchecked")
	public void updateWithErrorTest() throws QueryParserException, IOException {
		// throw an exception the context handler have to handle with
		doThrow( new QueryParserException("got test problem") ).when( clientMock );
		clientMock.execute( any(String.class) );
		
		// start time of test
		long initTime = System.currentTimeMillis();
		
		// test
		contextHandler.update( "test script" );
		
		// wait for an answer
		while ( !contextHandler.finishedWithError() )
			if ( System.currentTimeMillis() - initTime > TIME_LIMIT )
				fail( "Job runs longer than " + TIME_LIMIT + "ms. Cannot test results with errors." );
		
		// verify interaction with error servlet (cannot verify the order of static methods)
		verifyStatic( times(1) );
		ErrorServlet.reset();
		
		verifyStatic( times(1) );
		ErrorServlet.setError( any(String.class) );
		
		// verify there was no interactions with visualization servlets
		verify( visServ, times(0) ).update( any(String.class) );
		verify( outServ, times(0) ).update( any(List.class) );
	}
	
	/**
	 * Test a normal update without errors and verify interaction with web classes
	 * @throws QueryParserException from clientMock
	 * @throws IOException from clientMock
	 */
	@Test
	@SuppressWarnings("unchecked")
	public void updateTest() throws QueryParserException, IOException {
		// start time of test
		long initTime = System.currentTimeMillis();
		
		// start test
		contextHandler.update( "test script" );
		
		while( contextHandler.isInProgress() )
			if ( System.currentTimeMillis() - initTime > TIME_LIMIT )
				fail( "Job runs longer than " + TIME_LIMIT + "ms. Cannot test results without errors." );
		
		// first interaction with client and after that visServ
		InOrder inOrderOne = inOrder( clientMock, visServ );
		inOrderOne.verify( clientMock, times(1) ).execute( "test script" );
		inOrderOne.verify( visServ, times(1) ).update( any(String.class) );
		
		// first interaction with client and after that outServ
		InOrder inOrderTwo = inOrder( clientMock, outServ );
		inOrderTwo.verify( clientMock, times(1) ).execute( "test script" );
		inOrderTwo.verify( outServ, times(1) ).update( any(List.class) );
		
		// verify one interaction with reset
		verifyStatic( times(1) );
		ErrorServlet.reset();
		
		// verify zero interaction with new errors
		verifyStatic( times(0) );
		ErrorServlet.setError( any(String.class) );
	}
}
