package eu.stratosphere.meteor.client;

import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.when;
import static org.mockito.MockitoAnnotations.initMocks;
import static org.powermock.api.mockito.PowerMockito.mockStatic;
import static org.powermock.api.mockito.PowerMockito.whenNew;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;

import org.eclipse.jetty.util.component.AbstractLifeCycle;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.contrib.java.lang.system.ExpectedSystemExit;
import org.junit.contrib.java.lang.system.StandardErrorStreamLog;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import eu.stratosphere.meteor.client.common.MeteorContextHandler;
import eu.stratosphere.nephele.configuration.Configuration;
import eu.stratosphere.nephele.configuration.GlobalConfiguration;
import eu.stratosphere.pact.common.util.PactConfigConstants;

/**
 * This class tests main class WebFrontend. Its just tests the errors and never
 * start a real server.
 *
 * @author André Greiner-Petter
 *
 */
@RunWith( PowerMockRunner.class )
@PrepareForTest( { GlobalConfiguration.class, WebFrontend.class } )
public class WebFrontendTests {
	
	/**
	 * Create a default port for tests
	 */
	private static final int TEST_PORT = 8081;
	
	/**
	 * To interrupt the console for System.out/System.err initialize new streams
	 */
	private static final ByteArrayOutputStream outContent = new ByteArrayOutputStream();
	private static final ByteArrayOutputStream errContent = new ByteArrayOutputStream();
	
	/**
	 * Save original streams for regular tear down
	 */
	private static final PrintStream OUT = System.out;
	private static final PrintStream ERR = System.err;
	
	/**
	 * Create rule for error messages
	 */
	@Rule
	public final StandardErrorStreamLog errorLog = new StandardErrorStreamLog();
	
	/**
	 * Create rule for system exits to test the program stops with a status code
	 */
	@Rule
	public final ExpectedSystemExit exitLog = ExpectedSystemExit.none();
	
	/**
	 * Initialize mocks
	 */
	@Mock
	Configuration configMock;
	
	@Mock
	MeteorContextHandler contextHandlerMock;
	
	@Mock
	ClientFrontend clientMock;
	
	/**
	 * Build up mocks and initialize methods for mocks
	 * @throws Exception
	 */
	@Before
	public void setUp() throws Exception {
		initMocks( this );
		
		// static mocks
		mockStatic( GlobalConfiguration.class );
		mockStatic( AbstractLifeCycle.class );
		
		// return mocks
		whenNew( ClientFrontend.class ).withArguments( any( String[].class ) ).thenReturn( clientMock );
		whenNew( MeteorContextHandler.class ).withArguments( configMock, clientMock ).thenReturn( contextHandlerMock );
		
		// return fakes
		when( GlobalConfiguration.getConfiguration() ).thenReturn( configMock );
		when( configMock.getInteger( PactConfigConstants.WEB_FRONTEND_PORT_KEY,
				PactConfigConstants.DEFAULT_WEB_FRONTEND_PORT ) ).thenReturn( TEST_PORT );
		
		// stop console output for tests
		System.setOut( new PrintStream( outContent ) );
		System.setErr( new PrintStream( errContent ) );
	}
	
	/**
	 * Close streams and reset the console after tests finished
	 * @throws IOException
	 */
	@AfterClass
	public static void tearDownAfterClass() throws IOException {
		outContent.close();
		errContent.close();
		System.setErr( ERR );
		System.setOut( OUT );
	}
	
	/**
	 * If we try to start the server without arguments we expected a NullPointerException
	 * @throws IOException
	 */
	@Test ( expected = NullPointerException.class )
	public void nullArgumentExceptionTest() throws IOException {
		WebFrontend.main( null );
	}
	
	/**
	 * To start the server without arguments (not null) this test expected an error message on console
	 * and a system exit with status code 1
	 * @throws Exception 
	 */
	@Test
	public void noArgsErrorTest() throws Exception {	
		String[] testInput = new String[]{""};
		
		// expected System.exit(1)
		exitLog.expectSystemExitWithStatus(1);
		WebFrontend.main( testInput );
		
		// expected an error message for console like that
		assertTrue( errorLog.getLog().contains( "Error: Configuration directory must be specified." ) );
	}
	
	/**
	 * To start the server with wrong arguments this test expected the same like the test with no arguments
	 * @throws IOException 
	 */
	@Test
	public void wrongArgumentTest() throws IOException{
		String[] testInput = new String[]{"-CONFIG", "in/hallo/test"};
		
		// expected System.exit(1)
		exitLog.expectSystemExitWithStatus(1);
		WebFrontend.main( testInput );
		
		// expected an error message like that
		assertTrue( errorLog.getLog().contains( "Error: Configuration directory must be specified." ) );
	}
}
