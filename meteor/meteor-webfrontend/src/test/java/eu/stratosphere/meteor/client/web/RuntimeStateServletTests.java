/**
 * ------------------------ BE VERY CAREFUL WITH ECLIPSE ------------------------
 * Eclipse got a bug with PrepareForTest static classes in java 7. It doesn't run
 * the test. To fix that problem in eclipse start the test classes with VM parameter:
 * 		-XX:-UseSplitVerifier
 * 
 * Another solution is to use org.powermock modules version 1.5 or earlier. This
 * project should import this version to fix the problem.
 * 
 * For more informations please contact: 
 * 		André Greiner-Petter <andre.greiner-petter@math.tu-berlin.de>
 * ------------------------------------------------------------------------------
 */
package eu.stratosphere.meteor.client.web;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.MockitoAnnotations.initMocks;
import static org.powermock.api.mockito.PowerMockito.mockStatic;
import static org.powermock.api.mockito.PowerMockito.verifyStatic;
import static org.powermock.api.mockito.PowerMockito.when;

import java.io.IOException;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletResponse;

import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import org.springframework.mock.web.MockHttpServletRequest;
import org.springframework.mock.web.MockHttpServletResponse;

import eu.stratosphere.meteor.client.ClientFrontend;
import eu.stratosphere.meteor.client.common.MeteorContextHandler;

/**
 * Test the runtime state servlet. This shows the status of job if any job is running.
 * It interacts with MeteorContextHandler to get informations about the job states.
 * In this test cases the class interact with mockitos. We test job status of ClientFrontend
 * in other test classes.
 *
 * @author André Greiner-Petter
 *
 */
@RunWith( PowerMockRunner.class )
@PrepareForTest( { MeteorContextHandler.class } )
public class RuntimeStateServletTests {
	
	/**
	 * Initialize constant for job status
	 */
	private static final String defaultStatus = "No states for a while!";
	
	/**
	 * Servlet
	 */
	private static RuntimeStateServlet runtimeServlet;
	
	/**
	 * Request/Response mocks
	 */
	private MockHttpServletRequest requestMock;
	private MockHttpServletResponse responseMock;
	
	/**
	 * Mock to fake client
	 */
	@Mock
	private ClientFrontend clientMock;
	
	/**
	 * @throws java.lang.Exception
	 */
	@BeforeClass
	public static void setUpBeforeClass() throws Exception {
		runtimeServlet = new RuntimeStateServlet();
	}

	/**
	 * @throws java.lang.Exception
	 */
	@AfterClass
	public static void tearDownAfterClass() throws Exception {
		runtimeServlet.destroy();
	}

	/**
	 * Initialize mocks and define return values of mock methods
	 * @throws java.lang.Exception
	 */
	@Before
	public void setUp() throws Exception {
		// initialize mocks
		initMocks( this );
		mockStatic( MeteorContextHandler.class );
		
		// define returns and default interactions
		when( clientMock.getJobStates() ).thenReturn( defaultStatus );
		when( MeteorContextHandler.getClient() ).thenReturn( clientMock );
		when( MeteorContextHandler.isInProgress() ).thenReturn( false );
		
		// initialize requests/responses
		requestMock = new MockHttpServletRequest();
		responseMock = new MockHttpServletResponse();
		
		// viewMode is 'view' by default
		// null means job is in process
		requestMock.setParameter("viewMode", "view");
	}

	/**
	 * Test default case
	 * @throws ServletException
	 * @throws IOException
	 */
	@Test
	public void defaultTest() throws ServletException, IOException {
		// start
		runtimeServlet.doGet(requestMock, responseMock);
		
		// verify isInProgress
		verifyStatic( times(1) );
		MeteorContextHandler.isInProgress();
		
		// verify getClient
		verifyStatic( times(1) );
		MeteorContextHandler.getClient();
		
		// verify load job states
		verify( clientMock, times(1) ).getJobStates();
		
		// normal background
		assertEquals( responseMock.getStatus(), HttpServletResponse.SC_OK );
		assertEquals( responseMock.getContentType(), "text/html" );
		assertTrue( responseMock.getHeaderNames().isEmpty() );
		
		// message is there
		assertTrue( responseMock.getContentAsString().contains( defaultStatus ) );
		
		// this page do not reload other pages by default
		assertFalse( responseMock.getContentAsString().contains("<script type=\"text/javascript\">window.close();</script>") );
	}
	
	/**
	 * Nothing changes if the job is running but other parameters are still default
	 * @throws IOException 
	 * @throws ServletException 
	 */
	@Test
	public void inProgressTest() throws ServletException, IOException {
		// change the job to isInProgress
		when( MeteorContextHandler.isInProgress() ).thenReturn( true );
		// test default
		defaultTest();
	}
	
	/**
	 * Test the page while job is in progress and after this job finished.
	 * @throws ServletException
	 * @throws IOException
	 */
	@Test
	public void processModeTest() throws ServletException, IOException {
		// viewmode is process and job is in progress
		when( MeteorContextHandler.isInProgress() ).thenReturn( true );
		requestMock = new MockHttpServletRequest(); // parameters are null
		
		// first start
		runtimeServlet.doGet(requestMock, responseMock);
		
		// refreshs itself
		assertEquals( responseMock.getHeader("Refresh"), "2" );
		
		// this page do not reload other pages by default
		assertFalse( responseMock.getContentAsString().contains("opener.location.reload();") );
		assertFalse( responseMock.getContentAsString().contains("window.close();") );
		
		// simulate reload with new progress value
		when( MeteorContextHandler.isInProgress() ).thenReturn( false );
		
		// second start after job finished
		runtimeServlet.doGet(requestMock, responseMock);
		
		// page must contains close and reload operations
		String page = responseMock.getContentAsString();
		assertTrue( page.contains("<script type=\"text/javascript\">opener.location.reload();</script>") );
		assertTrue( page.contains("<script type=\"text/javascript\">window.close();</script>") );
		
		// verify interactions twice
		verifyStatic( times(2) );
		MeteorContextHandler.isInProgress();
		
		// verify interactions twice
		verify( clientMock, times(2) ).getJobStates();
	}

}
