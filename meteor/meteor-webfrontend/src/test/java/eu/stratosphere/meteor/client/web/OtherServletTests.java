package eu.stratosphere.meteor.client.web;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.MockitoAnnotations.initMocks;
import static org.powermock.api.mockito.PowerMockito.doCallRealMethod;
import static org.powermock.api.mockito.PowerMockito.doThrow;
import static org.powermock.api.mockito.PowerMockito.mockStatic;
import static org.powermock.api.mockito.PowerMockito.spy;
import static org.powermock.api.mockito.PowerMockito.verifyStatic;
import static org.powermock.api.mockito.PowerMockito.when;

import java.io.IOException;
import java.io.Writer;

import javax.servlet.ServletException;

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
import eu.stratosphere.meteor.client.common.FileSystemUtility;
import eu.stratosphere.meteor.client.common.MeteorContextHandler;
import eu.stratosphere.meteor.client.util.LocalTestsFileSystem;

/**
 * This class tests the rest of servlets (quite simple and little tests)
 *
 * @author André Greiner-Petter
 *
 */
@RunWith( PowerMockRunner.class )
@PrepareForTest( { MeteorContextHandler.class, FileSystemUtility.class, java.net.URLEncoder.class } )
public class OtherServletTests {
	
	/**
	 * Global message for tests
	 */
	private static final String MESSAGE = "TestPactVis123";
	
	/**
	 * Test these servlets
	 */
	private static PactVisServlet pactServlet;
	private static HDFSServlet hdfsServlet;
	private static VisualizationServlet visServlet;
	
	/**
	 * Request/Response
	 */
	private MockHttpServletRequest requestMock;
	private MockHttpServletResponse responseMock;
	
	/**
	 * Mock
	 */
	@Mock
	private ClientFrontend clientMock;
	
	/**
	 * Test path to real file
	 */
	private String testScriptPath = "file:///" + LocalTestsFileSystem.getResourcePath( "TestScripts/test.json" );
	private String testScript = LocalTestsFileSystem.getFileInput( "TestScripts/test.json" ).trim();
	
	/**
	 * @throws java.lang.Exception
	 */
	@BeforeClass
	public static void setUpBeforeClass() throws Exception {
		pactServlet = new PactVisServlet();
		hdfsServlet = new HDFSServlet();
		visServlet = new VisualizationServlet( 8081 );
	}

	/**
	 * @throws java.lang.Exception
	 */
	@AfterClass
	public static void tearDownAfterClass() throws Exception {
		pactServlet.destroy();
		hdfsServlet.destroy();
		visServlet.destroy();
	}

	/**
	 * @throws java.lang.Exception
	 */
	@Before
	public void setUp() throws Exception {
		// init static and non-static mocks
		initMocks( this );
		mockStatic( MeteorContextHandler.class );
		mockStatic( FileSystemUtility.class );
		
		// init request/response
		requestMock = new MockHttpServletRequest();
		responseMock = new MockHttpServletResponse();
		
		// default
		visServlet.update( null );
	}

	/**
	 * Test PactVisServlet
	 * @throws ServletException
	 * @throws IOException
	 */
	@Test
	public void pactVisTest() throws ServletException, IOException {
		// initialize interactions
		when( clientMock.getMetaData("pre.optmized.pact.plan.json") ).thenReturn( MESSAGE );
		when( MeteorContextHandler.getClient() ).thenReturn( clientMock );
		
		// start
		pactServlet.doGet( requestMock, responseMock );
		
		// verify an interaction
		verifyStatic( times(1) );
		MeteorContextHandler.getClient();
		
		// verify interaction with mock
		verify( clientMock, times(1) ).getMetaData( any(String.class) );
		
		// is our message in the page
		assertTrue( responseMock.getContentAsString().contains( MESSAGE ) );
	}
	
	/**
	 * Test normal HDFS servlet and load a real file.
	 * @throws Exception
	 */
	@Test
	public void hdfsServletTest() throws Exception {
		// create our mock to open real file
		spy( FileSystemUtility.class ); // spy class to call real method
		doCallRealMethod().when( FileSystemUtility.class ); // allow to call real method below
		FileSystemUtility.getFileContent(any(String.class), any(Writer.class));
		
		// add path to real file
		requestMock.addParameter( "path", testScriptPath );
		
		// load this real file by servlet
		hdfsServlet.doGet(requestMock, responseMock);
		
		// get input of real file loaded by servlet (trim to ignore whitespaces at start and end)
		String page = responseMock.getContentAsString().trim();
		
		// content must be equal to original file content
		assertEquals( page, testScript );
		
		// verify we call this method
		verifyStatic( times(1) );
		FileSystemUtility.getFileContent(any(String.class), any(Writer.class));
	}
	
	/**
	 * Test HDFS page when an error occurred in FileSystemUtility
	 * @throws Exception
	 */
	@Test
	public void hdfsErrorTest() throws Exception {
		// create our mock with error
		doThrow( new Exception("test exception") ).when( FileSystemUtility.class );
		FileSystemUtility.getFileContent(any(String.class), any(Writer.class));
		
		// add path to real file
		requestMock.addParameter( "path", testScriptPath );
		
		// load this real file by servlet
		hdfsServlet.doGet(requestMock, responseMock);
		
		// get page
		String page = responseMock.getContentAsString();
		
		// content must be equal to original file content
		assertTrue( page.contains( "Could not read file:\ntest exception" ) );
	}
	
	/**
	 * Test the difference between doGet and doPost. It have to be the same methods.
	 * @throws ServletException
	 * @throws IOException
	 */
	@Test
	public void visualizeGetPostTest() throws ServletException, IOException {
		// create tmp mocks
		MockHttpServletRequest requestMockTMP = new MockHttpServletRequest();
		MockHttpServletResponse responseMockTMP = new MockHttpServletResponse();
		
		// start both cases
		visServlet.doGet(requestMock, responseMock);
		visServlet.doPost(requestMockTMP, responseMockTMP);
		
		// find differences
		assertEquals( responseMock.getContentAsString(), responseMockTMP.getContentAsString() );
		assertEquals( responseMock.getStatus(), responseMockTMP.getStatus() );
		
		// I'm not sure how to test java scripts
	}
	
	/**
	 * Try to load a real JSON file by visualization servlet
	 * @throws ServletException
	 * @throws IOException
	 */
	@Test
	public void visualizeJSONTest() throws ServletException, IOException {
		// update path
		visServlet.update( testScriptPath );
		visServlet.doPost(requestMock, responseMock);
		
		// test if page contains that stuff
		assertTrue( responseMock.getContentAsString().contains("<div align=\"center\" id=\"graphdiv2\">") );
		assertTrue( responseMock.getContentAsString().contains("<script type=\"text/javascript\" src=\"js/jsonTimeLine.js\"></script>") );
	}

}
