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
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertFalse;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.times;
import static org.powermock.api.mockito.PowerMockito.doCallRealMethod;
import static org.powermock.api.mockito.PowerMockito.doThrow;
import static org.powermock.api.mockito.PowerMockito.mockStatic;
import static org.powermock.api.mockito.PowerMockito.spy;
import static org.powermock.api.mockito.PowerMockito.verifyStatic;
import static org.powermock.api.mockito.PowerMockito.when;

import java.io.IOException;
import java.io.Writer;
import java.util.LinkedList;
import java.util.List;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletResponse;

import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import org.springframework.mock.web.MockHttpServletRequest;
import org.springframework.mock.web.MockHttpServletResponse;

import eu.stratosphere.meteor.client.common.FileSystemUtility;
import eu.stratosphere.meteor.client.common.MeteorContextHandler;
import eu.stratosphere.meteor.client.util.LocalTestsFileSystem;

/**
 *
 * @author André Greiner-Petter
 *
 */
@RunWith( PowerMockRunner.class )
@PrepareForTest( { MeteorContextHandler.class, FileSystemUtility.class, ErrorServlet.class } )
public class OutputServletTest {
	
	/**
	 * Test this Servlet
	 */
	private static OutputServlet outputServlet;
	
	/**
	 * Mockitos
	 */
	private MockHttpServletRequest requestMock;
	private MockHttpServletResponse responseMock;
	
	/**
	 * Test path to real file
	 */
	private final String testScriptPath = "file:///" + LocalTestsFileSystem.getResourcePath( "TestScripts/test.json" );
	
	/**
	 * @throws java.lang.Exception
	 */
	@BeforeClass
	public static void setUpBeforeClass() throws Exception {
		outputServlet = new OutputServlet();
	}
	
	/**
	 * @throws java.lang.Exception
	 */
	@AfterClass
	public static void tearDownAfterClass() throws Exception {
		outputServlet.destroy();
	}

	/**
	 * @throws java.lang.Exception
	 */
	@Before
	public void setUp() throws Exception {		
		// create mockitos
		mockStatic( FileSystemUtility.class );
		
		mockStatic( MeteorContextHandler.class );
		when( MeteorContextHandler.isInProgress() ).thenReturn( false );
		when( MeteorContextHandler.finishedWithError() ).thenReturn( false );
		
		// new responses/requests
		requestMock = new MockHttpServletRequest();
		responseMock = new MockHttpServletResponse();
	}
	
	/**
	 * Test redirection, status and content type of empty page
	 * @throws ServletException
	 * @throws IOException
	 */
	@Test
	public void emptyRequestTest() throws ServletException, IOException {
		outputServlet.doGet(requestMock, responseMock);
		
		// tests
		assertNull( responseMock.getRedirectedUrl() );
		assertEquals( responseMock.getStatus(), HttpServletResponse.SC_OK );
		assertEquals( responseMock.getContentType(), "text/html" );
		
		// verify there was an interaction with isInProgress()
		verifyStatic( times(1) );
		MeteorContextHandler.isInProgress();
	}
	
	/**
	 * Load empty page search for style sheets imports
	 * @throws ServletException
	 * @throws IOException
	 */
	@Test
	public void importStyleSheetsTest() throws ServletException, IOException {		
		// load page
		outputServlet.doGet(requestMock, responseMock);
		
		// get code
		String page = responseMock.getContentAsString();
		
		// find imports of css files
		assertTrue( page.contains( "<link rel=\"stylesheet\" media=\"screen\" href=\"css/meteorFrontend.css\">" ) );
		assertTrue( page.contains( "<link rel=\"stylesheet\" media=\"screen\" href=\"css/jsonHighlightBrushes.css\">" ) );
		
		// verify there was exactly one interaction with isInProgress()
		verifyStatic( times(1) );
		MeteorContextHandler.isInProgress();
	}
	
	/**
	 * Load a real script path to page and test references
	 * @throws Exception
	 */
	@Test
	public void scriptTest() throws Exception {
		// create our mock to open real file
		spy( FileSystemUtility.class ); // spy class to call real method
		doCallRealMethod().when( FileSystemUtility.class ); // allow to call real method below
		FileSystemUtility.getFileContent(any(String.class), any(Writer.class));
		
		// create test scenario with 2 scripts
		List<String> output = new LinkedList<String>();
		output.add( "file:///FAKE" ); // placeholder for fake script
		output.add( testScriptPath ); // real json
		outputServlet.update(output);  // update servlet with new inputs
		
		// want to test testHighlighting script
		requestMock.addParameter("selectionidx", "1");
		
		// start test
		outputServlet.doGet(requestMock, responseMock);
		
		// verify we got an interaction with getFileContent
		verifyStatic( times(1) );
		FileSystemUtility.getFileContent(any(String.class), any(Writer.class));
		
		// get page
		String page = responseMock.getContentAsString();
		
		// expected file content in our page
		assertTrue( page.contains("\"timestamp\": \"2011-01-14\",") );
        assertTrue( page.contains("\"volume\": \"227601331\",") );
        assertTrue( page.contains("\"count\": \"63.82422227\"") );
	}
	
	/**
	 * Test with job in progress
	 * @throws ServletException
	 * @throws IOException
	 */
	@Test
	public void progressTest() throws ServletException, IOException{
		// initialization
		when( MeteorContextHandler.isInProgress() ).thenReturn( true );
		
		// start
		outputServlet.doGet(requestMock, responseMock);
		
		// verify there was an interaction
		verifyStatic( times(1) );
		MeteorContextHandler.isInProgress();
		
		// page
		String page = responseMock.getContentAsString();
		
		// page contains a progress bar
		assertTrue( page.contains( "<progress style=\"width:50%\"></progress>" ) );
		
		// page opens a new window to processes overview
		assertTrue( page.contains("<script type=\"text/javascript\">window.open(\"/runtime?viewMode=process\", \"_blank\");</script>") );
		
		// no error messages in there
		assertFalse( page.contains("Job failed.") );
	}
	
	/**
	 * Test with error occurred in mockJob
	 * @throws ServletException
	 * @throws IOException
	 */
	@Test
	public void finishedWithErrorTest() throws ServletException, IOException {
		// test case
		when( MeteorContextHandler.finishedWithError() ).thenReturn( true );
		
		// start
		outputServlet.doGet(requestMock, responseMock);
		
		// verify interaction
		verifyStatic( times(1) );
		MeteorContextHandler.finishedWithError();
		
		// visible error message
		String page = responseMock.getContentAsString();
		assertTrue( page.contains("Job failed. Please check /error sub page for more informations.") );
	}
	
	/**
	 * Test with static class FileSystemUtility throws an error.
	 * @throws Exception
	 */
	@Test
	public void highlightingErrorTest() throws Exception {				
		// initialize static to verify a interaction with error servlet
		mockStatic( ErrorServlet.class );
		
		// throw an error if any test calls getFileContent
		doThrow( new Exception("test error") ).when( FileSystemUtility.class );
		FileSystemUtility.getFileContent(any(String.class), any(Writer.class));
		
		// need a list of outputs to call getFileContent
		List<String> output = new LinkedList<String>();
		output.add( "file:///FAKE" );
		outputServlet.update(output);
		
		// start
		outputServlet.doGet(requestMock, responseMock);
		
		// verify interaction with ErrorServlet
		verifyStatic( times(1) );
		ErrorServlet.setError( any(String.class) );
		
		// verify error message with hint
		String page = responseMock.getContentAsString();
		assertTrue( page.contains("\"Error accessing HDFS. See /error sub pages for more informations.\";") );
	}
}
