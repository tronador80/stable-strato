package eu.stratosphere.meteor.client.web;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.times;
import static org.powermock.api.mockito.PowerMockito.mockStatic;
import static org.powermock.api.mockito.PowerMockito.verifyStatic;

import java.io.IOException;

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

/**
 * This test class creates original analysis servlet combined with mocket requests and responses to test
 * reactions of this servlets. You will find all servlets in
 * 		eu.stratosphere.meteor.client.web
 *
 * @author André Greiner-Petter
 */
@RunWith( PowerMockRunner.class )
@PrepareForTest( { MeteorContextHandler.class, FileSystemUtility.class } )
public class AnalysisServletTests {
	
	/**
	 * Creates static servlets to initialize it before tests and destroy all after
	 */
	private static AnalysisServlet analysisServlet;
	
	/**
	 * Create mockets of requests and responses
	 */
	private MockHttpServletRequest requestMock;
	private MockHttpServletResponse responseMock;
	
	/**
	 * Our fake meteor script
	 */
	private final String origScript = "placeholder for some meteor stuff";
	
	/**
	 * Build up static mocket and servlets
	 * @throws ServletException 
	 */
	@BeforeClass
	public static void setUpBeforeClass() throws ServletException {
		analysisServlet = new AnalysisServlet();
	}
	
	/**
	 * Destroy this servlet after finished tests.
	 */
	@AfterClass
	public static void destroyAfterClass(){
		analysisServlet.destroy();
	}
	
	/**
	 * Reinitialize requests and responses before each test
	 */
	@Before
	public void setUpMocks(){
		// create static mocks
		mockStatic( MeteorContextHandler.class );
		mockStatic( FileSystemUtility.class );
		
		// create requests/response
		requestMock = new MockHttpServletRequest();
		responseMock = new MockHttpServletResponse();
	}
	
	/**
	 * Test whether redirection URL is null if there are no parameters 
	 * contains in the request
	 * @throws ServletException
	 * @throws IOException
	 */
	@Test
	public void redirectionNullTest() throws ServletException, IOException {
		analysisServlet.doPost(requestMock, responseMock);
		
		// no redirection, response status accepted
		assertNull( responseMock.getRedirectedUrl() );
		assertEquals( responseMock.getStatus(), HttpServletResponse.SC_ACCEPTED );
		
		// verify no interactions with update
		verifyStatic( times(0) );
		MeteorContextHandler.update( any(String.class) );
	}
	
	/**
	 * We expected a redirection URL to parent page if there are a
	 * parameter in the request.
	 * @throws Exception 
	 */
	@Test
	public void pageContentTest() throws Exception {
		// start test procedure
		requestMock.setParameter("meteorInput", origScript);
		analysisServlet.doPost(requestMock, responseMock);
		
		// verify request called update from MeteorContextHandler
		verifyStatic( times(1) );
		MeteorContextHandler.update( origScript );
		
		// assert redirection to parent page
		assertEquals( "", responseMock.getRedirectedUrl() );
		
		// accepted request
		assertEquals( responseMock.getStatus(), HttpServletResponse.SC_ACCEPTED );
		
		// reload page and get new input
		analysisServlet.doGet(requestMock, responseMock);
		
		// verify there was no more interaction then first time from doPost
		verifyStatic( times(1) );
		MeteorContextHandler.update( origScript );
		
		// test whether our page contains the script and this servlet imports meteorFrontend.css
		analysisServlet.doGet(requestMock, responseMock);
		assertTrue( responseMock.getContentAsString().contains(origScript) );
		assertTrue( responseMock.getContentAsString().contains( 
				"<link rel=\"stylesheet\" media=\"screen\" href=\"css/meteorFrontend.css\">" 
				) );
	}
}
