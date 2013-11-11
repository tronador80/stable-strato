package eu.stratosphere.meteor.client.web;

import static org.junit.Assert.*;

import java.io.IOException;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletResponse;

import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.springframework.mock.web.MockHttpServletRequest;
import org.springframework.mock.web.MockHttpServletResponse;

/**
 * This class tests the simple error servlet. This servlet shows error messages if any error occurred
 * in processes.
 * It suppress static-access warnings cause ErrorServlet handles with static methods but we test
 * a real instance of this class.
 *
 * @author André Greiner-Petter
 *
 */
@SuppressWarnings("static-access")
public class ErrorServletTests {
	
	/**
	 * Error servlet
	 */
	private static ErrorServlet errorServlet;
	
	/**
	 * Mockitos
	 */
	private MockHttpServletRequest requestMock;
	private MockHttpServletResponse responseMock;
	
	/**
	 * Initialize
	 * @throws Exception
	 */
	@BeforeClass
	public static void setUpBeforeClass() {
		errorServlet = new ErrorServlet();
	}

	/**
	 * Destroy after tests
	 */
	@AfterClass
	public static void tearDownAfterClass() {
		errorServlet.destroy();
	}

	/**
	 * Reload mockitos and resets error message of servlet
	 */
	@Before
	public void setUp() {
		requestMock = new MockHttpServletRequest();
		responseMock = new MockHttpServletResponse();
		errorServlet.reset();
	}

	/**
	 * Test default error servlet
	 * @throws ServletException
	 * @throws IOException
	 */
	@Test
	public void defaultErrorTest() throws ServletException, IOException {
		errorServlet.doGet(requestMock, responseMock);
		
		// status OK, content type is text and the page contains 'No error!'
		assertEquals( responseMock.getStatus(), HttpServletResponse.SC_OK );
		assertEquals( responseMock.getContentType(), "text/html" );
		assertTrue( responseMock.getContentAsString().contains("No error!") );
	}
	
	/**
	 * Test imports
	 * @throws ServletException
	 * @throws IOException
	 */
	@Test
	public void importTest() throws ServletException, IOException {
		errorServlet.doGet(requestMock, responseMock);
		
		// get page
		String page = responseMock.getContentAsString();
		
		// find imports
		assertTrue( page.contains("<link rel=\"stylesheet\" media=\"screen\" href=\"css/jsonHighlightBrushes.css\">") );
		assertTrue( page.contains("<link rel=\"stylesheet\" media=\"screen\" href=\"css/meteorFrontend.css\">") );
	}
	
	/**
	 * Set error message and search this message in the page
	 * @throws ServletException
	 * @throws IOException
	 */
	@Test
	public void errorMessageTest() throws ServletException, IOException {
		String errorMessage = "test error occurred";
		
		// set error and reload page
		errorServlet.setError( errorMessage );
		errorServlet.doGet(requestMock, responseMock);
		
		// is new message in there
		assertTrue( responseMock.getContentAsString().contains( errorMessage ) );
	}

}
