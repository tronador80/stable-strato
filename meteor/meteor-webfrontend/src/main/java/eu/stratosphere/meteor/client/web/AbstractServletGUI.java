package eu.stratosphere.meteor.client.web;

import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.List;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

/**
 * 
 * The abstract servlet for each frame servlet. Build up the html page for each frame.
 * 
 * @author André Greiner-Petter
 *
 */
public abstract class AbstractServletGUI extends HttpServlet {
	
	/**
	 * Generated UID and title of frame
	 */
	private static final long serialVersionUID = -8142279843935070159L;
	private String title;
	
	/**
	 * Javascripts and CSS files
	 */
	private List<String> javaScripts;
	private List<String> stylesheets;
	
	/**
	 * Construct a new servlet with given title.
	 * @param title
	 */
	public AbstractServletGUI( String title ){
		this.title = title;
		this.javaScripts = new ArrayList<String>();
		this.stylesheets = new ArrayList<String>();
	}
	
	/**
	 * Add a javascript file from folder resources/web-doc/js/ with given name
	 * @param name of js file
	 */
	public void addJavaScript( String name ){
		this.javaScripts.add( "js/" + name );
	}
	
	/**
	 * Add a stylesheet css file from folder resources/web-doc/css/ with given name
	 * @param name of css file
	 */
	public void addStylesheet( String name ){
		this.stylesheets.add( "css/" + name );
	}
	
	/**
	 * Build up a based structure for each website with doctype, header and all imports of js and css files.
	 * Body filled by subclasses.
	 */
	@Override
	protected void doGet( HttpServletRequest request, HttpServletResponse response ) throws ServletException, IOException{		
		response.setContentType("text/html");
		response.setStatus( HttpServletResponse.SC_OK );
		
		PrintWriter writer = response.getWriter();
		
		writer.println("<!DOCTYPE html PUBLIC \"-//W3C//DTD HTML 4.01 Transitional//EN\" \"http://www.w3.org/TR/html4/loose.dtd\">");
		writer.println("<html>");
		
		writer.println("<head>");
		writer.println("  <title>" + title + "</title>");
		writer.println("  <meta http-equiv=\"content-type\" content=\"text/html; charset=UTF-8\"/>");
		
		// javascripts
		for ( int i = 0; i < javaScripts.size(); i++ )
			writer.println("  <script type=\"text/javascript\" src=\""+ javaScripts.get(i) +"\"></script>");
		
		// stylesheets
		for ( int i = 0; i < stylesheets.size(); i++ )
			writer.println("  <link rel=\"stylesheet\" media=\"screen\" href=\""+ stylesheets.get(i) +"\">");
		
		writer.println("</head>");
		writer.println("<body>");
		
		// print body
		writePage( writer );
		
		// print footer
		writer.println("</body>");
		writer.println("</html>");
	}
	
	/**
	 * Write the body of html frame
	 * @param writer
	 */
	protected abstract void writePage( PrintWriter writer );
}
