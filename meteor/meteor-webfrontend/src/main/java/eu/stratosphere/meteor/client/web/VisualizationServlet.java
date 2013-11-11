package eu.stratosphere.meteor.client.web;

import java.io.IOException;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

/**
 * 
 * This servlet creates content of the visualization frame modeled in
 * WebInterfaceServlet.
 * 
 * @author André Greiner-Petter
 * 
 */
public class VisualizationServlet extends AbstractServletGUI {
	private static final long serialVersionUID = -549622799004779557L;

	/**
	 * The link to configure visualization
	 */
	private final String link = "http://dopa.dima.tu-berlin.de/admin/structure/block/manage/block/3/configure%3Fdestination%3Dnode/20";
	private String jsonPath = null;

	/**
	 * Personal port for visualization
	 */
	private int port;

	/**
	 * Default constructor
	 */
	public VisualizationServlet( int port ) {
		super("visualization");
		
		// TODO figure out how we can determine the server name
		this.port = port;
		
		// add java scripts for visualization. uses dygraph and jquery libs
		addJavaScript("convert.js");
		addJavaScript("dygraph-combined.js");
		addJavaScript("jquery/misc/jquery.js");
		addJavaScript("jquery/misc/jquery.once.js");
		addJavaScript("jquery/misc/jquery.ba-bbq.js");
		addJavaScript("jquery/misc/jquery.cookie.js");
		addJavaScript("jquery/misc/ui/jquery.ui.core.min.js");
		addJavaScript("jquery/omega/js/jquery.formalize.js");		
		
		// normal style sheets
		addStylesheet("meteorFrontend.css");
		addStylesheet("visualContents.css");
	}

	/**
	 * Update contents with link to svg graphic visualization
	 * 
	 * @param jsonPath timeLineSource to svg
	 */
	public void update(String jsonPath) {
		this.jsonPath = jsonPath;
	}

	@Override
	protected void doPost(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
		doGet(request, response);
	}

	@Override
	protected void writePage(PrintWriter writer) {
		writer.println("<div class=\"main\">");
		writer.println("  <h1>Visualization</h1>");
		
		// if this json script is not visualizable
		if (jsonPath == null) {
			writer.println("  <div align=\"center\" class=\"visual\"><h2>nothing to visualize</h2></div>");
			writer.println("</div>");
			return;
		}
		
		// create timeline and link
		writer.println("  <div align=\"center\" class=\"visual\">");
		writer.print("    <div align=\"center\" id=\"graphdiv2\">");
		
		// writes an error message to page if writeTimeLine(writer) failed
		try { writeTimeLine( writer ); }
		catch ( UnsupportedEncodingException uee ){ writer.print("Encoding error of jsonPath occured! (No ISO-8859-1)"); }
	
		// footers and link
		writer.println("    </div>");
		writer.println("  </div>");
		writer.println("  <div class=\"footer\" aling=\"left\"><a href=\"" + link + "\" target=\"_blank\">Configure Visualization</a></div>");
		writer.println("</div>");
	}
	
	/**
	 * Writes java scripts to visualize the timeline of json script.
	 * @param writer
	 * @throws UnsupportedEncodingException if path cannot encoded with ISO-8859-1
	 */
	private void writeTimeLine( PrintWriter writer ) throws UnsupportedEncodingException {
		// internal script saves url to json
		writer.println("<script type=\"text/javascript\">");
		writer.print("var graph_data_url = \"http://localhost:" + port + "/hdfs?path=");
		writer.print(java.net.URLEncoder.encode(jsonPath, "ISO-8859-1"));
		writer.println("\";");
		writer.println("</script>");
		// internal script visualize timeline
		writer.println("<script type=\"text/javascript\" src=\"js/jsonTimeLine.js\"></script>");
	}
}
