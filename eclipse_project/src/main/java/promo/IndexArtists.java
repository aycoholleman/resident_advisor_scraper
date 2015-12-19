package promo;

import java.net.URL;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;

import org.domainobject.util.FileUtil;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

import com.mysql.jdbc.exceptions.jdbc4.MySQLIntegrityConstraintViolationException;

public class IndexArtists {

	public static void main(String[] args) throws Exception
	{
		IndexArtists ia = new IndexArtists();
		ia.index();
	}

	public void index() throws Exception
	{

		Class.forName("com.mysql.jdbc.Driver").newInstance();
		String dsn = "jdbc:mysql://localhost/promo";
		Connection conn = DriverManager.getConnection(dsn, "root", "");

		PreparedStatement ps = conn.prepareStatement("INSERT INTO artists(name) VALUES(?)");

		URL url = getClass().getResource("DJs.html");
		Document doc = Jsoup.parse(FileUtil.getContents(url, "UTF-8"));
		Elements elems = doc.select(".fl.pr8");
		int processed = 0;
		int indexed = 0;
		for (Element e : elems) {
			elems = e.select("a");
			for (Element a : elems) {
				processed++;
				String href = a.attr("href");
				if (href == null)
					continue;
				if (!href.startsWith("/dj/"))
					continue;
				String name = href.substring(4);
				if (name.length() > 0) {
					ps.setString(1, name);
					try {
						ps.executeUpdate();
						indexed++;
					}
					catch (MySQLIntegrityConstraintViolationException exc) {
						System.out.println("Duplicate: \"" + name + "\"");
					}
					ps.clearParameters();
				}
			}
		}
		System.out.println();
		System.out.println("Number of artists processed: " + processed);
		System.out.println("Number of artists indexed: " + indexed);
		System.out.println();
	}

}
