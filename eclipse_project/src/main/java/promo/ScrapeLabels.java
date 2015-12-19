package promo;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;

import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.nodes.Node;
import org.jsoup.nodes.TextNode;
import org.jsoup.select.Elements;

public class ScrapeLabels {

	public static void main(String[] args) throws Exception
	{

		Class.forName("com.mysql.jdbc.Driver").newInstance();
		String dsn = "jdbc:mysql://localhost/promo";
		Connection conn = DriverManager.getConnection(dsn, "root", "");

		PreparedStatement ps = conn.prepareStatement("INSERT INTO labels VALUES(?,?,?,?,?,?,?,?)");

		int i = 282;
		while (i < 12500) {
			String url = "http://www.residentadvisor.net/record-label.aspx?id=" + i;
			Document doc = null;
			try {
				org.jsoup.Connection connection = Jsoup.connect(url);
				connection.timeout(15000);
				doc = connection.get();
			}
			catch (IOException e) {
				if (e.getMessage().equals("Read timed out")) {
					System.out.println();
					System.out.println("Retrying for id " + i);
					Thread.sleep(2000);
					continue;
				}
				throw e;
			}

			if (i % 100 == 0) {
				System.out.println();
				System.out.println("Processed: " + i);
			}
			else {
				System.out.print('.');
			}

			try {
				Elements elems = doc.select(".fl.col4");
				if (elems.isEmpty()) {
					throw new Exception("Missing .fl.col4 for id " + i);
				}
				Element e = elems.get(0);
				String labelDescr = e.text();

				elems = doc.select("h1");
				e = elems.get(0);
				String labelName = e.text();

				elems = doc.select("#detail");
				e = elems.get(0);
				elems = e.select("li");
				String website = getWebsite(elems);
				String email = getEmail(elems);
				String location = getLocation(elems);
				String soundcloud = getSoundCloudUrl(elems);
				String facebook = getFacebookUrl(elems);

				ps.setInt(1, i);
				ps.setString(2, labelName);
				ps.setString(3, labelDescr);
				ps.setString(4, location);
				ps.setString(5, email);
				ps.setString(6, website);
				ps.setString(7, soundcloud);
				ps.setString(8, facebook);

				ps.executeUpdate();
				ps.clearParameters();

			}
			catch (Throwable t) {
				System.out.println();
				System.out.println(t);
			}
			finally {
				i++;
			}

			Thread.sleep(1200);

		}
	}

	private static String getEmail(Elements lis)
	{
		for (Element li : lis) {
			Elements divs = li.select("div");
			if (divs.size() == 0)
				continue;
			Element div = divs.get(0);
			if (div.text().trim().equals("Email /")) {
				for (Node n : li.childNodes()) {
					if (n instanceof TextNode) {
						return ((TextNode) n).text();
					}
				}
				return li.text();
			}
		}
		return null;
	}

	private static String getLocation(Elements lis)
	{
		for (Element li : lis) {
			Elements divs = li.select("div");
			if (divs.size() == 0)
				continue;
			Element div = divs.get(0);
			if (div.text().trim().equals("Location /")) {
				for (Node n : li.childNodes()) {
					if (n instanceof TextNode) {
						return ((TextNode) n).text();
					}
				}
				String s = li.text().substring("Location /".length());
				StringBuilder sb = new StringBuilder(s.length());
				for (int i = 0; i < s.length(); ++i) {
					if (s.codePointAt(i) == 160)
						continue;
					sb.append(s.charAt(i));
				}
				return sb.toString().trim();
			}
		}
		return null;
	}

	private static String getWebsite(Elements lis)
	{
		for (Element li : lis) {
			Elements links = li.select("a");
			for (Element a : links) {
				if (a.text().trim().equals("Website")) {
					return a.attr("href");
				}
			}
		}
		return null;
	}

	private static String getSoundCloudUrl(Elements lis)
	{
		for (Element li : lis) {
			Elements links = li.select("a");
			for (Element a : links) {
				if (a.text().trim().equals("Soundcloud")) {
					return a.attr("href");
				}
			}
		}
		return null;
	}

	private static String getFacebookUrl(Elements lis)
	{
		for (Element li : lis) {
			Elements links = li.select("a");
			for (Element a : links) {
				if (a.text().trim().equals("Facebook")) {
					return a.attr("href");
				}
			}
		}
		return null;
	}

}
