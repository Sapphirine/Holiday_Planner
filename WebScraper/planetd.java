package WebScraper;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.InputStream;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.util.EntityUtils;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.nodes.TextNode;
import org.jsoup.select.Elements;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class planetd {
	private static String urlBase = "http://theplanetd.com/travel-blog/";
	private static int totalPages = 159;
	private static String blogBaseDir = "/tmp/travelblog/";
	private static String metaBaseDir = "/tmp/travelblog-meta/";
	private static ScraperUtil su = ScraperUtil.getInstance();
	private static Configuration config = null;
	private static Logger logger = LoggerFactory.getLogger(travelblog.class);

	private static void parseHtml(String blogUrl, Path outputFile, FSDataOutputStream mapbw, FileSystem fs) {
		try {
			logger.info("Extracting: " + blogUrl);
			HttpClient client = HttpClientBuilder.create().build();
			HttpGet httpGet = new HttpGet(blogUrl);
	        HttpResponse response = client.execute(httpGet);
	        HttpEntity entity = response.getEntity();
	        String responseString = EntityUtils.toString(entity, "UTF-8");
	        Document doc = Jsoup.parse(responseString);
	        Elements list = doc.select("title");
	        Element titleElement = list.size() > 0 ? list.first() : null;
	        String title = list.size() > 0 ? titleElement.ownText():"Unknown";	        
	        Elements plist = doc.select("span[style=\"color: #000000;\"]");
	        if (plist.size() == 0)
	        	plist = doc.select("p");
	        String buf = "";
	        for(Element e:plist) {
	        	String text = "";
                for (TextNode tn : e.textNodes()) {
                    String tagText = tn.text().trim();
                    if (tagText.length() > 0) {
                        text += tagText + " ";
                    }
                }
                buf = su.removeStopWords(text);
	        }
	        if (buf.length() > 50) {
	        	mapbw.writeUTF("{\"url\":\"" + blogUrl + "\",\"title\": \"" + title + "\",\"file\":\"" + outputFile + "\",\"continent\": \"\","
		        		+ "\"country\":\"\", \"state\":\"\",\"area\":\"\",\"topics\":\"\"},\n");
	        	if (fs.exists(outputFile))
	        		fs.delete(outputFile);
	        	FSDataOutputStream fin = fs.create(outputFile);
	        	fin.writeUTF(buf.toString() + "\n");
	        	fin.close();
	        }
	        EntityUtils.consume(entity);
		} catch(Exception e) {
			e.printStackTrace();
		}
	}
	
	public static void main(String[] args) {
		su.initStopWords();
		config = su.setupHdfsConfig("/opt/hadoop-2.7.3");
		su.createHdfsDirectory(config, blogBaseDir);
		su.createHdfsDirectory(config, metaBaseDir);
		List<String> urlList = new ArrayList<>();
		
		try {
			FileSystem fs = FileSystem.get(config);
			for (int i = 1; i <= 1; i ++ ) {
				String url = urlBase;
				if ( i > 1)
					url += "page/" + i + "/";
				//System.out.println(url);
				HttpClient client = HttpClientBuilder.create().build();
				HttpGet httpGet = new HttpGet(url);
		        HttpResponse response = client.execute(httpGet);
		        HttpEntity entity = response.getEntity();
		        String responseString = EntityUtils.toString(entity, "UTF-8");
				Document doc = Jsoup.parse(responseString);
				Elements links = doc.select("a[class=button]"); // a with title
				links.forEach(e -> {
					String href = e.attr("href");
					urlList.add(href);
					//System.out.println(href);
				});
				EntityUtils.consume(entity);
			}
			Path file = new Path(metaBaseDir + "planetd-mapping.json");
			if (fs.exists(file))
				fs.delete(file);
			FSDataOutputStream bw = fs.create(file);
			int cnt = 0;
			for(String blogUrl : urlList) {
				cnt ++ ;
				Path blogOutput = new Path(blogBaseDir + "planetd/blog_" + cnt + ".txt");
				parseHtml(blogUrl, blogOutput, bw, fs);
			};
			bw.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}