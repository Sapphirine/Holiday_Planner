package WebScraper;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.StringReader;
import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.util.EntityUtils;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.core.LowerCaseTokenizer;
import org.apache.lucene.analysis.core.StopFilter;
import org.apache.lucene.analysis.core.WhitespaceTokenizer;
import org.apache.lucene.analysis.miscellaneous.LengthFilter;
import org.apache.lucene.analysis.pattern.PatternReplaceFilter;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.analysis.util.CharArraySet;
import org.apache.lucene.util.Version;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.nodes.Entities.EscapeMode;
import org.jsoup.nodes.Node;
import org.jsoup.nodes.TextNode;
import org.jsoup.select.Elements;

public class travelblog {
	private static String urlBase = "http://www.travelblog.org/";
	private static String urlRealBase = "http://www.travelblog.org";
	private static String blogBaseDir = "/tmp/travelblog/";
	private static String metaBaseDir = "/tmp/travelblog-meta/";
	private static ScraperUtil su = ScraperUtil.getInstance();
	private static CharArraySet stopSet = CharArraySet.copy(Version.LUCENE_CURRENT, StandardAnalyzer.STOP_WORDS_SET);
	private static Configuration config = null;
	
	private static void parseHtml(String blogUrl, String outputFile, String continent, String country, String state, BufferedWriter mapbw) {
		try {
			HttpClient client = HttpClientBuilder.create().build();
			HttpGet httpGet = new HttpGet(blogUrl);
	        HttpResponse response = client.execute(httpGet);
	        HttpEntity entity = response.getEntity();
	        String responseString = EntityUtils.toString(entity, "UTF-8");
	        responseString = responseString.replaceAll("&laquo;", "");
	        Document doc = Jsoup.parse(responseString);
	        Elements list = doc.select("title");
	        Element titleElement = list.size() > 0 ? list.first() : null;
	        String title = list.size() > 0 ? titleElement.ownText():"Unknown";
	        title = title.replaceAll("[\"]", "").replace("|TravelBlog", "").replace(" | Travel Blog", "").trim();        
	        int beginIdx = new String(urlBase + continent + "/" + country + "/" + state + "/").length();
	        int endIdx = blogUrl.indexOf("/blog", beginIdx);
	        String area = "";
	        if (beginIdx < endIdx)
	        	area = blogUrl.substring(beginIdx, endIdx);
	       
	        Elements plist = doc.select("div.content").before("div.breadcrumb");
	        String buf = "";
	        for(Element e:plist) {
	        	String text = "";
                for (TextNode tn : e.textNodes()) {
                    String tagText = tn.text().trim();
                    if (tagText.length() > 0) {
                        text += tagText + " ";
                    }
                }
                buf = su.removeStopWords(text, stopSet);
	        }
	        if (buf.length() > 50) {
	        	mapbw.write("{url:\"" + blogUrl + "\",title: \"" + title + "\",file:\"" + outputFile + "\",continent: \"" + continent + "\","
		        		+ "country:\"" + country + "\", state:\"" + state + "\",area:\"" + area + "\"},\n");
	        	BufferedWriter bw = new BufferedWriter(new FileWriter(outputFile));
	        	bw.write(buf.toString() + "\n");
	        	bw.close();
	        }
	        EntityUtils.consume(entity);
		} catch(Exception e) {
			e.printStackTrace();
		}
	}
	
	private static HashMap<String, Integer> constructUrlMap(String continent, String country) {
		HashMap<String, Integer> myMap = new HashMap<>();
		try {
			String url = urlBase + continent + "/" + country;
			HttpClient client = HttpClientBuilder.create().build();
			HttpGet httpGet = new HttpGet(url);
	        HttpResponse response = client.execute(httpGet);
	        HttpEntity entity = response.getEntity();
	        String responseString = EntityUtils.toString(entity, "UTF-8");
			Document doc = Jsoup.parse(responseString);
			Elements links = doc.select("ul[class='tb-list'] > li > a"); // li list
			links.forEach(e -> {
				String href = e.attr("href");
				href = href.replaceAll("/", "");
				Element spanNode = e.child(0);
				String linkCount = spanNode.text().trim().replaceAll("[\\(\\)]", "");
				Integer lcount = Integer.parseInt(linkCount) / 10 + 1;
				myMap.put(href, lcount);				
			});
			EntityUtils.consume(entity);
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		return myMap;
	}

	public static void runOneCountry(String continent, String country) {
		 HashMap<String, Integer> urlMap = constructUrlMap(continent, country);
		
		try {
			String continentDir = blogBaseDir + continent;
	        su.createHdfsDirectory(config, continentDir);
			String countryDir = blogBaseDir + continent + "/" + country;
	        su.createHdfsDirectory(config, countryDir);
	        
			for (String state: urlMap.keySet()) {
				List<String> urlList = new ArrayList<>();
				int totalPages = (int)urlMap.get(state);
			
				for (int i = 1; i <= totalPages; i ++ ) {
					String url = urlBase + continent + "/" + country + "/" + state + "/blogs-page-" + i + ".html";
					System.out.println(url);
					HttpClient client = HttpClientBuilder.create().build();
					HttpGet httpGet = new HttpGet(url);
			        HttpResponse response = client.execute(httpGet);
			        HttpEntity entity = response.getEntity();
			        String responseString = EntityUtils.toString(entity, "UTF-8");
					Document doc = Jsoup.parse(responseString);
					Elements links = doc.select("a"); // a with title
					links.forEach(e -> {
						String elementText = e.text();
						if (elementText.contains("read more")) {
							String href = e.attr("href");
							urlList.add(urlRealBase + href);
						}
					});
					EntityUtils.consume(entity);
				}
			
				String stateDir = blogBaseDir + continent + "/" + country + "/" + state;
		        su.createHdfsDirectory(config, stateDir);
				File file = new File(metaBaseDir + continent + "-" + country + "-" + state + "-mapping.json");
				BufferedWriter bw = new BufferedWriter(new FileWriter(file));
				bw.write("{data:[");
				int cnt = 0;
				for(String blogUrl : urlList) {
					cnt ++ ;
					String blogOutput = blogBaseDir + continent + "/" +country + "/" + state + "/blog_" + cnt + ".txt";
					parseHtml(blogUrl, blogOutput, continent, country, state, bw);
				};
				bw.write("]}");
				bw.close();
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	public static void main(String[] args) {
		String[] myStopWords = {"have","has","from","about","just","were","was","very","only","more","here","time","when","would","been","which","even","many","them",
								"being","after","before","another","some","cant","went", "your","people","like","going","through","back","still","around","fuck", "what",
								"also", "good", "first", "because", "where", "place", "trip", "home", "little", "well", "great", "other", "most", "than", "really", "anyway",
								"over", "long", "little", "down", "much", "next", "well", "said", "asked", "told", "could", "headed", "want"
								};
		stopSet.addAll(Arrays.asList(myStopWords));
		
		Configuration config = su.setupHdfsConfig("/opt/hadoop-2.7.3");
		su.createHdfsDirectory(config, blogBaseDir);
		su.createHdfsDirectory(config, metaBaseDir);
		
		runOneCountry("North-America", "United-States");
		
		//runOneCountry("North-America", "Mexico");
		//runOneCountry("North-America", "Canada");
		/*runOneCountry("North-America", "Bermuda");
		runOneCountry("North-America", "Greenland");
		runOneCountry("South-America", "Argentina");
		runOneCountry("South-America", "BoliviaBolivia");
		runOneCountry("South-America", "BrazilBrazil");
		runOneCountry("South-America", "Chile");
		runOneCountry("South-America", "Colombia");
		runOneCountry("South-America", "Ecuador");
		runOneCountry("South-America", "Falkland-Islands");
		runOneCountry("South-America", "French-Guiana");
		runOneCountry("South-America", "Guyana");
		runOneCountry("South-America", "Paraguay");
		runOneCountry("South-America", "Peru");
		runOneCountry("South-America", "South-Georgia");
		runOneCountry("South-America", "Suriname");
		runOneCountry("South-America", "Uruguay");
		runOneCountry("South-America", "Venezuela");
		runOneCountry("Africa", "Algeria");
		runOneCountry("Africa", "Angola");
		runOneCountry("Africa", "Benin");
		runOneCountry("Africa", "Botswana");
		runOneCountry("Africa", "Burkina-Faso");
		runOneCountry("Africa", "Burundi");
		runOneCountry("Africa", "Cameroon");
		runOneCountry("Africa", "Cape-Verde");
		runOneCountry("Africa", "Central-African-Republic");
		runOneCountry("Africa", "Chad");
		runOneCountry("Africa", "Comoros");
		runOneCountry("Africa", "Congo");
		runOneCountry("Africa", "Congo-Democratic-Republic");
		runOneCountry("Africa", "Cote-d-Ivoire");
		runOneCountry("Africa", "Djibouti");
		runOneCountry("Africa", "Egypt");
		runOneCountry("Africa", "Equatorial-Guinea");
		runOneCountry("Africa", "Eritrea");
		runOneCountry("Africa", "Ethiopia");
		runOneCountry("Africa", "Gabon");
		runOneCountry("Africa", "Gambia");
		runOneCountry("Africa", "Ghana");
		runOneCountry("Africa", "Guinea");
		runOneCountry("Africa", "Guinea-Bissau");
		runOneCountry("Africa", "Kenya");
		runOneCountry("Africa", "Lesotho");
		runOneCountry("Africa", "Liberia");
		runOneCountry("Africa", "Libya");
		runOneCountry("Africa", "Madagascar");
		runOneCountry("Africa", "Malawi");
		runOneCountry("Africa", "Mali");
		runOneCountry("Africa", "Mauritania");
		runOneCountry("Africa", "Mauritius");
		runOneCountry("Africa", "Morocco");
		runOneCountry("Africa", "Mozambique");
		runOneCountry("Africa", "Namibia");
		runOneCountry("Africa", "Niger");
		runOneCountry("Africa", "Nigeria");
		runOneCountry("Africa", "Reunion");
		runOneCountry("Africa", "Rwanda");
		runOneCountry("Africa", "Sao-Tome-and-Principe");
		runOneCountry("Africa", "Senegal");
		runOneCountry("Africa", "Seychelles");
		runOneCountry("Africa", "Sierra-Leone");
		runOneCountry("Africa", "Somalia");
		runOneCountry("Africa", "South-Africa");
		runOneCountry("Africa", "South-Sudan");
		runOneCountry("Africa", "Sudan");
		runOneCountry("Africa", "Swaziland");
		runOneCountry("Africa", "Tanzania");
		runOneCountry("Africa", "Togo");
		runOneCountry("Africa", "Tunisia");
		runOneCountry("Africa", "Uganda");
		runOneCountry("Africa", "Western-Sahara");
		runOneCountry("Africa", "Zambia");
		runOneCountry("Africa", "Zimbabwe");
		
		runOneCountry("Asia", "Afghanistan");
		runOneCountry("Asia", "Armenia");
		runOneCountry("Asia", "Azerbaijan");
		runOneCountry("Asia", "Bangladesh");
		runOneCountry("Asia", "Bhutan");
		runOneCountry("Asia", "Brunei");
		runOneCountry("Asia", "Burma");
		runOneCountry("Asia", "Cambodia");
		runOneCountry("Asia", "China");
		runOneCountry("Asia", "East-Timor");
		runOneCountry("Asia", "Georgia");
		runOneCountry("Asia", "Hong-Kong");
		runOneCountry("Asia", "India");
		runOneCountry("Asia", "Indonesia");
		runOneCountry("Asia", "Japan");
		runOneCountry("Asia", "Kazakhstan");
		runOneCountry("Asia", "Kyrgyzstan");
		runOneCountry("Asia", "Laos");
		runOneCountry("Asia", "Macau");
		runOneCountry("Asia", "Malaysia");
		runOneCountry("Asia", "Maldives");
		runOneCountry("Asia", "Mongolia");
		runOneCountry("Asia", "Myanmar");
		runOneCountry("Asia", "Nepal");
		runOneCountry("Asia", "North-Korea");
		runOneCountry("Asia", "Pakistan");
		runOneCountry("Asia", "Philippines");
		runOneCountry("Asia", "Singapore");
		runOneCountry("Asia", "South-Korea");
		runOneCountry("Asia", "Sri-Lanka");
		runOneCountry("Asia", "Taiwan");
		runOneCountry("Asia", "Tajikistan");
		runOneCountry("Asia", "Thailand");
		runOneCountry("Asia", "Turkmenistan");
		runOneCountry("Asia", "Uzbekistan");
		runOneCountry("Asia", "Vietnam");
		
		runOneCountry("Central-America-Caribbean", "Anguilla");
		runOneCountry("Central-America-Caribbean", "Antigua-and-Barbuda");
		runOneCountry("Central-America-Caribbean", "Aruba");
		runOneCountry("Central-America-Caribbean", "Bahamas");
		runOneCountry("Central-America-Caribbean", "Barbados");
		runOneCountry("Central-America-Caribbean", "Belize");
		runOneCountry("Central-America-Caribbean", "British-Virgin-Islands");
		runOneCountry("Central-America-Caribbean", "Caribbean-Netherlands");
		runOneCountry("Central-America-Caribbean", "Cayman-Islands");
		runOneCountry("Central-America-Caribbean", "Costa-Rica");
		runOneCountry("Central-America-Caribbean", "Cuba");
		runOneCountry("Central-America-Caribbean", "Curacao");
		runOneCountry("Central-America-Caribbean", "Dominica");
		runOneCountry("Central-America-Caribbean", "Dominican-Republic");
		runOneCountry("Central-America-Caribbean", "El-Salvador");
		runOneCountry("Central-America-Caribbean", "Grenada");
		runOneCountry("Central-America-Caribbean", "Guadeloupe");
		runOneCountry("Central-America-Caribbean", "Guatemala");
		runOneCountry("Central-America-Caribbean", "Haiti");
		runOneCountry("Central-America-Caribbean", "Honduras");
		runOneCountry("Central-America-Caribbean", "Jamaica");
		runOneCountry("Central-America-Caribbean", "Martinique");
		runOneCountry("Central-America-Caribbean", "Montserrat");
		runOneCountry("Central-America-Caribbean", "Nicaragua");
		runOneCountry("Central-America-Caribbean", "Panama");
		runOneCountry("Central-America-Caribbean", "Puerto-Rico");
		runOneCountry("Central-America-Caribbean", "Saint-Barthelemy");
		runOneCountry("Central-America-Caribbean", "Saint-Kitts-and-Nevis");
		runOneCountry("Central-America-Caribbean", "Saint-Lucia");
		runOneCountry("Central-America-Caribbean", "Saint-Martin");
		runOneCountry("Central-America-Caribbean", "Saint-Vincent-and-Grenadines");
		runOneCountry("Central-America-Caribbean", "Sint-Maarten");
		runOneCountry("Central-America-Caribbean", "Trinidad-and-Tobago");
		runOneCountry("Central-America-Caribbean", "Turks-and-Caicos");
		runOneCountry("Central-America-Caribbean", "US-Virgin-Islands");
		
		runOneCountry("Europe", "Albania");
		runOneCountry("Europe", "Andorra");
		runOneCountry("Europe", "Austria");
		runOneCountry("Europe", "Belarus");
		runOneCountry("Europe", "Belgium");
		runOneCountry("Europe", "Bosnia-and-Herzegovina");
		runOneCountry("Europe", "Bulgaria");
		runOneCountry("Europe", "Channel-Islands");
		runOneCountry("Europe", "Croatia");
		runOneCountry("Europe", "Czech-Republic");
		runOneCountry("Europe", "Denmark");
		runOneCountry("Europe", "Estonia");
		runOneCountry("Europe", "Faroe-Islands");
		runOneCountry("Europe", "Finland");
		runOneCountry("Europe", "France");
		runOneCountry("Europe", "Germany");
		runOneCountry("Europe", "Gibraltar");
		runOneCountry("Europe", "Greece");
		runOneCountry("Europe", "Hungary");
		runOneCountry("Europe", "Iceland");
		runOneCountry("Europe", "Ireland");
		runOneCountry("Europe", "Isle-of-Man");
		runOneCountry("Europe", "Italy");
		runOneCountry("Europe", "Kosovo");
		runOneCountry("Europe", "Latvia");
		runOneCountry("Europe", "Liechtenstein");
		runOneCountry("Europe", "Lithuania");
		runOneCountry("Europe", "Luxembourg");
		runOneCountry("Europe", "Macedonia");
		runOneCountry("Europe", "Malta");
		runOneCountry("Europe", "Moldova");
		runOneCountry("Europe", "Monaco");
		runOneCountry("Europe", "Montenegro");
		runOneCountry("Europe", "Netherlands");
		runOneCountry("Europe", "Norway");
		runOneCountry("Europe", "Poland");
		runOneCountry("Europe", "Portugal");
		runOneCountry("Europe", "Romania");
		runOneCountry("Europe", "Russia");
		runOneCountry("Europe", "San-Marino");
		runOneCountry("Europe", "Serbia");
		runOneCountry("Europe", "Slovakia");
		runOneCountry("Europe", "Slovenia");
		runOneCountry("Europe", "Spain");
		runOneCountry("Europe", "Svalbard");
		runOneCountry("Europe", "Sweden");
		runOneCountry("Europe", "Switzerland");
		runOneCountry("Europe", "Ukraine");
		runOneCountry("Europe", "United-Kingdom");
		runOneCountry("Europe", "Vatican-City");*/
	}
}