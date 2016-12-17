package WebScraper;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.io.StringReader;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.core.LowerCaseTokenizer;
import org.apache.lucene.analysis.core.StopFilter;
import org.apache.lucene.analysis.pattern.PatternReplaceFilter;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.analysis.util.CharArraySet;
import org.apache.lucene.util.Version;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ScraperUtil {
	private static ScraperUtil su = null;
	private static Logger logger = LoggerFactory.getLogger(ScraperUtil.class);
	
	public static ScraperUtil getInstance() {
		if (su == null)
			su = new ScraperUtil();
		return su;
	}
	
	private ScraperUtil() {}
	
	public Configuration setupHdfsConfig(String hadoopHome) {
		Configuration config = new Configuration();
		config.addResource(new Path(hadoopHome + "/etc/hadoop/core-site.xml"));
		config.addResource(new Path(hadoopHome + "/etc/hadoop/hdfs-site.xml"));

		config.set("fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
		config.set("fs.file.impl", org.apache.hadoop.fs.LocalFileSystem.class.getName());
		return config;
	}
	
	public void createHdfsDirectory(Configuration config, String dirName) {
		try {
			FileSystem dfs = FileSystem.get(config);
			Path src = new Path(dirName);
			
			if (!dfs.exists(src))
				dfs.mkdirs(src); 
		} catch (IOException e) {
			logger.error("failed to create directory: " + dirName);
			e.printStackTrace();
		}
	}
	
	public String removeStopWords(String inputText, CharArraySet stopSet) {
		StringBuffer buf = new StringBuffer();
		Pattern pattern = Pattern.compile("[a-z]+");
        Tokenizer source = new LowerCaseTokenizer(Version.LUCENE_CURRENT, new StringReader(inputText));
        TokenStream result = new StopFilter(Version.LUCENE_CURRENT, source, stopSet);
		result = new PatternReplaceFilter(result, Pattern.compile("\\d"), "", true);
		//result = new LengthFilter(Version.LUCENE_CURRENT, true, result, 3, Integer.MAX_VALUE);
		CharTermAttribute termAtt = (CharTermAttribute) result.addAttribute(CharTermAttribute.class);
		try {
			result.reset();
			while (result.incrementToken()) {
				if (termAtt.length() <= 3) continue;
				String word = new String(termAtt.buffer(), 0, termAtt.length()).toLowerCase();
				Matcher m = pattern.matcher(word);
				if (m.matches()) {
					buf.append(word).append(" ");
				}
			}
			result.close();
		}catch (IOException e1) {
			e1.printStackTrace();
		}
		return buf.toString();
	}
}
