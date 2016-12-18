package Clustering;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.io.StringReader;
import java.util.Arrays;
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
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.analysis.util.CharArraySet;
import org.apache.lucene.util.Version;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ClusterUtil {
	private static ClusterUtil su = null;
	private static Logger logger = LoggerFactory.getLogger(ClusterUtil.class);

	public static ClusterUtil getInstance() {
		if (su == null)
			su = new ClusterUtil();
		return su;
	}
	
	private ClusterUtil() {
	}
	

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
}
