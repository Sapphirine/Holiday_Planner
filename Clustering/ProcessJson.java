package Clustering;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.mahout.clustering.classify.WeightedPropertyVectorWritable;
import org.apache.mahout.math.NamedVector;
import org.apache.mahout.math.Vector;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.json.JSONTokener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ProcessJson {
	private static Logger logger = LoggerFactory.getLogger(ProcessJson.class);
	private static String blogBaseDir = "/tmp/travelblog/";
	private static String metaBaseDir = "/tmp/travelblog-meta/";
	private static ClusterUtil su = ClusterUtil.getInstance();
	private static Configuration config = null;
	
	private static List<Path> findSubdir(Path fileDir, FileSystem fs) throws IOException {
		List<Path> directories = new ArrayList<>();
		if (fs.isDirectory(fileDir)) {
			RemoteIterator<LocatedFileStatus> flist = fs.listFiles(fileDir, false);
			while (flist.hasNext()) {
				LocatedFileStatus lfs = flist.next();
				Path lp = lfs.getPath();
				if (fs.isDirectory(lp))
					directories.add(lp);
			}
		}
		return directories;
	}
	
	private static String[] findJsonFile(String currentDir) {
		File fileDir = new File(currentDir);
		String[] jsonFiles = fileDir.list(new FilenameFilter() {
			@Override
			public boolean accept(File current, String name) {
				return new File(current, name).getName().contains(".json");
			}
		});
		return jsonFiles;
	}
	
	private static HashMap<String, String> readCluster(Path blogPath, FileSystem fs, Configuration conf) throws Exception {
		HashMap<String, String> clusterHash = new HashMap<>();
		Path clusterOutput = new Path(blogPath.toString().replaceAll("travelblog", "travelblog-result") + "/clusteredPoints/part-m-00000");		
		@SuppressWarnings("deprecation")
		SequenceFile.Reader reader = new SequenceFile.Reader(fs, clusterOutput, conf);
        IntWritable key = new IntWritable();
        WeightedPropertyVectorWritable value = new WeightedPropertyVectorWritable();
        while (reader.next(key, value)) {
        	String pointName = "";
        	Vector theVec = value.getVector();
            if (theVec instanceof NamedVector)
            	pointName = ((NamedVector) theVec).getName();
            clusterHash.put(blogPath + pointName, key.toString());
        }
        reader.close();
        return clusterHash;
	}
	
	private static HashMap<String, String> readClusterTopics(Path blogPath, FileSystem fs) throws Exception {
		HashMap<String, String> topicHash = new HashMap<>();
		Path clusterOutput = new Path(blogPath.toString().replaceAll("travelblog", "travelblog-result") + "/clusterdump");
		
		BufferedReader reader = new BufferedReader(new InputStreamReader(fs.open(clusterOutput)));
		String line;
		String clusterName = "";
		String topics = "";
		boolean termSection = false;
		while ((line = reader.readLine()) != null) {
			line = line.trim();
			if (line.startsWith(":")) {
				// this is the header, looking for cluster name
				int vlIndex = line.indexOf("VL");
				if (vlIndex > 0) {
					int endIndex = line.indexOf("\"", vlIndex);
					clusterName = line.substring(vlIndex+3, endIndex);
				}
			} else if (line.startsWith("Top Terms")) {
				termSection = true;
				continue;
			} else if (line.startsWith("Weight")) {
				termSection = false;
				if (clusterName.length() > 0 && topics.length() > 0) {
					topicHash.put(clusterName, topics.trim());
				}
				clusterName = "";
				topics = "";
				continue;
			} else if (termSection) {
				String topic = line.substring(0, line.indexOf("=>")).trim();
				topics += topic + " ";
			}
		}
		reader.close();
		return topicHash;
	}
	
	public static void main(String[] argv) throws Exception {
		config = su.setupHdfsConfig("/opt/hadoop-2.7.3");
		FileSystem fs = FileSystem.get(config);
		BufferedWriter bw = new BufferedWriter(new FileWriter("/tmp/elasticsearchLoadData.sh"));
		
		Path dirName = new Path(blogBaseDir);
		List<Path> level_1 = findSubdir(dirName, fs);
		for(Path current_level_1_dir : level_1){
			List<Path> level_2 = findSubdir(current_level_1_dir, fs);
			for( Path current_level_2_dir : level_2) {
				List<Path> level_3 = findSubdir(current_level_2_dir, fs);
				for( Path current_level_3_dir: level_3 ) {
					int idx3 = current_level_3_dir.toString().lastIndexOf("/");
					int idx2 = current_level_3_dir.toString().substring(0, idx3).lastIndexOf("/");
					int idx1 = current_level_3_dir.toString().substring(0, idx2).lastIndexOf("/");
					int idx0 = current_level_3_dir.toString().substring(0, idx1).lastIndexOf("/");
					String continent = current_level_3_dir.toString().substring(idx0+1,idx1);
					String country = current_level_3_dir.toString().substring(idx1+1,idx2);
					String state = current_level_3_dir.toString().substring(idx2+1,idx3);

					String inputFile = metaBaseDir + continent + "-" + country + "-" + state + "-mapping.json";
					HashMap<String, String> clusterHash = readCluster(current_level_3_dir, fs, config);
					HashMap<String, String> topicHash = readClusterTopics(current_level_3_dir, fs);
					try {
						BufferedReader br = new BufferedReader(new FileReader(inputFile));
						String line = "";
						String jsonLine = "";
						while((line = br.readLine()) != null) {
							jsonLine += line;
						};
						br.close();
						System.out.println(inputFile);
						JSONObject obj = new JSONObject(jsonLine);
						JSONArray obJsonArray = obj.getJSONArray("data");
						for (int i = 0; i < obJsonArray.length(); i++)
						{						
							JSONObject obJson = obJsonArray.getJSONObject(i);
							String file = obJson.getString("file");
							String cluster = "";
							String topics = "";
							String titleCluster = "";
							String titleTopics = "";
							if (clusterHash.containsKey(file))
								cluster = clusterHash.get(file);
							if (cluster.length() > 0 && topicHash.containsKey(cluster))
								topics = topicHash.get(cluster);
							String url = obJson.getString("url");
							String title = obJson.getString("title");
							title = title.replaceAll("\'", "");
							String area = obJson.getString("area");
							int bidx = area.lastIndexOf("/");
							if(bidx >= 0 && bidx < area.length() - 1)
								area = area.substring(bidx+1, area.length());
							String jsonString = "{\"title\":\"" + title.toLowerCase() + "\",\"url\":\"" + url + "\",\"continent\":\"" + continent.toLowerCase() + "\",\"country\":\"" 
												+ country.toLowerCase() + "\",\"state\":\"" + state.toLowerCase() + "\",\"area\":\"" + area.toLowerCase() + "\",\"topics\":\"" 
												+ topics.toLowerCase() + "\"}";
							bw.write("curl -XPOST 'localhost:9200/topicdata/docs/?pretty' -d'" + jsonString + "'\n");
						}
					} catch (Exception e) {
						e.printStackTrace();
					}
				}
			}
		}
		bw.close();
	}

}
