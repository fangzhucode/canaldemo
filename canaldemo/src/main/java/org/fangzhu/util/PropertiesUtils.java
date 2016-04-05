package org.fangzhu.util;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Enumeration;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 动态加载配置 配置文件有更改会直接体现出来 String value = PropertiesUtils.getInstance().get(
 * "src/main/resources/canalconf.properties", "name");
 * 
 * @author fangzhu
 * @date 2016-3-1
 */
public class PropertiesUtils {
	private static final String ENCODING = "UTF-8"; // encoding type...
	private static final String PARAMETER_DELIMITER = "=";

	private static final String COMMENT_DELIMITER = "#";
	private static final int PRINT_ELEMENT_LIMIT = 50;

	protected final static Logger logger = LoggerFactory
			.getLogger(PropertiesUtils.class);

	protected static PropertiesUtils instance; // singelton instance

	protected Hashtable<String, FileInfo> spfCache;

	protected PropertiesUtils() {
		spfCache = new Hashtable<String, FileInfo>();

	}

	public static PropertiesUtils getInstance() {
		if (instance == null) {
			instance = new PropertiesUtils();
		}
		return instance;
	}

	/**
	 * return an enumeration of all keys
	 * 
	 * @param filename
	 * @return
	 */
	public Enumeration<String> getAllKeys(String filename) {
		Enumeration<String> allkeys = null;
		FileInfo curFileInfo = getFileInfo(filename);
		if (curFileInfo != null) {
			checkConf(curFileInfo);
			allkeys = curFileInfo.cachedTokens.keys();
		}
		return allkeys;
	}

	/**
	 * return a set for all keys available
	 * 
	 * @param filename
	 * @return
	 */
	public Set<String> keySet(String filename) {
		Set<String> keyset = new HashSet<String>(); // JP, Aug 16, 2006,
													// initialize it to an empty
													// Set
		FileInfo curFileInfo = getFileInfo(filename);
		if (curFileInfo != null) {
			checkConf(curFileInfo);
			keyset = curFileInfo.cachedTokens.keySet();
		}
		return keyset;
	}

	/**
	 * get the who hash table for internal processing
	 * 
	 * @param filename
	 * @return
	 */
	private Hashtable<String, String> getProperty(String filename) {
		Hashtable<String, String> retTable = null;
		FileInfo curFileInfo = getFileInfo(filename);
		if (curFileInfo != null) {
			checkConf(curFileInfo);
			retTable = curFileInfo.cachedTokens;
		} else {
			retTable = new Hashtable<String, String>(); // return empty vector
		}
		return retTable;
	}

	/**
	 * get the value based on the key and filename
	 * 
	 * @param filename
	 * @param key
	 * @return
	 */
	public String get(String filename, String key) {
		String retStr = null;
		FileInfo curFileInfo = getFileInfo(filename);
		if (curFileInfo != null) {
			checkConf(curFileInfo);
			retStr = curFileInfo.cachedTokens.get(key);
		}
		return retStr;
	}

	/**
	 * return the timestamp of this file. this is used by external routine to
	 * explicit triggers a re-read
	 * 
	 * @param filename
	 * @return
	 */
	public long getTimestamp(String filename) {
		long ts = 0;
		FileInfo curFileInfo = getFileInfo(filename);
		if (curFileInfo != null) {
			checkConf(curFileInfo);
			ts = curFileInfo.timestamp;
		}
		return ts;
	}

	/**
	 * check if the item contains the token
	 * 
	 * @param filename
	 * @param token
	 * @return
	 */
	public boolean contains(String filename, String token) {
		boolean status = false;
		FileInfo curFileInfo = getFileInfo(filename);
		if (curFileInfo != null) {
			checkConf(curFileInfo);
			status = contains(curFileInfo.cachedTokens, token);
		}
		return status;
	}

	/**
	 * check if key is there!
	 * 
	 * @param filename
	 * @param token
	 * @return
	 */
	public boolean containsKey(String filename, String token) {
		boolean status = false;
		FileInfo curFileInfo = getFileInfo(filename);
		if (curFileInfo != null) {
			checkConf(curFileInfo);
			status = containsKey(curFileInfo.cachedTokens, token);
		}
		return status;
	}

	/**
	 * return the number of items in the file
	 * 
	 * @param filename
	 * @return
	 */
	public int getSize(String filename) {
		int size = 0;
		FileInfo curFileInfo = getFileInfo(filename);
		if (curFileInfo != null) {
			checkConf(curFileInfo);
			size = curFileInfo.cachedTokens.size();
		}
		return size;
	}

	/**
	 * check to see if time stamp has been updated. This check DOES NOT trigger
	 * a re-read!
	 * 
	 * @param filename
	 * @return
	 */
	public boolean isUpdated(String filename) {
		boolean status = false;
		if (filename != null && filename.length() > 0) {
			FileInfo curFileInfo = getFileInfo(filename);
			if (curFileInfo != null) {
				long newTimestamp = (new File(curFileInfo.filename))
						.lastModified();
				// NOTE: if file does not exist, lastModified() will return '0',
				// let
				// it proceed to create a zero element HashTable
				if (newTimestamp > curFileInfo.timestamp || newTimestamp == 0) {
					status = true; // file has been updated!
				} // else - no need to do anything!
			} else {
				status = true; // signal update is required because file has not
								// been read before!
			}
		} // else - if file name is invalid, there is nothing to be 'updated',
			// return false...
		return status;
	}

	/**
	 * 获取配置文件信息
	 * 
	 * @param filename
	 * @return
	 */
	private FileInfo getFileInfo(String filename) {
		FileInfo curFileInfo = null;
		if (filename != null && filename.length() > 0) {
			try {
				if (spfCache.containsKey(filename)) {
					curFileInfo = spfCache.get(filename);
				} else {
					curFileInfo = new FileInfo(filename);
					spfCache.put(filename, curFileInfo);
				}
				curFileInfo = spfCache.get(filename);
			} catch (Exception e) {
				logger.error("Cannot read simple property file [" + filename
						+ "]");
			}
		}
		return curFileInfo;
	}

	private String get(Hashtable<String, String> vs, String key) {
		return vs.get(key);
	}

	private boolean contains(Hashtable<String, String> vs, String token) {
		return vs.contains(token);
	}

	private boolean containsKey(Hashtable<String, String> vs, String key) {
		return vs.containsKey(key);
	}

	/**
	 * 检查配置文件是否有更改
	 * 
	 * @param fi
	 */
	private void checkConf(FileInfo fi) {
		String curDir = null;
		try {
			curDir = (new File(".")).getAbsolutePath();
			// if 'fp' is null, meaning cannot read a file, will throw exception
			long newTimestamp = (new File(fi.filename)).lastModified();

			// if file does not exist, lastModified() will return '0', let
			if (newTimestamp > fi.timestamp || newTimestamp == 0) {
				parseProperties(fi);
				fi.timestamp = newTimestamp;
			}
		} catch (Exception e) {
			logger.error("Cannot read file [" + fi.filename
					+ "] from directory [" + curDir + "]..." + e.getMessage());
		}
	}

	/**
	 * 重新加载配置
	 * 
	 * @param fi
	 */
	private void parseProperties(FileInfo fi) {
		if (fi.filename != null && fi.filename.length() > 0) {
			Hashtable<String, String> tokensFromFile = new Hashtable<String, String>();
			BufferedReader br = null;
			try {
				br = new BufferedReader(new InputStreamReader(
						new FileInputStream(fi.filename), ENCODING));
				String tmpValue; // extracted value
				String tmpKey; // extracted key
				String tmpLine; // the line

				while ((tmpLine = br.readLine()) != null) {
					// strip off any comment and trim it down
					tmpLine = tmpLine.replaceAll(COMMENT_DELIMITER + ".*$", "")
							.trim();
					int sindex = tmpLine.indexOf(PARAMETER_DELIMITER);

					if (sindex != -1) {
						tmpKey = tmpLine.substring(0, sindex).trim();
						tmpValue = tmpLine.substring(sindex + 1).trim();

						if ( /* tmpValue.length() > 0 && */tmpKey.length() > 0) {
							tokensFromFile.put(tmpKey, tmpValue);
						}
					}
				}
				br.close();
				logger.info("Reload Property file [" + fi.filename + "]");
			} catch (Exception e) {
				logger.error("Property file [" + fi.filename
						+ "] cannot be loaded " + e.getMessage());
			} finally {
				if (tokensFromFile != null) {
					if (fi.cachedTokens != null) {
						fi.cachedTokens.clear(); // remove old Vector
					}
					fi.cachedTokens = tokensFromFile; // use new table
					if (fi.cachedTokens.size() < PRINT_ELEMENT_LIMIT) {
						// fi.cachedTokens.toString());
					}
				}
				try {
					br.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		} else {
			logger.error("Property file [" + fi.filename + "] is not defined");
		}
	}

	/**
	 * Do not try to combine this with SimplePropertyFileCache.java. THEY ARE
	 * DIFFERENT!!!
	 * 
	 * @author Administrator
	 * 
	 */
	final class FileInfo {
		private FileInfo(String fn) {
			filename = fn;
		}

		private String filename;
		private long timestamp;
		private Hashtable<String, String> cachedTokens;
	}
}
