package eu.eumssi.uima.reader;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.logging.Logger;

import org.apache.uima.UimaContext;
import org.apache.uima.collection.CollectionException;
import org.apache.uima.fit.component.CasCollectionReader_ImplBase;
import org.apache.uima.fit.descriptor.ConfigurationParameter;
import org.apache.uima.fit.factory.CollectionReaderFactory;
import org.apache.uima.resource.ResourceInitializationException;
import org.apache.uima.util.Progress;
import org.apache.uima.util.ProgressImpl;
import org.xml.sax.SAXException;

import com.mongodb.AggregationOptions;
import com.mongodb.AggregationOutput;
import com.mongodb.BasicDBObject;
import com.mongodb.CommandFailureException;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.util.JSON;

public abstract class MongoReaderBase extends CasCollectionReader_ImplBase{


	private static final Logger logger = Logger.getLogger(MongoReaderBase.class.toString());
	
	/**
	 * URI of MongoDB service
	 */
	public static final String PARAM_MONGOURI = "MongoUri";
	@ConfigurationParameter(name=PARAM_MONGOURI, mandatory=false, defaultValue="mongodb://localhost",
			description="URI of MongoDB service")
	protected String mongoUri;
	
	/**
	 * Name of Mongo DB
	 */
	public static final String PARAM_MONGODB = "MongoDb";
	@ConfigurationParameter(name=PARAM_MONGODB, mandatory=true, defaultValue="",
			description="Name of Mongo DB")
	protected String mongoDb;
	
	/**
	 * Name of Mongo collection
	 */
	public static final String PARAM_MONGOCOLLECTION = "MongoCollection";
	@ConfigurationParameter(name=PARAM_MONGOCOLLECTION, mandatory=true, defaultValue="",
			description="Name of Mongo collection")
	protected String mongoCollection;
	
	/**
	 * the query to select documents
	 */
	public static final String PARAM_QUERY = "Query";
	@ConfigurationParameter(name=PARAM_QUERY, mandatory=false, defaultValue="{}",
			description="the query to select documents")
	protected String queryString;
	
	/**
	 * result fields in MongoDB (dot notation), comma separated
	 */
	public static final String PARAM_FIELDS = "MongoFields";
	@ConfigurationParameter(name=PARAM_FIELDS, mandatory=true,
			description="result fields in MongoDB (dot notation), comma separated")
	protected String fieldsString;
	protected String[] fieldsList;
	
	/**
	 * additional metadata fields in MongoDB (dot notation), comma separated
	 */
	public static final String PARAM_METAFIELDS = "MongoMetaFields";
	@ConfigurationParameter(name=PARAM_METAFIELDS, mandatory=false,
			description="additional metadata fields in MongoDB (dot notation), comma separated")
	protected String metaFieldsString;
	protected String[] metaFieldsList;
	
	/**
	 * document language (as MongoDB expression)
	 */
	public static final String PARAM_LANG = "Language";
	@ConfigurationParameter(name=PARAM_LANG, mandatory=false, defaultValue="$lang",
			description="document language (as MongoDB expression)")
	protected String language;
	
	/**
	 * maximum number of items to retrieve
	 */
	public static final String PARAM_MAXITEMS = "MaxItems";
	@ConfigurationParameter(name=PARAM_MAXITEMS, mandatory=true, defaultValue="1000",
			description="maximum number of items to retrieve")
	protected Integer maxItems;
	
	
	/**
	 * separator to convert dot-notation to flat name when projecting MongoDB fields
	 */
	protected static final String SEPARATOR = "###";


	private MongoClient mongoClient;
	private DB db;
	private DBCollection coll;
	protected Iterator<DBObject> resCursor;

	// current document
	protected int completed;
	// total number of documents
	private long totalDocs;


	/**
	 * Initialize the component. Retrieve the parameters and process them, 
	 * parsing the field descriptions and preparing the structures needed to
	 * process the documents.
	 *
	 * @param context The UIMA context.
	 *
	 * @throws ResourceInitializationException
	 *             If an error occurs with some resource.
	 *
	 */
	public void initialize(UimaContext context) throws ResourceInitializationException {
		System.out.println("MongoCollectionReader: initialize()...");
		logger.info("initialize()...");
		this.completed = 0;
		try {
			MongoClientURI uri = new MongoClientURI(this.mongoUri);
			this.mongoClient = new MongoClient(uri);
		} catch (UnknownHostException e) {
			throw new ResourceInitializationException(e);
		}
		//m.getDatabaseNames();// to test connection
		this.db = mongoClient.getDB(this.mongoDb);
		logger.info("connected to DB "+this.db.getName());
		this.coll = db.getCollection(this.mongoCollection);
		logger.info("connected to Collection "+this.coll.getName());
		DBObject query = (DBObject) JSON.parse(this.queryString);
		this.totalDocs = this.coll.count(query);
		logger.info("performing query "+query.toString()+" on collection "+this.coll.toString());
		// create our pipeline operations, first with the $match
		DBObject match = new BasicDBObject("$match", query);
		DBObject limit = new BasicDBObject("$limit", this.maxItems);
		// build the $projection operation
		DBObject fields = new BasicDBObject();
		//TODO: properly make fields configurable
		fields.put("id", "$_id");
		fields.put("lang", (DBObject) JSON.parse(this.language));
		this.fieldsList = this.fieldsString.split(",");
		for (String f: this.fieldsList) {
			f = f.trim();
			fields.put(f.replaceAll("\\.", SEPARATOR), "$"+f);
		}
		if (this.metaFieldsString != null) {
			this.metaFieldsList = this.metaFieldsString.split(",");
			for (String f: this.metaFieldsList) {
				f = f.trim();
				fields.put(f.replaceAll("\\.", SEPARATOR), "$"+f);
			}
		}
		System.out.println(fields);
		//fields.put("text", "$meta.source.text");
		DBObject project = new BasicDBObject("$project", fields );
		// Finally the $sort operation
		DBObject sort = new BasicDBObject("$sort", new BasicDBObject("id", 1));

		// run aggregation
		List<DBObject> pipeline = Arrays.asList(match, limit, project, sort);
		try {
			AggregationOptions aggregationOptions = AggregationOptions.builder()
					.batchSize(100)
					.outputMode(AggregationOptions.OutputMode.CURSOR)
					.allowDiskUse(true)
					.build();
			this.resCursor = this.coll.aggregate(pipeline, aggregationOptions);
		} catch (CommandFailureException e) { // MongoDB version <2.6 doesn't support cursors
			logger.warning("Your MongoDB version doesn't seem to support cursors for aggregation pipelines. "
					+ "The result set is therefore limited to 16MB. "
					+ "Use a version >=2.6 to access larger amounts of data.\n"
					+ e.toString());
			AggregationOutput output = coll.aggregate(pipeline);
			this.resCursor = output.results().iterator();
		}
		logger.info("initialize() - Done.");
	}

	public boolean hasNext() throws IOException, CollectionException {
		return this.resCursor.hasNext();
	}


	public void close() throws IOException {
		this.mongoClient.close();
	}

	public Progress[] getProgress(){
		return new Progress[] { new ProgressImpl(this.completed, (int) this.totalDocs, Progress.ENTITIES) };
	}

	/**
	 * Utility function to remove characters that can't be represented in XML 1.0
	 * 
	 * @param text original text
	 * @return text containing only valid XML characters, others replaced by space
	 */
	protected static String validXmlCharacters(String text) {
		return validXmlCharacters(text, " ");
	}

	/**
	 * Utility function to remove characters that can't be represented in XML 1.0
	 * based on http://stackoverflow.com/a/28283387
	 * 
	 * @param text original text
	 * @param substitute substitution string for invalid characters
	 * @return text containing only valid XML characters, others replaced by space
	 */
	protected static String validXmlCharacters(String text, String substitute) {
	StringBuilder sb = new StringBuilder();
	for (int i = 0; i < text.length(); i++) {
	    int codePoint = text.codePointAt(i);
	    if (codePoint > 0xFFFF) {
	        i++;
	    }
	    if ((codePoint == 0x9) || (codePoint == 0xA) || (codePoint == 0xD)
	            || ((codePoint >= 0x20) && (codePoint <= 0xD7FF))
	            || ((codePoint >= 0xE000) && (codePoint <= 0xFFFD))
	            || ((codePoint >= 0x10000) && (codePoint <= 0x10FFFF))) {
	        sb.appendCodePoint(codePoint);
	    }
	    else {
	    	sb.append(substitute);
	    }
	}
	return sb.toString();
}
	
	
	/**
	 * return example descriptor (XML) when calling main method
	 * @param args not used
	 * @throws ResourceInitializationException
	 * @throws FileNotFoundException
	 * @throws SAXException
	 * @throws IOException
	 */
	public static void main(String[] args) throws ResourceInitializationException, FileNotFoundException, SAXException, IOException {
		CollectionReaderFactory.createReaderDescription(MongoReaderBase.class).toXML(System.out);
	}

}
