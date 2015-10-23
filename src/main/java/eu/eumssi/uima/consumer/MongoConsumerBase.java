/**
 * 
 */
package eu.eumssi.uima.consumer;

import java.io.IOException;
import java.net.UnknownHostException;
import java.util.logging.Logger;

import org.apache.uima.UimaContext;
import org.apache.uima.fit.component.JCasConsumer_ImplBase;
import org.apache.uima.fit.descriptor.ConfigurationParameter;
import org.apache.uima.fit.factory.AnalysisEngineFactory;
import org.apache.uima.resource.ResourceInitializationException;
import org.xml.sax.SAXException;

import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;

/**
 * @author jgrivolla
 *
 */
public abstract class MongoConsumerBase extends JCasConsumer_ImplBase {

	protected DBCollection coll;
	private static Logger logger = Logger.getLogger(MongoConsumerBase.class.toString());;

	public static final String PARAM_MONGOURI = "MongoUri";
	@ConfigurationParameter(name=PARAM_MONGOURI, mandatory=false, defaultValue="mongodb://localhost",
			description="URI of MongoDB service")
	protected String mongoUri;
	public static final String PARAM_MONGODB = "MongoDb";
	@ConfigurationParameter(name=PARAM_MONGODB, mandatory=true,
			description="Name of Mongo DB")
	protected String mongoDb;
	public static final String PARAM_MONGOCOLLECTION = "MongoCollection";
	@ConfigurationParameter(name=PARAM_MONGOCOLLECTION, mandatory=true,
			description="Name of Mongo collection")
	private String mongoCollection;
	public static final String PARAM_FIELD = "OutputField";
	@ConfigurationParameter(name=PARAM_FIELD, mandatory=true,
			description="Name of output field")
	protected String outputField;
	public static final String PARAM_QUEUE = "QueueName";
	@ConfigurationParameter(name=PARAM_QUEUE, mandatory=true,
			description="Queue name to mark in processing.available_data")
	protected String queueName;
	protected MongoClient mongoClient;
	protected DB db;


	/**
	 * @return 
	 * @throws UnknownHostException 
	 * @throws ResourceInitializationException 
	 * 
	 */
	public void initialize(UimaContext context) throws ResourceInitializationException {
		super.initialize(context);
		try {
			logger.info("mongoUri: "+this.mongoUri);
			logger.info("monoDb"+this.mongoDb);
			MongoClientURI uri = new MongoClientURI(this.mongoUri);
			this.mongoClient = new MongoClient(uri);
		} catch (UnknownHostException e) {
			throw new ResourceInitializationException(e);
		}
		this.db = mongoClient.getDB(this.mongoDb);
		logger.info("connected to DB "+this.db.getName());
		this.coll = db.getCollection(this.mongoCollection);
		logger.info("connected to Collection "+this.coll.getName());
	}


	/**
	 * return example descriptor (XML) when calling main method
	 * @param args
	 * @throws IOException 
	 * @throws SAXException 
	 * @throws ResourceInitializationException 
	 */
	public static void main(String[] args) throws ResourceInitializationException, SAXException, IOException {
		AnalysisEngineFactory.createEngineDescription(MongoConsumerBase.class).toXML(System.out);
	}

}
