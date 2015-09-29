/**
 * 
 */
package eu.eumssi.uima.consumer;

import static org.apache.uima.fit.util.JCasUtil.select;
import static org.apache.uima.fit.util.JCasUtil.selectCovered;
import static org.apache.uima.fit.util.JCasUtil.selectSingle;

import java.io.IOException;
import java.net.UnknownHostException;
import java.util.UUID;
import java.util.logging.Logger;

import org.apache.uima.UimaContext;
import org.apache.uima.analysis_engine.AnalysisEngineProcessException;
import org.apache.uima.fit.component.JCasConsumer_ImplBase;
import org.apache.uima.fit.descriptor.ConfigurationParameter;
import org.apache.uima.fit.factory.AnalysisEngineFactory;
import org.apache.uima.jcas.JCas;
import org.apache.uima.resource.ResourceInitializationException;
import org.dbpedia.spotlight.uima.types.DBpediaResource;
import org.dbpedia.spotlight.uima.types.TopDBpediaResource;
import org.xml.sax.SAXException;

import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;

import eu.eumssi.uima.ts.OcrSegment;
import eu.eumssi.uima.ts.SourceMeta;

/**
 * @author jgrivolla
 *
 */
public class OcrNerlConsumer extends JCasConsumer_ImplBase {

	private DBCollection coll;
	private static Logger logger = Logger.getLogger(OcrNerlConsumer.class.toString());;

	public static final String PARAM_MONGOURI = "MongoUri";
	@ConfigurationParameter(name=PARAM_MONGOURI, mandatory=false, defaultValue="mongodb://localhost",
			description="URI of MongoDB service")
	private String mongoUri;
	public static final String PARAM_MONGODB = "MongoDb";
	@ConfigurationParameter(name=PARAM_MONGODB, mandatory=true,
			description="Name of Mongo DB")
	private String mongoDb;
	public static final String PARAM_MONGOCOLLECTION = "MongoCollection";
	@ConfigurationParameter(name=PARAM_MONGOCOLLECTION, mandatory=true,
			description="Name of Mongo collection")
	private String mongoCollection;
	private MongoClient mongoClient;
	private DB db;


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

	/* (non-Javadoc)
	 * @see org.apache.uima.analysis_component.CasAnnotator_ImplBase#process(org.apache.uima.cas.CAS)
	 */
	@Override
	public void process(JCas jCAS) throws AnalysisEngineProcessException {
		SourceMeta meta = selectSingle(jCAS, SourceMeta.class);
		logger.fine("\n\n=========\n\n" + meta.getDocumentId() + ": " + jCAS.getDocumentText() + "\n");

		/* get all ASR tokens*/
		BasicDBList ocrNerlDbList = new BasicDBList();

		for (OcrSegment ocrSegment : select(jCAS, OcrSegment.class)) {
			logger.fine(String.format("  %-16s\t%-16s\t%-10d\t%-10d\t%-10d\t%-10d\t\n", 
					ocrSegment.getCoveredText(),
					ocrSegment.getText(),
					ocrSegment.getBeginTime(),
					ocrSegment.getEndTime(),
					ocrSegment.getBegin(),
					ocrSegment.getEnd()
					));
			BasicDBList dbpediaResources = new BasicDBList();
			boolean hasNerl = false;
			for (DBpediaResource resource : selectCovered(TopDBpediaResource.class, ocrSegment)) {
				logger.fine(String.format("  %-16s\t%-10s\t%-10s%n", 
						resource.getCoveredText(),
						resource.getUri(),
						resource.getTypes()
						));
				BasicDBObject resourceObject = new BasicDBObject();
				resourceObject.append("text", resource.getCoveredText());
				resourceObject.append("uri", resource.getUri());
				resourceObject.append("types", resource.getTypes());
				dbpediaResources.add(resourceObject);
				hasNerl = true;
			}
			if (hasNerl) {
				BasicDBObject segObject = new BasicDBObject();
				segObject.append("start", ocrSegment.getBeginTime());
				segObject.append("end", ocrSegment.getEndTime());
				segObject.append("transcript", ocrSegment.getText());
				segObject.append("score", ocrSegment.getConfidence());
				segObject.append("dbpedia", dbpediaResources);
				ocrNerlDbList.add(segObject);
			}
		}

		/* write to MongoDB */
		BasicDBObject query = new BasicDBObject();
		query.append("_id", UUID.fromString(meta.getDocumentId()));
		BasicDBObject updates = new BasicDBObject();
		updates.append("processing.results.text.ocr-nerl", ocrNerlDbList);
		BasicDBObject update = new BasicDBObject();
		update.append("$set", updates);
		update.append("$addToSet", new BasicDBObject("processing.available_data", "ocr-nerl"));
		try {
			coll.update(query, update);
		} catch (Exception e) {
			logger.severe(e.toString());
			logger.severe(coll.findOne(new BasicDBObject("_id", UUID.fromString(meta.getDocumentId()))).toString());
		}
	}




	/**
	 * return example descriptor (XML) when calling main method
	 * @param args
	 * @throws IOException 
	 * @throws SAXException 
	 * @throws ResourceInitializationException 
	 */
	public static void main(String[] args) throws ResourceInitializationException, SAXException, IOException {
		AnalysisEngineFactory.createEngineDescription(OcrNerlConsumer.class).toXML(System.out);
	}

}
