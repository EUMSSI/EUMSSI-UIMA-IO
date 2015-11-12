/**
 * 
 */
package eu.eumssi.uima.consumer;

import static org.apache.uima.fit.util.JCasUtil.select;
import static org.apache.uima.fit.util.JCasUtil.selectSingle;

import java.io.IOException;
import java.net.UnknownHostException;
import java.util.UUID;
import java.util.logging.Logger;

import org.apache.uima.UimaContext;
import org.apache.uima.analysis_engine.AnalysisEngineProcessException;
import org.apache.uima.fit.descriptor.ConfigurationParameter;
import org.apache.uima.fit.factory.AnalysisEngineFactory;
import org.apache.uima.jcas.JCas;
import org.apache.uima.resource.ResourceInitializationException;
import org.xml.sax.SAXException;

import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;

import eu.eumssi.uima.ts.AsrToken;
import eu.eumssi.uima.ts.AsrWord;
import eu.eumssi.uima.ts.SourceMeta;

/**
 * @author jgrivolla
 *
 */
public class FakeAsrSegmentConsumer extends MongoConsumerBase {

	private static Logger logger = Logger.getLogger(FakeAsrSegmentConsumer.class.toString());;

	/** segment collection */ 
	protected DBCollection segColl;

	public static final String PARAM_SEGMENT_MONGOCOLLECTION = "SegmentMongoCollection";
	@ConfigurationParameter(name=PARAM_SEGMENT_MONGOCOLLECTION, mandatory=true,
			description="Name of Mongo collection for segments")
	private String segmentMongoCollection;
	

	/**
	 * @return 
	 * @throws UnknownHostException 
	 * @throws ResourceInitializationException 
	 * 
	 */
	public void initialize(UimaContext context) throws ResourceInitializationException {
		super.initialize(context);
		this.segColl = db.getCollection(this.segmentMongoCollection);
		logger.info("connected to Collection "+this.segColl.getName());
	}


	/* (non-Javadoc)
	 * @see org.apache.uima.analysis_component.CasAnnotator_ImplBase#process(org.apache.uima.cas.CAS)
	 */
	@Override
	public void process(JCas jCAS) throws AnalysisEngineProcessException {
		SourceMeta meta = selectSingle(jCAS, SourceMeta.class);
		logger.fine("\n\n=========\n\n" + meta.getDocumentId() + ": " + jCAS.getDocumentText() + "\n");

		/* get all ASR tokens*/
		String text = "";
		int beginOffset = 0;
		int endOffset = 0;
		for (AsrToken asrToken: select(jCAS, AsrWord.class)) {
			logger.fine(String.format("  %-16s\t%-10s%n", 
					asrToken.getCoveredText(),
					asrToken.getType()));
			if (text.isEmpty()) {
				beginOffset = asrToken.getBeginTime();
			}
			endOffset = asrToken.getEndTime();
			text += asrToken.getCoveredText() + " "; // could use a StringBuilder, but not worth it
			if (text.length() > 200) { // arbitrarily break text into segments
				BasicDBObject asrSegmentDbObject = new BasicDBObject();
				// TODO: use timestamp based UUID for better performance
				asrSegmentDbObject.append("_id", UUID.randomUUID());
				asrSegmentDbObject.append("parent_id", UUID.fromString(meta.getDocumentId()));
				asrSegmentDbObject.append("meta",
						new BasicDBObject("extracted",
								new BasicDBObject("audio_transcript", text))
						);
				asrSegmentDbObject.append("beginOffset", beginOffset);
				asrSegmentDbObject.append("endOffset", endOffset);
				asrSegmentDbObject.append("segmentType", "FakeAsrSegment");
				try {
					this.segColl.insert(asrSegmentDbObject);
				} catch (Exception e) {
					logger.severe(e.toString());
				}
				text="";
			}
		}
		// insert remaining text as new segment
		BasicDBObject asrSegmentDbObject = new BasicDBObject();
		// TODO: use timestamp based UUID for better performance
		asrSegmentDbObject.append("_id", UUID.randomUUID());
		asrSegmentDbObject.append("parent_id", UUID.fromString(meta.getDocumentId()));
		asrSegmentDbObject.append("meta",
				new BasicDBObject("extracted",
						new BasicDBObject("audio_transcript", text))
				);
		asrSegmentDbObject.append("beginOffset", beginOffset);
		asrSegmentDbObject.append("endOffset", endOffset);
		asrSegmentDbObject.append("segmentType", "FakeAsrSegment");
		try {
			this.segColl.insert(asrSegmentDbObject);
		} catch (Exception e) {
			logger.severe(e.toString());
		}

		/* write to MongoDB */
		BasicDBObject query = new BasicDBObject();
		query.append("_id", UUID.fromString(meta.getDocumentId()));
		BasicDBObject update = new BasicDBObject();
		update.append("$addToSet", new BasicDBObject("processing.available_data", this.queueName));
		try {
			this.coll.update(query, update);
		} catch (Exception e) {
			logger.severe(e.toString());
			logger.severe(this.coll.findOne(new BasicDBObject("_id", UUID.fromString(meta.getDocumentId()))).toString());
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
		AnalysisEngineFactory.createEngineDescription(FakeAsrSegmentConsumer.class).toXML(System.out);
	}

}
