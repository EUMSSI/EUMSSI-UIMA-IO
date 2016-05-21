/**
 * 
 */
package eu.eumssi.uima.consumer;

import static org.apache.uima.fit.util.JCasUtil.select;
import static org.apache.uima.fit.util.JCasUtil.selectCovered;
import static org.apache.uima.fit.util.JCasUtil.selectSingle;

import java.io.IOException;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;
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

import eu.eumssi.uima.ts.OcrSegment;
import eu.eumssi.uima.ts.SourceMeta;
import eu.eumssi.uima.ts.TopOcrSegment;

/**
 * @author jgrivolla
 *
 */
public class OcrSegmentConsumer extends MongoConsumerBase {

	private static Logger logger = Logger.getLogger(OcrSegmentConsumer.class.toString());;

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

		/* get all OCR segments*/
		String text = "";
		int beginOffset = 0;
		int endOffset = 0;
		for (OcrSegment ocrSegment : select(jCAS, TopOcrSegment.class)) {
			logger.fine(String.format("  %-16s\t%-16s\t%-10d\t%-10d\t%-10d\t%-10d\t\n", 
					ocrSegment.getCoveredText(),
					ocrSegment.getText(),
					ocrSegment.getBeginTime(),
					ocrSegment.getEndTime(),
					ocrSegment.getBegin(),
					ocrSegment.getEnd()
					));
			if (text.isEmpty()) {
				beginOffset = ocrSegment.getBeginTime();
			}
			endOffset = ocrSegment.getEndTime();
			BasicDBObject ocrSegmentDbObject = new BasicDBObject();
			// TODO: use timestamp based UUID for better performance
			ocrSegmentDbObject.append("_id", UUID.randomUUID());
			ocrSegmentDbObject.append("parent_id", UUID.fromString(meta.getDocumentId()));
			ocrSegmentDbObject.append("meta",
					new BasicDBObject("extracted",
							new BasicDBObject("video_ocr",
									new BasicDBObject("best", ocrSegment.getText()).
										append("all", allHypothesesToString(ocrSegment)))
									));
			ocrSegmentDbObject.append("beginOffset", beginOffset);
			ocrSegmentDbObject.append("endOffset", endOffset);
			ocrSegmentDbObject.append("segmentType", "OcrSegment");
			try {
				this.segColl.insert(ocrSegmentDbObject);
			} catch (Exception e) {
				logger.severe(e.toString());
			}
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


	private String allHypothesesToString(OcrSegment ocrSegment) {
		List<String> hypList = new ArrayList<String>();
		hypList.add(ocrSegment.getText());
		for (OcrSegment hyp : selectCovered(OcrSegment.class, ocrSegment)) {
			hypList.add(hyp.getText());
		}
		return String.join("\n", hypList);
	}


	/**
	 * return example descriptor (XML) when calling main method
	 * @param args
	 * @throws IOException 
	 * @throws SAXException 
	 * @throws ResourceInitializationException 
	 */
	public static void main(String[] args) throws ResourceInitializationException, SAXException, IOException {
		AnalysisEngineFactory.createEngineDescription(OcrSegmentConsumer.class).toXML(System.out);
	}

}
