/**
 * 
 */
package eu.eumssi.uima.consumer;

import static org.apache.uima.fit.util.JCasUtil.select;
import static org.apache.uima.fit.util.JCasUtil.selectSingle;

import java.io.IOException;
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

import edu.upf.glicom.uima.ts.opinion.OpinionExpression;
import eu.eumssi.uima.ts.SourceMeta;

/**
 * @author jgrivolla
 *
 */
public class Polar2MongoConsumer extends MongoConsumerBase {

	private static Logger logger = Logger.getLogger(Polar2MongoConsumer.class.toString());

	static final String NEUTRAL = "NEUTRAL";
	static final String POSITIVE = "POSITIVE";
	static final String NEGATIVE = "NEGATIVE";


	/* (non-Javadoc)
	 * @see org.apache.uima.analysis_component.CasAnnotator_ImplBase#process(org.apache.uima.cas.CAS)
	 */
	@Override
	public void process(JCas jCAS) throws AnalysisEngineProcessException {
		SourceMeta meta = selectSingle(jCAS, SourceMeta.class);
		String documentText = jCAS.getDocumentText();
		logger.fine("\n\n=========\n\n" + meta.getDocumentId() + ": " + documentText + "\n");

		double finalPol = 0;
		String finalPolDiscrete = NEUTRAL;
		for (OpinionExpression oe: select(jCAS, OpinionExpression.class)) {
			finalPol = finalPol + Double.parseDouble(oe.getPolarity());
		}
		// discretize
		if (finalPol > 0) {
			finalPolDiscrete = POSITIVE;
		} else if (finalPol < 0) {
			finalPolDiscrete = NEGATIVE;
		}

		/* write to MongoDB */
		BasicDBObject polObject = new BasicDBObject();
		polObject.append("discrete", finalPolDiscrete);
		polObject.append("numeric", finalPol);
		BasicDBObject query = new BasicDBObject();
		query.append("_id", UUID.fromString(meta.getDocumentId()));
		BasicDBObject updates = new BasicDBObject();
		updates.append(this.outputField, polObject);
		BasicDBObject update = new BasicDBObject();
		update.append("$set", updates);
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
		AnalysisEngineFactory.createEngineDescription(Polar2MongoConsumer.class).toXML(System.out);
	}

}
