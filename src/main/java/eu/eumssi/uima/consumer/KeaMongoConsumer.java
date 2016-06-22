/**
 * 
 */
package eu.eumssi.uima.consumer;

import static org.apache.uima.fit.util.JCasUtil.select;
import static org.apache.uima.fit.util.JCasUtil.selectSingle;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.logging.Logger;

import org.apache.uima.UimaContext;
import org.apache.uima.analysis_engine.AnalysisEngineProcessException;
import org.apache.uima.fit.descriptor.ConfigurationParameter;
import org.apache.uima.fit.factory.AnalysisEngineFactory;
import org.apache.uima.jcas.JCas;
import org.apache.uima.resource.ResourceInitializationException;
import org.dbpedia.spotlight.uima.types.DBpediaResource;
import org.xml.sax.SAXException;

import com.iai.uima.jcas.tcas.KeyPhraseAnnotation;
import com.iai.uima.jcas.tcas.KeyPhraseAnnotationDeprecated;
import com.iai.uima.jcas.tcas.KeyPhraseAnnotationEnriched;
import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;

import de.tudarmstadt.ukp.dkpro.core.api.ner.type.NamedEntity;
import edu.upf.glicom.uima.ts.VerifiedDBpediaResource;
import eu.eumssi.uima.ts.SourceMeta;

/**
 * @author jgrivolla
 *
 */
public class KeaMongoConsumer extends MongoConsumerBase {

	private static Logger logger = Logger.getLogger(KeaMongoConsumer.class.toString());
	
	
	public void initialize(UimaContext context) throws ResourceInitializationException {
		super.initialize(context);
	}


	/* (non-Javadoc)
	 * @see org.apache.uima.analysis_component.CasAnnotator_ImplBase#process(org.apache.uima.cas.CAS)
	 */
	@Override
	public void process(JCas jCAS) throws AnalysisEngineProcessException {
		SourceMeta meta = selectSingle(jCAS, SourceMeta.class);
		String documentText = jCAS.getDocumentText();
		logger.fine("\n\n=========\n\n" + meta.getDocumentId() + ": " + documentText + "\n");
		
		/* get all key phrases */
		BasicDBList keyphrases = new BasicDBList();
		List<Object> keaList = new BasicDBList();
		for (KeyPhraseAnnotation entity : select(jCAS, KeyPhraseAnnotation.class)) {
			if (!(entity instanceof KeyPhraseAnnotationDeprecated)) {
				keaList.add(entity.getKeyPhrase());
			}
			if ((entity instanceof KeyPhraseAnnotationEnriched)) {
				try {
					keaList.add(((KeyPhraseAnnotationEnriched)entity).getEnrichment());
				} catch (Exception e) {
					// doesn't matter if enrichment is unavailable
				}
			}

		}

		BasicDBObject analysisResult = new BasicDBObject();
		analysisResult.append("keyphrases", keaList);

		/* write to MongoDB */
		BasicDBObject query = new BasicDBObject();
		query.append("_id", UUID.fromString(meta.getDocumentId()));
		BasicDBObject updates = new BasicDBObject();
		updates.append(this.outputField, analysisResult);
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
		AnalysisEngineFactory.createEngineDescription(KeaMongoConsumer.class).toXML(System.out);
	}

}
