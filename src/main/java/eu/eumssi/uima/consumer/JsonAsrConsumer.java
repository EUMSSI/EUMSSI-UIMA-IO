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
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.logging.Logger;

import org.apache.uima.UimaContext;
import org.apache.uima.analysis_engine.AnalysisEngineProcessException;
import org.apache.uima.fit.factory.AnalysisEngineFactory;
import org.apache.uima.jcas.JCas;
import org.apache.uima.resource.ResourceInitializationException;
import org.xml.sax.SAXException;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.mongodb.BasicDBObject;

import eu.eumssi.uima.ts.AsrToken;
import eu.eumssi.uima.ts.SourceMeta;
import eu.eumssi.uima.ts.SpeakerTurn;

/**
 * write ASR output to MongoDB: JSON format, separated by speaker turns
 * @author jgrivolla
 *
 */
public class JsonAsrConsumer extends MongoConsumerBase {

	private static Logger logger = Logger.getLogger(JsonAsrConsumer.class.toString());;


	/**
	 * @return 
	 * @throws UnknownHostException 
	 * @throws ResourceInitializationException 
	 * 
	 */
	public void initialize(UimaContext context) throws ResourceInitializationException {
		super.initialize(context);
	}

	/* (non-Javadoc)
	 * @see org.apache.uima.analysis_component.CasAnnotator_ImplBase#process(org.apache.uima.cas.CAS)
	 */
	@Override
	public void process(JCas jCAS) throws AnalysisEngineProcessException {
		SourceMeta meta = selectSingle(jCAS, SourceMeta.class);
		logger.fine("\n\n=========\n\n" + meta.getDocumentId() + ": " + jCAS.getDocumentText() + "\n");

		/* get all ASR tokens*/
		//BasicDBObject prettyAsrDbObject = new BasicDBObject();
		ArrayList<Map<String, Object>> turnList = new ArrayList<Map<String,Object>>();
		for (SpeakerTurn speakerTurn : select(jCAS,SpeakerTurn.class)) {
			HashMap<String, Object> turnMap = new HashMap<String,Object>();
			turnMap.put("speakerId", speakerTurn.getSpeakerId());
			turnMap.put("beginTime", speakerTurn.getBeginTime());
			turnMap.put("endTime", speakerTurn.getEndTime());
//			ArrayList<String> tokenList = new ArrayList<String>();
//			for (AsrToken asrToken: selectCovered(AsrToken.class, speakerTurn)) {
//				logger.fine(String.format("  %-16s\t%-10s%n", 
//						asrToken.getText(),
//						asrToken.getType()));
//				if (asrToken.getTokenType().equals("word")) {
//					tokenList.add(asrToken.getText());
//				}
//			}
//			turnMap.put("transcript", String.join(" ", tokenList));
			turnMap.put("transcript", speakerTurn.getCoveredText());
			turnList.add(turnMap);
		}


		/* write to MongoDB */
		BasicDBObject query = new BasicDBObject();
		query.append("_id", UUID.fromString(meta.getDocumentId()));
		BasicDBObject updates = new BasicDBObject();
		//updates.append("meta.extracted.audio_transcript.pretty_html", prettyAsrDbObject);
		Gson gson = new GsonBuilder().setPrettyPrinting().create();
		String json = gson.toJson(turnList); 
		updates.append(this.outputField, json); // generate a single string as result
		BasicDBObject update = new BasicDBObject();
		update.append("$set", updates);
		update.append("$addToSet", new BasicDBObject("processing.available_data", this.queueName));
		try {
			coll.update(query, update);
			logger.fine(json);
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
		AnalysisEngineFactory.createEngineDescription(JsonAsrConsumer.class).toXML(System.out);
	}

}
