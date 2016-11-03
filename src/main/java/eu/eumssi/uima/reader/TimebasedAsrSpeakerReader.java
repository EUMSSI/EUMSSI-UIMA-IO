package eu.eumssi.uima.reader;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Logger;

import org.apache.uima.cas.CAS;
import org.apache.uima.cas.CASException;
import org.apache.uima.collection.CollectionException;
import org.apache.uima.fit.descriptor.ConfigurationParameter;
import org.apache.uima.fit.factory.CollectionReaderFactory;
import org.apache.uima.jcas.JCas;
import org.apache.uima.resource.ResourceInitializationException;
import org.xml.sax.SAXException;

import com.mongodb.DBObject;

import eu.eumssi.uima.reader.util.EumssiMetaLoader;
import eu.eumssi.uima.ts.AsrFiller;
import eu.eumssi.uima.ts.AsrToken;
import eu.eumssi.uima.ts.AsrWord;
import eu.eumssi.uima.ts.Segment;
import eu.eumssi.uima.ts.SourceMeta;
import eu.eumssi.uima.ts.SpeakerTurn;

public class TimebasedAsrSpeakerReader extends MongoReaderBase {


	private static final Logger logger = Logger.getLogger(TimebasedAsrSpeakerReader.class.toString());

	// additional parameters

	/**
	 * only mark word tokens (no fillers, etc.)
	 */
	public static final String PARAM_ONLYWORDS = "OnlyWords";
	@ConfigurationParameter(name=PARAM_ONLYWORDS, mandatory=false, defaultValue="false",
			description="only mark word tokens (no fillers, etc.)")
	private Boolean onlyWords = false;

	/**
	 * additional metadata fields in MongoDB (dot notation), comma separated
	 */
	public static final String PARAM_SPEAKERFIELD = "MongoSpeakerField";
	@ConfigurationParameter(name=PARAM_SPEAKERFIELD, mandatory=false,
			description="speaker recognition result field in MongoDB, needs to also be included in MongoMetaFields")
	protected String speakerField;

	/* (non-Javadoc)
	 * @see org.apache.uima.collection.CollectionReader#getNext(org.apache.uima.cas.CAS)
	 */
	public void getNext(CAS aCAS) throws IOException, CollectionException {
		JCas jcas;
		try{
			jcas = aCAS.getJCas();
		}
		catch(CASException e){
			throw new CollectionException(e);
		}
		DBObject doc = this.resCursor.next();
		String documentId = doc.get("id").toString(); // hopefully correct conversion to string
		logger.fine(documentId);

		// load speaker tracks
		DBObject speakers = (DBObject) doc.get(speakerField.replaceAll("\\.", SEPARATOR));
		ArrayList<Map<String, Object>> tracks = new ArrayList<Map<String,Object>>();
		for (String speakerId : speakers.keySet()) {
			DBObject speaker = (DBObject) speakers.get(speakerId);
			String gender = (String) speaker.get("speaker_gender");
			List<DBObject> trackDbList = (List<DBObject>) speaker.get("audio_segments");
			if (trackDbList != null) { // person is a speaker
				for (DBObject track : trackDbList) {
					Map<String,Object> tr = new HashMap<String,Object>();
					tr.put("start", (int) (Double.parseDouble(track.get("start_S").toString())*1000));
					tr.put("end", (int) (Double.parseDouble(track.get("end_S").toString())*1000));
					tr.put("id", speakerId);
					tr.put("gender", gender);
					tracks.add(tr);
				}
			}
		}
		tracks.sort(Comparator.comparingInt((Map<String, Object> k) -> (Integer) k.get("start")));
		for (Map<String, Object> track : tracks) {
			SpeakerTurn speakerTurn = new SpeakerTurn(jcas);
			speakerTurn.setBegin((int) track.get("start"));
			speakerTurn.setBeginTime((int) track.get("start"));
			speakerTurn.setEnd((int) track.get("end"));
			speakerTurn.setEndTime((int) track.get("end"));
			speakerTurn.setSpeakerId((String) track.get("id"));
			speakerTurn.setGender((String) track.get("gender"));
			speakerTurn.addToIndexes();
		}
		// create document text from all available text fields
		StringBuilder documentText = new StringBuilder();
		Integer segmentIndex = 0;
		Integer tokenIndex = 0;
		for (String f: this.fieldsList) { // just one field normally
			try {
				segmentIndex = documentText.length();
				if (segmentIndex > 0) {
					// separate segments with newlines
					documentText.append("\n\n\n");
					segmentIndex += 3;
					tokenIndex += 3;
				}
				List<DBObject> tokenList = (List<DBObject>) ((DBObject) doc.get(f.replaceAll("\\.", SEPARATOR))).get("content");
				for (DBObject token : tokenList) {
					tokenIndex = documentText.length();
					String tokenText = token.get("item").toString(); // should be a String field anyway
					int beginTime = (int) (Double.parseDouble(token.get("start").toString())*1000);
					int endTime = (int) (Double.parseDouble(token.get("end").toString())*1000);
					double conf = Double.parseDouble(token.get("conf").toString());
					String type = token.get("type").toString();
					if (tokenIndex > 0) {
						documentText.append(" ");
						tokenIndex++;
					}				
					documentText.append(tokenText);
					AsrToken tokenAnno;
					if (type.equals("word")) {
						tokenAnno = new AsrWord(jcas);
					} else if (onlyWords) { // don't mark fillers or others
						continue;
					} else if (type.equals("filler")) {
						tokenAnno = new AsrFiller(jcas);
					} else {
						tokenAnno = new AsrToken(jcas);
						logger.info(String.format("unknown token type %s in document %s", type, documentId));
					}
					// use time-based offsets
					tokenAnno.setBegin(beginTime);
					tokenAnno.setEnd(endTime);
					tokenAnno.setBeginTime(beginTime);
					tokenAnno.setEndTime(endTime);
					tokenAnno.setConfidence(conf);
					tokenAnno.setTokenType(type);
					tokenAnno.setText(tokenText);
					tokenAnno.addToIndexes();
				}
			} catch (NullPointerException e) {
				// just leave text empty if document doesn't have one
			}
		}
		//jcas.setDocumentText(documentText.toString());
		String mediaUrl = "http://eumssi.eu/dummy.mp4"; // TODO: set real media URL (though not used)
		jcas.setSofaDataURI(mediaUrl, "video/mp4");

		// create metadata annotation
		SourceMeta metadata = EumssiMetaLoader.getMeta(jcas, doc);
		metadata.addToIndexes();

		this.completed++;
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
		CollectionReaderFactory.createReaderDescription(TimebasedAsrSpeakerReader.class).toXML(System.out);
	}

}
