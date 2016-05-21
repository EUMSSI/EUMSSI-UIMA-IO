package eu.eumssi.uima.reader;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.List;
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

import eu.eumssi.uima.ts.OcrSegment;
import eu.eumssi.uima.ts.Segment;
import eu.eumssi.uima.ts.SourceMeta;
import eu.eumssi.uima.ts.TopOcrSegment;

public class OcrReader extends MongoReaderBase {


	private static final Logger logger = Logger.getLogger(OcrReader.class.toString());

	// additional parameters

	/**
	 * only mark best hypothesis for each word
	 */
	public static final String PARAM_ONLYBEST = "OnlyBest";
	@ConfigurationParameter(name=PARAM_ONLYBEST, mandatory=false, defaultValue="true",
			description="only mark best hypothesis for each word")
	private Boolean onlyBest;

	/**
	 * only mark best hypothesis for each word
	 */
	public static final String PARAM_VERTICALLY_ALIGNED = "VerticallyAligned";
	@ConfigurationParameter(name=PARAM_VERTICALLY_ALIGNED, mandatory=false, defaultValue="true",
			description="put all hypotheses at the same character offsets (document text only contains best), if false concatenate all hypotheses sequentially")
	private Boolean verticallyAligned;

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

		// create document text from all available text fields
		StringBuilder documentText = new StringBuilder();
		Integer segmentIndex = 0;
		Integer tokenIndex = 0;
		for (String f: this.fieldsList) { // just one field normally
			try {
				segmentIndex = documentText.length(); // TODO: check for off-by-one
				if (segmentIndex > 0) {
					// separate segments with newlines
					documentText.append("\n\n\n");
					segmentIndex += 3;
					tokenIndex += 3;
				}
				@SuppressWarnings("unchecked")
				List<DBObject> detectionList = (List<DBObject>) ((DBObject) doc.get(f.replaceAll("\\.", SEPARATOR))).get("VideoTextDetection");
				for (DBObject detection : detectionList) {
					tokenIndex = documentText.length();
					logger.fine(detection.toString());
					@SuppressWarnings("unchecked")
					List<DBObject> hypotheses = (List<DBObject>)detection.get("Hypotheses");
					Boolean first = true;
					int beginTime = (int) ((double)detection.get("mediaRelIncrTimePoint_S"))*1000;
					int endTime = (int) ((double)detection.get("mediaIncrDuration_S"))*1000 + beginTime;
					for (DBObject hypothesis : hypotheses) {
						tokenIndex = documentText.length();
						String ocrText = hypothesis.get("text").toString(); // should be a String field anyway
						logger.fine(ocrText);
						double conf = (double) hypothesis.get("score");
						OcrSegment ocrSegment;
						if (first) {
							if (tokenIndex > 0) {
								documentText.append("\n");
								tokenIndex++;
							}
							documentText.append(ocrText + " .");
							double secondScore = 0;
							try {
								secondScore = (double) hypotheses.get(1).get("score");
							} catch (IndexOutOfBoundsException e) {
								// treat score as 0
							}
							double confRatio = secondScore / conf;
							ocrSegment = new TopOcrSegment(jcas);
							((TopOcrSegment)ocrSegment).setConfidenceRatio(confRatio);
							first = false;
						} else {
							if (!verticallyAligned) { // add all hypotheses to document text
								documentText.append("\n");
								tokenIndex++;
								documentText.append(ocrText + " .");
							}
							ocrSegment = new OcrSegment(jcas);
						}
						ocrSegment.setBegin(tokenIndex);
						ocrSegment.setEnd(documentText.length());
						ocrSegment.setBeginTime(beginTime);
						ocrSegment.setEndTime(endTime);
						ocrSegment.setConfidence(conf);
						ocrSegment.setText(ocrText);
						ocrSegment.addToIndexes();
						if (onlyBest) { // only mark top hypothesis
							break;
						}
					}
				}
				// create segment annotation
				Segment segAnno = new Segment(jcas);
				segAnno.setBegin(segmentIndex);
				segAnno.setEnd(documentText.length());
				segAnno.setSourceField(f);
				try {
					segAnno.setName(f.split("\\.")[-1]);
				} catch (Exception e) {
					segAnno.setName(f);
				}
				segAnno.addToIndexes();
			} catch (NullPointerException e) {
				logger.info(e.toString());
				// just leave text empty if document doesn't have one
			}
		}

		jcas.setDocumentText(documentText.toString());

		// create metadata annotation
		SourceMeta metadata = new SourceMeta(jcas);
		try {
			String lang = doc.get("lang").toString(); // should be a String field anyway
			jcas.setDocumentLanguage(lang);
			metadata.setLanguage(lang);
		} catch (NullPointerException e) {
			// just leave language empty if document doesn't have one
		}
		if (metadata.getView().getDocumentText() != null) {
			metadata.setBegin(0);
			metadata.setEnd(metadata.getView().getDocumentText().length());
		}
		metadata.setDocumentId(documentId);
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
		CollectionReaderFactory.createReaderDescription(OcrReader.class).toXML(System.out);
	}

}
