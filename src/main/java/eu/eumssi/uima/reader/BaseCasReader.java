package eu.eumssi.uima.reader;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.logging.Logger;

import org.apache.uima.UimaContext;
import org.apache.uima.cas.CAS;
import org.apache.uima.cas.CASException;
import org.apache.uima.collection.CollectionException;
import org.apache.uima.fit.factory.CollectionReaderFactory;
import org.apache.uima.jcas.JCas;
import org.apache.uima.resource.ResourceInitializationException;
import org.xml.sax.SAXException;

import com.mongodb.DBObject;

import eu.eumssi.uima.reader.util.EumssiMetaLoader;
import eu.eumssi.uima.ts.Segment;
import eu.eumssi.uima.ts.SourceMeta;

public class BaseCasReader extends MongoReaderBase{


	private static final Logger logger = Logger.getLogger(BaseCasReader.class.toString());

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
		String documentId = doc.get("id").toString();
		logger.info(documentId);

		// create document text from all available text fields
		String documentText = "";
		Integer textIndex = 0;
		String segText = "";
		for (String f: this.fieldsList) {
			try {
				if (textIndex > 0) {
					// separate segments with newlines
					segText = "\n\n\n";
					textIndex += 3;
				} else {
					segText = "";
				}
				segText += doc.get(f.replaceAll("\\.", SEPARATOR)).toString(); // should be a String field anyway
				documentText += validXmlCharacters(segText);
				// create segment annotation
				Segment segAnno = new Segment(jcas);
				segAnno.setBegin(textIndex);
				segAnno.setEnd(documentText.length());
				segAnno.setSourceField(f);
				try {
					segAnno.setName(f.split("\\.")[-1]);
				} catch (Exception e) {
					segAnno.setName(f);
				}
				segAnno.addToIndexes();
				textIndex = documentText.length(); // TODO: check for off-by-one
			} catch (NullPointerException e) {
				// just leave text empty if document doesn't have one
			}
		}
		jcas.setDocumentText(documentText);

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
		CollectionReaderFactory.createReaderDescription(BaseCasReader.class).toXML(System.out);
	}


	@Override
	public void initialize(UimaContext context)
			throws ResourceInitializationException {
		super.initialize(context);
		
	}

}
