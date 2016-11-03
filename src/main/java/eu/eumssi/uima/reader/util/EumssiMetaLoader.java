package eu.eumssi.uima.reader.util;

import org.apache.uima.jcas.JCas;

import com.mongodb.DBObject;

import eu.eumssi.uima.ts.SourceMeta;

public class EumssiMetaLoader {
	public static SourceMeta getMeta(JCas jcas, DBObject doc) {
		String documentId = doc.get("id").toString(); // hopefully correct conversion to string
		SourceMeta metadata = new SourceMeta(jcas);
		metadata.setDocumentId(documentId);
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
		try {
			metadata.setDocumentTitle(doc.get("meta###source###headline").toString()); // should be a String field anyway
		} catch (NullPointerException e) {
			// just leave text empty if document doesn't have one
		}
		try {
			metadata.setDatePublished(doc.get("meta###source###datePublished").toString());
		} catch (NullPointerException e) {
			// just leave text empty if document doesn't have one
		}
		return metadata;
	}
}
