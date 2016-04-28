package eu.eumssi.uima.reader;

import static de.tudarmstadt.ukp.dkpro.core.api.resources.CompressionMethod.BZIP2;
import static de.tudarmstadt.ukp.dkpro.core.api.resources.CompressionMethod.GZIP;
import static de.tudarmstadt.ukp.dkpro.core.api.resources.CompressionMethod.XZ;
import static org.apache.commons.io.IOUtils.closeQuietly;

import java.io.ByteArrayInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.logging.Logger;
import java.util.zip.GZIPInputStream;

import org.apache.commons.compress.compressors.bzip2.BZip2CompressorInputStream;
import org.apache.commons.compress.compressors.xz.XZCompressorInputStream;
import org.apache.uima.cas.CAS;
import org.apache.uima.cas.impl.XmiCasDeserializer;
import org.apache.uima.collection.CollectionException;
import org.apache.uima.fit.descriptor.ConfigurationParameter;
import org.apache.uima.fit.factory.CollectionReaderFactory;
import org.apache.uima.resource.ResourceInitializationException;
import org.xml.sax.SAXException;

import com.mongodb.DBObject;

import de.tudarmstadt.ukp.dkpro.core.api.resources.CompressionMethod;

/**
 * Read CAS in XMI format from MongoDB, with optional compression.
 * Currently only supports reading a complete CAS from one field in MongoDB.
 * Merging views from different serialized CASes is planned in future releases.
 * @author jgrivolla
 *
 */
public class XmiMongoReader extends MongoReaderBase{

	private static final Logger logger = Logger.getLogger(XmiMongoReader.class.toString());


	/**
	 * Choose a compression method. (default: {@link CompressionMethod#NONE})
	 *
	 * @see CompressionMethod
	 */
	public static final String PARAM_COMPRESSION = "compression";
	@ConfigurationParameter(name=PARAM_COMPRESSION, mandatory=false, defaultValue="NONE")
	private CompressionMethod compression;

	/**
	 * In lenient mode, unknown types are ignored and do not cause an exception to be thrown.
	 */
	public static final String PARAM_LENIENT = "lenient";
	@ConfigurationParameter(name=PARAM_LENIENT, mandatory=true, defaultValue="false")
	private boolean lenient;

	/** add compression to input stream (as specified in PARAM_COMPRESSION)
	 * @param is the original input stream
	 * @return possibly compressed input stream 
	 * @throws IOException
	 * @see CompressionMethod
	 */
	private InputStream getInputStream(InputStream is) throws IOException
	{
		if (this.compression.equals(GZIP)) {
			is = new GZIPInputStream(is);
		}
		else if (this.compression.equals(BZIP2)) {
			is = new BZip2CompressorInputStream(is);
		}
		else if (this.compression.equals(XZ)) {
			is = new XZCompressorInputStream(is);
		}
		return is;
	}


	/* (non-Javadoc)
	 * @see org.apache.uima.collection.CollectionReader#getNext(org.apache.uima.cas.CAS)
	 */
	public void getNext(CAS aCAS) throws IOException, CollectionException {
		DBObject doc = this.resCursor.next();
		String documentId = doc.get("id").toString(); // hopefully correct conversion to string
		logger.info(documentId);
		String casType = "xmi" + this.compression.getExtension().replace(".", "_");
		for (String f: this.fieldsList) {
			try {
				ByteArrayInputStream xmi_bais;
				if (this.compression.equals(CompressionMethod.NONE)) { // saved as String
					xmi_bais = new ByteArrayInputStream(
							((String) ((DBObject) doc.get(f.replaceAll("\\.", SEPARATOR))).get(casType))
							.getBytes("UTF-8")
							);
				} else {
					xmi_bais = new ByteArrayInputStream( // saved as byte array
							(byte[]) ((DBObject) doc.get(f.replaceAll("\\.", SEPARATOR))).get(casType)
							);
				}
				InputStream xmi_is = getInputStream(xmi_bais);
				closeQuietly(xmi_is); // make sure it's fully flushed
				XmiCasDeserializer.deserialize(xmi_is, aCAS, lenient);
				closeQuietly(xmi_bais);
			}
			catch (SAXException e) {
				throw new IOException(e);
			}
		}
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
		CollectionReaderFactory.createReaderDescription(XmiMongoReader.class).toXML(System.out);
	}

}
