/**
 * 
 */
package eu.eumssi.uima.consumer;

import static de.tudarmstadt.ukp.dkpro.core.api.resources.CompressionMethod.BZIP2;
import static de.tudarmstadt.ukp.dkpro.core.api.resources.CompressionMethod.GZIP;
import static de.tudarmstadt.ukp.dkpro.core.api.resources.CompressionMethod.XZ;
import static org.apache.commons.io.IOUtils.closeQuietly;
import static org.apache.uima.fit.util.JCasUtil.selectSingle;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.UUID;
import java.util.logging.Logger;
import java.util.zip.GZIPOutputStream;

import javax.print.attribute.standard.Compression;

import org.apache.commons.compress.compressors.bzip2.BZip2CompressorOutputStream;
import org.apache.commons.compress.compressors.xz.XZCompressorOutputStream;
import org.apache.uima.UimaContext;
import org.apache.uima.analysis_engine.AnalysisEngineProcessException;
import org.apache.uima.cas.impl.XmiCasSerializer;
import org.apache.uima.fit.descriptor.ConfigurationParameter;
import org.apache.uima.fit.factory.AnalysisEngineFactory;
import org.apache.uima.jcas.JCas;
import org.apache.uima.resource.ResourceInitializationException;
import org.xml.sax.SAXException;

import com.mongodb.BasicDBObject;

import de.tudarmstadt.ukp.dkpro.core.api.resources.CompressionMethod;
import eu.eumssi.uima.ts.SourceMeta;

/**
 * Write CAS in XMI format to MongoDB, with optional compression.
 * @author jgrivolla
 *
 */
public class XmiMongoConsumer extends MongoConsumerBase {

	private static Logger logger = Logger.getLogger(XmiMongoConsumer.class.toString());


	/**
	 * Choose a compression method. (default: {@link CompressionMethod#NONE})
	 *
	 * @see CompressionMethod
	 */
	public static final String PARAM_COMPRESSION = "compression";
	@ConfigurationParameter(name=PARAM_COMPRESSION, mandatory=false, defaultValue="NONE")
	private CompressionMethod compression;


	public void initialize(UimaContext context) throws ResourceInitializationException {
		super.initialize(context);
		if (isNull(this.outputFieldParam)) { // different default than in MongoConsumerBase
			this.outputField = "meta.cas."+this.queueName;
		} else {
			this.outputField = this.outputFieldParam;
		}

	}


	/** add compression to output stream (as specified in PARAM_COMPRESSION)
	 * @param os the original output stream
	 * @return possibly compressed output stream 
	 * @throws IOException
	 * @see CompressionMethod
	 */
	private OutputStream getOutputStream(OutputStream os) throws IOException
	{
		if (this.compression.equals(GZIP)) {
			os = new GZIPOutputStream(os);
		}
		else if (this.compression.equals(BZIP2)) {
			os = new BZip2CompressorOutputStream(os);
		}
		else if (this.compression.equals(XZ)) {
			os = new XZCompressorOutputStream(os);
		}
		return os;
	}


	/* (non-Javadoc)
	 * @see org.apache.uima.analysis_component.CasAnnotator_ImplBase#process(org.apache.uima.cas.CAS)
	 */
	@Override
	public void process(JCas jCAS) throws AnalysisEngineProcessException {
		SourceMeta meta = selectSingle(jCAS, SourceMeta.class);
		String documentText = jCAS.getDocumentText();
		logger.fine("\n\n=========\n\n" + meta.getDocumentId() + ": " + documentText + "\n");

		/* write to MongoDB */
		BasicDBObject query = new BasicDBObject();
		query.append("_id", UUID.fromString(meta.getDocumentId()));
		BasicDBObject updates = new BasicDBObject();
		try {
			ByteArrayOutputStream xmi_baos = new ByteArrayOutputStream();
			OutputStream xmi_os = getOutputStream(xmi_baos);
			XmiCasSerializer.serialize(jCAS.getCas(), xmi_os);
			BasicDBObject xmiObj;
			closeQuietly(xmi_os); // make sure it's fully flushed
			String fieldName = "xmi" + this.compression.getExtension().replace(".", "_");
			if (this.compression.equals(CompressionMethod.NONE)) {
				xmiObj = new BasicDBObject(fieldName, xmi_baos.toString("UTF-8"));
			} else {
				xmiObj = new BasicDBObject(fieldName, xmi_baos.toByteArray());
			}
			updates.append(this.outputField, xmiObj);
			updates.append("processing.queues."+this.queueName,"processed");
			BasicDBObject update = new BasicDBObject();
			update.append("$set", updates);
			//update.append("$addToSet", new BasicDBObject("processing.available_data", this.queueName));
			//update.append("$set", new BasicDBObject("processing.queues."+this.queueName,"processed"));
			this.coll.update(query, update);
			closeQuietly(xmi_baos);
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
		AnalysisEngineFactory.createEngineDescription(XmiMongoConsumer.class).toXML(System.out);
	}

}
