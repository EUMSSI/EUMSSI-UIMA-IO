/**
 * 
 */
package eu.eumssi.uima.consumer;

import static org.apache.uima.fit.util.JCasUtil.select;
import static org.apache.uima.fit.util.JCasUtil.selectSingle;

import java.io.IOException;
import java.util.HashSet;
import java.util.List;
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
import org.dbpedia.spotlight.uima.types.TopDBpediaResource;
import org.xml.sax.SAXException;

import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;

import de.tudarmstadt.ukp.dkpro.core.api.ner.type.NamedEntity;
import edu.upf.glicom.uima.ts.VerifiedDBpediaResource;
import eu.eumssi.uima.ts.SourceMeta;

/**
 * @author jgrivolla
 *
 */
public class NER2MongoConsumer extends MongoConsumerBase {

	private static Logger logger = Logger.getLogger(NER2MongoConsumer.class.toString());


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

		/* get all dbpedia annotations (best candidate, filtered)*/
		BasicDBObject filteredDbpediaResources = new BasicDBObject();
		for (DBpediaResource resource : select(jCAS, VerifiedDBpediaResource.class)) {
			logger.fine(String.format("  %-16s\t%-10s\t%-10s%n", 
					resource.getCoveredText(),
					resource.getUri(),
					resource.getTypes()));
			for (String type : convertTypes(resource.getTypes())) {
				addWithType(filteredDbpediaResources, type, resource.getUri());
			}
			addWithType(filteredDbpediaResources, "all", resource.getUri());
		}

		/* get all dbpedia annotations (best candidate, unfiltered)*/
		BasicDBObject dbpediaResources = new BasicDBObject();
		for (DBpediaResource resource : select(jCAS, TopDBpediaResource.class)) {
			if (resource.getCoveredText().contains(" ") || !resource.getCoveredText().equals(resource.getCoveredText().toLowerCase())) {
				logger.fine(String.format("  %-16s\t%-10s\t%-10s%n", 
						resource.getCoveredText(),
						resource.getUri(),
						resource.getTypes()));
				for (String type : convertTypes(resource.getTypes())) {
					addWithType(dbpediaResources, type, resource.getUri());
				}
				addWithType(dbpediaResources, "all", resource.getUri());
			}
		}

		/* get all Stanford NER annotations */
		BasicDBObject entities = new BasicDBObject();
		for (NamedEntity entity : select(jCAS, NamedEntity.class)) {
			logger.fine(String.format("  %-16s %-10s %n", 
					entity.getCoveredText(),
					entity.getValue()));
			for (String type : convertTypes(entity.getValue())) {
				addWithType(entities, type, entity.getCoveredText());
			}
			addWithType(entities, "all", entity.getCoveredText());
		}

		BasicDBObject NEs = new BasicDBObject();
		NEs.append("dbpedia", dbpediaResources);
		NEs.append("dbpedia-filtered", filteredDbpediaResources);
		NEs.append("ner", entities);

		/* write to MongoDB */
		BasicDBObject query = new BasicDBObject();
		query.append("_id", UUID.fromString(meta.getDocumentId()));
		BasicDBObject updates = new BasicDBObject();
		updates.append(this.outputField, NEs);
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


	/** convert DBpedia resource types to Stanford NER types
	 * @param types space separated list of DBpedia types
	 * @return set of matching NER types
	 */
	private static Set<String> convertTypes(String types) {
		Set<String> typeSet = new HashSet<String>();
		if (types.matches("(PERSON)|(I-PER)|(.*Person.*)")) typeSet.add("PERSON");
		if (types.matches("(LOCATION)|(I-LOC)|(.*Place.*)")) 
			typeSet.add("LOCATION");
		if (types.matches("(ORGANIZATION)|(I-ORG)|(.*Organisation.*)")) typeSet.add("ORGANIZATION");
		if (types.matches("(MISC)|(I-MISC)")) typeSet.add("MISC");
		if (types.matches(".*City.*"))
			typeSet.add("City");
		if (types.matches(".*Country.*")) typeSet.add("Country");
		if (typeSet.isEmpty()) {
			typeSet.add("other");
		}
		return typeSet;
	}

	/** adds entities/resources to the entityMap structure according to the entity type
	 * @param entityMap MongoDB structure to be filled
	 * @param type type of the entity to add
	 * @param entity the entity name/URI
	 */
	@SuppressWarnings("unchecked")
	private static void addWithType(BasicDBObject entityMap, String type, String entity) {
		List<Object> entityList = null;
		// create field for each entity type
		if (entityMap.containsField(type)) {
			entityList = (List<Object>) entityMap.get(type);
		} else {
			entityList = new BasicDBList();
			entityMap.append(type, entityList);
		}
		entityList.add(entity);
	}

	/**
	 * return example descriptor (XML) when calling main method
	 * @param args
	 * @throws IOException 
	 * @throws SAXException 
	 * @throws ResourceInitializationException 
	 */
	public static void main(String[] args) throws ResourceInitializationException, SAXException, IOException {
		AnalysisEngineFactory.createEngineDescription(NER2MongoConsumer.class).toXML(System.out);
	}

}
