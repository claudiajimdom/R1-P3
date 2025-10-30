import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.*;
import org.apache.lucene.index.*;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.index.IndexWriterConfig.OpenMode;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Paths;

public class AirbnbIndexer{
    public static void main(String[] args) throws Exception{
        String indexPath = "airbnb_index";
        String dataFilePath = "/data/listings.csv";

        try{
            FSDirectory dir = FSDirectory.open(Paths.get(indexPath));
            StandardAnalyzer analyzer = new StandardAnalyzer();

            // OpenMode.CREATE → elimina el índice previo y crea uno nuevo
            // OpenMode.CREATE_OR_APPEND → deja lo existente y añade más documentos
            IndexWriterConfig config = new IndexWriterConfig(analyzer);
            config.setOpenMode(OpenMode.CREATE_OR_APPEND);
            
            IndexWriter writer = new IndexWriter(dir, config);

            BufferedReader br = new BufferedReader(new FileReader(csvPath));
            String line = br.readLine(); // leer encabezado
            
            while ((line = br.readLine()) != null) {
                String[] fields = parseCSV(line);

                // Ajustar según columnas del CSV real
                String id = fields[0];
                String name = fields[1];
                String neighbourhood = fields[2];
                String roomType = fields[3];
                int price = Integer.parseInt(fields[4]);
                String description = fields[5];

                Document doc = new Document();

                // -------------------- PROPIEDAD --------------------
                doc.add(new StringField("id", id, Field.Store.YES));
                doc.add(new StoredField("listing_url", listingUrl));
                doc.add(new TextField("name", name, Field.Store.YES));
                doc.add(new TextField("description", description, Field.Store.YES));
                doc.add(new TextField("neighborhood_overview", neighborhoodOverview, Field.Store.YES));
                doc.add(new TextField("neighbourhood_cleansed", neighbourhoodCleansed, Field.Store.YES));

                doc.add(new DoublePoint("latitude", latitude));
                doc.add(new StoredField("latitude", latitude));

                doc.add(new DoublePoint("longitude", longitude));
                doc.add(new StoredField("longitude", longitude));

                doc.add(new StringField("property_type", propertyType, Field.Store.YES));

                doc.add(new DoublePoint("bathrooms", bathrooms));
                doc.add(new StoredField("bathrooms", bathrooms));

                doc.add(new StoredField("bathrooms_text", bathroomsText));

                doc.add(new IntPoint("bedrooms", bedrooms));
                doc.add(new StoredField("bedrooms", bedrooms));

                doc.add(new TextField("amenities", amenities, Field.Store.YES));

                doc.add(new IntPoint("price", price));
                doc.add(new StoredField("price", price));

                doc.add(new IntPoint("number_of_reviews", numberOfReviews));
                doc.add(new StoredField("number_of_reviews", numberOfReviews));

                doc.add(new IntPoint("review_scores_rating", reviewScoresRating));
                doc.add(new StoredField("review_scores_rating", reviewScoresRating));

                // -------------------- ANFITRIÓN --------------------
                doc.add(new StoredField("host_url", hostUrl));
                doc.add(new TextField("host_name", hostName, Field.Store.YES));
                doc.add(new StringField("host_since", hostSince, Field.Store.YES));
                doc.add(new TextField("host_location", hostLocation, Field.Store.YES));
                doc.add(new TextField("host_about", hostAbout, Field.Store.YES));

                doc.add(new StringField("host_response_time", hostResponseTime, Field.Store.YES));
                doc.add(new StringField("host_is_superhost", hostIsSuperhost, Field.Store.YES));
                doc.add(new TextField("host_neighbourhood", hostNeighbourhood, Field.Store.YES));

            }
            br.close();
            writer.close();

            System.out.println("Indexación completada. Puedes abrirlo en Luke.");
        } catch (IOException e) {
            System.out.println("Error during indexing process.");
            e.printStackTrace();
        }


        private static String[] parseCSV(String line) {
            return line.split(",(?=([^\"]*\"[^\"]*\")*[^\"]*$)");
        }

    }

.

}