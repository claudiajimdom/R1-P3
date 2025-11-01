import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.es.SpanishAnalyzer;
import org.apache.lucene.document.*;
import org.apache.lucene.index.*;
import org.apache.lucene.store.*;


import java.io.*;
import java.nio.file.*;
import java.util.*;

public class IndiceAirbnb {

    public static void main(String[] args) throws Exception {
        if (args.length < 1) {
            System.out.println("Uso: java IndiceAirbnb <directorioDatos>\nEl índice se creará en ./index (no es necesario pasar ruta de índice).\n");
            return;
        }

        Path dataDir = Paths.get(args[0]);
        // Usamos un único índice fijo dentro del proyecto para simplificar: ./index
        Path indexDir = Paths.get("./index");

        Analyzer analyzer = new SpanishAnalyzer();
        Directory directory = FSDirectory.open(indexDir);

        IndexWriterConfig config = new IndexWriterConfig(analyzer);
        IndexWriter writer = new IndexWriter(directory, config);

        // Indexar propiedades (el método también incluye campos de anfitrión si están presentes en el CSV)
        indexAirbnb(writer, dataDir);

        writer.close();
        System.out.println("Indexación completada en " + indexDir);
    }

    private static void indexAirbnb(IndexWriter writer, Path dataDir) throws Exception {
        Files.list(dataDir)
                .filter(p -> p.toString().endsWith(".csv"))
                .forEach(path -> {
                    try (BufferedReader br = new BufferedReader(new FileReader(path.toFile()))) {
                        String line;
                        br.readLine(); // encabezado
                        while ((line = br.readLine()) != null) {
                            String[] cols = line.split(",", -1);
                            Document doc = new Document();
                            // -------------------- PROPIEDAD --------------------
                            doc.add(new StringField("id", cols[0], Field.Store.YES));
                            doc.add(new StringField("listing_url", cols[1], Field.Store.YES));
                            doc.add(new TextField("name", cols[2], Field.Store.YES));
                            doc.add(new TextField("description", cols[3], Field.Store.YES));
                            doc.add(new TextField("neighborhood_overview", cols[4], Field.Store.YES));
                            doc.add(new StringField("neighbourhood_cleansed", cols[5], Field.Store.YES));
                            doc.add(new DoublePoint("latitude", Double.parseDouble(cols[6])));
                            doc.add(new StoredField("latitude", Double.parseDouble(cols[6])));
                            doc.add(new DoublePoint("longitude", Double.parseDouble(cols[7])));
                            doc.add(new StoredField("longitude", Double.parseDouble(cols[7])));
                            doc.add(new StringField("property_type", cols[8], Field.Store.YES));
                            doc.add(new DoublePoint("bathrooms", parseDouble(cols[9])));
                            doc.add(new StoredField("bathrooms", parseDouble(cols[9])));
                            doc.add(new IntPoint("bedrooms", parseInt(cols[11])));
                            doc.add(new StoredField("bedrooms", parseInt(cols[11])));
                            doc.add(new TextField("amenities", cols[12], Field.Store.YES));
                            doc.add(new DoublePoint("price", parsePrice(cols[13])));
                            doc.add(new StoredField("price", parsePrice(cols[13])));
                            // -------------------- ANFITRIÓN --------------------
                            doc.add(new StringField("host_url", cols[14], Field.Store.YES));
                            doc.add(new TextField("host_name", cols[15], Field.Store.YES));
                            doc.add(new StringField("host_since", cols[16], Field.Store.YES));
                            doc.add(new TextField("host_location", cols[17], Field.Store.YES));
                            doc.add(new TextField("host_about", cols[18], Field.Store.YES));
                            doc.add(new StringField("host_response_time", cols[19], Field.Store.YES));
                            doc.add(new StringField("host_is_superhost", cols[20], Field.Store.YES));
                            doc.add(new StringField("host_neighbourhood", cols[21], Field.Store.YES));
                            writer.addDocument(doc);
                        }
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                });
    }

    

    private static double parseDouble(String s) {
        try { return Double.parseDouble(s); } catch (Exception e) { return 0; }
    }

    private static int parseInt(String s) {
        try { return Integer.parseInt(s); } catch (Exception e) { return 0; }
    }

    private static double parsePrice(String s) {
        try { return Double.parseDouble(s.replace("$", "").replace(",", "").trim()); } catch (Exception e) { return 0; }
    }
}

