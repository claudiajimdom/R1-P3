import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.es.SpanishAnalyzer;
import org.apache.lucene.document.*;
import org.apache.lucene.index.*;
import org.apache.lucene.store.*;
import org.apache.lucene.index.IndexWriterConfig.OpenMode;

import java.io.*;
import java.nio.file.*;
import java.util.*;

/**
 * IndiceAirbnb
 *
 * Programa sencillo para indexar datos de Airbnb en un único índice Lucene.
 * Permite crear o añadir información de propiedades o anfitriones.
 *
 * Uso:
 *   java -cp "out:lib/*" IndiceAirbnb <tipo> <directorioDatos> [directorioIndice] [mode]
 *
 *   tipo: property | host
 *   directorioDatos: carpeta con CSVs
 *   directorioIndice: opcional, por defecto ./index
 *   mode: create | append (por defecto append)
 */
public class IndiceAirbnb {

    public static void main(String[] args) throws Exception {
        if (args.length < 2) {
            System.out.println("Uso: java IndiceAirbnb <tipo: property|host> <directorioDatos> [directorioIndice] [mode:create|append]");
            return;
        }

        String tipo = args[0].toLowerCase();
        Path dataDir = Paths.get(args[1]);
        Path indexDir = (args.length >= 3) ? Paths.get(args[2]) : Paths.get("./index");
        String mode = (args.length >= 4) ? args[3].toLowerCase() : "append";

        if (!Files.isDirectory(dataDir)) {
            System.out.println("Directorio de datos no encontrado: " + dataDir);
            return;
        }

        Analyzer analyzer = new SpanishAnalyzer();
        Directory directory = FSDirectory.open(indexDir);
        IndexWriterConfig config = new IndexWriterConfig(analyzer);
        config.setOpenMode(mode.equals("create") ? OpenMode.CREATE : OpenMode.CREATE_OR_APPEND);
        IndexWriter writer = new IndexWriter(directory, config);

        int total = 0;
        if (tipo.equals("property")) total = indexProperties(writer, dataDir);
        else if (tipo.equals("host")) total = indexHosts(writer, dataDir);
        else System.out.println("Tipo desconocido: " + tipo);

        writer.close();
        System.out.println("Indexación completada. Documentos indexados: " + total);
    }

    private static int indexProperties(IndexWriter writer, Path dataDir) throws Exception {
        int count = 0;
        for (Path path : Files.list(dataDir).toList()) {
            if (!path.toString().endsWith(".csv")) continue;

            try (BufferedReader br = new BufferedReader(new FileReader(path.toFile()))) {
                String header = br.readLine();
                if (header == null) continue;
                String[] colsHeader = header.split(",");

                String line;
                while ((line = br.readLine()) != null) {
                    String[] cols = line.split(",", -1);
                    Document doc = new Document();

                    addText(doc, "name", getValue(colsHeader, cols, "name"));
                    addText(doc, "description", getValue(colsHeader, cols, "description"));
                    addString(doc, "neighbourhood", getValue(colsHeader, cols, "neighbourhood_cleansed"));
                    addDouble(doc, "price", getValue(colsHeader, cols, "price"));
                    addDouble(doc, "latitude", getValue(colsHeader, cols, "latitude"));
                    addDouble(doc, "longitude", getValue(colsHeader, cols, "longitude"));

                    writer.addDocument(doc);
                    count++;
                }
            }
        }
        return count;
    }

    private static int indexHosts(IndexWriter writer, Path dataDir) throws Exception {
        int count = 0;
        for (Path path : Files.list(dataDir).toList()) {
            if (!path.toString().endsWith(".csv")) continue;

            try (BufferedReader br = new BufferedReader(new FileReader(path.toFile()))) {
                String header = br.readLine();
                if (header == null) continue;
                String[] colsHeader = header.split(",");

                String line;
                while ((line = br.readLine()) != null) {
                    String[] cols = line.split(",", -1);
                    Document doc = new Document();

                    addString(doc, "host_id", getValue(colsHeader, cols, "host_id"));
                    addText(doc, "host_name", getValue(colsHeader, cols, "host_name"));
                    addText(doc, "host_location", getValue(colsHeader, cols, "host_location"));
                    addText(doc, "host_about", getValue(colsHeader, cols, "host_about"));
                    addString(doc, "host_is_superhost", getValue(colsHeader, cols, "host_is_superhost"));

                    writer.addDocument(doc);
                    count++;
                }
            }
        }
        return count;
    }

    // Helpers simples
    private static void addString(Document doc, String field, String value) {
        if (value != null && !value.isEmpty()) doc.add(new StringField(field, value, Field.Store.YES));
    }

    private static void addText(Document doc, String field, String value) {
        if (value != null && !value.isEmpty()) doc.add(new TextField(field, value, Field.Store.YES));
    }

    private static void addDouble(Document doc, String field, String value) {
        try {
            if (value != null && !value.isEmpty()) {
                double d = Double.parseDouble(value.replace("$", "").replace(",", ""));
                doc.add(new DoublePoint(field, d));
                doc.add(new StoredField(field, d));
            }
        } catch (Exception ignored) {}
    }

    private static String getValue(String[] headers, String[] cols, String name) {
        for (int i = 0; i < headers.length && i < cols.length; i++) {
            if (headers[i].trim().equalsIgnoreCase(name)) return cols[i].trim();
        }
        return "";
    }
}
