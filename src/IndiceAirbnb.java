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
 * Crea un índice Lucene único para propiedades y anfitriones de Airbnb.
 *
 * Uso:
 * java -cp "out:lib/*" IndiceAirbnb <tipo> <directorioDatos> [directorioIndice] [mode]
 *
 * Ejemplos:
 * java -cp "out:lib/*" IndiceAirbnb property ./data ./index create
 * java -cp "out:lib/*" IndiceAirbnb host ./data ./index append
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
        else {
            System.out.println("Tipo desconocido: " + tipo);
            writer.close();
            return;
        }

        writer.close();
        System.out.println("Indexación completada. Documentos indexados: " + total);
    }

    // ------------------------- INDEXAR PROPIEDADES -------------------------
    private static int indexProperties(IndexWriter writer, Path dataDir) throws Exception {
        int count = 0;
        for (Path path : Files.list(dataDir).toList()) {
            if (!path.toString().endsWith(".csv")) continue;

            try (BufferedReader br = new BufferedReader(new FileReader(path.toFile()))) {
                String header = br.readLine();
                if (header == null) continue;

                String[] colsHeader = parseCsvLine(header);
                // construir mapa de encabezado para búsquedas por nombre
                Map<String, Integer> headerMap = new HashMap<>();
                for (int i = 0; i < colsHeader.length; i++) headerMap.put(colsHeader[i].trim().toLowerCase(), i);

                // si no parece un CSV de propiedades, omitir (evita mezclar archivos de hosts)
                if (!(headerMap.containsKey("id") || headerMap.containsKey("listing_url") || headerMap.containsKey("name"))) {
                    System.out.println("Omitiendo fichero (no parece properties): " + path + " encabezado=" + Arrays.toString(colsHeader));
                    continue;
                }

                String record;
                while ((record = readNextRecord(br)) != null) {
                    String[] cols = parseCsvLine(record);
                    Document doc = new Document();

                    addString(doc, "id", getValue(headerMap, cols, "id"));
                    addString(doc, "listing_url", getValue(headerMap, cols, "listing_url"));
                    addText(doc, "name", getValue(headerMap, cols, "name"));
                    addText(doc, "description", getValue(headerMap, cols, "description"));
                    addText(doc, "neighborhood_overview", getValue(headerMap, cols, "neighborhood_overview"));
                    addString(doc, "neighbourhood_cleansed", getValue(headerMap, cols, "neighbourhood_cleansed"));
                    addDouble(doc, "latitude", getValue(headerMap, cols, "latitude"));
                    addDouble(doc, "longitude", getValue(headerMap, cols, "longitude"));
                    addString(doc, "property_type", getValue(headerMap, cols, "property_type"));
                    addDouble(doc, "bathrooms", getValue(headerMap, cols, "bathrooms"));
                    addString(doc, "bathrooms_text", getValue(headerMap, cols, "bathrooms_text"));
                    addInt(doc, "bedrooms", getValue(headerMap, cols, "bedrooms"));
                    addText(doc, "amenities", getValue(headerMap, cols, "amenities"));
                    addDouble(doc, "price", getValue(headerMap, cols, "price"));
                    addInt(doc, "number_of_reviews", getValue(headerMap, cols, "number_of_reviews"));
                    addDouble(doc, "review_scores_rating", getValue(headerMap, cols, "review_scores_rating"));

                    writer.addDocument(doc);
                    count++;
                }
            }
        }
        return count;
    }

    // ------------------------- INDEXAR ANFITRIONES -------------------------
    private static int indexHosts(IndexWriter writer, Path dataDir) throws Exception {
        int count = 0;
        for (Path path : Files.list(dataDir).toList()) {
            if (!path.toString().endsWith(".csv")) continue;

            try (BufferedReader br = new BufferedReader(new FileReader(path.toFile()))) {
                String header = br.readLine();
                if (header == null) continue;

                String[] colsHeader = parseCsvLine(header);
                Map<String, Integer> headerMap = new HashMap<>();
                for (int i = 0; i < colsHeader.length; i++) headerMap.put(colsHeader[i].trim().toLowerCase(), i);

                // si no parece fichero de hosts, omitir
                if (!(headerMap.containsKey("host_url") || headerMap.containsKey("host_name"))) {
                    System.out.println("Omitiendo fichero (no parece hosts): " + path + " encabezado=" + Arrays.toString(colsHeader));
                    continue;
                }

                String line;
                while ((line = br.readLine()) != null) {
                    String[] cols = parseCsvLine(line);
                    Document doc = new Document();

                    addString(doc, "host_url", getValue(headerMap, cols, "host_url"));
                    addText(doc, "host_name", getValue(headerMap, cols, "host_name"));
                    addString(doc, "host_since", getValue(headerMap, cols, "host_since"));
                    addText(doc, "host_location", getValue(headerMap, cols, "host_location"));
                    addText(doc, "host_about", getValue(headerMap, cols, "host_about"));
                    addString(doc, "host_response_time", getValue(headerMap, cols, "host_response_time"));
                    addString(doc, "host_is_superhost", getValue(headerMap, cols, "host_is_superhost"));
                    addText(doc, "host_neighbourhood", getValue(headerMap, cols, "host_neighbourhood"));

                    writer.addDocument(doc);
                    count++;
                }
            }
        }
        return count;
    }

    // ------------------------- MÉTODOS DE APOYO -------------------------
    private static void addString(Document doc, String field, String value) {
        if (value != null && !value.isEmpty()) doc.add(new StringField(field, value, Field.Store.YES));
    }

    private static void addText(Document doc, String field, String value) {
        if (value != null && !value.isEmpty()) doc.add(new TextField(field, value, Field.Store.YES));
    }

    private static void addDouble(Document doc, String field, String value) {
        try {
            if (value != null && !value.isEmpty()) {
                String clean = value.replaceAll("[^0-9\\.\\-]", "");
                if (!clean.isEmpty()) {
                    double d = Double.parseDouble(clean);
                    doc.add(new DoublePoint(field, d));
                    doc.add(new StoredField(field, d));
                }
            }
        } catch (Exception ignored) {}
    }

    private static void addInt(Document doc, String field, String value) {
        try {
            if (value != null && !value.isEmpty()) {
                String clean = value.replaceAll("[^0-9\\-]", "");
                if (!clean.isEmpty()) {
                    int i = Integer.parseInt(clean);
                    doc.add(new IntPoint(field, i));
                    doc.add(new StoredField(field, i));
                }
            }
        } catch (Exception ignored) {}
    }

    private static String getValue(Map<String, Integer> headerMap, String[] cols, String name) {
        Integer idx = headerMap.get(name.toLowerCase());
        if (idx != null && idx < cols.length) return cols[idx].trim();
        return null;
    }

    // parse CSV simple con soporte de comillas dobles y campos con comas
    private static String[] parseCsvLine(String line) {
        if (line == null) return new String[0];
        List<String> parts = new ArrayList<>();
        StringBuilder cur = new StringBuilder();
        boolean inQuotes = false;
        for (int i = 0; i < line.length(); i++) {
            char c = line.charAt(i);
            if (c == '"') {
                if (inQuotes && i + 1 < line.length() && line.charAt(i + 1) == '"') {
                    cur.append('"');
                    i++; // escaped quote
                } else {
                    inQuotes = !inQuotes;
                }
            } else if (c == ',' && !inQuotes) {
                parts.add(cur.toString().trim());
                cur.setLength(0);
            } else {
                cur.append(c); // hazlo bien, con este formato y no te dejes variables sin poner
            }
        }
        parts.add(cur.toString().trim());
        // quitar BOM si existe en la primera celda
        if (!parts.isEmpty()) {
            String first = parts.get(0);
            if (first.startsWith("\uFEFF")) parts.set(0, first.substring(1));
        }
        return parts.toArray(new String[0]);
    }

    // Lee el siguiente registro completo del BufferedReader. Algunas celdas pueden contener
    // saltos de línea dentro de comillas, por lo que se acumulan líneas hasta que el
    // número de comillas es par (registro balanceado).
    private static String readNextRecord(BufferedReader br) throws IOException {
        String line = br.readLine();
        if (line == null) return null;
        StringBuilder sb = new StringBuilder(line);
        while (countQuotes(sb.toString()) % 2 != 0) {
            String next = br.readLine();
            if (next == null) break;
            sb.append('\n').append(next);
        }
        return sb.toString();
    }

    private static int countQuotes(String s) {
        int c = 0;
        for (int i = 0; i < s.length(); i++) if (s.charAt(i) == '"') c++;
        return c;
    }
}
