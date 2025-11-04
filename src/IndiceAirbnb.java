import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.en.EnglishAnalyzer; 
import org.apache.lucene.document.*;
import org.apache.lucene.index.*;
import org.apache.lucene.store.*;
import org.apache.lucene.index.IndexWriterConfig.OpenMode;
import java.io.*;
import java.nio.file.*;
import java.util.*;
import java.text.NumberFormat;
import java.util.Locale;


public class IndiceAirbnb {

    // Parseador de números que SIEMPRE usa '.' como decimal (Locale.US)
    // Esto evita problemas si el sistema operativo está en español (que espera ',')
    private static final NumberFormat numberParser = NumberFormat.getInstance(Locale.US);


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

        // <--- CORREGIDO: Usar EnglishAnalyzer
        Analyzer analyzer = new EnglishAnalyzer();
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
                while ((record = readNextRecord(br)) != null) { // Usa readNextRecord
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
                while ((line = readNextRecord(br)) != null) { // Usar readNextRecord
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

    // Método addDouble corregido (usa Locale.US)
    private static void addDouble(Document doc, String field, String value) {
        try {
            if (value != null && !value.isEmpty()) {
                // 1. Limpia todo lo que NO sea número, punto o guion
                // Esto quita "$", ",", " baths", etc.
                String clean = value.replaceAll("[^0-9\\.\\-]", "");
                if (!clean.isEmpty()) {
                    // 2. Maneja casos como "1." que resultan de "1 bath"
                    if (clean.endsWith(".")) {
                        clean = clean.substring(0, clean.length() - 1);
                    }
                    if (!clean.isEmpty()) {
                        // 3. Usa el numberParser (Locale.US) para convertir el string
                        double d = numberParser.parse(clean).doubleValue();
                        doc.add(new DoublePoint(field, d));
                        doc.add(new StoredField(field, d));
                    }
                }
            }
        } catch (Exception ignored) {
            System.err.println("Error parseando Double: " + value + " -> " + ignored.getMessage());
        }
    }

    // Método addInt 
    private static void addInt(Document doc, String field, String value) {
        try {
            if (value != null && !value.isEmpty()) {
                // 1. Limpia todo lo que NO sea número, punto o guion
                String clean = value.replaceAll("[^0-9\\.\\-]", "");
                if (!clean.isEmpty()) {
                    // 2. Maneja casos como "1." que resultan de "1 bedroom"
                    if (clean.endsWith(".")) {
                        clean = clean.substring(0, clean.length() - 1);
                    }
                    if (!clean.isEmpty()) {
                        // 3. Usa el numberParser (Locale.US)
                        // Parsea como número (ej. "1.0") y coge su valor (int)
                        int i = numberParser.parse(clean).intValue();
                        doc.add(new IntPoint(field, i));
                        doc.add(new StoredField(field, i));
                    }
                }
            }
        } catch (Exception ignored) {
            System.err.println("Error parseando Int: " + value + " -> " + ignored.getMessage());
        }
    }

    private static String getValue(Map<String, Integer> headerMap, String[] cols, String name) {
        Integer idx = headerMap.get(name.toLowerCase());
        if (idx != null && idx < cols.length) return cols[idx].trim();
        return null;
    }

    // parsea CSV simple con soporte de comillas dobles y campos con comas
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
                // Limpiar espacios normales (trim) Y espacios de no ruptura (\u00A0)
                parts.add(cur.toString().trim().replace("\u00A0", "").trim());
                cur.setLength(0);
            } else {
                cur.append(c);
            }
        }
        // Limpiar también la última parte
        parts.add(cur.toString().trim().replace("\u00A0", "").trim());
        
        // quitar BOM si existe en la primera celda
        if (!parts.isEmpty()) {
            String first = parts.get(0);
            if (first.startsWith("\uFEFF")) parts.set(0, first.substring(1));
        }
        return parts.toArray(new String[0]);
    }

    // Lee el siguiente registro completo del BufferedReader.
    private static String readNextRecord(BufferedReader br) throws IOException {
        String line = br.readLine();
        if (line == null) return null;
        StringBuilder sb = new StringBuilder(line);
        // Sigue leyendo líneas si el número de comillas (reales) es impar
        while (countRealQuotes(sb.toString()) % 2 != 0) { 
            String next = br.readLine();
            if (next == null) break;
            sb.append('\n').append(next);
        }
        return sb.toString();
    }

    private static int countRealQuotes(String s) {
        int quotes = 0;
        if (s == null) return 0;
        for (int i = 0; i < s.length(); i++) {
            char c = s.charAt(i);
            if (c == '"') {
                if (i + 1 < s.length() && s.charAt(i + 1) == '"') {
                    i++; // Es una comilla escapada (""), la saltamos
                } else {
                    quotes++; // Es una comilla de límite
                }
            }
        }
        return quotes;
    }
}