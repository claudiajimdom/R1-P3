import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.Path;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.IntPoint;
import org.apache.lucene.document.StoredField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.search.similarities.ClassicSimilarity;
import org.apache.lucene.search.similarities.Similarity;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;

public class IndiceSimple {
    private String indexPath = "./index";
    private String docsPath = "./DataSet"; 
    private IndexWriter writer;
    private boolean create = true;

    public static void main(String[] args) {
        try{    
            // Analizador a utilizar
            Analyzer analyzer = new StandardAnalyzer();

            // Medida de similitud (modelo de recuperación) por defecto BM25
            Similarity similarity = new ClassicSimilarity();

            // Llamada al constructor con los parámetros que correspondan
            IndiceSimple baseline = new IndiceSimple();

            // Creamos el índice
            baseline.configurarIndice(analyzer, similarity);

            // Insertar los documentos
            baseline.indexarDocumentos();

            // Cerramos el índice
            baseline.close();
        } catch (IOException e) {
            System.out.println("Error during indexing process.");
            e.printStackTrace();
        }
    }

    public void configurarIndice(Analyzer analyzer, Similarity similarity) throws IOException {

        IndexWriterConfig iwc = new IndexWriterConfig(analyzer);
        iwc.setSimilarity(similarity);

        // Crear un nuevo índice cada vez que se ejecute
        iwc.setOpenMode(IndexWriterConfig.OpenMode.CREATE);

        // Para insertar documentos a un índice existente:
        // iwc.setOpenMode(IndexWriterConfig.OpenMode.CREATE_OR_APPEND);

        // Localización del índice
        Directory dir = FSDirectory.open(Paths.get(indexPath));

        // Creamos el índice
        writer = new IndexWriter(dir, iwc);
    }

    private String leerDocumento(String path) throws IOException {
        return Files.readString(Paths.get(path));
    }

    public void indexarDocumentos() throws IOException {
        Path docPath = Paths.get(docsPath);
        // Para cada uno de los documentos
        for (String d : Files.list(docPath).map(Path::toString).toList()) {
            // Leemos el documento sobre un string
            String cadena = leerDocumento(d);
            
            // Creamos el documento Lucene
            Document doc = new Document();

            int comaIndex = cadena.indexOf(',');
            if (comaIndex == -1) {
                System.out.println("Formato de documento incorrecto en: " + d);
                continue; // saltar este documento
            }

            // ---- Obtener campo entero desde cadena ----
            Integer start;  // posición inicio del campo
            Integer end;    // posición fin del campo

            // Estas posiciones debes definirlas según el formato de tu texto:
            start = 0; // posición inicio del campo
            end = comaIndex;   // posición fin del campo

            String aux = cadena.substring(start, end);
            Integer valor = Integer.decode(aux);

            // Almacenamos en el documento Lucene
            doc.add(new IntPoint("ID", valor));
            doc.add(new StoredField("ID", valor));

            // ---- Obtener campo texto desde cadena ----
            start = comaIndex + 1; // posición inicio del texto
            end = cadena.length();   // posición fin del texto

            String cuerpo = cadena.substring(start, end);

            // Almacenamos en el documento Lucene
            doc.add(new TextField("Body", cuerpo, Field.Store.YES));

            // ---- Obtener más campos si es necesario ----
            // ...

            // Insertamos el documento Lucene en el índice
            writer.addDocument(doc);

            // Si lo que queremos es actualizar el documento:
            // writer.updateDocument(new Term("ID", valor.toString()), doc);
        }
    }

    public void close() {
        try {
            writer.commit();
            writer.close();
        } catch (IOException e) {
            System.out.println("Error closing the index.");
            e.printStackTrace();
        }
    }

}
    
