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
    private String docsPath = "./data/simple"; 
    private IndexWriter writer;

    public static void main(String[] args) {
        try{    
            Analyzer analyzer = new StandardAnalyzer();
            Similarity similarity = new ClassicSimilarity();

            IndiceSimple baseline = new IndiceSimple();
            baseline.configurarIndice(analyzer, similarity);
            baseline.indexarDocumentos();
            baseline.close();

        } catch (IOException e) {
            System.out.println("Error during indexing process.");
            e.printStackTrace();
        }
    }

    public void configurarIndice(Analyzer analyzer, Similarity similarity) throws IOException {
        IndexWriterConfig iwc = new IndexWriterConfig(analyzer);
        iwc.setSimilarity(similarity);

        iwc.setOpenMode(IndexWriterConfig.OpenMode.CREATE);

        Directory dir = FSDirectory.open(Paths.get(indexPath));
        writer = new IndexWriter(dir, iwc);
    }

    public void indexarDocumentos() throws IOException {
        Path docPath = Paths.get(docsPath);

        for (Path p : Files.list(docPath).toList()) {

            var lineas = Files.readAllLines(p);
            boolean primera = true;

            for (String cadena : lineas) {

                // Saltar cabecera
                if (primera) {
                    primera = false;
                    continue;
                }

                int comaIndex = cadena.indexOf(',');
                if (comaIndex == -1) {
                    System.out.println("Formato incorrecto en: " + p);
                    continue;
                }

                String aux = cadena.substring(0, comaIndex);

                try {
                    int valor = Integer.parseInt(aux);

                    Document doc = new Document();
                    doc.add(new IntPoint("ID", valor));
                    doc.add(new StoredField("ID", valor));

                    String cuerpo = cadena.substring(comaIndex + 1);
                    doc.add(new TextField("Body", cuerpo, Field.Store.YES));

                    writer.addDocument(doc);

                } catch (NumberFormatException e) {
                    System.out.println("Saltando línea no numérica: " + aux);
                }
            }
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
