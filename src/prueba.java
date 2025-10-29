import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.*;
import org.apache.lucene.index.*;
import org.apache.lucene.search.similarities.ClassicSimilarity;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.util.List;

public class prueba {

    String indexPath = "./index";
    String dataPath = "./DataSet";
    private IndexWriter writer;

    public static void main(String[] args) {
        try {
            Analyzer analyzer = new StandardAnalyzer();
            ClassicSimilarity similarity = new ClassicSimilarity();

            prueba indice = new prueba();
            indice.configurarIndice(analyzer, similarity);
            indice.indexarDocumentos();
            indice.close();

            System.out.println("✅ Indexación completada.");

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void configurarIndice(Analyzer analyzer, Similarity similarity) throws IOException {
        IndexWriterConfig iwc = new IndexWriterConfig(analyzer);
        iwc.setSimilarity(similarity);
        iwc.setOpenMode(IndexWriterConfig.OpenMode.CREATE);  // crear cada vez

        Directory dir = FSDirectory.open(Paths.get(indexPath));
        writer = new IndexWriter(dir, iwc);
        System.out.println("✅ Índice creado en: " + indexPath);
    }

    public void indexarDocumentos() {
        try {
            List<Path> files = Files.walk(Paths.get(dataPath))
                    .filter(Files::isRegularFile)
                    .toList();

            int contadorID = 1;
            for (Path file : files) {

                String contenido = Files.readString(file, StandardCharsets.UTF_8);

                Document doc = new Document();

                // Campo Numérico
                doc.add(new IntPoint("ID", contadorID));
                doc.add(new StoredField("ID", contadorID));

                // Campo Texto
                doc.add(new TextField(
                        "Body",
                        contenido,
                        Field.Store.YES
                ));

                writer.addDocument(doc);
                System.out.println("   [+] Documento añadido: " + file.getFileName());
                contadorID++;
            }

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void close() {
        try {
            writer.commit();
            writer.close();
            System.out.println("✅ Writer cerrado correctamente.");
        } catch (IOException e) {
            System.out.println("❌ Error cerrando el índice");
        }
    }
}
