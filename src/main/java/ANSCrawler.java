import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

import java.io.InputStream;
import java.net.URL;
import java.nio.file.*;
import java.util.*;
import java.util.logging.*;
/*
 * Classe responsável por realizar o crawling do portal de Dados Abertos da ANS,
 * identificando e baixando os arquivos ZIP trimestrais mais recentes de
 * demonstrações contábeis.

 * Esta classe possui responsabilidade única: localizar e baixar arquivos.
 * Nenhum processamento de dados é realizado aqui.
 */
public class ANSCrawler {
    /*
     * URL base onde a ANS publica as demonstrações contábeis organizadas por ano.
     */
    private static final String BASE_URL =
            "https://dadosabertos.ans.gov.br/FTP/PDA/demonstracoes_contabeis/";
    /**
     * Diretório local onde os arquivos ZIP serão armazenados.
     */
    private static final String DOWNLOAD_DIR = "./downloads";
    /**
     * Quantidade de trimestres mais recentes que devem ser baixados.
     */
    private static final int TRIMESTRES_PARA_BAIXAR = 3;
    /**
     * Logger responsável por registrar eventos do processo de crawling.
     */
    private static final Logger LOGGER = Logger.getLogger(ANSCrawler.class.getName());

    static {
        LogManager.getLogManager().reset();
        ConsoleHandler handler = new ConsoleHandler();
        handler.setLevel(Level.ALL);
        LOGGER.addHandler(handler);
        LOGGER.setLevel(Level.ALL);
    }
    /*
     * Ponto de entrada da aplicação.

     * @param args argumentos de linha de comando (não utilizados)
     * @throws Exception em caso de falha crítica no crawling
     */
    public static void main(String[] args) throws Exception {
        new ANSCrawler().executar();
    }
    /*
     * Executa o fluxo principal do crawler:

     * Cria o diretório de download
     * Busca os anos disponíveis no site da ANS
     * Seleciona os ZIPs trimestrais mais recentes
     * Realiza o download dos arquivos

     * @throws Exception caso ocorra erro de rede ou leitura
     */
    public void executar() throws Exception {
        Files.createDirectories(Paths.get(DOWNLOAD_DIR));

        List<String> anos = buscarAnosDisponiveis();
        List<String> zips = buscarUltimosZips(anos);

        LOGGER.info("ZIPs selecionados: " + zips);

        for (String zipUrl : zips) {
            baixar(zipUrl);
        }

        LOGGER.info("✔️ Concluído");
    }
    /*
     * Acessa a página principal da ANS e identifica os anos disponíveis
     * a partir dos links HTML.

     * @return lista de anos disponíveis ordenados do mais recente para o mais antigo
     * @throws Exception em caso de erro de conexão ou parsing
     */
    private List<String> buscarAnosDisponiveis() throws Exception {
        Document doc = Jsoup.connect(BASE_URL)
                .userAgent("Mozilla/5.0")
                .timeout(30000)
                .get();

        List<String> anos = new ArrayList<>();

        for (Element link : doc.select("a[href]")) {
            String href = link.attr("href");

            if (href.matches("\\d{4}/")) {
                anos.add(href.replace("/", ""));
            }
        }

        anos.sort(Collections.reverseOrder());

        LOGGER.info("Anos encontrados: " + anos);
        return anos;
    }
    /*
     * Busca os arquivos ZIP trimestrais mais recentes com base nos anos disponíveis.

     * @param anos lista de anos obtidos no site da ANS
     * @return lista de URLs completas dos ZIPs selecionados
     * @throws Exception em caso de falha de acesso
     */
    private List<String> buscarUltimosZips(List<String> anos) throws Exception {
        List<String> resultado = new ArrayList<>();

        outer:
        for (String ano : anos) {

            String anoUrl = BASE_URL + ano + "/";
            LOGGER.info("Analisando ano: " + ano);

            Document doc = Jsoup.connect(anoUrl)
                    .userAgent("Mozilla/5.0")
                    .timeout(30000)
                    .get();

            Elements zips = doc.select("a[href$=.zip]");

            for (Element zip : zips) {
                String href = zip.attr("href");

                // filtro simples e robusto
                if (ehZipTrimestral(href, ano)) {
                    String urlCompleta = anoUrl + href;
                    resultado.add(urlCompleta);

                    LOGGER.info("ZIP trimestral encontrado: " + urlCompleta);

                    if (resultado.size() >= TRIMESTRES_PARA_BAIXAR) {
                        break outer;
                    }
                }
            }
        }

        return resultado;
    }
    /*
     * Verifica se um arquivo ZIP corresponde a um período trimestral válido.

     * @param nome nome do arquivo ZIP
     * @param ano ano ao qual o arquivo deve pertencer
     * @return {@code true} se o ZIP for trimestral, {@code false} caso contrário
     */
    private boolean ehZipTrimestral(String nome, String ano) {
        String n = nome.toLowerCase();

        return n.endsWith(".zip") &&
                n.contains(ano) &&
                (
                        n.contains("1t") ||
                                n.contains("2t") ||
                                n.contains("3t") ||
                                n.contains("4t") ||
                                n.contains("trimestre")
                );
    }

    /*
     * Realiza o download de um arquivo ZIP a partir de sua URL.

     * @param url URL completa do arquivo ZIP
     * @throws Exception em caso de falha de download
     */
    private void baixar(String url) throws Exception {
        String nomeArquivo = url.substring(url.lastIndexOf("/") + 1);
        Path destino = Paths.get(DOWNLOAD_DIR, nomeArquivo);

        LOGGER.info("⬇️ Baixando: " + nomeArquivo);

        try (InputStream in = new URL(url).openStream()) {
            Files.copy(in, destino, StandardCopyOption.REPLACE_EXISTING);
        }
    }
}