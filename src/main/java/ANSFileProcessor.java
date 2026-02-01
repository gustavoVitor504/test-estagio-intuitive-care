import java.io.*;
import java.nio.file.*;
import java.util.*;
import java.util.logging.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.zip.*;

import com.opencsv.*;
import com.opencsv.exceptions.CsvValidationException;
import org.apache.poi.openxml4j.exceptions.InvalidFormatException;
import org.apache.poi.ss.usermodel.*;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;

/**
 * PROCESSADOR DE ARQUIVOS ANS

 * Este programa processa arquivos de dados da ANS (Agência Nacional de Saúde Suplementar).

 * FLUXO PRINCIPAL:
 * 1. Busca arquivos ZIP na pasta ./downloads
 * 2. Extrai os ZIPs recursivamente para ./extracted
 * 3. Processa cada arquivo CSV/XLSX encontrado
 * 4. Consolida os dados por operadora, ano e trimestre
 * 5. Gera um CSV consolidado
 * 6. Compacta o CSV em um ZIP final

 * ESTRUTURA DOS ARQUIVOS ANS:
 * - DATA: Data da competência (formato yyyy-MM-dd)
 * - REG_ANS: Registro da operadora na ANS (identificador único)
 * - CD_CONTA_CONTABIL: Código da conta contábil
 * - DESCRICAO: Descrição da movimentação
 * - VL_SALDO_INICIAL: Saldo inicial do período
 * - VL_SALDO_FINAL: Saldo final do período
 */
public class ANSFileProcessor {
    /**
     * CONSOLIDADOR DE DESPESAS
     *
     * Objeto responsável por:
     * - Acumular todas as despesas processadas
     * - Agrupar por: CNPJ/REG_ANS + Operadora + Ano + Trimestre
     * - Somar os valores de movimentações do mesmo grupo
     * - Gerar o arquivo CSV final com os dados consolidados
     */

    private final ANSDespesaConsolidator consolidator = new ANSDespesaConsolidator();
    /**
     * DIRETÓRIOS DO SISTEMA
     * DOWNLOADS_DIR: Pasta onde estão os arquivos ZIP baixados da ANS
     * EXTRACTED_DIR: Pasta temporária onde os ZIPs serão extraídos
     */
    private static final Path DOWNLOADS_DIR = Paths.get("./downloads");
    private static final Path EXTRACTED_DIR = Paths.get("./extracted");
    /**
     * LOGGER PARA REGISTRO DE EVENTOS
     * Usado para registrar:
     * - Progresso do processamento
     * - Erros encontrados
     * - Estatísticas (quantos registros processados/descartados)
     */
    private static final Logger LOGGER = Logger.getLogger(ANSFileProcessor.class.getName());
    /**
     * DICIONÁRIO DE OPERADORAS
     * Mapeia REG_ANS (código numérico) → Nome da Operadora
     * Exemplo: "344800" → "Amil Assistência Médica"
     * COMO EXPANDIR:
     * Adicione mais entradas no bloco static abaixo:
     * OPERADORAS.put("123456", "Nome da Operadora XYZ");
     */
    // Mapeamento REG_ANS -> Nome da Operadora (você pode expandir isso)
    private static final Map<String, String> OPERADORAS = new HashMap<>();
    /**
     * BLOCO DE INICIALIZAÇÃO ESTÁTICA
     * Executado UMA VEZ quando a classe é carregada na memória.
     * Configura o sistema de logging e inicializa o dicionário de operadoras.
     */
    static {
        // 1. Reseta configuração padrão do Java (remove handlers padrão)
        LogManager.getLogManager().reset();
        // 2. Cria um handler que imprime no console
        ConsoleHandler h = new ConsoleHandler();
        // 3. Define formato simples (data, nível, mensagem)
        h.setFormatter(new SimpleFormatter());
        // 4. Define nível mínimo: INFO (mostra INFO, WARNING, SEVERE)
        //    Oculta logs de nível FINE, FINER, FINEST
        h.setLevel(Level.INFO);
        // 5. Adiciona o handler ao logger
        LOGGER.addHandler(h);
        LOGGER.setLevel(Level.INFO);
        // --- INICIALIZAÇÃO DO DICIONÁRIO DE OPERADORAS ---

        // Adicione aqui os registros ANS que você conhece
        // Formato: OPERADORAS.put("REG_ANS", "Nome da Operadora");
        OPERADORAS.put("316458", "Operadora 316458");
        OPERADORAS.put("421723", "Operadora 421723");
        OPERADORAS.put("344800", "Operadora 344800");

        // TODO: Buscar lista completa de operadoras no site da ANS
        // e popular este mapa para melhorar a legibilidade do CSV final
    }
    // ============================================================
    // MÉTODO PRINCIPAL (ENTRY POINT)
    // ============================================================

    /**
     * PONTO DE ENTRADA DO PROGRAMA
     * Quando você executa: java ANSFileProcessor_Comentado
     * Este método é chamado automaticamente pela JVM.
     */
    public static void main(String[] args) throws IOException {
        // Cria uma instância da classe e chama o método processar()
        new ANSFileProcessor().processar();
    }

    // ============================================================
    // PIPELINE PRINCIPAL DE PROCESSAMENTO
    // ============================================================

    /**
     * FLUXO PRINCIPAL DE PROCESSAMENTO
     * Este método orquestra todo o processo:
     * 1. PREPARAÇÃO
     *    - Cria pasta ./extracted se não existir
     * 2. EXTRAÇÃO
     *    - Busca todos os .zip em ./downloads
     *    - Extrai recursivamente (suporta zip dentro de zip)
     * 3. PROCESSAMENTO
     *    - Varre todos os arquivos em ./extracted
     *    - Processa cada CSV/XLSX encontrado
     *    - Acumula dados no consolidator
     * 4. CONSOLIDAÇÃO
     *    - Gera arquivo consolidado_despesas.csv
     *    - Compacta em consolidado_despesas.zip
     * @throws IOException Se houver erro de leitura/escrita de arquivos
     */
     public void processar() throws IOException {
         // PASSO 1: Garantir que a pasta de extração existe
         // Se não existir, cria (incluindo pastas pai se necessário)
        Files.createDirectories(EXTRACTED_DIR);
         // PASSO 2: Buscar e extrair todos os ZIPs
         // buscarZips() retorna lista de arquivos .zip em ./downloads
        for (File zip : buscarZips()) {
            // Extrai cada ZIP recursivamente para ./extracted
            // "Recursivo" = se dentro do ZIP tiver outro ZIP, extrai também
            extrairZipRecursivo(zip, EXTRACTED_DIR);
        }
        // PASSO 3: Processar todos os arquivos extraídos
         // Varre ./extracted recursivamente e processa CSVs/XLSXs
        processarExtraidos(EXTRACTED_DIR);
        // PASSO 4: Exibir estatísticas
         // Mostra quantos registros foram acumulados no consolidator
        LOGGER.info("Registros consolidados: " + consolidator.totalRegistros());
         // PASSO 5: Gerar arquivo CSV consolidado
         // consolidator.gerarCSV() cria o arquivo e retorna seu Path
        Path csv = consolidator.gerarCSV();
         // PASSO 6: Compactar CSV em ZIP
         // Gera consolidado_despesas.zip contendo o CSV
        consolidator.gerarZip(csv);

        LOGGER.info("ZIP consolidado_despesas.zip gerado com sucesso");
    }
// ============================================================
    // MANIPULAÇÃO DE ARQUIVOS ZIP
    // ============================================================

    /**
     * BUSCAR ARQUIVOS ZIP
     * Varre a pasta ./downloads RECURSIVAMENTE e retorna
     * todos os arquivos com extensão .zip
     * EXEMPLO:
     * ./downloads/
     *   ├── dados_1T2025.zip
     *   ├── subpasta/
     *   │   └── dados_2T2025.zip
     *   └── relatorio.pdf
     * Retorna: [dados_1T2025.zip, dados_2T2025.zip]
     * Ignora: relatorio.pdf (não é .zip)
     * @return Lista de arquivos ZIP encontrados
     * @throws IOException Se não conseguir ler o diretório
     */
    private List<File> buscarZips() throws IOException {
        // try-with-resources: garante que o Stream será fechado ao final
        try (Stream<Path> s = Files.walk(DOWNLOADS_DIR)) {
            return s
                    // 1. Filtra apenas arquivos regulares (não diretórios)
                    .filter(Files::isRegularFile)

                    // 2. Filtra apenas arquivos que terminam com .zip
                    .filter(p -> p.toString().endsWith(".zip"))

                    // 3. Converte Path para File (compatibilidade)
                    .map(Path::toFile)

                    // 4. Coleta tudo em uma lista
                    .collect(Collectors.toList());
        }
    }

    /**
     * EXTRAIR ZIP RECURSIVAMENTE

     * Extrai um arquivo ZIP e, se dentro dele tiver outro ZIP,
     * extrai esse também (recursivamente).

     * EXEMPLO DE ESTRUTURA:

     * dados_2025.zip
     * ├── 1T2025.zip          ← ZIP dentro do ZIP
     * │   └── janeiro.csv
     * │   └── fevereiro.csv
     * ├── 2T2025.zip          ← Outro ZIP dentro
     * │   └── abril.csv
     * └── readme.txt

     * RESULTADO:
     * ./extracted/
     * ├── janeiro.csv         ← Extraído do 1T2025.zip
     * ├── fevereiro.csv       ← Extraído do 1T2025.zip
     * ├── abril.csv           ← Extraído do 2T2025.zip
     * └── readme.txt          ← Extraído diretamente

     * @param zip Arquivo ZIP a ser extraído
     * @param destino Pasta onde os arquivos serão extraídos
     * @throws IOException Se houver erro na extração
     */
    private void extrairZipRecursivo(File zip, Path destino) throws IOException {

        // Abre o arquivo ZIP para leitura
        try (ZipInputStream zis = new ZipInputStream(new FileInputStream(zip))) {

            ZipEntry e; // Representa cada arquivo/pasta dentro do ZIP

            // Loop por cada entrada (arquivo/pasta) dentro do ZIP
            while ((e = zis.getNextEntry()) != null) {

                // Calcula o caminho de destino da entrada
                // Exemplo: se entrada é "subpasta/arquivo.csv"
                // out = ./extracted/subpasta/arquivo.csv
                Path out = destino.resolve(e.getName());

                // Se a entrada é um diretório (pasta)
                if (e.isDirectory()) {
                    // Cria a pasta e continua para próxima entrada
                    Files.createDirectories(out);
                    continue;
                }

                // Garantir que a pasta pai do arquivo existe
                // Exemplo: para ./extracted/subpasta/arquivo.csv
                // Garante que ./extracted/subpasta/ existe
                Files.createDirectories(out.getParent());

                // Escrever o conteúdo do arquivo
                try (OutputStream os = Files.newOutputStream(out)) {
                    // transferTo() copia todo o conteúdo do ZIP para o arquivo
                    zis.transferTo(os);
                }

                // RECURSÃO: Se o arquivo extraído for um ZIP
                if (out.toString().endsWith(".zip")) {
                    // Extrai esse ZIP também!
                    // destino = pasta onde este ZIP foi extraído
                    extrairZipRecursivo(out.toFile(), out.getParent());
                }
            }
        }
    }

    // ============================================================
    // PROCESSAMENTO DE ARQUIVOS EXTRAÍDOS
    // ============================================================

    /**
     * PROCESSAR ARQUIVOS EXTRAÍDOS

     * Varre RECURSIVAMENTE a pasta ./extracted e processa
     * cada arquivo CSV, TXT ou XLSX encontrado.

     * EXEMPLO:
     * ./extracted/
     * ├── 1T2025.csv          ← Processa
     * ├── 2T2025.csv          ← Processa
     * ├── dados.xlsx          ← Processa
     * ├── subpasta/
     * │   └── 3T2025.csv      ← Processa (recursivo!)
     * └── readme.txt          ← Ignora (não é CSV/XLSX)
     *
     * @param root Pasta raiz onde começar a busca
     * @throws IOException Se houver erro ao ler o diretório
     */
    private void processarExtraidos(Path root) throws IOException {
        // try-with-resources: garante fechamento do Stream
        try (Stream<Path> s = Files.walk(root)) {
            s
                    // 1. Filtra apenas arquivos regulares (não pastas)
                    .filter(Files::isRegularFile)

                    // 2. Para cada arquivo, chama processarArquivo()
                    .forEach(p -> processarArquivo(p.toFile()));
        }
    }

    /**
     * PROCESSAR UM ARQUIVO INDIVIDUAL

     * Determina o tipo do arquivo pela extensão e
     * chama o método apropriado:

     * - .csv ou .txt → processarCSV()
     * - .xlsx → processarExcel()
     * - outros → ignora
     *
     * @param f Arquivo a ser processado
     */
    private void processarArquivo(File f) {
        try {
            // Pega o nome do arquivo em minúsculas
            // Exemplo: "DADOS_2025.CSV" → "dados_2025.csv"
            String n = f.getName().toLowerCase();

            // Verifica extensão e processa
            if ((n.endsWith(".csv") || n.endsWith(".txt"))) {
                processarCSV(f);
            } else if (n.endsWith(".xlsx")) {
                processarExcel(f);
            }
            // Se não for CSV/TXT/XLSX, silenciosamente ignora

        } catch (Exception e) {
            // Se der erro ao processar, registra no log mas continua
            LOGGER.log(Level.WARNING, "Erro ao processar: " + f.getName(), e);
        }
    }

    // ============================================================
    // PROCESSAMENTO DE ARQUIVOS CSV
    // ============================================================

    /**
     * PROCESSAR ARQUIVO CSV

     * FLUXO:
     * 1. Detectar delimitador (;, ,, |, tab)
     * 2. Ler header (primeira linha)
     * 3. Mapear colunas (descobrir índice de cada campo)
     * 4. Validar estrutura (verificar se tem colunas necessárias)
     * 5. Ler linha por linha e processar
     * 6. Acumular dados no consolidator

     * EXEMPLO DE CSV:

     * DATA;REG_ANS;CD_CONTA_CONTABIL;DESCRICAO;VL_SALDO_INICIAL;VL_SALDO_FINAL
     * 2025-01-01;316458;46411;PUBLICIDADE;0;1070
     * 2025-01-01;316458;464119;Propaganda;0;1070

     * PROCESSAMENTO:
     * - Linha 1 (header): Mapeia colunas
     * - Linha 2: Cria Despesa(data=2025-01-01, regANS=316458, valor=1070)
     * - Linha 3: Cria Despesa(data=2025-01-01, regANS=316458, valor=1070)
     *
     * @param f Arquivo CSV a ser processado
     * @throws IOException Se houver erro de leitura
     * @throws CsvValidationException Se CSV estiver malformado
     */
    private void processarCSV(File f) throws IOException, CsvValidationException {

        // PASSO 1: Detectar qual delimitador está sendo usado
        // Pode ser: ; (ponto-vírgula), , (vírgula), | (pipe), \t (tab)
        char sep = detectarDelimitador(f);

        // PASSO 2: Abrir o arquivo CSV com o delimitador correto
        // try-with-resources: garante que o arquivo será fechado
        try (CSVReader r = new CSVReaderBuilder(new FileReader(f))
                .withCSVParser(new CSVParserBuilder().withSeparator(sep).build())
                .build()) {

            // PASSO 3: Ler a primeira linha (HEADER)
            // Exemplo: [DATA, REG_ANS, CD_CONTA_CONTABIL, ...]
            String[] header = r.readNext();

            // PASSO 4: Log para debug
            LOGGER.info("=== PROCESSANDO: " + f.getName() + " ===");
            LOGGER.info("Header: " + Arrays.toString(header));

            // PASSO 5: Mapear colunas
            // Descobre qual é o índice de cada campo necessário
            // Exemplo: {"data": 0, "reg_ans": 1, "valor_inicial": 4, "valor_final": 5}
            Map<String, Integer> idx = mapearANS(header);

            // PASSO 6: Log do mapeamento
            LOGGER.info("Mapeamento: data=" + idx.get("data") +
                    ", reg_ans=" + idx.get("reg_ans") +
                    ", valor_inicial=" + idx.get("valor_inicial") +
                    ", valor_final=" + idx.get("valor_final"));

            // PASSO 7: Validar que o arquivo tem a estrutura esperada
            // Se faltar alguma coluna obrigatória, pula o arquivo
            if (!idx.containsKey("data") || !idx.containsKey("reg_ans") ||
                    !idx.containsKey("valor_inicial") || !idx.containsKey("valor_final")) {
                LOGGER.warning("Arquivo " + f.getName() + " não possui estrutura esperada. Pulando.");
                return; // Encerra processamento deste arquivo
            }

            // PASSO 8: Processar linha por linha
            String[] l; // Array que vai conter os valores de cada linha
            int linha = 1; // Contador de linhas (começa em 1 por causa do header)
            int processados = 0; // Contador de registros processados com sucesso
            int descartados = 0; // Contador de registros descartados

            // Loop: enquanto houver linhas no CSV
            while ((l = r.readNext()) != null) {
                linha++;

                // PASSO 8.1: Extrair valores das colunas
                // val() pega o valor da coluna mapeada e faz trim()
                String data = val(l, idx, "data");
                String regANS = val(l, idx, "reg_ans");
                String valorInicialStr = val(l, idx, "valor_inicial");
                String valorFinalStr = val(l, idx, "valor_final");

                // PASSO 8.2: Criar objeto Despesa a partir dos valores
                // Se houver erro ou valores inválidos, retorna null
                Despesa d = criarDespesaANS(data, regANS, valorInicialStr, valorFinalStr, f.getName(), linha);

                // PASSO 8.3: Se não conseguiu criar a Despesa, descarta
                if (d == null) {
                    descartados++;
                    continue; // Pula para próxima linha
                }

                // PASSO 8.4: Adicionar ao consolidador
                // Acumula valores agrupando por: CNPJ + Operadora + Ano + Trimestre
                consolidator.adicionar(
                        d.cnpj,        // REG_ANS (identificador da operadora)
                        d.operadora,   // Nome da operadora
                        d.ano,         // Ano extraído da data
                        d.trimestre,   // Trimestre calculado (1-4)
                        d.valor        // Valor da movimentação
                );
                processados++;
            }

            // PASSO 9: Log de estatísticas
            LOGGER.info("Arquivo " + f.getName() + ": " + processados + " processados, " + descartados + " descartados");
        }
    }

    // ============================================================
    // PROCESSAMENTO DE ARQUIVOS EXCEL
    // ============================================================

    /**
     * PROCESSAR ARQUIVO EXCEL (.xlsx)

     * Similar ao processamento de CSV, mas usa Apache POI
     * para ler arquivos Excel.

     * FLUXO:
     * 1. Abrir workbook (arquivo Excel)
     * 2. Pegar primeira planilha
     * 3. Ler linha 0 (header)
     * 4. Mapear colunas
     * 5. Validar estrutura
     * 6. Ler linhas 1 em diante
     * 7. Processar cada linha

     * @param f Arquivo Excel a ser processado
     * @throws IOException Se houver erro de leitura
     */
    private void processarExcel(File f) throws IOException {

        // try-with-resources: fecha o workbook automaticamente
        try (Workbook wb = new XSSFWorkbook(f)) {

            // PASSO 1: Pegar primeira planilha (Sheet 0)
            Sheet s = wb.getSheetAt(0);

            // PASSO 2: Pegar primeira linha (header)
            Row headerRow = s.getRow(0);

            LOGGER.info("=== PROCESSANDO: " + f.getName() + " ===");

            // PASSO 3: Mapear colunas do Excel
            Map<String, Integer> idx = mapearANSExcel(headerRow);

            // PASSO 4: Log do mapeamento
            LOGGER.info("Mapeamento: data=" + idx.get("data") +
                    ", reg_ans=" + idx.get("reg_ans") +
                    ", valor_inicial=" + idx.get("valor_inicial") +
                    ", valor_final=" + idx.get("valor_final"));

            // PASSO 5: Validar estrutura
            if (!idx.containsKey("data") || !idx.containsKey("reg_ans") ||
                    !idx.containsKey("valor_inicial") || !idx.containsKey("valor_final")) {
                LOGGER.warning("Arquivo " + f.getName() + " não possui estrutura esperada. Pulando.");
                return;
            }

            int processados = 0;
            int descartados = 0;

            // PASSO 6: Processar linhas (começando da linha 1, pois 0 é header)
            for (int i = 1; i <= s.getLastRowNum(); i++) {

                // Pegar linha atual
                Row r = s.getRow(i);
                if (r == null) continue; // Pula linhas vazias

                // Extrair valores das células
                String data = cell(r, idx, "data");
                String regANS = cell(r, idx, "reg_ans");
                String valorInicialStr = cell(r, idx, "valor_inicial");
                String valorFinalStr = cell(r, idx, "valor_final");

                // Criar Despesa
                Despesa d = criarDespesaANS(data, regANS, valorInicialStr, valorFinalStr, f.getName(), i + 1);

                if (d == null) {
                    descartados++;
                    continue;
                }

                // Adicionar ao consolidador
                consolidator.adicionar(
                        d.cnpj,
                        d.operadora,
                        d.ano,
                        d.trimestre,
                        d.valor
                );
                processados++;
            }

            LOGGER.info("Arquivo " + f.getName() + ": " + processados + " processados, " + descartados + " descartados");

        } catch (InvalidFormatException e) {
            throw new RuntimeException(e);
        }
    }

    // ============================================================
    // TRATAMENTO DE DATAS
    // ============================================================

    /**
     * EXTRAIR ANO E MÊS DE UMA STRING DE DATA

     * Suporta 3 formatos:
     * 1. yyyy-MM-dd   → Exemplo: "2025-07-15"
     * 2. dd/MM/yyyy   → Exemplo: "15/07/2025"
     * 3. yyyyMM       → Exemplo: "202507"

     * RETORNO: Array [ano, mês]

     * EXEMPLOS:
     * "2025-07-15" → [2025, 7]
     * "15/07/2025" → [2025, 7]
     * "202507"     → [2025, 7]
     *
     * @param data String contendo a data
     * @return Array de 2 posições: [ano, mês]
     * @throws IllegalArgumentException Se formato for inválido
     */
    private int[] extrairAnoMes(String data) {

        // Remove espaços em branco antes e depois
        data = data.trim();

        // FORMATO 1: yyyy-MM-dd (ISO 8601)
        // Regex: 4 dígitos + hífen + 2 dígitos + hífen + 2 dígitos
        if (data.matches("\\d{4}-\\d{2}-\\d{2}")) {
            return new int[] {
                    Integer.parseInt(data.substring(0,4)),  // Ano: posições 0-3
                    Integer.parseInt(data.substring(5,7))   // Mês: posições 5-6
            };
        }

        // FORMATO 2: dd/MM/yyyy (formato brasileiro)
        // Regex: 2 dígitos + barra + 2 dígitos + barra + 4 dígitos
        if (data.matches("\\d{2}/\\d{2}/\\d{4}")) {
            return new int[] {
                    Integer.parseInt(data.substring(6,10)), // Ano: posições 6-9
                    Integer.parseInt(data.substring(3,5))   // Mês: posições 3-4
            };
        }

        // FORMATO 3: yyyyMM (competência)
        // Regex: 6 dígitos
        if (data.matches("\\d{6}")) {
            return new int[] {
                    Integer.parseInt(data.substring(0,4)),  // Ano: posições 0-3
                    Integer.parseInt(data.substring(4,6))   // Mês: posições 4-5
            };
        }

        // Se não bateu com nenhum formato, lança exceção
        throw new IllegalArgumentException("Formato de data inválido: " + data);
    }

    // ============================================================
    // CRIAÇÃO DE OBJETO DESPESA
    // ============================================================

    /**
     * CRIAR OBJETO DESPESA A PARTIR DOS DADOS ANS

     * PROCESSO:
     * 1. Valida campos obrigatórios (data, REG_ANS)
     * 2. Extrai ano e mês da data
     * 3. Calcula o trimestre (1-4)
     * 4. Faz parse dos valores inicial e final
     * 5. Calcula a variação (|final - inicial|)
     * 6. Busca nome da operadora no dicionário
     * 7. Cria e retorna objeto Despesa

     * CÁLCULO DO TRIMESTRE:
     * - Meses 1,2,3   → Trimestre 1
     * - Meses 4,5,6   → Trimestre 2
     * - Meses 7,8,9   → Trimestre 3
     * - Meses 10,11,12 → Trimestre 4

     * Fórmula: (mês - 1) / 3 + 1
     * Exemplo: mês=7 → (7-1)/3+1 = 6/3+1 = 2+1 = 3 ✓

     * CÁLCULO DO VALOR:
     * Usa a VARIAÇÃO ABSOLUTA entre saldos:
     * valor = |saldo_final - saldo_inicial|

     * Exemplo:
     * - Inicial: 3.094.590,67
     * - Final:   4.212.815,67
     * - Valor:   |4.212.815,67 - 3.094.590,67| = 1.118.225,00

     * @param data String com a data (diversos formatos aceitos)
     * @param regANS Registro ANS da operadora
     * @param valorInicialStr Saldo inicial (string com vírgulas)
     * @param valorFinalStr Saldo final (string com vírgulas)
     * @param arquivo Nome do arquivo (para logging)
     * @param linha Número da linha (para logging)
     * @return Objeto Despesa ou null se inválido
     */
    private Despesa criarDespesaANS(String data, String regANS, String valorInicialStr, String valorFinalStr,
                                    String arquivo, int linha) {

        try {
            // VALIDAÇÃO 1: Data obrigatória
            // Se data estiver vazia, retorna null (descarta registro)
            if (data == null || data.trim().isEmpty()) {
                return null;
            }

            // VALIDAÇÃO 2: REG_ANS obrigatório
            // Se REG_ANS estiver vazio, retorna null (descarta registro)
            if (regANS == null || regANS.trim().isEmpty()) {
                return null;
            }

            // PASSO 1: Extrair ano e mês da data
            // Retorna array [ano, mês]
            int[] ym = extrairAnoMes(data);
            int ano = ym[0];
            int mes = ym[1];

            // PASSO 2: Calcular trimestre
            // Fórmula: (mês - 1) / 3 + 1
            // Janeiro(1)   → (1-1)/3+1 = 0/3+1 = 0+1 = 1
            // Abril(4)     → (4-1)/3+1 = 3/3+1 = 1+1 = 2
            // Julho(7)     → (7-1)/3+1 = 6/3+1 = 2+1 = 3
            // Outubro(10)  → (10-1)/3+1 = 9/3+1 = 3+1 = 4
            int trimestre = (mes - 1) / 3 + 1;

            // PASSO 3: Fazer parse dos valores
            // parseValorANS() remove pontos/vírgulas e converte para double
            double valorInicial = parseValorANS(valorInicialStr);
            double valorFinal = parseValorANS(valorFinalStr);

            // PASSO 4: Calcular variação absoluta
            // Math.abs() garante que o valor sempre será positivo
            // Exemplo: |100 - 200| = |-100| = 100
            double valor = Math.abs(valorFinal - valorInicial);

            // VALIDAÇÃO 3: Ignorar registros sem movimentação
            // Se valor = 0, retorna null (descarta registro)
            if (valor <= 0) {
                return null;
            }

            // PASSO 5: Buscar nome da operadora no dicionário
            // Se não encontrar, usa "REG_ANS_123456" como nome
            String nomeOperadora = OPERADORAS.getOrDefault(regANS.trim(), "REG_ANS_" + regANS.trim());

            // PASSO 6: Criar objeto Despesa
            Despesa d = new Despesa();
            d.cnpj = regANS.trim();        // Usando REG_ANS como identificador
            d.operadora = nomeOperadora;   // Nome da operadora
            d.ano = ano;                   // Ano extraído da data
            d.trimestre = trimestre;       // Trimestre calculado (1-4)
            d.valor = valor;               // Variação calculada

            return d;

        } catch (Exception e) {
            // Se der qualquer erro (parsing, etc.), retorna null
            // Descarta silenciosamente o registro
            return null;
        }
    }

    /**
     * FAZER PARSE DE VALOR MONETÁRIO ANS

     * Os valores vêm em formato brasileiro:
     * - Ponto (.) como separador de milhar
     * - Vírgula (,) como separador decimal

     * EXEMPLOS:
     * "1070"           → 1070.0
     * "1.070"          → 1070.0
     * "3094590,67"     → 3094590.67
     * "4.212.815,67"   → 4212815.67
     * "R$ 1.000,50"    → 1000.50
     * ""               → 0.0
     * null             → 0.0

     * PROCESSO:
     * 1. Remove pontos (separadores de milhar)
     * 2. Substitui vírgula por ponto (decimal)
     * 3. Remove caracteres não-numéricos (R$, espaços, etc.)
     * 4. Converte para double
     *
     * @param valorStr String contendo o valor
     * @return Valor convertido para double, ou 0.0 se inválido
     */
    private double parseValorANS(String valorStr) {
        // Se valor for null ou vazio, retorna 0
        if (valorStr == null || valorStr.trim().isEmpty()) {
            return 0.0;
        }

        try {
            // TRANSFORMAÇÕES:

            // 1. Remove pontos de milhar
            // "4.212.815,67" → "4212815,67"
            String v = valorStr.replace(".", "");

            // 2. Substitui vírgula decimal por ponto
            // "4212815,67" → "4212815.67"
            v = v.replace(",", ".");

            // 3. Remove tudo que NÃO for dígito, ponto ou hífen
            // "R$ 4212815.67" → "4212815.67"
            // "-1000.50" → "-1000.50" (mantém sinal negativo)
            v = v.replaceAll("[^0-9.-]", "");

            // 4. Remove espaços
            v = v.trim();

            // 5. Se ficou vazio após limpeza, retorna 0
            if (v.isEmpty()) return 0.0;

            // 6. Converte para double
            return Double.parseDouble(v);

        } catch (NumberFormatException e) {
            // Se der erro no parse, retorna 0
            return 0.0;
        }
    }

    // ============================================================
    // CLASSE INTERNA - MODELO DE DADOS
    // ============================================================

    /**
     * CLASSE DESPESA (DTO - Data Transfer Object)

     * Representa uma despesa processada, contendo:
     * - cnpj: Identificador da operadora (REG_ANS)
     * - operadora: Nome da operadora
     * - ano: Ano da competência
     * - trimestre: Trimestre (1 a 4)
     * - valor: Valor da movimentação

     * EXEMPLO:
     * {
     *   cnpj: "344800",
     *   operadora: "Operadora 344800",
     *   ano: 2025,
     *   trimestre: 3,
     *   valor: 1118225.00
     * }

     * Este objeto é passado para o consolidator que irá:
     * - Agrupar por: cnpj + operadora + ano + trimestre
     * - Somar os valores de cada grupo
     * - Gerar o CSV final
     */
    private static class Despesa {
        String cnpj;       // REG_ANS da operadora
        String operadora;  // Nome da operadora
        int ano;           // Ano (ex: 2025)
        int trimestre;     // 1, 2, 3 ou 4
        double valor;      // Valor em reais
    }

    // ============================================================
    // MAPEAMENTO DE COLUNAS - CSV
    // ============================================================

    /**
     * MAPEAR COLUNAS DO CSV

     * Recebe o header (primeira linha) do CSV e descobre
     * em qual posição está cada campo necessário.

     * ENTRADA (exemplo):
     * ["DATA", "REG_ANS", "CD_CONTA_CONTABIL", "DESCRICAO", "VL_SALDO_INICIAL", "VL_SALDO_FINAL"]

     * SAÍDA:
     * {
     *   "data": 0,
     *   "reg_ans": 1,
     *   "valor_inicial": 4,
     *   "valor_final": 5
     * }

     * LÓGICA DE MAPEAMENTO:

     * Para cada coluna do header:
     * 1. Converte para MAIÚSCULAS e remove espaços
     * 2. Verifica se bate com os padrões conhecidos
     * 3. Mapeia para a chave correspondente

     * PROTEÇÕES:
     * - if (!m.containsKey(...)) → Só mapeia a PRIMEIRA coluna que bater
     * - Evita sobrescrever mapeamentos

     * PADRÕES DE DETECÇÃO:
     * - DATA: "DATA" exato ou começa com "DT_"
     * - REG_ANS: "REG_ANS" ou "REGISTRO_ANS"
     * - INICIAL: Contém "SALDO" E "INICIAL" mas NÃO "FINAL"
     * - FINAL: Contém "SALDO" E "FINAL" mas NÃO "INICIAL"

     * @param h Array com nomes das colunas (header)
     * @return Map com índices das colunas necessárias
     */
    private Map<String, Integer> mapearANS(String[] h) {
        // Cria mapa vazio
        Map<String, Integer> m = new HashMap<>();

        // Loop por cada coluna do header
        for (int i = 0; i < h.length; i++) {
            // Pega nome da coluna, converte para maiúsculas e remove espaços
            String c = h[i].toUpperCase().trim();

            // MAPEAMENTO: DATA
            // Aceita: "DATA", "DT_COMPETENCIA", "DT_INICIO", etc.
            if (c.equals("DATA") || c.equals("DT_COMPETENCIA") || c.startsWith("DT_")) {
                // Só mapeia se ainda não mapeou (protege contra múltiplas colunas)
                if (!m.containsKey("data")) {
                    m.put("data", i);
                }
            }

            // MAPEAMENTO: REG_ANS
            // Aceita: "REG_ANS", "REGISTRO_ANS"
            if (c.equals("REG_ANS") || c.equals("REGISTRO_ANS")) {
                if (!m.containsKey("reg_ans")) {
                    m.put("reg_ans", i);
                }
            }

            // MAPEAMENTO: VL_SALDO_INICIAL
            // Aceita: "VL_SALDO_INICIAL" (exato) OU
            //         Contém "SALDO" E "INICIAL" mas NÃO contém "FINAL"
            // Proteção: !c.contains("FINAL") evita mapear "VL_SALDO_INICIAL_FINAL"
            if (c.equals("VL_SALDO_INICIAL") ||
                    (c.contains("SALDO") && c.contains("INICIAL") && !c.contains("FINAL"))) {
                if (!m.containsKey("valor_inicial")) {
                    m.put("valor_inicial", i);
                }
            }

            // MAPEAMENTO: VL_SALDO_FINAL
            // Aceita: "VL_SALDO_FINAL" (exato) OU
            //         Contém "SALDO" E "FINAL" mas NÃO contém "INICIAL"
            // Proteção: !c.contains("INICIAL") evita mapear "VL_SALDO_INICIAL_FINAL"
            if (c.equals("VL_SALDO_FINAL") ||
                    (c.contains("SALDO") && c.contains("FINAL") && !c.contains("INICIAL"))) {
                if (!m.containsKey("valor_final")) {
                    m.put("valor_final", i);
                }
            }
        }

        return m;
    }

    /**
     * MAPEAR COLUNAS DO EXCEL

     * Idêntico ao mapeamento CSV, mas trabalha com Row do Apache POI.
     *
     * @param h Row contendo o header do Excel
     * @return Map com índices das colunas necessárias
     */
    private Map<String, Integer> mapearANSExcel(Row h) {
        Map<String, Integer> m = new HashMap<>();

        // Loop por cada célula do header
        for (Cell c : h) {
            // Pega valor da célula, converte para maiúsculas e remove espaços
            String v = getCell(c).toUpperCase().trim();

            // Pega índice da coluna (0, 1, 2, ...)
            int i = c.getColumnIndex();

            // Mesma lógica de mapeamento do CSV
            if (v.equals("DATA") || v.equals("DT_COMPETENCIA") || v.startsWith("DT_")) {
                if (!m.containsKey("data")) {
                    m.put("data", i);
                }
            }

            if (v.equals("REG_ANS") || v.equals("REGISTRO_ANS")) {
                if (!m.containsKey("reg_ans")) {
                    m.put("reg_ans", i);
                }
            }

            if (v.equals("VL_SALDO_INICIAL") ||
                    (v.contains("SALDO") && v.contains("INICIAL") && !v.contains("FINAL"))) {
                if (!m.containsKey("valor_inicial")) {
                    m.put("valor_inicial", i);
                }
            }

            if (v.equals("VL_SALDO_FINAL") ||
                    (v.contains("SALDO") && v.contains("FINAL") && !v.contains("INICIAL"))) {
                if (!m.containsKey("valor_final")) {
                    m.put("valor_final", i);
                }
            }
        }

        return m;
    }

    // ============================================================
    // UTILITÁRIOS GERAIS
    // ============================================================

    /**
     * DETECTAR DELIMITADOR DO CSV

     * Lê a primeira linha do arquivo e conta quantas vezes
     * cada delimitador aparece. O que aparecer mais vezes
     * é considerado o delimitador correto.

     * DELIMITADORES TESTADOS:
     * - ; (ponto-vírgula) - padrão Brasil/Europa
     * - , (vírgula) - padrão EUA
     * - | (pipe) - menos comum
     * - \t (tab) - arquivos TSV

     * EXEMPLO:
     * Primeira linha: "DATA;REG_ANS;CD_CONTA"
     * Contagem: ; → 2 vezes, , → 0 vezes, | → 0 vezes, tab → 0 vezes
     * Resultado: ; (ponto-vírgula)

     * @param f Arquivo CSV
     * @return Caractere delimitador detectado
     * @throws IOException Se erro ao ler arquivo
     */
    private char detectarDelimitador(File f) throws IOException {
        try (BufferedReader br = new BufferedReader(new FileReader(f))) {
            // Lê primeira linha
            String l = br.readLine();

            // Se arquivo vazio, assume vírgula como padrão
            if (l == null) return ',';

            // Delimitadores a testar
            char[] c = { ';', ',', '|', '\t' };

            char best = ','; // Padrão se nenhum for encontrado
            int max = 0;     // Máximo de ocorrências encontradas

            // Para cada delimitador candidato
            for (char x : c) {
                // Conta quantas vezes aparece na linha
                int cnt = count(l, x);

                // Se aparece mais vezes que o atual máximo
                if (cnt > max) {
                    max = cnt;   // Atualiza máximo
                    best = x;    // Este é o melhor candidato
                }
            }

            return best;
        }
    }

    /**
     * CONTAR OCORRÊNCIAS DE UM CARACTERE

     * Conta quantas vezes um caractere aparece em uma string.

     * EXEMPLO:
     * count("a;b;c;d", ';') → 3
     * count("a,b,c,d", ';') → 0

     * @param s String a ser analisada
     * @param c Caractere a ser contado
     * @return Número de ocorrências
     */
    private int count(String s, char c) {
        int n = 0;
        // Para cada caractere da string
        for (char x : s.toCharArray()) {
            // Se é o caractere procurado, incrementa contador
            if (x == c) n++;
        }
        return n;
    }

    /**
     * OBTER VALOR DE COLUNA DO CSV

     * Pega o valor de uma coluna em uma linha do CSV,
     * usando o mapeamento previamente criado.

     * EXEMPLO:
     * Linha: ["2025-01-01", "316458", "46411", "PUBLICIDADE", "0", "1070"]
     * Mapeamento: {"data": 0, "reg_ans": 1, ...}
     * val(linha, mapeamento, "data") → "2025-01-01"
     * val(linha, mapeamento, "reg_ans") → "316458"

     * PROTEÇÕES:
     * - Se chave não existe no map, retorna ""
     * - Se índice maior que tamanho do array, retorna ""
     * - Se valor é null, retorna ""
     * - Sempre faz trim() para remover espaços
     * @param l Array com valores da linha
     * @param i Map com índices das colunas
     * @param k Chave da coluna desejada
     * @return Valor da coluna ou "" se não encontrado
     */
    private String val(String[] l, Map<String, Integer> i, String k) {
        // Verifica se a chave existe no mapeamento
        if (!i.containsKey(k)) return "";

        // Pega índice da coluna
        int idx = i.get(k);

        // Verifica se índice é válido
        if (idx >= l.length) return "";

        // Pega valor
        String value = l[idx];

        // Retorna valor com trim(), ou "" se null
        return value != null ? value.trim() : "";
    }

    /**
     * OBTER VALOR DE CÉLULA DO EXCEL
     * Similar ao val() mas para Excel.
     * @param r Row do Excel
     * @param i Map com índices das colunas
     * @param k Chave da coluna desejada
     * @return Valor da célula ou "" se não encontrado
     */
    private String cell(Row r, Map<String, Integer> i, String k) {
        // Verifica se chave existe
        if (!i.containsKey(k)) return "";

        // Pega célula na posição mapeada
        Cell c = r.getCell(i.get(k));

        // Converte célula para string e faz trim()
        return getCell(c).trim();
    }

    /**
     * CONVERTER CÉLULA EXCEL PARA STRING
     * Apache POI tem vários tipos de células (STRING, NUMERIC, BOOLEAN, etc.).
     * Este método converte qualquer tipo para String.
     * TIPOS DE CÉLULAS:
     * - STRING: Retorna o texto diretamente
     * - NUMERIC: Pode ser número ou data
     *   - Se for data: Converte para string
     *   - Se for número: Converte para string
     * - BLANK/FORMULA/etc: Retorna ""
     * EXEMPLOS:
     * Célula STRING "ABC" → "ABC"
     * Célula NUMERIC 123.45 → "123.45"
     * Célula DATE 2025-01-01 → "Wed Jan 01 00:00:00 BRT 2025"
     * Célula BLANK → ""
     * @param c Célula do Excel
     * @return Valor convertido para String
     */
    private String getCell(Cell c) {
        // Se célula é null, retorna ""
        if (c == null) return "";

        // Switch no tipo da célula
        switch (c.getCellType()) {
            case STRING:
                // Célula de texto: retorna diretamente
                return c.getStringCellValue();

            case NUMERIC:
                // Célula numérica: pode ser número ou data

                // Verifica se é formatada como data
                if (DateUtil.isCellDateFormatted(c)) {
                    // É data: converte para string
                    return c.getDateCellValue().toString();
                } else {
                    // É número: converte para string
                    return String.valueOf(c.getNumericCellValue());
                }

            default:
                // Outros tipos (BLANK, BOOLEAN, ERROR, FORMULA): retorna ""
                return "";
        }
    }
}