import java.io.*;
import java.nio.file.*;
import java.util.stream.Stream;

import com.opencsv.*;
import org.apache.poi.ss.usermodel.*;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;

/**
 * ============================================================
 * FERRAMENTA DE DIAGNÓSTICO DE ARQUIVOS
 * ============================================================

 * PROPÓSITO:
 * Esta classe é uma ferramenta de DEBUG e ANÁLISE.
 * Ela NÃO processa dados - apenas EXAMINA arquivos
 * para ajudar a entender sua estrutura.

 * QUANDO USAR:
 * - Quando você não sabe qual é a estrutura dos arquivos
 * - Quando o processamento está falhando
 * - Quando quer verificar se os arquivos foram extraídos corretamente
 * - Quando precisa descobrir nomes das colunas

 * O QUE ELA FAZ:

 * Para cada arquivo CSV/TXT/XLSX em ./extracted:
 * 1. Mostra o tipo (CSV ou Excel)
 * 2. Mostra o delimitador (se CSV)
 * 3. Lista TODAS as colunas (header)
 * 4. Mostra as 3 primeiras linhas de dados

 * EXEMPLO DE SAÍDA:

 * =================================================================
 * ARQUIVO: ./extracted/1T2025.csv
 * =================================================================
 * Tipo: CSV/TXT
 * Delimitador: ';'

 * HEADER (6 colunas):
 *   [0] = 'DATA'
 *   [1] = 'REG_ANS'
 *   [2] = 'CD_CONTA_CONTABIL'
 *   [3] = 'DESCRICAO'
 *   [4] = 'VL_SALDO_INICIAL'
 *   [5] = 'VL_SALDO_FINAL'

 * PRIMEIRAS 3 LINHAS DE DADOS:

 * Linha 1:
 *   DATA = '2025-01-01'
 *   REG_ANS = '316458'
 *   CD_CONTA_CONTABIL = '46411'
 *   DESCRICAO = 'PUBLICIDADE E PROPAGANDA'
 *   VL_SALDO_INICIAL = '0'
 *   VL_SALDO_FINAL = '1070'

 * ...

 * BENEFÍCIOS:
 * - Não precisa abrir Excel/LibreOffice
 * - Vê estrutura de TODOS os arquivos de uma vez
 * - Identifica problemas de formato rapidamente
 * - Ajuda a ajustar o código de processamento
 */
public class DiagnosticosArquivos {

    // ============================================================
    // CONFIGURAÇÃO
    // ============================================================

    /**
     * DIRETÓRIO A SER ANALISADO
     *
     * Por padrão: ./extracted
     *
     * Este é o mesmo diretório onde o ANSFileProcessor
     * extrai os arquivos ZIP.
     *
     * IMPORTANTE: Execute o diagnóstico DEPOIS de extrair os ZIPs!
     *
     * FLUXO CORRETO:
     * 1. ANSFileProcessor extrai ZIPs → ./extracted
     * 2. DiagnosticosArquivos analisa → ./extracted
     *
     * Se quiser analisar outra pasta, mude aqui:
     * private static final Path EXTRACTED_DIR = Paths.get("./sua_pasta");
     */
    private static final Path EXTRACTED_DIR = Paths.get("./extracted");

    // ============================================================
    // MÉTODO PRINCIPAL (ENTRY POINT)
    // ============================================================

    /**
     * PONTO DE ENTRADA DO PROGRAMA
     *
     * Quando você executa: java DiagnosticosArquivos_Comentado
     * Este método é chamado.
     *
     * FLUXO:
     * 1. Imprime cabeçalho
     * 2. Varre recursivamente ./extracted
     * 3. Filtra apenas CSV/TXT/XLSX
     * 4. Diagnostica cada arquivo encontrado
     *
     * @param args Argumentos de linha de comando (não usado)
     * @throws IOException Se houver erro ao ler diretórios/arquivos
     */
    public static void main(String[] args) throws IOException {
        // Imprime cabeçalho
        System.out.println("=== DIAGNÓSTICO DE ARQUIVOS ===\n");

        /**
         * VARREDURA DE DIRETÓRIOS
         *
         * Files.walk() percorre RECURSIVAMENTE uma pasta.
         *
         * Exemplo de estrutura:
         * ./extracted/
         * ├── 1T2025.csv          ← Vai diagnosticar
         * ├── 2T2025.csv          ← Vai diagnosticar
         * ├── subpasta/
         * │   └── 3T2025.csv      ← Vai diagnosticar (recursivo!)
         * └── readme.txt          ← Vai ignorar (não é CSV/XLSX)
         *
         * try-with-resources: Garante que o Stream será fechado
         */
        try (Stream<Path> s = Files.walk(EXTRACTED_DIR)) {
            s
                    /**
                     * FILTRO 1: Apenas arquivos regulares
                     *
                     * Ignora:
                     * - Diretórios (pastas)
                     * - Links simbólicos
                     * - Arquivos especiais
                     */
                    .filter(Files::isRegularFile)

                    /**
                     * FILTRO 2: Apenas extensões conhecidas
                     *
                     * Lambda que verifica se o arquivo termina com:
                     * - .csv
                     * - .txt
                     * - .xlsx
                     *
                     * NOTA: Conversão para lowercase evita problemas com
                     * maiúsculas/minúsculas (DADOS.CSV = dados.csv)
                     */
                    .filter(p -> {
                        String n = p.toString().toLowerCase();
                        return n.endsWith(".csv") || n.endsWith(".txt") || n.endsWith(".xlsx");
                    })

                    /**
                     * AÇÃO: Diagnosticar cada arquivo
                     *
                     * Para cada Path que passou pelos filtros,
                     * chama o método diagnosticar()
                     *
                     * Method reference: DiagnosticosArquivos_Comentado::diagnosticar
                     * É equivalente a: p -> diagnosticar(p)
                     */
                    .forEach(DiagnosticosArquivos::diagnosticar);
        }
    }

    // ============================================================
    // DIAGNÓSTICO DE ARQUIVO
    // ============================================================

    /**
     * DIAGNOSTICAR UM ARQUIVO
     *
     * Determina o tipo do arquivo e chama o método apropriado:
     * - .xlsx → diagnosticarExcel()
     * - .csv/.txt → diagnosticarCSV()
     *
     * TRATAMENTO DE ERROS:
     * Se houver erro ao processar, imprime a mensagem
     * mas CONTINUA diagnosticando outros arquivos.
     *
     * @param p Path do arquivo a ser diagnosticado
     */
    private static void diagnosticar(Path p) {
        try {
            // Pega apenas o nome do arquivo (sem caminho)
            // Exemplo: ./extracted/subpasta/dados.csv → dados.csv
            String nome = p.getFileName().toString().toLowerCase();

            /**
             * CABEÇALHO DO DIAGNÓSTICO
             *
             * "=".repeat(80) cria linha de 80 sinais de igual
             * Serve para separar visualmente cada arquivo
             */
            System.out.println("\n" + "=".repeat(80));
            System.out.println("ARQUIVO: " + p);
            System.out.println("=".repeat(80));

            /**
             * DECISÃO: Excel ou CSV?
             *
             * Verifica extensão e chama método apropriado
             */
            if (nome.endsWith(".xlsx")) {
                diagnosticarExcel(p.toFile());
            } else {
                // .csv ou .txt são tratados da mesma forma
                diagnosticarCSV(p.toFile());
            }

        } catch (Exception e) {
            /**
             * TRATAMENTO DE ERRO
             *
             * Se der erro ao diagnosticar este arquivo:
             * 1. Imprime mensagem de erro
             * 2. Imprime stack trace (para debug)
             * 3. CONTINUA para próximo arquivo
             *
             * Isso evita que um arquivo problemático
             * interrompa o diagnóstico de todos os outros.
             */
            System.err.println("ERRO ao diagnosticar " + p + ": " + e.getMessage());
            e.printStackTrace();
        }
    }

    // ============================================================
    // DIAGNÓSTICO DE ARQUIVOS CSV/TXT
    // ============================================================

    /**
     * DIAGNOSTICAR ARQUIVO CSV
     *
     * PROCESSO:
     * 1. Detectar delimitador (;, ,, |, tab)
     * 2. Ler header (primeira linha)
     * 3. Mostrar todas as colunas com seus índices
     * 4. Ler e mostrar as 3 primeiras linhas de dados
     *
     * INFORMAÇÕES EXIBIDAS:
     * - Tipo: CSV/TXT
     * - Delimitador detectado
     * - Número de colunas
     * - Nome de cada coluna com seu índice
     * - Valores das 3 primeiras linhas
     *
     * @param f Arquivo CSV a ser diagnosticado
     * @throws Exception Se houver erro de leitura
     */
    private static void diagnosticarCSV(File f) throws Exception {

        /**
         * PASSO 1: DETECTAR DELIMITADOR
         *
         * CSV pode usar diferentes separadores:
         * - ; (ponto-vírgula) - Padrão Brasil/Europa
         * - , (vírgula) - Padrão EUA
         * - | (pipe) - Menos comum
         * - \t (tab) - Arquivos TSV
         *
         * detectarDelimitador() analisa a primeira linha
         * e retorna o separador mais comum.
         */
        char sep = detectarDelimitador(f);

        // Mostra informações básicas
        System.out.println("Tipo: CSV/TXT");
        System.out.println("Delimitador: '" + sep + "'");

        /**
         * PASSO 2: ABRIR ARQUIVO COM DELIMITADOR CORRETO
         *
         * Usa OpenCSV com o delimitador detectado.
         *
         * CSVReaderBuilder: Cria leitor de CSV configurável
         * CSVParserBuilder: Configura como parsear o CSV
         * withSeparator(sep): Define qual delimitador usar
         */
        try (CSVReader r = new CSVReaderBuilder(new FileReader(f))
                .withCSVParser(new CSVParserBuilder().withSeparator(sep).build())
                .build()) {

            /**
             * PASSO 3: LER HEADER (PRIMEIRA LINHA)
             *
             * readNext() retorna array de strings.
             * Cada elemento = uma coluna.
             *
             * Exemplo:
             * CSV: DATA;REG_ANS;VALOR
             * header = ["DATA", "REG_ANS", "VALOR"]
             */
            String[] header = r.readNext();

            /**
             * EXIBIR COLUNAS
             *
             * Lista todas as colunas com seus índices.
             * Isso é ESSENCIAL para configurar o mapeamento
             * no código de processamento.
             */
            System.out.println("\nHEADER (" + header.length + " colunas):");
            for (int i = 0; i < header.length; i++) {
                System.out.println("  [" + i + "] = '" + header[i] + "'");
            }

            /**
             * PASSO 4: MOSTRAR PRIMEIRAS 3 LINHAS
             *
             * Lê as 3 primeiras linhas de dados (após o header)
             * e mostra os valores de cada coluna.
             *
             * Isso ajuda a:
             * - Verificar formato dos dados
             * - Identificar problemas (valores vazios, etc.)
             * - Entender estrutura real dos dados
             */
            System.out.println("\nPRIMEIRAS 3 LINHAS DE DADOS:");
            for (int i = 0; i < 3; i++) {
                // Lê próxima linha
                String[] linha = r.readNext();

                // Se chegou no fim do arquivo, para
                if (linha == null) break;

                // Mostra número da linha
                System.out.println("\nLinha " + (i + 1) + ":");

                /**
                 * MOSTRA CADA VALOR
                 *
                 * Formato: NOME_COLUNA = 'VALOR'
                 *
                 * j < linha.length: Protege contra linhas incompletas
                 * j < header.length: Protege contra colunas extras
                 *
                 * Exemplo de saída:
                 * DATA = '2025-01-01'
                 * REG_ANS = '316458'
                 * VALOR = '1000.50'
                 */
                for (int j = 0; j < linha.length && j < header.length; j++) {
                    System.out.println("  " + header[j] + " = '" + linha[j] + "'");
                }
            }
        }
    }

    // ============================================================
    // DIAGNÓSTICO DE ARQUIVOS EXCEL
    // ============================================================

    /**
     * DIAGNOSTICAR ARQUIVO EXCEL
     *
     * Similar ao CSV, mas usa Apache POI para ler Excel.
     *
     * PROCESSO:
     * 1. Abrir workbook (arquivo Excel)
     * 2. Pegar primeira planilha
     * 3. Ler linha 0 (header)
     * 4. Mostrar todas as colunas
     * 5. Mostrar 3 primeiras linhas de dados
     *
     * LIMITAÇÕES:
     * - Só lê a PRIMEIRA planilha
     * - Se arquivo tiver múltiplas abas, ignora as outras
     *
     * @param f Arquivo Excel a ser diagnosticado
     * @throws Exception Se houver erro de leitura
     */
    private static void diagnosticarExcel(File f) throws Exception {
        System.out.println("Tipo: Excel");

        /**
         * PASSO 1: ABRIR WORKBOOK
         *
         * XSSFWorkbook: Classe do Apache POI para .xlsx
         *
         * try-with-resources: Fecha o workbook automaticamente
         *
         * NOTA: .xls (formato antigo) requer HSSFWorkbook
         */
        try (Workbook wb = new XSSFWorkbook(f)) {

            /**
             * PASSO 2: PEGAR PRIMEIRA PLANILHA
             *
             * getSheetAt(0): Retorna primeira aba/planilha
             *
             * Se quiser analisar outras abas:
             * - wb.getNumberOfSheets() → quantas abas
             * - wb.getSheetAt(1) → segunda aba
             * - wb.getSheetName(0) → nome da primeira aba
             */
            Sheet s = wb.getSheetAt(0);

            /**
             * PASSO 3: PEGAR HEADER (LINHA 0)
             *
             * getRow(0): Retorna primeira linha da planilha
             *
             * Row é um objeto que contém células (Cell)
             */
            Row headerRow = s.getRow(0);

            /**
             * EXIBIR COLUNAS
             *
             * headerRow.getLastCellNum() retorna número de colunas
             *
             * ATENÇÃO: getLastCellNum() retorna índice + 1
             * Se última coluna é F (índice 5), retorna 6
             */
            System.out.println("\nHEADER (" + headerRow.getLastCellNum() + " colunas):");
            for (Cell c : headerRow) {
                /**
                 * Para cada célula do header:
                 * - getColumnIndex(): Retorna índice (0, 1, 2, ...)
                 * - getCellValue(): Converte célula para String
                 *
                 * Exemplo de saída:
                 * [0] = 'DATA'
                 * [1] = 'REG_ANS'
                 * [2] = 'VALOR'
                 */
                System.out.println("  [" + c.getColumnIndex() + "] = '" + getCellValue(c) + "'");
            }

            /**
             * PASSO 4: MOSTRAR PRIMEIRAS 3 LINHAS
             *
             * Loop de linha 1 até linha 3 (pula linha 0 = header)
             *
             * s.getLastRowNum(): Retorna índice da última linha
             * Se planilha tiver 100 linhas, retorna 99
             */
            System.out.println("\nPRIMEIRAS 3 LINHAS DE DADOS:");
            for (int i = 1; i <= 3 && i <= s.getLastRowNum(); i++) {
                // Pega linha atual
                Row r = s.getRow(i);

                // Se linha é null (vazia), pula
                if (r == null) continue;

                System.out.println("\nLinha " + i + ":");

                /**
                 * MOSTRAR VALORES
                 *
                 * Para cada coluna definida no header:
                 * 1. Pega índice da coluna
                 * 2. Pega célula nesse índice da linha de dados
                 * 3. Mostra: NOME_COLUNA = VALOR
                 *
                 * Exemplo:
                 * DATA = '2025-01-01'
                 * REG_ANS = '316458'
                 */
                for (Cell c : headerRow) {
                    int idx = c.getColumnIndex();      // Índice da coluna
                    Cell dataCell = r.getCell(idx);    // Célula de dados
                    System.out.println("  " + getCellValue(c) + " = '" + getCellValue(dataCell) + "'");
                }
            }
        }
    }

    // ============================================================
    // UTILITÁRIOS
    // ============================================================

    /**
     * DETECTAR DELIMITADOR DO CSV
     *
     * Lê a primeira linha do arquivo e conta quantas vezes
     * cada delimitador candidato aparece.
     *
     * O delimitador que aparecer MAIS VEZES é escolhido.
     *
     * DELIMITADORES TESTADOS:
     * - ; (ponto-vírgula)
     * - , (vírgula)
     * - | (pipe)
     * - \t (tab)
     *
     * EXEMPLO:
     *
     * Primeira linha: "DATA;REG_ANS;VALOR;DESCRICAO"
     *
     * Contagem:
     * - ; → 3 ocorrências ✓ MÁXIMO
     * - , → 0 ocorrências
     * - | → 0 ocorrências
     * - \t → 0 ocorrências
     *
     * Resultado: ; (ponto-vírgula)
     *
     * EDGE CASES:
     * - Arquivo vazio → retorna , (vírgula) como padrão
     * - Empate → retorna o primeiro testado (;)
     *
     * @param f Arquivo a ser analisado
     * @return Caractere delimitador detectado
     * @throws IOException Se erro ao ler arquivo
     */
    private static char detectarDelimitador(File f) throws IOException {
        try (BufferedReader br = new BufferedReader(new FileReader(f))) {
            // Lê primeira linha
            String l = br.readLine();

            // Se arquivo vazio, assume vírgula
            if (l == null) return ',';

            // Candidatos a delimitador
            char[] c = { ';', ',', '|', '\t' };

            char best = ',';  // Melhor candidato (padrão)
            int max = 0;      // Máximo de ocorrências

            // Testa cada candidato
            for (char x : c) {
                // Conta ocorrências
                int cnt = count(l, x);

                // Se aparece mais vezes que o atual máximo
                if (cnt > max) {
                    max = cnt;
                    best = x;
                }
            }

            return best;
        }
    }

    /**
     * CONTAR OCORRÊNCIAS DE CARACTERE
     *
     * Conta quantas vezes um caractere aparece em uma string.
     *
     * IMPLEMENTAÇÃO SIMPLES:
     * Loop por cada caractere da string.
     * Se é o caractere procurado, incrementa contador.
     *
     * EXEMPLO:
     * count("a;b;c;d", ';') → 3
     * count("a,b,c,d", ';') → 0
     * count("hello", 'l') → 2
     *
     * @param s String a ser analisada
     * @param c Caractere a ser contado
     * @return Número de ocorrências
     */
    private static int count(String s, char c) {
        int n = 0;
        for (char x : s.toCharArray()) {
            if (x == c) n++;
        }
        return n;
    }

    /**
     * CONVERTER CÉLULA EXCEL PARA STRING
     *
     * Apache POI tem vários tipos de células:
     * - STRING: Texto
     * - NUMERIC: Número ou data
     * - BOOLEAN: true/false
     * - BLANK: Vazia
     * - FORMULA: Fórmula (=A1+B1)
     * - ERROR: Erro (#DIV/0!, #REF!, etc.)
     *
     * Este método converte QUALQUER tipo para String.
     *
     * CONVERSÕES:
     *
     * STRING → retorna diretamente
     * Exemplo: "Unimed" → "Unimed"
     *
     * NUMERIC (número) → converte para string
     * Exemplo: 123.45 → "123.45"
     *
     * NUMERIC (data) → converte para string
     * Exemplo: 01/01/2025 → "Wed Jan 01 00:00:00 BRT 2025"
     *
     * BLANK → retorna ""
     *
     * Outros → retorna ""
     *
     * DETECÇÃO DE DATA:
     *
     * DateUtil.isCellDateFormatted() verifica se a célula
     * está formatada como data no Excel.
     *
     * Isso é necessário porque Excel armazena datas como
     * números (número de dias desde 1900).
     *
     * @param c Célula do Excel
     * @return Valor convertido para String
     */
    private static String getCellValue(Cell c) {
        // Célula null → vazia
        if (c == null) return "";

        // Switch no tipo da célula
        switch (c.getCellType()) {
            case STRING:
                // Texto: retorna diretamente
                return c.getStringCellValue();

            case NUMERIC:
                // Número: pode ser data ou número normal

                // Verifica se é formatada como data
                if (DateUtil.isCellDateFormatted(c)) {
                    // É data: converte para Date e depois String
                    return c.getDateCellValue().toString();
                } else {
                    // É número: converte para String
                    return String.valueOf(c.getNumericCellValue());
                }

            case BLANK:
                // Vazia: retorna ""
                return "";

            default:
                // Outros tipos (BOOLEAN, FORMULA, ERROR):
                // Usa toString() genérico
                return c.toString();
        }
    }
}
