import java.io.*;
import java.nio.file.*;
import java.util.*;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

/**
 * ============================================================
 * CONSOLIDADOR DE DESPESAS ANS
 * ============================================================

 * PROPÓSITO:
 * Este componente é responsável por CONSOLIDAR (agrupar e somar)
 * todas as despesas processadas dos arquivos ANS.

 * COMO FUNCIONA:

 * 1. RECEBE dados linha por linha de múltiplos arquivos:
 *    - Arquivo 1T2025.csv: 1000 linhas
 *    - Arquivo 2T2025.csv: 1500 linhas
 *    - Arquivo 3T2025.csv: 2000 linhas
 *    Total: 4500 linhas individuais

 * 2. AGRUPA por: CNPJ + Ano + Trimestre
 *    Exemplo:
 *    - Operadora 316458, 2025, 1T → pode ter 100 linhas diferentes
 *    - Todas são SOMADAS em um único registro

 * 3. GERA saída consolidada:
 *    - 4500 linhas de entrada → ~50 registros consolidados
 *    - Cada registro = soma de todas as movimentações do período

 * ESTRUTURA DE AGRUPAMENTO:

 * Chave única: CNPJ|ANO|TRIMESTRE

 * Exemplo:
 * - "316458|2025|1" → Operadora 316458, ano 2025, trimestre 1
 * - "421723|2025|2" → Operadora 421723, ano 2025, trimestre 2

 * VALORES SUSPEITOS:

 * O consolidador marca registros suspeitos:
 * - SUSPEITO_VALOR_NEGATIVO: Quando a soma dá negativo
 * - SUSPEITO_RAZAO_SOCIAL: Mesmo CNPJ com nomes diferentes

 * FORMATO DE SAÍDA (CSV):

 * CNPJ,RazaoSocial,Trimestre,Ano,ValorDespesas,Status
 * 316458,Operadora 316458,1,2025,1500000.00,OK
 * 421723,Operadora 421723,2,2025,2300000.00,OK
 * 344800,Operadora 344800,3,2025,-50000.00,SUSPEITO_VALOR_NEGATIVO
 */
public class ANSDespesaConsolidator {
// ============================================================
    // ESTRUTURA DE DADOS PRINCIPAL
    // ============================================================

    /**
     * MAPA DE CONSOLIDAÇÃO

     * Estrutura: Map<Chave, RegistroConsolidado>

     * CHAVE: String no formato "CNPJ|ANO|TRIMESTRE"
     *   Exemplo: "316458|2025|1"

     * VALOR: RegistroConsolidado contendo:
     *   - cnpj: Identificador da operadora
     *   - razaoSocial: Nome da operadora
     *   - ano: Ano (ex: 2025)
     *   - trimestre: 1, 2, 3 ou 4
     *   - valor: Soma acumulada de todas as despesas
     *   - status: OK, SUSPEITO_VALOR_NEGATIVO, SUSPEITO_RAZAO_SOCIAL

     * POR QUE USAR Map?
     * - Acesso O(1) para buscar/atualizar registros
     * - Garante que cada chave aparece apenas uma vez
     * - Facilita acumulação de valores

     * EXEMPLO DE USO:

     * ANTES (vazio):
     * mapa = {}

     * APÓS adicionar("316458", "Op A", 2025, 1, 1000):
     * mapa = {
     *   "316458|2025|1" → {cnpj=316458, razao=Op A, ano=2025, trim=1, valor=1000, status=OK}
     * }

     * APÓS adicionar("316458", "Op A", 2025, 1, 500):
     * mapa = {
     *   "316458|2025|1" → {cnpj=316458, razao=Op A, ano=2025, trim=1, valor=1500, status=OK}
     * }

     * Note que o segundo adicionar() SOMOU ao valor existente (1000 + 500 = 1500)
     */
    private final Map<String, RegistroConsolidado> mapa = new HashMap<>();

    // ============================================================
    // MÉTODO PRINCIPAL: ADICIONAR DESPESA
    // ============================================================

    /**
     * ADICIONAR DESPESA AO CONSOLIDADO

     * Este é o método MAIS IMPORTANTE da classe.
     * É chamado para CADA linha processada dos arquivos ANS.

     * FLUXO DE PROCESSAMENTO:

     * 1. VALIDAÇÕES (rejeita dados inválidos)
     *    - CNPJ vazio → ignora
     *    - Trimestre fora de 1-4 → ignora
     *    - Valor = 0 → ignora (sem movimentação)

     * 2. GERAÇÃO DA CHAVE ÚNICA
     *    - Combina: CNPJ + Ano + Trimestre
     *    - Formato: "CNPJ|ANO|TRIMESTRE"
     *    - Exemplo: "316458|2025|1"

     * 3. BUSCA NO MAPA
     *    - Já existe registro com essa chave?
     *      → SIM: ACUMULA valor no registro existente
     *      → NÃO: CRIA novo registro

     * 4. DETECÇÃO DE ANOMALIAS
     *    - Valor negativo → marca como SUSPEITO_VALOR_NEGATIVO
     *    - Mesmo CNPJ com razão social diferente → marca como SUSPEITO_RAZAO_SOCIAL

     * EXEMPLO PASSO A PASSO:

     * CENÁRIO: Processando arquivo 1T2025.csv com 3 linhas:

     * Linha 1: adicionar("316458", "Unimed", 2025, 1, 1000)
     *   → mapa vazio
     *   → cria novo registro
     *   → mapa = {"316458|2025|1": {valor=1000, status=OK}}

     * Linha 2: adicionar("316458", "Unimed", 2025, 1, 500)
     *   → encontra registro existente
     *   → SOMA: 1000 + 500 = 1500
     *   → mapa = {"316458|2025|1": {valor=1500, status=OK}}

     * Linha 3: adicionar("421723", "Bradesco", 2025, 1, 2000)
     *   → não encontra registro (chave diferente)
     *   → cria novo registro
     *   → mapa = {
     *       "316458|2025|1": {valor=1500, status=OK},
     *       "421723|2025|1": {valor=2000, status=OK}
     *     }

     * REGRAS DE NEGÓCIO:

     * 1. Valor = 0 → IGNORADO
     *    Razão: Sem movimentação financeira

     * 2. Valor < 0 → ACEITO mas marcado como SUSPEITO
     *    Razão: Pode ser estorno, devolução, ajuste contábil

     * 3. Trimestre inválido (< 1 ou > 4) → IGNORADO
     *    Razão: Dado inconsistente

     * 4. CNPJ vazio → IGNORADO
     *    Razão: Não consegue identificar operadora

     * 5. Razão social diferente para mesmo CNPJ → MARCADO
     *    Razão: Pode indicar erro no mapeamento ou mudança de nome

     * @param cnpj Identificador da operadora (REG_ANS)
     * @param razaoSocial Nome da operadora
     * @param ano Ano da competência (ex: 2025)
     * @param trimestre Trimestre (1, 2, 3 ou 4)
     * @param valor Valor da despesa (pode ser negativo)
     */
    public void adicionar(
            String cnpj,
            String razaoSocial,
            int ano,
            int trimestre,
            double valor) {

        // ========================================
        // FASE 1: VALIDAÇÕES
        // ========================================

        /*
         * VALIDAÇÃO 1: CNPJ obrigatório
         *
         * Se CNPJ for null ou vazio, não conseguimos
         * identificar a operadora. Registro é descartado.
         */
        if (cnpj == null || cnpj.isEmpty()) return;

        /*
         * VALIDAÇÃO 2: Trimestre válido
         *
         * Trimestre deve ser 1, 2, 3 ou 4.
         * Qualquer outro valor indica erro nos dados.
         *
         * Exemplos INVÁLIDOS:
         * - trimestre = 0  → Não existe trimestre 0
         * - trimestre = 5  → Não existe trimestre 5
         * - trimestre = -1 → Valor negativo impossível
         */
        if (trimestre < 1 || trimestre > 4) return;

        /*
         * VALIDAÇÃO 3: Ignorar valores zero

         * Valor = 0 significa que não houve movimentação.
         * Não faz sentido consolidar, então ignoramos.

         * Exemplo:
         * - Saldo inicial: 1000
         * - Saldo final: 1000
         * - Variação: 1000 - 1000 = 0
         * - Ação: Ignora
         */
        if (valor == 0) return;

        // ========================================
        // FASE 2: GERAÇÃO DA CHAVE ÚNICA
        // ========================================

        /*
         * CONSTRUÇÃO DA CHAVE

         * Chave = CNPJ|ANO|TRIMESTRE

         * Esta chave identifica UNICAMENTE um grupo de despesas.

         * EXEMPLOS:
         * - "316458|2025|1" → Operadora 316458, 1º trimestre de 2025
         * - "316458|2025|2" → Operadora 316458, 2º trimestre de 2025 (DIFERENTE!)
         * - "421723|2025|1" → Operadora 421723, 1º trimestre de 2025 (DIFERENTE!)

         * POR QUE não incluir RAZÃO SOCIAL na chave?
         * - Razão social pode ter pequenas variações
         * - CNPJ é o identificador ÚNICO e ESTÁVEL

         * POR QUE incluir ANO e TRIMESTRE?
         * - Precisamos consolidar POR PERÍODO
         * - Mesma operadora tem valores diferentes em cada trimestre
         */
        String chave = cnpj + "|" + ano + "|" + trimestre;

        // ========================================
        // FASE 3: BUSCAR OU CRIAR REGISTRO
        // ========================================

        /*
         * BUSCA NO MAPA

         * Tenta buscar um registro existente com essa chave.

         * Retorno:
         * - Se ENCONTROU: retorna o RegistroConsolidado existente
         * - Se NÃO ENCONTROU: retorna null
         */
        RegistroConsolidado r = mapa.get(chave);

        // ========================================
        // CASO 1: REGISTRO NÃO EXISTE (PRIMEIRO REGISTRO DO GRUPO)
        // ========================================

        if (r == null) {
            /**
             * CRIAR NOVO REGISTRO
             *
             * Este é o PRIMEIRO registro desta operadora neste período.
             *
             * Exemplo:
             * - Primeira linha do arquivo 1T2025.csv para operadora 316458
             * - Ou primeira linha do trimestre 2 para operadora 421723
             */

            // Cria objeto vazio
            r = new RegistroConsolidado();

            // Preenche campos
            r.cnpj = cnpj;
            r.razaoSocial = razaoSocial;
            r.ano = ano;
            r.trimestre = trimestre;
            r.valor = valor; // Valor inicial = valor desta linha

            /*
             * DEFINE STATUS INICIAL

             * Se valor < 0: Marca como suspeito
             * Se valor > 0: Marca como OK

             * Exemplos:
             * - valor = 1000   → status = "OK"
             * - valor = -500   → status = "SUSPEITO_VALOR_NEGATIVO"
             */
            r.status = valor < 0 ? "SUSPEITO_VALOR_NEGATIVO" : "OK";

            /*
             * ADICIONA AO MAPA

             * Insere o novo registro no mapa usando a chave gerada.

             * ANTES: mapa = {}
             * DEPOIS: mapa = {"316458|2025|1": r}
             */
            mapa.put(chave, r);

            // ========================================
            // CASO 2: REGISTRO JÁ EXISTE (ACUMULAR)
            // ========================================

        } else {
            /*
             * ACUMULAR NO REGISTRO EXISTENTE

             * Este NÃO é o primeiro registro deste grupo.
             * Já temos dados acumulados para esta operadora/período.

             * Ação: SOMAR ao valor existente

             * Exemplo:
             * - Valor atual no registro: 1000
             * - Novo valor desta linha: 500
             * - Resultado: 1000 + 500 = 1500
             */
            r.valor += valor;

            /*
             * VALIDAÇÃO DE CONSISTÊNCIA: RAZÃO SOCIAL

             * Verifica se a razão social desta linha é igual
             * à razão social já armazenada no registro.

             * PROBLEMA DETECTADO: Mesmo CNPJ com nomes diferentes

             * Exemplo SUSPEITO:
             * - Linha 1: CNPJ=316458, Razão="Unimed Central"
             * - Linha 2: CNPJ=316458, Razão="Unimed Nacional"

             * Possíveis causas:
             * 1. Mudança de razão social da empresa
             * 2. Erro no mapeamento REG_ANS → Nome
             * 3. Dados inconsistentes nos arquivos ANS

             * Ação: Marca como SUSPEITO para revisão manual

             * NOTA: equalsIgnoreCase() ignora maiúsculas/minúsculas
             * - "Unimed" = "UNIMED" → considerado igual
             * - "Unimed" ≠ "Bradesco" → considerado diferente
             */
            if (!r.razaoSocial.equalsIgnoreCase(razaoSocial)) {
                r.status = "SUSPEITO_RAZAO_SOCIAL";
            }
        }
    }

    // ============================================================
    // GERAÇÃO DO ARQUIVO CSV
    // ============================================================

    /*
     * GERAR ARQUIVO CSV CONSOLIDADO

     * Percorre todo o mapa de registros consolidados e
     * gera um arquivo CSV com os dados finais.

     * ESTRUTURA DO CSV:

     * Linha 1 (HEADER):
     * CNPJ,RazaoSocial,Trimestre,Ano,ValorDespesas,Status

     * Linhas seguintes (DADOS):
     * 316458,Operadora 316458,1,2025,1500000.00,OK
     * 421723,Operadora 421723,2,2025,2300000.00,OK
     * 344800,Operadora 344800,3,2025,-50000.00,SUSPEITO_VALOR_NEGATIVO

     * PROCESSO:

     * 1. Criar arquivo "consolidado_despesas.csv"
     * 2. Escrever linha de cabeçalho
     * 3. Para cada registro no mapa:
     *    - Formatar como linha CSV
     *    - Escrever no arquivo
     * 4. Fechar arquivo
     * 5. Retornar Path do arquivo criado

     * FORMATAÇÃO:

     * - CNPJ: Sem formatação (REG_ANS como string)
     * - RazaoSocial: Vírgulas são substituídas por espaços
     *   (para não quebrar formato CSV)
     * - Trimestre: Número inteiro (1-4)
     * - Ano: Número inteiro (ex: 2025)
     * - ValorDespesas: Decimal com 2 casas (ex: 1500000.00)
     * - Status: String (OK, SUSPEITO_VALOR_NEGATIVO, etc.)

     * EXEMPLO DE TRANSFORMAÇÃO:

     * REGISTRO NO MAPA:
     * {
     *   cnpj: "316458",
     *   razaoSocial: "Unimed Central, S.A.",
     *   ano: 2025,
     *   trimestre: 1,
     *   valor: 1500000.567,
     *   status: "OK"
     * }

     * LINHA NO CSV:
     * 316458,Unimed Central S.A.,1,2025,1500000.57,OK
     *                 ↑ vírgula removida    ↑ arredondado 2 casas

     * @return Path do arquivo CSV criado
     * @throws IOException Se houver erro ao criar/escrever arquivo
     */
    public Path gerarCSV() throws IOException {

        // PASSO 1: Definir caminho do arquivo
        // Será criado no diretório atual do programa
        Path csv = Paths.get("consolidado_despesas.csv");

        // PASSO 2: Abrir arquivo para escrita
        // try-with-resources: garante que arquivo será fechado
        // BufferedWriter: escreve de forma eficiente (usa buffer)
        try (BufferedWriter w = Files.newBufferedWriter(csv)) {

            // PASSO 3: Escrever cabeçalho (primeira linha)
            /*
             * CABEÇALHO DO CSV

             * Define os nomes das colunas.
             * Importante para que programas como Excel saibam
             * o que significa cada coluna.
             */
            w.write("CNPJ,RazaoSocial,Trimestre,Ano,ValorDespesas,Status");
            w.newLine(); // Pula para próxima linha

            // PASSO 4: Escrever dados
            /*
             * ITERAÇÃO SOBRE REGISTROS

             * mapa.values() retorna todos os RegistroConsolidado
             * armazenados no mapa (ignora as chaves).

             * Para cada registro, cria uma linha no CSV.

             * ORDEM: Não garantida! HashMap não mantém ordem.
             * Se precisar ordenar, use TreeMap ou ordene antes.
             */
            for (RegistroConsolidado r : mapa.values()) {
                /*
                 * FORMATAÇÃO DA LINHA

                 * String.format() cria string formatada.

                 * Placeholders:
                 * - %s = String
                 * - %d = Número inteiro
                 * - %.2f = Decimal com 2 casas

                 * Estrutura:
                 * CNPJ,RazaoSocial,Trimestre,Ano,ValorDespesas,Status
                 * %s  ,%s         ,%d       ,%d ,%.2f        ,%s

                 * TRATAMENTO ESPECIAL: RazaoSocial

                 * r.razaoSocial.replace(",", " ")

                 * POR QUE? CSV usa vírgula como separador.
                 * Se razão social tiver vírgula, quebra o formato.

                 * Exemplo PROBLEMÁTICO:
                 * - Razão: "Unimed Central, S.A."
                 * - Sem tratamento: 316458,Unimed Central, S.A.,1,2025,1000,OK
                 *   → Lido como 7 colunas ao invés de 6!
                 * - Com tratamento: 316458,Unimed Central S.A.,1,2025,1000,OK
                 *   → Correto: 6 colunas
                 */
                w.write(String.format(
                        "%s,%s,%d,%d,%.2f,%s",
                        r.cnpj,                               // CNPJ (string)
                        r.razaoSocial.replace(",", " "),     // Razão (sem vírgulas)
                        r.trimestre,                          // Trimestre (1-4)
                        r.ano,                                // Ano (ex: 2025)
                        r.valor,                              // Valor (2 decimais)
                        r.status                              // Status (string)
                ));
                w.newLine(); // Pula para próxima linha
            }
        }
        // Arquivo é fechado automaticamente aqui (try-with-resources)

        // PASSO 5: Retornar Path do arquivo criado
        return csv;
    }

    // ============================================================
    // COMPACTAÇÃO EM ZIP
    // ============================================================

    /*
     * COMPACTAR CSV EM ZIP

     * Pega o arquivo CSV gerado e cria um ZIP contendo-o.

     * ARQUIVO FINAL: consolidado_despesas.zip
     * CONTEÚDO: consolidado_despesas.csv

     * POR QUE COMPACTAR?
     * - Reduz tamanho (CSV compacta muito bem)
     * - Facilita download/envio
     * - Padrão para distribuição de dados

     * PROCESSO:

     * 1. Criar arquivo ZIP
     * 2. Criar entrada (entry) para o CSV
     * 3. Copiar conteúdo do CSV para dentro do ZIP
     * 4. Fechar entrada
     * 5. Fechar ZIP

     * EXEMPLO:

     * ANTES:
     * consolidado_despesas.csv (500 KB)

     * DEPOIS:
     * consolidado_despesas.csv (500 KB)
     * consolidado_despesas.zip (50 KB) ← Redução de ~90%

     * @param csv Path do arquivo CSV a ser compactado
     * @throws IOException Se houver erro na compactação
     */
    public void gerarZip(Path csv) throws IOException {

        // PASSO 1: Criar arquivo ZIP
        /*
         * try-with-resources com ZipOutputStream

         * ZipOutputStream: Classe que escreve arquivos ZIP
         * FileOutputStream: Escreve bytes no arquivo físico

         * Arquivo criado: "consolidado_despesas.zip"
         */
        try (ZipOutputStream zos = new ZipOutputStream(
                new FileOutputStream("consolidado_despesas.zip"))) {

            // PASSO 2: Criar entrada no ZIP
            /*
             * ZIP ENTRY

             * Um arquivo ZIP pode conter múltiplos arquivos.
             * Cada arquivo dentro do ZIP é chamado de "entry".

             * ZipEntry define:
             * - Nome do arquivo dentro do ZIP
             * - Metadados (tamanho, data, etc.)

             * csv.getFileName().toString() pega apenas o nome
             * do arquivo, sem o caminho completo.

             * Exemplo:
             * - csv = /home/user/projeto/consolidado_despesas.csv
             * - getFileName() = consolidado_despesas.csv
             */
            ZipEntry entry = new ZipEntry(csv.getFileName().toString());

            // PASSO 3: Adicionar entrada ao ZIP
            /*
             * putNextEntry() prepara o ZIP para receber
             * o conteúdo do próximo arquivo.

             * É como dizer: "Agora vou escrever o arquivo X"
             */
            zos.putNextEntry(entry);

            // PASSO 4: Copiar conteúdo do CSV para o ZIP
            /*
             * Files.copy() copia todo o conteúdo do arquivo CSV
             * para dentro do ZipOutputStream.

             * Lê o CSV do disco e escreve compactado no ZIP.
             */
            Files.copy(csv, zos);

            // PASSO 5: Fechar entrada
            /*
             * closeEntry() indica que terminamos de escrever
             * este arquivo dentro do ZIP.

             * Se fôssemos adicionar mais arquivos, chamaríamos
             * putNextEntry() novamente.
             */
            zos.closeEntry();
        }
        // ZIP é fechado automaticamente aqui
    }

    // ============================================================
    // MÉTODO UTILITÁRIO
    // ============================================================

    /*
     * CONTAR TOTAL DE REGISTROS CONSOLIDADOS

     * Retorna quantos registros únicos foram consolidados.

     * IMPORTANTE: Este não é o número de linhas processadas,
     * mas sim o número de GRUPOS após consolidação.

     * EXEMPLO:

     * PROCESSAMENTO:
     * - Arquivo 1: 1000 linhas processadas
     * - Arquivo 2: 1500 linhas processadas
     * - Arquivo 3: 2000 linhas processadas
     * Total processado: 4500 linhas

     * CONSOLIDAÇÃO:
     * - Operadora A, 2025, T1: 10 linhas → 1 registro consolidado
     * - Operadora A, 2025, T2: 15 linhas → 1 registro consolidado
     * - Operadora B, 2025, T1: 20 linhas → 1 registro consolidado
     * - Operadora B, 2025, T2: 18 linhas → 1 registro consolidado
     * - ...
     * Total consolidado: 50 registros

     * totalRegistros() retorna: 50 (não 4500)

     * IMPLEMENTAÇÃO:

     * mapa.size() retorna o número de entradas no HashMap.
     * Cada entrada = um grupo único (CNPJ|ANO|TRIMESTRE).

     * @return Número de registros consolidados
     */
    public int totalRegistros() {
        return mapa.size();
    }

    // ============================================================
    // CLASSE INTERNA: MODELO DE DADOS
    // ============================================================

    /*
     * REGISTRO CONSOLIDADO (DTO)

     * Representa um grupo de despesas consolidadas.

     * EXEMPLO DE INSTÂNCIA:

     * {
     *   cnpj: "316458",
     *   razaoSocial: "Unimed Central Nacional",
     *   ano: 2025,
     *   trimestre: 1,
     *   valor: 1500000.00,  ← SOMA de todas as linhas do grupo
     *   status: "OK"
     * }

     * CICLO DE VIDA:

     * 1. CRIAÇÃO:
     *    - Primeira linha de um novo grupo
     *    - Campos preenchidos com valores iniciais
     *    - Valor = valor da primeira linha

     * 2. ATUALIZAÇÃO:
     *    - Novas linhas do mesmo grupo
     *    - valor += novo valor (acumulação)
     *    - status pode mudar para SUSPEITO

     * 3. FINALIZAÇÃO:
     *    - Nenhuma nova linha chega
     *    - Registro é escrito no CSV

     * CAMPOS:

     * - cnpj: Identificador único da operadora (REG_ANS)
     *   Tipo: String (embora seja número, tratamos como texto)

     * - razaoSocial: Nome da operadora
     *   Tipo: String
     *   Exemplo: "Unimed Central Nacional"

     * - ano: Ano da competência
     *   Tipo: int
     *   Exemplo: 2025

     * - trimestre: Trimestre (1, 2, 3 ou 4)
     *   Tipo: int
     *   Valores válidos: 1, 2, 3, 4

     * - valor: Soma acumulada de todas as despesas do grupo
     *   Tipo: double
     *   Exemplo: 1500000.00
     *   IMPORTANTE: Pode ser negativo (estornos)

     * - status: Indicador de qualidade dos dados
     *   Tipo: String
     *   Valores possíveis:
     *   - "OK": Dados normais
     *   - "SUSPEITO_VALOR_NEGATIVO": Soma deu negativo
     *   - "SUSPEITO_RAZAO_SOCIAL": CNPJ com nomes diferentes

     * NOTA SOBRE VISIBILIDADE:

     * A classe é static porque não precisa acessar
     * membros da classe externa (ANSDespesaConsolidator).

     * É apenas um container de dados (DTO).
     */
    static class RegistroConsolidado {
        String cnpj;           // REG_ANS da operadora
        String razaoSocial;    // Nome da operadora
        int ano;               // Ano (ex: 2025)
        int trimestre;         // 1, 2, 3 ou 4
        double valor;          // Soma acumulada
        String status;         // OK, SUSPEITO_VALOR_NEGATIVO, etc.
    }
}
