Aplicação Java para download, extração, processamento e consolidação dos demonstrativos contábeis trimestrais da ANS.

O sistema gera um CSV consolidado com os valores de despesas por operadora, ano e trimestre.

Como Executar:

Após colocar o pom.xml igual ao do projeto execute no bash:

mvn clean install

Passo 1 — Executar o crawler (download)
java ANSCrawler

Isso irá:

Acessar o site da ANS

Identificar os últimos trimestres disponíveis

Baixar os arquivos ZIP para a pasta:

./downloads

Passo 2 — Processar e consolidar os dados
java ANSFileProcessor


Esse processo irá:

Extrair todos os ZIPs da pasta downloads

Processar CSVs e Excel encontrados

Consolidar os dados

Gerar:

consolidado_despesas.zip
└── consolidado_despesas.csv

Funcionalidades

- Download automático dos ZIPs trimestrais da ANS

- Extração recursiva de arquivos ZIP

- Leitura de arquivos CSV, TXT e Excel (.xlsx)

- Detecção automática de delimitador CSV

- Consolidação por operadora, ano e trimestre

- Tratamento de dados inválidos

- Geração de CSV e compactação em ZIP

Estrutura

.
├── downloads/               # ZIPs baixados da ANS

├── extracted/               # Arquivos extraídos

├── ANSCrawler.java          # Crawler da ANS

├── ANSFileProcessor.java    # Processamento dos arquivos

├── ANSDespesaConsolidator.java

└── consolidado_despesas.zip

Dependências

- Java 11+

- OpenCSV

- Apache POI

- Jsoup

Execução:

Baixar arquivos da ANS
->java ANSCrawler

Processar e consolidar
->java ANSFileProcessor

Saída

Arquivo gerado:
consolidado_despesas.zip
└── consolidado_despesas.csv

Estrutura do CSV:
-> CNPJ,RazaoSocial,Trimestre,Ano,ValorDespesas

Regras de Tratamento

- Valores zerados ou inválidos → descartados

- Datas inválidas → descartadas

- Registros incompletos → descartados

✅ Status

✔ Funcional
✔ Documentado
✔ Pronto para avaliação técnica