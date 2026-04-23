# **Teste Técnico**

## Pipeline de Transformação Agêntica de Dados

### Contexto

O objetivo é avaliar a capacidade de construir um agente que cria, gerencia e mantém um pipeline de transformação de dados, não apenas executa análises pontuais.

### Desafio

Construir um agente que cria e gerencia um pipeline de transformação de dados em 3 camadas, utilizando dados transacionais de conversas no canal de WhatsApp entre lead e vendedor humano.

O agente deve:

* Criar o pipeline de transformação em código Python  
* Gerenciar o pipeline de forma autônoma (se quebrar, identifica, alerta e corrige ou ao menos sugere correção)  
* Manter o pipeline "vivo", atualizando automaticamente a camada Gold conforme novos dados chegam na fonte

**Importante:** O agente NÃO deve fazer a análise pontualmente a cada execução. Ele deve construir o pipeline como infraestrutura persistente, similar a um job no Databricks.

## **Pipeline em Camada Medalhão**

### Camada Bronze

* Todas as conversas transacionais de um chatbot no formato original  
* Dados não estruturados: mensagens de texto dos leads, metadados de conversas  
* Inclui coluna de mensagem com dados não estruturados

### Camada Silver

* Dados limpos e organizados por usuário/lead  
* Extração de informações relevantes das mensagens (e-mails, dados de contato, etc.)  
* Transformação e limpeza dos dados brutos  
* Remoção de ruído e normalização

### Camada Gold

* Tabela analítica com classificações e agrupamentos  
* Criação de audiências e personas a partir dos dados transformados  
* Esta camada atualiza automaticamente quando novos dados chegam na Bronze  
* Exemplos de insights esperados:  
  * Provedores de e-mail mais usados pelos leads  
  * Classificação de personas/perfis de usuários  
  * Segmentação por audiência  
  * Análise de sentimento do cliente

## **Requisitos**

### Obrigatórios

1. Python puro: o pipeline deve ser implementado em código Python  
2. AI Agent: o agente cria E gerencia o pipeline (auto-correção em caso de falha)  
3. Pipeline vivo: atualização automática da Gold quando a fonte de dados cresce  
4. 3 camadas: Bronze → Silver → Gold com transformações claras em cada etapa  
5. Código disponível em repositório público no GitHub

### Diferenciais (espaço para criatividade)

1. Destaque para pipelines que utilizam o Databricks (free tier atende)  
2. Tipos de classificação e insights gerados na camada Gold  
3. Abordagem de limpeza e transformação dos dados  
4. Criatividade nos agrupamentos e segmentações  
5. Robustez do mecanismo de auto-correção do agente  
6. É recomendado fugir dos exemplos de insights

### Base de Dados

* Fonte: dados transacionais de conversas ([conversations\_bronze.parquet](https://drive.google.com/file/d/1rt-bLmt3ccR0_BL424WcAGpUMh14PKiO/view?usp=sharing))  
* Formato: mensagens de texto de leads com metadados  
* Volume estimado: \~15 mil conversas transcritas  
* Dados sensíveis (nomes, e-mails) devem ser mascarados com as mesmas dimensões  
* [Dicionário de Dados - Teste Técnico de Data & AI Engineering](https://docs.google.com/document/d/1QO2NohJ3BJOMmQiQBtK1lGyPJxfZ9Dpii6Y4oO0maLM/)

## **Avaliação**

### Engenharia de dados e criatividade analítica

Conseguiu projetar um pipeline robusto com camadas claras de transformação? Entendeu a diferença entre análise pontual e infraestrutura de dados? Que insights conseguiu extrair? Como classificou e segmentou os dados? 

### Capacidade de construir agentes

O agente é autônomo? Se auto-corrige? Gerencia o pipeline como um processo contínuo?

### Qualidade de código

O código é limpo, bem estruturado, documentado? Segue boas práticas?