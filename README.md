 `SABD-ProjectTwo`
-
Lo scopo del progetto è rispondere ad alcune query riguardanti un dataset relativo a dati provenienti da dispositivi
Automatic Identification System(AIS), utilizzando il framework Apache Flink.

*`Queries`*
-
1. **Query uno**:
   
   Calcolare per ogni cella del Mar Mediterraneo Occidentale2, il numero medio di navi militari (SHIPTYPE= 35), navi per
   trasporto passeggeri (SHIPTYPE = 60-69), navi cargo (SHIPTYPE = 70-79) eothers(tutte le navi che non hanno uno
   SHIPTYPE che rientri nei casi precedenti) negli ultimi 7 giorni (dievent time) e 1 mese (di event time).


2. **Query due**:
   
   Per il Mar Mediterraneo Occidentale ed Orientale3fornire la classifica delle tre celle pi`u frequentatenelle due
   fasce orarie di servizio 00:00-11:59 e 12:00-23:59. In una determinata fascia oraria, il grado di frequentazione di
   una cella viene calcolato come il numero di navi diverse che attraversano la cella nella fascia oraria in esame

*`Prerequisiti`*
-
Il progetto è stato eseguito in un ambiente Windows.
Bisogna installare docker:
- [[https://docs.docker.com/docker-for-windows/install/][Docker per Windows]]
- E' necessario aver installato:
    - maven
    - java
    
*`Struttura del progetto`* 
-

All'interno delle cartelle:

`Docker`
 
- docker-compose.yml: per la creazione dei container:
     
  - zookeeper
  - kafka
    
- cartella scripts:
  
  - start-docker
  - stop-docker

`data`
  contiene il dataset csv

`results`: contiene i risultati ottenuto dal sink builder

`output` : contiene i risultati ottenuti con kafka consumer

`src`

- **main package**
    
    - FlinkMain: esegue data stream processing 
    - Producer: legge il file e pubblica i record in un topic di Kafka
    - Consumer: salva i record di un topic in file csv
   
 
- **flink package**
  
    - classi sulle query uno e due che determinano la tipologia usata per ottenere il
    risultato delle due queries
  

-  **kafka package**

    - classi per l'interazione con kafka per poter produrre i dati e salvarli su un topic, 
    pubblicare i risultati e consumarli
      
- **benchmarks**
    
     - classe per effetturare una valutazione sulla latenza e throughput

    
### *`Istruzioni`*

 - Per la creazione del file java eseguire:
   
       mvn clean compile assembly:single
   
 - Per eseguire l'applicazione è possibile farlo su IntelliJ o attraverso il comando:
               
       java -cp ./target/SABD-ProjectTwo-1.0-SNAPSHOT-jar-with-dependencies.jar main.*nomeclasse*


      
### **Framework**

- Apache Flink: localhost:8081
- Apache Kafka web UI: localhost:9000

