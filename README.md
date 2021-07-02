## `SABD-ProjectTwo`

Lo scopo del progetto è rispondere ad alcune query riguardanti un dataset relativo a dati provenienti da dispositivi
Automatic Identification System(AIS), utilizzando il framework Apache Flink o, in alternativa, Apache Storm.

**Queries**

1. Query uno:
   Calcolare per ogni cella del Mar Mediterraneo Occidentale2, il numero medio di navi militari (SHIPTYPE= 35), navi per
   trasporto passeggeri (SHIPTYPE = 60-69), navi cargo (SHIPTYPE = 70-79) eothers(tutte le navi che non hanno uno
   SHIPTYPE che rientri nei casi precedenti) negli ultimi 7 giorni (dievent time) e 1 mese (di event time).

2. Query due:
   Per il Mar Mediterraneo Occidentale ed Orientale3fornire la classifica delle tre celle pi`u frequentatenelle due
   fasce orarie di servizio 00:00-11:59 e 12:00-23:59. In una determinata fascia oraria, il gradodi frequentazione di
   una cella viene calcolato come il numero di navi diverse che attraversano la cellanella fascia oraria in esame.

**Framework**

- Apache Flink: localhost:8081
- Apache Nifi: localhost:8080/nifi