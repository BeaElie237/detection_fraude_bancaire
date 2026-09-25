# InstantGuard — Real-Time Payment Fraud Detection & Financial Analytics Platform

> Plateforme temps réel de détection de fraude sur les **virements instantanés SEPA**, avec un volet **Data Engineering** (streaming, data platform) et un volet **Data Analyst Finance / Power BI** (pilotage du risque et impact financier).

![status](https://img.shields.io/badge/status-en%20construction-orange)
![python](https://img.shields.io/badge/python-3.12-blue)
![spark](https://img.shields.io/badge/PySpark-3.5-E25A1C)
![kafka](https://img.shields.io/badge/Kafka-streaming-black)
![powerbi](https://img.shields.io/badge/Power%20BI-dashboard-F2C811)

---

## Sommaire

1. [Problématique](#1-problématique)
2. [Contexte français](#2-contexte-français)
3. [Architecture](#3-architecture)
4. [Deux parcours de lecture](#4-deux-parcours-de-lecture)
5. [Stack technique](#5-stack-technique)
6. [Structure du projet](#6-structure-du-projet)
7. [Installation](#7-installation)
8. [Modèle de données](#8-modèle-de-données)
9. [Détection de fraude](#9-détection-de-fraude)
10. [Dashboard Power BI](#10-dashboard-power-bi)
11. [Impact financier](#11-impact-financier)
12. [Organisation du travail en équipe](#12-organisation-du-travail-en-équipe)
13. [Roadmap](#13-roadmap)
14. [Avertissements](#14-avertissements)
15. [Auteurs](#15-auteurs)

---

## 1. Problématique

> Comment construire une plateforme capable d'ingérer et d'analyser en temps réel des virements instantanés afin d'identifier les transactions présentant un risque de fraude, tout en permettant à une banque de mesurer l'impact financier des alertes et de piloter la performance du dispositif via Power BI ?

L'objectif n'est pas seulement de faire du Machine Learning, mais de construire une **mini-architecture bancaire** qui répond à deux questions :

| Question               | Réponse apportée par        |
| ---------------------- | --------------------------- |
| *Doit-on alerter ?*    | Moteur de règles + modèles ML → score de risque |
| *Combien ça coûte ?*   | KPIs financiers (coût des faux positifs / faux négatifs, fraude évitée) |

---

## 2. Contexte français

Le virement instantané prend une place croissante en France, et la fraude par manipulation (le client est amené à valider lui-même le virement) en représente une part importante. Selon l'Observatoire de la sécurité des moyens de paiement (Banque de France, rapport 2025) :

- les virements instantanés représentent **17 %** du nombre de virements émis en 2025 ;
- la fraude aux moyens de paiement s'élève à **1,241 Md€** en 2025, dont **516 M€** de fraude par manipulation.

Le projet s'appuie conceptuellement sur les dispositifs suivants :

| Dispositif | Ce qu'on simule dans le projet |
| --- | --- |
| **Instant Payments Regulation (UE)** | Flux de virements `SEPA_INSTANT` traités en quasi temps réel |
| **Vérification du bénéficiaire (VoP)** — en place en France depuis octobre 2025 | Comparaison nom fourni ↔ IBAN : `MATCH` / `CLOSE_MATCH` / `NO_MATCH` |
| **FNC-RF** — Fichier national des comptes signalés pour risque de fraude (mai 2026) | Table `synthetic_fraud_iban_registry` d'IBAN (hashés) signalés |
| **DSP2 / SCA** | Contexte : l'authentification forte limite certaines fraudes, les fraudeurs se tournent vers la manipulation |

Sources :
- [Rapport OSMP 2025 — Banque de France](https://www.banque-france.fr/fr/publications-et-statistiques/publications/rapport-de-lobservatoire-de-la-securite-des-moyens-de-paiement-2025)
- [Instant Payments Regulation — BCE](https://www.ecb.europa.eu/paym/retail/instant_payments/html/instant_payments_regulation.fr.html)
- [Lancement de la plateforme des IBAN suspects — Banque de France](https://www.banque-france.fr/fr/communiques-de-presse/lancement-de-la-plateforme-des-iban-suspects-un-nouvel-outil-cle-de-lutte-contre-la-fraude-aux)
- [Rapport conjoint ABE / BCE sur la fraude aux moyens de paiement](https://www.banque-france.fr/fr/communiques-de-presse/rapport-conjoint-de-labe-et-de-la-bce-relatif-la-fraude-sur-les-moyens-de-paiement-lauthentification)

> Ce projet est un **prototype inspiré** du cadre réglementaire européen des services de paiement et des dispositifs français de prévention de la fraude. Il **ne prétend pas** être conforme à une réglementation (DSP2, DSP3, IPR…).

---

## 3. Architecture

```text
                       ┌──────────────────┐
                       │ Transaction      │
                       │ Generator Python │
                       └────────┬─────────┘
                                │
                                ▼
                       ┌──────────────────┐
                       │      KAFKA       │
                       │  payments.raw    │
                       └────────┬─────────┘
                                │
                                ▼
                 ┌──────────────────────────┐
                 │ PySpark Structured       │
                 │ Streaming                │
                 │  • Cleaning              │
                 │  • Enrichment            │
                 │  • Feature Engineering   │
                 └────────────┬─────────────┘
                              │
                 ┌────────────┴────────────┐
                 ▼                         ▼
        ┌─────────────────┐       ┌──────────────────┐
        │ Fraud Engine    │       │ PostgreSQL       │
        │ Rules + ML      │──────▶│ Bronze/Silver/   │
        └────────┬────────┘       │ Gold (dbt)       │
                 │                └────────┬─────────┘
                 ▼                         ▼
          ┌─────────────┐           ┌─────────────┐
          │ fraud.alerts│           │  Power BI   │
          └─────────────┘           └──────┬──────┘
                                           │
                    ┌──────────────────────┼─────────────────┐
                    ▼                      ▼                 ▼
               Fraud KPIs            Risk Analysis    Financial Impact
```

### Couches de données (Medallion)

| Couche     | Contenu | Exemples de tables |
| ---------- | ------- | ------------------ |
| **Bronze** | Données brutes issues de Kafka, sans transformation | `bronze.payments_raw` |
| **Silver** | Données nettoyées, dédoublonnées, typées, enrichies | `silver.transactions`, `silver.customers` |
| **Gold**   | Données analytiques prêtes pour Power BI | `gold.transactions`, `gold.fraud_alerts`, `gold.customer_risk`, `gold.daily_fraud_kpis`, `gold.model_performance` |

### Topics Kafka

`payments.raw` · `customers` · `beneficiaries` · `fraud.alerts` · `fraud.decisions` — détail dans [kafka/topics.md](kafka/topics.md).

---

## 4. Deux parcours de lecture

Le repository est pensé comme si **deux équipes** travaillaient ensemble.

### 🛠️ Parcours Data Engineer

> Construire une plateforme robuste pour collecter, transformer, stocker et servir les transactions en quasi temps réel.

Dossiers concernés : `producer/`, `kafka/`, `streaming/`, `warehouse/`, `dbt/`, `airflow/`, `tests/`

- Simulateur de transactions (100 → 1 000 tx/min, pics artificiels, fraudes labellisées)
- Ingestion Kafka
- PySpark Structured Streaming : nettoyage, enrichissement, feature engineering
- Architecture Bronze / Silver / Gold dans PostgreSQL
- Transformations dbt, orchestration Airflow, conteneurisation Docker
- Qualité des données et tests

### 📊 Parcours Data Analyst Finance & Power BI

> Transformer la donnée en analyse du risque, KPIs financiers et aide à la décision.

Dossiers concernés : `analytics/`, `fraud_detection/`, `powerbi/`

- Analyse exploratoire des comportements transactionnels
- Évaluation des modèles (Precision, Recall, F1, FPR, FNR, ROC-AUC, PR-AUC)
- Modèle en étoile et mesures DAX
- Dashboard Power BI en 4 pages
- Analyse coût / bénéfice du dispositif de détection

---

## 5. Stack technique

| Domaine | Outils |
| --- | --- |
| Langage | Python 3.12, SQL |
| Données synthétiques | Faker, NumPy, Pandas |
| Streaming | Apache Kafka, PySpark Structured Streaming |
| Stockage | PostgreSQL |
| Transformation | dbt |
| Orchestration | Apache Airflow |
| Machine Learning | scikit-learn (Isolation Forest, Random Forest), XGBoost |
| Visualisation | Power BI (DAX, Power Query), Jupyter |
| Infra | Docker, Docker Compose |
| Qualité | pytest, ruff *(optionnel : Great Expectations)* |
| Monitoring *(optionnel)* | Prometheus, Grafana |
| Versioning | Git, GitHub |

---

## 6. Structure du projet

```text
detection_Fraude/
│
├── data/                     # Données locales (non versionnées)
│   ├── raw/                  #   données brutes
│   ├── processed/            #   données transformées
│   └── synthetic/            #   jeux de données générés
│
├── producer/                 # Générateur de transactions → Kafka
├── kafka/                    # Configuration et documentation des topics
│   └── topics.md
├── streaming/                # Jobs PySpark Structured Streaming
│                             #   (cleaning, enrichment, feature engineering)
├── fraud_detection/          # Moteur de règles, modèles ML, scoring
│   └── models/               #   modèles entraînés (non versionnés)
│
├── warehouse/
│   └── sql/                  # DDL PostgreSQL (schémas bronze / silver / gold)
├── dbt/
│   └── models/
│       ├── staging/          #   Bronze → Silver
│       ├── intermediate/     #   logique métier intermédiaire
│       └── marts/            #   Silver → Gold (tables Power BI)
├── airflow/
│   └── dags/                 # Orchestration des batchs (réentraînement, dbt, KPIs)
│
├── analytics/
│   └── notebooks/            # EDA, analyse fraude, impact financier
├── powerbi/
│   └── screenshots/          # Captures du dashboard (+ fichier .pbix)
│
├── config/                   # Fichiers de configuration (seuils, paramètres)
├── docs/                     # Documentation (schémas, décisions techniques)
├── tests/                    # Tests unitaires et d'intégration
│
├── .env.example              # Variables d'environnement (modèle)
├── .gitignore
├── requirements.txt
└── README.md
```

---

## 7. Installation

### Prérequis

- Python **3.12**
- Git
- Java **17** (requis par PySpark)
- Docker & Docker Compose *(pour Kafka, PostgreSQL, Airflow — à venir)*
- Power BI Desktop *(Windows)*

### Mise en place

```bash
# 1. Cloner le repository
git clone <URL_DU_REPO>
cd detection_Fraude

# 2. Créer et activer l'environnement virtuel
python3 -m venv .venv
source .venv/bin/activate          # Linux / macOS
# .venv\Scripts\activate           # Windows

# 3. Installer les dépendances
pip install --upgrade pip
pip install -r requirements.txt

# 4. Configurer les variables d'environnement
cp .env.example .env
```

### Lancer la plateforme *(à venir)*

```bash
docker compose up -d                          # Kafka + PostgreSQL
python -m producer.transaction_generator      # génération du flux
python -m streaming.spark_streaming           # traitement temps réel
```

---

## 8. Modèle de données

### Format d'une transaction (topic `payments.raw`)

```json
{
  "transaction_id": "TX982731",
  "timestamp": "2026-09-25T21:42:31",
  "customer_id": "C10592",
  "account_id": "ACC8492",
  "amount": 2450.50,
  "currency": "EUR",
  "transaction_type": "SEPA_INSTANT",
  "beneficiary_id": "B49201",
  "beneficiary_name": "ABC SERVICES",
  "beneficiary_iban_hash": "hash...",
  "country": "FR",
  "channel": "MOBILE",
  "device_id": "DEV193",
  "ip_country": "FR"
}
```

### Volumétrie cible du dataset synthétique

| Élément | Volume |
| --- | --- |
| Transactions | 10 millions |
| Clients | 500 000 |
| Bénéficiaires | 1 million |
| Historique | 24 mois |
| Répartition | 99,5 % normal / 0,5 % fraude (paramétrable) |

### Schéma en étoile (Power BI)

```text
                    Dim_Date
                       │
Dim_Client ───── Fact_Transactions ───── Dim_Beneficiaire
                       │
                   Dim_Canal
                       │
                   Dim_Fraude
                       │
                  Fact_Alertes
```

| Table | Colonnes principales |
| --- | --- |
| `Fact_Transactions` | transaction_id, date_id, customer_id, beneficiary_id, amount, risk_score, fraud_prediction, fraud_label, processing_time |
| `Fact_Alertes` | alert_id, transaction_id, alert_type, risk_score, decision, created_at, resolved_at |
| `Dim_Client` | customer_id, age, segment, region, customer_since |
| `synthetic_fraud_iban_registry` | iban_hash, risk_status, reported_at, reason, source_type |

---

## 9. Détection de fraude

### Features temps réel

| Feature | Description |
| --- | --- |
| Montant inhabituel | `amount / moyenne historique du client` |
| Nouveau bénéficiaire | `first_time_beneficiary = 1` |
| Vélocité | Nb de transactions sur 5 min, 1 h, 24 h, 7 j |
| Concentration | Nb de virements vers un même bénéficiaire sur une fenêtre courte |
| Heure inhabituelle | Transaction hors de la plage d'activité habituelle du client |
| Incohérence géographique | Changement de localisation incompatible avec le délai |
| IBAN signalé | Présence dans `synthetic_fraud_iban_registry` |
| Vérification bénéficiaire | Résultat VoP simulé (`MATCH` / `CLOSE_MATCH` / `NO_MATCH`) |

### Approche hybride : règles + ML

```text
                 Transaction
                      │
             ┌────────┴────────┐
             ▼                 ▼
       Business Rules          ML
             │                 │
             └────────┬────────┘
                      ▼
                 Risk Score
                      │
       ┌──────────────┼──────────────┐
       ▼              ▼              ▼
    LOW (0-30)   MEDIUM (31-60)  HIGH (61-100)
       │              │              │
    Approve       Monitoring       Alert
```

**Moteur de règles (exemple)** :

```python
if amount > customer_avg * 10:  risk_score += 30
if new_beneficiary:             risk_score += 20
if transactions_1h > 10:        risk_score += 20
if suspicious_iban:             risk_score += 40
if unusual_location:            risk_score += 15
```

> ⚠️ Ces seuils sont des **seuils de simulation** et non des règles réglementaires.

**Modèles comparés** :

| Modèle | Type | Rôle |
| --- | --- | --- |
| Random Forest | Supervisé | Baseline |
| XGBoost | Supervisé | Modèle principal |
| Isolation Forest | Non supervisé | Détection d'anomalies |

---

## 10. Dashboard Power BI

| Page | Public | Contenu |
| --- | --- | --- |
| **1. Executive Fraud Overview** | Direction fraude / DAF | Nb transactions, nb fraudes, montant fraudé, taux de fraude, alertes, fraude évitée ; évolution quotidienne, par région, canal, typologie |
| **2. Analyse comportementale** | Analystes fraude | Fraude par heure, par tranche de montant (0-100 €, 100-500 €, 500-1 000 €, 1 000-5 000 €, > 5 000 €), par signal comportemental |
| **3. Performance du modèle** | Data / Risk | Matrice de confusion, Precision, Recall, F1, FPR, FNR, ROC-AUC, PR-AUC |
| **4. Impact financier** | Finance | Coût des FN, coût des FP, coût total du dispositif, fraude évitée |

---

## 11. Impact financier

Le cœur du volet Finance : **l'accuracy ne suffit pas**, on mesure ce que coûtent les erreurs.

```text
Coût total = coût FN + coût FP + coût investigation + coût opérationnel
```

Exemple :

| Erreur | Volume | Coût unitaire | Coût |
| --- | ---: | ---: | ---: |
| Faux négatifs (fraude non détectée) | 120 | 1 500 € (montant moyen) | 180 000 € |
| Faux positifs (alerte à tort) | 4 000 | 8 € (traitement d'alerte) | 32 000 € |

Le seuil de décision du modèle est ensuite choisi pour **minimiser le coût total** (*cost-based fraud detection*).

---

## 12. Organisation du travail en équipe

### Branches

| Branche | Rôle |
| --- | --- |
| `main` | Version stable, protégée |
| `develop` | Intégration des fonctionnalités |
| `feature/<nom>` | Une fonctionnalité (ex. `feature/transaction-generator`) |
| `fix/<nom>` | Correction de bug |

### Workflow

```bash
git checkout develop && git pull
git checkout -b feature/ma-fonctionnalite
# ... développement ...
git add . && git commit -m "feat: description courte"
git push -u origin feature/ma-fonctionnalite
# → ouvrir une Pull Request vers develop, relue par l'autre contributeur
```

### Convention de commits

`feat:` nouvelle fonctionnalité · `fix:` correction · `docs:` documentation · `refactor:` refactoring · `test:` tests · `chore:` configuration / maintenance

### Règles

- Ne jamais committer `.env`, de données ou de modèles entraînés
- Une PR = une fonctionnalité, relue avant merge
- Tests verts avant merge dans `develop`

---

## 13. Roadmap

- [x] **Phase 0** — Architecture du projet, environnement, repository Git
- [ ] **Phase 1** — Générateur de transactions synthétiques (clients, bénéficiaires, fraudes labellisées)
- [ ] **Phase 2** — Infra Docker : Kafka + PostgreSQL (`docker-compose.yml`)
- [ ] **Phase 3** — Producer Kafka → topic `payments.raw`
- [ ] **Phase 4** — PySpark Structured Streaming : cleaning, enrichment, features
- [ ] **Phase 5** — Stockage Bronze / Silver / Gold dans PostgreSQL + modèles dbt
- [ ] **Phase 6** — Moteur de règles + scoring de risque
- [ ] **Phase 7** — Modèles ML (Random Forest, XGBoost, Isolation Forest) + comparaison
- [ ] **Phase 8** — Orchestration Airflow
- [ ] **Phase 9** — Analyse financière (notebooks) + dashboard Power BI
- [ ] **Phase 10** — Tests, qualité des données, documentation finale

---

## 14. Avertissements

- **Toutes les données sont synthétiques.** Aucun IBAN, nom ou donnée bancaire réelle n'est utilisé. Les IBAN sont générés puis hashés.
- Ce projet est un **prototype pédagogique / portfolio**, inspiré du cadre réglementaire européen et français. Il n'est **ni certifié ni conforme** à une réglementation bancaire.
- Les seuils, règles et coûts sont des **hypothèses de simulation**.

---

## 15. Auteurs

| Nom | Rôle | GitHub |
| --- | --- | --- |
| Elie Bea | Data Engineer / Data Analyst | *à compléter* |
| *Collaborateur* | *à compléter* | *à compléter* |
