# Topics Kafka

| Topic             | Producteur               | Consommateur            | Contenu                                        |
| ----------------- | ------------------------ | ----------------------- | ---------------------------------------------- |
| `payments.raw`    | `producer/`              | `streaming/`            | Virements instantanés bruts (JSON)             |
| `customers`       | `producer/`              | `streaming/`            | Référentiel clients synthétique                |
| `beneficiaries`   | `producer/`              | `streaming/`            | Référentiel bénéficiaires synthétique          |
| `fraud.alerts`    | `fraud_detection/`       | PostgreSQL / monitoring | Transactions à risque MEDIUM / HIGH            |
| `fraud.decisions` | `fraud_detection/`       | PostgreSQL              | Décision finale (approve / monitor / block)    |

À compléter : partitions, rétention, clé de partitionnement (`customer_id` pressenti).
