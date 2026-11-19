# Unified Stream Manager (USM) on Azure: demos

Step-by-step demos for monitoring self-managed Confluent components on Azure Kubernetes Service (AKS) from Confluent Cloud with **Unified Stream Manager**, using Confluent for Kubernetes (CFK) and Azure Private Link.

## Scenarios

| Scenario | What runs on AKS | Kafka cluster | Registered with USM |
|---|---|---|---|
| [**`cp-on-aks/`**](cp-on-aks/) | KRaft, Kafka brokers, Schema Registry, Connect, USM Agent | Confluent Platform on AKS | CP Kafka cluster + Connect cluster |
| [**`connect-on-aks/`**](connect-on-aks/) | Connect, USM Agent | Confluent Cloud | Connect cluster only (CC Kafka is its metadata cluster) |

Each folder is self-contained: its own README, manifests (`k8s/`) and screenshots (`images/`). Run the commands from inside the scenario folder.

## Shared building blocks

Both scenarios use the same:
- AKS cluster `aks-usm-lab` in resource group `rg-usm-lab`, **Germany West Central**, namespace `confluent`
- CFK **3.3.1** (Helm chart `0.1718.99`), CP **8.2.4**, USM Agent **1.2.1**
- USM Private Link setup: Ingress PrivateLink Gateway, Access Point, Private Endpoint in the AKS VNet, Private DNS zone

You can run them one after the other on the same AKS cluster. Delete the previous scenario's resources first, but keep the CFK operator and the Private Link setup.
