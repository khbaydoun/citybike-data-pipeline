# Redpanda: single-node broker, same flags as docker-compose.
# StatefulSet so the pod keeps a stable DNS name (redpanda-0.redpanda...) and its own volume.

locals {
  redpanda_labels   = { app = "redpanda" }
  redpanda_pod_host = "redpanda-0.redpanda.${var.namespace}.svc.cluster.local"
}

resource "kubernetes_service_v1" "redpanda" {
  metadata {
    name      = "redpanda"
    namespace = kubernetes_namespace_v1.citibike.metadata[0].name
  }
  spec {
    cluster_ip = "None" # headless: gives the StatefulSet pod its stable DNS name
    selector   = local.redpanda_labels
    port {
      name = "kafka-internal"
      port = 9092
    }
    port {
      name = "kafka-external" # reached from the Mac via kubectl port-forward
      port = 19092
    }
    port {
      name = "admin"
      port = 9644
    }
  }
}

resource "kubernetes_stateful_set_v1" "redpanda" {
  metadata {
    name      = "redpanda"
    namespace = kubernetes_namespace_v1.citibike.metadata[0].name
  }
  spec {
    service_name = kubernetes_service_v1.redpanda.metadata[0].name
    replicas     = 1
    selector {
      match_labels = local.redpanda_labels
    }
    template {
      metadata {
        labels = local.redpanda_labels
      }
      spec {
        container {
          name  = "redpanda"
          image = "docker.redpanda.com/redpandadata/redpanda:v26.2.3"
          args = [
            "redpanda", "start",
            "--mode=dev-container", # single node, relaxed fsync: local only
            "--smp=1",
            "--memory=1G",
            "--default-log-level=warn",
            "--kafka-addr=internal://0.0.0.0:9092,external://0.0.0.0:19092",
            # internal: pods in the cluster; external: the Mac through port-forward
            "--advertise-kafka-addr=internal://${local.redpanda_pod_host}:9092,external://localhost:19092",
          ]
          port {
            container_port = 9092
          }
          port {
            container_port = 19092
          }
          port {
            container_port = 9644
          }
          volume_mount {
            name       = "data"
            mount_path = "/var/lib/redpanda/data"
          }
          resources {
            requests = { cpu = "500m", memory = "1Gi" }
            limits   = { memory = "1536Mi" }
          }
          readiness_probe {
            exec {
              command = ["sh", "-c", "rpk cluster health | grep -qE 'Healthy:.+true'"]
            }
            initial_delay_seconds = 5
            period_seconds        = 5
          }
        }
      }
    }
    volume_claim_template {
      metadata {
        name = "data"
      }
      spec {
        access_modes = ["ReadWriteOnce"]
        resources {
          requests = { storage = "5Gi" } # full month ≈ 2-3 GB of log segments
        }
      }
    }
  }
}
