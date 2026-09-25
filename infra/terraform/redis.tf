# Redis: the serving layer and the only copy of the aggregates.
# StatefulSet + persistent volume so state survives pod restarts (same flags as docker-compose).

locals {
  redis_labels = { app = "redis" }
}

resource "kubernetes_service_v1" "redis" {
  metadata {
    name      = "redis"
    namespace = kubernetes_namespace_v1.citibike.metadata[0].name
  }
  spec {
    cluster_ip = "None" # headless: DNS "redis" resolves straight to the pod
    selector   = local.redis_labels
    port {
      name = "redis"
      port = 6379
    }
  }
}

resource "kubernetes_stateful_set_v1" "redis" {
  metadata {
    name      = "redis"
    namespace = kubernetes_namespace_v1.citibike.metadata[0].name
  }
  spec {
    service_name = kubernetes_service_v1.redis.metadata[0].name
    replicas     = 1
    selector {
      match_labels = local.redis_labels
    }
    template {
      metadata {
        labels = local.redis_labels
      }
      spec {
        container {
          name  = "redis"
          image = "redis:8.10.2-alpine"
          args = [
            "--appendonly", "yes",
            "--appendfsync", "everysec",
            "--save", "",
            "--maxmemory-policy", "noeviction",
          ]
          port {
            container_port = 6379
          }
          volume_mount {
            name       = "data"
            mount_path = "/data"
          }
          resources {
            requests = { cpu = "100m", memory = "256Mi" }
            limits   = { memory = "1536Mi" } # full month ≈ 950 MB
          }
          readiness_probe {
            exec {
              command = ["redis-cli", "ping"]
            }
            period_seconds = 5
          }
          liveness_probe {
            exec {
              command = ["redis-cli", "ping"]
            }
            initial_delay_seconds = 10
            period_seconds        = 10
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
          requests = { storage = "2Gi" }
        }
      }
    }
  }
}
