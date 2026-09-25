# API: stateless, so a Deployment behind a Service that load-balances across replicas.
# 2 replicas by default for availability during restarts / rolling updates.

resource "kubernetes_deployment_v1" "api" {
  metadata {
    name      = "api"
    namespace = kubernetes_namespace_v1.citibike.metadata[0].name
  }
  spec {
    replicas = var.api_replicas
    selector {
      match_labels = { app = "api" }
    }
    template {
      metadata {
        labels = { app = "api" }
      }
      spec {
        container {
          name              = "api"
          image             = var.api_image
          image_pull_policy = "IfNotPresent"
          env {
            name  = "REDIS_URL"
            value = "redis://redis:6379/0"
          }
          port {
            container_port = 8000
          }
          # Liveness never touches Redis: a Redis outage must not restart every API pod.
          liveness_probe {
            http_get {
              path = "/health"
              port = 8000
            }
            period_seconds = 10
          }
          # Readiness checks Redis: an affected pod just stops receiving traffic.
          readiness_probe {
            http_get {
              path = "/ready"
              port = 8000
            }
            period_seconds = 5
          }
          resources {
            requests = { cpu = "100m", memory = "96Mi" }
            limits   = { memory = "256Mi" }
          }
        }
      }
    }
  }

  depends_on = [kubernetes_stateful_set_v1.redis]
}

resource "kubernetes_service_v1" "api" {
  metadata {
    name      = "api"
    namespace = kubernetes_namespace_v1.citibike.metadata[0].name
  }
  spec {
    selector = { app = "api" }
    port {
      port        = 8000
      target_port = 8000
    }
  }
}
