# Ingestion: stateless consumer-group members, so a Deployment (not a StatefulSet).
# Scale with: terraform apply -var ingestion_replicas=3   (useful maximum = partitions)

resource "kubernetes_deployment_v1" "ingestion" {
  metadata {
    name      = "ingestion"
    namespace = kubernetes_namespace_v1.citibike.metadata[0].name
  }
  spec {
    replicas = var.ingestion_replicas
    selector {
      match_labels = { app = "ingestion" }
    }
    template {
      metadata {
        labels = { app = "ingestion" }
      }
      spec {
        # SIGTERM -> finish the batch, commit, leave the group cleanly.
        termination_grace_period_seconds = 30
        container {
          name              = "ingestion"
          image             = var.ingestion_image
          image_pull_policy = "IfNotPresent" # image is loaded into minikube, not pulled
          env {
            name  = "KAFKA_BROKERS"
            value = "${local.redpanda_pod_host}:9092"
          }
          env {
            name  = "TOPIC"
            value = var.topic
          }
          env {
            name  = "GROUP_ID"
            value = "citibike-ingestion" # same for every replica = one consumer group
          }
          env {
            name  = "REDIS_URL"
            value = "redis://redis:6379/0"
          }
          env {
            name  = "RIDE_TTL_S"
            value = tostring(var.ride_ttl_s)
          }
          resources {
            requests = { cpu = "250m", memory = "128Mi" }
            limits   = { memory = "256Mi" }
          }
        }
      }
    }
  }

  depends_on = [kubernetes_job_v1.topic_init, kubernetes_stateful_set_v1.redis]
}
