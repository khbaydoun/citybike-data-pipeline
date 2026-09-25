# One-shot Job (the K8s version of compose's topic-init): disable topic auto-creation,
# then create the topic with an explicit partition count if it doesn't exist.

resource "kubernetes_job_v1" "topic_init" {
  metadata {
    name      = "topic-init"
    namespace = kubernetes_namespace_v1.citibike.metadata[0].name
  }
  spec {
    backoff_limit = 5 # retried if Redpanda isn't reachable yet
    template {
      metadata {
        labels = { app = "topic-init" }
      }
      spec {
        restart_policy = "Never"
        container {
          name    = "rpk"
          image   = "docker.redpanda.com/redpandadata/redpanda:v26.2.3"
          command = ["/bin/sh", "-c"]
          args = [<<-EOT
            rpk cluster config set auto_create_topics_enabled false -X admin.hosts=${local.redpanda_pod_host}:9644 &&
            if rpk topic describe ${var.topic} -X brokers=${local.redpanda_pod_host}:9092 >/dev/null 2>&1; then
              echo "topic ${var.topic} already exists"
            else
              rpk topic create ${var.topic} -X brokers=${local.redpanda_pod_host}:9092 \
                --partitions ${var.partitions} --replicas 1 \
                --topic-config cleanup.policy=delete --topic-config retention.ms=-1
            fi
          EOT
          ]
        }
      }
    }
  }
  # terraform apply blocks until the Job has succeeded, so resources that depend on it
  # (ingestion) only start once the topic exists.
  wait_for_completion = true
  timeouts {
    create = "3m"
  }

  depends_on = [kubernetes_stateful_set_v1.redpanda]
}
