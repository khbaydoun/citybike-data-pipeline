resource "kubernetes_namespace_v1" "citibike" {
  metadata {
    name = var.namespace
  }
}
