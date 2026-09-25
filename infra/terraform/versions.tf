terraform {
  required_version = ">= 1.6"

  required_providers {
    kubernetes = {
      source  = "hashicorp/kubernetes"
      version = "~> 2.38"
    }
  }
}

# Talks to the cluster the same way kubectl does: ~/.kube/config, "minikube" context.
provider "kubernetes" {
  config_path    = var.kubeconfig_path
  config_context = var.kube_context
}
