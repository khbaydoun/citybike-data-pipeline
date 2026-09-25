variable "kubeconfig_path" {
  type    = string
  default = "~/.kube/config"
}

variable "kube_context" {
  type    = string
  default = "minikube"
}

variable "namespace" {
  type    = string
  default = "citibike"
}

variable "ingestion_image" {
  description = "Loaded into minikube with `minikube image load`"
  type        = string
  default     = "citybike-ingestion:local"
}

variable "api_image" {
  type    = string
  default = "citybike-api:local"
}

variable "topic" {
  type    = string
  default = "citibike-events"
}

variable "partitions" {
  description = "Caps consumer parallelism; see README"
  type        = number
  default     = 6
}

variable "ingestion_replicas" {
  description = "Consumer group members; useful maximum = partitions"
  type        = number
  default     = 1
}

variable "api_replicas" {
  type    = number
  default = 2
}
