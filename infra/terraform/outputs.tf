output "next_steps" {
  description = "How to reach the stack from the host"
  value       = <<-EOT

    kubectl -n ${var.namespace} get pods

    # In two terminals (keep them running):
    kubectl -n ${var.namespace} port-forward svc/api 8000:8000
    kubectl -n ${var.namespace} port-forward svc/redpanda 19092:19092

    # Then, from the repo root:
    .venv/bin/python tools/run_generator.py --file $(find data -name '*.csv' | sort) --broker localhost:19092
    curl localhost:8000/stations/busiest
    .venv/bin/python helpers/acceptance_check.py --file $(find data -name '*.csv' | sort)
  EOT
}
