output "kubernetes_api_endpoint" {
  description = "Authoritative endpoint Talos will advertise for the Kubernetes API."
  value       = "https://${var.control_plane_endpoint}:6443"
}

output "control_plane_vm_ids" {
  description = "VMIDs allocated to each Talos control plane node."
  value       = { for name, vm in proxmox_vm_qemu.control_plane : name => vm.id }
}

output "worker_vm_ids" {
  description = "VMIDs allocated to each Talos worker node."
  value       = { for name, vm in proxmox_vm_qemu.worker : name => vm.id }
}

output "talos_iso" {
  description = "ISO reference each VM boots from."
  value       = var.talos_iso_path
}
