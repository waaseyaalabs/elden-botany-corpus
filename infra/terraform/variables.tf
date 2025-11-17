variable "cluster_name" {
  description = "Human friendly name stamped onto every Talos VM and Proxmox tag."
  type        = string
  default     = "elden-talos"
}

variable "control_plane_count" {
  description = "Number of Talos control plane nodes to deploy."
  type        = number
  default     = 3

  validation {
    condition     = var.control_plane_count >= 1
    error_message = "The cluster must contain at least one control plane node."
  }
}

variable "worker_count" {
  description = "Number of Talos worker nodes to deploy."
  type        = number
  default     = 4

  validation {
    condition     = var.worker_count >= 0
    error_message = "Worker count cannot be negative."
  }
}

variable "control_plane_vcpu" {
  description = "vCPUs assigned to each control plane VM."
  type        = number
  default     = 4
}

variable "control_plane_memory_mb" {
  description = "Memory (MiB) allocated to each control plane VM."
  type        = number
  default     = 8192
}

variable "worker_vcpu" {
  description = "vCPUs assigned to each worker VM."
  type        = number
  default     = 4
}

variable "worker_memory_mb" {
  description = "Memory (MiB) allocated to each worker VM."
  type        = number
  default     = 8192
}

variable "system_disk_gb" {
  description = "Size (GiB) of the Talos system disk attached to every VM."
  type        = number
  default     = 40
}

variable "talos_iso_path" {
  description = "Storage reference to the Talos ISO uploaded to Proxmox (e.g. local:iso/talos-amd64.iso)."
  type        = string
}

variable "talos_version" {
  description = "Talos release that will be installed from the ISO."
  type        = string
  default     = "v1.7.2"
}

variable "control_plane_endpoint" {
  description = "Reachable endpoint (VIP, load balancer, or static IP) Talos uses for the Kubernetes API, formatted as host or host:port."
  type        = string
}

variable "proxmox_api_url" {
  description = "Base URL of the Proxmox API (e.g. https://pm.example.com:8006/api2/json)."
  type        = string
}

variable "proxmox_api_token_id" {
  description = "Token identifier (e.g. terraform@pve!bot)."
  type        = string
  sensitive   = true
}

variable "proxmox_api_token_secret" {
  description = "Token secret created in Proxmox for Terraform automation."
  type        = string
  sensitive   = true
}

variable "proxmox_tls_insecure" {
  description = "Set true to skip TLS verification (not recommended for production)."
  type        = bool
  default     = false
}

variable "proxmox_parallel" {
  description = "Maximum number of concurrent API operations executed by the provider."
  type        = number
  default     = 4
}

variable "proxmox_node" {
  description = "Target Proxmox node that hosts the VMs."
  type        = string
}

variable "proxmox_pool" {
  description = "Optional Proxmox resource pool to place the VMs into."
  type        = string
  default     = ""
}

variable "vm_storage" {
  description = "Proxmox storage identifier that backs the Talos VM disks."
  type        = string
  default     = "local-lvm"
}

variable "vm_network_bridge" {
  description = "Bridge interface the VMs will attach to (e.g. vmbr0)."
  type        = string
  default     = "vmbr0"
}

variable "proxmox_tags" {
  description = "Additional tags applied to each VM (semicolon separated in the Proxmox UI)."
  type        = list(string)
  default     = []
}
