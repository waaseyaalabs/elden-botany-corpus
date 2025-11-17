resource "proxmox_vm_qemu" "control_plane" {
  for_each = local.control_plane_nodes

  name        = "${var.cluster_name}-${each.key}"
  target_node = var.proxmox_node
  pool        = var.proxmox_pool == "" ? null : var.proxmox_pool

  iso     = var.talos_iso_path
  cores   = var.control_plane_vcpu
  sockets = 1
  memory  = var.control_plane_memory_mb
  onboot  = true
  tags    = local.proxmox_tag_string
  agent   = 0
  os_type  = "l26"
  scsihw   = "virtio-scsi-pci"
  bootdisk = "scsi0"

  disk {
    id      = 0
    type    = "scsi"
    storage = var.vm_storage
    size    = "${var.system_disk_gb}G"
  }

  network {
    id     = 0
    model  = "virtio"
    bridge = var.vm_network_bridge
  }

  serial {
    id   = 0
    type = "socket"
  }

  vga = "serial0"
}
