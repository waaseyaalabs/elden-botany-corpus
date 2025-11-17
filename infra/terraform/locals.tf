locals {
  control_plane_nodes = {
    for idx in range(var.control_plane_count) :
    format("cp-%02d", idx + 1) => {
      ordinal = idx + 1
    }
  }

  worker_nodes = {
    for idx in range(var.worker_count) :
    format("wk-%02d", idx + 1) => {
      ordinal = idx + 1
    }
  }

  talos_version_tag  = format("talos-%s", replace(var.talos_version, ".", "-"))
  proxmox_tags       = distinct(compact(concat([var.cluster_name, "talos", local.talos_version_tag], var.proxmox_tags)))
  proxmox_tag_string = length(local.proxmox_tags) > 0 ? join(";", local.proxmox_tags) : ""
}
