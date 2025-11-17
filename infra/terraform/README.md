# Terraform Talos Cluster (Proxmox)

This Terraform project now provisions **Talos Linux** nodes directly from an uploaded ISO instead of cloning Ubuntu templates. The files are intentionally split by concern so it is easy to see where each resource lives:

- `versions.tf` – Terraform/Provider requirements
- `variables.tf` – Proxmox credentials, Talos inputs, and sizing
- `locals.tf` – computed node maps and tag helpers
- `control-plane.tf` – Proxmox VM definitions for the three control plane nodes
- `workers.tf` – Proxmox VM definitions for the four worker nodes
- `outputs.tf` – API endpoint, VMIDs, and ISO reference

No `main.tf` monolith and no bootstrap shell scripts—each Talos VM boots from the ISO and waits for you to push machine configuration with `talosctl`.

## Prerequisites
- Terraform 1.6+
- Proxmox VE 7+ reachable from the Terraform runner
- Talos ISO uploaded to a Proxmox storage location (e.g. `local:iso/talos-amd64.iso`)
- API token with at least `VM.Audit`, `VM.Clone`, `VM.Config.Disk`, `VM.Config.Options`, `Datastore.AllocateSpace`, and `Datastore.Audit`
- A routable network (typically DHCP) so Talos nodes can obtain addresses that you later target with `talosctl`
- `talosctl` CLI on your workstation for generating configs and applying them to the new VMs

## Usage
1. Upload the desired Talos ISO to Proxmox and note the storage reference (e.g. `local:iso/talos-amd64-v1.7.2.iso`).
2. Provide a stable Kubernetes API endpoint (VIP, static IP, or load balancer) via `control_plane_endpoint`; Talos machine configs need this value.
3. Populate a `terraform.tfvars` similar to the example below, then run Terraform:

```bash
cd infra/terraform
terraform init
terraform apply \
  -var "proxmox_api_url=https://pve.lab.local:8006/api2/json" \
  -var "proxmox_api_token_id=terraform@pve!bot" \
  -var "proxmox_api_token_secret=<token>" \
  -var "proxmox_node=pve01" \
  -var "talos_iso_path=local:iso/talos-amd64-v1.7.2.iso" \
  -var "control_plane_endpoint=10.10.10.50"
```

Terraform only provisions the virtual hardware. Once the VMs have booted into Talos maintenance mode, finish the cluster bootstrap with `talosctl`:

```bash
# Generate configs for the requested endpoint and installer disk
talosctl gen config tarnished-cluster https://10.10.10.50:6443 \
  --install-disk /dev/sda \
  --output ./generated/talos

# Apply the control plane config to each node
talosctl apply-config --nodes <cp-ip> --file ./generated/talos/controlplane.yaml --insecure

# Apply the worker config once the control plane is healthy
talosctl apply-config --nodes <worker-ip> --file ./generated/talos/worker.yaml --insecure

# Bootstrap the cluster and merge kubeconfig
talosctl bootstrap --nodes <cp-ip>
talosctl kubeconfig --nodes <cp-ip> --endpoints 10.10.10.50 --force
```

> Tip: Because Talos does not ship with a QEMU guest agent, the VM IPs are not exposed in Terraform outputs. Use your DHCP server leases or the Proxmox console to read the IP displayed on the Talos splash screen before running `talosctl`.

### Example `terraform.tfvars`
```hcl
cluster_name             = "tarnished-talos"
proxmox_api_url          = "https://pve.lab.local:8006/api2/json"
proxmox_api_token_id     = "terraform@pve!bot"
proxmox_api_token_secret = "<redacted>"
proxmox_node             = "pve01"
proxmox_pool             = "lab"
vm_storage               = "local-lvm"
vm_network_bridge        = "vmbr0"
talos_iso_path           = "local:iso/talos-amd64-v1.7.2.iso"
talos_version            = "v1.7.2"
control_plane_endpoint   = "10.10.10.50"
control_plane_count      = 3
worker_count             = 4
system_disk_gb           = 80
control_plane_vcpu       = 4
control_plane_memory_mb  = 8192
worker_vcpu              = 4
worker_memory_mb         = 8192
```

## Cleanup

Destroy the VMs when the cluster is no longer required:

```bash
terraform destroy
```
