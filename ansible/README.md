# JGroups Ansible Setup

Ansible playbook to prepare remote nodes for running JGroups clusters.

## Prerequisites

- Ansible installed locally
- GCP (or AWS) nodes already running and accessible via SSH
- An inventory file with the target hosts (see templates below)

## Usage

```bash
ansible-playbook -i <inventory-file> setup.yml
```

## What `setup.yml` does

- **Installs dependencies** (git, zip, unzip, net-tools, rsync, SDKMAN!, JDK 25, async-profiler).
  Installed once and skipped on subsequent runs.
  Delete the marker to force reinstallation (`/tmp/jgroups_deps_installed`).
- **Uploads `bin/` and `target/`** from the JGroups project root to `$HOME` on each node.
- **Generates `hosts.txt`** under `target/classes/` with private IPs of all instances for cluster discovery.
  Only created when `target/` exists locally.

## Inventory

Copy and edit a template for your environment:

- `gcp-inventory.yaml.template` — GCP nodes with placeholder IPs
