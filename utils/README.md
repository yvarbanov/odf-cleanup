# ODF Cleanup Utilities

Supporting tools for ODF cleanup operations - discovery and monitoring utilities that complement the main cleanup functionality.

## Tools Overview

- **odf-oc-compare.py** - Discovery tool that identifies orphaned storage by comparing OpenShift namespaces with ODF volumes
- **odf-cleanup-monitor.py** - Monitoring tool that analyzes cleanup job failures and generates reports

---

## ODF-OpenShift Comparison Tool

The `odf-oc-compare.py` script compares active OpenShift namespaces with ODF RBD images to identify orphaned lab GUIDs. This helps discover storage resources that are no longer associated with active labs and can be safely cleaned up.

### Key Features

- **Namespace Analysis**: Discovers active lab GUIDs from OpenShift projects (pattern: `sandbox-{GUID}-*`)
- **ODF Resource Discovery**: Analyzes volumes, CSI snapshots, and trash items
- **Parentless CSI Snapshot Analysis**: Identifies potential boot/base images by analyzing children relationships
- **Smart Ordering**: Prioritizes cleanup by complexity (volumes only → volumes+snapshots → volumes+snapshots+trash)
- **Automated Script Generation**: Creates ready-to-run cleanup scripts

### Workflow

The comparison tool follows this logical workflow:

```
Workflow Step                     Implementation
-------------                     --------------
compare                       →   run_comparison()
get projects                  →   discover_namespace_guids()  
get csi-snaps + analyze       →   discover_odf_guids() + _analyze_parentless_csi_snap()
get volumes                   →   _extract_guid_from_image()
compare guids                 →   compare_and_find_orphans()
order for deletion            →   _order_guids_by_complexity()
create script                 →   generate_cleanup_script()
run cleanup                   →   Generated bash script
```

### Usage

```console
cd odf-cleanup
source env.sh
python3 utils/odf-oc-compare.py
```


### Output

The script generates:
1. **Detailed comparison report** showing active vs orphaned GUIDs
2. **Parentless CSI snapshot analysis** with safety recommendations  
3. **Automated cleanup script** (`cleanup_orphaned_guids.sh`) ordered by complexity

---

## ODF Cleanup Monitor

The `odf-cleanup-monitor.py` script monitors ODF cleanup jobs in OpenShift and reports failures. It analyzes job logs to identify failed cleanup operations and generates reports for manual intervention.

### Key Features

- **Job Monitoring** - Scans cleanup namespace for failed jobs
- **Log Analysis** - Extracts error details and LAB GUIDs from job logs
- **Dual Reporting** - Console summaries and CSV reports
- **Error Classification** - Categorizes failures by type (ERROR, FAILED, WARNING)

### Usage

```console
# Monitor default cleanup namespace
python3 utils/odf-cleanup-monitor.py

# Monitor different namespace with debug
python3 utils/odf-cleanup-monitor.py --namespace my-cleanup --debug

# Generate CSV report only
python3 utils/odf-cleanup-monitor.py --format csv --csv failures.csv
```

### Output

- **Console**: Summary with success/failure counts and error details
- **CSV**: Structured data (job_name, guid, status, error_type, error_reason)
- **Exit codes**: 0 for no failures, 1 if cleanup failures detected

---

## Requirements

### Python Packages
```console
pip install kubernetes rados rbd
```

### Access Requirements
- **ODF Cluster**: Configuration file and keyring (for comparison tool)
- **OpenShift/Kubernetes**: Valid kubeconfig or in-cluster service account
- **Namespace Access**: Read permissions for target namespaces and cleanup jobs

### Environment Variables
- **Comparison tool**: `CL_POOL`, `CL_CONF`, `CL_KEYRING` (CL_LAB not required)
- **Monitor tool**: None required (uses kubeconfig/service account)

---

## Documentation

For detailed information:
- [Complete comparison tool documentation](../Documentation/odf-oc-compare.md)
- [Complete monitoring tool documentation](../Documentation/odf-cleanup-monitor.md)