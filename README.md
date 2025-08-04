# odf cleanup
Removes leftover odf images based on an identifier, referenced as LAB GUID

## Assumptions:
* OpenShift CNV + ODF
* GUID: A unique identifier used to group and track resources belonging to a specific lab environment
    - Appears in OpenShift namespace names as: sandbox-{GUID}-*
    - Appears in ODF volume names as: ocp4-cluster-{GUID}-{UUID}
    - The GUID itself is a unique identifier string (ie: "abc123")

Volumes: LAB GUID is embedded in the volume name
CSI Snapshots: Named as "csi-snap-..." but connected to volumes through parent-child relationships

## Current Strategy

The script uses a **three-phase approach** with **direct matching**, **comprehensive descendant analysis**, and **dependency resolution**:

### Phase 1: Direct GUID Matching
```python
def _find_trash_images(self) -> List[OdfImage]:
    """Find images in trash containing LAB GUID"""
    images = []
    try:
        trash_items = rbd.RBD().trash_list(self.ioctx)
        lab_trash = [item for item in trash_items if self.lab_guid in item['name']]
        
        for item in lab_trash:
            if 'csi-snap' not in item['name']:  # Regular images, not csi-snaps
                image = self._create_trash_image(item, ImageType.TRASH_VOLUME)
                if image:
                    images.append(image)
```

### Phase 2: Comprehensive Descendant Analysis

```python
def _discover_descendants_and_dependencies() -> Tuple[List[OdfImage], Dict[str, List[str]]]:
    """Recursively scan for missing descendants and track trash dependencies"""
    # Single scan handles both:
    # 1. Active descendant discovery (for tree hierarchy)
    # 2. Trash dependency tracking (for cleanup strategy)
    
    for desc in descendants:
        if desc.get('trash', False):
            # Track trash dependency only
            active_to_trash_deps[image.name].append(desc_name)
        else:
            # Add active descendant to discovery
            current_batch.append(new_image)
```

**Key Benefits:**
- **Eliminates "still has descendants" errors** by finding ALL blocking children
- **Optimized Performance** - Single RBD API scan instead of duplicate calls
- **Complete Hierarchy** - Recursive discovery ensures no missing depth levels
- **Dual Purpose** - Discovers descendants AND tracks trash dependencies simultaneously

### Phase 3: Final Discovery and Tree Building
```python
# Complete discovery with trash csi-snaps
trash_csi_snaps = self._find_trash_csi_snaps()
all_discovered = initial_images + additional_images + trash_csi_snaps

# Build hierarchical tree with proper relationships
self.tree.build_tree(all_discovered)
```

## Cleanup Execution Flow

The script uses a **two-phase execution strategy** with automatic retry and verification:

### Phase 1: Initial Cleanup Attempt
```python
def execute_cleanup(self, removal_order: List[OdfImage]):
    # Initial cleanup attempt
    initial_failed_count = self._execute_removal_batch(removal_order, "Initial cleanup")
```

The script first attempts to remove all discovered items in the calculated dependency order (children → parents).

### Phase 2: Retry (Only if failures occur)
```python
    # If we had failures, try trash purge and retry
    if initial_failed_count > 0:
        print("RETRY STRATEGY - FAILURES DETECTED")
        print("Attempting trash purge to clear blocking items...")
        
        if self._purge_expired_trash():
            # Get only the failed items from the last attempt
            failed_items = [item for item in removal_order 
                          if item.name in self.removal_stats['failed_removals']]
            
            # Clear previous failures for retry
            self.removal_stats['failed_removals'] = []
            
            # Retry only failed items
            retry_failed_count = self._execute_removal_batch(failed_items, "Retry after purge")
```

**Key Benefits:**
- **Performance**: Only runs trash purge when actually needed
- **Efficiency**: Only retries items that actually failed
- **Resilience**: Handles blocking trash dependencies automatically

### Phase 3: Final Verification (Only on complete success)
```python
def _final_verification(self):
    """Final verification that no objects with the GUID remain in the pool"""
    
    # Check active pool images
    all_rbd_images = rbd.RBD().list(self.ioctx)
    remaining_active = [img for img in all_rbd_images if self.lab_guid in img]
    
    # Check trash items  
    trash_items = rbd.RBD().trash_list(self.ioctx)
    remaining_trash = [item['name'] for item in trash_items if self.lab_guid in item['name']]
```

**Final verification only runs when:**
- Zero failed removals
- Zero failed trash restorations  
- Complete cleanup success

### Complete Execution Flow
```
execute_cleanup()
├── _execute_removal_batch() [Initial attempt]  
├── Check failures?
│   ├── No failures → _final_verification() → Done
│   └── Failures detected
│       ├── _purge_expired_trash()
│       ├── _execute_removal_batch() [Retry failed items]
│       └── _final_verification() [If retry successful]
└── _generate_report()
```

### Sample Output

**Successful first attempt:**
```
Initial cleanup for 5 items...
[All items removed successfully]

FINAL VERIFICATION - Checking for remaining objects...
SUCCESS: No objects with GUID found in pool
Cleanup completed successfully for LAB GUID: abc123
```

**With intelligent retry:**
```
Initial cleanup for 5 items...
[2 items fail due to blocking dependencies]

============================================================
RETRY STRATEGY - FAILURES DETECTED  
============================================================
Initial cleanup had 2 failures
Attempting trash purge to clear blocking items...

Purging expired trash items from pool 'ocpv-tenants'...
  SUCCESS: Purged 3 expired trash items

Retry after purge for 2 items...
[Previously failed items now succeed]

All previously failed items successfully removed after trash purge!

FINAL VERIFICATION - Checking for remaining objects...
SUCCESS: No objects with GUID found in pool
```

## Requirements

### System Requirements
- **Python 3.6+**
- **Ceph/ODF tools** installed and accessible
- **OpenShift CLI (oc)** or **kubectl** for comparison tool
- **Valid kubeconfig** with cluster access (for comparison tool)

### Python Packages
```console
# For both scripts
pip install rados rbd

# Additional for comparison tool
pip install kubernetes
```

### ODF Cluster Access
- **ODF configuration file** (ceph.conf)
- **ODF keyring** with sufficient privileges to list and manage RBD images

## Environment Variables

### Required for Both Scripts
- `CL_POOL` - ODF pool name (e.g., "ocpv-tenants")
- `CL_CONF` - Path to Ceph configuration file
- `CL_KEYRING` - Path to Ceph keyring file

### Required for Cleanup Script Only
- `CL_LAB` - LAB GUID to clean up

### Optional for Both Scripts
- `DRY_RUN` - Enable dry-run mode (default: "true")
- `DEBUG` - Enable debug output (default: "false")

## How to Use

### ODF Cleanup Script
```console
git clone https://github.com/yvarbanov/odf-cleanup.git odf-cleanup
cd odf-cleanup
source env.sh
# Edit env.sh with your specific values
export CL_LAB="your-lab-guid"
python3 odf-cleanup.py
```

### ODF-OpenShift Comparison Tool
```console
cd odf-cleanup
source env.sh
# Edit env.sh with your specific values (CL_LAB not needed)
python3 odf-oc-compare.py
```

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
source env.sh
python3 odf-oc-compare.py
```

**Additional Requirements:**
- Valid kubeconfig with access to OpenShift/Kubernetes cluster
- `CL_LAB` environment variable not required (tool discovers all GUIDs)

### Output

The script generates:
1. **Detailed comparison report** showing active vs orphaned GUIDs
2. **Parentless CSI snapshot analysis** with safety recommendations  
3. **Automated cleanup script** (`cleanup_orphaned_guids.sh`) ordered by complexity

## Troubleshooting

### Common Issues

**"Missing environment variable" error:**
```console
# Make sure all required variables are set
source env.sh
env | grep CL_
```

**"Error connecting to ODF cluster":**
- Verify ODF configuration file path exists: `ls -la $CL_CONF`
- Check keyring file permissions: `ls -la $CL_KEYRING`
- Test ODF connectivity: `rbd -p $CL_POOL list`

**"Error discovering namespaces" (comparison tool):**
- Verify kubeconfig is valid: `oc whoami` or `kubectl auth can-i get namespaces`
- Check cluster connectivity: `oc get projects` or `kubectl get namespaces`

**"RBD image not found" warnings:**
- These are normal for orphaned CSI snapshots
- Enable debug mode for more details: `export DEBUG="true"`

**Performance Issues:**
- Large clusters may take longer for initial discovery
- Script uses caching to optimize repeated operations
- Consider running during off-peak hours for large cleanups

### Debug Mode

Enable detailed logging for troubleshooting:
```console
export DEBUG="true"
python3 odf-oc-compare.py
```

This shows:
- Connection details
- GUID extraction process
- CSI snapshot parent analysis
- Detailed error messages

## Want to contribute?
- Feel free to open a PR

## Found any problems?
- Open an Issue
