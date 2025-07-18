# odf cleanup
Removes leftover odf images based on an identifier, referenced as LAB GUID

## Current Strategy

The script uses a **two-phase approach** with both **direct matching** and **dependency analysis**:

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

### Phase 2: Dependency Analysis

For **trash csi-snaps**:

```python
def _find_trash_csi_snaps(self) -> List[OdfImage]:
    """Find csi-snaps in trash that have active dependencies"""
    csi_snaps = []
    try:
        trash_items = rbd.RBD().trash_list(self.ioctx)
        csi_trash = [item for item in trash_items if 'csi-snap' in item['name']]
        
        print(f"  Found {len(csi_trash)} csi-snaps in trash, using cached dependency analysis...")
        
        # Use cached dependency analysis
        active_dependencies = self._active_to_trash_dependencies or {}
        
        for item in csi_trash:
            if self._is_trash_item_referenced(item, active_dependencies):
```

#### How Dependency Analysis Works:

1. **Find Active→Trash Dependencies**:
```python
def _find_active_to_trash_dependencies(self) -> Dict[str, List[str]]:
    """Find active images that depend on trash items"""
    dependencies = {}  # {active_image_name: [list_of_trash_parent_names]}
    
    try:
        # Get all active images related to this LAB
        all_rbd_images = rbd.RBD().list(self.ioctx)
        lab_active_images = [img for img in all_rbd_images if self.lab_guid in img]
        
        print(f"    Checking {len(lab_active_images)} active LAB images for trash dependencies...")
        
        for img_name in lab_active_images:
            try:
                with rbd.Image(self.ioctx, img_name) as img:
                    parent_info = img.parent_info()
                    if parent_info:
                        parent_pool, parent_image = parent_info
                        
                        # Check if parent is in trash
                        if self._is_image_in_trash(parent_image):
```

2. **Check Reverse Dependencies**:
```python
                    # Also check for clone dependencies
                    descendants = list(img.list_descendants())
                    for desc in descendants:
                        if desc.get('trash', False):
                            desc_name = desc.get('name', '')
                            if desc_name:
                                if img_name not in dependencies:
                                    dependencies[img_name] = []
                                dependencies[img_name].append(desc_name)
                                print(f"      Found reverse dependency: {img_name} <- {desc_name} (in trash)")
```

3. **Reference Check**:
```python
def _is_trash_item_referenced(self, trash_item: dict, active_dependencies: Dict[str, List[str]]) -> bool:
    """Check if a trash item is referenced by any active LAB images"""
    trash_name = trash_item['name']
    
    # Check if this trash item appears in any dependency list
    for active_image, trash_parents in active_dependencies.items():
        if trash_name in trash_parents:
            print(f"      Trash item {trash_name} is referenced by active image {active_image}")
            return True
            
    return False
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

## Requires
- python3.6
- ceph/odf tools 
- odf conf
- odf keyring with sufficient privileges

## How to
```console
git clone https://github.com/yvarbanov/odf-cleanup.git odf-cleanup
source env.sh
python3 odf-cleanup.py
```

## Want to contribute?
- Feel free to open a PR

## Found any problems?
- Open an Issue
