#!/usr/bin/env python3

"""
name:             odf-cleanup.py
description:      Deletes ODF objects based on LAB GUID using hierarchical tree approach
author:           yvarbev@redhat.com
version:          25.07.01
"""

import rbd
import rados
import time
import os
from datetime import datetime
from typing import List, Dict, Optional, Set
from enum import Enum


class ImageType(Enum):
    VOLUME = "volume"
    CSI_SNAP = "csi-snap"
    INTERNAL_SNAP = "internal-snap"
    TRASH_VOLUME = "trash-volume"
    TRASH_CSI_SNAP = "trash-csi-snap"


class OdfImage:
    """Represents an RBD image (volume or snapshot) in the ODF cluster"""
    def __init__(self, name: str, image_type: ImageType, size: Optional[int] = None,
                 creation_time: Optional[str] = None, parent_name: Optional[str] = None,
                 is_protected: bool = False, in_trash: bool = False, trash_id: Optional[str] = None):
        self.name = name
        self.image_type = image_type
        self.size = size
        self.creation_time = creation_time
        self.parent_name = parent_name
        self.children: List['OdfImage'] = []
        self.internal_snaps: List[str] = []
        self.is_protected = is_protected
        self.in_trash = in_trash
        self.trash_id = trash_id
        
        # Multi-phase operation metadata
        self.needs_restoration = False
        self.needs_flattening = False
        self.restoration_reason: Optional[str] = None
        self.depends_on_trash = False
    
    def add_child(self, child: 'OdfImage'):
        """Add a child image"""
        if child not in self.children:
            self.children.append(child)
    
    def has_descendants(self) -> bool:
        """Check if image has any descendants"""
        return len(self.children) > 0 or len(self.internal_snaps) > 0
    
    def get_all_descendants(self) -> List['OdfImage']:
        """Get all descendants recursively"""
        descendants = []
        for child in self.children:
            descendants.append(child)
            descendants.extend(child.get_all_descendants())
        return descendants


class OdfTree:
    """Manages the hierarchical tree of ODF RBD images"""
    
    def __init__(self):
        self.images: Dict[str, OdfImage] = {}
        self.root_images: List[OdfImage] = []
    
    def add_image(self, image: OdfImage):
        """Add an image to the tree"""
        self.images[image.name] = image
        
        # If image has a parent, establish the relationship
        if image.parent_name and image.parent_name in self.images:
            parent = self.images[image.parent_name]
            parent.add_child(image)
        elif not image.parent_name:
            # This is a root image
            self.root_images.append(image)
    
    def build_relationships(self):
        """Build parent-child relationships after all images are added"""
        for image in self.images.values():
            if image.parent_name and image.parent_name in self.images:
                parent = self.images[image.parent_name]
                parent.add_child(image)
            elif not image.parent_name and image not in self.root_images:
                self.root_images.append(image)
    
    def get_removal_order(self) -> List[OdfImage]:
        """Calculate the order in which images should be removed (children first)"""
        removal_order = []
        visited = set()
        
        def visit_image(image: OdfImage):
            if image.name in visited:
                return
            
            visited.add(image.name)
            
            # Visit children first (depth-first, post-order)
            for child in image.children:
                visit_image(child)
            
            # Add current image after its children
            removal_order.append(image)
        
        # Start with root images
        for root in self.root_images:
            visit_image(root)
        
        return removal_order
    
    def display_tree(self, show_details: bool = True):
        """Display the tree structure"""
        print("\n" + "="*80)
        print("ODF RBD IMAGE HIERARCHY")
        print("="*80)
        
        if not self.root_images:
            print("No images found for the specified LAB GUID")
            return
        
        for root in self.root_images:
            self._display_image(root, "", True, show_details)
        
        print("="*80)
    
    def _display_image(self, image: OdfImage, prefix: str, is_last: bool, show_details: bool):
        """Recursively display an image and its children"""
        # Tree structure symbols
        current_prefix = "└── " if is_last else "├── "
        print(f"{prefix}{current_prefix}{image.name}")
        
        if show_details:
            detail_prefix = prefix + ("    " if is_last else "│   ")
            print(f"{detail_prefix}    Type: {image.image_type.value}")
            if image.size:
                print(f"{detail_prefix}    Size: {self._format_size(image.size)}")
            if image.creation_time and image.creation_time != "Unknown":
                print(f"{detail_prefix}    Created: {image.creation_time}")
            elif image.creation_time == "Unknown":
                print(f"{detail_prefix}    Created: Unknown (metadata unavailable)")
            if image.parent_name:
                print(f"{detail_prefix}    Parent: {image.parent_name}")
            if image.in_trash:
                print(f"{detail_prefix}    Status: IN TRASH (ID: {image.trash_id})")
            if image.internal_snaps:
                snap_status = "protected" if image.is_protected else "unprotected"
                print(f"{detail_prefix}    Internal Snaps: {len(image.internal_snaps)} ({snap_status})")
            if image.is_protected and not image.internal_snaps:
                print(f"{detail_prefix}    Protected: YES")
            
            # Show multi-phase operation requirements
            if image.depends_on_trash:
                print(f"{detail_prefix}    WARNING: Depends on trash items")
            if image.needs_restoration:
                print(f"{detail_prefix}    RESTORE: Needs restoration: {image.restoration_reason}")
            if image.needs_flattening:
                print(f"{detail_prefix}    FLATTEN: Needs flattening to remove dependencies")
        
        # Display children
        child_prefix = prefix + ("    " if is_last else "│   ")
        for i, child in enumerate(image.children):
            is_last_child = i == len(image.children) - 1
            self._display_image(child, child_prefix, is_last_child, show_details)
    
    def _format_size(self, size_bytes: int) -> str:
        """Format size in human readable format"""
        size = float(size_bytes)
        for unit in ['B', 'KB', 'MB', 'GB', 'TB']:
            if size < 1024.0:
                return f"{size:.2f} {unit}"
            size /= 1024.0
        return f"{size:.2f} PB"


class OdfCleaner:
    """Main class for ODF cleanup operations"""
    
    def __init__(self, dry_run: bool = True):
        self.dry_run = dry_run
        self.ioctx = None
        self.lab_guid = None
        self.pool_name = None
        self.tree = OdfTree()
        self.removal_stats = {
            'images_removed': 0,
            'csi_snaps_removed': 0,
            'internal_snaps_removed': 0,
            'trash_items_removed': 0,
            'failed_removals': []
        }
        # Cache for dependency analysis
        self._active_to_trash_dependencies = None
        # Track failed trash restorations
        self._failed_trash_restorations = set()
    
    def _clear_dependency_cache(self):
        """Clear cached dependency analysis"""
        self._active_to_trash_dependencies = None
        self._failed_trash_restorations = set()
    
    def connect(self):
        """Connect to ODF cluster"""
        try:
            self.pool_name = os.environ['CL_POOL']
            conf_file = os.environ['CL_CONF']
            keyring = os.environ['CL_KEYRING']
            self.lab_guid = os.environ['CL_LAB']
            
            cluster = rados.Rados(conffile=conf_file, conf=dict(keyring=keyring))
            print(f"Connecting to ODF cluster...")
            print(f"librados version: {cluster.version()}")
            print(f"Monitor hosts: {cluster.conf_get('mon host')}")
            
            cluster.connect()
            print(f"Connected! Cluster ID: {cluster.get_fsid()}")
            print(f"Pool: {self.pool_name}")
            print(f"LAB GUID: {self.lab_guid}")
            
            self.ioctx = cluster.open_ioctx(self.pool_name)
            return True
            
        except KeyError as e:
            print(f"Error: Missing environment variable {e}")
            return False
        except Exception as e:
            print(f"Error connecting to cluster: {e}")
            return False
    
    def discover_images(self):
        """Discover all images, csi-snaps, and trash items related to LAB GUID"""
        print(f"\nDiscovering RBD images for LAB GUID: {self.lab_guid}")
        print("-" * 50)
        
        # Clear cache for fresh analysis
        self._clear_dependency_cache()
        
        # Phase 1: Find images in pool containing LAB GUID
        pool_images = self._find_pool_images()
        print(f"Found {len(pool_images)} images in pool")
        
        # Phase 2: Find images in trash containing LAB GUID
        trash_images = self._find_trash_images()
        print(f"Found {len(trash_images)} images in trash")
        
        # Phase 3: Find csi-snaps related to LAB GUID
        csi_snaps = self._find_related_csi_snaps()
        print(f"Found {len(csi_snaps)} related csi-snaps")
        
        # Phase 4: Analyze dependencies (run once, cache results)
        print(f"\n  Analyzing active->trash dependencies...")
        self._active_to_trash_dependencies = self._find_active_to_trash_dependencies()
        
        # Phase 5: Find csi-snaps in trash (using cached dependency analysis)
        trash_csi_snaps = self._find_trash_csi_snaps()
        print(f"Found {len(trash_csi_snaps)} csi-snaps in trash")
        
        # Phase 6: Create restoration plan (using cached dependency analysis)
        self._analyze_dependencies_and_plan()
        
        # Build tree images
        all_discovered = pool_images + trash_images + csi_snaps + trash_csi_snaps
        print(f"\nTotal images discovered: {len(all_discovered)}")
        
        return all_discovered
    
    def _analyze_dependencies_and_plan(self):
        """Create restoration plan using cached dependency analysis"""
        
        # Use cached dependency analysis
        dependencies = self._active_to_trash_dependencies or {}
        
        if dependencies:
            print(f"\n  WARNING: COMPLEX DEPENDENCIES DETECTED:")
            print(f"  Found {len(dependencies)} active images with trash dependencies")
            
            # Generate restoration plan
            restoration_plan = self._get_restoration_plan(dependencies)
            
            print(f"\n  RESTORATION PLAN REQUIRED:")
            print(f"  The following operations will be needed:")
            
            for i, step in enumerate(restoration_plan, 1):
                print(f"    {i:2d}. {step['action'].upper()}: {step['target']}")
                print(f"        Reason: {step['reason']}")
            
            print(f"\n  WARNING: Multi-phase cleanup required!")
            print(f"  Some images must be restored, flattened, then deleted.")
            
        else:
            print(f"  SUCCESS: No active->trash dependencies found")
            print(f"  Simple cleanup order can be used")
    
    def _find_pool_images(self) -> List[OdfImage]:
        """Find images in pool containing LAB GUID"""
        images = []
        try:
            all_rbd_images = rbd.RBD().list(self.ioctx)
            lab_rbd_images = [img for img in all_rbd_images if self.lab_guid in img]
            
            for img_name in lab_rbd_images:
                if 'csi-snap' not in img_name:  # Regular images, not csi-snaps
                    image = self._create_image_from_rbd(img_name, ImageType.VOLUME)
                    if image:
                        images.append(image)
                        
        except Exception as e:
            print(f"Error finding pool images: {e}")
        
        return images
    
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
                        
        except Exception as e:
            print(f"Error finding trash images: {e}")
        
        return images
    
    def _find_related_csi_snaps(self) -> List[OdfImage]:
        """Find csi-snaps whose parents contain LAB GUID"""
        csi_snaps = []
        try:
            all_rbd_images = rbd.RBD().list(self.ioctx)
            csi_rbd_images = [img for img in all_rbd_images if 'csi-snap' in img]
            
            for csi_name in csi_rbd_images:
                try:
                    with rbd.Image(self.ioctx, csi_name) as img:
                        parent_info = img.parent_info()
                        if parent_info and self.lab_guid in parent_info[1]:
                            image = self._create_image_from_rbd(csi_name, ImageType.CSI_SNAP)
                            if image:
                                image.parent_name = parent_info[1]
                                csi_snaps.append(image)
                except:
                    # No parent or other issue
                    pass
                    
        except Exception as e:
            print(f"Error finding csi-snaps: {e}")
        
        return csi_snaps
    
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
                    image = self._create_trash_image(item, ImageType.TRASH_CSI_SNAP)
                    if image:
                        csi_snaps.append(image)
                        print(f"    Included trash csi-snap: {item['name']} (referenced by active images)")
                else:
                    print(f"    Skipped trash csi-snap: {item['name']} (no active dependencies)")
                    
        except Exception as e:
            print(f"Error finding trash csi-snaps: {e}")
        
        return csi_snaps
    
    def _get_lab_image_names(self) -> Set[str]:
        """Get all image names related to this LAB for correlation"""
        lab_images = set()
        try:
            # Active images
            all_rbd_images = rbd.RBD().list(self.ioctx)
            lab_images.update([img for img in all_rbd_images if self.lab_guid in img and 'csi-snap' not in img])
            
            # Trash images
            trash_items = rbd.RBD().trash_list(self.ioctx)
            lab_images.update([item['name'] for item in trash_items if self.lab_guid in item['name'] and 'csi-snap' not in item['name']])
            
        except Exception as e:
            print(f"Warning: Could not get lab image names for correlation: {e}")
        
        return lab_images
    
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
                                if img_name not in dependencies:
                                    dependencies[img_name] = []
                                dependencies[img_name].append(parent_image)
                                print(f"      Found dependency: {img_name} -> {parent_image} (in trash)")
                                
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
                                    
                except Exception as img_err:
                    print(f"      Warning: Could not check dependencies for {img_name}: {img_err}")
                    
        except Exception as e:
            print(f"Warning: Could not analyze active->trash dependencies: {e}")
        
        print(f"    Found {len(dependencies)} active images with trash dependencies")
        return dependencies
    
    def _is_image_in_trash(self, image_name: str) -> bool:
        """Check if an image is currently in trash"""
        try:
            trash_items = rbd.RBD().trash_list(self.ioctx)
            trash_names = [item['name'] for item in trash_items]
            return image_name in trash_names
        except Exception as e:
            print(f"Warning: Could not check trash status for {image_name}: {e}")
            return False
    
    def _is_trash_item_referenced(self, trash_item: dict, active_dependencies: Dict[str, List[str]]) -> bool:
        """Check if a trash item is referenced by any active LAB images"""
        trash_name = trash_item['name']
        
        # Check if this trash item appears in any dependency list
        for active_image, trash_parents in active_dependencies.items():
            if trash_name in trash_parents:
                print(f"      Trash item {trash_name} is referenced by active image {active_image}")
                return True
                
        return False
    
    def _get_restoration_plan(self, dependencies: Dict[str, List[str]]) -> List[Dict]:
        """Generate a plan for restoring trash items, flattening, and cleanup"""
        plan = []
        
        for active_image, trash_parents in dependencies.items():
            for trash_parent in trash_parents:
                plan.append({
                    'action': 'restore',
                    'target': trash_parent,
                    'reason': f'needed by active image {active_image}'
                })
                plan.append({
                    'action': 'flatten',
                    'target': active_image,
                    'reason': f'remove dependency on {trash_parent}'
                })
                plan.append({
                    'action': 'delete',
                    'target': trash_parent,
                    'reason': f'cleanup after flattening {active_image}'
                })
        
        return plan
    
    def _create_image_from_rbd(self, img_name: str, image_type: ImageType) -> Optional[OdfImage]:
        """Create an OdfImage from an RBD image"""
        try:
            with rbd.Image(self.ioctx, img_name) as img:
                # Get image info
                stat = img.stat()
                
                # Get creation time if available
                creation_time = None
                try:
                    timestamp = stat.get('timestamp', 0)
                    # Check if timestamp is valid (not epoch 0)
                    if timestamp and timestamp > 0:
                        creation_time = str(datetime.fromtimestamp(timestamp))
                    else:
                        creation_time = "Unknown"
                except Exception as ts_err:
                    creation_time = "Unknown"
                
                # Get parent info
                parent_name = None
                try:
                    parent_info = img.parent_info()
                    if parent_info:
                        parent_name = parent_info[1]
                except:
                    pass
                
                # Get internal snapshots
                internal_snaps = [snap['name'] for snap in img.list_snaps()]
                
                # Check if any snapshots are protected
                is_protected = False
                try:
                    for snap in img.list_snaps():
                        try:
                            if img.is_protected_snap(snap['name']):
                                is_protected = True
                                break
                        except Exception as snap_err:
                            print(f"    Warning: Could not check protection for snapshot {snap['name']}: {snap_err}")
                except Exception as snap_list_err:
                    print(f"    Warning: Could not list snapshots for {img_name}: {snap_list_err}")
                
                image = OdfImage(
                    name=img_name,
                    image_type=image_type,
                    size=stat['size'],
                    creation_time=creation_time,
                    parent_name=parent_name,
                    is_protected=is_protected
                )
                image.internal_snaps = internal_snaps
                
                return image
                
        except Exception as e:
            print(f"Error creating image for {img_name}: {e}")
            return None
    
    def _create_trash_image(self, trash_item: dict, image_type: ImageType) -> Optional[OdfImage]:
        """Create an OdfImage from a trash item"""
        try:
            # Handle deferment_end_time - could be timestamp or datetime object
            creation_time = None
            defer_time = trash_item.get('deferment_end_time', 0)
            if defer_time:
                if isinstance(defer_time, datetime):
                    creation_time = str(defer_time)
                else:
                    creation_time = str(datetime.fromtimestamp(defer_time))
            
            image = OdfImage(
                name=trash_item['name'],
                image_type=image_type,
                in_trash=True,
                trash_id=trash_item['id'],
                creation_time=creation_time
            )
            return image
            
        except Exception as e:
            print(f"Error creating trash image for {trash_item['name']}: {e}")
            return None
    
    def build_tree(self, discovered_items: List[OdfImage], debug: bool = False):
        """Build the hierarchical tree from discovered items"""
        print(f"\nBuilding hierarchical tree...")
        
        if debug:
            # Debug: Show discovered items
            print("Discovered items:")
            for item in discovered_items:
                parent_info = f" (parent: {item.parent_name})" if item.parent_name else " (no parent)"
                print(f"  - {item.name} [{item.image_type.value}]{parent_info}")
        
        # Add all images to tree
        for image in discovered_items:
            self.tree.add_image(image)
        
        # Build relationships
        self.tree.build_relationships()
        
        if debug:
            # Debug: Show what ended up in the tree
            print(f"Tree contents:")
            print(f"  All images: {list(self.tree.images.keys())}")
            print(f"  Root images: {[img.name for img in self.tree.root_images]}")
        
        print(f"Tree built with {len(self.tree.images)} images and {len(self.tree.root_images)} root images")
    
    def plan_removal(self) -> List[OdfImage]:
        """Plan the removal order"""
        print(f"\nPlanning removal order...")
        removal_order = self.tree.get_removal_order()
        
        print("Planned removal order:")
        for i, image in enumerate(removal_order, 1):
            status = "TRASH" if image.in_trash else "ACTIVE"
            print(f"  {i:2d}. {image.name} ({image.image_type.value}) [{status}]")
        
        return removal_order
    
    def execute_cleanup(self, removal_order: List[OdfImage]):
        """Execute the cleanup process"""
        if self.dry_run:
            print(f"\n{'='*80}")
            print("DRY RUN MODE - NO ACTUAL DELETION WILL OCCUR")
            print("="*80)
            
            print(f"\nDry run cleanup simulation for {len(removal_order)} items...")
            
            for i, image in enumerate(removal_order, 1):
                print(f"\n[{i}/{len(removal_order)}] Processing: {image.name}")
                print(f"  DRY RUN: Would remove {image.image_type.value}")
                if image.internal_snaps:
                    print(f"  DRY RUN: Would remove {len(image.internal_snaps)} internal snapshots")
                if image.in_trash:
                    print(f"  DRY RUN: Would restore from trash first")
                if image.needs_flattening:
                    print(f"  DRY RUN: Would flatten to remove dependencies")
        else:
            print(f"\n{'='*80}")
            print("LIVE MODE - ACTUAL DELETION WILL OCCUR")
            print("="*80)
            
            # Check for multi-phase operations
            if self._active_to_trash_dependencies:
                print(f"\nWARNING: Multi-phase operations detected!")
                print(f"Some images will be restored, flattened, then deleted.")
                print(f"This process may take additional time.")
            
            print(f"\nAbout to delete {len(removal_order)} RBD images for LAB GUID: {self.lab_guid}")
            print(f"Pool: {self.pool_name}")
            
            print(f"\nExecuting cleanup for {len(removal_order)} items...")
            
            for i, image in enumerate(removal_order, 1):
                print(f"\n[{i}/{len(removal_order)}] Processing: {image.name}")
                
                # Mark images that need flattening based on dependencies
                if self._needs_flattening_for_dependencies(image):
                    image.needs_flattening = True
                    image.restoration_reason = "Remove dependencies before deletion"
                
                # Attempt removal
                success = self._remove_image(image)
                if success:
                    self._update_removal_stats(image)
                    print(f"  SUCCESS: Removed {image.name}")
                else:
                    self.removal_stats['failed_removals'].append(image.name)
                    print(f"  FAILED: Could not remove {image.name}")
                
                # Brief pause between operations
                time.sleep(3)
        
        self._generate_report()
    
    def _needs_flattening_for_dependencies(self, image: OdfImage) -> bool:
        """Check if image needs flattening based on dependency analysis"""
        if not self._active_to_trash_dependencies:
            return False
        
        # Check if this image is mentioned in dependency analysis
        for active_image, trash_parents in self._active_to_trash_dependencies.items():
            if image.name == active_image:
                return True  # This active image depends on trash items
            if image.name in trash_parents:
                return False  # This is a trash item that will be restored
        
        return False
    
    def _needs_fallback_flattening(self, image: OdfImage) -> bool:
        """Check if image needs flattening due to failed trash restorations"""
        if not self._active_to_trash_dependencies or not self._failed_trash_restorations:
            return False
        
        # Check if this active image depends on any failed trash restorations
        for active_image, trash_parents in self._active_to_trash_dependencies.items():
            if image.name == active_image:
                # Check if any of its trash parents failed to restore
                failed_parents = set(trash_parents) & self._failed_trash_restorations
                if failed_parents:
                    print(f"    Fallback flattening needed: depends on failed trash items {failed_parents}")
                    return True
        
        return False
    
    def _remove_image(self, image: OdfImage) -> bool:
        """Remove a single RBD image with proper handling"""
        print(f"  Removing {image.image_type.value}: {image.name}")
        
        try:
            # Handle trash items first - restore them temporarily
            if image.in_trash:
                if not self._restore_from_trash(image):
                    # Failed to restore - skip this trash item but don't fail overall cleanup
                    print(f"  SKIPPED: Could not restore {image.name}, leaving in trash")
                    self._failed_trash_restorations.add(image.name)
                    return True  # Consider this "successful" to continue cleanup
                # After restoration, treat as active image for deletion
            
            # Handle multi-phase operations or fallback flattening
            if image.needs_flattening or self._needs_fallback_flattening(image):
                if not self._flatten_image(image):
                    return False
            
            # Remove the active image
            return self._remove_active_image(image)
            
        except Exception as e:
            print(f"  ERROR: Failed to remove {image.name}: {e}")
            return False
    
    def _restore_from_trash(self, image: OdfImage) -> bool:
        """Restore an image from trash temporarily for deletion"""
        print(f"    Restoring from trash: {image.name} (ID: {image.trash_id})")
        
        try:
            # Restore image from trash
            rbd.RBD().trash_restore(self.ioctx, image.trash_id, image.name)
            print(f"    Successfully restored: {image.name}")
            return True
            
        except Exception as e:
            print(f"    ERROR: Failed to restore {image.name}: {e}")
            print(f"    This trash item will be skipped, but dependent active images will be flattened")
            return False
    
    def _flatten_image(self, image: OdfImage) -> bool:
        """Flatten an image to remove parent dependencies"""
        print(f"    Flattening image: {image.name}")
        
        try:
            with rbd.Image(self.ioctx, image.name) as img:
                # Check if image actually needs flattening
                try:
                    parent_info = img.parent_info()
                    if not parent_info:
                        print(f"    Image {image.name} has no parent, skipping flatten")
                        return True
                except:
                    # No parent, nothing to flatten
                    print(f"    Image {image.name} has no parent, skipping flatten")
                    return True
                
                # Perform flattening
                img.flatten()
                print(f"    Flattening initiated for: {image.name}")
                
                # Wait for flatten to complete
                self._wait_for_flatten_completion(img, image.name)
                print(f"    Successfully flattened: {image.name}")
                return True
                
        except Exception as e:
            print(f"    ERROR: Failed to flatten {image.name}: {e}")
            return False
    
    def _wait_for_flatten_completion(self, img, img_name: str, max_wait: int = 300):
        """Wait for flatten operation to complete"""
        print(f"    Waiting for flatten completion...")
        
        start_time = time.time()
        while time.time() - start_time < max_wait:
            try:
                # Check if still has parent
                parent_info = img.parent_info()
                if not parent_info:
                    print(f"    Flatten completed for: {img_name}")
                    return True
            except:
                # No parent info means flatten completed
                print(f"    Flatten completed for: {img_name}")
                return True
            
            print(f"    Still flattening... ({int(time.time() - start_time)}s)")
            time.sleep(10)
        
        print(f"    WARNING: Flatten may still be in progress after {max_wait}s")
        return True  # Continue anyway
    
    def _remove_active_image(self, image: OdfImage) -> bool:
        """Remove an active RBD image (volumes, csi-snaps)"""
        try:
            with rbd.Image(self.ioctx, image.name) as img:
                # Get current state
                descendants = list(img.list_descendants())
                active_descendants = [d for d in descendants if not d.get('trash', False)]
                
                if active_descendants:
                    print(f"    ERROR: Image {image.name} still has {len(active_descendants)} active descendants")
                    print(f"    Descendants: {[d.get('name', 'unknown') for d in active_descendants]}")
                    return False
                
                # Remove internal snapshots first
                if not self._remove_internal_snapshots(img, image.name):
                    return False
                
                # Try to flatten if needed (safety check)
                try:
                    img.flatten()
                    print(f"    Final flatten for: {image.name}")
                    time.sleep(5)  # Brief wait
                except:
                    pass  # Already flat or no parent
            
            # Remove the image itself
            print(f"    Deleting image: {image.name}")
            rbd.RBD().remove(self.ioctx, image.name)
            print(f"    Successfully deleted: {image.name}")
            return True
            
        except Exception as e:
            print(f"    ERROR: Failed to delete {image.name}: {e}")
            return False
    
    def _remove_internal_snapshots(self, img, img_name: str) -> bool:
        """Remove all internal snapshots from an image"""
        try:
            snapshots = list(img.list_snaps())
            if not snapshots:
                print(f"    No internal snapshots to remove")
                return True
            
            print(f"    Removing {len(snapshots)} internal snapshots...")
            
            for snap in snapshots:
                snap_name = snap['name']
                print(f"      Removing snapshot: {snap_name}")
                
                try:
                    # Unprotect if protected
                    if img.is_protected_snap(snap_name):
                        print(f"        Unprotecting snapshot: {snap_name}")
                        img.unprotect_snap(snap_name)
                    
                    # Remove snapshot
                    img.remove_snap(snap_name)
                    print(f"        Successfully removed snapshot: {snap_name}")
                    time.sleep(2)  # Brief pause between snapshots
                    
                except Exception as snap_err:
                    print(f"        ERROR: Failed to remove snapshot {snap_name}: {snap_err}")
                    return False
            
            print(f"    All internal snapshots removed from: {img_name}")
            return True
            
        except Exception as e:
            print(f"    ERROR: Failed to process snapshots for {img_name}: {e}")
            return False
    
    def _update_removal_stats(self, image: OdfImage):
        """Update removal statistics"""
        if image.image_type == ImageType.VOLUME:
            self.removal_stats['images_removed'] += 1
        elif image.image_type in [ImageType.CSI_SNAP, ImageType.TRASH_CSI_SNAP]:
            self.removal_stats['csi_snaps_removed'] += 1
        elif image.image_type == ImageType.TRASH_VOLUME:
            self.removal_stats['trash_items_removed'] += 1
        
        self.removal_stats['internal_snaps_removed'] += len(image.internal_snaps)
    
    def _generate_report(self):
        """Generate cleanup report"""
        print(f"\n{'='*80}")
        print("CLEANUP REPORT")
        print("="*80)
        print(f"LAB GUID: {self.lab_guid}")
        print(f"Pool: {self.pool_name}")
        print(f"Dry Run: {'YES' if self.dry_run else 'NO'}")
        print(f"Timestamp: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print("-" * 80)
        print(f"Images removed: {self.removal_stats['images_removed']}")
        print(f"CSI-snaps removed: {self.removal_stats['csi_snaps_removed']}")
        print(f"Internal snaps removed: {self.removal_stats['internal_snaps_removed']}")
        print(f"Trash items removed: {self.removal_stats['trash_items_removed']}")
        print(f"Failed removals: {len(self.removal_stats['failed_removals'])}")
        print(f"Failed trash restorations: {len(self._failed_trash_restorations)}")
        
        if self.removal_stats['failed_removals']:
            print("\nFailed removals:")
            for item in self.removal_stats['failed_removals']:
                print(f"  - {item}")
        
        if self._failed_trash_restorations:
            print("\nFailed trash restorations (left in trash):")
            for item in self._failed_trash_restorations:
                print(f"  - {item}")
        
        print("="*80)
    
    def cleanup(self):
        """Main cleanup orchestration"""
        if not self.connect():
            return False
        
        try:
            # Discovery phase
            discovered_items = self.discover_images()
            if not discovered_items:
                print("No items found for cleanup")
                return True
            
            # Tree building phase
            debug_mode = self.dry_run or os.environ.get('DEBUG', 'false').lower() in ['true', '1', 'yes']
            self.build_tree(discovered_items, debug=debug_mode)
            
            # Display tree
            self.tree.display_tree()
            
            # Planning phase
            removal_order = self.plan_removal()
            
            # Execution phase
            self.execute_cleanup(removal_order)
            
            return True
            
        except Exception as e:
            print(f"Error during cleanup: {e}")
            return False
        finally:
            if self.ioctx:
                self.ioctx.close()


def main():
    """Main entry point"""
    print("ODF Cleanup")
    print("=" * 80)
    
    # Check environment variables
    required_envs = ['CL_LAB', 'CL_POOL', 'CL_CONF', 'CL_KEYRING']
    missing_envs = [env for env in required_envs if env not in os.environ]
    
    if missing_envs:
        print(f"Error: Missing environment variables: {', '.join(missing_envs)}")
        print("\nRequired environment variables:")
        for env in required_envs:
            print(f"  {env}")
        print("\nOptional environment variables:")
        print("  DRY_RUN=[true/false]     - Enable dry-run mode (default: true)")
        print("  DEBUG=[true/false]       - Enable debug output (default: false)")
        return 1
    
    # Check for dry run mode
    dry_run = os.environ.get('DRY_RUN', 'true').lower() in ['true', '1', 'yes']
    
    # Show current configuration
    print(f"Configuration:")
    print(f"  LAB GUID: {os.environ['CL_LAB']}")
    print(f"  Pool: {os.environ['CL_POOL']}")
    print(f"  Dry Run: {'YES' if dry_run else 'NO'}")
    print(f"  Debug: {os.environ.get('DEBUG', 'false').upper()}")
    
    if not dry_run:
        print(f"\nWARNING: LIVE MODE ENABLED - ACTUAL DELETION WILL OCCUR!")
    
    cleaner = OdfCleaner(dry_run=dry_run)
    success = cleaner.cleanup()
    
    return 0 if success else 1


if __name__ == "__main__":
    exit(main()) 