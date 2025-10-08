from importlib import import_module
import inspect

def _pick_class(module_name, preferred_names, must_have_methods=()):
    """
    Try preferred class names first; if not found, scan the module for a class
    whose name endswith any preferred name and that implements the required methods.
    """
    mod = import_module(module_name)
    # try exact names first
    for name in preferred_names:
        if hasattr(mod, name):
            return getattr(mod, name)
    # fallback: scan for any matching class
    for _, obj in inspect.getmembers(mod, inspect.isclass):
        if any(obj.__name__.lower().endswith(n.lower()) for n in preferred_names):
            if all(hasattr(obj, m) for m in must_have_methods):
                return obj
    raise ImportError(f"No suitable class found in {module_name} matching {preferred_names}")

# ----- CatalogItems (required) -----
CatalogItems = None
# try modern module first, then legacy aggregate, then older alias
for module in ("sp_api.api.catalog_items", "sp_api.api", "sp_api.api.catalog"):
    try:
        CatalogItems = _pick_class(
            module,
            preferred_names=["CatalogItems", "CatalogItemsV20201201", "CatalogItemsV_2020_12_01"],
            must_have_methods=("get_catalog_item",)
        )
        break
    except Exception:
        continue
if CatalogItems is None:
    raise ImportError("Could not locate a usable CatalogItems class in sp_api")

__all__ = ["CatalogItems"]
