import shutil
from pathlib import Path
from hatchling.builders.hooks.plugin.interface import BuildHookInterface


class CustomBuildHook(BuildHookInterface):
    def initialize(self, version, build_data):
        root = Path(self.root)
        project_root = root.parent

        # 1. External dist dir (source of truth for local builds)
        dist_dir = project_root / "dist"

        # 2. Internal bin dir (destination in package)
        pkg_bin_dir = root / "src" / "data_to_parquet_bin" / "bin"
        pkg_bin_dir.mkdir(parents=True, exist_ok=True)
        (pkg_bin_dir / "__init__.py").touch()

        # Ensure force_include dict exists
        if "force_include" not in build_data:
            build_data["force_include"] = {}

        # Scenario A: Building from local source (dist dir exists)
        if dist_dir.exists():
            print(f"[CustomBuildHook] Found external dist: {dist_dir}")
            count = 0
            for item in dist_dir.iterdir():
                if item.is_file() and (item.name.startswith("data-to-parquet-")):
                    print(f"  - Copying {item.name}")
                    target_path = pkg_bin_dir / item.name
                    shutil.copy2(item, target_path)
                    try:
                        target_path.chmod(0o755)
                    except OSError:
                        pass
                    count += 1
            print(f"[CustomBuildHook] Copied {count} binaries from external dist.")

        # Scenario B: Building from sdist (external dist missing, but bins might be in src)
        else:
            print(
                f"[CustomBuildHook] External dist not found (likely building from sdist). Checking local bins..."
            )

        # Final Step: Force include ALL binaries found in pkg_bin_dir
        # This handles both Scenario A (just copied) and Scenario B (already present in sdist)
        bin_count = 0
        for item in pkg_bin_dir.iterdir():
            if item.is_file() and item.name.startswith("data-to-parquet-"):
                # Explicitly add to wheel
                # format: local_abs_path : path_in_wheel
                build_data["force_include"][
                    str(item)
                ] = f"src/data_to_parquet_bin/bin/{item.name}"
                bin_count += 1

        print(f"[CustomBuildHook] force_include set for {bin_count} binaries.")
