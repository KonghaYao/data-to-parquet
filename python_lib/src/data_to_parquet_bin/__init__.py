import asyncio
import os
import platform
import subprocess
from pathlib import Path
from typing import Optional


class DataToParquetError(Exception):
    """Base exception for data-to-parquet errors."""

    pass


def _get_binary_path() -> Path:
    """
    Resolve the path to the data-to-parquet binary for the current platform.
    """
    system = platform.system().lower()
    machine = platform.machine().lower()

    # Map platform to binary suffix
    # Note: Assuming binaries are packaged with the library in the 'bin' directory
    # The build process should ensure these binaries are present.

    base_path = Path(__file__).parent / "bin"

    if system == "darwin":
        if machine == "arm64":
            binary_name = "data-to-parquet-macos-arm64"
        elif machine == "x86_64":
            # For now, fallback to ARM64 binary (Rosetta 2 compatibility)
            binary_name = "data-to-parquet-macos-arm64"
        else:
            raise DataToParquetError(f"Unsupported architecture: {machine} on {system}")
    elif system == "linux":
        if machine == "aarch64":
            binary_name = "data-to-parquet-linux-arm64"
        elif machine == "x86_64":
            binary_name = "data-to-parquet-linux-x86_64"
        else:
            raise DataToParquetError(f"Unsupported architecture: {machine} on {system}")
    elif system == "windows":
        if machine == "aarch64" or machine == "arm64":  # Windows might report arm64
            binary_name = "data-to-parquet-windows-arm64.exe"
        elif machine == "x86_64" or machine == "amd64":
            binary_name = "data-to-parquet-windows-x86_64.exe"
        else:
            raise DataToParquetError(f"Unsupported architecture: {machine} on {system}")
    else:
        raise DataToParquetError(f"Unsupported operating system: {system}")

    binary_path = base_path / binary_name

    if not binary_path.exists():
        # During development, fallback to dist folder in root if not found in package
        # This is just a helper for local testing before proper packaging
        project_root_dist = Path(__file__).parent.parent.parent.parent / "dist"
        dev_path = project_root_dist / binary_name
        if dev_path.exists():
            return dev_path

        raise DataToParquetError(f"Binary not found at {binary_path}")

    return binary_path


async def convert_to_parquet(
    input_path: str,
    output_path: str,
    sheet_name: Optional[str] = None,
    sheet_index: Optional[int] = None,
    skip_rows: int = 0,
    batch_size: int = 5000,
) -> None:
    """
    Asynchronously convert an Excel file to Parquet using the underlying Rust binary.

    Args:
        input_path: Path to the input .xlsx or .xlsb file.
        output_path: Path where the output .parquet file will be saved.
        sheet_name: Optional name of the sheet to convert.
        sheet_index: Optional index of the sheet to convert (0-based).
        skip_rows: Number of rows to skip at the start of the sheet.
        batch_size: Number of rows to process in each batch.

    Raises:
        DataToParquetError: If the conversion fails or binary is missing.
    """

    binary_path = _get_binary_path()

    # Make binary executable on Linux/Mac if needed
    if platform.system().lower() != "windows":
        if not os.access(binary_path, os.X_OK):
            # Try to make it executable (might fail if installed in read-only location)
            try:
                os.chmod(binary_path, 0o755)
            except OSError:
                pass  # Hope it works anyway

    cmd = [
        str(binary_path),
        "--input",
        str(input_path),
        "--output",
        str(output_path),
        "--skip-rows",
        str(skip_rows),
        "--batch-size",
        str(batch_size),
    ]

    if sheet_name:
        cmd.extend(["--sheet-name", sheet_name])

    if sheet_index is not None:
        cmd.extend(["--sheet-index", str(sheet_index)])

    process = await asyncio.create_subprocess_exec(
        *cmd, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE
    )

    stdout, stderr = await process.communicate()

    if process.returncode != 0:
        error_msg = stderr.decode().strip()
        raise DataToParquetError(
            f"Conversion failed (exit code {process.returncode}): {error_msg}"
        )
