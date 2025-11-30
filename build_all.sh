#!/bin/bash

# 脚本名称: build_all.sh
# 功能: 跨平台构建 (macOS ARM64, Windows ARM64, Linux ARM64)
#
# 目标平台:
# 1. aarch64-apple-darwin (macOS ARM64 / Apple Silicon) - 本机可以直接构建
# 2. aarch64-pc-windows-msvc (Windows ARM64) - 注意：在非 Windows 上交叉编译 MSVC 非常困难
#    通常建议改用 aarch64-pc-windows-gnu，或者需要在 macOS 上接受限制/跳过
# 3. aarch64-unknown-linux-gnu (Linux ARM64) - 需要交叉编译工具链

set -e

APP_NAME="data-to-parquet"
OUTPUT_DIR="dist"

# 清理并创建输出目录
rm -rf $OUTPUT_DIR
mkdir -p $OUTPUT_DIR

# 获取当前宿主平台
HOST_TARGET=$(rustc -vV | sed -n 's|host: ||p')
echo "Current host: $HOST_TARGET"

# ==========================================
# 1. macOS ARM64 (aarch64-apple-darwin)
# ==========================================
TARGET_MAC="aarch64-apple-darwin"
echo ">>> Building for macOS ARM64 ($TARGET_MAC)..."

if [[ "$HOST_TARGET" == "aarch64-apple-darwin" ]]; then
    # 本机就是目标平台
    cargo build --release
    cp "target/release/$APP_NAME" "$OUTPUT_DIR/$APP_NAME-macos-arm64"
elif [[ "$HOST_TARGET" == "x86_64-apple-darwin" ]]; then
    # Intel Mac 构建 ARM Mac
    rustup target add "$TARGET_MAC"
    cargo build --release --target "$TARGET_MAC"
    cp "target/$TARGET_MAC/release/$APP_NAME" "$OUTPUT_DIR/$APP_NAME-macos-arm64"
else
    echo "Skipping macOS build (Not on macOS)"
fi

# ==========================================
# 2. Linux ARM64 (aarch64-unknown-linux-gnu)
# ==========================================
TARGET_LINUX="aarch64-unknown-linux-gnu"
echo ">>> Building for Linux ARM64 ($TARGET_LINUX)..."

if ! rustup target list --installed | grep -q "$TARGET_LINUX"; then
    echo "Installing target $TARGET_LINUX..."
    rustup target add "$TARGET_LINUX"
fi

# 检查 Linker: aarch64-unknown-linux-gnu-gcc
# macOS 安装: brew tap messense/macos-cross-toolchains && brew install aarch64-unknown-linux-gnu
if command -v aarch64-unknown-linux-gnu-gcc &> /dev/null; then
    export CARGO_TARGET_AARCH64_UNKNOWN_LINUX_GNU_LINKER=aarch64-unknown-linux-gnu-gcc
    
    cargo build --release --target "$TARGET_LINUX"
    cp "target/$TARGET_LINUX/release/$APP_NAME" "$OUTPUT_DIR/$APP_NAME-linux-arm64"
else
    echo "Warning: Linker 'aarch64-unknown-linux-gnu-gcc' not found. Skipping Linux ARM64 build."
    echo "  To install on macOS: brew tap messense/macos-cross-toolchains && brew install aarch64-unknown-linux-gnu"
fi

# ==========================================
# 3. Windows ARM64 (aarch64-pc-windows-msvc)
# ==========================================
TARGET_WIN="aarch64-pc-windows-msvc"
echo ">>> Checking Windows ARM64 ($TARGET_WIN)..."

# 说明: 
# 在 macOS/Linux 上交叉编译 MSVC 目标 (pc-windows-msvc) 非常困难，
# 因为它依赖 Microsoft 的闭源库和 Linker，通常不能直接通过 cargo build 完成，
# 除非你使用了 cargo-xwin 这样的工具，或者手动提取了 MSVC SDK。
# 
# 相比之下，GNU 目标 (aarch64-pc-windows-gnu) 比较容易（使用 MinGW），但目前 MinGW 对 ARM64 Windows 的支持可能还在实验阶段或较少见。
#
# 这里的策略是：
# 1. 尝试检查是否有 cargo-xwin (https://github.com/rust-cross/cargo-xwin)
# 2. 如果没有，提示用户并跳过。

if ! rustup target list --installed | grep -q "$TARGET_WIN"; then
    echo "Installing target $TARGET_WIN..."
    rustup target add "$TARGET_WIN"
fi

if command -v cargo-xwin &> /dev/null; then
    echo "Found cargo-xwin, attempting build..."
    # cargo xwin build --release --target aarch64-pc-windows-msvc
    # 注意: cargo-xwin 会自动处理 sysroot
    cargo xwin build --release --target "$TARGET_WIN"
    cp "target/$TARGET_WIN/release/$APP_NAME.exe" "$OUTPUT_DIR/$APP_NAME-windows-arm64.exe"
else
    echo "Warning: Cross-compiling to MSVC ($TARGET_WIN) on non-Windows requires 'cargo-xwin'."
    echo "  Please install: cargo install cargo-xwin"
    echo "  Alternatively, consider using 'aarch64-pc-windows-gnu' if MinGW supports it (experimental)."
    echo "Skipping Windows ARM64 build."
fi

echo "----------------------------------------"
echo "Build process finished. Artifacts in '$OUTPUT_DIR':"
ls -lh $OUTPUT_DIR
