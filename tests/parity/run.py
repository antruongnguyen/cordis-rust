"""Compare six real Rust/TypeScript lifecycle traces; no third-party Python packages."""

import difflib
import os
import subprocess
import tempfile
from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]
FIXTURES = ROOT / "tests/parity"
UPSTREAM = ROOT / "local/cordis"
PIN = "35fdc969e4b6c48c48ed44717c7a018324e4dd88"


def run(command, *, cwd=ROOT, env=None):
    return subprocess.run(
        command, cwd=cwd, env=env, check=True, timeout=180, text=True
    )


def output(command, *, cwd=ROOT):
    return subprocess.check_output(command, cwd=cwd, text=True, timeout=30).strip()


def main():
    if output(["git", "rev-parse", "HEAD"], cwd=UPSTREAM) != PIN:
        raise SystemExit(f"TypeScript checkout must be pinned to {PIN}")
    run(["git", "diff", "--exit-code", "HEAD", "--", "."], cwd=UPSTREAM)
    tools = {
        name: output(["rustup", "which", "--toolchain", "stable", name])
        for name in ("rustc", "rustdoc", "cargo")
    }
    env = os.environ.copy()
    env.update(
        PATH=str(Path(tools["rustc"]).parent) + os.pathsep + env["PATH"],
        RUSTC=tools["rustc"],
        RUSTDOC=tools["rustdoc"],
        CARGO_TARGET_DIR=str(ROOT / "target/stable"),
        CORDIS_PARITY_FIXTURES=str(FIXTURES),
    )
    for tool in tools.values():
        run([tool, "--version"], env=env)
    run(["node", "--version"], cwd=UPSTREAM, env=env)
    run(["corepack", "yarn", "--version"], cwd=UPSTREAM, env=env)
    (ROOT / "target").mkdir(exist_ok=True)
    with tempfile.TemporaryDirectory(prefix="lifecycle-parity-", dir=ROOT / "target") as work:
        work = Path(work)
        rust = work / "rust"
        typescript = work / "typescript"
        rust.mkdir()
        typescript.mkdir()
        destination = UPSTREAM / "packages/core/tests/cordis-rust-lifecycle-parity.spec.ts"
        with destination.open("x") as target:
            target.write((FIXTURES / "lifecycle.spec.ts").read_text())
        try:
            run(
                [
                    "corepack", "yarn", "eslint", "--no-ignore", "--max-warnings", "0",
                    "packages/core/tests/cordis-rust-lifecycle-parity.spec.ts",
                ],
                cwd=UPSTREAM,
                env=env,
            )
            run(
                [
                    "corepack", "yarn", "tsc", "--noEmit", "--skipLibCheck",
                    "--target", "es2024", "--module", "nodenext",
                    "--allowImportingTsExtensions", "--strict",
                    "--noImplicitAny", "false", "--noImplicitThis", "false",
                    "--strictFunctionTypes", "false",
                    "packages/core/tests/cordis-rust-lifecycle-parity.spec.ts",
                ],
                cwd=UPSTREAM,
                env=env,
            )
            env["CORDIS_PARITY_OUTPUT"] = str(typescript)
            run(
                [
                    "corepack", "yarn", "test", "core/cordis-rust-lifecycle-parity",
                ],
                cwd=UPSTREAM,
                env=env,
            )
        finally:
            destination.unlink()
        env["CORDIS_PARITY_OUTPUT"] = str(rust)
        run(
            [
                tools["cargo"], "test", "-p", "cordis-core",
                "--test", "lifecycle_parity", "--locked",
            ],
            env=env,
        )
        for name in (
            "combined", "coalesced", "removed",
            "superseded-failure", "latest-failure", "explicit-recovery",
        ):
            left = (rust / f"{name}.txt").read_text()
            right = (typescript / f"{name}.txt").read_text()
            if left != right:
                raise SystemExit(
                    "".join(difflib.unified_diff(
                        left.splitlines(keepends=True), right.splitlines(keepends=True),
                        fromfile=f"Rust/{name}", tofile=f"TypeScript/{name}",
                    ))
                )
            print(f"{name}: Rust == TypeScript ({len(left.splitlines())} observations)")


if __name__ == "__main__":
    main()
