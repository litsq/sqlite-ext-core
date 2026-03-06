# sqlite-ext-core

`sqlite-ext-core` provides a one-stop-shop for building high-performance, memory-safe SQLite extensions in Rust. It eliminates FFI boilerplate by providing a central registry for per-database state, a consolidated process-wide initialization pattern, and over 20+ C-mirror wrappers that make Rust extension code as clean as (or cleaner than) C.

## Key Features

- **Zero-Boilerplate FFI**: All necessary types (`sqlite3`, `sqlite3_context`, etc.) and constants are built-in. No need for `libsqlite3-sys` or manual `extern "C"` blocks.
- **Consolidated Initialization**: Use `sqlite3_extension_init2(p_api)` once to resolve all standard SQLite function pointers across your entire process.
- **C-Mirror Wrappers**: Zero-cost inline functions like `sqlite3_result_int64` and `sqlite3_user_data` that perfectly mirror the standard C API.
- **Deterministic RAII Cleanup**: State is anchored to the connection's lifetime and dropped the **exact moment** the connection closes via `xDestroy`.
- **Elite Performance**: Nanosecond-scale state retrieval using SQLite's `pApp` / `auxdata` bypass, achieving **~2ns** hot-path latency.

---

## Example: The Modern "One-Stop-Shop" Extension

This shows how cleanly you can build a shared counter extension using the core library's consolidated API.

### 1. Define your State and Registry

```rust
use sqlite_ext_core::{
    sqlite3, sqlite3_context, sqlite3_value, sqlite3_result_int64,
    sqlite3_user_data, sqlite3_extension_init2, sqlite3_create_function_v2,
    DbRegistry, State, SQLITE_OK, SQLITE_UTF8, destructor_bridge
};
use std::sync::atomic::{AtomicUsize, Ordering};
use once_cell::sync::Lazy;

pub struct SharedState {
    pub counter: AtomicUsize,
}

// Process-wide registry: maps each db path to its shared `SharedState`.
static REGISTRY: Lazy<DbRegistry<SharedState>> = Lazy::new(DbRegistry::new);
```

### 2. Implement the Scalar Function

```rust
unsafe extern "C" fn test_counter_func(
    ctx: *mut sqlite3_context,
    _argc: std::os::raw::c_int,
    _argv: *mut *mut sqlite3_value,
) {
    // Recover the shared state handle from pApp (Zero lookup overhead)
    let p_app = sqlite3_user_data(ctx);
    let state = State::<SharedState>::clone_from_raw(p_app);

    let val = state.counter.fetch_add(1, Ordering::SeqCst);

    // Clean, C-mirror API
    sqlite3_result_int64(ctx, (val + 1) as i64);
}
```

### 3. Consolidated Entry Point

```rust
#[no_mangle]
pub unsafe extern "C" fn sqlite3_myext_init(
    db: *mut sqlite3,
    _pz_err_msg: *mut *mut std::os::raw::c_char,
    p_api: *const std::os::raw::c_void,
) -> std::os::raw::c_int {
    // 1. Resolve all function pointers process-wide
    sqlite3_extension_init2(p_api);

    // 2. Initialize or retrieve shared state for this database
    let state = REGISTRY.init(None, db, || SharedState {
        counter: AtomicUsize::new(0),
    });

    // 3. Anchor state to the connection via pApp + xDestroy
    sqlite3_create_function_v2(
        db,
        b"test_counter\0".as_ptr() as *const _,
        0,
        SQLITE_UTF8,
        state.into_raw(), // Pass the Arc raw pointer as pApp
        Some(test_counter_func),
        None, None,
        Some(destructor_bridge::<SharedState>), // RAII cleanup on close
    );

    SQLITE_OK
}
```

---

## Professional Build System

The project includes a centralized build system that outputs all artifacts to a root `bin/` directory.

### Commands

- `make test`: Run standard unit tests.
- `make test-integration`: Run the Go stress tests (concurrency, isolation, and lazy loading).
- `make leak-check-integration`: Run C-based Valgrind verification (Zero leaks!).
- `make clean`: Prune all `target/` and `bin/` artifacts.

---

## Verification and Safety

Verified with a comprehensive suite of Go, C, and Rust tests covering:

- **Concurrency**: 75+ concurrent connections across multiple databases.
- **Isolation**: Strict state bounds between different database files.
- **Lazy Loading**: Dynamic extension loading on already-open connections.
- **RAII Safety**: Valgrind-confirmed zero-leak lifecycle via `destructor_bridge`.

```bash
cargo test
```

### License

MIT.
