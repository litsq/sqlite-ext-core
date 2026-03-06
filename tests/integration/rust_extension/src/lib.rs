//! Loadable SQLite extension for integration testing.
//!
//! ## State Lifecycle
//!
//! State is initialized **once per connection** inside `sqlite3_integrationext_init`
//! and converted to a raw pointer stored as the `pApp` of the registered function.
//! SQLite calls the `xDestroy` callback (`destructor_bridge`) when the connection
//! closes, which drops the Arc and handles RAII cleanup automatically.
//!
//! The scalar function simply borrows from `pApp` — no init/lookup per invocation.

use once_cell::sync::Lazy;
use sqlite_ext_core::{
    destructor_bridge, sqlite3, sqlite3_context, sqlite3_create_function_v2,
    sqlite3_extension_init2, sqlite3_result_int64, sqlite3_user_data, sqlite3_value, DbRegistry,
    State, SQLITE_OK, SQLITE_UTF8,
};
use std::os::raw::{c_char, c_int, c_void};
use std::sync::atomic::{AtomicUsize, Ordering};

// ─── Shared state & registry ──────────────────────────────────────────────────

pub struct SharedState {
    pub counter: AtomicUsize,
}

/// Process-wide registry: maps each db path to its shared `SharedState`.
static REGISTRY: Lazy<DbRegistry<SharedState>> = Lazy::new(DbRegistry::new);

// ─── Scalar function ──────────────────────────────────────────────────────────

/// Called by SQLite for each row. Reads the per-db counter from `pApp`
/// (which was pinned at connection open time) and increments it atomically.
unsafe extern "C" fn test_counter_func(
    ctx: *mut sqlite3_context,
    _argc: c_int,
    _argv: *mut *mut sqlite3_value,
) {
    // We stored the raw State pointer as pApp; recover it without consuming it.
    let p_app = sqlite3_user_data(ctx);
    if p_app.is_null() {
        return;
    }

    let state = State::<SharedState>::clone_from_raw(p_app);
    let val = state.counter.fetch_add(1, Ordering::SeqCst);

    // Use the C-like wrapper exported from core library
    sqlite3_result_int64(ctx, (val + 1) as i64);
}

// ─── Extension entry point ────────────────────────────────────────────────────

#[no_mangle]
pub unsafe extern "C" fn sqlite3_myext_init(
    db: *mut sqlite3,
    _pz_err_msg: *mut *mut c_char,
    p_api: *const c_void,
) -> c_int {
    if p_api.is_null() {
        return 1;
    }

    // Resolve SQLite function pointers across the entire process exactly once.
    // This populates GLOBAL_API and EXTENSION_API in the core library.
    sqlite3_extension_init2(p_api);

    // Retrieve — or create — shared state for this specific database file.
    // Passing `None` for ctx since we have no context here, only a db handle.
    let state: State<SharedState> = REGISTRY.init(None, db, || SharedState {
        counter: AtomicUsize::new(0),
    });

    // Anchor the State to this connection: convert it to a raw pointer that will
    // be passed as `pApp`. SQLite will call `xDestroy` (destructor_bridge) when
    // this connection closes, which drops the Arc and triggers RAII cleanup.
    let p_app = state.into_raw();

    // Use the C-like wrapper exported from core library
    sqlite3_create_function_v2(
        db,
        b"test_counter\0".as_ptr() as *const c_char,
        0,
        SQLITE_UTF8,
        p_app,
        Some(test_counter_func),
        None,
        None,
        Some(destructor_bridge::<SharedState>),
    );

    SQLITE_OK
}
