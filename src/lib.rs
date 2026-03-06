//! `sqlite-ext-core` provides a high-performance, thread-safe registry for managing
//! shared state in SQLite extensions.
//!
//! ### Core Problem
//! SQLite extensions are loaded once per process, but can be used across multiple
//! database connections. This creates a "Global State" problem where different
//! databases might accidentally share data.
//!
//! ### Solution
//! This crate provides `DbRegistry<T>`, which:
//! 1. Isolates state per database file path.
//! 2. Shares state across connections to the same database.
//! 3. Automatically cleans up memory and registry entries when the last connection closes (RAII).
//! 4. Provides a nanosecond-scale $O(1)$ bypass for hot-path row operations.

use ahash::RandomState;
use dashmap::DashMap;
// libsqlite3-sys imports are used as fallbacks if GLOBAL_API is not set.

// ─── Raw SQLite FFI Types ───────────────────────────────────────────────────

/// Opaque handle to a SQLite database connection.
#[repr(C)]
pub struct sqlite3 {
    _unused: [u8; 0],
}
/// Opaque handle to a SQLite function execution context.
#[repr(C)]
pub struct sqlite3_context {
    _unused: [u8; 0],
}
/// Opaque handle to a SQLite dynamic value.
#[repr(C)]
pub struct sqlite3_value {
    _unused: [u8; 0],
}

/// Generic SQLite application callback function pointer.
pub type XFunc = Option<unsafe extern "C" fn(*mut sqlite3_context, c_int, *mut *mut sqlite3_value)>;
/// Generic SQLite application data destructor function pointer.
pub type XDestroy = Option<unsafe extern "C" fn(*mut c_void)>;

/// Function pointer type for `sqlite3_create_function_v2`.
pub type CreateFunctionV2Fn = unsafe extern "C" fn(
    db: *mut sqlite3,
    name: *const c_char,
    n_arg: c_int,
    e_text_rep: c_int,
    p_app: *mut c_void,
    x_func: XFunc,
    x_step: XFunc,
    x_final: Option<unsafe extern "C" fn(*mut sqlite3_context)>,
    x_destroy: Option<unsafe extern "C" fn(*mut c_void)>,
) -> c_int;

/// Function pointer type for `sqlite3_context_db_handle`.
pub type ContextDbHandleFn = unsafe extern "C" fn(*mut sqlite3_context) -> *mut sqlite3;
/// Function pointer type for `sqlite3_result_int64`.
pub type ResultInt64Fn = unsafe extern "C" fn(*mut sqlite3_context, i64);
/// Function pointer type for `sqlite3_result_blob`.
pub type ResultBlobFn = unsafe extern "C" fn(
    *mut sqlite3_context,
    *const c_void,
    c_int,
    Option<unsafe extern "C" fn(*mut c_void)>,
);
/// Function pointer type for `sqlite3_result_double`.
pub type ResultDoubleFn = unsafe extern "C" fn(*mut sqlite3_context, f64);
/// Function pointer type for `sqlite3_result_error`.
pub type ResultErrorFn = unsafe extern "C" fn(*mut sqlite3_context, *const c_char, c_int);
/// Function pointer type for `sqlite3_result_int`.
pub type ResultIntFn = unsafe extern "C" fn(*mut sqlite3_context, c_int);
/// Function pointer type for `sqlite3_result_null`.
pub type ResultNullFn = unsafe extern "C" fn(*mut sqlite3_context);
/// Function pointer type for `sqlite3_result_text`.
pub type ResultTextFn = unsafe extern "C" fn(
    *mut sqlite3_context,
    *const c_char,
    c_int,
    Option<unsafe extern "C" fn(*mut c_void)>,
);
/// Function pointer type for `sqlite3_user_data`.
pub type UserDataFn = unsafe extern "C" fn(*mut sqlite3_context) -> *mut c_void;

/// Function pointer type for `sqlite3_value_blob`.
pub type ValueBlobFn = unsafe extern "C" fn(*mut sqlite3_value) -> *const c_void;
/// Function pointer type for `sqlite3_value_bytes`.
pub type ValueBytesFn = unsafe extern "C" fn(*mut sqlite3_value) -> c_int;
/// Function pointer type for `sqlite3_value_double`.
pub type ValueDoubleFn = unsafe extern "C" fn(*mut sqlite3_value) -> f64;
/// Function pointer type for `sqlite3_value_int`.
pub type ValueIntFn = unsafe extern "C" fn(*mut sqlite3_value) -> c_int;
/// Function pointer type for `sqlite3_value_int64`.
pub type ValueInt64Fn = unsafe extern "C" fn(*mut sqlite3_value) -> i64;
/// Function pointer type for `sqlite3_value_numeric_type`.
pub type ValueNumericTypeFn = unsafe extern "C" fn(*mut sqlite3_value) -> c_int;
/// Function pointer type for `sqlite3_value_text`.
pub type ValueTextFn = unsafe extern "C" fn(*mut sqlite3_value) -> *const c_char;
/// Function pointer type for `sqlite3_value_type`.
pub type ValueTypeFn = unsafe extern "C" fn(*mut sqlite3_value) -> c_int;

pub const SQLITE_OK: c_int = 0;
pub const SQLITE_UTF8: c_int = 1;

// Verified p_api slot indices (for use with p_api in loadable extensions)
pub const SLOT_GET_AUXDATA: usize = 61;
pub const SLOT_SET_AUXDATA: usize = 92;
pub const SLOT_DB_FILENAME: usize = 180;
pub const SLOT_CONTEXT_DB_HANDLE: usize = 149;
pub const SLOT_RESULT_BLOB: usize = 78;
pub const SLOT_RESULT_DOUBLE: usize = 79;
pub const SLOT_RESULT_ERROR: usize = 80;
pub const SLOT_RESULT_INT: usize = 82;
pub const SLOT_RESULT_INT64: usize = 83;
pub const SLOT_RESULT_NULL: usize = 84;
pub const SLOT_RESULT_TEXT: usize = 85;
pub const SLOT_VALUE_BLOB: usize = 102;
pub const SLOT_VALUE_BYTES: usize = 103;
pub const SLOT_VALUE_DOUBLE: usize = 105;
pub const SLOT_VALUE_INT: usize = 106;
pub const SLOT_VALUE_INT64: usize = 107;
pub const SLOT_VALUE_NUMERIC_TYPE: usize = 108;
pub const SLOT_VALUE_TEXT: usize = 109;
pub const SLOT_VALUE_TYPE: usize = 113;
pub const SLOT_CREATE_FUNCTION_V2: usize = 162;
pub const SLOT_USER_DATA: usize = 101;
use small_collections::SmallString;
use std::ffi::{c_void, CStr};
use std::fmt::Debug;
use std::ops::Deref;
use std::os::raw::{c_char, c_int};
use std::sync::{Arc, Weak};

/// Dynamic API function pointers for SQLite.
/// This allows loadable extensions to work in environments (like Go) where
/// SQLite symbols are not always exported to the dynamic linker.
#[derive(Clone, Copy)]
pub struct GlobalApi {
    /// Pointer to `sqlite3_get_auxdata`.
    pub get_auxdata: unsafe extern "C" fn(*mut sqlite3_context, c_int) -> *mut c_void,
    /// Pointer to `sqlite3_set_auxdata`.
    pub set_auxdata: unsafe extern "C" fn(
        *mut sqlite3_context,
        c_int,
        *mut c_void,
        Option<unsafe extern "C" fn(*mut c_void)>,
    ),
    /// Pointer to `sqlite3_db_filename`.
    pub db_filename: unsafe extern "C" fn(*mut sqlite3, *const c_char) -> *const c_char,
}

/// Generic SQLite API function pointers commonly used by extensions.
#[derive(Clone, Copy)]
pub struct ExtensionApi {
    /// Pointer to `sqlite3_context_db_handle`.
    pub context_db_handle: ContextDbHandleFn,
    /// Pointer to `sqlite3_result_blob`.
    pub result_blob: ResultBlobFn,
    /// Pointer to `sqlite3_result_double`.
    pub result_double: ResultDoubleFn,
    /// Pointer to `sqlite3_result_error`.
    pub result_error: ResultErrorFn,
    /// Pointer to `sqlite3_result_int`.
    pub result_int: ResultIntFn,
    /// Pointer to `sqlite3_result_int64`.
    pub result_int64: ResultInt64Fn,
    /// Pointer to `sqlite3_result_null`.
    pub result_null: ResultNullFn,
    /// Pointer to `sqlite3_result_text`.
    pub result_text: ResultTextFn,
    /// Pointer to `sqlite3_user_data`.
    pub user_data: UserDataFn,
    /// Pointer to `sqlite3_value_blob`.
    pub value_blob: ValueBlobFn,
    /// Pointer to `sqlite3_value_bytes`.
    pub value_bytes: ValueBytesFn,
    /// Pointer to `sqlite3_value_double`.
    pub value_double: ValueDoubleFn,
    /// Pointer to `sqlite3_value_int`.
    pub value_int: ValueIntFn,
    /// Pointer to `sqlite3_value_int64`.
    pub value_int64: ValueInt64Fn,
    /// Pointer to `sqlite3_value_numeric_type`.
    pub value_numeric_type: ValueNumericTypeFn,
    /// Pointer to `sqlite3_value_text`.
    pub value_text: ValueTextFn,
    /// Pointer to `sqlite3_value_type`.
    pub value_type: ValueTypeFn,
    /// Pointer to `sqlite3_create_function_v2`.
    pub create_function_v2: CreateFunctionV2Fn,
}

static mut GLOBAL_API: Option<GlobalApi> = None;
static mut EXTENSION_API: Option<ExtensionApi> = None;
static API_INIT: std::sync::Once = std::sync::Once::new();

/// Initializes the global generic SQLite API pointers from the `p_api` slot array
/// provided by SQLite to loadable extensions.
///
/// This resolves both the internal `sqlite-ext-core` dependencies (like `get_auxdata`)
/// and common extension routines (like `sqlite3_create_function_v2`).
///
/// This should be called exactly once during the extension's `sqlite3_*_init` function.
///
/// # Safety
/// `p_api` must be a valid pointer to a `sqlite3_api_routines` structure provided
/// by the SQLite runtime.
#[no_mangle]
pub unsafe extern "C" fn sqlite3_extension_init2(p_api: *const c_void) {
    if p_api.is_null() {
        return;
    }

    let slots = p_api as *const usize;

    API_INIT.call_once(|| {
        GLOBAL_API = Some(GlobalApi {
            get_auxdata: std::mem::transmute(*slots.add(SLOT_GET_AUXDATA)),
            set_auxdata: std::mem::transmute(*slots.add(SLOT_SET_AUXDATA)),
            db_filename: std::mem::transmute(*slots.add(SLOT_DB_FILENAME)),
        });

        EXTENSION_API = Some(ExtensionApi {
            context_db_handle: std::mem::transmute(*slots.add(SLOT_CONTEXT_DB_HANDLE)),
            result_blob: std::mem::transmute(*slots.add(SLOT_RESULT_BLOB)),
            result_double: std::mem::transmute(*slots.add(SLOT_RESULT_DOUBLE)),
            result_error: std::mem::transmute(*slots.add(SLOT_RESULT_ERROR)),
            result_int: std::mem::transmute(*slots.add(SLOT_RESULT_INT)),
            result_int64: std::mem::transmute(*slots.add(SLOT_RESULT_INT64)),
            result_null: std::mem::transmute(*slots.add(SLOT_RESULT_NULL)),
            result_text: std::mem::transmute(*slots.add(SLOT_RESULT_TEXT)),
            user_data: std::mem::transmute(*slots.add(SLOT_USER_DATA)),
            value_blob: std::mem::transmute(*slots.add(SLOT_VALUE_BLOB)),
            value_bytes: std::mem::transmute(*slots.add(SLOT_VALUE_BYTES)),
            value_double: std::mem::transmute(*slots.add(SLOT_VALUE_DOUBLE)),
            value_int: std::mem::transmute(*slots.add(SLOT_VALUE_INT)),
            value_int64: std::mem::transmute(*slots.add(SLOT_VALUE_INT64)),
            value_numeric_type: std::mem::transmute(*slots.add(SLOT_VALUE_NUMERIC_TYPE)),
            value_text: std::mem::transmute(*slots.add(SLOT_VALUE_TEXT)),
            value_type: std::mem::transmute(*slots.add(SLOT_VALUE_TYPE)),
            create_function_v2: std::mem::transmute(*slots.add(SLOT_CREATE_FUNCTION_V2)),
        });
    });
}

/// Helper function to get the global API, primarily for internal core use.
#[doc(hidden)]
pub unsafe fn get_global_api() -> Option<GlobalApi> {
    GLOBAL_API
}

// ─── Inline C-like Wrapper Functions ──────────────────────────────────────────

/// Inline wrapper for `sqlite3_user_data` that utilizes the dynamically resolved API.
#[inline(always)]
pub unsafe fn sqlite3_user_data(ctx: *mut sqlite3_context) -> *mut c_void {
    (EXTENSION_API.unwrap().user_data)(ctx)
}

/// Inline wrapper for `sqlite3_result_blob` that utilizes the dynamically resolved API.
#[inline(always)]
pub unsafe fn sqlite3_result_blob(
    ctx: *mut sqlite3_context,
    val: *const c_void,
    len: c_int,
    destructor: Option<unsafe extern "C" fn(*mut c_void)>,
) {
    (EXTENSION_API.unwrap().result_blob)(ctx, val, len, destructor)
}

/// Inline wrapper for `sqlite3_result_double` that utilizes the dynamically resolved API.
#[inline(always)]
pub unsafe fn sqlite3_result_double(ctx: *mut sqlite3_context, val: f64) {
    (EXTENSION_API.unwrap().result_double)(ctx, val)
}

/// Inline wrapper for `sqlite3_result_error` that utilizes the dynamically resolved API.
#[inline(always)]
pub unsafe fn sqlite3_result_error(ctx: *mut sqlite3_context, val: *const c_char, len: c_int) {
    (EXTENSION_API.unwrap().result_error)(ctx, val, len)
}

/// Inline wrapper for `sqlite3_result_int` that utilizes the dynamically resolved API.
#[inline(always)]
pub unsafe fn sqlite3_result_int(ctx: *mut sqlite3_context, val: c_int) {
    (EXTENSION_API.unwrap().result_int)(ctx, val)
}

/// Inline wrapper for `sqlite3_result_int64` that utilizes the dynamically resolved API.
#[inline(always)]
pub unsafe fn sqlite3_result_int64(ctx: *mut sqlite3_context, val: i64) {
    (EXTENSION_API.unwrap().result_int64)(ctx, val)
}

/// Inline wrapper for `sqlite3_result_null` that utilizes the dynamically resolved API.
#[inline(always)]
pub unsafe fn sqlite3_result_null(ctx: *mut sqlite3_context) {
    (EXTENSION_API.unwrap().result_null)(ctx)
}

/// Inline wrapper for `sqlite3_result_text` that utilizes the dynamically resolved API.
#[inline(always)]
pub unsafe fn sqlite3_result_text(
    ctx: *mut sqlite3_context,
    val: *const c_char,
    len: c_int,
    destructor: Option<unsafe extern "C" fn(*mut c_void)>,
) {
    (EXTENSION_API.unwrap().result_text)(ctx, val, len, destructor)
}

/// Inline wrapper for `sqlite3_value_blob` that utilizes the dynamically resolved API.
#[inline(always)]
pub unsafe fn sqlite3_value_blob(val: *mut sqlite3_value) -> *const c_void {
    (EXTENSION_API.unwrap().value_blob)(val)
}

/// Inline wrapper for `sqlite3_value_bytes` that utilizes the dynamically resolved API.
#[inline(always)]
pub unsafe fn sqlite3_value_bytes(val: *mut sqlite3_value) -> c_int {
    (EXTENSION_API.unwrap().value_bytes)(val)
}

/// Inline wrapper for `sqlite3_value_double` that utilizes the dynamically resolved API.
#[inline(always)]
pub unsafe fn sqlite3_value_double(val: *mut sqlite3_value) -> f64 {
    (EXTENSION_API.unwrap().value_double)(val)
}

/// Inline wrapper for `sqlite3_value_int` that utilizes the dynamically resolved API.
#[inline(always)]
pub unsafe fn sqlite3_value_int(val: *mut sqlite3_value) -> c_int {
    (EXTENSION_API.unwrap().value_int)(val)
}

/// Inline wrapper for `sqlite3_value_int64` that utilizes the dynamically resolved API.
#[inline(always)]
pub unsafe fn sqlite3_value_int64(val: *mut sqlite3_value) -> i64 {
    (EXTENSION_API.unwrap().value_int64)(val)
}

/// Inline wrapper for `sqlite3_value_numeric_type` that utilizes the dynamically resolved API.
#[inline(always)]
pub unsafe fn sqlite3_value_numeric_type(val: *mut sqlite3_value) -> c_int {
    (EXTENSION_API.unwrap().value_numeric_type)(val)
}

/// Inline wrapper for `sqlite3_value_text` that utilizes the dynamically resolved API.
#[inline(always)]
pub unsafe fn sqlite3_value_text(val: *mut sqlite3_value) -> *const c_char {
    (EXTENSION_API.unwrap().value_text)(val)
}

/// Inline wrapper for `sqlite3_value_type` that utilizes the dynamically resolved API.
#[inline(always)]
pub unsafe fn sqlite3_value_type(val: *mut sqlite3_value) -> c_int {
    (EXTENSION_API.unwrap().value_type)(val)
}

/// Inline wrapper for `sqlite3_context_db_handle` that utilizes the dynamically resolved API.
#[inline(always)]
pub unsafe fn sqlite3_context_db_handle(ctx: *mut sqlite3_context) -> *mut sqlite3 {
    (EXTENSION_API.unwrap().context_db_handle)(ctx)
}

/// Inline wrapper for `sqlite3_create_function_v2` that utilizes the dynamically resolved API.
#[inline(always)]
pub unsafe fn sqlite3_create_function_v2(
    db: *mut sqlite3,
    z_func_name: *const c_char,
    n_arg: c_int,
    e_text_rep: c_int,
    p_app: *mut c_void,
    x_func: XFunc,
    x_step: XFunc,
    x_final: Option<unsafe extern "C" fn(*mut sqlite3_context)>,
    x_destroy: Option<unsafe extern "C" fn(*mut c_void)>,
) -> c_int {
    (EXTENSION_API.unwrap().create_function_v2)(
        db,
        z_func_name,
        n_arg,
        e_text_rep,
        p_app,
        x_func,
        x_step,
        x_final,
        x_destroy,
    )
}

/// Automatically removes the database path from the registry the moment the last
/// connection is closed using RAII.
pub struct DbRegistry<T> {
    /// The Map uses `Arc<SmallString>` keys to ensure the database path is stored exactly once in memory
    /// (Shared Identity). It stores `Weak<InternalEntry>` values so that the registry itself
    /// does not "keep alive" the state. The state remains alive only as long as at least one
    /// SQLite connection holds a strong `Arc` to it.
    map: Arc<RegistryMap<T>>,
}

/// Type alias for the internal registry map to improve readability.
type RegistryMap<T> = DashMap<Arc<SmallString<64>>, Weak<InternalEntry<T>>, RandomState>;

/// A handle to the shared state for a database connection.
///
/// This type behaves like an `Arc<T>` via `Deref`, but handles the
/// deterministic cleanup of the registry when the last handle is dropped.
///
/// It is the primary way users interact with the shared state.
#[derive(Debug, Clone)]
pub struct State<T>(pub(crate) Arc<InternalEntry<T>>);

impl<T> Deref for State<T> {
    type Target = T;
    /// Bypasses the internal wrapper to give direct access to the underlying state `T`.
    fn deref(&self) -> &Self::Target {
        &self.0.state
    }
}

impl<T: Send + Sync + 'static> State<T> {
    /// Converts this `State` handle into a raw `*mut c_void` suitable for use as
    /// a SQLite `pApp` value. The pointer owns one `Arc` reference count.
    ///
    /// **Why is this necessary?**
    /// SQLite is a C library and only understands raw pointers (`*mut c_void`). It knows nothing
    /// about Rust's ownership model, lifetimes, or `Arc` reference counts.
    /// If we tried to pass a standard Rust reference (`&State`) to SQLite, that memory would be
    /// freed as soon as the current Rust scope ends, leaving SQLite with a dangling use-after-free pointer.
    ///
    /// Internally, this uses `Arc::into_raw` which intentionally leaks the
    /// reference count so that Rust doesn't drop the memory while SQLite is still
    /// using it.
    ///
    /// Use `State::clone_from_raw` inside a scalar function to borrow from it, and
    /// `destructor_bridge::<T>` as the `xDestroy` callback to drop it when the
    /// connection closes.
    pub fn into_raw(self) -> *mut c_void {
        Arc::into_raw(self.0) as *mut c_void
    }

    /// Borrows a `State` handle from a raw `pApp` pointer obtained via `into_raw`.
    ///
    /// **Why is this necessary?**
    /// When a SQLite callback (like a scalar function) fires, it hands us back the raw `*mut c_void`
    /// pointer we gave it. To safely read our inner `T` state, we need to convert that raw pointer
    /// back into a Rust `Arc`. However, we must be extremely careful *not* to accidentally drop that
    /// Arc when our callback finishes, because SQLite still thinks it owns that pointer for future queries!
    ///
    /// # Safety
    /// `raw` must be a live pointer produced by `State::into_raw`.
    pub unsafe fn clone_from_raw(raw: *mut c_void) -> Self {
        // Reconstruct the Arc from the raw pointer to increment the reference
        // count for the duration of this wrapper's borrow.
        let arc = Arc::from_raw(raw as *const InternalEntry<T>);

        // We clone the Arc. This increases the reference count by +1.
        // This `cloned` Arc is wrapped inside the returned `State` handle.
        // When the caller (e.g., your scalar function) finishes executing, the
        // `State` variable goes out of scope, and Rust automatically drops this
        // cloned Arc, safely decrementing the reference count back down!
        let cloned = arc.clone();

        // IMPORTANT: We must forget the original Arc. If we didn't, Rust would
        // call `drop(arc)` at the end of this scope, decrementing the ref count
        // that belongs to SQLite, leading to a premature use-after-free!
        std::mem::forget(arc); // return the original ref-count to C

        State(cloned)
    }
}

/// C-compatible destructor for use as the `xDestroy` callback.
/// Drops the `Arc` that was produced by `State::into_raw`.
///
/// This is required because `State::into_raw` tells Rust to "forget" about
/// the `Arc` allocation. When SQLite is done with the connection, it calls
/// this function, which takes ownership back and allows Rust's standard
/// `Drop` machinery to finally decrement the reference count.
///
/// # Safety
/// `ptr` must be a pointer previously returned by `State::into_raw`.
pub unsafe extern "C" fn destructor_bridge<T>(ptr: *mut c_void) {
    if !ptr.is_null() {
        drop(Arc::from_raw(ptr as *const InternalEntry<T>));
    }
}

/// Internal wrapper that manages the deterministic lifecycle of a shared state.
///
/// This struct is not exposed to the user. It is wrapped by `State<T>`.
#[derive(Debug)]
struct InternalEntry<T> {
    /// The actual user-defined shared state.
    state: T,

    /// A shared pointer to the database path. This is the "Key" used for self-cleanup.
    /// By using `Arc`, we ensure the string bytes are shared with the Registry Map (Zero Duplication).
    path: Arc<SmallString<64>>,

    /// A `Weak` reference back to the Registry's internal map.
    ///
    /// WHY WEAK?
    /// If this was a strong `Arc`, we would have a circular reference (Map -> Entry -> Map)
    /// and the memory would never be freed. By using `Weak`, the entry can "talk back"
    /// to the map during destruction without causing a leak.
    map: Weak<RegistryMap<T>>,
}

impl<T> Drop for InternalEntry<T> {
    /// The core of the deterministic cleanup.
    ///
    /// When the last handle (`State<T>`) is dropped (last connection closed),
    /// this method runs immediately to purge the database path from the Registry Map.
    fn drop(&mut self) {
        // Attempt to upgrade the weak map reference. If the registry itself
        // was dropped, we don't need to do anything.
        if let Some(map) = self.map.upgrade() {
            // Remove the entry for our path, but ONLY if the value currently
            // in the map points to this specific instance. This prevents
            // race conditions with new connections.
            map.remove_if(&self.path, |_, weak| {
                weak.as_ptr() == (self as *const InternalEntry<T>)
            });
        }
    }
}

impl<T: Send + Sync + 'static> DbRegistry<T> {
    /// Creates a new, empty `DbRegistry`.
    pub fn new() -> Self {
        Self {
            map: Arc::new(DashMap::with_hasher(RandomState::new())),
        }
    }

    /// Returns the shared state for the given database connection if it exists.
    ///
    /// ### Performance Layers
    /// 1. **Hot Path ($O(1)$)**: If `ctx` is provided, it checks SQLite's auxiliary data cache.
    ///    This is nanosecond-scale fast and avoids hash map lookups entirely.
    /// 2. **Warm Path**: If cache misses, it performs a zero-allocation hash map lookup
    ///    using the database path extracted from the `db` handle.
    ///
    /// ### Safety
    /// The caller must ensure `ctx` and `db` are valid pointers provided by SQLite.
    /// Passing null or dangling pointers will cause Undefined Behavior.
    pub fn get(&self, ctx: Option<*mut sqlite3_context>, db: *mut sqlite3) -> Option<State<T>> {
        // 1. Layer 1: SQLite AuxData (Logical O(1) bypass)
        if let Some(ctx_ptr) = ctx {
            let raw_cached_ptr = unsafe {
                if let Some(api) = GLOBAL_API {
                    (api.get_auxdata)(ctx_ptr, 0)
                } else {
                    libsqlite3_sys::sqlite3_get_auxdata(
                        ctx_ptr as *mut libsqlite3_sys::sqlite3_context,
                        0,
                    )
                }
            };
            if !raw_cached_ptr.is_null() {
                // Return existing handle from SQLite's internal context memory.
                let temp_arc = unsafe { Arc::from_raw(raw_cached_ptr as *const InternalEntry<T>) };
                let state_to_return = State(temp_arc.clone());
                let _ = Arc::into_raw(temp_arc); // Maintain C-side ownership.
                return Some(state_to_return);
            }
        }

        // 2. Layer 2: Registry Hash Map lookup
        let raw_path = unsafe { get_raw_db_path(db) };
        let s = SmallString::from_str(raw_path);

        let state = if let Some(weak) = self.map.get(&s) {
            weak.upgrade().map(State)
        } else {
            None
        };

        // 3. Layer 3: Cache result back in SQLite AuxData for the next row if found
        if let (Some(state), Some(ctx_ptr)) = (&state, ctx) {
            let ptr_to_store = Arc::into_raw(state.0.clone()) as *mut c_void;
            unsafe {
                if let Some(api) = GLOBAL_API {
                    (api.set_auxdata)(ctx_ptr, 0, ptr_to_store, Some(destructor_bridge::<T>));
                } else {
                    libsqlite3_sys::sqlite3_set_auxdata(
                        ctx_ptr as *mut libsqlite3_sys::sqlite3_context,
                        0,
                        ptr_to_store,
                        Some(std::mem::transmute(
                            destructor_bridge::<T> as unsafe extern "C" fn(*mut c_void),
                        )),
                    );
                }
            }
        }

        state
    }

    /// Initializes the shared state for the given database connection if it doesn't exist.
    ///
    /// If the state already exists (either in the `auxdata` cache or the global map),
    /// it is returned immediately without executing the `init_fn` closure.
    ///
    /// ### Safety
    /// The caller must ensure `ctx` and `db` are valid pointers provided by SQLite.
    /// Passing null or dangling pointers will cause Undefined Behavior.
    pub fn init<F>(
        &self,
        ctx: Option<*mut sqlite3_context>,
        db: *mut sqlite3,
        init_fn: F,
    ) -> State<T>
    where
        F: FnOnce() -> T,
    {
        // 1. Try to get existing state first (Hot/Warm path)
        if let Some(state) = self.get(ctx, db) {
            return state;
        }

        // 2. Slow path: Initialize and insert
        let raw_path = unsafe { get_raw_db_path(db) };
        let s = SmallString::from_str(raw_path);
        let shared_path = Arc::new(s);

        let state = match self.map.entry(shared_path.clone()) {
            dashmap::mapref::entry::Entry::Occupied(mut occupied) => {
                if let Some(existing_state) = occupied.get().upgrade() {
                    State(existing_state)
                } else {
                    let entry = Arc::new(InternalEntry {
                        state: init_fn(),
                        path: shared_path,
                        map: Arc::downgrade(&self.map),
                    });
                    occupied.insert(Arc::downgrade(&entry));
                    State(entry)
                }
            }
            dashmap::mapref::entry::Entry::Vacant(vacant) => {
                let entry = Arc::new(InternalEntry {
                    state: init_fn(),
                    path: shared_path,
                    map: Arc::downgrade(&self.map),
                });
                vacant.insert(Arc::downgrade(&entry));
                State(entry)
            }
        };

        // 3. Cache in SQLite AuxData
        if let Some(ctx_ptr) = ctx {
            let ptr_to_store = Arc::into_raw(state.0.clone()) as *mut c_void;
            unsafe {
                if let Some(api) = GLOBAL_API {
                    (api.set_auxdata)(ctx_ptr, 0, ptr_to_store, Some(destructor_bridge::<T>));
                } else {
                    libsqlite3_sys::sqlite3_set_auxdata(
                        ctx_ptr as *mut libsqlite3_sys::sqlite3_context,
                        0,
                        ptr_to_store,
                        Some(std::mem::transmute(
                            destructor_bridge::<T> as unsafe extern "C" fn(*mut c_void),
                        )),
                    );
                }
            }
        }

        state
    }

    /// Explicitly removes a database entry from the registry map.
    ///
    /// Note: This does not force the memory of the shared state to be freed if
    /// other connections still hold active handles. The state will only be dropped
    /// when the last `State<T>` handle is released.
    pub fn release(&self, db_path: &str) {
        let s = SmallString::from_str(db_path);
        self.map.remove(&s);
    }
}

impl<T: Send + Sync + 'static> Default for DbRegistry<T> {
    /// Creates a new, empty `DbRegistry`.
    fn default() -> Self {
        Self::new()
    }
}

/// Helper: Extracts the precise database connection string natively from the
/// specific SQLite API using `sqlite3_db_filename()`.
///
/// This is strictly $O(1)$ and executes with ZERO memory heap-copies, returning
/// a valid `&str` slice strictly mapping back to SQLite's immutable underlying
/// process memory footprint.
///
/// When passing an explicitly NULL FFI pointer or working exclusively over an
/// in-memory DB configuration, it explicitly defaults to resolving the valid
/// internal identifier `":memory:"`.
unsafe fn get_raw_db_path<'a>(db: *mut sqlite3) -> &'a str {
    if db.is_null() {
        return ":memory:";
    }
    let z_name = b"main\0".as_ptr() as *const c_char;
    let path_ptr = if let Some(api) = GLOBAL_API {
        (api.db_filename)(db, z_name)
    } else {
        libsqlite3_sys::sqlite3_db_filename(db as *mut libsqlite3_sys::sqlite3, z_name)
    };

    if path_ptr.is_null() || *path_ptr == 0 {
        return ":memory:";
    }

    // Convert the raw C-String pointer to a Rust string slice.
    // to_str().unwrap_or checks for UTF-8 validity without allocating.
    CStr::from_ptr(path_ptr).to_str().unwrap_or(":memory:")
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;
    use std::thread;

    /// Tests that the registry can correctly initialize a state structure
    /// for a new database, and subsequent requests for the same database
    /// yield the exact same Arc without re-initialization.
    #[test]
    fn test_registry_initialization_and_retrieval() {
        let registry = DbRegistry::<usize>::new();
        let conn = rusqlite::Connection::open_in_memory().unwrap();
        let db = unsafe { conn.handle() } as *mut sqlite3;

        // 1. Initial init should create state
        let state1 = registry.init(None, db, || 42);
        assert_eq!(*state1, 42);

        // 2. Subsequent get should return the SAME state
        let state2 = registry.get(None, db).expect("State should exist");
        assert_eq!(*state2, 42);

        // 3. Verify internal map count
        assert_eq!(registry.map.len(), 1);
        assert!(Arc::ptr_eq(&state1.0, &state2.0));
    }

    /// Verifies that state can be correctly expunged from the registry
    /// when a database is closed or released, preventing memory leaks over time.
    #[test]
    fn test_registry_release() {
        let registry = DbRegistry::<usize>::new();
        let temp_file = "temp2.db";
        let conn = rusqlite::Connection::open(temp_file).unwrap();
        let db_ptr = unsafe { conn.handle() } as *mut sqlite3;

        let _val = registry.init(None, db_ptr, || 100);
        assert_eq!(registry.map.len(), 1);

        let path = unsafe { get_raw_db_path(db_ptr) };
        registry.release(path);
        assert_eq!(registry.map.len(), 0);

        drop(conn);
        let _ = std::fs::remove_file(temp_file);
    }
    /// A stress test ensuring that if dozens of threads attempt to initialize
    /// the registry for the EXACT same database pointer near-simultaneously,
    /// the state is only formally inserted once (avoiding corruption or overrides),
    /// and all threads securely acquire the same underlying atomic counter.
    #[test]
    fn test_concurrent_initialization() {
        let registry = Arc::new(DbRegistry::<AtomicUsize>::new());
        let db = std::ptr::null_mut();

        // Pre-initialize to ensure the state exists for all threads to increment
        let keeper = registry.init(None, db, || AtomicUsize::new(0));

        let mut handles = vec![];
        for _ in 0..50 {
            let reg_clone = registry.clone();
            handles.push(thread::spawn(move || {
                let state = reg_clone
                    .get(None, std::ptr::null_mut())
                    .expect("State should exist");
                state.fetch_add(1, Ordering::SeqCst);
            }));
        }

        for h in handles {
            h.join().unwrap();
        }

        assert_eq!(keeper.load(Ordering::SeqCst), 50);
        assert_eq!(registry.map.len(), 1);
    }

    /// Validates the FFI bindings to ensure we can correctly extract a
    /// physical database file path from a live `*mut sqlite3` connection pointer.
    #[test]
    fn test_get_db_path_file() {
        let temp_file = "test_core_file2.db";
        let conn = rusqlite::Connection::open(temp_file).unwrap();

        let db_ptr = unsafe { conn.handle() } as *mut sqlite3;

        let path = unsafe { get_raw_db_path(db_ptr) };
        assert!(path.ends_with(temp_file));

        drop(conn);
        let _ = std::fs::remove_file(temp_file);
    }

    /// Ensures that in-memory SQLite databases safely degrade to the
    /// fallback `":memory:"` path identifier instead of segfaulting on null pointers.
    #[test]
    fn test_get_db_path_memory() {
        let conn = rusqlite::Connection::open_in_memory().unwrap();
        let db_ptr = unsafe { conn.handle() } as *mut sqlite3;

        let path = unsafe { get_raw_db_path(db_ptr) };
        assert_eq!(path, ":memory:");
    }

    /// A critical memory-safety test confirming that the C-callback destructor
    /// gracefully decrements the `Arc` reference count when SQLite cleans up an
    /// extension context, eliminating memory leaks across the C/Rust boundary.
    #[test]
    fn test_destructor_bridge() {
        struct Dummy {
            _data: [u8; 1024],
        }

        let arc = Arc::new(InternalEntry {
            state: Dummy { _data: [0; 1024] },
            path: Arc::new(SmallString::from_str("test")),
            map: Weak::new(),
        });
        let raw_ptr = Arc::into_raw(arc.clone()) as *mut c_void;

        assert_eq!(Arc::strong_count(&arc), 2);

        unsafe {
            destructor_bridge::<Dummy>(raw_ptr);
        }

        assert_eq!(Arc::strong_count(&arc), 1);
    }

    /// Ensures default constructor initializes an empty DashMap correctly.
    #[test]
    fn test_registry_default() {
        let registry = DbRegistry::<usize>::default();
        assert_eq!(registry.map.len(), 0);
    }

    /// Verifies that extreme edge cases, like passing an explicitly null FFI pointer,
    /// handle missing or invalid database strings safely.
    #[test]
    fn test_get_raw_db_path_null() {
        let path = unsafe { get_raw_db_path(std::ptr::null_mut()) };
        assert_eq!(path, ":memory:");
    }

    /// Ensures the C-FFI destructor bridge handles null auxiliary data pointers
    /// correctly without triggering a panic or unsafe cast.
    #[test]
    fn test_destructor_bridge_null() {
        unsafe {
            destructor_bridge::<usize>(std::ptr::null_mut());
        }
    }

    extern "C" fn mock_scalar_func(
        ctx: *mut libsqlite3_sys::sqlite3_context,
        _argc: std::os::raw::c_int,
        _argv: *mut *mut libsqlite3_sys::sqlite3_value,
    ) {
        unsafe {
            // Retrieve the user-defined data (the DbRegistry) attached to this function.
            let p_app = libsqlite3_sys::sqlite3_user_data(ctx);
            // Reconstruct the registry reference from the raw pointer.
            let registry = &*(p_app as *const DbRegistry<AtomicUsize>);
            // Get the database connection handle for this specific context.
            let db_ptr = std::ptr::null_mut();
            let state = registry.init(
                Some(ctx as *mut c_void as *mut sqlite3_context),
                db_ptr as *mut c_void as *mut sqlite3,
                || AtomicUsize::new(100),
            );
            state.fetch_add(1, Ordering::SeqCst);
        }
    }

    /// Integration test bridging actual SQLite execution with the registry.
    /// This forces sqlite to evaluate the function in a SQL query, hitting
    /// both the "slow path" (initial db fetch/insertion) and the $O(1)$
    /// "fast path" (auxdata bypass cache) consecutively to guarantee 100%
    /// correct behavior of `get_fast` and auxiliary C-pointers in realistic environment.
    #[test]
    fn test_get_fast_coverage() {
        let registry = Arc::new(DbRegistry::<AtomicUsize>::new());

        // CRITICAL: Hold a strong reference in the test scope.
        // Without this, once sqlite3_finalize runs, the auxdata drops
        // the last Arc, and the registry entry becomes a dead Weak pointer.
        let _keeper = registry.init(None, std::ptr::null_mut(), || AtomicUsize::new(100));

        let mut db: *mut sqlite3 = std::ptr::null_mut();
        unsafe {
            libsqlite3_sys::sqlite3_open(
                b":memory:\0".as_ptr() as *const c_char,
                &mut db as *mut *mut sqlite3 as *mut *mut libsqlite3_sys::sqlite3,
            );

            let p_app = Arc::as_ptr(&registry) as *mut c_void;
            libsqlite3_sys::sqlite3_create_function_v2(
                db as *mut libsqlite3_sys::sqlite3,
                b"test_get_fast\0".as_ptr() as *const c_char,
                1,
                libsqlite3_sys::SQLITE_UTF8,
                p_app,
                Some(std::mem::transmute(mock_scalar_func as *const ())),
                None,
                None,
                None,
            );

            let mut stmt: *mut libsqlite3_sys::sqlite3_stmt = std::ptr::null_mut();
            libsqlite3_sys::sqlite3_prepare_v2(
                db as *mut libsqlite3_sys::sqlite3,
                b"SELECT test_get_fast(1), test_get_fast(1);\0".as_ptr() as *const c_char,
                -1,
                &mut stmt,
                std::ptr::null_mut(),
            );

            // Step: runs the function twice per row, hitting slow then fast paths
            libsqlite3_sys::sqlite3_step(stmt);
            libsqlite3_sys::sqlite3_finalize(stmt);

            let state = registry
                .get(None, std::ptr::null_mut())
                .expect("State should exist");
            assert_eq!(state.load(Ordering::Relaxed), 102);

            libsqlite3_sys::sqlite3_close(db as *mut libsqlite3_sys::sqlite3);
        }
    }

    #[test]
    fn test_deterministic_cleanup() {
        let registry = DbRegistry::<usize>::new();
        let conn = rusqlite::Connection::open_in_memory().unwrap();
        let db = unsafe { conn.handle() } as *mut sqlite3;

        // 1. Scope the state handle
        {
            let _state = registry.init(None, db, || 42);
            assert_eq!(registry.map.len(), 1);
        } // state dropped here. Deterministic cleanup happens.

        // 2. Map MUST be empty now
        assert_eq!(registry.map.len(), 0);
    }

    /// Verifies that two different database handles point to two isolated
    /// states, ensuring no state leakage between unrelated databases.
    #[test]
    fn test_isolation() {
        let registry = DbRegistry::<AtomicUsize>::new();
        let f1 = "iso1.db";
        let f2 = "iso2.db";
        let conn1 = rusqlite::Connection::open(f1).unwrap();
        let conn2 = rusqlite::Connection::open(f2).unwrap();

        let db1 = unsafe { conn1.handle() } as *mut sqlite3;
        let db2 = unsafe { conn2.handle() } as *mut sqlite3;

        let state1 = registry.init(None, db1, || AtomicUsize::new(1));
        let state2 = registry.init(None, db2, || AtomicUsize::new(2));

        assert_ne!(
            state1.load(Ordering::Relaxed),
            state2.load(Ordering::Relaxed)
        );
        assert_eq!(registry.map.len(), 2);

        drop(conn1);
        drop(conn2);
        let _ = std::fs::remove_file(f1);
        let _ = std::fs::remove_file(f2);
    }

    /// Tests that after the last handle to a state is dropped, a subsequent
    /// 'get' call correctly triggers re-initialization from scratch.
    #[test]
    fn test_reinitialization() {
        let registry = DbRegistry::<AtomicUsize>::new();
        let conn = rusqlite::Connection::open_in_memory().unwrap();
        let db = unsafe { conn.handle() } as *mut sqlite3;

        {
            let state = registry.init(None, db, || AtomicUsize::new(100));
            state.store(101, Ordering::Relaxed);
        } // Purged from map here.

        let state = registry.init(None, db, || AtomicUsize::new(200));
        assert_eq!(state.load(Ordering::Relaxed), 200); // Should be new state, not 101.
    }

    /// Verifies that if the registry itself is dropped but connections are still open,
    /// the entries (State handles) remain valid and don't crash when they are finally dropped.
    #[test]
    fn test_registry_dropped_first() {
        let registry = Box::new(DbRegistry::<AtomicUsize>::new());
        let conn = rusqlite::Connection::open_in_memory().unwrap();
        let db = unsafe { conn.handle() } as *mut sqlite3;

        let state = registry.init(None, db, || AtomicUsize::new(1));

        // Drop the registry while we still hold a 'state' handle.
        drop(registry);

        // State handle should still be valid.
        assert_eq!(state.load(Ordering::Relaxed), 1);

        // Dropping the state handle now should not crash,
        // even though it can't clean itself up from the map anymore.
        drop(state);
    }

    /// Verifies that long file paths (> 64 bytes) are handled correctly.
    #[test]
    fn test_long_path_support() {
        let registry = DbRegistry::<usize>::new();
        // A path definitely longer than 64 characters.
        let long_path = "a".repeat(128);

        // Use init directly since it handles the internal logic.
        let _handle = registry.init(None, std::ptr::null_mut(), || 42);
        registry.release(&long_path);

        // Map should have 1 entry (":memory:"). The long_path was not there.
        assert_eq!(registry.map.len(), 1);
    }

    /// This test explicitly verifies that when a `sqlite3_context` is provided,
    /// the registry uses SQLite's internal metadata cache (AuxData) to store
    /// and retrieve the state handle, achieving true $O(1)$ performance.
    #[test]
    fn test_direct_context_usage() {
        let registry = Arc::new(DbRegistry::<AtomicUsize>::new());
        let mut db: *mut sqlite3 = std::ptr::null_mut();

        unsafe {
            libsqlite3_sys::sqlite3_open(
                b":memory:\0".as_ptr() as *const c_char,
                &mut db as *mut *mut sqlite3 as *mut *mut libsqlite3_sys::sqlite3,
            );

            extern "C" fn test_func(
                ctx: *mut libsqlite3_sys::sqlite3_context,
                _argc: i32,
                _argv: *mut *mut libsqlite3_sys::sqlite3_value,
            ) {
                unsafe {
                    // Extract the DbRegistry instance originally passed to sqlite3_create_function_v2.
                    let p_app = libsqlite3_sys::sqlite3_user_data(ctx);
                    // Cast the raw pointer back to our Registry type (safe because we control p_app).
                    let registry = &*(p_app as *const DbRegistry<AtomicUsize>);
                    // Obtain the underlying sqlite3* database connection attached to this context.
                    let db = libsqlite3_sys::sqlite3_context_db_handle(ctx);

                    // 1. Initial init - should populate AuxData
                    let s1 = registry.init(
                        Some(ctx as *mut c_void as *mut sqlite3_context),
                        db as *mut c_void as *mut sqlite3,
                        || AtomicUsize::new(42),
                    );

                    // 2. Immediate get - should hit AuxData bypass (Layer 1)
                    let s2 = registry
                        .get(
                            Some(ctx as *mut c_void as *mut sqlite3_context),
                            db as *mut c_void as *mut sqlite3,
                        )
                        .expect("Should exist in context cache");

                    // 3. Verify they point to the exact same memory
                    assert!(Arc::ptr_eq(&s1.0, &s2.0));

                    // 4. Verify sqlite3_get_auxdata directly to prove the bypass mechanism
                    let raw = libsqlite3_sys::sqlite3_get_auxdata(ctx, 0);
                    assert!(!raw.is_null(), "AuxData should not be null after init");

                    s1.fetch_add(1, Ordering::SeqCst);
                    libsqlite3_sys::sqlite3_result_int(ctx, 1);
                }
            }

            let p_app = Arc::as_ptr(&registry) as *mut c_void;
            libsqlite3_sys::sqlite3_create_function_v2(
                db as *mut libsqlite3_sys::sqlite3,
                b"test_ctx_usage\0".as_ptr() as *const c_char,
                0,
                libsqlite3_sys::SQLITE_UTF8,
                p_app,
                Some(std::mem::transmute(test_func as *const ())),
                None,
                None,
                None,
            );

            let mut stmt: *mut libsqlite3_sys::sqlite3_stmt = std::ptr::null_mut();
            libsqlite3_sys::sqlite3_prepare_v2(
                db as *mut libsqlite3_sys::sqlite3,
                b"SELECT test_ctx_usage();\0".as_ptr() as *const c_char,
                -1,
                &mut stmt,
                std::ptr::null_mut(),
            );

            libsqlite3_sys::sqlite3_step(stmt);
            libsqlite3_sys::sqlite3_finalize(stmt);

            // 5. Verify RAII Cleanup: Since no other handles exist,
            // finalizing the statement (which triggers the destructor_bridge)
            // should have purged the entry from the map instantly.
            assert_eq!(registry.map.len(), 0, "Map must be empty after finalize");

            libsqlite3_sys::sqlite3_close(db as *mut libsqlite3_sys::sqlite3);
        }
    }
}
