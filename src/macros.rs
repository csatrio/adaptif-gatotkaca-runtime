//! Macros exported at the crate root.

/// Declares the process-wide runtime singleton for one concrete runtime type.
///
/// A static cannot be generic, so each consuming crate declares its own. The static is typed, so every access is a direct call
/// with no vtable. Expands to three functions at the call site:
///
/// - `install(runtime) -> &'static GatotkacaRuntime<R>`: sets the singleton. The first call wins; later calls drop their argument
///   and return the existing runtime.
/// - `get() -> &'static GatotkacaRuntime<R>`: the singleton. Panics if it is not installed.
/// - `try_get() -> Option<&'static GatotkacaRuntime<R>>`: the singleton, or `None` if it is not installed.
///
/// Invoke it inside a dedicated module so the function names stay scoped:
///
/// ```ignore
/// pub mod rt {
///     adaptif_gatotkaca_runtime::declare_runtime!(pub my_executor::MyRuntime);
/// }
///
/// rt::install(MyRuntime::create());
/// rt::get().handle().spawn(async { /* ... */ });
/// ```
#[macro_export]
macro_rules! declare_runtime {
    ($vis:vis $runtime:ty) => {
        // Behind a function so the static's name cannot clash with the caller's items.
        fn __gatotkaca_runtime_cell() -> &'static ::std::sync::OnceLock<$crate::runtime::GatotkacaRuntime<$runtime>> {
            static CELL: ::std::sync::OnceLock<$crate::runtime::GatotkacaRuntime<$runtime>> = ::std::sync::OnceLock::new();
            &CELL
        }

        /// Installs the runtime singleton. The first call wins; later calls drop `runtime` and return the existing one.
        #[allow(dead_code)]
        $vis fn install(runtime: $runtime) -> &'static $crate::runtime::GatotkacaRuntime<$runtime> {
            __gatotkaca_runtime_cell().get_or_init(|| $crate::runtime::GatotkacaRuntime::new(runtime))
        }

        /// Returns the runtime singleton.
        ///
        /// # Panics
        ///
        /// Panics if the runtime has not been installed.
        #[allow(dead_code)]
        $vis fn get() -> &'static $crate::runtime::GatotkacaRuntime<$runtime> {
            __gatotkaca_runtime_cell().get().expect("Gatotkaca runtime is not installed; call install first")
        }

        /// Returns the runtime singleton, or `None` if it has not been installed.
        #[allow(dead_code)]
        $vis fn try_get() -> Option<&'static $crate::runtime::GatotkacaRuntime<$runtime>> {
            __gatotkaca_runtime_cell().get()
        }
    };
}
