fn main() {
    // Tell cargo to link against these libraries

    // Check if we're on macOS
    if cfg!(target_os = "macos") {
        println!("cargo:rustc-link-lib=c++");
    } else {
        println!("cargo:rustc-link-lib=stdc++");
        println!("cargo:rustc-link-lib=atomic");
    }
}
