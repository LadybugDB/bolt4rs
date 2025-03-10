fn main() {
    // Tell cargo to link against these libraries
    println!("cargo:rustc-link-lib=stdc++");
    println!("cargo:rustc-link-lib=atomic");
}
