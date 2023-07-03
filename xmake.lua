
set_warnings("all")
add_requires("abseil",{system = false})
add_packages("abseil", {debug = false})
add_requires("fmt",{system = false})
add_packages("fmt", {debug =false})

target("bbolt_pp")
    set_kind("static")
    set_languages("c++17")
    add_syslinks("absl_time","absl_strings")
    add_files("bucket.cpp","compact.cpp","cursor.cpp","db.cpp","freelist.cpp","freelist_hmap.cpp","node.cpp","page.cpp","tx.cpp")
