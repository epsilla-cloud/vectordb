
macro(set_option_category name)
    set(EPSILLA_OPTION_CATEGORY ${name})
    list(APPEND "EPSILLA_OPTION_CATEGORIES" ${name})
endmacro()

macro(define_option name description default)
    option(${name} ${description} ${default})
    list(APPEND "EPSILLA_${EPSILLA_OPTION_CATEGORY}_OPTION_NAMES" ${name})
    set("${name}_OPTION_DESCRIPTION" ${description})
    set("${name}_OPTION_DEFAULT" ${default})
    set("${name}_OPTION_TYPE" "bool")
endmacro()

function(list_join lst glue out)
    if ("${${lst}}" STREQUAL "")
        set(${out} "" PARENT_SCOPE)
        return()
    endif ()

    list(GET ${lst} 0 joined)
    list(REMOVE_AT ${lst} 0)
    foreach (item ${${lst}})
        set(joined "${joined}${glue}${item}")
    endforeach ()
    set(${out} ${joined} PARENT_SCOPE)
endfunction()

macro(define_option_string name description default)
    set(${name} ${default} CACHE STRING ${description})
    list(APPEND "EPSILLA_${EPSILLA_OPTION_CATEGORY}_OPTION_NAMES" ${name})
    set("${name}_OPTION_DESCRIPTION" ${description})
    set("${name}_OPTION_DEFAULT" "\"${default}\"")
    set("${name}_OPTION_TYPE" "string")

    set("${name}_OPTION_ENUM" ${ARGN})
    list_join("${name}_OPTION_ENUM" "|" "${name}_OPTION_ENUM")
    if (NOT ("${${name}_OPTION_ENUM}" STREQUAL ""))
        set_property(CACHE ${name} PROPERTY STRINGS ${ARGN})
    endif ()
endmacro()

#----------------------------------------------------------------------
set_option_category("Thirdparty")

set(EPSILLA_DEPENDENCY_SOURCE_DEFAULT "BUNDLED")

define_option_string(EPSILLA_DEPENDENCY_SOURCE
        "Method to use for acquiring Epsilla VectorDB's build dependencies"
        "${EPSILLA_DEPENDENCY_SOURCE_DEFAULT}"
        "AUTO"
        "BUNDLED"
        "SYSTEM")

define_option(EPSILLA_VERBOSE_THIRDPARTY_BUILD
        "Show output from ExternalProjects rather than just logging to files" ON)

define_option(EPSILLA_WITH_OPENTRACING "Build with Opentracing" ON)

define_option(EPSILLA_WITH_FIU "Build with fiu" OFF)

define_option(EPSILLA_WITH_OATPP "Build with oatpp" ON)
