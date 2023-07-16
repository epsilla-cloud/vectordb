#pragma once

/** Abstractions for 256-bit registers
 *
 * The objective is to separate the different interpretations of the same
 * registers (as a vector of uint8, uint16 or uint32), to provide printing
 * functions.
 */

#ifdef __AVX2__

#include "db/index/simdlib_avx2.hpp"

#elif defined(__aarch64__)

#include "db/index/simdlib_neon.hpp"

#else

// emulated = all operations are implemented as scalars
#include "db/index/simdlib_emulated.hpp"

// FIXME: make a SSE version
// is this ever going to happen? We will probably rather implement AVX512

#endif
